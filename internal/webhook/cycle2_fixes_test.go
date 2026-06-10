package webhook

// Cycle-2 fix tests (T14):
//   H-1b: backup S3 vaultSecret.path shape validation (logical path passes,
//         leading slash / empty rejected, explicit data/ form warns).
//   L-8:  the mutating webhook records the ACTUAL admission operation.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// vaultPathBackupCluster returns a cluster with an enabled S3 backup whose
// credentials come from the given Vault path.
func vaultPathBackupCluster(path string) *cbv1alpha1.CloudberryCluster {
	cluster := newValidCluster()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Destination: cbv1alpha1.BackupDestination{
			Type: backupDestinationTypeS3,
			S3: &cbv1alpha1.S3Destination{
				Bucket:      "bucket",
				VaultSecret: &cbv1alpha1.S3VaultSecret{Path: path},
			},
		},
		Image: "backup-image:latest",
	}
	return cluster
}

func TestValidateBackup_VaultSecretPath_LogicalPathPasses(t *testing.T) {
	var warnings admission.Warnings
	err := validateBackup(vaultPathBackupCluster("secret/cloudberry/backup-s3"), &warnings)
	require.NoError(t, err)
	assert.Empty(t, warnings, "the documented logical path must pass without warnings")
}

func TestValidateBackup_VaultSecretPath_DataFormPassesWithWarning(t *testing.T) {
	var warnings admission.Warnings
	err := validateBackup(vaultPathBackupCluster("secret/data/cloudberry/backup-s3"), &warnings)
	require.NoError(t, err, "the explicit KV-v2 request path stays accepted (back-compat)")
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "secret/cloudberry/backup-s3",
		"the warning suggests the logical form")
}

func TestValidateBackup_VaultSecretPath_LeadingSlashRejected(t *testing.T) {
	var warnings admission.Warnings
	err := validateBackup(vaultPathBackupCluster("/secret/cloudberry/backup-s3"), &warnings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not start with")
}

func TestValidateBackup_VaultSecretPath_EmptyRejected(t *testing.T) {
	// An empty vaultSecret.path with a credentialSecret present used to slip
	// through silently; an explicitly configured vaultSecret must carry a
	// non-empty path.
	cluster := vaultPathBackupCluster("")
	cluster.Spec.Backup.Destination.S3.CredentialSecret =
		&cbv1alpha1.S3CredentialSecret{Name: "s3-creds"}

	var warnings admission.Warnings
	err := validateBackup(cluster, &warnings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be empty")
}

func TestValidateS3VaultSecretPath_Table(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantErr   bool
		wantWarns int
	}{
		{"logical", "secret/foo", false, 0},
		{"logical multi-segment", "secret/a/b/c", false, 0},
		{"data form", "secret/data/foo", false, 1},
		{"data form multi-segment", "secret/data/a/b/c", false, 1},
		{"empty", "", true, 0},
		{"leading slash", "/secret/foo", true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var warnings admission.Warnings
			err := validateS3VaultSecretPath(tc.path, &warnings)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Len(t, warnings, tc.wantWarns)
		})
	}
}

// ----------------------------------------------------------------------------
// L-8: mutating webhook admission operation label
// ----------------------------------------------------------------------------

// admissionCtx returns a context carrying an admission request with the given
// operation, as controller-runtime provides to webhook handlers.
func admissionCtx(op admissionv1.Operation) context.Context {
	return admission.NewContextWithRequest(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{Operation: op},
	})
}

func TestAdmissionOperationFromContext(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{"create", admissionCtx(admissionv1.Create), admissionOpCreate},
		{"update", admissionCtx(admissionv1.Update), admissionOpUpdate},
		{"delete", admissionCtx(admissionv1.Delete), admissionOpDelete},
		{"connect maps to create", admissionCtx(admissionv1.Connect), admissionOpCreate},
		{"unknown maps to create", admissionCtx(admissionv1.Operation("FUZZ")), admissionOpCreate},
		{"no request falls back to create", context.Background(), admissionOpCreate},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, admissionOperationFromContext(tc.ctx))
		})
	}
}

// mutatingOpRecorder captures RecordWebhookAdmission label tuples.
type mutatingOpRecorder struct {
	metrics.NoopRecorder
	calls [][3]string
}

func (m *mutatingOpRecorder) RecordWebhookAdmission(webhook, operation, result string) {
	m.calls = append(m.calls, [3]string{webhook, operation, result})
}

func TestDefaulter_RecordsActualAdmissionOperation(t *testing.T) {
	rec := &mutatingOpRecorder{}
	d := NewCloudberryClusterDefaulter(rec)

	require.NoError(t, d.Default(admissionCtx(admissionv1.Update), newValidCluster()))
	require.NoError(t, d.Default(admissionCtx(admissionv1.Create), newValidCluster()))

	require.Len(t, rec.calls, 2)
	assert.Equal(t, [3]string{webhookMutating, admissionOpUpdate, admissionAllowed}, rec.calls[0],
		"an update admission must be recorded with the update operation label")
	assert.Equal(t, [3]string{webhookMutating, admissionOpCreate, admissionAllowed}, rec.calls[1])
}

func TestDefaulter_NoAdmissionRequest_FallsBackToCreate(t *testing.T) {
	rec := &mutatingOpRecorder{}
	d := NewCloudberryClusterDefaulter(rec)

	require.NoError(t, d.Default(context.Background(), newValidCluster()))
	require.Len(t, rec.calls, 1)
	assert.Equal(t, admissionOpCreate, rec.calls[0][1])
}
