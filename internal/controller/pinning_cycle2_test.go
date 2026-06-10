package controller

// Cycle-2 pinning tests (T13) for the protected live flows:
//
//   - backup state machine transitions driven through applyBackupJobToStatus
//     (scheduled/in-progress -> success / failed), including the de-duplicated
//     failure events and the status-struct stability across no-op reconciles;
//   - cluster TLS issuance / rotation / skip behavior (reconcileClusterTLSSecret).
//
// These tests were landed BEFORE the cycle-2 behavior fixes (T1-T12) and must
// keep passing after them: any diff here is an explicit, reviewed change.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// pinningBackupJob returns an operator-shaped backup Job in the given terminal
// or running state for the pinning cluster.
func pinningBackupJob(name, operation string, succeeded, failed int32) *batchv1.Job {
	start := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	completion := metav1.NewTime(start.Add(5 * time.Minute))
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{util.LabelBackupOperation: operation},
		},
		Status: batchv1.JobStatus{
			Succeeded: succeeded,
			Failed:    failed,
			StartTime: &start,
		},
	}
	if succeeded > 0 {
		job.Status.CompletionTime = &completion
	}
	return job
}

// pinningAdminReconciler builds an AdminReconciler with fakes for the backup
// status pinning tests.
func pinningAdminReconciler(t *testing.T) (*AdminReconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := newTestScheme()
	recorder := record.NewFakeRecorder(20)
	r := NewAdminReconciler(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme, recorder, builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil,
	)
	return r, recorder
}

// countEventsWithReason counts events containing the given reason substring.
func countEventsWithReason(events []string, reason string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, reason) {
			n++
		}
	}
	return n
}

// TestPin_BackupStatus_TransitionsAndStability pins the backup state machine:
// an in-progress Job populates status, a success transition updates it, and a
// repeated reconcile of the SAME Job+status leaves the entire status struct
// byte-identical (no churn on no-op reconciles).
func TestPin_BackupStatus_TransitionsAndStability(t *testing.T) {
	r, _ := pinningAdminReconciler(t)
	cluster := newTestCluster()

	running := pinningBackupJob("test-cluster-backup-20260101120000", util.BackupOperationBackup, 0, 0)
	r.applyBackupJobToStatus(cluster, running, backupStatusInProgress)
	assert.Equal(t, backupStatusInProgress, cluster.Status.LastBackupStatus)
	assert.Equal(t, running.Name, cluster.Status.LastBackupJobName)
	assert.Equal(t, "20260101120000", cluster.Status.LastBackupTimestamp)

	// Transition to success.
	done := pinningBackupJob("test-cluster-backup-20260101120000", util.BackupOperationBackup, 1, 0)
	r.applyBackupJobToStatus(cluster, done, backupStatusSuccess)
	assert.Equal(t, backupStatusSuccess, cluster.Status.LastBackupStatus)
	require.NotEmpty(t, cluster.Status.BackupHistory)
	assert.Equal(t, backupStatusSuccess, cluster.Status.BackupHistory[0].Status)

	// No-op reconcile: same Job, same status — the status struct must be
	// identical before/after (history is deduplicated by timestamp).
	before := cluster.Status.DeepCopy()
	r.applyBackupJobToStatus(cluster, done, backupStatusSuccess)
	assert.True(t, equality.Semantic.DeepEqual(*before, cluster.Status),
		"status must be unchanged on a no-op reconcile of the same Job+status")
}

// TestPin_BackupFailureEvent_DeDuplicated pins the existing event gate: a
// backup Job transitioning into Failed emits exactly ONE Warning Event, and
// periodic reconciles of the unchanged failed Job emit none.
func TestPin_BackupFailureEvent_DeDuplicated(t *testing.T) {
	r, recorder := pinningAdminReconciler(t)
	cluster := newTestCluster()

	failed := pinningBackupJob("test-cluster-backup-20260101130000", util.BackupOperationBackup, 0, 1)
	for i := 0; i < 3; i++ {
		r.applyBackupJobToStatus(cluster, failed, backupStatusFailed)
	}

	events := drainEvents(recorder)
	assert.Equal(t, 1, countEventsWithReason(events, cbv1alpha1.EventReasonBackupFailed),
		"exactly one BackupFailed event for repeated reconciles of the same failed Job")
}

// TestPin_RestoreFailureEvent_DeDuplicated pins the restore-side event gate.
func TestPin_RestoreFailureEvent_DeDuplicated(t *testing.T) {
	r, recorder := pinningAdminReconciler(t)
	cluster := newTestCluster()

	failed := pinningBackupJob("test-cluster-restore-20260101140000", util.BackupOperationRestore, 0, 1)
	for i := 0; i < 3; i++ {
		r.applyBackupJobToStatus(cluster, failed, backupStatusFailed)
	}

	events := drainEvents(recorder)
	assert.Equal(t, 1, countEventsWithReason(events, cbv1alpha1.EventReasonRestoreFailed),
		"exactly one RestoreFailed event for repeated reconciles of the same failed Job")
}

// TestPin_ClusterTLS_IssuanceCreatesSecret pins the issuance path: a cluster
// requesting auto-issuance with no existing Secret gets an operator-labeled
// Opaque Secret with ca.crt/tls.crt/tls.key and a ClusterTLSIssued event.
func TestPin_ClusterTLS_IssuanceCreatesSecret(t *testing.T) {
	vc := pkiVaultClient()
	r, recorder := tlsTestReconciler(vc)
	cluster := tlsTestCluster()

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))

	secret := getTLSSecret(t, r)
	assert.Equal(t, corev1.SecretTypeOpaque, secret.Type)
	assert.Equal(t, "CERT-PEM", string(secret.Data[secretKeyTLSCert]))
	assert.Equal(t, "KEY-PEM", string(secret.Data[secretKeyTLSKey]))
	assert.Equal(t, "CA-PEM", string(secret.Data[secretKeyCACert]))
	assert.True(t, isOperatorManagedTLSSecret(secret))

	events := drainEvents(recorder)
	assert.Equal(t, 1, countEventsWithReason(events, cbv1alpha1.EventReasonClusterTLSIssued))
}

// TestPin_ClusterTLS_ValidSecretNotTouched pins the no-op path: an
// operator-managed Secret with a far-from-expiry certificate is left
// completely unchanged (no update, no event) on repeated reconciles.
func TestPin_ClusterTLS_ValidSecretNotTouched(t *testing.T) {
	vc := pkiVaultClient()
	r, recorder := tlsTestReconciler(vc)
	cluster := tlsTestCluster()

	certPEM := selfSignedPEM(t, time.Now().Add(-time.Hour), time.Now().Add(365*24*time.Hour))
	existing := operatorManagedTLSSecret(certPEM)
	require.NoError(t, r.client.Create(context.Background(), existing))
	created := getTLSSecret(t, r)

	for i := 0; i < 3; i++ {
		require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))
	}

	after := getTLSSecret(t, r)
	assert.Equal(t, created.ResourceVersion, after.ResourceVersion,
		"valid operator-managed TLS secret must not be updated on no-op reconciles")
	assert.Empty(t, vc.writePath, "no PKI issue call for a valid certificate")
	assert.Empty(t, drainEvents(recorder))
}

// TestPin_ClusterTLS_RotationRenewsOnce pins the rotation path: a certificate
// past the rotation threshold is renewed (data replaced from Vault PKI) with a
// ClusterTLSRenewed event.
func TestPin_ClusterTLS_RotationRenewsOnce(t *testing.T) {
	vc := pkiVaultClient()
	r, recorder := tlsTestReconciler(vc)
	cluster := tlsTestCluster()

	// Certificate at >2/3 of its lifetime: rotation threshold passed.
	certPEM := selfSignedPEM(t, time.Now().Add(-10*24*time.Hour), time.Now().Add(time.Hour))
	require.NoError(t, r.client.Create(context.Background(), operatorManagedTLSSecret(certPEM)))

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))

	secret := getTLSSecret(t, r)
	assert.Equal(t, "CERT-PEM", string(secret.Data[secretKeyTLSCert]),
		"rotated secret must carry the freshly issued certificate")
	events := drainEvents(recorder)
	assert.Equal(t, 1, countEventsWithReason(events, cbv1alpha1.EventReasonClusterTLSRenewed))
}

// TestPin_ClusterTLS_UserProvidedSecretSkipped pins the skip path: a Secret
// without the operator labels is NEVER touched, even when expired.
func TestPin_ClusterTLS_UserProvidedSecretSkipped(t *testing.T) {
	vc := pkiVaultClient()
	r, recorder := tlsTestReconciler(vc)
	cluster := tlsTestCluster()

	certPEM := selfSignedPEM(t, time.Now().Add(-48*time.Hour), time.Now().Add(-time.Hour))
	userSecret := operatorManagedTLSSecret(certPEM)
	userSecret.Labels = nil // user-provided: no operator labels
	require.NoError(t, r.client.Create(context.Background(), userSecret))
	created := getTLSSecret(t, r)

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))

	after := getTLSSecret(t, r)
	assert.Equal(t, created.ResourceVersion, after.ResourceVersion,
		"user-provided TLS secret must never be modified")
	assert.Empty(t, vc.writePath)
	assert.Empty(t, drainEvents(recorder))
}

// TestPin_ClusterTLS_NoVaultClientFails pins the failure path: a cluster
// requesting auto-issuance with no operator Vault client gets an error and a
// ClusterTLSFailed Warning event.
func TestPin_ClusterTLS_NoVaultClientFails(t *testing.T) {
	r, recorder := tlsTestReconciler(nil)
	cluster := tlsTestCluster()

	err := r.reconcileClusterTLSSecret(context.Background(), cluster)
	require.Error(t, err)
	events := drainEvents(recorder)
	assert.Equal(t, 1, countEventsWithReason(events, cbv1alpha1.EventReasonClusterTLSFailed))
}
