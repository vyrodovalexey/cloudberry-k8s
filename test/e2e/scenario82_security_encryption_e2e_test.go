//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 82: Security and Encryption (E2E)
// ============================================================================
//
// User journey: an operator-managed S3-destination cluster keeps credentials out
// of the rendered ConfigMap (only ${...} placeholders), injects them only as env
// from a Secret, renders the resolved plugin config to an ephemeral in-pod path,
// runs backup Jobs as a dedicated minimal-RBAC SA (denied unrelated Secrets),
// flips the plugin encryption option from the CR, and propagates
// jobTemplate.imagePullSecrets to the backup Job pod spec:
//
//	82a credentials never on disk : the S3 ConfigMap carries ONLY ${...}
//	    placeholders; the running pod exposes the creds only via SecretKeyRef env.
//	82b ephemeral render          : /tmp/s3-config.yaml is rendered IN-POD at
//	    runtime (envsubst); the ConfigMap keeps placeholders.
//	82c minimal RBAC              : the backup SA CAN get backup-relevant Secrets
//	    but CANNOT get an unrelated Secret (denied).
//	82d encryption flip           : encryption on -> plugin option on; flips to
//	    off when the CR is patched (verified via the plugin option, not HTTPS).
//	82e imagePullSecrets          : the backup Job pod spec carries [regcred].
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity: BuildBackupJob for an S3 cluster with
//     jobTemplate.imagePullSecrets + encryption on carries imagePullSecrets
//     [regcred], ServiceAccountName cloudberry-backup-sa, AWS creds via
//     SecretKeyRef (no literal Value), and S3_ENCRYPTION=on; the rendered
//     ConfigMap carries placeholders only.
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario82-security-encryption.sh, which drives the full
//     82a-e lifecycle (ConfigMap placeholders + creds env, in-pod ephemeral
//     render, RBAC deny via kubectl auth can-i + an in-pod SA token, encryption
//     on->off flip via CR patch, imagePullSecrets in the Job pod spec) against a
//     running cluster.
//
// SECURITY NOTE: the builder cannot prove an SA's API authorization (82c) or the
// kubelet's runtime env injection / in-pod ephemeral render (82a A3 / 82b live).
// Those are asserted by the live script; the deterministic portion proves the
// operator's rendered spec (placeholders, SecretKeyRef, imagePullSecrets,
// encryption env) — split exactly as the live script documents.
// ============================================================================

const (
	// envS82Cluster overrides the live security/encryption cluster name.
	envS82Cluster = "SCENARIO82_S3_CLUSTER"
	// envS82Script overrides the live script path.
	envS82Script = "SCENARIO82_SCRIPT"

	scenario82E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario82E2ECredSecret  = "s3-credentials"
	scenario82E2ERegCred     = "regcred"
	scenario82E2ETS          = "20260608020000"
	scenario82E2EBackupSA    = "cloudberry-backup-sa"

	scenario82E2ES3ConfigKey = "s3-plugin-config.yaml.tpl"
)

// Scenario82SecurityEncryptionE2ESuite tests the S3 security/encryption backup
// Job rendering (builder parity) and the KUBECONFIG-gated live portion.
type Scenario82SecurityEncryptionE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario82(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario82SecurityEncryptionE2ESuite))
}

func (s *Scenario82SecurityEncryptionE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario82E2ECluster builds a running cluster with an S3 backup destination
// (encryption on, jobTemplate.imagePullSecrets [regcred]) mirroring the
// scenario82-s3 sample CR.
func scenario82E2ECluster(name, encryption string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario82E2EBackupImage,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://host.docker.internal:9000",
				Region:         "us-east-1",
				Folder:         "scenario82",
				Encryption:     encryption,
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           scenario82E2ECredSecret,
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
		JobTemplate: &cbv1alpha1.BackupJobTemplate{
			ServiceAccountName: scenario82E2EBackupSA,
			ImagePullSecrets: []cbv1alpha1.ImagePullSecret{
				{Name: scenario82E2ERegCred},
			},
		},
	}
	return cluster
}

func scenario82E2EPullSecretNames(podSpec corev1.PodSpec) []string {
	names := make([]string, 0, len(podSpec.ImagePullSecrets))
	for _, ref := range podSpec.ImagePullSecrets {
		names = append(names, ref.Name)
	}
	return names
}

// --- 82.1: builder parity (infra-free) — S3 security/encryption Job spec ---

// TestE2E_Scenario82_BackupJobParity verifies BuildBackupJob for an S3 cluster
// with encryption on + jobTemplate.imagePullSecrets carries imagePullSecrets
// [regcred], ServiceAccountName cloudberry-backup-sa, AWS creds via SecretKeyRef
// (no literal Value), and S3_ENCRYPTION=on.
func (s *Scenario82SecurityEncryptionE2ESuite) TestE2E_Scenario82_BackupJobParity() {
	cluster := scenario82E2ECluster("test-s82e2e-backup", "on")

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario82E2ETS,
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)

	podSpec := job.Spec.Template.Spec

	// 82e: imagePullSecrets present.
	assert.Equal(s.T(), []string{scenario82E2ERegCred}, scenario82E2EPullSecretNames(podSpec))
	assert.Contains(s.T(), podSpec.ImagePullSecrets,
		corev1.LocalObjectReference{Name: scenario82E2ERegCred})
	// 82c: dedicated backup SA.
	assert.Equal(s.T(), scenario82E2EBackupSA, podSpec.ServiceAccountName)

	require.NotEmpty(s.T(), podSpec.Containers)
	c := podSpec.Containers[0]

	// 82a A3 + 82d: creds via SecretKeyRef (no Value); encryption env on.
	var sawAccess, sawSecret bool
	var encryption string
	for _, e := range c.Env {
		switch e.Name {
		case "AWS_ACCESS_KEY_ID":
			sawAccess = true
			require.NotNil(s.T(), e.ValueFrom)
			require.NotNil(s.T(), e.ValueFrom.SecretKeyRef)
			assert.Equal(s.T(), scenario82E2ECredSecret, e.ValueFrom.SecretKeyRef.Name)
			assert.Empty(s.T(), e.Value)
		case "AWS_SECRET_ACCESS_KEY":
			sawSecret = true
			require.NotNil(s.T(), e.ValueFrom)
			require.NotNil(s.T(), e.ValueFrom.SecretKeyRef)
			assert.Equal(s.T(), scenario82E2ECredSecret, e.ValueFrom.SecretKeyRef.Name)
			assert.Empty(s.T(), e.Value)
		case "S3_ENCRYPTION":
			encryption = e.Value
		}
	}
	assert.True(s.T(), sawAccess, "env must contain AWS_ACCESS_KEY_ID")
	assert.True(s.T(), sawSecret, "env must contain AWS_SECRET_ACCESS_KEY")
	assert.Equal(s.T(), "on", encryption, "S3_ENCRYPTION must be on")

	// 82b: the rendered script renders the resolved config to the ephemeral path.
	require.NotEmpty(s.T(), c.Args)
	script := c.Args[0]
	assert.Contains(s.T(), script, "envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml")
}

// TestE2E_Scenario82_S3ConfigMapPlaceholdersParity verifies the rendered S3
// ConfigMap carries ONLY ${...} placeholders (no literal credential material).
func (s *Scenario82SecurityEncryptionE2ESuite) TestE2E_Scenario82_S3ConfigMapPlaceholdersParity() {
	cluster := scenario82E2ECluster("test-s82e2e-cm", "on")
	cm := builder.NewBuilder().BuildBackupS3ConfigMap(cluster)
	require.NotNil(s.T(), cm, "S3 destination must render an S3 ConfigMap")

	tpl := cm.Data[scenario82E2ES3ConfigKey]
	require.NotEmpty(s.T(), tpl)
	assert.Contains(s.T(), tpl, "aws_access_key_id: ${AWS_ACCESS_KEY_ID}")
	assert.Contains(s.T(), tpl, "aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}")
	assert.Contains(s.T(), tpl, "encryption: ${S3_ENCRYPTION}")
	assert.NotContains(s.T(), tpl, "minioadmin")
	assert.NotContains(s.T(), tpl, "AKIAIOSFODNN7EXAMPLE")
}

// TestE2E_Scenario82_EncryptionFlipParity verifies the encryption flip on the
// rendered backup Job env (on -> on, off -> off).
func (s *Scenario82SecurityEncryptionE2ESuite) TestE2E_Scenario82_EncryptionFlipParity() {
	for _, tc := range []struct {
		encryption string
		want       string
	}{
		{encryption: "on", want: "on"},
		{encryption: "off", want: "off"},
	} {
		cluster := scenario82E2ECluster("test-s82e2e-enc", tc.encryption)
		job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
			Timestamp: scenario82E2ETS,
			Databases: []string{"mydb"},
		})
		require.NotNil(s.T(), job)
		var got string
		for _, e := range job.Spec.Template.Spec.Containers[0].Env {
			if e.Name == "S3_ENCRYPTION" {
				got = e.Value
			}
		}
		assert.Equal(s.T(), tc.want, got, "S3_ENCRYPTION must flip with the CR encryption")
	}
}

// --- 82.2: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario82_LiveSecurityEncryption is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or backup tooling. When live, it shells out to the scenario82 live script,
// which drives the full 82a-e lifecycle: it asserts the S3 ConfigMap carries
// placeholders only + the running pod's creds env uses SecretKeyRef, the in-pod
// ephemeral render of /tmp/s3-config.yaml resolves the creds, the backup SA is
// denied an unrelated Secret but allowed the backup-relevant Secrets, the
// encryption plugin option flips on->off via a CR patch, and the backup Job pod
// spec carries imagePullSecrets [regcred].
func (s *Scenario82SecurityEncryptionE2ESuite) TestE2E_Scenario82_LiveSecurityEncryption() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live security/encryption verification")
	}

	cluster := os.Getenv(envS82Cluster)
	if cluster == "" {
		cluster = "scenario82-s3"
	}

	script := os.Getenv(envS82Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario82-security-encryption.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	// The live script is self-contained, idempotent and re-runnable; it drives
	// the full security/encryption lifecycle and prints a per-check PASS/FAIL
	// summary.
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario82 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario82 live script must pass all security/encryption checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
