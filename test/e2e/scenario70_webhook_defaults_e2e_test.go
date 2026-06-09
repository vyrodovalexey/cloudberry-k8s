//go:build e2e

package e2e

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 70: Webhook backup defaults tests (E2E)
// ============================================================================
//
// This suite mirrors the functional Scenario 70 defaulting cases at the e2e
// layer by exercising the defaulter directly (infra-free, deterministic). When
// a live cluster is available (KUBECONFIG set), the optional live test ALSO
// CREATES a minimal-backup CloudberryCluster against the real API server and
// READS IT BACK (Get) to assert the 12 backup defaults were PERSISTED by the
// mutating webhook on the stored object, then deletes it for cleanup. The live
// test is skipped when no cluster/KUBECONFIG is available, consistent with the
// other live-gated e2e tests.
//
// The defaulter mutates the object IN PLACE; the mutated object is what the API
// server persists, so reading the CR back surfaces the applied defaults.
// envKubeconfig is reused from the Scenario 69 e2e suite (package-scoped).
// ============================================================================

// scenario70DefaultsNamespace is the namespace used for the live defaults CR.
const scenario70DefaultsNamespace = "cloudberry-test"

// Scenario70WebhookDefaultsE2ESuite tests the backup webhook defaulting rules
// end-to-end.
type Scenario70WebhookDefaultsE2ESuite struct {
	suite.Suite
	ctx       context.Context
	defaulter *webhook.CloudberryClusterDefaulter
}

func TestE2E_Scenario70(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario70WebhookDefaultsE2ESuite))
}

func (s *Scenario70WebhookDefaultsE2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.defaulter = webhook.NewCloudberryClusterDefaulter()
}

// scenario70E2EMinimalBackupCluster builds a valid cluster with the MINIMAL
// backup spec only (enabled, image, s3 destination with bucket +
// credentialSecret.name). Gpbackup/Gprestore/Retention/JobTemplate are left
// unset so the mutating webhook must default them.
func scenario70E2EMinimalBackupCluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           "cloudberry-backups",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "backup-s3-credentials"},
			},
		},
	}
	return cluster
}

// assertScenario70E2EDefaults verifies the 12 defaulted backup fields on a
// backup spec that has been run through (or persisted by) the defaulter.
func (s *Scenario70WebhookDefaultsE2ESuite) assertScenario70E2EDefaults(backup *cbv1alpha1.BackupSpec) {
	t := s.T()

	require.NotNil(t, backup.Gpbackup, "gpbackup must be allocated by defaulter")
	assert.Equal(t, int32(1), backup.Gpbackup.CompressionLevel, "gpbackup.compressionLevel")
	assert.Equal(t, "gzip", backup.Gpbackup.CompressionType, "gpbackup.compressionType")
	assert.Equal(t, int32(1), backup.Gpbackup.Jobs, "gpbackup.jobs")
	assert.False(t, backup.Gpbackup.SingleDataFile, "gpbackup.singleDataFile")
	require.NotNil(t, backup.Gpbackup.WithStats, "gpbackup.withStats defaulted (non-nil)")
	assert.True(t, *backup.Gpbackup.WithStats, "gpbackup.withStats")

	require.NotNil(t, backup.Gprestore, "gprestore must be allocated by defaulter")
	assert.Equal(t, int32(1), backup.Gprestore.Jobs, "gprestore.jobs")
	require.NotNil(t, backup.Gprestore.WithStats, "gprestore.withStats defaulted (non-nil)")
	assert.True(t, *backup.Gprestore.WithStats, "gprestore.withStats")

	assert.Equal(t, int32(3), backup.Retention.FullCount, "retention.fullCount")
	assert.Equal(t, "30d", backup.Retention.MaxAge, "retention.maxAge")

	require.NotNil(t, backup.JobTemplate, "jobTemplate must be allocated by defaulter")
	require.NotNil(t, backup.JobTemplate.BackoffLimit, "jobTemplate.backoffLimit")
	assert.Equal(t, int32(2), *backup.JobTemplate.BackoffLimit, "jobTemplate.backoffLimit")
	require.NotNil(t, backup.JobTemplate.ActiveDeadlineSeconds, "jobTemplate.activeDeadlineSeconds")
	assert.Equal(t, int64(7200), *backup.JobTemplate.ActiveDeadlineSeconds, "jobTemplate.activeDeadlineSeconds")
	require.NotNil(t, backup.JobTemplate.TTLSecondsAfterFinished, "jobTemplate.ttlSecondsAfterFinished")
	assert.Equal(t, int32(86400), *backup.JobTemplate.TTLSecondsAfterFinished, "jobTemplate.ttlSecondsAfterFinished")
}

// TestE2E_Scenario70_DefaulterAppliesBackupDefaults exercises the defaulter
// directly for the minimal backup spec (parity with the functional suite).
func (s *Scenario70WebhookDefaultsE2ESuite) TestE2E_Scenario70_DefaulterAppliesBackupDefaults() {
	cluster := scenario70E2EMinimalBackupCluster("e2e-s70-defaults")
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))
	s.assertScenario70E2EDefaults(cluster.Spec.Backup)
}

// TestE2E_Scenario70_LiveAPIServerPersistsDefaults creates a minimal-backup CR
// against a live API server (via KUBECONFIG), reads it back, and asserts the 12
// backup defaults were persisted by the mutating webhook on the stored object,
// then deletes it. Skipped when no cluster/KUBECONFIG is available.
func (s *Scenario70WebhookDefaultsE2ESuite) TestE2E_Scenario70_LiveAPIServerPersistsDefaults() {
	kubeconfig := os.Getenv(envKubeconfig)
	if kubeconfig == "" {
		s.T().Skip("KUBECONFIG not set, skipping live API-server defaults test")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		s.T().Skipf("could not build kubeconfig %q: %v", kubeconfig, err)
	}

	scheme := testutil.NewTestK8sEnv().Scheme
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		s.T().Skipf("could not build live client: %v", err)
	}

	cluster := scenario70E2EMinimalBackupCluster("s70-defaults")
	cluster.Namespace = scenario70DefaultsNamespace

	if createErr := cl.Create(s.ctx, cluster); createErr != nil {
		s.T().Skipf("could not create CR on live API server: %v", createErr)
	}

	// Ensure cleanup of the persisted CR.
	defer func() {
		_ = cl.Delete(s.ctx, cluster)
	}()

	got := &cbv1alpha1.CloudberryCluster{}
	getErr := cl.Get(s.ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: scenario70DefaultsNamespace,
	}, got)
	require.NoError(s.T(), getErr, "persisted CR should be readable")
	require.NotNil(s.T(), got.Spec.Backup, "persisted backup spec")

	// The mutating webhook should have defaulted the 12 fields on the stored CR.
	s.assertScenario70E2EDefaults(got.Spec.Backup)
}
