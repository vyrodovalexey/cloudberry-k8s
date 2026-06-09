//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 85: All Backup REST API Endpoints (E2E)
// ============================================================================
//
// User journey: an admin drives the operator's 7 backup REST API endpoints over
// the OIDC-authed, TLS (Vault-PKI) REST API against a deployed S3-destination
// cluster (scenario85-s3 with a schedule). They list backups (85a), create a
// FULL backup (85b -> Job whose args match the gpbackupOptions including
// --leaf-partition-data), read the backup (85c), restore it (85e -> Job whose
// args match the gprestoreOptions including --data-only/--resize-cluster), delete
// it (85d -> a cleanup Job), list job statuses (85f) and read the schedule
// (85g -> CronJob + nextScheduleTime).
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity: BuildBackupJob renders --leaf-partition-data on a FULL
//     backup when gpbackupOptions.LeafPartitionData is set (the GAP-B fix), and
//     the full gpbackupOptions arg set; BuildRestoreJob renders the full
//     gprestoreOptions arg set (--data-only/--resize-cluster/--redirect-db/...);
//     BuildRetentionCleanupJob carries operation=cleanup. These are exactly the
//     Jobs the live script asserts via `kubectl get job -o jsonpath`.
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario85-api-endpoints.sh, which obtains an OIDC bearer
//     token from Keycloak, port-forwards the TLS REST API and calls all 7
//     endpoints, asserting responses + the created backup/restore/cleanup Job
//     args.
// ============================================================================

const (
	// envS85Cluster overrides the live API cluster name.
	envS85Cluster = "SCENARIO85_S3_CLUSTER"
	// envS85Script overrides the live script path.
	envS85Script = "SCENARIO85_SCRIPT"

	scenario85E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario85E2ECredSecret  = "backup-s3-credentials"
	scenario85E2EDB          = "mydb"
	scenario85E2ETS          = "20260101010101"
)

// Scenario85APIEndpointsE2ESuite tests backup/restore/cleanup Job rendering
// (builder parity) and the KUBECONFIG-gated live portion.
type Scenario85APIEndpointsE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario85(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario85APIEndpointsE2ESuite))
}

func (s *Scenario85APIEndpointsE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario85E2ECluster builds a running S3-destination backup cluster (with a
// schedule so 85g returns a CronJob) mirroring the scenario85-s3 sample CR.
func scenario85E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Image:    scenario85E2EBackupImage,
		Schedule: "0 2 * * *",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://host.docker.internal:9000",
				Region:         "us-east-1",
				Folder:         "scenario85",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           scenario85E2ECredSecret,
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return cluster
}

// --- 85.1: builder parity for 85b — full backup gpbackupOptions -> Job args ---

// TestE2E_Scenario85_BackupJobArgsParity verifies BuildBackupJob renders EVERY
// gpbackupOption flag the 85b request maps to — including --leaf-partition-data
// on a FULL backup (the GAP-B fix) — into the Job the live script asserts via
// `kubectl get job -o jsonpath`.
func (s *Scenario85APIEndpointsE2ESuite) TestE2E_Scenario85_BackupJobArgsParity() {
	cluster := scenario85E2ECluster("test-s85e2e-backup")
	b := builder.NewBuilder()

	job := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario85E2ETS,
		Type:      "full",
		Databases: []string{scenario85E2EDB},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			SingleDataFile:    true,
			CopyQueueSize:     8,
			LeafPartitionData: true,
			WithStats:         true,
			WithoutGlobals:    true,
		},
		IncludeSchemas: []string{"public"},
		ExcludeTables:  []string{"public.tmp"},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])
	assert.Equal(s.T(), "full", job.Labels[util.LabelBackupType])

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	script := job.Spec.Template.Spec.Containers[0].Args[0]
	for _, want := range []string{
		"'--dbname' '" + scenario85E2EDB + "'",
		"'--single-data-file'",
		"'--copy-queue-size' '8'",
		"'--include-schema' 'public'",
		"'--exclude-table' 'public.tmp'",
		"'--leaf-partition-data'",
		"'--with-stats'",
		"'--without-globals'",
	} {
		assert.Containsf(s.T(), script, want, "85b builder parity: must render %q", want)
	}
	// GAP-B: --leaf-partition-data exactly once on a FULL backup (NOT incremental).
	assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"),
		"85b: --leaf-partition-data must be emitted exactly once on a full backup")
	assert.NotContains(s.T(), script, "'--incremental'")
	assert.NotContains(s.T(), script, "'--jobs'")
}

// --- 85.2: builder parity for 85e — full restore gprestoreOptions -> Job args ---

// TestE2E_Scenario85_RestoreJobArgsParity verifies BuildRestoreJob renders EVERY
// gprestoreOption flag the 85e request maps to (--data-only/--resize-cluster/
// --redirect-db/--run-analyze/...), including the include-table > include-schema
// and run-analyze > with-stats exclusivity resolutions.
func (s *Scenario85APIEndpointsE2ESuite) TestE2E_Scenario85_RestoreJobArgsParity() {
	cluster := scenario85E2ECluster("test-s85e2e-restore")
	b := builder.NewBuilder()

	job := b.BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp:      scenario85E2ETS,
		Databases:      []string{scenario85E2EDB},
		RedirectDb:     "mydb_restored",
		RedirectSchema: "restored",
		IncludeSchemas: []string{"public"},
		IncludeTables:  []string{"public.users", "public.orders"},
		ExcludeTables:  []string{"public.audit"},
		Gprestore: &cbv1alpha1.GprestoreOptions{
			Jobs:            4,
			CreateDb:        true,
			WithGlobals:     true,
			WithStats:       true,
			RunAnalyze:      true,
			OnErrorContinue: true,
			TruncateTable:   true,
			DataOnly:        true,
			ResizeCluster:   true,
		},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	script := job.Spec.Template.Spec.Containers[0].Args[0]
	for _, want := range []string{
		"'--timestamp' '" + scenario85E2ETS + "'",
		"'--jobs' '4'",
		"'--redirect-db' 'mydb_restored'",
		"'--redirect-schema' 'restored'",
		"'--create-db'",
		"'--with-globals'",
		"'--run-analyze'",
		"'--on-error-continue'",
		"'--truncate-table'",
		"'--data-only'",
		"'--resize-cluster'",
		"'--include-table' 'public.users'",
		"'--include-table' 'public.orders'",
		"'--exclude-table' 'public.audit'",
	} {
		assert.Containsf(s.T(), script, want, "85e builder parity: must render %q", want)
	}
	// include-table wins over include-schema; run-analyze wins over with-stats;
	// dataOnly set (not metadataOnly) -> no --metadata-only.
	assert.NotContains(s.T(), script, "'--include-schema'")
	assert.NotContains(s.T(), script, "'--with-stats'")
	assert.NotContains(s.T(), script, "'--metadata-only'")
}

// --- 85.3: builder parity for 85d — cleanup Job operation label ---

// TestE2E_Scenario85_CleanupJobParity verifies BuildRetentionCleanupJob carries
// the avsoft.io/backup-operation=cleanup label and a gpbackman backup-delete
// script — exactly what the live script asserts after DELETE /backups/{ts}.
func (s *Scenario85APIEndpointsE2ESuite) TestE2E_Scenario85_CleanupJobParity() {
	cluster := scenario85E2ECluster("test-s85e2e-cleanup")
	b := builder.NewBuilder()

	job := b.BuildRetentionCleanupJob(cluster, scenario85E2ETS)
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupOperationCleanup, job.Labels[util.LabelBackupOperation],
		"85d: BuildRetentionCleanupJob must set avsoft.io/backup-operation=cleanup")
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	assert.Contains(s.T(), job.Spec.Template.Spec.Containers[0].Args[0], "backup-delete",
		"85d: cleanup Job must run gpbackman backup-delete")
}

// --- 85.4: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario85_LiveAPIEndpoints is the live-cluster portion. It self-skips
// when KUBECONFIG is unset so the suite never requires a real cluster, Keycloak
// or backup tooling. When live, it shells out to the scenario85 live script,
// which obtains an OIDC bearer token from Keycloak (realm test, client
// cloudberry-operator, an admin-role user), port-forwards the operator's TLS
// (Vault-PKI) REST API and calls all 7 endpoints, asserting responses + the
// created backup/restore/cleanup Job args.
func (s *Scenario85APIEndpointsE2ESuite) TestE2E_Scenario85_LiveAPIEndpoints() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live API-endpoints verification")
	}

	cluster := os.Getenv(envS85Cluster)
	if cluster == "" {
		cluster = "scenario85-s3"
	}

	script := os.Getenv(envS85Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario85-api-endpoints.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	// The live script is self-contained, idempotent and re-runnable; it drives
	// all 7 endpoints and prints a per-check PASS/FAIL summary.
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario85 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario85 live script must pass all endpoint checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
