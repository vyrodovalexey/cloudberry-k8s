package builder

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// newBackupCluster returns a test cluster with an S3 backup configured.
func newBackupCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    "cloudberry-backup:2.1.0",
		Retention: cbv1alpha1.BackupRetention{
			FullCount: 3,
			MaxAge:    "30d",
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:   "cloudberry-backups",
				Endpoint: "http://minio:9000",
				Region:   "us-east-1",
				Folder:   "/backups",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name: "backup-s3-credentials",
				},
			},
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 6,
			CompressionType:  "gzip",
			Jobs:             4,
			WithStats:        util.Ptr(true),
		},
		Gprestore: &cbv1alpha1.GprestoreOptions{
			Jobs:      4,
			WithStats: util.Ptr(true),
		},
	}
	return cluster
}

// mustGpbackupArgs renders gpbackup args for tests, applying the builder's
// database defaulting (withDefaultBackupDatabase, as the Job/CronJob build
// paths do) and failing the test on a render error.
func mustGpbackupArgs(
	t *testing.T,
	cluster *cbv1alpha1.CloudberryCluster,
	opts *cbv1alpha1.GpbackupOptions,
	jobOpts *BackupJobOptions,
) []string {
	t.Helper()
	args, err := buildGpbackupArgs(cluster, opts, withDefaultBackupDatabase(jobOpts))
	require.NoError(t, err)
	return args
}

func TestBuildGpbackupArgs(t *testing.T) {
	t.Run("compression level and jobs", func(t *testing.T) {
		args := mustGpbackupArgs(t, newBackupCluster(), &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 6,
			CompressionType:  "zstd",
			Jobs:             4,
			WithStats:        util.Ptr(true),
		}, nil)
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--compression-level 6")
		assert.Contains(t, joined, "--compression-type zstd")
		assert.Contains(t, joined, "--jobs 4")
		assert.Contains(t, joined, "--with-stats")
		assert.NotContains(t, joined, "--single-data-file")
	})

	t.Run("single data file excludes jobs and includes copy queue", func(t *testing.T) {
		args := mustGpbackupArgs(t, newBackupCluster(), &cbv1alpha1.GpbackupOptions{
			SingleDataFile: true,
			CopyQueueSize:  4,
			Jobs:           4,
		}, nil)
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--single-data-file")
		assert.Contains(t, joined, "--copy-queue-size 4")
		assert.NotContains(t, joined, "--jobs")
	})

	t.Run("no compression overrides level", func(t *testing.T) {
		args := mustGpbackupArgs(t, newBackupCluster(), &cbv1alpha1.GpbackupOptions{
			NoCompression:    true,
			CompressionLevel: 9,
		}, nil)
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--no-compression")
		assert.NotContains(t, joined, "--compression-level")
	})

	t.Run("incremental with leaf partition and from timestamp", func(t *testing.T) {
		args := mustGpbackupArgs(t, newBackupCluster(), &cbv1alpha1.GpbackupOptions{
			Incremental:       true,
			LeafPartitionData: true,
		}, &BackupJobOptions{FromTimestamp: "20260518020000"})
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--incremental")
		assert.Contains(t, joined, "--leaf-partition-data")
		assert.Contains(t, joined, "--from-timestamp 20260518020000")
	})

	t.Run("include schema and exclude table and dbname", func(t *testing.T) {
		args := mustGpbackupArgs(t, newBackupCluster(), &cbv1alpha1.GpbackupOptions{}, &BackupJobOptions{
			Databases:      []string{"mydb"},
			IncludeSchemas: []string{"public", "analytics"},
			ExcludeTables:  []string{"public.temp_data"},
		})
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--dbname mydb")
		assert.Contains(t, joined, "--include-schema public")
		assert.Contains(t, joined, "--include-schema analytics")
		assert.Contains(t, joined, "--exclude-table public.temp_data")
	})

	t.Run("nil options does not panic", func(t *testing.T) {
		args := mustGpbackupArgs(t, newBackupCluster(), nil, nil)
		assert.Contains(t, strings.Join(args, " "), pluginConfigFlag)
	})
}

// TestBuildGpbackupArgs_EmptyDbnameError pins the builder-level guard: an
// unresolved target database is a build-time ERROR (gpbackup hard-requires
// --dbname), never a silently broken command.
func TestBuildGpbackupArgs_EmptyDbnameError(t *testing.T) {
	tests := []struct {
		name    string
		jobOpts *BackupJobOptions
	}{
		{"nil job options", nil},
		{"empty databases slice", &BackupJobOptions{Databases: []string{}}},
		{"empty first database", &BackupJobOptions{Databases: []string{""}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args, err := buildGpbackupArgs(newBackupCluster(), nil, tc.jobOpts)
			require.Error(t, err)
			assert.Nil(t, args)
			assert.Contains(t, err.Error(), "no target database specified")
		})
	}
}

// TestWithDefaultBackupDatabase pins the database-defaulting semantics shared
// by BuildBackupJob and BuildBackupCronJob.
func TestWithDefaultBackupDatabase(t *testing.T) {
	t.Run("nil opts default to the coordinator database", func(t *testing.T) {
		opts := withDefaultBackupDatabase(nil)
		require.NotNil(t, opts)
		assert.Equal(t, []string{defaultCoordinatorDatabase}, opts.Databases)
	})

	t.Run("explicit databases are preserved", func(t *testing.T) {
		in := &BackupJobOptions{Databases: []string{"mydb"}}
		assert.Same(t, in, withDefaultBackupDatabase(in))
	})

	t.Run("caller's empty opts are not mutated", func(t *testing.T) {
		in := &BackupJobOptions{Timestamp: "20260101010101"}
		out := withDefaultBackupDatabase(in)
		assert.Empty(t, in.Databases, "the caller's value must stay untouched")
		assert.Equal(t, []string{defaultCoordinatorDatabase}, out.Databases)
		assert.Equal(t, in.Timestamp, out.Timestamp)
	})
}

// TestBuildBackupJob_DefaultsDatabase verifies an on-demand Job built with no
// databases still renders a VALID gpbackup command targeting the coordinator
// database, with CBDB_DATABASE mirroring the rendered --dbname.
func TestBuildBackupJob_DefaultsDatabase(t *testing.T) {
	b := NewBuilder()
	job := b.BuildBackupJob(newBackupCluster(), &BackupJobOptions{Timestamp: "20260101020000"})
	require.NotNil(t, job)

	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "'--dbname' '"+defaultCoordinatorDatabase+"'")

	vals := envValueMap(job.Spec.Template.Spec.Containers[0].Env)
	assert.Equal(t, defaultCoordinatorDatabase, vals[envCBDBDatabase])
}

// TestBuildBackupCronJob_RendersDbname pins the scheduled-backup fix: the
// CronJob's gpbackup command MUST carry --dbname (gpbackup aborts without it),
// defaulted to the coordinator database since the CRD has no database field.
func TestBuildBackupCronJob_RendersDbname(t *testing.T) {
	b := NewBuilder()
	cj := b.BuildBackupCronJob(newBackupCluster())
	require.NotNil(t, cj)

	script := cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "'--dbname' '"+defaultCoordinatorDatabase+"'")
}

func TestBuildGprestoreArgs(t *testing.T) {
	// gprestore forbids --include-schema together with --include-table. When
	// BOTH filters are supplied the builder emits --include-table (table-level
	// precedence) and OMITS --include-schema.
	t.Run("both filters: include-table wins, include-schema omitted", func(t *testing.T) {
		args := buildGprestoreArgs(newBackupCluster(), &cbv1alpha1.GprestoreOptions{
			Jobs:            4,
			CreateDb:        true,
			WithStats:       util.Ptr(true),
			RunAnalyze:      true,
			OnErrorContinue: true,
			TruncateTable:   true,
		}, &RestoreJobOptions{
			Timestamp:      "20260519020000",
			RedirectDb:     "mydb_restored",
			RedirectSchema: "restored",
			IncludeSchemas: []string{"public"},
			IncludeTables:  []string{"public.users"},
		})
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--timestamp 20260519020000")
		assert.Contains(t, joined, "--jobs 4")
		assert.Contains(t, joined, "--create-db")
		// gprestore forbids --run-analyze together with --with-stats. With both
		// set the builder emits --run-analyze (precedence) and OMITS
		// --with-stats so the gprestore invocation stays valid.
		assert.Contains(t, joined, "--run-analyze")
		assert.NotContains(t, joined, "--with-stats")
		assert.Contains(t, joined, "--on-error-continue")
		assert.Contains(t, joined, "--truncate-table")
		assert.Contains(t, joined, "--redirect-db mydb_restored")
		assert.Contains(t, joined, "--redirect-schema restored")
		assert.Contains(t, joined, "--include-table public.users")
		assert.NotContains(t, joined, "--include-schema")
	})

	// When ONLY includeSchemas is set, --include-schema is emitted per schema.
	t.Run("schema-only: include-schema emitted, no include-table", func(t *testing.T) {
		args := buildGprestoreArgs(newBackupCluster(), &cbv1alpha1.GprestoreOptions{Jobs: 4}, &RestoreJobOptions{
			Timestamp:      "20260519020000",
			IncludeSchemas: []string{"public", "analytics"},
		})
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--include-schema public")
		assert.Contains(t, joined, "--include-schema analytics")
		assert.NotContains(t, joined, "--include-table")
	})

	// When ONLY withStats is set (runAnalyze=false), --with-stats is emitted and
	// --run-analyze is absent (complementary to the mutual-exclusivity rule).
	t.Run("with-stats only: with-stats emitted, no run-analyze", func(t *testing.T) {
		args := buildGprestoreArgs(newBackupCluster(), &cbv1alpha1.GprestoreOptions{
			Jobs:      4,
			WithStats: util.Ptr(true),
		}, &RestoreJobOptions{Timestamp: "20260519020000"})
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--with-stats")
		assert.NotContains(t, joined, "--run-analyze")
	})
}

func TestBuildBackupS3ConfigMap(t *testing.T) {
	b := NewBuilder()

	t.Run("s3 destination produces configmap", func(t *testing.T) {
		cm := b.BuildBackupS3ConfigMap(newBackupCluster())
		require.NotNil(t, cm)
		assert.Equal(t, util.BackupS3ConfigMapName("test-cluster"), cm.Name)
		assert.Contains(t, cm.Data, s3ConfigTemplateKey)
		assert.Contains(t, cm.Data[s3ConfigTemplateKey], "gpbackup_s3_plugin")
		assert.Contains(t, cm.Data[s3ConfigTemplateKey], "${S3_BUCKET}")
		require.Len(t, cm.OwnerReferences, 1)
	})

	t.Run("non-s3 destination returns nil", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Destination.Type = "local"
		cluster.Spec.Backup.Destination.S3 = nil
		assert.Nil(t, b.BuildBackupS3ConfigMap(cluster))
	})

	t.Run("config template omits aws_signature_version", func(t *testing.T) {
		// aws_signature_version was intentionally removed: the version-matched
		// gpbackup_s3_plugin (2.1.0-incubating) rejects the unknown field. SigV4
		// is the default for both AWS and MinIO, so the template must NOT emit it.
		cm := b.BuildBackupS3ConfigMap(newBackupCluster())
		require.NotNil(t, cm)
		assert.NotContains(t, cm.Data[s3ConfigTemplateKey], "aws_signature_version")
		assert.NotContains(t, cm.Data[s3ConfigTemplateKey], "S3_AWS_SIGNATURE_VERSION")
		assert.Contains(t, cm.Data[s3ConfigTemplateKey], "endpoint: ${S3_ENDPOINT}")
		// executablepath stays pinned to the canonical /usr/local/bin path.
		assert.Contains(t, cm.Data[s3ConfigTemplateKey],
			"executablepath: /usr/local/bin/gpbackup_s3_plugin")
	})
}

func TestBuildS3EnvForcePathStyle(t *testing.T) {
	t.Run("force path style true with endpoint", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Destination.S3.ForcePathStyle = true
		env := buildS3Env(cluster, cluster.Spec.Backup.Destination.S3)

		vals := envMap(env)
		assert.Equal(t, "true", vals["S3_FORCE_PATH_STYLE"])
		// S3_AWS_SIGNATURE_VERSION was intentionally removed (SigV4 is the
		// default for the version-matched plugin); buildS3Env must NOT emit it.
		_, hasSigVer := vals["S3_AWS_SIGNATURE_VERSION"]
		assert.False(t, hasSigVer, "S3_AWS_SIGNATURE_VERSION must not be set")
		assert.Equal(t, "http://minio:9000", vals["S3_ENDPOINT"])
	})

	t.Run("force path style false default", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Destination.S3.ForcePathStyle = false
		env := buildS3Env(cluster, cluster.Spec.Backup.Destination.S3)
		assert.Equal(t, "false", envMap(env)["S3_FORCE_PATH_STYLE"])
	})

	t.Run("nil s3 returns nil", func(t *testing.T) {
		assert.Nil(t, buildS3Env(newBackupCluster(), nil))
	})
}

func TestBuildBackupEnvVaultSecretCredentials(t *testing.T) {
	cluster := newBackupCluster()
	// Vault-based credentials: no CredentialSecret, only VaultSecret.
	cluster.Spec.Backup.Destination.S3.CredentialSecret = nil
	cluster.Spec.Backup.Destination.S3.VaultSecret = &cbv1alpha1.S3VaultSecret{
		Path: "secret/data/cloudberry/backup-s3",
	}

	env := buildBackupEnv(cluster)
	wantSecret := util.BackupS3VaultCredentialsSecretName(cluster.Name)

	var hasAccessKey, hasSecretKey bool
	for _, e := range env {
		if e.Name == "AWS_ACCESS_KEY_ID" {
			require.NotNil(t, e.ValueFrom)
			require.NotNil(t, e.ValueFrom.SecretKeyRef)
			assert.Equal(t, wantSecret, e.ValueFrom.SecretKeyRef.Name)
			assert.Equal(t, "aws_access_key_id", e.ValueFrom.SecretKeyRef.Key)
			hasAccessKey = true
		}
		if e.Name == "AWS_SECRET_ACCESS_KEY" {
			require.NotNil(t, e.ValueFrom)
			require.NotNil(t, e.ValueFrom.SecretKeyRef)
			assert.Equal(t, wantSecret, e.ValueFrom.SecretKeyRef.Name)
			assert.Equal(t, "aws_secret_access_key", e.ValueFrom.SecretKeyRef.Key)
			hasSecretKey = true
		}
	}
	assert.True(t, hasAccessKey, "expected AWS_ACCESS_KEY_ID from materialized vault secret")
	assert.True(t, hasSecretKey, "expected AWS_SECRET_ACCESS_KEY from materialized vault secret")
}

func TestResolveS3CredentialSource(t *testing.T) {
	cluster := newBackupCluster()

	t.Run("credential secret takes precedence with custom fields", func(t *testing.T) {
		s3 := &cbv1alpha1.S3Destination{
			CredentialSecret: &cbv1alpha1.S3CredentialSecret{
				Name:           "my-creds",
				AccessKeyField: "ak",
				SecretKeyField: "sk",
			},
			VaultSecret: &cbv1alpha1.S3VaultSecret{Path: "secret/data/x"},
		}
		name, ak, sk := resolveS3CredentialSource(cluster, s3)
		assert.Equal(t, "my-creds", name)
		assert.Equal(t, "ak", ak)
		assert.Equal(t, "sk", sk)
	})

	t.Run("vault secret used when no credential secret", func(t *testing.T) {
		s3 := &cbv1alpha1.S3Destination{
			VaultSecret: &cbv1alpha1.S3VaultSecret{Path: "secret/data/x"},
		}
		name, ak, sk := resolveS3CredentialSource(cluster, s3)
		assert.Equal(t, util.BackupS3VaultCredentialsSecretName(cluster.Name), name)
		assert.Equal(t, "aws_access_key_id", ak)
		assert.Equal(t, "aws_secret_access_key", sk)
	})

	t.Run("neither set returns empty", func(t *testing.T) {
		name, _, _ := resolveS3CredentialSource(cluster, &cbv1alpha1.S3Destination{})
		assert.Empty(t, name)
	})

	t.Run("empty vault path returns empty", func(t *testing.T) {
		s3 := &cbv1alpha1.S3Destination{VaultSecret: &cbv1alpha1.S3VaultSecret{Path: ""}}
		name, _, _ := resolveS3CredentialSource(cluster, s3)
		assert.Empty(t, name)
	})
}

// envMap converts a slice of EnvVars to a name->value map for plain-value vars.
func envMap(env []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

func TestBuildBackupCronJob(t *testing.T) {
	b := NewBuilder()

	t.Run("schedule produces cronjob", func(t *testing.T) {
		cj := b.BuildBackupCronJob(newBackupCluster())
		require.NotNil(t, cj)
		assert.Equal(t, util.BackupCronJobName("test-cluster"), cj.Name)
		assert.Equal(t, "0 2 * * *", cj.Spec.Schedule)
		require.NotNil(t, cj.Spec.SuccessfulJobsHistoryLimit)
		assert.Equal(t, int32(3), *cj.Spec.SuccessfulJobsHistoryLimit)
		container := cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
		assert.Equal(t, "cloudberry-backup:2.1.0", container.Image)
		assert.Equal(t, backupContainerName, container.Name)
	})

	t.Run("empty schedule returns nil", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Schedule = ""
		assert.Nil(t, b.BuildBackupCronJob(cluster))
	})
}

func TestBuildBackupJob(t *testing.T) {
	b := NewBuilder()
	job := b.BuildBackupJob(newBackupCluster(), &BackupJobOptions{
		Timestamp: "20260519020000",
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)
	assert.Equal(t, util.BackupJobName("test-cluster", "20260519020000"), job.Name)
	assert.Equal(t, util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])
	assert.Equal(t, util.BackupServiceAccountName("test-cluster"),
		job.Spec.Template.Spec.ServiceAccountName)
}

func TestBuildRestoreJob(t *testing.T) {
	b := NewBuilder()
	job := b.BuildRestoreJob(newBackupCluster(), &RestoreJobOptions{
		Timestamp: "20260519020000",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)
	assert.Equal(t, util.RestoreJobName("test-cluster", "20260519020000"), job.Name)
	assert.Equal(t, util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])
	assert.Equal(t, restoreContainerName, job.Spec.Template.Spec.Containers[0].Name)
}

// containerEnvValue returns the value of the named env var on the container, or
// "" when absent.
func containerEnvValue(c corev1.Container, name string) string {
	for _, env := range c.Env {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}

// TestBuildRestoreJobS3FolderOverride asserts that S3FolderOverride redirects the
// restore Job's S3_FOLDER env (and thus the rendered plugin config "folder:") to
// the override, which is the cross-cluster migration fix. Without an override the
// cluster's own folder is used (regression guard).
func TestBuildRestoreJobS3FolderOverride(t *testing.T) {
	b := NewBuilder()

	t.Run("override redirects S3_FOLDER", func(t *testing.T) {
		job := b.BuildRestoreJob(newBackupCluster(), &RestoreJobOptions{
			Timestamp:        "20260519020000",
			S3FolderOverride: "scenario87-src",
		})
		require.NotNil(t, job)
		assert.Equal(t, "scenario87-src",
			containerEnvValue(job.Spec.Template.Spec.Containers[0], "S3_FOLDER"))
	})

	t.Run("no override keeps cluster folder", func(t *testing.T) {
		job := b.BuildRestoreJob(newBackupCluster(), &RestoreJobOptions{
			Timestamp: "20260519020000",
		})
		require.NotNil(t, job)
		// newBackupCluster configures folder "/backups".
		assert.Equal(t, "/backups",
			containerEnvValue(job.Spec.Template.Spec.Containers[0], "S3_FOLDER"))
	})
}

// TestBuildPostRestoreValidationJobS3FolderOverride asserts the validation Job
// mirrors the restore Job's S3 folder override.
func TestBuildPostRestoreValidationJobS3FolderOverride(t *testing.T) {
	b := NewBuilder()

	job := b.BuildPostRestoreValidationJob(newBackupCluster(), &ValidationJobOptions{
		Timestamp:        "20260519020000",
		Database:         "mydb",
		S3FolderOverride: "scenario87-src",
	})
	require.NotNil(t, job)
	assert.Equal(t, "scenario87-src",
		containerEnvValue(job.Spec.Template.Spec.Containers[0], "S3_FOLDER"))

	// Without the override the validation Job uses the cluster's own folder.
	jobNoOverride := b.BuildPostRestoreValidationJob(newBackupCluster(), &ValidationJobOptions{
		Timestamp: "20260519020000",
		Database:  "mydb",
	})
	require.NotNil(t, jobNoOverride)
	assert.Equal(t, "/backups",
		containerEnvValue(jobNoOverride.Spec.Template.Spec.Containers[0], "S3_FOLDER"))
}

func TestBuildRetentionCleanupJob(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{
		FullCount:        3,
		IncrementalCount: 10,
		MaxAge:           "30d",
	}
	job := b.BuildRetentionCleanupJob(cluster, "20260519020000")
	require.NotNil(t, job)
	assert.Equal(t, util.RetentionCleanupJobName("test-cluster", "20260519020000"), job.Name)
	assert.Equal(t, util.BackupOperationCleanup, job.Labels[util.LabelBackupOperation])

	container := job.Spec.Template.Spec.Containers[0]
	require.Len(t, container.Args, 1)
	script := container.Args[0]

	// Real gpbackman commands only — no legacy/invalid tokens.
	assert.NotContains(t, script, "gpbackman delete")
	assert.NotContains(t, script, "--keep-full")
	assert.NotContains(t, script, "--older-than ")
	// Count-based enforcement enumerates and deletes the oldest excess.
	assert.Contains(t, script, "backup-info --type \"$1\"")
	assert.Contains(t, script, "_gpbackman_timestamps 'full'")
	assert.Contains(t, script, "_gpbackman_timestamps 'incremental'")
	assert.Contains(t, script, "backup-delete --timestamp \"$1\" --cascade")
	// Time-based enforcement uses backup-clean --older-than-days.
	assert.Contains(t, script, "backup-clean --older-than-days 30")
	// Deletion-count marker + termination-message write.
	assert.Contains(t, script, "RETENTION_DELETED=")
	assert.Contains(t, script, "/dev/termination-log")
	// Recoverable count via the pod log fallback.
	assert.Equal(t, corev1.TerminationMessageFallbackToLogsOnError,
		container.TerminationMessagePolicy)
}

func TestBuildRetentionCleanupJobMaxAgeOnly(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{MaxAge: "720h"}
	job := b.BuildRetentionCleanupJob(cluster, "20260519020000")
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "backup-clean --older-than-days 30")
	// No count-based enumeration when only maxAge is set.
	assert.NotContains(t, script, "_gpbackman_timestamps 'full'")
	assert.NotContains(t, script, "_gpbackman_timestamps 'incremental'")
}

func TestParseMaxAgeDays(t *testing.T) {
	tests := []struct {
		name  string
		input string
		days  int
		ok    bool
	}{
		{"days suffix", "30d", 30, true},
		{"weeks suffix", "4w", 28, true},
		{"hours duration", "720h", 30, true},
		{"sub-day duration rounds up", "25h", 1, true},
		{"bare integer days", "30", 30, true},
		{"minutes duration", "43200m", 30, true},
		{"empty", "", 0, false},
		{"junk", "notaduration", 0, false},
		{"negative days suffix", "-5d", 0, false},
		{"zero days suffix", "0d", 0, true},
		{"bare negative integer", "-5", 0, false},
		{"sub-day duration rounds up to 1", "5h", 1, true},
		{"single char no number", "d", 0, false},
		{"weeks uppercase", "2W", 14, true},
		{"days uppercase", "10D", 10, true},
		{"unknown suffix", "30y", 0, false},
		{"zero duration", "0s", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			days, ok := parseMaxAgeDays(tc.input)
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.Equal(t, tc.days, days)
			}
		})
	}
}

// TestRetentionPolicy verifies retentionPolicy returns the configured policy and
// a zero policy when backup is not configured.
func TestRetentionPolicy(t *testing.T) {
	t.Run("nil backup returns zero policy", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Backup = nil
		got := retentionPolicy(cluster)
		assert.Equal(t, cbv1alpha1.BackupRetention{}, got)
	})

	t.Run("configured backup returns policy", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		}
		got := retentionPolicy(cluster)
		assert.Equal(t, int32(3), got.FullCount)
		assert.Equal(t, int32(10), got.IncrementalCount)
		assert.Equal(t, "30d", got.MaxAge)
	})
}

// TestBuildRetentionCleanupJobFullCountOnly verifies a fullCount-only policy
// renders the full count block and omits the incremental block and backup-clean.
func TestBuildRetentionCleanupJobFullCountOnly(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{FullCount: 3}
	job := b.BuildRetentionCleanupJob(cluster, "20260519020000")
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(t, script, "backup-info --type \"$1\"")
	assert.Contains(t, script, "_gpbackman_timestamps 'full'")
	assert.Contains(t, script, "backup-delete --timestamp \"$1\" --cascade")
	assert.Contains(t, script, "KEEP=3")
	// No incremental count block and no time-based backup-clean.
	assert.NotContains(t, script, "_gpbackman_timestamps 'incremental'")
	assert.NotContains(t, script, "backup-clean --older-than-days")
}

// TestBuildRetentionCleanupJobIncrementalCountOnly verifies an incrementalCount-
// only policy renders the incremental block and omits the full block and clean.
func TestBuildRetentionCleanupJobIncrementalCountOnly(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{IncrementalCount: 10}
	job := b.BuildRetentionCleanupJob(cluster, "20260519020000")
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(t, script, "_gpbackman_timestamps 'incremental'")
	assert.Contains(t, script, "KEEP=10")
	// No full count block and no time-based backup-clean.
	assert.NotContains(t, script, "_gpbackman_timestamps 'full'")
	assert.NotContains(t, script, "backup-clean --older-than-days")
}

// TestBuildRetentionCleanupJobNoPolicy verifies the cleanup Job still builds a
// valid Job (safe no-op script) when no retention policy is configured.
func TestBuildRetentionCleanupJobNoPolicy(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{}
	var job *batchv1.Job
	require.NotPanics(t, func() {
		job = b.BuildRetentionCleanupJob(cluster, "20260519020000")
	})
	require.NotNil(t, job)
	assert.Equal(t, util.BackupOperationCleanup, job.Labels[util.LabelBackupOperation])

	container := job.Spec.Template.Spec.Containers[0]
	require.Len(t, container.Args, 1)
	script := container.Args[0]
	// No-op: no count loops and no backup-clean, but the marker still emitted.
	assert.NotContains(t, script, "_gpbackman_timestamps 'full'")
	assert.NotContains(t, script, "_gpbackman_timestamps 'incremental'")
	assert.NotContains(t, script, "backup-clean --older-than-days")
	assert.Contains(t, script, retentionDeletedMarker)
	assert.Contains(t, script, "/dev/termination-log")
	// The cleanup container runs the script via sh -c.
	assert.Equal(t, []string{shellCommand, shellFlag}, container.Command)
}

// TestBuildRetentionCleanupJobAllPolicies verifies all three blocks plus the
// deletion-count marker and termination-log write render when every policy is set.
func TestBuildRetentionCleanupJobAllPolicies(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{
		FullCount:        3,
		IncrementalCount: 10,
		MaxAge:           "30d",
	}
	job := b.BuildRetentionCleanupJob(cluster, "20260519020000")
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(t, script, "_gpbackman_timestamps 'full'")
	assert.Contains(t, script, "_gpbackman_timestamps 'incremental'")
	assert.Contains(t, script, "backup-clean --older-than-days 30")
	assert.Contains(t, script, "--cascade")
	assert.Contains(t, script, retentionDeletedMarker)
	assert.Contains(t, script, "/dev/termination-log")
}

// TestBuildRetentionCleanupScriptValidShell verifies the rendered cleanup script
// is valid POSIX/bash syntax via `sh -n` when a shell is available.
func TestBuildRetentionCleanupScriptValidShell(t *testing.T) {
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh not available")
	}
	cluster := newBackupCluster()
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{
		FullCount:        3,
		IncrementalCount: 10,
		MaxAge:           "30d",
	}
	script := buildGpbackmanRetentionScript(cluster)

	cmd := exec.Command(shell, "-n") //nolint:gosec // fixed shell, script via stdin
	cmd.Stdin = strings.NewReader(script)
	out, runErr := cmd.CombinedOutput()
	require.NoError(t, runErr, "sh -n reported a syntax error: %s", string(out))
}

func TestBuildBackupJobTemplateOverrides(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	backoff := int32(5)
	cluster.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{
		ServiceAccountName: "custom-sa",
		BackoffLimit:       &backoff,
		NodeSelector:       map[string]string{"disktype": "ssd"},
	}
	job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000"})
	require.NotNil(t, job)
	assert.Equal(t, "custom-sa", job.Spec.Template.Spec.ServiceAccountName)
	require.NotNil(t, job.Spec.BackoffLimit)
	assert.Equal(t, int32(5), *job.Spec.BackoffLimit)
	assert.Equal(t, "ssd", job.Spec.Template.Spec.NodeSelector["disktype"])
}

func TestBuildBackupEnvS3Credentials(t *testing.T) {
	env := buildBackupEnv(newBackupCluster())
	var hasAccessKey, hasSecretKey bool
	for _, e := range env {
		if e.Name == "AWS_ACCESS_KEY_ID" && e.ValueFrom != nil {
			hasAccessKey = true
			assert.Equal(t, "aws_access_key_id", e.ValueFrom.SecretKeyRef.Key)
		}
		if e.Name == "AWS_SECRET_ACCESS_KEY" && e.ValueFrom != nil {
			hasSecretKey = true
		}
	}
	assert.True(t, hasAccessKey, "expected AWS_ACCESS_KEY_ID env from secret")
	assert.True(t, hasSecretKey, "expected AWS_SECRET_ACCESS_KEY env from secret")
}

func TestBuildGprestoreArgsDataMetadataResize(t *testing.T) {
	t.Run("data-only and resize-cluster", func(t *testing.T) {
		args := buildGprestoreArgs(newBackupCluster(), &cbv1alpha1.GprestoreOptions{
			DataOnly:      true,
			ResizeCluster: true,
		}, nil)
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--data-only")
		assert.Contains(t, joined, "--resize-cluster")
		assert.NotContains(t, joined, "--metadata-only")
	})

	t.Run("metadata-only", func(t *testing.T) {
		args := buildGprestoreArgs(newBackupCluster(), &cbv1alpha1.GprestoreOptions{MetadataOnly: true}, nil)
		joined := strings.Join(args, " ")
		assert.Contains(t, joined, "--metadata-only")
		assert.NotContains(t, joined, "--data-only")
	})
}

func TestBuildGpbackupArgsIncrementalFromTimestamp(t *testing.T) {
	// An incremental request type alone (no explicit Incremental flag) must still
	// emit the incremental flags plus the pinned base timestamp.
	args := mustGpbackupArgs(t, newBackupCluster(), &cbv1alpha1.GpbackupOptions{
		Incremental:       true,
		LeafPartitionData: true,
	}, &BackupJobOptions{
		Type:          "incremental",
		FromTimestamp: "20260518020000",
	})
	joined := strings.Join(args, " ")
	assert.Contains(t, joined, "--incremental")
	assert.Contains(t, joined, "--leaf-partition-data")
	assert.Contains(t, joined, "--from-timestamp 20260518020000")
}

func TestBuildGpbackupArgsIncludeTable(t *testing.T) {
	args := mustGpbackupArgs(t, newBackupCluster(), &cbv1alpha1.GpbackupOptions{SingleDataFile: true}, &BackupJobOptions{
		IncludeTables: []string{"public.users", "public.orders"},
	})
	joined := strings.Join(args, " ")
	assert.Contains(t, joined, "--single-data-file")
	assert.Contains(t, joined, "--include-table public.users")
	assert.Contains(t, joined, "--include-table public.orders")
}

func TestBuildBackupJobPreBackupInitContainer(t *testing.T) {
	b := NewBuilder()
	job := b.BuildBackupJob(newBackupCluster(), &BackupJobOptions{Timestamp: "20260519020000"})
	require.NotNil(t, job)

	initContainers := job.Spec.Template.Spec.InitContainers
	require.Len(t, initContainers, 1)
	ic := initContainers[0]
	assert.Equal(t, preBackupCheckContainerName, ic.Name)
	assert.Equal(t, "cloudberry-backup:2.1.0", ic.Image)
	require.NotEmpty(t, ic.Args)
	assert.Contains(t, ic.Args[0], "gp_segment_configuration")
	assert.Contains(t, ic.Args[0], "pg_stat_activity")

	// The main gpbackup container must still be present and follow the init container.
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, backupContainerName, job.Spec.Template.Spec.Containers[0].Name)
}

func TestBuildBackupCronJobPreBackupInitContainer(t *testing.T) {
	b := NewBuilder()
	cj := b.BuildBackupCronJob(newBackupCluster())
	require.NotNil(t, cj)
	initContainers := cj.Spec.JobTemplate.Spec.Template.Spec.InitContainers
	require.Len(t, initContainers, 1)
	assert.Equal(t, preBackupCheckContainerName, initContainers[0].Name)
}

func TestPreBackupDestinationCheckLocal(t *testing.T) {
	cluster := newBackupCluster()
	cluster.Spec.Backup.Destination.Type = "local"
	cluster.Spec.Backup.Destination.S3 = nil
	cluster.Spec.Backup.Destination.Local = &cbv1alpha1.LocalDestination{
		PersistentVolumeClaim: "backup-pvc",
		Path:                  "/data/backups",
	}
	script := preBackupCheckScript(cluster)
	assert.Contains(t, script, "df -Pk")
	assert.Contains(t, script, "/data/backups")
}

func TestBuildPostRestoreValidationJob(t *testing.T) {
	b := NewBuilder()
	job := b.BuildPostRestoreValidationJob(newBackupCluster(), &ValidationJobOptions{
		Timestamp: "20260519020000",
		Database:  "mydb",
	})
	require.NotNil(t, job)
	assert.Equal(t, util.PostRestoreValidationJobName("test-cluster", "20260519020000"), job.Name)
	assert.Equal(t, util.BackupOperationValidate, job.Labels[util.LabelBackupOperation])

	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	c := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, validateContainerName, c.Name)
	require.NotEmpty(t, c.Args)
	assert.Contains(t, c.Args[0], "indisvalid")
	assert.Contains(t, c.Args[0], defaultHealthCheckQuery)

	var pgdb string
	for _, e := range c.Env {
		if e.Name == "PGDATABASE" {
			pgdb = e.Value
		}
	}
	assert.Equal(t, "mydb", pgdb)
}

func TestBuildPostRestoreValidationJobDefaultQuery(t *testing.T) {
	b := NewBuilder()
	job := b.BuildPostRestoreValidationJob(newBackupCluster(), nil)
	require.NotNil(t, job)
	assert.Contains(t, job.Spec.Template.Spec.Containers[0].Args[0], "SELECT 1")
}

func TestPreBackupDestinationCheckS3(t *testing.T) {
	script := preBackupCheckScript(newBackupCluster())
	// The S3 branch performs a real, fail-closed SigV4 HEAD reachability check.
	assert.Contains(t, script, "verifying s3 bucket reachability")
	assert.Contains(t, script, "${S3_ENDPOINT")
	assert.Contains(t, script, "${S3_BUCKET}")
	assert.Contains(t, script, "${S3_REGION:-us-east-1}")
	assert.Contains(t, script, "${AWS_ACCESS_KEY_ID}")
	assert.Contains(t, script, "${AWS_SECRET_ACCESS_KEY}")
	assert.Contains(t, script, "AWS4-HMAC-SHA256")
	assert.Contains(t, script, "-X HEAD")
	assert.Contains(t, script, "--max-time")
	// Fail-closed: non-2xx/3xx must exit 1.
	assert.Contains(t, script, "s3 bucket unreachable")
	assert.Contains(t, script, "exit 1")
}

func TestPreBackupDestinationCheckNoBackup(t *testing.T) {
	cluster := newTestCluster()
	assert.Empty(t, preBackupDestinationCheck(cluster))
}

func TestSetEnvVarReplacesExisting(t *testing.T) {
	c := newBackupCluster()
	job := NewBuilder().BuildPostRestoreValidationJob(c, &ValidationJobOptions{
		Timestamp: "20260519020000",
		Database:  "db_one",
	})
	// PGDATABASE must be replaced (not duplicated) by setEnvVar.
	var count int
	var value string
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "PGDATABASE" {
			count++
			value = e.Value
		}
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, "db_one", value)
}

func TestJobTemplateOverridesResourcesAndTolerations(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{
		Resources: &cbv1alpha1.ResourceRequirements{
			Requests: &cbv1alpha1.ResourceList{CPU: "100m", Memory: "256Mi"},
			Limits:   &cbv1alpha1.ResourceList{CPU: "500m", Memory: "512Mi"},
		},
		Tolerations: []cbv1alpha1.Toleration{
			{Key: "dedicated", Operator: "Equal", Value: "backup", Effect: "NoSchedule"},
		},
	}
	job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000"})
	require.NotNil(t, job)

	container := job.Spec.Template.Spec.Containers[0]
	assert.False(t, container.Resources.Requests.Cpu().IsZero())
	assert.False(t, container.Resources.Limits.Memory().IsZero())
	require.Len(t, job.Spec.Template.Spec.Tolerations, 1)
	assert.Equal(t, "dedicated", job.Spec.Template.Spec.Tolerations[0].Key)
}

// TestBuildBackupJob_DefaultBackoffAndDeadline_Scenario83 asserts the Scenario 83
// builder defaults: with NO jobTemplate the on-demand backup Job carries
// BackoffLimit==2 (defaultBackoffLimit) and ActiveDeadlineSeconds==7200
// (defaultActiveDeadlineSeconds).
func TestBuildBackupJob_DefaultBackoffAndDeadline_Scenario83(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.JobTemplate = nil

	job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000"})
	require.NotNil(t, job)
	require.NotNil(t, job.Spec.BackoffLimit)
	assert.Equal(t, int32(2), *job.Spec.BackoffLimit)
	require.NotNil(t, job.Spec.ActiveDeadlineSeconds)
	assert.Equal(t, int64(7200), *job.Spec.ActiveDeadlineSeconds)
}

// TestBuildBackupCronJob_DefaultBackoffAndDeadline_Scenario83 asserts the same
// defaults reach the CronJob's JobTemplate spec when no jobTemplate is set.
func TestBuildBackupCronJob_DefaultBackoffAndDeadline_Scenario83(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.JobTemplate = nil

	cj := b.BuildBackupCronJob(cluster)
	require.NotNil(t, cj)
	spec := cj.Spec.JobTemplate.Spec
	require.NotNil(t, spec.BackoffLimit)
	assert.Equal(t, int32(2), *spec.BackoffLimit)
	require.NotNil(t, spec.ActiveDeadlineSeconds)
	assert.Equal(t, int64(7200), *spec.ActiveDeadlineSeconds)
}

// TestBuildBackupJob_JobTemplateBackoffAndDeadlineOverride_Scenario83 asserts a
// jobTemplate setting BackoffLimit=5 and ActiveDeadlineSeconds=5 reaches the
// on-demand backup Job spec verbatim (the deadline/force-failure knobs).
func TestBuildBackupJob_JobTemplateBackoffAndDeadlineOverride_Scenario83(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	backoff := int32(5)
	deadline := int64(5)
	cluster.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{
		BackoffLimit:          &backoff,
		ActiveDeadlineSeconds: &deadline,
	}

	job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000"})
	require.NotNil(t, job)
	require.NotNil(t, job.Spec.BackoffLimit)
	assert.Equal(t, int32(5), *job.Spec.BackoffLimit)
	require.NotNil(t, job.Spec.ActiveDeadlineSeconds)
	assert.Equal(t, int64(5), *job.Spec.ActiveDeadlineSeconds)
}

// TestBuildBackupCronJob_JobTemplateBackoffAndDeadlineOverride_Scenario83 asserts
// the same override reaches the CronJob's JobTemplate spec.
func TestBuildBackupCronJob_JobTemplateBackoffAndDeadlineOverride_Scenario83(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	backoff := int32(5)
	deadline := int64(5)
	cluster.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{
		BackoffLimit:          &backoff,
		ActiveDeadlineSeconds: &deadline,
	}

	cj := b.BuildBackupCronJob(cluster)
	require.NotNil(t, cj)
	spec := cj.Spec.JobTemplate.Spec
	require.NotNil(t, spec.BackoffLimit)
	assert.Equal(t, int32(5), *spec.BackoffLimit)
	require.NotNil(t, spec.ActiveDeadlineSeconds)
	assert.Equal(t, int64(5), *spec.ActiveDeadlineSeconds)
}

// TestBuildJobSpec_OverridePropagatesToAllJobs_Scenario83 confirms the
// jobTemplate override routes through buildJobSpec uniformly: restore, validation
// and retention-cleanup Jobs all carry the overridden BackoffLimit==5 and
// ActiveDeadlineSeconds==5.
func TestBuildJobSpec_OverridePropagatesToAllJobs_Scenario83(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	backoff := int32(5)
	deadline := int64(5)
	cluster.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{
		BackoffLimit:          &backoff,
		ActiveDeadlineSeconds: &deadline,
	}

	restore := b.BuildRestoreJob(cluster, &RestoreJobOptions{Timestamp: "20260519020000"})
	validate := b.BuildPostRestoreValidationJob(cluster, &ValidationJobOptions{Timestamp: "20260519020000"})
	cleanup := b.BuildRetentionCleanupJob(cluster, "20260519020000")

	for name, spec := range map[string]batchv1.JobSpec{
		"restore":  restore.Spec,
		"validate": validate.Spec,
		"cleanup":  cleanup.Spec,
	} {
		require.NotNilf(t, spec.BackoffLimit, "%s BackoffLimit", name)
		assert.Equalf(t, int32(5), *spec.BackoffLimit, "%s BackoffLimit", name)
		require.NotNilf(t, spec.ActiveDeadlineSeconds, "%s ActiveDeadlineSeconds", name)
		assert.Equalf(t, int64(5), *spec.ActiveDeadlineSeconds, "%s ActiveDeadlineSeconds", name)
	}
}

// TestBuildJobSpec_NilTemplateFieldsRetainDefaults_Scenario83 verifies that a
// jobTemplate present but with nil BackoffLimit/ActiveDeadlineSeconds retains the
// Scenario 83 defaults (2 / 7200).
func TestBuildJobSpec_NilTemplateFieldsRetainDefaults_Scenario83(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{
		ServiceAccountName: "custom-sa",
	}

	job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000"})
	require.NotNil(t, job)
	require.NotNil(t, job.Spec.BackoffLimit)
	assert.Equal(t, int32(2), *job.Spec.BackoffLimit)
	require.NotNil(t, job.Spec.ActiveDeadlineSeconds)
	assert.Equal(t, int64(7200), *job.Spec.ActiveDeadlineSeconds)
}
