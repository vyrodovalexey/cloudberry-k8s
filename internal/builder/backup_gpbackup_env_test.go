package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// envValueMap collapses plain-value env vars into a name->value map. Vars
// sourced via ValueFrom (e.g. AWS_* secretKeyRef) are recorded with an empty
// value but still present as keys so callers can assert presence.
func envValueMap(env []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

// envHasSecretKeyRef reports whether the named env var is sourced from a
// Secret key reference.
func envHasSecretKeyRef(env []corev1.EnvVar, name string) bool {
	for _, e := range env {
		if e.Name == name && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			return true
		}
	}
	return false
}

func TestApplyBackupGpbackupEnv(t *testing.T) {
	t.Run("nil gpOpts applies defaults and passed database", func(t *testing.T) {
		podSpec := corev1.PodSpec{
			Containers: []corev1.Container{{Name: backupContainerName}},
		}
		applyBackupGpbackupEnv(&podSpec, "mydb", nil)

		vals := envValueMap(podSpec.Containers[0].Env)
		assert.Equal(t, "mydb", vals[envCBDBDatabase])
		assert.Equal(t, defaultCompressionLevel, vals[envCompressionLevel])
		assert.Equal(t, "1", vals[envCompressionLevel])
		assert.Equal(t, defaultCompressionType, vals[envCompressionType])
		assert.Equal(t, "gzip", vals[envCompressionType])
		assert.Equal(t, defaultBackupJobs, vals[envBackupJobs])
		assert.Equal(t, "1", vals[envBackupJobs])
	})

	t.Run("set gpOpts reflects compression and jobs", func(t *testing.T) {
		podSpec := corev1.PodSpec{
			Containers: []corev1.Container{{Name: backupContainerName}},
		}
		applyBackupGpbackupEnv(&podSpec, "salesdb", &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 5,
			CompressionType:  "zstd",
			Jobs:             4,
		})

		vals := envValueMap(podSpec.Containers[0].Env)
		assert.Equal(t, "salesdb", vals[envCBDBDatabase])
		assert.Equal(t, "5", vals[envCompressionLevel])
		assert.Equal(t, "zstd", vals[envCompressionType])
		assert.Equal(t, "4", vals[envBackupJobs])
	})

	t.Run("empty database value still emits CBDB_DATABASE", func(t *testing.T) {
		podSpec := corev1.PodSpec{
			Containers: []corev1.Container{{Name: backupContainerName}},
		}
		applyBackupGpbackupEnv(&podSpec, "", nil)

		vals := envValueMap(podSpec.Containers[0].Env)
		_, present := vals[envCBDBDatabase]
		assert.True(t, present, "CBDB_DATABASE must be present even when empty")
		assert.Equal(t, "", vals[envCBDBDatabase])
	})

	t.Run("zero-value gpOpts fields fall back to defaults", func(t *testing.T) {
		podSpec := corev1.PodSpec{
			Containers: []corev1.Container{{Name: backupContainerName}},
		}
		// A non-nil gpOpts whose fields are all zero must still yield defaults.
		applyBackupGpbackupEnv(&podSpec, "db", &cbv1alpha1.GpbackupOptions{})

		vals := envValueMap(podSpec.Containers[0].Env)
		assert.Equal(t, "1", vals[envCompressionLevel])
		assert.Equal(t, "gzip", vals[envCompressionType])
		assert.Equal(t, "1", vals[envBackupJobs])
	})

	t.Run("empty containers is a no-op and does not panic", func(t *testing.T) {
		podSpec := corev1.PodSpec{Containers: nil}
		assert.NotPanics(t, func() {
			applyBackupGpbackupEnv(&podSpec, "mydb", nil)
		})
		assert.Empty(t, podSpec.Containers)
	})

	t.Run("only first container receives env vars", func(t *testing.T) {
		podSpec := corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: backupContainerName},
				{Name: "sidecar"},
			},
		}
		applyBackupGpbackupEnv(&podSpec, "mydb", nil)

		first := envValueMap(podSpec.Containers[0].Env)
		assert.Equal(t, "mydb", first[envCBDBDatabase])
		assert.Empty(t, podSpec.Containers[1].Env)
	})
}

func TestFirstDatabase(t *testing.T) {
	tests := []struct {
		name      string
		databases []string
		want      string
	}{
		{name: "nil slice returns empty", databases: nil, want: ""},
		{name: "empty slice returns empty", databases: []string{}, want: ""},
		{name: "single element returns it", databases: []string{"mydb"}, want: "mydb"},
		{name: "multiple elements returns first", databases: []string{"mydb", "x"}, want: "mydb"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, firstDatabase(tt.databases))
		})
	}
}

func TestBuildBackupJobGpbackupEnv(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	// Cluster-level gpbackup: CompressionLevel=6, CompressionType=gzip, Jobs=4.
	job := b.BuildBackupJob(cluster, &BackupJobOptions{
		Timestamp: "20260519020000",
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)

	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	container := job.Spec.Template.Spec.Containers[0]
	env := container.Env
	vals := envValueMap(env)

	// New informational gpbackup env vars.
	assert.Equal(t, "mydb", vals[envCBDBDatabase])
	assert.Equal(t, "6", vals[envCompressionLevel])
	assert.Equal(t, "gzip", vals[envCompressionType])
	assert.Equal(t, "4", vals[envBackupJobs])

	// Pre-existing connection env vars must still be present.
	assert.NotEmpty(t, vals["PGHOST"])
	assert.NotEmpty(t, vals["PGPORT"])

	// AWS_* credentials still sourced from the Secret (secretKeyRef).
	assert.True(t, envHasSecretKeyRef(env, "AWS_ACCESS_KEY_ID"))
	assert.True(t, envHasSecretKeyRef(env, "AWS_SECRET_ACCESS_KEY"))
}

func TestBuildBackupJobGpbackupEnvPerRequestOverride(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	// Per-request gpbackup overrides the cluster defaults.
	job := b.BuildBackupJob(cluster, &BackupJobOptions{
		Timestamp: "20260519020000",
		Databases: []string{"otherdb"},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 5,
			CompressionType:  "zstd",
			Jobs:             4,
		},
	})
	require.NotNil(t, job)

	vals := envValueMap(job.Spec.Template.Spec.Containers[0].Env)
	assert.Equal(t, "otherdb", vals[envCBDBDatabase])
	assert.Equal(t, "5", vals[envCompressionLevel])
	assert.Equal(t, "zstd", vals[envCompressionType])
	assert.Equal(t, "4", vals[envBackupJobs])
}

func TestBuildRestoreJobGpbackupEnv(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	job := b.BuildRestoreJob(cluster, &RestoreJobOptions{
		Timestamp: "20260519020000",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)

	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	container := job.Spec.Template.Spec.Containers[0]
	env := container.Env
	vals := envValueMap(env)

	// Restore Jobs carry the same informational gpbackup env (database from the
	// restore request; compression/jobs from the cluster's gpbackupOptions).
	assert.Equal(t, "mydb", vals[envCBDBDatabase])
	assert.Equal(t, "6", vals[envCompressionLevel])
	assert.Equal(t, "gzip", vals[envCompressionType])
	assert.Equal(t, "4", vals[envBackupJobs])

	assert.NotEmpty(t, vals["PGHOST"])
	assert.NotEmpty(t, vals["PGPORT"])
	assert.True(t, envHasSecretKeyRef(env, "AWS_ACCESS_KEY_ID"))
	assert.True(t, envHasSecretKeyRef(env, "AWS_SECRET_ACCESS_KEY"))
}

func TestBuildBackupCronJobGpbackupEnv(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cj := b.BuildBackupCronJob(cluster)
	require.NotNil(t, cj)

	containers := cj.Spec.JobTemplate.Spec.Template.Spec.Containers
	require.Len(t, containers, 1)
	vals := envValueMap(containers[0].Env)

	// CronJob databases are resolved at runtime, so CBDB_DATABASE is emitted
	// empty (but still present and inspectable).
	cbdb, present := vals[envCBDBDatabase]
	assert.True(t, present, "CBDB_DATABASE must be present on the cronjob container")
	assert.Equal(t, "", cbdb)

	// Compression/jobs come from the cluster-level gpbackup options.
	assert.Equal(t, "6", vals[envCompressionLevel])
	assert.Equal(t, "gzip", vals[envCompressionType])
	assert.Equal(t, "4", vals[envBackupJobs])
}

func TestBuildBackupJobGpbackupEnvDefaultsWithoutGpbackup(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	// No cluster-level gpbackup options and no per-request options -> defaults.
	cluster.Spec.Backup.Gpbackup = nil
	job := b.BuildBackupJob(cluster, &BackupJobOptions{
		Timestamp: "20260519020000",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)

	vals := envValueMap(job.Spec.Template.Spec.Containers[0].Env)
	assert.Equal(t, "mydb", vals[envCBDBDatabase])
	assert.Equal(t, "1", vals[envCompressionLevel])
	assert.Equal(t, "gzip", vals[envCompressionType])
	assert.Equal(t, "1", vals[envBackupJobs])
}
