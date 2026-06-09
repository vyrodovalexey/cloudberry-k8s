package builder

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// newLocalBackupCluster returns a test cluster with a LOCAL backup destination
// (PVC mounted at the given path). It mirrors newBackupCluster() but swaps the
// S3 destination for a local one. path defaults to /backups when empty, and pvc
// defaults to "backup-pvc" when empty.
func newLocalBackupCluster(path, pvc string) *cbv1alpha1.CloudberryCluster {
	cluster := newBackupCluster()
	if pvc == "" {
		pvc = "backup-pvc"
	}
	cluster.Spec.Backup.Destination = cbv1alpha1.BackupDestination{
		Type: destinationTypeLocal,
		S3:   nil,
		Local: &cbv1alpha1.LocalDestination{
			Path:                  path,
			PersistentVolumeClaim: pvc,
		},
	}
	return cluster
}

// TestLocalBackupDirScenario81 is a table-driven test for localBackupDir: it
// resolves the configured Local.Path when set and falls back to the default
// /backups mount path otherwise.
func TestLocalBackupDirScenario81(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c *cbv1alpha1.CloudberryCluster)
		want   string
	}{
		{
			name:   "local with explicit /backups path",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { c.Spec.Backup.Destination.Local.Path = "/backups" },
			want:   "/backups",
		},
		{
			name:   "local with empty path defaults to /backups",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { c.Spec.Backup.Destination.Local.Path = "" },
			want:   localBackupMountPath,
		},
		{
			name:   "local with custom path",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { c.Spec.Backup.Destination.Local.Path = "/data/bk" },
			want:   "/data/bk",
		},
		{
			name: "nil Local defaults to /backups",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Destination.Local = nil
			},
			want: localBackupMountPath,
		},
		{
			name: "nil Backup defaults to /backups",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = nil
			},
			want: localBackupMountPath,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newLocalBackupCluster("", "")
			tc.mutate(cluster)
			assert.Equal(t, tc.want, localBackupDir(cluster))
		})
	}
}

// TestIsLocalDestinationScenario81 is a table-driven test for isLocalDestination.
func TestIsLocalDestinationScenario81(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c *cbv1alpha1.CloudberryCluster)
		want   bool
	}{
		{
			name:   "local destination is local",
			mutate: func(_ *cbv1alpha1.CloudberryCluster) {},
			want:   true,
		},
		{
			name: "s3 destination is not local",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Destination.Type = destinationTypeS3
			},
			want: false,
		},
		{
			name: "nil backup is not local",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = nil
			},
			want: false,
		},
		{
			name: "unknown destination type is not local",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Destination.Type = "gcs"
			},
			want: false,
		},
		{
			name: "empty destination type is not local",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Destination.Type = ""
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newLocalBackupCluster("/backups", "backup-pvc")
			tc.mutate(cluster)
			assert.Equal(t, tc.want, isLocalDestination(cluster))
		})
	}
}

// TestBackupDestinationArgsScenario81 is a table-driven test (Scenario 81 §A)
// for backupDestinationArgs: local destinations seed `--backup-dir <path>`
// while s3/nil/unknown destinations seed ONLY `--plugin-config /tmp/s3-config.yaml`.
// gpbackup REJECTS `--plugin-config` together with `--backup-dir`, so the S3 path
// must NOT carry `--backup-dir`; the per-segment backup dirs + history DB are
// handled by running gpbackup inside the coordinator pod (the coordinator-exec
// model, see renderToolScript/coordinatorExecScript and spec 11 §MPP Dispatch).
func TestBackupDestinationArgsScenario81(t *testing.T) {
	// s3Args is the expected S3 leading-arg slice: plugin-config ONLY (DATA → S3
	// via the plugin; gpbackup runs inside the coordinator pod for the segment
	// dirs + history DB).
	s3Args := []string{pluginConfigFlag, s3RenderedConfigPath}
	tests := []struct {
		name   string
		mutate func(c *cbv1alpha1.CloudberryCluster)
		want   []string
	}{
		{
			name:   "local with /backups path",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { setLocalDestination(c, "/backups") },
			want:   []string{backupDirFlag, "/backups"},
		},
		{
			name:   "local with empty path defaults to /backups",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { setLocalDestination(c, "") },
			want:   []string{backupDirFlag, "/backups"},
		},
		{
			name:   "local with custom path",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { setLocalDestination(c, "/data/bk") },
			want:   []string{backupDirFlag, "/data/bk"},
		},
		{
			name:   "s3 destination uses plugin-config only (no --backup-dir)",
			mutate: func(_ *cbv1alpha1.CloudberryCluster) {}, // default S3 cluster.
			want:   s3Args,
		},
		{
			name: "nil backup defaults to s3 plugin-config only",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = nil
			},
			want: s3Args,
		},
		{
			name: "unknown destination type defaults to s3 plugin-config only",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Destination.Type = "gcs"
				c.Spec.Backup.Destination.S3 = nil
			},
			want: s3Args,
		},
		{
			name: "empty destination type defaults to s3 plugin-config only",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Destination.Type = ""
				c.Spec.Backup.Destination.S3 = nil
			},
			want: s3Args,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newBackupCluster()
			tc.mutate(cluster)
			assert.Equal(t, tc.want, backupDestinationArgs(cluster))
		})
	}
}

// setLocalDestination switches a cluster's backup destination to local with the
// given path (and a default PVC), clearing the S3 destination.
func setLocalDestination(c *cbv1alpha1.CloudberryCluster, path string) {
	c.Spec.Backup.Destination = cbv1alpha1.BackupDestination{
		Type: destinationTypeLocal,
		S3:   nil,
		Local: &cbv1alpha1.LocalDestination{
			Path:                  path,
			PersistentVolumeClaim: "backup-pvc",
		},
	}
}

// TestBuildGpbackupArgsScenario81 (Scenario 81 §B) asserts the destination-aware
// leading args for gpbackup: local emits `--backup-dir <local.path>` and NO
// plugin; s3 emits ONLY `--plugin-config /tmp/s3-config.yaml` (gpbackup rejects
// --plugin-config together with --backup-dir; the segment dirs + history DB are
// handled by the coordinator-exec model). The non-destination flags
// (dbname/compression/jobs) are unaffected.
func TestBuildGpbackupArgsScenario81(t *testing.T) {
	gpOpts := &cbv1alpha1.GpbackupOptions{
		CompressionLevel: 6,
		CompressionType:  "gzip",
		Jobs:             4,
	}
	jobOpts := &BackupJobOptions{Databases: []string{"mydb"}}

	tests := []struct {
		name           string
		cluster        *cbv1alpha1.CloudberryCluster
		wantContains   []string
		wantNotContain []string
	}{
		{
			name:    "local cluster emits --backup-dir and not --plugin-config",
			cluster: newLocalBackupCluster("/backups", "backup-pvc"),
			wantContains: []string{
				"--backup-dir /backups",
				"--dbname mydb",
				"--compression-level 6",
				"--compression-type gzip",
				"--jobs 4",
			},
			wantNotContain: []string{
				pluginConfigFlag,
				s3RenderedConfigPath,
			},
		},
		{
			name:    "local cluster with custom path",
			cluster: newLocalBackupCluster("/data/backups", "backup-pvc"),
			wantContains: []string{
				"--backup-dir /data/backups",
				"--dbname mydb",
			},
			wantNotContain: []string{pluginConfigFlag},
		},
		{
			name:    "s3 cluster keeps --plugin-config only (no --backup-dir, which gpbackup rejects)",
			cluster: newBackupCluster(),
			wantContains: []string{
				pluginConfigFlag + " " + s3RenderedConfigPath,
				"--dbname mydb",
				"--compression-level 6",
				"--jobs 4",
			},
			// gpbackup REJECTS --plugin-config together with --backup-dir, so the
			// S3 args must NOT carry --backup-dir at all.
			wantNotContain: []string{
				backupDirFlag,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := buildGpbackupArgs(tc.cluster, gpOpts, jobOpts)
			joined := strings.Join(args, " ")
			for _, want := range tc.wantContains {
				assert.Contains(t, joined, want, "args must contain %q", want)
			}
			for _, notWant := range tc.wantNotContain {
				assert.NotContains(t, joined, notWant, "args must NOT contain %q", notWant)
			}
		})
	}
}

// TestBuildGprestoreArgsScenario81 (Scenario 81 §C) asserts the destination-aware
// leading args for gprestore: local emits `--backup-dir <local.path>` and NO
// plugin; s3 emits ONLY `--plugin-config /tmp/s3-config.yaml` (gprestore rejects
// --plugin-config together with --backup-dir; the run happens inside the
// coordinator pod). The timestamp/jobs/create-db flags are unaffected.
func TestBuildGprestoreArgsScenario81(t *testing.T) {
	grOpts := &cbv1alpha1.GprestoreOptions{
		Jobs:     4,
		CreateDb: true,
	}
	jobOpts := &RestoreJobOptions{
		Timestamp: "20260608020000",
		Databases: []string{"mydb"},
	}

	tests := []struct {
		name           string
		cluster        *cbv1alpha1.CloudberryCluster
		wantContains   []string
		wantNotContain []string
	}{
		{
			name:    "local cluster emits --backup-dir and not --plugin-config",
			cluster: newLocalBackupCluster("/backups", "backup-pvc"),
			wantContains: []string{
				"--backup-dir /backups",
				"--timestamp 20260608020000",
				"--jobs 4",
				"--create-db",
			},
			wantNotContain: []string{
				pluginConfigFlag,
				s3RenderedConfigPath,
			},
		},
		{
			name:    "s3 cluster keeps --plugin-config only (no --backup-dir, which gprestore rejects)",
			cluster: newBackupCluster(),
			wantContains: []string{
				pluginConfigFlag + " " + s3RenderedConfigPath,
				"--timestamp 20260608020000",
				"--jobs 4",
				"--create-db",
			},
			wantNotContain: []string{
				backupDirFlag,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := buildGprestoreArgs(tc.cluster, grOpts, jobOpts)
			joined := strings.Join(args, " ")
			for _, want := range tc.wantContains {
				assert.Contains(t, joined, want, "args must contain %q", want)
			}
			for _, notWant := range tc.wantNotContain {
				assert.NotContains(t, joined, notWant, "args must NOT contain %q", notWant)
			}
		})
	}
}

// TestRenderToolScriptScenario81 (Scenario 81 §D) asserts that the rendered tool
// script for a LOCAL destination skips the S3-config envsubst/cat render block
// (so the Job does not crash under `set -euo pipefail` reading a missing
// /etc/gpbackup template) while keeping the set -euo pipefail + gpEnv/ssh
// preambles and the tool invocation with --backup-dir. The S3 sub-case is a
// regression guard that the envsubst -> /tmp/s3-config.yaml render is retained.
func TestRenderToolScriptScenario81(t *testing.T) {
	t.Run("local destination skips s3-config render", func(t *testing.T) {
		cluster := newLocalBackupCluster("/backups", "backup-pvc")
		args := buildGpbackupArgs(cluster, cluster.Spec.Backup.Gpbackup, &BackupJobOptions{
			Databases: []string{"mydb"},
		})
		script := renderToolScript(cluster, "gpbackup", args)

		// The S3-config render block must be ABSENT for local.
		assert.NotContains(t, script, s3ConfigMountPath)         // /etc/gpbackup
		assert.NotContains(t, script, s3ConfigTemplateKey)       // s3-plugin-config.yaml.tpl
		assert.NotContains(t, script, "> "+s3RenderedConfigPath) // > /tmp/s3-config.yaml
		assert.NotContains(t, script, "envsubst <")

		// Preambles and invocation must still be present.
		assert.True(t, strings.HasPrefix(script, "set -euo pipefail"),
			"script must start with set -euo pipefail")
		assert.Contains(t, script, `if [ -n "${GPHOME:-}" ]`) // gpEnvPreamble
		assert.Contains(t, script, "/etc/cloudberry/ssh/id_ed25519")
		assert.Contains(t, script, "gpbackup")
		assert.Contains(t, script, "--backup-dir")
		assert.Contains(t, script, shellQuote("/backups"))
	})

	t.Run("s3 destination renders the s3 plugin config and execs in the coordinator", func(t *testing.T) {
		cluster := newBackupCluster()
		args := buildGpbackupArgs(cluster, cluster.Spec.Backup.Gpbackup, &BackupJobOptions{
			Databases: []string{"mydb"},
		})
		script := renderToolScript(cluster, "gpbackup", args)

		// Regression guard: the S3 render of /etc/gpbackup -> /tmp/s3-config.yaml
		// must be present for the S3 destination (rendered in the Job pod, then
		// staged into the coordinator pod).
		assert.Contains(t, script, s3ConfigMountPath)
		assert.Contains(t, script, s3ConfigTemplateKey)
		assert.Contains(t, script, "> "+s3RenderedConfigPath)
		assert.Contains(t, script, "envsubst <")

		// Coordinator-exec model: the real gpbackup runs INSIDE the coordinator
		// pod via kubectl exec (spec 11 §MPP Dispatch). The S3 args carry only
		// --plugin-config (never --backup-dir, which gpbackup rejects together).
		assert.Contains(t, script, "\"${KUBECTL}\" exec")
		assert.Contains(t, script, "KUBECTL="+kubectlBin)
		assert.Contains(t, script, util.CoordinatorPodName(cluster.Name))
		assert.Contains(t, script, "gpbackup --plugin-config")
		assert.NotContains(t, script, backupDirFlag)
	})

	t.Run("local rendered script is valid bash", func(t *testing.T) {
		shell := lookupShellScenario81(t)
		cluster := newLocalBackupCluster("/backups", "backup-pvc")
		args := buildGpbackupArgs(cluster, cluster.Spec.Backup.Gpbackup, &BackupJobOptions{
			Databases: []string{"mydb"},
		})
		script := renderToolScript(cluster, "gpbackup", args)

		cmd := exec.Command(shell, "-n") //nolint:gosec // fixed shell, script via stdin
		cmd.Stdin = strings.NewReader(script)
		out, runErr := cmd.CombinedOutput()
		require.NoError(t, runErr, "%s -n reported a syntax error: %s", shell, string(out))
	})
}

// lookupShellScenario81 returns a usable POSIX shell path for `-n` syntax checks,
// preferring bash and falling back to sh. It skips the test when none is found.
func lookupShellScenario81(t *testing.T) string {
	t.Helper()
	if bash, err := exec.LookPath("bash"); err == nil {
		return bash
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no bash/sh available; skipping shell syntax check")
	}
	return sh
}

// TestBuildBackupJobLocalDestinationScenario81 (Scenario 81 §E) asserts the
// on-demand backup Job for a LOCAL cluster mounts the named PVC at the local
// path, passes --backup-dir to the container, and carries NO s3-config
// ConfigMap volume nor /etc/gpbackup mount.
func TestBuildBackupJobLocalDestinationScenario81(t *testing.T) {
	b := NewBuilder()
	cluster := newLocalBackupCluster("/backups", "backup-pvc")
	job := b.BuildBackupJob(cluster, &BackupJobOptions{
		Timestamp: "20260608020000",
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)

	podSpec := job.Spec.Template.Spec
	assertLocalBackupPodSpec(t, podSpec, "/backups", "backup-pvc")

	// The gpbackup invocation carries --backup-dir and not --plugin-config.
	script := podSpec.Containers[0].Args[0]
	assert.Contains(t, script, "--backup-dir")
	assert.Contains(t, script, shellQuote("/backups"))
	assert.NotContains(t, script, pluginConfigFlag)
}

// TestBuildRestoreJobLocalDestinationScenario81 (Scenario 81 §E) asserts the
// restore Job for a LOCAL cluster mounts the PVC at the path and passes
// --backup-dir, with no s3-config volume/mount.
func TestBuildRestoreJobLocalDestinationScenario81(t *testing.T) {
	b := NewBuilder()
	cluster := newLocalBackupCluster("/data/backups", "backup-pvc")
	job := b.BuildRestoreJob(cluster, &RestoreJobOptions{
		Timestamp: "20260608020000",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)

	podSpec := job.Spec.Template.Spec
	assertLocalBackupPodSpec(t, podSpec, "/data/backups", "backup-pvc")

	script := podSpec.Containers[0].Args[0]
	assert.Contains(t, script, "--backup-dir")
	assert.Contains(t, script, shellQuote("/data/backups"))
	assert.NotContains(t, script, pluginConfigFlag)
}

// TestBuildBackupJobS3VolumeRegressionScenario81 (Scenario 81 §E regression)
// asserts the S3 backup Job still mounts the s3-plugin-config ConfigMap volume
// at /etc/gpbackup and does NOT carry the local PVC volume.
func TestBuildBackupJobS3VolumeRegressionScenario81(t *testing.T) {
	b := NewBuilder()
	job := b.BuildBackupJob(newBackupCluster(), &BackupJobOptions{
		Timestamp: "20260608020000",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)

	podSpec := job.Spec.Template.Spec
	// The S3 plugin-config ConfigMap volume IS present.
	s3Vol := findVolume(podSpec.Volumes, s3ConfigVolumeName)
	require.NotNil(t, s3Vol, "s3 Job must include the s3-plugin-config volume")
	require.NotNil(t, s3Vol.ConfigMap, "s3-plugin-config volume must be a ConfigMap")

	// No local PVC volume for the S3 destination.
	assert.Nil(t, findVolume(podSpec.Volumes, localBackupVolumeName),
		"s3 Job must NOT include the local backup PVC volume")

	// The container mounts /etc/gpbackup.
	c := podSpec.Containers[0]
	assert.True(t, hasMount(c.VolumeMounts, s3ConfigVolumeName, s3ConfigMountPath),
		"s3 container must mount the plugin config at /etc/gpbackup")
}

// assertLocalBackupPodSpec asserts the Scenario 81 local-destination pod wiring:
// a PVC volume named backup-data with the given claim mounted at the given path,
// no s3-plugin-config volume, and no /etc/gpbackup mount. The shared SSH +
// backup-history wiring (Scenario 71) must still be present.
func assertLocalBackupPodSpec(t *testing.T, podSpec corev1.PodSpec, path, claim string) {
	t.Helper()

	// The local PVC volume IS present and bound to the configured claim.
	pvcVol := findVolume(podSpec.Volumes, localBackupVolumeName)
	require.NotNil(t, pvcVol, "local Job must include the backup-data PVC volume")
	require.NotNil(t, pvcVol.PersistentVolumeClaim, "backup-data volume must be a PVC")
	assert.Equal(t, claim, pvcVol.PersistentVolumeClaim.ClaimName)

	// The s3-plugin-config ConfigMap volume must be ABSENT.
	assert.Nil(t, findVolume(podSpec.Volumes, s3ConfigVolumeName),
		"local Job must NOT include the s3-plugin-config volume")

	require.NotEmpty(t, podSpec.Containers)
	c := podSpec.Containers[0]

	// The PVC is mounted at the local path; no /etc/gpbackup mount.
	assert.True(t, hasMount(c.VolumeMounts, localBackupVolumeName, path),
		"container must mount backup-data at %s", path)
	for _, m := range c.VolumeMounts {
		assert.NotEqual(t, s3ConfigMountPath, m.MountPath,
			"local container must NOT mount the s3 config at /etc/gpbackup")
	}

	// Scenario 71 shared SSH + history wiring is retained.
	require.NotNil(t, findVolume(podSpec.Volumes, sshSecretVolumeName),
		"local Job must still include the cluster-ssh volume")
	require.NotNil(t, findVolume(podSpec.Volumes, backupHistoryVolumeName),
		"local Job must still include the backup-history volume")

	// The local container carries NO S3_*/AWS_* env (buildBackupEnv non-S3 path).
	for _, e := range c.Env {
		assert.NotEqual(t, "S3_BUCKET", e.Name)
		assert.NotEqual(t, "S3_ENDPOINT", e.Name)
		assert.NotEqual(t, "AWS_ACCESS_KEY_ID", e.Name)
		assert.NotEqual(t, "AWS_SECRET_ACCESS_KEY", e.Name)
	}
}

// TestBuildBackupVolumesLocalScenario81 covers buildBackupVolumes/Mounts for the
// local destination, including the PVC-less edge case (no volume emitted).
func TestBuildBackupVolumesLocalScenario81(t *testing.T) {
	t.Run("local with pvc emits PVC volume and mount", func(t *testing.T) {
		cluster := newLocalBackupCluster("/data/bk", "my-pvc")
		volumes := buildBackupVolumes(cluster)
		vol := findVolume(volumes, localBackupVolumeName)
		require.NotNil(t, vol)
		require.NotNil(t, vol.PersistentVolumeClaim)
		assert.Equal(t, "my-pvc", vol.PersistentVolumeClaim.ClaimName)
		assert.Nil(t, findVolume(volumes, s3ConfigVolumeName))

		mounts := buildBackupVolumeMounts(cluster)
		assert.True(t, hasMount(mounts, localBackupVolumeName, "/data/bk"))
	})

	t.Run("local with empty path mounts at default /backups", func(t *testing.T) {
		cluster := newLocalBackupCluster("", "my-pvc")
		mounts := buildBackupVolumeMounts(cluster)
		assert.True(t, hasMount(mounts, localBackupVolumeName, localBackupMountPath))
	})

	t.Run("local without pvc emits no volume or mount", func(t *testing.T) {
		cluster := newLocalBackupCluster("/backups", "")
		cluster.Spec.Backup.Destination.Local.PersistentVolumeClaim = ""
		assert.Empty(t, buildBackupVolumes(cluster))
		assert.Empty(t, buildBackupVolumeMounts(cluster))
	})

	t.Run("nil Local emits no volume or mount", func(t *testing.T) {
		cluster := newLocalBackupCluster("/backups", "backup-pvc")
		cluster.Spec.Backup.Destination.Local = nil
		assert.Empty(t, buildBackupVolumes(cluster))
		assert.Empty(t, buildBackupVolumeMounts(cluster))
	})

	t.Run("nil backup emits no volume or mount", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Backup = nil
		assert.Empty(t, buildBackupVolumes(cluster))
		assert.Empty(t, buildBackupVolumeMounts(cluster))
	})

	t.Run("unknown destination type emits no volume or mount", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Destination.Type = "gcs"
		cluster.Spec.Backup.Destination.S3 = nil
		assert.Empty(t, buildBackupVolumes(cluster))
		assert.Empty(t, buildBackupVolumeMounts(cluster))
	})
}

// TestBuildGpbackmanRetentionScriptLocalScenario81 (Scenario 81 §F) asserts the
// retention script for a LOCAL destination uses DEST_FLAGS='--backup-dir <path>'
// for the gpbackman commands and does NOT render the S3 plugin config. The S3
// sub-case is a regression guard that the plugin-config DEST_FLAGS + S3 render
// are retained.
func TestBuildGpbackmanRetentionScriptLocalScenario81(t *testing.T) {
	t.Run("local destination uses --backup-dir DEST_FLAGS and no s3 render", func(t *testing.T) {
		cluster := newLocalBackupCluster("/backups", "backup-pvc")
		cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		}
		script := buildGpbackmanRetentionScript(cluster)

		// DEST_FLAGS is the local --backup-dir selector.
		assert.Contains(t, script, "DEST_FLAGS="+shellQuote(backupDirFlag+" /backups"))
		// gpbackman commands use ${DEST_FLAGS}.
		assert.Contains(t, script, "backup-delete --timestamp \"$1\" --cascade ${DEST_FLAGS}")
		assert.Contains(t, script, "backup-clean --older-than-days 30 ${DEST_FLAGS}")

		// The S3 plugin config render must be ABSENT for local.
		assert.NotContains(t, script, s3ConfigMountPath)
		assert.NotContains(t, script, s3ConfigTemplateKey)
		assert.NotContains(t, script, pluginConfigFlag)
		assert.NotContains(t, script, "envsubst <")
	})

	t.Run("local custom path", func(t *testing.T) {
		cluster := newLocalBackupCluster("/data/bk", "backup-pvc")
		cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{FullCount: 2}
		script := buildGpbackmanRetentionScript(cluster)
		assert.Contains(t, script, "DEST_FLAGS="+shellQuote(backupDirFlag+" /data/bk"))
		assert.NotContains(t, script, pluginConfigFlag)
	})

	t.Run("s3 destination keeps --plugin-config DEST_FLAGS and s3 render", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{
			FullCount: 3,
			MaxAge:    "30d",
		}
		script := buildGpbackmanRetentionScript(cluster)

		// Regression guard: S3 render + plugin-config DEST_FLAGS retained.
		assert.Contains(t, script, "DEST_FLAGS="+shellQuote(pluginConfigFlag+" "+s3RenderedConfigPath))
		assert.Contains(t, script, s3ConfigMountPath)
		assert.Contains(t, script, s3ConfigTemplateKey)
		assert.Contains(t, script, "envsubst <")
		assert.NotContains(t, script, backupDirFlag)
	})

	t.Run("local retention script is valid bash", func(t *testing.T) {
		shell := lookupShellScenario81(t)
		cluster := newLocalBackupCluster("/backups", "backup-pvc")
		cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		}
		script := buildGpbackmanRetentionScript(cluster)

		cmd := exec.Command(shell, "-n") //nolint:gosec // fixed shell, script via stdin
		cmd.Stdin = strings.NewReader(script)
		out, runErr := cmd.CombinedOutput()
		require.NoError(t, runErr, "%s -n reported a syntax error: %s", shell, string(out))
	})
}

// TestBuildRetentionCleanupJobLocalScenario81 asserts the retention cleanup Job
// for a local cluster renders the local DEST_FLAGS and mounts the PVC volume.
func TestBuildRetentionCleanupJobLocalScenario81(t *testing.T) {
	b := NewBuilder()
	cluster := newLocalBackupCluster("/backups", "backup-pvc")
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{FullCount: 3}
	job := b.BuildRetentionCleanupJob(cluster, "20260608020000")
	require.NotNil(t, job)

	podSpec := job.Spec.Template.Spec
	pvcVol := findVolume(podSpec.Volumes, localBackupVolumeName)
	require.NotNil(t, pvcVol, "local cleanup Job must mount the backup PVC")
	assert.Nil(t, findVolume(podSpec.Volumes, s3ConfigVolumeName))

	script := podSpec.Containers[0].Args[0]
	assert.Contains(t, script, "DEST_FLAGS="+shellQuote(backupDirFlag+" /backups"))
}

// TestBuildBackupS3ConfigMapLocalNilScenario81 (Scenario 81 §G) is a regression
// guard: BuildBackupS3ConfigMap returns nil for a local destination.
func TestBuildBackupS3ConfigMapLocalNilScenario81(t *testing.T) {
	b := NewBuilder()
	cluster := newLocalBackupCluster("/backups", "backup-pvc")
	assert.Nil(t, b.BuildBackupS3ConfigMap(cluster),
		"BuildBackupS3ConfigMap must return nil for a local destination")
}

// TestBuildBackupEnvLocalNoS3Scenario81 verifies buildBackupEnv for a local
// destination returns only the PG* connection env and NO S3_*/AWS_* env.
func TestBuildBackupEnvLocalNoS3Scenario81(t *testing.T) {
	cluster := newLocalBackupCluster("/backups", "backup-pvc")
	env := buildBackupEnv(cluster)

	names := make(map[string]bool, len(env))
	for _, e := range env {
		names[e.Name] = true
	}
	assert.True(t, names["PGHOST"], "PG* connection env must still be present")
	assert.False(t, names["S3_BUCKET"], "local env must NOT carry S3_BUCKET")
	assert.False(t, names["S3_ENDPOINT"], "local env must NOT carry S3_ENDPOINT")
	assert.False(t, names["AWS_ACCESS_KEY_ID"], "local env must NOT carry AWS_ACCESS_KEY_ID")
	assert.False(t, names["AWS_SECRET_ACCESS_KEY"], "local env must NOT carry AWS_SECRET_ACCESS_KEY")
}
