package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// countFlag counts how many times flag appears as a standalone token in args.
func countFlag(args []string, flag string) int {
	n := 0
	for _, a := range args {
		if a == flag {
			n++
		}
	}
	return n
}

// TestAppendIncrementalArgs_Scenario78 is a table-driven test for the Scenario
// 78a forced-leaf-partition-data wiring in appendIncrementalArgs (exercised via
// buildGpbackupArgs). It asserts that whenever an incremental backup is
// effective both --incremental and --leaf-partition-data are emitted EXACTLY
// once (no duplicate even when LeafPartitionData is also explicitly set), that
// the per-Job incremental type forces the flags on a full spec, that
// FromTimestamp is wired through, and that full backups emit NEITHER flag.
func TestAppendIncrementalArgs_Scenario78(t *testing.T) {
	tests := []struct {
		name            string
		opts            *cbv1alpha1.GpbackupOptions
		jobOpts         *BackupJobOptions
		wantIncremental int
		wantLeaf        int
		wantContains    []string
		wantNotContain  []string
	}{
		{
			name: "incremental spec without leaf-partition-data forces leaf flag once",
			opts: &cbv1alpha1.GpbackupOptions{
				Incremental:       true,
				LeafPartitionData: false,
			},
			jobOpts:         nil,
			wantIncremental: 1,
			wantLeaf:        1,
			wantContains:    []string{"--incremental", "--leaf-partition-data"},
		},
		{
			name: "incremental spec with leaf-partition-data does not duplicate leaf flag",
			opts: &cbv1alpha1.GpbackupOptions{
				Incremental:       true,
				LeafPartitionData: true,
			},
			jobOpts:         nil,
			wantIncremental: 1,
			wantLeaf:        1,
			wantContains:    []string{"--incremental", "--leaf-partition-data"},
		},
		{
			name:            "per-job incremental on full spec forces both flags",
			opts:            &cbv1alpha1.GpbackupOptions{Incremental: false},
			jobOpts:         &BackupJobOptions{Type: util.BackupTypeIncremental},
			wantIncremental: 1,
			wantLeaf:        1,
			wantContains:    []string{"--incremental", "--leaf-partition-data"},
		},
		{
			name:            "incremental with from-timestamp pin",
			opts:            &cbv1alpha1.GpbackupOptions{Incremental: true},
			jobOpts:         &BackupJobOptions{FromTimestamp: "20260101010101"},
			wantIncremental: 1,
			wantLeaf:        1,
			wantContains: []string{
				"--incremental",
				"--leaf-partition-data",
				"--from-timestamp 20260101010101",
			},
		},
		{
			name:            "full backup emits neither incremental nor leaf flag",
			opts:            &cbv1alpha1.GpbackupOptions{Incremental: false},
			jobOpts:         nil,
			wantIncremental: 0,
			wantLeaf:        0,
			wantNotContain:  []string{"--incremental", "--leaf-partition-data", "--from-timestamp"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := mustGpbackupArgs(t, newBackupCluster(), tc.opts, tc.jobOpts)
			joined := strings.Join(args, " ")

			assert.Equal(t, tc.wantIncremental, countFlag(args, "--incremental"),
				"--incremental count mismatch in %q", joined)
			assert.Equal(t, tc.wantLeaf, countFlag(args, "--leaf-partition-data"),
				"--leaf-partition-data count mismatch in %q", joined)

			for _, want := range tc.wantContains {
				assert.Contains(t, joined, want)
			}
			for _, notWant := range tc.wantNotContain {
				assert.NotContains(t, joined, notWant)
			}
		})
	}
}

// TestEffectiveBackupType_Scenario78 is a table-driven test for the
// effectiveBackupType helper added for Scenario 78b: it resolves "incremental"
// when the spec OR per-request options enable incremental, else "full".
func TestEffectiveBackupType_Scenario78(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c *cbv1alpha1.CloudberryCluster)
		opts   *BackupJobOptions
		want   string
	}{
		{
			name:   "full spec, no opts => full",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { c.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{} },
			opts:   nil,
			want:   util.BackupTypeFull,
		},
		{
			name: "spec incremental => incremental",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: true}
			},
			opts: nil,
			want: util.BackupTypeIncremental,
		},
		{
			name:   "full spec, opts.Type incremental => incremental",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { c.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{} },
			opts:   &BackupJobOptions{Type: util.BackupTypeIncremental},
			want:   util.BackupTypeIncremental,
		},
		{
			name:   "full spec, opts.Gpbackup.Incremental => incremental",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { c.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{} },
			opts:   &BackupJobOptions{Gpbackup: &cbv1alpha1.GpbackupOptions{Incremental: true}},
			want:   util.BackupTypeIncremental,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newBackupCluster()
			tc.mutate(cluster)
			assert.Equal(t, tc.want, effectiveBackupType(cluster, tc.opts))
		})
	}
}

// TestBuildBackupJobBackupTypeLabel_Scenario78 verifies BuildBackupJob stamps
// the avsoft.io/backup-type label on BOTH the Job metadata and the pod template
// for incremental and full backups.
func TestBuildBackupJobBackupTypeLabel_Scenario78(t *testing.T) {
	b := NewBuilder()

	t.Run("incremental cluster labels job full=incremental", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: true}
		job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260101010101"})
		require.NotNil(t, job)
		assert.Equal(t, util.BackupTypeIncremental, job.Labels[util.LabelBackupType],
			"Job metadata must carry the incremental backup-type label")
		assert.Equal(t, util.BackupTypeIncremental,
			job.Spec.Template.ObjectMeta.Labels[util.LabelBackupType],
			"pod template must carry the incremental backup-type label")
	})

	t.Run("full cluster labels job full", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: false}
		job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260101010101"})
		require.NotNil(t, job)
		assert.Equal(t, util.BackupTypeFull, job.Labels[util.LabelBackupType])
		assert.Equal(t, util.BackupTypeFull,
			job.Spec.Template.ObjectMeta.Labels[util.LabelBackupType])
	})

	t.Run("per-job incremental on full spec labels incremental", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: false}
		job := b.BuildBackupJob(cluster, &BackupJobOptions{
			Timestamp: "20260101010101",
			Type:      util.BackupTypeIncremental,
		})
		require.NotNil(t, job)
		assert.Equal(t, util.BackupTypeIncremental, job.Labels[util.LabelBackupType])
		assert.Equal(t, util.BackupTypeIncremental,
			job.Spec.Template.ObjectMeta.Labels[util.LabelBackupType])
	})
}

// TestBuildBackupCronJobBackupTypeLabel_Scenario78 verifies BuildBackupCronJob
// stamps the avsoft.io/backup-type label on the CronJob metadata, the
// jobTemplate metadata and the pod template for incremental and full specs.
func TestBuildBackupCronJobBackupTypeLabel_Scenario78(t *testing.T) {
	b := NewBuilder()

	t.Run("incremental spec labels cronjob incremental", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: true}
		cj := b.BuildBackupCronJob(cluster)
		require.NotNil(t, cj)
		assert.Equal(t, util.BackupTypeIncremental, cj.Labels[util.LabelBackupType],
			"CronJob metadata must carry the incremental backup-type label")
		assert.Equal(t, util.BackupTypeIncremental,
			cj.Spec.JobTemplate.ObjectMeta.Labels[util.LabelBackupType],
			"jobTemplate must carry the incremental backup-type label")
		assert.Equal(t, util.BackupTypeIncremental,
			cj.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels[util.LabelBackupType],
			"pod template must carry the incremental backup-type label")
	})

	t.Run("full spec labels cronjob full", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: false}
		cj := b.BuildBackupCronJob(cluster)
		require.NotNil(t, cj)
		assert.Equal(t, util.BackupTypeFull, cj.Labels[util.LabelBackupType])
		assert.Equal(t, util.BackupTypeFull,
			cj.Spec.JobTemplate.ObjectMeta.Labels[util.LabelBackupType])
		assert.Equal(t, util.BackupTypeFull,
			cj.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels[util.LabelBackupType])
	})
}
