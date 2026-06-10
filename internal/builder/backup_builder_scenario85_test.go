package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestAppendLeafPartitionDataArgs_Scenario85 is the Scenario 85 GAP-B
// table-driven regression for the leaf-partition-data wiring on FULL backups
// (and its interaction with the incremental path). It asserts the net invariant:
//   - full + LeafPartitionData=true  => EXACTLY ONE --leaf-partition-data, NO --incremental
//   - full + LeafPartitionData=false => NO --leaf-partition-data
//   - incremental (spec or per-Job) + any LeafPartitionData => EXACTLY ONE
//     --leaf-partition-data WITH --incremental (not duplicated).
func TestAppendLeafPartitionDataArgs_Scenario85(t *testing.T) {
	tests := []struct {
		name            string
		opts            *cbv1alpha1.GpbackupOptions
		jobOpts         *BackupJobOptions
		wantLeaf        int
		wantIncremental int
		wantContains    []string
		wantNotContain  []string
	}{
		{
			name:            "full backup with leaf-partition-data emits exactly one leaf flag and no incremental",
			opts:            &cbv1alpha1.GpbackupOptions{LeafPartitionData: true},
			jobOpts:         nil,
			wantLeaf:        1,
			wantIncremental: 0,
			wantContains:    []string{"--leaf-partition-data"},
			wantNotContain:  []string{"--incremental"},
		},
		{
			name:            "full backup without leaf-partition-data emits no leaf flag",
			opts:            &cbv1alpha1.GpbackupOptions{LeafPartitionData: false},
			jobOpts:         nil,
			wantLeaf:        0,
			wantIncremental: 0,
			wantNotContain:  []string{"--leaf-partition-data", "--incremental"},
		},
		{
			name: "full backup with leaf-partition-data via per-Job full type emits one leaf flag",
			opts: &cbv1alpha1.GpbackupOptions{LeafPartitionData: true},
			jobOpts: &BackupJobOptions{
				Type:      util.BackupTypeFull,
				Databases: []string{"mydb"},
			},
			wantLeaf:        1,
			wantIncremental: 0,
			wantContains:    []string{"--leaf-partition-data", "--dbname mydb"},
			wantNotContain:  []string{"--incremental"},
		},
		{
			name:            "incremental via opts with leaf-partition-data does not duplicate leaf flag",
			opts:            &cbv1alpha1.GpbackupOptions{Incremental: true, LeafPartitionData: true},
			jobOpts:         nil,
			wantLeaf:        1,
			wantIncremental: 1,
			wantContains:    []string{"--incremental", "--leaf-partition-data"},
		},
		{
			name:            "incremental via opts without leaf-partition-data still emits exactly one leaf flag",
			opts:            &cbv1alpha1.GpbackupOptions{Incremental: true, LeafPartitionData: false},
			jobOpts:         nil,
			wantLeaf:        1,
			wantIncremental: 1,
			wantContains:    []string{"--incremental", "--leaf-partition-data"},
		},
		{
			name:            "incremental via per-Job type with leaf-partition-data does not duplicate leaf flag",
			opts:            &cbv1alpha1.GpbackupOptions{LeafPartitionData: true},
			jobOpts:         &BackupJobOptions{Type: util.BackupTypeIncremental},
			wantLeaf:        1,
			wantIncremental: 1,
			wantContains:    []string{"--incremental", "--leaf-partition-data"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := mustGpbackupArgs(t, newBackupCluster(), tc.opts, tc.jobOpts)
			joined := strings.Join(args, " ")

			assert.Equal(t, tc.wantLeaf, countFlag(args, "--leaf-partition-data"),
				"--leaf-partition-data count mismatch in %q", joined)
			assert.Equal(t, tc.wantIncremental, countFlag(args, "--incremental"),
				"--incremental count mismatch in %q", joined)

			for _, want := range tc.wantContains {
				assert.Contains(t, joined, want)
			}
			for _, notWant := range tc.wantNotContain {
				assert.NotContains(t, joined, notWant)
			}
		})
	}
}

// TestIsEffectivelyIncremental_Scenario85 is the truth table for the
// isEffectivelyIncremental helper that drives both the incremental flags and the
// full-backup leaf-partition-data branch. It must be true when opts.Incremental
// is set OR jobOpts.Type=="incremental", and false otherwise (including nil
// inputs).
func TestIsEffectivelyIncremental_Scenario85(t *testing.T) {
	tests := []struct {
		name    string
		opts    *cbv1alpha1.GpbackupOptions
		jobOpts *BackupJobOptions
		want    bool
	}{
		{
			name:    "nil opts and nil jobOpts => false",
			opts:    nil,
			jobOpts: nil,
			want:    false,
		},
		{
			name:    "opts.Incremental true => true",
			opts:    &cbv1alpha1.GpbackupOptions{Incremental: true},
			jobOpts: nil,
			want:    true,
		},
		{
			name:    "jobOpts.Type incremental => true",
			opts:    &cbv1alpha1.GpbackupOptions{},
			jobOpts: &BackupJobOptions{Type: util.BackupTypeIncremental},
			want:    true,
		},
		{
			name:    "neither incremental => false",
			opts:    &cbv1alpha1.GpbackupOptions{Incremental: false},
			jobOpts: &BackupJobOptions{Type: util.BackupTypeFull},
			want:    false,
		},
		{
			name:    "both set => true",
			opts:    &cbv1alpha1.GpbackupOptions{Incremental: true},
			jobOpts: &BackupJobOptions{Type: util.BackupTypeIncremental},
			want:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isEffectivelyIncremental(tc.opts, tc.jobOpts))
		})
	}
}

// TestBuildBackupJobFullLeafPartitionData_Scenario85 verifies the GAP-B fix
// end-to-end through BuildBackupJob: a FULL backup with LeafPartitionData=true
// renders a gpbackup script that contains exactly one --leaf-partition-data and
// no --incremental.
func TestBuildBackupJobFullLeafPartitionData_Scenario85(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{}

	job := b.BuildBackupJob(cluster, &BackupJobOptions{
		Timestamp: "20260101010101",
		Type:      util.BackupTypeFull,
		Databases: []string{"mydb"},
		Gpbackup:  &cbv1alpha1.GpbackupOptions{LeafPartitionData: true},
	})
	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Equal(t, 1, strings.Count(script, "--leaf-partition-data"))
	assert.NotContains(t, script, "--incremental")
}
