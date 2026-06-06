package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestBackupSpec_DeepCopy_FullyPopulated exercises every non-nil pointer branch
// of BackupSpec.DeepCopyInto and the nested backup destination types
// (S3Destination, LocalDestination, S3CredentialSecret, S3Multipart,
// GpbackupOptions, GprestoreOptions, BackupJobTemplate). It verifies the copy is
// deeply equal yet independent of the source.
func TestBackupSpec_DeepCopy_FullyPopulated(t *testing.T) {
	backoff := int32(3)
	deadline := int64(600)
	ttl := int32(120)

	src := &BackupSpec{
		Retention: BackupRetention{FullCount: 5, IncrementalCount: 10, MaxAge: "30d"},
		Destination: BackupDestination{
			Type: "s3",
			S3: &S3Destination{
				Bucket:           "my-bucket",
				Endpoint:         "minio:9000",
				Region:           "us-east-1",
				Folder:           "backups",
				Encryption:       "on",
				ForcePathStyle:   true,
				CredentialSecret: &S3CredentialSecret{Name: "s3-creds", AccessKeyField: "ak", SecretKeyField: "sk"},
				VaultSecret:      &S3VaultSecret{Path: "secret/data/s3", AccessKeyField: "ak", SecretKeyField: "sk"},
				Multipart: &S3Multipart{
					BackupMaxConcurrentRequests:  4,
					BackupMultipartChunksize:     "10MB",
					RestoreMaxConcurrentRequests: 2,
					RestoreMultipartChunksize:    "20MB",
				},
			},
		},
		Gpbackup:  &GpbackupOptions{CompressionLevel: 5, CompressionType: "zstd", SingleDataFile: true, Jobs: 4},
		Gprestore: &GprestoreOptions{Jobs: 8, CreateDb: true, RunAnalyze: true},
		JobTemplate: &BackupJobTemplate{
			Resources:               &ResourceRequirements{},
			NodeSelector:            map[string]string{"disk": "ssd"},
			Tolerations:             []Toleration{{Key: "k", Value: "v", Effect: "NoSchedule"}},
			BackoffLimit:            &backoff,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttl,
		},
	}

	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src.Destination.S3, copied.Destination.S3)
	assert.NotSame(t, src.Destination.S3.CredentialSecret, copied.Destination.S3.CredentialSecret)
	assert.NotSame(t, src.Destination.S3.Multipart, copied.Destination.S3.Multipart)
	assert.NotSame(t, src.Gpbackup, copied.Gpbackup)
	assert.NotSame(t, src.Gprestore, copied.Gprestore)
	assert.NotSame(t, src.JobTemplate, copied.JobTemplate)

	// Mutate the copy and ensure the source is untouched.
	copied.Destination.S3.Bucket = "changed"
	copied.Gpbackup.Jobs = 99
	copied.JobTemplate.NodeSelector["disk"] = "hdd"
	*copied.JobTemplate.BackoffLimit = 9
	assert.Equal(t, "my-bucket", src.Destination.S3.Bucket)
	assert.Equal(t, int32(4), src.Gpbackup.Jobs)
	assert.Equal(t, "ssd", src.JobTemplate.NodeSelector["disk"])
	assert.Equal(t, int32(3), *src.JobTemplate.BackoffLimit)
}

// TestBackupDestination_DeepCopy_Local exercises the Local (non-S3) branch of
// BackupDestination.DeepCopyInto and LocalDestination.DeepCopy.
func TestBackupDestination_DeepCopy_Local(t *testing.T) {
	src := &BackupDestination{
		Type:  "local",
		Local: &LocalDestination{Path: "/backups", PersistentVolumeClaim: "backup-pvc"},
	}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src.Local, copied.Local)

	copied.Local.Path = "/other"
	assert.Equal(t, "/backups", src.Local.Path)
}

// TestBackupHistoryEntry_DeepCopy exercises BackupHistoryEntry.DeepCopy.
func TestBackupHistoryEntry_DeepCopy(t *testing.T) {
	src := &BackupHistoryEntry{
		Timestamp: "20250101000000", Type: "full", Status: "Success",
		Size: "2.4Gi", Duration: "5m32s",
	}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src, copied)
}

// TestGpbackupOptions_DeepCopy exercises GpbackupOptions.DeepCopy.
func TestGpbackupOptions_DeepCopy(t *testing.T) {
	src := &GpbackupOptions{CompressionLevel: 9, CompressionType: "gzip", Incremental: true}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src, copied)
}

// TestGprestoreOptions_DeepCopy exercises GprestoreOptions.DeepCopy.
func TestGprestoreOptions_DeepCopy(t *testing.T) {
	src := &GprestoreOptions{Jobs: 4, CreateDb: true, MetadataOnly: true}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src, copied)
}

// TestLocalDestination_DeepCopy exercises LocalDestination.DeepCopy directly.
func TestLocalDestination_DeepCopy(t *testing.T) {
	src := &LocalDestination{Path: "/p", PersistentVolumeClaim: "pvc"}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src, copied)
}

// TestS3CredentialSecret_DeepCopy exercises S3CredentialSecret.DeepCopy.
func TestS3CredentialSecret_DeepCopy(t *testing.T) {
	src := &S3CredentialSecret{Name: "s", AccessKeyField: "a", SecretKeyField: "b"}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src, copied)
}

// TestS3Multipart_DeepCopy exercises S3Multipart.DeepCopy.
func TestS3Multipart_DeepCopy(t *testing.T) {
	src := &S3Multipart{
		BackupMaxConcurrentRequests: 4, BackupMultipartChunksize: "10MB",
		RestoreMaxConcurrentRequests: 2, RestoreMultipartChunksize: "20MB",
	}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src, copied)
}

// TestCloudberryClusterStatus_DeepCopy_FullyPopulated exercises every non-nil
// branch of CloudberryClusterStatus.DeepCopyInto: time pointers, BackupHistory,
// Conditions, and FailedSegments.
func TestCloudberryClusterStatus_DeepCopy_FullyPopulated(t *testing.T) {
	now := metav1.Now()
	src := &CloudberryClusterStatus{
		Phase:                "Running",
		CoordinatorReady:     true,
		SegmentsReady:        4,
		SegmentsTotal:        4,
		LastReconcileTime:    &now,
		LastConfigChangeTime: &now,
		LastBackupTime:       &now,
		BackupHistory: []BackupHistoryEntry{
			{Timestamp: "20250101000000", Type: "full", Status: "Success"},
		},
		Conditions: []metav1.Condition{
			{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllUp"},
		},
		FailedSegments: []FailedSegment{
			{ContentID: 1, Hostname: "h1", Role: "primary", Status: "down"},
		},
	}

	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src.LastReconcileTime, copied.LastReconcileTime)
	require.Len(t, copied.BackupHistory, 1)
	require.Len(t, copied.FailedSegments, 1)

	// Mutate the copy and ensure source independence.
	copied.BackupHistory[0].Status = "Failed"
	copied.FailedSegments[0].Status = "up"
	assert.Equal(t, "Success", src.BackupHistory[0].Status)
	assert.Equal(t, "down", src.FailedSegments[0].Status)
}

// TestCloudberryClusterStatus_DeepCopy_Empty exercises the all-nil branch.
func TestCloudberryClusterStatus_DeepCopy_Empty(t *testing.T) {
	src := &CloudberryClusterStatus{Phase: "Pending"}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.Nil(t, copied.BackupHistory)
	assert.Nil(t, copied.LastReconcileTime)
}
