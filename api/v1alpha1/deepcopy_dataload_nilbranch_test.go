package v1alpha1

// Deepcopy tests for the remaining low-coverage generated functions: the
// nil-receiver DeepCopy branches and the fully-populated pointer branches of
// the data-loading job types (GploadJobSpec, PxfJobSpec, PartitioningSpec,
// ErrorHandlingSpec, PxfExtensionsSpec, PxfCustomConnector, GpbackupOptions
// WithStats, BackupSpec.Validation, DataLoadingSpec.HealthChecks). Copies
// must be deeply equal yet fully independent of their source.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// boolPtr is declared in deepcopy_affinity_test.go and reused here.

// TestDeepCopy_NilReceivers_BackupAndDataLoading pins the
// `if in == nil { return nil }` guard of the generated DeepCopy methods for
// the backup/data-loading types whose nil branch was uncovered.
func TestDeepCopy_NilReceivers_BackupAndDataLoading(t *testing.T) {
	tests := []struct {
		name string
		call func() any
	}{
		{"BackupHistoryEntry", func() any { return (*BackupHistoryEntry)(nil).DeepCopy() }},
		{"ErrorHandlingSpec", func() any { return (*ErrorHandlingSpec)(nil).DeepCopy() }},
		{"GpbackupOptions", func() any { return (*GpbackupOptions)(nil).DeepCopy() }},
		{"GprestoreOptions", func() any { return (*GprestoreOptions)(nil).DeepCopy() }},
		{"GploadJobSpec", func() any { return (*GploadJobSpec)(nil).DeepCopy() }},
		{"LocalDestination", func() any { return (*LocalDestination)(nil).DeepCopy() }},
		{"PartitioningSpec", func() any { return (*PartitioningSpec)(nil).DeepCopy() }},
		{"PxfCustomConnector", func() any { return (*PxfCustomConnector)(nil).DeepCopy() }},
		{"PxfExtensionsSpec", func() any { return (*PxfExtensionsSpec)(nil).DeepCopy() }},
		{"PxfJobSpec", func() any { return (*PxfJobSpec)(nil).DeepCopy() }},
		{"S3CredentialSecret", func() any { return (*S3CredentialSecret)(nil).DeepCopy() }},
		{"S3Destination", func() any { return (*S3Destination)(nil).DeepCopy() }},
		{"S3Multipart", func() any { return (*S3Multipart)(nil).DeepCopy() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.call()

			// A typed nil inside `any` is non-nil as an interface; assert on
			// the reflected value instead.
			assert.Nil(t, got, "DeepCopy of a nil receiver must return nil")
		})
	}
}

// TestGploadJobSpec_DeepCopy_FullyPopulated exercises every pointer/slice
// branch of GploadJobSpec.DeepCopyInto (InputSource, FilePaths, Header,
// MatchColumns, UpdateColumns, Preload, PostActions, ErrorHandling).
func TestGploadJobSpec_DeepCopy_FullyPopulated(t *testing.T) {
	src := &GploadJobSpec{
		TargetTable:   "public.raw_data",
		Mode:          "merge",
		Format:        "csv",
		InputSource:   &GploadInputSourceSpec{Type: "gpfdist", Host: "gpfdist-svc", Port: 8080},
		FilePaths:     []string{"/data/*.csv", "/data/extra/*.csv"},
		Delimiter:     "|",
		Header:        boolPtr(true),
		Encoding:      "UTF-8",
		MatchColumns:  []string{"id"},
		UpdateColumns: []string{"value", "updated_at"},
		Preload:       &GploadPreloadSpec{Truncate: boolPtr(true)},
		PostActions:   []string{"ANALYZE public.raw_data"},
		ErrorHandling: &ErrorHandlingSpec{
			SegmentRejectLimit:     100,
			SegmentRejectLimitType: "rows",
			LogErrors:              boolPtr(true),
		},
	}

	copied := src.DeepCopy()

	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src.InputSource, copied.InputSource)
	assert.NotSame(t, src.Header, copied.Header)
	assert.NotSame(t, src.Preload, copied.Preload)
	assert.NotSame(t, src.Preload.Truncate, copied.Preload.Truncate)
	assert.NotSame(t, src.ErrorHandling, copied.ErrorHandling)
	assert.NotSame(t, src.ErrorHandling.LogErrors, copied.ErrorHandling.LogErrors)

	// Mutating the copy must never write through to the source.
	copied.FilePaths[0] = "changed"
	copied.MatchColumns[0] = "changed"
	copied.UpdateColumns[0] = "changed"
	copied.PostActions[0] = "changed"
	*copied.Header = false
	*copied.ErrorHandling.LogErrors = false
	assert.Equal(t, "/data/*.csv", src.FilePaths[0])
	assert.Equal(t, "id", src.MatchColumns[0])
	assert.Equal(t, "value", src.UpdateColumns[0])
	assert.Equal(t, "ANALYZE public.raw_data", src.PostActions[0])
	assert.True(t, *src.Header)
	assert.True(t, *src.ErrorHandling.LogErrors)
}

// TestPxfJobSpec_DeepCopy_FullyPopulated exercises every pointer branch of
// PxfJobSpec.DeepCopyInto (FilterPushdown, ColumnProjection, Partitioning,
// ErrorHandling, Continuous) and the nested PartitioningSpec copy.
func TestPxfJobSpec_DeepCopy_FullyPopulated(t *testing.T) {
	src := &PxfJobSpec{
		Server:           "s3-server",
		Profile:          "s3:parquet",
		Resource:         "bucket/path",
		TargetTable:      "public.sales",
		Mode:             "insert",
		LoadMethod:       "external-table",
		SourceFilter:     "",
		FilterPushdown:   boolPtr(true),
		ColumnProjection: boolPtr(false),
		Partitioning: &PartitioningSpec{
			Column:   "sale_date",
			Range:    "2024-01-01:2026-12-31",
			Interval: "1:month",
		},
		ErrorHandling: &ErrorHandlingSpec{SegmentRejectLimit: 10, SegmentRejectLimitType: "percent"},
		Continuous:    boolPtr(true),
		BatchSize:     500,
		FlushInterval: "30s",
	}

	copied := src.DeepCopy()

	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src.FilterPushdown, copied.FilterPushdown)
	assert.NotSame(t, src.ColumnProjection, copied.ColumnProjection)
	assert.NotSame(t, src.Partitioning, copied.Partitioning)
	assert.NotSame(t, src.ErrorHandling, copied.ErrorHandling)
	assert.NotSame(t, src.Continuous, copied.Continuous)

	copied.Partitioning.Column = "changed"
	*copied.FilterPushdown = false
	assert.Equal(t, "sale_date", src.Partitioning.Column)
	assert.True(t, *src.FilterPushdown)

	// The standalone PartitioningSpec.DeepCopy non-nil path.
	part := src.Partitioning.DeepCopy()
	require.NotNil(t, part)
	assert.Equal(t, src.Partitioning, part)
	assert.NotSame(t, src.Partitioning, part)
}

// TestPxfExtensionsAndConnector_DeepCopy exercises the PxfExtensionsSpec
// pointer branches and the PxfCustomConnector value copy.
func TestPxfExtensionsAndConnector_DeepCopy(t *testing.T) {
	ext := &PxfExtensionsSpec{Pxf: boolPtr(true), PxfFdw: boolPtr(false)}

	extCopy := ext.DeepCopy()

	require.NotNil(t, extCopy)
	assert.Equal(t, ext, extCopy)
	assert.NotSame(t, ext.Pxf, extCopy.Pxf)
	assert.NotSame(t, ext.PxfFdw, extCopy.PxfFdw)
	*extCopy.Pxf = false
	assert.True(t, *ext.Pxf)

	conn := &PxfCustomConnector{Name: "kafka", JarURL: "https://repo/kafka-connector.jar"}

	connCopy := conn.DeepCopy()

	require.NotNil(t, connCopy)
	assert.Equal(t, conn, connCopy)
	assert.NotSame(t, conn, connCopy)
	connCopy.Name = "changed"
	assert.Equal(t, "kafka", conn.Name)
}

// TestGpbackupOptions_DeepCopy_WithStatsPointer exercises the WithStats
// pointer branch of GpbackupOptions.DeepCopyInto (nil vs set are distinct
// states — the webhook defaulting contract).
func TestGpbackupOptions_DeepCopy_WithStatsPointer(t *testing.T) {
	src := &GpbackupOptions{
		CompressionLevel: 6,
		WithStats:        boolPtr(false), // explicit false must survive the copy
	}

	copied := src.DeepCopy()

	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	require.NotNil(t, copied.WithStats)
	assert.NotSame(t, src.WithStats, copied.WithStats)
	*copied.WithStats = true
	assert.False(t, *src.WithStats, "mutating the copy must not flip the source")
}

// TestBackupSpec_DeepCopy_ValidationBranch exercises the Validation pointer
// branch of BackupSpec.DeepCopyInto.
func TestBackupSpec_DeepCopy_ValidationBranch(t *testing.T) {
	src := &BackupSpec{
		Validation: &BackupValidation{
			Enabled:          boolPtr(true),
			HealthCheckQuery: "SELECT 1",
		},
	}

	copied := src.DeepCopy()

	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src.Validation, copied.Validation)
	assert.NotSame(t, src.Validation.Enabled, copied.Validation.Enabled)
	*copied.Validation.Enabled = false
	assert.True(t, *src.Validation.Enabled)
}

// TestDataLoadingSpec_DeepCopy_AllBranches exercises every pointer branch of
// DataLoadingSpec.DeepCopyInto (Pxf, Gpfdist, Jobs, JobTemplate, HealthChecks).
func TestDataLoadingSpec_DeepCopy_AllBranches(t *testing.T) {
	replicas := int32(2)
	src := &DataLoadingSpec{
		Pxf:     &PxfSpec{Enabled: true, Image: "pxf:1", Port: 5888},
		Gpfdist: &GpfdistSpec{Enabled: true, Replicas: &replicas, Image: "gpfdist:1", Port: 8080},
		Jobs: []DataLoadingJob{
			{
				Name: "load-sales", Type: "pxf", Enabled: true,
				PxfJob: &PxfJobSpec{Server: "s3", Profile: "s3:csv", TargetTable: "t"},
			},
			{
				Name: "load-files", Type: "gpload",
				GploadJob: &GploadJobSpec{TargetTable: "t2", FilePaths: []string{"/d/*.csv"}},
			},
		},
		JobTemplate: &DataLoadingJobTemplate{
			NodeSelector: map[string]string{"disk": "ssd"},
		},
		HealthChecks: &DataLoadHealthChecksSpec{
			Enabled:          boolPtr(true),
			DiskMinFreeMB:    64,
			ScratchSizeLimit: "256Mi",
		},
	}

	copied := src.DeepCopy()

	require.NotNil(t, copied)
	assert.Equal(t, src, copied)
	assert.NotSame(t, src.Pxf, copied.Pxf)
	assert.NotSame(t, src.Gpfdist, copied.Gpfdist)
	assert.NotSame(t, src.Gpfdist.Replicas, copied.Gpfdist.Replicas)
	assert.NotSame(t, src.JobTemplate, copied.JobTemplate)
	assert.NotSame(t, src.HealthChecks, copied.HealthChecks)
	assert.NotSame(t, src.HealthChecks.Enabled, copied.HealthChecks.Enabled)
	require.Len(t, copied.Jobs, 2)
	assert.NotSame(t, src.Jobs[0].PxfJob, copied.Jobs[0].PxfJob)
	assert.NotSame(t, src.Jobs[1].GploadJob, copied.Jobs[1].GploadJob)

	copied.Jobs[0].PxfJob.Server = "changed"
	copied.HealthChecks.ScratchSizeLimit = "1Gi"
	assert.Equal(t, "s3", src.Jobs[0].PxfJob.Server)
	assert.Equal(t, "256Mi", src.HealthChecks.ScratchSizeLimit)
}
