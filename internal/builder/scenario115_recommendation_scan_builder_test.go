package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// newScanCluster returns a test cluster with a FULL storage block: disk
// monitoring on, the recommendation scan enabled with a schedule and all five
// thresholds (spec 13 §C.1/C.3/C.5). It is the Scenario 115 builder fixture.
func newScanCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 3 * * 0",
			BloatThreshold:      20,
			SkewThreshold:       50,
			AgeThreshold:        500000000,
			IndexBloatThreshold: 30,
			ScanDuration:        "2h",
		},
		UsageReport: &cbv1alpha1.UsageReportSpec{Enabled: true, Monthly: true},
	}
	return cluster
}

// scanContainerEnv returns the env vars of the recommendation-scan container as
// a name->value map (plain-value vars only).
func scanContainerEnv(t *testing.T, cj *batchv1.CronJob) map[string]string {
	t.Helper()
	require.Len(t, cj.Spec.JobTemplate.Spec.Template.Spec.Containers, 1)
	return envMap(cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env)
}

// TestBuildRecommendationScanCronJob_Full (115-C5 builder) asserts a FULL
// enabled scan + schedule produces a correctly-shaped CronJob: name, schedule,
// ForbidConcurrent, both history limits == 3, the cluster owner reference, the
// component label, and the env vars carrying the five thresholds + scan
// duration.
func TestBuildRecommendationScanCronJob_Full(t *testing.T) {
	b := NewBuilder()
	cluster := newScanCluster()

	cj := b.BuildRecommendationScanCronJob(cluster)
	require.NotNil(t, cj, "full enabled scan + schedule must produce a CronJob")

	// Name + namespace.
	assert.Equal(t, util.RecommendationScanCronJobName("test-cluster"), cj.Name)
	assert.Equal(t, "test-cluster-recommendation-scan", cj.Name)
	assert.Equal(t, "default", cj.Namespace)

	// Schedule + concurrency + history limits.
	assert.Equal(t, "0 3 * * 0", cj.Spec.Schedule)
	assert.Equal(t, batchv1.ForbidConcurrent, cj.Spec.ConcurrencyPolicy)
	require.NotNil(t, cj.Spec.SuccessfulJobsHistoryLimit)
	require.NotNil(t, cj.Spec.FailedJobsHistoryLimit)
	assert.Equal(t, int32(3), *cj.Spec.SuccessfulJobsHistoryLimit)
	assert.Equal(t, int32(3), *cj.Spec.FailedJobsHistoryLimit)

	// Owner reference points at the cluster (controller + block-owner-deletion).
	require.Len(t, cj.OwnerReferences, 1)
	owner := cj.OwnerReferences[0]
	assert.Equal(t, cluster.Name, owner.Name)
	assert.Equal(t, "CloudberryCluster", owner.Kind)
	require.NotNil(t, owner.Controller)
	assert.True(t, *owner.Controller)
	require.NotNil(t, owner.BlockOwnerDeletion)
	assert.True(t, *owner.BlockOwnerDeletion)

	// Component label on the CronJob and the pod template.
	assert.Equal(t, util.ComponentRecommendationScan, cj.Labels[util.LabelComponent])
	assert.Equal(t, "test-cluster", cj.Labels[util.LabelCluster])
	podLabels := cj.Spec.JobTemplate.Spec.Template.Labels
	assert.Equal(t, util.ComponentRecommendationScan, podLabels[util.LabelComponent])

	// Env vars carry the thresholds + scan duration.
	env := scanContainerEnv(t, cj)
	assert.Equal(t, "20", env["SCAN_BLOAT_THRESHOLD"])
	assert.Equal(t, "50", env["SCAN_SKEW_THRESHOLD"])
	assert.Equal(t, "500000000", env["SCAN_AGE_THRESHOLD"])
	assert.Equal(t, "30", env["SCAN_INDEX_BLOAT_THRESHOLD"])
	assert.Equal(t, "2h", env["SCAN_DURATION"])
}

// TestBuildRecommendationScanCronJob_NilCases pins the nil-means-delete gate:
// the builder returns nil for every partial/disabled spec so the ensure helper
// GCs any stale CronJob (spec 13 §C.5).
func TestBuildRecommendationScanCronJob_NilCases(t *testing.T) {
	b := NewBuilder()

	tests := []struct {
		name   string
		mutate func(*cbv1alpha1.CloudberryCluster)
	}{
		{
			name:   "storage nil",
			mutate: func(c *cbv1alpha1.CloudberryCluster) { c.Spec.Storage = nil },
		},
		{
			name: "disk monitoring false",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Storage.DiskMonitoring = false
			},
		},
		{
			name: "recommendation scan nil",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Storage.RecommendationScan = nil
			},
		},
		{
			name: "scan disabled",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Storage.RecommendationScan.Enabled = false
			},
		},
		{
			name: "empty schedule",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Storage.RecommendationScan.Schedule = ""
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newScanCluster()
			tc.mutate(cluster)
			assert.Nil(t, b.BuildRecommendationScanCronJob(cluster))
		})
	}
}
