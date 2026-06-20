package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// ============================================================================
// Scenario 114: Mutating Webhook Defaults (storage-recommendations D.1-D.6)
// ============================================================================
//
// The mutating webhook (setStorageManagementDefaults) applies six defaults
// D.1-D.6 when spec.storage.recommendationScan is present and enabled and the
// corresponding field is unset/zero. Explicit user-supplied values are always
// preserved.
//
// Unlike the in-package TestSetStorageManagementDefaults (which calls the
// unexported setStorageManagementDefaults directly), this suite drives the
// PUBLIC entrypoint CloudberryClusterDefaulter.Default(ctx, cluster)
// end-to-end so it exercises Default -> setClusterDefaults ->
// setStorageManagementDefaults. The six rule IDs map to recommendation-scan
// fields as follows:
//   - D.1 schedule            -> "0 3 * * 0"
//   - D.2 bloatThreshold      -> 20
//   - D.3 skewThreshold       -> 50
//   - D.4 ageThreshold        -> int64(500000000)
//   - D.5 indexBloatThreshold -> 30
//   - D.6 scanDuration        -> "2h"
// ============================================================================

// scenario114EnabledScanCluster returns a minimal valid cluster whose
// recommendation scan is enabled with ALL six defaulted fields omitted, so the
// public Default() entrypoint populates every D.1-D.6 default.
func scenario114EnabledScanCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newMinimalCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true,
		},
	}
	return cluster
}

// TestScenario114_AllOmitted_DefaultsApplied — 114-ALL-omitted: an enabled scan
// with all six fields omitted gets ALL six defaults after the public Default().
func TestScenario114_AllOmitted_DefaultsApplied(t *testing.T) {
	d := NewCloudberryClusterDefaulter()
	cluster := scenario114EnabledScanCluster()

	require.NoError(t, d.Default(context.Background(), cluster))

	require.NotNil(t, cluster.Spec.Storage)
	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(t, scan)
	assert.Equal(t, "0 3 * * 0", scan.Schedule, "D.1 schedule")
	assert.Equal(t, int32(20), scan.BloatThreshold, "D.2 bloatThreshold")
	assert.Equal(t, int32(50), scan.SkewThreshold, "D.3 skewThreshold")
	assert.Equal(t, int64(500000000), scan.AgeThreshold, "D.4 ageThreshold")
	assert.Equal(t, int32(30), scan.IndexBloatThreshold, "D.5 indexBloatThreshold")
	assert.Equal(t, "2h", scan.ScanDuration, "D.6 scanDuration")
}

// TestScenario114_PerFieldDefaults — 114-D1..114-D6: per-D granularity. For each
// field, start from enabled+all-omitted, run the public Default(), and assert
// that single field equals its default value.
func TestScenario114_PerFieldDefaults(t *testing.T) {
	tests := []struct {
		id     string
		assert func(t *testing.T, scan *cbv1alpha1.RecommendationScanSpec)
	}{
		{
			id: "114-D1",
			assert: func(t *testing.T, scan *cbv1alpha1.RecommendationScanSpec) {
				assert.Equal(t, "0 3 * * 0", scan.Schedule, "D.1 schedule")
			},
		},
		{
			id: "114-D2",
			assert: func(t *testing.T, scan *cbv1alpha1.RecommendationScanSpec) {
				assert.Equal(t, int32(20), scan.BloatThreshold, "D.2 bloatThreshold")
			},
		},
		{
			id: "114-D3",
			assert: func(t *testing.T, scan *cbv1alpha1.RecommendationScanSpec) {
				assert.Equal(t, int32(50), scan.SkewThreshold, "D.3 skewThreshold")
			},
		},
		{
			id: "114-D4",
			assert: func(t *testing.T, scan *cbv1alpha1.RecommendationScanSpec) {
				assert.Equal(t, int64(500000000), scan.AgeThreshold, "D.4 ageThreshold")
			},
		},
		{
			id: "114-D5",
			assert: func(t *testing.T, scan *cbv1alpha1.RecommendationScanSpec) {
				assert.Equal(t, int32(30), scan.IndexBloatThreshold, "D.5 indexBloatThreshold")
			},
		},
		{
			id: "114-D6",
			assert: func(t *testing.T, scan *cbv1alpha1.RecommendationScanSpec) {
				assert.Equal(t, "2h", scan.ScanDuration, "D.6 scanDuration")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			d := NewCloudberryClusterDefaulter()
			cluster := scenario114EnabledScanCluster()

			require.NoError(t, d.Default(context.Background(), cluster))

			require.NotNil(t, cluster.Spec.Storage)
			scan := cluster.Spec.Storage.RecommendationScan
			require.NotNil(t, scan)
			tc.assert(t, scan)
		})
	}
}

// TestScenario114_ExplicitValuesPreserved — 114-PRESERVE: an enabled scan with
// explicit NON-default values is not overwritten by the public Default().
func TestScenario114_ExplicitValuesPreserved(t *testing.T) {
	d := NewCloudberryClusterDefaulter()
	cluster := newMinimalCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 1 * * *",
			BloatThreshold:      10,
			SkewThreshold:       25,
			AgeThreshold:        100000000,
			IndexBloatThreshold: 15,
			ScanDuration:        "1h",
		},
	}

	require.NoError(t, d.Default(context.Background(), cluster))

	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(t, scan)
	assert.Equal(t, "0 1 * * *", scan.Schedule, "D.1 explicit schedule preserved")
	assert.Equal(t, int32(10), scan.BloatThreshold, "D.2 explicit bloatThreshold preserved")
	assert.Equal(t, int32(25), scan.SkewThreshold, "D.3 explicit skewThreshold preserved")
	assert.Equal(t, int64(100000000), scan.AgeThreshold, "D.4 explicit ageThreshold preserved")
	assert.Equal(t, int32(15), scan.IndexBloatThreshold, "D.5 explicit indexBloatThreshold preserved")
	assert.Equal(t, "1h", scan.ScanDuration, "D.6 explicit scanDuration preserved")
}

// TestScenario114_DisabledNoOp — 114-DISABLED-noop: a disabled scan with omitted
// fields gets NONE of the defaults applied after the public Default().
func TestScenario114_DisabledNoOp(t *testing.T) {
	d := NewCloudberryClusterDefaulter()
	cluster := newMinimalCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: false,
		},
	}

	require.NoError(t, d.Default(context.Background(), cluster))

	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(t, scan)
	assert.Equal(t, "", scan.Schedule, "D.1 not applied when disabled")
	assert.Equal(t, int32(0), scan.BloatThreshold, "D.2 not applied when disabled")
	assert.Equal(t, int32(0), scan.SkewThreshold, "D.3 not applied when disabled")
	assert.Equal(t, int64(0), scan.AgeThreshold, "D.4 not applied when disabled")
	assert.Equal(t, int32(0), scan.IndexBloatThreshold, "D.5 not applied when disabled")
	assert.Equal(t, "", scan.ScanDuration, "D.6 not applied when disabled")
}

// TestScenario114_ControlNilStorage — 114-CONTROL: a cluster with nil storage is
// left with nil storage by the public Default() (no panic, no allocation).
func TestScenario114_ControlNilStorage(t *testing.T) {
	d := NewCloudberryClusterDefaulter()
	cluster := newMinimalCluster()
	require.Nil(t, cluster.Spec.Storage)

	require.NotPanics(t, func() {
		require.NoError(t, d.Default(context.Background(), cluster))
	})

	assert.Nil(t, cluster.Spec.Storage, "nil storage stays nil")
}

// TestScenario114_RecordsAllowedAdmission asserts a public Default() pass records
// a mutating-webhook admission with result "allowed" (defaulting never denies).
func TestScenario114_RecordsAllowedAdmission(t *testing.T) {
	rec := newCapturingRecorder()
	d := NewCloudberryClusterDefaulter(rec)
	cluster := scenario114EnabledScanCluster()

	require.NoError(t, d.Default(context.Background(), cluster))

	assert.Equal(t, 1, rec.admissions)
	assert.Equal(t, webhookMutating, rec.lastWebhook)
	assert.Equal(t, admissionOpCreate, rec.lastOperation)
	assert.Equal(t, admissionAllowed, rec.lastResult)

	// Defaults still landed on the same pass that recorded the metric.
	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(t, scan)
	assert.Equal(t, "0 3 * * 0", scan.Schedule, "D.1 schedule on allowed pass")
}

// TestScenario114_UpdateOperationRecordsAllowed drives the public Default() with
// an UPDATE admission request in context and asserts the admission is recorded
// with the update operation label and result "allowed", while D.1-D.6 still land.
func TestScenario114_UpdateOperationRecordsAllowed(t *testing.T) {
	rec := newCapturingRecorder()
	d := NewCloudberryClusterDefaulter(rec)
	cluster := scenario114EnabledScanCluster()

	ctx := admissionCtx(admissionv1.Update)
	require.NoError(t, d.Default(ctx, cluster))

	assert.Equal(t, 1, rec.admissions)
	assert.Equal(t, webhookMutating, rec.lastWebhook)
	assert.Equal(t, admissionOpUpdate, rec.lastOperation, "update op recorded for an update admission")
	assert.Equal(t, admissionAllowed, rec.lastResult)

	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(t, scan)
	assert.Equal(t, "0 3 * * 0", scan.Schedule, "D.1 schedule on update pass")
	assert.Equal(t, int32(20), scan.BloatThreshold, "D.2 bloatThreshold on update pass")
	assert.Equal(t, "2h", scan.ScanDuration, "D.6 scanDuration on update pass")
}
