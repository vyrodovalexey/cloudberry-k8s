package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// fdwBaseline returns a cluster whose baseline s3-ingest job (dl.Jobs[0],
// s3-datalake / s3:parquet read) is mutated by fn for the Scenario 103 W.25 /
// W.17 cases. The baseline has no schedule conflict for these jobs because we
// clear the schedule on the s3-ingest job before mutation (loadMethod=fdw +
// continuous is the only schedule-sensitive case, handled per-case).
func fdwBaseline(fn func(j *cbv1alpha1.PxfJobSpec)) *cbv1alpha1.CloudberryCluster {
	return clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
		// dl.Jobs[0] is the s3-ingest pxf read job; clear its schedule so a
		// continuous=true mutation does not trip the W.23c schedule rule before
		// reaching W.25 (we want W.25 to be the rejecting rule).
		dl.Jobs[0].Schedule = ""
		if fn != nil {
			fn(dl.Jobs[0].PxfJob)
		}
	})
}

// TestValidateDataLoading_Scenario103 exercises the Scenario 103 webhook rules:
//   - W.25 (validateLoadMethod): loadMethod enum + loadMethod=fdw is a READ path
//     only (reject fdw+writable, reject fdw+continuous; admit fdw read) [U2]
//   - W.17 (validateSourceFilter tweak): sourceFilter valid on a fdw read OR a
//     writable export, still rejected on a plain external-table read [U2]
//
// All cases run through the public validateDataLoading path (mirroring the
// existing W.* / Scenario 102 webhook table tests).
func TestValidateDataLoading_Scenario103(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		// ---- W.25 enum -------------------------------------------------------
		{
			// SC103-W25-ENUM: an unrecognized loadMethod is DENIED.
			name: "W.25 loadMethod bogus rejected (enum)",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = "bogus"
			}),
			expectErr:   true,
			errContains: "must be external-table or fdw",
		},
		{
			// loadMethod explicitly external-table is the default path => ADMIT.
			name: "W.25 loadMethod external-table accepted",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = "external-table"
			}),
			expectErr: false,
		},
		{
			// loadMethod unset (empty) => ADMIT (the default path).
			name: "W.25 loadMethod unset accepted",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = ""
			}),
			expectErr: false,
		},
		// ---- W.25 fdw read-only constraints ----------------------------------
		{
			// SC103-W25-FDW-OK: loadMethod=fdw on a normal read profile (s3:parquet,
			// no mode) is ADMITTED.
			name: "W.25 loadMethod fdw on read profile accepted",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = "fdw"
			}),
			expectErr: false,
		},
		{
			// SC103-W25-FDW-WRITABLE: loadMethod=fdw + mode=writable is DENIED
			// (a writable FDW export is out of scope). Use a writable format so
			// the W.10b writable-format guard does not trip first.
			name: "W.25 loadMethod fdw + mode writable rejected",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = "fdw"
				j.Mode = "writable"
				j.Profile = "s3:text" // writable format so W.10b passes; W.25 must reject
			}),
			expectErr:   true,
			errContains: "not valid with mode=writable",
		},
		{
			// SC103-W25 (continuous): loadMethod=fdw + continuous=true is DENIED
			// (fdw is a one-off persistent load, not a streaming consume loop).
			name: "W.25 loadMethod fdw + continuous rejected",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = "fdw"
				j.Continuous = boolPtr(true)
			}),
			expectErr:   true,
			errContains: "continuous",
		},
		{
			// loadMethod=fdw + continuous=false is fine (the *bool is explicitly
			// false, not the one-off-violating true).
			name: "W.25 loadMethod fdw + continuous false accepted",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = "fdw"
				j.Continuous = boolPtr(false)
			}),
			expectErr: false,
		},

		// ---- W.17 tweak (sourceFilter on fdw read) ---------------------------
		{
			// SC103-W17-FDW-FILTER: sourceFilter on a fdw read job is ADMITTED
			// (the extended W.17 allows the predicate on the fdw read INSERT).
			name: "W.17 sourceFilter on fdw read accepted",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = "fdw"
				j.SourceFilter = "region='us-east'"
			}),
			expectErr: false,
		},
		{
			// SC103-W17-EXT-READ-DENY: sourceFilter on a PLAIN external-table read
			// (loadMethod unset, not writable) is DENIED.
			name: "W.17 sourceFilter on plain external-table read rejected",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.SourceFilter = "region='us-east'"
			}),
			expectErr:   true,
			errContains: "fdw read job",
		},
		{
			// The same denial names the writable-export allowance in the message.
			name: "W.17 sourceFilter on plain external-table read names writable export",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.SourceFilter = "region='us-east'"
			}),
			expectErr:   true,
			errContains: "writable export job",
		},
		{
			// W.17 (existing) — sourceFilter on a writable export is STILL ADMITTED
			// (loadMethod unset, mode=writable, writable format).
			name: "W.17 sourceFilter on writable export still accepted",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.Mode = "writable"
				j.Profile = "s3:text"
				j.SourceFilter = "region='us-east'"
			}),
			expectErr: false,
		},
		{
			// W.17(b) sanity — a stacked-query / comment in the sourceFilter is
			// rejected even on the (allowed) fdw read path.
			name: "W.17 sourceFilter with stacked query on fdw read rejected",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = "fdw"
				j.SourceFilter = "region='us-east'; DROP TABLE events"
			}),
			expectErr:   true,
			errContains: "statement terminators or SQL comments",
		},
		{
			// W.17 — no sourceFilter is always fine on a fdw read.
			name: "W.17 fdw read without sourceFilter accepted",
			cluster: fdwBaseline(func(j *cbv1alpha1.PxfJobSpec) {
				j.LoadMethod = "fdw"
				j.SourceFilter = ""
			}),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDataLoading(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateLoadMethod_Direct exercises the validateLoadMethod helper directly
// (W.25) across the enum + fdw read-only constraints, independent of the full
// validateDataLoading wiring.
func TestValidateLoadMethod_Direct(t *testing.T) {
	tests := []struct {
		name        string
		pxf         cbv1alpha1.PxfJobSpec
		expectErr   bool
		errContains string
	}{
		{"unset accepted", cbv1alpha1.PxfJobSpec{}, false, ""},
		{"external-table accepted", cbv1alpha1.PxfJobSpec{LoadMethod: "external-table"}, false, ""},
		{"fdw read accepted", cbv1alpha1.PxfJobSpec{LoadMethod: "fdw"}, false, ""},
		{
			"bogus rejected",
			cbv1alpha1.PxfJobSpec{LoadMethod: "bogus"},
			true, "must be external-table or fdw",
		},
		{
			"fdw + writable rejected",
			cbv1alpha1.PxfJobSpec{LoadMethod: "fdw", Mode: "writable"},
			true, "not valid with mode=writable",
		},
		{
			"fdw + continuous rejected",
			cbv1alpha1.PxfJobSpec{LoadMethod: "fdw", Continuous: boolPtr(true)},
			true, "continuous",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateLoadMethod(&tc.pxf, 0)
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateSourceFilter_Direct exercises the W.17 validateSourceFilter helper
// directly (the tweaked mode/method gate + the sanity scan).
func TestValidateSourceFilter_Direct(t *testing.T) {
	tests := []struct {
		name        string
		pxf         cbv1alpha1.PxfJobSpec
		expectErr   bool
		errContains string
	}{
		{"empty filter accepted", cbv1alpha1.PxfJobSpec{}, false, ""},
		{
			"fdw read with filter accepted",
			cbv1alpha1.PxfJobSpec{LoadMethod: "fdw", SourceFilter: "region='us-east'"},
			false, "",
		},
		{
			"writable export with filter accepted",
			cbv1alpha1.PxfJobSpec{Mode: "writable", SourceFilter: "region='us-east'"},
			false, "",
		},
		{
			"plain external-table read with filter rejected",
			cbv1alpha1.PxfJobSpec{SourceFilter: "region='us-east'"},
			true, "fdw read job",
		},
		{
			"fdw read with stacked query rejected",
			cbv1alpha1.PxfJobSpec{LoadMethod: "fdw", SourceFilter: "x=1; DROP TABLE t"},
			true, "statement terminators or SQL comments",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateSourceFilter(&tc.pxf, 0)
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
