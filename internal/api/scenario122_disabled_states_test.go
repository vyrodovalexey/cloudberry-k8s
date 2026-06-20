package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
)

// ============================================================================
// Scenario 122c — C.12 usageReport disabled state + re-enablement (API layer).
//
// The usage-report endpoint is a READ-ONLY soft-gate: with usageReport disabled
// (or nil / storage nil) it returns 200 {usageReportEnabled:false, entries:[],
// total:0} (NOT a 400 — the 400 RECOMMENDATION_SCAN_NOT_ENABLED is reserved for
// the mutating POST scan), and the DB is never queried. Re-enabling returns
// usageReportEnabled:true with the collected entries. These tests assert the
// disabled→re-enable round-trip explicitly for Scenario 122; the broader P.6
// matrix lives in scenario119/scenario120.
// ============================================================================

// scenario122UsageEntries returns a small canned usage report for the re-enable
// assertion.
func scenario122UsageEntries() []db.UsageReportEntry {
	return []db.UsageReportEntry{
		{Month: "2026-06", Database: "testdb", SizeBytes: 1073741824, SizeHuman: "1 GB", Connections: 4},
	}
}

// usageReportRequest issues the P.6 GET against the given server and returns the
// decoded JSON envelope.
func usageReportRequest(t *testing.T, s interface {
	handleGetUsageReport(http.ResponseWriter, *http.Request)
}) (int, map[string]interface{}) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/usage-report?namespace=default&month=2026-06", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()
	s.handleGetUsageReport(rr, req)
	return rr.Code, decodeJSON(t, rr)
}

// 122c-C12-disabled — usageReport disabled/nil/storage-nil → 200 soft-gate with
// usageReportEnabled:false + empty entries; the DB is never opened.
func TestScenario122c_C12_Disabled(t *testing.T) {
	tests := []struct {
		name    string
		storage *cbv1alpha1.StorageManagementSpec
	}{
		{
			name:    "usageReport nil",
			storage: &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true},
		},
		{
			name: "usageReport disabled",
			storage: &cbv1alpha1.StorageManagementSpec{
				DiskMonitoring: true,
				UsageReport:    &cbv1alpha1.UsageReportSpec{Enabled: false},
			},
		},
		{
			name:    "storage nil",
			storage: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newTestCluster("test-cluster", "default")
			cluster.Spec.Storage = tt.storage
			// Usable data exists, but the disabled gate must short-circuit it.
			dbClient := &mockDBClient{usageReport: scenario122UsageEntries()}
			s := newTestServerWithDB(dbClient, cluster)

			code, resp := usageReportRequest(t, s)

			// Soft 200, NOT a 400.
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, false, resp["usageReportEnabled"],
				"disabled usage report must report usageReportEnabled:false")
			assert.Equal(t, float64(0), resp["total"])
			entries, ok := resp["entries"].([]interface{})
			require.True(t, ok)
			assert.Empty(t, entries, "disabled usage report must be empty")
			assert.Zero(t, dbClient.closeCalls, "the disabled gate must not open a DB client")
		})
	}
}

// 122c-C12-reenable — usageReport enabled → 200 with usageReportEnabled:true and
// the collected entries (reactivation).
func TestScenario122c_C12_Reenable(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		UsageReport:    &cbv1alpha1.UsageReportSpec{Enabled: true},
	}
	dbClient := &mockDBClient{usageReport: scenario122UsageEntries()}
	s := newTestServerWithDB(dbClient, cluster)

	code, resp := usageReportRequest(t, s)

	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, true, resp["usageReportEnabled"],
		"re-enabled usage report must report usageReportEnabled:true")
	assert.Equal(t, float64(1), resp["total"], "re-enabled report carries the collected entries")
	entries, ok := resp["entries"].([]interface{})
	require.True(t, ok)
	require.Len(t, entries, 1)
	first, ok := entries[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "testdb", first["database"])
}
