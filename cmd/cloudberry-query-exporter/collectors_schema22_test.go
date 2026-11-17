package main

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollectorSQL_MatchesCloudberry22GpToolkitSchema is the DEF-3 regression
// test (TASK-U027): every gp_toolkit query must target the Cloudberry 2.2.0
// schema (live-verified on the acceptance cluster, ns cloudberry-test) and must
// not reference the pre-2.2.0 column names that made the collectors fail with
// SQLSTATE 42703 (U015: table_skew / resgroup_status / spill_files down).
//
// 2.2.0 ground truth (pg_attribute on the live cluster):
//
//	gp_skew_coefficients          skcoid, skcnamespace, skcrelname, skccoeff
//	gp_resgroup_status            groupid, groupname, num_running, num_queueing,
//	                              num_queued, num_executed, total_queue_duration
//	gp_resgroup_status_per_host   groupid, groupname, hostname, cpu_usage,
//	                              memory_usage
//	gp_resgroup_iostats_per_host  rsgname, hostname, tablespace, rbps, wbps,
//	                              riops, wiops
//	gp_workfile_usage_per_segment segid, size, numfiles  (NO hostname column)
func TestCollectorSQL_MatchesCloudberry22GpToolkitSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sql      string
		contains []string
		forbids  []string
	}{
		{
			name: "resgroup status summary",
			sql:  resgroupStatusSQL,
			contains: []string{
				"groupname AS rsgname",
				"gp_toolkit.gp_resgroup_status",
			},
			forbids: []string{"SELECT rsgname"}, // pre-2.2.0 column name
		},
		{
			name: "resgroup status per host",
			sql:  resgroupStatusPerHostSQL,
			contains: []string{
				"groupname AS rsgname",
				"cpu_usage AS cpu",
				"memory_usage AS memory_used",
			},
			// pre-2.2.0 columns dropped in 2.2.0 — selecting them is the defect.
			forbids: []string{
				"SELECT rsgname",
				"memory_available",
				"memory_quota_used",
				"memory_shared_used",
			},
		},
		{
			name: "resgroup iostats per host",
			sql:  resgroupIOStatsSQL,
			contains: []string{
				"rsgname", "hostname", "tablespace",
				"rbps", "wbps", "riops", "wiops",
				"gp_toolkit.gp_resgroup_iostats_per_host",
			},
			forbids: []string{},
		},
		{
			name: "spill files per segment",
			sql:  spillFilePerSegmentSQL,
			contains: []string{
				"gp_workfile_usage_per_segment",
				// 2.2.0 dropped the view's hostname column: it must be resolved
				// via gp_segment_configuration (content -> primary hostname).
				"JOIN gp_segment_configuration g ON g.content = w.segid AND g.role = 'p'",
				"g.hostname",
			},
			forbids: []string{"GROUP BY segid, hostname"}, // old hostless-view shape
		},
		{
			name: "spill files summary",
			sql:  spillFileSummarySQL,
			contains: []string{
				"gp_workfile_usage_per_query",
			},
			forbids: []string{},
		},
		{
			name: "table skew",
			sql:  tableSkewSQL,
			contains: []string{
				"skcnamespace as schemaname",
				"skcrelname as tablename",
				"skccoeff",
				"gp_toolkit.gp_skew_coefficients",
			},
			forbids: []string{
				"psdname",
				"psdrelname",
				"psdskewcoefficient",
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, want := range tc.contains {
				assert.Contains(t, tc.sql, want)
			}
			for _, ban := range tc.forbids {
				assert.NotContains(t, tc.sql, ban,
					"pre-2.2.0 gp_toolkit column drift is the DEF-3 defect")
			}
		})
	}
}

// TestCollectTableSkew_22SchemaRows drives collectTableSkew with a 2.2.0-shaped
// result set (skcnamespace/skcrelname/skccoeff via the float8 cast alias) and
// asserts the emitted series — and that the query actually sent carries the
// skc* column names.
func TestCollectTableSkew_22SchemaRows(t *testing.T) {
	var gotQuery string
	conn, cleanup := newMockConn(t, func(query string) []byte {
		gotQuery = query
		return rowsResponseTyped([]fieldDesc{
			textField("schemaname"), textField("tablename"), float8Field("skew_coefficient"),
		}, [][]string{
			{"public", "cloudberry_query_history", "10.347"},
			{"public", "pxf_loaded", "2.262"},
		})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	err := mc.collectTableSkew(context.Background(), conn, testLogger())
	require.NoError(t, err)

	for _, name := range []string{"skcnamespace", "skcrelname", "skccoeff"} {
		assert.Contains(t, gotQuery, name)
	}
	assert.NotContains(t, gotQuery, "psd")
	assert.InDelta(t, 10.347,
		testGaugeVec(t, mc.tableSkewCoefficient, "public", "cloudberry_query_history"), 0.001)
	assert.InDelta(t, 2.262,
		testGaugeVec(t, mc.tableSkewCoefficient, "public", "pxf_loaded"), 0.001)
}

// TestCollectResgroupStatus_22SchemaRows drives both halves of the resgroup
// status collector with 2.2.0-shaped result sets (groupname/cpu_usage/
// memory_usage) and asserts the running/cpu/memory series publish while the
// dropped memory families (available/quota/shared) stay empty.
func TestCollectResgroupStatus_22SchemaRows(t *testing.T) {
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "per_host") {
			return rowsResponseTyped([]fieldDesc{
				textField("rsgname"), textField("hostname"),
				float8Field("cpu"), float8Field("memory_used"),
			}, [][]string{{"admin_group", "seg-0.host", "12.5", "204800"}})
		}
		return rowsResponseTyped([]fieldDesc{
			textField("rsgname"), int8Field("num_running"), int8Field("num_queueing"),
			int8Field("num_executed"), textField("total_queue_duration"),
		}, [][]string{{"admin_group", "3", "1", "42", "00:00:10.000000"}})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	err := mc.collectResgroupStatus(context.Background(), conn, testLogger())
	require.NoError(t, err)

	assert.Equal(t, float64(3), testGaugeVec(t, mc.resgroupRunningQueries, "admin_group"))
	assert.Equal(t, float64(1), testGaugeVec(t, mc.resgroupQueuedQueries, "admin_group"))
	assert.InDelta(t, 12.5,
		testGaugeVec(t, mc.resgroupCPUUsagePercent, "admin_group", "seg-0.host"), 0.001)
	assert.InDelta(t, float64(204800),
		testGaugeVec(t, mc.resgroupMemoryUsageBytes, "admin_group", "seg-0.host"), 0.001)
	assert.Zero(t, testutil.CollectAndCount(mc.resgroupMemoryAvailableBytes))
}

// TestCollectSpillFilePerSegment_22SchemaJoin drives the per-segment spill
// collector with the joined 2.2.0 shape (segment_id via gp_segment_configuration
// hostname) and asserts the per-segment bytes series publish with both labels.
func TestCollectSpillFilePerSegment_22SchemaJoin(t *testing.T) {
	var gotQuery string
	conn, cleanup := newMockConn(t, func(query string) []byte {
		gotQuery = query
		return rowsResponseTyped([]fieldDesc{
			textField("segment_id"), textField("hostname"), int8Field("bytes"),
		}, [][]string{
			{"-1", "coord.host", "4096"},
			{"0", "seg-0.host", "8192"},
		})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	err := mc.collectSpillFilePerSegment(context.Background(), conn, testLogger())
	require.NoError(t, err)

	assert.Contains(t, gotQuery, "JOIN gp_segment_configuration")
	assert.NotContains(t, gotQuery, "GROUP BY segid, hostname")
	assert.Equal(t, float64(8192), testGaugeVec(t, mc.spillFilesPerSegment, "0", "seg-0.host"))
	assert.Equal(t, float64(4096), testGaugeVec(t, mc.spillFilesPerSegment, "-1", "coord.host"))
}
