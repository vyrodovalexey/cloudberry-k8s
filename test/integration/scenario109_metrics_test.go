//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 109: All Prometheus Metrics (M.1–M.16) — integration
// ============================================================================
//
// Mirrors the Scenario 108 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when the stack is down). The REAL metric-flow proof (induce real data-loading
// activity → assert the implemented series land in VM with the right labels +
// the absent series are honestly missing) is the e2e Part B. This integration
// layer is a thin VictoriaMetrics PromQL-SHAPE proof:
//
//   1. (always, no infra) the catalog is well-formed and the absent-metric list
//      is internally consistent.
//
//   2. (VM-reachable) GET /api/v1/query against VictoriaMetrics succeeds and the
//      response has the expected Prometheus result envelope; AND each
//      intentionally-absent metric (M.4/M.5/M.7/M.15/M.16 + synthetic M.6) has
//      ZERO samples — a PASSING honesty check that holds REGARDLESS of whether
//      any operator activity has occurred (these series must NEVER exist).
//
// HONESTY: nothing is synthesized — when VM is unreachable the suite skips
// cleanly. The absent-metric assertion is the regression lock: a future
// fabricated pxf_records_total / pxf_bytes_transferred_total / pxf_errors_total /
// pxf_active_connections / gpfdist_* would make this fail.
//
// ENV (all overridable, no hardcode-only):
//   VICTORIAMETRICS_ADDR — VM base URL (default http://127.0.0.1:8428).
//   SCENARIO109_VM_BASE  — overrides VICTORIAMETRICS_ADDR for this scenario.
// ============================================================================

const (
	envScenario109VMBase   = "SCENARIO109_VM_BASE"
	scenario109DefaultVM   = "http://127.0.0.1:8428"
	scenario109VMTimeout   = 15 * time.Second
	scenario109VMQueryPath = "/api/v1/query"
)

// Scenario109Suite drives the Scenario 109 VM PromQL-shape + honesty probe, gated
// on VictoriaMetrics reachability.
type Scenario109Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario109(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario109Suite))
}

func (s *Scenario109Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 2*time.Minute)
}

func (s *Scenario109Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario109VMBase resolves the VictoriaMetrics base URL (SCENARIO109_VM_BASE >
// VICTORIAMETRICS_ADDR > default).
func scenario109VMBase() string {
	if v := strings.TrimSpace(os.Getenv(envScenario109VMBase)); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("VICTORIAMETRICS_ADDR")); v != "" {
		return v
	}
	return scenario109DefaultVM
}

// scenario109VMResult is the minimal Prometheus query envelope.
type scenario109VMResult struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// scenario109VMQuery runs an instant PromQL query against VM. It returns the
// parsed envelope and whether the request itself succeeded (reachability).
func (s *Scenario109Suite) scenario109VMQuery(query string) (*scenario109VMResult, bool) {
	u := scenario109VMBase() + scenario109VMQueryPath + "?query=" + url.QueryEscape(query)
	ctx, cancel := context.WithTimeout(s.ctx, scenario109VMTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var parsed scenario109VMResult
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, false
	}
	return &parsed, true
}

// scenario109RequireVM skips cleanly unless VictoriaMetrics answers a trivial
// query (reachability gate).
func (s *Scenario109Suite) scenario109RequireVM() {
	if _, ok := s.scenario109VMQuery("vm_app_version"); !ok {
		s.T().Skipf("VictoriaMetrics not reachable at %s, skipping Scenario 109 VM probe "+
			"[CONFIG-ONLY: the real metric-flow proof is the e2e Part B]", scenario109VMBase())
	}
}

// TestIntegration_Scenario109_CatalogHonest asserts the Scenario 109 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario109Suite) TestIntegration_Scenario109_CatalogHonest() {
	catalog := cases.Scenario109Cases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	for _, tc := range catalog {
		assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
		seen[tc.ID] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
	}
	// The absent-metric list must be non-empty and unique (the honesty lock).
	require.NotEmpty(s.T(), cases.Scenario109AbsentMetrics)
	absentSeen := map[string]bool{}
	for _, name := range cases.Scenario109AbsentMetrics {
		assert.Falsef(s.T(), absentSeen[name], "duplicate absent metric %s", name)
		absentSeen[name] = true
	}
}

// TestIntegration_Scenario109_VMQueryShape proves the VM query endpoint answers
// with a well-formed Prometheus envelope (resultType=vector) when reachable —
// the shared-helper PromQL-shape proof the e2e Part B builds on. Skips cleanly
// when VM is unreachable.
func (s *Scenario109Suite) TestIntegration_Scenario109_VMQueryShape() {
	s.scenario109RequireVM()

	res, ok := s.scenario109VMQuery("up")
	require.True(s.T(), ok, "a reachable VM must answer the 'up' query")
	assert.Equal(s.T(), "success", res.Status, "VM query status must be success")
	assert.Equal(s.T(), "vector", res.Data.ResultType,
		"an instant query must return a vector result type")
	s.T().Logf("scenario109 VM PromQL-shape OK at %s (%d 'up' series)",
		scenario109VMBase(), len(res.Data.Result))
}

// TestIntegration_Scenario109_AbsentMetricsHaveZeroSeries covers the honesty lock
// at the integration layer (109-HONESTY-L shape): each intentionally-absent
// metric (M.4/M.5/M.7/M.15/M.16 + synthetic M.6 pxf_errors_total) MUST have ZERO
// samples in VictoriaMetrics — a NOT-present metric is a PASSING honesty check.
// This holds regardless of operator activity (these series must never exist).
// Skips cleanly when VM is unreachable.
func (s *Scenario109Suite) TestIntegration_Scenario109_AbsentMetricsHaveZeroSeries() {
	s.scenario109RequireVM()

	for _, name := range cases.Scenario109AbsentMetrics {
		name := name
		s.Run(name, func() {
			res, ok := s.scenario109VMQuery(name)
			require.Truef(s.T(), ok, "VM must answer the query for %s", name)
			assert.Emptyf(s.T(), res.Data.Result,
				"intentionally-absent metric %s must have ZERO series in VM (honesty PASS)", name)
			s.T().Logf("scenario109 honesty: %s absent in VM (PASS)", name)
		})
	}
}
