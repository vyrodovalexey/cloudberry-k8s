package cases

// ============================================================================
// Scenario 116 — Disk Usage Monitoring (Status + Metric)
// (reconciliation rules R.2, S.1, M.1 for spec.storage.diskMonitoring)
// ============================================================================
//
// Acceptance scenario: with spec.storage.diskMonitoring:true, every reconcile
// (and the steady-state refresh) MEASURES the worst-case segment-volume
// filesystem usage via db.GetDiskUsagePercent (gp_toolkit.gp_disk_free) and:
//   - R.2  reconcileStorage()/refreshStorageOnSteadyState() call recordDiskUsage
//          which measures usage and updates the metric each reconcile (the
//          measurement step — NOT a republish of the stale status field).
//   - S.1  status.diskUsagePercent is populated with the CURRENT measured value
//          and tracks growth as data grows.
//   - M.1  cloudberry_disk_usage_percent{cluster,namespace} is set FROM the same
//          measured value so the gauge MATCHES the status (M.1 invariant).
//   - When gp_disk_free is unavailable the DB returns db.ErrDiskUsageUnavailable;
//          the controller SKIPS honestly (no fabricated value, status untouched).
//   - The live cross-check compares the metric against actual `df` filesystem
//          usage on a segment data volume.
//
// Layers (reusing the shared layer vocabulary):
//   - U : unit       (internal/db + internal/controller direct; no build tag).
//   - F : functional (drive Reconcile/reconcileStorage over a fake-client
//     TestK8sEnv with an injected dbFactory; //go:build functional).
//   - L : live e2e   (`kubectl apply` diskMonitoring:true → scrape /metrics +
//     GET status → cross-check `df`; //go:build e2e, KUBECONFIG + SCENARIO116_LIVE
//     gated; Part A always runs).
// ============================================================================

// Scenario116Gate enumerates the disk-monitoring-gate state a Scenario 116 case
// exercises. The measurement runs only on the monitoring gate; the disabled gate
// is the early-return no-op.
const (
	// Scenario116GateMonitoring means diskMonitoring:true: reconcileStorage and
	// refreshStorageOnSteadyState reach recordDiskUsage and measure.
	Scenario116GateMonitoring = "monitoring"
	// Scenario116GateDisabled means diskMonitoring:false (or storage nil):
	// no measurement, no status/metric update.
	Scenario116GateDisabled = "disabled"
	// Scenario116GateNone is used for the CONTROL / aggregate rows.
	Scenario116GateNone = "n/a"
)

// Scenario116Layer enumerates the assertion layer of a Scenario 116 case,
// reusing the shared layer vocabulary.
const (
	// Scenario116LayerUnit is the db/controller-direct unit layer.
	Scenario116LayerUnit = Scenario104LayerBuilder
	// Scenario116LayerFunctional is the Reconcile/reconcileStorage functional layer.
	Scenario116LayerFunctional = Scenario104LayerReconcile
	// Scenario116LayerLive is the live `kubectl apply` + scrape contract layer.
	Scenario116LayerLive = Scenario104LayerLive
)

// Scenario 116 well-known live values (mirror the e2e Part B env defaults).
const (
	// Scenario116Namespace is the default deploy namespace for the live (-L) rows.
	Scenario116Namespace = "cloudberry-test"
	// Scenario116DefaultCluster is the default (SHORT) live cluster name base.
	Scenario116DefaultCluster = "s116"
	// Scenario116MetricName is the Prometheus gauge the metric rows assert.
	Scenario116MetricName = "cloudberry_disk_usage_percent"
	// Scenario116CrossCheckTolerance is the allowed absolute drift (pct points)
	// between the gauge and the `df`-derived worst-case usage in CROSSCHECK-L.
	Scenario116CrossCheckTolerance = 5
)

// Scenario116Case describes one Scenario 116 sub-case. It is a flat catalog row
// identical in shape to Scenario115Case: the rule family, the assertion Layer,
// the monitoring Gate, a short Expected outcome token, and a Description.
type Scenario116Case struct {
	// ID is the catalog rule id (e.g. "116-R2-U", "116-M1-F", "116-CROSSCHECK-L").
	ID string
	// Req is the rule family the row proves: "R.2", "S.1", "M.1", "TRACK",
	// "CONTROL", "DISABLED", "DBERR", "CROSSCHECK", "PERSIST".
	Req string
	// Layer is the assertion layer: Scenario116LayerUnit / Functional / Live.
	Layer string
	// Field is the dotted spec/status path the row asserts (empty for aggregate rows).
	Field string
	// ExpectedValue is the value asserted at Field, rendered as a string (empty
	// for the aggregate rows).
	ExpectedValue string
	// Gate is the monitoring gate the row exercises: monitoring / disabled / n/a.
	Gate string
	// Expected is a short human outcome token.
	Expected string
	// Description explains the case + the gate rationale.
	Description string
}

// Scenario116Cases returns the full Scenario 116 catalog: the per-rule -U/-F/-L
// rows for R.2/S.1/M.1, plus the TRACK-growth (-U/-F), DISABLED-noop,
// DBERR-nonfatal, CROSSCHECK-L, CONTROL, and PERSIST-L cross-cutting rows. The
// -U rows are owned in internal/db + internal/controller; the -F rows resolve at
// the functional Reconcile layer; the -L rows require a live cluster and are
// resolved (or SKIP cleanly) in the integration/e2e live parts.
func Scenario116Cases() []Scenario116Case {
	cases := []Scenario116Case{}
	cases = append(cases, scenario116PerRuleCases()...)
	cases = append(cases, scenario116CrossCuttingCases()...)
	return cases
}

// scenario116Rule carries the per-rule metadata shared across the U/F/L layers.
type scenario116Rule struct {
	req      string
	idStem   string
	expected string
	desc     string
}

// scenario116Rules returns the canonical R.2/S.1/M.1 rule table. The per-layer
// rows are generated from it so the U/F/L catalogs cannot drift.
func scenario116Rules() []scenario116Rule {
	return []scenario116Rule{
		{
			req: "R.2", idStem: "R2",
			expected: "usage measured",
			desc: "reconcileStorage/refreshStorageOnSteadyState measure disk usage via " +
				"db.GetDiskUsagePercent (gp_toolkit.gp_disk_free) and update the metric each reconcile",
		},
		{
			req: "S.1", idStem: "S1",
			expected: "status==measured",
			desc: "status.diskUsagePercent is populated with the CURRENT measured value " +
				"(not the stale field) and tracks growth",
		},
		{
			req: "M.1", idStem: "M1",
			expected: "metric==status",
			desc: "cloudberry_disk_usage_percent{cluster,namespace} is set FROM the measured " +
				"value so the gauge matches status.diskUsagePercent (M.1 invariant)",
		},
	}
}

// scenario116PerRuleCases returns the per-rule -U/-F/-L rows. For every rule
// R.2/S.1/M.1 there is a unit (-U), functional (-F), and live (-L) row, each
// gated on disk monitoring.
func scenario116PerRuleCases() []Scenario116Case {
	rules := scenario116Rules()
	out := make([]Scenario116Case, 0, len(rules)*3)
	for _, r := range rules {
		out = append(out,
			Scenario116Case{
				ID: "116-" + r.idStem + "-U", Req: r.req, Layer: Scenario116LayerUnit,
				Gate: Scenario116GateMonitoring, Expected: r.expected,
				Description: "[UNIT] " + r.desc + " (db/controller-direct over a stub DB client).",
			},
			Scenario116Case{
				ID: "116-" + r.idStem + "-F", Req: r.req, Layer: Scenario116LayerFunctional,
				Gate: Scenario116GateMonitoring, Expected: r.expected,
				Description: "[FUNCTIONAL] " + r.desc + " (drive Reconcile/reconcileStorage over a TestK8sEnv).",
			},
			Scenario116Case{
				ID: "116-" + r.idStem + "-L", Req: r.req, Layer: Scenario116LayerLive,
				Gate: Scenario116GateMonitoring, Expected: "live " + r.expected,
				Description: "[LIVE-ONLY] kubectl apply diskMonitoring:true → " + r.desc + ".",
			},
		)
	}
	return out
}

// scenario116CrossCuttingCases returns the TRACK-growth (-U/-F), DISABLED-noop,
// DBERR-nonfatal, CROSSCHECK-L, CONTROL, and PERSIST-L rows.
func scenario116CrossCuttingCases() []Scenario116Case {
	return []Scenario116Case{
		{
			ID: "116-TRACK-growth-U", Req: "TRACK", Layer: Scenario116LayerUnit,
			Gate: Scenario116GateMonitoring, Expected: "growth tracked",
			Description: "[UNIT] two successive recordDiskUsage calls with an increasing measured " +
				"value produce an increasing status AND an increasing published metric — no " +
				"sticky/cached/max-only behavior.",
		},
		{
			ID: "116-TRACK-growth-F", Req: "TRACK", Layer: Scenario116LayerFunctional,
			Gate: Scenario116GateMonitoring, Expected: "growth tracked",
			Description: "[FUNCTIONAL] reconcile twice with the dbFactory returning a higher % on the " +
				"2nd pass; status + metric both increase, proving growth is tracked on settled clusters.",
		},
		{
			ID: "116-DISABLED-noop", Req: "DISABLED", Layer: Scenario116LayerFunctional,
			Gate: Scenario116GateDisabled, Expected: "no measurement",
			Description: "[FUNCTIONAL] diskMonitoring:false → reconcileStorage early-returns: the DB " +
				"factory is NEVER called, status.diskUsagePercent is unchanged, no gauge published.",
		},
		{
			ID: "116-DBERR-nonfatal", Req: "DBERR", Layer: Scenario116LayerFunctional,
			Gate: Scenario116GateMonitoring, Expected: "reconcile ok, status unchanged",
			Description: "[FUNCTIONAL] GetDiskUsagePercent returns db.ErrDiskUsageUnavailable (or a " +
				"generic error) → reconcile returns nil, StorageConfigured stays True, and " +
				"status.diskUsagePercent is NOT fabricated (left at its prior value).",
		},
		{
			ID: "116-CONTROL", Req: "CONTROL", Layer: Scenario116LayerFunctional,
			Gate: Scenario116GateMonitoring, Expected: "no reconcile error",
			Description: "[FUNCTIONAL] the full reconcile path (gate→measure→S.1/M.1→R.5) returns NO " +
				"error with a healthy DB stub — the no-false-positive control.",
		},
		{
			ID: "116-CROSSCHECK-L", Req: "CROSSCHECK", Layer: Scenario116LayerLive,
			Gate: Scenario116GateMonitoring, Expected: "metric within tolerance of df",
			Description: "[LIVE-ONLY] read cloudberry_disk_usage_percent from /metrics, run `df` on a " +
				"segment data volume pod, and assert the gauge is within tolerance of the df-derived " +
				"worst-case usage; also assert metric == status.",
		},
		{
			ID: "116-PERSIST-L", Req: "PERSIST", Layer: Scenario116LayerLive,
			Field: "status.diskUsagePercent", ExpectedValue: "",
			Gate: Scenario116GateMonitoring, Expected: "GET → status persisted & current",
			Description: "[LIVE-ONLY] after applying diskMonitoring:true and settling, a GET'd cluster " +
				"carries a persisted status.diskUsagePercent that the metric matches (M.1==S.1) and " +
				"that the steady-state path keeps current.",
		},
	}
}
