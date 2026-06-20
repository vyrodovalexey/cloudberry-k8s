package cases

// ============================================================================
// Scenario 115 — Enable Storage Management with Full Configuration
// (reconciliation rules R.1, C.1, C.3, C.5, R.5)
// ============================================================================
//
// Acceptance scenario: applying a CloudberryCluster with a FULL spec.storage
// block — diskMonitoring:true; recommendationScan enabled with schedule
// "0 3 * * 0" and all five thresholds (bloat/skew/age/indexBloat/scanDuration);
// usageReport enabled+monthly — drives AdminReconciler.reconcileStorage() to:
//   - R.1  proceed past the diskMonitoring gate (admin_controller.go:3571)
//   - C.1  accept/parse the recommendationScan config (schedule + thresholds)
//   - C.3  accept the threshold set (bloat/skew/age/indexBloat/scanDuration)
//   - C.5  CREATE a CronJob "<cluster>-recommendation-scan" for the schedule
//          (BuildRecommendationScanCronJob + ensureRecommendationScanCronJob),
//          GC'd when the scan is disabled
//   - R.5  set Status condition StorageConfigured=True (reason StorageReconciled)
//   - CONTROL: the full reconcile path returns ZERO errors (no false positive).
//
// The only newly-added production behavior is C.5 (materialize the schedule as a
// CronJob); R.1/C.1/C.3/R.5 are regression/contract coverage over already-shipped
// behavior. usageReport is parsed without a CronJob side-effect.
//
// Layers (reusing the shared layer vocabulary):
//   - U : unit       (internal/builder + internal/controller; no build tag).
//   - F : functional (drive AdminReconciler.Reconcile / reconcileStorage over a
//     fake-client TestK8sEnv; //go:build functional).
//   - L : live e2e   (`kubectl apply` the full storage block → GET → assert the
//     CronJob exists + StorageConfigured=True + the block persisted; //go:build
//     e2e + integration, KUBECONFIG + SCENARIO115_LIVE gated; Part A always runs).
// ============================================================================

// Scenario115Gate enumerates the storage-gate state a Scenario 115 case
// exercises. The full reconcile path runs only on the full gate; the disabled
// gate is the early-return no-op.
const (
	// Scenario115GateFull means the FULL storage block is applied
	// (diskMonitoring:true + recommendationScan enabled + usageReport).
	Scenario115GateFull = "full"
	// Scenario115GateDisabled means diskMonitoring:false (or storage nil):
	// reconcileStorage returns early (no CronJob, no condition).
	Scenario115GateDisabled = "disabled"
	// Scenario115GateNone is used for the CONTROL / aggregate rows.
	Scenario115GateNone = "n/a"
)

// Scenario115Layer enumerates the assertion layer of a Scenario 115 case,
// reusing the shared layer vocabulary.
const (
	// Scenario115LayerUnit is the builder/controller-direct unit layer.
	Scenario115LayerUnit = Scenario104LayerBuilder
	// Scenario115LayerFunctional is the Reconcile/reconcileStorage functional layer.
	Scenario115LayerFunctional = Scenario104LayerReconcile
	// Scenario115LayerLive is the live `kubectl apply` persisted-contract layer.
	Scenario115LayerLive = Scenario104LayerLive
)

// Scenario 115 well-known live values (mirror the e2e Part B env defaults).
const (
	// Scenario115Namespace is the default deploy namespace for the live (-L) rows.
	Scenario115Namespace = "cloudberry-test"
	// Scenario115DefaultCluster is the default (SHORT) live cluster name base.
	Scenario115DefaultCluster = "s115"
	// Scenario115Schedule is the recommendation-scan schedule the full block carries.
	Scenario115Schedule = "0 3 * * 0"
	// Scenario115CronJobSuffix is the recommendation-scan CronJob name suffix.
	Scenario115CronJobSuffix = "-recommendation-scan"
)

// Scenario115Case describes one Scenario 115 sub-case. It is a flat catalog row
// identical in shape to Scenario114Case: the rule family, the assertion Layer,
// the storage Gate, a short Expected outcome token, and a Description.
type Scenario115Case struct {
	// ID is the catalog rule id (e.g. "115-R1-U", "115-C5-F", "115-PERSIST-L").
	ID string
	// Req is the rule family the row proves: "R.1", "C.1", "C.3", "C.5", "R.5",
	// "CONTROL", "DISABLED", "USAGE", "PERSIST".
	Req string
	// Layer is the assertion layer: Scenario115LayerUnit / Functional / Live.
	Layer string
	// Field is the dotted spec path the row asserts (empty for aggregate rows).
	Field string
	// ExpectedValue is the value asserted at Field, rendered as a string (empty
	// for the aggregate rows).
	ExpectedValue string
	// Gate is the storage gate the row exercises: full / disabled / n/a.
	Gate string
	// Expected is a short human outcome token.
	Expected string
	// Description explains the case + the gate rationale.
	Description string
}

// Scenario115Cases returns the full Scenario 115 catalog: the per-rule -U/-F/-L
// rows for R.1/C.1/C.3/C.5/R.5, plus the CONTROL-noerror, DISABLED-noop,
// USAGE-accept, and live PERSIST-L cross-cutting rows. The -U rows are owned in
// internal/builder + internal/controller; the -F rows resolve at the functional
// Reconcile layer; the -L rows require a live cluster and are resolved (or SKIP
// cleanly) in the integration/e2e live parts.
func Scenario115Cases() []Scenario115Case {
	cases := []Scenario115Case{}
	cases = append(cases, scenario115PerRuleCases()...)
	cases = append(cases, scenario115CrossCuttingCases()...)
	return cases
}

// scenario115Rule carries the per-rule metadata shared across the U/F/L layers.
type scenario115Rule struct {
	req      string
	idStem   string
	expected string
	desc     string
}

// scenario115Rules returns the canonical R.1/C.1/C.3/C.5/R.5 rule table. The
// per-layer rows are generated from it so the U/F/L catalogs cannot drift.
func scenario115Rules() []scenario115Rule {
	return []scenario115Rule{
		{
			req: "R.1", idStem: "R1",
			expected: "gate proceeds",
			desc:     "diskMonitoring:true gate proceeds; reconcileStorage does not short-circuit",
		},
		{
			req: "C.1", idStem: "C1",
			expected: "scan config accepted",
			desc:     "recommendationScan config (enabled + schedule) is accepted/parsed, not rewritten",
		},
		{
			req: "C.3", idStem: "C3",
			expected: "thresholds accepted",
			desc:     "the five thresholds (bloat/skew/age/indexBloat/scanDuration) are accepted unchanged",
		},
		{
			req: "C.5", idStem: "C5",
			expected: "CronJob created",
			desc:     "a CronJob <cluster>-recommendation-scan is created for the schedule (GC'd when disabled)",
		},
		{
			req: "R.5", idStem: "R5",
			expected: "StorageConfigured=True",
			desc:     "Status condition StorageConfigured is set True (reason StorageReconciled)",
		},
	}
}

// scenario115PerRuleCases returns the per-rule -U/-F/-L rows. For every rule
// R.1/C.1/C.3/C.5/R.5 there is a unit (-U), functional (-F), and live (-L) row,
// each gated on the full storage block.
func scenario115PerRuleCases() []Scenario115Case {
	rules := scenario115Rules()
	out := make([]Scenario115Case, 0, len(rules)*3)
	for _, r := range rules {
		out = append(out,
			Scenario115Case{
				ID: "115-" + r.idStem + "-U", Req: r.req, Layer: Scenario115LayerUnit,
				Gate: Scenario115GateFull, Expected: r.expected,
				Description: "[UNIT] " + r.desc + " (builder/controller-direct over a fake client).",
			},
			Scenario115Case{
				ID: "115-" + r.idStem + "-F", Req: r.req, Layer: Scenario115LayerFunctional,
				Gate: Scenario115GateFull, Expected: r.expected,
				Description: "[FUNCTIONAL] " + r.desc + " (drive Reconcile/reconcileStorage over a TestK8sEnv).",
			},
			Scenario115Case{
				ID: "115-" + r.idStem + "-L", Req: r.req, Layer: Scenario115LayerLive,
				Gate: Scenario115GateFull, Expected: "persisted " + r.expected,
				Description: "[LIVE-ONLY] kubectl apply the full storage block → " + r.desc + ".",
			},
		)
	}
	return out
}

// scenario115CrossCuttingCases returns the CONTROL-noerror, DISABLED-noop,
// USAGE-accept, and live PERSIST-L rows.
func scenario115CrossCuttingCases() []Scenario115Case {
	return []Scenario115Case{
		{
			ID: "115-CONTROL-noerror", Req: "CONTROL", Layer: Scenario115LayerFunctional,
			Gate: Scenario115GateFull, Expected: "no reconcile error",
			Description: "[FUNCTIONAL] the full reconcile path (R.1→C.1→C.3→C.5→R.5) returns NO " +
				"error — the no-false-positive control (a CronJob create failure would surface here).",
		},
		{
			ID: "115-DISABLED-noop", Req: "DISABLED", Layer: Scenario115LayerFunctional,
			Gate: Scenario115GateDisabled, Expected: "no CronJob, no condition",
			Description: "[FUNCTIONAL] diskMonitoring:false → reconcileStorage returns early: NO " +
				"recommendation-scan CronJob is created AND NO StorageConfigured condition is added.",
		},
		{
			ID: "115-USAGE-accept", Req: "USAGE", Layer: Scenario115LayerFunctional,
			Field: "storage.usageReport.enabled", ExpectedValue: "true",
			Gate: Scenario115GateFull, Expected: "usageReport accepted",
			Description: "[FUNCTIONAL] usageReport{enabled:true, monthly:true} is parsed without " +
				"error; reconcileStorage returns nil and the usageReport survives a reconcile pass.",
		},
		{
			ID: "115-PERSIST-L", Req: "PERSIST", Layer: Scenario115LayerLive,
			Field: "storage.recommendationScan.schedule", ExpectedValue: Scenario115Schedule,
			Gate: Scenario115GateFull, Expected: "GET → block persisted + CronJob exists",
			Description: "[LIVE-ONLY] after applying the FULL block the GET'd cluster carries " +
				"StorageConfigured=True with the scan/thresholds/usageReport persisted AND the " +
				"recommendation-scan CronJob exists with schedule " + Scenario115Schedule + ".",
		},
	}
}
