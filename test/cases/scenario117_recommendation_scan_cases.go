package cases

// ============================================================================
// Scenario 117 — Recommendation Scan Across All Four Types
// (reconciliation rules S.2, M.2, R.3, R.4, RT.1–RT.4, C.6–C.9, M.4 for
//  spec.storage.recommendationScan)
// ============================================================================
//
// Acceptance scenario: with spec.storage.recommendationScan.enabled:true, every
// reconcile (and the steady-state refresh) RUNS all FOUR threshold-aware
// recommendation scans — bloat / skew / age / index_bloat — via the
// db.Get{Bloat,Skew,Age,IndexBloat}Recommendations(ctx, db.RecommendationThresholds)
// queries and:
//   - R.3  reconcileStorage()/refreshStorageOnSteadyState() call
//          recordRecommendations which PROCESSES the recommendationScan config
//          (the four CRD thresholds) and threads them into the queries.
//   - S.2/R.4 status.recommendationCount is set to the CURRENT total active count
//          (sum across the four types), NOT the stale carry-forward value.
//   - M.2  cloudberry_recommendations_total{type=bloat|skew|age|index_bloat} is
//          set for EACH of the four types from that type's count (including 0 so
//          a cleared type's gauge resets). M.2==count invariant: the sum of the
//          per-type gauges equals status.recommendationCount.
//   - M.4  cloudberry_table_bloat_ratio{table} continues to be published from the
//          bloat recs (the bloat scan runs once and feeds both count and ratio).
//   - C.6–C.9 each type's DB query GATES on its CRD threshold (>= is inclusive):
//          bloat→BloatThreshold, skew→SkewThreshold, age→AgeThreshold,
//          index_bloat→IndexBloatThreshold.
//   - RT.1–RT.4 each type emits a recommendation of the matching Type when a row
//          is above the threshold.
//   - Honest per-type fallback (mirror Scenario 116): when a gp_toolkit view is
//          missing the type counts 0 + a log — never a fabricated value — and the
//          reconcile stays non-fatal.
//
// Layers (reusing the shared layer vocabulary):
//   - U : unit       (internal/db + internal/controller direct; no build tag).
//   - F : functional (drive Reconcile/reconcileStorage over a fake-client
//     TestK8sEnv with an injected dbFactory; //go:build functional).
//   - L : live e2e   (`kubectl apply` recommendationScan:true → scrape /metrics +
//     GET status; //go:build e2e, KUBECONFIG + SCENARIO117_LIVE gated; Part A
//     always runs).
// ============================================================================

// Scenario117Gate enumerates the recommendation-scan-gate state a Scenario 117
// case exercises. The scan runs only on the scanning gate; the disabled gate is
// the early-return no-op.
const (
	// Scenario117GateScanning means recommendationScan.enabled:true: the engine
	// runs all four scans, counts per type, and publishes the metrics/status.
	Scenario117GateScanning = "scanning"
	// Scenario117GateDisabled means recommendationScan nil / enabled:false: no
	// scan, no status/metric update.
	Scenario117GateDisabled = "disabled"
	// Scenario117GateNone is used for the CONTROL / aggregate rows.
	Scenario117GateNone = "n/a"
)

// Scenario117Layer enumerates the assertion layer of a Scenario 117 case,
// reusing the shared layer vocabulary.
const (
	// Scenario117LayerUnit is the db/controller-direct unit layer.
	Scenario117LayerUnit = Scenario104LayerBuilder
	// Scenario117LayerFunctional is the Reconcile/reconcileStorage functional layer.
	Scenario117LayerFunctional = Scenario104LayerReconcile
	// Scenario117LayerLive is the live `kubectl apply` + scrape contract layer.
	Scenario117LayerLive = Scenario104LayerLive
)

// Scenario 117 recommendation-type tokens (mirror controller recTypeBloat/Skew/
// Age/IndexBloat and the type label on cloudberry_recommendations_total).
const (
	Scenario117TypeBloat      = "bloat"
	Scenario117TypeSkew       = "skew"
	Scenario117TypeAge        = "age"
	Scenario117TypeIndexBloat = "index_bloat"
)

// Scenario 117 well-known live values (mirror the e2e Part B env defaults).
const (
	// Scenario117Namespace is the default deploy namespace for the live (-L) rows.
	Scenario117Namespace = "cloudberry-test"
	// Scenario117DefaultCluster is the default (SHORT) live cluster name base.
	Scenario117DefaultCluster = "s117"
	// Scenario117RecsMetricName is the Prometheus gauge the per-type rows assert.
	Scenario117RecsMetricName = "cloudberry_recommendations_total"
	// Scenario117BloatRatioMetricName is the Prometheus gauge the M.4 rows assert.
	Scenario117BloatRatioMetricName = "cloudberry_table_bloat_ratio"
)

// Scenario117Case describes one Scenario 117 sub-case. It is a flat catalog row:
// the rule family, the assertion Layer, the sub-scenario / recommendation type
// it exercises, the scan Gate, a short Expected outcome token, and a
// Description. Field/ExpectedValue carry the dotted status path for the rows that
// pin a concrete status value.
type Scenario117Case struct {
	// ID is the catalog rule id (e.g. "117a-RT1-U", "117-M2-bytype-F").
	ID string
	// Req is the rule family the row proves: "RT.1".."RT.4", "C.6".."C.9",
	// "M.4", "S.2", "R.4", "M.2", "R.3", "DISABLED", "DBERR", "CONTROL",
	// "PERSIST".
	Req string
	// Layer is the assertion layer: Scenario117LayerUnit / Functional / Live.
	Layer string
	// SubScenario is "117a".."117d" or "" for the aggregate / cross-cutting rows.
	SubScenario string
	// RecType is the recommendation type the row exercises:
	// "bloat"|"skew"|"age"|"index_bloat"|"" (aggregate).
	RecType string
	// Field is the dotted status path the row asserts (empty for aggregate rows).
	Field string
	// ExpectedValue is the value asserted at Field, rendered as a string (empty
	// for the aggregate rows).
	ExpectedValue string
	// Gate is the scan gate the row exercises: scanning / disabled / n/a.
	Gate string
	// Expected is a short human outcome token.
	Expected string
	// Description explains the case + the gate rationale.
	Description string
}

// Scenario117Cases returns the full Scenario 117 catalog: the per-rule -U/-F/-L
// rows for the four sub-scenarios (117a bloat RT.1/C.6/M.4, 117b skew RT.2/C.7,
// 117c age RT.3/C.8, 117d index RT.4/C.9), plus the sub-scenario CLEAR/BOUNDARY
// rows and the cross-cutting count (S.2/R.4), per-type metric (M.2), processed
// (R.3), DISABLED-noop, DBERR-nonfatal, CONTROL, and PERSIST rows. The -U rows
// are owned in internal/db + internal/controller; the -F rows resolve at the
// functional Reconcile layer; the -L rows require a live cluster and are resolved
// (or SKIP cleanly) in the integration/e2e live parts.
func Scenario117Cases() []Scenario117Case {
	cases := []Scenario117Case{}
	cases = append(cases, scenario117PerRuleCases()...)
	cases = append(cases, scenario117SubScenarioExtraCases()...)
	cases = append(cases, scenario117CrossCuttingCases()...)
	return cases
}

// scenario117Rule carries the per-rule metadata shared across the U/F/L layers,
// scoped to a sub-scenario + recommendation type.
type scenario117Rule struct {
	req         string
	idStem      string
	subScenario string
	recType     string
	expected    string
	desc        string
}

// scenario117Rules returns the canonical per-type rule table. The per-layer rows
// are generated from it so the U/F/L catalogs cannot drift. Each sub-scenario
// contributes its RT.n (the type emits its recommendation) and C.n (the threshold
// gates the row) rules; 117a additionally contributes M.4 (table_bloat_ratio).
func scenario117Rules() []scenario117Rule {
	return []scenario117Rule{
		// ----- 117a TABLE BLOAT (RT.1, C.6, M.4; threshold = bloatThreshold) -----
		{
			req: "RT.1", idStem: "117a-RT1", subScenario: "117a", recType: Scenario117TypeBloat,
			expected: "bloat rec emitted",
			desc: "GetBloatRecommendations returns Type=\"bloat\" rows where dead_pct >= " +
				"bloatThreshold; the controller counts them into recommendationCount",
		},
		{
			req: "C.6", idStem: "117a-C6", subScenario: "117a", recType: Scenario117TypeBloat,
			expected: "below-threshold excluded",
			desc: "dead_pct STRICTLY below bloatThreshold yields NO bloat row; the threshold is " +
				"carried into the SQL gate ($1) and threaded from spec.bloatThreshold",
		},
		{
			req: "M.4", idStem: "117a-M4", subScenario: "117a", recType: Scenario117TypeBloat,
			expected: "table_bloat_ratio set",
			desc: "recordRecommendations publishes cloudberry_table_bloat_ratio{table}=rec.Ratio " +
				"from the bloat recs (the bloat scan runs once, feeding both count and ratio)",
		},
		// ----- 117b DATA SKEW (RT.2, C.7; threshold = skewThreshold) -------------
		{
			req: "RT.2", idStem: "117b-RT2", subScenario: "117b", recType: Scenario117TypeSkew,
			expected: "skew rec emitted",
			desc: "GetSkewRecommendations returns Type=\"skew\" rows where skew coeff >= " +
				"skewThreshold; honest fallback when gp_toolkit.gp_skew_coefficients is absent",
		},
		{
			req: "C.7", idStem: "117b-C7", subScenario: "117b", recType: Scenario117TypeSkew,
			expected: "below-threshold excluded",
			desc: "skew coeff below skewThreshold → NO skew row; the controller threads " +
				"spec.skewThreshold into the query gate",
		},
		// ----- 117c XID AGE (RT.3, C.8; threshold = ageThreshold) ----------------
		{
			req: "RT.3", idStem: "117c-RT3", subScenario: "117c", recType: Scenario117TypeAge,
			expected: "age rec emitted",
			desc: "GetAgeRecommendations returns Type=\"age\" rows where age(relfrozenxid) >= " +
				"ageThreshold; no dead-tuple proxy fallback",
		},
		{
			req: "C.8", idStem: "117c-C8", subScenario: "117c", recType: Scenario117TypeAge,
			expected: "below-threshold excluded",
			desc: "age below ageThreshold → NO age row; the controller threads spec.ageThreshold " +
				"(int64) into the query gate",
		},
		// ----- 117d INDEX BLOAT (RT.4, C.9; threshold = indexBloatThreshold) -----
		{
			req: "RT.4", idStem: "117d-RT4", subScenario: "117d", recType: Scenario117TypeIndexBloat,
			expected: "index_bloat rec emitted",
			desc: "GetIndexBloatRecommendations returns Type=\"index_bloat\" rows where the bloat " +
				"estimate >= indexBloatThreshold; honest fallback on a missing stat view",
		},
		{
			req: "C.9", idStem: "117d-C9", subScenario: "117d", recType: Scenario117TypeIndexBloat,
			expected: "below-threshold excluded",
			desc: "index bloat below indexBloatThreshold → NO row; the controller threads " +
				"spec.indexBloatThreshold into the query gate",
		},
	}
}

// scenario117PerRuleCases returns the per-rule -U/-F/-L rows. For every rule in
// scenario117Rules there is a unit (-U), functional (-F), and live (-L) row, each
// gated on scanning.
func scenario117PerRuleCases() []Scenario117Case {
	rules := scenario117Rules()
	out := make([]Scenario117Case, 0, len(rules)*3)
	for _, r := range rules {
		out = append(out,
			Scenario117Case{
				ID: r.idStem + "-U", Req: r.req, Layer: Scenario117LayerUnit,
				SubScenario: r.subScenario, RecType: r.recType,
				Gate: Scenario117GateScanning, Expected: r.expected,
				Description: "[UNIT] " + r.desc + " (db/controller-direct over a stub DB client).",
			},
			Scenario117Case{
				ID: r.idStem + "-F", Req: r.req, Layer: Scenario117LayerFunctional,
				SubScenario: r.subScenario, RecType: r.recType,
				Gate: Scenario117GateScanning, Expected: r.expected,
				Description: "[FUNCTIONAL] " + r.desc + " (drive Reconcile/reconcileStorage over a TestK8sEnv).",
			},
			Scenario117Case{
				ID: r.idStem + "-L", Req: r.req, Layer: Scenario117LayerLive,
				SubScenario: r.subScenario, RecType: r.recType,
				Gate: Scenario117GateScanning, Expected: "live " + r.expected,
				Description: "[LIVE-ONLY] kubectl apply recommendationScan:true → " + r.desc + ".",
			},
		)
	}
	return out
}

// scenario117SubScenarioExtraCases returns the sub-scenario CLEAR / BOUNDARY
// rows: 117a-CLEAR (a bloated table is VACUUMed → the bloat rec clears and the
// gauge/count drop) and 117b-BOUNDARY (a skew rec appears at exactly the
// threshold and disappears one tick over it on the next scan).
func scenario117SubScenarioExtraCases() []Scenario117Case {
	return []Scenario117Case{
		{
			ID: "117a-CLEAR-F", Req: "RT.1", Layer: Scenario117LayerFunctional,
			SubScenario: "117a", RecType: Scenario117TypeBloat,
			Gate: Scenario117GateScanning, Expected: "bloat clears on rescan",
			Description: "[FUNCTIONAL] a second scan with the bloat type returning 0 rows drops the " +
				"recommendations_total{type=bloat} gauge to 0 (published every scan) AND decreases " +
				"recommendationCount — no stale/sticky gauge.",
		},
		{
			ID: "117a-CLEAR-L", Req: "RT.1", Layer: Scenario117LayerLive,
			SubScenario: "117a", RecType: Scenario117TypeBloat,
			Gate: Scenario117GateScanning, Expected: "live bloat clears after VACUUM",
			Description: "[LIVE-ONLY] bloat fixture, then VACUUM (FULL) the table so dead_pct→0; the " +
				"next scan clears the bloat recommendation (recommendations_total{type=bloat} drops " +
				"and recommendationCount decreases).",
		},
		{
			ID: "117b-BOUNDARY-F", Req: "C.7", Layer: Scenario117LayerFunctional,
			SubScenario: "117b", RecType: Scenario117TypeSkew,
			Gate: Scenario117GateScanning, Expected: "skew flips at boundary",
			Description: "[FUNCTIONAL] reconcile twice across the threshold boundary: a tight threshold " +
				"includes the skew rec (>= is inclusive); loosening it one tick over the coefficient " +
				"removes the rec; recommendationCount reflects the flip both ways.",
		},
		{
			ID: "117b-BOUNDARY-L", Req: "C.7", Layer: Scenario117LayerLive,
			SubScenario: "117b", RecType: Scenario117TypeSkew,
			Gate: Scenario117GateScanning, Expected: "live skew flips at boundary",
			Description: "[LIVE-ONLY] skew fixture; PATCH skewThreshold at/over the coefficient and " +
				"rescan each time: the skew rec appears at the boundary then disappears one tick over " +
				"it (clean SKIP when gp_skew_coefficients is absent).",
		},
	}
}

// scenario117CrossCuttingCases returns the count (S.2/R.4), per-type metric
// (M.2), processed (R.3), DISABLED-noop, DBERR-nonfatal, CONTROL, and PERSIST
// rows.
func scenario117CrossCuttingCases() []Scenario117Case {
	return []Scenario117Case{
		{
			ID: "117-S2-R4-count-U", Req: "S.2", Layer: Scenario117LayerUnit,
			Field: "status.recommendationCount", ExpectedValue: "sum of all four per-type counts",
			Gate: Scenario117GateScanning, Expected: "count==sum",
			Description: "[UNIT] recordRecommendations computes total = sum of the four per-type counts " +
				"and writes Status.RecommendationCount = total (S.2/R.4), replacing the stale value.",
		},
		{
			ID: "117-S2-R4-count-F", Req: "R.4", Layer: Scenario117LayerFunctional,
			Field: "status.recommendationCount", ExpectedValue: "current active count",
			Gate: Scenario117GateScanning, Expected: "count==sum",
			Description: "[FUNCTIONAL] after reconcile, status.recommendationCount == the active count " +
				"(NOT the stale carry-forward); it reflects the CURRENT scan and persists across a GET.",
		},
		{
			ID: "117-S2-R4-count-L", Req: "R.4", Layer: Scenario117LayerLive,
			Field: "status.recommendationCount", ExpectedValue: "live active total",
			Gate: Scenario117GateScanning, Expected: "live count==sum",
			Description: "[LIVE-ONLY] GET cluster → status.recommendationCount == the active total " +
				"across all four types after a settle.",
		},
		{
			ID: "117-M2-bytype-U", Req: "M.2", Layer: Scenario117LayerUnit,
			Gate: Scenario117GateScanning, Expected: "metric per type",
			Description: "[UNIT] SetRecommendationsTotal is called once PER type with that type's count; " +
				"the type label is in {bloat,skew,age,index_bloat} and is published even when 0.",
		},
		{
			ID: "117-M2-bytype-F", Req: "M.2", Layer: Scenario117LayerFunctional,
			Gate: Scenario117GateScanning, Expected: "metric==count (sum)",
			Description: "[FUNCTIONAL] cloudberry_recommendations_total{type} is set per type after " +
				"reconcile and the sum of the gauges == recommendationCount (M.2==count invariant).",
		},
		{
			ID: "117-M2-bytype-L", Req: "M.2", Layer: Scenario117LayerLive,
			Gate: Scenario117GateScanning, Expected: "live metric per type",
			Description: "[LIVE-ONLY] scrape /metrics → recommendations_total{type=bloat|skew|age|" +
				"index_bloat} present and the sum == status.recommendationCount.",
		},
		{
			ID: "117-R3-processed-U", Req: "R.3", Layer: Scenario117LayerUnit,
			Gate: Scenario117GateScanning, Expected: "config processed",
			Description: "[UNIT] recordRecommendations reads the recommendationScan config (the four " +
				"thresholds) and calls all four Get methods with those thresholds threaded in.",
		},
		{
			ID: "117-R3-processed-F", Req: "R.3", Layer: Scenario117LayerFunctional,
			Gate: Scenario117GateScanning, Expected: "config processed",
			Description: "[FUNCTIONAL] reconcile with the scan enabled drives all four scans + the " +
				"duration observation; the processing is reflected in the count & per-type metrics.",
		},
		{
			ID: "117-R3-processed-L", Req: "R.3", Layer: Scenario117LayerLive,
			Gate: Scenario117GateScanning, Expected: "live config processed",
			Description: "[LIVE-ONLY] kubectl apply recommendationScan:true → the operator runs the scan " +
				"and the recommendation_scan_duration_seconds histogram + per-type gauges are exposed.",
		},
		{
			ID: "117-DISABLED-noop", Req: "DISABLED", Layer: Scenario117LayerFunctional,
			Gate: Scenario117GateDisabled, Expected: "no scan",
			Description: "[FUNCTIONAL] recommendationScan nil / enabled:false → recordRecommendations is " +
				"NOT run: the count is untouched, the DB factory is never called for recs, and no " +
				"recommendations_total is published.",
		},
		{
			ID: "117-DBERR-nonfatal", Req: "DBERR", Layer: Scenario117LayerFunctional,
			Gate: Scenario117GateScanning, Expected: "reconcile ok, type=0",
			Description: "[FUNCTIONAL] a single Get* returns an error / honest fallback → reconcile " +
				"returns nil, StorageConfigured stays True, count = sum of the SUCCESSFUL types, and " +
				"the failing type contributes 0 (never a fabricated value).",
		},
		{
			ID: "117-CONTROL", Req: "CONTROL", Layer: Scenario117LayerFunctional,
			Gate: Scenario117GateScanning, Expected: "no reconcile error",
			Description: "[FUNCTIONAL] the full reconcile path (gate→4 scans→count→M.2→duration→R.5) " +
				"returns NO error with a healthy DB stub — the no-false-positive control.",
		},
		{
			ID: "117-PERSIST-L", Req: "PERSIST", Layer: Scenario117LayerLive,
			Field: "status.recommendationCount", ExpectedValue: "",
			Gate: Scenario117GateScanning, Expected: "GET → count persisted & current",
			Description: "[LIVE-ONLY] after applying recommendationScan:true and settling, a GET'd cluster " +
				"carries a persisted status.recommendationCount that the per-type metric sum matches and " +
				"that the steady-state path keeps current.",
		},
	}
}
