package cases

// ============================================================================
// Scenario 122 — Disabled States (C.2 / C.4 / C.12) + Re-enablement
// (reconciliation rules C.2 diskMonitoring, C.4 recommendationScan, C.12
//  usageReport for spec.storage.*)
// ============================================================================
//
// Acceptance scenario: flip each of the three storage feature gates to false →
// the disabled behavior holds; flip back to true → each reactivates.
//
//   - 122a C.2 (spec.storage.diskMonitoring:false): reconcileStorage /
//     refreshStorageOnSteadyState STOP measuring disk usage (recordDiskUsage is
//     not reached; the DB factory is never consulted). RESET-on-disable:
//     status.diskUsagePercent is reset to 0 AND cloudberry_disk_usage_percent is
//     published 0 so the disabled state is an explicit signal, not a frozen
//     stale reading. The recommendation-scan CronJob is GC'd. Re-enabling
//     repopulates from the measured value (M.1==S.1).
//
//   - 122b C.4 (spec.storage.recommendationScan.enabled:false): NO scan runs
//     (recordRecommendations is not called) and the CronJob is GC'd. CLEAR-on-
//     disable: clearRecommendations resets status.recommendationCount to 0 and
//     publishes cloudberry_recommendations_total{type}=0 for ALL FOUR types
//     (bloat/skew/age/index_bloat) + clears recommendationScanTruncated, so an
//     enabled→disabled scan leaves no stale count/gauge. POST
//     …/storage/recommendations/scan → 400 RECOMMENDATION_SCAN_NOT_ENABLED.
//     Re-enabling re-creates the CronJob and resumes recordRecommendations.
//
//   - 122c C.12 (spec.storage.usageReport.enabled:false): the usage-report
//     endpoint/CLI SOFT-gates: GET …/storage/usage-report → 200
//     {usageReportEnabled:false, entries:[], total:0} (NOT a 400). Re-enabling
//     returns content (usageReportEnabled:true, per-db/per-table entries).
//
// Note: status fields RecommendationCount + DiskUsagePercent had omitempty
// REMOVED so a 0 reliably persists (kubectl get shows 0 on disabled); the
// authoritative disabled signal at the unit/functional layers is the metric
// gauge reset (a direct metric call, not subject to omitempty).
//
// Layers (reusing the shared layer vocabulary):
//   - U : unit       (internal/controller + internal/api direct over a stub DB +
//     fake client + real PrometheusRecorder; no build tag).
//   - F : functional (drive Reconcile/reconcileStorage over a fake-client
//     TestK8sEnv with an injected dbFactory; the C.12 soft-gate through the REAL
//     api.Server router; //go:build functional).
//   - L : live e2e   (`kubectl patch` flip → scrape /metrics + GET status/API/CLI;
//     //go:build e2e, KUBECONFIG + SCENARIO122_LIVE gated; Part A always runs).
// ============================================================================

// Scenario122Gate enumerates the feature-gate state a Scenario 122 case
// exercises: disabled (the gate off), enabled (the re-enable flip), or n/a (the
// cross-cutting control / persistence rows).
const (
	// Scenario122GateDisabled means the feature gate is off: the disabled
	// behavior (reset/clear/soft-gate) holds.
	Scenario122GateDisabled = "disabled"
	// Scenario122GateEnabled means the feature gate is flipped back on: the
	// reactivation behavior (repopulate) holds.
	Scenario122GateEnabled = "enabled"
	// Scenario122GateNone is used for the CONTROL / persistence rows.
	Scenario122GateNone = "n/a"
)

// Scenario122Layer enumerates the assertion layer of a Scenario 122 case,
// reusing the shared layer vocabulary.
const (
	// Scenario122LayerUnit is the controller/api-direct unit layer.
	Scenario122LayerUnit = Scenario104LayerBuilder
	// Scenario122LayerFunctional is the Reconcile/api-router functional layer.
	Scenario122LayerFunctional = Scenario104LayerReconcile
	// Scenario122LayerLive is the live `kubectl patch` + scrape contract layer.
	Scenario122LayerLive = Scenario104LayerLive
)

// Scenario 122 recommendation-type tokens (mirror controller recTypeBloat/Skew/
// Age/IndexBloat and the type label on cloudberry_recommendations_total).
const (
	Scenario122TypeBloat      = "bloat"
	Scenario122TypeSkew       = "skew"
	Scenario122TypeAge        = "age"
	Scenario122TypeIndexBloat = "index_bloat"
)

// Scenario 122 well-known live values (mirror the e2e Part B env defaults).
const (
	// Scenario122Namespace is the default deploy namespace for the live (-L) rows.
	Scenario122Namespace = "cloudberry-test"
	// Scenario122DefaultCluster is the default (SHORT) live cluster name base.
	Scenario122DefaultCluster = "s122"
	// Scenario122DiskMetricName is the C.2 disk-usage gauge the 122a rows assert.
	Scenario122DiskMetricName = "cloudberry_disk_usage_percent"
	// Scenario122RecsMetricName is the C.4 per-type recommendations gauge.
	Scenario122RecsMetricName = "cloudberry_recommendations_total"
	// Scenario122CronJobSuffix is the recommendation-scan CronJob name suffix
	// (util.RecommendationScanCronJobName → <cluster>-<suffix>).
	Scenario122CronJobSuffix = "recommendation-scan"
	// Scenario122ScanNotEnabledCode is the POST-scan 400 error code (C.4 disabled).
	Scenario122ScanNotEnabledCode = "RECOMMENDATION_SCAN_NOT_ENABLED"
)

// Scenario122Case describes one Scenario 122 sub-case. It is a flat catalog row:
// the rule family (Req), the assertion Layer, the sub-scenario / feature (122a /
// 122b / 122c), the gate State, a short Assert outcome token, the Gate, and a
// Description. Field/ExpectedValue carry the dotted status path for the rows that
// pin a concrete status value.
type Scenario122Case struct {
	// ID is the catalog rule id (e.g. "122a-C2-disabled-U", "122b-C4-reenable-F").
	ID string
	// Req is the rule family the row proves: "C.2", "C.4", "C.12", "CONTROL",
	// "PERSIST".
	Req string
	// Layer is the assertion layer: Scenario122LayerUnit / Functional / Live.
	Layer string
	// Feature is "122a" / "122b" / "122c" or "" for the cross-cutting rows.
	Feature string
	// State is the disabled/enabled lifecycle leg the row exercises: "disabled"
	// (the gate off) or "enabled" (the re-enable flip) or "" (control rows).
	State string
	// Field is the dotted status path the row asserts (empty for non-status rows).
	Field string
	// ExpectedValue is the value asserted at Field, rendered as a string (empty
	// for the rows that do not pin a concrete status value).
	ExpectedValue string
	// Gate is the feature-gate the row exercises: disabled / enabled / n/a.
	Gate string
	// Assert is a short human outcome token.
	Assert string
	// Description explains the case + the gate rationale.
	Description string
}

// Scenario122Cases returns the full Scenario 122 catalog: the 122a C.2
// disabled/re-enable rows, the 122b C.4 disabled/re-enable rows, the 122c C.12
// disabled/re-enable rows, and the cross-cutting CONTROL / PERSIST rows. The -U
// rows are owned in internal/controller + internal/api; the -F rows resolve at
// the functional Reconcile / api-router layer; the -L rows require a live
// cluster and are resolved (or SKIP cleanly) in the integration/e2e live parts.
func Scenario122Cases() []Scenario122Case {
	cases := []Scenario122Case{}
	cases = append(cases, scenario122DiskMonitoringCases()...)
	cases = append(cases, scenario122RecommendationScanCases()...)
	cases = append(cases, scenario122UsageReportCases()...)
	cases = append(cases, scenario122CrossCuttingCases()...)
	return cases
}

// scenario122DiskMonitoringCases returns the 122a C.2 disabled + re-enable rows
// across the U/F/L layers.
func scenario122DiskMonitoringCases() []Scenario122Case {
	const disabledDesc = "diskMonitoring:false → reconcileStorage does NOT measure disk usage " +
		"(recordDiskUsage not reached; DB factory never consulted); RESET-on-disable: " +
		"status.diskUsagePercent==0 AND cloudberry_disk_usage_percent published 0; the " +
		"recommendation-scan CronJob is GC'd"
	const reenableDesc = "flip diskMonitoring:true → recordDiskUsage resumes; status.diskUsagePercent " +
		"and cloudberry_disk_usage_percent repopulate from the measured value (M.1==S.1)"
	return []Scenario122Case{
		{
			ID: "122a-C2-disabled-U", Req: "C.2", Layer: Scenario122LayerUnit,
			Feature: "122a", State: "disabled",
			Field: "status.diskUsagePercent", ExpectedValue: "0",
			Gate: Scenario122GateDisabled, Assert: "no measure, gauge 0",
			Description: "[UNIT] " + disabledDesc + " (controller-direct over a stub DB client).",
		},
		{
			ID: "122a-C2-disabled-F", Req: "C.2", Layer: Scenario122LayerFunctional,
			Feature: "122a", State: "disabled",
			Field: "status.diskUsagePercent", ExpectedValue: "0",
			Gate: Scenario122GateDisabled, Assert: "GET → 0, gauge 0",
			Description: "[FUNCTIONAL] " + disabledDesc + " (drive Reconcile over a TestK8sEnv).",
		},
		{
			ID: "122a-C2-disabled-L", Req: "C.2", Layer: Scenario122LayerLive,
			Feature: "122a", State: "disabled",
			Field: "status.diskUsagePercent", ExpectedValue: "0",
			Gate: Scenario122GateDisabled, Assert: "live status 0 / gauge no-advance",
			Description: "[LIVE-ONLY] kubectl patch diskMonitoring:false → GET status.diskUsagePercent==0 " +
				"(or scrape cloudberry_disk_usage_percent→0); CronJob NotFound.",
		},
		{
			ID: "122a-C2-reenable-U", Req: "C.2", Layer: Scenario122LayerUnit,
			Feature: "122a", State: "enabled",
			Gate: Scenario122GateEnabled, Assert: "measure resumes",
			Description: "[UNIT] " + reenableDesc + " (controller-direct over a stub DB client).",
		},
		{
			ID: "122a-C2-reenable-F", Req: "C.2", Layer: Scenario122LayerFunctional,
			Feature: "122a", State: "enabled",
			Gate: Scenario122GateEnabled, Assert: "GET → measured, gauge==status",
			Description: "[FUNCTIONAL] " + reenableDesc + " (reconcile off→on over a TestK8sEnv).",
		},
		{
			ID: "122a-C2-reenable-L", Req: "C.2", Layer: Scenario122LayerLive,
			Feature: "122a", State: "enabled",
			Gate: Scenario122GateEnabled, Assert: "live repopulate",
			Description: "[LIVE-ONLY] kubectl patch diskMonitoring:true → gauge + status repopulate to the " +
				"live measured % (CONFIG-ONLY degrade when not observable in-window).",
		},
	}
}

// scenario122RecommendationScanCases returns the 122b C.4 disabled + re-enable
// rows across the U/F/L layers, including the POST-scan 400 disabled row.
func scenario122RecommendationScanCases() []Scenario122Case {
	const disabledDesc = "recommendationScan.enabled:false → NO scan runs (recordRecommendations not " +
		"called); CLEAR-on-disable: status.recommendationCount==0 AND " +
		"cloudberry_recommendations_total{type}=0 for ALL FOUR types (bloat/skew/age/index_bloat) + " +
		"recommendationScanTruncated cleared; the recommendation-scan CronJob is GC'd"
	const reenableDesc = "flip recommendationScan.enabled:true → recordRecommendations resumes (count = " +
		"sum of per-type; per-type gauges repopulate) and ensureRecommendationScanCronJob re-creates " +
		"the CronJob"
	return []Scenario122Case{
		{
			ID: "122b-C4-disabled-U", Req: "C.4", Layer: Scenario122LayerUnit,
			Feature: "122b", State: "disabled",
			Field: "status.recommendationCount", ExpectedValue: "0",
			Gate: Scenario122GateDisabled, Assert: "count 0, gauges 0, no scan",
			Description: "[UNIT] " + disabledDesc + " (controller-direct; a stale count is cleared to 0).",
		},
		{
			ID: "122b-C4-disabled-F", Req: "C.4", Layer: Scenario122LayerFunctional,
			Feature: "122b", State: "disabled",
			Field: "status.recommendationCount", ExpectedValue: "0",
			Gate: Scenario122GateDisabled, Assert: "GET → 0, gauges 0, CronJob NotFound",
			Description: "[FUNCTIONAL] " + disabledDesc + " (reconcile enabled→disabled over a TestK8sEnv).",
		},
		{
			ID: "122b-C4-disabled-api-F", Req: "C.4", Layer: Scenario122LayerFunctional,
			Feature: "122b", State: "disabled",
			Gate: Scenario122GateDisabled, Assert: "POST scan → 400 NOT_ENABLED",
			Description: "[FUNCTIONAL] POST …/storage/recommendations/scan with the scan disabled → " +
				"400 RECOMMENDATION_SCAN_NOT_ENABLED (through the REAL api.Server router).",
		},
		{
			ID: "122b-C4-disabled-L", Req: "C.4", Layer: Scenario122LayerLive,
			Feature: "122b", State: "disabled",
			Field: "status.recommendationCount", ExpectedValue: "0",
			Gate: Scenario122GateDisabled, Assert: "live count 0, gauges 0, CronJob gone, POST 400",
			Description: "[LIVE-ONLY] kubectl patch recommendationScan.enabled:false → " +
				"status.recommendationCount==0 + recommendations_total{type}→0 + CronJob " +
				"<cluster>-recommendation-scan NotFound + POST scan → 400.",
		},
		{
			ID: "122b-C4-reenable-U", Req: "C.4", Layer: Scenario122LayerUnit,
			Feature: "122b", State: "enabled",
			Gate: Scenario122GateEnabled, Assert: "scan resumes, count==sum",
			Description: "[UNIT] " + reenableDesc + "; the clear-on-disable does NOT run on the enabled " +
				"path (idempotency guard — must not zero a fresh scan).",
		},
		{
			ID: "122b-C4-reenable-F", Req: "C.4", Layer: Scenario122LayerFunctional,
			Feature: "122b", State: "enabled",
			Field: "status.recommendationCount", ExpectedValue: "active total",
			Gate: Scenario122GateEnabled, Assert: "GET → count, gauges, CronJob present",
			Description: "[FUNCTIONAL] " + reenableDesc + " (reconcile disabled→enabled over a TestK8sEnv).",
		},
		{
			ID: "122b-C4-reenable-L", Req: "C.4", Layer: Scenario122LayerLive,
			Feature: "122b", State: "enabled",
			Gate: Scenario122GateEnabled, Assert: "live CronJob + count resume, POST 200",
			Description: "[LIVE-ONLY] kubectl patch recommendationScan.enabled:true → CronJob present + " +
				"count + per-type gauges repopulate; POST scan → 200 (CONFIG-ONLY degrade in-window).",
		},
	}
}

// scenario122UsageReportCases returns the 122c C.12 disabled + re-enable rows
// (the read-only usage-report soft-gate) across the U/F/L layers.
func scenario122UsageReportCases() []Scenario122Case {
	const disabledDesc = "usageReport.enabled:false → GET …/storage/usage-report SOFT-gates: 200 " +
		"{usageReportEnabled:false, entries:[], total:0} (NOT a 400); the DB is never opened; the " +
		"cloudberry-ctl usage-report CLI inherits the disabled/empty result"
	const reenableDesc = "flip usageReport.enabled:true → 200 {usageReportEnabled:true, entries:[…]} " +
		"with per-db/per-table content; the CLI returns the populated report"
	return []Scenario122Case{
		{
			ID: "122c-C12-disabled-U", Req: "C.12", Layer: Scenario122LayerUnit,
			Feature: "122c", State: "disabled",
			Gate: Scenario122GateDisabled, Assert: "200 enabled:false, empty",
			Description: "[UNIT] " + disabledDesc + " (handleGetUsageReport-direct).",
		},
		{
			ID: "122c-C12-disabled-F", Req: "C.12", Layer: Scenario122LayerFunctional,
			Feature: "122c", State: "disabled",
			Gate: Scenario122GateDisabled, Assert: "200 enabled:false, empty",
			Description: "[FUNCTIONAL] " + disabledDesc + " (through the REAL api.Server router).",
		},
		{
			ID: "122c-C12-disabled-L", Req: "C.12", Layer: Scenario122LayerLive,
			Feature: "122c", State: "disabled",
			Gate: Scenario122GateDisabled, Assert: "live API/CLI enabled:false, empty",
			Description: "[LIVE-ONLY] kubectl patch usageReport.enabled:false → API + CLI both report " +
				"usageReportEnabled:false + empty.",
		},
		{
			ID: "122c-C12-reenable-U", Req: "C.12", Layer: Scenario122LayerUnit,
			Feature: "122c", State: "enabled",
			Gate: Scenario122GateEnabled, Assert: "200 enabled:true, entries",
			Description: "[UNIT] " + reenableDesc + " (handleGetUsageReport-direct).",
		},
		{
			ID: "122c-C12-reenable-F", Req: "C.12", Layer: Scenario122LayerFunctional,
			Feature: "122c", State: "enabled",
			Gate: Scenario122GateEnabled, Assert: "200 enabled:true, entries",
			Description: "[FUNCTIONAL] " + reenableDesc + " (through the REAL api.Server router).",
		},
		{
			ID: "122c-C12-reenable-L", Req: "C.12", Layer: Scenario122LayerLive,
			Feature: "122c", State: "enabled",
			Gate: Scenario122GateEnabled, Assert: "live API/CLI enabled:true, content",
			Description: "[LIVE-ONLY] kubectl patch usageReport.enabled:true → API + CLI return content " +
				"(usageReportEnabled:true, per-db/per-table entries; CONFIG-ONLY degrade in-window).",
		},
	}
}

// scenario122CrossCuttingCases returns the CONTROL and PERSIST cross-cutting
// rows.
func scenario122CrossCuttingCases() []Scenario122Case {
	return []Scenario122Case{
		{
			ID: "122-CONTROL", Req: "CONTROL", Layer: Scenario122LayerFunctional,
			Gate: Scenario122GateNone, Assert: "no reconcile error round-trip",
			Description: "[FUNCTIONAL] the full disabled-state reconcile path (gate → clear-on-disable → " +
				"GC → no R.5 error) returns NO error with a healthy DB stub; the disabled→enabled→disabled " +
				"re-enable round-trip also returns no error (no-false-positive control).",
		},
		{
			ID: "122-PERSIST-L", Req: "PERSIST", Layer: Scenario122LayerLive,
			State: "disabled", Field: "status.recommendationCount", ExpectedValue: "0",
			Gate: Scenario122GateDisabled, Assert: "GET → persisted 0, metric-sum matches",
			Description: "[LIVE-ONLY] after applying recommendationScan.enabled:false and settling, a GET'd " +
				"cluster carries a PERSISTED status.recommendationCount==0 that the per-type metric sum " +
				"matches and that the steady-state path keeps at 0 (not re-stale).",
		},
	}
}
