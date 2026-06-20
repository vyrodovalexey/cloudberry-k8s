package cases

// ============================================================================
// Scenario 118 — Scan Scheduling and Duration Limit
// (reconciliation rules C.5, C.10, M.3 + webhook W.5 for
//  spec.storage.recommendationScan)
// ============================================================================
//
// Acceptance scenario, two sub-scenarios:
//
//   - 118a Schedule firing (C.5 + M.3): a near-future cron (e.g. */5 * * * *)
//     drives the recommendation-scan CronJob (`<cluster>-recommendation-scan`,
//     Scenario 115's BuildRecommendationScanCronJob) to carry that schedule
//     verbatim; the cloudberry_recommendation_scan_duration_seconds histogram
//     (M.3) is populated after each reconcile-driven scan.
//
//   - 118b scanDuration cap (C.10 + M.3): scanDuration is enforced as the scan
//     timeout via resolveScanDuration (empty/invalid/<=0 -> 10s; >24h -> 24h;
//     else verbatim). When the scan hits the deadline mid-run the run is
//     TRUNCATED: Status.RecommendationScanTruncated=true,
//     cloudberry_recommendation_scan_truncated_total{cluster,namespace}
//     increments, LastRecommendationScanTime is set, only the types that
//     completed are counted (un-run types count 0, no fabrication), and the M.3
//     histogram observes the CAPPED elapsed.
//
//   - W.5 validation: an invalid scanDuration is rejected by the webhook
//     ("storage.recommendationScan.scanDuration \"<v>\" must be a valid Go
//     duration"); valid/empty (and a disabled scan's bad duration) ADMIT.
//
// Layers (reusing the shared layer vocabulary):
//   - U : unit       (internal/controller + internal/webhook + internal/metrics
//     direct; no build tag).
//   - F : functional (drive Reconcile/reconcileStorage + ValidateCreate over a
//     fake-client TestK8sEnv with an injected dbFactory; //go:build functional).
//   - L : live e2e   (`kubectl apply` near-future cron / tiny scanDuration ->
//     assert the CronJob schedule + scrape /metrics + GET status;
//     //go:build e2e, KUBECONFIG + SCENARIO118_LIVE gated; Part A always runs).
// ============================================================================

// Scenario118Gate enumerates the recommendation-scan-gate state a Scenario 118
// case exercises. The scan runs only on the scanning gate; the disabled gate is
// the early-return no-op; n/a is the control / aggregate / validation rows.
const (
	// Scenario118GateScanning means recommendationScan.enabled:true: the engine
	// runs the scan under the scanDuration cap and publishes the duration /
	// truncation signals.
	Scenario118GateScanning = "scanning"
	// Scenario118GateDisabled means recommendationScan nil / enabled:false: no
	// scan, no cap, no truncation flag/counter.
	Scenario118GateDisabled = "disabled"
	// Scenario118GateNone is used for the CONTROL / validation / aggregate rows.
	Scenario118GateNone = "n/a"
)

// Scenario118Layer enumerates the assertion layer of a Scenario 118 case,
// reusing the shared layer vocabulary.
const (
	// Scenario118LayerUnit is the controller/webhook/metrics-direct unit layer.
	Scenario118LayerUnit = Scenario104LayerBuilder
	// Scenario118LayerFunctional is the Reconcile/ValidateCreate functional layer.
	Scenario118LayerFunctional = Scenario104LayerReconcile
	// Scenario118LayerLive is the live `kubectl apply` + scrape contract layer.
	Scenario118LayerLive = Scenario104LayerLive
)

// Scenario 118 well-known live values (mirror the e2e Part B env defaults).
const (
	// Scenario118Namespace is the default deploy namespace for the live (-L) rows.
	Scenario118Namespace = "cloudberry-test"
	// Scenario118DefaultCluster is the default (SHORT) live cluster name base.
	Scenario118DefaultCluster = "s118"
	// Scenario118NearFutureSchedule is the near-future cron the 118a live rows
	// apply so the recommendation-scan CronJob fires within the test window.
	Scenario118NearFutureSchedule = "*/5 * * * *"
	// Scenario118TinyScanDuration is the tiny scanDuration the 118b live rows
	// apply so a loaded-DB scan deterministically trips the cap.
	Scenario118TinyScanDuration = "10ms"
	// Scenario118CronJobSuffix is the recommendation-scan CronJob name suffix
	// (Scenario 115's util.RecommendationScanCronJobName -> <cluster>-<suffix>).
	Scenario118CronJobSuffix = "recommendation-scan"
	// Scenario118DurationMetricName is the M.3 histogram the duration rows assert.
	Scenario118DurationMetricName = "cloudberry_recommendation_scan_duration_seconds"
	// Scenario118TruncatedMetricName is the truncation counter the cap rows assert.
	Scenario118TruncatedMetricName = "cloudberry_recommendation_scan_truncated_total"
	// Scenario118CronJobMetricName is the C.5 CronJob-provisioned gauge.
	Scenario118CronJobMetricName = "cloudberry_recommendation_scan_cronjob"
	// Scenario118TruncatedField is the dotted status path the truncation rows pin.
	Scenario118TruncatedField = "status.recommendationScanTruncated"
	// Scenario118LastScanField is the dotted status path the persistence rows pin.
	Scenario118LastScanField = "status.lastRecommendationScanTime"
)

// Scenario118Case describes one Scenario 118 sub-case. It is a flat catalog row:
// the rule family, the assertion Layer, the sub-scenario it exercises, the scan
// Gate, a short Expected outcome token, and a Description. Field/ExpectedValue
// carry the dotted status path for the rows that pin a concrete status value.
type Scenario118Case struct {
	// ID is the catalog rule id (e.g. "118a-C5-schedule-L", "118b-TRUNCATE-F").
	ID string
	// Req is the rule family the row proves: "C.5", "C.10", "M.3", "TRUNCATE",
	// "W.5", "DISABLED", "CONTROL", "PERSIST".
	Req string
	// Layer is the assertion layer: Scenario118LayerUnit / Functional / Live.
	Layer string
	// SubScenario is "118a" / "118b" or "" for the cross-cutting / validation rows.
	SubScenario string
	// Field is the dotted status path the row asserts (empty for non-status rows).
	Field string
	// ExpectedValue is the value asserted at Field, rendered as a string (empty
	// for the rows that do not pin a concrete status value).
	ExpectedValue string
	// Gate is the scan gate the row exercises: scanning / disabled / n/a.
	Gate string
	// Expected is a short human outcome token.
	Expected string
	// Description explains the case + the gate rationale.
	Description string
}

// Scenario118Cases returns the full Scenario 118 catalog: the 118a schedule +
// duration rows (C.5 / M.3), the 118b cap / truncation / capped-duration rows
// (C.10 / TRUNCATE / M.3), the W.5 validation rows, and the cross-cutting
// DISABLED / CONTROL / PERSIST rows. The -U rows are owned in
// internal/controller + internal/webhook + internal/metrics; the -F rows
// resolve at the functional Reconcile/ValidateCreate layer; the -L rows require
// a live cluster and are resolved (or SKIP cleanly) in the integration/e2e live
// parts.
func Scenario118Cases() []Scenario118Case {
	cases := []Scenario118Case{}
	cases = append(cases, scenario118ScheduleCases()...)
	cases = append(cases, scenario118DurationCases()...)
	cases = append(cases, scenario118CapCases()...)
	cases = append(cases, scenario118ValidationCases()...)
	cases = append(cases, scenario118CrossCuttingCases()...)
	return cases
}

// scenario118ScheduleCases returns the 118a schedule-firing rows (C.5): the
// CronJob carries the configured cron verbatim across the U/F/L layers.
func scenario118ScheduleCases() []Scenario118Case {
	const desc = "the recommendation-scan CronJob `<cluster>-recommendation-scan` carries the " +
		"configured cron schedule verbatim (C.5; Scenario 115's BuildRecommendationScanCronJob), " +
		"ForbidConcurrent + history limits 3 + cluster OwnerRefs"
	return []Scenario118Case{
		{
			ID: "118a-C5-schedule-U", Req: "C.5", Layer: Scenario118LayerUnit,
			SubScenario: "118a", Gate: Scenario118GateScanning, Expected: "schedule carried verbatim",
			Description: "[UNIT] " + desc + " (builder-direct).",
		},
		{
			ID: "118a-C5-schedule-F", Req: "C.5", Layer: Scenario118LayerFunctional,
			SubScenario: "118a", Gate: Scenario118GateScanning, Expected: "CronJob created with schedule",
			Description: "[FUNCTIONAL] reconcileStorage -> ensureRecommendationScanCronJob creates the " +
				"CronJob with the configured schedule; " + desc + " (drive Reconcile over a TestK8sEnv).",
		},
		{
			ID: "118a-C5-schedule-L", Req: "C.5", Layer: Scenario118LayerLive,
			SubScenario: "118a", Gate: Scenario118GateScanning, Expected: "live CronJob schedule */5",
			Description: "[LIVE-ONLY] kubectl apply schedule \"*/5 * * * *\" -> the " +
				"<cluster>-recommendation-scan CronJob exists with that schedule (C.5).",
		},
	}
}

// scenario118DurationCases returns the 118a M.3 duration-histogram rows: the
// recommendation_scan_duration_seconds histogram is populated after each scan.
func scenario118DurationCases() []Scenario118Case {
	const desc = "ObserveRecommendationScanDuration is called exactly once per scan, populating the " +
		"cloudberry_recommendation_scan_duration_seconds histogram (M.3) under {cluster,namespace}"
	return []Scenario118Case{
		{
			ID: "118a-M3-duration-U", Req: "M.3", Layer: Scenario118LayerUnit,
			SubScenario: "118a", Gate: Scenario118GateScanning, Expected: "observe once per scan",
			Description: "[UNIT] " + desc + " (controller-direct with a counting recorder).",
		},
		{
			ID: "118a-M3-duration-F", Req: "M.3", Layer: Scenario118LayerFunctional,
			SubScenario: "118a", Gate: Scenario118GateScanning, Expected: "histogram _count +1",
			Description: "[FUNCTIONAL] after a reconcile with the scan enabled the histogram _count " +
				"increases by 1; " + desc + " (drive Reconcile, scrape the registry).",
		},
		{
			ID: "118a-M3-duration-L", Req: "M.3", Layer: Scenario118LayerLive,
			SubScenario: "118a", Gate: Scenario118GateScanning, Expected: "live _count > 0",
			Description: "[LIVE-ONLY] scrape /metrics -> recommendation_scan_duration_seconds_count > 0 " +
				"after a reconcile-driven scan (M.3).",
		},
	}
}

// scenario118CapCases returns the 118b scanDuration-cap rows: the cap is
// enforced (C.10), a deadline trip truncates the run (TRUNCATE) and the
// duration histogram records the CAPPED elapsed (M.3).
func scenario118CapCases() []Scenario118Case {
	return []Scenario118Case{
		// ---- C.10 cap enforced as the scan timeout -------------------------
		{
			ID: "118b-C10-cap-U", Req: "C.10", Layer: Scenario118LayerUnit,
			SubScenario: "118b", Gate: Scenario118GateScanning, Expected: "single shared budget caps scan",
			Description: "[UNIT] recordRecommendations derives the scan dbCtx deadline from " +
				"resolveScanDuration(scanDuration) (NOT a hardcoded 10s); a tiny \"10ms\" cap against a " +
				"blocking DB trips context.DeadlineExceeded and the single shared budget bounds the " +
				"TOTAL four-Get* scan (not 4x the timeout).",
		},
		{
			ID: "118b-C10-cap-F", Req: "C.10", Layer: Scenario118LayerFunctional,
			SubScenario: "118b", Gate: Scenario118GateScanning, Expected: "reconcile non-fatal under cap",
			Description: "[FUNCTIONAL] a reconcile with a tiny scanDuration + a blocking dbFactory " +
				"completes WITHOUT hanging; reconcile returns nil and StorageConfigured stays True.",
		},
		{
			ID: "118b-C10-cap-L", Req: "C.10", Layer: Scenario118LayerLive,
			SubScenario: "118b", Gate: Scenario118GateScanning, Expected: "live scan caps, no hang",
			Description: "[LIVE-ONLY] kubectl apply scanDuration \"10ms\" on a loaded DB -> the " +
				"operator-driven scan terminates at the cap; the reconcile does NOT hang.",
		},
		// ---- TRUNCATE signal (status flag + counter) -----------------------
		{
			ID: "118b-TRUNCATE-U", Req: "TRUNCATE", Layer: Scenario118LayerUnit,
			SubScenario: "118b", Field: Scenario118TruncatedField, ExpectedValue: "true",
			Gate: Scenario118GateScanning, Expected: "flag+counter, partial counts",
			Description: "[UNIT] on a deadline trip recordRecommendations sets " +
				"Status.RecommendationScanTruncated=true, increments " +
				"cloudberry_recommendation_scan_truncated_total, sets LastRecommendationScanTime, and " +
				"counts ONLY the types that completed (un-run types count 0, no fabrication); a later " +
				"clean scan resets the flag to false (never sticky).",
		},
		{
			ID: "118b-TRUNCATE-F", Req: "TRUNCATE", Layer: Scenario118LayerFunctional,
			SubScenario: "118b", Field: Scenario118TruncatedField, ExpectedValue: "true",
			Gate: Scenario118GateScanning, Expected: "GET truncated=true, counter +1",
			Description: "[FUNCTIONAL] after a reconcile that trips the cap, a GET'd cluster carries " +
				"status.recommendationScanTruncated=true and the truncation counter is present; a " +
				"no-truncate scan (generous cap) leaves the flag false.",
		},
		{
			ID: "118b-TRUNCATE-L", Req: "TRUNCATE", Layer: Scenario118LayerLive,
			SubScenario: "118b", Field: Scenario118TruncatedField, ExpectedValue: "true",
			Gate: Scenario118GateScanning, Expected: "live truncated=true, counter up",
			Description: "[LIVE-ONLY] kubectl get -o jsonpath status.recommendationScanTruncated == true " +
				"after a capped scan; recommendation_scan_truncated_total increases (CONFIG-ONLY degrade " +
				"when the cap can't be deterministically tripped live in-window).",
		},
		// ---- M.3 reflects the capped run -----------------------------------
		{
			ID: "118b-M3-capped-U", Req: "M.3", Layer: Scenario118LayerUnit,
			SubScenario: "118b", Gate: Scenario118GateScanning, Expected: "observed elapsed ~= cap",
			Description: "[UNIT] on a capped scan ObserveRecommendationScanDuration is STILL called once " +
				"with the elapsed reflecting the CAPPED run (bounded near the cap, NOT the unbounded " +
				"block).",
		},
		{
			ID: "118b-M3-capped-F", Req: "M.3", Layer: Scenario118LayerFunctional,
			SubScenario: "118b", Gate: Scenario118GateScanning, Expected: "capped histogram _count +1",
			Description: "[FUNCTIONAL] after the capped reconcile the histogram _count increases by 1 and " +
				"the observed sample is bounded near the cap.",
		},
		{
			ID: "118b-M3-capped-L", Req: "M.3", Layer: Scenario118LayerLive,
			SubScenario: "118b", Gate: Scenario118GateScanning, Expected: "live capped sample recorded",
			Description: "[LIVE-ONLY] recommendation_scan_duration_seconds observes a sample for the " +
				"capped run; the histogram count advances.",
		},
	}
}

// scenario118ValidationCases returns the W.5 webhook rows: an invalid
// scanDuration is rejected (enabled-gated), valid/empty (and disabled+invalid)
// ADMIT.
func scenario118ValidationCases() []Scenario118Case {
	return []Scenario118Case{
		{
			ID: "118-VALIDATE-duration-U", Req: "W.5", Layer: Scenario118LayerUnit,
			Gate: Scenario118GateNone, Expected: "reject bad, accept good/empty",
			Description: "[UNIT] validateStorageManagement REJECTS an unparseable scanDuration with the " +
				"W.5 message (storage.recommendationScan.scanDuration \"<v>\" must be a valid Go " +
				"duration) and ACCEPTS \"30s\"/\"2h\"/\"\"; enabled-gated so a disabled scan's bad " +
				"duration ADMITs.",
		},
		{
			ID: "118-VALIDATE-duration-F", Req: "W.5", Layer: Scenario118LayerFunctional,
			Gate: Scenario118GateNone, Expected: "ValidateCreate denies bad duration",
			Description: "[FUNCTIONAL] CloudberryClusterValidator.ValidateCreate (the same chain the " +
				"admission webhook uses) DENIES enabled + invalid scanDuration and ADMITs valid/empty.",
		},
		{
			ID: "118-VALIDATE-duration-L", Req: "W.5", Layer: Scenario118LayerLive,
			Gate: Scenario118GateNone, Expected: "live apiserver rejects bad duration",
			Description: "[LIVE-ONLY] kubectl apply an invalid scanDuration -> the apiserver rejects with " +
				"the W.5 error; a valid duration is accepted.",
		},
	}
}

// scenario118CrossCuttingCases returns the DISABLED-noop, CONTROL, and PERSIST
// cross-cutting rows.
func scenario118CrossCuttingCases() []Scenario118Case {
	return []Scenario118Case{
		{
			ID: "118-DISABLED-noop", Req: "DISABLED", Layer: Scenario118LayerFunctional,
			Gate: Scenario118GateDisabled, Expected: "no scan, no cap, no truncate",
			Description: "[FUNCTIONAL] recommendationScan nil / enabled:false -> recordRecommendations is " +
				"NOT run: no cap, no duration observe, no truncation flag/counter, the count untouched.",
		},
		{
			ID: "118-CONTROL", Req: "CONTROL", Layer: Scenario118LayerFunctional,
			Gate: Scenario118GateScanning, Expected: "completes, flag false, observe once",
			Description: "[FUNCTIONAL] the full path with a healthy fast DB stub + a generous " +
				"scanDuration:\"2h\": the scan COMPLETES, RecommendationScanTruncated=false, the duration " +
				"is observed once, and the reconcile returns no error (no-false-positive control).",
		},
		{
			ID: "118-PERSIST-L", Req: "PERSIST", Layer: Scenario118LayerLive,
			Field: Scenario118LastScanField, ExpectedValue: "",
			Gate: Scenario118GateScanning, Expected: "GET truncated/lastScan persisted",
			Description: "[LIVE-ONLY] after applying + settling, a GET'd cluster carries a persisted " +
				"status.recommendationScanTruncated (and optional status.lastRecommendationScanTime) " +
				"consistent with the last scan; steady-state keeps it current.",
		},
	}
}
