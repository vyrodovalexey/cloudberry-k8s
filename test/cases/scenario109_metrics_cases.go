package cases

// ============================================================================
// Scenario 109 — All Prometheus Metrics (M.1–M.16)
// ============================================================================
//
// Acceptance scenario: every PXF / data-loading / gpfdist metric in the metrics
// spec (M.1–M.16) is accounted for HONESTLY. A metric is either:
//
//   - IMPLEMENTED — emitted from a NAMED REAL source and asserted PRESENT in
//     VictoriaMetrics with the right label keys after real activity, OR
//   - ABSENT — intentionally NOT registered/emitted because there is no honest
//     source (synthesizing a value would be dishonest). An ABSENT assertion (the
//     series has ZERO samples in VM) is a PASSING honesty check, NOT a failure.
//
// Per-metric decision (task-breakdown §1):
//   M.1  pxf_service_up{cluster,namespace,segment_host}         IMPLEMENTED (1/0 from real pxf container readiness)
//   M.2  pxf_requests_total  → actuator http_server_requests_*  IMPLEMENTED (actuator passthrough; native names)
//   M.3  pxf_request_duration_seconds → actuator histogram      IMPLEMENTED (actuator passthrough; native names)
//   M.4  pxf_bytes_transferred_total                            ABSENT (no honest source)
//   M.5  pxf_records_total                                      ABSENT (substituted by data_loading_rows_total)
//   M.6  pxf_errors_total                                       FOLDED into data_loading_errors_total (+actuator non-2xx)
//   M.7  pxf_active_connections                                 ABSENT (no honest source; threads.busy is a proxy)
//   M.8  data_loading_jobs_active                               IMPLEMENTED (done)
//   M.9  data_loading_rows_total{job,source_type}               IMPLEMENTED (done; DATALOAD_ROWS marker)
//   M.10 data_loading_bytes_total{cluster,namespace,job,source_type} IMPLEMENTED (DATALOAD_BYTES marker; absent when unmeasured)
//   M.11 data_loading_errors_total{job}                         IMPLEMENTED (done; Job Failed)
//   M.12 data_loading_job_duration_seconds                      IMPLEMENTED (done; histogram)
//   M.13 data_loading_job_last_success_timestamp{job}           IMPLEMENTED (done)
//   M.14 data_loading_job_status{job} (0/1/2/3)                 IMPLEMENTED (done; k8s status)
//   M.15 gpfdist_connections_active                             ABSENT (gpfdist has no scrapable endpoint)
//   M.16 gpfdist_bytes_served_total                             ABSENT (gpfdist has no scrapable endpoint)
//
// The -U rows are pure recorder/parser unit cases (owned in internal/*). The -F
// rows are resolved at the functional layer (the real reconcile + a spy recorder
// asserting HONEST recorder calls). The -L rows require the deployed cluster +
// VictoriaMetrics and are resolved (or SKIP cleanly) in the e2e suite. The
// -ABSENT rows assert documented absence (a NOT-emitted metric is a PASS).
// ============================================================================

// Scenario109Layer enumerates the assertion layer a Scenario 109 case resolves
// at, sharing the Scenario 104/105 vocabulary ("builder"/"reconcile"/"live").
const (
	// Scenario109LayerUnit is a pure recorder/parser unit case (infra-free) —
	// owned in internal/metrics + internal/controller; mirrored at functional
	// where a registry-level honesty assertion is useful.
	Scenario109LayerUnit = Scenario104LayerBuilder
	// Scenario109LayerFunctional is a controller/reconcile-level case resolved
	// over a fake client + a spy metrics recorder (infra-free, deterministic).
	Scenario109LayerFunctional = Scenario104LayerReconcile
	// Scenario109LayerLive requires the deployed cluster + VictoriaMetrics
	// (live-only).
	Scenario109LayerLive = Scenario104LayerLive
)

// Scenario 109 well-known names + values (mirror the production metric names in
// internal/metrics/metrics.go and the live e2e defaults).
const (
	// Scenario109Namespace is the default deploy namespace for the live (-L) rows.
	Scenario109Namespace = "cloudberry-test"
	// Scenario109DefaultCluster is the default (SHORT) live cluster name.
	Scenario109DefaultCluster = "s109"

	// --- IMPLEMENTED metric names (asserted PRESENT in VM) -------------------
	Scenario109MetricServiceUp   = "cloudberry_pxf_service_up"
	Scenario109MetricBytesTotal  = "cloudberry_data_loading_bytes_total"
	Scenario109MetricRowsTotal   = "cloudberry_data_loading_rows_total"
	Scenario109MetricJobsActive  = "cloudberry_data_loading_jobs_active"
	Scenario109MetricErrorsTotal = "cloudberry_data_loading_errors_total"
	Scenario109MetricJobDuration = "cloudberry_data_loading_job_duration_seconds"
	Scenario109MetricLastSuccess = "cloudberry_data_loading_job_last_success_timestamp"
	Scenario109MetricJobStatus   = "cloudberry_data_loading_job_status"
	// Scenario109ActuatorRequestsCount / Bucket are the REAL PXF Spring Boot
	// actuator request-count + latency-histogram series (M.2/M.3) under their
	// native names (NOT renamed to pxf_requests_total) — scraped by vmagent from
	// :5888/actuator/prometheus when the scrape job is wired in.
	Scenario109ActuatorRequestsCount = "http_server_requests_seconds_count"
	Scenario109ActuatorRequestBucket = "http_server_requests_seconds_bucket"

	// --- ABSENT metric names (asserted NOT in VM — honesty passes) -----------
	Scenario109MetricBytesTransferred  = "cloudberry_pxf_bytes_transferred_total"
	Scenario109MetricRecordsTotal      = "cloudberry_pxf_records_total"
	Scenario109MetricActiveConnections = "cloudberry_pxf_active_connections"
	Scenario109MetricGpfdistConns      = "cloudberry_gpfdist_connections_active"
	Scenario109MetricGpfdistBytes      = "cloudberry_gpfdist_bytes_served_total"
	Scenario109MetricPxfErrors         = "cloudberry_pxf_errors_total"
)

// Scenario109AbsentMetrics is the canonical list of intentionally-absent metric
// names: each MUST have ZERO samples in VictoriaMetrics (a passing honesty
// assertion) and MUST be unregistered in the operator's Prometheus registry.
var Scenario109AbsentMetrics = []string{
	Scenario109MetricBytesTransferred,
	Scenario109MetricRecordsTotal,
	Scenario109MetricActiveConnections,
	Scenario109MetricGpfdistConns,
	Scenario109MetricGpfdistBytes,
	Scenario109MetricPxfErrors,
}

// Scenario109Case describes one Scenario 109 sub-case. It mirrors the
// Scenario105Case SHAPE: a flat catalog row carrying an ID + the metric
// requirement family + the resolution Layer + a human Expected token + a
// Description that names the HONEST signal. Live-only rows are marked
// [LIVE-ONLY]; absence rows name the documented-absence rationale.
type Scenario109Case struct {
	// ID is the catalog rule id (e.g. "109-M1-F", "109-M1-KILL", "109-HONESTY").
	ID string
	// Req is the metric family the row proves: "M.1".."M.16", "HONESTY", "VM".
	Req string
	// Layer is the assertion layer: Scenario109LayerUnit, Scenario109LayerFunctional
	// or Scenario109LayerLive.
	Layer string
	// Expected is a short outcome token / human description of the asserted
	// outcome.
	Expected string
	// Description explains the case and names the HONEST signal. Live-only rows
	// are marked [LIVE-ONLY]; absence rows assert a NOT-emitted metric (a PASS).
	Description string
}

// Scenario109Cases returns the full Scenario 109 catalog (task-breakdown §3):
// the per-metric -U/-F/-L rows, the honesty/absence rows, and the KILL/CYCLE
// special rows. The -U rows are owned in internal/*; the -F rows resolve at the
// functional layer; the -L rows require the deployed cluster + VictoriaMetrics
// and are resolved (or SKIP cleanly) in e2e.
func Scenario109Cases() []Scenario109Case {
	cases := []Scenario109Case{}
	cases = append(cases, scenario109ImplementedCases()...)
	cases = append(cases, scenario109AbsentCases()...)
	cases = append(cases, scenario109CrossCuttingCases()...)
	return cases
}

// scenario109ImplementedCases returns the rows for the IMPLEMENTED metrics
// (asserted PRESENT in VM with the right label keys).
//
//nolint:funlen // an exhaustive per-implemented-metric table.
func scenario109ImplementedCases() []Scenario109Case {
	return []Scenario109Case{
		// --- M.1 pxf_service_up (IMPLEMENTED) ------------------------------
		{
			ID: "109-M1-U", Req: "M.1", Layer: Scenario109LayerUnit,
			Expected: "SetPXFServiceUp writes pxf_service_up{segment_host} 0/1",
			Description: "SetPXFServiceUp gathers the per-host gauge value 1/0; owned at " +
				"internal/metrics/metrics_scenario109_test.go (TestSetPXFServiceUp_Scenario109).",
		},
		{
			ID: "109-M1-F", Req: "M.1", Layer: Scenario109LayerFunctional,
			Expected: "reconcilePxf sets one service_up per OBSERVED host from real readiness",
			Description: "feed segment-primary pods of mixed pxf readiness → SetPXFServiceUp is " +
				"called once per observed host with 1 (Ready) / 0 (not); a host with no pxf " +
				"container reports 0; NO pods → no call (no fabricated host).",
		},
		{
			ID: "109-M1-L", Req: "M.1", Layer: Scenario109LayerLive,
			Expected: "LIVE cloudberry_pxf_service_up{segment_host}=1 on healthy segments",
			Description: "[LIVE-ONLY] query VM after activity: pxf_service_up present with a " +
				"segment_host label and value 1 on a healthy segment.",
		},
		{
			ID: "109-M1-KILL", Req: "M.1", Layer: Scenario109LayerLive,
			Expected: "LIVE kill pxf on one segment → that host's series → 0",
			Description: "[LIVE-ONLY, SCENARIO109_KILL=1] stop pxf on one segment, wait → that " +
				"segment_host's pxf_service_up → 0; restore. Destructive/optional/last.",
		},

		// --- M.2 pxf_requests_total (IMPLEMENTED via actuator passthrough) --
		{
			ID: "109-M2-F", Req: "M.2", Layer: Scenario109LayerFunctional,
			Expected: "builder enables the actuator prometheus endpoint (exposure.include)",
			Description: "the PXF sidecar env enables management.endpoints.web.exposure.include=" +
				"health,prometheus so :5888/actuator/prometheus serves the REAL request count; " +
				"owned at internal/builder/dataload_builder_scenario109_test.go.",
		},
		{
			ID: "109-M2-L", Req: "M.2", Layer: Scenario109LayerLive,
			Expected: "LIVE actuator http_server_requests_seconds_count present in VM",
			Description: "[LIVE-ONLY] after a pxf:// read, the actuator request-count series is " +
				"present in VM for the pxf sidecar target (CONFIG-ONLY if the actuator scrape " +
				"job isn't wired in this env). NO invented server/profile/operation label.",
		},

		// --- M.3 pxf_request_duration_seconds (IMPLEMENTED via actuator) ----
		{
			ID: "109-M3-L", Req: "M.3", Layer: Scenario109LayerLive,
			Expected: "LIVE actuator http_server_requests_seconds_bucket present in VM",
			Description: "[LIVE-ONLY] the REAL actuator latency histogram (_bucket/_count) is " +
				"present in VM after loads (CONFIG-ONLY if the actuator scrape isn't wired).",
		},

		// --- M.8 data_loading_jobs_active (DONE) ---------------------------
		{
			ID: "109-M8-F", Req: "M.8", Layer: Scenario109LayerFunctional,
			Expected:    "SetDataLoadingJobsActive reflects the enabled-job count",
			Description: "reconcile with K enabled jobs → SetDataLoadingJobsActive called with K.",
		},
		{
			ID: "109-M8-L", Req: "M.8", Layer: Scenario109LayerLive,
			Expected:    "LIVE cloudberry_data_loading_jobs_active present in VM",
			Description: "[LIVE-ONLY] jobs_active present in VM after jobs are configured.",
		},

		// --- M.9 data_loading_rows_total (DONE) ----------------------------
		{
			ID: "109-M9-F", Req: "M.9", Layer: Scenario109LayerFunctional,
			Expected: "DATALOAD_ROWS marker → RecordDataLoadingRows{job,source_type}",
			Description: "a succeeded Job whose pod carries DATALOAD_ROWS → RecordDataLoadingRows " +
				"with the right job + source_type (harvested, never synthesized).",
		},
		{
			ID: "109-M9-L", Req: "M.9", Layer: Scenario109LayerLive,
			Expected:    "LIVE rows_total{job,source_type} present after a real load",
			Description: "[LIVE-ONLY] data_loading_rows_total with job + source_type labels present.",
		},

		// --- M.10 data_loading_bytes_total (IMPLEMENTED) -------------------
		{
			ID: "109-M10-U", Req: "M.10", Layer: Scenario109LayerUnit,
			Expected: "parseDataLoadBytesMessage happy/zero/garbage/absent",
			Description: "DATALOAD_BYTES=12345→(12345,true); =abc→(0,false); absent→(0,false); " +
				"owned at internal/controller/dataload_controller_scenario109_test.go.",
		},
		{
			ID: "109-M10-F", Req: "M.10", Layer: Scenario109LayerFunctional,
			Expected: "marker pod → RecordDataLoadingBytes; no marker → NOT called",
			Description: "a succeeded Job whose pod carries a DATALOAD_BYTES marker → " +
				"RecordDataLoadingBytes{job,source_type,bytes}; WITHOUT the marker → bytes NOT " +
				"recorded (honest absence).",
		},
		{
			ID: "109-M10-L", Req: "M.10", Layer: Scenario109LayerLive,
			Expected: "LIVE bytes_total present for a LOCAL gpload job, else honest ABSENT",
			Description: "[LIVE-ONLY] data_loading_bytes_total{job,source_type} present for a LOCAL " +
				"gpload load with a real byte count; if only external/pxf loads ran, assert it's " +
				"honestly ABSENT for those and log CONFIG-ONLY (never fabricated).",
		},

		// --- M.11 data_loading_errors_total (DONE) -------------------------
		{
			ID: "109-M11-F", Req: "M.11", Layer: Scenario109LayerFunctional,
			Expected: "Job Failed → RecordDataLoadingErrors{job} (+ job_status=3)",
			Description: "a terminal Failed Job → RecordDataLoadingErrors called for the job; this " +
				"is the HONEST M.6 error signal (no synthetic pxf_errors_total).",
		},
		{
			ID: "109-M11-L", Req: "M.11", Layer: Scenario109LayerLive,
			Expected:    "LIVE errors_total{job} present after a forced failure",
			Description: "[LIVE-ONLY] data_loading_errors_total{job} present after the forced bad load.",
		},

		// --- M.12 data_loading_job_duration_seconds (DONE) -----------------
		{
			ID: "109-M12-F", Req: "M.12", Layer: Scenario109LayerFunctional,
			Expected: "succeeded Job with start/completion → ObserveDataLoadingJobDuration",
			Description: "a succeeded Job with start+completion timestamps → the duration histogram " +
				"is observed for the job.",
		},
		{
			ID: "109-M12-L", Req: "M.12", Layer: Scenario109LayerLive,
			Expected:    "LIVE job_duration_seconds histogram (_bucket/_count) present",
			Description: "[LIVE-ONLY] the duration histogram buckets/count present in VM after a run.",
		},

		// --- M.13 data_loading_job_last_success_timestamp (DONE) -----------
		{
			ID: "109-M13-F", Req: "M.13", Layer: Scenario109LayerFunctional,
			Expected: "succeeded Job → SetDataLoadingJobLastSuccess from completionTime",
			Description: "the last-success timestamp gauge is set from the Job's completionTime on " +
				"success only.",
		},
		{
			ID: "109-M13-L", Req: "M.13", Layer: Scenario109LayerLive,
			Expected:    "LIVE last_success_timestamp{job} present after a successful load",
			Description: "[LIVE-ONLY] data_loading_job_last_success_timestamp{job} present in VM.",
		},

		// --- M.14 data_loading_job_status (DONE) ---------------------------
		{
			ID: "109-M14-F", Req: "M.14", Layer: Scenario109LayerFunctional,
			Expected: "status gauge tracks the current code (0/1/2/3)",
			Description: "SetDataLoadingJobStatus is always called with the current code; running→1, " +
				"success→2.",
		},
		{
			ID: "109-M14-CYCLE", Req: "M.14", Layer: Scenario109LayerFunctional,
			Expected: "lifecycle 0/1 → 2 (success) and → 3 (failure)",
			Description: "drive a Job lifecycle and assert SetDataLoadingJobStatus observes the success " +
				"code 2 and (on a forced failure) the failure code 3.",
		},
		{
			ID: "109-M14-L", Req: "M.14", Layer: Scenario109LayerLive,
			Expected: "LIVE job_status{job} shows a 2 (success) and a 3 (failure)",
			Description: "[LIVE-ONLY] data_loading_job_status{job} present in VM with a success (2) and a " +
				"forced-failure (3) terminal value.",
		},
	}
}

// scenario109AbsentCases returns the documented-absence rows: each asserts a
// NOT-emitted metric (a passing honesty check).
func scenario109AbsentCases() []Scenario109Case {
	return []Scenario109Case{
		{
			ID: "109-M4-ABSENT", Req: "M.4", Layer: Scenario109LayerUnit,
			Expected: "cloudberry_pxf_bytes_transferred_total NOT registered/emitted",
			Description: "PXF 2.1.0 exposes no honest per-transfer byte counter → the metric is " +
				"never registered and has ZERO series in VM (honest absence = PASS).",
		},
		{
			ID: "109-M5-ABSENT", Req: "M.5", Layer: Scenario109LayerUnit,
			Expected: "cloudberry_pxf_records_total NOT emitted (substituted by rows_total)",
			Description: "no PXF-native record counter; record throughput is observed via the honest " +
				"data_loading_rows_total instead. pxf_records_total has ZERO series in VM.",
		},
		{
			ID: "109-M6-FOLD", Req: "M.6", Layer: Scenario109LayerFunctional,
			Expected: "no synthetic pxf_errors_total; errors fold into data_loading_errors_total",
			Description: "a forced load failure increments data_loading_errors_total{job} (+ job_status=3); " +
				"NO synthetic cloudberry_pxf_errors_total is registered/emitted.",
		},
		{
			ID: "109-M7-ABSENT", Req: "M.7", Layer: Scenario109LayerUnit,
			Expected: "cloudberry_pxf_active_connections NOT emitted",
			Description: "no honest live-connection gauge (tomcat.threads.busy is a proxy, never relabeled " +
				"as active_connections). ZERO series in VM.",
		},
		{
			ID: "109-M15-ABSENT", Req: "M.15", Layer: Scenario109LayerUnit,
			Expected: "cloudberry_gpfdist_connections_active NOT emitted",
			Description: "gpfdist exposes no scrapable endpoint (only a log file) → no active-connection " +
				"metric. ZERO series in VM.",
		},
		{
			ID: "109-M16-ABSENT", Req: "M.16", Layer: Scenario109LayerUnit,
			Expected:    "cloudberry_gpfdist_bytes_served_total NOT emitted",
			Description: "gpfdist exposes no scrapable served-byte counter → no metric. ZERO series in VM.",
		},
	}
}

// scenario109CrossCuttingCases returns the registry-honesty guard + the live VM
// reachability rows.
func scenario109CrossCuttingCases() []Scenario109Case {
	return []Scenario109Case{
		{
			ID: "109-HONESTY", Req: "HONESTY", Layer: Scenario109LayerUnit,
			Expected: "the absent families are ALL unregistered (regression lock)",
			Description: "a single guard enumerating M.4/M.5/M.7/M.15/M.16 + synthetic M.6 " +
				"pxf_errors_total asserting NONE are registered in the operator's Prometheus " +
				"registry (locks against future fabrication).",
		},
		{
			ID: "109-HONESTY-L", Req: "HONESTY", Layer: Scenario109LayerLive,
			Expected: "the absent families have ZERO series in VictoriaMetrics",
			Description: "[LIVE-ONLY] query VM for each Scenario109AbsentMetrics name and assert ZERO " +
				"samples — a NOT-present metric is a PASSING honesty check.",
		},
		{
			ID: "109-VM-L", Req: "VM", Layer: Scenario109LayerLive,
			Expected: "VictoriaMetrics reachable + the implemented families scrape in",
			Description: "[LIVE-ONLY] after inducing data-loading activity, the implemented data-loading " +
				"families (jobs_active/rows/errors/duration/last_success/status + pxf_service_up) " +
				"appear in VM. SKIP cleanly when VM/KUBECONFIG absent.",
		},
	}
}
