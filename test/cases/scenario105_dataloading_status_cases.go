package cases

// ============================================================================
// Scenario 105 — DataLoadingStatus PXF Fields (S.1–S.5)
// ============================================================================
//
// Acceptance scenario (verbatim): "With PXF running and several jobs configured,
// verify status.dataLoading.pxf.status: Running; stop PXF on a segment → status
// reflects Error/Stopped. pxf.servers equals the count of configured server
// definitions (S.2). pxf.extensionsInstalled lists pxf and pxf_fdw (S.3). Run
// jobs concurrently → activeJobs matches (S.4). After job runs, each jobs[] entry
// has name, lastRun, lastStatus, rowsLoaded, duration populated correctly (S.5)."
//
// HONESTY INVARIANT (project-wide, enforced by every row below):
//   - pxf.status derives ONLY from the real segment-primary "pxf" container
//     readiness (ContainerStatuses). Never exec, never HTTP-probe, never
//     synthesize. The mapping (locked in util.PXFStatusFromReadiness) is:
//       total==0/unobservable → ABSENT(""); ready==total>0 → "Running";
//       0<ready<total → "Error"; ready==0 && total>0 → "Stopped".
//   - pxf.extensionsInstalled derives ONLY from a real pg_extension probe; it is
//     ABSENT (nil) when the DB is unreachable OR no PXF extensions are installed
//     (an empty array is NEVER synthesized).
//   - rowsLoaded is NEVER synthesized: it is present only on a succeeded Job that
//     carried a harvested DATALOAD_ROWS marker.
//
// The -B (builder/unit-provable) rows are deterministic/offline and are resolved
// in the functional + integration suites over fakes/envtest + the shared
// util.PXF* helpers. The -L (live/e2e) rows require the deployed operator + a
// real (or staged) PXF/DB and are resolved (or SKIP cleanly) in the e2e suite.
// ============================================================================

// Scenario105Layer enumerates the assertion layer a Scenario 105 case resolves
// at, sharing the Scenario 104 vocabulary ("builder"/"reconcile"/"live").
const (
	// Scenario105LayerBuilder is a pure, byte/logic-provable case resolved over
	// fakes/envtest + the shared util.PXF* helpers (offline, deterministic).
	Scenario105LayerBuilder = Scenario104LayerBuilder
	// Scenario105LayerReconcile is an envtest/controller-level case (the
	// reconcilePxf / patchDataLoadingStatus paths over a fake client).
	Scenario105LayerReconcile = Scenario104LayerReconcile
	// Scenario105LayerLive requires a running cluster (live-only).
	Scenario105LayerLive = Scenario104LayerLive
)

// Scenario 105 well-known names + values (mirror the production honesty contract
// in internal/util/pxf.go, the api/v1alpha1 DataLoadingPxfStatus type, and the
// reconcilePxf / patchDataLoadingStatus paths).
const (
	// Scenario105Namespace is the deploy namespace for the live (-L) rows.
	Scenario105Namespace = "cloudberry-test"
	// Scenario105PxfContainerName is the segment-primary PXF sidecar container
	// whose readiness is the SOLE source of truth for pxf.status (S.1).
	Scenario105PxfContainerName = "pxf"
	// Scenario105StatusRunning/Error/Stopped are the honest, observed pxf.status
	// values (an UNOBSERVABLE state is the empty string / absent).
	Scenario105StatusRunning = "Running"
	Scenario105StatusError   = "Error"
	Scenario105StatusStopped = "Stopped"
	// Scenario105ExtensionPxf / Scenario105ExtensionPxfFdw are the two PXF client
	// extensions pxf.extensionsInstalled reports from a real pg_extension probe.
	Scenario105ExtensionPxf    = "pxf"
	Scenario105ExtensionPxfFdw = "pxf_fdw"
	// Scenario105StatusMetric is the honest observed-status gauge (0=Stopped,
	// 1=Running, 2=Error); emitted ONLY when the status is observable.
	Scenario105StatusMetric = "cloudberry_pxf_status"
	// Scenario105ExtensionsMetric is the observed-extensions-count gauge; emitted
	// ONLY when the pg_extension probe is observable.
	Scenario105ExtensionsMetric = "cloudberry_pxf_extensions_installed"
)

// Scenario105Case describes one Scenario 105 sub-case. It mirrors the
// Scenario104Case SHAPE (a small, flat catalog row carrying an ID + the spec
// requirement family + the resolution Layer + a human Expected token + a
// Description that names the HONEST signal and marks [LIVE-ONLY] where the only
// honest proof is a running cluster).
type Scenario105Case struct {
	// ID is the catalog rule id (e.g. "105-S1-B1", "105-S1-L2").
	ID string
	// Req is the spec requirement family the row proves: "S.1" (pxf.status),
	// "S.2" (pxf.servers), "S.3" (pxf.extensionsInstalled), "S.4" (activeJobs),
	// "S.5" (jobs[] runtime fields) or "MX" (honest metrics).
	Req string
	// Layer is the assertion layer: Scenario105LayerBuilder (pure),
	// Scenario105LayerReconcile (envtest) or Scenario105LayerLive (live-only).
	Layer string
	// Expected is a short outcome token / human description of the asserted
	// outcome (e.g. "all pxf containers ready → status==Running").
	Expected string
	// Description explains the case and names the HONEST signal. Live-only rows
	// are marked [LIVE-ONLY]; rows that may degrade to config-only carry
	// [CONFIG-ONLY-IF-...].
	Description string
}

// Scenario105Cases returns the full Scenario 105 catalog (task-breakdown §4):
// the per-requirement builder rows (-B), and the live fail+restore / observation
// rows (-L). Builder rows are resolved against the shared util.PXF* helpers + a
// fake/envtest client in the functional + integration suites; rows whose only
// honest signal is a running cluster carry Layer "live" (marked [LIVE-ONLY]) and
// are resolved (or SKIP cleanly) in the e2e suite.
func Scenario105Cases() []Scenario105Case {
	return []Scenario105Case{
		// --- S.1 — pxf.status (health from real ContainerStatuses) -----------
		{
			ID: "105-S1-B1", Req: "S.1", Layer: Scenario105LayerReconcile,
			Expected: "all segment-primary pxf containers Ready → status==Running",
			Description: "feed a fake PodList where every segment-primary 'pxf' " +
				"ContainerStatus.Ready=true → reconcilePxf stamps status==\"Running\" " +
				"(derived ONLY from real readiness via util.PXFStatusFromReadiness).",
		},
		{
			ID: "105-S1-B2", Req: "S.1", Layer: Scenario105LayerReconcile,
			Expected: "KEY TRANSITION: 0<ready<total → status==Error",
			Description: "one segment's 'pxf' container not-Ready while others Ready " +
				"(partial outage) → status==\"Error\" (degraded). The KEY 'stop PXF on a " +
				"segment → Error' transition, proven from real ContainerStatuses.",
		},
		{
			ID: "105-S1-B3", Req: "S.1", Layer: Scenario105LayerReconcile,
			Expected: "KEY TRANSITION: all pxf containers not-Ready (total>0) → status==Stopped",
			Description: "every segment-primary 'pxf' container not-Ready (total>0, " +
				"ready==0) → status==\"Stopped\". The full-outage transition.",
		},
		{
			ID: "105-S1-B4", Req: "S.1", Layer: Scenario105LayerReconcile,
			Expected: "no segment-primary pods (or no pxf ContainerStatus) → status ABSENT",
			Description: "zero segment-primary pods → status ABSENT (\"\"), NOT " +
				"\"Running\"/\"Stopped\" — honesty (unobservable). A pod-list error is " +
				"NON-FATAL and likewise leaves status absent.",
		},
		{
			ID: "105-S1-L1", Req: "S.1", Layer: Scenario105LayerLive,
			Expected: "LIVE happy path: PXF running → pxf.status==Running, ready==total>0",
			Description: "[LIVE-ONLY] with PXF running on every segment, GET pxf/status " +
				"(or read the CR status) → status==\"Running\" and readySidecars==totalSidecars>0.",
		},
		{
			ID: "105-S1-L2", Req: "S.1", Layer: Scenario105LayerLive,
			Expected: "LIVE KEY TRANSITION: stop PXF on ONE segment → Error/Stopped; restore → Running",
			Description: "[LIVE-ONLY][CONFIG-ONLY-IF-PXF-STOP-NOT-REPRODUCIBLE] stop PXF on " +
				"ONE segment (kubectl exec <segpod> -c pxf -- pxf stop, the scenario104 " +
				"mechanism) → pxf.status transitions to \"Error\" (degraded; \"Stopped\" if a " +
				"single segment); RESTORE → returns to \"Running\". Generous eventually-timeouts " +
				"(readiness probe periods ~10–30s).",
		},
		{
			ID: "105-S1-L3", Req: "S.1", Layer: Scenario105LayerLive,
			Expected: "LIVE honesty on a non-pxf image → status NOT forced to Running",
			Description: "[LIVE-ONLY] on cloudberry-official (no real pxf agent) status is NOT " +
				"forced to \"Running\" — asserts config-only / absent health honestly.",
		},

		// --- S.2 — pxf.servers == count of configured server definitions ------
		{
			ID: "105-S2-B1", Req: "S.2", Layer: Scenario105LayerReconcile,
			Expected: "N server definitions → status.dataLoading.pxf.servers==N",
			Description: "N pxf server definitions in spec → reconcilePxf sets " +
				"pxf.servers==N (regression-pin; already implemented).",
		},
		{
			ID: "105-S2-B2", Req: "S.2", Layer: Scenario105LayerReconcile,
			Expected: "pxf.enabled with 0 servers → configured=true, servers=0",
			Description: "pxf enabled but zero servers → configured=true, servers=0 " +
				"(honest config count, not a live-reachable count).",
		},
		{
			ID: "105-S2-B3", Req: "S.2", Layer: Scenario105LayerReconcile,
			Expected: "servers survives the MergePatch (read-back == N)",
			Description: "the servers value survives patchDataLoadingStatus and reads back " +
				"== N on a fake status subresource client.",
		},
		{
			ID: "105-S2-L1", Req: "S.2", Layer: Scenario105LayerLive,
			Expected: "LIVE: K server definitions → CR status.pxf.servers==K",
			Description: "[LIVE-ONLY] deploy with K server definitions → the CR " +
				"status.dataLoading.pxf.servers reads back K.",
		},

		// --- S.3 — pxf.extensionsInstalled lists pxf and pxf_fdw -------------
		{
			ID: "105-S3-B1", Req: "S.3", Layer: Scenario105LayerReconcile,
			Expected: "pg_extension has both → extensionsInstalled==[pxf,pxf_fdw]",
			Description: "ListPXFExtensions returns [pxf,pxf_fdw] (deterministic order) → " +
				"extensionsInstalled==[pxf,pxf_fdw].",
		},
		{
			ID: "105-S3-B2", Req: "S.3", Layer: Scenario105LayerReconcile,
			Expected: "only pxf present → extensionsInstalled==[pxf] (honest subset)",
			Description: "ListPXFExtensions returns only [pxf] → extensionsInstalled==[pxf]; " +
				"never padded with a fabricated pxf_fdw.",
		},
		{
			ID: "105-S3-B3", Req: "S.3", Layer: Scenario105LayerReconcile,
			Expected: "DB reachable, none installed → extensionsInstalled ABSENT",
			Description: "a reachable DB with neither extension → extensionsInstalled ABSENT " +
				"(nil); an empty array is NEVER synthesized.",
		},
		{
			ID: "105-S3-B4", Req: "S.3", Layer: Scenario105LayerReconcile,
			Expected: "DB UNREACHABLE / query error → extensionsInstalled ABSENT, non-fatal",
			Description: "ListPXFExtensions errors (DB unreachable) → extensionsInstalled NIL " +
				"(absent) and reconcile still succeeds (best-effort, non-fatal).",
		},
		{
			ID: "105-S3-B5", Req: "S.3", Layer: Scenario105LayerReconcile,
			Expected: "patch emits extensionsInstalled only when non-nil (MergePatch leak guard)",
			Description: "patchDataLoadingStatus emits status.dataLoading.pxf.extensionsInstalled " +
				"ONLY when non-nil; absent extensions are OMITTED from the patch body.",
		},
		{
			ID: "105-S3-L1", Req: "S.3", Layer: Scenario105LayerLive,
			Expected: "LIVE pxf image → extensionsInstalled contains pxf and pxf_fdw",
			Description: "[LIVE-ONLY] on a real PXF image the pg_extension query → " +
				"status.pxf.extensionsInstalled lists pxf and pxf_fdw.",
		},
		{
			ID: "105-S3-L2", Req: "S.3", Layer: Scenario105LayerLive,
			Expected: "LIVE non-pxf image → extensionsInstalled ABSENT (honest)",
			Description: "[LIVE-ONLY] on cloudberry-official (extensions absent/stub) → " +
				"extensionsInstalled ABSENT (not synthesized) — honesty.",
		},

		// --- S.4 — activeJobs matches enabled/concurrent jobs ---------------
		{
			ID: "105-S4-B1", Req: "S.4", Layer: Scenario105LayerReconcile,
			Expected: "M jobs, K enabled → activeJobs==K, configuredJobs==M",
			Description: "reconcileDataLoading counts enabled jobs → activeJobs==K and " +
				"configuredJobs==M (concurrency does not change the enabled-count invariant).",
		},
		{
			ID: "105-S4-L1", Req: "S.4", Layer: Scenario105LayerLive,
			Expected: "LIVE: several enabled jobs run concurrently → activeJobs==enabled count",
			Description: "[LIVE-ONLY] run several enabled jobs CONCURRENTLY → " +
				"status.dataLoading.activeJobs equals the enabled-job count and is STABLE " +
				"across in-flight runs (activeJobs is enabled-count, not in-flight count); " +
				"dataLoadingJobs mirrors activeJobs.",
		},

		// --- S.5 — per-job jobs[] runtime fields populated correctly --------
		{
			ID: "105-S5-B1", Req: "S.5", Layer: Scenario105LayerReconcile,
			Expected: "Succeeded Job + DATALOAD_ROWS marker → name/lastRun/lastStatus/rowsLoaded/duration",
			Description: "a terminal Succeeded Job + a DATALOAD_ROWS marker pod → jobs[i] " +
				"carries name, lastRun (=startTime), lastStatus==\"Succeeded\", " +
				"rowsLoaded==marker and duration (start→completion), all harvested honestly.",
		},
		{
			ID: "105-S5-B2", Req: "S.5", Layer: Scenario105LayerReconcile,
			Expected: "Running/Pending Job → lastStatus set, rowsLoaded & duration ABSENT",
			Description: "a non-terminal Job → lastStatus set but rowsLoaded & duration " +
				"ABSENT (honest: not yet terminal / no marker).",
		},
		{
			ID: "105-S5-B3", Req: "S.5", Layer: Scenario105LayerReconcile,
			Expected: "Failed Job → lastStatus==Failed, rowsLoaded ABSENT (never synthesized)",
			Description: "a Failed Job → lastStatus==\"Failed\" and rowsLoaded ABSENT " +
				"(rows are never synthesized for a non-successful run).",
		},
		{
			ID: "105-S5-B4", Req: "S.5", Layer: Scenario105LayerReconcile,
			Expected: "Job never executed → jobs[i] has only name/enabled (no runtime fields)",
			Description: "a job with no observed Job → jobs[i] carries only name/enabled; " +
				"no lastRun/lastStatus/rowsLoaded/duration are present.",
		},
		{
			ID: "105-S5-L1", Req: "S.5", Layer: Scenario105LayerLive,
			Expected: "LIVE: after a real run, jobs[] entry shows correct runtime fields",
			Description: "[LIVE-ONLY] after a real job run, the CR jobs[] entry shows correct " +
				"name/lastRun/lastStatus/rowsLoaded/duration; rowsLoaded matches the rows " +
				"actually loaded (marker-harvested, not synthesized).",
		},

		// --- (cross-cutting) honest metrics --------------------------------
		{
			ID: "105-MX-B1", Req: "MX", Layer: Scenario105LayerReconcile,
			Expected: "cloudberry_pxf_status gauge recorded for observable states; NOT when absent",
			Description: "the honest cloudberry_pxf_status gauge is recorded (Running→1, " +
				"Error→2, Stopped→0) ONLY when the status is observable; when status is " +
				"ABSENT the gauge is NOT recorded (no series forced).",
		},
		{
			ID: "105-MX-B2", Req: "MX", Layer: Scenario105LayerReconcile,
			Expected: "cloudberry_pxf_extensions_installed == len(extensionsInstalled); not emitted when DB unreachable",
			Description: "the cloudberry_pxf_extensions_installed gauge equals " +
				"len(extensionsInstalled); it is NOT emitted when the DB is unreachable or no " +
				"extensions are observed.",
		},
	}
}
