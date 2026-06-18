package cases

// ============================================================================
// Scenario 107 — All Data-Loading API Endpoints (P.1–P.15)
// ============================================================================
//
// Acceptance scenario: every data-loading REST endpoint is FULL (no 501 stubs
// other than the honest P.14 "no clientset" path). The catalog enumerates each
// endpoint's happy-path side effect + the negative/edge contract + the RBAC
// matrix + the cross-cutting honesty invariants, mirroring the Scenario 106
// catalog SHAPE (a stable ID, a Req family, a resolution Layer, an Expected
// token, an honesty-naming Description).
//
// Endpoints (all FULL):
//   P.1  GET    pxf/status                       Basic
//   P.2  GET    pxf/servers                      Basic
//   P.3  POST   pxf/servers                      Operator (201 + rendered config)
//   P.4  PUT    pxf/servers/{server}             Operator
//   P.5  DELETE pxf/servers/{server}             Admin    (409 SERVER_IN_USE)
//   P.6  POST   pxf/sync                         Operator (202)
//   P.7  GET    jobs                             Basic
//   P.8  POST   jobs                             Operator (201)
//   P.9  GET    jobs/{job}                       Basic
//   P.10 PUT    jobs/{job}                       Operator
//   P.11 DELETE jobs/{job}                       Admin
//   P.12 POST   jobs/{job}/start                 Operator (202, real Job)
//   P.13 POST   jobs/{job}/stop                  Operator (202/200)
//   P.14 GET    jobs/{job}/logs                  Basic    (501 no-clientset / stream)
//   P.15 GET    external-tables                  Basic    (observed/expected honesty)
//
// HONESTY INVARIANTS (enforced by every -F/-L row that touches them):
//   - P.3/P.4/P.5 surface the new server's RENDERED "<server>__*.xml" keys via the
//     SAME builder the controller uses; a real ConfigMap Data diff fires the
//     honest cloudberry_pxf_servers_changed_total signal EXACTLY once (the shared
//     util.DiffPXFServerNames helper), never on a no-op.
//   - P.5 mirrors webhook W.9: a server still referenced by a job → 409
//     SERVER_IN_USE listing the referencing jobs, NO mutation.
//   - P.12 creates a REAL batchv1.Job (util.DataLoadJobName); it records NO rows
//     metric at start (rows are harvested at Job COMPLETION).
//   - P.13 deletes the real Job (Background propagation) and is an honest 200
//     no-op when nothing is running.
//   - P.14 returns 501 LOGS_NOT_AVAILABLE when no clientset is configured (never
//     fabricated logs); otherwise streams REAL pod logs.
//   - P.15 splits DB-observed tables (Observed, ABSENT/observedAvailable=false
//     when unreachable — NEVER synthesized) from spec-derived Expected tables
//     (clearly labeled, never claimed to "exist").
// ============================================================================

// Scenario107Layer enumerates the assertion layer a Scenario 107 case resolves
// at, sharing the Scenario 104/105/106 vocabulary ("builder"/"reconcile"/"live").
const (
	// Scenario107LayerFunctional is a functional/router-level case resolved over
	// the REAL api.Server router + auth/RBAC middleware + a fake k8s client +
	// MockDBClient (infra-free, deterministic).
	Scenario107LayerFunctional = Scenario104LayerReconcile
	// Scenario107LayerBuilder is a pure builder/byte-provable case (offline).
	Scenario107LayerBuilder = Scenario104LayerBuilder
	// Scenario107LayerLive requires a deployed operator + real DB/PXF (live-only).
	Scenario107LayerLive = Scenario104LayerLive
)

// Scenario 107 well-known names + namespace (mirror the live e2e defaults so the
// Part A catalog and the live Part B agree).
const (
	// Scenario107Namespace is the default deploy namespace for the live rows.
	Scenario107Namespace = "cloudberry-test"
	// Scenario107DefaultCluster is the default (SHORT) live cluster name.
	Scenario107DefaultCluster = "s107"
	// Scenario107ServersChangedMetric is the honest applied-change counter
	// incremented EXACTLY once per real PXF servers ConfigMap Data diff.
	Scenario107ServersChangedMetric = "cloudberry_pxf_servers_changed_total"
)

// Scenario107Case describes one Scenario 107 sub-case. It mirrors the
// Scenario106Case SHAPE: a flat catalog row carrying an ID + the endpoint
// requirement family + the resolution Layer + a human Expected token + a
// Description that names the HONEST signal and marks [LIVE-ONLY] where the only
// honest proof is a running cluster.
type Scenario107Case struct {
	// ID is the catalog rule id (e.g. "107-P3-F", "107-P5-409", "107-RBAC").
	ID string
	// Req is the endpoint family the row proves: "P.1".."P.15", "RBAC" or "MX".
	Req string
	// Layer is the assertion layer: Scenario107LayerFunctional,
	// Scenario107LayerBuilder or Scenario107LayerLive.
	Layer string
	// Expected is a short outcome token / human description of the asserted
	// outcome.
	Expected string
	// Description explains the case and names the HONEST signal. Live-only rows
	// are marked [LIVE-ONLY].
	Description string
}

// Scenario107Cases returns the full Scenario 107 catalog (task-breakdown §2):
// the per-endpoint happy-path + side-effect rows (-F/-B/-L), the edge/negative
// rows, the RBAC matrix row and the cross-cutting honesty rows. The -F rows are
// resolved over the real router in the functional suite; the -L rows require the
// deployed operator + a real DB/PXF and are resolved (or SKIP cleanly) in the
// e2e suite.
//
//nolint:funlen // a flat exhaustive catalog of 15 endpoints + edges is one table.
func Scenario107Cases() []Scenario107Case {
	cases := []Scenario107Case{}
	cases = append(cases, scenario107HappyPathCases()...)
	cases = append(cases, scenario107EdgeCases()...)
	cases = append(cases, scenario107CrossCuttingCases()...)
	return cases
}

// scenario107HappyPathCases returns the per-endpoint happy-path + side-effect
// rows (functional + builder + live).
//
//nolint:funlen // exhaustive per-endpoint happy-path table.
func scenario107HappyPathCases() []Scenario107Case {
	return []Scenario107Case{
		{
			ID: "107-P1-F", Req: "P.1", Layer: Scenario107LayerFunctional,
			Expected: "200 honest pxf status from real pod readiness",
			Description: "GET pxf/status → 200 {servers,configured,readySidecars,totalSidecars}; " +
				"readiness derived ONLY from the seeded segment-primary pods' pxf ContainerStatuses.",
		},
		{
			ID: "107-P1-L", Req: "P.1", Layer: Scenario107LayerLive,
			Expected: "LIVE pxf status",
			Description: "[LIVE-ONLY] GET pxf/status against the deployed cluster reports live " +
				"sidecar readiness. SKIP cleanly when KUBECONFIG/live env absent.",
		},
		{
			ID: "107-P2-F", Req: "P.2", Layer: Scenario107LayerFunctional,
			Expected: "200 servers list equals spec",
			Description: "GET pxf/servers → 200 {servers,total} equals spec.dataLoading.pxf.servers " +
				"(credential secret REFERENCES only, never literal values).",
		},
		{
			ID: "107-P2-L", Req: "P.2", Layer: Scenario107LayerLive,
			Expected:    "LIVE servers list matches CR",
			Description: "[LIVE-ONLY] GET pxf/servers against the live operator API matches the CR.",
		},
		{
			ID: "107-P3-F", Req: "P.3", Layer: Scenario107LayerFunctional,
			Expected: "201 + rendered keys; CR gains the server",
			Description: "POST pxf/servers {name,type,config} → 201; response renderedKeys carry " +
				"the NEW server's <server>__*.xml keys; re-GET the CR shows the new server.",
		},
		{
			ID: "107-P3-B", Req: "P.3", Layer: Scenario107LayerBuilder,
			Expected: "builder emits exactly the new server's keys",
			Description: "BuildPXFServersConfigMap over the augmented spec emits the new server's " +
				"<server>__*.xml keys (pure builder proof).",
		},
		{
			ID: "107-P3-L", Req: "P.3", Layer: Scenario107LayerLive,
			Expected: "LIVE 201 + CM regenerates with new server",
			Description: "[LIVE-ONLY] POST a throwaway server against the live API → 201 + rendered " +
				"config; it appears in the CR and the <cluster>-pxf-servers ConfigMap regenerates.",
		},
		{
			ID: "107-P4-F", Req: "P.4", Layer: Scenario107LayerFunctional,
			Expected: "200; surgical update reflected in the CR",
			Description: "PUT pxf/servers/{server} {config} → 200; re-GET shows the NEW config; " +
				"the named server only is mutated (others untouched).",
		},
		{
			ID: "107-P4-L", Req: "P.4", Layer: Scenario107LayerLive,
			Expected: "LIVE update re-renders the changed key",
			Description: "[LIVE-ONLY] PUT pxf/servers/{server} against the live API re-renders " +
				"only that server's key.",
		},
		{
			ID: "107-P5-F", Req: "P.5", Layer: Scenario107LayerFunctional,
			Expected: "200; unreferenced server removed from the CR",
			Description: "DELETE pxf/servers/{server} (unreferenced) → 200; the server is removed " +
				"from the CR (re-GET) and its keys vanish on the next render.",
		},
		{
			ID: "107-P5-L", Req: "P.5", Layer: Scenario107LayerLive,
			Expected: "LIVE delete removes server keys",
			Description: "[LIVE-ONLY] DELETE a throwaway server against the live API → removed " +
				"from the CR + the CM keys gone.",
		},
		{
			ID: "107-P6-F", Req: "P.6", Layer: Scenario107LayerFunctional,
			Expected: "202 sync re-renders the CM + bumps the STS",
			Description: "POST pxf/sync → 202 {synced,configMap}; the <cluster>-pxf-servers CM is " +
				"(re)created and the segment-primary restart-trigger is bumped.",
		},
		{
			ID: "107-P6-L", Req: "P.6", Layer: Scenario107LayerLive,
			Expected:    "LIVE sync refresh",
			Description: "[LIVE-ONLY] POST pxf/sync against the live API → 202; CM refresh.",
		},
		{
			ID: "107-P7-F", Req: "P.7", Layer: Scenario107LayerFunctional,
			Expected:    "200 jobs list equals spec",
			Description: "GET jobs → 200 {jobs,total} equals spec.dataLoading.jobs.",
		},
		{
			ID: "107-P7-L", Req: "P.7", Layer: Scenario107LayerLive,
			Expected:    "LIVE jobs list matches CR",
			Description: "[LIVE-ONLY] GET jobs against the live API matches the CR.",
		},
		{
			ID: "107-P8-F", Req: "P.8", Layer: Scenario107LayerFunctional,
			Expected:    "201; CR gains the job",
			Description: "POST jobs {name,type:pxf,pxfJob} → 201; re-GET shows the job.",
		},
		{
			ID: "107-P8-L", Req: "P.8", Layer: Scenario107LayerLive,
			Expected:    "LIVE 201; CR gains the job",
			Description: "[LIVE-ONLY] POST a throwaway job against the live API → 201; the CR gains it.",
		},
		{
			ID: "107-P9-F", Req: "P.9", Layer: Scenario107LayerFunctional,
			Expected:    "200 the created job spec",
			Description: "GET jobs/{job} → 200 the job spec; matches what P.8 created.",
		},
		{
			ID: "107-P9-L", Req: "P.9", Layer: Scenario107LayerLive,
			Expected:    "LIVE get",
			Description: "[LIVE-ONLY] GET jobs/{job} against the live API.",
		},
		{
			ID: "107-P10-F", Req: "P.10", Layer: Scenario107LayerFunctional,
			Expected:    "200; re-GET reflects the update",
			Description: "PUT jobs/{job} {enabled,schedule} → 200; re-GET reflects the update.",
		},
		{
			ID: "107-P10-L", Req: "P.10", Layer: Scenario107LayerLive,
			Expected:    "LIVE schedule update",
			Description: "[LIVE-ONLY] PUT jobs/{job} against the live API updates the schedule.",
		},
		{
			ID: "107-P11-F", Req: "P.11", Layer: Scenario107LayerFunctional,
			Expected:    "200; re-GET 404s the job",
			Description: "DELETE jobs/{job} → 200; re-GET 404s the job; the spec slice shrinks.",
		},
		{
			ID: "107-P11-L", Req: "P.11", Layer: Scenario107LayerLive,
			Expected:    "LIVE delete; owned Job GC'd",
			Description: "[LIVE-ONLY] DELETE jobs/{job} against the live API; owned Job/CronJob GC'd.",
		},
		{
			ID: "107-P12-F", Req: "P.12", Layer: Scenario107LayerFunctional,
			Expected: "202; real batchv1.Job created; NO rows metric",
			Description: "POST jobs/{job}/start → 202; a real batchv1.Job named " +
				"DataLoadJobName(cluster,job) exists in the fake client; NO rows metric recorded.",
		},
		{
			ID: "107-P12-L", Req: "P.12", Layer: Scenario107LayerLive,
			Expected: "LIVE one-off Job runs",
			Description: "[LIVE-ONLY] POST jobs/{job}/start against the live API → 202; the " +
				"data-loading Job object is created.",
		},
		{
			ID: "107-P13-F", Req: "P.13", Layer: Scenario107LayerFunctional,
			Expected: "202; the batchv1.Job is deleted",
			Description: "POST jobs/{job}/stop → 202 {stopped:true}; the batchv1.Job is deleted " +
				"(fake client Get → NotFound).",
		},
		{
			ID: "107-P13-L", Req: "P.13", Layer: Scenario107LayerLive,
			Expected:    "LIVE Job reaped",
			Description: "[LIVE-ONLY] POST jobs/{job}/stop against the live API → Job + pods reaped.",
		},
		{
			ID: "107-P14-F", Req: "P.14", Layer: Scenario107LayerFunctional,
			Expected: "streams REAL pod logs via the fake clientset",
			Description: "GET jobs/{job}/logs with a clientset-backed pod streams REAL logs " +
				"(uses the k8s Job name util.DataLoadJobName for pod correlation).",
		},
		{
			ID: "107-P14-L", Req: "P.14", Layer: Scenario107LayerLive,
			Expected:    "LIVE pod log stream",
			Description: "[LIVE-ONLY] GET jobs/{job}/logs against the live API streams the Job pod logs.",
		},
		{
			ID: "107-P15-F", Req: "P.15", Layer: Scenario107LayerFunctional,
			Expected: "observed populated + observedAvailable true; expected labeled",
			Description: "GET external-tables → 200; observed = MockDBClient.ListExternalTables rows " +
				"+ observedAvailable true; expected derived from spec pxf jobs (foreign_<job>/target).",
		},
		{
			ID: "107-P15-B", Req: "P.15", Layer: Scenario107LayerBuilder,
			Expected: "expected[].foreignTable == ForeignTableName(job)",
			Description: "for each fdw pxf job, the expected table name equals builder." +
				"ForeignTableName(job) (pure builder/derivation proof).",
		},
		{
			ID: "107-P15-L", Req: "P.15", Layer: Scenario107LayerLive,
			Expected: "LIVE observed reflects the live DB; expected labeled",
			Description: "[LIVE-ONLY] GET external-tables against the live API; observed reflects " +
				"the live catalog (or is honestly ABSENT); expected lists foreign_<job>/target tables.",
		},
	}
}

// scenario107EdgeCases returns the edge/negative rows.
//
//nolint:funlen // exhaustive negative table.
func scenario107EdgeCases() []Scenario107Case {
	return []Scenario107Case{
		{
			ID: "107-P2-404", Req: "P.2", Layer: Scenario107LayerFunctional,
			Expected: "404 CLUSTER_NOT_FOUND", Description: "GET pxf/servers, missing cluster → 404.",
		},
		{
			ID: "107-P3-404", Req: "P.3", Layer: Scenario107LayerFunctional,
			Expected:    "404 before mutation",
			Description: "POST pxf/servers, missing cluster → 404 (no mutation).",
		},
		{
			ID: "107-P3-409", Req: "P.3", Layer: Scenario107LayerFunctional,
			Expected:    "409 SERVER_EXISTS; CR unchanged",
			Description: "POST pxf/servers, duplicate name → 409 SERVER_EXISTS; CR unchanged.",
		},
		{
			ID: "107-P3-400", Req: "P.3", Layer: Scenario107LayerFunctional,
			Expected:    "400 PXF_NOT_ENABLED",
			Description: "POST pxf/servers on a PXF-disabled cluster → 400 PXF_NOT_ENABLED.",
		},
		{
			ID: "107-P4-404S", Req: "P.4", Layer: Scenario107LayerFunctional,
			Expected:    "404 SERVER_NOT_FOUND; CR unchanged",
			Description: "PUT pxf/servers/{server}, unknown server → 404 SERVER_NOT_FOUND; CR unchanged.",
		},
		{
			ID: "107-P5-404", Req: "P.5", Layer: Scenario107LayerFunctional,
			Expected:    "404 SERVER_NOT_FOUND",
			Description: "DELETE pxf/servers/{server}, unknown server → 404 SERVER_NOT_FOUND.",
		},
		{
			ID: "107-P5-409", Req: "P.5", Layer: Scenario107LayerFunctional,
			Expected: "409 SERVER_IN_USE listing referencing jobs; NO mutation",
			Description: "DELETE a server still referenced by a job → 409 SERVER_IN_USE listing the " +
				"referencing job(s); NO mutation (mirrors webhook W.9).",
		},
		{
			ID: "107-P5-409L", Req: "P.5", Layer: Scenario107LayerLive,
			Expected:    "LIVE 409 on referenced server delete",
			Description: "[LIVE-ONLY] DELETE a referenced server against the live API → 409; CR/CM unchanged.",
		},
		{
			ID: "107-P8-409", Req: "P.8", Layer: Scenario107LayerFunctional,
			Expected:    "409 JOB_EXISTS; CR unchanged",
			Description: "POST jobs, duplicate job name → 409 JOB_EXISTS; CR unchanged.",
		},
		{
			ID: "107-P8-400", Req: "P.8", Layer: Scenario107LayerFunctional,
			Expected:    "400 INVALID_REQUEST; CR unchanged",
			Description: "POST jobs, bad type / unknown referenced server → 400 INVALID_REQUEST; CR unchanged.",
		},
		{
			ID: "107-P9-404", Req: "P.9", Layer: Scenario107LayerFunctional,
			Expected: "404 JOB_NOT_FOUND", Description: "GET jobs/{job}, unknown job → 404 JOB_NOT_FOUND.",
		},
		{
			ID: "107-P10-404", Req: "P.10", Layer: Scenario107LayerFunctional,
			Expected:    "404 JOB_NOT_FOUND; CR unchanged",
			Description: "PUT jobs/{job}, unknown job → 404 JOB_NOT_FOUND; CR unchanged.",
		},
		{
			ID: "107-P11-404", Req: "P.11", Layer: Scenario107LayerFunctional,
			Expected: "404 JOB_NOT_FOUND", Description: "DELETE jobs/{job}, unknown job → 404 JOB_NOT_FOUND.",
		},
		{
			ID: "107-P12-404", Req: "P.12", Layer: Scenario107LayerFunctional,
			Expected:    "404; no Job created",
			Description: "start unknown job → 404 JOB_NOT_FOUND; no batchv1.Job created.",
		},
		{
			ID: "107-P12-409", Req: "P.12", Layer: Scenario107LayerFunctional,
			Expected:    "409 JOB_ALREADY_RUNNING",
			Description: "start when a Job of that name already exists → 409 JOB_ALREADY_RUNNING.",
		},
		{
			ID: "107-P13-NOOP", Req: "P.13", Layer: Scenario107LayerFunctional,
			Expected:    "200 honest no-op",
			Description: "stop when no Job exists → 200 {stopped:false} honest idempotent no-op.",
		},
		{
			ID: "107-P14-501", Req: "P.14", Layer: Scenario107LayerFunctional,
			Expected: "501 LOGS_NOT_AVAILABLE (honesty)",
			Description: "GET jobs/{job}/logs with s.clientset == nil → 501 LOGS_NOT_AVAILABLE " +
				"(no fabricated logs).",
		},
		{
			ID: "107-P14-404", Req: "P.14", Layer: Scenario107LayerFunctional,
			Expected:    "404 JOB_NOT_FOUND",
			Description: "logs with a clientset but no backing pod → 404 JOB_NOT_FOUND.",
		},
		{
			ID: "107-P15-EMPTY", Req: "P.15", Layer: Scenario107LayerFunctional,
			Expected: "observed null + observedAvailable false; expected present",
			Description: "external-tables with no db factory → observed null + observedAvailable " +
				"false; expected still present (NOTHING synthesized as 'exists').",
		},
		{
			ID: "107-P15-DBERR", Req: "P.15", Layer: Scenario107LayerFunctional,
			Expected: "observed ABSENT on DB error; expected labeled",
			Description: "external-tables, ListExternalTables errors → observed ABSENT (not 'none'); " +
				"expected labeled and present; honest.",
		},
	}
}

// scenario107CrossCuttingCases returns the RBAC matrix + honesty rows.
func scenario107CrossCuttingCases() []Scenario107Case {
	return []Scenario107Case{
		{
			ID: "107-RBAC", Req: "RBAC", Layer: Scenario107LayerFunctional,
			Expected: "403 below the required tier; allowed at/above",
			Description: "For each endpoint a caller below the required tier → 403: Basic for " +
				"status/list/get/logs/external-tables (P.1,P.2,P.7,P.9,P.14,P.15); Operator for " +
				"create/update/start/stop/sync (P.3,P.4,P.6,P.8,P.10,P.12,P.13); Admin for delete " +
				"(P.5,P.11). At/above the required tier the permission gate passes.",
		},
		{
			ID: "107-MX-F1", Req: "MX", Layer: Scenario107LayerFunctional,
			Expected: "P.3/P.4/P.5 fire servers-changed EXACTLY once on a real diff",
			Description: "a server create/update/delete that changes the rendered ConfigMap Data " +
				"fires cloudberry_pxf_servers_changed_total EXACTLY once (real diff via " +
				"util.DiffPXFServerNames).",
		},
		{
			ID: "107-MX-F3", Req: "MX", Layer: Scenario107LayerFunctional,
			Expected: "P.12 start records NO rows metric",
			Description: "POST jobs/{job}/start records NO cloudberry_data_loading_rows_total " +
				"(rows only on Job completion) — start-time honesty.",
		},
	}
}
