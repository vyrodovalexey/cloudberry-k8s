package cases

// ============================================================================
// Scenario 108 — All CLI Commands (L.1–L.16)
// ============================================================================
//
// Acceptance scenario: the cloudberry-ctl (cobra) data-loading + pxf command
// surface is FULL and drives the Scenario 107 operator REST API 1:1. Scenario
// 108 is overwhelmingly CLI-only wiring on top of Scenario 107's FULL REST
// surface; exactly ONE item (L.15 test-read) adds new operator code (a REST
// endpoint + db.Client.ReadPXFSourceSample + fakes).
//
// The catalog mirrors the Scenario 107 SHAPE (a stable ID, a Req family, a
// resolution Layer, an Expected token, an honesty-naming Description). The -F
// rows are resolved by driving the cobra root command in-process against a
// recording httptest server (asserting the REAL HTTP method/path/body the CLI
// produced, never just an exit code). The -L rows require the deployed operator
// + a real DB/PXF and are resolved (or SKIP cleanly) in the e2e suite.
//
// CLI verbs (all FULL):
//   L.1  pxf status                       Basic    GET  pxf/status
//   L.2  pxf servers list                 Basic    GET  pxf/servers
//   L.3  pxf servers create               Operator POST pxf/servers (+ credential-secret)
//   L.4  pxf servers update <name>        Operator PUT  pxf/servers/{server}
//   L.5  pxf servers delete <name>        Admin    DELETE pxf/servers/{server}
//   L.6  pxf sync                         Operator POST pxf/sync (202)
//   L.7  pxf restart                      Operator POST pxf/restart (202; rolls pods)
//   L.8  data-loading jobs list           Basic    GET  jobs
//   L.9  jobs create --type pxf           Operator POST jobs (pxfJob DTO)
//   L.10 jobs start <job>                 Operator POST jobs/{job}/start (real Job)
//   L.11 jobs stop <job>                  Operator POST jobs/{job}/stop
//   L.12 jobs delete <job>                Admin    DELETE jobs/{job}
//   L.13 jobs logs --job                  Basic    GET  jobs/{job}/logs (stream / fallback)
//   L.14 jobs create --type gpload        Operator POST jobs (gploadJob DTO)
//   L.15 data-loading test-read --limit   Basic    GET  test-read (honest preview)
//   L.16 jobs create --from-yaml          Operator POST jobs (YAML → DTO)
//
// HONESTY INVARIANTS (enforced by the -F/-L rows that touch them):
//   - L.3-flags / L.9 / L.15-limit / L.16-yaml: a missing/invalid required input
//     is a CLEAN local usage error BEFORE any HTTP call (the recorder stays
//     empty) — never a half-built request.
//   - L.13-fallback: when the stream endpoint is unavailable the command does NOT
//     fail; it prints the kubectl fallback hint naming the k8s Job
//     (util.DataLoadJobName), never a fabricated log.
//   - L.15-absent: an available:false test-read renders cleanly and exits 0;
//     test-read records NO metric and NEVER fabricates rows. The backing db
//     read ALWAYS drops the transient external table (L.15-clean).
// ============================================================================

// Scenario108Layer enumerates the assertion layer a Scenario 108 case resolves
// at, sharing the Scenario 104/105/106/107 vocabulary.
const (
	// Scenario108LayerFunctional is a functional/CLI-level case resolved by
	// driving the cobra root command in-process against a recording httptest
	// operator stand-in (infra-free, deterministic).
	Scenario108LayerFunctional = Scenario104LayerReconcile
	// Scenario108LayerBuilder is a pure builder/byte-provable case (offline) —
	// e.g. the L.15 backing db read's transient-table cleanup contract.
	Scenario108LayerBuilder = Scenario104LayerBuilder
	// Scenario108LayerLive requires a deployed operator + real DB/PXF (live-only).
	Scenario108LayerLive = Scenario104LayerLive
)

// Scenario 108 well-known names + namespace (mirror the live e2e defaults so the
// Part A catalog and the live Part B agree).
const (
	// Scenario108Namespace is the default deploy namespace for the live rows.
	Scenario108Namespace = "cloudberry-test"
	// Scenario108DefaultCluster is the default (SHORT) live cluster name.
	Scenario108DefaultCluster = "s108"
)

// Scenario108Case describes one Scenario 108 sub-case. It mirrors the
// Scenario107Case SHAPE: a flat catalog row carrying an ID + the CLI command
// requirement family + the resolution Layer + a human Expected token + a
// Description that names the asserted effect. Live-only rows are marked
// [LIVE-ONLY].
type Scenario108Case struct {
	// ID is the catalog rule id (e.g. "108-L3-F", "108-L3-flags", "108-RBAC").
	ID string
	// Req is the CLI command family the row proves: "L.1".."L.16", "RBAC".
	Req string
	// Layer is the assertion layer: Scenario108LayerFunctional,
	// Scenario108LayerBuilder or Scenario108LayerLive.
	Layer string
	// Expected is a short outcome token / human description of the asserted
	// outcome.
	Expected string
	// Description explains the case and names the asserted effect. Live-only rows
	// are marked [LIVE-ONLY].
	Description string
}

// Scenario108Cases returns the full Scenario 108 catalog (task-breakdown §2):
// the per-command happy-path + side-effect rows (-F/-L), the named edge rows and
// the RBAC row. The -F rows are resolved by driving the CLI in-process; the -L
// rows require the deployed operator and are resolved (or SKIP cleanly) in e2e.
func Scenario108Cases() []Scenario108Case {
	cases := []Scenario108Case{}
	cases = append(cases, scenario108HappyPathCases()...)
	cases = append(cases, scenario108EdgeCases()...)
	cases = append(cases, scenario108CrossCuttingCases()...)
	return cases
}

// scenario108HappyPathCases returns the per-command happy-path + side-effect
// rows (functional + live), one -F and one -L per L.1–L.16.
//
//nolint:funlen // an exhaustive per-command happy-path table of 16 verbs.
func scenario108HappyPathCases() []Scenario108Case {
	return []Scenario108Case{
		{
			ID: "108-L1-F", Req: "L.1", Layer: Scenario108LayerFunctional,
			Expected: "pxf status → GET pxf/status",
			Description: "cloudberry-ctl pxf status issues GET .../data-loading/pxf/status; " +
				"honest sidecar readiness rendered (read, no mutation).",
		},
		{
			ID: "108-L1-L", Req: "L.1", Layer: Scenario108LayerLive,
			Expected: "LIVE pxf status",
			Description: "[LIVE-ONLY] pxf status against the deployed cluster reports live " +
				"sidecar readiness. SKIP cleanly when KUBECONFIG/live env absent.",
		},
		{
			ID: "108-L2-F", Req: "L.2", Layer: Scenario108LayerFunctional,
			Expected: "pxf servers list → GET pxf/servers",
			Description: "cloudberry-ctl pxf servers list issues GET .../data-loading/pxf/servers " +
				"on the namespaced cluster path.",
		},
		{
			ID: "108-L2-L", Req: "L.2", Layer: Scenario108LayerLive,
			Expected:    "LIVE servers list matches CR",
			Description: "[LIVE-ONLY] pxf servers list against the live operator API matches the CR.",
		},
		{
			ID: "108-L3-F", Req: "L.3", Layer: Scenario108LayerFunctional,
			Expected: "POST pxf/servers with name/type/config/credentialSecrets",
			Description: "pxf servers create --name --type --endpoint --bucket --credential-secret " +
				"POSTs a body whose config{} carries the endpoint/bucket and credentialSecrets[] " +
				"the secret refs (s3 server gains it in the CR).",
		},
		{
			ID: "108-L3-L", Req: "L.3", Layer: Scenario108LayerLive,
			Expected: "LIVE create throwaway s3 server (with --credential-secret)",
			Description: "[LIVE-ONLY] pxf servers create a throwaway s3 server against the live API → " +
				"201; CR + <cluster>-pxf-servers ConfigMap regenerate (webhook requires credentialSecrets).",
		},
		{
			ID: "108-L4-F", Req: "L.4", Layer: Scenario108LayerFunctional,
			Expected: "PUT pxf/servers/{server} with the new endpoint",
			Description: "pxf servers update <name> --endpoint PUTs to .../pxf/servers/<name>; " +
				"config carries the new endpoint (only that server mutated).",
		},
		{
			ID: "108-L4-L", Req: "L.4", Layer: Scenario108LayerLive,
			Expected:    "LIVE update re-renders the changed key",
			Description: "[LIVE-ONLY] pxf servers update against the live API re-renders only that server's key.",
		},
		{
			ID: "108-L5-F", Req: "L.5", Layer: Scenario108LayerFunctional,
			Expected:    "DELETE pxf/servers/{server}",
			Description: "pxf servers delete <name> issues DELETE .../pxf/servers/<name>.",
		},
		{
			ID: "108-L5-L", Req: "L.5", Layer: Scenario108LayerLive,
			Expected: "LIVE delete throwaway server",
			Description: "[LIVE-ONLY] pxf servers delete the throwaway server against the live API → " +
				"removed from the CR + the CM keys gone.",
		},
		{
			ID: "108-L6-F", Req: "L.6", Layer: Scenario108LayerFunctional,
			Expected:    "POST pxf/sync",
			Description: "pxf sync issues POST .../data-loading/pxf/sync (CM refresh + STS bump; 202).",
		},
		{
			ID: "108-L6-L", Req: "L.6", Layer: Scenario108LayerLive,
			Expected:    "LIVE sync refresh",
			Description: "[LIVE-ONLY] pxf sync against the live API → 202; CM refresh.",
		},
		{
			ID: "108-L7-F", Req: "L.7", Layer: Scenario108LayerFunctional,
			Expected:    "POST pxf/restart",
			Description: "pxf restart issues POST .../data-loading/pxf/restart (segment-primary STS roll bump; 202).",
		},
		{
			ID: "108-L7-L", Req: "L.7", Layer: Scenario108LayerLive,
			Expected: "LIVE restart (rolls pods)",
			Description: "[LIVE-ONLY] pxf restart against the live API → 202; sidecars ROLL (pod recreation). " +
				"Run last / assert 202 only — heavy.",
		},
		{
			ID: "108-L8-F", Req: "L.8", Layer: Scenario108LayerFunctional,
			Expected:    "data-loading jobs list → GET jobs",
			Description: "data-loading jobs list issues GET .../data-loading/jobs ({jobs,total}=spec).",
		},
		{
			ID: "108-L8-L", Req: "L.8", Layer: Scenario108LayerLive,
			Expected:    "LIVE jobs list matches CR",
			Description: "[LIVE-ONLY] data-loading jobs list against the live API matches the CR.",
		},
		{
			ID: "108-L9-F", Req: "L.9", Layer: Scenario108LayerFunctional,
			Expected: "POST jobs with a pxfJob DTO from flags",
			Description: "data-loading jobs create --type pxf --server --profile --resource --target " +
				"POSTs a body with type=pxf + a pxfJob{server,profile,resource,targetTable,mode}; no gploadJob.",
		},
		{
			ID: "108-L9-L", Req: "L.9", Layer: Scenario108LayerLive,
			Expected: "LIVE pxf job create (throwaway)",
			Description: "[LIVE-ONLY] create a throwaway pxf job against the live API → 201; CR gains it " +
				"(valid server, mode insert, valid 5-field cron if scheduled).",
		},
		{
			ID: "108-L10-F", Req: "L.10", Layer: Scenario108LayerFunctional,
			Expected:    "POST jobs/{job}/start",
			Description: "data-loading jobs start <job> issues POST .../data-loading/jobs/<job>/start.",
		},
		{
			ID: "108-L10-L", Req: "L.10", Layer: Scenario108LayerLive,
			Expected:    "LIVE start one-off (Job created)",
			Description: "[LIVE-ONLY] jobs start against the live API → 202; the data-loading Job object is created.",
		},
		{
			ID: "108-L11-F", Req: "L.11", Layer: Scenario108LayerFunctional,
			Expected:    "POST jobs/{job}/stop",
			Description: "data-loading jobs stop <job> issues POST .../data-loading/jobs/<job>/stop.",
		},
		{
			ID: "108-L11-L", Req: "L.11", Layer: Scenario108LayerLive,
			Expected:    "LIVE stop (Job reaped)",
			Description: "[LIVE-ONLY] jobs stop against the live API → Job + pods reaped.",
		},
		{
			ID: "108-L12-F", Req: "L.12", Layer: Scenario108LayerFunctional,
			Expected:    "DELETE jobs/{job}",
			Description: "data-loading jobs delete <job> issues DELETE .../data-loading/jobs/<job>.",
		},
		{
			ID: "108-L12-L", Req: "L.12", Layer: Scenario108LayerLive,
			Expected:    "LIVE delete (owned Job GC'd)",
			Description: "[LIVE-ONLY] jobs delete against the live API; CR slice shrinks; owned Job/CronJob GC'd.",
		},
		{
			ID: "108-L13-F", Req: "L.13", Layer: Scenario108LayerFunctional,
			Expected: "GET jobs/{job}/logs (stream); follow/tail honored",
			Description: "data-loading jobs logs --job streams the operator log body to stdout; " +
				"--follow/--tail map to follow=true&tailLines=N query params.",
		},
		{
			ID: "108-L13-L", Req: "L.13", Layer: Scenario108LayerLive,
			Expected: "LIVE logs stream OR 409 LOGS_NOT_READY",
			Description: "[LIVE-ONLY] jobs logs against the live API streams the Job pod logs " +
				"(accept stream OR 409 LOGS_NOT_READY when the pod is not ready yet).",
		},
		{
			ID: "108-L14-F", Req: "L.14", Layer: Scenario108LayerFunctional,
			Expected: "POST jobs with a gploadJob DTO from flags",
			Description: "data-loading jobs create --type gpload --gpfdist-host --gpfdist-port --file-path " +
				"--format POSTs a body with type=gpload + gploadJob{inputSource:gpfdist,filePaths}; no pxfJob.",
		},
		{
			ID: "108-L14-L", Req: "L.14", Layer: Scenario108LayerLive,
			Expected:    "LIVE gpload job create",
			Description: "[LIVE-ONLY] create a gpload job against the live API (--from-yaml or flags) → CR gains it.",
		},
		{
			ID: "108-L15-F", Req: "L.15", Layer: Scenario108LayerFunctional,
			Expected: "GET test-read with limit=10; prints REAL sampled rows",
			Description: "data-loading test-read --job/--server/--profile/--resource --limit 10 GETs " +
				".../data-loading/test-read; the CLI prints the rows the fake db returns (NOT fabricated).",
		},
		{
			ID: "108-L15-B", Req: "L.15", Layer: Scenario108LayerBuilder,
			Expected: "ReadPXFSourceSample ALWAYS drops the transient table",
			Description: "the L.15 backing db read creates a TRANSIENT external table → SELECT * LIMIT N → " +
				"ALWAYS DROP (deferred, even on error); identifiers sanitized; rows never fabricated.",
		},
		{
			ID: "108-L15-L", Req: "L.15", Layer: Scenario108LayerLive,
			Expected: "LIVE test-read reads ≤N rows or honest ABSENT",
			Description: "[LIVE-ONLY] test-read --limit 10 against the live API reads ≤10 rows from a real " +
				"PXF source (transient table created & DROPPED) or honest available:false if the source is down.",
		},
		{
			ID: "108-L16-F", Req: "L.16", Layer: Scenario108LayerFunctional,
			Expected: "jobs create --from-yaml reads+unmarshals → POST jobs",
			Description: "data-loading jobs create --from-yaml reads a YAML file and POSTs the unmarshalled " +
				"job body verbatim (YAML keys map 1:1 to the DTO JSON tags).",
		},
		{
			ID: "108-L16-L", Req: "L.16", Layer: Scenario108LayerLive,
			Expected:    "LIVE from-yaml create a complex job",
			Description: "[LIVE-ONLY] jobs create --from-yaml a complex job against the live API → reconciled into the CR.",
		},
	}
}

// scenario108EdgeCases returns the named edge rows (task-breakdown §2 edges).
//
//nolint:funlen // exhaustive named-edge table.
func scenario108EdgeCases() []Scenario108Case {
	return []Scenario108Case{
		{
			ID: "108-L3-flags", Req: "L.3", Layer: Scenario108LayerFunctional,
			Expected: "missing --name/--type → usage error, NO HTTP call",
			Description: "pxf servers create with a missing required flag (no --name or no --type) is a " +
				"clean usage error and NO API request is made (the guard fires before newClient()).",
		},
		{
			ID: "108-L4-flags", Req: "L.4", Layer: Scenario108LayerFunctional,
			Expected:    "missing server name → usage error, NO HTTP call",
			Description: "pxf servers update with no positional name and no --name is a clean error; NO HTTP call.",
		},
		{
			ID: "108-L5-flags", Req: "L.5", Layer: Scenario108LayerFunctional,
			Expected:    "missing server name → usage error, NO HTTP call",
			Description: "pxf servers delete with no name is a clean error; NO HTTP call.",
		},
		{
			ID: "108-L9-name", Req: "L.9", Layer: Scenario108LayerFunctional,
			Expected:    "missing --name → usage error, NO HTTP call",
			Description: "jobs create without --name and without --from-yaml is a clean usage error; NO HTTP call.",
		},
		{
			ID: "108-L9-type", Req: "L.9", Layer: Scenario108LayerFunctional,
			Expected:    "invalid --type → usage error, NO HTTP call",
			Description: "jobs create --type <bogus> is a clean error (valid types: pxf, gpload); NO HTTP call.",
		},
		{
			ID: "108-L9-cron", Req: "L.9", Layer: Scenario108LayerLive,
			Expected: "invalid cron → operator 400 surfaced verbatim",
			Description: "[LIVE-ONLY] jobs create --schedule with an invalid (non-5-field) cron → the operator " +
				"webhook rejects; the CLI surfaces 400 VALIDATION_FAILED; CR unchanged.",
		},
		{
			ID: "108-L13-fallback", Req: "L.13", Layer: Scenario108LayerFunctional,
			Expected: "stream unavailable → kubectl fallback hint; exit 0",
			Description: "jobs logs when the stream endpoint is unavailable prints the kubectl fallback " +
				"instruction naming the k8s Job (util.DataLoadJobName) and returns success (no fabricated log).",
		},
		{
			ID: "108-L15-limit", Req: "L.15", Layer: Scenario108LayerFunctional,
			Expected: "--limit 0/negative → usage error, NO HTTP call; default 10",
			Description: "test-read --limit 0 or negative is a clean usage error and NO HTTP call; an omitted " +
				"--limit defaults to 10 in the query; --limit bounds the printed rows to ≤N.",
		},
		{
			ID: "108-L15-source", Req: "L.15", Layer: Scenario108LayerFunctional,
			Expected: "neither --job nor profile+resource → usage error, NO HTTP call",
			Description: "test-read with neither --job nor both --profile and --resource is a clean usage " +
				"error; NO HTTP call.",
		},
		{
			ID: "108-L15-absent", Req: "L.15", Layer: Scenario108LayerFunctional,
			Expected: "available:false renders cleanly, exit 0",
			Description: "an honest available:false test-read response (DB/source unreachable) renders without " +
				"crashing and exits 0; NEVER fabricated rows, NEVER a 500 for mere unreachability.",
		},
		{
			ID: "108-L15-clean", Req: "L.15", Layer: Scenario108LayerLive,
			Expected: "LIVE transient table gone after test-read",
			Description: "[LIVE-ONLY] after test-read, the transient external table is gone from the catalog " +
				"(a follow-up external-tables/observed shows no leftover sample table).",
		},
		{
			ID: "108-L16-yaml", Req: "L.16", Layer: Scenario108LayerFunctional,
			Expected: "malformed/missing YAML → clean error, NO POST",
			Description: "jobs create --from-yaml on a malformed YAML file surfaces a clean parse error and NO " +
				"POST; a missing file → a clean read error; --from-yaml takes precedence over conflicting flags.",
		},
	}
}

// scenario108CrossCuttingCases returns the RBAC row (CLI tier parity with the
// Scenario 107 route tiers).
func scenario108CrossCuttingCases() []Scenario108Case {
	return []Scenario108Case{
		{
			ID: "108-RBAC", Req: "RBAC", Layer: Scenario108LayerLive,
			Expected: "below-tier → 403 surfaced",
			Description: "[LIVE-ONLY] the CLI verbs inherit the Scenario 107 route tiers: test-read/list/" +
				"status Basic; servers create/update + jobs create/start/stop + sync Operator; servers/jobs " +
				"delete Admin. A below-tier caller's 403 is surfaced as a CLI error.",
		},
	}
}
