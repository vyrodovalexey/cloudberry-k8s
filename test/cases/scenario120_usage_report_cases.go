package cases

// ============================================================================
// Scenario 120 — Usage Reporting (C.11, C.13)
// (the monthly usage report is GENERATED — per-table + per-database content —
// and RETRIEVABLE via the API endpoint P.6 AND via the CLI)
// ============================================================================
//
// Acceptance scenario: with spec.storage.usageReport.enabled:true (+ monthly:true)
// the operator GENERATES a monthly usage report whose CONTENT = per-table AND
// per-database storage consumption over the month (C.11), and that report is
// RETRIEVABLE via the API endpoint P.6 (GET …/storage/usage-report, with an
// optional ?month=YYYY-MM scope) AND via the CLI (cloudberry-ctl storage
// usage-report --cluster X --namespace cloudberry-test [--month YYYY-MM]) (C.13).
// When usageReport.enabled:false the report is UNAVAILABLE: the API returns
// usageReportEnabled:false + empty entries (a SOFT 200 gate for a READ, NOT a
// 400) and the CLI prints the same honest empty report (exit 0).
//
// HONEST ON-DEMAND MODEL: the report is computed on demand at request time from
// live catalog sizes, scoped/labeled by the requested month. The operator does
// not persist month-over-month snapshots; growthBytes/growthHuman/queryCount are
// reported as an honest 0/empty (no fabricated baseline). Because the pgx pool
// is single-database, the per-table breakdown is attached only to the entry of
// the connected database (other database entries carry an empty Tables slice —
// honestly, the operator cannot size tables in a database it is not connected to).
//
// Layers (reusing the shared layer vocabulary):
//   - U : unit       (httptest + mock DB client / pgxmock directly against the
//     handlers in internal/api + internal/db + the cobra cmd in
//     cmd/cloudberry-ctl; no build tag).
//   - F : functional (drive the FULL api.Server router through withAuth +
//     withPermission over a fake-client store + an injected dbFactory whose
//     GetUsageReport returns entries carrying the per-table breakdown;
//     //go:build functional).
//   - L : live e2e   (//go:build e2e; Part A catalog-direct always runs, Part B
//     KUBECONFIG + SCENARIO120_LIVE-gated port-forward + auth + curl of P.6 +
//     a live cloudberry-ctl usage-report invocation against the deployed cluster).
// ============================================================================

// Scenario120Layer enumerates the assertion layer of a Scenario 120 case,
// reusing the shared layer vocabulary.
const (
	// Scenario120LayerUnit is the handler-direct / pgxmock / cobra unit layer.
	Scenario120LayerUnit = Scenario104LayerBuilder
	// Scenario120LayerFunctional is the full-router functional layer.
	Scenario120LayerFunctional = Scenario104LayerReconcile
	// Scenario120LayerLive is the live port-forward + curl + ctl contract layer.
	Scenario120LayerLive = Scenario104LayerLive
)

// Scenario120Gate enumerates the feature-flag state a Scenario 120 case
// exercises. The usage-report endpoint gates on spec.storage.usageReport.enabled.
const (
	// Scenario120GateEnabled means usageReport.enabled:true.
	Scenario120GateEnabled = "enabled"
	// Scenario120GateDisabled means usageReport is nil / disabled.
	Scenario120GateDisabled = "disabled"
	// Scenario120GateNone is used for the cross-cutting / control rows.
	Scenario120GateNone = "n/a"
)

// Scenario120Channel enumerates the retrieval channel a C.13 row exercises:
// the REST API endpoint P.6 or the cloudberry-ctl CLI.
const (
	// Scenario120ChannelAPI is the REST API endpoint P.6.
	Scenario120ChannelAPI = "api"
	// Scenario120ChannelCLI is the cloudberry-ctl storage usage-report command.
	Scenario120ChannelCLI = "cli"
	// Scenario120ChannelNone is used for the DB-content / cross-cutting rows.
	Scenario120ChannelNone = "n/a"
)

// Scenario 120 well-known live values (mirror the e2e Part B env defaults).
const (
	// Scenario120Namespace is the default deploy namespace for the live (-L) rows.
	Scenario120Namespace = "cloudberry-test"
	// Scenario120DefaultCluster is the default (SHORT) live cluster name base.
	Scenario120DefaultCluster = "s120"
	// Scenario120Endpoint is the P.6 usage-report REST path (relative to the
	// cluster subresource base).
	Scenario120Endpoint = "/storage/usage-report"
	// Scenario120MonthParam is the query-string key both the CLI sets and the API
	// reads to scope/label the report by month (exact match — R4).
	Scenario120MonthParam = "month"
	// Scenario120ExampleMonth is the documented example month label (YYYY-MM).
	Scenario120ExampleMonth = "2026-05"
	// Scenario120NotFoundCode is the 404 error code P.6 returns for a missing
	// cluster.
	Scenario120NotFoundCode = "CLUSTER_NOT_FOUND"
)

// Scenario120Case describes one Scenario 120 sub-case. It is a flat catalog row:
// the stable ID, the requirement family it proves (C.11 / C.13 / MONTH /
// DISABLED / CONTROL / PERSIST), the assertion Layer, the retrieval Channel
// (api / cli / n/a), the feature Gate, a short Assert outcome token, and a
// Description. Field/ExpectedValue carry an optional dotted status/JSON path the
// row pins.
type Scenario120Case struct {
	// ID is the catalog rule id (e.g. "120-C11-generate-U", "120-C13-cli-L").
	ID string
	// Req is the rule family the row proves: "C.11", "C.13", "MONTH",
	// "DISABLED", "CONTROL", "PERSIST".
	Req string
	// Layer is the assertion layer: Scenario120LayerUnit / Functional / Live.
	Layer string
	// Channel is the retrieval channel: api / cli / n/a.
	Channel string
	// Endpoint is the REST path (relative to /clusters/{name}) the row exercises
	// (empty for the pure CLI/DB rows).
	Endpoint string
	// Method is the HTTP method (GET), empty for the CLI/DB rows.
	Method string
	// Field is the dotted JSON/status path the row asserts (empty otherwise).
	Field string
	// ExpectedValue is the value asserted at Field, rendered as a string (empty
	// when the row does not pin a concrete value).
	ExpectedValue string
	// Gate is the feature gate the row exercises: enabled / disabled / n/a.
	Gate string
	// Assert is a short human outcome token (the asserted contract).
	Assert string
	// Description explains the case + the gate / channel / honesty rationale.
	Description string
}

// Scenario120Cases returns the full Scenario 120 catalog: the C.11 generate
// (content) rows, the C.13 api + cli retrieve rows, the MONTH-param rows, the
// DISABLED-unavailable rows, plus the CONTROL + PERSIST cross-cutting rows. The
// -U rows are owned in internal/db + internal/api + cmd/cloudberry-ctl; the -F
// rows resolve at the functional full-router layer; the -L rows require a live
// cluster and are resolved (or SKIP cleanly) in the integration/e2e live parts.
func Scenario120Cases() []Scenario120Case {
	cases := []Scenario120Case{}
	cases = append(cases, scenario120GenerateCases()...)
	cases = append(cases, scenario120APIRetrieveCases()...)
	cases = append(cases, scenario120CLIRetrieveCases()...)
	cases = append(cases, scenario120MonthParamCases()...)
	cases = append(cases, scenario120DisabledCases()...)
	cases = append(cases, scenario120CrossCuttingCases()...)
	return cases
}

// scenario120GenerateCases returns the 120-C11-generate rows: the monthly report
// is GENERATED with per-table AND per-database storage consumption content.
func scenario120GenerateCases() []Scenario120Case {
	const desc = "GENERATE a monthly usage report whose CONTENT = per-DATABASE size/connections AND " +
		"a bounded per-TABLE breakdown (Tables:[{schema,table,sizeBytes,sizeHuman}], size-desc, " +
		"LIMIT 50) on the connected-db entry; growth/queryCount are an honest 0 (on-demand, no " +
		"persisted history); the month is stamped as the scope label on each entry"
	return []Scenario120Case{
		{
			ID: "120-C11-generate-U", Req: "C.11", Layer: Scenario120LayerUnit,
			Channel: Scenario120ChannelNone,
			Field:   "entries[connected].tables", ExpectedValue: "[{schema,table,sizeBytes,sizeHuman}]",
			Gate: Scenario120GateEnabled, Assert: "per-db + per-table content; growth/queryCount honest 0",
			Description: "[UNIT] " + desc + " (db.GetUsageReport over pgxmock: the per-database usage " +
				"query AND the per-table enrichment query are routed by SQL content; the connected-db " +
				"(\"testdb\") entry carries the per-table breakdown, others honestly empty).",
		},
		{
			ID: "120-C11-generate-F", Req: "C.11", Layer: Scenario120LayerFunctional,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateEnabled, Assert: "router 200 entries carry per-table content",
			Description: "[FUNCTIONAL] full router (Basic) GET 200; the injected dbFactory yields entries " +
				"whose first (connected) database carries a non-empty tables[] breakdown; total==len(entries).",
		},
		{
			ID: "120-C11-generate-L", Req: "C.11", Layer: Scenario120LayerLive,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateEnabled, Assert: "live 200 connected-db entry has tables[] (or honest empty)",
			Description: "[LIVE-ONLY] curl P.6 (enabled, DB reachable) -> 200; >=1 entry; the connected-db " +
				"entry carries tables[] with positive sizeBytes when the DB has user tables, else honest " +
				"empty / per-table degraded to CONFIG-ONLY when the DB is unreachable in-window.",
		},
	}
}

// scenario120APIRetrieveCases returns the 120-C13-api rows: the report is
// RETRIEVABLE via the REST API endpoint P.6.
func scenario120APIRetrieveCases() []Scenario120Case {
	const desc = "RETRIEVE the report via GET …/storage/usage-report -> 200 {cluster, month, " +
		"entries:[…with tables…], total, usageReportEnabled:true} under PermissionBasic"
	return []Scenario120Case{
		{
			ID: "120-C13-api-U", Req: "C.13", Layer: Scenario120LayerUnit,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Field: "usageReportEnabled", ExpectedValue: "true",
			Gate: Scenario120GateEnabled, Assert: "200 envelope with enriched entries",
			Description: "[UNIT] " + desc + " (handleGetUsageReport over a mock DB client whose entries " +
				"carry db.TableUsage); the per-table content is present in the JSON envelope.",
		},
		{
			ID: "120-C13-api-F", Req: "C.13", Layer: Scenario120LayerFunctional,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateEnabled, Assert: "router 200 real enriched entries",
			Description: "[FUNCTIONAL] full router (Basic) GET 200; the real enriched entries from the " +
				"injected dbFactory are surfaced through P.6.",
		},
		{
			ID: "120-C13-api-L", Req: "C.13", Layer: Scenario120LayerLive,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateEnabled, Assert: "live 200 usageReportEnabled:true + entries (or honest empty)",
			Description: "[LIVE-ONLY] curl GET usage-report (enabled) -> 200 with entries + (per-table when " +
				"the DB has user tables, else honest empty); usageReportEnabled reflects the live CR spec.",
		},
	}
}

// scenario120CLIRetrieveCases returns the 120-C13-cli rows: the report is
// RETRIEVABLE via the cloudberry-ctl CLI, which GETs the P.6 endpoint.
func scenario120CLIRetrieveCases() []Scenario120Case {
	const desc = "RETRIEVE the report via `cloudberry-ctl storage usage-report --cluster X " +
		"--namespace cloudberry-test [--month YYYY-MM]` which GETs …/storage/usage-report (the " +
		"--month flag threads ?month=; namespace always encodes)"
	return []Scenario120Case{
		{
			ID: "120-C13-cli-U", Req: "C.13", Layer: Scenario120LayerUnit,
			Channel: Scenario120ChannelCLI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateEnabled, Assert: "cobra GET path …/usage-report (no month key without --month)",
			Description: "[UNIT] " + desc + " (cobra usage-report cmd against a recording httptest server: " +
				"WITHOUT --month the GET query carries no month key, namespace= still encodes).",
		},
		{
			ID: "120-C13-cli-F", Req: "C.13", Layer: Scenario120LayerFunctional,
			Channel: Scenario120ChannelCLI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateEnabled, Assert: "cli end-to-end records GET …/usage-report; body printed",
			Description: "[FUNCTIONAL] CLI end-to-end through cobra -> recording server asserts METHOD=GET, " +
				"PATH=/clusters/X/storage/usage-report; the response body (entries with tables) is printed.",
		},
		{
			ID: "120-C13-cli-L", Req: "C.13", Layer: Scenario120LayerLive,
			Channel: Scenario120ChannelCLI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateEnabled, Assert: "live ctl prints usage report incl. per-table (or CONFIG-ONLY)",
			Description: "[LIVE-ONLY] live cloudberry-ctl storage usage-report against the deployed operator " +
				"(auth + port-forward gated) -> prints the usage report incl. per-table content (or honest " +
				"empty); degraded to CONFIG-ONLY when the ctl cannot be run live in-window.",
		},
	}
}

// scenario120MonthParamCases returns the 120-MONTH-param rows: the --month/?month=
// scope threads end-to-end (CLI sets month=, API reads it and echoes it).
func scenario120MonthParamCases() []Scenario120Case {
	return []Scenario120Case{
		{
			ID: "120-MONTH-param-U", Req: "MONTH", Layer: Scenario120LayerUnit,
			Channel: Scenario120ChannelCLI, Endpoint: Scenario120Endpoint, Method: "GET",
			Field: Scenario120MonthParam, ExpectedValue: Scenario120ExampleMonth,
			Gate: Scenario120GateEnabled, Assert: "cobra --month 2026-05 records ?month=2026-05; API echoes envelope.month",
			Description: "[UNIT] cobra usage-report --month 2026-05 -> the recorded query string carries " +
				"month=2026-05 (AND namespace= when set), matching the API's r.URL.Query().Get(\"month\"); " +
				"the handler reads month and echoes it in envelope.month.",
		},
		{
			ID: "120-MONTH-param-F", Req: "MONTH", Layer: Scenario120LayerFunctional,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Field: "month", ExpectedValue: Scenario120ExampleMonth,
			Gate: Scenario120GateEnabled, Assert: "router GET ?month=2026-05 -> envelope month==2026-05",
			Description: "[FUNCTIONAL] full router GET …?month=2026-05 -> envelope month==\"2026-05\"; the " +
				"month is passed through to GetUsageReport(ctx, \"2026-05\") (entry labels carry it).",
		},
	}
}

// scenario120DisabledCases returns the 120-DISABLED-unavailable rows: with
// usageReport.enabled:false the report is UNAVAILABLE (soft 200, NOT 400).
func scenario120DisabledCases() []Scenario120Case {
	const desc = "usageReport nil/disabled -> 200 {usageReportEnabled:false, entries:[]} (a SOFT gate for " +
		"a READ, NOT a 400); no DB call; the CLI prints the same honest empty report (exit 0)"
	return []Scenario120Case{
		{
			ID: "120-DISABLED-unavailable-U", Req: "DISABLED", Layer: Scenario120LayerUnit,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Field: "usageReportEnabled", ExpectedValue: "false",
			Gate: Scenario120GateDisabled, Assert: "200 unavailable empty (no DB call)",
			Description: "[UNIT] " + desc + " (handleGetUsageReport: the disabled gate short-circuits the " +
				"query, the DB client is never opened).",
		},
		{
			ID: "120-DISABLED-unavailable-F", Req: "DISABLED", Layer: Scenario120LayerFunctional,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateDisabled, Assert: "router 200 usageReportEnabled:false + empty",
			Description: "[FUNCTIONAL] full router (Basic) GET disabled -> 200 usageReportEnabled:false + " +
				"empty entries (even with usable data injected, the gate short-circuits).",
		},
		{
			ID: "120-DISABLED-unavailable-L", Req: "DISABLED", Layer: Scenario120LayerLive,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateDisabled, Assert: "live 200 usageReportEnabled:false + empty; CLI exit 0",
			Description: "[LIVE-ONLY] curl GET usage-report on a CR with usageReport.enabled:false -> 200, " +
				"usageReportEnabled:false, empty entries; the live ctl run prints the unavailable/empty " +
				"report (exit 0, no error).",
		},
	}
}

// scenario120CrossCuttingCases returns the CONTROL + PERSIST cross-cutting rows.
func scenario120CrossCuttingCases() []Scenario120Case {
	return []Scenario120Case{
		{
			ID: "120-CONTROL", Req: "CONTROL", Layer: Scenario120LayerFunctional,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateEnabled, Assert: "healthy fast DB -> populated per-db + per-table shape",
			Description: "[FUNCTIONAL] a healthy fast DB stub + an enabled CR: P.6 returns the populated " +
				"REAL enriched shape (per-database + per-table) with no error — a no-false-positive control.",
		},
		{
			ID: "120-PERSIST-L", Req: "PERSIST", Layer: Scenario120LayerLive,
			Channel: Scenario120ChannelAPI, Endpoint: Scenario120Endpoint, Method: "GET",
			Gate: Scenario120GateEnabled, Assert: "two GETs reflect current catalog; no snapshot; growth stays 0",
			Description: "[LIVE-ONLY] DELIBERATE NON-PERSISTENCE: two GETs of usage-report (no mutating call " +
				"between) both reflect the CURRENT live catalog; there is NO stored snapshot / " +
				"month-over-month history; growthBytes stays an honest 0 (the on-demand model).",
		},
	}
}
