package cases

// ============================================================================
// Scenario 119 — All Storage API Endpoints (P.1–P.6)
// (the SIX spec.storage REST endpoints returning REAL data)
// ============================================================================
//
// Acceptance scenario: all six spec.storage REST endpoints return REAL data
// (no stubs), enforce the read/write permission split, and honour the universal
// best-effort / non-fatal contract (a DB-unavailability yields an honest empty /
// cached shape with HTTP 200, NEVER a 500; a missing cluster yields 404):
//
//   - 119a P.1 GET  /storage/disk-usage           -> 200 {cluster,
//     diskUsagePercent (== status.diskUsagePercent — the M.1==S.1 invariant from
//     Scenario 116), diskUsage:[per-db], diskUsageBySegment:[per-tablespace]}.
//   - 119b P.2 GET  /storage/tables               -> 200 {tables:[{schema,table,
//     sizeBytes,sizeHuman,bloatPercent,skewPercent,rowCount}], total}.
//   - 119c P.3 GET  /storage/tables/{schema}/{table} -> 200 {schema,table,
//     sizeBytes,rowCount,bloatPercent,skewPercent,lastVacuum,lastAnalyze,
//     indexSizes:[{name,sizeBytes,sizeHuman}]}.
//   - 119d P.4 GET  /storage/recommendations      -> 200 {cluster,
//     recommendations:[{type,target,value,ratio,severity,description}],
//     recommendationCount (live len, else cached status), total}.
//   - 119e P.5 POST /storage/recommendations/scan -> 202 {status:"scan
//     initiated", cluster} when enabled (recommendation_scan_duration_seconds
//     _count advances per POST); 400 RECOMMENDATION_SCAN_NOT_ENABLED when
//     disabled. Permission: Operator.
//   - 119f P.6 GET  /storage/usage-report         -> 200 {month, entries, total,
//     usageReportEnabled}: false + empty when usageReport disabled (a SOFT gate
//     for a READ, NOT the *_NOT_ENABLED 400 reserved for mutating endpoints);
//     true + entries when enabled.
//
// Reads require PermissionBasic; the scan POST requires PermissionOperator.
//
// Layers (reusing the shared layer vocabulary):
//   - U : unit       (httptest + mock DB client + mock cluster store directly
//     against the handlers in internal/api + internal/db pgxmock; no build tag).
//   - F : functional (drive the FULL api.Server router through withAuth +
//     withPermission over a fake-client store + injected dbFactory;
//     //go:build functional).
//   - L : live e2e   (//go:build e2e; Part A catalog-direct always runs, Part B
//     KUBECONFIG + SCENARIO119_LIVE-gated port-forward + auth + curl of the six
//     endpoints against the deployed cluster).
// ============================================================================

// Scenario119Layer enumerates the assertion layer of a Scenario 119 case,
// reusing the shared layer vocabulary.
const (
	// Scenario119LayerUnit is the handler-direct / pgxmock unit layer.
	Scenario119LayerUnit = Scenario104LayerBuilder
	// Scenario119LayerFunctional is the full-router functional layer.
	Scenario119LayerFunctional = Scenario104LayerReconcile
	// Scenario119LayerLive is the live port-forward + curl contract layer.
	Scenario119LayerLive = Scenario104LayerLive
)

// Scenario119Gate enumerates the feature-flag state a Scenario 119 case
// exercises. The scan/usage-report endpoints gate on their spec.storage feature
// flag; the always-on endpoints use the none gate.
const (
	// Scenario119GateEnabled means the gated feature (recommendationScan /
	// usageReport) is enabled.
	Scenario119GateEnabled = "enabled"
	// Scenario119GateDisabled means the gated feature is nil / disabled.
	Scenario119GateDisabled = "disabled"
	// Scenario119GateNone is used for the always-on endpoints + the
	// cross-cutting / control rows.
	Scenario119GateNone = "n/a"
)

// Scenario 119 well-known live values (mirror the e2e Part B env defaults).
const (
	// Scenario119Namespace is the default deploy namespace for the live (-L) rows.
	Scenario119Namespace = "cloudberry-test"
	// Scenario119DefaultCluster is the default (SHORT) live cluster name base.
	Scenario119DefaultCluster = "s119"
	// Scenario119DurationMetricName is the P.5 scan-duration histogram the live
	// rows assert advances per POST.
	Scenario119DurationMetricName = "cloudberry_recommendation_scan_duration_seconds"
	// Scenario119ScanNotEnabledCode is the 400 error code the disabled scan POST
	// returns.
	Scenario119ScanNotEnabledCode = "RECOMMENDATION_SCAN_NOT_ENABLED"
	// Scenario119NotFoundCode is the 404 error code each endpoint returns for a
	// missing cluster.
	Scenario119NotFoundCode = "CLUSTER_NOT_FOUND"
)

// Scenario119Case describes one Scenario 119 sub-case. It is a flat catalog row:
// the stable ID, the rule family (P-id / cross-cutting family), the assertion
// Layer, the REST Endpoint + Method it exercises, the feature Gate, a short
// Expected/Assert outcome token, and a Description. Field/ExpectedValue carry an
// optional dotted status/JSON path the row pins.
type Scenario119Case struct {
	// ID is the catalog rule id (e.g. "119a-P1-U", "119e-P5-L", "119-AUTH").
	ID string
	// Req is the rule family the row proves: "P.1".."P.6", "NOTFOUND", "AUTH",
	// "DBERR", "CONTROL", "PERSIST".
	Req string
	// Layer is the assertion layer: Scenario119LayerUnit / Functional / Live.
	Layer string
	// Endpoint is the REST path (relative to /clusters/{name}) the row exercises.
	Endpoint string
	// Method is the HTTP method (GET / POST), empty for the aggregate rows.
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
	// Description explains the case + the gate / best-effort rationale.
	Description string
}

// Scenario119Cases returns the full Scenario 119 catalog: the six per-endpoint
// U/F/L rows (P.1–P.6) plus the cross-cutting NOTFOUND / AUTH / DBERR-nonfatal /
// CONTROL / PERSIST rows. The -U rows are owned in internal/api + internal/db;
// the -F rows resolve at the functional full-router layer; the -L rows require a
// live cluster and are resolved (or SKIP cleanly) in the integration/e2e live
// parts.
func Scenario119Cases() []Scenario119Case {
	cases := []Scenario119Case{}
	cases = append(cases, scenario119DiskUsageCases()...)
	cases = append(cases, scenario119TablesCases()...)
	cases = append(cases, scenario119TableDetailCases()...)
	cases = append(cases, scenario119RecommendationsCases()...)
	cases = append(cases, scenario119ScanCases()...)
	cases = append(cases, scenario119UsageReportCases()...)
	cases = append(cases, scenario119CrossCuttingCases()...)
	return cases
}

// scenario119DiskUsageCases returns the 119a P.1 disk-usage rows: the
// status-sourced percent (M.1==S.1) plus the per-db + per-tablespace breakdown.
func scenario119DiskUsageCases() []Scenario119Case {
	const ep = "/storage/disk-usage"
	const desc = "GET disk-usage returns 200 {diskUsagePercent (== status.diskUsagePercent, the " +
		"M.1==S.1 Scenario-116 invariant), diskUsage:[per-db], diskUsageBySegment:[per-tablespace]}; " +
		"the breakdown is best-effort additive (honestly empty when the DB is unavailable)"
	return []Scenario119Case{
		{
			ID: "119a-P1-U", Req: "P.1", Layer: Scenario119LayerUnit,
			Endpoint: ep, Method: "GET", Field: "diskUsagePercent", ExpectedValue: "status.diskUsagePercent",
			Gate: Scenario119GateNone, Assert: "200 percent==status + breakdown",
			Description: "[UNIT] " + desc + " (handler-direct over a mock DB client).",
		},
		{
			ID: "119a-P1-F", Req: "P.1", Layer: Scenario119LayerFunctional,
			Endpoint: ep, Method: "GET", Field: "diskUsagePercent", ExpectedValue: "status.diskUsagePercent",
			Gate: Scenario119GateNone, Assert: "router 200 percent==status",
			Description: "[FUNCTIONAL] full router (Basic perm) GET returns 200 with the real percent " +
				"matching status + non-empty diskUsage + the segment breakdown when a dbFactory is injected.",
		},
		{
			ID: "119a-P1-L", Req: "P.1", Layer: Scenario119LayerLive,
			Endpoint: ep, Method: "GET", Field: "diskUsagePercent", ExpectedValue: "status.diskUsagePercent",
			Gate: Scenario119GateNone, Assert: "live 200 percent==status",
			Description: "[LIVE-ONLY] curl GET disk-usage returns 200; diskUsagePercent == live " +
				"status.diskUsagePercent (M.1==S.1); breakdown present or honestly empty.",
		},
	}
}

// scenario119TablesCases returns the 119b P.2 storage/tables rows: the real
// per-table size/bloat/skew/rowCount listing.
func scenario119TablesCases() []Scenario119Case {
	const ep = "/storage/tables"
	const desc = "GET tables returns 200 {tables:[{schema,table,sizeBytes,sizeHuman,bloatPercent," +
		"skewPercent,rowCount}], total==len}"
	return []Scenario119Case{
		{
			ID: "119b-P2-U", Req: "P.2", Layer: Scenario119LayerUnit,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateNone, Assert: "200 real rows, total==len",
			Description: "[UNIT] " + desc + " (handler-direct + new GetTables mock; skew honest-0 " +
				"fallback when gp_toolkit absent — no fabrication).",
		},
		{
			ID: "119b-P2-F", Req: "P.2", Layer: Scenario119LayerFunctional,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateNone, Assert: "router 200 real rows",
			Description: "[FUNCTIONAL] full router (Basic) GET 200; the real rows from the injected " +
				"dbFactory are surfaced; " + desc + ".",
		},
		{
			ID: "119b-P2-L", Req: "P.2", Layer: Scenario119LayerLive,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateNone, Assert: "live 200 tables list",
			Description: "[LIVE-ONLY] curl GET tables returns 200 with the documented shape; the rows " +
				"reflect the live DB (or honestly empty if the DB is unreachable).",
		},
	}
}

// scenario119TableDetailCases returns the 119c P.3 table-detail rows: the real
// size/bloat/skew + index sizes for one table.
func scenario119TableDetailCases() []Scenario119Case {
	const ep = "/storage/tables/{schema}/{table}"
	const desc = "GET table-detail returns 200 {schema,table,sizeBytes,rowCount,bloatPercent," +
		"skewPercent,lastVacuum,lastAnalyze,indexSizes:[{name,sizeBytes,sizeHuman}]}; a DB error " +
		"degrades to the honest minimal {schema,table} shape (200, NOT 500)"
	return []Scenario119Case{
		{
			ID: "119c-P3-U", Req: "P.3", Layer: Scenario119LayerUnit,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateNone, Assert: "200 detail + indexSizes",
			Description: "[UNIT] " + desc + " (handler-direct over a mock GetTableDetails).",
		},
		{
			ID: "119c-P3-F", Req: "P.3", Layer: Scenario119LayerFunctional,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateNone, Assert: "router 200 real detail",
			Description: "[FUNCTIONAL] full router (Basic) GET 200; the real detail (incl. indexSizes) " +
				"from the injected dbFactory is surfaced.",
		},
		{
			ID: "119c-P3-L", Req: "P.3", Layer: Scenario119LayerLive,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateNone, Assert: "live 200 detail",
			Description: "[LIVE-ONLY] curl GET tables/public/<sometable> returns 200 with real " +
				"size/bloat; an unknown table is honestly reflected.",
		},
	}
}

// scenario119RecommendationsCases returns the 119d P.4 recommendations rows: the
// real four-type list with recommendationCount == live len (else cached status).
func scenario119RecommendationsCases() []Scenario119Case {
	const ep = "/storage/recommendations"
	const desc = "GET recommendations returns 200 {recommendations:[{type,target (schema.table)," +
		"value,ratio,severity,description}], recommendationCount (LIVE len when DB reachable, else " +
		"cached status.recommendationCount), total==len}"
	return []Scenario119Case{
		{
			ID: "119d-P4-U", Req: "P.4", Layer: Scenario119LayerUnit,
			Endpoint: ep, Method: "GET", Field: "recommendationCount", ExpectedValue: "len(recommendations)",
			Gate: Scenario119GateNone, Assert: "200 real recs, count==len",
			Description: "[UNIT] " + desc + " (handler-direct over the four Get* mocks); a DB-unavailable " +
				"fetch falls back to the cached count with an empty list (200, NOT 500).",
		},
		{
			ID: "119d-P4-F", Req: "P.4", Layer: Scenario119LayerFunctional,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateNone, Assert: "router 200 real recs",
			Description: "[FUNCTIONAL] full router (Basic) GET 200; the real recs from the injected " +
				"dbFactory are surfaced; honest 0-list + cached count when no DB.",
		},
		{
			ID: "119d-P4-L", Req: "P.4", Layer: Scenario119LayerLive,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateNone, Assert: "live 200 recs list",
			Description: "[LIVE-ONLY] curl GET recommendations returns 200; the list reflects the live " +
				"thresholds; counts consistent with status.recommendationCount.",
		},
	}
}

// scenario119ScanCases returns the 119e P.5 scan rows: the enabled-202 (duration
// metric advances) vs disabled-400 RECOMMENDATION_SCAN_NOT_ENABLED gate.
func scenario119ScanCases() []Scenario119Case {
	const ep = "/storage/recommendations/scan"
	return []Scenario119Case{
		{
			ID: "119e-P5-U", Req: "P.5", Layer: Scenario119LayerUnit,
			Endpoint: ep, Method: "POST",
			Gate: Scenario119GateEnabled, Assert: "202 + duration _count advances per POST",
			Description: "[UNIT] enabled -> 202 {status:\"scan initiated\", cluster}; " +
				"ObserveRecommendationScanDuration is called exactly once per POST (the " +
				Scenario119DurationMetricName + "_count advances by 1 per POST, independent of cron); " +
				"disabled -> 400 " + Scenario119ScanNotEnabledCode + " and no scan runs. Permission: Operator.",
		},
		{
			ID: "119e-P5-F", Req: "P.5", Layer: Scenario119LayerFunctional,
			Endpoint: ep, Method: "POST",
			Gate: Scenario119GateEnabled, Assert: "router 202 enabled / 400 disabled",
			Description: "[FUNCTIONAL] full router (Operator perm) POST 202 when enabled (duration " +
				"_count advances); disabled -> 400 " + Scenario119ScanNotEnabledCode + ".",
		},
		{
			ID: "119e-P5-L", Req: "P.5", Layer: Scenario119LayerLive,
			Endpoint: ep, Method: "POST",
			Gate: Scenario119GateEnabled, Assert: "live 202 + metrics _count advances",
			Description: "[LIVE-ONLY] curl POST scan returns 202 (enabled); scrape the operator /metrics " +
				"-> " + Scenario119DurationMetricName + "_count increased vs the pre-POST baseline; 400 " +
				"when disabled.",
		},
	}
}

// scenario119UsageReportCases returns the 119f P.6 usage-report rows: the
// enabled (entries + flag true) vs disabled (empty + flag false soft-gate)
// behavior.
func scenario119UsageReportCases() []Scenario119Case {
	const ep = "/storage/usage-report"
	const desc = "GET usage-report returns 200 {month, entries, total, usageReportEnabled}: " +
		"false + empty entries when usageReport is nil/disabled (a SOFT gate for a READ, NOT the " +
		"*_NOT_ENABLED 400 reserved for mutating endpoints); true + entries when enabled+DB"
	return []Scenario119Case{
		{
			ID: "119f-P6-U", Req: "P.6", Layer: Scenario119LayerUnit,
			Endpoint: ep, Method: "GET", Field: "usageReportEnabled", ExpectedValue: "spec.storage.usageReport.enabled",
			Gate: Scenario119GateEnabled, Assert: "200 enabled entries / disabled empty",
			Description: "[UNIT] " + desc + " (handler-direct); enabled+DB error -> empty entries + flag " +
				"true (200, NOT 500).",
		},
		{
			ID: "119f-P6-F", Req: "P.6", Layer: Scenario119LayerFunctional,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateEnabled, Assert: "router 200 enabled/disabled",
			Description: "[FUNCTIONAL] full router (Basic) GET 200; real entries when enabled+DB; honest " +
				"empty + usageReportEnabled:false when disabled.",
		},
		{
			ID: "119f-P6-L", Req: "P.6", Layer: Scenario119LayerLive,
			Endpoint: ep, Method: "GET",
			Gate: Scenario119GateEnabled, Assert: "live 200 + flag per spec",
			Description: "[LIVE-ONLY] curl GET usage-report returns 200 with usage entries (DB reachable) " +
				"or honest empty; usageReportEnabled reflects the live CR spec.",
		},
	}
}

// scenario119CrossCuttingCases returns the NOTFOUND / AUTH / DBERR-nonfatal /
// CONTROL / PERSIST cross-cutting rows that span all six endpoints.
func scenario119CrossCuttingCases() []Scenario119Case {
	return []Scenario119Case{
		{
			ID: "119-NOTFOUND", Req: "NOTFOUND", Layer: Scenario119LayerUnit,
			Gate: Scenario119GateNone, Assert: "404 before any DB call",
			Description: "[UNIT+FUNCTIONAL] each of the six endpoints returns 404 " +
				Scenario119NotFoundCode + " for a missing cluster (writeClusterNotFound), checked " +
				"BEFORE any DB call (no client opened).",
		},
		{
			ID: "119-AUTH", Req: "AUTH", Layer: Scenario119LayerFunctional,
			Gate: Scenario119GateNone, Assert: "reads Basic, scan Operator",
			Description: "[FUNCTIONAL+LIVE] reads enforce PermissionBasic, the scan POST enforces " +
				"PermissionOperator; missing/invalid creds -> 401; insufficient perm -> 403. Live: " +
				"anonymous curl -> 401.",
		},
		{
			ID: "119-DBERR-nonfatal", Req: "DBERR", Layer: Scenario119LayerUnit,
			Gate: Scenario119GateNone, Assert: "DB error -> honest empty 200, never 500",
			Description: "[UNIT] for P.1/P.2/P.3/P.4/P.6: dbFactory==nil OR NewClient err OR query err " +
				"-> honest empty/cached shape, status 200, NEVER a 500 (mirrors collectDiskUsage).",
		},
		{
			ID: "119-CONTROL", Req: "CONTROL", Layer: Scenario119LayerFunctional,
			Gate: Scenario119GateEnabled, Assert: "healthy DB -> all six populated",
			Description: "[FUNCTIONAL] healthy fast DB stub: all six endpoints return populated REAL " +
				"shapes with no error (a no-false-positive control across the suite).",
		},
		{
			ID: "119-PERSIST-L", Req: "PERSIST", Layer: Scenario119LayerLive,
			Gate: Scenario119GateEnabled, Assert: "post-scan P.4 reflects refreshed list; P.1 stays==status",
			Description: "[LIVE-ONLY] after a live P.5 POST + settle, P.4 GET reflects the refreshed " +
				"recommendation list / status.recommendationCount; P.1 diskUsagePercent stays == live " +
				"status.diskUsagePercent.",
		},
	}
}
