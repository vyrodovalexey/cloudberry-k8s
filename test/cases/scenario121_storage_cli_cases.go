package cases

// ============================================================================
// Scenario 121 — All Storage CLI Commands (L.1–L.6)
// ============================================================================
//
// Acceptance scenario: the cloudberry-ctl (cobra) `storage` command surface is
// FULL and drives the Scenario 119/120 operator REST API (the P.1–P.6 storage
// endpoints) 1:1. Scenario 121 is overwhelmingly CLI-only wiring on top of the
// existing storage REST surface; the ONLY production change is the L.3
// `tables detail` command gaining --schema/--table flags (positional args stay
// supported for backward compat).
//
// This storage-recommendations CLI family (L.1–L.6) is DISTINCT from the
// data-loading L.1–L.16 CLI family of Scenario 108 — the L naming COLLIDES, so
// the catalog IDs are 121-prefixed (121a-L1-*, …) to disambiguate.
//
// CLI verbs (all FULL):
//   L.1  storage disk-usage                       Basic    GET  storage/disk-usage
//   L.2  storage tables list                      Basic    GET  storage/tables
//   L.3  storage tables detail --schema --table   Basic    GET  storage/tables/{schema}/{table}
//   L.4  storage recommendations list             Basic    GET  storage/recommendations
//   L.5  storage recommendations scan             Operator POST storage/recommendations/scan
//   L.6  storage usage-report --month             Basic    GET  storage/usage-report?month=YYYY-MM
//
// The catalog mirrors the Scenario 108 SHAPE (a stable ID, a Req family, a
// resolution Layer, an Expected token, an honesty-naming Description). The -F
// rows are resolved by driving the cobra root command in-process against a
// recording httptest server (asserting the REAL HTTP method/path/query the CLI
// produced, never just an exit code). The -L rows require the deployed operator
// + a storage-enabled cluster and are resolved (or SKIP cleanly) in e2e.
//
// HONESTY INVARIANTS (enforced by the -U/-F rows that touch them):
//   - 121-DETAIL-missing: a missing schema/table (neither --schema/--table nor
//     positional args) is a CLEAN local usage error BEFORE any HTTP call (the
//     recorder stays empty) — never a half-built /tables// request.
//   - 121-MONTH-period: --month=2026-05 round-trips as ?month=2026-05 AND the
//     report echoes that month LABEL. It is an ON-DEMAND report labeled by the
//     month, NOT a persisted historical snapshot (cross-ref Scenario 120 C.11).
//   - 121-CONTROL: an unknown storage subcommand is a clean cobra error that
//     issues NO HTTP call; each command requires --cluster.
// ============================================================================

// Scenario121Layer enumerates the assertion layer a Scenario 121 case resolves
// at, sharing the Scenario 104/105/106/107/108 vocabulary.
const (
	// Scenario121LayerUnit is a unit/CLI-direct case resolved by driving the
	// cobra command tree in-process against a recording httptest operator
	// stand-in (newCtlRecorderServer + runCtl) — infra-free, deterministic.
	Scenario121LayerUnit = Scenario104LayerBuilder
	// Scenario121LayerFunctional is a functional/CLI-level case resolved by
	// driving the real ctl OperatorClient against a real api.Server router
	// (fake controller-runtime client + fake db.Client + auth/RBAC).
	Scenario121LayerFunctional = Scenario104LayerReconcile
	// Scenario121LayerLive requires a deployed operator + a storage-enabled
	// cluster (live-only; SKIPs cleanly when the live env is absent).
	Scenario121LayerLive = Scenario104LayerLive
)

// Scenario 121 well-known names + namespace (mirror the live e2e defaults so the
// Part A catalog and the live Part B agree).
const (
	// Scenario121Namespace is the default deploy namespace for the live rows.
	Scenario121Namespace = "cloudberry-test"
	// Scenario121DefaultCluster is the default (SHORT) live cluster name.
	Scenario121DefaultCluster = "s121"
)

// Scenario121Case describes one Scenario 121 sub-case. It mirrors the
// Scenario108Case SHAPE — a flat catalog row carrying an ID + the CLI command
// requirement family + the resolution Layer + the HTTP Method + the API Path +
// a human Assert token + a Description that names the asserted effect, plus the
// Command line it proves and the run Gate. Live-only rows are marked [LIVE-ONLY].
type Scenario121Case struct {
	// ID is the catalog rule id (e.g. "121a-L1-U", "121-DETAIL-flags").
	ID string
	// Req is the CLI command family the row proves: "L.1".."L.6", "CONTROL",
	// "PERSIST".
	Req string
	// Layer is the assertion layer: Scenario121LayerUnit,
	// Scenario121LayerFunctional or Scenario121LayerLive.
	Layer string
	// Command is the ctl command line the row proves (without auth/cluster flags).
	Command string
	// Method is the HTTP method the command builds (GET/POST), or "" for the
	// negative/no-HTTP rows.
	Method string
	// Path is the API path (relative to the cluster subresource root) the
	// command builds, or "" for the negative/no-HTTP rows.
	Path string
	// Assert is a short outcome token / human description of the asserted outcome.
	Assert string
	// Gate is "ALWAYS" (runs in every CI lane) or "LIVE" (KUBECONFIG +
	// SCENARIO121_LIVE gated; SKIPs cleanly otherwise).
	Gate string
	// Description explains the case and names the asserted effect. Live-only rows
	// are marked [LIVE-ONLY].
	Description string
}

// Scenario121Cases returns the full Scenario 121 catalog (task-breakdown §1):
// the per-command happy-path + side-effect rows (-U/-F/-L), the named edge rows
// (DETAIL-* + MONTH-period) and the cross-cutting rows (CONTROL + PERSIST-L).
// The -U rows are resolved by the cmd/cloudberry-ctl recorder suite; the -F rows
// by the functional suite; the -L rows require the deployed operator and are
// resolved (or SKIP cleanly) in e2e.
func Scenario121Cases() []Scenario121Case {
	cases := []Scenario121Case{}
	cases = append(cases, scenario121HappyPathCases()...)
	cases = append(cases, scenario121EdgeCases()...)
	cases = append(cases, scenario121CrossCuttingCases()...)
	return cases
}

// scenario121HappyPathCases returns the per-command happy-path + side-effect
// rows (unit + functional + live), one -U, one -F and one -L per L.1–L.6.
//
//nolint:funlen // an exhaustive per-command happy-path table of 6 verbs × 3 layers.
func scenario121HappyPathCases() []Scenario121Case {
	return []Scenario121Case{
		// L.1 storage disk-usage --------------------------------------------
		{
			ID: "121a-L1-U", Req: "L.1", Layer: Scenario121LayerUnit,
			Command: "storage disk-usage", Method: "GET", Path: "storage/disk-usage",
			Assert: "GET storage/disk-usage; ?namespace= encoded; exactly 1 request",
			Gate:   "ALWAYS",
			Description: "cloudberry-ctl storage disk-usage builds exactly one GET to " +
				".../storage/disk-usage with the namespace encoded; no body.",
		},
		{
			ID: "121a-L1-F", Req: "L.1", Layer: Scenario121LayerFunctional,
			Command: "storage disk-usage", Method: "GET", Path: "storage/disk-usage",
			Assert: "catalog row well-formed + in-process GET assertion",
			Gate:   "ALWAYS",
			Description: "the disk-usage read reaches the operator and returns 200 with the " +
				"diskUsagePercent shape (driven through the real ctl client + api.Server).",
		},
		{
			ID: "121a-L1-L", Req: "L.1", Layer: Scenario121LayerLive,
			Command: "storage disk-usage", Method: "GET", Path: "storage/disk-usage",
			Assert: "LIVE disk-usage prints diskUsagePercent; exit 0",
			Gate:   "LIVE",
			Description: "[LIVE-ONLY] storage disk-usage against the deployed cluster prints disk " +
				"usage (diskUsagePercent); exit 0; payload non-empty.",
		},
		// L.2 storage tables list -------------------------------------------
		{
			ID: "121b-L2-U", Req: "L.2", Layer: Scenario121LayerUnit,
			Command: "storage tables list", Method: "GET", Path: "storage/tables",
			Assert: "GET storage/tables; ?namespace= encoded; exactly 1 request",
			Gate:   "ALWAYS",
			Description: "cloudberry-ctl storage tables list builds a single GET to " +
				".../storage/tables; no body.",
		},
		{
			ID: "121b-L2-F", Req: "L.2", Layer: Scenario121LayerFunctional,
			Command: "storage tables list", Method: "GET", Path: "storage/tables",
			Assert: "catalog row well-formed + in-process GET assertion",
			Gate:   "ALWAYS",
			Description: "the tables list read reaches the operator and returns 200 with the " +
				"tables shape (total == len of the mock rows).",
		},
		{
			ID: "121b-L2-L", Req: "L.2", Layer: Scenario121LayerLive,
			Command: "storage tables list", Method: "GET", Path: "storage/tables",
			Assert: "LIVE tables list prints tables; exit 0",
			Gate:   "LIVE",
			Description: "[LIVE-ONLY] storage tables list against the deployed cluster lists the " +
				"per-table storage info; exit 0.",
		},
		// L.3 storage tables detail -----------------------------------------
		{
			ID: "121c-L3-U", Req: "L.3", Layer: Scenario121LayerUnit,
			Command: "storage tables detail --schema public --table orders",
			Method:  "GET", Path: "storage/tables/public/orders",
			Assert: "GET storage/tables/public/orders (segments url-escaped); 1 request",
			Gate:   "ALWAYS",
			Description: "cloudberry-ctl storage tables detail --schema public --table orders builds " +
				"GET .../storage/tables/public/orders; the flag values become the path segments.",
		},
		{
			ID: "121c-L3-F", Req: "L.3", Layer: Scenario121LayerFunctional,
			Command: "storage tables detail --schema public --table orders",
			Method:  "GET", Path: "storage/tables/public/orders",
			Assert: "catalog row well-formed + in-process flag-driven GET assertion",
			Gate:   "ALWAYS",
			Description: "the flag-driven detail read reaches the operator and returns 200 with the " +
				"single-table detail shape (driven through the real ctl client + api.Server).",
		},
		{
			ID: "121c-L3-L", Req: "L.3", Layer: Scenario121LayerLive,
			Command: "storage tables detail --schema public --table <a-real-table>",
			Method:  "GET", Path: "storage/tables/{schema}/{table}",
			Assert: "LIVE tables detail --schema/--table prints detail; exit 0",
			Gate:   "LIVE",
			Description: "[LIVE-ONLY] storage tables detail --schema/--table against the deployed " +
				"cluster prints the table detail (table chosen from the L.2 list; degrade to " +
				"CONFIG-ONLY if no user tables).",
		},
		// L.4 storage recommendations list ----------------------------------
		{
			ID: "121d-L4-U", Req: "L.4", Layer: Scenario121LayerUnit,
			Command: "storage recommendations list", Method: "GET", Path: "storage/recommendations",
			Assert: "GET storage/recommendations; ?namespace= encoded; 1 request",
			Gate:   "ALWAYS",
			Description: "cloudberry-ctl storage recommendations list builds a single GET to " +
				".../storage/recommendations; no body.",
		},
		{
			ID: "121d-L4-F", Req: "L.4", Layer: Scenario121LayerFunctional,
			Command: "storage recommendations list", Method: "GET", Path: "storage/recommendations",
			Assert: "catalog row well-formed + in-process GET assertion",
			Gate:   "ALWAYS",
			Description: "the recommendations list read reaches the operator and returns 200 with " +
				"the recommendationCount / recommendations shape.",
		},
		{
			ID: "121d-L4-L", Req: "L.4", Layer: Scenario121LayerLive,
			Command: "storage recommendations list", Method: "GET", Path: "storage/recommendations",
			Assert: "LIVE recommendations list prints recommendationCount; exit 0",
			Gate:   "LIVE",
			Description: "[LIVE-ONLY] storage recommendations list against the deployed cluster " +
				"prints the active recommendations (recommendationCount); exit 0.",
		},
		// L.5 storage recommendations scan ----------------------------------
		{
			ID: "121e-L5-U", Req: "L.5", Layer: Scenario121LayerUnit,
			Command: "storage recommendations scan", Method: "POST",
			Path:   "storage/recommendations/scan",
			Assert: "POST storage/recommendations/scan (METHOD=POST not GET); 1 request",
			Gate:   "ALWAYS",
			Description: "cloudberry-ctl storage recommendations scan builds a POST (NOT GET) to " +
				".../storage/recommendations/scan; nil/empty body.",
		},
		{
			ID: "121e-L5-F", Req: "L.5", Layer: Scenario121LayerFunctional,
			Command: "storage recommendations scan", Method: "POST",
			Path:   "storage/recommendations/scan",
			Assert: "catalog row well-formed + in-process POST assertion (Operator tier)",
			Gate:   "ALWAYS",
			Description: "the scan POST reaches the operator and is accepted (202) when the scan is " +
				"enabled; an Operator-tier caller is required (RBAC parity).",
		},
		{
			ID: "121e-L5-L", Req: "L.5", Layer: Scenario121LayerLive,
			Command: "storage recommendations scan", Method: "POST",
			Path:   "storage/recommendations/scan",
			Assert: "LIVE scan initiated (202-equivalent); exit 0",
			Gate:   "LIVE",
			Description: "[LIVE-ONLY] storage recommendations scan against the deployed cluster " +
				"initiates a scan (\"scan initiated\" / 202); requires recommendationScan enabled " +
				"(degrade/400 if not).",
		},
		// L.6 storage usage-report --month ----------------------------------
		{
			ID: "121f-L6-U", Req: "L.6", Layer: Scenario121LayerUnit,
			Command: "storage usage-report --month 2026-05", Method: "GET",
			Path:   "storage/usage-report?month=2026-05",
			Assert: "GET storage/usage-report?month=2026-05&namespace=…; 1 request",
			Gate:   "ALWAYS",
			Description: "cloudberry-ctl storage usage-report --month 2026-05 builds a GET to " +
				".../storage/usage-report whose query carries month=2026-05 (the correct reporting " +
				"period). NOTE overlaps 120-C13-cli-month; cross-references the scenario120 test.",
		},
		{
			ID: "121f-L6-F", Req: "L.6", Layer: Scenario121LayerFunctional,
			Command: "storage usage-report --month 2026-05", Method: "GET",
			Path:   "storage/usage-report?month=2026-05",
			Assert: "catalog row well-formed + in-process month= query assertion",
			Gate:   "ALWAYS",
			Description: "the usage-report read threads month=2026-05 to the operator and the report " +
				"echoes that month LABEL (the period round-trips; on-demand, not a snapshot).",
		},
		{
			ID: "121f-L6-L", Req: "L.6", Layer: Scenario121LayerLive,
			Command: "storage usage-report --month 2026-05", Method: "GET",
			Path:   "storage/usage-report?month=2026-05",
			Assert: "LIVE usage-report --month 2026-05 echoes month=2026-05; exit 0",
			Gate:   "LIVE",
			Description: "[LIVE-ONLY] storage usage-report --month 2026-05 against the deployed " +
				"cluster prints the May 2026 report; the month label echoes 2026-05 (correct period; " +
				"requires usageReport enabled). HONEST: on-demand report LABELED by month.",
		},
	}
}

// scenario121EdgeCases returns the named edge rows (task-breakdown §1 edges):
// the four DETAIL-* rows (the L.3 flag work) + the L.6 MONTH-period row.
//
//nolint:funlen // exhaustive named-edge table.
func scenario121EdgeCases() []Scenario121Case {
	return []Scenario121Case{
		{
			ID: "121-DETAIL-flags", Req: "L.3", Layer: Scenario121LayerUnit,
			Command: "storage tables detail --schema public --table orders",
			Method:  "GET", Path: "storage/tables/public/orders",
			Assert: "--schema/--table build .../tables/public/orders (PathEscape'd)",
			Gate:   "ALWAYS",
			Description: "--schema public --table orders builds the path .../storage/tables/public/" +
				"orders. The flag values become the {schema}/{table} path segments (url.PathEscape).",
		},
		{
			ID: "121-DETAIL-positional", Req: "L.3", Layer: Scenario121LayerUnit,
			Command: "storage tables detail public users",
			Method:  "GET", Path: "storage/tables/public/users",
			Assert: "positional [public users] → .../tables/public/users (backward compat)",
			Gate:   "ALWAYS",
			Description: "BACKWARD COMPAT: positional args [public users] still resolve to " +
				".../storage/tables/public/users (the existing main_test.go contract keeps passing).",
		},
		{
			ID: "121-DETAIL-precedence", Req: "L.3", Layer: Scenario121LayerUnit,
			Command: "storage tables detail --schema public --table orders sales legacy",
			Method:  "GET", Path: "storage/tables/public/orders",
			Assert: "both flags + positional → FLAGS win",
			Gate:   "ALWAYS",
			Description: "when BOTH flags and positional args are supplied, flags win (single " +
				"documented rule); the path uses the flag values (public/orders, not sales/legacy).",
		},
		{
			ID: "121-DETAIL-missing", Req: "L.3", Layer: Scenario121LayerUnit,
			Command: "storage tables detail", Method: "", Path: "",
			Assert: "neither flags nor positional → clean usage error, NO HTTP call",
			Gate:   "ALWAYS",
			Description: "neither flags NOR positional args (schema/table both empty) → a CLEAN local " +
				"usage error BEFORE any HTTP call (recorder count() == 0); the message names BOTH " +
				"input forms (--schema/--table or positional args). Never a half-built /tables//.",
		},
		{
			ID: "121-MONTH-period", Req: "L.6", Layer: Scenario121LayerUnit,
			Command: "storage usage-report --month 2026-05", Method: "GET",
			Path:   "storage/usage-report?month=2026-05",
			Assert: "month=2026-05 round-trips (request + report label)",
			Gate:   "ALWAYS",
			Description: "--month=2026-05 → the request carries month=2026-05 AND the report echoes " +
				"that month label (the \"correct reporting period\" = the month param/label round-" +
				"trips). HONEST: on-demand report labeled by month, NOT a persisted snapshot (§2.3).",
		},
	}
}

// scenario121CrossCuttingCases returns the CONTROL (negative) row and the
// PERSIST-L (RBAC/persistence parity) live row.
func scenario121CrossCuttingCases() []Scenario121Case {
	return []Scenario121Case{
		{
			ID: "121-CONTROL", Req: "CONTROL", Layer: Scenario121LayerUnit,
			Command: "storage bogus / <command without --cluster>", Method: "", Path: "",
			Assert: "unknown subcommand / missing --cluster → clean error, NO HTTP call",
			Gate:   "ALWAYS",
			Description: "an unknown storage subcommand (e.g. storage bogus) is a clean cobra " +
				"\"unknown command\" error and issues NO HTTP call (recorder stays empty); each of " +
				"the 6 commands requires --cluster — omitting it is a clean error with NO HTTP call.",
		},
		{
			ID: "121-PERSIST-L", Req: "PERSIST", Layer: Scenario121LayerLive,
			Command: "storage recommendations scan; storage recommendations list",
			Method:  "POST", Path: "storage/recommendations/scan",
			Assert: "LIVE RBAC parity + scan effect persists",
			Gate:   "LIVE",
			Description: "[LIVE-ONLY] persistence/RBAC parity: the read commands (L.1/L.2/L.3/L.4/" +
				"L.6) are Basic-tier; the scan (L.5) is Operator-tier (P.5). A below-tier caller's " +
				"403 is surfaced as a CLI error. The scan's effect persists (a follow-up " +
				"recommendations list reflects the scan). SKIP cleanly when live env absent.",
		},
	}
}
