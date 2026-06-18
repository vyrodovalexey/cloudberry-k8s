package cases

// ============================================================================
// Scenario 110 — Webhook Validation (All Rules) (W.1–W.15)
// ============================================================================
//
// Acceptance scenario: EACH of the 15 data-loading webhook rules must REJECT an
// otherwise-valid CloudberryCluster carrying EXACTLY ONE violation, with a
// DESCRIPTIVE (field-path + reason) error, AND the rejected CR must NOT persist.
//
// Scenario 110 is the COMPLETE, systematic W.1–W.15 LIVE matrix. It is NOT a
// re-implementation of Scenario 89's validator-direct tests; its unique value is
// the per-rule rejection-SOURCE analysis (webhook vs CRD OpenAPI schema vs both)
// plus an explicit CONTROL (a valid CR admits) and a NO-PERSIST guarantee.
//
// Rejection SOURCE (task-breakdown §1):
//   - WEBHOOK  : only internal/webhook/validating.go rejects it (no schema
//     constraint on the field). On a live apply the user sees OUR descriptive
//     message. (11 rules: W.1, W.2×2, W.4×2, W.5×2, W.6, W.7×2, W.9, W.10, W.13, W.14)
//   - SCHEMA   : the CRD OpenAPI (kubebuilder Enum) rejects it at the apiserver
//     BEFORE the webhook runs; the webhook ALSO has the rule (defense-in-depth)
//     but never sees the request. The apiserver enum error IS itself descriptive
//     (it names the field + the allowed values). (3 rules: W.3, W.8, W.15)
//   - BOTH     : expression-dependent — an OMITTED required key → SCHEMA
//     `required`; an EMPTY-STRING value of the same field → WEBHOOK. The -L row
//     uses the omitted-key expression (the natural `kubectl apply` of a YAML
//     missing the field). (2 rules: W.11, W.12)
//
// Layers:
//   - U : unit  (internal/webhook — validator-direct; owned by
//     internal/webhook/scenario110_validation_test.go, the LIVE unit layer).
//   - F : functional (admission via CloudberryClusterValidator.ValidateCreate
//     over a base-valid CR with one violation; test/functional).
//   - L : live e2e (`kubectl apply` → reject → GET NotFound; test/e2e Part B,
//     KUBECONFIG + SCENARIO110_LIVE gated against the deployed Vault-PKI webhook).
// ============================================================================

// Scenario110Source enumerates the rejection source of a Scenario 110 case.
const (
	// Scenario110SourceWebhook means only the webhook rejects it (the user sees
	// our descriptive message on a live apply).
	Scenario110SourceWebhook = "webhook"
	// Scenario110SourceSchema means the CRD OpenAPI enum rejects it at the
	// apiserver before the webhook runs (the apiserver enum error is descriptive).
	Scenario110SourceSchema = "schema"
	// Scenario110SourceBoth means the source is expression-dependent: an omitted
	// required key → schema; an empty-string value → webhook.
	Scenario110SourceBoth = "both"
	// Scenario110SourceNone is used for the CONTROL/NOPERSIST cross-cutting rows.
	Scenario110SourceNone = "n/a"
)

// Scenario110Layer enumerates the assertion layer of a Scenario 110 case,
// reusing the shared layer vocabulary.
const (
	// Scenario110LayerUnit is the validator-direct unit layer (internal/webhook).
	Scenario110LayerUnit = Scenario104LayerBuilder
	// Scenario110LayerFunctional is the admission-entrypoint functional layer.
	Scenario110LayerFunctional = Scenario104LayerReconcile
	// Scenario110LayerLive is the live `kubectl apply` reject + no-persist layer.
	Scenario110LayerLive = Scenario104LayerLive
)

// Scenario 110 well-known live defaults (mirror the e2e Part B env defaults).
const (
	// Scenario110Namespace is the default deploy namespace for the live (-L) rows.
	Scenario110Namespace = "cloudberry-test"
	// Scenario110DefaultCluster is the default (SHORT) live cluster name base.
	Scenario110DefaultCluster = "s110"
)

// Scenario110Case describes one Scenario 110 sub-case. It mirrors the
// Scenario109Case SHAPE (a flat catalog row), adding the rejection Source, the
// single OffendingField, the descriptive ErrorSubstrings expected in the
// rejection, and the NoPersist contract flag.
type Scenario110Case struct {
	// ID is the catalog rule id (e.g. "110-W1-U", "110-W11-L", "110-CONTROL-admit-L").
	ID string
	// Req is the rule family the row proves: "W.1".."W.15", "CONTROL", "NOPERSIST".
	Req string
	// Layer is the assertion layer: Scenario110LayerUnit / Functional / Live.
	Layer string
	// Source is the rejection source: webhook / schema / both / n/a.
	Source string
	// OffendingField is the single mutated field that triggers the rejection.
	OffendingField string
	// ErrorSubstrings are ALL required to appear in the rejection (field path +
	// reason for webhook rows; the apiserver enum/required wording for schema rows).
	ErrorSubstrings []string
	// NoPersist is true when the rejected CR must NOT persist (a follow-up GET is
	// NotFound). It is the explicit no-persist contract for the -L rows.
	NoPersist bool
	// Expected is a short human outcome token.
	Expected string
	// Description explains the case + names the rejection source rationale.
	Description string
}

// Scenario110WebhookCases returns the full Scenario 110 catalog: per-rule
// -U/-F/-L rows (with the OR sub-cases), plus the CONTROL + NOPERSIST
// cross-cutting rows. The -U rows are owned in internal/webhook; the -F rows
// resolve at the functional admission layer; the -L rows require the deployed
// Vault-PKI webhook and are resolved (or SKIP cleanly) in the e2e Part B.
func Scenario110WebhookCases() []Scenario110Case {
	cases := []Scenario110Case{}
	cases = append(cases, scenario110UnitCases()...)
	cases = append(cases, scenario110FunctionalCases()...)
	cases = append(cases, scenario110LiveCases()...)
	cases = append(cases, scenario110CrossCuttingCases()...)
	return cases
}

// scenario110UnitCases returns the validator-direct (-U) rows. The three
// SCHEMA-enum rules (W.3/W.8/W.15) assert the webhook DEFENSE-IN-DEPTH message
// here (the unit layer has no apiserver/CRD); the live source is asserted at -L.
//
//nolint:funlen // an exhaustive per-rule table.
func scenario110UnitCases() []Scenario110Case {
	return []Scenario110Case{
		{
			ID: "110-W1-U", Req: "W.1", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.image",
			ErrorSubstrings: []string{"dataLoading.pxf.image is required when pxf.enabled is true"},
			Expected:        "ValidateCreate rejects empty pxf.image",
			Description:     "pxf.enabled with empty pxf.image → webhook rejects (no schema minLength).",
		},
		{
			ID: "110-W2-empty-U", Req: "W.2", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[0].name",
			ErrorSubstrings: []string{"dataLoading.pxf.servers[0].name", "is required"},
			Expected:        "ValidateCreate rejects empty server name",
			Description:     "server name empty-string passes schema required → webhook rejects.",
		},
		{
			ID: "110-W2-dup-U", Req: "W.2", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[1].name",
			ErrorSubstrings: []string{"dataLoading.pxf.servers[1].name", "is a duplicate"},
			Expected:        "ValidateCreate rejects duplicate server name",
			Description:     "duplicate server name is a cross-element uniqueness check → webhook only.",
		},
		{
			ID: "110-W3-U", Req: "W.3", Layer: Scenario110LayerUnit, Source: Scenario110SourceSchema,
			OffendingField:  "dataLoading.pxf.servers[0].type",
			ErrorSubstrings: []string{"dataLoading.pxf.servers[0].type must be one of", `"ftp"`},
			Expected:        "validator defense-in-depth rejects type ftp",
			Description:     "live source is SCHEMA enum; the -U row asserts the webhook DiD message.",
		},
		{
			ID: "110-W4-endpoint-U", Req: "W.4", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[0].config[fs.s3a.endpoint]",
			ErrorSubstrings: []string{"must include", `"fs.s3a.endpoint"`},
			Expected:        "ValidateCreate rejects s3 server missing endpoint",
			Description:     "config is a free map; the webhook owns the s3 endpoint requirement.",
		},
		{
			ID: "110-W4-creds-U", Req: "W.4", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[0].credentialSecrets",
			ErrorSubstrings: []string{"must include", "credentialSecrets"},
			Expected:        "ValidateCreate rejects s3 server missing credentials",
			Description:     "credentialSecrets is a free array; the webhook owns the requirement.",
		},
		{
			ID: "110-W5-driver-U", Req: "W.5", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[1].config[jdbc.driver]",
			ErrorSubstrings: []string{"must include", `"jdbc.driver"`},
			Expected:        "ValidateCreate rejects jdbc server missing driver",
			Description:     "config is a free map; the webhook owns the jdbc.driver requirement.",
		},
		{
			ID: "110-W5-url-U", Req: "W.5", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[1].config[jdbc.url]",
			ErrorSubstrings: []string{"must include", `"jdbc.url"`},
			Expected:        "ValidateCreate rejects jdbc server missing url",
			Description:     "config is a free map; the webhook owns the jdbc.url requirement.",
		},
		{
			ID: "110-W6-U", Req: "W.6", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[2].config[fs.defaultFS]",
			ErrorSubstrings: []string{"must include", `"fs.defaultFS"`},
			Expected:        "ValidateCreate rejects hdfs server missing fs.defaultFS",
			Description:     "config is a free map; the webhook owns the fs.defaultFS requirement.",
		},
		{
			ID: "110-W7-empty-U", Req: "W.7", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].name",
			ErrorSubstrings: []string{"dataLoading.jobs[0].name", "is required"},
			Expected:        "ValidateCreate rejects empty job name",
			Description:     "job name empty-string passes schema required → webhook rejects.",
		},
		{
			ID: "110-W7-dup-U", Req: "W.7", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[1].name",
			ErrorSubstrings: []string{"dataLoading.jobs[1].name", "is a duplicate"},
			Expected:        "ValidateCreate rejects duplicate job name",
			Description:     "duplicate job name is a cross-element uniqueness check → webhook only.",
		},
		{
			ID: "110-W8-U", Req: "W.8", Layer: Scenario110LayerUnit, Source: Scenario110SourceSchema,
			OffendingField:  "dataLoading.jobs[0].type",
			ErrorSubstrings: []string{`dataLoading.jobs[0].type must be "pxf" or "gpload"`, `"spark"`},
			Expected:        "validator defense-in-depth rejects type spark",
			Description:     "live source is SCHEMA enum; the -U row asserts the webhook DiD message.",
		},
		{
			ID: "110-W9-U", Req: "W.9", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField: "dataLoading.jobs[0].pxfJob.server",
			ErrorSubstrings: []string{
				"dataLoading.jobs[0].pxfJob.server", "does not reference a defined pxf.servers[].name",
			},
			Expected:    "ValidateCreate rejects undefined server reference",
			Description: "undefined-reference cross-check is webhook-only (key present, value bad).",
		},
		{
			ID: "110-W10-U", Req: "W.10", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.profile",
			ErrorSubstrings: []string{"dataLoading.jobs[0].pxfJob.profile", "is not a valid PXF"},
			Expected:        "ValidateCreate rejects invalid profile",
			Description:     "profile has no enum; the webhook owns the profile validity check.",
		},
		{
			ID: "110-W11-U", Req: "W.11", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.targetTable",
			ErrorSubstrings: []string{"dataLoading.jobs[0].pxfJob.targetTable is required"},
			Expected:        "ValidateCreate rejects empty pxfJob.targetTable",
			Description:     "empty-string expression reaches the webhook (the -L row uses omitted-key → SCHEMA).",
		},
		{
			ID: "110-W12-U", Req: "W.12", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[1].gploadJob.targetTable",
			ErrorSubstrings: []string{"dataLoading.jobs[1].gploadJob.targetTable is required"},
			Expected:        "ValidateCreate rejects empty gploadJob.targetTable",
			Description:     "empty-string expression reaches the webhook (the -L row uses omitted-key → SCHEMA).",
		},
		{
			ID: "110-W13-U", Req: "W.13", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].schedule",
			ErrorSubstrings: []string{"dataLoading.jobs[0].schedule is not a valid cron expression"},
			Expected:        "ValidateCreate rejects bad cron schedule",
			Description:     "schedule has no schema pattern; the webhook owns the cron validity check.",
		},
		{
			ID: "110-W14-U", Req: "W.14", Layer: Scenario110LayerUnit, Source: Scenario110SourceWebhook,
			OffendingField: "dataLoading.jobs[0].pxfJob.partitioning",
			ErrorSubstrings: []string{
				"dataLoading.jobs[0].pxfJob.partitioning requires column, range, and interval together",
			},
			Expected:    "ValidateCreate rejects partial partitioning",
			Description: "column without range+interval is a cross-field check; the webhook owns it.",
		},
		{
			ID: "110-W15-U", Req: "W.15", Layer: Scenario110LayerUnit, Source: Scenario110SourceSchema,
			OffendingField:  "dataLoading.jobs[0].pxfJob.errorHandling.segmentRejectLimitType",
			ErrorSubstrings: []string{`segmentRejectLimitType must be "rows" or "percent"`, `"fraction"`},
			Expected:        "validator defense-in-depth rejects fraction",
			Description:     "live source is SCHEMA enum; the -U row asserts the webhook DiD message.",
		},
	}
}

// scenario110FunctionalCases returns the functional (-F) rows. These drive the
// SAME CloudberryClusterValidator.ValidateCreate admission entrypoint over a
// base-valid CR with one violation. The SCHEMA-enum rules assert the webhook DiD
// message at this layer (no apiserver in the functional harness).
//
//nolint:funlen // an exhaustive per-rule table.
func scenario110FunctionalCases() []Scenario110Case {
	return []Scenario110Case{
		{
			ID: "110-W1-F", Req: "W.1", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.image",
			ErrorSubstrings: []string{"dataLoading.pxf.image"},
			Expected:        "admission denies empty pxf.image",
			Description:     "drive ValidateCreate over base-valid CR with empty pxf.image.",
		},
		{
			ID: "110-W2-empty-F", Req: "W.2", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[0].name",
			ErrorSubstrings: []string{"name", "required"},
			Expected:        "admission denies empty server name",
			Description:     "empty server name → ValidateCreate rejects.",
		},
		{
			ID: "110-W2-dup-F", Req: "W.2", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[1].name",
			ErrorSubstrings: []string{"name", "duplicate"},
			Expected:        "admission denies duplicate server name",
			Description:     "duplicate server name → ValidateCreate rejects.",
		},
		{
			ID: "110-W3-F", Req: "W.3", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[0].type",
			ErrorSubstrings: []string{"dataLoading.pxf.servers", "type"},
			Expected:        "admission denies type ftp (DiD)",
			Description:     "webhook defense-in-depth rejects the bad enum (live source is SCHEMA).",
		},
		{
			ID: "110-W4-endpoint-F", Req: "W.4", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[0].config[fs.s3a.endpoint]",
			ErrorSubstrings: []string{"fs.s3a.endpoint"},
			Expected:        "admission denies s3 missing endpoint",
			Description:     "drop fs.s3a.endpoint → ValidateCreate rejects.",
		},
		{
			ID: "110-W4-creds-F", Req: "W.4", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[0].credentialSecrets",
			ErrorSubstrings: []string{"credentialSecrets"},
			Expected:        "admission denies s3 missing credentials",
			Description:     "nil credentialSecrets → ValidateCreate rejects.",
		},
		{
			ID: "110-W5-driver-F", Req: "W.5", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[1].config[jdbc.driver]",
			ErrorSubstrings: []string{"jdbc.driver"},
			Expected:        "admission denies jdbc missing driver",
			Description:     "drop jdbc.driver → ValidateCreate rejects.",
		},
		{
			ID: "110-W5-url-F", Req: "W.5", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[1].config[jdbc.url]",
			ErrorSubstrings: []string{"jdbc.url"},
			Expected:        "admission denies jdbc missing url",
			Description:     "drop jdbc.url → ValidateCreate rejects.",
		},
		{
			ID: "110-W6-F", Req: "W.6", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[2].config[fs.defaultFS]",
			ErrorSubstrings: []string{"fs.defaultFS"},
			Expected:        "admission denies hdfs missing fs.defaultFS",
			Description:     "drop fs.defaultFS → ValidateCreate rejects.",
		},
		{
			ID: "110-W7-empty-F", Req: "W.7", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].name",
			ErrorSubstrings: []string{"name", "required"},
			Expected:        "admission denies empty job name",
			Description:     "empty job name → ValidateCreate rejects.",
		},
		{
			ID: "110-W7-dup-F", Req: "W.7", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[1].name",
			ErrorSubstrings: []string{"name", "duplicate"},
			Expected:        "admission denies duplicate job name",
			Description:     "duplicate job name → ValidateCreate rejects.",
		},
		{
			ID: "110-W8-F", Req: "W.8", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].type",
			ErrorSubstrings: []string{"dataLoading.jobs", "type"},
			Expected:        "admission denies type spark (DiD)",
			Description:     "webhook defense-in-depth rejects the bad enum (live source is SCHEMA).",
		},
		{
			ID: "110-W9-F", Req: "W.9", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.server",
			ErrorSubstrings: []string{"pxfJob.server"},
			Expected:        "admission denies undefined server",
			Description:     "pxfJob.server=does-not-exist → ValidateCreate rejects.",
		},
		{
			ID: "110-W10-F", Req: "W.10", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.profile",
			ErrorSubstrings: []string{"pxfJob.profile"},
			Expected:        "admission denies invalid profile",
			Description:     "pxfJob.profile=s3:nonsense → ValidateCreate rejects.",
		},
		{
			ID: "110-W11-F", Req: "W.11", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.targetTable",
			ErrorSubstrings: []string{"pxfJob.targetTable"},
			Expected:        "admission denies empty pxfJob.targetTable",
			Description:     "empty-string targetTable reaches the webhook → ValidateCreate rejects.",
		},
		{
			ID: "110-W12-F", Req: "W.12", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[1].gploadJob.targetTable",
			ErrorSubstrings: []string{"gploadJob.targetTable"},
			Expected:        "admission denies empty gploadJob.targetTable",
			Description:     "empty-string targetTable reaches the webhook → ValidateCreate rejects.",
		},
		{
			ID: "110-W13-F", Req: "W.13", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].schedule",
			ErrorSubstrings: []string{"schedule"},
			Expected:        "admission denies bad cron",
			Description:     "schedule='not a cron' → ValidateCreate rejects.",
		},
		{
			ID: "110-W14-F", Req: "W.14", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.partitioning",
			ErrorSubstrings: []string{"partitioning"},
			Expected:        "admission denies partial partitioning",
			Description:     "column without range+interval → ValidateCreate rejects.",
		},
		{
			ID: "110-W15-F", Req: "W.15", Layer: Scenario110LayerFunctional, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.errorHandling.segmentRejectLimitType",
			ErrorSubstrings: []string{"segmentRejectLimitType"},
			Expected:        "admission denies fraction (DiD)",
			Description:     "webhook defense-in-depth rejects the bad enum (live source is SCHEMA).",
		},
	}
}

// scenario110LiveCases returns the live (-L) rows. Each row carries the LIVE
// rejection source: WEBHOOK rows assert our descriptive message; SCHEMA rows
// (W.3/W.8/W.15) assert the apiserver enum wording; the BOTH rows (W.11/W.12)
// use the OMITTED-key expression and assert the apiserver `required` wording.
// Every -L row carries the NoPersist contract (a follow-up GET → NotFound).
//
//nolint:funlen // an exhaustive per-rule live table.
func scenario110LiveCases() []Scenario110Case {
	return []Scenario110Case{
		{
			ID: "110-W1-L", Req: "W.1", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField: "dataLoading.pxf.image", ErrorSubstrings: []string{"dataLoading.pxf.image"},
			NoPersist: true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies empty pxf.image with our descriptive message.",
		},
		{
			ID: "110-W2-dup-L", Req: "W.2", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField: "dataLoading.pxf.servers[1].name", ErrorSubstrings: []string{"duplicate"},
			NoPersist: true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies duplicate server name (cross-element uniqueness).",
		},
		{
			ID: "110-W3-L", Req: "W.3", Layer: Scenario110LayerLive, Source: Scenario110SourceSchema,
			OffendingField:  "dataLoading.pxf.servers[0].type",
			ErrorSubstrings: []string{"Unsupported value", "ftp"},
			NoPersist:       true, Expected: "apply denied by CRD enum; GET NotFound",
			Description: "CRD OpenAPI enum rejects type ftp at the apiserver before the webhook.",
		},
		{
			ID: "110-W4-endpoint-L", Req: "W.4", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[0].config[fs.s3a.endpoint]",
			ErrorSubstrings: []string{"fs.s3a.endpoint"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies s3 server missing fs.s3a.endpoint.",
		},
		{
			ID: "110-W5-driver-L", Req: "W.5", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[1].config[jdbc.driver]",
			ErrorSubstrings: []string{"jdbc.driver"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies jdbc server missing jdbc.driver.",
		},
		{
			ID: "110-W6-L", Req: "W.6", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.pxf.servers[2].config[fs.defaultFS]",
			ErrorSubstrings: []string{"fs.defaultFS"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies hdfs server missing fs.defaultFS.",
		},
		{
			ID: "110-W7-dup-L", Req: "W.7", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField: "dataLoading.jobs[1].name", ErrorSubstrings: []string{"duplicate"},
			NoPersist: true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies duplicate job name (cross-element uniqueness).",
		},
		{
			ID: "110-W8-L", Req: "W.8", Layer: Scenario110LayerLive, Source: Scenario110SourceSchema,
			OffendingField:  "dataLoading.jobs[0].type",
			ErrorSubstrings: []string{"Unsupported value", "spark"},
			NoPersist:       true, Expected: "apply denied by CRD enum; GET NotFound",
			Description: "CRD OpenAPI enum rejects type spark at the apiserver before the webhook.",
		},
		{
			ID: "110-W9-L", Req: "W.9", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.server",
			ErrorSubstrings: []string{"pxfJob.server"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies pxfJob.server referencing an undefined server.",
		},
		{
			ID: "110-W10-L", Req: "W.10", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.profile",
			ErrorSubstrings: []string{"pxfJob.profile"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies an invalid pxfJob.profile.",
		},
		{
			ID: "110-W11-L", Req: "W.11", Layer: Scenario110LayerLive, Source: Scenario110SourceBoth,
			OffendingField:  "dataLoading.jobs[0].pxfJob.targetTable",
			ErrorSubstrings: []string{"targetTable"},
			NoPersist:       true, Expected: "apply denied (omitted-key → SCHEMA required); GET NotFound",
			Description: "the -L row OMITS targetTable → CRD required rejects it at the apiserver.",
		},
		{
			ID: "110-W12-L", Req: "W.12", Layer: Scenario110LayerLive, Source: Scenario110SourceBoth,
			OffendingField:  "dataLoading.jobs[1].gploadJob.targetTable",
			ErrorSubstrings: []string{"targetTable"},
			NoPersist:       true, Expected: "apply denied (omitted-key → SCHEMA required); GET NotFound",
			Description: "the -L row OMITS targetTable → CRD required rejects it at the apiserver.",
		},
		{
			ID: "110-W13-L", Req: "W.13", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField: "dataLoading.jobs[0].schedule", ErrorSubstrings: []string{"schedule"},
			NoPersist: true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies an invalid cron schedule.",
		},
		{
			ID: "110-W14-L", Req: "W.14", Layer: Scenario110LayerLive, Source: Scenario110SourceWebhook,
			OffendingField:  "dataLoading.jobs[0].pxfJob.partitioning",
			ErrorSubstrings: []string{"partitioning"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies partitioning column without range+interval.",
		},
		{
			ID: "110-W15-L", Req: "W.15", Layer: Scenario110LayerLive, Source: Scenario110SourceSchema,
			OffendingField:  "dataLoading.jobs[0].pxfJob.errorHandling.segmentRejectLimitType",
			ErrorSubstrings: []string{"Unsupported value", "fraction"},
			NoPersist:       true, Expected: "apply denied by CRD enum; GET NotFound",
			Description: "CRD OpenAPI enum rejects segmentRejectLimitType fraction before the webhook.",
		},
	}
}

// scenario110CrossCuttingCases returns the CONTROL (a valid CR admits) + the
// NOPERSIST contract rows.
func scenario110CrossCuttingCases() []Scenario110Case {
	return []Scenario110Case{
		{
			ID: "110-CONTROL-admit-F", Req: "CONTROL", Layer: Scenario110LayerFunctional,
			Source: Scenario110SourceNone, Expected: "ValidateCreate → NoError",
			Description: "the fully-valid base CR passes admission (proves no false-positive).",
		},
		{
			ID: "110-CONTROL-admit-L", Req: "CONTROL", Layer: Scenario110LayerLive,
			Source: Scenario110SourceNone, Expected: "apply OK; GET found; then deleted",
			Description: "[LIVE-ONLY] a fully-valid CR applies on the live apiserver (no " +
				"false-positive), then is cleaned up.",
		},
		{
			ID: "110-NOPERSIST-L", Req: "NOPERSIST", Layer: Scenario110LayerLive,
			Source: Scenario110SourceNone, NoPersist: true, Expected: "GET → NotFound per rejected rule",
			Description: "[LIVE-ONLY] after each rejected -L apply, GET the name returns NotFound " +
				"(the rejected CR never persisted); realized as a per-rule follow-up GET.",
		},
	}
}
