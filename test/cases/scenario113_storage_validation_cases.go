package cases

// ============================================================================
// Scenario 113 — Validation Rules (Negative Tests) for storage-recommendation
// thresholds (W.1–W.4)
// ============================================================================
//
// Acceptance scenario: EACH of the four storage-recommendation threshold rules
// must REJECT an otherwise-valid CloudberryCluster carrying EXACTLY ONE
// out-of-range threshold on spec.storage.recommendationScan, with a DESCRIPTIVE
// (field-path + reason + bad-value) error, AND the rejected CR must NOT persist.
//
// The rule family is gated on recommendationScan.enabled == true. All four rules
// are WEBHOOK-source (Option A): the CRD OpenAPI schema carries NO enum/Min/Max
// markers for these fields, so on a live `kubectl apply` the user always sees OUR
// descriptive message from internal/webhook/validating.go::validateStorageManagement.
//
// Scenario 113 is the COMPLETE, systematic W.1–W.4 matrix. Beyond the rejects it
// proves the symmetric BOUNDARY accepts (bloat/skew/indexBloat = 0 and 100,
// age = 0), and an explicit CONTROL (a fully-valid enabled scan admits) plus a
// NO-PERSIST guarantee on the live path.
//
// Layers (reusing the shared layer vocabulary):
//   - U : unit  (internal/webhook — validator-direct; owned by
//     internal/webhook/scenario113_validation_test.go, the LIVE unit layer).
//   - F : functional (admission via CloudberryClusterValidator.ValidateCreate
//     over a base-valid CR with one mutation; test/functional).
//   - L : live e2e (`kubectl apply` → reject → GET NotFound; test/e2e Part B,
//     KUBECONFIG + SCENARIO113_LIVE gated against the deployed Vault-PKI webhook).
// ============================================================================

// Scenario113Source enumerates the rejection source of a Scenario 113 case. All
// four rules are webhook-authoritative (no CRD schema constraint on the fields).
const (
	// Scenario113SourceWebhook means only the webhook rejects it (the user sees
	// our descriptive message on a live apply).
	Scenario113SourceWebhook = "webhook"
	// Scenario113SourceNone is used for the CONTROL/BOUNDARY/NOPERSIST rows.
	Scenario113SourceNone = "n/a"
)

// Scenario113Layer enumerates the assertion layer of a Scenario 113 case,
// reusing the shared layer vocabulary.
const (
	// Scenario113LayerUnit is the validator-direct unit layer (internal/webhook).
	Scenario113LayerUnit = Scenario104LayerBuilder
	// Scenario113LayerFunctional is the admission-entrypoint functional layer.
	Scenario113LayerFunctional = Scenario104LayerReconcile
	// Scenario113LayerLive is the live `kubectl apply` reject + no-persist layer.
	Scenario113LayerLive = Scenario104LayerLive
)

// Scenario 113 well-known live defaults (mirror the e2e Part B env defaults).
const (
	// Scenario113Namespace is the default deploy namespace for the live (-L) rows.
	Scenario113Namespace = "cloudberry-test"
	// Scenario113DefaultCluster is the default (SHORT) live cluster name base.
	Scenario113DefaultCluster = "s113"
)

// Scenario113Case describes one Scenario 113 sub-case. It mirrors the
// Scenario110Case SHAPE (a flat catalog row): the rejection Source, the single
// OffendingField, the descriptive ErrorSubstrings expected in the rejection, and
// the NoPersist contract flag.
type Scenario113Case struct {
	// ID is the catalog rule id (e.g. "113-W1-150-U", "113-W2-F", "113-BOUNDARY-bloat0-F").
	ID string
	// Req is the rule family the row proves: "W.1".."W.4", "BOUNDARY",
	// "CONTROL", "NOPERSIST".
	Req string
	// Layer is the assertion layer: Scenario113LayerUnit / Functional / Live.
	Layer string
	// Source is the rejection source: webhook / n/a.
	Source string
	// OffendingField is the single mutated field that triggers the rejection
	// (empty for the BOUNDARY/CONTROL/NOPERSIST rows).
	OffendingField string
	// ErrorSubstrings are ALL required to appear in the rejection (field path +
	// reason + bad value for the webhook reject rows).
	ErrorSubstrings []string
	// NoPersist is true when the rejected CR must NOT persist (a follow-up GET is
	// NotFound). It is the explicit no-persist contract for the -L reject rows.
	NoPersist bool
	// Expected is a short human outcome token.
	Expected string
	// Description explains the case + names the rejection source rationale.
	Description string
}

// Scenario113Cases returns the full Scenario 113 catalog: per-rule -U/-F/-L reject
// rows, the BOUNDARY accepts, and the CONTROL + NOPERSIST cross-cutting rows. The
// -U rows are owned in internal/webhook; the -F rows resolve at the functional
// admission layer; the -L rows require the deployed Vault-PKI webhook and are
// resolved (or SKIP cleanly) in the e2e Part B.
func Scenario113Cases() []Scenario113Case {
	cases := []Scenario113Case{}
	cases = append(cases, scenario113UnitCases()...)
	cases = append(cases, scenario113FunctionalCases()...)
	cases = append(cases, scenario113LiveCases()...)
	cases = append(cases, scenario113CrossCuttingCases()...)
	return cases
}

// scenario113UnitCases returns the validator-direct (-U) rows. These mirror the
// owned unit matrix in internal/webhook/scenario113_validation_test.go.
func scenario113UnitCases() []Scenario113Case {
	return []Scenario113Case{
		{
			ID: "113-W1-150-U", Req: "W.1", Layer: Scenario113LayerUnit, Source: Scenario113SourceWebhook,
			OffendingField: "storage.recommendationScan.bloatThreshold",
			ErrorSubstrings: []string{
				"storage.recommendationScan.bloatThreshold", "must be between 0 and 100", "150",
			},
			Expected:    "ValidateCreate rejects bloatThreshold=150",
			Description: "bloatThreshold above the upper bound → webhook rejects (no schema Max).",
		},
		{
			ID: "113-W1-neg1-U", Req: "W.1", Layer: Scenario113LayerUnit, Source: Scenario113SourceWebhook,
			OffendingField: "storage.recommendationScan.bloatThreshold",
			ErrorSubstrings: []string{
				"storage.recommendationScan.bloatThreshold", "must be between 0 and 100", "-1",
			},
			Expected:    "ValidateCreate rejects bloatThreshold=-1",
			Description: "bloatThreshold below the lower bound → webhook rejects (no schema Min).",
		},
		{
			ID: "113-W2-U", Req: "W.2", Layer: Scenario113LayerUnit, Source: Scenario113SourceWebhook,
			OffendingField: "storage.recommendationScan.skewThreshold",
			ErrorSubstrings: []string{
				"storage.recommendationScan.skewThreshold", "must be between 0 and 100", "101",
			},
			Expected:    "ValidateCreate rejects skewThreshold=101",
			Description: "skewThreshold above the upper bound → webhook rejects (no schema Max).",
		},
		{
			ID: "113-W3-U", Req: "W.3", Layer: Scenario113LayerUnit, Source: Scenario113SourceWebhook,
			OffendingField: "storage.recommendationScan.indexBloatThreshold",
			ErrorSubstrings: []string{
				"storage.recommendationScan.indexBloatThreshold", "must be between 0 and 100", "200",
			},
			Expected:    "ValidateCreate rejects indexBloatThreshold=200",
			Description: "indexBloatThreshold above the upper bound → webhook rejects (no schema Max).",
		},
		{
			ID: "113-W4-U", Req: "W.4", Layer: Scenario113LayerUnit, Source: Scenario113SourceWebhook,
			OffendingField: "storage.recommendationScan.ageThreshold",
			ErrorSubstrings: []string{
				"storage.recommendationScan.ageThreshold", "must be non-negative", "-5",
			},
			Expected:    "ValidateCreate rejects ageThreshold=-5",
			Description: "ageThreshold negative → webhook rejects (no schema Min).",
		},
	}
}

// scenario113FunctionalCases returns the functional (-F) rows. These drive the
// SAME CloudberryClusterValidator.ValidateCreate admission entrypoint over a
// base-valid CR with one mutation. The BOUNDARY accepts + CONTROL admit are
// resolved here too (a base-valid mutation that ADMITS).
//
//nolint:funlen // an exhaustive per-rule table.
func scenario113FunctionalCases() []Scenario113Case {
	return []Scenario113Case{
		{
			ID: "113-W1-150-F", Req: "W.1", Layer: Scenario113LayerFunctional, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.bloatThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.bloatThreshold", "150"},
			Expected:        "admission denies bloatThreshold=150",
			Description:     "drive ValidateCreate over base-valid CR with bloatThreshold=150.",
		},
		{
			ID: "113-W1-neg1-F", Req: "W.1", Layer: Scenario113LayerFunctional, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.bloatThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.bloatThreshold", "-1"},
			Expected:        "admission denies bloatThreshold=-1",
			Description:     "drive ValidateCreate over base-valid CR with bloatThreshold=-1.",
		},
		{
			ID: "113-W2-F", Req: "W.2", Layer: Scenario113LayerFunctional, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.skewThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.skewThreshold", "101"},
			Expected:        "admission denies skewThreshold=101",
			Description:     "drive ValidateCreate over base-valid CR with skewThreshold=101.",
		},
		{
			ID: "113-W3-F", Req: "W.3", Layer: Scenario113LayerFunctional, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.indexBloatThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.indexBloatThreshold", "200"},
			Expected:        "admission denies indexBloatThreshold=200",
			Description:     "drive ValidateCreate over base-valid CR with indexBloatThreshold=200.",
		},
		{
			ID: "113-W4-F", Req: "W.4", Layer: Scenario113LayerFunctional, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.ageThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.ageThreshold", "-5"},
			Expected:        "admission denies ageThreshold=-5",
			Description:     "drive ValidateCreate over base-valid CR with ageThreshold=-5.",
		},
		{
			ID: "113-BOUNDARY-bloat0-F", Req: "BOUNDARY", Layer: Scenario113LayerFunctional,
			Source: Scenario113SourceNone, Expected: "ValidateCreate → NoError (bloat=0)",
			Description: "lower-bound bloatThreshold=0 ADMITS (inclusive bound).",
		},
		{
			ID: "113-BOUNDARY-bloat100-F", Req: "BOUNDARY", Layer: Scenario113LayerFunctional,
			Source: Scenario113SourceNone, Expected: "ValidateCreate → NoError (bloat=100)",
			Description: "upper-bound bloatThreshold=100 ADMITS (inclusive bound).",
		},
		{
			ID: "113-BOUNDARY-skew0-F", Req: "BOUNDARY", Layer: Scenario113LayerFunctional,
			Source: Scenario113SourceNone, Expected: "ValidateCreate → NoError (skew=0)",
			Description: "lower-bound skewThreshold=0 ADMITS (inclusive bound).",
		},
		{
			ID: "113-BOUNDARY-skew100-F", Req: "BOUNDARY", Layer: Scenario113LayerFunctional,
			Source: Scenario113SourceNone, Expected: "ValidateCreate → NoError (skew=100)",
			Description: "upper-bound skewThreshold=100 ADMITS (inclusive bound).",
		},
		{
			ID: "113-BOUNDARY-indexBloat0-F", Req: "BOUNDARY", Layer: Scenario113LayerFunctional,
			Source: Scenario113SourceNone, Expected: "ValidateCreate → NoError (indexBloat=0)",
			Description: "lower-bound indexBloatThreshold=0 ADMITS (inclusive bound).",
		},
		{
			ID: "113-BOUNDARY-indexBloat100-F", Req: "BOUNDARY", Layer: Scenario113LayerFunctional,
			Source: Scenario113SourceNone, Expected: "ValidateCreate → NoError (indexBloat=100)",
			Description: "upper-bound indexBloatThreshold=100 ADMITS (inclusive bound).",
		},
		{
			ID: "113-BOUNDARY-age0-F", Req: "BOUNDARY", Layer: Scenario113LayerFunctional,
			Source: Scenario113SourceNone, Expected: "ValidateCreate → NoError (age=0)",
			Description: "lower-bound ageThreshold=0 ADMITS (non-negative inclusive bound).",
		},
		{
			ID: "113-CONTROL-admit-F", Req: "CONTROL", Layer: Scenario113LayerFunctional,
			Source: Scenario113SourceNone, Expected: "ValidateCreate → NoError",
			Description: "the fully-valid enabled-scan base CR passes admission (no false-positive).",
		},
	}
}

// scenario113LiveCases returns the live (-L) reject rows. Every rule is
// WEBHOOK-sourced, so each row asserts OUR descriptive message on the live apply
// and carries the NoPersist contract (a follow-up GET → NotFound). The CONTROL +
// NOPERSIST cross-cutting rows live in scenario113CrossCuttingCases.
func scenario113LiveCases() []Scenario113Case {
	return []Scenario113Case{
		{
			ID: "113-W1-150-L", Req: "W.1", Layer: Scenario113LayerLive, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.bloatThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.bloatThreshold", "150"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies bloatThreshold=150 with our descriptive message.",
		},
		{
			ID: "113-W1-neg1-L", Req: "W.1", Layer: Scenario113LayerLive, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.bloatThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.bloatThreshold", "-1"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies bloatThreshold=-1 with our descriptive message.",
		},
		{
			ID: "113-W2-L", Req: "W.2", Layer: Scenario113LayerLive, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.skewThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.skewThreshold", "101"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies skewThreshold=101 with our descriptive message.",
		},
		{
			ID: "113-W3-L", Req: "W.3", Layer: Scenario113LayerLive, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.indexBloatThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.indexBloatThreshold", "200"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies indexBloatThreshold=200 with our descriptive message.",
		},
		{
			ID: "113-W4-L", Req: "W.4", Layer: Scenario113LayerLive, Source: Scenario113SourceWebhook,
			OffendingField:  "storage.recommendationScan.ageThreshold",
			ErrorSubstrings: []string{"storage.recommendationScan.ageThreshold", "-5"},
			NoPersist:       true, Expected: "apply denied; GET NotFound",
			Description: "live webhook denies ageThreshold=-5 with our descriptive message.",
		},
	}
}

// scenario113CrossCuttingCases returns the CONTROL (a valid enabled scan admits)
// + the NOPERSIST contract rows.
func scenario113CrossCuttingCases() []Scenario113Case {
	return []Scenario113Case{
		{
			ID: "113-CONTROL-admit-L", Req: "CONTROL", Layer: Scenario113LayerLive,
			Source: Scenario113SourceNone, Expected: "apply OK; GET found; then deleted",
			Description: "[LIVE-ONLY] a fully-valid enabled-scan CR applies on the live apiserver " +
				"(no false-positive), then is cleaned up.",
		},
		{
			ID: "113-NOPERSIST-L", Req: "NOPERSIST", Layer: Scenario113LayerLive,
			Source: Scenario113SourceNone, NoPersist: true, Expected: "GET → NotFound per rejected rule",
			Description: "[LIVE-ONLY] after each rejected -L apply, GET the name returns NotFound " +
				"(the rejected CR never persisted); realized as a per-rule follow-up GET.",
		},
	}
}
