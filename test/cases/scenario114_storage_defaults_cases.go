package cases

// ============================================================================
// Scenario 114 — Mutating Webhook Defaults for storage-recommendation scan
// (D.1–D.6)
// ============================================================================
//
// Acceptance scenario: applying a CloudberryCluster whose
// spec.storage.recommendationScan is ENABLED (enabled: true) with every other
// scan field omitted yields a PERSISTED object carrying all six injected
// defaults D.1–D.6. The defaults are applied ONLY when enabled:true (a disabled
// scan is a no-op) and explicit user-supplied values are always PRESERVED.
//
// The mutating webhook (internal/webhook/mutating.go::setStorageManagementDefaults,
// reached through the public CloudberryClusterDefaulter.Default()) injects:
//   - D.1 schedule            -> "0 3 * * 0"
//   - D.2 bloatThreshold      -> 20
//   - D.3 skewThreshold       -> 50
//   - D.4 ageThreshold        -> int64(500000000)
//   - D.5 indexBloatThreshold -> 30
//   - D.6 scanDuration        -> "2h"
//
// These defaults are enabled-gated, which is precisely why the webhook stays
// authoritative rather than relying on static +kubebuilder:default CRD markers
// (which cannot honor the scan.Enabled gate). The corresponding mutating-
// admission metric is
// cloudberry_webhook_admission_total{webhook="mutating",result="allowed"}
// (defaulting never denies admission).
//
// Layers (reusing the shared layer vocabulary):
//   - U : unit       (internal/webhook — defaulter-direct via the public
//     Default(); owned by internal/webhook/scenario114_defaults_test.go).
//   - F : functional (drive CloudberryClusterDefaulter.Default(ctx, cluster)
//     over a minimal enabled+omitted scan; test/functional).
//   - L : live e2e   (`kubectl apply` minimal enabled scan → GET → assert the
//     six defaults persisted by the deployed webhook; test/e2e Part B,
//     KUBECONFIG + SCENARIO114_LIVE gated).
// ============================================================================

// Scenario114Gate enumerates the enabled-gate state a Scenario 114 case
// exercises. Defaults are injected only on the enabled gate.
const (
	// Scenario114GateEnabled means recommendationScan.enabled == true: the six
	// defaults D.1–D.6 are injected for omitted fields.
	Scenario114GateEnabled = "enabled"
	// Scenario114GateDisabled means recommendationScan.enabled == false: the
	// defaulter is a no-op (no field is injected).
	Scenario114GateDisabled = "disabled"
	// Scenario114GateNone is used for the CONTROL row (nil storage; no scan).
	Scenario114GateNone = "n/a"
)

// Scenario114Layer enumerates the assertion layer of a Scenario 114 case,
// reusing the shared layer vocabulary.
const (
	// Scenario114LayerUnit is the defaulter-direct unit layer (internal/webhook).
	Scenario114LayerUnit = Scenario104LayerBuilder
	// Scenario114LayerFunctional is the public-Default() functional layer.
	Scenario114LayerFunctional = Scenario104LayerReconcile
	// Scenario114LayerLive is the live `kubectl apply` persisted-defaults layer.
	Scenario114LayerLive = Scenario104LayerLive
)

// Scenario 114 well-known live defaults (mirror the e2e Part B env defaults).
const (
	// Scenario114Namespace is the default deploy namespace for the live (-L) rows.
	Scenario114Namespace = "cloudberry-test"
	// Scenario114DefaultCluster is the default (SHORT) live cluster name base.
	Scenario114DefaultCluster = "s114"
)

// Scenario114Case describes one Scenario 114 sub-case. It is a flat catalog row:
// the defaulted Field, the ExpectedValue the webhook injects (rendered as a
// string), and the enabled Gate that governs whether the default is applied.
type Scenario114Case struct {
	// ID is the catalog rule id (e.g. "114-D1-U", "114-D2-F", "114-ALL-omitted-L").
	ID string
	// Req is the rule family the row proves: "D.1".."D.6", "ALL", "PRESERVE",
	// "DISABLED", "CONTROL", "PERSIST".
	Req string
	// Layer is the assertion layer: Scenario114LayerUnit / Functional / Live.
	Layer string
	// Field is the dotted spec path of the defaulted field (empty for the
	// ALL/PRESERVE/DISABLED/CONTROL/PERSIST aggregate rows).
	Field string
	// ExpectedValue is the value the webhook injects on the enabled gate,
	// rendered as a string (empty for the aggregate rows).
	ExpectedValue string
	// Gate is the enabled-gate the row exercises: enabled / disabled / n/a.
	Gate string
	// Expected is a short human outcome token.
	Expected string
	// Description explains the case + the gate rationale.
	Description string
}

// Scenario114Cases returns the full Scenario 114 catalog: the per-D -U/-F/-L
// rows, the ALL-omitted -U/-F/-L aggregate, the PRESERVE -U/-F rows, the
// DISABLED-noop -U/-F rows, the CONTROL row, and the live PERSIST -L row. The -U
// rows are owned in internal/webhook; the -F rows resolve at the functional
// Default() layer; the -L rows require the deployed webhook and are resolved (or
// SKIP cleanly) in the e2e Part B.
func Scenario114Cases() []Scenario114Case {
	cases := []Scenario114Case{}
	cases = append(cases, scenario114PerFieldCases()...)
	cases = append(cases, scenario114AggregateCases()...)
	cases = append(cases, scenario114CrossCuttingCases()...)
	return cases
}

// scenario114Default carries the per-D field metadata shared across the
// U/F/L layers.
type scenario114Default struct {
	req      string
	field    string
	expected string
}

// scenario114Defaults returns the canonical D.1–D.6 field/value table. The
// per-layer rows are generated from it so the U/F/L catalogs cannot drift.
func scenario114Defaults() []scenario114Default {
	return []scenario114Default{
		{req: "D.1", field: "storage.recommendationScan.schedule", expected: "0 3 * * 0"},
		{req: "D.2", field: "storage.recommendationScan.bloatThreshold", expected: "20"},
		{req: "D.3", field: "storage.recommendationScan.skewThreshold", expected: "50"},
		{req: "D.4", field: "storage.recommendationScan.ageThreshold", expected: "500000000"},
		{req: "D.5", field: "storage.recommendationScan.indexBloatThreshold", expected: "30"},
		{req: "D.6", field: "storage.recommendationScan.scanDuration", expected: "2h"},
	}
}

// scenario114PerFieldCases returns the per-D -U/-F/-L rows. For every default
// D.1–D.6 there is a unit (-U), functional (-F), and live (-L) row, each gated
// on enabled:true and carrying the dotted Field + ExpectedValue.
func scenario114PerFieldCases() []Scenario114Case {
	defaults := scenario114Defaults()
	out := make([]Scenario114Case, 0, len(defaults)*3)
	for _, d := range defaults {
		dn := d.req[len("D."):] // "1".."6"
		out = append(out,
			Scenario114Case{
				ID: "114-D" + dn + "-U", Req: d.req, Layer: Scenario114LayerUnit,
				Field: d.field, ExpectedValue: d.expected, Gate: Scenario114GateEnabled,
				Expected: "Default() injects " + d.req,
				Description: "[UNIT] public Default() over an enabled+omitted scan injects " +
					d.req + " (" + d.field + " = " + d.expected + ").",
			},
			Scenario114Case{
				ID: "114-D" + dn + "-F", Req: d.req, Layer: Scenario114LayerFunctional,
				Field: d.field, ExpectedValue: d.expected, Gate: Scenario114GateEnabled,
				Expected: "Default() injects " + d.req,
				Description: "[FUNCTIONAL] drive Default(ctx, cluster) over an enabled+omitted " +
					"scan; " + d.field + " = " + d.expected + ".",
			},
			Scenario114Case{
				ID: "114-D" + dn + "-L", Req: d.req, Layer: Scenario114LayerLive,
				Field: d.field, ExpectedValue: d.expected, Gate: Scenario114GateEnabled,
				Expected: "persisted " + d.req,
				Description: "[LIVE-ONLY] kubectl apply a minimal enabled scan → GET → " +
					d.field + " = " + d.expected + " was persisted by the webhook.",
			},
		)
	}
	return out
}

// scenario114AggregateCases returns the ALL-omitted -U/-F/-L rows: an enabled
// scan with EVERY field omitted gets ALL six defaults injected/persisted at once.
func scenario114AggregateCases() []Scenario114Case {
	return []Scenario114Case{
		{
			ID: "114-ALL-omitted-U", Req: "ALL", Layer: Scenario114LayerUnit,
			Gate: Scenario114GateEnabled, Expected: "all six defaults injected",
			Description: "[UNIT] public Default() over an enabled scan with all six fields " +
				"omitted injects D.1–D.6 in a single pass.",
		},
		{
			ID: "114-ALL-omitted-F", Req: "ALL", Layer: Scenario114LayerFunctional,
			Gate: Scenario114GateEnabled, Expected: "all six defaults injected",
			Description: "[FUNCTIONAL] Default(ctx, cluster) over an enabled scan with all six " +
				"fields omitted injects D.1–D.6.",
		},
		{
			ID: "114-ALL-omitted-L", Req: "ALL", Layer: Scenario114LayerLive,
			Gate: Scenario114GateEnabled, Expected: "all six defaults persisted",
			Description: "[LIVE-ONLY] kubectl apply a minimal enabled scan → GET → all six " +
				"defaults D.1–D.6 were persisted by the deployed webhook.",
		},
	}
}

// scenario114CrossCuttingCases returns the PRESERVE (-U/-F), DISABLED-noop
// (-U/-F), CONTROL, and live PERSIST (-L) rows.
func scenario114CrossCuttingCases() []Scenario114Case {
	return []Scenario114Case{
		{
			ID: "114-PRESERVE-U", Req: "PRESERVE", Layer: Scenario114LayerUnit,
			Gate: Scenario114GateEnabled, Expected: "explicit values preserved",
			Description: "[UNIT] an enabled scan with explicit NON-default values is NOT " +
				"overwritten by Default() (D.1–D.6 preserved).",
		},
		{
			ID: "114-PRESERVE-F", Req: "PRESERVE", Layer: Scenario114LayerFunctional,
			Gate: Scenario114GateEnabled, Expected: "explicit values preserved",
			Description: "[FUNCTIONAL] an enabled scan with explicit NON-default values survives " +
				"Default(ctx, cluster) (D.1–D.6 preserved).",
		},
		{
			ID: "114-DISABLED-noop-U", Req: "DISABLED", Layer: Scenario114LayerUnit,
			Gate: Scenario114GateDisabled, Expected: "no defaults applied",
			Description: "[UNIT] a disabled scan (enabled:false) with omitted fields gets NONE " +
				"of D.1–D.6 from Default() (the enabled gate holds).",
		},
		{
			ID: "114-DISABLED-noop-F", Req: "DISABLED", Layer: Scenario114LayerFunctional,
			Gate: Scenario114GateDisabled, Expected: "no defaults applied",
			Description: "[FUNCTIONAL] a disabled scan with omitted fields gets NONE of D.1–D.6 " +
				"from Default(ctx, cluster) (the enabled gate holds).",
		},
		{
			ID: "114-CONTROL", Req: "CONTROL", Layer: Scenario114LayerFunctional,
			Gate: Scenario114GateNone, Expected: "nil storage stays nil",
			Description: "[FUNCTIONAL] a cluster with nil storage is left with nil storage by " +
				"Default() (no panic, no allocation) — the no-false-positive control.",
		},
		{
			ID: "114-PERSIST-L", Req: "PERSIST", Layer: Scenario114LayerLive,
			Gate: Scenario114GateEnabled, Expected: "GET → six defaults persisted",
			Description: "[LIVE-ONLY] after applying a minimal enabled scan the GET'd object " +
				"carries all six webhook-injected defaults (the persisted-by-webhook contract).",
		},
	}
}
