package cases

// ============================================================================
// Scenario 112 — Disabled States (DIS.1–DIS.3)
// ============================================================================
//
// Acceptance scenario: the data-loading subsystem's DISABLED states (DIS.1–DIS.3)
// are each accounted for HONESTLY, mirroring the Scenario 111 catalog SHAPE (a
// flat per-state row with a Layer + an honesty Class + an Expected token + a
// Description). The catalog documents the SAME IDs the unit/functional/e2e layers
// resolve so every layer agrees on what is proven and where.
//
// HONESTY boundary (carried from the development-plan + task-breakdown):
//   - DIS.1 (dataLoading.enabled:false) is REAL-provable: cleanupDataLoading
//     tears down the gpfdist Deployment/Service/PVC, the dataload Jobs/CronJobs,
//     the gpload control-file ConfigMaps, the <cluster>-pxf-servers ConfigMap and
//     the PXF NetworkPolicy; the segment-primary STS is re-rendered WITHOUT the
//     pxf sidecar; Status.DataLoading is cleared; ConditionDataLoadingConfigured
//     goes False reason DataLoadingDisabled; a one-shot DataLoadingDisabled event
//     fires; the data-loading API reports DATA_LOADING_NOT_ENABLED; re-enable is
//     an idempotent redeploy.
//   - DIS.2 (dataLoading.enabled:true, pxf.enabled:false) is REAL-provable: no
//     pxf sidecar / ConfigMap / NetworkPolicy / extension; gpload jobs still
//     build + run (independent of PXF); the PXF API reports PXF_NOT_ENABLED.
//   - DIS.3 (gpfdist.enabled:false, DL enabled) is REAL-provable for the GC + the
//     local gpload path; the gpfdist-sourced gpload job's dependency-missing
//     signal is the HONEST runtime failure (gpload cannot reach the absent
//     gpfdist host) — NOT a fabricated pre-flight check (HC.4 is gated OFF when
//     gpfdist is disabled, per the §0 caveat of the task-breakdown).
//
// Layers (shared Scenario 104/111 vocabulary):
//   - U : unit       (controller/builder/api-direct — owned in internal/*).
//   - F : functional (reconcile/builder-driven over a fake client; test/functional).
//   - L : live e2e   (deployed cluster s112, KUBECONFIG + SCENARIO112_LIVE gated;
//     test/e2e Part B).
// ============================================================================

// Scenario112Layer enumerates the assertion layer a Scenario 112 case resolves
// at, reusing the shared Scenario 104/111 layer vocabulary.
const (
	// Scenario112LayerUnit is the controller/builder/api-direct unit layer
	// (owned in internal/*).
	Scenario112LayerUnit = Scenario111LayerUnit
	// Scenario112LayerFunctional is the reconcile/builder-driven functional layer.
	Scenario112LayerFunctional = Scenario111LayerFunctional
	// Scenario112LayerLive is the live (deployed cluster s112) layer.
	Scenario112LayerLive = Scenario111LayerLive
)

// Scenario 112 well-known live defaults (mirror the e2e Part B env defaults).
const (
	// Scenario112Namespace is the default deploy namespace for the live (-L) rows.
	Scenario112Namespace = "cloudberry-test"
	// Scenario112DefaultCluster is the default (SHORT) live cluster name base.
	Scenario112DefaultCluster = "s112"
)

// Scenario112RealClass / Scenario112ConfigOnlyClass tag the honesty class of a
// Scenario 112 case, reusing the Scenario 111 honesty vocabulary: a REAL row
// proves the disabled-state property end-to-end; a CONFIG-ONLY row verifies the
// RENDERED config (and never fabricates a signal the env cannot prove — e.g. a
// pre-flight health check that does not run when gpfdist is disabled).
const (
	// Scenario112RealClass marks a REAL-provable disabled-state property.
	Scenario112RealClass = Scenario111RealClass
	// Scenario112ConfigOnlyClass marks a CONFIG-ONLY property.
	Scenario112ConfigOnlyClass = Scenario111ConfigOnlyClass
)

// Scenario112Case describes one Scenario 112 sub-case. It mirrors the
// Scenario111Case SHAPE (a flat catalog row): the requirement family (Req), the
// assertion Layer, the honesty Class (real vs config-only), the expected outcome
// token, and a human Description.
type Scenario112Case struct {
	// ID is the catalog row id (e.g. "112-DIS1-TEARDOWN", "112-DIS3-DEPMISSING").
	ID string
	// Req is the disabled-state family the row proves: "DIS.1".."DIS.3".
	Req string
	// Layer is the assertion layer: Scenario112LayerUnit / Functional / Live.
	Layer string
	// Class is the honesty class: Scenario112RealClass / Scenario112ConfigOnlyClass.
	Class string
	// Expected is a short human outcome token.
	Expected string
	// Description explains the case + names the disabled-state property + the proof.
	Description string
}

// Scenario112DisabledStatesCases returns the full Scenario 112 catalog: the
// per-state -U/-F/-L rows for DIS.1–DIS.3 plus the sub-cases (TEARDOWN /
// APIDISABLED / REENABLE for DIS.1; PXFOFF / GPLOADOK for DIS.2; NOGPFDIST /
// LOCALOK / DEPMISSING for DIS.3). The -U rows are owned in internal/* (the live
// controller/api unit tests); the -F rows resolve at the functional layer; the
// -L rows require the deployed cluster s112 and are resolved (or SKIP cleanly) in
// the e2e Part B.
func Scenario112DisabledStatesCases() []Scenario112Case {
	cases := []Scenario112Case{}
	cases = append(cases, scenario112UnitCases()...)
	cases = append(cases, scenario112FunctionalCases()...)
	cases = append(cases, scenario112LiveCases()...)
	return cases
}

// scenario112UnitCases returns the controller/builder/api-direct (-U) rows. These
// are owned by the internal/{controller,api}/*scenario112*_test.go unit tests;
// the catalog records them so the functional/e2e layers document the same IDs.
//
//nolint:funlen // an exhaustive per-state unit table.
func scenario112UnitCases() []Scenario112Case {
	return []Scenario112Case{
		{
			ID: "112-DIS1-U", Req: "DIS.1", Layer: Scenario112LayerUnit, Class: Scenario112RealClass,
			Expected:    "cleanupDataLoading deletes all stale objects + clears status",
			Description: "cleanupDataLoading over a fake client seeded with gpfdist+Jobs/CronJobs+control-file CMs: all deleted; Status.DataLoading nil; jobs_active=0; condition False reason DataLoadingDisabled.",
		},
		{
			ID: "112-DIS1-U-CMDEL", Req: "DIS.1", Layer: Scenario112LayerUnit, Class: Scenario112RealClass,
			Expected:    "ensurePxfServersConfigMap deletes the stale CM",
			Description: "with pxfSidecarEnabled=false a stale <cluster>-pxf-servers CM is deleted; absent ⇒ no-op; never errors.",
		},
		{
			ID: "112-DIS1-U-LABELSCOPE", Req: "DIS.1", Layer: Scenario112LayerUnit, Class: Scenario112RealClass,
			Expected:    "only this-cluster dataload objects deleted",
			Description: "label+cluster scoping: a foreign cluster's dataload objects AND a same-cluster non-dataload object are untouched.",
		},
		{
			ID: "112-DIS1-APIDISABLED", Req: "DIS.1", Layer: Scenario112LayerUnit, Class: Scenario112RealClass,
			Expected:    "mutations 400 DATA_LOADING_NOT_ENABLED; list/get 200 disabled envelope",
			Description: "the data-loading REST surface: mutating endpoints 400 DATA_LOADING_NOT_ENABLED; list/get 200 disabled envelope; PXF precedence (DL gate first).",
		},
		{
			ID: "112-DIS2-U", Req: "DIS.2", Layer: Scenario112LayerUnit, Class: Scenario112RealClass,
			Expected:    "pxfSidecarEnabled=false ⇒ all PXF builders nil/empty",
			Description: "pxf.enabled=false ⇒ BuildPXFSidecarContainers/Volumes empty; BuildPXFServersConfigMap nil; BuildPXFClusterNetworkPolicy nil.",
		},
		{
			ID: "112-DIS3-U", Req: "DIS.3", Layer: Scenario112LayerUnit, Class: Scenario112RealClass,
			Expected:    "gpfdist off ⇒ reconcileGpfdist takes the deleteGpfdistResources path",
			Description: "gpfdist.enabled=false (DL enabled) ⇒ reconcileGpfdist → deleteGpfdistResources; dataLoadGpfdistEnabled=false.",
		},
	}
}

// scenario112FunctionalCases returns the functional (-F) rows. These drive the
// reconcile/builder path over a fake client.
//
//nolint:funlen // an exhaustive per-state functional table.
func scenario112FunctionalCases() []Scenario112Case {
	return []Scenario112Case{
		{
			ID: "112-DIS1-TEARDOWN", Req: "DIS.1", Layer: Scenario112LayerFunctional, Class: Scenario112RealClass,
			Expected:    "reconcile (disabled) tears down everything",
			Description: "flip enabled true→false + reconcile: gpfdist/ConfigMap/Jobs/CronJobs/NetworkPolicy gone; status cleared; condition False; one DataLoadingDisabled event.",
		},
		{
			ID: "112-DIS1-APIDISABLED", Req: "DIS.1", Layer: Scenario112LayerFunctional, Class: Scenario112RealClass,
			Expected:    "mutations 400; list/get disabled envelope; PXF precedence",
			Description: "DL-disabled: data-loading mutating endpoints 400 DATA_LOADING_NOT_ENABLED; list 200 disabled envelope; a PXF endpoint reports DATA_LOADING_NOT_ENABLED first.",
		},
		{
			ID: "112-DIS1-REENABLE", Req: "DIS.1", Layer: Scenario112LayerFunctional, Class: Scenario112RealClass,
			Expected:    "re-enable reconcile redeploys (idempotent)",
			Description: "false→true + reconcile: gpfdist Deployment/Service + the enabled dataload Job recreated; idempotent on a 2nd pass (no duplicates).",
		},
		{
			ID: "112-DIS2-PXFOFF", Req: "DIS.2", Layer: Scenario112LayerFunctional, Class: Scenario112RealClass,
			Expected:    "no pxf ConfigMap / sidecar / NetworkPolicy",
			Description: "DL on + pxf off: BuildPXFServersConfigMap nil (no <cluster>-pxf-servers CM); no pxf sidecar on the segment STS; no PXF NetworkPolicy.",
		},
		{
			ID: "112-DIS2-GPLOADOK", Req: "DIS.2", Layer: Scenario112LayerFunctional, Class: Scenario112RealClass,
			Expected:    "a gpload job still builds",
			Description: "DL on + pxf off: a gpload-type job still builds a Job (BuildDataLoadJob→gpload path) + its control-file ConfigMap (gpload independent of PXF).",
		},
		{
			ID: "112-DIS3-NOGPFDIST", Req: "DIS.3", Layer: Scenario112LayerFunctional, Class: Scenario112RealClass,
			Expected:    "gpfdist Deployment/Service/PVC GC'd",
			Description: "gpfdist off (DL enabled): deleteGpfdistResources removes the seeded gpfdist Deployment/Service/PVC.",
		},
		{
			ID: "112-DIS3-LOCALOK", Req: "DIS.3", Layer: Scenario112LayerFunctional, Class: Scenario112RealClass,
			Expected:    "a local-source gpload job still builds",
			Description: "gpfdist off + a gpload inputSource.type=local job: the local job builds (no gpfdist dependency); unaffected by the gpfdist GC.",
		},
		{
			ID: "112-DIS3-DEPMISSING", Req: "DIS.3", Layer: Scenario112LayerFunctional, Class: Scenario112ConfigOnlyClass,
			Expected:    "gpfdist-source job targets the absent host (honest runtime failure)",
			Description: "gpfdist off + a gpload inputSource.type=gpfdist job: the gpfdist resources are absent + the local job is unaffected; the honest dependency-missing signal is the RUNTIME gpload failure (no fabricated pre-flight HC — HC.4 is gated off when gpfdist disabled).",
		},
	}
}

// scenario112LiveCases returns the live (-L) rows. The REAL rows assert the
// disabled-state property end-to-end against the deployed cluster s112 (which
// starts enabled + pxf + gpfdist + jobs); the destructive flips are gated behind
// SCENARIO112_LIVE and self-contained (defer-restore to the enabled baseline).
// Every -L row SKIPS cleanly when the live env is absent.
//
//nolint:funlen // an exhaustive per-state live table.
func scenario112LiveCases() []Scenario112Case {
	return []Scenario112Case{
		{
			ID: "112-DIS1-TEARDOWN", Req: "DIS.1", Layer: Scenario112LayerLive, Class: Scenario112RealClass,
			Expected:    "live patch enabled=false ⇒ everything GONE",
			Description: "[LIVE] kubectl patch dataLoading.enabled=false: the pxf sidecar removed from the segment-primary pod; gpfdist Deployment NotFound; <cluster>-pxf-servers CM NotFound; dataload Jobs/CronJobs NotFound; NetworkPolicy NotFound.",
		},
		{
			ID: "112-DIS1-APIDISABLED", Req: "DIS.1", Layer: Scenario112LayerLive, Class: Scenario112RealClass,
			Expected:    "API reports DATA_LOADING_NOT_ENABLED; jobs_active=0 in VM",
			Description: "[LIVE] with DL disabled: the operator REST data-loading endpoints report DATA_LOADING_NOT_ENABLED; cloudberry_data_loading_jobs_active=0 in VictoriaMetrics.",
		},
		{
			ID: "112-DIS1-REENABLE", Req: "DIS.1", Layer: Scenario112LayerLive, Class: Scenario112RealClass,
			Expected:    "live patch enabled=true ⇒ REDEPLOY",
			Description: "[LIVE] kubectl patch dataLoading.enabled=true: gpfdist Deployment back; <cluster>-pxf-servers CM back; pxf sidecar back on the segment-primary pod (generous STS-roll timeouts).",
		},
		{
			ID: "112-DIS2-PXFOFF", Req: "DIS.2", Layer: Scenario112LayerLive, Class: Scenario112RealClass,
			Expected:    "live pxf off ⇒ no pxf sidecar / CM",
			Description: "[LIVE] kubectl patch pxf.enabled=false (DL stays on): no pxf container on the segment pod; no <cluster>-pxf-servers CM.",
		},
		{
			ID: "112-DIS2-GPLOADOK", Req: "DIS.2", Layer: Scenario112LayerLive, Class: Scenario112RealClass,
			Expected:    "a gpload-type job still launches",
			Description: "[LIVE] pxf off, DL on: a gpload-type Job is still created/launched by the operator (gpload independent of PXF).",
		},
		{
			ID: "112-DIS3-NOGPFDIST", Req: "DIS.3", Layer: Scenario112LayerLive, Class: Scenario112RealClass,
			Expected:    "live gpfdist off ⇒ Deployment/Service GONE",
			Description: "[LIVE] kubectl patch gpfdist.enabled=false: <cluster>-gpfdist Deployment + Service NotFound.",
		},
		{
			ID: "112-DIS3-LOCALOK", Req: "DIS.3", Layer: Scenario112LayerLive, Class: Scenario112RealClass,
			Expected:    "a local-source gpload job still launches",
			Description: "[LIVE] gpfdist off: a gpload inputSource.type=local Job still launches (no gpfdist dependency).",
		},
		{
			ID: "112-DIS3-DEPMISSING", Req: "DIS.3", Layer: Scenario112LayerLive, Class: Scenario112RealClass,
			Expected:    "gpfdist-source job ends Failed (honest dependency-missing)",
			Description: "[LIVE] gpfdist off: a gpfdist-source gpload Job ends Failed (gpload cannot reach the absent gpfdist host) → Job Failed / errors; the HONEST runtime signal, not a fabricated pre-flight HC.",
		},
	}
}
