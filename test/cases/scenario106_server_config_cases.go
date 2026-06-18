package cases

// ============================================================================
// Scenario 106 — Server Configuration Update / Delete (SL.7–SL.8)
// ============================================================================
//
// Acceptance scenario (verbatim): "Patch a PXF server's config (e.g. the
// minio-warehouse fs.s3a.endpoint) → the operator re-renders that server's
// <server>__s3-site.xml in the shared <cluster>-pxf-servers ConfigMap (SL.7) and
// the sidecars pick up the new value on the next sync. Remove a server from
// dataLoading.pxf.servers[] → its <server>__*.xml keys vanish from the ConfigMap
// and any external/foreign table referencing it FAILS until recreated (SL.8)."
//
// HONESTY INVARIANT (project-wide, enforced by every row below):
//   - The PXFServersChanged event AND the cloudberry_pxf_servers_changed_total
//     counter fire ONLY on a REAL ConfigMap Data diff (a server NAME added /
//     removed, or a server's rendered per-file values changed). They NEVER fire
//     on a no-op reconcile, a labels-only change, or a first-time create. The
//     diff is computed by the shared, pure internal/util.DiffPXFServerNames over
//     the existing vs desired rendered Data — the SAME helper the controller
//     (emitPXFServersChanged) and the API sync path (recordPXFServersChanged)
//     consume, so they never disagree.
//   - The re-render is SURGICAL: patching one server's endpoint changes ONLY that
//     server's <server>__s3-site.xml value; the data-key SET is unchanged and
//     every other server's keys stay byte-identical.
//   - A delete drops EXACTLY the removed server's "<server>__*.xml" keys and is
//     prefix-boundary safe (srv vs srv2 are independent).
//   - The live negative (SL.8-L1) is a REAL failing SELECT against a foreign
//     table whose server was deleted — never asserted abstractly.
//
// The -B (builder/unit-provable) rows are deterministic/offline and resolved over
// the builder + the shared util.DiffPXFServerNames/FormatPXFServersChangedMessage
// helpers. The -F (functional/reconcile) rows drive the top-level reconcile / sync
// entrypoint over a fake client + spy metrics + a fake EventRecorder. The -L
// (live/e2e) rows require the deployed operator + a real PXF/DB and are resolved
// (or SKIP cleanly) in the e2e suite.
// ============================================================================

// Scenario106Layer enumerates the assertion layer a Scenario 106 case resolves
// at, sharing the Scenario 104/105 vocabulary ("builder"/"reconcile"/"live").
const (
	// Scenario106LayerBuilder is a pure, byte/logic-provable case resolved over
	// the builder + the shared util.DiffPXFServerNames helper (offline,
	// deterministic).
	Scenario106LayerBuilder = Scenario104LayerBuilder
	// Scenario106LayerReconcile is a functional/controller-level case (the
	// ensurePxfServersConfigMap / syncPXFServersConfigMap paths over a fake
	// client + spy metrics + a fake EventRecorder).
	Scenario106LayerReconcile = Scenario104LayerReconcile
	// Scenario106LayerLive requires a running cluster (live-only).
	Scenario106LayerLive = Scenario104LayerLive
)

// Scenario 106 well-known names + values (mirror the production honesty contract
// in internal/util/pxf.go, the api/v1alpha1 EventReasonPXFServersChanged event
// reason, and the ensurePxfServersConfigMap / syncPXFServersConfigMap paths).
const (
	// Scenario106Namespace is the deploy namespace for the live (-L) rows.
	Scenario106Namespace = "cloudberry-test"
	// Scenario106EventReason is the Normal event reason emitted on a real PXF
	// servers Data diff (api/v1alpha1.EventReasonPXFServersChanged).
	Scenario106EventReason = "PXFServersChanged"
	// Scenario106ChangedMetric is the honest applied-change counter incremented
	// EXACTLY once per real ConfigMap Data diff.
	Scenario106ChangedMetric = "cloudberry_pxf_servers_changed_total"
	// Scenario106UpdateServer is the s3 server whose fs.s3a.endpoint is patched in
	// the SL.7 update rows.
	Scenario106UpdateServer = "minio-warehouse"
	// Scenario106UpdateFile is the rendered per-server file the endpoint routes to
	// (re-rendered on the SL.7 patch).
	Scenario106UpdateFile = "minio-warehouse__s3-site.xml"
)

// Scenario106Case describes one Scenario 106 sub-case. It mirrors the Scenario
// 105Case SHAPE (a small, flat catalog row carrying an ID + the spec requirement
// family + the resolution Layer + a human Expected token + a Description that
// names the HONEST signal and marks [LIVE-ONLY] where the only honest proof is a
// running cluster).
type Scenario106Case struct {
	// ID is the catalog rule id (e.g. "106-SL7-B1", "106-SL8-L1", "106-MX-B1").
	ID string
	// Req is the spec requirement family the row proves: "SL.7" (update/patch),
	// "SL.8" (delete) or "MX" (event/metric honesty).
	Req string
	// Layer is the assertion layer: Scenario106LayerBuilder (pure),
	// Scenario106LayerReconcile (fake client) or Scenario106LayerLive (live-only).
	Layer string
	// Expected is a short outcome token / human description of the asserted
	// outcome.
	Expected string
	// Description explains the case and names the HONEST signal. Live-only rows
	// are marked [LIVE-ONLY].
	Description string
}

// Scenario106Cases returns the full Scenario 106 catalog (task-breakdown §3):
// the per-requirement builder rows (-B), the functional reconcile/sync rows (-F),
// the cross-cutting metric/event honesty rows (-MX) and the live update/delete
// rows (-L). Builder + functional rows are resolved against the shared
// util.DiffPXFServerNames helper + a fake client in the functional + integration
// suites; rows whose only honest signal is a running cluster carry Layer "live"
// (marked [LIVE-ONLY]) and are resolved (or SKIP cleanly) in the e2e suite.
func Scenario106Cases() []Scenario106Case {
	return []Scenario106Case{
		// --- SL.7 — Update (patch endpoint; re-render the changed key) --------
		{
			ID: "106-SL7-B1", Req: "SL.7", Layer: Scenario106LayerBuilder,
			Expected: "patched fs.s3a.endpoint → minio-warehouse__s3-site.xml carries NEW, not OLD",
			Description: "patch fs.s3a.endpoint on the minio-warehouse (type s3) server from " +
				"OLD to NEW; BuildPXFServersConfigMap re-renders so minio-warehouse__s3-site.xml " +
				"CONTAINS the NEW endpoint and does NOT contain the OLD endpoint.",
		},
		{
			ID: "106-SL7-B2", Req: "SL.7", Layer: Scenario106LayerBuilder,
			Expected: "surgical: ONLY minio-warehouse__s3-site.xml changes; key SET unchanged",
			Description: "patching the endpoint changes ONLY the minio-warehouse__s3-site.xml " +
				"value; the set of data keys is unchanged (no key added/removed) and every " +
				"OTHER server's keys stay byte-identical (DiffPXFServerNames → updated=" +
				"[minio-warehouse], added/removed empty).",
		},
		{
			ID: "106-SL7-F1", Req: "SL.7", Layer: Scenario106LayerReconcile,
			Expected: "reconcile/sync persists the NEW endpoint + fires event/metric ONCE",
			Description: "seed the persisted CM with the OLD endpoint, then drive the top-level " +
				"reconcile/sync entrypoint with the patched endpoint: the persisted " +
				"minio-warehouse__s3-site.xml carries the NEW endpoint (old absent) and the " +
				"PXFServersChanged event AND cloudberry_pxf_servers_changed_total increment " +
				"fire EXACTLY ONCE (real diff, updated=[minio-warehouse]).",
		},
		{
			ID: "106-SL7-F2", Req: "SL.7", Layer: Scenario106LayerReconcile,
			Expected: "second identical reconcile → byte-identical → NO event, NO metric",
			Description: "re-reconcile the SAME (already-patched) spec → byte-identical Data → " +
				"NO update, NO event, NO metric increment (honesty: no-op).",
		},
		{
			ID: "106-SL7-L1", Req: "SL.7", Layer: Scenario106LayerLive,
			Expected: "LIVE: patch endpoint → CM regenerates → sidecar resolves NEW endpoint",
			Description: "[LIVE-ONLY] on a PXF+minio cluster, PATCH the minio-warehouse endpoint " +
				"in the CR; wait for (or trigger via pxf sync) CM regeneration; assert the live " +
				"minio-warehouse__s3-site.xml carries the NEW endpoint; if feasible exec a " +
				"segment-primary pxf sidecar and cat the resolved servers/minio-warehouse/" +
				"s3-site.xml to confirm the NEW endpoint; assert the PXFServersChanged event " +
				"was recorded and the counter increased. SKIP cleanly if PXF/minio unavailable.",
		},

		// --- SL.8 — Delete (remove a server; drop its keys; foreign table fails)
		{
			ID: "106-SL8-B1", Req: "SL.8", Layer: Scenario106LayerBuilder,
			Expected: "remove one server → EXACTLY its <server>__*.xml keys absent; others intact",
			Description: "remove one server from pxf.servers[]; BuildPXFServersConfigMap → " +
				"EXACTLY the removed server's <server>__*.xml keys are absent; every OTHER " +
				"server's keys are byte-identical to before.",
		},
		{
			ID: "106-SL8-B2", Req: "SL.8", Layer: Scenario106LayerBuilder,
			Expected: "remove a MULTI-FILE server → ALL its keys gone; prefix-boundary safe",
			Description: "removing a MULTI-FILE server (e.g. hdfs with core- + hdfs-site.xml, " +
				"optional hive/hbase) drops ALL of that server's <server>__*.xml keys and NONE " +
				"belonging to a same-prefixed-but-distinct server name (srv vs srv2 keys are " +
				"independent).",
		},
		{
			ID: "106-SL8-F1", Req: "SL.8", Layer: Scenario106LayerReconcile,
			Expected: "reconcile/sync drops the removed keys + fires event/metric ONCE",
			Description: "seed a CM with N servers, then drive the top-level reconcile/sync with " +
				"a SHRUNK spec (one server removed): the persisted CM full-replaces so the " +
				"removed keys are GONE (others intact) and the PXFServersChanged event AND " +
				"counter increment fire EXACTLY ONCE (removed=[<server>]).",
		},
		{
			ID: "106-SL8-F2", Req: "SL.8", Layer: Scenario106LayerReconcile,
			Expected: "second identical shrunk reconcile → byte-identical → NO event, NO metric",
			Description: "re-reconcile the shrunk spec → byte-identical → NO update, NO event, " +
				"NO metric (honesty: no-op).",
		},
		{
			ID: "106-SL8-L1", Req: "SL.8", Layer: Scenario106LayerLive,
			Expected: "LIVE REAL NEGATIVE: delete server → keys gone → SELECT against ext table ERRORS",
			Description: "[LIVE-ONLY] create (or reuse) an external/foreign table referencing a " +
				"server; PATCH the cluster to REMOVE that server from pxf.servers[]; wait for CM " +
				"regeneration; assert the server's <server>__*.xml keys are GONE; then run a " +
				"SELECT against the external table and assert it ERRORS (a REAL captured query " +
				"error, not asserted abstractly). SKIP cleanly if PXF/DB unavailable.",
		},

		// --- MX — Metric / Event honesty (cross-cutting) ----------------------
		{
			ID: "106-MX-B1", Req: "MX", Layer: Scenario106LayerReconcile,
			Expected: "real diff fires EXACTLY one event + one counter increment",
			Description: "a reconcile/sync whose desired ConfigMap Data differs from existing " +
				"increments cloudberry_pxf_servers_changed_total by exactly 1 and emits ONE " +
				"PXFServersChanged event (the shared util.DiffPXFServerNames + " +
				"FormatPXFServersChangedMessage drive the message).",
		},
		{
			ID: "106-MX-B2", Req: "MX", Layer: Scenario106LayerReconcile,
			Expected: "no-op (byte-identical Data) increments by 0 and emits NO event",
			Description: "a reconcile/sync with byte-identical Data increments the counter by 0 " +
				"and emits NO event (the core honesty invariant).",
		},
		{
			ID: "106-MX-B3", Req: "MX", Layer: Scenario106LayerReconcile,
			Expected: "labels-only change does NOT fire the SERVERS-CHANGED signal",
			Description: "when only Labels differ (Data identical) the CM is updated but the " +
				"counter/event do NOT fire (the signal tracks the SERVER SET, not labels).",
		},
		{
			ID: "106-MX-B4", Req: "MX", Layer: Scenario106LayerReconcile,
			Expected: "PXF-disabled / desired==nil path: no event, no metric, no panic",
			Description: "PXF-disabled / desired == nil path: no event, no metric, no panic " +
				"(nil-recorder + nil-metrics guards) and reconcile returns nil.",
		},
	}
}
