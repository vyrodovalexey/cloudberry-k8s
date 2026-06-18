package cases

// ============================================================================
// Scenario 111 — Security (SE.1–SE.6, SL.6)
// ============================================================================
//
// Acceptance scenario: the PXF/data-loading security controls SE.1–SE.6 (+ the
// SL.6 no-plaintext-secret essence) are each accounted for HONESTLY, mirroring
// the Scenario 110 catalog SHAPE (a flat per-control row with a Layer + an
// honesty flag).
//
// HONESTY boundary (carried from the development-plan + task-breakdown):
//   - SE.1 / SL.6 are REAL-provable: the <cluster>-pxf-servers ConfigMap holds
//     ONLY ${...} placeholders (no literal secret); the resolved XML lives only
//     in the ephemeral pod emptyDir (live: ConfigMap scan + sidecar exec).
//   - SE.5 is REAL-provable: a cluster NetworkPolicy confines cross-pod :5888
//     while loads keep working (same-pod localhost is never policy-controlled).
//   - SE.6 is REAL-provable: a dedicated minimal-privilege role gets ONLY the
//     pxf protocol grants, is NOSUPERUSER, and CANNOT do unrelated ops.
//   - SE.2 / SE.3 (source TLS) are CONFIG-ONLY: the rendered jdbc-site.xml /
//     s3-site.xml carry the TLS params; a LIVE encrypted handshake is asserted
//     ONLY if the source actually speaks TLS, else CONFIG-ONLY (never faked).
//   - SE.4 (Kerberos) is CONFIG-ONLY: the keytab Secret is mounted + the
//     hadoop security props are rendered; a LIVE Kerberos handshake is NOT
//     provable (the test env has no KDC) — never faked.
//
// Layers (shared vocabulary):
//   - U : unit  (builder/db/webhook direct — owned in internal/*).
//   - F : functional (reconcile/builder-driven over a fake client; test/functional).
//   - L : live e2e (deployed cluster s111, KUBECONFIG + SCENARIO111_LIVE gated;
//     test/e2e Part B).
// ============================================================================

// Scenario111Layer enumerates the assertion layer a Scenario 111 case resolves
// at, reusing the shared Scenario 104 layer vocabulary
// ("builder"/"reconcile"/"live").
const (
	// Scenario111LayerUnit is the builder/db/webhook-direct unit layer (internal/*).
	Scenario111LayerUnit = Scenario104LayerBuilder
	// Scenario111LayerFunctional is the reconcile/builder-driven functional layer.
	Scenario111LayerFunctional = Scenario104LayerReconcile
	// Scenario111LayerLive is the live (deployed cluster s111) layer.
	Scenario111LayerLive = Scenario104LayerLive
)

// Scenario 111 well-known live defaults (mirror the e2e Part B env defaults).
const (
	// Scenario111Namespace is the default deploy namespace for the live (-L) rows.
	Scenario111Namespace = "cloudberry-test"
	// Scenario111DefaultCluster is the default (SHORT) live cluster name base.
	Scenario111DefaultCluster = "s111"
	// Scenario111PxfPort is the PXF service port that the SE.5 cluster
	// NetworkPolicy deliberately OMITS from the cross-pod ingress set.
	Scenario111PxfPort = 5888
	// Scenario111DataLoaderRole is the example dedicated minimal-privilege role
	// name used in the SE.6 catalog rows (opt-in; empty ⇒ gpadmin fallback).
	Scenario111DataLoaderRole = "cb_dataload"
)

// Scenario111RealClass / Scenario111ConfigOnlyClass tag the honesty class of a
// Scenario 111 case: a REAL row proves the security property end-to-end; a
// CONFIG-ONLY row verifies the RENDERED config (and never fabricates a live
// handshake the env cannot prove).
const (
	// Scenario111RealClass marks a REAL-provable security property.
	Scenario111RealClass = "real"
	// Scenario111ConfigOnlyClass marks a CONFIG-ONLY property (verify rendered
	// config; never fake a live handshake).
	Scenario111ConfigOnlyClass = "config-only"
)

// Scenario111Case describes one Scenario 111 sub-case. It mirrors the
// Scenario110Case SHAPE (a flat catalog row) for the security controls: the
// requirement family (Req), the assertion Layer, the honesty Class (real vs
// config-only), the expected outcome token, and a human Description.
type Scenario111Case struct {
	// ID is the catalog row id (e.g. "111-SE1-U", "111-SE5-LOADOK", "111-SE6-DENY").
	ID string
	// Req is the control family the row proves: "SE.1".."SE.6", "SL.6".
	Req string
	// Layer is the assertion layer: Scenario111LayerUnit / Functional / Live.
	Layer string
	// Class is the honesty class: Scenario111RealClass / Scenario111ConfigOnlyClass.
	Class string
	// Expected is a short human outcome token.
	Expected string
	// Description explains the case + names the security property + the proof.
	Description string
}

// Scenario111SecurityCases returns the full Scenario 111 catalog: the per-control
// -U/-F/-L rows for SE.1–SE.6 + SL.6, plus the honesty rows (SE2/SE3/SE4
// CONFIG-ONLY), the negative control (SE5-LOADOK), the least-privilege row
// (SE6-DENY) and the no-plaintext row (SE1-NOPLAINTEXT). The -U rows are owned in
// internal/* (db/builder/webhook); the -F rows resolve at the functional layer;
// the -L rows require the deployed cluster s111 and are resolved (or SKIP
// cleanly) in the e2e Part B.
func Scenario111SecurityCases() []Scenario111Case {
	cases := []Scenario111Case{}
	cases = append(cases, scenario111UnitCases()...)
	cases = append(cases, scenario111FunctionalCases()...)
	cases = append(cases, scenario111LiveCases()...)
	return cases
}

// scenario111UnitCases returns the builder/db/webhook-direct (-U) rows. These are
// owned by the internal/{db,builder,webhook}/*scenario111*_test.go unit tests;
// the catalog records them so the functional/e2e layers document the same IDs.
//
//nolint:funlen // an exhaustive per-control unit table.
func scenario111UnitCases() []Scenario111Case {
	return []Scenario111Case{
		{
			ID: "111-SE1-U", Req: "SE.1", Layer: Scenario111LayerUnit, Class: Scenario111RealClass,
			Expected:    "ConfigMap credential values are all ${...} placeholders",
			Description: "BuildPXFServersConfigMap output: every credential-derived value is a ${...} token, no literal secret.",
		},
		{
			ID: "111-SE1-NOPLAINTEXT", Req: "SE.1", Layer: Scenario111LayerUnit, Class: Scenario111RealClass,
			Expected:    "no literal secret string anywhere in the ConfigMap",
			Description: "a known secret value never appears in the rendered ConfigMap; only the ${...} placeholder token does.",
		},
		{
			ID: "111-SL6-U", Req: "SL.6", Layer: Scenario111LayerUnit, Class: Scenario111RealClass,
			Expected:    "multi-server ConfigMap holds only placeholders",
			Description: "identical placeholder-only assertion over a multi-server (s3+jdbc) cluster (SL.6 essence == SE.1).",
		},
		{
			ID: "111-SE2-U", Req: "SE.2", Layer: Scenario111LayerUnit, Class: Scenario111ConfigOnlyClass,
			Expected:    "jdbc-site.xml carries the JDBC TLS/ssl params",
			Description: "CONFIG-ONLY: a jdbc server with ssl jdbc.url + ssl connection props renders them into jdbc-site.xml.",
		},
		{
			ID: "111-SE3-U", Req: "SE.3", Layer: Scenario111LayerUnit, Class: Scenario111ConfigOnlyClass,
			Expected:    "s3-site.xml carries fs.s3a.connection.ssl.enabled=true",
			Description: "CONFIG-ONLY: an s3 server with fs.s3a.connection.ssl.enabled=true renders it into s3-site.xml.",
		},
		{
			ID: "111-SE4-U", Req: "SE.4", Layer: Scenario111LayerUnit, Class: Scenario111ConfigOnlyClass,
			Expected:    "core-site kerberos props rendered + keytab mounted",
			Description: "CONFIG-ONLY: a Kerberos hdfs server renders hadoop.security.authentication=kerberos + principal + keytab path; keytab Secret mounted.",
		},
		{
			ID: "111-SE5-U", Req: "SE.5", Layer: Scenario111LayerUnit, Class: Scenario111RealClass,
			Expected:    "cluster NetworkPolicy omits cross-pod :5888",
			Description: "BuildPXFClusterNetworkPolicy: segment-primary selector, ingress 5432+exporters, NO 5888; nil when PXF disabled.",
		},
		{
			ID: "111-SE6-U", Req: "SE.6", Layer: Scenario111LayerUnit, Class: Scenario111RealClass,
			Expected:    "dedicated role gets ONLY pxf protocol grants, NOSUPERUSER",
			Description: "EnsureDataLoaderRole: CREATE ROLE NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN + GRANT SELECT,INSERT ON PROTOCOL pxf only; gpadmin fallback preserved.",
		},
	}
}

// scenario111FunctionalCases returns the functional (-F) rows. These drive the
// builder/reconcile path over a fake client (SE.1/SL.6/SE.2/SE.3/SE.4 render +
// keytab mount, SE.5 ensure-policy, SE.6 spy-db dedicated-role wiring).
//
//nolint:funlen // an exhaustive per-control functional table.
func scenario111FunctionalCases() []Scenario111Case {
	return []Scenario111Case{
		{
			ID: "111-SE1-F", Req: "SE.1", Layer: Scenario111LayerFunctional, Class: Scenario111RealClass,
			Expected:    "rendered ConfigMap Data has only ${...}",
			Description: "full builder over a multi-server cluster: Data carries only ${...} for credential props, no literal secret.",
		},
		{
			ID: "111-SL6-F", Req: "SL.6", Layer: Scenario111LayerFunctional, Class: Scenario111RealClass,
			Expected:    "no literal secret in any rendered body",
			Description: "given a known secret value, assert it appears in NO ConfigMap body; only the placeholder token does.",
		},
		{
			ID: "111-SE2-F", Req: "SE.2", Layer: Scenario111LayerFunctional, Class: Scenario111ConfigOnlyClass,
			Expected:    "reconcile carries the JDBC TLS config through",
			Description: "CONFIG-ONLY: the built ConfigMap <srv>__jdbc-site.xml carries the ssl jdbc.url + connection props.",
		},
		{
			ID: "111-SE3-F", Req: "SE.3", Layer: Scenario111LayerFunctional, Class: Scenario111ConfigOnlyClass,
			Expected:    "reconcile carries the s3 TLS toggle through",
			Description: "CONFIG-ONLY: the built ConfigMap <srv>__s3-site.xml carries fs.s3a.connection.ssl.enabled=true.",
		},
		{
			ID: "111-SE4-F", Req: "SE.4", Layer: Scenario111LayerFunctional, Class: Scenario111ConfigOnlyClass,
			Expected:    "reconcile wires the Kerberos config + keytab volume",
			Description: "CONFIG-ONLY: <srv>__core-site.xml carries kerberos props; the segment pod + sidecar carry the keytab Secret volume/mount.",
		},
		{
			ID: "111-SE5-F", Req: "SE.5", Layer: Scenario111LayerFunctional, Class: Scenario111RealClass,
			Expected:    "ensure-policy creates the NetworkPolicy",
			Description: "the built NetworkPolicy is applied to a fake client; the object exists with the segment-primary selector and NO 5888 ingress.",
		},
		{
			ID: "111-SE6-F", Req: "SE.6", Layer: Scenario111LayerFunctional, Class: Scenario111RealClass,
			Expected:    "spy db: EnsureDataLoaderRole called only when opted-in",
			Description: "with DataLoaderRole set ⇒ a spy db.Client records EnsureDataLoaderRole(role); unset ⇒ gpadmin no-op, not called.",
		},
	}
}

// scenario111LiveCases returns the live (-L) rows. The REAL rows assert the
// security property end-to-end against the deployed cluster s111; the CONFIG-ONLY
// rows verify the rendered config (and never fabricate a live handshake the env
// cannot prove). Every -L row SKIPS cleanly when the live env is absent.
//
//nolint:funlen // an exhaustive per-control live table.
func scenario111LiveCases() []Scenario111Case {
	return []Scenario111Case{
		{
			ID: "111-SE1-L", Req: "SE.1", Layer: Scenario111LayerLive, Class: Scenario111RealClass,
			Expected:    "ConfigMap ${...} only; resolved value only in the pod fs",
			Description: "[LIVE] kubectl get cm <cluster>-pxf-servers shows ${...} (no literal); exec sidecar cat resolves the value in the emptyDir only.",
		},
		{
			ID: "111-SE1-NOPLAINTEXT", Req: "SE.1", Layer: Scenario111LayerLive, Class: Scenario111RealClass,
			Expected:    "the real secret value is ABSENT from the ConfigMap",
			Description: "[LIVE] grep the actual secret from backup-s3-credentials and assert it is ABSENT from the live ConfigMap Data.",
		},
		{
			ID: "111-SL6-L", Req: "SL.6", Layer: Scenario111LayerLive, Class: Scenario111RealClass,
			Expected:    "live ConfigMap has no literal secrets",
			Description: "[LIVE] every credential property value in the live ConfigMap is a ${...} token (no literal secret).",
		},
		{
			ID: "111-SE2-CONFIGONLY", Req: "SE.2", Layer: Scenario111LayerLive, Class: Scenario111ConfigOnlyClass,
			Expected:    "rendered jdbc-site.xml carries the ssl params",
			Description: "[LIVE CONFIG-ONLY] the live jdbc-site.xml carries the ssl jdbc.url params; a real encrypted handshake is asserted ONLY if the source speaks TLS.",
		},
		{
			ID: "111-SE3-CONFIGONLY", Req: "SE.3", Layer: Scenario111LayerLive, Class: Scenario111ConfigOnlyClass,
			Expected:    "rendered s3-site.xml carries fs.s3a.connection.ssl.enabled",
			Description: "[LIVE CONFIG-ONLY] the live s3-site.xml carries fs.s3a.connection.ssl.enabled=true; a real TLS handshake is asserted ONLY if MinIO/S3 speaks TLS.",
		},
		{
			ID: "111-SE4-CONFIGONLY", Req: "SE.4", Layer: Scenario111LayerLive, Class: Scenario111ConfigOnlyClass,
			Expected:    "keytab mounted + core-site kerberos props present",
			Description: "[LIVE CONFIG-ONLY] if a Kerberos hdfs server is deployed: the keytab volume is mounted on the sidecar + core-site has the kerberos props; live Hadoop auth is NOT provable (no KDC).",
		},
		{
			ID: "111-SE5-L", Req: "SE.5", Layer: Scenario111LayerLive, Class: Scenario111RealClass,
			Expected:    "cluster NetworkPolicy exists, no cross-pod :5888",
			Description: "[LIVE] kubectl get netpol shows the cluster NetworkPolicy with the segment-primary selector and NO 5888 ingress rule.",
		},
		{
			ID: "111-SE5-LOADOK", Req: "SE.5", Layer: Scenario111LayerLive, Class: Scenario111RealClass,
			Expected:    "a load still SUCCEEDS/launches under the policy",
			Description: "[LIVE negative control] with the NetworkPolicy applied, a PXF load still SUCCEEDS (same-pod localhost is never policy-controlled).",
		},
		{
			ID: "111-SE6-L", Req: "SE.6", Layer: Scenario111LayerLive, Class: Scenario111RealClass,
			Expected:    "dedicated role exists, NOSUPERUSER, has pxf grants",
			Description: "[LIVE] if a dedicated role is configured: psql asserts it exists, is NOSUPERUSER, and has the pxf protocol grants. gpadmin default ⇒ honest CONFIG-ONLY skip.",
		},
		{
			ID: "111-SE6-DENY", Req: "SE.6", Layer: Scenario111LayerLive, Class: Scenario111RealClass,
			Expected:    "dedicated role CANNOT do an unrelated op",
			Description: "[LIVE least-privilege] psql as the dedicated role: an unrelated/privileged op (e.g. CREATE ROLE) is DENIED with a permission error.",
		},
	}
}
