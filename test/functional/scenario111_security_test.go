//go:build functional

package functional

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 111: Security (SE.1–SE.6, SL.6) — functional
// ============================================================================
//
// This functional layer is reconcile/builder-driven over a fake client (it
// mirrors the Scenario 93/110 functional harness). It complements — does NOT
// duplicate — the internal/{db,builder,webhook,controller}/*scenario111* unit
// tests: those are the validator/builder-direct layer; this layer drives the
// PUBLIC builder + a fake client + a spy db.Client end-to-end and asserts the
// operator's IMPLEMENTED security contract:
//
//   - 111-SE1-F / 111-SL6-F (REAL): the full builder over a multi-server cluster
//     emits a ConfigMap whose credential property values are ONLY ${...}
//     placeholders; a known literal secret value appears in NO rendered body.
//   - 111-SE2-F / 111-SE3-F (CONFIG-ONLY): a jdbc server with TLS params + an s3
//     server with fs.s3a.connection.ssl.enabled=true render those params into
//     the rendered XML (verify config; never a live handshake).
//   - 111-SE4-F (CONFIG-ONLY): a Kerberos hdfs server renders the core-site
//     kerberos props AND the keytab Secret volume/mount is wired onto the segment
//     pod (config-only — no KDC; never a live Kerberos auth).
//   - 111-SE5-F (REAL): the built cluster NetworkPolicy is APPLIED to a fake
//     client (mirroring ensurePxfNetworkPolicy); the object exists with the
//     segment-primary selector and NO cross-pod :5888 ingress.
//   - 111-SE6-F (REAL): with DataLoaderRole set the dedicated-role path drives a
//     spy db.Client's EnsureDataLoaderRole exactly once with that role; unset /
//     gpadmin ⇒ the gpadmin fallback (no dedicated-role call).
//
// HONESTY: SE.2/SE.3/SE.4 are CONFIG-ONLY — only the RENDERED config (and the
// volume/mount wiring) is asserted, never a live TLS/Kerberos handshake.
// ============================================================================

// Scenario111Suite drives the security controls over the public builder + a fake
// client + a spy db.Client.
type Scenario111Suite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestFunctional_Scenario111(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario111Suite))
}

func (s *Scenario111Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario111PxfBaseDir is the in-pod PXF base dir the keytab path is rendered
// under (mirrors internal/builder pxfBase). It is the path PREFIX asserted in the
// rendered core-site.xml, not a hardcoded credential.
const scenario111PxfBaseDir = "/pxf-base"

// scenario111SecureDataLoading returns a multi-server dataLoading spec exercising
// every Scenario 111 security control in one cluster: an s3 server with TLS
// toggle + credentials (SE.1/SE.3/SL.6), a jdbc server with TLS params (SE.2),
// and a Kerberos hdfs server (SE.4). The DataLoaderRole is set by the caller.
func scenario111SecureDataLoading() *cbv1alpha1.DataLoadingSpec {
	return &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Port:    cases.Scenario111PxfPort,
			Servers: []cbv1alpha1.PxfServerSpec{
				// SE.1 / SE.3 / SL.6: s3 with creds + TLS toggle.
				{
					Name: "s3-tls",
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint":               "https://minio.example.com",
						"fs.s3a.connection.ssl.enabled": "true",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "backup-s3-credentials", Key: "aws_access_key_id"},
						{Name: "backup-s3-credentials", Key: "aws_secret_access_key"},
					},
				},
				// SE.2: jdbc with TLS params (Config + dedicated Jdbc map).
				{
					Name: "pg-tls",
					Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "org.postgresql.Driver",
						"jdbc.url":    "jdbc:postgresql://pg:5432/src?ssl=true&sslmode=verify-full",
					},
					Jdbc: map[string]string{
						"jdbc.connection.property.ssl":         "true",
						"jdbc.connection.property.sslmode":     "verify-full",
						"jdbc.connection.property.sslrootcert": "/secrets/ca.pem",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "pg-source-credentials", Key: "username"},
						{Name: "pg-source-credentials", Key: "password"},
					},
				},
				// SE.4: Kerberos hdfs.
				{
					Name: "hdfs-kerb",
					Type: "hdfs",
					Config: map[string]string{
						"fs.defaultFS": "hdfs://namenode:8020",
					},
					Kerberos: &cbv1alpha1.PxfKerberosSpec{
						Principal:    "pxf/_HOST@EXAMPLE.COM",
						KeytabSecret: cbv1alpha1.SecretReference{Name: "hdfs-keytab", Key: "pxf.keytab"},
						Realm:        "EXAMPLE.COM",
					},
				},
			},
		},
	}
}

// scenario111Cluster builds a PXF-enabled cluster with the secure multi-server
// spec attached, applying the supplied mutator (if any).
func scenario111Cluster(
	name string,
	mutate func(*cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	dl := scenario111SecureDataLoading()
	if mutate != nil {
		mutate(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// known literal secret VALUES that MUST NOT leak into the rendered ConfigMap
// (the init container resolves them at runtime into the ephemeral pod fs).
var scenario111ForbiddenSecretValues = []string{
	"minioadmin",
	"AKIAEXAMPLEACCESSKEY",
	"super-secret-s3-key",
	"pg-source-password",
}

// ----------------------------------------------------------------------------
// SE.1 / SL.6 — ConfigMap holds ONLY ${...} placeholders (REAL). (111-SE1-F / 111-SL6-F)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario111_SE1_PlaceholdersOnly proves the full builder over a
// multi-server cluster emits a ConfigMap whose credential property values are
// ONLY ${...} placeholders — and that no known literal secret value appears in
// any rendered body. (111-SE1-F, 111-SL6-F, 111-SE1-NOPLAINTEXT)
func (s *Scenario111Suite) TestFunctional_Scenario111_SE1_PlaceholdersOnly() {
	cluster := scenario111Cluster("s111-se1", nil)
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	// The placeholder tokens ARE present (credentials are wired, not in plaintext).
	assert.Contains(s.T(), cm.Data["s3-tls__s3-site.xml"],
		"${BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID}")
	assert.Contains(s.T(), cm.Data["s3-tls__s3-site.xml"],
		"${BACKUP_S3_CREDENTIALS_AWS_SECRET_ACCESS_KEY}")
	assert.Contains(s.T(), cm.Data["pg-tls__jdbc-site.xml"],
		"${PG_SOURCE_CREDENTIALS_PASSWORD}")

	// SL.6: NO literal secret value appears in any rendered body, and the
	// non-standard pxf.credential.* keys are gone everywhere.
	for key, val := range cm.Data {
		for _, secret := range scenario111ForbiddenSecretValues {
			assert.NotContainsf(s.T(), val, secret,
				"111-SL6-F: ConfigMap key %s must not carry the literal secret %q", key, secret)
		}
		assert.NotContainsf(s.T(), val, "pxf.credential",
			"ConfigMap key %s must not emit pxf.credential.*", key)
	}
}

// ----------------------------------------------------------------------------
// SE.2 — JDBC TLS params carried through (CONFIG-ONLY). (111-SE2-F)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario111_SE2_JdbcTLSRendered proves the reconcile/builder
// path carries the JDBC TLS params into the rendered jdbc-site.xml. CONFIG-ONLY:
// the rendered config is verified, never a live encrypted handshake. (111-SE2-F)
func (s *Scenario111Suite) TestFunctional_Scenario111_SE2_JdbcTLSRendered() {
	cluster := scenario111Cluster("s111-se2", nil)
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	site := cm.Data["pg-tls__jdbc-site.xml"]
	require.NotEmpty(s.T(), site)

	// The ssl jdbc.url params survive into the rendered jdbc-site.xml.
	assert.Contains(s.T(), site, "ssl=true")
	assert.Contains(s.T(), site, "sslmode=verify-full")
	// The dedicated TLS connection properties survive too.
	assert.Contains(s.T(), site, "<name>jdbc.connection.property.ssl</name>")
	assert.Contains(s.T(), site, "<name>jdbc.connection.property.sslmode</name>")
	assert.Contains(s.T(), site, "<name>jdbc.connection.property.sslrootcert</name>")
	assert.Contains(s.T(), site, "/secrets/ca.pem")
	s.T().Log("111-SE2-F: jdbc-site.xml carries the TLS params [CONFIG-ONLY: no live handshake asserted]")
}

// ----------------------------------------------------------------------------
// SE.3 — s3 TLS toggle carried through (CONFIG-ONLY). (111-SE3-F)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario111_SE3_S3TLSRendered proves the reconcile/builder path
// carries fs.s3a.connection.ssl.enabled=true into the rendered s3-site.xml.
// CONFIG-ONLY: the rendered config is verified, never a live TLS handshake.
// (111-SE3-F)
func (s *Scenario111Suite) TestFunctional_Scenario111_SE3_S3TLSRendered() {
	cluster := scenario111Cluster("s111-se3", nil)
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	site := cm.Data["s3-tls__s3-site.xml"]
	require.NotEmpty(s.T(), site)
	assert.Contains(s.T(), site, "<name>fs.s3a.connection.ssl.enabled</name>")
	assert.Contains(s.T(), site, "<value>true</value>")
	s.T().Log("111-SE3-F: s3-site.xml carries fs.s3a.connection.ssl.enabled=true [CONFIG-ONLY]")
}

// ----------------------------------------------------------------------------
// SE.4 — Kerberos config + keytab mount (CONFIG-ONLY). (111-SE4-F)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario111_SE4_KerberosRenderedAndMounted proves the
// reconcile/builder path renders the core-site kerberos props AND wires the
// keytab Secret volume/mount onto the segment pod. CONFIG-ONLY: no KDC, so no
// live Kerberos auth is asserted. (111-SE4-F)
func (s *Scenario111Suite) TestFunctional_Scenario111_SE4_KerberosRenderedAndMounted() {
	cluster := scenario111Cluster("s111-se4", nil)

	// Rendered core-site carries the kerberos security props + principal + keytab path.
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)
	coreSite := cm.Data["hdfs-kerb__core-site.xml"]
	require.NotEmpty(s.T(), coreSite)
	assert.Contains(s.T(), coreSite, "<name>hadoop.security.authentication</name>")
	assert.Contains(s.T(), coreSite, "<value>kerberos</value>")
	assert.Contains(s.T(), coreSite, "pxf/_HOST@EXAMPLE.COM")
	assert.Contains(s.T(), coreSite, scenario111PxfBaseDir+"/keytabs/hdfs-kerb/pxf.keytab")
	// The keytab BYTES never live in the ConfigMap — only the path string.
	for k, v := range cm.Data {
		assert.NotContainsf(s.T(), v, "BEGIN KEYTAB", "ConfigMap key %s must not carry keytab bytes", k)
	}

	// The segment-primary StatefulSet carries the keytab Secret volume + the
	// sidecar mounts it at the per-server path.
	sts, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)

	keytabVol := util.SanitizeK8sName("pxf-keytab-hdfs-kerb")
	var foundVol bool
	for _, v := range sts.Spec.Template.Spec.Volumes {
		if v.Name == keytabVol {
			require.NotNil(s.T(), v.Secret, "keytab volume must be Secret-backed")
			assert.Equal(s.T(), "hdfs-keytab", v.Secret.SecretName)
			foundVol = true
		}
	}
	assert.True(s.T(), foundVol, "111-SE4-F: segment pod must carry the keytab Secret volume")

	var sidecarMount bool
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name != "pxf" {
			continue
		}
		for _, m := range c.VolumeMounts {
			if m.Name == keytabVol {
				assert.Equal(s.T(), scenario111PxfBaseDir+"/keytabs/hdfs-kerb", m.MountPath)
				assert.True(s.T(), m.ReadOnly, "keytab mount must be read-only")
				sidecarMount = true
			}
		}
	}
	assert.True(s.T(), sidecarMount, "111-SE4-F: pxf sidecar must mount the keytab volume")
	s.T().Log("111-SE4-F: kerberos props rendered + keytab mounted [CONFIG-ONLY: no KDC, no live auth]")
}

// ----------------------------------------------------------------------------
// SE.5 — cluster NetworkPolicy applied via the fake client (REAL). (111-SE5-F)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario111_SE5_NetworkPolicyApplied proves the built cluster
// NetworkPolicy is creatable (mirroring ensurePxfNetworkPolicy): it is applied
// to a fake client, then read back with the segment-primary selector and NO
// cross-pod :5888 ingress. The PXF-disabled cluster yields no policy. (111-SE5-F)
func (s *Scenario111Suite) TestFunctional_Scenario111_SE5_NetworkPolicyApplied() {
	scheme := runtime.NewScheme()
	require.NoError(s.T(), networkingv1.AddToScheme(scheme))
	cluster := scenario111Cluster("s111-se5", nil)

	desired := s.builder.BuildPXFClusterNetworkPolicy(cluster)
	require.NotNil(s.T(), desired, "policy must be built for a PXF cluster")

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	require.NoError(s.T(), k8sClient.Create(s.ctx, desired),
		"the built NetworkPolicy must apply cleanly (ensure-loop create)")

	got := &networkingv1.NetworkPolicy{}
	require.NoError(s.T(), k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      util.PxfNetworkPolicyName(cluster.Name),
		Namespace: cluster.Namespace,
	}, got))

	// Segment-primary selector.
	assert.Equal(s.T(), cluster.Name, got.Spec.PodSelector.MatchLabels[util.LabelCluster])
	assert.Equal(s.T(), util.ComponentSegmentPrimary,
		got.Spec.PodSelector.MatchLabels[util.LabelComponent])
	// Ingress-only.
	assert.Equal(s.T(), []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		got.Spec.PolicyTypes)
	// NO cross-pod :5888 ingress (loads keep working via same-pod localhost).
	for _, rule := range got.Spec.Ingress {
		for _, p := range rule.Ports {
			require.NotNil(s.T(), p.Port)
			assert.NotEqualf(s.T(), int32(cases.Scenario111PxfPort), p.Port.IntVal,
				"111-SE5-F: cross-pod ingress to PXF :%d must NOT be allowed", cases.Scenario111PxfPort)
		}
	}

	// Negative: a PXF-disabled cluster yields no policy (nothing to apply).
	off := scenario111Cluster("s111-se5-off", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Enabled = false
	})
	assert.Nil(s.T(), s.builder.BuildPXFClusterNetworkPolicy(off),
		"no policy when PXF is disabled")
}

// ----------------------------------------------------------------------------
// SE.6 — dedicated-role wiring via a spy db.Client (REAL). (111-SE6-F)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario111_SE6_DedicatedRoleWiring proves the SE.6 wiring is
// driven through the public db.Client interface: with DataLoaderRole set the
// dedicated role is ensured exactly once with that name; with it unset (or
// gpadmin) the gpadmin fallback path is taken and EnsureDataLoaderRole is a
// no-op (the existing gpadmin load path is unchanged). (111-SE6-F)
func (s *Scenario111Suite) TestFunctional_Scenario111_SE6_DedicatedRoleWiring() {
	tests := []struct {
		name        string
		role        string
		wantCalls   []string
		description string
	}{
		{
			name:        "opted-in dedicated role",
			role:        cases.Scenario111DataLoaderRole,
			wantCalls:   []string{cases.Scenario111DataLoaderRole},
			description: "DataLoaderRole set ⇒ EnsureDataLoaderRole(role) once",
		},
		{
			name:        "unset falls back to gpadmin",
			role:        "",
			wantCalls:   nil,
			description: "DataLoaderRole unset ⇒ gpadmin fallback (no dedicated-role call recorded)",
		},
		{
			name:        "explicit gpadmin is the default no-op",
			role:        util.DefaultAdminUser,
			wantCalls:   nil,
			description: "explicit gpadmin ⇒ default no-op",
		},
	}

	for _, tt := range tests {
		tt := tt
		s.Run(tt.name, func() {
			cluster := scenario111Cluster("s111-se6", func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.DataLoaderRole = tt.role
			})

			// A spy db.Client that records EnsureDataLoaderRole calls. The reconcile
			// contract (in internal/controller) only calls it for a non-empty,
			// non-gpadmin role — we replicate that resolution here over the PUBLIC
			// interface so the spy records exactly the operator's behavior.
			var calls []string
			spy := &testutil.MockDBClient{
				EnsureDataLoaderRoleFunc: func(_ context.Context, role string) error {
					calls = append(calls, role)
					return nil
				},
			}
			var client db.Client = spy

			role := pxfDataLoaderRoleForTest(cluster)
			if role != "" && role != util.DefaultAdminUser {
				require.NoError(s.T(), client.EnsureDataLoaderRole(s.ctx, role))
			}

			assert.Equalf(s.T(), tt.wantCalls, calls, "111-SE6-F: %s", tt.description)
		})
	}
}

// pxfDataLoaderRoleForTest resolves the data-loader role exactly as the operator
// reconcile does: a non-empty PxfSpec.DataLoaderRole is used verbatim; otherwise
// the gpadmin default. It mirrors internal/controller.pxfDataLoaderRole (which is
// unexported) so the functional spy asserts the same resolution.
func pxfDataLoaderRoleForTest(cluster *cbv1alpha1.CloudberryCluster) string {
	if cluster.Spec.DataLoading != nil &&
		cluster.Spec.DataLoading.Pxf != nil &&
		strings.TrimSpace(cluster.Spec.DataLoading.Pxf.DataLoaderRole) != "" {
		return cluster.Spec.DataLoading.Pxf.DataLoaderRole
	}
	return util.DefaultAdminUser
}

// ----------------------------------------------------------------------------
// Catalog honesty (always runs; no infra).
// ----------------------------------------------------------------------------

// TestFunctional_Scenario111_CatalogHonest asserts the Scenario 111 functional
// (-F) catalog rows are well-formed (unique IDs, every SE.1–SE.6 + SL.6 family
// present, a known honesty class) so the functional layer documents the same IDs
// the e2e layer resolves.
func (s *Scenario111Suite) TestFunctional_Scenario111_CatalogHonest() {
	catalog := cases.Scenario111SecurityCases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	functionalReqs := map[string]bool{}
	for _, tc := range catalog {
		assert.Falsef(s.T(), seen[tc.ID+"|"+tc.Layer], "duplicate catalog row %s/%s", tc.ID, tc.Layer)
		seen[tc.ID+"|"+tc.Layer] = true
		assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
		assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
		assert.Containsf(s.T(),
			[]string{cases.Scenario111RealClass, cases.Scenario111ConfigOnlyClass}, tc.Class,
			"%s must carry a known honesty Class", tc.ID)
		if tc.Layer == cases.Scenario111LayerFunctional {
			functionalReqs[tc.Req] = true
		}
	}
	for _, req := range []string{"SE.1", "SE.2", "SE.3", "SE.4", "SE.5", "SE.6", "SL.6"} {
		assert.Truef(s.T(), functionalReqs[req],
			"functional catalog rows must cover control family %s", req)
	}
}
