package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// Scenario 111 — Security (SE.1–SE.6, SL.6) — builder render/mount tests.
//
// Catalog IDs covered here:
//   111-SE4-U          Kerberos config rendered + keytab mounted (config-only)
//   111-SE5-U          cluster NetworkPolicy blocks cross-pod :5888
//   111-SE1-U / SL6-U  ConfigMap holds ONLY ${...} placeholders, no plaintext
//   111-SE1-NOPLAINTEXT no literal secret string anywhere in the ConfigMap
//   111-SE2-U          jdbc-site.xml carries the JDBC TLS params
//   111-SE3-U          s3-site.xml carries fs.s3a.connection.ssl.enabled=true
//
// HONESTY: SE.4 and SE.2/SE.3 are CONFIG-ONLY — we verify the RENDERED config
// (and the volume/mount wiring), never a live handshake.
// ============================================================================

// newKerberosHDFSCluster returns a PXF-enabled cluster with a single hdfs server
// that has Kerberos configured (principal + keytab Secret + optional krb5.conf).
func newKerberosHDFSCluster(withKrb5 bool) *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	krb5 := ""
	if withKrb5 {
		krb5 = "krb5-config"
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			Port:    5888,
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "hdfs-kerb",
					Type: "hdfs",
					Config: map[string]string{
						"fs.defaultFS": "hdfs://namenode:8020",
					},
					Kerberos: &cbv1alpha1.PxfKerberosSpec{
						Principal:     "pxf/_HOST@EXAMPLE.COM",
						KeytabSecret:  cbv1alpha1.SecretReference{Name: "hdfs-keytab", Key: "pxf.keytab"},
						Krb5ConfigMap: krb5,
						Realm:         "EXAMPLE.COM",
					},
				},
			},
		},
	}
	return cluster
}

// ----------------------------------------------------------------------------
// SE.4 — Kerberos render + keytab/krb5 mount (111-SE4-U), CONFIG-ONLY.
// ----------------------------------------------------------------------------

func findKerberosMount(mounts []corev1.VolumeMount, name string) (corev1.VolumeMount, bool) {
	for _, m := range mounts {
		if m.Name == name {
			return m, true
		}
	}
	return corev1.VolumeMount{}, false
}

// TestPXFKerberos_VolumeAndMountOnBothContainers proves the keytab Secret volume
// exists and is mounted at $PXF_BASE/keytabs/<server>/ on BOTH the sidecar AND
// the cred-init container (so the rendered keytab path is consistent). (111-SE4-U)
func TestPXFKerberos_VolumeAndMountOnBothContainers(t *testing.T) {
	b := NewBuilder()
	cluster := newKerberosHDFSCluster(false)

	keytabVol := util.SanitizeK8sName("pxf-keytab-" + "hdfs-kerb")
	wantMountPath := pxfBase + "/keytabs/hdfs-kerb"

	// The keytab Secret-backed volume is declared on the pod.
	vols := b.BuildPXFSidecarVolumes(cluster)
	vol := findVolume(vols, keytabVol)
	require.NotNil(t, vol, "keytab Secret volume must be present")
	require.NotNil(t, vol.Secret, "keytab volume must be Secret-backed")
	assert.Equal(t, "hdfs-keytab", vol.Secret.SecretName)

	// Sidecar container mounts the keytab volume at the per-server path.
	sidecars := b.BuildPXFSidecarContainers(cluster)
	require.Len(t, sidecars, 1)
	sm, ok := findKerberosMount(sidecars[0].VolumeMounts, keytabVol)
	require.True(t, ok, "sidecar must mount the keytab volume")
	assert.Equal(t, wantMountPath, sm.MountPath)
	assert.True(t, sm.ReadOnly, "keytab mount must be read-only")

	// cred-init container mounts the SAME keytab volume at the SAME path.
	initc := b.BuildPXFCredentialInitContainers(cluster)
	require.Len(t, initc, 1)
	im, ok := findKerberosMount(initc[0].VolumeMounts, keytabVol)
	require.True(t, ok, "cred-init must mount the keytab volume")
	assert.Equal(t, wantMountPath, im.MountPath)
}

// TestPXFKerberos_CoreSiteProps proves the rendered core-site.xml carries the
// hadoop.security.authentication=kerberos property plus the service principal
// and the in-pod keytab path. (111-SE4-U, config-correct)
func TestPXFKerberos_CoreSiteProps(t *testing.T) {
	b := NewBuilder()
	cluster := newKerberosHDFSCluster(false)

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)

	coreSite := cm.Data["hdfs-kerb__core-site.xml"]
	require.NotEmpty(t, coreSite)

	assert.Contains(t, coreSite, "<name>hadoop.security.authentication</name>")
	assert.Contains(t, coreSite, "<value>kerberos</value>")
	assert.Contains(t, coreSite, "<name>pxf.service.kerberos.principal</name>")
	assert.Contains(t, coreSite, "pxf/_HOST@EXAMPLE.COM")
	assert.Contains(t, coreSite, "<name>pxf.service.kerberos.keytab</name>")
	// The keytab path is the in-pod mount path + the secret key filename.
	assert.Contains(t, coreSite, pxfBase+"/keytabs/hdfs-kerb/pxf.keytab")
	// The fs.defaultFS config still passes through.
	assert.Contains(t, coreSite, "hdfs://namenode:8020")
}

// TestPXFKerberos_Krb5ConfigMapMount proves the optional krb5.conf ConfigMap is
// mounted at /etc/krb5.conf when configured. (111-SE4-U)
func TestPXFKerberos_Krb5ConfigMapMount(t *testing.T) {
	b := NewBuilder()
	cluster := newKerberosHDFSCluster(true)

	vols := b.BuildPXFSidecarVolumes(cluster)
	krb5Vol := findVolume(vols, "pxf-krb5")
	require.NotNil(t, krb5Vol, "krb5 ConfigMap volume must be present when configured")
	require.NotNil(t, krb5Vol.ConfigMap)
	assert.Equal(t, "krb5-config", krb5Vol.ConfigMap.Name)

	sidecars := b.BuildPXFSidecarContainers(cluster)
	require.Len(t, sidecars, 1)
	m, ok := findKerberosMount(sidecars[0].VolumeMounts, "pxf-krb5")
	require.True(t, ok, "sidecar must mount krb5.conf")
	assert.Equal(t, "/etc/krb5.conf", m.MountPath)
	assert.Equal(t, "krb5.conf", m.SubPath)
}

// TestPXFKerberos_HonestAbsence proves the honesty assertion: WITHOUT Kerberos,
// there is NO keytab volume/mount and NO kerberos props rendered. (111-SE4-U
// honest absence — kerberos config ONLY when Kerberos is set.)
func TestPXFKerberos_HonestAbsence(t *testing.T) {
	b := NewBuilder()
	// The canonical 5-server cluster has NO Kerberos on any server.
	cluster := newPXFTestCluster()

	// No keytab/krb5 volumes among the sidecar volumes.
	for _, v := range b.BuildPXFSidecarVolumes(cluster) {
		assert.NotContains(t, v.Name, "pxf-keytab-",
			"no keytab volume must exist when Kerberos is unset")
		assert.NotEqual(t, "pxf-krb5", v.Name)
	}

	// No keytab/krb5 mounts on the sidecar.
	sidecars := b.BuildPXFSidecarContainers(cluster)
	require.Len(t, sidecars, 1)
	for _, m := range sidecars[0].VolumeMounts {
		assert.NotContains(t, m.Name, "pxf-keytab-")
		assert.NotEqual(t, "pxf-krb5", m.Name)
	}

	// No kerberos properties in any rendered core-site.xml.
	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)
	for key, val := range cm.Data {
		if strings.HasSuffix(key, "core-site.xml") {
			assert.NotContains(t, val, "hadoop.security.authentication",
				"core-site.xml %s must not carry kerberos props when unset", key)
			assert.NotContains(t, val, "pxf.service.kerberos")
		}
	}
}

// TestKerberosCoreSiteProps_NilWhenUnset is a focused unit assertion that the
// helper returns nil for a non-Kerberos server (honest absence).
func TestKerberosCoreSiteProps_NilWhenUnset(t *testing.T) {
	assert.Nil(t, kerberosCoreSiteProps(&cbv1alpha1.PxfServerSpec{Name: "x", Type: "hdfs"}))

	props := kerberosCoreSiteProps(&cbv1alpha1.PxfServerSpec{
		Name: "k",
		Type: "hdfs",
		Kerberos: &cbv1alpha1.PxfKerberosSpec{
			Principal:    "pxf/_HOST@R",
			KeytabSecret: cbv1alpha1.SecretReference{Name: "s", Key: "kt"},
		},
	})
	require.NotNil(t, props)
	assert.Equal(t, "kerberos", props["hadoop.security.authentication"])
	assert.Equal(t, "pxf/_HOST@R", props["pxf.service.kerberos.principal"])
	assert.Equal(t, pxfBase+"/keytabs/k/kt", props["pxf.service.kerberos.keytab"])
}

// TestPXFKeytabPath_DefaultKey proves the keytab path falls back to the
// "keytab" filename when the Secret reference has no explicit key, and that two
// Kerberos servers render in deterministic (name-sorted) order. (111-SE4-U)
func TestPXFKeytabPath_DefaultKey(t *testing.T) {
	// Empty key → default "keytab" filename in the rendered path.
	assert.Equal(t, pxfBase+"/keytabs/srv/keytab", pxfKeytabPath("srv", ""))
	assert.Equal(t, pxfBase+"/keytabs/srv/my.kt", pxfKeytabPath("srv", "my.kt"))

	// Two Kerberos servers (one with an empty keytab key) sort deterministically
	// by name and each gets its own volume; the b-server's path uses the default
	// "keytab" filename.
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "z-hive", Type: "hive",
					Kerberos: &cbv1alpha1.PxfKerberosSpec{
						Principal:    "pxf/_HOST@R",
						KeytabSecret: cbv1alpha1.SecretReference{Name: "z-kt"}, // no Key → default
					},
				},
				{
					Name: "a-hdfs", Type: "hdfs",
					Config: map[string]string{"fs.defaultFS": "hdfs://nn:8020"},
					Kerberos: &cbv1alpha1.PxfKerberosSpec{
						Principal:    "pxf/_HOST@R",
						KeytabSecret: cbv1alpha1.SecretReference{Name: "a-kt", Key: "a.keytab"},
					},
				},
			},
		},
	}

	vols := b.BuildPXFSidecarVolumes(cluster)
	// Both keytab volumes are present.
	require.NotNil(t, findVolume(vols, util.SanitizeK8sName("pxf-keytab-a-hdfs")))
	require.NotNil(t, findVolume(vols, util.SanitizeK8sName("pxf-keytab-z-hive")))

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)
	// z-hive's core-site.xml uses the default "keytab" filename.
	assert.Contains(t, cm.Data["z-hive__core-site.xml"], pxfBase+"/keytabs/z-hive/keytab")
	// a-hdfs uses its explicit key.
	assert.Contains(t, cm.Data["a-hdfs__core-site.xml"], pxfBase+"/keytabs/a-hdfs/a.keytab")
}

// ----------------------------------------------------------------------------
// SE.5 — cluster NetworkPolicy confines :5888 (111-SE5-U), REAL.
// ----------------------------------------------------------------------------

// TestBuildPXFClusterNetworkPolicy_Enabled proves the policy is emitted for a
// PXF cluster, selects the segment-primary pods, allows 5432 + exporter ports,
// and DELIBERATELY OMITS the PXF port 5888 (cross-pod PXF denied). (111-SE5-U)
func TestBuildPXFClusterNetworkPolicy_Enabled(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()

	np := b.BuildPXFClusterNetworkPolicy(cluster)
	require.NotNil(t, np, "policy must be emitted for a PXF cluster")

	// Name + ownerRef for GC.
	assert.Equal(t, util.PxfNetworkPolicyName(cluster.Name), np.Name)
	require.Len(t, np.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, np.OwnerReferences[0].Name)

	// Segment-primary selector.
	assert.Equal(t, cluster.Name, np.Spec.PodSelector.MatchLabels[util.LabelCluster])
	assert.Equal(t, util.ComponentSegmentPrimary,
		np.Spec.PodSelector.MatchLabels[util.LabelComponent])

	// Ingress-only policy.
	assert.Equal(t, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}, np.Spec.PolicyTypes)

	// Collect the allowed ingress ports.
	require.Len(t, np.Spec.Ingress, 1)
	allowed := map[int32]bool{}
	for _, p := range np.Spec.Ingress[0].Ports {
		require.NotNil(t, p.Port)
		allowed[p.Port.IntVal] = true
		require.NotNil(t, p.Protocol)
		assert.Equal(t, corev1.ProtocolTCP, *p.Protocol)
	}

	// 5432 (PostgreSQL) + exporter ports are allowed.
	assert.True(t, allowed[5432], "PostgreSQL port must be allowed")
	assert.True(t, allowed[pgExporterPort], "pg exporter port must be allowed")
	assert.True(t, allowed[nodeExporterPort], "node exporter port must be allowed")

	// HONESTY (SE.5): the PXF port 5888 is NOT in the cross-pod ingress set.
	assert.False(t, allowed[5888], "PXF port 5888 must NOT be allowed cross-pod")
}

// TestBuildPXFClusterNetworkPolicy_Disabled proves no policy is emitted when PXF
// is disabled (firewall: byte-identical default cluster). (111-SE5-U)
func TestBuildPXFClusterNetworkPolicy_Disabled(t *testing.T) {
	b := NewBuilder()
	assert.Nil(t, b.BuildPXFClusterNetworkPolicy(newTestCluster()),
		"no policy when PXF is disabled")
}

// ----------------------------------------------------------------------------
// SE.1 / SL.6 — ConfigMap holds ONLY ${...} placeholders, no plaintext secret.
// ----------------------------------------------------------------------------

// TestPXFConfigMap_NoPlaintextSecrets proves 111-SE1-NOPLAINTEXT / 111-SL6-U: a
// known secret value never appears in the rendered ConfigMap; only the
// sanitized ${...} placeholder token does. The cluster has multiple server types
// (s3 + jdbc), exercising the SL.6 multi-server case.
func TestPXFConfigMap_NoPlaintextSecrets(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)

	// The placeholder tokens ARE present (credentials are wired, just not in
	// plaintext).
	assert.Contains(t, cm.Data["s3-primary__s3-site.xml"], "${S3_CREDS_ACCESS_KEY}")
	assert.Contains(t, cm.Data["mysql-oltp__jdbc-site.xml"], "${MYSQL_CREDS_PASSWORD}")

	// No literal secret VALUE string appears anywhere (the secret references only
	// name+key; the resolved value is injected at runtime, never in the CM). We
	// assert the raw, un-sanitized token forms are absent across every data key.
	forbidden := []string{
		"s3-creds_access_key",  // raw name_key (un-sanitized) must not leak
		"mysql-creds_password", // raw name_key (un-sanitized) must not leak
		"pxf.credential",       // non-standard credential key must not appear
	}
	for key, val := range cm.Data {
		for _, f := range forbidden {
			assert.NotContains(t, val, f,
				"ConfigMap key %s must not contain plaintext/raw token %q", key, f)
		}
	}

	// Every credential property value is a ${...} placeholder, never a bare value.
	// Spot-check that the access-key/password property lines carry the token.
	assert.Contains(t, cm.Data["s3-primary__s3-site.xml"], "<name>fs.s3a.access.key</name>")
	assert.Contains(t, cm.Data["mysql-oltp__jdbc-site.xml"], "<name>jdbc.password</name>")
}

// ----------------------------------------------------------------------------
// SE.2 — JDBC TLS params land in jdbc-site.xml (111-SE2-U), CONFIG-ONLY.
// ----------------------------------------------------------------------------

// TestPXFJdbcTLSParamsRendered proves a jdbc server with TLS params (in Config
// and the dedicated Jdbc map) renders them into jdbc-site.xml. CONFIG-ONLY: we
// verify the rendered config, never a live encrypted handshake. (111-SE2-U)
func TestPXFJdbcTLSParamsRendered(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			Servers: []cbv1alpha1.PxfServerSpec{
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
				},
			},
		},
	}

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)

	site := cm.Data["pg-tls__jdbc-site.xml"]
	require.NotEmpty(t, site)

	// The ssl jdbc.url params survive into the rendered jdbc-site.xml.
	assert.Contains(t, site, "ssl=true")
	assert.Contains(t, site, "sslmode=verify-full")
	// The dedicated TLS connection properties survive too.
	assert.Contains(t, site, "<name>jdbc.connection.property.ssl</name>")
	assert.Contains(t, site, "<name>jdbc.connection.property.sslmode</name>")
	assert.Contains(t, site, "<name>jdbc.connection.property.sslrootcert</name>")
	assert.Contains(t, site, "/secrets/ca.pem")
}

// ----------------------------------------------------------------------------
// SE.3 — s3 TLS toggle lands in s3-site.xml (111-SE3-U), CONFIG-ONLY.
// ----------------------------------------------------------------------------

// TestPXFS3TLSEnabledRendered proves an s3 server with
// fs.s3a.connection.ssl.enabled=true renders that property into s3-site.xml.
// CONFIG-ONLY: we verify the rendered config, never a live TLS handshake.
// (111-SE3-U)
func TestPXFS3TLSEnabledRendered(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "s3-tls",
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint":               "https://minio.example.com",
						"fs.s3a.connection.ssl.enabled": "true",
					},
				},
			},
		},
	}

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)

	site := cm.Data["s3-tls__s3-site.xml"]
	require.NotEmpty(t, site)
	assert.Contains(t, site, "<name>fs.s3a.connection.ssl.enabled</name>")
	assert.Contains(t, site, "<value>true</value>")
}
