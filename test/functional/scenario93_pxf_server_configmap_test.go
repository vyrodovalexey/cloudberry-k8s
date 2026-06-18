//go:build functional

package functional

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 93: Server ConfigMap, File Mapping, Extensions, Sync (functional)
// ============================================================================
//
// The PXF server-config rendering (file-mapping per server type), the credential
// init container, the extension/grant DB setup and the shared-ConfigMap sync
// invariant are all implemented. This functional suite drives the BUILDER
// (infra-free) over a FULL multi-server dataLoading.pxf spec covering ALL the
// SL types and asserts the operator's IMPLEMENTED server-config contract:
//
//   - SL.1–SL.6 (BuildPXFServersConfigMap): the EXACT per-server data-key set
//     ("<server>__<file>.xml"), one logical directory per server (the "<server>__"
//     prefix grouping), the right Config keys in the right *-site.xml, and
//     credential ${PLACEHOLDER} tokens (NOT literal secret values) in the bodies.
//   - RP.8 (BuildPXFCredentialInitContainers): the pxf-cred-init init container,
//     its SecretKeyRef env (sanitized names matching the placeholders), the
//     templates mount + the envsubst render script.
//   - RP.12 (segment-primary StatefulSet): the SAME <cluster>-pxf-servers
//     ConfigMap is mounted as pxf-templates on every segment-primary pod, and the
//     builder output is DETERMINISTIC (building twice yields byte-identical
//     ConfigMap data) — the shared-ConfigMap-is-sync invariant.
//   - RP.9/RP.10/RP.11: asserted at the functional layer via the catalog contract
//     (the unit-level db tests cover the exact statements; the live e2e checks
//     pg_extension + the protocol grant).
//   - CatalogHonest: every cases.Scenario93Cases() fact is asserted against the
//     built objects.
//   - Negative: pxf disabled => BuildPXFServersConfigMap returns nil.
//
// SECRET-NAME NOTE: the credential Secret names (backup-s3-credentials,
// mysql-credentials, pg-source-credentials) are referenced BY NAME only; their
// values are injected by the init container at runtime and never appear in the
// ConfigMap, so this builder-direct suite needs no live Secrets.
// ============================================================================

// Scenario93Suite exercises the PXF server ConfigMap, file-mapping, the
// credential init container and the shared-ConfigMap sync invariant.
type Scenario93Suite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestFunctional_Scenario93(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario93Suite))
}

func (s *Scenario93Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario93FullDataLoading returns the FULL multi-server dataLoading spec
// exercised by this scenario: SIX PXF servers covering every SL file-mapping
// branch — s3 (SL.1), hdfs with Hive+Hbase (SL.2), two jdbc (SL.3), a hive-typed
// server (SL.4) and an hbase-typed server (SL.5) — plus pxf enabled + image +
// extensions. The Config/credential values mirror cases.Scenario93Cases()
// exactly so CatalogHonest stays honest.
func scenario93FullDataLoading() *cbv1alpha1.DataLoadingSpec {
	return &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled:  true,
			Image:    "cloudberry-pxf:2.1.0",
			JvmOpts:  "-Xmx1g -Xms256m",
			Port:     5888,
			LogLevel: "INFO",
			Extensions: &cbv1alpha1.PxfExtensionsSpec{
				Pxf:    util.Ptr(true),
				PxfFdw: util.Ptr(true),
			},
			Servers: []cbv1alpha1.PxfServerSpec{
				// SL.1: s3 (s3-datalake).
				{
					Name: "s3-datalake",
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint":          "http://minio:9000",
						"fs.s3a.path.style.access": "true",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "backup-s3-credentials", Key: "aws_access_key_id"},
						{Name: "backup-s3-credentials", Key: "aws_secret_access_key"},
					},
				},
				// SL.2: hdfs (hadoop-cluster) — core+hdfs+hive+hbase.
				{
					Name: "hadoop-cluster",
					Type: "hdfs",
					Config: map[string]string{
						"fs.defaultFS":    "hdfs://namenode:8020",
						"dfs.replication": "1",
					},
					Hive: map[string]string{
						"hive.metastore.uris": "thrift://hive-metastore:9083",
					},
					Hbase: map[string]string{
						"hbase.zookeeper.quorum": "zk:2181",
					},
				},
				// SL.3: jdbc (mysql-oltp).
				{
					Name: "mysql-oltp",
					Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "com.mysql.cj.jdbc.Driver",
						"jdbc.url":    "jdbc:mysql://mysql:3306/oltp",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "mysql-credentials", Key: "username"},
						{Name: "mysql-credentials", Key: "password"},
					},
				},
				// SL.3: jdbc (postgres-source).
				{
					Name: "postgres-source",
					Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "org.postgresql.Driver",
						"jdbc.url":    "jdbc:postgresql://pgsource:5432/sourcedb",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "pg-source-credentials", Key: "username"},
						{Name: "pg-source-credentials", Key: "password"},
					},
				},
				// SL.4: hive-typed (hive-warehouse) — core+hive.
				{
					Name: "hive-warehouse",
					Type: "hive",
					Config: map[string]string{
						"fs.defaultFS":        "hdfs://namenode:8020",
						"hive.metastore.uris": "thrift://hive-metastore:9083",
					},
				},
				// SL.5: hbase-typed (hbase-store) — core+hbase.
				{
					Name: "hbase-store",
					Type: "hbase",
					Config: map[string]string{
						"fs.defaultFS":           "hdfs://namenode:8020",
						"hbase.zookeeper.quorum": "zk:2181",
					},
				},
			},
		},
	}
}

// scenario93Cluster builds a valid cluster with the full multi-server
// dataLoading spec attached, applying the supplied mutator (if any).
func scenario93Cluster(
	name string,
	mutate func(*cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	dl := scenario93FullDataLoading()
	if mutate != nil {
		mutate(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// scenario93PxfContainer returns the named container from a list.
func scenario93Container(containers []corev1.Container, name string) (corev1.Container, bool) {
	for _, c := range containers {
		if c.Name == name {
			return c, true
		}
	}
	return corev1.Container{}, false
}

// TestFunctional_Scenario93_FileMapping asserts SL.1–SL.5: the EXACT per-server
// data-key set the builder emits and that the right Config keys land in the
// right *-site.xml file. It also asserts one logical directory per server (the
// "<server>__" prefix grouping of the data keys).
func (s *Scenario93Suite) TestFunctional_Scenario93_FileMapping() {
	cluster := scenario93Cluster("s93-mapping", nil)
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)
	assert.Equal(s.T(), builder.PxfServersConfigMapName(cluster.Name), cm.Name)

	// SL.1 s3-datalake → s3-site.xml (fs.s3a.* Config + cred placeholders).
	s.assertKeyContains(cm, "s3-datalake__s3-site.xml",
		"fs.s3a.endpoint", "http://minio:9000",
		"fs.s3a.path.style.access",
		"fs.s3a.access.key", "fs.s3a.secret.key")

	// SL.2 hadoop-cluster → core+hdfs (always) + hive + hbase.
	s.assertKeyContains(cm, "hadoop-cluster__core-site.xml", "fs.defaultFS", "hdfs://namenode:8020")
	s.assertKeyContains(cm, "hadoop-cluster__hdfs-site.xml", "dfs.replication")
	s.assertKeyContains(cm, "hadoop-cluster__hive-site.xml", "hive.metastore.uris", "thrift://hive-metastore:9083")
	s.assertKeyContains(cm, "hadoop-cluster__hbase-site.xml", "hbase.zookeeper.quorum", "zk:2181")

	// SL.3 jdbc → jdbc-site.xml each (jdbc.driver/url + cred placeholders).
	s.assertKeyContains(cm, "mysql-oltp__jdbc-site.xml",
		"jdbc.driver", "com.mysql.cj.jdbc.Driver",
		"jdbc.url", "jdbc:mysql://mysql:3306/oltp",
		"jdbc.user", "jdbc.password")
	s.assertKeyContains(cm, "postgres-source__jdbc-site.xml",
		"jdbc.driver", "org.postgresql.Driver",
		"jdbc.url", "jdbc:postgresql://pgsource:5432/sourcedb")

	// SL.4 hive-typed → core-site + hive-site (both always).
	s.assertKeyContains(cm, "hive-warehouse__core-site.xml", "fs.defaultFS")
	s.assertKeyContains(cm, "hive-warehouse__hive-site.xml", "hive.metastore.uris")

	// SL.5 hbase-typed → core-site + hbase-site (both always).
	s.assertKeyContains(cm, "hbase-store__core-site.xml", "fs.defaultFS")
	s.assertKeyContains(cm, "hbase-store__hbase-site.xml", "hbase.zookeeper.quorum")

	// The EXACT per-server data-key set (excluding the non-server
	// connectors.properties key): nothing more, nothing less per server.
	serverKeys := map[string]bool{}
	for k := range cm.Data {
		if strings.Contains(k, "__") {
			serverKeys[k] = true
		}
	}
	expectedServerKeys := []string{
		"s3-datalake__s3-site.xml",
		"hadoop-cluster__core-site.xml",
		"hadoop-cluster__hdfs-site.xml",
		"hadoop-cluster__hive-site.xml",
		"hadoop-cluster__hbase-site.xml",
		"mysql-oltp__jdbc-site.xml",
		"postgres-source__jdbc-site.xml",
		"hive-warehouse__core-site.xml",
		"hive-warehouse__hive-site.xml",
		"hbase-store__core-site.xml",
		"hbase-store__hbase-site.xml",
	}
	assert.Len(s.T(), serverKeys, len(expectedServerKeys),
		"the exact server data-key set must match (one logical directory per server)")
	for _, k := range expectedServerKeys {
		assert.Containsf(s.T(), serverKeys, k, "server data key %s must be present", k)
	}

	// One logical directory per server: group the "<server>__" prefixes and
	// assert the per-server file counts (the "<server>" before "__" is the
	// directory the init container materializes).
	dirs := map[string]int{}
	for k := range serverKeys {
		server := k[:strings.Index(k, "__")]
		dirs[server]++
	}
	assert.Equal(s.T(), 6, len(dirs), "exactly six logical server directories")
	assert.Equal(s.T(), 1, dirs["s3-datalake"])
	assert.Equal(s.T(), 4, dirs["hadoop-cluster"])
	assert.Equal(s.T(), 1, dirs["mysql-oltp"])
	assert.Equal(s.T(), 1, dirs["postgres-source"])
	assert.Equal(s.T(), 2, dirs["hive-warehouse"])
	assert.Equal(s.T(), 2, dirs["hbase-store"])

	// hdfs server has no mapred/yarn keys → those optional files are absent.
	assert.NotContains(s.T(), cm.Data, "hadoop-cluster__mapred-site.xml")
	assert.NotContains(s.T(), cm.Data, "hadoop-cluster__yarn-site.xml")
}

// TestFunctional_Scenario93_PlaceholdersNotLiterals asserts SL.6: every
// credentialed XML body carries ${PLACEHOLDER} env-var refs and NO literal
// secret value appears in any rendered body.
func (s *Scenario93Suite) TestFunctional_Scenario93_PlaceholdersNotLiterals() {
	cluster := scenario93Cluster("s93-placeholders", nil)
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	// Credential ${...} placeholders present (sanitized, uppercased, name+key).
	s.assertKeyContains(cm, "s3-datalake__s3-site.xml",
		"${BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID}",
		"${BACKUP_S3_CREDENTIALS_AWS_SECRET_ACCESS_KEY}")
	s.assertKeyContains(cm, "mysql-oltp__jdbc-site.xml",
		"${MYSQL_CREDENTIALS_USERNAME}", "${MYSQL_CREDENTIALS_PASSWORD}")
	s.assertKeyContains(cm, "postgres-source__jdbc-site.xml",
		"${PG_SOURCE_CREDENTIALS_USERNAME}", "${PG_SOURCE_CREDENTIALS_PASSWORD}")

	// NO literal secret values anywhere (the init container resolves them at
	// runtime; the ConfigMap holds only placeholders).
	for k, v := range cm.Data {
		assert.NotContainsf(s.T(), v, "minioadmin",
			"ConfigMap key %s must not carry a literal secret value", k)
		assert.NotContainsf(s.T(), v, "pxfpass",
			"ConfigMap key %s must not carry a literal secret value", k)
		// And the non-standard pxf.credential.* keys are gone everywhere.
		assert.NotContainsf(s.T(), v, "pxf.credential",
			"ConfigMap key %s must not emit pxf.credential.*", k)
	}
}

// TestFunctional_Scenario93_CredentialInitContainer asserts RP.8: the
// pxf-cred-init init container exists, mounts the templates ConfigMap, exposes
// the credentialSecrets as SecretKeyRef env (sanitized names matching the
// placeholders), and runs the envsubst render script writing nested
// <server>/<file>.xml into the shared pxf-servers emptyDir.
func (s *Scenario93Suite) TestFunctional_Scenario93_CredentialInitContainer() {
	cluster := scenario93Cluster("s93-credinit", nil)

	inits := s.builder.BuildPXFCredentialInitContainers(cluster)
	require.Len(s.T(), inits, 1)
	initC := inits[0]
	assert.Equal(s.T(), "pxf-cred-init", initC.Name)

	// Mounts: the templates ConfigMap (render SOURCE) + the shared pxf-servers
	// emptyDir (render DESTINATION).
	mounts := map[string]string{}
	for _, m := range initC.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	assert.Equal(s.T(), "/pxf-templates", mounts["pxf-templates"])
	assert.Equal(s.T(), "/pxf-base/servers", mounts["pxf-servers"])

	// The render script: envsubst over the templates dir into the servers dir,
	// splitting "<server>__<file>" → "<server>/<file>" (nested per-server layout).
	require.Len(s.T(), initC.Args, 1)
	script := initC.Args[0]
	assert.Contains(s.T(), script, "envsubst")
	assert.Contains(s.T(), script, "/pxf-templates")
	assert.Contains(s.T(), script, "/pxf-base/servers")
	assert.Contains(s.T(), script, "__", "script splits the <server>__<file> keys")

	// Env: every credential is a SecretKeyRef (never plaintext), and the env var
	// NAME matches the sanitized placeholder token byte-for-byte.
	envByName := map[string]corev1.EnvVar{}
	for _, e := range initC.Env {
		require.NotNilf(s.T(), e.ValueFrom, "cred env %s must be a SecretKeyRef", e.Name)
		require.NotNilf(s.T(), e.ValueFrom.SecretKeyRef, "cred env %s must be a SecretKeyRef", e.Name)
		assert.Empty(s.T(), e.Value, "cred env %s must not carry plaintext", e.Name)
		envByName[e.Name] = e
	}

	// The sanitized names match the placeholders emitted into the XML bodies.
	expectEnv := map[string]struct{ secret, key string }{
		"BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID":     {"backup-s3-credentials", "aws_access_key_id"},
		"BACKUP_S3_CREDENTIALS_AWS_SECRET_ACCESS_KEY": {"backup-s3-credentials", "aws_secret_access_key"},
		"MYSQL_CREDENTIALS_USERNAME":                  {"mysql-credentials", "username"},
		"MYSQL_CREDENTIALS_PASSWORD":                  {"mysql-credentials", "password"},
		"PG_SOURCE_CREDENTIALS_USERNAME":              {"pg-source-credentials", "username"},
		"PG_SOURCE_CREDENTIALS_PASSWORD":              {"pg-source-credentials", "password"},
	}
	for name, want := range expectEnv {
		e, ok := envByName[name]
		require.Truef(s.T(), ok, "credential init env %s (matching placeholder) must be present", name)
		assert.Equalf(s.T(), want.secret, e.ValueFrom.SecretKeyRef.Name,
			"env %s must source secret %s", name, want.secret)
		assert.Equalf(s.T(), want.key, e.ValueFrom.SecretKeyRef.Key,
			"env %s must source key %s", name, want.key)
	}
}

// TestFunctional_Scenario93_SharedConfigMapSync asserts RP.12: the segment-primary
// StatefulSet mounts the <cluster>-pxf-servers ConfigMap (as pxf-templates) on
// the credential init container, and the builder output is DETERMINISTIC —
// building the ConfigMap twice yields byte-identical Data. Because the SAME
// builder output is used for EVERY segment-primary pod, every sidecar renders
// byte-identical resolved configs: the shared ConfigMap IS the sync mechanism.
func (s *Scenario93Suite) TestFunctional_Scenario93_SharedConfigMapSync() {
	cluster := scenario93Cluster("s93-sync", nil)

	sts, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)

	// The pxf-templates volume on the segment pod template is ConfigMap-backed by
	// the <cluster>-pxf-servers ConfigMap (the shared sync source).
	var templatesVol *corev1.Volume
	for i := range sts.Spec.Template.Spec.Volumes {
		if sts.Spec.Template.Spec.Volumes[i].Name == "pxf-templates" {
			templatesVol = &sts.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	require.NotNil(s.T(), templatesVol, "segment pod template mounts the pxf-templates volume")
	require.NotNil(s.T(), templatesVol.ConfigMap)
	assert.Equal(s.T(), builder.PxfServersConfigMapName(cluster.Name),
		templatesVol.ConfigMap.Name,
		"the shared <cluster>-pxf-servers ConfigMap is the sync source on every segment pod")

	// The credential init container mounts pxf-templates (so EVERY segment pod's
	// init container resolves from the SAME ConfigMap → identical configs).
	initC, ok := scenario93Container(sts.Spec.Template.Spec.InitContainers, "pxf-cred-init")
	require.True(s.T(), ok, "segment pod template carries the pxf-cred-init container")
	hasTemplatesMount := false
	for _, m := range initC.VolumeMounts {
		if m.Name == "pxf-templates" {
			hasTemplatesMount = true
		}
	}
	assert.True(s.T(), hasTemplatesMount,
		"the credential init container mounts the shared templates ConfigMap")

	// Determinism: building the ConfigMap twice yields byte-identical Data
	// (the shared-ConfigMap-is-sync invariant — all sidecars render the same
	// bytes because the source bytes are deterministic).
	cm1 := s.builder.BuildPXFServersConfigMap(cluster)
	cm2 := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm1)
	require.NotNil(s.T(), cm2)
	assert.Equal(s.T(), cm1.Data, cm2.Data,
		"BuildPXFServersConfigMap must be deterministic (byte-identical shared config)")

	// A second cluster object with the SAME spec also yields identical data
	// (proves no hidden per-call state); the data keys are stable across builds.
	other := scenario93Cluster("s93-sync", nil)
	cmOther := s.builder.BuildPXFServersConfigMap(other)
	require.NotNil(s.T(), cmOther)
	assert.Equal(s.T(), cm1.Data, cmOther.Data,
		"identical specs must render byte-identical ConfigMap data")
}

// TestFunctional_Scenario93_ExtensionsAndGrantContract asserts RP.9/RP.10/RP.11
// at the functional layer via the catalog contract: the data-loader role the
// operator GRANTs the pxf protocol to is the deterministic gpadmin admin
// identity. The exact CREATE EXTENSION / GRANT statements are unit-proven in the
// db tests and verified live against the coordinator by the e2e suite.
func (s *Scenario93Suite) TestFunctional_Scenario93_ExtensionsAndGrantContract() {
	// The data-loader role the GRANT targets is gpadmin (the default admin),
	// which always exists as a superuser. This anchors the RP.11 contract row.
	assert.Equal(s.T(), "gpadmin", util.DefaultAdminUser,
		"the data-loader role the pxf protocol is GRANTed to is gpadmin")

	// The catalog records the RP.9/RP.10/RP.11 targets so CatalogHonest can
	// cross-check them; assert they are present and well-formed.
	byID := map[string]cases.Scenario93Case{}
	for _, tc := range cases.Scenario93Cases() {
		byID[tc.ID] = tc
	}
	require.Contains(s.T(), byID, "RP.9")
	require.Contains(s.T(), byID, "RP.10")
	require.Contains(s.T(), byID, "RP.11")
	assert.Contains(s.T(), byID["RP.9"].Target, "pxf")
	assert.Contains(s.T(), byID["RP.10"].Target, "pxf_fdw")
	assert.Contains(s.T(), byID["RP.11"].Description, "gpadmin")
	assert.Contains(s.T(), byID["RP.11"].Description, "PROTOCOL pxf")
}

// TestFunctional_Scenario93_NegativeNoConfigMap asserts the blast radius: a
// pxf-disabled cluster (and a dataLoading-disabled cluster) produces NO servers
// ConfigMap and NO credential init container.
func (s *Scenario93Suite) TestFunctional_Scenario93_NegativeNoConfigMap() {
	pxfOff := scenario93Cluster("s93-pxf-off", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Enabled = false
	})
	assert.Nil(s.T(), s.builder.BuildPXFServersConfigMap(pxfOff),
		"pxf-disabled cluster must produce no servers ConfigMap")
	assert.Empty(s.T(), s.builder.BuildPXFCredentialInitContainers(pxfOff),
		"pxf-disabled cluster must produce no credential init container")

	dlOff := scenario93Cluster("s93-dl-off", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Enabled = false
	})
	assert.Nil(s.T(), s.builder.BuildPXFServersConfigMap(dlOff),
		"dataLoading-disabled cluster must produce no servers ConfigMap")

	// An empty image also disables the sidecar (blast-radius firewall).
	noImage := scenario93Cluster("s93-no-image", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Image = ""
	})
	assert.Nil(s.T(), s.builder.BuildPXFServersConfigMap(noImage),
		"empty-image cluster must produce no servers ConfigMap")
}

// TestFunctional_Scenario93_CatalogHonest iterates cases.Scenario93Cases() and
// asserts each documented fact against the built objects: SL.1–SL.6 against the
// ConfigMap, RP.8 against the init container, RP.12 against the shared-ConfigMap
// mount, and RP.9/RP.10/RP.11 against the contract (the live e2e checks the DB).
func (s *Scenario93Suite) TestFunctional_Scenario93_CatalogHonest() {
	catalog := cases.Scenario93Cases()
	require.NotEmpty(s.T(), catalog, "Scenario 93 catalog must be non-empty")

	cluster := scenario93Cluster("s93-catalog", nil)
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)
	inits := s.builder.BuildPXFCredentialInitContainers(cluster)
	require.Len(s.T(), inits, 1)
	initC := inits[0]
	sts, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID+"_"+tc.Name, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch {
			case strings.HasPrefix(tc.ID, "SL."):
				s.assertSLCase(cm, tc)
			case tc.ID == "RP.8":
				s.assertRP8(initC)
			case tc.ID == "RP.12":
				s.assertRP12(sts, cluster)
			case tc.ID == "RP.9" || tc.ID == "RP.10" || tc.ID == "RP.11":
				// Contract-only at the functional layer (live e2e checks the DB).
				assert.NotEmpty(s.T(), tc.Target, "RP DB row must name a target")
				assert.NotEmpty(s.T(), tc.Description)
			default:
				s.T().Fatalf("unhandled catalog ID %s", tc.ID)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

// assertSLCase asserts one SL.x catalog row against the built ConfigMap: the
// exact ExpectedKeys are present, the KeyContains substrings appear in the right
// keys, and the ForbiddenSubstrings appear nowhere.
func (s *Scenario93Suite) assertSLCase(cm *corev1.ConfigMap, tc cases.Scenario93Case) {
	for _, k := range tc.ExpectedKeys {
		assert.Containsf(s.T(), cm.Data, k, "%s: ConfigMap must carry key %s", tc.ID, k)
	}
	for key, subs := range tc.KeyContains {
		body := cm.Data[key]
		for _, sub := range subs {
			assert.Containsf(s.T(), body, sub,
				"%s: key %s body must contain %q", tc.ID, key, sub)
		}
	}
	for _, forbidden := range tc.ForbiddenSubstrings {
		for k, v := range cm.Data {
			assert.NotContainsf(s.T(), v, forbidden,
				"%s: key %s must not contain forbidden literal %q", tc.ID, k, forbidden)
		}
	}
}

// assertRP8 asserts the RP.8 catalog row against the credential init container.
func (s *Scenario93Suite) assertRP8(initC corev1.Container) {
	assert.Equal(s.T(), "pxf-cred-init", initC.Name)
	require.Len(s.T(), initC.Args, 1)
	assert.Contains(s.T(), initC.Args[0], "envsubst")
	require.NotEmpty(s.T(), initC.Env, "init container must carry SecretKeyRef env")
	for _, e := range initC.Env {
		require.NotNilf(s.T(), e.ValueFrom, "cred env %s must be a SecretKeyRef", e.Name)
	}
}

// assertRP12 asserts the RP.12 catalog row: the shared <cluster>-pxf-servers
// ConfigMap is the pxf-templates source on the segment pod, and the builder
// output is deterministic (so every sidecar renders byte-identical configs).
func (s *Scenario93Suite) assertRP12(
	sts *appsv1.StatefulSet,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	var templatesCM string
	for i := range sts.Spec.Template.Spec.Volumes {
		v := sts.Spec.Template.Spec.Volumes[i]
		if v.Name == "pxf-templates" && v.ConfigMap != nil {
			templatesCM = v.ConfigMap.Name
		}
	}
	assert.Equal(s.T(), builder.PxfServersConfigMapName(cluster.Name), templatesCM,
		"the shared <cluster>-pxf-servers ConfigMap is the pxf-templates source")

	cm1 := s.builder.BuildPXFServersConfigMap(cluster)
	cm2 := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm1)
	require.NotNil(s.T(), cm2)
	assert.Equal(s.T(), cm1.Data, cm2.Data, "shared ConfigMap data must be deterministic")
}

// assertKeyContains asserts the ConfigMap has the key and its body contains all
// the given substrings.
func (s *Scenario93Suite) assertKeyContains(cm *corev1.ConfigMap, key string, subs ...string) {
	require.Containsf(s.T(), cm.Data, key, "ConfigMap must carry key %s", key)
	body := cm.Data[key]
	for _, sub := range subs {
		assert.Containsf(s.T(), body, sub, "key %s body must contain %q", key, sub)
	}
}
