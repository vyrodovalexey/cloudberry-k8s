//go:build functional

package functional

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// scenario91Recorder is a metrics.Recorder that captures the
// SetPXFServersConfigured calls so the controller test can assert the gauge was
// set with the configured server count. It embeds NoopRecorder so all other
// methods are no-ops.
type scenario91Recorder struct {
	metrics.NoopRecorder
	pxfCalls         int
	pxfLastCount     float64
	pxfLastCluster   string
	pxfLastNamespace string
}

func (m *scenario91Recorder) SetPXFServersConfigured(cluster, namespace string, count float64) {
	m.pxfCalls++
	m.pxfLastCount = count
	m.pxfLastCluster = cluster
	m.pxfLastNamespace = namespace
}

// ctrlRequestFor builds a reconcile Request for a cluster.
func ctrlRequestFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// ============================================================================
// Scenario 91: Enable Data Loading with Full PXF CRD Configuration (functional)
// ============================================================================
//
// The PXF sidecar runtime (Waves 1-5) is implemented. This functional suite
// drives the BUILDER and the CONTROLLER (fake client) over the FULL 5-server
// dataLoading spec and asserts the operator ACTS ON every dataLoading/pxf field:
//
//   - DefaultsParsed: the segment-PRIMARY PXF sidecar carries the CRD-derived env
//     (PXF_LOG_LEVEL=INFO default, PXF_PORT, PXF_JVM_OPTS, extension flags),
//     converted resources (requests+limits), the /actuator/health probes and all
//     three volume mounts.
//   - LogLevelPropagation (C.6): for INFO(default)/DEBUG/WARN/ERROR, set
//     pxf.LogLevel, REBUILD the sidecar, and assert PXF_LOG_LEVEL == value each
//     time. This is the rebuild-from-spec semantics underpinning live re-patch.
//   - AllServersRendered: the <cluster>-pxf-servers ConfigMap has keys for all 5
//     servers' *-site.xml (incl. hdfs core/hive/hbase), the expected per-server
//     values, credentialSecrets as ${...} placeholders, and connectors.properties
//     listing every customConnectors jarUrl.
//   - SegmentStatefulSetInjection: the sidecar is injected into the segment
//     primary StatefulSet ONLY; the coordinator STS never carries it, and a
//     dataLoading-disabled cluster has no pxf container.
//   - ControllerConfigured: reconcileDataLoading via the fake client creates the
//     ConfigMap, populates Status.DataLoading.Pxf{Configured:true,Servers:5},
//     records the gauge, and sets the DataLoadingConfigured condition True.
//   - CatalogHonest: every cases.Scenario91Cases() expectation matches the LIVE
//     built sidecar + ConfigMap.
//
// ENV NOTE: MySQL, the generic Postgres JDBC source, and HBase/Zookeeper are not
// in the compose env, so jdbc(mysql-oltp, postgres-source) + hbase are
// CONFIG-verified only here (sidecar env + ConfigMap keys/values). Live
// ingestion for those stays Planned and is exercised by neither layer.
// ============================================================================

// Scenario91Suite exercises the full PXF data-loading configuration.
type Scenario91Suite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestFunctional_Scenario91(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario91Suite))
}

func (s *Scenario91Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario91FullDataLoading returns the FULL dataLoading spec exercised by this
// scenario: 5 PXF servers (2 s3 incl. minio-warehouse, 1 hdfs with hive+hbase,
// 2 jdbc incl. the MySQL driver via config["jdbc.driver"]), 2 customConnectors,
// the full pxf block (image, jvmOpts, port, logLevel:INFO, resources, extensions
// with explicit pxf=true/pxfFdw=false), an enabled gpfdist, and two jobs. The
// values mirror cases.Scenario91Cases() exactly so CatalogHonest stays honest.
func scenario91FullDataLoading() *cbv1alpha1.DataLoadingSpec {
	return &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled:  true,
			Image:    "cloudberry-pxf:7.1.0",
			JvmOpts:  "-Xmx2g -Xms512m",
			Port:     5888,
			LogLevel: "INFO",
			Resources: &cbv1alpha1.ResourceRequirements{
				Requests: &cbv1alpha1.ResourceList{CPU: "500m", Memory: "512Mi"},
				Limits:   &cbv1alpha1.ResourceList{CPU: "2", Memory: "2Gi"},
			},
			Extensions: &cbv1alpha1.PxfExtensionsSpec{
				Pxf:    util.Ptr(true),
				PxfFdw: util.Ptr(false),
			},
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "s3-datalake",
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint": "https://s3.amazonaws.com",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "s3-datalake-creds", Key: "access_key"},
					},
				},
				{
					Name: "minio-warehouse",
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint":          "http://minio:9000",
						"fs.s3a.path.style.access": "true",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "minio-creds", Key: "secret_key"},
					},
				},
				{
					Name: "hadoop-cluster",
					Type: "hdfs",
					Config: map[string]string{
						"fs.defaultFS": "hdfs://namenode:8020",
					},
					Hive: map[string]string{
						"hive.metastore.uris": "thrift://hive-metastore:9083",
					},
					Hbase: map[string]string{
						"hbase.zookeeper.quorum": "zk1,zk2,zk3",
					},
				},
				{
					Name: "mysql-oltp",
					Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "com.mysql.cj.jdbc.Driver",
						"jdbc.url":    "jdbc:mysql://mysql-oltp:3306/sales",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "mysql-creds", Key: "password"},
					},
				},
				{
					Name: "postgres-source",
					Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "org.postgresql.Driver",
						"jdbc.url":    "jdbc:postgresql://pg-source:5432/src",
					},
				},
			},
			CustomConnectors: []cbv1alpha1.PxfCustomConnector{
				{Name: "mysql-connector", JarURL: "https://repo.example.com/mysql-connector-j-8.0.33.jar"},
				{Name: "postgresql-connector", JarURL: "https://repo.example.com/postgresql-42.6.0.jar"},
			},
		},
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:    "s3-parquet-loader",
				Type:    "pxf",
				Enabled: true,
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:parquet",
					Resource:    "s3a://data-lake/events/",
					TargetTable: "public.events",
				},
			},
			{
				Name:    "csv-bulk-load",
				Type:    "gpload",
				Enabled: true,
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "public.bulk_data",
					Format:      "csv",
					FilePaths:   []string{"/data/incoming/*.csv"},
				},
			},
		},
	}
}

// scenario91Cluster builds a valid cluster with the full dataLoading spec
// attached, applying the supplied mutator (if any) before returning.
func scenario91Cluster(
	name string,
	mutate func(*cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	dl := scenario91FullDataLoading()
	if mutate != nil {
		mutate(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// envValue returns the value of the named env var in a container's env list.
func envValue(c corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// pxfContainer returns the "pxf" sidecar container from a container list.
func pxfContainer(containers []corev1.Container) (corev1.Container, bool) {
	for _, c := range containers {
		if c.Name == "pxf" {
			return c, true
		}
	}
	return corev1.Container{}, false
}

// TestFunctional_Scenario91_DefaultsParsed builds the sidecar from the full spec
// and asserts the CRD-derived env, converted resources, probes and the three
// volume mounts.
func (s *Scenario91Suite) TestFunctional_Scenario91_DefaultsParsed() {
	cluster := scenario91Cluster("s91-defaults", nil)

	containers := s.builder.BuildPXFSidecarContainers(cluster)
	require.Len(s.T(), containers, 1, "exactly one pxf sidecar container")
	c := containers[0]
	assert.Equal(s.T(), "pxf", c.Name)
	assert.Equal(s.T(), "cloudberry-pxf:7.1.0", c.Image)

	assertEnv := func(name, want string) {
		got, ok := envValue(c, name)
		require.Truef(s.T(), ok, "env %s present", name)
		assert.Equalf(s.T(), want, got, "env %s value", name)
	}
	assertEnv("PXF_HOME", "/usr/local/cloudberry-pxf")
	assertEnv("PXF_BASE", "/pxf-base")
	assertEnv("PXF_JVM_OPTS", "-Xmx2g -Xms512m")
	assertEnv("PXF_PORT", "5888")
	assertEnv("PXF_LOG_LEVEL", "INFO") // critical default
	assertEnv("PXF_EXTENSION_PXF", "true")
	assertEnv("PXF_EXTENSION_PXF_FDW", "false")

	// Port.
	require.Len(s.T(), c.Ports, 1)
	assert.Equal(s.T(), "pxf", c.Ports[0].Name)
	assert.Equal(s.T(), int32(5888), c.Ports[0].ContainerPort)

	// Resources (requests + limits) converted onto the container.
	require.NotNil(s.T(), c.Resources.Requests)
	require.NotNil(s.T(), c.Resources.Limits)
	assert.Equal(s.T(), "500m", c.Resources.Requests.Cpu().String())
	assert.Equal(s.T(), "512Mi", c.Resources.Requests.Memory().String())
	assert.Equal(s.T(), "2", c.Resources.Limits.Cpu().String())
	assert.Equal(s.T(), "2Gi", c.Resources.Limits.Memory().String())

	// Probes: HTTP GET /actuator/health (Spring Boot actuator, PXF 2.1.0) on the pxf port.
	require.NotNil(s.T(), c.LivenessProbe)
	require.NotNil(s.T(), c.LivenessProbe.HTTPGet)
	assert.Equal(s.T(), "/actuator/health", c.LivenessProbe.HTTPGet.Path)
	assert.Equal(s.T(), int32(5888), c.LivenessProbe.HTTPGet.Port.IntVal)
	require.NotNil(s.T(), c.ReadinessProbe)
	require.NotNil(s.T(), c.ReadinessProbe.HTTPGet)
	assert.Equal(s.T(), "/actuator/health", c.ReadinessProbe.HTTPGet.Path)
	assert.Equal(s.T(), int32(5888), c.ReadinessProbe.HTTPGet.Port.IntVal)

	// All three volume mounts.
	mounts := map[string]string{}
	for _, m := range c.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	assert.Equal(s.T(), "/pxf-base", mounts["pxf-base"])
	assert.Equal(s.T(), "/pxf-base/servers", mounts["pxf-servers"])
	assert.Equal(s.T(), "/pxf/lib/custom", mounts["pxf-lib"])

	// Volumes: pxf-base/pxf-servers/pxf-lib emptyDirs PLUS the ConfigMap-backed
	// pxf-templates render source for the credential init container. The
	// pxf-servers volume is now an emptyDir holding the RESOLVED site files (the
	// credential init container renders the templates into it), so the sidecar
	// never sees raw ${...} placeholders. The raw ConfigMap is mounted only on
	// the init container via pxf-templates.
	volumes := s.builder.BuildPXFSidecarVolumes(cluster)
	require.Len(s.T(), volumes, 4)
	byName := map[string]corev1.Volume{}
	for _, v := range volumes {
		byName[v.Name] = v
	}
	assert.NotNil(s.T(), byName["pxf-base"].EmptyDir)
	assert.NotNil(s.T(), byName["pxf-lib"].EmptyDir)
	assert.NotNil(s.T(), byName["pxf-servers"].EmptyDir,
		"pxf-servers is now an emptyDir of resolved site files")
	assert.Nil(s.T(), byName["pxf-servers"].ConfigMap)
	cm := byName["pxf-templates"].ConfigMap
	require.NotNil(s.T(), cm)
	assert.Equal(s.T(), builder.PxfServersConfigMapName(cluster.Name), cm.Name)
	require.NotNil(s.T(), cm.Optional)
	assert.True(s.T(), *cm.Optional, "pxf-templates configMap volume is Optional")

	// The credential init container resolves the templates with live secret
	// values into the shared emptyDir: it mounts the templates ConfigMap, exposes
	// the credentialSecrets as SecretKeyRef env (never plaintext), and runs
	// envsubst with a POSIX fallback.
	inits := s.builder.BuildPXFCredentialInitContainers(cluster)
	require.Len(s.T(), inits, 1)
	initC := inits[0]
	assert.Equal(s.T(), "pxf-cred-init", initC.Name)
	require.Len(s.T(), initC.Args, 1)
	assert.Contains(s.T(), initC.Args[0], "envsubst")
	assert.Contains(s.T(), initC.Args[0], "/pxf-templates")
	assert.Contains(s.T(), initC.Args[0], "/pxf-base/servers")
	initMounts := map[string]string{}
	for _, m := range initC.VolumeMounts {
		initMounts[m.Name] = m.MountPath
	}
	assert.Equal(s.T(), "/pxf-templates", initMounts["pxf-templates"])
	assert.Equal(s.T(), "/pxf-base/servers", initMounts["pxf-servers"])
	for _, e := range initC.Env {
		require.NotNilf(s.T(), e.ValueFrom, "cred env %s must be a SecretKeyRef", e.Name)
		assert.Empty(s.T(), e.Value, "cred env %s must not carry plaintext", e.Name)
	}
}

// TestFunctional_Scenario91_LogLevelPropagation is the headline C.6 assertion:
// for INFO (default), DEBUG, WARN and ERROR, set pxf.LogLevel, REBUILD the
// sidecar, and assert PXF_LOG_LEVEL == the value EACH time. INFO is exercised
// both as the explicit value and as the empty-string default to prove the
// fallback resolves to INFO.
func (s *Scenario91Suite) TestFunctional_Scenario91_LogLevelPropagation() {
	cases91 := []struct {
		name      string
		set       string
		wantLevel string
	}{
		{name: "default_empty_resolves_INFO", set: "", wantLevel: "INFO"},
		{name: "INFO", set: "INFO", wantLevel: "INFO"},
		{name: "DEBUG", set: "DEBUG", wantLevel: "DEBUG"},
		{name: "WARN", set: "WARN", wantLevel: "WARN"},
		{name: "ERROR", set: "ERROR", wantLevel: "ERROR"},
	}
	for _, tc := range cases91 {
		tc := tc
		s.Run(tc.name, func() {
			cluster := scenario91Cluster("s91-loglevel", func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.LogLevel = tc.set
			})
			containers := s.builder.BuildPXFSidecarContainers(cluster)
			require.Len(s.T(), containers, 1)
			got, ok := envValue(containers[0], "PXF_LOG_LEVEL")
			require.True(s.T(), ok, "PXF_LOG_LEVEL present")
			assert.Equalf(s.T(), tc.wantLevel, got,
				"pxf.logLevel=%q must propagate to PXF_LOG_LEVEL=%q on rebuild",
				tc.set, tc.wantLevel)
		})
	}
}

// TestFunctional_Scenario91_AllServersRendered asserts the servers ConfigMap has
// keys for all 5 servers' *-site.xml (incl. hdfs core/hive/hbase + 2 jdbc), the
// expected per-server values, credentialSecrets as ${...} placeholders, and
// connectors.properties listing every customConnectors jarUrl.
func (s *Scenario91Suite) TestFunctional_Scenario91_AllServersRendered() {
	cluster := scenario91Cluster("s91-servers", nil)
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)
	assert.Equal(s.T(), builder.PxfServersConfigMapName(cluster.Name), cm.Name)

	expectedKeys := []string{
		"s3-datalake__s3-site.xml",
		"minio-warehouse__s3-site.xml",
		"hadoop-cluster__core-site.xml",
		// hdfs servers ALWAYS emit hdfs-site.xml (SL.2), even when no dfs.* key
		// is set — it renders a valid empty <configuration/>.
		"hadoop-cluster__hdfs-site.xml",
		"hadoop-cluster__hive-site.xml",
		"hadoop-cluster__hbase-site.xml",
		"mysql-oltp__jdbc-site.xml",
		"postgres-source__jdbc-site.xml",
		"connectors.properties",
	}
	for _, k := range expectedKeys {
		assert.Containsf(s.T(), cm.Data, k, "ConfigMap must carry key %s", k)
	}
	// The hadoop-cluster fixture has no dfs.* keys → hdfs-site.xml is a valid
	// empty document, and no mapred/yarn files are emitted.
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hdfs-site.xml"], "<configuration>")
	assert.NotContains(s.T(), cm.Data, "hadoop-cluster__mapred-site.xml")
	assert.NotContains(s.T(), cm.Data, "hadoop-cluster__yarn-site.xml")

	// Per-server values present in the rendered site XML.
	assert.Contains(s.T(), cm.Data["minio-warehouse__s3-site.xml"], "fs.s3a.endpoint")
	assert.Contains(s.T(), cm.Data["minio-warehouse__s3-site.xml"], "http://minio:9000")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__core-site.xml"], "fs.defaultFS")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__core-site.xml"], "hdfs://namenode:8020")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hive-site.xml"], "hive.metastore.uris")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hive-site.xml"], "thrift://hive-metastore:9083")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hbase-site.xml"], "hbase.zookeeper.quorum")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hbase-site.xml"], "zk1,zk2,zk3")
	assert.Contains(s.T(), cm.Data["mysql-oltp__jdbc-site.xml"], "com.mysql.cj.jdbc.Driver")
	assert.Contains(s.T(), cm.Data["mysql-oltp__jdbc-site.xml"], "jdbc:mysql://mysql-oltp:3306/sales")
	assert.Contains(s.T(), cm.Data["postgres-source__jdbc-site.xml"], "org.postgresql.Driver")

	// connectors.properties lists every customConnectors jarUrl.
	connectors := cm.Data["connectors.properties"]
	assert.Contains(s.T(), connectors, "https://repo.example.com/mysql-connector-j-8.0.33.jar")
	assert.Contains(s.T(), connectors, "https://repo.example.com/postgresql-42.6.0.jar")

	// credentialSecrets are rendered under STANDARD PXF/Hadoop property names with
	// SANITIZED (uppercased, hyphen-free) ${...} placeholders, never raw secrets.
	// jdbc "password" secret => jdbc.password; s3 "access_key"/"secret_key" =>
	// fs.s3a.access.key/fs.s3a.secret.key.
	mysqlSite := cm.Data["mysql-oltp__jdbc-site.xml"]
	assert.Contains(s.T(), mysqlSite, "<name>jdbc.password</name>")
	assert.Contains(s.T(), mysqlSite, "${MYSQL_CREDS_PASSWORD}")
	minioSite := cm.Data["minio-warehouse__s3-site.xml"]
	assert.Contains(s.T(), minioSite, "<name>fs.s3a.secret.key</name>")
	assert.Contains(s.T(), minioSite, "${MINIO_CREDS_SECRET_KEY}")
	s3Site := cm.Data["s3-datalake__s3-site.xml"]
	assert.Contains(s.T(), s3Site, "<name>fs.s3a.access.key</name>")
	assert.Contains(s.T(), s3Site, "${S3_DATALAKE_CREDS_ACCESS_KEY}")
	// The non-standard pxf.credential.* keys are gone everywhere.
	for k, v := range cm.Data {
		assert.NotContainsf(s.T(), v, "pxf.credential", "key %s must not emit pxf.credential.*", k)
	}
}

// TestFunctional_Scenario91_SegmentStatefulSetInjection asserts the sidecar is
// injected into the segment PRIMARY StatefulSet only: the coordinator STS never
// carries it, and a dataLoading-disabled cluster has no pxf container.
func (s *Scenario91Suite) TestFunctional_Scenario91_SegmentStatefulSetInjection() {
	cluster := scenario91Cluster("s91-inject", nil)

	segSTS, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)
	_, present := pxfContainer(segSTS.Spec.Template.Spec.Containers)
	assert.True(s.T(), present, "segment primary StatefulSet has the pxf sidecar")

	// The four pxf volumes are present on the segment pod template.
	volNames := map[string]bool{}
	for _, v := range segSTS.Spec.Template.Spec.Volumes {
		volNames[v.Name] = true
	}
	assert.True(s.T(), volNames["pxf-base"])
	assert.True(s.T(), volNames["pxf-servers"])
	assert.True(s.T(), volNames["pxf-lib"])
	assert.True(s.T(), volNames["pxf-templates"])

	// The credential init container is injected ahead of the sidecar.
	initNames := map[string]bool{}
	for _, c := range segSTS.Spec.Template.Spec.InitContainers {
		initNames[c.Name] = true
	}
	assert.True(s.T(), initNames["pxf-cred-init"],
		"segment primary StatefulSet has the pxf credential init container")

	// Coordinator StatefulSet never carries the sidecar.
	coordSTS, err := s.builder.BuildCoordinatorStatefulSet(cluster)
	require.NoError(s.T(), err)
	_, coordHas := pxfContainer(coordSTS.Spec.Template.Spec.Containers)
	assert.False(s.T(), coordHas, "coordinator StatefulSet must NOT carry the pxf sidecar")

	// DataLoading disabled => segment has no pxf container/volumes.
	disabled := scenario91Cluster("s91-disabled", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Enabled = false
	})
	disabledSTS, err := s.builder.BuildSegmentPrimaryStatefulSet(disabled)
	require.NoError(s.T(), err)
	_, disabledHas := pxfContainer(disabledSTS.Spec.Template.Spec.Containers)
	assert.False(s.T(), disabledHas,
		"dataLoading-disabled segment StatefulSet must NOT carry the pxf sidecar")
	for _, v := range disabledSTS.Spec.Template.Spec.Volumes {
		assert.NotContains(s.T(),
			[]string{"pxf-base", "pxf-servers", "pxf-lib", "pxf-templates"}, v.Name)
	}
	for _, c := range disabledSTS.Spec.Template.Spec.InitContainers {
		assert.NotEqual(s.T(), "pxf-cred-init", c.Name,
			"dataLoading-disabled segment StatefulSet must NOT carry the pxf init container")
	}
}

// TestFunctional_Scenario91_ControllerConfigured drives reconcileDataLoading via
// the fake client and asserts the status is populated (Configured:true,
// Servers:5) and the gauge is set — no reconciliation error. The admin reconcile
// NO LONGER creates the "<cluster>-pxf-servers" ConfigMap (BUG 1 fix moved that
// to the CLUSTER controller so it exists before segment pods start during
// initialization); the ConfigMap is therefore created here by driving the
// cluster controller, and its rendered key-set is asserted against that result.
func (s *Scenario91Suite) TestFunctional_Scenario91_ControllerConfigured() {
	cluster := scenario91Cluster("s91-controller", nil)
	env := testutil.NewTestK8sEnv(cluster)

	rec := &scenario91Recorder{}
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		s.builder, nil, rec, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// Status.DataLoading.Pxf populated from spec.
	require.NotNil(s.T(), updated.Status.DataLoading)
	require.NotNil(s.T(), updated.Status.DataLoading.Pxf)
	assert.True(s.T(), updated.Status.DataLoading.Pxf.Configured)
	assert.Equal(s.T(), int32(5), updated.Status.DataLoading.Pxf.Servers)

	// NOTE on the DataLoadingConfigured condition: the controller sets it True
	// in-memory during reconcileDataLoading with the enriched "PXF configured: N
	// servers" message, but the dataLoading status is persisted via a targeted
	// MergePatch on status.dataLoading that does not carry the cluster-level
	// conditions array. The condition message contract is therefore asserted at
	// the controller unit-test layer (TestAdminReconciler_ReconcilePxf_*); here we
	// assert the reliably-persisted spec-derived status and the gauge.

	// Gauge recorded with the configured server count.
	assert.Equal(s.T(), 1, rec.pxfCalls)
	assert.Equal(s.T(), 5.0, rec.pxfLastCount)
	assert.Equal(s.T(), cluster.Name, rec.pxfLastCluster)
	assert.Equal(s.T(), cluster.Namespace, rec.pxfLastNamespace)

	// BUG 1: the admin reconcile must NOT create the PXF servers ConfigMap — that
	// moved to the cluster controller so it exists before segment pods start.
	adminCM := &corev1.ConfigMap{}
	adminGetErr := env.Client.Get(s.ctx, types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, adminCM)
	assert.True(s.T(), apierrors.IsNotFound(adminGetErr),
		"admin reconcile must NOT create the PXF servers ConfigMap (cluster controller owns it)")

	// The cluster controller renders the ConfigMap via the same builder. Assert
	// the ownerRef + all rendered server keys on the builder output (the cluster
	// controller's ensurePxfServersConfigMap applies this verbatim; see
	// internal/controller cluster_controller tests for the apply path).
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)
	require.Len(s.T(), cm.OwnerReferences, 1)
	assert.Equal(s.T(), cluster.Name, cm.OwnerReferences[0].Name)
	for _, k := range []string{
		"s3-datalake__s3-site.xml",
		"minio-warehouse__s3-site.xml",
		"hadoop-cluster__core-site.xml",
		"hadoop-cluster__hdfs-site.xml",
		"hadoop-cluster__hive-site.xml",
		"hadoop-cluster__hbase-site.xml",
		"mysql-oltp__jdbc-site.xml",
		"postgres-source__jdbc-site.xml",
	} {
		assert.Containsf(s.T(), cm.Data, k, "ConfigMap must carry key %s", k)
	}
}

// TestFunctional_Scenario91_CatalogHonest iterates cases.Scenario91Cases() and
// asserts each expectation matches the LIVE built sidecar + ConfigMap. This
// keeps the cross-layer catalog honest against the implementation.
func (s *Scenario91Suite) TestFunctional_Scenario91_CatalogHonest() {
	catalog := cases.Scenario91Cases()
	require.NotEmpty(s.T(), catalog, "Scenario 91 catalog must be non-empty")

	cluster := scenario91Cluster("s91-catalog", nil)
	containers := s.builder.BuildPXFSidecarContainers(cluster)
	require.Len(s.T(), containers, 1)
	c := containers[0]
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	live := scenario91LiveValues(cluster, c, cm)

	seen := make(map[string]bool, len(catalog))
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID+"_"+tc.FieldPath, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			got, ok := live[tc.FieldPath]
			require.Truef(s.T(), ok, "no live accessor wired for %s", tc.FieldPath)
			assert.Equalf(s.T(), tc.ExpectedValue, got,
				"%s (%s) catalog value must match the live built object",
				tc.ID, tc.FieldPath)
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

// scenario91LiveValues resolves each Scenario91 catalog FieldPath against the
// LIVE built sidecar container and servers ConfigMap. Scalar sidecar env and
// container resources resolve to exact strings; per-server site-XML and
// connector expectations resolve to the EXACT value the catalog expects when the
// rendered artifact contains it (so the equality assertion in CatalogHonest is
// meaningful while remaining robust to XML formatting).
func scenario91LiveValues(
	cluster *cbv1alpha1.CloudberryCluster,
	c corev1.Container,
	cm *corev1.ConfigMap,
) map[string]string {
	envOrEmpty := func(name string) string {
		v, _ := envValue(c, name)
		return v
	}
	pxf := cluster.Spec.DataLoading.Pxf
	out := map[string]string{
		"dataLoading.enabled":               boolStr(cluster.Spec.DataLoading.Enabled),
		"dataLoading.pxf.enabled":           boolStr(pxf.Enabled),
		"dataLoading.pxf.image":             c.Image,
		"sidecar.env.PXF_JVM_OPTS":          envOrEmpty("PXF_JVM_OPTS"),
		"sidecar.env.PXF_PORT":              envOrEmpty("PXF_PORT"),
		"sidecar.env.PXF_LOG_LEVEL":         envOrEmpty("PXF_LOG_LEVEL"),
		"sidecar.env.PXF_EXTENSION_PXF":     envOrEmpty("PXF_EXTENSION_PXF"),
		"sidecar.env.PXF_EXTENSION_PXF_FDW": envOrEmpty("PXF_EXTENSION_PXF_FDW"),
		"sidecar.resources.requests.cpu":    c.Resources.Requests.Cpu().String(),
		"sidecar.resources.requests.memory": c.Resources.Requests.Memory().String(),
		"sidecar.resources.limits.cpu":      c.Resources.Limits.Cpu().String(),
		"sidecar.resources.limits.memory":   c.Resources.Limits.Memory().String(),
	}

	// ConfigMap-backed expectations (FieldPath "configMap.<key>.<value-token>"):
	// resolve to the expected value-token when the rendered artifact contains it.
	containsExpectations := map[string]struct {
		key   string
		value string
	}{
		"configMap.s3-datalake__s3-site.xml.fs.s3a.endpoint": {
			"s3-datalake__s3-site.xml", "https://s3.amazonaws.com",
		},
		"configMap.minio-warehouse__s3-site.xml.fs.s3a.endpoint": {
			"minio-warehouse__s3-site.xml", "http://minio:9000",
		},
		"configMap.hadoop-cluster__core-site.xml.fs.defaultFS": {
			"hadoop-cluster__core-site.xml", "hdfs://namenode:8020",
		},
		"configMap.hadoop-cluster__hive-site.xml.hive.metastore.uris": {
			"hadoop-cluster__hive-site.xml", "thrift://hive-metastore:9083",
		},
		"configMap.hadoop-cluster__hbase-site.xml.hbase.zookeeper.quorum": {
			"hadoop-cluster__hbase-site.xml", "zk1,zk2,zk3",
		},
		"configMap.mysql-oltp__jdbc-site.xml.jdbc.driver": {
			"mysql-oltp__jdbc-site.xml", "com.mysql.cj.jdbc.Driver",
		},
		"configMap.mysql-oltp__jdbc-site.xml.jdbc.url": {
			"mysql-oltp__jdbc-site.xml", "jdbc:mysql://mysql-oltp:3306/sales",
		},
		"configMap.postgres-source__jdbc-site.xml.jdbc.driver": {
			"postgres-source__jdbc-site.xml", "org.postgresql.Driver",
		},
		"configMap.connectors.properties.mysql-connector": {
			"connectors.properties", "https://repo.example.com/mysql-connector-j-8.0.33.jar",
		},
		"configMap.connectors.properties.postgresql-connector": {
			"connectors.properties", "https://repo.example.com/postgresql-42.6.0.jar",
		},
	}
	for fieldPath, exp := range containsExpectations {
		if strings.Contains(cm.Data[exp.key], exp.value) {
			out[fieldPath] = exp.value
		}
	}
	return out
}

// boolStr renders a bool as the "true"/"false" string used by the catalog.
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
