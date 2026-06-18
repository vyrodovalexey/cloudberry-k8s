//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 91: Enable Data Loading with Full PXF CRD Configuration (E2E)
// ============================================================================
//
// This suite mirrors the functional Scenario 91 cases at the e2e layer:
//   - builder-direct (infra-free): build the segment-primary PXF sidecar and the
//     servers ConfigMap from the FULL 5-server spec and assert the CRD-derived
//     env (PXF_LOG_LEVEL=INFO default), the rendered *-site.xml keys/values for
//     all 5 servers, logLevel propagation across INFO/DEBUG/WARN/ERROR, and the
//     CatalogHonest cross-check against cases.Scenario91Cases().
//   - KUBECONFIG-gated live (TestE2E_Scenario91_LivePXFConfigured): apply (or
//     patch onto) a deployed cluster the full dataLoading spec, wait for the
//     segment-primary pods to carry the "pxf" sidecar with PXF_LOG_LEVEL=INFO,
//     assert the <cluster>-pxf-servers ConfigMap exists with all 5 server keys,
//     assert status.dataLoading.pxf.configured=true + servers=5, then re-patch
//     pxf.logLevel=DEBUG and wait for the segment pod template PXF_LOG_LEVEL to
//     become DEBUG (rolling update). Skipped cleanly when KUBECONFIG is unset.
//
// ENV CONSTRAINT: MySQL, the generic Postgres JDBC source, and HBase/Zookeeper
// are NOT in the docker-compose env. Live 100MB/server ingestion is only
// exercisable against s3/MinIO, HDFS and Hive; jdbc(mysql-oltp, postgres-source)
// + hbase are CONFIG-verified only (sidecar + ConfigMap keys). This e2e test
// therefore config-verifies all 5 servers and verifies logLevel propagation; it
// does NOT assert live JDBC/HBase ingestion.
// ============================================================================

// envKubeconfigS91 gates the live cluster test.
const envKubeconfigS91 = "KUBECONFIG"

// scenario91LiveNamespace is the namespace used for the live cluster test.
const scenario91LiveNamespace = "cloudberry-test"

// scenario91LiveTimeout bounds each live wait loop.
const scenario91LiveTimeout = 5 * time.Minute

// scenario91LivePollInterval is the live poll interval.
const scenario91LivePollInterval = 5 * time.Second

// Scenario91E2ESuite tests the full PXF data-loading configuration end-to-end.
type Scenario91E2ESuite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestE2E_Scenario91(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario91E2ESuite))
}

func (s *Scenario91E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario91E2EFullDataLoading returns the FULL dataLoading spec exercised by
// this scenario: 5 PXF servers (2 s3 incl. minio-warehouse with
// fs.s3a.endpoint http://minio:9000, 1 hdfs with Config fs.defaultFS + Hive
// hive.metastore.uris + Hbase hbase.zookeeper.quorum, 2 jdbc incl. mysql-oltp
// with jdbc.driver=com.mysql.cj.jdbc.Driver and postgres-source with
// org.postgresql.Driver), 2 customConnectors, and the full pxf block
// (image, jvmOpts, port, logLevel:INFO, resources, extensions pxf=true/
// pxfFdw=false). The values mirror cases.Scenario91Cases() exactly.
func scenario91E2EFullDataLoading() *cbv1alpha1.DataLoadingSpec {
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

// scenario91E2ECluster builds a valid cluster in the given namespace with the
// full dataLoading spec attached.
func scenario91E2ECluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, namespace).Build()
	cluster.Spec.DataLoading = scenario91E2EFullDataLoading()
	return cluster
}

// scenario91E2EEnvValue returns the named env value of a container.
func scenario91E2EEnvValue(c corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// scenario91E2EPxfContainer returns the "pxf" sidecar container from a list.
func scenario91E2EPxfContainer(containers []corev1.Container) (corev1.Container, bool) {
	for _, c := range containers {
		if c.Name == "pxf" {
			return c, true
		}
	}
	return corev1.Container{}, false
}

// TestE2E_Scenario91_SidecarBuilt (builder-direct, infra-free) builds the
// segment-primary PXF sidecar from the full spec and asserts the CRD-derived env
// and probes.
func (s *Scenario91E2ESuite) TestE2E_Scenario91_SidecarBuilt() {
	cluster := scenario91E2ECluster("e2e-s91", "default")

	sts, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)
	c, present := scenario91E2EPxfContainer(sts.Spec.Template.Spec.Containers)
	require.True(s.T(), present, "segment primary StatefulSet carries the pxf sidecar")

	assertEnv := func(name, want string) {
		got, ok := scenario91E2EEnvValue(c, name)
		require.Truef(s.T(), ok, "env %s present", name)
		assert.Equalf(s.T(), want, got, "env %s value", name)
	}
	assertEnv("PXF_HOME", "/usr/local/cloudberry-pxf")
	assertEnv("PXF_BASE", "/pxf-base")
	assertEnv("PXF_JVM_OPTS", "-Xmx2g -Xms512m")
	assertEnv("PXF_PORT", "5888")
	assertEnv("PXF_LOG_LEVEL", "INFO")
	assertEnv("PXF_EXTENSION_PXF", "true")
	assertEnv("PXF_EXTENSION_PXF_FDW", "false")

	require.NotNil(s.T(), c.ReadinessProbe)
	require.NotNil(s.T(), c.ReadinessProbe.HTTPGet)
	assert.Equal(s.T(), "/actuator/health", c.ReadinessProbe.HTTPGet.Path)
	assert.Equal(s.T(), int32(5888), c.ReadinessProbe.HTTPGet.Port.IntVal)

	// Coordinator StatefulSet never carries the sidecar.
	coord, err := s.builder.BuildCoordinatorStatefulSet(cluster)
	require.NoError(s.T(), err)
	_, coordHas := scenario91E2EPxfContainer(coord.Spec.Template.Spec.Containers)
	assert.False(s.T(), coordHas, "coordinator StatefulSet must NOT carry the pxf sidecar")
}

// TestE2E_Scenario91_ServersConfigMapRendered (builder-direct) asserts the
// servers ConfigMap renders all 5 servers' *-site.xml keys/values + connectors.
func (s *Scenario91E2ESuite) TestE2E_Scenario91_ServersConfigMapRendered() {
	cluster := scenario91E2ECluster("e2e-s91-cm", "default")
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	for _, k := range []string{
		"s3-datalake__s3-site.xml",
		"minio-warehouse__s3-site.xml",
		"hadoop-cluster__core-site.xml",
		// hdfs servers ALWAYS emit hdfs-site.xml (SL.2).
		"hadoop-cluster__hdfs-site.xml",
		"hadoop-cluster__hive-site.xml",
		"hadoop-cluster__hbase-site.xml",
		"mysql-oltp__jdbc-site.xml",
		"postgres-source__jdbc-site.xml",
		"connectors.properties",
	} {
		assert.Containsf(s.T(), cm.Data, k, "ConfigMap must carry key %s", k)
	}
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hdfs-site.xml"], "<configuration>")
	assert.Contains(s.T(), cm.Data["minio-warehouse__s3-site.xml"], "http://minio:9000")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__core-site.xml"], "hdfs://namenode:8020")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hive-site.xml"], "thrift://hive-metastore:9083")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hbase-site.xml"], "zk1,zk2,zk3")
	assert.Contains(s.T(), cm.Data["mysql-oltp__jdbc-site.xml"], "com.mysql.cj.jdbc.Driver")
	assert.Contains(s.T(), cm.Data["postgres-source__jdbc-site.xml"], "org.postgresql.Driver")
	assert.Contains(s.T(), cm.Data["connectors.properties"],
		"https://repo.example.com/mysql-connector-j-8.0.33.jar")
	assert.Contains(s.T(), cm.Data["connectors.properties"],
		"https://repo.example.com/postgresql-42.6.0.jar")
}

// TestE2E_Scenario91_LogLevelPropagation (builder-direct) proves the C.6
// rebuild-from-spec semantics across all four levels.
func (s *Scenario91E2ESuite) TestE2E_Scenario91_LogLevelPropagation() {
	for _, tc := range []struct {
		name, set, want string
	}{
		{"default_empty_resolves_INFO", "", "INFO"},
		{"INFO", "INFO", "INFO"},
		{"DEBUG", "DEBUG", "DEBUG"},
		{"WARN", "WARN", "WARN"},
		{"ERROR", "ERROR", "ERROR"},
	} {
		tc := tc
		s.Run(tc.name, func() {
			cluster := scenario91E2ECluster("e2e-s91-ll", "default")
			cluster.Spec.DataLoading.Pxf.LogLevel = tc.set
			containers := s.builder.BuildPXFSidecarContainers(cluster)
			require.Len(s.T(), containers, 1)
			got, ok := scenario91E2EEnvValue(containers[0], "PXF_LOG_LEVEL")
			require.True(s.T(), ok)
			assert.Equalf(s.T(), tc.want, got,
				"pxf.logLevel=%q must propagate to PXF_LOG_LEVEL=%q", tc.set, tc.want)
		})
	}
}

// TestE2E_Scenario91_CatalogHonest (builder-direct) cross-checks the catalog
// against the live built sidecar + ConfigMap.
func (s *Scenario91E2ESuite) TestE2E_Scenario91_CatalogHonest() {
	catalog := cases.Scenario91Cases()
	require.NotEmpty(s.T(), catalog)

	cluster := scenario91E2ECluster("e2e-s91-cat", "default")
	containers := s.builder.BuildPXFSidecarContainers(cluster)
	require.Len(s.T(), containers, 1)
	c := containers[0]
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID+"_"+tc.FieldPath, func() {
			got, ok := scenario91E2ELiveValue(cluster, c, cm, tc.FieldPath)
			require.Truef(s.T(), ok, "no live accessor wired for %s", tc.FieldPath)
			assert.Equalf(s.T(), tc.ExpectedValue, got,
				"%s (%s) catalog value must match the live built object",
				tc.ID, tc.FieldPath)
		})
	}
}

// scenario91E2ELiveValue resolves a single Scenario91 catalog FieldPath against
// the live built sidecar + ConfigMap. Sidecar env / resources resolve to exact
// strings; ConfigMap value-token paths resolve to the expected token when the
// rendered artifact contains it.
func scenario91E2ELiveValue(
	cluster *cbv1alpha1.CloudberryCluster,
	c corev1.Container,
	cm *corev1.ConfigMap,
	fieldPath string,
) (string, bool) {
	env := func(name string) string {
		v, _ := scenario91E2EEnvValue(c, name)
		return v
	}
	pxf := cluster.Spec.DataLoading.Pxf
	scalar := map[string]string{
		"dataLoading.enabled":               scenario91E2EBool(cluster.Spec.DataLoading.Enabled),
		"dataLoading.pxf.enabled":           scenario91E2EBool(pxf.Enabled),
		"dataLoading.pxf.image":             c.Image,
		"sidecar.env.PXF_JVM_OPTS":          env("PXF_JVM_OPTS"),
		"sidecar.env.PXF_PORT":              env("PXF_PORT"),
		"sidecar.env.PXF_LOG_LEVEL":         env("PXF_LOG_LEVEL"),
		"sidecar.env.PXF_EXTENSION_PXF":     env("PXF_EXTENSION_PXF"),
		"sidecar.env.PXF_EXTENSION_PXF_FDW": env("PXF_EXTENSION_PXF_FDW"),
		"sidecar.resources.requests.cpu":    c.Resources.Requests.Cpu().String(),
		"sidecar.resources.requests.memory": c.Resources.Requests.Memory().String(),
		"sidecar.resources.limits.cpu":      c.Resources.Limits.Cpu().String(),
		"sidecar.resources.limits.memory":   c.Resources.Limits.Memory().String(),
	}
	if v, ok := scalar[fieldPath]; ok {
		return v, true
	}

	containsExpectations := map[string]struct{ key, value string }{
		"configMap.s3-datalake__s3-site.xml.fs.s3a.endpoint":              {"s3-datalake__s3-site.xml", "https://s3.amazonaws.com"},
		"configMap.minio-warehouse__s3-site.xml.fs.s3a.endpoint":          {"minio-warehouse__s3-site.xml", "http://minio:9000"},
		"configMap.hadoop-cluster__core-site.xml.fs.defaultFS":            {"hadoop-cluster__core-site.xml", "hdfs://namenode:8020"},
		"configMap.hadoop-cluster__hive-site.xml.hive.metastore.uris":     {"hadoop-cluster__hive-site.xml", "thrift://hive-metastore:9083"},
		"configMap.hadoop-cluster__hbase-site.xml.hbase.zookeeper.quorum": {"hadoop-cluster__hbase-site.xml", "zk1,zk2,zk3"},
		"configMap.mysql-oltp__jdbc-site.xml.jdbc.driver":                 {"mysql-oltp__jdbc-site.xml", "com.mysql.cj.jdbc.Driver"},
		"configMap.mysql-oltp__jdbc-site.xml.jdbc.url":                    {"mysql-oltp__jdbc-site.xml", "jdbc:mysql://mysql-oltp:3306/sales"},
		"configMap.postgres-source__jdbc-site.xml.jdbc.driver":            {"postgres-source__jdbc-site.xml", "org.postgresql.Driver"},
		"configMap.connectors.properties.mysql-connector":                 {"connectors.properties", "https://repo.example.com/mysql-connector-j-8.0.33.jar"},
		"configMap.connectors.properties.postgresql-connector":            {"connectors.properties", "https://repo.example.com/postgresql-42.6.0.jar"},
	}
	if exp, ok := containsExpectations[fieldPath]; ok {
		if strings.Contains(cm.Data[exp.key], exp.value) {
			return exp.value, true
		}
		return "", true
	}
	return "", false
}

// scenario91E2EBool renders a bool as "true"/"false".
func scenario91E2EBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// TestE2E_Scenario91_LivePXFConfigured is the KUBECONFIG-gated live test. It
// requires a deployed cluster: it patches the full dataLoading spec onto a
// freshly-created cluster, waits for the segment-primary StatefulSet pod
// template to carry the "pxf" sidecar with PXF_LOG_LEVEL=INFO, asserts the
// <cluster>-pxf-servers ConfigMap exists with all 5 server keys and that
// status.dataLoading.pxf.configured=true + servers=5, then re-patches
// pxf.logLevel=DEBUG and waits for the segment pod template PXF_LOG_LEVEL to
// become DEBUG (rolling update). It config-verifies all 5 servers and verifies
// logLevel propagation; it does NOT assert live JDBC/HBase ingestion (those
// backends are not in the env). Skipped cleanly when KUBECONFIG is unset.
func (s *Scenario91E2ESuite) TestE2E_Scenario91_LivePXFConfigured() {
	kubeconfig := os.Getenv(envKubeconfigS91)
	if kubeconfig == "" {
		s.T().Skip("KUBECONFIG not set, skipping live PXF-configured test")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		s.T().Skipf("could not build kubeconfig %q: %v", kubeconfig, err)
	}

	scheme := testutil.NewTestK8sEnv().Scheme
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		s.T().Skipf("could not build live client: %v", err)
	}

	name := fmt.Sprintf("live-s91-%d", time.Now().UnixNano())
	cluster := scenario91E2ECluster(name, scenario91LiveNamespace)

	if createErr := cl.Create(s.ctx, cluster); createErr != nil {
		s.T().Skipf("could not create CR on live cluster (operator/webhook/namespace "+
			"may be unavailable): %v", createErr)
	}
	defer func() {
		_ = cl.Delete(s.ctx, cluster)
	}()

	// Wait for the segment-primary StatefulSet pod template to carry the pxf
	// sidecar with PXF_LOG_LEVEL=INFO (operator reconciles + rolls the STS).
	stsName := util.SegmentPrimaryName(name)
	s.waitForSegmentPxfLogLevel(cl, stsName, "INFO")

	// The <cluster>-pxf-servers ConfigMap exists with all 5 server keys.
	cmName := builder.PxfServersConfigMapName(name)
	cm := &corev1.ConfigMap{}
	require.Eventually(s.T(), func() bool {
		getErr := cl.Get(s.ctx, types.NamespacedName{
			Name: cmName, Namespace: scenario91LiveNamespace,
		}, cm)
		return getErr == nil
	}, scenario91LiveTimeout, scenario91LivePollInterval,
		"servers ConfigMap %s must be created by the operator", cmName)
	for _, k := range []string{
		"s3-datalake__s3-site.xml",
		"minio-warehouse__s3-site.xml",
		"hadoop-cluster__core-site.xml",
		"hadoop-cluster__hive-site.xml",
		"hadoop-cluster__hbase-site.xml",
		"mysql-oltp__jdbc-site.xml",
		"postgres-source__jdbc-site.xml",
	} {
		assert.Containsf(s.T(), cm.Data, k, "live ConfigMap must carry key %s", k)
	}

	// status.dataLoading.pxf.configured=true + servers=5.
	require.Eventually(s.T(), func() bool {
		got := &cbv1alpha1.CloudberryCluster{}
		if getErr := cl.Get(s.ctx, types.NamespacedName{
			Name: name, Namespace: scenario91LiveNamespace,
		}, got); getErr != nil {
			return false
		}
		dl := got.Status.DataLoading
		return dl != nil && dl.Pxf != nil && dl.Pxf.Configured && dl.Pxf.Servers == 5
	}, scenario91LiveTimeout, scenario91LivePollInterval,
		"status.dataLoading.pxf must report configured=true, servers=5")

	// Re-patch pxf.logLevel=DEBUG and wait for the rolled segment pod template
	// PXF_LOG_LEVEL to become DEBUG (the propagation proof).
	patched := &cbv1alpha1.CloudberryCluster{}
	require.NoError(s.T(), cl.Get(s.ctx, types.NamespacedName{
		Name: name, Namespace: scenario91LiveNamespace,
	}, patched))
	patched.Spec.DataLoading.Pxf.LogLevel = "DEBUG"
	require.NoError(s.T(), cl.Update(s.ctx, patched))

	s.waitForSegmentPxfLogLevel(cl, stsName, "DEBUG")
}

// waitForSegmentPxfLogLevel polls the segment-primary StatefulSet until its pod
// template carries the "pxf" sidecar with PXF_LOG_LEVEL == want.
func (s *Scenario91E2ESuite) waitForSegmentPxfLogLevel(
	cl client.Client, stsName, want string,
) {
	require.Eventuallyf(s.T(), func() bool {
		sts := &appsv1.StatefulSet{}
		if err := cl.Get(s.ctx, types.NamespacedName{
			Name: stsName, Namespace: scenario91LiveNamespace,
		}, sts); err != nil {
			return false
		}
		c, ok := scenario91E2EPxfContainer(sts.Spec.Template.Spec.Containers)
		if !ok {
			return false
		}
		got, ok := scenario91E2EEnvValue(c, "PXF_LOG_LEVEL")
		return ok && got == want
	}, scenario91LiveTimeout, scenario91LivePollInterval,
		"segment-primary pxf sidecar PXF_LOG_LEVEL must become %q", want)
}
