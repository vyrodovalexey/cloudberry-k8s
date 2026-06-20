package builder

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	testPxfImage      = "cloudberry/pxf:2.1.0"
	testMySQLDriver   = "com.mysql.cj.jdbc.Driver"
	testJDBCURLKey    = "jdbc.url"
	testJDBCDriverKey = "jdbc.driver"
)

// newPXFTestCluster returns a cluster with the canonical 5-server PXF spec:
// 2 s3 servers, 1 hdfs server with hive + hbase, and 2 jdbc servers (one MySQL,
// one Postgres) plus custom connectors. ptr to bools exercise the extension
// toggles.
func newPXFTestCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	extPxf := true
	extFdw := false
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			Port:    5888,
			JvmOpts: "-Xmx2g -Xms512m",
			// LogLevel intentionally empty to exercise the INFO default.
			Extensions: &cbv1alpha1.PxfExtensionsSpec{Pxf: &extPxf, PxfFdw: &extFdw},
			Resources: &cbv1alpha1.ResourceRequirements{
				Requests: &cbv1alpha1.ResourceList{CPU: "500m", Memory: "512Mi"},
				Limits:   &cbv1alpha1.ResourceList{CPU: "2", Memory: "2Gi"},
			},
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "s3-primary",
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint":          "https://minio.example.com",
						"fs.s3a.path.style.access": "true",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "s3-creds", Key: "access_key"},
					},
				},
				{
					Name: "s3-secondary",
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint": "https://s3.amazonaws.com",
					},
				},
				{
					Name: "hdfs-main",
					Type: "hdfs",
					Config: map[string]string{
						"fs.defaultFS": "hdfs://namenode:8020",
					},
					Hive: map[string]string{
						"hive.metastore.uris": "thrift://hive:9083",
					},
					Hbase: map[string]string{
						"hbase.zookeeper.quorum": "zk1,zk2,zk3",
					},
				},
				{
					Name: "mysql-oltp",
					Type: "jdbc",
					Config: map[string]string{
						testJDBCDriverKey: testMySQLDriver,
						testJDBCURLKey:    "jdbc:mysql://mysql:3306/app",
					},
					Jdbc: map[string]string{
						"jdbc.pool.enabled": "true",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "mysql-creds", Key: "password"},
					},
				},
				{
					Name: "postgres-source",
					Type: "jdbc",
					Config: map[string]string{
						testJDBCDriverKey: "org.postgresql.Driver",
						testJDBCURLKey:    "jdbc:postgresql://pg:5432/src",
					},
				},
			},
			CustomConnectors: []cbv1alpha1.PxfCustomConnector{
				{Name: "mysql-connector", JarURL: "https://repo.example.com/mysql-connector-j-8.0.33.jar"},
				{Name: "custom-fmt", JarURL: "https://repo.example.com/custom-fmt-1.0.jar"},
			},
		},
	}
	return cluster
}

func findEnv(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func TestPxfSidecarEnabled_Matrix(t *testing.T) {
	t.Run("dataLoading nil => false", func(t *testing.T) {
		c := newTestCluster()
		assert.False(t, pxfSidecarEnabled(c))
	})
	t.Run("dataLoading disabled => false", func(t *testing.T) {
		c := newTestCluster()
		c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
			Enabled: false,
			Pxf:     &cbv1alpha1.PxfSpec{Enabled: true, Image: testPxfImage},
		}
		assert.False(t, pxfSidecarEnabled(c))
	})
	t.Run("pxf nil => false", func(t *testing.T) {
		c := newTestCluster()
		c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
		assert.False(t, pxfSidecarEnabled(c))
	})
	t.Run("pxf disabled => false", func(t *testing.T) {
		c := newTestCluster()
		c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
			Enabled: true,
			Pxf:     &cbv1alpha1.PxfSpec{Enabled: false, Image: testPxfImage},
		}
		assert.False(t, pxfSidecarEnabled(c))
	})
	t.Run("empty image => false", func(t *testing.T) {
		c := newTestCluster()
		c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
			Enabled: true,
			Pxf:     &cbv1alpha1.PxfSpec{Enabled: true, Image: ""},
		}
		assert.False(t, pxfSidecarEnabled(c))
	})
	t.Run("all conditions hold => true", func(t *testing.T) {
		assert.True(t, pxfSidecarEnabled(newPXFTestCluster()))
	})
}

func TestBuildPXFSidecarContainers_Disabled(t *testing.T) {
	b := NewBuilder()
	assert.Empty(t, b.BuildPXFSidecarContainers(newTestCluster()))
	assert.Empty(t, b.BuildPXFSidecarVolumes(newTestCluster()))
	assert.Nil(t, b.BuildPXFServersConfigMap(newTestCluster()))
}

func TestBuildPXFSidecarContainers_Env(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()

	containers := b.BuildPXFSidecarContainers(cluster)
	require.Len(t, containers, 1)
	c := containers[0]

	assert.Equal(t, pxfContainerName, c.Name)
	assert.Equal(t, testPxfImage, c.Image)

	// Env assertions — these names/values are the contract the next agent's
	// Scenario 91 tests assert against.
	assertEnv := func(name, want string) {
		got, ok := findEnv(c.Env, name)
		require.True(t, ok, "env %s present", name)
		assert.Equal(t, want, got, "env %s value", name)
	}
	assertEnv("PXF_HOME", "/usr/local/cloudberry-pxf")
	assertEnv("PXF_BASE", "/pxf-base")
	assertEnv("PXF_JVM_OPTS", "-Xmx2g -Xms512m")
	assertEnv("PXF_PORT", "5888")
	assertEnv("PXF_LOG_LEVEL", "INFO") // critical default
	assertEnv("PXF_EXTENSION_PXF", "true")
	assertEnv("PXF_EXTENSION_PXF_FDW", "false")

	// Port.
	require.Len(t, c.Ports, 1)
	assert.Equal(t, pxfPortName, c.Ports[0].Name)
	assert.Equal(t, int32(5888), c.Ports[0].ContainerPort)

	// Resources (requests + limits).
	require.NotNil(t, c.Resources.Requests)
	require.NotNil(t, c.Resources.Limits)
	assert.Equal(t, "500m", c.Resources.Requests.Cpu().String())
	assert.Equal(t, "512Mi", c.Resources.Requests.Memory().String())
	assert.Equal(t, "2", c.Resources.Limits.Cpu().String())
	assert.Equal(t, "2Gi", c.Resources.Limits.Memory().String())

	// Probes hit the Spring Boot actuator /actuator/health on the pxf port.
	require.NotNil(t, c.LivenessProbe)
	require.NotNil(t, c.LivenessProbe.HTTPGet)
	assert.Equal(t, pxfStatusPath, c.LivenessProbe.HTTPGet.Path)
	assert.Equal(t, int32(5888), c.LivenessProbe.HTTPGet.Port.IntVal)
	// Liveness must not rely on the pathologically tight 1s default timeout; a
	// slightly more tolerant timeout absorbs transient GC/load spikes.
	assert.GreaterOrEqual(t, c.LivenessProbe.TimeoutSeconds, int32(3),
		"liveness timeoutSeconds must be more tolerant than the 1s default")
	require.NotNil(t, c.ReadinessProbe)
	require.NotNil(t, c.ReadinessProbe.HTTPGet)
	assert.Equal(t, pxfStatusPath, c.ReadinessProbe.HTTPGet.Path)
	assert.Equal(t, int32(5888), c.ReadinessProbe.HTTPGet.Port.IntVal)

	// StartupProbe protects PXF's slow (~50s) Spring Boot cold start: it reuses
	// the SAME /actuator/health handler on the pxf port but with a generous
	// startup budget (failureThreshold * periodSeconds) so a mid-boot health
	// check never trips liveness into a SIGKILL/CrashLoopBackOff. This is what
	// removes the need for the manual StatefulSet startupProbe patch.
	require.NotNil(t, c.StartupProbe, "PXF sidecar must define a StartupProbe")
	require.NotNil(t, c.StartupProbe.HTTPGet)
	assert.Equal(t, pxfStatusPath, c.StartupProbe.HTTPGet.Path)
	assert.Equal(t, int32(5888), c.StartupProbe.HTTPGet.Port.IntVal)
	assert.Equal(t, int32(5), c.StartupProbe.PeriodSeconds,
		"startup probe period")
	assert.Equal(t, int32(24), c.StartupProbe.FailureThreshold,
		"startup probe failureThreshold (=120s boot budget at 5s period)")
	// Sanity: the startup budget must comfortably cover the ~50s boot.
	startupBudget := c.StartupProbe.PeriodSeconds * c.StartupProbe.FailureThreshold
	assert.GreaterOrEqual(t, startupBudget, int32(100),
		"startup budget must cover the slow Spring Boot cold start")

	// VolumeMounts.
	mounts := map[string]string{}
	for _, m := range c.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	assert.Equal(t, "/pxf-base", mounts[pxfBaseVolumeName])
	assert.Equal(t, "/pxf-base/servers", mounts[pxfServersVolumeName])
	assert.Equal(t, "/pxf/lib/custom", mounts[pxfLibVolumeName])
}

// TestBuildPXFSidecarContainers_ActuatorPrometheusEnv covers 109-M2/M3-U: the
// built PXF sidecar container exposes the Spring Boot Actuator Prometheus
// endpoint by carrying MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE=health,prometheus
// and MANAGEMENT_ENDPOINT_PROMETHEUS_ENABLED=true. These env→Spring-property
// vars are what make :5888/actuator/prometheus serve the REAL
// http_server_requests_seconds_* series (M.2 request count + M.3 latency
// histogram) under their native actuator names — the operator never fabricates a
// synthetic cloudberry_pxf_requests_total.
func TestBuildPXFSidecarContainers_ActuatorPrometheusEnv(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()

	containers := b.BuildPXFSidecarContainers(cluster)
	require.Len(t, containers, 1)
	c := containers[0]

	include, ok := findEnv(c.Env, "MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE")
	require.True(t, ok, "actuator exposure-include env must be present (M.2/M.3)")
	assert.Equal(t, "health,prometheus", include,
		"the actuator must expose BOTH health and the prometheus endpoint")

	enabled, ok := findEnv(c.Env, "MANAGEMENT_ENDPOINT_PROMETHEUS_ENABLED")
	require.True(t, ok, "actuator prometheus-enabled env must be present (M.2/M.3)")
	assert.Equal(t, "true", enabled)
}

// TestBuildPXFSidecarContainers_ActuatorEnv_Defaults covers 109-M2/M3-U with a
// minimal (defaults-only) PXF spec: the actuator env is still emitted, so the
// prometheus endpoint is enabled regardless of optional spec fields.
func TestBuildPXFSidecarContainers_ActuatorEnv_Defaults(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
		},
	}

	containers := b.BuildPXFSidecarContainers(cluster)
	require.Len(t, containers, 1)
	c := containers[0]

	include, ok := findEnv(c.Env, "MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE")
	require.True(t, ok)
	assert.Equal(t, "health,prometheus", include)
	enabled, ok := findEnv(c.Env, "MANAGEMENT_ENDPOINT_PROMETHEUS_ENABLED")
	require.True(t, ok)
	assert.Equal(t, "true", enabled)
}

func TestBuildPXFSidecarContainers_Defaults(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			// Port, JvmOpts, LogLevel, Extensions all unset → defaults.
		},
	}

	containers := b.BuildPXFSidecarContainers(cluster)
	require.Len(t, containers, 1)
	c := containers[0]

	got, _ := findEnv(c.Env, "PXF_PORT")
	assert.Equal(t, "5888", got)
	got, _ = findEnv(c.Env, "PXF_JVM_OPTS")
	assert.Equal(t, defaultPxfJvmOpts, got)
	got, _ = findEnv(c.Env, "PXF_LOG_LEVEL")
	assert.Equal(t, "INFO", got)
	// nil extensions => both flags default to true.
	got, _ = findEnv(c.Env, "PXF_EXTENSION_PXF")
	assert.Equal(t, "true", got)
	got, _ = findEnv(c.Env, "PXF_EXTENSION_PXF_FDW")
	assert.Equal(t, "true", got)
	assert.Equal(t, int32(5888), c.Ports[0].ContainerPort)
}

// TestBuildPXFSidecarContainers_LogLevelLoop proves rebuild-from-spec semantics:
// each logLevel value flows verbatim into PXF_LOG_LEVEL on every rebuild. This
// underpins the re-patch propagation requirement.
func TestBuildPXFSidecarContainers_LogLevelLoop(t *testing.T) {
	b := NewBuilder()
	for _, level := range []string{"DEBUG", "WARN", "ERROR"} {
		level := level
		t.Run(level, func(t *testing.T) {
			cluster := newPXFTestCluster()
			cluster.Spec.DataLoading.Pxf.LogLevel = level

			containers := b.BuildPXFSidecarContainers(cluster)
			require.Len(t, containers, 1)
			got, ok := findEnv(containers[0].Env, "PXF_LOG_LEVEL")
			require.True(t, ok)
			assert.Equal(t, level, got)
		})
	}
}

func TestBuildPXFSidecarVolumes(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()

	volumes := b.BuildPXFSidecarVolumes(cluster)
	// Four volumes: pxf-base/pxf-servers/pxf-lib emptyDirs plus the
	// ConfigMap-backed pxf-templates render source for the credential init.
	require.Len(t, volumes, 4)

	byName := map[string]corev1.Volume{}
	for _, v := range volumes {
		byName[v.Name] = v
	}

	require.Contains(t, byName, pxfBaseVolumeName)
	assert.NotNil(t, byName[pxfBaseVolumeName].EmptyDir)

	require.Contains(t, byName, pxfLibVolumeName)
	assert.NotNil(t, byName[pxfLibVolumeName].EmptyDir)

	// pxf-servers is now an emptyDir holding the RESOLVED site files (rendered by
	// the credential init container), NOT the raw ConfigMap.
	require.Contains(t, byName, pxfServersVolumeName)
	assert.NotNil(t, byName[pxfServersVolumeName].EmptyDir)
	assert.Nil(t, byName[pxfServersVolumeName].ConfigMap)

	// pxf-templates is the ConfigMap-backed render source (Optional).
	require.Contains(t, byName, pxfTemplatesVolumeName)
	cm := byName[pxfTemplatesVolumeName].ConfigMap
	require.NotNil(t, cm)
	assert.Equal(t, util.SanitizeK8sName("test-cluster-pxf-servers"), cm.Name)
	require.NotNil(t, cm.Optional)
	assert.True(t, *cm.Optional)
}

func TestBuildPXFCredentialInitContainers(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()

	inits := b.BuildPXFCredentialInitContainers(cluster)
	require.Len(t, inits, 1)
	c := inits[0]
	assert.Equal(t, pxfCredInitContainerName, c.Name)
	assert.Equal(t, cluster.Spec.DataLoading.Pxf.Image, c.Image)
	assert.Equal(t, []string{shellCommand, shellFlag}, c.Command)
	require.Len(t, c.Args, 1)

	// The script renders templates into the shared emptyDir with an envsubst +
	// POSIX fallback.
	assert.Contains(t, c.Args[0], "envsubst")
	assert.Contains(t, c.Args[0], "_PXF_ENVSUBST_EOF_")
	assert.Contains(t, c.Args[0], pxfTemplatesMountPath)
	assert.Contains(t, c.Args[0], pxfServersMountPath)

	// Mounts: read the raw templates, write the resolved files.
	mounts := map[string]string{}
	for _, m := range c.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	assert.Equal(t, pxfTemplatesMountPath, mounts[pxfTemplatesVolumeName])
	assert.Equal(t, pxfServersMountPath, mounts[pxfServersVolumeName])

	// The script writes resolved files into the NATIVE nested layout
	// (<server>/<file>.xml), so the entrypoint reorg is a no-op.
	assert.Contains(t, c.Args[0], "*__*")
	assert.Contains(t, c.Args[0], "${server}/${file}")

	// The script bounded-waits for the ConfigMap volume to be projected before
	// iterating it, so it never races the kubelet symlink swap and renders an
	// empty templates dir. The poll loops on at least one *.xml appearing and is
	// bounded (deterministic, never blocks forever).
	assert.Contains(t, c.Args[0], `ls "${SRC}"/*.xml`,
		"init script must bounded-poll for the projected ConfigMap templates")
	assert.Contains(t, c.Args[0], "sleep ",
		"init script must sleep between template-dir poll attempts")

	// Credential env: each credentialSecret reference becomes a SecretKeyRef env
	// whose Name is the SANITIZED token (uppercase, hyphen-free), never a
	// plaintext value, and the SecretKeyRef points at the raw secret name + key.
	envByName := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		envByName[e.Name] = e
	}
	for name, e := range envByName {
		require.NotNilf(t, e.ValueFrom, "env %s must be a SecretKeyRef", name)
		require.NotNil(t, e.ValueFrom.SecretKeyRef)
		assert.Empty(t, e.Value, "env %s must not carry a plaintext value", name)
		assert.NotContains(t, name, "-", "env var name %s must not contain hyphens", name)
		assert.Equal(t, strings.ToUpper(name), name, "env var name %s must be uppercased", name)
	}
	// The sanitized names for the test cluster's two credential secrets are
	// present and reference the raw (hyphenated) secret names.
	require.Contains(t, envByName, "S3_CREDS_ACCESS_KEY")
	assert.Equal(t, "s3-creds", envByName["S3_CREDS_ACCESS_KEY"].ValueFrom.SecretKeyRef.Name)
	assert.Equal(t, "access_key", envByName["S3_CREDS_ACCESS_KEY"].ValueFrom.SecretKeyRef.Key)
	require.Contains(t, envByName, "MYSQL_CREDS_PASSWORD")
	assert.Equal(t, "mysql-creds", envByName["MYSQL_CREDS_PASSWORD"].ValueFrom.SecretKeyRef.Name)

	// Disabled cluster => no init container.
	assert.Empty(t, b.BuildPXFCredentialInitContainers(newTestCluster()))
}

func TestBuildPXFServersConfigMap(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)
	assert.Equal(t, util.SanitizeK8sName("test-cluster-pxf-servers"), cm.Name)
	assert.Equal(t, "default", cm.Namespace)
	assert.Equal(t, util.ComponentPxf, cm.Labels[util.LabelComponent])
	assert.Equal(t, "test-cluster", cm.Labels[util.LabelCluster])
	require.Len(t, cm.OwnerReferences, 1)
	assert.Equal(t, "test-cluster", cm.OwnerReferences[0].Name)

	// Expected keys for all 5 servers. hdfs servers ALWAYS emit core-site.xml
	// AND hdfs-site.xml (SL.2), plus the optional hive/hbase site files from the
	// dedicated maps.
	expectedKeys := []string{
		"s3-primary__s3-site.xml",
		"s3-secondary__s3-site.xml",
		"hdfs-main__core-site.xml",
		"hdfs-main__hdfs-site.xml",
		"hdfs-main__hive-site.xml",
		"hdfs-main__hbase-site.xml",
		"mysql-oltp__jdbc-site.xml",
		"postgres-source__jdbc-site.xml",
		pxfConnectorsDataKey,
	}
	for _, k := range expectedKeys {
		assert.Contains(t, cm.Data, k, "ConfigMap key %s present", k)
	}

	// hdfs-main has no dfs.* keys, so hdfs-site.xml is a valid empty document.
	assert.Contains(t, cm.Data["hdfs-main__hdfs-site.xml"], "<configuration>")
	// No mapred/yarn keys configured → those files are NOT emitted.
	assert.NotContains(t, cm.Data, "hdfs-main__mapred-site.xml")
	assert.NotContains(t, cm.Data, "hdfs-main__yarn-site.xml")

	// Site-XML value content assertions.
	assert.Contains(t, cm.Data["s3-primary__s3-site.xml"], "fs.s3a.endpoint")
	assert.Contains(t, cm.Data["s3-primary__s3-site.xml"], "https://minio.example.com")
	assert.Contains(t, cm.Data["hdfs-main__core-site.xml"], "fs.defaultFS")
	assert.Contains(t, cm.Data["hdfs-main__core-site.xml"], "hdfs://namenode:8020")
	assert.Contains(t, cm.Data["hdfs-main__hive-site.xml"], "hive.metastore.uris")
	assert.Contains(t, cm.Data["hdfs-main__hive-site.xml"], "thrift://hive:9083")
	assert.Contains(t, cm.Data["hdfs-main__hbase-site.xml"], "hbase.zookeeper.quorum")
	assert.Contains(t, cm.Data["hdfs-main__hbase-site.xml"], "zk1,zk2,zk3")

	// jdbc.driver (MySQL) round-trips into jdbc-site.xml, plus jdbc.url.
	mysqlSite := cm.Data["mysql-oltp__jdbc-site.xml"]
	assert.Contains(t, mysqlSite, testJDBCDriverKey)
	assert.Contains(t, mysqlSite, testMySQLDriver)
	assert.Contains(t, mysqlSite, testJDBCURLKey)
	assert.Contains(t, mysqlSite, "jdbc:mysql://mysql:3306/app")
	assert.Contains(t, mysqlSite, "jdbc.pool.enabled")

	// Custom connector jarUrls are represented (sorted).
	connectors := cm.Data[pxfConnectorsDataKey]
	assert.Contains(t, connectors, "https://repo.example.com/mysql-connector-j-8.0.33.jar")
	assert.Contains(t, connectors, "https://repo.example.com/custom-fmt-1.0.jar")
	assert.Contains(t, connectors, "mysql-connector=")
	assert.Contains(t, connectors, "custom-fmt=")

	// Credential secrets render under STANDARD PXF/Hadoop property names with a
	// SANITIZED (uppercased, hyphen-free) ${PLACEHOLDER}, never raw secret text.
	// s3-primary's "access_key" secret maps to fs.s3a.access.key.
	s3Primary := cm.Data["s3-primary__s3-site.xml"]
	assert.Contains(t, s3Primary, "<name>fs.s3a.access.key</name>")
	assert.Contains(t, s3Primary, "${S3_CREDS_ACCESS_KEY}")
	assert.NotContains(t, s3Primary, "pxf.credential")
	assert.NotContains(t, s3Primary, "${s3-creds_access_key}")
	// mysql-oltp's "password" secret maps to jdbc.password.
	assert.Contains(t, mysqlSite, "<name>jdbc.password</name>")
	assert.Contains(t, mysqlSite, "${MYSQL_CREDS_PASSWORD}")
	assert.NotContains(t, mysqlSite, "pxf.credential")

	// s3-secondary has no hive/hbase keys.
	assert.NotContains(t, cm.Data, "s3-secondary__hive-site.xml")
}

// TestCredentialProperties_StandardMapping proves the BUG 2 fix: each
// credentialSecrets[] entry maps to the correct standard PXF/Hadoop property
// based on the server type and the secret key's role (key-name heuristic with
// order fallback), and hdfs/hive/hbase emit no inline credentials.
func TestCredentialProperties_StandardMapping(t *testing.T) {
	t.Run("s3 by key-name heuristic", func(t *testing.T) {
		out := credentialProperties("s3", []cbv1alpha1.SecretReference{
			{Name: "creds", Key: "aws_access_key_id"},
			{Name: "creds", Key: "aws_secret_access_key"},
		})
		assert.Equal(t, "${CREDS_AWS_ACCESS_KEY_ID}", out["fs.s3a.access.key"])
		assert.Equal(t, "${CREDS_AWS_SECRET_ACCESS_KEY}", out["fs.s3a.secret.key"])
	})
	t.Run("jdbc by key-name heuristic", func(t *testing.T) {
		out := credentialProperties("jdbc", []cbv1alpha1.SecretReference{
			{Name: "db", Key: "username"},
			{Name: "db", Key: "password"},
		})
		assert.Equal(t, "${DB_USERNAME}", out["jdbc.user"])
		assert.Equal(t, "${DB_PASSWORD}", out["jdbc.password"])
	})
	t.Run("s3 order fallback for ambiguous keys", func(t *testing.T) {
		out := credentialProperties("s3", []cbv1alpha1.SecretReference{
			{Name: "creds", Key: "k1"},
			{Name: "creds", Key: "k2"},
		})
		assert.Equal(t, "${CREDS_K1}", out["fs.s3a.access.key"])
		assert.Equal(t, "${CREDS_K2}", out["fs.s3a.secret.key"])
	})
	t.Run("hdfs/hive/hbase emit no inline credentials", func(t *testing.T) {
		for _, typ := range []string{"hdfs", "hive", "hbase", "weird"} {
			out := credentialProperties(typ, []cbv1alpha1.SecretReference{
				{Name: "creds", Key: "access_key"},
			})
			assert.Nil(t, out, "type %s must not emit inline credentials", typ)
		}
	})
	t.Run("no refs => nil", func(t *testing.T) {
		assert.Nil(t, credentialProperties("s3", nil))
	})
}

// TestPxfSanitizeEnvName proves the BUG 1 fix: invalid shell variable
// characters (notably hyphens) are replaced with "_", the result is uppercased,
// and a leading digit is prefixed with "_".
func TestPxfSanitizeEnvName(t *testing.T) {
	cases := map[string]string{
		"backup-s3-credentials_aws_access_key_id": "BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID",
		"s3-creds_access_key":                     "S3_CREDS_ACCESS_KEY",
		"already_valid":                           "ALREADY_VALID",
		"9starts-with-digit":                      "_9STARTS_WITH_DIGIT",
		"with.dots and spaces":                    "WITH_DOTS_AND_SPACES",
	}
	for in, want := range cases {
		assert.Equal(t, want, pxfSanitizeEnvName(in), "sanitize %q", in)
	}
	assert.Empty(t, pxfSanitizeEnvName(""))
}

// TestPxfCredentialEnvNameMatchesPlaceholder proves the env var Name and the
// site-XML placeholder token are produced by the SAME helper, so they can never
// diverge (the root cause of BUG 1's POSIX-fallback failure).
func TestPxfCredentialEnvNameMatchesPlaceholder(t *testing.T) {
	name, key := "backup-s3-credentials", "aws_access_key_id"
	envName := pxfCredentialEnvName(name, key)
	assert.Equal(t, "BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID", envName)
	assert.Equal(t, "${"+envName+"}", credentialPlaceholderValue(name, key))
	assert.NotContains(t, envName, "-", "env var name must not contain hyphens")
	assert.Empty(t, pxfCredentialEnvName("", "k"))
}

func TestRenderSiteXML_Deterministic(t *testing.T) {
	kv := map[string]string{
		"z.key": "1",
		"a.key": "2",
		"m.key": "3",
	}
	first := renderSiteXML(kv)
	second := renderSiteXML(kv)
	assert.Equal(t, first, second, "output is stable across runs")

	// Keys appear in sorted order.
	aIdx := strings.Index(first, "a.key")
	mIdx := strings.Index(first, "m.key")
	zIdx := strings.Index(first, "z.key")
	assert.Less(t, aIdx, mIdx)
	assert.Less(t, mIdx, zIdx)

	// Well-formed XML structure.
	assert.Contains(t, first, "<configuration>")
	assert.Contains(t, first, "</configuration>")
	assert.Contains(t, first, "<name>a.key</name>")
	assert.Contains(t, first, "<value>2</value>")
}

func TestRenderSiteXML_Empty(t *testing.T) {
	out := renderSiteXML(nil)
	assert.Contains(t, out, "<configuration>")
	assert.Contains(t, out, "</configuration>")
}

func TestRenderPXFConnectors_SortedAndEmpty(t *testing.T) {
	assert.Empty(t, renderPXFConnectors(nil))

	out := renderPXFConnectors([]cbv1alpha1.PxfCustomConnector{
		{Name: "zeta", JarURL: "u-z"},
		{Name: "alpha", JarURL: "u-a"},
	})
	alphaIdx := strings.Index(out, "alpha=")
	zetaIdx := strings.Index(out, "zeta=")
	assert.Less(t, alphaIdx, zetaIdx)
}

func TestBuildPXFServersConfigMap_HiveAndHBaseTypes(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "hive-only", Type: "hive", Config: map[string]string{"hive.metastore.uris": "thrift://h:9083"}},
				{Name: "hbase-only", Type: "hbase", Config: map[string]string{"hbase.zookeeper.quorum": "z1"}},
				{Name: "unknown-srv", Type: "weird", Config: map[string]string{"k": "v"}},
			},
		},
	}

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)
	// hive/hbase-typed servers ALWAYS emit core-site.xml in addition to their
	// dedicated site file (SL.4 / SL.5).
	assert.Contains(t, cm.Data, "hive-only__core-site.xml")
	assert.Contains(t, cm.Data, "hive-only__hive-site.xml")
	assert.Contains(t, cm.Data, "hbase-only__core-site.xml")
	assert.Contains(t, cm.Data, "hbase-only__hbase-site.xml")
	// hive-only's Config has only hive.* keys → hive-site.xml carries them and
	// core-site.xml is a valid empty document.
	assert.Contains(t, cm.Data["hive-only__hive-site.xml"], "hive.metastore.uris")
	assert.Contains(t, cm.Data["hive-only__core-site.xml"], "<configuration>")
	assert.Contains(t, cm.Data["hbase-only__hbase-site.xml"], "hbase.zookeeper.quorum")
	// Unknown type falls back to core-site.xml preserving the config.
	assert.Contains(t, cm.Data, "unknown-srv__core-site.xml")
	assert.Contains(t, cm.Data["unknown-srv__core-site.xml"], "<name>k</name>")
}

// TestBuildPXFServersConfigMap_HadoopClusterSiteFiles is the Scenario 97 SITE.*
// render assertion at the BuildPXFServersConfigMap layer (the integration-shaped
// ConfigMap, not just renderOneServer). It builds the canonical "hadoop-cluster"
// hdfs server carrying fs.defaultFS + a dedicated Hive map (hive.metastore.uris)
// + a dedicated Hbase map (hbase.zookeeper.quorum) and asserts the exact data
// keys + values land in the rendered site files:
//
//   - SITE.1: hadoop-cluster__hive-site.xml  ⊇ hive.metastore.uris = thrift://hive-metastore:9083
//   - SITE.2: hadoop-cluster__hbase-site.xml ⊇ hbase.zookeeper.quorum = hbase:2181
//   - SITE.3: hadoop-cluster__core-site.xml  ⊇ fs.defaultFS = hdfs://namenode:8020
//   - SITE.4: hadoop-cluster__hdfs-site.xml  ALWAYS present (valid <configuration>)
func TestBuildPXFServersConfigMap_HadoopClusterSiteFiles(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			Servers: []cbv1alpha1.PxfServerSpec{
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
						"hbase.zookeeper.quorum": "hbase:2181",
					},
				},
			},
		},
	}

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)

	// All four hdfs site files are present (core + hdfs ALWAYS, hive + hbase
	// from the dedicated maps).
	for _, key := range []string{
		"hadoop-cluster__core-site.xml",
		"hadoop-cluster__hdfs-site.xml",
		"hadoop-cluster__hive-site.xml",
		"hadoop-cluster__hbase-site.xml",
	} {
		assert.Contains(t, cm.Data, key, "ConfigMap must contain %s", key)
	}

	// SITE.1 — hive-site.xml carries the metastore URI property name + value.
	assert.Contains(t, cm.Data["hadoop-cluster__hive-site.xml"], "hive.metastore.uris")
	assert.Contains(t, cm.Data["hadoop-cluster__hive-site.xml"], "thrift://hive-metastore:9083")
	// SITE.2 — hbase-site.xml carries the zookeeper quorum property name + value.
	assert.Contains(t, cm.Data["hadoop-cluster__hbase-site.xml"], "hbase.zookeeper.quorum")
	assert.Contains(t, cm.Data["hadoop-cluster__hbase-site.xml"], "hbase:2181")
	// SITE.3 — core-site.xml carries fs.defaultFS.
	assert.Contains(t, cm.Data["hadoop-cluster__core-site.xml"], "fs.defaultFS")
	assert.Contains(t, cm.Data["hadoop-cluster__core-site.xml"], "hdfs://namenode:8020")
	// SITE.4 — hdfs-site.xml is ALWAYS emitted (no dfs.* keys → valid empty doc).
	assert.Contains(t, cm.Data["hadoop-cluster__hdfs-site.xml"], "<configuration>")
	// Isolation: fs.* must not leak into hdfs-site; hive/hbase keys stay in their
	// own files (and out of core-site).
	assert.NotContains(t, cm.Data["hadoop-cluster__hdfs-site.xml"], "fs.defaultFS")
	assert.NotContains(t, cm.Data["hadoop-cluster__core-site.xml"], "hive.metastore.uris")
	assert.NotContains(t, cm.Data["hadoop-cluster__core-site.xml"], "hbase.zookeeper.quorum")
}

// TestBuildPXFServersConfigMap_HadoopClusterConfigFragment proves the SITE.*
// Config-fragment variant: when the hive.*/hbase.* keys live in the server's
// Config map (instead of the dedicated Hive/Hbase maps) they route to the SAME
// hive-site.xml / hbase-site.xml files (and NOT into core-site.xml), so an
// operator can configure the hadoop-cluster server either way and get the same
// rendered server-config. Mirrors the renderOneServer "Config hive.* fallback"
// case but asserts it through BuildPXFServersConfigMap.
func TestBuildPXFServersConfigMap_HadoopClusterConfigFragment(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "hadoop-cluster",
					Type: "hdfs",
					// No dedicated Hive/Hbase maps — the hive.*/hbase.* fragment
					// lives in Config and must still route to the site files.
					Config: map[string]string{
						"fs.defaultFS":           "hdfs://namenode:8020",
						"hive.metastore.uris":    "thrift://hive-metastore:9083",
						"hbase.zookeeper.quorum": "hbase:2181",
					},
				},
			},
		},
	}

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)

	for _, key := range []string{
		"hadoop-cluster__core-site.xml",
		"hadoop-cluster__hdfs-site.xml",
		"hadoop-cluster__hive-site.xml",
		"hadoop-cluster__hbase-site.xml",
	} {
		assert.Contains(t, cm.Data, key, "ConfigMap must contain %s", key)
	}

	// Same SITE.1/SITE.2 values as the dedicated-map variant.
	assert.Contains(t, cm.Data["hadoop-cluster__hive-site.xml"], "hive.metastore.uris")
	assert.Contains(t, cm.Data["hadoop-cluster__hive-site.xml"], "thrift://hive-metastore:9083")
	assert.Contains(t, cm.Data["hadoop-cluster__hbase-site.xml"], "hbase.zookeeper.quorum")
	assert.Contains(t, cm.Data["hadoop-cluster__hbase-site.xml"], "hbase:2181")
	// SITE.3 — fs.defaultFS only in core-site.
	assert.Contains(t, cm.Data["hadoop-cluster__core-site.xml"], "hdfs://namenode:8020")
	// The hive.*/hbase.* fragment must NOT leak into core-site.xml.
	assert.NotContains(t, cm.Data["hadoop-cluster__core-site.xml"], "hive.metastore.uris")
	assert.NotContains(t, cm.Data["hadoop-cluster__core-site.xml"], "hbase.zookeeper.quorum")
}

func TestCredentialPlaceholderValue(t *testing.T) {
	// Placeholders are sanitized + uppercased (BUG 1 fix).
	assert.Equal(t, "${NAME}", credentialPlaceholderValue("name", ""))
	assert.Equal(t, "${NAME_KEY}", credentialPlaceholderValue("name", "key"))
	assert.Equal(t, "${MY_SECRET_MY_KEY}", credentialPlaceholderValue("my-secret", "my-key"))
}

func TestPxfServersConfigMapName(t *testing.T) {
	assert.Equal(t, util.SanitizeK8sName("c-pxf-servers"), PxfServersConfigMapName("c"))
}

func containerNames(c []corev1.Container) []string {
	names := make([]string, 0, len(c))
	for _, x := range c {
		names = append(names, x.Name)
	}
	return names
}

func volumeNames(v []corev1.Volume) []string {
	names := make([]string, 0, len(v))
	for _, x := range v {
		names = append(names, x.Name)
	}
	return names
}

// TestBuildSegmentPrimaryStatefulSet_PXFInjection is the blast-radius firewall:
// pxf enabled => the segment primary STS gains the pxf container + 3 volumes;
// pxf disabled / dataLoading nil => container & volume sets are unchanged
// (byte-identical baseline).
func TestBuildSegmentPrimaryStatefulSet_PXFInjection(t *testing.T) {
	b := NewBuilder()

	// Baseline: default cluster (DataLoading == nil).
	baseline := newTestCluster()
	baseSTS, err := b.BuildSegmentPrimaryStatefulSet(baseline)
	require.NoError(t, err)
	baseContainers := containerNames(baseSTS.Spec.Template.Spec.Containers)
	baseVolumes := volumeNames(baseSTS.Spec.Template.Spec.Volumes)
	assert.NotContains(t, baseContainers, pxfContainerName)
	assert.NotContains(t, baseVolumes, pxfBaseVolumeName)

	t.Run("pxf enabled adds container and volumes", func(t *testing.T) {
		cluster := newPXFTestCluster()
		sts, buildErr := b.BuildSegmentPrimaryStatefulSet(cluster)
		require.NoError(t, buildErr)

		names := containerNames(sts.Spec.Template.Spec.Containers)
		assert.Contains(t, names, pxfContainerName)
		assert.Len(t, names, len(baseContainers)+1)

		vols := volumeNames(sts.Spec.Template.Spec.Volumes)
		assert.Contains(t, vols, pxfBaseVolumeName)
		assert.Contains(t, vols, pxfServersVolumeName)
		assert.Contains(t, vols, pxfLibVolumeName)
	})

	t.Run("pxf disabled leaves segment STS unchanged", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
			Enabled: true,
			Pxf:     &cbv1alpha1.PxfSpec{Enabled: false, Image: testPxfImage},
		}
		sts, buildErr := b.BuildSegmentPrimaryStatefulSet(cluster)
		require.NoError(t, buildErr)

		assert.Equal(t, baseContainers, containerNames(sts.Spec.Template.Spec.Containers))
		assert.Equal(t, baseVolumes, volumeNames(sts.Spec.Template.Spec.Volumes))
	})

	t.Run("dataLoading nil leaves segment STS unchanged", func(t *testing.T) {
		sts, buildErr := b.BuildSegmentPrimaryStatefulSet(newTestCluster())
		require.NoError(t, buildErr)
		assert.Equal(t, baseContainers, containerNames(sts.Spec.Template.Spec.Containers))
		assert.Equal(t, baseVolumes, volumeNames(sts.Spec.Template.Spec.Volumes))
	})
}

// TestBuildCoordinatorStatefulSet_NoPXFInjection confirms PXF never leaks into
// the coordinator pod even when PXF is fully enabled (segment-primary scope).
func TestBuildCoordinatorStatefulSet_NoPXFInjection(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()
	sts, err := b.BuildCoordinatorStatefulSet(cluster)
	require.NoError(t, err)
	assert.NotContains(t, containerNames(sts.Spec.Template.Spec.Containers), pxfContainerName)
	assert.NotContains(t, volumeNames(sts.Spec.Template.Spec.Volumes), pxfBaseVolumeName)
}

// ============================================================================
// File-mapping (SL.1–SL.6) — splitHadoopSiteFiles + renderPXFServer
// ============================================================================

// TestSplitHadoopSiteFiles exercises the deterministic prefix split (T1): every
// recognized prefix routes to its canonical site file, unprefixed keys fall back
// to core-site.xml, and empty/nil input yields an empty (non-nil) map.
func TestSplitHadoopSiteFiles(t *testing.T) {
	t.Run("routes each prefix to its site file", func(t *testing.T) {
		frags := splitHadoopSiteFiles(map[string]string{
			"fs.defaultFS":             "hdfs://nn:8020",
			"dfs.replication":          "3",
			"mapreduce.framework":      "yarn",
			"mapred.job.tracker":       "jt:9001",
			"yarn.resourcemanager":     "rm:8032",
			"hive.metastore.uris":      "thrift://h:9083",
			"hbase.zookeeper.quorum":   "zk1",
			"some.unprefixed.property": "v",
		})
		assert.Equal(t, "hdfs://nn:8020", frags[pxfFileCoreSite]["fs.defaultFS"])
		assert.Equal(t, "v", frags[pxfFileCoreSite]["some.unprefixed.property"])
		assert.Equal(t, "3", frags[pxfFileHDFSSite]["dfs.replication"])
		assert.Equal(t, "yarn", frags[pxfFileMapredSite]["mapreduce.framework"])
		assert.Equal(t, "jt:9001", frags[pxfFileMapredSite]["mapred.job.tracker"])
		assert.Equal(t, "rm:8032", frags[pxfFileYarnSite]["yarn.resourcemanager"])
		assert.Equal(t, "thrift://h:9083", frags[pxfFileHiveSite]["hive.metastore.uris"])
		assert.Equal(t, "zk1", frags[pxfFileHBaseSite]["hbase.zookeeper.quorum"])
	})
	t.Run("nil input yields empty non-nil map", func(t *testing.T) {
		frags := splitHadoopSiteFiles(nil)
		require.NotNil(t, frags)
		assert.Empty(t, frags)
	})
	t.Run("only-fs input yields only core-site", func(t *testing.T) {
		frags := splitHadoopSiteFiles(map[string]string{"fs.defaultFS": "x"})
		assert.Contains(t, frags, pxfFileCoreSite)
		assert.NotContains(t, frags, pxfFileHDFSSite)
	})
}

// renderOneServer is a helper: it renders a single server into a fresh data map
// and returns it.
func renderOneServer(server *cbv1alpha1.PxfServerSpec) map[string]string {
	data := make(map[string]string)
	renderPXFServer(server, data)
	return data
}

// keySet returns the sorted set of keys of a map for exact-set assertions.
func keySet(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestRenderPXFServer_FileMapping is the SL.1–SL.6 table: for each server type it
// asserts the EXACT set of "<server>__<file>.xml" keys produced and that the
// right Hadoop properties land in the right site file, with credential
// placeholders (never literal secrets).
func TestRenderPXFServer_FileMapping(t *testing.T) {
	t.Run("SL.1 s3 → s3-site.xml only", func(t *testing.T) {
		data := renderOneServer(&cbv1alpha1.PxfServerSpec{
			Name: "s3-datalake", Type: pxfServerTypeS3,
			Config: map[string]string{"fs.s3a.endpoint": "https://minio:9000"},
			CredentialSecrets: []cbv1alpha1.SecretReference{
				{Name: "s3-creds", Key: "access_key"},
				{Name: "s3-creds", Key: "secret_key"},
			},
		})
		assert.Equal(t, []string{"s3-datalake__s3-site.xml"}, keySet(data))
		site := data["s3-datalake__s3-site.xml"]
		assert.Contains(t, site, "<name>fs.s3a.endpoint</name>")
		assert.Contains(t, site, "<name>fs.s3a.access.key</name>")
		assert.Contains(t, site, "${S3_CREDS_ACCESS_KEY}")
		assert.Contains(t, site, "${S3_CREDS_SECRET_KEY}")
		assert.NotContains(t, site, "access_key</value>") // no literal secret value
	})

	t.Run("SL.2 hdfs → core+hdfs(+hive+hbase) with dfs.* populated", func(t *testing.T) {
		data := renderOneServer(&cbv1alpha1.PxfServerSpec{
			Name: "hadoop-cluster", Type: pxfServerTypeHDFS,
			Config: map[string]string{
				"fs.defaultFS":    "hdfs://nn:8020",
				"dfs.replication": "3",
			},
			Hive:  map[string]string{"hive.metastore.uris": "thrift://hive:9083"},
			Hbase: map[string]string{"hbase.zookeeper.quorum": "zk1,zk2,zk3"},
		})
		assert.Equal(t, []string{
			"hadoop-cluster__core-site.xml",
			"hadoop-cluster__hbase-site.xml",
			"hadoop-cluster__hdfs-site.xml",
			"hadoop-cluster__hive-site.xml",
		}, keySet(data))
		assert.Contains(t, data["hadoop-cluster__core-site.xml"], "fs.defaultFS")
		assert.Contains(t, data["hadoop-cluster__hdfs-site.xml"], "dfs.replication")
		assert.Contains(t, data["hadoop-cluster__hive-site.xml"], "hive.metastore.uris")
		assert.Contains(t, data["hadoop-cluster__hbase-site.xml"], "hbase.zookeeper.quorum")
		// fs.* must NOT leak into hdfs-site.xml and vice versa.
		assert.NotContains(t, data["hadoop-cluster__hdfs-site.xml"], "fs.defaultFS")
		assert.NotContains(t, data["hadoop-cluster__core-site.xml"], "dfs.replication")
	})

	t.Run("SL.2 hdfs without dfs.* still emits minimal hdfs-site.xml", func(t *testing.T) {
		data := renderOneServer(&cbv1alpha1.PxfServerSpec{
			Name: "hdfs-min", Type: pxfServerTypeHDFS,
			Config: map[string]string{"fs.defaultFS": "hdfs://nn:8020"},
		})
		assert.Equal(t, []string{
			"hdfs-min__core-site.xml",
			"hdfs-min__hdfs-site.xml",
		}, keySet(data))
		assert.Contains(t, data["hdfs-min__hdfs-site.xml"], "<configuration>")
	})

	t.Run("SL.2 hdfs with mapreduce.*/yarn.* emits mapred+yarn", func(t *testing.T) {
		data := renderOneServer(&cbv1alpha1.PxfServerSpec{
			Name: "hdfs-yarn", Type: pxfServerTypeHDFS,
			Config: map[string]string{
				"fs.defaultFS":         "hdfs://nn:8020",
				"mapreduce.framework":  "yarn",
				"yarn.resourcemanager": "rm:8032",
			},
		})
		assert.Equal(t, []string{
			"hdfs-yarn__core-site.xml",
			"hdfs-yarn__hdfs-site.xml",
			"hdfs-yarn__mapred-site.xml",
			"hdfs-yarn__yarn-site.xml",
		}, keySet(data))
		assert.Contains(t, data["hdfs-yarn__mapred-site.xml"], "mapreduce.framework")
		assert.Contains(t, data["hdfs-yarn__yarn-site.xml"], "yarn.resourcemanager")
	})

	t.Run("SL.2 hdfs hive-site falls back to Config hive.* when no Hive map", func(t *testing.T) {
		data := renderOneServer(&cbv1alpha1.PxfServerSpec{
			Name: "hdfs-cfghive", Type: pxfServerTypeHDFS,
			Config: map[string]string{
				"fs.defaultFS":        "hdfs://nn:8020",
				"hive.metastore.uris": "thrift://hive:9083",
			},
		})
		assert.Equal(t, []string{
			"hdfs-cfghive__core-site.xml",
			"hdfs-cfghive__hdfs-site.xml",
			"hdfs-cfghive__hive-site.xml",
		}, keySet(data))
		assert.Contains(t, data["hdfs-cfghive__hive-site.xml"], "hive.metastore.uris")
		// hive.* must NOT leak into core-site.xml.
		assert.NotContains(t, data["hdfs-cfghive__core-site.xml"], "hive.metastore.uris")
	})

	t.Run("SL.3 jdbc → jdbc-site.xml only", func(t *testing.T) {
		for _, name := range []string{"mysql-oltp", "postgres-source"} {
			data := renderOneServer(&cbv1alpha1.PxfServerSpec{
				Name: name, Type: pxfServerTypeJDBC,
				Config: map[string]string{
					testJDBCDriverKey: "drv",
					testJDBCURLKey:    "jdbc:x",
				},
				CredentialSecrets: []cbv1alpha1.SecretReference{
					{Name: name + "-credentials", Key: "username"},
					{Name: name + "-credentials", Key: "password"},
				},
			})
			assert.Equal(t, []string{name + "__jdbc-site.xml"}, keySet(data))
			site := data[name+"__jdbc-site.xml"]
			assert.Contains(t, site, "<name>jdbc.driver</name>")
			assert.Contains(t, site, "<name>jdbc.url</name>")
			assert.Contains(t, site, "<name>jdbc.user</name>")
			assert.Contains(t, site, "<name>jdbc.password</name>")
			assert.Contains(t, site, "${"+pxfSanitizeEnvName(name+"-credentials_password")+"}")
		}
	})

	t.Run("SL.4 hive → core-site + hive-site (dedicated map wins)", func(t *testing.T) {
		data := renderOneServer(&cbv1alpha1.PxfServerSpec{
			Name: "hive-warehouse", Type: pxfServerTypeHive,
			Config: map[string]string{
				"fs.defaultFS":        "hdfs://nn:8020",
				"hive.metastore.uris": "thrift://ignored:1", // overridden by Hive map
			},
			Hive: map[string]string{"hive.metastore.uris": "thrift://hive:9083"},
		})
		assert.Equal(t, []string{
			"hive-warehouse__core-site.xml",
			"hive-warehouse__hive-site.xml",
		}, keySet(data))
		assert.Contains(t, data["hive-warehouse__core-site.xml"], "fs.defaultFS")
		assert.Contains(t, data["hive-warehouse__hive-site.xml"], "thrift://hive:9083")
		assert.NotContains(t, data["hive-warehouse__hive-site.xml"], "thrift://ignored:1")
	})

	t.Run("SL.4 hive empty Config still emits core-site", func(t *testing.T) {
		data := renderOneServer(&cbv1alpha1.PxfServerSpec{
			Name: "hive-empty", Type: pxfServerTypeHive,
			Hive: map[string]string{"hive.metastore.uris": "thrift://hive:9083"},
		})
		assert.Equal(t, []string{
			"hive-empty__core-site.xml",
			"hive-empty__hive-site.xml",
		}, keySet(data))
		assert.Contains(t, data["hive-empty__core-site.xml"], "<configuration>")
	})

	t.Run("SL.5 hbase → core-site + hbase-site (dedicated map wins)", func(t *testing.T) {
		data := renderOneServer(&cbv1alpha1.PxfServerSpec{
			Name: "hbase-store", Type: pxfServerTypeHBase,
			Config: map[string]string{
				"fs.defaultFS":           "hdfs://nn:8020",
				"hbase.zookeeper.quorum": "ignored",
			},
			Hbase: map[string]string{"hbase.zookeeper.quorum": "zk1,zk2,zk3"},
		})
		assert.Equal(t, []string{
			"hbase-store__core-site.xml",
			"hbase-store__hbase-site.xml",
		}, keySet(data))
		assert.Contains(t, data["hbase-store__core-site.xml"], "fs.defaultFS")
		assert.Contains(t, data["hbase-store__hbase-site.xml"], "zk1,zk2,zk3")
		assert.NotContains(t, data["hbase-store__hbase-site.xml"], ">ignored<")
	})

	t.Run("SL.5 hbase Config hbase.* fallback when no Hbase map", func(t *testing.T) {
		data := renderOneServer(&cbv1alpha1.PxfServerSpec{
			Name: "hbase-cfg", Type: pxfServerTypeHBase,
			Config: map[string]string{
				"fs.defaultFS":           "hdfs://nn:8020",
				"hbase.zookeeper.quorum": "zkA",
			},
		})
		assert.Equal(t, []string{
			"hbase-cfg__core-site.xml",
			"hbase-cfg__hbase-site.xml",
		}, keySet(data))
		assert.Contains(t, data["hbase-cfg__hbase-site.xml"], "zkA")
		assert.NotContains(t, data["hbase-cfg__core-site.xml"], "hbase.zookeeper.quorum")
	})
}

// TestRenderPXFServer_ObjectStoreConfig is the Scenario 96 server-config matrix
// (CFG.1-CFG.8). Every object-store server type (s3/gs/abfss/wasbs — incl. the
// Dell-ECS custom-endpoint and MinIO path-style variants of s3) routes through
// the SAME s3-site.xml renderer: the emitted ConfigMap key is
// "<server>__s3-site.xml" and the expected fs.* property keys/values are present.
func TestRenderPXFServer_ObjectStoreConfig(t *testing.T) {
	tests := []struct {
		name    string
		server  cbv1alpha1.PxfServerSpec
		wantKey string
		// contains are substrings that MUST appear in the rendered s3-site.xml.
		contains []string
		// absent are substrings that must NOT appear.
		absent []string
	}{
		{
			// CFG (OS.1-OS.5): AWS-style s3 datalake — endpoint + credentials
			// fold into the standard fs.s3a.access.key/secret.key properties.
			name: "CFG s3-datalake AWS endpoint+creds",
			server: cbv1alpha1.PxfServerSpec{
				Name: "s3-datalake", Type: pxfServerTypeS3,
				Config: map[string]string{"fs.s3a.endpoint": "s3.amazonaws.com"},
				CredentialSecrets: []cbv1alpha1.SecretReference{
					{Name: "aws-creds", Key: "access_key"},
					{Name: "aws-creds", Key: "secret_key"},
				},
			},
			wantKey: "s3-datalake__s3-site.xml",
			contains: []string{
				"<name>fs.s3a.endpoint</name>", "<value>s3.amazonaws.com</value>",
				"<name>fs.s3a.access.key</name>", "${AWS_CREDS_ACCESS_KEY}",
				"<name>fs.s3a.secret.key</name>", "${AWS_CREDS_SECRET_KEY}",
			},
			absent: []string{"<value>access_key</value>", "pxf.credential"},
		},
		{
			// CFG minio-warehouse: MinIO is an s3 server with path-style access.
			name: "CFG minio-warehouse path-style",
			server: cbv1alpha1.PxfServerSpec{
				Name: "minio-warehouse", Type: pxfServerTypeS3,
				Config: map[string]string{
					"fs.s3a.endpoint":          "http://minio:9000",
					"fs.s3a.path.style.access": "true",
				},
			},
			wantKey: "minio-warehouse__s3-site.xml",
			contains: []string{
				"<name>fs.s3a.path.style.access</name>", "<value>true</value>",
				"<name>fs.s3a.endpoint</name>", "<value>http://minio:9000</value>",
			},
		},
		{
			// CFG.1/CFG.2: GCS — gs type routes to s3-site.xml (object-store
			// renderer); the fs.* keys are preserved.
			name: "CFG.1/CFG.2 gcs-datalake gs type → s3-site.xml",
			server: cbv1alpha1.PxfServerSpec{
				Name: "gcs-datalake", Type: pxfServerTypeGS,
				Config: map[string]string{
					"fs.gs.project.id": "my-project",
					"fs.s3a.endpoint":  "storage.googleapis.com",
				},
				CredentialSecrets: []cbv1alpha1.SecretReference{
					{Name: "gcs-creds", Key: "access"},
					{Name: "gcs-creds", Key: "secret"},
				},
			},
			wantKey: "gcs-datalake__s3-site.xml",
			contains: []string{
				"<name>fs.gs.project.id</name>", "<value>my-project</value>",
				"<name>fs.s3a.access.key</name>", "${GCS_CREDS_ACCESS}",
				"<name>fs.s3a.secret.key</name>", "${GCS_CREDS_SECRET}",
			},
		},
		{
			// CFG.3/CFG.4: Azure ADLS Gen2 — abfss type → s3-site.xml.
			name: "CFG.3/CFG.4 adls-gen2 abfss type → s3-site.xml",
			server: cbv1alpha1.PxfServerSpec{
				Name: "adls-gen2", Type: pxfServerTypeAbfss,
				Config: map[string]string{
					"fs.azure.account.auth.type": "SharedKey",
				},
			},
			wantKey: "adls-gen2__s3-site.xml",
			contains: []string{
				"<name>fs.azure.account.auth.type</name>", "<value>SharedKey</value>",
			},
		},
		{
			// CFG.5/CFG.6: Azure Blob — wasbs type → s3-site.xml.
			name: "CFG.5/CFG.6 azure-blob wasbs type → s3-site.xml",
			server: cbv1alpha1.PxfServerSpec{
				Name: "azure-blob", Type: pxfServerTypeWasbs,
				Config: map[string]string{
					"fs.azure.storage.emulator.account.name": "devstoreaccount1",
				},
			},
			wantKey: "azure-blob__s3-site.xml",
			contains: []string{
				"<name>fs.azure.storage.emulator.account.name</name>",
				"<value>devstoreaccount1</value>",
			},
		},
		{
			// CFG.7/CFG.8: Dell ECS — an s3 server with a CUSTOM fs.s3a.endpoint
			// override; the override renders into the s3-site.xml verbatim.
			name: "CFG.7/CFG.8 dell-ecs s3 custom endpoint override",
			server: cbv1alpha1.PxfServerSpec{
				Name: "dell-ecs", Type: pxfServerTypeS3,
				Config: map[string]string{
					"fs.s3a.endpoint":               "https://ecs.dell.example.com:9021",
					"fs.s3a.connection.ssl.enabled": "true",
				},
				CredentialSecrets: []cbv1alpha1.SecretReference{
					{Name: "ecs-creds", Key: "access_key"},
					{Name: "ecs-creds", Key: "secret_key"},
				},
			},
			wantKey: "dell-ecs__s3-site.xml",
			contains: []string{
				"<name>fs.s3a.endpoint</name>",
				"<value>https://ecs.dell.example.com:9021</value>",
				"<name>fs.s3a.connection.ssl.enabled</name>", "<value>true</value>",
				"<name>fs.s3a.access.key</name>", "${ECS_CREDS_ACCESS_KEY}",
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			data := renderOneServer(&tc.server)

			// The emitted key set is EXACTLY the single s3-site.xml key — all
			// object stores route through the s3-site.xml renderer.
			assert.Equal(t, []string{tc.wantKey}, keySet(data),
				"object-store server must emit exactly one s3-site.xml key")

			site := data[tc.wantKey]
			assert.Contains(t, site, "<configuration>")
			for _, want := range tc.contains {
				assert.Contains(t, site, want, "s3-site.xml must contain %q", want)
			}
			for _, no := range tc.absent {
				assert.NotContains(t, site, no, "s3-site.xml must not contain %q", no)
			}
		})
	}
}

// TestBuildPXFServersConfigMap_ObjectStoreServers proves CFG.* end-to-end via
// the public ConfigMap builder: a cluster declaring gs/abfss/wasbs/dell-ecs/
// minio servers produces the "<server>__s3-site.xml" keys with the expected
// path-style / endpoint properties present.
func TestBuildPXFServersConfigMap_ObjectStoreServers(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   testPxfImage,
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "gcs-datalake", Type: pxfServerTypeGS, Config: map[string]string{"fs.gs.project.id": "p1"}},
				{Name: "adls-gen2", Type: pxfServerTypeAbfss, Config: map[string]string{"fs.azure.account.auth.type": "SharedKey"}},
				{Name: "azure-blob", Type: pxfServerTypeWasbs, Config: map[string]string{"fs.azure.account.key": "k"}},
				{
					Name: "minio-warehouse", Type: pxfServerTypeS3,
					Config: map[string]string{
						"fs.s3a.endpoint":          "http://minio:9000",
						"fs.s3a.path.style.access": "true",
					},
				},
			},
		},
	}

	cm := b.BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm)

	for _, key := range []string{
		"gcs-datalake__s3-site.xml",
		"adls-gen2__s3-site.xml",
		"azure-blob__s3-site.xml",
		"minio-warehouse__s3-site.xml",
	} {
		assert.Contains(t, cm.Data, key, "ConfigMap must contain %q", key)
	}

	// MinIO path-style key/value present in the rendered s3-site.xml.
	assert.Contains(t, cm.Data["minio-warehouse__s3-site.xml"], "fs.s3a.path.style.access")
	assert.Contains(t, cm.Data["minio-warehouse__s3-site.xml"], "<value>true</value>")
	assert.Contains(t, cm.Data["gcs-datalake__s3-site.xml"], "fs.gs.project.id")

	// Object-store servers never emit hdfs/core/hive/hbase site files.
	for _, absent := range []string{
		"gcs-datalake__core-site.xml",
		"adls-gen2__hdfs-site.xml",
		"azure-blob__core-site.xml",
	} {
		assert.NotContains(t, cm.Data, absent)
	}
}

// TestRenderPXFServer_ByteStable proves the rendered site bodies are
// byte-identical across repeated renders (sorted keys → deterministic output).
func TestRenderPXFServer_ByteStable(t *testing.T) {
	spec := &cbv1alpha1.PxfServerSpec{
		Name: "hadoop-cluster", Type: pxfServerTypeHDFS,
		Config: map[string]string{
			"fs.defaultFS":     "hdfs://nn:8020",
			"dfs.replication":  "3",
			"dfs.nameservices": "ns1",
		},
		Hive: map[string]string{"hive.metastore.uris": "thrift://hive:9083"},
	}
	first := renderOneServer(spec)
	second := renderOneServer(spec)
	assert.Equal(t, first, second)
}

// TestRenderPXFServer_NoLiteralSecrets asserts SL.6 across every credentialed
// server type: the rendered XML carries ${PLACEHOLDER} tokens, never the literal
// secret name/value content beyond the placeholder.
func TestRenderPXFServer_NoLiteralSecrets(t *testing.T) {
	servers := []cbv1alpha1.PxfServerSpec{
		{
			Name: "s3", Type: pxfServerTypeS3,
			CredentialSecrets: []cbv1alpha1.SecretReference{{Name: "sec", Key: "access"}},
		},
		{
			Name: "db", Type: pxfServerTypeJDBC,
			CredentialSecrets: []cbv1alpha1.SecretReference{{Name: "sec", Key: "password"}},
		},
	}
	for i := range servers {
		data := renderOneServer(&servers[i])
		for k, body := range data {
			assert.Contains(t, body, "${", "key %s carries a placeholder", k)
			// The placeholder is the sanitized token, never the raw key value.
			assert.NotContains(t, body, "<value>access</value>")
			assert.NotContains(t, body, "<value>password</value>")
		}
	}
}
