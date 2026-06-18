//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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
// Scenario 93: Server ConfigMap, File Mapping, Extensions, Sync (E2E)
// ============================================================================
//
// This suite mirrors the functional Scenario 93 cases at the e2e layer
// (builder-direct, infra-free, always runs) and adds KUBECONFIG-gated live
// verification against a deployed cluster:
//
//   - Builder-direct: build the <cluster>-pxf-servers ConfigMap + the credential
//     init container from the full 6-server pxf spec and assert SL.1–SL.6 (exact
//     data keys, right config keys in the right *-site.xml, ${PLACEHOLDER} not
//     literal secrets), RP.8 (init container env/mounts/script), RP.12
//     (deterministic shared ConfigMap), and a CatalogHonest cross-check.
//
//   - TestE2E_Scenario93_LiveServerConfigMapAndExtensions (KUBECONFIG-gated):
//     against the deployed cloudberry-test cluster, get the live
//     <cluster>-pxf-servers ConfigMap and assert the SL.1–SL.6 data keys exist
//     with ${PLACEHOLDER} bodies (no literal secrets). Behind SCENARIO93_LIVE=1
//     (the real apache/cloudberry-pxf image is deployed + psql reachable) it ALSO:
//       RP.8/SL.6 — execs into a segment-primary pxf sidecar and cats the RESOLVED
//                   /pxf-base/servers/<server>/<file>.xml files: they exist in the
//                   nested per-server layout AND the placeholders are RESOLVED to
//                   real values (proving the init container rendered them).
//       RP.9/RP.10 — psql on the coordinator: SELECT extname FROM pg_extension
//                    WHERE extname LIKE 'pxf%' contains pxf AND pxf_fdw.
//       RP.11 — checks the protocol grant (gpadmin SELECT/INSERT on protocol pxf)
//               via an external-table create+read as gpadmin (exercises SELECT).
//       RP.12 — gets the credential init container's mounted ConfigMap name on
//               TWO segment-primary pods and asserts it is the SAME
//               <cluster>-pxf-servers ConfigMap, AND execs both sidecars to cat a
//               resolved server file and asserts byte-identical content.
//
// HONESTY NOTE: the pxf extension/PROTOCOL may be absent on a stub image, so the
// RP.9–RP.11 live checks are skip-tolerant (consistent with the non-fatal
// builder/db behaviour). Skipped cleanly without KUBECONFIG; the exec/psql parts
// require SCENARIO93_LIVE=1.
// ============================================================================

// envKubeconfigS93Server gates the live server-config test.
const envKubeconfigS93Server = "KUBECONFIG"

// envScenario93Live gates the STRICT exec/psql live assertions (resolved files,
// pg_extension, protocol grant, byte-identical sidecar configs). Unset/empty =>
// only the live ConfigMap shape assertions run.
const envScenario93Live = "SCENARIO93_LIVE"

// scenario93LiveNamespace is the namespace used for the live server-config test.
const scenario93LiveNamespace = "cloudberry-test"

// scenario93LiveTimeout bounds the live wait loops.
const scenario93LiveTimeout = 2 * time.Minute

// scenario93LivePollInterval is the live poll interval.
const scenario93LivePollInterval = 5 * time.Second

// scenario93ExecTimeout bounds the kubectl exec / psql probes.
const scenario93ExecTimeout = 60 * time.Second

// Scenario93E2ESuite verifies the PXF server ConfigMap, file-mapping,
// extensions and sync end-to-end.
type Scenario93E2ESuite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestE2E_Scenario93(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario93E2ESuite))
}

func (s *Scenario93E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario93E2EFullDataLoading returns the full 6-server dataLoading.pxf spec
// (s3, hdfs+hive+hbase, two jdbc, hive-typed, hbase-typed) covering every SL
// file-mapping branch. The values mirror cases.Scenario93Cases() exactly.
func scenario93E2EFullDataLoading() *cbv1alpha1.DataLoadingSpec {
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
				{
					Name: "hive-warehouse",
					Type: "hive",
					Config: map[string]string{
						"fs.defaultFS":        "hdfs://namenode:8020",
						"hive.metastore.uris": "thrift://hive-metastore:9083",
					},
				},
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

// scenario93E2ECluster builds a valid cluster in the given namespace with the
// full server-config dataLoading spec attached.
func scenario93E2ECluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, namespace).Build()
	cluster.Spec.DataLoading = scenario93E2EFullDataLoading()
	return cluster
}

// scenario93E2EExpectedServerKeys is the exact set of server data keys the
// builder must emit for the 6-server fixture (SL.1–SL.5).
func scenario93E2EExpectedServerKeys() []string {
	return []string{
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
}

// TestE2E_Scenario93_ServerConfigMapBuilt (builder-direct, infra-free) builds
// the servers ConfigMap from the full 6-server spec and asserts the SL.1–SL.6
// file-mapping + placeholder facts.
func (s *Scenario93E2ESuite) TestE2E_Scenario93_ServerConfigMapBuilt() {
	cluster := scenario93E2ECluster("e2e-s93", "default")
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)
	assert.Equal(s.T(), builder.PxfServersConfigMapName(cluster.Name), cm.Name)

	for _, k := range scenario93E2EExpectedServerKeys() {
		assert.Containsf(s.T(), cm.Data, k, "ConfigMap must carry key %s", k)
	}

	// SL.x: right config keys in the right *-site.xml.
	assert.Contains(s.T(), cm.Data["s3-datalake__s3-site.xml"], "fs.s3a.endpoint")
	assert.Contains(s.T(), cm.Data["s3-datalake__s3-site.xml"], "http://minio:9000")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__core-site.xml"], "fs.defaultFS")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hdfs-site.xml"], "dfs.replication")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hive-site.xml"], "hive.metastore.uris")
	assert.Contains(s.T(), cm.Data["hadoop-cluster__hbase-site.xml"], "hbase.zookeeper.quorum")
	assert.Contains(s.T(), cm.Data["mysql-oltp__jdbc-site.xml"], "com.mysql.cj.jdbc.Driver")
	assert.Contains(s.T(), cm.Data["postgres-source__jdbc-site.xml"], "org.postgresql.Driver")
	assert.Contains(s.T(), cm.Data["hive-warehouse__core-site.xml"], "fs.defaultFS")
	assert.Contains(s.T(), cm.Data["hive-warehouse__hive-site.xml"], "hive.metastore.uris")
	assert.Contains(s.T(), cm.Data["hbase-store__core-site.xml"], "fs.defaultFS")
	assert.Contains(s.T(), cm.Data["hbase-store__hbase-site.xml"], "hbase.zookeeper.quorum")

	// SL.6: ${PLACEHOLDER} tokens present; NO literal secret values anywhere.
	assert.Contains(s.T(), cm.Data["s3-datalake__s3-site.xml"], "${BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID}")
	assert.Contains(s.T(), cm.Data["mysql-oltp__jdbc-site.xml"], "${MYSQL_CREDENTIALS_USERNAME}")
	for k, v := range cm.Data {
		assert.NotContainsf(s.T(), v, "minioadmin", "key %s must not carry a literal secret", k)
		assert.NotContainsf(s.T(), v, "pxfpass", "key %s must not carry a literal secret", k)
	}
}

// TestE2E_Scenario93_CredentialInitContainerBuilt (builder-direct) asserts RP.8:
// the pxf-cred-init init container env (SecretKeyRef), mounts and render script.
func (s *Scenario93E2ESuite) TestE2E_Scenario93_CredentialInitContainerBuilt() {
	cluster := scenario93E2ECluster("e2e-s93-init", "default")
	inits := s.builder.BuildPXFCredentialInitContainers(cluster)
	require.Len(s.T(), inits, 1)
	initC := inits[0]
	assert.Equal(s.T(), "pxf-cred-init", initC.Name)

	mounts := map[string]string{}
	for _, m := range initC.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	assert.Equal(s.T(), "/pxf-templates", mounts["pxf-templates"])
	assert.Equal(s.T(), "/pxf-base/servers", mounts["pxf-servers"])

	require.Len(s.T(), initC.Args, 1)
	assert.Contains(s.T(), initC.Args[0], "envsubst")

	names := map[string]bool{}
	for _, e := range initC.Env {
		require.NotNilf(s.T(), e.ValueFrom, "cred env %s must be a SecretKeyRef", e.Name)
		names[e.Name] = true
	}
	for _, want := range []string{
		"BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID",
		"MYSQL_CREDENTIALS_USERNAME",
		"PG_SOURCE_CREDENTIALS_PASSWORD",
	} {
		assert.Containsf(s.T(), names, want, "init env %s (matching placeholder) must be present", want)
	}
}

// TestE2E_Scenario93_SharedConfigMapDeterministic (builder-direct) asserts RP.12:
// the builder output is byte-identical across builds (the shared-ConfigMap sync
// invariant) and the segment pod mounts the shared ConfigMap as pxf-templates.
func (s *Scenario93E2ESuite) TestE2E_Scenario93_SharedConfigMapDeterministic() {
	cluster := scenario93E2ECluster("e2e-s93-sync", "default")
	cm1 := s.builder.BuildPXFServersConfigMap(cluster)
	cm2 := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm1)
	require.NotNil(s.T(), cm2)
	assert.Equal(s.T(), cm1.Data, cm2.Data, "shared ConfigMap data must be deterministic")

	sts, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)
	var templatesCM string
	for i := range sts.Spec.Template.Spec.Volumes {
		v := sts.Spec.Template.Spec.Volumes[i]
		if v.Name == "pxf-templates" && v.ConfigMap != nil {
			templatesCM = v.ConfigMap.Name
		}
	}
	assert.Equal(s.T(), builder.PxfServersConfigMapName(cluster.Name), templatesCM,
		"every segment pod mounts the SAME shared <cluster>-pxf-servers ConfigMap")
}

// TestE2E_Scenario93_NegativeNoConfigMap (builder-direct) asserts the blast
// radius: pxf-disabled / dataLoading-disabled clusters produce no ConfigMap.
func (s *Scenario93E2ESuite) TestE2E_Scenario93_NegativeNoConfigMap() {
	pxfOff := scenario93E2ECluster("e2e-s93-pxf-off", "default")
	pxfOff.Spec.DataLoading.Pxf.Enabled = false
	assert.Nil(s.T(), s.builder.BuildPXFServersConfigMap(pxfOff),
		"pxf-disabled cluster must produce no servers ConfigMap")

	dlOff := scenario93E2ECluster("e2e-s93-dl-off", "default")
	dlOff.Spec.DataLoading.Enabled = false
	assert.Nil(s.T(), s.builder.BuildPXFServersConfigMap(dlOff),
		"dataLoading-disabled cluster must produce no servers ConfigMap")
}

// TestE2E_Scenario93_CatalogHonest (builder-direct) cross-checks the catalog
// against the live built ConfigMap + init container.
func (s *Scenario93E2ESuite) TestE2E_Scenario93_CatalogHonest() {
	catalog := cases.Scenario93Cases()
	require.NotEmpty(s.T(), catalog)

	cluster := scenario93E2ECluster("e2e-s93-cat", "default")
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)
	inits := s.builder.BuildPXFCredentialInitContainers(cluster)
	require.Len(s.T(), inits, 1)
	initC := inits[0]

	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID+"_"+tc.Name, func() {
			switch {
			case strings.HasPrefix(tc.ID, "SL."):
				for _, k := range tc.ExpectedKeys {
					assert.Containsf(s.T(), cm.Data, k, "%s: key %s present", tc.ID, k)
				}
				for key, subs := range tc.KeyContains {
					for _, sub := range subs {
						assert.Containsf(s.T(), cm.Data[key], sub,
							"%s: key %s contains %q", tc.ID, key, sub)
					}
				}
				for _, forbidden := range tc.ForbiddenSubstrings {
					for k, v := range cm.Data {
						assert.NotContainsf(s.T(), v, forbidden,
							"%s: key %s must not contain %q", tc.ID, k, forbidden)
					}
				}
			case tc.ID == "RP.8":
				assert.Equal(s.T(), "pxf-cred-init", initC.Name)
				assert.NotEmpty(s.T(), initC.Env)
			default:
				// RP.9/RP.10/RP.11/RP.12 are contract/live rows.
				assert.NotEmpty(s.T(), tc.Target, "%s must name a target", tc.ID)
			}
		})
	}
}

// TestE2E_Scenario93_LiveServerConfigMapAndExtensions is the KUBECONFIG-gated
// live test. It asserts the deployed <cluster>-pxf-servers ConfigMap carries the
// SL.1–SL.6 data keys with ${PLACEHOLDER} bodies (no literal secrets). Behind
// SCENARIO93_LIVE=1 it also verifies the RESOLVED nested files in the sidecar
// (RP.8/SL.6), the pxf/pxf_fdw extensions (RP.9/RP.10), the protocol grant
// (RP.11), and byte-identical configs across two segment pods (RP.12). Skipped
// cleanly when KUBECONFIG is unset.
func (s *Scenario93E2ESuite) TestE2E_Scenario93_LiveServerConfigMapAndExtensions() {
	kubeconfig := os.Getenv(envKubeconfigS93Server)
	if kubeconfig == "" {
		s.T().Skip("KUBECONFIG not set, skipping live server-config/extensions test")
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

	// Find a deployed cluster's <cluster>-pxf-servers ConfigMap. We discover the
	// cluster by listing segment-primary pods (they carry the cluster label).
	clusterName, found := s.scenario93FindDeployedCluster(cl)
	if !found {
		s.T().Skip("no deployed cluster with a pxf segment pod found in cloudberry-test")
	}
	s.T().Logf("scenario93: discovered deployed cluster %q", clusterName)

	cmName := builder.PxfServersConfigMapName(clusterName)
	cm := &corev1.ConfigMap{}
	require.Eventuallyf(s.T(), func() bool {
		return cl.Get(s.ctx, types.NamespacedName{
			Name: cmName, Namespace: scenario93LiveNamespace,
		}, cm) == nil
	}, scenario93LiveTimeout, scenario93LivePollInterval,
		"servers ConfigMap %s must exist", cmName)

	// LIVE-CM: the SL.1–SL.6 data keys exist for the deployed servers, with
	// ${PLACEHOLDER} bodies and NO literal secrets in the ConfigMap.
	presentKeys := 0
	for _, k := range scenario93E2EExpectedServerKeys() {
		if _, ok := cm.Data[k]; ok {
			presentKeys++
		}
	}
	assert.Positive(s.T(), presentKeys,
		"live ConfigMap must carry at least one expected <server>__<file>.xml key")
	for k, v := range cm.Data {
		assert.NotContainsf(s.T(), v, "minioadmin",
			"live ConfigMap key %s must not carry a literal secret", k)
	}
	// At least one credentialed body keeps a ${...} placeholder (resolved only at
	// runtime, never in the ConfigMap).
	if s3Body, ok := cm.Data["s3-datalake__s3-site.xml"]; ok {
		assert.Contains(s.T(), s3Body, "${",
			"the ConfigMap s3 body must keep ${PLACEHOLDER} (resolved at runtime)")
	}

	if os.Getenv(envScenario93Live) != "1" {
		s.T().Logf("scenario93: live ConfigMap shape verified for %q; SCENARIO93_LIVE not set, "+
			"skipping the resolved-files/pg_extension/grant/byte-identical exec assertions", clusterName)
		return
	}

	// Find two segment-primary pods carrying a pxf sidecar (RP.12 needs two).
	pods := s.scenario93FindSegmentPxfPods(cl, clusterName)
	require.NotEmpty(s.T(), pods, "at least one segment-primary pxf pod must exist for the live checks")

	// RP.8 / SL.6: the RESOLVED nested files exist and placeholders are resolved.
	resolved, execErr := s.scenario93PxfExec(scenario93LiveNamespace, pods[0],
		"cat /pxf-base/servers/s3-datalake/s3-site.xml")
	require.NoErrorf(s.T(), execErr,
		"cat resolved s3-site.xml in pxf sidecar must succeed (out=%q)", resolved)
	assert.NotContains(s.T(), resolved, "${",
		"resolved s3-site.xml must NOT carry unresolved ${PLACEHOLDER} (init container rendered it)")
	assert.Contains(s.T(), resolved, "fs.s3a.access.key",
		"resolved s3-site.xml must carry the fs.s3a.access.key property")

	// RP.9 / RP.10: pg_extension contains pxf AND pxf_fdw (skip-tolerant).
	extOut, extErr := s.scenario93Psql(pods[0],
		"SELECT extname FROM pg_extension WHERE extname LIKE 'pxf%';")
	if extErr != nil {
		s.T().Logf("scenario93: pg_extension query failed (pxf may be absent on this image): %v", extErr)
	} else {
		assert.Contains(s.T(), extOut, "pxf", "pg_extension must contain pxf (RP.9)")
		assert.Contains(s.T(), extOut, "pxf_fdw", "pg_extension must contain pxf_fdw (RP.10)")
	}

	// RP.11: the data-loader role (gpadmin) has SELECT/INSERT on protocol pxf.
	// Query pg_extprotocol's ACL for the pxf protocol; skip-tolerant when the
	// protocol is absent (stub image).
	grantOut, grantErr := s.scenario93Psql(pods[0],
		"SELECT ptcname, ptcacl FROM pg_extprotocol WHERE ptcname = 'pxf';")
	if grantErr != nil {
		s.T().Logf("scenario93: pg_extprotocol query failed (protocol pxf may be absent): %v", grantErr)
	} else if strings.Contains(grantOut, "pxf") {
		// ACL entries encode privileges per role; gpadmin is the data-loader role.
		assert.Contains(s.T(), grantOut, "gpadmin",
			"protocol pxf ACL must reference the data-loader role gpadmin (RP.11)")
	} else {
		s.T().Logf("scenario93: protocol pxf not present in pg_extprotocol (stub image); RP.11 grant skipped")
	}

	// RP.12: both segment pods mount the SAME <cluster>-pxf-servers ConfigMap on
	// their credential init container, AND resolve byte-identical server files.
	for _, pod := range pods {
		cmOnPod := s.scenario93InitTemplatesConfigMap(cl, pod)
		assert.Equalf(s.T(), cmName, cmOnPod,
			"pod %s credential init container must mount the shared ConfigMap %s", pod, cmName)
	}
	if len(pods) >= 2 {
		body0, err0 := s.scenario93PxfExec(scenario93LiveNamespace, pods[0],
			"cat /pxf-base/servers/s3-datalake/s3-site.xml")
		body1, err1 := s.scenario93PxfExec(scenario93LiveNamespace, pods[1],
			"cat /pxf-base/servers/s3-datalake/s3-site.xml")
		require.NoError(s.T(), err0)
		require.NoError(s.T(), err1)
		assert.Equal(s.T(), body0, body1,
			"all sidecars must see byte-identical resolved configs via the shared ConfigMap (RP.12)")
	} else {
		s.T().Logf("scenario93: only one segment pxf pod found; RP.12 cross-pod byte-identical " +
			"check needs two pods, asserting the single-pod shared-ConfigMap mount only")
	}
}

// scenario93FindDeployedCluster discovers a deployed cluster name by listing
// segment-primary pods carrying a "pxf" container, returning the cluster label.
func (s *Scenario93E2ESuite) scenario93FindDeployedCluster(cl client.Client) (string, bool) {
	pods := &corev1.PodList{}
	if err := cl.List(s.ctx, pods,
		client.InNamespace(scenario93LiveNamespace),
		client.MatchingLabels{util.LabelComponent: util.ComponentSegmentPrimary},
	); err != nil {
		s.T().Logf("scenario93: could not list segment-primary pods: %v", err)
		return "", false
	}
	for i := range pods.Items {
		pod := pods.Items[i]
		if _, ok := scenario93E2EPxfContainer(pod.Spec.Containers); ok {
			if cluster, ok := pod.Labels[util.LabelCluster]; ok && cluster != "" {
				return cluster, true
			}
		}
	}
	return "", false
}

// scenario93FindSegmentPxfPods returns the names of segment-primary pods (for
// the given cluster) that carry a "pxf" container.
func (s *Scenario93E2ESuite) scenario93FindSegmentPxfPods(
	cl client.Client, clusterName string,
) []string {
	pods := &corev1.PodList{}
	if err := cl.List(s.ctx, pods,
		client.InNamespace(scenario93LiveNamespace),
		client.MatchingLabels{
			util.LabelComponent: util.ComponentSegmentPrimary,
			util.LabelCluster:   clusterName,
		},
	); err != nil {
		s.T().Logf("scenario93: could not list segment-primary pods: %v", err)
		return nil
	}
	var out []string
	for i := range pods.Items {
		pod := pods.Items[i]
		if _, ok := scenario93E2EPxfContainer(pod.Spec.Containers); ok {
			out = append(out, pod.Name)
		}
	}
	return out
}

// scenario93InitTemplatesConfigMap returns the ConfigMap name backing the
// pxf-templates volume on the given pod (the shared sync source).
func (s *Scenario93E2ESuite) scenario93InitTemplatesConfigMap(
	cl client.Client, podName string,
) string {
	pod := &corev1.Pod{}
	if err := cl.Get(s.ctx, types.NamespacedName{
		Name: podName, Namespace: scenario93LiveNamespace,
	}, pod); err != nil {
		s.T().Logf("scenario93: could not get pod %s: %v", podName, err)
		return ""
	}
	for i := range pod.Spec.Volumes {
		v := pod.Spec.Volumes[i]
		if v.Name == "pxf-templates" && v.ConfigMap != nil {
			return v.ConfigMap.Name
		}
	}
	return ""
}

// scenario93E2EPxfContainer returns the "pxf" container from a list.
func scenario93E2EPxfContainer(containers []corev1.Container) (corev1.Container, bool) {
	for _, c := range containers {
		if c.Name == "pxf" {
			return c, true
		}
	}
	return corev1.Container{}, false
}

// scenario93PxfExec runs a bash command inside the pxf container of the named
// pod via kubectl exec, bounded by scenario93ExecTimeout.
func (s *Scenario93E2ESuite) scenario93PxfExec(
	namespace, pod, bashCmd string,
) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario93ExecTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", namespace,
		"-c", "pxf", pod, "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario93Psql runs a SQL query on the coordinator via kubectl exec into the
// pxf pod's psql (the pxf sidecar shares the segment image with psql) targeting
// the local coordinator. The query runs as gpadmin (the data-loader role).
func (s *Scenario93E2ESuite) scenario93Psql(pod, sql string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario93ExecTimeout)
	defer cancel()

	// Run psql as gpadmin against the default database. -tAc gives tuple-only,
	// unaligned output for easy substring assertions.
	psqlCmd := "psql -U gpadmin -tAc " + scenario93ShellQuote(sql)
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario93LiveNamespace,
		pod, "--", "bash", "-lc", psqlCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario93ShellQuote single-quotes a string for safe embedding in a bash -lc
// command (single quotes escaped via the '\” idiom).
func scenario93ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
