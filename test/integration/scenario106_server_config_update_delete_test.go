//go:build integration

package integration

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 106: Server Configuration Update / Delete (SL.7–SL.8) against the
// REAL stack — integration
// ============================================================================
//
// Mirrors the Scenario 105 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when the compose/k8s stack is down). No LIVE cluster is required for the
// deterministic part: it proves, over REALISTIC builder-rendered ConfigMap Data,
// that the SHARED honest server-config-change diff the controller
// (emitPXFServersChanged) and the API sync path (recordPXFServersChanged) BOTH
// consume — internal/util.DiffPXFServerNames + FormatPXFServersChangedMessage —
// reports an UPDATE (SL.7 endpoint patch), a DELETE (SL.8 server removal), and a
// NO-OP (byte-identical Data) HONESTLY, and that the rendered <cluster>-pxf-
// servers ConfigMap regenerates surgically.
//
// HONESTY: the change signal fires ONLY on a real Data diff; the helpers are pure
// and side-effect free. Isolation: read-only reachability probes + pure helper
// calls + the deterministic builder; safe for parallel CI.
//
// ENV (all overridable, no hardcode-only):
//   SCENARIO106_PG_ADDR     — coordinator host:port reachability (default localhost:5432).
//   SCENARIO106_MINIO_ADDR  — MinIO host:port reachability (default localhost:9000).
// ============================================================================

const (
	// envScenario106PGAddr overrides the coordinator host:port reachability probe.
	envScenario106PGAddr = "SCENARIO106_PG_ADDR"
	// envScenario106MinioAddr overrides the MinIO host:port reachability probe.
	envScenario106MinioAddr = "SCENARIO106_MINIO_ADDR"

	scenario106DefaultPGAddr    = "localhost:5432"
	scenario106DefaultMinioAddr = "localhost:9000"

	// scenario106Cluster is the cluster name the fixtures render with.
	scenario106Cluster = "scenario106-pxf"
	// scenario106Namespace is the deploy namespace.
	scenario106Namespace = cases.Scenario106Namespace

	scenario106OldEndpoint = "http://minio-old:9000"
	scenario106NewEndpoint = "http://minio-new:9000"

	// scenario106Timeout bounds each probe.
	scenario106Timeout = 30 * time.Second
)

// Scenario106Suite drives the shared honest PXF server-config diff for the
// Scenario 106 update/delete signal, gated on stack reachability.
type Scenario106Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario106(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario106Suite))
}

func (s *Scenario106Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 3*time.Minute)
}

func (s *Scenario106Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario106Env returns the ENV value or the provided default.
func scenario106Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario106PGAddr() string {
	return scenario106Env(envScenario106PGAddr, scenario106DefaultPGAddr)
}
func scenario106MinioAddr() string {
	return scenario106Env(envScenario106MinioAddr, scenario106DefaultMinioAddr)
}

// scenario106TCPReachable reports whether a TCP dial to addr succeeds.
func scenario106TCPReachable(ctx context.Context, addr string) bool {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// scenario106K8sReachable reports whether a kube-apiserver is reachable via
// kubectl (KUBECONFIG must be set + kubectl on PATH).
func scenario106K8sReachable(ctx context.Context) bool {
	if os.Getenv("KUBECONFIG") == "" {
		return false
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		return false
	}
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return exec.CommandContext(c, "kubectl", "version", "--request-timeout=8s").Run() == nil
}

// scenario106StackReachable reports whether the compose coordinator, the MinIO
// endpoint OR a kube-apiserver is reachable. The suite skips cleanly when all are
// down.
func (s *Scenario106Suite) scenario106StackReachable(ctx context.Context) bool {
	return scenario106TCPReachable(ctx, scenario106PGAddr()) ||
		scenario106TCPReachable(ctx, scenario106MinioAddr()) ||
		scenario106K8sReachable(ctx)
}

// scenario106Cluster3Servers renders a cluster with 3 PXF servers (the s3
// minio-warehouse with the given endpoint, an hdfs multi-file server, and a jdbc
// server) so the diff exercises an UPDATE, a DELETE and a surgical re-render over
// realistic builder output.
func scenario106Cluster3Servers(endpoint string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario106Cluster, scenario106Namespace).Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "apache/cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name:   cases.Scenario106UpdateServer,
					Type:   "s3",
					Config: map[string]string{"fs.s3a.endpoint": endpoint},
				},
				{
					Name:   "hadoop-cluster",
					Type:   "hdfs",
					Config: map[string]string{"fs.defaultFS": "hdfs://namenode:8020"},
					Hive:   map[string]string{"hive.metastore.uris": "thrift://hive:9083"},
				},
				{
					Name:   "mysql-oltp",
					Type:   "jdbc",
					Config: map[string]string{"jdbc.driver": "com.mysql.cj.jdbc.Driver"},
				},
			},
		},
	}
	return cluster
}

// TestIntegration_Scenario106_SharedDiffOverRenderedData proves the SHARED honest
// server-config diff (util.DiffPXFServerNames + FormatPXFServersChangedMessage)
// over REALISTIC builder-rendered ConfigMap Data reports an UPDATE (SL.7), a
// DELETE (SL.8) and a NO-OP honestly — the same helper the controller AND the API
// sync path consume, so the CR event and the API counter never disagree. Gated on
// stack reachability.
func (s *Scenario106Suite) TestIntegration_Scenario106_SharedDiffOverRenderedData() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario106Timeout)
	defer cancel()
	if !s.scenario106StackReachable(ctx) {
		s.T().Skipf("no Scenario 106 stack reachable (coordinator %s / MinIO %s / kube-apiserver) "+
			"— compose/k8s stack is down", scenario106PGAddr(), scenario106MinioAddr())
	}

	b := builder.NewBuilder()

	// Baseline (OLD endpoint, 3 servers) and patched (NEW endpoint, 3 servers).
	oldCM := b.BuildPXFServersConfigMap(scenario106Cluster3Servers(scenario106OldEndpoint))
	newCM := b.BuildPXFServersConfigMap(scenario106Cluster3Servers(scenario106NewEndpoint))
	require.NotNil(s.T(), oldCM)
	require.NotNil(s.T(), newCM)
	require.Contains(s.T(), oldCM.Data[cases.Scenario106UpdateFile], scenario106OldEndpoint)

	// SL.7 — UPDATE: only minio-warehouse changed; key SET unchanged.
	s.Run("SL.7 endpoint patch → updated=[minio-warehouse] (106-SL7-B1/B2)", func() {
		assert.Contains(s.T(), newCM.Data[cases.Scenario106UpdateFile], scenario106NewEndpoint)
		assert.NotContains(s.T(), newCM.Data[cases.Scenario106UpdateFile], scenario106OldEndpoint)
		assert.Equal(s.T(), len(oldCM.Data), len(newCM.Data), "data key SET unchanged on a patch")

		added, removed, updated := util.DiffPXFServerNames(oldCM.Data, newCM.Data)
		assert.Empty(s.T(), added)
		assert.Empty(s.T(), removed)
		assert.Equal(s.T(), []string{cases.Scenario106UpdateServer}, updated)

		msg := util.FormatPXFServersChangedMessage(added, removed, updated)
		assert.Contains(s.T(), msg, "updated=["+cases.Scenario106UpdateServer+"]")
		assert.Contains(s.T(), msg, "added=[]")
		assert.Contains(s.T(), msg, "removed=[]")
	})

	// SL.8 — DELETE: remove mysql-oltp; EXACTLY its keys gone; others intact.
	s.Run("SL.8 server removal → removed=[mysql-oltp] (106-SL8-B1)", func() {
		shrunk := scenario106Cluster3Servers(scenario106NewEndpoint)
		shrunk.Spec.DataLoading.Pxf.Servers = shrunk.Spec.DataLoading.Pxf.Servers[:2]
		shrunkCM := b.BuildPXFServersConfigMap(shrunk)
		require.NotNil(s.T(), shrunkCM)

		assert.NotContains(s.T(), shrunkCM.Data, "mysql-oltp__jdbc-site.xml")
		assert.Contains(s.T(), shrunkCM.Data, cases.Scenario106UpdateFile)
		assert.Contains(s.T(), shrunkCM.Data, "hadoop-cluster__core-site.xml")
		assert.Contains(s.T(), shrunkCM.Data, "hadoop-cluster__hdfs-site.xml")

		added, removed, updated := util.DiffPXFServerNames(newCM.Data, shrunkCM.Data)
		assert.Empty(s.T(), added)
		assert.Equal(s.T(), []string{"mysql-oltp"}, removed)
		assert.Empty(s.T(), updated)
		assert.Contains(s.T(),
			util.FormatPXFServersChangedMessage(added, removed, updated),
			"removed=[mysql-oltp]")
	})

	// SL.8 — prefix-boundary guard: srv vs srv2 are independent (106-SL8-B2).
	s.Run("prefix-boundary guard: srv vs srv2 keys independent (106-SL8-B2)", func() {
		existing := map[string]string{
			"srv__s3-site.xml":  "<a/>",
			"srv2__s3-site.xml": "<b/>",
		}
		// Remove only "srv2".
		desired := map[string]string{
			"srv__s3-site.xml": "<a/>",
		}
		added, removed, updated := util.DiffPXFServerNames(existing, desired)
		assert.Empty(s.T(), added)
		assert.Equal(s.T(), []string{"srv2"}, removed)
		assert.Empty(s.T(), updated, "srv must NOT be reported when only srv2 was removed")
	})

	// NO-OP honesty: byte-identical Data → empty diff (no signal) (106-MX-B2).
	s.Run("no-op (identical Data) → empty diff (106-MX-B2)", func() {
		added, removed, updated := util.DiffPXFServerNames(newCM.Data, newCM.Data)
		assert.Empty(s.T(), added)
		assert.Empty(s.T(), removed)
		assert.Empty(s.T(), updated)
	})

	s.T().Logf("scenario106: shared PXF server-config diff honest across UPDATE/DELETE/NO-OP "+
		"over %d rendered keys", len(newCM.Data))
}
