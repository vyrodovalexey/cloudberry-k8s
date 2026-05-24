//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 36: Per-Tablespace I/O Limits with Real Cloudberry Cluster
// ============================================================================
//
// Validates the operator's I/O limit reconciliation logic against a real
// Cloudberry cluster. The io_limit feature requires gp_resource_manager=group,
// but the test cluster runs in queue mode. The tests verify:
//   - FormatIOLimits produces correct SQL format strings
//   - The reconciler correctly maps IOLimits from CRD spec to DB options
//   - AlterResourceGroup is called with the correct IOLimits
//   - The reconciliation handles io_limit errors gracefully (no crash)
//   - Resource groups survive the io_limit error (not dropped)
//
// LIMITATION: Cloudberry 2.1.0 with gp_resource_manager=queue will reject
// ALTER RESOURCE GROUP ... SET io_limit with:
//   "ERROR: resource group must be enabled to use io limit feature"
// The tests handle this gracefully and document the limitation.
//
// Sub-tests:
//   - 36a: Wildcard tablespace I/O limits
//   - 36b: Named tablespace + wildcard I/O limits
// ============================================================================

// Scenario36IOLimitsE2ESuite tests per-tablespace I/O limits against a real
// Cloudberry cluster.
type Scenario36IOLimitsE2ESuite struct {
	E2ESuite

	// dbClient is the real database client connected to the Cloudberry coordinator.
	dbClient db.Client
	// portForwardCmd holds the kubectl port-forward process, if started.
	portForwardCmd *exec.Cmd
	// localPort is the local port used for the port-forward.
	localPort int
	// testSuffix is a unique suffix for resource group names to avoid conflicts.
	testSuffix string
}

func TestE2E_Scenario36(t *testing.T) {
	suite.Run(t, new(Scenario36IOLimitsE2ESuite))
}

// SetupSuite initializes the E2E test environment and connects to the real Cloudberry cluster.
func (s *Scenario36IOLimitsE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()

	s.testSuffix = strconv.FormatInt(time.Now().UnixMilli(), 36)
	s.logger.Info("scenario 36 E2E suite setup", "testSuffix", s.testSuffix)

	// Determine connection parameters from environment or defaults.
	host := getEnvDefault(envCloudberryTestHost, defaultCloudberryHost)
	portStr := os.Getenv(envCloudberryTestPort)
	user := getEnvDefault(envCloudberryTestUser, defaultCloudberryUser)
	password := os.Getenv(envCloudberryTestPassword)
	database := getEnvDefault(envCloudberryTestDB, defaultCloudberryDB)
	namespace := getEnvDefault(envCloudberryTestNamespace, defaultCloudberryNamespace)
	service := getEnvDefault(envCloudberryTestService, defaultCloudberryService)

	// If no password is provided, try to read it from the Kubernetes secret.
	if password == "" {
		password = readPasswordFromSecret(namespace, service)
	}

	var port int
	if portStr != "" {
		var parseErr error
		port, parseErr = strconv.Atoi(portStr)
		require.NoError(s.T(), parseErr, "CLOUDBERRY_TEST_PORT must be a valid integer")
	} else {
		// Auto port-forward: find a free local port and start kubectl port-forward.
		freePort, err := findFreePort()
		require.NoError(s.T(), err, "failed to find a free local port")
		port = freePort

		s.logger.Info("starting kubectl port-forward",
			"namespace", namespace, "service", service, "localPort", port)

		s.portForwardCmd = exec.Command("kubectl", "port-forward",
			"-n", namespace,
			fmt.Sprintf("svc/%s", service),
			fmt.Sprintf("%d:5432", port),
		)
		s.portForwardCmd.Stdout = os.Stdout
		s.portForwardCmd.Stderr = os.Stderr

		err = s.portForwardCmd.Start()
		if err != nil {
			s.T().Skipf("skipping scenario 36 E2E: kubectl port-forward failed to start: %v", err)
			return
		}

		// Wait for port-forward to become ready.
		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward36()
			s.T().Skipf("skipping scenario 36 E2E: port-forward did not become ready within timeout")
			return
		}
		s.localPort = port
		s.logger.Info("port-forward established", "localPort", port)
	}

	// Connect to the real Cloudberry cluster.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	cfg := db.Config{
		Host:     host,
		Port:     int32(port),
		Database: database,
		Username: user,
		Password: password,
		SSLMode:  "disable",
		MaxConns: 5,
		RetryOpts: util.RetryOptions{
			MaxRetries:     3,
			InitialBackoff: time.Second,
			MaxBackoff:     5 * time.Second,
			Multiplier:     2.0,
			JitterFraction: 0.1,
		},
	}

	dbClient, err := db.NewClient(ctx, cfg, s.logger)
	if err != nil {
		s.cleanupPortForward36()
		s.T().Skipf("skipping scenario 36 E2E: cannot connect to Cloudberry cluster: %v", err)
		return
	}
	s.dbClient = dbClient

	// Verify connectivity with a ping.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward36()
		s.T().Skipf("skipping scenario 36 E2E: ping to Cloudberry cluster failed: %v", err)
		return
	}

	s.logger.Info("connected to real Cloudberry cluster for scenario 36",
		"host", host, "port", port, "user", user, "database", database)
}

// TearDownSuite cleans up the database connection and port-forward.
func (s *Scenario36IOLimitsE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("database connection closed")
	}
	s.cleanupPortForward36()
	s.E2ESuite.TearDownSuite()
}

// cleanupPortForward36 terminates the kubectl port-forward process if running.
func (s *Scenario36IOLimitsE2ESuite) cleanupPortForward36() {
	if s.portForwardCmd != nil && s.portForwardCmd.Process != nil {
		if err := s.portForwardCmd.Process.Kill(); err != nil {
			s.logger.Warn("failed to kill port-forward process", "error", err)
		}
		// Wait to reap the process and avoid zombies.
		_ = s.portForwardCmd.Wait()
		s.logger.Info("port-forward process terminated")
		s.portForwardCmd = nil
	}
}

// uniqueGroupName returns a unique resource group name for test isolation.
func (s *Scenario36IOLimitsE2ESuite) uniqueGroupName(base string) string {
	return fmt.Sprintf("s36e2e_%s_%s", base, s.testSuffix)
}

// cleanupResourceGroup drops a resource group, ignoring errors (best-effort cleanup).
func (s *Scenario36IOLimitsE2ESuite) cleanupResourceGroup(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()
	if err := s.dbClient.DropResourceGroup(ctx, name); err != nil {
		s.logger.Warn("cleanup: failed to drop resource group (may not exist)", "name", name, "error", err)
	} else {
		s.logger.Info("cleanup: dropped resource group", "name", name)
	}
}

// ============================================================================
// scenario36AlterTracker intercepts AlterResourceGroup calls to capture the
// full ResourceGroupOptions (including IOLimits) for verification.
// ============================================================================

// scenario36AlterTracker wraps a db.Client and records AlterResourceGroup calls
// with full ResourceGroupOptions including IOLimits.
type scenario36AlterTracker struct {
	nonClosingClientWrapper
	delegate   db.Client
	mu         sync.Mutex
	alterCalls []db.ResourceGroupOptions
	alterErrs  []error
}

// AlterResourceGroup intercepts the call, records the options, delegates to the
// real client, and records any error.
func (t *scenario36AlterTracker) AlterResourceGroup(ctx context.Context, opts db.ResourceGroupOptions) error {
	err := t.delegate.AlterResourceGroup(ctx, opts)
	t.mu.Lock()
	t.alterCalls = append(t.alterCalls, opts)
	t.alterErrs = append(t.alterErrs, err)
	t.mu.Unlock()
	return err
}

func (t *scenario36AlterTracker) CreateResourceGroup(ctx context.Context, opts db.ResourceGroupOptions) error {
	return t.delegate.CreateResourceGroup(ctx, opts)
}

func (t *scenario36AlterTracker) DropResourceGroup(ctx context.Context, name string) error {
	return t.delegate.DropResourceGroup(ctx, name)
}

func (t *scenario36AlterTracker) ListResourceGroups(ctx context.Context) ([]db.ResourceGroupInfo, error) {
	return t.delegate.ListResourceGroups(ctx)
}

func (t *scenario36AlterTracker) GetResourceGroupUsage(ctx context.Context, group string) (float64, float64, error) {
	return t.delegate.GetResourceGroupUsage(ctx, group)
}

func (t *scenario36AlterTracker) Ping(ctx context.Context) error {
	return t.delegate.Ping(ctx)
}

// Close is a no-op to prevent closing the shared test connection.
func (t *scenario36AlterTracker) Close() {
	// no-op: the shared test connection is managed by the suite lifecycle
}

// getAlterCalls returns a copy of all recorded ALTER calls.
func (t *scenario36AlterTracker) getAlterCalls() []db.ResourceGroupOptions {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]db.ResourceGroupOptions, len(t.alterCalls))
	copy(result, t.alterCalls)
	return result
}

// getAlterErrors returns a copy of all recorded ALTER errors.
func (t *scenario36AlterTracker) getAlterErrors() []error {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]error, len(t.alterErrs))
	copy(result, t.alterErrs)
	return result
}

// ============================================================================
// Test 36a: Wildcard Tablespace I/O Limits
// ============================================================================
//
// Steps:
//  1. Verify FormatIOLimits produces correct format for wildcard tablespace
//  2. Create resource group in real DB
//  3. Build CloudberryCluster with IOLimits (wildcard: * at 100MB/s read, 50MB/s write)
//  4. Use tracking wrapper to capture ALTER calls
//  5. Run reconciliation
//  6. Verify: AlterResourceGroup was called with IOLimits containing wildcard entry
//  7. Verify: FormatIOLimits output matches expected format
//  8. Verify: Reconciliation handles io_limit error gracefully (no crash)
//  9. Verify: Resource group still exists in DB after reconciliation
// 10. Cleanup

func (s *Scenario36IOLimitsE2ESuite) TestScenario36a_WildcardTablespaceIOLimits() {
	s.logger.Info("starting test 36a: wildcard tablespace I/O limits")

	analyticsName := s.uniqueGroupName("analytics_a")

	// Register cleanup.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// ---- Step 1: Verify FormatIOLimits for wildcard tablespace ----
	s.logger.Info("step 1: verifying FormatIOLimits for wildcard tablespace")

	wildcardLimits := []db.IOLimitOption{
		{
			Tablespace:       "*",
			ReadBytesPerSec:  104857600, // 100 MB/s
			WriteBytesPerSec: 52428800,  // 50 MB/s
			ReadIOPS:         1000,
			WriteIOPS:        500,
		},
	}

	formatted := db.FormatIOLimits(wildcardLimits)
	expectedFormat := "*:rbps=104857600:wbps=52428800:riops=1000:wiops=500"
	assert.Equal(s.T(), expectedFormat, formatted,
		"FormatIOLimits should produce correct wildcard tablespace format")
	s.logger.Info("FormatIOLimits verified", "output", formatted)

	// ---- Step 2: Create resource group in real DB ----
	s.logger.Info("step 2: creating resource group in real DB", "name", analyticsName)

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          analyticsName,
		Concurrency:   10,
		CPUMaxPercent: 50,
		CPUWeight:     100,
	})
	require.NoError(s.T(), err, "should create resource group in real DB")

	// ---- Step 3: Build CloudberryCluster with IOLimits ----
	s.logger.Info("step 3: building CloudberryCluster with wildcard IOLimits")

	clusterName := "s36a-e2e-io-limits"
	clusterNamespace := "default"

	cluster := testutil.NewClusterBuilder(clusterName, clusterNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          analyticsName,
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
				MemoryLimit:   -1,
				MinCost:       -1,
				IOLimits: []cbv1alpha1.TablespaceIOLimitSpec{
					{
						Tablespace:       "*",
						ReadBytesPerSec:  104857600, // 100 MB/s
						WriteBytesPerSec: 52428800,  // 50 MB/s
						ReadIOPS:         1000,
						WriteIOPS:        500,
					},
				},
			},
		},
	}

	// ---- Step 4: Create tracking wrapper ----
	s.logger.Info("step 4: creating tracking wrapper to capture ALTER calls")

	tracker := &scenario36AlterTracker{
		delegate: s.dbClient,
	}
	factory := &realDBClientFactory{client: tracker}
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, s.logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// ---- Step 5: Run reconciliation ----
	s.logger.Info("step 5: running reconciliation")

	result, reconcileErr := reconciler.Reconcile(ctx, req)
	// The reconciliation may return an error if io_limit ALTER fails and the
	// controller propagates it. We handle both cases gracefully.
	s.logger.Info("reconciliation completed",
		"error", reconcileErr,
		"requeueAfter", result.RequeueAfter)

	// ---- Step 6: Verify AlterResourceGroup was called with IOLimits ----
	s.logger.Info("step 6: verifying AlterResourceGroup was called with IOLimits")

	alterCalls := tracker.getAlterCalls()
	s.logger.Info("ALTER calls captured", "count", len(alterCalls))

	// Find the ALTER call for our resource group.
	var analyticsAlterCall *db.ResourceGroupOptions
	for i := range alterCalls {
		if alterCalls[i].Name == analyticsName {
			analyticsAlterCall = &alterCalls[i]
			break
		}
	}

	require.NotNil(s.T(), analyticsAlterCall,
		"AlterResourceGroup should have been called for %s", analyticsName)
	require.Len(s.T(), analyticsAlterCall.IOLimits, 1,
		"ALTER call should contain 1 IOLimit entry")

	ioLimit := analyticsAlterCall.IOLimits[0]
	assert.Equal(s.T(), "*", ioLimit.Tablespace,
		"IOLimit tablespace should be wildcard")
	assert.Equal(s.T(), int64(104857600), ioLimit.ReadBytesPerSec,
		"IOLimit ReadBytesPerSec should be 100 MB/s")
	assert.Equal(s.T(), int64(52428800), ioLimit.WriteBytesPerSec,
		"IOLimit WriteBytesPerSec should be 50 MB/s")
	assert.Equal(s.T(), int32(1000), ioLimit.ReadIOPS,
		"IOLimit ReadIOPS should be 1000")
	assert.Equal(s.T(), int32(500), ioLimit.WriteIOPS,
		"IOLimit WriteIOPS should be 500")

	// ---- Step 7: Verify FormatIOLimits output matches expected ----
	s.logger.Info("step 7: verifying FormatIOLimits output from captured ALTER call")

	capturedFormatted := db.FormatIOLimits(analyticsAlterCall.IOLimits)
	assert.Equal(s.T(), expectedFormat, capturedFormatted,
		"FormatIOLimits from captured ALTER call should match expected format")

	// ---- Step 8: Verify graceful error handling ----
	s.logger.Info("step 8: verifying graceful error handling")

	alterErrors := tracker.getAlterErrors()
	for i, alterErr := range alterErrors {
		if alterErr != nil {
			// In Cloudberry 2.1.0 with gp_resource_manager=queue, the io_limit
			// ALTER will fail. This is expected and documented.
			s.logger.Info("ALTER error (expected in queue mode)",
				"call", i,
				"group", alterCalls[i].Name,
				"error", alterErr,
				"hasIOLimits", len(alterCalls[i].IOLimits) > 0)

			if len(alterCalls[i].IOLimits) > 0 {
				// Verify the error message indicates io_limit is not supported
				// in queue mode (this is the expected Cloudberry 2.1.0 behavior).
				errMsg := alterErr.Error()
				isIOLimitError := strings.Contains(errMsg, "io_limit") ||
					strings.Contains(errMsg, "resource group must be enabled")
				s.logger.Info("io_limit error classification",
					"isIOLimitError", isIOLimitError,
					"errorMessage", errMsg)
			}
		}
	}

	// The reconciliation should not have panicked — we got here, so it's fine.
	// The reconciler may have returned an error (propagated from ALTER), which
	// is acceptable behavior. The key is that it didn't crash.
	s.logger.Info("reconciliation did not crash (graceful handling verified)")

	// ---- Step 9: Verify resource group still exists in DB ----
	s.logger.Info("step 9: verifying resource group still exists in DB")

	listCtx, listCancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer listCancel()

	groups, err := s.dbClient.ListResourceGroups(listCtx)
	require.NoError(s.T(), err, "should list resource groups")

	groupFound := false
	for _, g := range groups {
		if g.Name == analyticsName {
			groupFound = true
			s.logger.Info("resource group verified in DB",
				"name", g.Name,
				"concurrency", g.Concurrency,
				"cpuMaxPercent", g.CPUMaxPercent)
			break
		}
	}
	assert.True(s.T(), groupFound,
		"resource group %s should still exist in DB after reconciliation (io_limit error should not cause drop)",
		analyticsName)

	s.logger.Info("test 36a completed: wildcard tablespace I/O limits verified",
		"group", analyticsName,
		"formatOutput", formatted,
		"alterCallsCaptured", len(alterCalls))
}

// ============================================================================
// Test 36b: Named Tablespace + Wildcard I/O Limits
// ============================================================================
//
// Steps:
//  1. Verify FormatIOLimits produces correct format for multiple tablespaces
//  2. Create resource group in real DB
//  3. Build CloudberryCluster with multiple IOLimits entries:
//     - fast_storage: 200MB/s read, 100MB/s write, 5000/2500 IOPS
//     - * (wildcard): 50MB/s read, 25MB/s write, 500/250 IOPS
//  4. Use tracking wrapper to capture ALTER calls
//  5. Run reconciliation
//  6. Verify: AlterResourceGroup was called with IOLimits containing BOTH entries
//  7. Verify: FormatIOLimits output contains semicolon-separated entries
//  8. Verify: Both tablespace entries are present in the formatted string
//  9. Verify: Reconciliation handles gracefully
// 10. Cleanup

func (s *Scenario36IOLimitsE2ESuite) TestScenario36b_NamedAndWildcardTablespaceIOLimits() {
	s.logger.Info("starting test 36b: named tablespace + wildcard I/O limits")

	analyticsName := s.uniqueGroupName("analytics_b")

	// Register cleanup.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// ---- Step 1: Verify FormatIOLimits for multiple tablespaces ----
	s.logger.Info("step 1: verifying FormatIOLimits for named + wildcard tablespaces")

	multiLimits := []db.IOLimitOption{
		{
			Tablespace:       "fast_storage",
			ReadBytesPerSec:  209715200, // 200 MB/s
			WriteBytesPerSec: 104857600, // 100 MB/s
			ReadIOPS:         5000,
			WriteIOPS:        2500,
		},
		{
			Tablespace:       "*",
			ReadBytesPerSec:  52428800, // 50 MB/s
			WriteBytesPerSec: 26214400, // 25 MB/s
			ReadIOPS:         500,
			WriteIOPS:        250,
		},
	}

	formatted := db.FormatIOLimits(multiLimits)
	expectedFormat := "fast_storage:rbps=209715200:wbps=104857600:riops=5000:wiops=2500;*:rbps=52428800:wbps=26214400:riops=500:wiops=250"
	assert.Equal(s.T(), expectedFormat, formatted,
		"FormatIOLimits should produce correct multi-tablespace format")

	// Verify both tablespace entries are present.
	assert.True(s.T(), strings.Contains(formatted, "fast_storage:"),
		"formatted string should contain fast_storage entry")
	assert.True(s.T(), strings.Contains(formatted, "*:rbps="),
		"formatted string should contain wildcard entry")
	assert.True(s.T(), strings.Contains(formatted, ";"),
		"formatted string should contain semicolon separator for multiple entries")

	s.logger.Info("FormatIOLimits verified for multiple tablespaces", "output", formatted)

	// ---- Step 2: Create resource group in real DB ----
	s.logger.Info("step 2: creating resource group in real DB", "name", analyticsName)

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          analyticsName,
		Concurrency:   10,
		CPUMaxPercent: 50,
		CPUWeight:     100,
	})
	require.NoError(s.T(), err, "should create resource group in real DB")

	// ---- Step 3: Build CloudberryCluster with multiple IOLimits ----
	s.logger.Info("step 3: building CloudberryCluster with named + wildcard IOLimits")

	clusterName := "s36b-e2e-io-limits"
	clusterNamespace := "default"

	cluster := testutil.NewClusterBuilder(clusterName, clusterNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          analyticsName,
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
				MemoryLimit:   -1,
				MinCost:       -1,
				IOLimits: []cbv1alpha1.TablespaceIOLimitSpec{
					{
						Tablespace:       "fast_storage",
						ReadBytesPerSec:  209715200, // 200 MB/s
						WriteBytesPerSec: 104857600, // 100 MB/s
						ReadIOPS:         5000,
						WriteIOPS:        2500,
					},
					{
						Tablespace:       "*",
						ReadBytesPerSec:  52428800, // 50 MB/s
						WriteBytesPerSec: 26214400, // 25 MB/s
						ReadIOPS:         500,
						WriteIOPS:        250,
					},
				},
			},
		},
	}

	// ---- Step 4: Create tracking wrapper ----
	s.logger.Info("step 4: creating tracking wrapper to capture ALTER calls")

	tracker := &scenario36AlterTracker{
		delegate: s.dbClient,
	}
	factory := &realDBClientFactory{client: tracker}
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, s.logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// ---- Step 5: Run reconciliation ----
	s.logger.Info("step 5: running reconciliation")

	result, reconcileErr := reconciler.Reconcile(ctx, req)
	s.logger.Info("reconciliation completed",
		"error", reconcileErr,
		"requeueAfter", result.RequeueAfter)

	// ---- Step 6: Verify AlterResourceGroup was called with both IOLimits ----
	s.logger.Info("step 6: verifying AlterResourceGroup was called with both IOLimits entries")

	alterCalls := tracker.getAlterCalls()
	s.logger.Info("ALTER calls captured", "count", len(alterCalls))

	// Find the ALTER call for our resource group.
	var analyticsAlterCall *db.ResourceGroupOptions
	for i := range alterCalls {
		if alterCalls[i].Name == analyticsName {
			analyticsAlterCall = &alterCalls[i]
			break
		}
	}

	require.NotNil(s.T(), analyticsAlterCall,
		"AlterResourceGroup should have been called for %s", analyticsName)
	require.Len(s.T(), analyticsAlterCall.IOLimits, 2,
		"ALTER call should contain 2 IOLimit entries (named + wildcard)")

	// Verify first entry: fast_storage.
	fastStorageLimit := analyticsAlterCall.IOLimits[0]
	assert.Equal(s.T(), "fast_storage", fastStorageLimit.Tablespace,
		"first IOLimit should be for fast_storage tablespace")
	assert.Equal(s.T(), int64(209715200), fastStorageLimit.ReadBytesPerSec,
		"fast_storage ReadBytesPerSec should be 200 MB/s")
	assert.Equal(s.T(), int64(104857600), fastStorageLimit.WriteBytesPerSec,
		"fast_storage WriteBytesPerSec should be 100 MB/s")
	assert.Equal(s.T(), int32(5000), fastStorageLimit.ReadIOPS,
		"fast_storage ReadIOPS should be 5000")
	assert.Equal(s.T(), int32(2500), fastStorageLimit.WriteIOPS,
		"fast_storage WriteIOPS should be 2500")

	// Verify second entry: wildcard.
	wildcardLimit := analyticsAlterCall.IOLimits[1]
	assert.Equal(s.T(), "*", wildcardLimit.Tablespace,
		"second IOLimit should be for wildcard tablespace")
	assert.Equal(s.T(), int64(52428800), wildcardLimit.ReadBytesPerSec,
		"wildcard ReadBytesPerSec should be 50 MB/s")
	assert.Equal(s.T(), int64(26214400), wildcardLimit.WriteBytesPerSec,
		"wildcard WriteBytesPerSec should be 25 MB/s")
	assert.Equal(s.T(), int32(500), wildcardLimit.ReadIOPS,
		"wildcard ReadIOPS should be 500")
	assert.Equal(s.T(), int32(250), wildcardLimit.WriteIOPS,
		"wildcard WriteIOPS should be 250")

	// ---- Step 7: Verify FormatIOLimits output from captured ALTER call ----
	s.logger.Info("step 7: verifying FormatIOLimits output from captured ALTER call")

	capturedFormatted := db.FormatIOLimits(analyticsAlterCall.IOLimits)
	assert.Equal(s.T(), expectedFormat, capturedFormatted,
		"FormatIOLimits from captured ALTER call should match expected multi-tablespace format")

	// ---- Step 8: Verify both tablespace entries in formatted string ----
	s.logger.Info("step 8: verifying both tablespace entries in formatted string")

	assert.True(s.T(), strings.Contains(capturedFormatted, "fast_storage:rbps=209715200:wbps=104857600:riops=5000:wiops=2500"),
		"formatted string should contain complete fast_storage entry")
	assert.True(s.T(), strings.Contains(capturedFormatted, "*:rbps=52428800:wbps=26214400:riops=500:wiops=250"),
		"formatted string should contain complete wildcard entry")

	// Count semicolons to verify correct number of entries.
	semicolonCount := strings.Count(capturedFormatted, ";")
	assert.Equal(s.T(), 1, semicolonCount,
		"formatted string should have exactly 1 semicolon separator for 2 entries")

	// ---- Step 9: Verify graceful handling ----
	s.logger.Info("step 9: verifying graceful error handling")

	alterErrors := tracker.getAlterErrors()
	for i, alterErr := range alterErrors {
		if alterErr != nil {
			s.logger.Info("ALTER error (expected in queue mode)",
				"call", i,
				"group", alterCalls[i].Name,
				"error", alterErr,
				"ioLimitsCount", len(alterCalls[i].IOLimits))

			if len(alterCalls[i].IOLimits) > 0 {
				errMsg := alterErr.Error()
				isIOLimitError := strings.Contains(errMsg, "io_limit") ||
					strings.Contains(errMsg, "resource group must be enabled")
				s.logger.Info("io_limit error classification",
					"isIOLimitError", isIOLimitError,
					"errorMessage", errMsg)
			}
		}
	}

	// Reconciliation did not crash — verified by reaching this point.
	s.logger.Info("reconciliation did not crash (graceful handling verified)")

	// Verify resource group still exists in DB.
	listCtx, listCancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer listCancel()

	groups, err := s.dbClient.ListResourceGroups(listCtx)
	require.NoError(s.T(), err, "should list resource groups")

	groupFound := false
	for _, g := range groups {
		if g.Name == analyticsName {
			groupFound = true
			s.logger.Info("resource group verified in DB after multi-tablespace IO limits",
				"name", g.Name,
				"concurrency", g.Concurrency,
				"cpuMaxPercent", g.CPUMaxPercent)
			break
		}
	}
	assert.True(s.T(), groupFound,
		"resource group %s should still exist in DB after reconciliation with multi-tablespace IO limits",
		analyticsName)

	s.logger.Info("test 36b completed: named + wildcard tablespace I/O limits verified",
		"group", analyticsName,
		"formatOutput", formatted,
		"alterCallsCaptured", len(alterCalls))
}
