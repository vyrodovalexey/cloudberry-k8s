package idle

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// ============================================================================
// Test helpers
// ============================================================================

// mockDBClient implements db.Client for idle daemon testing.
// Only the methods used by the daemon are implemented with real logic;
// all others return zero values.
type mockDBClient struct {
	mu                              sync.Mutex
	pingFn                          func(ctx context.Context) error
	listSessionsWithResourceGroupFn func(ctx context.Context) ([]db.SessionWithGroup, error)
	terminateSessionFn              func(ctx context.Context, pid int32) (bool, error)
	terminatedPIDs                  []int32
}

func (m *mockDBClient) ListSessionsWithResourceGroup(ctx context.Context) ([]db.SessionWithGroup, error) {
	if m.listSessionsWithResourceGroupFn != nil {
		return m.listSessionsWithResourceGroupFn(ctx)
	}
	return []db.SessionWithGroup{}, nil
}

func (m *mockDBClient) TerminateSession(ctx context.Context, pid int32) (bool, error) {
	m.mu.Lock()
	m.terminatedPIDs = append(m.terminatedPIDs, pid)
	m.mu.Unlock()
	if m.terminateSessionFn != nil {
		return m.terminateSessionFn(ctx, pid)
	}
	return true, nil
}

func (m *mockDBClient) getTerminatedPIDs() []int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]int32, len(m.terminatedPIDs))
	copy(result, m.terminatedPIDs)
	return result
}

// Stub implementations for the rest of db.Client interface.
func (m *mockDBClient) Ping(ctx context.Context) error {
	if m.pingFn != nil {
		return m.pingFn(ctx)
	}
	return nil
}
func (m *mockDBClient) Close() {}
func (m *mockDBClient) GetSegmentConfiguration(_ context.Context) ([]db.SegmentInfo, error) {
	return nil, nil
}
func (m *mockDBClient) SetParameter(_ context.Context, _, _ string, _ db.ParameterScope) error {
	return nil
}
func (m *mockDBClient) ShowParameter(_ context.Context, _ string) (string, error) { return "", nil }
func (m *mockDBClient) ReloadConfig(_ context.Context) error                      { return nil }
func (m *mockDBClient) ListSessions(_ context.Context) ([]db.Session, error)      { return nil, nil }
func (m *mockDBClient) CancelQuery(_ context.Context, _ int32) (bool, error)      { return true, nil }
func (m *mockDBClient) CreateRole(_ context.Context, _ db.RoleOptions) error      { return nil }
func (m *mockDBClient) AlterRole(_ context.Context, _ db.RoleOptions) error       { return nil }
func (m *mockDBClient) DropRole(_ context.Context, _ string) error                { return nil }
func (m *mockDBClient) Vacuum(_ context.Context, _ db.VacuumOptions) error        { return nil }
func (m *mockDBClient) Analyze(_ context.Context, _ string) error                 { return nil }
func (m *mockDBClient) Reindex(_ context.Context, _ db.ReindexOptions) error      { return nil }
func (m *mockDBClient) GetDiskUsage(_ context.Context, _ string) ([]db.DiskUsage, error) {
	return nil, nil
}
func (m *mockDBClient) GetReplicationLag(_ context.Context) (int64, error) { return 0, nil }
func (m *mockDBClient) PromoteStandby(_ context.Context) error             { return nil }
func (m *mockDBClient) GetMaxConnections(_ context.Context) (int32, error) { return 100, nil }
func (m *mockDBClient) GetActiveQueryCount(_ context.Context) (int32, int32, int32, error) {
	return 0, 0, 0, nil
}
func (m *mockDBClient) GetResourceGroupUsage(_ context.Context, _ string) (float64, float64, error) {
	return 0, 0, nil
}
func (m *mockDBClient) CreateResourceGroup(_ context.Context, _ db.ResourceGroupOptions) error {
	return nil
}
func (m *mockDBClient) AlterResourceGroup(_ context.Context, _ db.ResourceGroupOptions) error {
	return nil
}
func (m *mockDBClient) DropResourceGroup(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) ListResourceGroups(_ context.Context) ([]db.ResourceGroupInfo, error) {
	return nil, nil
}
func (m *mockDBClient) AssignRoleResourceGroup(_ context.Context, _, _ string) error { return nil }
func (m *mockDBClient) CreateResourceQueue(_ context.Context, _ db.ResourceQueueOptions) error {
	return nil
}
func (m *mockDBClient) DropResourceQueue(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) ListResourceQueues(_ context.Context) ([]db.ResourceQueueInfo, error) {
	return nil, nil
}
func (m *mockDBClient) CreateBackup(_ context.Context, _ db.BackupOptions) (*db.BackupInfo, error) {
	return nil, nil
}
func (m *mockDBClient) RestoreBackup(_ context.Context, _ db.RestoreOptions) error { return nil }
func (m *mockDBClient) ListBackups(_ context.Context) ([]db.BackupInfo, error)     { return nil, nil }
func (m *mockDBClient) DeleteBackup(_ context.Context, _ string) error             { return nil }
func (m *mockDBClient) CreateDataLoadingJob(_ context.Context, _ db.DataLoadingJobConfig) error {
	return nil
}
func (m *mockDBClient) StartDataLoadingJob(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) StopDataLoadingJob(_ context.Context, _ string) error  { return nil }
func (m *mockDBClient) ListDataLoadingJobs(_ context.Context) ([]db.DataLoadingJobStatus, error) {
	return nil, nil
}
func (m *mockDBClient) GetStorageDiskUsage(_ context.Context) ([]db.DiskUsageInfo, error) {
	return nil, nil
}
func (m *mockDBClient) GetBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) GetSkewRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) GetAgeRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) GetIndexBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) TriggerRecommendationScan(_ context.Context) error { return nil }
func (m *mockDBClient) GetTableDetails(_ context.Context, _, _ string) (*db.TableDetail, error) {
	return nil, nil
}
func (m *mockDBClient) GetUsageReport(_ context.Context, _ string) ([]db.UsageReportEntry, error) {
	return nil, nil
}
func (m *mockDBClient) InitializeMirrors(_ context.Context, _ db.MirrorInitOptions) error {
	return nil
}
func (m *mockDBClient) ConfigureReplication(_ context.Context, _ db.ReplicationOptions) error {
	return nil
}
func (m *mockDBClient) GetMirrorSyncStatus(_ context.Context) ([]db.MirrorSyncInfo, error) {
	return nil, nil
}
func (m *mockDBClient) TriggerFTSProbe(_ context.Context) error               { return nil }
func (m *mockDBClient) TerminateAllBackends(_ context.Context) (int32, error) { return 0, nil }
func (m *mockDBClient) CancelAllQueries(_ context.Context) (int32, error)     { return 0, nil }
func (m *mockDBClient) LogRotate(_ context.Context) error                     { return nil }
func (m *mockDBClient) RegisterNewSegments(_ context.Context, _ db.SegmentRegistrationOptions) error {
	return nil
}
func (m *mockDBClient) RedistributeData(_ context.Context, _ db.RedistributionOptions) error {
	return nil
}
func (m *mockDBClient) GetRedistributionProgress(_ context.Context) (int32, error) { return 0, nil }
func (m *mockDBClient) DeregisterSegments(_ context.Context, _ int32) error        { return nil }
func (m *mockDBClient) RedistributeBeforeScaleIn(_ context.Context, _ db.ScaleInRedistributionOptions) error {
	return nil
}
func (m *mockDBClient) AnalyzeSkew(_ context.Context, _ string) ([]db.TableSkewInfo, error) {
	return nil, nil
}
func (m *mockDBClient) RebalanceTable(_ context.Context, _, _, _, _ string) error { return nil }
func (m *mockDBClient) ListUserDatabases(_ context.Context) ([]string, error)     { return nil, nil }
func (m *mockDBClient) SetupExporterRole(_ context.Context, _ string) error       { return nil }
func (m *mockDBClient) SetupPXFExtensions(_ context.Context) (int, error)         { return 2, nil }
func (m *mockDBClient) EnsureDataLoaderRole(_ context.Context, _ string) error    { return nil }
func (m *mockDBClient) ListPXFExtensions(_ context.Context) ([]string, error)     { return nil, nil }
func (m *mockDBClient) ListExternalTables(_ context.Context) ([]db.ExternalTableInfo, error) {
	return nil, nil
}
func (m *mockDBClient) ReadPXFSourceSample(
	_ context.Context, _, _, _ string, _ int,
) (*db.PXFSourceSample, error) {
	return nil, nil
}
func (m *mockDBClient) GetQueryDetail(_ context.Context, pid int32) (*db.QueryDetail, error) {
	return &db.QueryDetail{PID: pid, State: "active", Query: "SELECT 1"}, nil
}
func (m *mockDBClient) EnsureQueryHistoryTable(_ context.Context) error { return nil }
func (m *mockDBClient) InsertQueryHistory(_ context.Context, _ *db.QueryHistoryEntry) error {
	return nil
}
func (m *mockDBClient) GetQueryHistory(_ context.Context, _ db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
	return []db.QueryHistoryEntry{}, 0, nil
}
func (m *mockDBClient) GetQueryHistoryDetail(_ context.Context, _ string) (*db.QueryHistoryEntry, error) {
	return nil, fmt.Errorf("not found")
}
func (m *mockDBClient) ExportQueryHistoryCSV(_ context.Context, _ db.QueryHistoryFilter, _ io.Writer) error {
	return nil
}
func (m *mockDBClient) CleanupQueryHistory(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}
func (m *mockDBClient) MoveQueryToResourceGroup(_ context.Context, _ int32, _ string) error {
	return nil
}

// trackingMetricsRecorder wraps NoopRecorder and records idle termination calls.
type trackingMetricsRecorder struct {
	metrics.NoopRecorder
	mu               sync.Mutex
	idleTerminations []idleTerminationRecord
}

type idleTerminationRecord struct {
	Cluster   string
	Namespace string
	Rule      string
}

func (r *trackingMetricsRecorder) RecordIdleSessionTermination(cluster, namespace, rule string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.idleTerminations = append(r.idleTerminations, idleTerminationRecord{
		Cluster:   cluster,
		Namespace: namespace,
		Rule:      rule,
	})
}

func (r *trackingMetricsRecorder) getTerminations() []idleTerminationRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]idleTerminationRecord, len(r.idleTerminations))
	copy(result, r.idleTerminations)
	return result
}

// ============================================================================
// ParseIdleRules Tests
// ============================================================================

func TestParseIdleRules_ValidRules(t *testing.T) {
	crdRules := []cbv1alpha1.IdleSessionRule{
		{Name: "rule-30m", Enabled: true, ResourceGroup: "etl_group", IdleTimeout: "30m", ExcludeInTransaction: true, TerminateMessage: "idle too long"},
		{Name: "rule-10s", Enabled: true, ResourceGroup: "dev_group", IdleTimeout: "10s"},
		{Name: "rule-1h", Enabled: true, ResourceGroup: "batch_group", IdleTimeout: "1h"},
	}

	rules, err := ParseIdleRules(crdRules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(rules))
	}

	if rules[0].IdleTimeout != 30*time.Minute {
		t.Errorf("expected 30m, got %v", rules[0].IdleTimeout)
	}
	if rules[1].IdleTimeout != 10*time.Second {
		t.Errorf("expected 10s, got %v", rules[1].IdleTimeout)
	}
	if rules[2].IdleTimeout != time.Hour {
		t.Errorf("expected 1h, got %v", rules[2].IdleTimeout)
	}
	if rules[0].TerminateMessage != "idle too long" {
		t.Errorf("expected terminate message 'idle too long', got %q", rules[0].TerminateMessage)
	}
}

func TestParseIdleRules_InvalidDuration(t *testing.T) {
	crdRules := []cbv1alpha1.IdleSessionRule{
		{Name: "bad-rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: "invalid"},
	}

	_, err := ParseIdleRules(crdRules)
	if err == nil {
		t.Fatal("expected error for invalid duration, got nil")
	}
}

func TestParseIdleRules_EmptyRules(t *testing.T) {
	rules, err := ParseIdleRules(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 0 {
		t.Fatalf("expected 0 rules, got %d", len(rules))
	}
}

func TestParseIdleRules_DisabledRule(t *testing.T) {
	crdRules := []cbv1alpha1.IdleSessionRule{
		{Name: "disabled", Enabled: false, ResourceGroup: "grp", IdleTimeout: "5m"},
	}

	rules, err := ParseIdleRules(crdRules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rules[0].Enabled {
		t.Error("expected rule to be disabled")
	}
}

func TestParseIdleRules_AllFields(t *testing.T) {
	crdRules := []cbv1alpha1.IdleSessionRule{
		{
			Name:                 "full-rule",
			Enabled:              true,
			ResourceGroup:        "analytics",
			IdleTimeout:          "15m",
			ExcludeInTransaction: true,
			TerminateMessage:     "Session terminated due to inactivity",
		},
	}

	rules, err := ParseIdleRules(crdRules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r := rules[0]
	if r.Name != "full-rule" {
		t.Errorf("expected name 'full-rule', got %q", r.Name)
	}
	if !r.Enabled {
		t.Error("expected enabled=true")
	}
	if r.ResourceGroup != "analytics" {
		t.Errorf("expected resource group 'analytics', got %q", r.ResourceGroup)
	}
	if r.IdleTimeout != 15*time.Minute {
		t.Errorf("expected 15m, got %v", r.IdleTimeout)
	}
	if !r.ExcludeInTransaction {
		t.Error("expected excludeInTransaction=true")
	}
	if r.TerminateMessage != "Session terminated due to inactivity" {
		t.Errorf("unexpected terminate message: %q", r.TerminateMessage)
	}
}

// ============================================================================
// isSessionIdle Tests
// ============================================================================

func TestIsSessionIdle_IdleExceedsTimeout(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle",
			QueryStart: now.Add(-2 * time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute}

	if !isSessionIdle(session, rule, now) {
		t.Error("expected session to be idle")
	}
}

func TestIsSessionIdle_IdleBelowTimeout(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle",
			QueryStart: now.Add(-30 * time.Second),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute}

	if isSessionIdle(session, rule, now) {
		t.Error("expected session NOT to be idle (below timeout)")
	}
}

func TestIsSessionIdle_IdleExactTimeout(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle",
			QueryStart: now.Add(-time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute}

	// Exact timeout should NOT trigger (we use > not >=).
	if isSessionIdle(session, rule, now) {
		t.Error("expected session NOT to be idle at exact timeout boundary")
	}
}

func TestIsSessionIdle_ActiveNeverTerminated(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "active",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute}

	if isSessionIdle(session, rule, now) {
		t.Error("active sessions should never be terminated")
	}
}

func TestIsSessionIdle_InTransactionExcluded(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle in transaction",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute, ExcludeInTransaction: true}

	if isSessionIdle(session, rule, now) {
		t.Error("in-transaction session should be excluded when ExcludeInTransaction=true")
	}
}

func TestIsSessionIdle_InTransactionNotExcluded(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle in transaction",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute, ExcludeInTransaction: false}

	if !isSessionIdle(session, rule, now) {
		t.Error("in-transaction session should be terminated when ExcludeInTransaction=false")
	}
}

func TestIsSessionIdle_AbortedTransactionExcluded(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle in transaction (aborted)",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute, ExcludeInTransaction: true}

	if isSessionIdle(session, rule, now) {
		t.Error("aborted transaction session should be excluded when ExcludeInTransaction=true")
	}
}

func TestIsSessionIdle_AbortedTransactionNotExcluded(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle in transaction (aborted)",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute, ExcludeInTransaction: false}

	if !isSessionIdle(session, rule, now) {
		t.Error("aborted transaction session should be terminated when ExcludeInTransaction=false")
	}
}

func TestIsSessionIdle_EmptyState(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute}

	if isSessionIdle(session, rule, now) {
		t.Error("empty state should not be considered idle")
	}
}

func TestIsSessionIdle_UnknownState(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "fastpath function call",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute}

	if isSessionIdle(session, rule, now) {
		t.Error("unknown state should not be considered idle")
	}
}

func TestIsSessionIdle_ResourceGroupMismatch(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "other_group",
	}
	rule := IdleRule{Enabled: true, ResourceGroup: "target_group", IdleTimeout: time.Minute}

	if isSessionIdle(session, rule, now) {
		t.Error("session in different resource group should not match")
	}
}

func TestIsSessionIdle_EmptyRuleResourceGroup(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "any_group",
	}
	// Rule with empty resource group matches all sessions.
	rule := IdleRule{Enabled: true, ResourceGroup: "", IdleTimeout: time.Minute}

	if !isSessionIdle(session, rule, now) {
		t.Error("rule with empty resource group should match all sessions")
	}
}

func TestIsSessionIdle_DisabledRule(t *testing.T) {
	now := time.Now()
	session := db.SessionWithGroup{
		Session: db.Session{
			PID:        100,
			State:      "idle",
			QueryStart: now.Add(-10 * time.Minute),
		},
		ResourceGroup: "grp",
	}
	rule := IdleRule{Enabled: false, ResourceGroup: "grp", IdleTimeout: time.Minute}

	if isSessionIdle(session, rule, now) {
		t.Error("disabled rule should never match")
	}
}

// ============================================================================
// Daemon Lifecycle Tests
// ============================================================================

func TestDaemon_New_ValidConfig(t *testing.T) {
	d := New(Config{
		ClusterName:  "test-cluster",
		Namespace:    "default",
		ScanInterval: 5 * time.Second,
		DBClient:     &mockDBClient{},
		Metrics:      &metrics.NoopRecorder{},
		Logger:       slog.Default(),
	})

	if d == nil {
		t.Fatal("expected non-nil daemon")
	}
	if d.config.ScanInterval != 5*time.Second {
		t.Errorf("expected 5s scan interval, got %v", d.config.ScanInterval)
	}
}

func TestDaemon_New_DefaultScanInterval(t *testing.T) {
	d := New(Config{
		ClusterName: "test-cluster",
		DBClient:    &mockDBClient{},
		Metrics:     &metrics.NoopRecorder{},
	})

	if d.config.ScanInterval != DefaultScanInterval {
		t.Errorf("expected default scan interval %v, got %v", DefaultScanInterval, d.config.ScanInterval)
	}
}

func TestDaemon_StartStop(t *testing.T) {
	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: 10 * time.Millisecond,
		DBClient:     &mockDBClient{},
		Metrics:      &metrics.NoopRecorder{},
	})

	ctx := context.Background()
	d.Start(ctx)

	// Give the scan loop time to run at least once.
	time.Sleep(50 * time.Millisecond)

	// Stop should not hang.
	done := make(chan struct{})
	go func() {
		d.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success — daemon stopped cleanly.
	case <-time.After(2 * time.Second):
		t.Fatal("daemon Stop() timed out — possible goroutine leak")
	}
}

func TestDaemon_StartIdempotent(t *testing.T) {
	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: 10 * time.Millisecond,
		DBClient:     &mockDBClient{},
		Metrics:      &metrics.NoopRecorder{},
	})

	ctx := context.Background()
	d.Start(ctx)
	d.Start(ctx) // Second call should be a no-op.
	d.Stop()
}

func TestDaemon_StopWithoutStart(t *testing.T) {
	d := New(Config{
		ClusterName: "test-cluster",
		DBClient:    &mockDBClient{},
		Metrics:     &metrics.NoopRecorder{},
	})

	// Stop without Start should not panic.
	d.Stop()
}

func TestDaemon_UpdateRules(t *testing.T) {
	d := New(Config{
		ClusterName: "test-cluster",
		DBClient:    &mockDBClient{},
		Metrics:     &metrics.NoopRecorder{},
	})

	rules := []IdleRule{
		{Name: "rule1", Enabled: true, IdleTimeout: time.Minute},
		{Name: "rule2", Enabled: false, IdleTimeout: 5 * time.Minute},
	}

	d.UpdateRules(rules)

	got := d.Rules()
	if len(got) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(got))
	}
	if got[0].Name != "rule1" || got[1].Name != "rule2" {
		t.Errorf("unexpected rule names: %v", got)
	}
}

func TestDaemon_UpdateRulesWhileRunning(t *testing.T) {
	mock := &mockDBClient{}
	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: 10 * time.Millisecond,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})

	ctx := context.Background()
	d.Start(ctx)
	defer d.Stop()

	// Update rules while the daemon is running.
	for i := range 10 {
		d.UpdateRules([]IdleRule{
			{Name: fmt.Sprintf("rule-%d", i), Enabled: true, IdleTimeout: time.Minute},
		})
	}

	// Verify the last update took effect.
	rules := d.Rules()
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].Name != "rule-9" {
		t.Errorf("expected rule-9, got %q", rules[0].Name)
	}
}

// ============================================================================
// scanAndEnforce Tests
// ============================================================================

func TestScanAndEnforce_TerminatesIdleSession(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:        42,
						Username:   "testuser",
						State:      "idle",
						QueryStart: time.Now().Add(-5 * time.Minute),
					},
					ResourceGroup: "etl_group",
				},
			}, nil
		},
	}
	recorder := &trackingMetricsRecorder{}

	d := New(Config{
		ClusterName:  "test-cluster",
		Namespace:    "default",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      recorder,
	})

	d.UpdateRules([]IdleRule{
		{Name: "etl-idle", Enabled: true, ResourceGroup: "etl_group", IdleTimeout: time.Minute},
	})

	_, _, err := d.scanAndEnforce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pids := mock.getTerminatedPIDs()
	if len(pids) != 1 || pids[0] != 42 {
		t.Errorf("expected PID 42 terminated, got %v", pids)
	}

	terms := recorder.getTerminations()
	if len(terms) != 1 {
		t.Fatalf("expected 1 termination record, got %d", len(terms))
	}
	if terms[0].Rule != "etl-idle" {
		t.Errorf("expected rule 'etl-idle', got %q", terms[0].Rule)
	}
}

func TestScanAndEnforce_SkipsActiveSession(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:        42,
						State:      "active",
						QueryStart: time.Now().Add(-5 * time.Minute),
					},
					ResourceGroup: "grp",
				},
			}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.getTerminatedPIDs()) != 0 {
		t.Error("active session should not be terminated")
	}
}

func TestScanAndEnforce_ExcludesInTransaction(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:        42,
						State:      "idle in transaction",
						QueryStart: time.Now().Add(-5 * time.Minute),
					},
					ResourceGroup: "grp",
				},
			}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute, ExcludeInTransaction: true},
	})

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.getTerminatedPIDs()) != 0 {
		t.Error("in-transaction session should be excluded")
	}
}

func TestScanAndEnforce_TerminatesInTransactionWhenNotExcluded(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:        42,
						State:      "idle in transaction",
						QueryStart: time.Now().Add(-5 * time.Minute),
					},
					ResourceGroup: "grp",
				},
			}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute, ExcludeInTransaction: false},
	})

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pids := mock.getTerminatedPIDs()
	if len(pids) != 1 || pids[0] != 42 {
		t.Errorf("expected PID 42 terminated, got %v", pids)
	}
}

func TestScanAndEnforce_MatchesResourceGroup(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 1, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "target_group",
				},
				{
					Session:       db.Session{PID: 2, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "other_group",
				},
			}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "target_group", IdleTimeout: time.Minute},
	})

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pids := mock.getTerminatedPIDs()
	if len(pids) != 1 || pids[0] != 1 {
		t.Errorf("expected only PID 1 terminated, got %v", pids)
	}
}

func TestScanAndEnforce_SkipsDisabledRule(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 42, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "grp",
				},
			}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "disabled-rule", Enabled: false, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.getTerminatedPIDs()) != 0 {
		t.Error("disabled rule should not terminate any session")
	}
}

func TestScanAndEnforce_MultipleRulesMultipleSessions(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 1, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "group_a",
				},
				{
					Session:       db.Session{PID: 2, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "group_b",
				},
				{
					Session:       db.Session{PID: 3, State: "active", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "group_a",
				},
			}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule-a", Enabled: true, ResourceGroup: "group_a", IdleTimeout: time.Minute},
		{Name: "rule-b", Enabled: true, ResourceGroup: "group_b", IdleTimeout: time.Minute},
	})

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pids := mock.getTerminatedPIDs()
	if len(pids) != 2 {
		t.Fatalf("expected 2 terminations, got %d: %v", len(pids), pids)
	}
	// PID 1 (group_a, idle) and PID 2 (group_b, idle) should be terminated.
	// PID 3 (group_a, active) should NOT be terminated.
	pidSet := map[int32]bool{}
	for _, p := range pids {
		pidSet[p] = true
	}
	if !pidSet[1] || !pidSet[2] {
		t.Errorf("expected PIDs 1 and 2 terminated, got %v", pids)
	}
	if pidSet[3] {
		t.Error("PID 3 (active) should not be terminated")
	}
}

func TestScanAndEnforce_DBListError(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})

	_, _, err := d.scanAndEnforce(context.Background())
	if err == nil {
		t.Fatal("expected error from DB list failure")
	}
}

func TestScanAndEnforce_DBTerminateError(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 42, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "grp",
				},
			}, nil
		},
		terminateSessionFn: func(_ context.Context, _ int32) (bool, error) {
			return false, fmt.Errorf("terminate failed")
		},
	}

	recorder := &trackingMetricsRecorder{}
	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      recorder,
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})

	// Should not return error — terminate errors are logged but don't stop the scan.
	_, _, err := d.scanAndEnforce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No metric should be recorded for failed termination.
	if len(recorder.getTerminations()) != 0 {
		t.Error("no metric should be recorded for failed termination")
	}
}

func TestScanAndEnforce_NoRules(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			t.Error("ListSessionsWithResourceGroup should not be called when there are no rules")
			return nil, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	// No rules set.

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScanAndEnforce_NoSessions(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.getTerminatedPIDs()) != 0 {
		t.Error("no sessions should mean no terminations")
	}
}

func TestScanAndEnforce_RecordsMetric(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 42, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "grp",
				},
			}, nil
		},
	}
	recorder := &trackingMetricsRecorder{}

	d := New(Config{
		ClusterName:  "my-cluster",
		Namespace:    "prod",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      recorder,
	})
	d.UpdateRules([]IdleRule{
		{Name: "idle-rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	terms := recorder.getTerminations()
	if len(terms) != 1 {
		t.Fatalf("expected 1 termination, got %d", len(terms))
	}
	if terms[0].Cluster != "my-cluster" {
		t.Errorf("expected cluster 'my-cluster', got %q", terms[0].Cluster)
	}
	if terms[0].Namespace != "prod" {
		t.Errorf("expected namespace 'prod', got %q", terms[0].Namespace)
	}
	if terms[0].Rule != "idle-rule" {
		t.Errorf("expected rule 'idle-rule', got %q", terms[0].Rule)
	}
}

func TestScanAndEnforce_EmptyResourceGroupSession(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 42, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "", // No resource group assigned.
				},
			}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "specific_group", IdleTimeout: time.Minute},
	})

	if _, _, err := d.scanAndEnforce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.getTerminatedPIDs()) != 0 {
		t.Error("session with empty resource group should not match a rule targeting a specific group")
	}
}

// ============================================================================
// Integration-style Tests
// ============================================================================

func TestDaemon_ScanLoop_TerminatesAfterTimeout(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 99, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "grp",
				},
			}, nil
		},
	}
	recorder := &trackingMetricsRecorder{}

	d := New(Config{
		ClusterName:  "test-cluster",
		Namespace:    "default",
		ScanInterval: 20 * time.Millisecond,
		DBClient:     mock,
		Metrics:      recorder,
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})

	ctx := context.Background()
	d.Start(ctx)

	// Wait for at least one scan cycle.
	time.Sleep(100 * time.Millisecond)
	d.Stop()

	pids := mock.getTerminatedPIDs()
	if len(pids) == 0 {
		t.Error("expected at least one termination from the scan loop")
	}

	terms := recorder.getTerminations()
	if len(terms) == 0 {
		t.Error("expected at least one metric recorded")
	}
}

func TestDaemon_ScanLoop_ContextCancellation(t *testing.T) {
	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: 10 * time.Millisecond,
		DBClient:     &mockDBClient{},
		Metrics:      &metrics.NoopRecorder{},
	})

	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)

	// Cancel the parent context.
	cancel()

	// The daemon should stop cleanly.
	done := make(chan struct{})
	go func() {
		d.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success.
	case <-time.After(2 * time.Second):
		t.Fatal("daemon did not stop after context cancellation")
	}
}

// ============================================================================
// healthCheck Tests
// ============================================================================

func TestDaemon_HealthCheck_NilDBClient(t *testing.T) {
	d := New(Config{
		ClusterName: "test-cluster",
		Namespace:   "default",
		DBClient:    nil,
		Metrics:     &metrics.NoopRecorder{},
	})

	// Should not panic with nil DBClient.
	d.healthCheck(context.Background())
}

func TestDaemon_HealthCheck_PingSuccess(t *testing.T) {
	mock := &mockDBClient{}
	d := New(Config{
		ClusterName: "test-cluster",
		Namespace:   "default",
		DBClient:    mock,
		Metrics:     &metrics.NoopRecorder{},
	})

	// Ping succeeds — no reconnect should be attempted.
	d.healthCheck(context.Background())
}

func TestDaemon_HealthCheck_PingFailure_TriggersReconnect(t *testing.T) {
	pingFail := &mockDBClient{
		pingFn: func(_ context.Context) error {
			return fmt.Errorf("connection lost")
		},
	}

	reconnectCalled := false
	factory := &mockDBClientFactory{
		newClientFn: func(_ context.Context) (db.Client, error) {
			reconnectCalled = true
			return &mockDBClient{}, nil
		},
	}

	d := New(Config{
		ClusterName:     "test-cluster",
		Namespace:       "default",
		DBClient:        pingFail,
		DBClientFactory: factory,
		Metrics:         &metrics.NoopRecorder{},
	})

	d.healthCheck(context.Background())
	if !reconnectCalled {
		t.Error("expected reconnect to be called after ping failure")
	}
}

// ============================================================================
// attemptReconnect Tests
// ============================================================================

func TestDaemon_AttemptReconnect_NilFactory(t *testing.T) {
	d := New(Config{
		ClusterName:     "test-cluster",
		Namespace:       "default",
		DBClient:        &mockDBClient{},
		DBClientFactory: nil,
		Metrics:         &metrics.NoopRecorder{},
	})

	// Should not panic, just skip reconnect.
	d.attemptReconnect(context.Background())
}

func TestDaemon_AttemptReconnect_Success(t *testing.T) {
	oldClient := &mockDBClient{}
	newClient := &mockDBClient{}

	factory := &mockDBClientFactory{
		newClientFn: func(_ context.Context) (db.Client, error) {
			return newClient, nil
		},
	}

	d := New(Config{
		ClusterName:     "test-cluster",
		Namespace:       "default",
		DBClient:        oldClient,
		DBClientFactory: factory,
		Metrics:         &metrics.NoopRecorder{},
	})
	d.consecutiveFails = 5

	d.attemptReconnect(context.Background())

	// After successful reconnect, consecutiveFails should be reset.
	if d.consecutiveFails != 0 {
		t.Errorf("expected consecutiveFails=0 after reconnect, got %d", d.consecutiveFails)
	}
	// The new client should be set.
	if d.config.DBClient != newClient {
		t.Error("expected DBClient to be replaced with new client")
	}
}

func TestDaemon_AttemptReconnect_AllRetrysFail(t *testing.T) {
	factory := &mockDBClientFactory{
		newClientFn: func(_ context.Context) (db.Client, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	d := New(Config{
		ClusterName:     "test-cluster",
		Namespace:       "default",
		ScanInterval:    10 * time.Millisecond,
		DBClient:        &mockDBClient{},
		DBClientFactory: factory,
		Metrics:         &metrics.NoopRecorder{},
	})
	d.consecutiveFails = 5

	// Use a context with timeout to prevent the backoff from taking too long.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	d.attemptReconnect(ctx)

	// consecutiveFails should NOT be reset since all retries failed.
	if d.consecutiveFails != 5 {
		t.Errorf("expected consecutiveFails=5 after all retries fail, got %d", d.consecutiveFails)
	}
}

func TestDaemon_AttemptReconnect_ContextCanceled(t *testing.T) {
	factory := &mockDBClientFactory{
		newClientFn: func(_ context.Context) (db.Client, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	d := New(Config{
		ClusterName:     "test-cluster",
		Namespace:       "default",
		DBClient:        &mockDBClient{},
		DBClientFactory: factory,
		Metrics:         &metrics.NoopRecorder{},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	d.attemptReconnect(ctx)
	// Should return quickly without panic.
}

// ============================================================================
// scanAndEnforce with consecutive failures triggering reconnect
// ============================================================================

func TestDaemon_ScanLoop_ConsecutiveFailuresTriggersReconnect(t *testing.T) {
	failCount := 0
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			failCount++
			return nil, fmt.Errorf("connection lost")
		},
	}

	reconnectCalled := false
	factory := &mockDBClientFactory{
		newClientFn: func(_ context.Context) (db.Client, error) {
			reconnectCalled = true
			return &mockDBClient{}, nil
		},
	}

	d := New(Config{
		ClusterName:     "test-cluster",
		Namespace:       "default",
		ScanInterval:    10 * time.Millisecond,
		DBClient:        mock,
		DBClientFactory: factory,
		Metrics:         &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})

	ctx := context.Background()
	d.Start(ctx)

	// Wait for enough scan cycles to trigger reconnect (3 consecutive failures).
	time.Sleep(200 * time.Millisecond)
	d.Stop()

	if !reconnectCalled {
		t.Error("expected reconnect to be called after 3 consecutive failures")
	}
}

func TestDaemon_ScanLoop_SuccessResetsConsecutiveFails(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		Namespace:    "default",
		ScanInterval: 10 * time.Millisecond,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})
	d.consecutiveFails = 2

	ctx := context.Background()
	d.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	d.Stop()

	if d.consecutiveFails != 0 {
		t.Errorf("expected consecutiveFails=0 after successful scan, got %d", d.consecutiveFails)
	}
}

// ============================================================================
// terminateSession edge cases
// ============================================================================

func TestScanAndEnforce_TerminateReturnsFalse(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 42, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "grp",
				},
			}, nil
		},
		terminateSessionFn: func(_ context.Context, _ int32) (bool, error) {
			return false, nil // Session already ended.
		},
	}
	recorder := &trackingMetricsRecorder{}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      recorder,
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute},
	})

	_, _, err := d.scanAndEnforce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No metric should be recorded when terminate returns false.
	if len(recorder.getTerminations()) != 0 {
		t.Error("no metric should be recorded when terminate returns false")
	}
}

func TestScanAndEnforce_NilMetrics(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 42, State: "idle", QueryStart: time.Now().Add(-5 * time.Minute)},
					ResourceGroup: "grp",
				},
			}, nil
		},
	}

	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      nil, // nil metrics
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: time.Minute, TerminateMessage: "custom msg"},
	})

	// Should not panic with nil metrics.
	_, _, err := d.scanAndEnforce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDaemon_New_NegativeScanInterval(t *testing.T) {
	d := New(Config{
		ClusterName:  "test-cluster",
		ScanInterval: -1 * time.Second,
		DBClient:     &mockDBClient{},
		Metrics:      &metrics.NoopRecorder{},
	})

	if d.config.ScanInterval != DefaultScanInterval {
		t.Errorf("expected default scan interval for negative value, got %v", d.config.ScanInterval)
	}
}

// ============================================================================
// Additional mock helpers for idle daemon tests
// ============================================================================

// mockDBClientFactory implements idle.DBClientFactory for testing.
type mockDBClientFactory struct {
	newClientFn func(ctx context.Context) (db.Client, error)
}

func (f *mockDBClientFactory) NewClient(ctx context.Context) (db.Client, error) {
	if f.newClientFn != nil {
		return f.newClientFn(ctx)
	}
	return &mockDBClient{}, nil
}
