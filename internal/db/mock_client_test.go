package db

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// fullMockDBClient is a configurable mock that allows per-method error injection.
type fullMockDBClient struct {
	// Error fields for each method.
	pingErr                error
	getSegErr              error
	setParamErr            error
	showParamErr           error
	reloadErr              error
	listSessionsErr        error
	cancelQueryErr         error
	terminateSessionErr    error
	createRoleErr          error
	alterRoleErr           error
	dropRoleErr            error
	vacuumErr              error
	analyzeErr             error
	reindexErr             error
	getDiskUsageErr        error
	getRepLagErr           error
	promoteErr             error
	maxConnections         int32
	maxConnectionsErr      error
	getActiveQueryErr      error
	getResGroupUsageErr    error
	createResGroupErr      error
	alterResGroupErr       error
	dropResGroupErr        error
	listResGroupsErr       error
	assignRoleResGroupErr  error
	createResQueueErr      error
	dropResQueueErr        error
	listResQueuesErr       error
	createBackupErr        error
	restoreBackupErr       error
	listBackupsErr         error
	deleteBackupErr        error
	createDataLoadErr      error
	startDataLoadErr       error
	stopDataLoadErr        error
	listDataLoadErr        error
	getStorageDiskErr      error
	getBloatErr            error
	getSkewErr             error
	getAgeErr              error
	getIndexBloatErr       error
	triggerRecScanErr      error
	getTableDetailsErr     error
	getUsageReportErr      error
	initMirrorsErr         error
	configRepErr           error
	getMirrorSyncErr       error
	triggerFTSErr          error
	terminateAllErr        error
	cancelAllErr           error
	logRotateErr           error
	registerSegErr         error
	redistributeErr        error
	getRedistProgressErr   error
	deregisterErr          error
	redistBeforeScaleInErr error
	analyzeSkewErr         error
	rebalanceTableErr      error
	listUserDBsErr         error

	// Return values.
	segments           []SegmentInfo
	showParamVal       string
	sessions           []Session
	cancelResult       bool
	terminateResult    bool
	diskUsages         []DiskUsage
	repLag             int64
	activeQueries      int32
	queuedQueries      int32
	blockedQueries     int32
	cpuUsage           float64
	memUsage           float64
	resGroups          []ResourceGroupInfo
	resQueues          []ResourceQueueInfo
	backupInfo         *BackupInfo
	backups            []BackupInfo
	dataLoadJobs       []DataLoadingJobStatus
	storageDiskUsages  []DiskUsageInfo
	bloatRecs          []Recommendation
	skewRecs           []Recommendation
	ageRecs            []Recommendation
	indexBloatRecs     []Recommendation
	tableDetail        *TableDetail
	usageReport        []UsageReportEntry
	mirrorSyncInfos    []MirrorSyncInfo
	terminatedBackends int32
	canceledQueries    int32
	redistProgress     int32
	skewInfos          []TableSkewInfo
	userDatabases      []string

	closeCalls int
}

func (m *fullMockDBClient) Ping(_ context.Context) error { return m.pingErr }
func (m *fullMockDBClient) Close()                       { m.closeCalls++ }
func (m *fullMockDBClient) GetSegmentConfiguration(_ context.Context) ([]SegmentInfo, error) {
	return m.segments, m.getSegErr
}
func (m *fullMockDBClient) SetParameter(_ context.Context, _, _ string, _ ParameterScope) error {
	return m.setParamErr
}
func (m *fullMockDBClient) ShowParameter(_ context.Context, _ string) (string, error) {
	return m.showParamVal, m.showParamErr
}
func (m *fullMockDBClient) ReloadConfig(_ context.Context) error { return m.reloadErr }
func (m *fullMockDBClient) ListSessions(_ context.Context) ([]Session, error) {
	return m.sessions, m.listSessionsErr
}
func (m *fullMockDBClient) CancelQuery(_ context.Context, _ int32) (bool, error) {
	return m.cancelResult, m.cancelQueryErr
}
func (m *fullMockDBClient) TerminateSession(_ context.Context, _ int32) (bool, error) {
	return m.terminateResult, m.terminateSessionErr
}
func (m *fullMockDBClient) CreateRole(_ context.Context, _ RoleOptions) error {
	return m.createRoleErr
}
func (m *fullMockDBClient) AlterRole(_ context.Context, _ RoleOptions) error {
	return m.alterRoleErr
}
func (m *fullMockDBClient) DropRole(_ context.Context, _ string) error { return m.dropRoleErr }
func (m *fullMockDBClient) Vacuum(_ context.Context, _ VacuumOptions) error {
	return m.vacuumErr
}
func (m *fullMockDBClient) Analyze(_ context.Context, _ string) error { return m.analyzeErr }
func (m *fullMockDBClient) Reindex(_ context.Context, _ ReindexOptions) error {
	return m.reindexErr
}
func (m *fullMockDBClient) GetDiskUsage(_ context.Context, _ string) ([]DiskUsage, error) {
	return m.diskUsages, m.getDiskUsageErr
}
func (m *fullMockDBClient) GetReplicationLag(_ context.Context) (int64, error) {
	return m.repLag, m.getRepLagErr
}
func (m *fullMockDBClient) PromoteStandby(_ context.Context) error { return m.promoteErr }
func (m *fullMockDBClient) GetMaxConnections(_ context.Context) (int32, error) {
	return m.maxConnections, m.maxConnectionsErr
}
func (m *fullMockDBClient) GetActiveQueryCount(_ context.Context) (int32, int32, int32, error) {
	return m.activeQueries, m.queuedQueries, m.blockedQueries, m.getActiveQueryErr
}
func (m *fullMockDBClient) GetResourceGroupUsage(_ context.Context, _ string) (float64, float64, error) {
	return m.cpuUsage, m.memUsage, m.getResGroupUsageErr
}
func (m *fullMockDBClient) CreateResourceGroup(_ context.Context, _ ResourceGroupOptions) error {
	return m.createResGroupErr
}
func (m *fullMockDBClient) AlterResourceGroup(_ context.Context, _ ResourceGroupOptions) error {
	return m.alterResGroupErr
}
func (m *fullMockDBClient) DropResourceGroup(_ context.Context, _ string) error {
	return m.dropResGroupErr
}
func (m *fullMockDBClient) ListResourceGroups(_ context.Context) ([]ResourceGroupInfo, error) {
	return m.resGroups, m.listResGroupsErr
}
func (m *fullMockDBClient) AssignRoleResourceGroup(_ context.Context, _, _ string) error {
	return m.assignRoleResGroupErr
}
func (m *fullMockDBClient) CreateResourceQueue(_ context.Context, _ ResourceQueueOptions) error {
	return m.createResQueueErr
}
func (m *fullMockDBClient) DropResourceQueue(_ context.Context, _ string) error {
	return m.dropResQueueErr
}
func (m *fullMockDBClient) ListResourceQueues(_ context.Context) ([]ResourceQueueInfo, error) {
	return m.resQueues, m.listResQueuesErr
}
func (m *fullMockDBClient) CreateBackup(_ context.Context, opts BackupOptions) (*BackupInfo, error) {
	if m.backupInfo != nil {
		return m.backupInfo, m.createBackupErr
	}
	return &BackupInfo{ID: "test-backup", Type: opts.Type, Status: "InProgress"}, m.createBackupErr
}
func (m *fullMockDBClient) RestoreBackup(_ context.Context, _ RestoreOptions) error {
	return m.restoreBackupErr
}
func (m *fullMockDBClient) ListBackups(_ context.Context) ([]BackupInfo, error) {
	return m.backups, m.listBackupsErr
}
func (m *fullMockDBClient) DeleteBackup(_ context.Context, _ string) error {
	return m.deleteBackupErr
}
func (m *fullMockDBClient) CreateDataLoadingJob(_ context.Context, _ DataLoadingJobConfig) error {
	return m.createDataLoadErr
}
func (m *fullMockDBClient) StartDataLoadingJob(_ context.Context, _ string) error {
	return m.startDataLoadErr
}
func (m *fullMockDBClient) StopDataLoadingJob(_ context.Context, _ string) error {
	return m.stopDataLoadErr
}
func (m *fullMockDBClient) ListDataLoadingJobs(_ context.Context) ([]DataLoadingJobStatus, error) {
	return m.dataLoadJobs, m.listDataLoadErr
}
func (m *fullMockDBClient) GetStorageDiskUsage(_ context.Context) ([]DiskUsageInfo, error) {
	return m.storageDiskUsages, m.getStorageDiskErr
}
func (m *fullMockDBClient) GetBloatRecommendations(_ context.Context) ([]Recommendation, error) {
	return m.bloatRecs, m.getBloatErr
}
func (m *fullMockDBClient) GetSkewRecommendations(_ context.Context) ([]Recommendation, error) {
	return m.skewRecs, m.getSkewErr
}
func (m *fullMockDBClient) GetAgeRecommendations(_ context.Context) ([]Recommendation, error) {
	return m.ageRecs, m.getAgeErr
}
func (m *fullMockDBClient) GetIndexBloatRecommendations(_ context.Context) ([]Recommendation, error) {
	return m.indexBloatRecs, m.getIndexBloatErr
}
func (m *fullMockDBClient) TriggerRecommendationScan(_ context.Context) error {
	return m.triggerRecScanErr
}
func (m *fullMockDBClient) GetTableDetails(_ context.Context, s, t string) (*TableDetail, error) {
	if m.tableDetail != nil {
		return m.tableDetail, m.getTableDetailsErr
	}
	return &TableDetail{Schema: s, Table: t}, m.getTableDetailsErr
}
func (m *fullMockDBClient) GetUsageReport(_ context.Context, _ string) ([]UsageReportEntry, error) {
	return m.usageReport, m.getUsageReportErr
}
func (m *fullMockDBClient) InitializeMirrors(_ context.Context, _ MirrorInitOptions) error {
	return m.initMirrorsErr
}
func (m *fullMockDBClient) ConfigureReplication(_ context.Context, _ ReplicationOptions) error {
	return m.configRepErr
}
func (m *fullMockDBClient) GetMirrorSyncStatus(_ context.Context) ([]MirrorSyncInfo, error) {
	return m.mirrorSyncInfos, m.getMirrorSyncErr
}
func (m *fullMockDBClient) TriggerFTSProbe(_ context.Context) error { return m.triggerFTSErr }
func (m *fullMockDBClient) TerminateAllBackends(_ context.Context) (int32, error) {
	return m.terminatedBackends, m.terminateAllErr
}
func (m *fullMockDBClient) CancelAllQueries(_ context.Context) (int32, error) {
	return m.canceledQueries, m.cancelAllErr
}
func (m *fullMockDBClient) LogRotate(_ context.Context) error { return m.logRotateErr }
func (m *fullMockDBClient) RegisterNewSegments(_ context.Context, _ SegmentRegistrationOptions) error {
	return m.registerSegErr
}
func (m *fullMockDBClient) RedistributeData(_ context.Context, _ RedistributionOptions) error {
	return m.redistributeErr
}
func (m *fullMockDBClient) GetRedistributionProgress(_ context.Context) (int32, error) {
	return m.redistProgress, m.getRedistProgressErr
}
func (m *fullMockDBClient) DeregisterSegments(_ context.Context, _ int32) error {
	return m.deregisterErr
}
func (m *fullMockDBClient) RedistributeBeforeScaleIn(_ context.Context, _ ScaleInRedistributionOptions) error {
	return m.redistBeforeScaleInErr
}
func (m *fullMockDBClient) AnalyzeSkew(_ context.Context, _ string) ([]TableSkewInfo, error) {
	return m.skewInfos, m.analyzeSkewErr
}
func (m *fullMockDBClient) RebalanceTable(_ context.Context, _, _, _, _ string) error {
	return m.rebalanceTableErr
}
func (m *fullMockDBClient) ListSessionsWithResourceGroup(_ context.Context) ([]SessionWithGroup, error) {
	return nil, nil
}
func (m *fullMockDBClient) ListUserDatabases(_ context.Context) ([]string, error) {
	return m.userDatabases, m.listUserDBsErr
}
func (m *fullMockDBClient) SetupExporterRole(_ context.Context, _ string) error    { return nil }
func (m *fullMockDBClient) SetupPXFExtensions(_ context.Context) (int, error)      { return 2, nil }
func (m *fullMockDBClient) EnsureDataLoaderRole(_ context.Context, _ string) error { return nil }
func (m *fullMockDBClient) ListPXFExtensions(_ context.Context) ([]string, error) {
	return []string{"pxf", "pxf_fdw"}, nil
}
func (m *fullMockDBClient) ListExternalTables(_ context.Context) ([]ExternalTableInfo, error) {
	return nil, nil
}
func (m *fullMockDBClient) ReadPXFSourceSample(
	_ context.Context, _, _, _ string, _ int,
) (*PXFSourceSample, error) {
	return nil, nil
}
func (m *fullMockDBClient) GetQueryDetail(_ context.Context, pid int32) (*QueryDetail, error) {
	return &QueryDetail{PID: pid, State: "active", Query: "SELECT 1"}, nil
}
func (m *fullMockDBClient) EnsureQueryHistoryTable(_ context.Context) error { return nil }
func (m *fullMockDBClient) InsertQueryHistory(_ context.Context, _ *QueryHistoryEntry) error {
	return nil
}
func (m *fullMockDBClient) GetQueryHistory(_ context.Context, _ QueryHistoryFilter) ([]QueryHistoryEntry, int, error) {
	return []QueryHistoryEntry{}, 0, nil
}
func (m *fullMockDBClient) GetQueryHistoryDetail(_ context.Context, _ string) (*QueryHistoryEntry, error) {
	return nil, fmt.Errorf("not found")
}
func (m *fullMockDBClient) ExportQueryHistoryCSV(_ context.Context, _ QueryHistoryFilter, _ io.Writer) error {
	return nil
}
func (m *fullMockDBClient) CleanupQueryHistory(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}
func (m *fullMockDBClient) MoveQueryToResourceGroup(_ context.Context, _ int32, _ string) error {
	return nil
}

// TestFullMockDBClient_ImplementsInterface verifies the mock implements Client.
func TestFullMockDBClient_ImplementsInterface(t *testing.T) {
	var _ Client = &fullMockDBClient{}
}

// TestFullMockDBClient_ErrorPaths tests all error paths of the mock client.
func TestFullMockDBClient_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func() *fullMockDBClient
		action  func(c *fullMockDBClient) error
		wantErr string
	}{
		{
			name:    "Ping error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{pingErr: fmt.Errorf("conn refused")} },
			action:  func(c *fullMockDBClient) error { return c.Ping(ctx) },
			wantErr: "conn refused",
		},
		{
			name:    "GetSegmentConfiguration error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{getSegErr: fmt.Errorf("seg error")} },
			action:  func(c *fullMockDBClient) error { _, err := c.GetSegmentConfiguration(ctx); return err },
			wantErr: "seg error",
		},
		{
			name:    "SetParameter error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{setParamErr: fmt.Errorf("param error")} },
			action:  func(c *fullMockDBClient) error { return c.SetParameter(ctx, "k", "v", ParameterScope{}) },
			wantErr: "param error",
		},
		{
			name:  "ShowParameter error",
			setup: func() *fullMockDBClient { return &fullMockDBClient{showParamErr: fmt.Errorf("show error")} },
			action: func(c *fullMockDBClient) error {
				_, err := c.ShowParameter(ctx, "k")
				return err
			},
			wantErr: "show error",
		},
		{
			name:    "ReloadConfig error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{reloadErr: fmt.Errorf("reload error")} },
			action:  func(c *fullMockDBClient) error { return c.ReloadConfig(ctx) },
			wantErr: "reload error",
		},
		{
			name:    "CreateResourceQueue error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{createResQueueErr: fmt.Errorf("queue error")} },
			action:  func(c *fullMockDBClient) error { return c.CreateResourceQueue(ctx, ResourceQueueOptions{Name: "q"}) },
			wantErr: "queue error",
		},
		{
			name:    "DropResourceQueue error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{dropResQueueErr: fmt.Errorf("drop error")} },
			action:  func(c *fullMockDBClient) error { return c.DropResourceQueue(ctx, "q") },
			wantErr: "drop error",
		},
		{
			name:  "ListResourceQueues error",
			setup: func() *fullMockDBClient { return &fullMockDBClient{listResQueuesErr: fmt.Errorf("list error")} },
			action: func(c *fullMockDBClient) error {
				_, err := c.ListResourceQueues(ctx)
				return err
			},
			wantErr: "list error",
		},
		{
			name:    "InitializeMirrors error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{initMirrorsErr: fmt.Errorf("mirror error")} },
			action:  func(c *fullMockDBClient) error { return c.InitializeMirrors(ctx, MirrorInitOptions{}) },
			wantErr: "mirror error",
		},
		{
			name:    "ConfigureReplication error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{configRepErr: fmt.Errorf("rep error")} },
			action:  func(c *fullMockDBClient) error { return c.ConfigureReplication(ctx, ReplicationOptions{}) },
			wantErr: "rep error",
		},
		{
			name:  "GetMirrorSyncStatus error",
			setup: func() *fullMockDBClient { return &fullMockDBClient{getMirrorSyncErr: fmt.Errorf("sync error")} },
			action: func(c *fullMockDBClient) error {
				_, err := c.GetMirrorSyncStatus(ctx)
				return err
			},
			wantErr: "sync error",
		},
		{
			name:    "TriggerFTSProbe error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{triggerFTSErr: fmt.Errorf("fts error")} },
			action:  func(c *fullMockDBClient) error { return c.TriggerFTSProbe(ctx) },
			wantErr: "fts error",
		},
		{
			name:  "TerminateAllBackends error",
			setup: func() *fullMockDBClient { return &fullMockDBClient{terminateAllErr: fmt.Errorf("term error")} },
			action: func(c *fullMockDBClient) error {
				_, err := c.TerminateAllBackends(ctx)
				return err
			},
			wantErr: "term error",
		},
		{
			name:  "CancelAllQueries error",
			setup: func() *fullMockDBClient { return &fullMockDBClient{cancelAllErr: fmt.Errorf("cancel error")} },
			action: func(c *fullMockDBClient) error {
				_, err := c.CancelAllQueries(ctx)
				return err
			},
			wantErr: "cancel error",
		},
		{
			name:    "LogRotate error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{logRotateErr: fmt.Errorf("rotate error")} },
			action:  func(c *fullMockDBClient) error { return c.LogRotate(ctx) },
			wantErr: "rotate error",
		},
		{
			name:    "RegisterNewSegments error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{registerSegErr: fmt.Errorf("reg error")} },
			action:  func(c *fullMockDBClient) error { return c.RegisterNewSegments(ctx, SegmentRegistrationOptions{}) },
			wantErr: "reg error",
		},
		{
			name:    "RedistributeData error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{redistributeErr: fmt.Errorf("redist error")} },
			action:  func(c *fullMockDBClient) error { return c.RedistributeData(ctx, RedistributionOptions{}) },
			wantErr: "redist error",
		},
		{
			name:  "GetRedistributionProgress error",
			setup: func() *fullMockDBClient { return &fullMockDBClient{getRedistProgressErr: fmt.Errorf("progress error")} },
			action: func(c *fullMockDBClient) error {
				_, err := c.GetRedistributionProgress(ctx)
				return err
			},
			wantErr: "progress error",
		},
		{
			name:    "DeregisterSegments error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{deregisterErr: fmt.Errorf("dereg error")} },
			action:  func(c *fullMockDBClient) error { return c.DeregisterSegments(ctx, 2) },
			wantErr: "dereg error",
		},
		{
			name: "RedistributeBeforeScaleIn error",
			setup: func() *fullMockDBClient {
				return &fullMockDBClient{redistBeforeScaleInErr: fmt.Errorf("scalein error")}
			},
			action: func(c *fullMockDBClient) error {
				return c.RedistributeBeforeScaleIn(ctx, ScaleInRedistributionOptions{})
			},
			wantErr: "scalein error",
		},
		{
			name:  "AnalyzeSkew error",
			setup: func() *fullMockDBClient { return &fullMockDBClient{analyzeSkewErr: fmt.Errorf("skew error")} },
			action: func(c *fullMockDBClient) error {
				_, err := c.AnalyzeSkew(ctx, "db")
				return err
			},
			wantErr: "skew error",
		},
		{
			name:    "RebalanceTable error",
			setup:   func() *fullMockDBClient { return &fullMockDBClient{rebalanceTableErr: fmt.Errorf("rebal error")} },
			action:  func(c *fullMockDBClient) error { return c.RebalanceTable(ctx, "db", "s", "t", "k") },
			wantErr: "rebal error",
		},
		{
			name:  "ListUserDatabases error",
			setup: func() *fullMockDBClient { return &fullMockDBClient{listUserDBsErr: fmt.Errorf("list db error")} },
			action: func(c *fullMockDBClient) error {
				_, err := c.ListUserDatabases(ctx)
				return err
			},
			wantErr: "list db error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.setup()
			err := tt.action(c)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestFullMockDBClient_SuccessPaths tests all success paths with return values.
func TestFullMockDBClient_SuccessPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("TerminateAllBackends returns count", func(t *testing.T) {
		c := &fullMockDBClient{terminatedBackends: 5}
		count, err := c.TerminateAllBackends(ctx)
		require.NoError(t, err)
		assert.Equal(t, int32(5), count)
	})

	t.Run("CancelAllQueries returns count", func(t *testing.T) {
		c := &fullMockDBClient{canceledQueries: 3}
		count, err := c.CancelAllQueries(ctx)
		require.NoError(t, err)
		assert.Equal(t, int32(3), count)
	})

	t.Run("GetRedistributionProgress returns progress", func(t *testing.T) {
		c := &fullMockDBClient{redistProgress: 75}
		progress, err := c.GetRedistributionProgress(ctx)
		require.NoError(t, err)
		assert.Equal(t, int32(75), progress)
	})

	t.Run("ListResourceQueues returns queues", func(t *testing.T) {
		queues := []ResourceQueueInfo{
			{Name: "q1", ActiveStatements: 10, Priority: "HIGH"},
			{Name: "q2", ActiveStatements: 5, Priority: "LOW"},
		}
		c := &fullMockDBClient{resQueues: queues}
		result, err := c.ListResourceQueues(ctx)
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, "q1", result[0].Name)
	})

	t.Run("GetMirrorSyncStatus returns infos", func(t *testing.T) {
		infos := []MirrorSyncInfo{
			{ContentID: 0, IsSynced: true, State: "streaming"},
			{ContentID: 1, IsSynced: false, ReplicationLag: 1024, State: "catchup"},
		}
		c := &fullMockDBClient{mirrorSyncInfos: infos}
		result, err := c.GetMirrorSyncStatus(ctx)
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.True(t, result[0].IsSynced)
		assert.False(t, result[1].IsSynced)
	})

	t.Run("AnalyzeSkew returns skew infos", func(t *testing.T) {
		skews := []TableSkewInfo{
			{Database: "db1", Schema: "public", Table: "t1", SkewCoefficient: 15.5, RowCount: 1000},
		}
		c := &fullMockDBClient{skewInfos: skews}
		result, err := c.AnalyzeSkew(ctx, "db1")
		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, 15.5, result[0].SkewCoefficient)
	})

	t.Run("ListUserDatabases returns databases", func(t *testing.T) {
		c := &fullMockDBClient{userDatabases: []string{"postgres", "mydb", "analytics"}}
		result, err := c.ListUserDatabases(ctx)
		require.NoError(t, err)
		assert.Len(t, result, 3)
	})

	t.Run("Close increments counter", func(t *testing.T) {
		c := &fullMockDBClient{}
		c.Close()
		c.Close()
		assert.Equal(t, 2, c.closeCalls)
	})
}

// TestPgxClient_ResourceQueue_Mock tests resource queue operations via mock PG server.
func TestPgxClient_CreateResourceQueue_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("CREATE RESOURCE QUEUE")
	})
	defer cleanup()

	tests := []struct {
		name string
		opts ResourceQueueOptions
	}{
		{
			name: "with all options",
			opts: ResourceQueueOptions{
				Name: "test_queue", ActiveStatements: 10,
				MemoryLimit: "2GB", Priority: "HIGH",
				MaxCost: 1000, MinCost: 100,
			},
		},
		{
			name: "with minimal options",
			opts: ResourceQueueOptions{Name: "minimal_queue"},
		},
		{
			name: "with active statements only",
			opts: ResourceQueueOptions{Name: "active_queue", ActiveStatements: 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.CreateResourceQueue(context.Background(), tt.opts)
			assert.NoError(t, err)
		})
	}
}

func TestPgxClient_CreateResourceQueue_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("queue already exists")
	})
	defer cleanup()

	err := client.CreateResourceQueue(context.Background(), ResourceQueueOptions{Name: "existing"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "creating resource queue")
}

func TestPgxClient_DropResourceQueue_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("DROP RESOURCE QUEUE")
	})
	defer cleanup()

	err := client.DropResourceQueue(context.Background(), "test_queue")
	assert.NoError(t, err)
}

func TestPgxClient_DropResourceQueue_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("queue not found")
	})
	defer cleanup()

	err := client.DropResourceQueue(context.Background(), "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dropping resource queue")
}

func TestPgxClient_ListResourceQueues_Mock(t *testing.T) {
	rqFields := []fieldDesc{
		textField("rsqname"), int4Field("active_statements"),
		textField("memory_limit"), textField("priority"),
		float8Field("max_cost"), float8Field("min_cost"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(rqFields, [][]string{
			{"analytics_queue", "10", "2GB", "HIGH", "1000", "100"},
			{"etl_queue", "5", "-1", "LOW", "-1", "0"},
		})
	})
	defer cleanup()

	queues, err := client.ListResourceQueues(context.Background())
	assert.NoError(t, err)
	require.Len(t, queues, 2)
	assert.Equal(t, "analytics_queue", queues[0].Name)
	assert.Equal(t, int32(10), queues[0].ActiveStatements)
	assert.Equal(t, "HIGH", queues[0].Priority)
}

func TestPgxClient_ListResourceQueues_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("query failed")
	})
	defer cleanup()

	_, err := client.ListResourceQueues(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying resource queues")
}

func TestPgxClient_ListResourceQueues_Empty(t *testing.T) {
	rqFields := []fieldDesc{
		textField("rsqname"), int4Field("active_statements"),
		textField("memory_limit"), textField("priority"),
		float8Field("max_cost"), float8Field("min_cost"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(rqFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	queues, err := client.ListResourceQueues(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, queues)
}

// TestPgxClient_TriggerFTSProbe_Mock tests FTS probe trigger.
func TestPgxClient_TriggerFTSProbe_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	err := client.TriggerFTSProbe(context.Background())
	assert.NoError(t, err)
}

func TestPgxClient_TriggerFTSProbe_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("FTS probe failed")
	})
	defer cleanup()

	err := client.TriggerFTSProbe(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "triggering FTS probe")
}

// TestPgxClient_TerminateAllBackends_Mock tests backend termination.
func TestPgxClient_TerminateAllBackends_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{int4Field("count")},
			[]string{"5"},
		)
	})
	defer cleanup()

	count, err := client.TerminateAllBackends(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int32(5), count)
}

func TestPgxClient_TerminateAllBackends_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("terminate failed")
	})
	defer cleanup()

	_, err := client.TerminateAllBackends(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "terminating all backends")
}

// TestPgxClient_CancelAllQueries_Mock tests query cancellation.
func TestPgxClient_CancelAllQueries_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{int4Field("count")},
			[]string{"3"},
		)
	})
	defer cleanup()

	count, err := client.CancelAllQueries(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int32(3), count)
}

func TestPgxClient_CancelAllQueries_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("cancel failed")
	})
	defer cleanup()

	_, err := client.CancelAllQueries(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "canceling all queries")
}

// TestPgxClient_LogRotate_Mock tests log rotation.
func TestPgxClient_LogRotate_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	err := client.LogRotate(context.Background())
	assert.NoError(t, err)
}

func TestPgxClient_LogRotate_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("log rotate failed")
	})
	defer cleanup()

	err := client.LogRotate(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rotating log file")
}

// TestPgxClient_GetRedistributionProgress_Mock tests redistribution progress.
func TestPgxClient_GetRedistributionProgress_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{int4Field("total"), int4Field("analyzed")},
			[]string{"100", "75"},
		)
	})
	defer cleanup()

	progress, err := client.GetRedistributionProgress(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int32(75), progress)
}

func TestPgxClient_GetRedistributionProgress_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("progress query failed")
	})
	defer cleanup()

	_, err := client.GetRedistributionProgress(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying redistribution progress")
}

func TestPgxClient_GetRedistributionProgress_ZeroTables(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{int4Field("total"), int4Field("analyzed")},
			[]string{"0", "0"},
		)
	})
	defer cleanup()

	progress, err := client.GetRedistributionProgress(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int32(100), progress)
}

// TestPgxClient_CreateResourceGroup_DefaultParams tests default params when none specified.
func TestPgxClient_CreateResourceGroup_DefaultParams(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("CREATE RESOURCE GROUP")
	})
	defer cleanup()

	// No concurrency, cpu, or weight specified — should use default cpu_max_percent=20.
	err := client.CreateResourceGroup(context.Background(), ResourceGroupOptions{Name: "default_group"})
	assert.NoError(t, err)
}

// TestBuildConnectionString_InvalidPort tests invalid port handling.
func TestBuildConnectionString_InvalidPort(t *testing.T) {
	tests := []struct {
		name string
		port int32
	}{
		{"negative port", -1},
		{"port too high", 70000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Host: "localhost", Port: tt.port, Database: "db",
				Username: "user", Password: "pass", SSLMode: "disable",
			}
			_, err := buildConnectionString(cfg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid port")
		})
	}
}

// TestPgConnURL_String tests the pgConnURL String method.
func TestPgConnURL_String(t *testing.T) {
	tests := []struct {
		name     string
		url      pgConnURL
		contains []string
	}{
		{
			name: "basic URL",
			url: pgConnURL{
				host: "localhost", port: 5432, database: "testdb",
				user: "admin", password: "secret", sslMode: "disable",
			},
			contains: []string{"postgres://", "localhost:5432", "testdb", "sslmode=disable"},
		},
		{
			name: "URL with special chars in password",
			url: pgConnURL{
				host: "db.example.com", port: 5433, database: "prod",
				user: "user", password: "p@ss w0rd", sslMode: "require",
			},
			contains: []string{"db.example.com:5433", "prod", "sslmode=require"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.url.String()
			for _, c := range tt.contains {
				assert.Contains(t, result, c)
			}
		})
	}
}

// TestPgConnURL_String_MetacharacterRoundTrip verifies that credentials and the
// database name containing DSN metacharacters ('@', '/', '?', '#', '&', '=',
// ':') round-trip without corruption or parameter injection (W1-A1/E-1). The
// produced URL must parse back — via both net/url and pgx ParseConfig — to the
// ORIGINAL field values, and sslmode must remain exactly the configured value.
func TestPgConnURL_String_MetacharacterRoundTrip(t *testing.T) {
	u := pgConnURL{
		host:     "db.example.com",
		port:     5433,
		database: "weird/db?name#x",
		user:     "u@ser:name",
		password: "p@ss/w0rd?x=1&y=2#z:colon",
		sslMode:  "require",
	}

	raw := u.String()

	parsed, err := url.Parse(raw)
	require.NoError(t, err)
	assert.Equal(t, "postgres", parsed.Scheme)
	assert.Equal(t, u.user, parsed.User.Username())
	pw, hasPw := parsed.User.Password()
	require.True(t, hasPw)
	assert.Equal(t, u.password, pw)
	// Leading "/" stripped → original database name preserved.
	assert.Equal(t, u.database, strings.TrimPrefix(parsed.Path, "/"))
	// sslmode must be exactly the configured value — no injected extra params.
	assert.Equal(t, u.sslMode, parsed.Query().Get("sslmode"))
	assert.Len(t, parsed.Query(), 1, "no extra connection parameters may be injected")

	// pgx must also parse the DSN back to the original credentials.
	cfg, err := pgconn.ParseConfig(raw)
	require.NoError(t, err)
	assert.Equal(t, u.user, cfg.User)
	assert.Equal(t, u.password, cfg.Password)
	assert.Equal(t, u.host, cfg.Host)
	assert.Equal(t, uint16(u.port), cfg.Port)
	assert.Equal(t, u.database, cfg.Database)
}

// TestPgConnURL_String_EmptySSLMode verifies the W1-A1 regression edge case: an
// empty sslMode still yields a valid DSN that pgconn.ParseConfig accepts, with a
// single "sslmode=" (empty value) query parameter and no panic (TASK 12).
func TestPgConnURL_String_EmptySSLMode(t *testing.T) {
	u := pgConnURL{
		host:     "localhost",
		port:     5432,
		database: "testdb",
		user:     "admin",
		password: "secret",
		sslMode:  "", // explicitly empty
	}

	raw := u.String()

	parsed, err := url.Parse(raw)
	require.NoError(t, err)
	// Exactly one query param, the empty-valued sslmode (matches prior behavior).
	assert.Len(t, parsed.Query(), 1, "exactly one connection parameter expected")
	values, ok := parsed.Query()["sslmode"]
	require.True(t, ok, "sslmode parameter must be present")
	require.Len(t, values, 1)
	assert.Equal(t, "", values[0], "sslmode value must be empty")

	// pgx must accept the empty-sslmode DSN without error.
	cfg, err := pgconn.ParseConfig(raw)
	require.NoError(t, err)
	assert.Equal(t, u.user, cfg.User)
	assert.Equal(t, u.database, cfg.Database)
}

// TestMirrorInitOptions_Construction tests MirrorInitOptions struct.
func TestMirrorInitOptions_Construction(t *testing.T) {
	opts := MirrorInitOptions{
		Layout:       "group",
		SegmentCount: 4,
		Parallelism:  2,
	}
	assert.Equal(t, "group", opts.Layout)
	assert.Equal(t, int32(4), opts.SegmentCount)
	assert.Equal(t, int32(2), opts.Parallelism)
}

// TestReplicationOptions_Construction tests ReplicationOptions struct.
func TestReplicationOptions_Construction(t *testing.T) {
	opts := ReplicationOptions{Mode: "sync"}
	assert.Equal(t, "sync", opts.Mode)
}

// TestMirrorSyncInfo_Construction tests MirrorSyncInfo struct.
func TestMirrorSyncInfo_Construction(t *testing.T) {
	info := MirrorSyncInfo{
		ContentID:      1,
		IsSynced:       true,
		ReplicationLag: 0,
		State:          "streaming",
	}
	assert.Equal(t, int32(1), info.ContentID)
	assert.True(t, info.IsSynced)
	assert.Equal(t, "streaming", info.State)
}

// TestSegmentRegistrationOptions_Construction tests SegmentRegistrationOptions struct.
func TestSegmentRegistrationOptions_Construction(t *testing.T) {
	opts := SegmentRegistrationOptions{
		OldCount:       2,
		NewCount:       4,
		MirrorEnabled:  true,
		SegmentService: "test-segment-headless",
		ClusterName:    "test-cluster",
		Port:           6000,
	}
	assert.Equal(t, int32(2), opts.OldCount)
	assert.Equal(t, int32(4), opts.NewCount)
	assert.True(t, opts.MirrorEnabled)
}

// TestRedistributionOptions_Construction tests RedistributionOptions struct.
func TestRedistributionOptions_Construction(t *testing.T) {
	opts := RedistributionOptions{
		Database:      "mydb",
		ExcludeTables: []string{"temp_table", "audit_*"},
		Parallelism:   4,
	}
	assert.Equal(t, "mydb", opts.Database)
	assert.Len(t, opts.ExcludeTables, 2)
}

// TestScaleInRedistributionOptions_Construction tests ScaleInRedistributionOptions struct.
func TestScaleInRedistributionOptions_Construction(t *testing.T) {
	opts := ScaleInRedistributionOptions{
		NewCount:      2,
		Database:      "mydb",
		ExcludeTables: []string{"temp_*"},
	}
	assert.Equal(t, int32(2), opts.NewCount)
	assert.Equal(t, "mydb", opts.Database)
}

// TestScaleInTableInfo_Construction tests scaleInTableInfo struct.
func TestScaleInTableInfo_Construction(t *testing.T) {
	info := scaleInTableInfo{
		schema:  "public",
		table:   "orders",
		distKey: "customer_id",
	}
	assert.Equal(t, "public", info.schema)
	assert.Equal(t, "orders", info.table)
	assert.Equal(t, "customer_id", info.distKey)
}

// TestResourceQueueOptions_Construction tests ResourceQueueOptions struct.
func TestResourceQueueOptions_Construction(t *testing.T) {
	opts := ResourceQueueOptions{
		Name:             "analytics",
		ActiveStatements: 20,
		MemoryLimit:      "4GB",
		Priority:         "HIGH",
		MaxCost:          5000,
		MinCost:          100,
	}
	assert.Equal(t, "analytics", opts.Name)
	assert.Equal(t, int32(20), opts.ActiveStatements)
	assert.Equal(t, "4GB", opts.MemoryLimit)
	assert.Equal(t, "HIGH", opts.Priority)
	assert.Equal(t, float64(5000), opts.MaxCost)
	assert.Equal(t, float64(100), opts.MinCost)
}

// TestResourceQueueInfo_Construction tests ResourceQueueInfo struct.
func TestResourceQueueInfo_Construction(t *testing.T) {
	info := ResourceQueueInfo{
		Name:             "etl_queue",
		ActiveStatements: 5,
		MemoryLimit:      "2GB",
		Priority:         "MEDIUM",
		MaxCost:          1000,
		MinCost:          50,
		ActiveWaiters:    2,
	}
	assert.Equal(t, "etl_queue", info.Name)
	assert.Equal(t, int32(2), info.ActiveWaiters)
}

// TestBuildConnectionString_EmptyHost tests with empty host.
func TestBuildConnectionString_EmptyHost(t *testing.T) {
	cfg := Config{
		Host: "", Port: 5432, Database: "db",
		Username: "user", Password: "pass", SSLMode: "disable",
	}
	// pgx should handle empty host (defaults to localhost).
	result, err := buildConnectionString(cfg)
	assert.NoError(t, err)
	assert.NotEmpty(t, result)
}

// TestBuildConnectionString_EmptyUsername tests with empty username.
func TestBuildConnectionString_EmptyUsername(t *testing.T) {
	cfg := Config{
		Host: "localhost", Port: 5432, Database: "db",
		Username: "", Password: "pass", SSLMode: "disable",
	}
	result, err := buildConnectionString(cfg)
	assert.NoError(t, err)
	assert.NotEmpty(t, result)
}

// TestBuildConnectionString_LongPassword tests with a very long password.
func TestBuildConnectionString_LongPassword(t *testing.T) {
	cfg := Config{
		Host: "localhost", Port: 5432, Database: "db",
		Username: "user", Password: strings.Repeat("a", 1000), SSLMode: "disable",
	}
	result, err := buildConnectionString(cfg)
	assert.NoError(t, err)
	assert.NotEmpty(t, result)
}

// TestPgxClient_InitializeMirrors_Mock tests mirror initialization.
func TestPgxClient_InitializeMirrors_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	err := client.InitializeMirrors(context.Background(), MirrorInitOptions{
		Layout: "group", SegmentCount: 4, Parallelism: 2,
	})
	assert.NoError(t, err)
}

func TestPgxClient_InitializeMirrors_PingError(t *testing.T) {
	// Create a client with a closed pool to simulate ping failure.
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("connection refused")
	})
	defer cleanup()

	// Close the pool to force ping failure.
	client.pool.Close()

	err := client.InitializeMirrors(context.Background(), MirrorInitOptions{
		Layout: "group", SegmentCount: 4, Parallelism: 2,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database not reachable")
}

// TestPgxClient_ConfigureReplication_Mock tests replication configuration.
func TestPgxClient_ConfigureReplication_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	err := client.ConfigureReplication(context.Background(), ReplicationOptions{Mode: "sync"})
	assert.NoError(t, err)
}

func TestPgxClient_ConfigureReplication_PingError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	client.pool.Close()

	err := client.ConfigureReplication(context.Background(), ReplicationOptions{Mode: "sync"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database not reachable")
}

// TestPgxClient_GetMirrorSyncStatus_Mock tests mirror sync status retrieval.
func TestPgxClient_GetMirrorSyncStatus_Mock(t *testing.T) {
	mirrorFields := []fieldDesc{
		int4Field("content_id"), boolField("is_synced"),
		int8Field("replication_lag"), textField("state"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(mirrorFields, [][]string{
			{"0", "t", "0", "streaming"},
			{"1", "f", "1024", "catchup"},
		})
	})
	defer cleanup()

	infos, err := client.GetMirrorSyncStatus(context.Background())
	assert.NoError(t, err)
	require.Len(t, infos, 2)
	assert.True(t, infos[0].IsSynced)
	assert.Equal(t, "streaming", infos[0].State)
	assert.False(t, infos[1].IsSynced)
	assert.Equal(t, int64(1024), infos[1].ReplicationLag)
}

func TestPgxClient_GetMirrorSyncStatus_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("mirror query failed")
	})
	defer cleanup()

	_, err := client.GetMirrorSyncStatus(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying mirror sync status")
}

// TestPgxClient_DeregisterSegments_Mock tests segment deregistration.
func TestPgxClient_DeregisterSegments_Mock(t *testing.T) {
	callCount := 0
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		callCount++
		if callCount == 1 {
			// Ping
			return execResponse("SELECT 1")
		}
		// SET allow_system_table_mods or DELETE
		return execResponse("DELETE 4")
	})
	defer cleanup()

	err := client.DeregisterSegments(context.Background(), 2)
	assert.NoError(t, err)
}

func TestPgxClient_DeregisterSegments_PingError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	client.pool.Close()

	err := client.DeregisterSegments(context.Background(), 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database not reachable")
}

// TestPgxClient_ListUserDatabases_Mock tests listing user databases.
func TestPgxClient_ListUserDatabases_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"datname"}, [][]string{
			{"postgres"},
			{"mydb"},
			{"analytics"},
		})
	})
	defer cleanup()

	dbs, err := client.ListUserDatabases(context.Background())
	assert.NoError(t, err)
	require.Len(t, dbs, 3)
	assert.Equal(t, "postgres", dbs[0])
}

func TestPgxClient_ListUserDatabases_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("query failed")
	})
	defer cleanup()

	_, err := client.ListUserDatabases(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying databases")
}

// TestClientFactory_NewClient_SSLModes tests SSL mode determination in factory.
func TestClientFactory_NewClient_SSLEnabled(t *testing.T) {
	scheme := newTestScheme()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.AdminPasswordSecretName("test-cluster"),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("secret"),
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	factory := NewClientFactory(k8sClient, nil)

	t.Run("SSL enabled without CertSecret uses require", func(t *testing.T) {
		cluster := &cbv1alpha1.CloudberryCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster",
				Namespace: "default",
			},
			Spec: cbv1alpha1.CloudberryClusterSpec{
				Coordinator: cbv1alpha1.CoordinatorSpec{Port: 5432},
				Auth: &cbv1alpha1.AuthSpec{
					SSL: &cbv1alpha1.SSLSpec{
						Enabled: true,
					},
				},
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		// Will fail at DB connection, but exercises the SSL mode logic.
		_, err := factory.NewClient(ctx, cluster)
		assert.Error(t, err)
	})

	t.Run("SSL enabled with CertSecret uses verify-ca", func(t *testing.T) {
		cluster := &cbv1alpha1.CloudberryCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster",
				Namespace: "default",
			},
			Spec: cbv1alpha1.CloudberryClusterSpec{
				Coordinator: cbv1alpha1.CoordinatorSpec{Port: 5432},
				Auth: &cbv1alpha1.AuthSpec{
					SSL: &cbv1alpha1.SSLSpec{
						Enabled:    true,
						CertSecret: &cbv1alpha1.CertSecretRef{Name: "my-cert"},
					},
				},
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		// The cert Secret "my-cert" does not exist, so verify-ca resolution
		// fails while reading the SSL root CA, which confirms the verify-ca
		// path (rather than verify-full) is taken when a CertSecret is set.
		_, err := factory.NewClient(ctx, cluster)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reading SSL root CA")
	})

	t.Run("SSL disabled uses disable", func(t *testing.T) {
		cluster := &cbv1alpha1.CloudberryCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster",
				Namespace: "default",
			},
			Spec: cbv1alpha1.CloudberryClusterSpec{
				Coordinator: cbv1alpha1.CoordinatorSpec{Port: 5432},
				Auth: &cbv1alpha1.AuthSpec{
					SSL: &cbv1alpha1.SSLSpec{
						Enabled: false,
					},
				},
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := factory.NewClient(ctx, cluster)
		assert.Error(t, err)
	})
}
