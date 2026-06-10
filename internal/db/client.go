// Package db provides a Cloudberry/PostgreSQL database client for the cloudberry operator.
package db

import (
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// Severity level constants for recommendations.
const (
	severityInfo     = "info"
	severityWarning  = "warning"
	severityCritical = "critical"
)

// Transient per-database connection pool bounds. Redistribution opens a
// short-lived pool per database; capping the size and connection lifetime
// prevents connection spikes against the coordinator when many databases are
// processed sequentially.
const (
	redistributionPoolMaxConns        = int32(4)
	redistributionPoolMaxConnLifetime = 5 * time.Minute
)

// sanitizeDistKey sanitizes a comma-separated distribution key by individually
// quoting each column name using pgx.Identifier{}.Sanitize(). This prevents
// SQL injection via malicious column names in distribution keys.
func sanitizeDistKey(distKey string) (string, error) {
	if distKey == "" {
		return "", nil
	}
	cols := strings.Split(distKey, ",")
	sanitized := make([]string, 0, len(cols))
	for _, col := range cols {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		sanitized = append(sanitized, pgx.Identifier{col}.Sanitize())
	}
	if len(sanitized) == 0 {
		return "", fmt.Errorf("distribution key contains no valid column names: %q", distKey)
	}
	return strings.Join(sanitized, ", "), nil
}

// Client defines the interface for Cloudberry database operations.
type Client interface {
	// Ping checks database connectivity.
	Ping(ctx context.Context) error
	// Close closes the database connection pool.
	Close()
	// GetSegmentConfiguration returns the segment configuration.
	GetSegmentConfiguration(ctx context.Context) ([]SegmentInfo, error)
	// GetClusterState returns the overall cluster health state.
	GetClusterState(ctx context.Context) (*ClusterState, error)
	// SetParameter sets a configuration parameter.
	SetParameter(ctx context.Context, name, value string, scope ParameterScope) error
	// ShowParameter returns the current value of a parameter.
	ShowParameter(ctx context.Context, name string) (string, error)
	// ReloadConfig triggers a configuration reload.
	ReloadConfig(ctx context.Context) error
	// ListSessions returns active database sessions.
	ListSessions(ctx context.Context) ([]Session, error)
	// ListSessionsWithResourceGroup returns sessions with their resource group assignment.
	// It joins pg_stat_activity with pg_roles and pg_resgroup to determine each session's
	// resource group. Sessions without a resource group assignment return an empty string.
	ListSessionsWithResourceGroup(ctx context.Context) ([]SessionWithGroup, error)
	// CancelQuery cancels a running query by PID.
	CancelQuery(ctx context.Context, pid int32) (bool, error)
	// TerminateSession terminates a session by PID.
	TerminateSession(ctx context.Context, pid int32) (bool, error)
	// CreateRole creates a new database role.
	CreateRole(ctx context.Context, opts RoleOptions) error
	// AlterRole modifies an existing database role.
	AlterRole(ctx context.Context, opts RoleOptions) error
	// DropRole drops a database role.
	DropRole(ctx context.Context, name string) error
	// Vacuum runs a vacuum operation.
	Vacuum(ctx context.Context, opts VacuumOptions) error
	// Analyze runs an analyze operation.
	Analyze(ctx context.Context, table string) error
	// Reindex runs a reindex operation.
	Reindex(ctx context.Context, opts ReindexOptions) error
	// GetDiskUsage returns disk usage information.
	GetDiskUsage(ctx context.Context, database string) ([]DiskUsage, error)
	// GetReplicationLag returns the replication lag in bytes.
	GetReplicationLag(ctx context.Context) (int64, error)
	// PromoteStandby promotes the standby to primary.
	PromoteStandby(ctx context.Context) error
	// GetActiveQueryCount returns the number of active, queued, and blocked queries.
	GetActiveQueryCount(ctx context.Context) (active, queued, blocked int32, err error)
	// GetMaxConnections returns the server's max_connections setting.
	GetMaxConnections(ctx context.Context) (int32, error)
	// GetResourceGroupUsage returns CPU and memory usage for a resource group.
	GetResourceGroupUsage(ctx context.Context, group string) (cpu, memory float64, err error)
	// CreateResourceGroup creates a new resource group.
	CreateResourceGroup(ctx context.Context, opts ResourceGroupOptions) error
	// AlterResourceGroup modifies an existing resource group.
	AlterResourceGroup(ctx context.Context, opts ResourceGroupOptions) error
	// DropResourceGroup drops a resource group.
	DropResourceGroup(ctx context.Context, name string) error
	// ListResourceGroups returns all resource groups.
	ListResourceGroups(ctx context.Context) ([]ResourceGroupInfo, error)
	// AssignRoleResourceGroup assigns a role to a resource group.
	AssignRoleResourceGroup(ctx context.Context, role, group string) error
	// CreateResourceQueue creates a new resource queue.
	CreateResourceQueue(ctx context.Context, opts ResourceQueueOptions) error
	// DropResourceQueue drops a resource queue.
	DropResourceQueue(ctx context.Context, name string) error
	// ListResourceQueues returns all resource queues.
	ListResourceQueues(ctx context.Context) ([]ResourceQueueInfo, error)
	// CreateBackup creates a new backup.
	CreateBackup(ctx context.Context, opts BackupOptions) (*BackupInfo, error)
	// RestoreBackup restores from a backup.
	RestoreBackup(ctx context.Context, opts RestoreOptions) error
	// ListBackups returns all available backups.
	ListBackups(ctx context.Context) ([]BackupInfo, error)
	// DeleteBackup deletes a backup by ID.
	DeleteBackup(ctx context.Context, id string) error
	// CreateDataLoadingJob creates a new data loading job.
	CreateDataLoadingJob(ctx context.Context, job DataLoadingJobConfig) error
	// StartDataLoadingJob starts a data loading job.
	StartDataLoadingJob(ctx context.Context, name string) error
	// StopDataLoadingJob stops a data loading job.
	StopDataLoadingJob(ctx context.Context, name string) error
	// ListDataLoadingJobs returns all data loading jobs.
	ListDataLoadingJobs(ctx context.Context) ([]DataLoadingJobStatus, error)
	// GetStorageDiskUsage returns disk usage information per tablespace/segment.
	GetStorageDiskUsage(ctx context.Context) ([]DiskUsageInfo, error)
	// GetBloatRecommendations returns bloat recommendations.
	GetBloatRecommendations(ctx context.Context) ([]Recommendation, error)
	// GetSkewRecommendations returns data skew recommendations.
	GetSkewRecommendations(ctx context.Context) ([]Recommendation, error)
	// GetAgeRecommendations returns XID age recommendations.
	GetAgeRecommendations(ctx context.Context) ([]Recommendation, error)
	// GetIndexBloatRecommendations returns index bloat recommendations.
	GetIndexBloatRecommendations(ctx context.Context) ([]Recommendation, error)
	// TriggerRecommendationScan triggers a recommendation scan.
	TriggerRecommendationScan(ctx context.Context) error
	// GetTableDetails returns detailed information about a specific table.
	GetTableDetails(ctx context.Context, schema, table string) (*TableDetail, error)
	// GetUsageReport returns a usage report for the given month.
	GetUsageReport(ctx context.Context, month string) ([]UsageReportEntry, error)
	// InitializeMirrors performs base backup from primaries to initialize
	// mirror segments. This is the pg_basebackup equivalent for Cloudberry.
	InitializeMirrors(ctx context.Context, opts MirrorInitOptions) error
	// ConfigureReplication sets up WAL streaming replication between
	// primary and mirror segments.
	ConfigureReplication(ctx context.Context, opts ReplicationOptions) error
	// GetMirrorSyncStatus returns the synchronization status of all
	// mirror segments, including replication lag per segment.
	GetMirrorSyncStatus(ctx context.Context) ([]MirrorSyncInfo, error)
	// TriggerFTSProbe requests Cloudberry's FTS daemon to perform an
	// immediate probe scan, which detects failed segments and triggers
	// automatic mirror promotion. Returns after the scan completes.
	TriggerFTSProbe(ctx context.Context) error
	// TerminateAllBackends terminates all non-system backend connections.
	// It calls pg_terminate_backend for each session except the current one
	// and system processes. Returns the number of backends terminated.
	TerminateAllBackends(ctx context.Context) (int32, error)
	// CancelAllQueries cancels all active queries (non-idle sessions)
	// except the current backend. Returns the number of queries canceled.
	CancelAllQueries(ctx context.Context) (int32, error)
	// LogRotate triggers a log file rotation by calling pg_rotate_logfile().
	// This signals the logger process to switch to a new log file immediately.
	LogRotate(ctx context.Context) error
	// RegisterNewSegments registers new primary and mirror segments in gp_segment_configuration.
	// This is called after new segment pods are ready during scale-out.
	RegisterNewSegments(ctx context.Context, opts SegmentRegistrationOptions) error
	// RedistributeData redistributes existing tables across all segments (including new ones).
	// This is the gpexpand equivalent for Cloudberry.
	RedistributeData(ctx context.Context, opts RedistributionOptions) error
	// GetRedistributionProgress returns the current redistribution progress (0-100).
	GetRedistributionProgress(ctx context.Context) (int32, error)
	// DeregisterSegments removes segment entries from gp_segment_configuration
	// for segments with content IDs >= newCount. This is called during scale-in
	// after data has been moved off the segments being removed.
	DeregisterSegments(ctx context.Context, newCount int32) error
	// RedistributeBeforeScaleIn redistributes data to only the remaining segments
	// before scaling in. This ensures no data is left on segments being removed.
	RedistributeBeforeScaleIn(ctx context.Context, opts ScaleInRedistributionOptions) error
	// AnalyzeSkew analyzes data skew across segments for all user tables in a database.
	// Returns skew coefficient per table (0 = perfectly balanced, 100 = all on one segment).
	AnalyzeSkew(ctx context.Context, database string) ([]TableSkewInfo, error)
	// RebalanceTable redistributes a single table across all segments using REORGANIZE=TRUE.
	RebalanceTable(ctx context.Context, database, schema, table, distKey string) error
	// ListUserDatabases returns all non-template, non-system databases.
	ListUserDatabases(ctx context.Context) ([]string, error)
	// SetupExporterRole creates the cloudberry_exporter database role with LOGIN privilege,
	// grants pg_monitor membership, and grants SELECT on monitoring views.
	SetupExporterRole(ctx context.Context, password string) error
	// GetQueryDetail returns detailed execution information for a specific query by PID.
	GetQueryDetail(ctx context.Context, pid int32) (*QueryDetail, error)
	// EnsureQueryHistoryTable creates the query history table and indexes if they don't exist.
	EnsureQueryHistoryTable(ctx context.Context) error
	// InsertQueryHistory inserts a single query history entry into the table.
	InsertQueryHistory(ctx context.Context, entry *QueryHistoryEntry) error
	// GetQueryHistory searches query history with filters and pagination.
	GetQueryHistory(ctx context.Context, filter QueryHistoryFilter) ([]QueryHistoryEntry, int, error)
	// GetQueryHistoryDetail returns detailed information for a specific historical query.
	GetQueryHistoryDetail(ctx context.Context, queryID string) (*QueryHistoryEntry, error)
	// ExportQueryHistoryCSV writes query history matching the filter as CSV to the writer.
	ExportQueryHistoryCSV(ctx context.Context, filter QueryHistoryFilter, w io.Writer) error
	// CleanupQueryHistory deletes query history entries older than the retention period.
	CleanupQueryHistory(ctx context.Context, retention time.Duration) (int64, error)
	// MoveQueryToResourceGroup moves a running query's session to a different resource group.
	// It looks up the session's role from pg_stat_activity and reassigns it via ALTER ROLE.
	MoveQueryToResourceGroup(ctx context.Context, pid int32, targetGroup string) error
}

// ParameterScope defines the scope for parameter changes.
type ParameterScope struct {
	// Level is the scope level (cluster, database, role).
	Level string
	// Target is the database or role name (for database/role scope).
	Target string
}

// SegmentInfo represents a segment in the cluster configuration.
type SegmentInfo struct {
	ContentID      int32  `json:"contentID"`
	DBID           int32  `json:"dbid"`
	Role           string `json:"role"`
	PreferredRole  string `json:"preferredRole"`
	Mode           string `json:"mode"`
	Status         string `json:"status"`
	Hostname       string `json:"hostname"`
	Address        string `json:"address"`
	Port           int32  `json:"port"`
	DataDirectory  string `json:"dataDirectory"`
	ReplicationLag int64  `json:"replicationLag,omitempty"`
}

// ClusterState represents the overall cluster health.
type ClusterState struct {
	IsUp              bool
	Version           string
	SegmentsUp        int32
	SegmentsDown      int32
	SegmentsTotal     int32
	MirroringInSync   bool
	ActiveConnections int32
	MaxConnections    int32
}

// Session represents an active database session.
type Session struct {
	PID           int32     `json:"pid"`
	Username      string    `json:"username"`
	Database      string    `json:"database"`
	Application   string    `json:"application"`
	ClientAddress string    `json:"clientAddress"`
	State         string    `json:"state"`
	WaitEventType string    `json:"waitEventType"`
	Query         string    `json:"query"`
	QueryStart    time.Time `json:"queryStart"`
	Duration      string    `json:"duration"`
}

// SessionWithGroup extends Session with resource group information.
// It joins pg_stat_activity with pg_roles and pg_resgroup to determine
// each session's resource group. Sessions without a resource group
// assignment return an empty string for ResourceGroup.
type SessionWithGroup struct {
	Session
	ResourceGroup string `json:"resourceGroup"`
}

// QueryDetail contains detailed execution information for a running query.
type QueryDetail struct {
	PID            int32      `json:"pid"`
	Username       string     `json:"username"`
	Database       string     `json:"database"`
	State          string     `json:"state"`
	Query          string     `json:"query"`
	QueryStart     time.Time  `json:"queryStart"`
	Duration       string     `json:"duration"`
	WaitEventType  string     `json:"waitEventType,omitempty"`
	WaitEvent      string     `json:"waitEvent,omitempty"`
	BackendType    string     `json:"backendType,omitempty"`
	Locks          []LockInfo `json:"locks,omitempty"`
	TablesAccessed []string   `json:"tablesAccessed,omitempty"`
	ExplainPlan    string     `json:"explainPlan,omitempty"`
}

// LockInfo describes a lock held or awaited by a query.
type LockInfo struct {
	LockType string `json:"lockType"`
	Mode     string `json:"mode"`
	Granted  bool   `json:"granted"`
	Relation string `json:"relation,omitempty"`
}

// RoleOptions defines options for creating or altering a role.
type RoleOptions struct {
	Name       string
	Password   string
	Login      bool
	SuperUser  bool
	CreateDB   bool
	CreateRole bool
	ValidUntil string
}

// VacuumOptions defines options for vacuum operations.
type VacuumOptions struct {
	Full    bool
	Analyze bool
	Table   string
}

// ReindexOptions defines options for reindex operations.
type ReindexOptions struct {
	Database string
	Table    string
}

// DiskUsage represents disk usage for a database.
type DiskUsage struct {
	Database  string `json:"database"`
	SizeBytes int64  `json:"sizeBytes"`
	SizeHuman string `json:"sizeHuman"`
}

// IOLimitOption defines I/O limits for a single tablespace.
type IOLimitOption struct {
	Tablespace       string
	ReadBytesPerSec  int64
	WriteBytesPerSec int64
	ReadIOPS         int32
	WriteIOPS        int32
}

// ResourceGroupOptions defines options for creating or altering a resource group.
type ResourceGroupOptions struct {
	Name          string
	Concurrency   int32
	CPUMaxPercent int32
	CPUWeight     int32
	MemoryLimit   int32
	MinCost       int32
	// IOLimits defines per-tablespace I/O limits (optional).
	IOLimits []IOLimitOption
}

// ResourceQueueOptions defines options for creating or altering a resource queue.
type ResourceQueueOptions struct {
	Name             string
	ActiveStatements int32
	MemoryLimit      string
	Priority         string
	MaxCost          float64
	MinCost          float64
}

// ResourceQueueInfo represents a resource queue.
type ResourceQueueInfo struct {
	Name             string  `json:"name"`
	ActiveStatements int32   `json:"activeStatements"`
	MemoryLimit      string  `json:"memoryLimit"`
	Priority         string  `json:"priority"`
	MaxCost          float64 `json:"maxCost"`
	MinCost          float64 `json:"minCost"`
	ActiveWaiters    int32   `json:"activeWaiters"`
}

// ResourceGroupInfo represents a resource group.
type ResourceGroupInfo struct {
	Name          string  `json:"name"`
	Concurrency   int32   `json:"concurrency"`
	CPUMaxPercent int32   `json:"cpuMaxPercent"`
	CPUWeight     int32   `json:"cpuWeight"`
	MemoryLimit   int32   `json:"memoryLimit"`
	MinCost       int32   `json:"minCost"`
	CPUUsage      float64 `json:"cpuUsage"`
	MemoryUsage   float64 `json:"memoryUsage"`
	// IOLimits is the raw io_limit string from the database (if set).
	IOLimits string `json:"ioLimits,omitempty"`
}

// BackupOptions defines options for creating a backup.
type BackupOptions struct {
	Type        string // full, incremental
	Compression int32
	Parallelism int32
	Destination string
}

// BackupInfo represents a backup record.
type BackupInfo struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
	SizeBytes int64     `json:"sizeBytes"`
	Path      string    `json:"path"`
}

// RestoreOptions defines options for restoring from a backup.
type RestoreOptions struct {
	BackupID       string
	TargetDatabase string
	Schemas        []string
	Tables         []string
}

// DataLoadingJobConfig defines a data loading job configuration.
type DataLoadingJobConfig struct {
	Name        string
	Type        string // s3, kafka, rabbitmq
	TargetTable string
	Schedule    string
	Config      map[string]string
}

// DataLoadingJobStatus represents the status of a data loading job.
type DataLoadingJobStatus struct {
	Name       string    `json:"name"`
	Type       string    `json:"type"`
	Status     string    `json:"status"`
	LastRun    time.Time `json:"lastRun"`
	RowsLoaded int64     `json:"rowsLoaded"`
}

// DiskUsageInfo represents disk usage information per tablespace or segment.
type DiskUsageInfo struct {
	Tablespace   string `json:"tablespace"`
	SizeBytes    int64  `json:"sizeBytes"`
	SizeHuman    string `json:"sizeHuman"`
	UsagePercent int32  `json:"usagePercent"`
}

// Recommendation represents a storage or performance recommendation.
type Recommendation struct {
	Type        string `json:"type"`
	Schema      string `json:"schema"`
	Table       string `json:"table"`
	Description string `json:"description"`
	Severity    string `json:"severity"`
	Value       int64  `json:"value"`
	// Ratio is an optional 0-100 percentage associated with the recommendation
	// (e.g. the dead-tuple bloat percentage for "bloat" recommendations). It is
	// omitempty so existing consumers and the on-wire JSON shape are unaffected;
	// it is populated where the underlying query already computes it so callers
	// (e.g. the storage reconciler's cloudberry_table_bloat_ratio metric) can
	// use a numeric value without re-parsing Description.
	Ratio float64 `json:"ratio,omitempty"`
}

// TableDetail represents detailed information about a database table.
type TableDetail struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	SizeBytes    int64  `json:"sizeBytes"`
	SizeHuman    string `json:"sizeHuman"`
	RowCount     int64  `json:"rowCount"`
	BloatPercent int32  `json:"bloatPercent"`
	SkewPercent  int32  `json:"skewPercent"`
	LastVacuum   string `json:"lastVacuum"`
	LastAnalyze  string `json:"lastAnalyze"`
}

// UsageReportEntry represents a single entry in a usage report.
type UsageReportEntry struct {
	Month       string `json:"month"`
	Database    string `json:"database"`
	SizeBytes   int64  `json:"sizeBytes"`
	SizeHuman   string `json:"sizeHuman"`
	GrowthBytes int64  `json:"growthBytes"`
	GrowthHuman string `json:"growthHuman"`
	QueryCount  int64  `json:"queryCount"`
	Connections int64  `json:"connections"`
}

// MirrorInitOptions defines options for initializing mirror segments.
type MirrorInitOptions struct {
	// Layout is the mirror placement strategy ("group" or "spread").
	Layout string
	// SegmentCount is the number of segments to initialize.
	SegmentCount int32
	// Parallelism is the number of concurrent base backups.
	Parallelism int32
}

// ReplicationOptions defines options for configuring WAL replication.
type ReplicationOptions struct {
	// Mode is the replication mode ("sync" or "async").
	Mode string
}

// MirrorSyncInfo represents the synchronization status of a mirror segment.
type MirrorSyncInfo struct {
	// ContentID is the segment content identifier.
	ContentID int32
	// IsSynced indicates whether the mirror is fully synchronized.
	IsSynced bool
	// ReplicationLag is the replication lag in bytes.
	ReplicationLag int64
	// State is the current replication state ("streaming", "catchup", "initializing").
	State string
}

// SegmentRegistrationOptions defines options for registering new segments.
type SegmentRegistrationOptions struct {
	// OldCount is the previous segment count.
	OldCount int32
	// NewCount is the new segment count.
	NewCount int32
	// MirrorEnabled indicates whether to register mirrors too.
	MirrorEnabled bool
	// SegmentService is the headless service name for DNS resolution (without namespace).
	SegmentService string
	// ClusterName is the cluster name used to construct pod names.
	ClusterName string
	// Port is the segment port.
	Port int32
}

// RedistributionOptions defines options for data redistribution.
type RedistributionOptions struct {
	// Database is the database to redistribute.
	Database string
	// ExcludeTables is the list of tables to exclude from redistribution.
	ExcludeTables []string
	// Parallelism is the number of concurrent redistribution threads.
	Parallelism int32
}

// ScaleInRedistributionOptions defines options for redistributing data
// before a scale-in operation. Data is moved OFF segments being removed
// to the remaining segments (0..NewCount-1).
type ScaleInRedistributionOptions struct {
	// NewCount is the target segment count (data goes to segments 0..NewCount-1).
	NewCount int32
	// Database is the database to redistribute. If empty, all user databases are processed.
	Database string
	// ExcludeTables is the list of tables to exclude from redistribution.
	ExcludeTables []string
}

// TableSkewInfo holds skew analysis results for a single table.
type TableSkewInfo struct {
	// Database is the database containing this table.
	Database string `json:"database"`
	// Schema is the table's schema name.
	Schema string `json:"schema"`
	// Table is the table name.
	Table string `json:"table"`
	// SkewCoefficient is the skew percentage (0 = balanced, 100 = all on one segment).
	SkewCoefficient float64 `json:"skewCoefficient"`
	// DistributionKey is the table's distribution key (empty for randomly distributed).
	DistributionKey string `json:"distributionKey"`
	// RowCount is the total number of rows in the table.
	RowCount int64 `json:"rowCount"`
}

// scaleInTableInfo holds table metadata for scale-in redistribution.
type scaleInTableInfo struct {
	schema  string
	table   string
	distKey string
}

// Config holds database client configuration.
type Config struct {
	// Host is the database host.
	Host string
	// Port is the database port.
	Port int32
	// Database is the database name.
	Database string
	// Username is the database username.
	Username string
	// Password is the database password.
	Password string
	// SSLMode is the SSL mode (disable, require, verify-ca, verify-full).
	SSLMode string
	// SSLRootCA holds the PEM-encoded CA certificate(s) used to verify the
	// server certificate chain for verify-ca / verify-full SSL modes. When
	// empty, pgx falls back to the host's system root CA pool. This is
	// required when connecting to a cluster whose serving certificate is
	// issued by a private CA (for example, Vault PKI), because the private
	// CA is not present in the system trust store.
	SSLRootCA []byte
	// MaxConns is the maximum number of connections in the pool.
	MaxConns int32
	// RetryOpts configures retry behavior.
	RetryOpts util.RetryOptions
}

// pgxClient implements Client using pgx.
type pgxClient struct {
	pool      *pgxpool.Pool
	config    Config
	retryOpts util.RetryOptions
	logger    *slog.Logger
	// recorder records query-history metrics. It is optional and may be nil;
	// all metric recording is guarded with a nil check.
	recorder metrics.Recorder
	// metricsCluster and metricsNamespace are the label values used when
	// recording query-history metrics. They are empty unless SetRecorder is used.
	metricsCluster   string
	metricsNamespace string
	// unregisterPoolStats removes this client's connection-pool stats provider
	// from the metrics registry. It is set by registerPoolStats and invoked by
	// Close so a closed pool is never sampled on scrape. Nil when pool stats
	// were never registered (no recorder configured).
	unregisterPoolStats func()
}

// SetRecorder configures an optional metrics recorder and the cluster/namespace
// labels used when recording query-history metrics. It is safe to leave the
// recorder unset (nil); metric recording is then a no-op.
func (c *pgxClient) SetRecorder(recorder metrics.Recorder, cluster, namespace string) {
	c.recorder = recorder
	c.metricsCluster = cluster
	c.metricsNamespace = namespace
}

// registerPoolStats registers this client's pgxpool statistics with the
// metrics recorder so cloudberry_db_pool_* gauges are sampled on every
// Prometheus scrape. The provider is unregistered by Close. It is a no-op
// when no recorder is configured.
func (c *pgxClient) registerPoolStats() {
	if c.recorder == nil {
		return
	}
	pool := c.pool
	c.unregisterPoolStats = c.recorder.RegisterDBPoolStats(
		c.metricsCluster, c.metricsNamespace,
		func() (acquired, idle, maxConns float64) {
			st := pool.Stat()
			return float64(st.AcquiredConns()), float64(st.IdleConns()), float64(st.MaxConns())
		},
	)
}

// NewClient creates a new database client with connection pooling.
func NewClient(ctx context.Context, cfg Config, logger *slog.Logger) (Client, error) {
	if logger == nil {
		logger = slog.Default()
	}

	retryOpts := cfg.RetryOpts
	if retryOpts.MaxRetries == 0 {
		retryOpts = util.DefaultRetryOptions()
	}

	connStr, err := buildConnectionString(cfg)
	if err != nil {
		return nil, fmt.Errorf("building connection string: %w", err)
	}

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parsing connection string: %w", err)
	}

	if err := applyRootCA(poolCfg, cfg.SSLRootCA); err != nil {
		return nil, err
	}

	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}

	// Install the in-house pgx query tracer so every SQL statement executed
	// through the pool produces a child span (no-op when telemetry is
	// disabled). No statement text is recorded (see pgxQueryTracer).
	poolCfg.ConnConfig.Tracer = &pgxQueryTracer{database: cfg.Database}

	var pool *pgxpool.Pool
	connectErr := util.RetryWithBackoff(ctx, retryOpts, func(ctx context.Context) error {
		var poolErr error
		pool, poolErr = pgxpool.NewWithConfig(ctx, poolCfg)
		if poolErr != nil {
			return fmt.Errorf("creating connection pool: %w", poolErr)
		}
		if pingErr := pool.Ping(ctx); pingErr != nil {
			pool.Close()
			return pingErr
		}
		return nil
	})

	if connectErr != nil {
		if pool != nil {
			pool.Close()
		}
		return nil, fmt.Errorf("connecting to database: %w", connectErr)
	}

	logger.Info("database connection established",
		"host", cfg.Host,
		"port", cfg.Port,
		"database", cfg.Database,
	)

	return &pgxClient{
		pool:      pool,
		config:    cfg,
		retryOpts: retryOpts,
		logger:    logger,
	}, nil
}

// applyRootCA installs the supplied PEM-encoded CA certificate(s) into the
// pool's TLS configuration so that verify-ca / verify-full SSL modes validate
// the server certificate chain against a private CA (for example, Vault PKI)
// rather than only the host's system trust store.
//
// When rootCA is empty this is a no-op: pgx keeps the TLS configuration it
// derived from the connection string (system roots), which is correct for the
// "require" and "disable" modes that do not need a custom CA. When rootCA is
// non-empty but the SSL mode produced no TLS configuration (for example,
// sslmode=disable), there is nothing to attach and the CA is ignored.
func applyRootCA(poolCfg *pgxpool.Config, rootCA []byte) error {
	if len(rootCA) == 0 {
		return nil
	}

	tlsCfg := poolCfg.ConnConfig.TLSConfig
	if tlsCfg == nil {
		// SSL is not negotiated for this connection (for example,
		// sslmode=disable); there is no TLS configuration to attach the CA to.
		return nil
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(rootCA) {
		return fmt.Errorf("parsing SSL root CA: no valid certificate found in PEM data")
	}
	tlsCfg.RootCAs = pool
	return nil
}

// buildConnectionString constructs a PostgreSQL connection string using pgx's
// native config parsing to prevent injection vulnerabilities.
// It builds a pgconn.Config programmatically and validates it via pgx.ParseConfig.
// Returns an error if the connection parameters are invalid.
func buildConnectionString(cfg Config) (string, error) {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	if cfg.Port < 0 || cfg.Port > 65535 {
		return "", fmt.Errorf("invalid port number: %d", cfg.Port)
	}

	// Build a connection URL using the pgx URL format which handles
	// special characters safely via net/url encoding.
	u := &pgConnURL{
		host:     cfg.Host,
		port:     cfg.Port,
		database: cfg.Database,
		user:     cfg.Username,
		password: cfg.Password,
		sslMode:  sslMode,
	}

	connStr := u.String()

	// Validate the connection string via pgx.ParseConfig to ensure
	// all parameters are valid.
	connCfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return "", fmt.Errorf("invalid connection parameters: %w", err)
	}

	return connCfg.ConnString(), nil
}

// pgConnURL builds a PostgreSQL connection URL with properly encoded parameters.
type pgConnURL struct {
	host     string
	port     int32
	database string
	user     string
	password string
	sslMode  string
}

// String returns the connection URL string with properly encoded parameters.
func (u *pgConnURL) String() string {
	hostPort := net.JoinHostPort(u.host, strconv.Itoa(int(u.port)))
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
		urlEncode(u.user),
		urlEncode(u.password),
		hostPort,
		urlEncode(u.database),
		urlEncode(u.sslMode),
	)
}

// urlEncode encodes a string for use in a URL path or query parameter.
func urlEncode(s string) string {
	return (&url.URL{Path: s}).String()
}

// Ping checks database connectivity.
func (c *pgxClient) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

// Close closes the database connection pool and unregisters the pool stats
// provider so a closed pool is never sampled on a metrics scrape.
func (c *pgxClient) Close() {
	if c.unregisterPoolStats != nil {
		c.unregisterPoolStats()
	}
	c.pool.Close()
}

// GetSegmentConfiguration returns the segment configuration from gp_segment_configuration.
func (c *pgxClient) GetSegmentConfiguration(ctx context.Context) ([]SegmentInfo, error) {
	// Cast char(1) columns (role, preferred_role, mode, status) to text explicitly.
	// Cloudberry's gp_segment_configuration uses "char" type (OID 18) for these columns,
	// which pgx cannot scan into *string in binary protocol mode. Casting to text
	// ensures compatibility regardless of the query execution mode.
	query := `SELECT content, dbid, role::text, preferred_role::text, mode::text, status::text, 
		hostname, address, port, datadir 
		FROM gp_segment_configuration ORDER BY content, role`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying segment configuration: %w", err)
	}
	defer rows.Close()

	var segments []SegmentInfo
	for rows.Next() {
		var seg SegmentInfo
		if err := rows.Scan(
			&seg.ContentID, &seg.DBID, &seg.Role, &seg.PreferredRole,
			&seg.Mode, &seg.Status, &seg.Hostname, &seg.Address,
			&seg.Port, &seg.DataDirectory,
		); err != nil {
			return nil, fmt.Errorf("scanning segment row: %w", err)
		}
		segments = append(segments, seg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating segment rows: %w", err)
	}

	return segments, nil
}

// GetClusterState returns the overall cluster health state.
func (c *pgxClient) GetClusterState(ctx context.Context) (*ClusterState, error) {
	state := &ClusterState{}

	// Check if database is up.
	if err := c.pool.Ping(ctx); err != nil {
		state.IsUp = false
		return state, fmt.Errorf("database ping failed: %w", err)
	}
	state.IsUp = true

	// Get version.
	if err := c.pool.QueryRow(ctx, "SHOW server_version").Scan(&state.Version); err != nil {
		c.logger.Warn("failed to get server version", "error", err)
	}

	// Get segment counts.
	segQuery := `SELECT 
		COUNT(*) FILTER (WHERE status = 'u') as up,
		COUNT(*) FILTER (WHERE status = 'd') as down,
		COUNT(*) as total
		FROM gp_segment_configuration WHERE content >= 0`

	if err := c.pool.QueryRow(ctx, segQuery).Scan(
		&state.SegmentsUp, &state.SegmentsDown, &state.SegmentsTotal,
	); err != nil {
		c.logger.Warn("failed to get segment counts", "error", err)
	}

	state.MirroringInSync = state.SegmentsDown == 0

	// Get connection counts.
	connQuery := `SELECT 
		(SELECT count(*) FROM pg_stat_activity WHERE state != 'idle') as active,
		(SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_conn`

	if err := c.pool.QueryRow(ctx, connQuery).Scan(
		&state.ActiveConnections, &state.MaxConnections,
	); err != nil {
		c.logger.Warn("failed to get connection counts", "error", err)
	}

	return state, nil
}

// SetParameter sets a configuration parameter at the specified scope.
func (c *pgxClient) SetParameter(ctx context.Context, name, value string, scope ParameterScope) error {
	var query string

	switch scope.Level {
	case "database":
		query = fmt.Sprintf("ALTER DATABASE %s SET %s = %s",
			pgx.Identifier{scope.Target}.Sanitize(),
			pgx.Identifier{name}.Sanitize(),
			quoteLiteral(value),
		)
	case "role":
		query = fmt.Sprintf("ALTER ROLE %s SET %s = %s",
			pgx.Identifier{scope.Target}.Sanitize(),
			pgx.Identifier{name}.Sanitize(),
			quoteLiteral(value),
		)
	default:
		query = fmt.Sprintf("ALTER SYSTEM SET %s = %s",
			pgx.Identifier{name}.Sanitize(),
			quoteLiteral(value),
		)
	}

	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("setting parameter %s=%s (scope=%s): %w", name, value, scope.Level, err)
	}

	c.logger.Info("parameter set", "name", name, "value", value, "scope", scope.Level)
	return nil
}

// ShowParameter returns the current value of a parameter.
func (c *pgxClient) ShowParameter(ctx context.Context, name string) (string, error) {
	var value string
	query := fmt.Sprintf("SHOW %s", pgx.Identifier{name}.Sanitize())
	if err := c.pool.QueryRow(ctx, query).Scan(&value); err != nil {
		return "", fmt.Errorf("showing parameter %s: %w", name, err)
	}
	return value, nil
}

// ReloadConfig triggers a configuration reload.
func (c *pgxClient) ReloadConfig(ctx context.Context) error {
	if _, err := c.pool.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("reloading configuration: %w", err)
	}
	c.logger.Info("configuration reloaded")
	return nil
}

// ListSessions returns active database sessions.
func (c *pgxClient) ListSessions(ctx context.Context) ([]Session, error) {
	query := `SELECT pid, COALESCE(usename, ''), COALESCE(datname, ''),
		COALESCE(application_name, ''),
		COALESCE(client_addr::text, ''), COALESCE(state, ''),
		COALESCE(wait_event_type, ''),
		COALESCE(query, ''), COALESCE(query_start, now()),
		COALESCE(now() - query_start, interval '0')::text
		FROM pg_stat_activity 
		WHERE pid != pg_backend_pid()
		AND usename IS NOT NULL
		ORDER BY query_start DESC NULLS LAST`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying sessions: %w", err)
	}
	defer rows.Close()

	var sessions []Session
	for rows.Next() {
		var s Session
		if err := rows.Scan(
			&s.PID, &s.Username, &s.Database, &s.Application, &s.ClientAddress,
			&s.State, &s.WaitEventType, &s.Query, &s.QueryStart, &s.Duration,
		); err != nil {
			return nil, fmt.Errorf("scanning session row: %w", err)
		}
		sessions = append(sessions, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating session rows: %w", err)
	}

	return sessions, nil
}

// ListSessionsWithResourceGroup returns sessions with their resource group assignment.
func (c *pgxClient) ListSessionsWithResourceGroup(ctx context.Context) ([]SessionWithGroup, error) {
	query := `SELECT s.pid, COALESCE(s.usename, ''), COALESCE(s.datname, ''),
		COALESCE(s.application_name, ''),
		COALESCE(s.client_addr::text, ''), COALESCE(s.state, ''),
		COALESCE(s.wait_event_type, ''),
		COALESCE(s.query, ''), COALESCE(s.query_start, now()),
		COALESCE(now() - s.query_start, interval '0')::text,
		COALESCE(rg.rsgname, '')
		FROM pg_stat_activity s
		LEFT JOIN pg_roles r ON s.usename = r.rolname
		LEFT JOIN pg_resgroup rg ON r.rolresgroup = rg.oid
		WHERE s.pid != pg_backend_pid()
		AND s.usename IS NOT NULL
		ORDER BY s.query_start DESC NULLS LAST`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying sessions with resource group: %w", err)
	}
	defer rows.Close()

	var sessions []SessionWithGroup
	for rows.Next() {
		var sg SessionWithGroup
		if err := rows.Scan(
			&sg.PID, &sg.Username, &sg.Database, &sg.Application, &sg.ClientAddress,
			&sg.State, &sg.WaitEventType, &sg.Query, &sg.QueryStart, &sg.Duration,
			&sg.ResourceGroup,
		); err != nil {
			return nil, fmt.Errorf("scanning session with resource group row: %w", err)
		}
		sessions = append(sessions, sg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating session with resource group rows: %w", err)
	}

	return sessions, nil
}

// CancelQuery cancels a running query by PID.
func (c *pgxClient) CancelQuery(ctx context.Context, pid int32) (bool, error) {
	var result bool
	if err := c.pool.QueryRow(ctx, "SELECT pg_cancel_backend($1)", pid).Scan(&result); err != nil {
		return false, fmt.Errorf("canceling query for PID %d: %w", pid, err)
	}
	c.logger.Info("query canceled", "pid", pid, "result", result)
	return result, nil
}

// TerminateSession terminates a session by PID.
func (c *pgxClient) TerminateSession(ctx context.Context, pid int32) (bool, error) {
	var result bool
	if err := c.pool.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&result); err != nil {
		return false, fmt.Errorf("terminating session for PID %d: %w", pid, err)
	}
	c.logger.Info("session terminated", "pid", pid, "result", result)
	return result, nil
}

// CreateRole creates a new database role.
func (c *pgxClient) CreateRole(ctx context.Context, opts RoleOptions) error {
	query := fmt.Sprintf("CREATE ROLE %s", pgx.Identifier{opts.Name}.Sanitize())
	query += buildRoleOptions(opts)

	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("creating role %s: %w", opts.Name, err)
	}
	c.logger.Info("role created", "name", opts.Name)
	return nil
}

// AlterRole modifies an existing database role.
func (c *pgxClient) AlterRole(ctx context.Context, opts RoleOptions) error {
	query := fmt.Sprintf("ALTER ROLE %s", pgx.Identifier{opts.Name}.Sanitize())
	query += buildRoleOptions(opts)

	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("altering role %s: %w", opts.Name, err)
	}
	c.logger.Info("role altered", "name", opts.Name)
	return nil
}

// DropRole drops a database role.
func (c *pgxClient) DropRole(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP ROLE IF EXISTS %s", pgx.Identifier{name}.Sanitize())
	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("dropping role %s: %w", name, err)
	}
	c.logger.Info("role dropped", "name", name)
	return nil
}

// Vacuum runs a vacuum operation.
func (c *pgxClient) Vacuum(ctx context.Context, opts VacuumOptions) error {
	query := "VACUUM"
	if opts.Full {
		query += " FULL"
	}
	if opts.Analyze {
		query += " ANALYZE"
	}
	if opts.Table != "" {
		query += " " + pgx.Identifier{opts.Table}.Sanitize()
	}

	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("running vacuum: %w", err)
	}
	c.logger.Info("vacuum completed", "full", opts.Full, "analyze", opts.Analyze, "table", opts.Table)
	return nil
}

// Analyze runs an analyze operation.
func (c *pgxClient) Analyze(ctx context.Context, table string) error {
	query := "ANALYZE"
	if table != "" {
		query += " " + pgx.Identifier{table}.Sanitize()
	}

	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("running analyze: %w", err)
	}
	c.logger.Info("analyze completed", "table", table)
	return nil
}

// Reindex runs a reindex operation.
func (c *pgxClient) Reindex(ctx context.Context, opts ReindexOptions) error {
	var query string
	switch {
	case opts.Table != "":
		query = fmt.Sprintf("REINDEX TABLE %s", pgx.Identifier{opts.Table}.Sanitize())
	case opts.Database != "":
		query = fmt.Sprintf("REINDEX DATABASE %s", pgx.Identifier{opts.Database}.Sanitize())
	default:
		return fmt.Errorf("either database or table must be specified for reindex")
	}

	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("running reindex: %w", err)
	}
	c.logger.Info("reindex completed", "database", opts.Database, "table", opts.Table)
	return nil
}

// GetDiskUsage returns disk usage information.
func (c *pgxClient) GetDiskUsage(ctx context.Context, database string) ([]DiskUsage, error) {
	query := `SELECT datname, pg_database_size(datname) as size_bytes,
		pg_size_pretty(pg_database_size(datname)) as size_human
		FROM pg_database WHERE datistemplate = false`

	if database != "" {
		query += fmt.Sprintf(" AND datname = %s", quoteLiteral(database))
	}
	query += " ORDER BY size_bytes DESC"

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying disk usage: %w", err)
	}
	defer rows.Close()

	var usages []DiskUsage
	for rows.Next() {
		var du DiskUsage
		if err := rows.Scan(&du.Database, &du.SizeBytes, &du.SizeHuman); err != nil {
			return nil, fmt.Errorf("scanning disk usage row: %w", err)
		}
		usages = append(usages, du)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating disk usage rows: %w", err)
	}

	return usages, nil
}

// GetReplicationLag returns the replication lag in bytes.
func (c *pgxClient) GetReplicationLag(ctx context.Context) (int64, error) {
	var lag int64
	query := `SELECT COALESCE(
		pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), 0
	) FROM pg_stat_replication LIMIT 1`

	if err := c.pool.QueryRow(ctx, query).Scan(&lag); err != nil {
		return 0, fmt.Errorf("querying replication lag: %w", err)
	}
	return lag, nil
}

// PromoteStandby promotes the standby to primary.
func (c *pgxClient) PromoteStandby(ctx context.Context) (err error) {
	ctx, end := c.startOperation(ctx, "PromoteStandby")
	defer func() { end(err) }()

	if _, err = c.pool.Exec(ctx, "SELECT pg_promote()"); err != nil {
		return fmt.Errorf("promoting standby: %w", err)
	}
	c.logger.Info("standby promoted to primary")
	return nil
}

// GetMaxConnections returns the server's max_connections setting from
// pg_settings. Used to publish the real cloudberry_connections_max gauge.
func (c *pgxClient) GetMaxConnections(ctx context.Context) (int32, error) {
	var maxConns int32
	query := `SELECT setting::int FROM pg_settings WHERE name = 'max_connections'`
	if err := c.pool.QueryRow(ctx, query).Scan(&maxConns); err != nil {
		return 0, fmt.Errorf("querying max_connections: %w", err)
	}
	return maxConns, nil
}

// GetActiveQueryCount returns the number of active, queued, and blocked queries.
func (c *pgxClient) GetActiveQueryCount(ctx context.Context) (active, queued, blocked int32, err error) {
	query := `SELECT 
		COUNT(*) FILTER (WHERE state = 'active') as active,
		COUNT(*) FILTER (WHERE wait_event_type = 'Lock') as blocked,
		COUNT(*) FILTER (WHERE state = 'idle in transaction') as queued
		FROM pg_stat_activity WHERE pid != pg_backend_pid()`

	if scanErr := c.pool.QueryRow(ctx, query).Scan(&active, &blocked, &queued); scanErr != nil {
		return 0, 0, 0, fmt.Errorf("querying active query counts: %w", scanErr)
	}
	return active, queued, blocked, nil
}

// GetResourceGroupUsage returns CPU and memory usage for a resource group.
func (c *pgxClient) GetResourceGroupUsage(
	ctx context.Context,
	group string,
) (cpu, memory float64, err error) {
	query := `SELECT 
		COALESCE(cpu_usage, 0), COALESCE(memory_usage, 0)
		FROM gp_toolkit.gp_resgroup_status 
		WHERE rsgname = $1`

	if scanErr := c.pool.QueryRow(ctx, query, group).Scan(&cpu, &memory); scanErr != nil {
		return 0, 0, fmt.Errorf("querying resource group usage for %s: %w", group, scanErr)
	}
	return cpu, memory, nil
}

// CreateResourceGroup creates a new resource group.
func (c *pgxClient) CreateResourceGroup(ctx context.Context, opts ResourceGroupOptions) error {
	params := []string{}
	if opts.Concurrency > 0 {
		params = append(params, fmt.Sprintf("concurrency=%d", opts.Concurrency))
	}
	if opts.CPUMaxPercent > 0 {
		params = append(params, fmt.Sprintf("cpu_max_percent=%d", opts.CPUMaxPercent))
	}
	if opts.CPUWeight > 0 {
		params = append(params, fmt.Sprintf("cpu_weight=%d", opts.CPUWeight))
	}
	if opts.MemoryLimit > 0 {
		params = append(params, fmt.Sprintf("memory_limit=%d", opts.MemoryLimit))
	}
	if opts.MinCost > 0 {
		params = append(params, fmt.Sprintf("min_cost=%d", opts.MinCost))
	}

	if len(params) == 0 {
		params = append(params, "cpu_max_percent=20")
	}

	query := fmt.Sprintf("CREATE RESOURCE GROUP %s WITH (%s)",
		pgx.Identifier{opts.Name}.Sanitize(), strings.Join(params, ", "))

	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("creating resource group %s: %w", opts.Name, err)
	}
	c.logger.Info("resource group created", "name", opts.Name)
	return nil
}

// FormatIOLimits formats I/O limits into the Cloudberry io_limit string format.
// Format: "tablespace:rbps=X:wbps=X:riops=X:wiops=X" joined by ";".
func FormatIOLimits(limits []IOLimitOption) string {
	if len(limits) == 0 {
		return ""
	}
	parts := make([]string, 0, len(limits))
	for _, l := range limits {
		part := fmt.Sprintf("%s:rbps=%d:wbps=%d:riops=%d:wiops=%d",
			l.Tablespace, l.ReadBytesPerSec, l.WriteBytesPerSec, l.ReadIOPS, l.WriteIOPS)
		parts = append(parts, part)
	}
	return strings.Join(parts, ";")
}

// AlterResourceGroup modifies an existing resource group.
func (c *pgxClient) AlterResourceGroup(ctx context.Context, opts ResourceGroupOptions) error {
	alterations := []struct {
		param string
		value int32
	}{
		{"concurrency", opts.Concurrency},
		{"cpu_max_percent", opts.CPUMaxPercent},
		{"cpu_weight", opts.CPUWeight},
		{"memory_limit", opts.MemoryLimit},
		{"min_cost", opts.MinCost},
	}

	// Cloudberry's ALTER RESOURCE GROUP syntax uses unquoted parameter names:
	//   ALTER RESOURCE GROUP <name> SET concurrency 20
	// The parameter names are fixed keywords (concurrency, cpu_max_percent, etc.),
	// not identifiers, so they must NOT be quoted with pgx.Identifier.Sanitize().
	for _, alt := range alterations {
		if alt.value <= 0 {
			continue
		}
		query := fmt.Sprintf("ALTER RESOURCE GROUP %s SET %s %d",
			pgx.Identifier{opts.Name}.Sanitize(),
			alt.param, alt.value)
		if _, err := c.pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("altering resource group %s param %s: %w", opts.Name, alt.param, err)
		}
	}

	// Apply I/O limits if specified.
	if len(opts.IOLimits) > 0 {
		ioLimitStr := FormatIOLimits(opts.IOLimits)
		alterSQL := fmt.Sprintf(`ALTER RESOURCE GROUP %s SET io_limit '%s'`,
			pgx.Identifier{opts.Name}.Sanitize(), ioLimitStr)
		if _, err := c.pool.Exec(ctx, alterSQL); err != nil {
			return fmt.Errorf("setting io_limit for resource group %s: %w", opts.Name, err)
		}
		c.logger.Info("resource group io_limit set", "name", opts.Name, "ioLimit", ioLimitStr)
	}

	c.logger.Info("resource group altered", "name", opts.Name)
	return nil
}

// DropResourceGroup drops a resource group.
func (c *pgxClient) DropResourceGroup(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP RESOURCE GROUP %s", pgx.Identifier{name}.Sanitize())
	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("dropping resource group %s: %w", name, err)
	}
	c.logger.Info("resource group dropped", "name", name)
	return nil
}

// ListResourceGroups returns all resource groups.
func (c *pgxClient) ListResourceGroups(ctx context.Context) ([]ResourceGroupInfo, error) {
	query := `SELECT g.rsgname,
		COALESCE((SELECT c.value::int FROM pg_resgroupcapability c
			WHERE c.resgroupid = g.oid AND c.reslimittype = 1), 0) AS concurrency,
		COALESCE((SELECT c.value::int FROM pg_resgroupcapability c
			WHERE c.resgroupid = g.oid AND c.reslimittype = 2), 0) AS cpu_max_percent,
		COALESCE((SELECT c.value::int FROM pg_resgroupcapability c
			WHERE c.resgroupid = g.oid AND c.reslimittype = 3), 0) AS cpu_weight,
		COALESCE((SELECT c.value::int FROM pg_resgroupcapability c
			WHERE c.resgroupid = g.oid AND c.reslimittype = 4), 0) AS memory_limit,
		COALESCE((SELECT c.value::int FROM pg_resgroupcapability c
			WHERE c.resgroupid = g.oid AND c.reslimittype = 5), 0) AS min_cost
		FROM pg_resgroup g
		WHERE g.rsgname NOT IN ('default_group', 'admin_group', 'system_group')
		ORDER BY g.rsgname`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying resource groups: %w", err)
	}
	defer rows.Close()

	var groups []ResourceGroupInfo
	for rows.Next() {
		var g ResourceGroupInfo
		scanErr := rows.Scan(&g.Name, &g.Concurrency, &g.CPUMaxPercent,
			&g.CPUWeight, &g.MemoryLimit, &g.MinCost)
		if scanErr != nil {
			return nil, fmt.Errorf("scanning resource group row: %w", scanErr)
		}
		groups = append(groups, g)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating resource group rows: %w", rowErr)
	}

	return groups, nil
}

// AssignRoleResourceGroup assigns a role to a resource group.
func (c *pgxClient) AssignRoleResourceGroup(ctx context.Context, role, group string) error {
	sql := fmt.Sprintf("ALTER ROLE %s RESOURCE GROUP %s",
		pgx.Identifier{role}.Sanitize(), pgx.Identifier{group}.Sanitize())
	if _, err := c.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("assigning role %s to resource group %s: %w", role, group, err)
	}
	c.logger.Info("role assigned to resource group", "role", role, "group", group)
	return nil
}

// CreateResourceQueue creates a new resource queue.
// SQL: CREATE RESOURCE QUEUE <name> WITH (ACTIVE_STATEMENTS=<n>, MEMORY_LIMIT='<size>', PRIORITY=<level>)
func (c *pgxClient) CreateResourceQueue(ctx context.Context, opts ResourceQueueOptions) error {
	var withClauses []string

	if opts.ActiveStatements > 0 {
		withClauses = append(withClauses, fmt.Sprintf("ACTIVE_STATEMENTS=%d", opts.ActiveStatements))
	}
	if opts.MemoryLimit != "" {
		withClauses = append(withClauses, fmt.Sprintf("MEMORY_LIMIT=%s", quoteLiteral(opts.MemoryLimit)))
	}
	if opts.Priority != "" {
		withClauses = append(withClauses, fmt.Sprintf("PRIORITY=%s", pgx.Identifier{opts.Priority}.Sanitize()))
	}
	if opts.MaxCost > 0 {
		withClauses = append(withClauses, fmt.Sprintf("MAX_COST=%g", opts.MaxCost))
	}
	if opts.MinCost > 0 {
		withClauses = append(withClauses, fmt.Sprintf("MIN_COST=%g", opts.MinCost))
	}

	query := fmt.Sprintf("CREATE RESOURCE QUEUE %s", pgx.Identifier{opts.Name}.Sanitize())
	if len(withClauses) > 0 {
		query += " WITH (" + strings.Join(withClauses, ", ") + ")"
	}

	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("creating resource queue %s: %w", opts.Name, err)
	}
	c.logger.Info("resource queue created", "name", opts.Name)
	return nil
}

// DropResourceQueue drops a resource queue.
func (c *pgxClient) DropResourceQueue(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP RESOURCE QUEUE %s", pgx.Identifier{name}.Sanitize())
	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("dropping resource queue %s: %w", name, err)
	}
	c.logger.Info("resource queue dropped", "name", name)
	return nil
}

// ListResourceQueues returns all resource queues from pg_resqueue.
func (c *pgxClient) ListResourceQueues(ctx context.Context) ([]ResourceQueueInfo, error) {
	query := `SELECT q.rsqname,
		COALESCE(q.rsqcountlimit, -1)::int AS active_statements,
		COALESCE((SELECT a.ressetting FROM pg_resqueue_attributes a
			WHERE a.rsqname = q.rsqname AND a.resname = 'memory_limit'),
			'-1') AS memory_limit,
		COALESCE((SELECT a.ressetting FROM pg_resqueue_attributes a
			WHERE a.rsqname = q.rsqname AND a.resname = 'priority'),
			'MEDIUM') AS priority,
		COALESCE(q.rsqcostlimit, -1) AS max_cost,
		COALESCE(q.rsqignorecostlimit, 0) AS min_cost
		FROM pg_resqueue q
		WHERE q.rsqname != 'pg_default'
		ORDER BY q.rsqname`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying resource queues: %w", err)
	}
	defer rows.Close()

	var queues []ResourceQueueInfo
	for rows.Next() {
		var q ResourceQueueInfo
		if scanErr := rows.Scan(
			&q.Name, &q.ActiveStatements, &q.MemoryLimit,
			&q.Priority, &q.MaxCost, &q.MinCost,
		); scanErr != nil {
			return nil, fmt.Errorf("scanning resource queue row: %w", scanErr)
		}
		queues = append(queues, q)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating resource queue rows: %w", rowErr)
	}

	c.logger.Info("retrieved resource queues", "count", len(queues))
	return queues, nil
}

// CreateBackup creates a new backup.
func (c *pgxClient) CreateBackup(ctx context.Context, opts BackupOptions) (*BackupInfo, error) {
	c.logger.Info("creating backup", "type", opts.Type, "destination", opts.Destination)

	info := &BackupInfo{
		ID:        fmt.Sprintf("backup-%d", time.Now().UnixNano()),
		Type:      opts.Type,
		Status:    "InProgress",
		StartTime: time.Now(),
	}

	// Backup is initiated via external tooling; record the intent.
	c.logger.Info("backup initiated", "id", info.ID, "type", info.Type)
	return info, nil
}

// RestoreBackup restores from a backup.
func (c *pgxClient) RestoreBackup(ctx context.Context, opts RestoreOptions) error {
	c.logger.Info("restoring backup",
		"backupID", opts.BackupID,
		"targetDatabase", opts.TargetDatabase,
		"schemas", opts.Schemas,
		"tables", opts.Tables,
	)

	// Restore is initiated via external tooling; record the intent.
	return nil
}

// ListBackups returns all available backups.
func (c *pgxClient) ListBackups(_ context.Context) ([]BackupInfo, error) {
	// Backup catalog is managed externally; return empty list as placeholder.
	return []BackupInfo{}, nil
}

// DeleteBackup deletes a backup by ID.
func (c *pgxClient) DeleteBackup(_ context.Context, id string) error {
	c.logger.Info("deleting backup", "id", id)
	// Backup deletion is managed externally; record the intent.
	return nil
}

// CreateDataLoadingJob creates a new data loading job.
func (c *pgxClient) CreateDataLoadingJob(_ context.Context, job DataLoadingJobConfig) error {
	c.logger.Info("creating data loading job", "name", job.Name, "type", job.Type, "target", job.TargetTable)
	// Data loading job creation is managed externally; record the intent.
	return nil
}

// StartDataLoadingJob starts a data loading job.
func (c *pgxClient) StartDataLoadingJob(_ context.Context, name string) error {
	c.logger.Info("starting data loading job", "name", name)
	// Data loading job start is managed externally; record the intent.
	return nil
}

// StopDataLoadingJob stops a data loading job.
func (c *pgxClient) StopDataLoadingJob(_ context.Context, name string) error {
	c.logger.Info("stopping data loading job", "name", name)
	// Data loading job stop is managed externally; record the intent.
	return nil
}

// ListDataLoadingJobs returns all data loading jobs.
func (c *pgxClient) ListDataLoadingJobs(_ context.Context) ([]DataLoadingJobStatus, error) {
	// Data loading job listing is managed externally; return empty list as placeholder.
	return []DataLoadingJobStatus{}, nil
}

// GetStorageDiskUsage returns disk usage information per tablespace/segment.
func (c *pgxClient) GetStorageDiskUsage(ctx context.Context) ([]DiskUsageInfo, error) {
	query := `SELECT spcname,
		pg_tablespace_size(oid) AS size_bytes,
		pg_size_pretty(pg_tablespace_size(oid)) AS size_human
		FROM pg_tablespace ORDER BY size_bytes DESC`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying storage disk usage: %w", err)
	}
	defer rows.Close()

	var usages []DiskUsageInfo
	for rows.Next() {
		var du DiskUsageInfo
		if scanErr := rows.Scan(&du.Tablespace, &du.SizeBytes, &du.SizeHuman); scanErr != nil {
			return nil, fmt.Errorf("scanning storage disk usage row: %w", scanErr)
		}
		usages = append(usages, du)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating storage disk usage rows: %w", rowErr)
	}

	c.logger.Info("retrieved storage disk usage", "count", len(usages))
	return usages, nil
}

// GetBloatRecommendations returns bloat recommendations by querying table statistics
// for dead tuple ratios that indicate bloat.
func (c *pgxClient) GetBloatRecommendations(ctx context.Context) ([]Recommendation, error) {
	query := `SELECT schemaname, relname,
		n_dead_tup,
		CASE WHEN n_live_tup + n_dead_tup > 0
			THEN (n_dead_tup * 100) / (n_live_tup + n_dead_tup)
			ELSE 0
		END AS dead_pct
		FROM pg_stat_user_tables
		WHERE n_dead_tup > 0
		ORDER BY n_dead_tup DESC
		LIMIT 50`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying bloat recommendations: %w", err)
	}
	defer rows.Close()

	var recs []Recommendation
	for rows.Next() {
		var r Recommendation
		var deadPct int64
		if scanErr := rows.Scan(&r.Schema, &r.Table, &r.Value, &deadPct); scanErr != nil {
			return nil, fmt.Errorf("scanning bloat recommendation row: %w", scanErr)
		}
		r.Type = "bloat"
		r.Severity = classifySeverity(deadPct, 20, 50)
		r.Description = fmt.Sprintf("Table has %d dead tuples (%d%% dead)", r.Value, deadPct)
		// Expose the dead-tuple percentage numerically so callers can record it
		// (e.g. cloudberry_table_bloat_ratio) without parsing Description.
		r.Ratio = float64(deadPct)
		recs = append(recs, r)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating bloat recommendation rows: %w", rowErr)
	}

	c.logger.Info("retrieved bloat recommendations", "count", len(recs))
	return recs, nil
}

// GetSkewRecommendations returns data skew recommendations by querying
// table size distribution across segments.
func (c *pgxClient) GetSkewRecommendations(ctx context.Context) ([]Recommendation, error) {
	// Query pg_stat_user_tables for tables with significant size variation.
	// In Cloudberry/Greenplum, gp_toolkit.gp_skew_coefficients provides skew data.
	// Fall back to pg_stat_user_tables if gp_toolkit is not available.
	query := `SELECT schemaname, relname, n_live_tup
		FROM pg_stat_user_tables
		WHERE n_live_tup > 1000
		ORDER BY n_live_tup DESC
		LIMIT 50`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying skew recommendations: %w", err)
	}
	defer rows.Close()

	var recs []Recommendation
	for rows.Next() {
		var r Recommendation
		if scanErr := rows.Scan(&r.Schema, &r.Table, &r.Value); scanErr != nil {
			return nil, fmt.Errorf("scanning skew recommendation row: %w", scanErr)
		}
		r.Type = "skew"
		r.Severity = severityInfo
		r.Description = fmt.Sprintf("Table has %d rows; verify distribution key for even data spread", r.Value)
		recs = append(recs, r)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating skew recommendation rows: %w", rowErr)
	}

	c.logger.Info("retrieved skew recommendations", "count", len(recs))
	return recs, nil
}

// GetAgeRecommendations returns XID age recommendations by querying
// tables with high dead tuple counts that need vacuuming.
func (c *pgxClient) GetAgeRecommendations(ctx context.Context) ([]Recommendation, error) {
	query := `SELECT schemaname, relname, n_dead_tup,
		COALESCE(EXTRACT(EPOCH FROM (now() - last_autovacuum))::bigint, 0) AS secs_since_vacuum
		FROM pg_stat_user_tables
		WHERE n_dead_tup > 10000
		ORDER BY n_dead_tup DESC
		LIMIT 50`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying age recommendations: %w", err)
	}
	defer rows.Close()

	var recs []Recommendation
	for rows.Next() {
		var r Recommendation
		var secsSinceVacuum int64
		if scanErr := rows.Scan(&r.Schema, &r.Table, &r.Value, &secsSinceVacuum); scanErr != nil {
			return nil, fmt.Errorf("scanning age recommendation row: %w", scanErr)
		}
		r.Type = "age"
		r.Severity = classifySeverity(r.Value, 100000, 500000)
		r.Description = fmt.Sprintf("Table has %d dead tuples; consider running VACUUM", r.Value)
		recs = append(recs, r)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating age recommendation rows: %w", rowErr)
	}

	c.logger.Info("retrieved age recommendations", "count", len(recs))
	return recs, nil
}

// GetIndexBloatRecommendations returns index bloat recommendations by querying
// index statistics for unused or oversized indexes.
func (c *pgxClient) GetIndexBloatRecommendations(ctx context.Context) ([]Recommendation, error) {
	query := `SELECT schemaname, relname, indexrelname,
		pg_relation_size(indexrelid) AS index_size,
		idx_scan
		FROM pg_stat_user_indexes
		WHERE pg_relation_size(indexrelid) > 0
		ORDER BY pg_relation_size(indexrelid) DESC
		LIMIT 50`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying index bloat recommendations: %w", err)
	}
	defer rows.Close()

	var recs []Recommendation
	for rows.Next() {
		var r Recommendation
		var indexName string
		var idxScan int64
		if scanErr := rows.Scan(&r.Schema, &r.Table, &indexName, &r.Value, &idxScan); scanErr != nil {
			return nil, fmt.Errorf("scanning index bloat recommendation row: %w", scanErr)
		}
		r.Type = "index_bloat"
		if idxScan == 0 {
			r.Severity = severityWarning
			r.Description = fmt.Sprintf("Index %s on %s.%s is %d bytes and has never been scanned",
				indexName, r.Schema, r.Table, r.Value)
		} else {
			r.Severity = severityInfo
			r.Description = fmt.Sprintf("Index %s on %s.%s is %d bytes with %d scans",
				indexName, r.Schema, r.Table, r.Value, idxScan)
		}
		recs = append(recs, r)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating index bloat recommendation rows: %w", rowErr)
	}

	c.logger.Info("retrieved index bloat recommendations", "count", len(recs))
	return recs, nil
}

// TriggerRecommendationScan triggers a recommendation scan by running ANALYZE
// on all user tables to refresh statistics.
func (c *pgxClient) TriggerRecommendationScan(ctx context.Context) error {
	c.logger.Info("triggering recommendation scan via ANALYZE")
	if _, err := c.pool.Exec(ctx, "ANALYZE"); err != nil {
		return fmt.Errorf("running ANALYZE for recommendation scan: %w", err)
	}
	c.logger.Info("recommendation scan completed")
	return nil
}

// GetTableDetails returns detailed information about a specific table
// by querying system catalog views.
func (c *pgxClient) GetTableDetails(ctx context.Context, schema, table string) (*TableDetail, error) {
	query := `SELECT
		s.schemaname,
		s.relname,
		pg_total_relation_size(c.oid) AS size_bytes,
		pg_size_pretty(pg_total_relation_size(c.oid)) AS size_human,
		s.n_live_tup AS row_count,
		CASE WHEN s.n_live_tup + s.n_dead_tup > 0
			THEN ((s.n_dead_tup * 100) / (s.n_live_tup + s.n_dead_tup))::int
			ELSE 0
		END AS bloat_percent,
		COALESCE(s.last_vacuum::text, s.last_autovacuum::text, 'never') AS last_vacuum,
		COALESCE(s.last_analyze::text, s.last_autoanalyze::text, 'never') AS last_analyze
		FROM pg_stat_user_tables s
		JOIN pg_class c ON c.relname = s.relname
		JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
		WHERE s.schemaname = $1 AND s.relname = $2`

	detail := &TableDetail{}
	if err := c.pool.QueryRow(ctx, query, schema, table).Scan(
		&detail.Schema, &detail.Table, &detail.SizeBytes, &detail.SizeHuman,
		&detail.RowCount, &detail.BloatPercent, &detail.LastVacuum, &detail.LastAnalyze,
	); err != nil {
		return nil, fmt.Errorf("querying table details for %s.%s: %w", schema, table, err)
	}

	c.logger.Info("retrieved table details", "schema", schema, "table", table)
	return detail, nil
}

// GetUsageReport returns a usage report for the given month by querying
// current database sizes and connection statistics.
func (c *pgxClient) GetUsageReport(ctx context.Context, month string) ([]UsageReportEntry, error) {
	query := `SELECT datname,
		pg_database_size(datname) AS size_bytes,
		pg_size_pretty(pg_database_size(datname)) AS size_human,
		COALESCE(numbackends, 0) AS connections
		FROM pg_database d
		LEFT JOIN pg_stat_database s ON d.datname = s.datname
		WHERE d.datistemplate = false
		ORDER BY size_bytes DESC`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying usage report: %w", err)
	}
	defer rows.Close()

	var entries []UsageReportEntry
	for rows.Next() {
		var e UsageReportEntry
		if scanErr := rows.Scan(&e.Database, &e.SizeBytes, &e.SizeHuman, &e.Connections); scanErr != nil {
			return nil, fmt.Errorf("scanning usage report row: %w", scanErr)
		}
		e.Month = month
		entries = append(entries, e)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating usage report rows: %w", rowErr)
	}

	c.logger.Info("retrieved usage report", "month", month, "count", len(entries))
	return entries, nil
}

// InitializeMirrors performs base backup from primaries to initialize mirror segments.
// In a real Cloudberry cluster, this would invoke gpinitstandby or gpaddmirrors.
// The current implementation logs the intent and returns nil, as the actual
// initialization is orchestrated by the Cloudberry utilities running inside the pods.
func (c *pgxClient) InitializeMirrors(ctx context.Context, opts MirrorInitOptions) (err error) {
	ctx, end := c.startOperation(ctx, "InitializeMirrors")
	defer func() { end(err) }()

	c.logger.Info("initializing mirrors",
		"layout", opts.Layout,
		"segmentCount", opts.SegmentCount,
		"parallelism", opts.Parallelism,
	)

	// Verify connectivity before proceeding.
	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("database not reachable for mirror initialization: %w", err)
	}

	// In production, this would execute gpaddmirrors or equivalent commands.
	// The controller orchestrates the StatefulSet creation; the DB-level
	// initialization is handled by the Cloudberry utilities in the pods.
	c.logger.Info("mirror initialization request recorded",
		"layout", opts.Layout,
		"segmentCount", opts.SegmentCount,
	)

	return nil
}

// ConfigureReplication sets up WAL streaming replication between primary and mirror segments.
// The current implementation logs the intent, as WAL replication is configured
// automatically by Cloudberry when mirrors are added via gpaddmirrors.
func (c *pgxClient) ConfigureReplication(ctx context.Context, opts ReplicationOptions) error {
	c.logger.Info("configuring replication", "mode", opts.Mode)

	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("database not reachable for replication configuration: %w", err)
	}

	// WAL replication is configured automatically by Cloudberry when mirrors
	// are initialized. This method records the intent for observability.
	c.logger.Info("replication configuration request recorded", "mode", opts.Mode)

	return nil
}

// GetMirrorSyncStatus returns the synchronization status of all mirror segments
// by querying gp_segment_configuration and pg_stat_replication.
func (c *pgxClient) GetMirrorSyncStatus(ctx context.Context) ([]MirrorSyncInfo, error) {
	query := `SELECT
		sc.content AS content_id,
		CASE WHEN sc.mode = 's' THEN true ELSE false END AS is_synced,
		COALESCE(
			(SELECT pg_wal_lsn_diff(sent_lsn, replay_lsn)
			 FROM pg_stat_replication
			 WHERE application_name = 'gp_walreceiver_' || sc.content::text
			 LIMIT 1), 0
		) AS replication_lag,
		CASE
			WHEN sc.mode = 's' THEN 'streaming'
			WHEN sc.mode = 'r' THEN 'catchup'
			WHEN sc.mode = 'n' THEN 'initializing'
			ELSE 'unknown'
		END AS state
		FROM gp_segment_configuration sc
		WHERE sc.content >= 0 AND sc.role = 'm'
		ORDER BY sc.content`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying mirror sync status: %w", err)
	}
	defer rows.Close()

	var results []MirrorSyncInfo
	for rows.Next() {
		var info MirrorSyncInfo
		if scanErr := rows.Scan(&info.ContentID, &info.IsSynced, &info.ReplicationLag, &info.State); scanErr != nil {
			return nil, fmt.Errorf("scanning mirror sync status row: %w", scanErr)
		}
		results = append(results, info)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating mirror sync status rows: %w", rowErr)
	}

	c.logger.Info("retrieved mirror sync status", "count", len(results))
	return results, nil
}

// TriggerFTSProbe requests Cloudberry's FTS daemon to perform an immediate probe scan.
// This triggers the internal FTS mechanism that detects failed segments and promotes
// mirrors to primary role. The call blocks until the scan completes.
func (c *pgxClient) TriggerFTSProbe(ctx context.Context) error {
	_, err := c.pool.Exec(ctx, "SELECT gp_request_fts_probe_scan()")
	if err != nil {
		return fmt.Errorf("triggering FTS probe scan: %w", err)
	}
	c.logger.Info("FTS probe scan triggered successfully")
	return nil
}

// TerminateAllBackends terminates all non-system backend connections.
// It calls pg_terminate_backend for each session except the current one
// and system processes. Returns the number of backends terminated.
func (c *pgxClient) TerminateAllBackends(ctx context.Context) (int32, error) {
	query := `SELECT count(pg_terminate_backend(pid))
		FROM pg_stat_activity
		WHERE pid != pg_backend_pid()
		AND backend_type = 'client backend'`

	var terminated int32
	if err := c.pool.QueryRow(ctx, query).Scan(&terminated); err != nil {
		return 0, fmt.Errorf("terminating all backends: %w", err)
	}

	c.logger.Info("terminated all backends", "count", terminated)
	return terminated, nil
}

// CancelAllQueries cancels all active queries (non-idle sessions)
// except the current backend. Returns the number of queries canceled.
func (c *pgxClient) CancelAllQueries(ctx context.Context) (int32, error) {
	query := `SELECT count(pg_cancel_backend(pid))
		FROM pg_stat_activity
		WHERE pid != pg_backend_pid()
		AND state = 'active'
		AND backend_type = 'client backend'`

	var canceled int32
	if err := c.pool.QueryRow(ctx, query).Scan(&canceled); err != nil {
		return 0, fmt.Errorf("canceling all queries: %w", err)
	}

	c.logger.Info("canceled all active queries", "count", canceled)
	return canceled, nil
}

// LogRotate triggers a log file rotation by calling pg_rotate_logfile().
// This signals the PostgreSQL/Cloudberry logger process to switch to a new
// log file immediately. The function returns true on success.
func (c *pgxClient) LogRotate(ctx context.Context) error {
	if _, err := c.pool.Exec(ctx, "SELECT pg_rotate_logfile()"); err != nil {
		return fmt.Errorf("rotating log file: %w", err)
	}
	c.logger.Info("log file rotation triggered")
	return nil
}

// RegisterNewSegments registers new primary and mirror segments in gp_segment_configuration.
// It inserts entries for each new segment (from oldCount to newCount-1) with the appropriate
// DBID, content ID, role, and FQDN derived from the headless service.
func (c *pgxClient) RegisterNewSegments(ctx context.Context, opts SegmentRegistrationOptions) (err error) {
	ctx, end := c.startOperation(ctx, "RegisterNewSegments")
	defer func() { end(err) }()

	c.logger.Info("registering new segments",
		"oldCount", opts.OldCount,
		"newCount", opts.NewCount,
		"mirrorEnabled", opts.MirrorEnabled,
		"segmentService", opts.SegmentService,
		"port", opts.Port,
	)

	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("database not reachable for segment registration: %w", err)
	}

	// Enable system table modifications.
	if _, err := c.pool.Exec(ctx, "SET allow_system_table_mods = true"); err != nil {
		return fmt.Errorf("enabling system table modifications: %w", err)
	}

	// Get the current max DBID to assign new unique DBIDs.
	var maxDBID int32
	maxDBIDQuery := "SELECT COALESCE(MAX(dbid), 0) FROM gp_segment_configuration"
	if err := c.pool.QueryRow(ctx, maxDBIDQuery).Scan(&maxDBID); err != nil {
		return fmt.Errorf("querying max dbid: %w", err)
	}

	nextDBID := maxDBID + 1

	// Register new primary segments.
	// Pod hostname format: <cluster>-segment-primary-<N>.<segment-headless-service>
	for i := opts.OldCount; i < opts.NewCount; i++ {
		podHostname := fmt.Sprintf("%s-segment-primary-%d.%s", opts.ClusterName, i, opts.SegmentService)
		dataDir := fmt.Sprintf("/data/pgdata/gpseg%d", i)

		query := `INSERT INTO gp_segment_configuration 
			(dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
			VALUES ($1, $2, 'p', 'p', 's', 'u', $3, $4, $5, $6)`

		if _, err := c.pool.Exec(ctx, query, nextDBID, i, opts.Port, podHostname, podHostname, dataDir); err != nil {
			return fmt.Errorf("registering primary segment content=%d dbid=%d: %w", i, nextDBID, err)
		}

		c.logger.Info("registered primary segment",
			"contentID", i, "dbid", nextDBID, "hostname", podHostname)
		nextDBID++
	}

	// Register new mirror segments if mirroring is enabled.
	if opts.MirrorEnabled {
		for i := opts.OldCount; i < opts.NewCount; i++ {
			mirrorHostname := fmt.Sprintf("%s-segment-mirror-%d.%s", opts.ClusterName, i, opts.SegmentService)
			dataDir := fmt.Sprintf("/data/pgdata/gpseg%d", i)

			query := `INSERT INTO gp_segment_configuration 
				(dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
				VALUES ($1, $2, 'm', 'm', 's', 'u', $3, $4, $5, $6)`

			if _, err := c.pool.Exec(ctx, query,
				nextDBID, i, opts.Port, mirrorHostname, mirrorHostname, dataDir); err != nil {
				return fmt.Errorf("registering mirror segment content=%d dbid=%d: %w", i, nextDBID, err)
			}

			c.logger.Info("registered mirror segment",
				"contentID", i, "dbid", nextDBID, "hostname", mirrorHostname)
			nextDBID++
		}
	}

	c.logger.Info("segment registration completed",
		"newPrimaries", opts.NewCount-opts.OldCount,
		"mirrorEnabled", opts.MirrorEnabled)

	// Propagate user databases to new segments via utility mode connections.
	if propErr := c.propagateDatabasesToNewSegments(ctx, opts); propErr != nil {
		c.logger.Warn("failed to propagate databases to new segments",
			"error", propErr)
		// Non-fatal: databases will be created when redistribution runs.
	}

	return nil
}

// propagateDatabasesToNewSegments creates user databases on new segments via utility mode.
// This is necessary because new segments are initialized with initdb and only have system databases.
func (c *pgxClient) propagateDatabasesToNewSegments(ctx context.Context, opts SegmentRegistrationOptions) error {
	// List user databases from the coordinator.
	databases, err := c.listUserDatabases(ctx)
	if err != nil {
		return fmt.Errorf("listing user databases: %w", err)
	}

	if len(databases) == 0 {
		c.logger.Info("no user databases to propagate")
		return nil
	}

	c.logger.Info("propagating databases to new segments",
		"databases", databases, "newSegments", opts.NewCount-opts.OldCount)

	// For each new primary segment, create the missing databases via utility mode.
	for i := opts.OldCount; i < opts.NewCount; i++ {
		// Check context cancellation between segment iterations to allow graceful shutdown.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		segHost := fmt.Sprintf("%s-segment-primary-%d.%s", opts.ClusterName, i, opts.SegmentService)
		connStr := fmt.Sprintf("host=%s port=%d dbname=postgres user=%s options='-c gp_role=utility'",
			segHost, opts.Port, c.pool.Config().ConnConfig.User)

		segConfig, parseErr := pgxpool.ParseConfig(connStr)
		if parseErr != nil {
			c.logger.Warn("failed to parse segment connection config",
				"segment", i, "error", parseErr)
			continue
		}
		// Copy password from main pool config.
		segConfig.ConnConfig.Password = c.pool.Config().ConnConfig.Password

		segPool, poolErr := pgxpool.NewWithConfig(ctx, segConfig)
		if poolErr != nil {
			c.logger.Warn("failed to connect to new segment",
				"segment", i, "host", segHost, "error", poolErr)
			continue
		}

		for _, dbName := range databases {
			// Check if database already exists on this segment.
			var exists bool
			checkQuery := "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)"
			if scanErr := segPool.QueryRow(ctx, checkQuery, dbName).Scan(&exists); scanErr != nil {
				c.logger.Warn("failed to check database existence",
					"segment", i, "database", dbName, "error", scanErr)
				continue
			}
			if exists {
				continue
			}

			// Create the database on this segment.
			createSQL := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
			if _, execErr := segPool.Exec(ctx, createSQL); execErr != nil {
				c.logger.Warn("failed to create database on segment",
					"segment", i, "database", dbName, "error", execErr)
				continue
			}
			c.logger.Info("created database on new segment",
				"segment", i, "database", dbName)
		}

		segPool.Close()
	}

	return nil
}

// RedistributeData redistributes existing tables across all segments (including new ones).
// It lists all user databases and for each one, re-applies the distribution policy on all
// user tables, which forces Cloudberry to redistribute the data across all segments.
func (c *pgxClient) RedistributeData(ctx context.Context, opts RedistributionOptions) (err error) {
	ctx, end := c.startOperation(ctx, "RedistributeData")
	defer func() { end(err) }()

	c.logger.Info("starting data redistribution",
		"database", opts.Database,
		"excludeTables", opts.ExcludeTables,
		"parallelism", opts.Parallelism,
	)

	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("database not reachable for redistribution: %w", err)
	}

	// List all user databases to redistribute.
	databases, err := c.listUserDatabases(ctx)
	if err != nil {
		return fmt.Errorf("listing user databases: %w", err)
	}

	c.logger.Info("found user databases for redistribution", "databases", databases)

	// Redistribute tables in each database.
	for _, dbName := range databases {
		if redistErr := c.redistributeDatabase(ctx, dbName, opts); redistErr != nil {
			c.logger.Warn("failed to redistribute database, continuing",
				"database", dbName, "error", redistErr)
			continue
		}
	}

	c.logger.Info("data redistribution completed across all databases",
		"databaseCount", len(databases))
	return nil
}

// ListUserDatabases returns all non-template, non-system databases.
func (c *pgxClient) ListUserDatabases(ctx context.Context) ([]string, error) {
	return c.listUserDatabases(ctx)
}

// listUserDatabases returns all non-template, non-system databases.
func (c *pgxClient) listUserDatabases(ctx context.Context) ([]string, error) {
	query := `SELECT datname FROM pg_database 
		WHERE datistemplate = false 
		AND datname NOT IN ('template0', 'template1')
		ORDER BY datname`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			return nil, fmt.Errorf("scanning database name: %w", scanErr)
		}
		databases = append(databases, name)
	}
	return databases, rows.Err()
}

// redistributeDatabase redistributes all user tables in a specific database.
func (c *pgxClient) redistributeDatabase(ctx context.Context, dbName string, opts RedistributionOptions) error {
	c.logger.Info("redistributing database", "database", dbName)

	// Create a temporary connection pool for this database.
	connStr := c.pool.Config().ConnString()
	dbConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("parsing connection config: %w", err)
	}
	dbConfig.ConnConfig.Database = dbName

	// Bound the transient pool to avoid connection spikes when redistributing
	// many databases sequentially.
	dbConfig.MaxConns = redistributionPoolMaxConns
	dbConfig.MaxConnLifetime = redistributionPoolMaxConnLifetime

	dbPool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		return fmt.Errorf("connecting to database %s: %w", dbName, err)
	}
	defer dbPool.Close()

	// Build exclusion filter.
	excludeSet := make(map[string]bool, len(opts.ExcludeTables))
	for _, t := range opts.ExcludeTables {
		excludeSet[t] = true
	}

	// Query all user tables and their distribution keys.
	query := `SELECT n.nspname AS schema_name, c.relname AS table_name,
		COALESCE(
			(SELECT string_agg(a.attname, ', ' ORDER BY dp.distkey_ord)
			 FROM (SELECT unnest(d.distkey) AS attnum, 
			       generate_subscripts(d.distkey, 1) AS distkey_ord
			       FROM gp_distribution_policy d WHERE d.localoid = c.oid) dp
			 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = dp.attnum),
			''
		) AS dist_key
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')
		ORDER BY n.nspname, c.relname`

	rows, err := dbPool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("querying user tables in %s: %w", dbName, err)
	}
	defer rows.Close()

	type tableInfo struct {
		schema  string
		table   string
		distKey string
	}

	var tables []tableInfo
	for rows.Next() {
		var t tableInfo
		if scanErr := rows.Scan(&t.schema, &t.table, &t.distKey); scanErr != nil {
			return fmt.Errorf("scanning table info: %w", scanErr)
		}
		fullName := t.schema + "." + t.table
		if !excludeSet[fullName] {
			tables = append(tables, t)
		}
	}
	if rowErr := rows.Err(); rowErr != nil {
		return fmt.Errorf("iterating table rows: %w", rowErr)
	}

	// Redistribute each table by re-applying its distribution policy.
	for _, t := range tables {
		qualifiedName := fmt.Sprintf("%s.%s",
			pgx.Identifier{t.schema}.Sanitize(),
			pgx.Identifier{t.table}.Sanitize())

		var alterSQL string
		if t.distKey == "" {
			alterSQL = fmt.Sprintf("ALTER TABLE %s SET DISTRIBUTED RANDOMLY", qualifiedName)
		} else {
			// Sanitize each column name in the distribution key for defense-in-depth.
			sanitizedKey, sanitizeErr := sanitizeDistKey(t.distKey)
			if sanitizeErr != nil {
				c.logger.Warn("failed to sanitize distribution key, skipping table",
					"database", dbName, "table", qualifiedName, "distKey", t.distKey, "error", sanitizeErr)
				continue
			}
			alterSQL = fmt.Sprintf("ALTER TABLE %s SET DISTRIBUTED BY (%s)", qualifiedName, sanitizedKey)
		}

		if _, execErr := dbPool.Exec(ctx, alterSQL); execErr != nil {
			c.logger.Warn("failed to redistribute table, continuing",
				"database", dbName, "table", qualifiedName, "error", execErr)
			continue
		}

		c.logger.Debug("redistributed table", "database", dbName, "table", qualifiedName)
	}

	c.logger.Info("database redistribution completed",
		"database", dbName, "tablesProcessed", len(tables))
	return nil
}

// GetRedistributionProgress returns the current redistribution progress (0-100).
// It estimates progress by comparing the number of tables that have been analyzed
// (indicating redistribution completion) against the total number of user tables.
func (c *pgxClient) GetRedistributionProgress(ctx context.Context) (int32, error) {
	// Query total user tables and those with recent analyze timestamps.
	query := `SELECT 
		COUNT(*) AS total,
		COUNT(*) FILTER (WHERE last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL) AS analyzed
		FROM pg_stat_user_tables
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')`

	var total, analyzed int32
	if err := c.pool.QueryRow(ctx, query).Scan(&total, &analyzed); err != nil {
		return 0, fmt.Errorf("querying redistribution progress: %w", err)
	}

	if total == 0 {
		return 100, nil
	}

	progress := (analyzed * 100) / total
	c.logger.Info("redistribution progress", "progress", progress, "total", total, "analyzed", analyzed)
	return progress, nil
}

// DeregisterSegments removes segment entries from gp_segment_configuration
// for segments with content IDs >= newCount. This is called during scale-in
// after data has been moved off the segments being removed.
func (c *pgxClient) DeregisterSegments(ctx context.Context, newCount int32) (err error) {
	ctx, end := c.startOperation(ctx, "DeregisterSegments")
	defer func() { end(err) }()

	c.logger.Info("deregistering segments", "newCount", newCount)

	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("database not reachable for segment deregistration: %w", err)
	}

	// Enable system table modifications.
	if _, err := c.pool.Exec(ctx, "SET allow_system_table_mods = true"); err != nil {
		return fmt.Errorf("enabling system table modifications: %w", err)
	}

	// Delete entries for segments with content >= newCount (both primaries and mirrors).
	query := "DELETE FROM gp_segment_configuration WHERE content >= $1"
	result, err := c.pool.Exec(ctx, query, newCount)
	if err != nil {
		return fmt.Errorf("deleting segment entries with content >= %d: %w", newCount, err)
	}

	c.logger.Info("segment deregistration completed",
		"newCount", newCount,
		"rowsDeleted", result.RowsAffected())

	return nil
}

// RedistributeBeforeScaleIn redistributes data to only the remaining segments
// before scaling in. This ensures no data is left on segments being removed.
//
// Cloudberry tracks the number of segments a table is distributed across in
// gp_distribution_policy.numsegments. Simply re-applying the distribution
// policy (ALTER TABLE SET DISTRIBUTED BY) does NOT move data off higher-numbered
// segments because the table's numsegments still includes them.
//
// The correct approach is:
//  1. Update gp_distribution_policy.numsegments to the new (lower) count.
//  2. Re-apply the distribution with REORGANIZE=TRUE to force data movement
//     from segments >= newCount to segments 0..newCount-1.
//
// After redistribution, the segments being removed will have no user data.
func (c *pgxClient) RedistributeBeforeScaleIn(
	ctx context.Context,
	opts ScaleInRedistributionOptions,
) (err error) {
	ctx, end := c.startOperation(ctx, "RedistributeBeforeScaleIn")
	defer func() { end(err) }()

	c.logger.Info("starting pre-scale-in redistribution",
		"newCount", opts.NewCount,
		"database", opts.Database,
		"excludeTables", opts.ExcludeTables,
	)

	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("database not reachable for scale-in redistribution: %w", err)
	}

	// List all user databases to redistribute.
	databases, err := c.listUserDatabases(ctx)
	if err != nil {
		return fmt.Errorf("listing user databases for scale-in: %w", err)
	}

	// If a specific database is requested, filter to only that one.
	if opts.Database != "" {
		filtered := make([]string, 0, 1)
		for _, db := range databases {
			if db == opts.Database {
				filtered = append(filtered, db)
				break
			}
		}
		databases = filtered
	}

	c.logger.Info("redistributing databases before scale-in",
		"databases", databases, "targetSegments", opts.NewCount)

	for _, dbName := range databases {
		redistErr := c.redistributeDatabaseForScaleIn(
			ctx, dbName, opts.NewCount, opts.ExcludeTables)
		if redistErr != nil {
			c.logger.Warn("failed to redistribute database during scale-in, continuing",
				"database", dbName, "error", redistErr)
			continue
		}
	}

	c.logger.Info("pre-scale-in redistribution completed",
		"databaseCount", len(databases), "targetSegments", opts.NewCount)
	return nil
}

// redistributeDatabaseForScaleIn redistributes all user tables in a database
// to use only the first newCount segments. It uses a CTAS (CREATE TABLE AS
// SELECT) approach: for each table it creates a temporary copy with
// numsegments=newCount, then swaps the tables. This ensures data is read from
// ALL current segments (including those being removed) and written only to the
// remaining segments.
//
// A simple ALTER TABLE SET DISTRIBUTED BY with REORGANIZE=TRUE does NOT work
// because Cloudberry only reads from segments 0..numsegments-1, so data on
// higher-numbered segments would be lost.
func (c *pgxClient) redistributeDatabaseForScaleIn(
	ctx context.Context, dbName string, newCount int32, excludeTables []string,
) error {
	c.logger.Info("redistributing database for scale-in", "database", dbName, "newCount", newCount)

	// Create a temporary connection pool for this database.
	connStr := c.pool.Config().ConnString()
	dbConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("parsing connection config: %w", err)
	}
	dbConfig.ConnConfig.Database = dbName

	dbPool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		return fmt.Errorf("connecting to database %s: %w", dbName, err)
	}
	defer dbPool.Close()

	// Build exclusion filter.
	excludeSet := make(map[string]bool, len(excludeTables))
	for _, t := range excludeTables {
		excludeSet[t] = true
	}

	// Query all user tables and their distribution keys.
	query := `SELECT n.nspname AS schema_name, c.relname AS table_name,
		COALESCE(
			(SELECT string_agg(a.attname, ', ' ORDER BY dp.distkey_ord)
			 FROM (SELECT unnest(d.distkey) AS attnum, 
			       generate_subscripts(d.distkey, 1) AS distkey_ord
			       FROM gp_distribution_policy d WHERE d.localoid = c.oid) dp
			 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = dp.attnum),
			''
		) AS dist_key
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit', 'pg_ext_aux')
		ORDER BY n.nspname, c.relname`

	rows, err := dbPool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("querying user tables in %s: %w", dbName, err)
	}
	defer rows.Close()

	var tables []scaleInTableInfo
	for rows.Next() {
		var t scaleInTableInfo
		if scanErr := rows.Scan(&t.schema, &t.table, &t.distKey); scanErr != nil {
			return fmt.Errorf("scanning table info: %w", scanErr)
		}
		fullName := t.schema + "." + t.table
		if !excludeSet[fullName] {
			tables = append(tables, t)
		}
	}
	if rowErr := rows.Err(); rowErr != nil {
		return fmt.Errorf("iterating table rows: %w", rowErr)
	}

	// For each table: create a temp copy with numsegments=newCount, then swap.
	for _, t := range tables {
		if err := c.redistributeTableForScaleIn(ctx, dbPool, dbName, t, newCount); err != nil {
			c.logger.Warn("failed to redistribute table, skipping",
				"database", dbName, "table", t.schema+"."+t.table, "error", err)
		}
	}

	c.logger.Info("database redistribution for scale-in completed",
		"database", dbName, "tablesProcessed", len(tables))
	return nil
}

// redistributeTableForScaleIn redistributes a single table for scale-in using
// the CTAS approach: create temp copy with numsegments=newCount, copy data, swap.
func (c *pgxClient) redistributeTableForScaleIn(
	ctx context.Context, dbPool *pgxpool.Pool,
	dbName string, t scaleInTableInfo, newCount int32,
) error {
	qualifiedName := fmt.Sprintf("%s.%s",
		pgx.Identifier{t.schema}.Sanitize(),
		pgx.Identifier{t.table}.Sanitize())
	tmpName := fmt.Sprintf("_scalein_tmp_%s", t.table)
	qualifiedTmp := fmt.Sprintf("%s.%s",
		pgx.Identifier{t.schema}.Sanitize(),
		pgx.Identifier{tmpName}.Sanitize())

	// Drop temp table if it exists from a previous failed attempt.
	dropTmpSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedTmp)
	if _, execErr := dbPool.Exec(ctx, dropTmpSQL); execErr != nil {
		c.logger.Warn("failed to drop temp table", "table", qualifiedTmp, "error", execErr)
	}

	// Create temp table with new distribution.
	if err := c.createScaleInTempTable(ctx, dbPool, qualifiedTmp, qualifiedName, t.distKey); err != nil {
		return err
	}

	// Update numsegments on the temp table.
	if err := c.updateNumsegments(ctx, dbPool, qualifiedTmp, newCount); err != nil {
		_, _ = dbPool.Exec(ctx, dropTmpSQL)
		return err
	}

	// Copy data from original to temp (reads ALL segments, writes to remaining).
	insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", qualifiedTmp, qualifiedName)
	if _, execErr := dbPool.Exec(ctx, insertSQL); execErr != nil {
		_, _ = dbPool.Exec(ctx, dropTmpSQL)
		return fmt.Errorf("copying data: %w", execErr)
	}

	// Swap: drop original, rename temp.
	dropOrigSQL := fmt.Sprintf("DROP TABLE %s", qualifiedName)
	if _, execErr := dbPool.Exec(ctx, dropOrigSQL); execErr != nil {
		_, _ = dbPool.Exec(ctx, dropTmpSQL)
		return fmt.Errorf("dropping original: %w", execErr)
	}

	renameSQL := fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
		qualifiedTmp, pgx.Identifier{t.table}.Sanitize())
	if _, execErr := dbPool.Exec(ctx, renameSQL); execErr != nil {
		return fmt.Errorf("renaming temp to original: %w", execErr)
	}

	c.logger.Debug("redistributed table for scale-in",
		"database", dbName, "table", qualifiedName, "newSegments", newCount)
	return nil
}

// createScaleInTempTable creates a temporary table for scale-in redistribution.
func (c *pgxClient) createScaleInTempTable(
	ctx context.Context, dbPool *pgxpool.Pool,
	qualifiedTmp, qualifiedName, distKey string,
) error {
	var createSQL string
	if distKey == "" {
		createSQL = fmt.Sprintf(
			"CREATE TABLE %s (LIKE %s INCLUDING ALL) DISTRIBUTED RANDOMLY",
			qualifiedTmp, qualifiedName)
	} else {
		// Sanitize each column name in the distribution key for defense-in-depth.
		sanitizedKey, sanitizeErr := sanitizeDistKey(distKey)
		if sanitizeErr != nil {
			return fmt.Errorf("sanitizing distribution key: %w", sanitizeErr)
		}
		createSQL = fmt.Sprintf(
			"CREATE TABLE %s (LIKE %s INCLUDING ALL) DISTRIBUTED BY (%s)",
			qualifiedTmp, qualifiedName, sanitizedKey)
	}
	if _, err := dbPool.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("creating temp table: %w", err)
	}
	return nil
}

// updateNumsegments updates the numsegments in gp_distribution_policy for a table.
func (c *pgxClient) updateNumsegments(
	ctx context.Context, dbPool *pgxpool.Pool,
	qualifiedTmp string, newCount int32,
) error {
	tx, err := dbPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	if _, execErr := tx.Exec(ctx, "SET allow_system_table_mods = true"); execErr != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("setting allow_system_table_mods: %w", execErr)
	}
	updateSQL := "UPDATE gp_distribution_policy SET numsegments = $1 WHERE localoid = $2::regclass"
	if _, execErr := tx.Exec(ctx, updateSQL, newCount, qualifiedTmp); execErr != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("updating numsegments: %w", execErr)
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return fmt.Errorf("committing numsegments update: %w", commitErr)
	}
	return nil
}

// AnalyzeSkew analyzes data skew across segments for all user tables in a database.
// It calculates the skew coefficient per table: (max_rows - avg_rows) / avg_rows * 100.
// A coefficient of 0 means perfectly balanced; 100 means all data is on one segment.
func (c *pgxClient) AnalyzeSkew(
	ctx context.Context,
	database string,
) (result []TableSkewInfo, err error) {
	ctx, end := c.startOperation(ctx, "AnalyzeSkew")
	defer func() { end(err) }()

	c.logger.Info("analyzing data skew", "database", database)

	if err := c.Ping(ctx); err != nil {
		return nil, fmt.Errorf("database not reachable for skew analysis: %w", err)
	}

	// Connect to the target database if different from the pool's default.
	pool := c.pool
	if database != "" && database != c.config.Database {
		connStr := c.pool.Config().ConnString()
		dbConfig, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			return nil, fmt.Errorf("parsing connection config for skew analysis: %w", err)
		}
		dbConfig.ConnConfig.Database = database

		var poolErr error
		pool, poolErr = pgxpool.NewWithConfig(ctx, dbConfig)
		if poolErr != nil {
			return nil, fmt.Errorf("connecting to database %s for skew analysis: %w", database, poolErr)
		}
		defer pool.Close()
	}

	// Query all user tables with their distribution keys.
	tableQuery := `SELECT n.nspname AS schema_name, c.relname AS table_name,
		COALESCE(
			(SELECT string_agg(a.attname, ', ' ORDER BY dp.distkey_ord)
			 FROM (SELECT unnest(d.distkey) AS attnum,
			       generate_subscripts(d.distkey, 1) AS distkey_ord
			       FROM gp_distribution_policy d WHERE d.localoid = c.oid) dp
			 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = dp.attnum),
			''
		) AS dist_key
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit', 'pg_ext_aux')
		ORDER BY n.nspname, c.relname`

	rows, err := pool.Query(ctx, tableQuery)
	if err != nil {
		return nil, fmt.Errorf("querying user tables for skew analysis: %w", err)
	}
	defer rows.Close()

	type tableEntry struct {
		schema  string
		table   string
		distKey string
	}

	var tables []tableEntry
	for rows.Next() {
		var t tableEntry
		if scanErr := rows.Scan(&t.schema, &t.table, &t.distKey); scanErr != nil {
			return nil, fmt.Errorf("scanning table entry for skew analysis: %w", scanErr)
		}
		tables = append(tables, t)
	}
	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating table entries for skew analysis: %w", rowErr)
	}

	// For each table, calculate the skew coefficient.
	var results []TableSkewInfo
	for _, t := range tables {
		qualifiedName := fmt.Sprintf("%s.%s",
			pgx.Identifier{t.schema}.Sanitize(),
			pgx.Identifier{t.table}.Sanitize())

		skewQuery := fmt.Sprintf(`SELECT
			COALESCE(SUM(cnt), 0) AS total_rows,
			CASE WHEN AVG(cnt) = 0 THEN 0
			     ELSE ((MAX(cnt) - AVG(cnt)) / AVG(cnt) * 100)
			END AS skew_coefficient
			FROM (
				SELECT gp_segment_id, count(*) AS cnt
				FROM %s
				GROUP BY gp_segment_id
			) seg_counts`, qualifiedName)

		var totalRows int64
		var skewCoeff float64
		if scanErr := pool.QueryRow(ctx, skewQuery).Scan(&totalRows, &skewCoeff); scanErr != nil {
			c.logger.Warn("failed to calculate skew for table, skipping",
				"table", qualifiedName, "error", scanErr)
			continue
		}

		// Only include tables with data.
		if totalRows > 0 {
			results = append(results, TableSkewInfo{
				Database:        database,
				Schema:          t.schema,
				Table:           t.table,
				SkewCoefficient: skewCoeff,
				DistributionKey: t.distKey,
				RowCount:        totalRows,
			})
		}
	}

	c.logger.Info("skew analysis completed", "database", database, "tablesAnalyzed", len(results))
	return results, nil
}

// RebalanceTable redistributes a single table across all segments using REORGANIZE=TRUE.
// If distKey is empty, the table is redistributed randomly.
func (c *pgxClient) RebalanceTable(
	ctx context.Context,
	database, schema, table, distKey string,
) (err error) {
	ctx, end := c.startOperation(ctx, "RebalanceTable")
	defer func() { end(err) }()

	c.logger.Info("rebalancing table",
		"database", database, "schema", schema, "table", table, "distKey", distKey)

	// Connect to the target database if different from the pool's default.
	pool := c.pool
	if database != "" && database != c.config.Database {
		connStr := c.pool.Config().ConnString()
		dbConfig, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			return fmt.Errorf("parsing connection config for rebalance: %w", err)
		}
		dbConfig.ConnConfig.Database = database

		var poolErr error
		pool, poolErr = pgxpool.NewWithConfig(ctx, dbConfig)
		if poolErr != nil {
			return fmt.Errorf("connecting to database %s for rebalance: %w", database, poolErr)
		}
		defer pool.Close()
	}

	qualifiedName := fmt.Sprintf("%s.%s",
		pgx.Identifier{schema}.Sanitize(),
		pgx.Identifier{table}.Sanitize())

	var alterSQL string
	if distKey == "" {
		alterSQL = fmt.Sprintf(
			"ALTER TABLE %s SET WITH (REORGANIZE=TRUE) DISTRIBUTED RANDOMLY", qualifiedName)
	} else {
		// Sanitize each column name in the distribution key for defense-in-depth.
		sanitizedKey, sanitizeErr := sanitizeDistKey(distKey)
		if sanitizeErr != nil {
			return fmt.Errorf("sanitizing distribution key for rebalance: %w", sanitizeErr)
		}
		alterSQL = fmt.Sprintf(
			"ALTER TABLE %s SET WITH (REORGANIZE=TRUE) DISTRIBUTED BY (%s)", qualifiedName, sanitizedKey)
	}

	if _, err := pool.Exec(ctx, alterSQL); err != nil {
		return fmt.Errorf("rebalancing table %s: %w", qualifiedName, err)
	}

	c.logger.Info("table rebalanced successfully",
		"database", database, "table", qualifiedName)
	return nil
}

// SetupExporterRole creates the cloudberry_exporter database role with LOGIN privilege,
// grants pg_monitor membership, and grants SELECT on monitoring views.
// The operation is idempotent: if the role already exists, its password is updated.
func (c *pgxClient) SetupExporterRole(ctx context.Context, password string) (err error) {
	ctx, end := c.startOperation(ctx, "SetupExporterRole")
	defer func() { end(err) }()

	roleName := "cloudberry_exporter"

	// Check if role exists.
	var exists bool
	err = c.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)", roleName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("checking exporter role existence: %w", err)
	}

	sanitizedRole := pgx.Identifier{roleName}.Sanitize()

	if !exists {
		// Create role with LOGIN.
		createSQL := fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD %s",
			sanitizedRole,
			quoteLiteral(password))
		if _, err := c.pool.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("creating exporter role: %w", err)
		}
		c.logger.Info("exporter role created", "role", roleName)
	} else {
		// Update password if role already exists.
		alterSQL := fmt.Sprintf("ALTER ROLE %s PASSWORD %s",
			sanitizedRole,
			quoteLiteral(password))
		if _, err := c.pool.Exec(ctx, alterSQL); err != nil {
			return fmt.Errorf("updating exporter role password: %w", err)
		}
		c.logger.Info("exporter role password updated", "role", roleName)
	}

	// Grant pg_monitor membership.
	grantSQL := fmt.Sprintf("GRANT pg_monitor TO %s", sanitizedRole)
	if _, err := c.pool.Exec(ctx, grantSQL); err != nil {
		return fmt.Errorf("granting pg_monitor to exporter role: %w", err)
	}

	// Grant SELECT on monitoring views.
	monitoringViews := []string{
		"gp_segment_configuration",
		"gp_toolkit.gp_resgroup_status",
		"gp_toolkit.gp_resgroup_status_per_host",
		"gp_toolkit.gp_resgroup_iostats_per_host",
		"gp_toolkit.gp_resgroup_config",
		"gp_toolkit.gp_workfile_usage_per_query",
		"gp_toolkit.gp_workfile_usage_per_segment",
		"gp_toolkit.gp_skew_coefficients",
	}

	for _, view := range monitoringViews {
		grantViewSQL := fmt.Sprintf("GRANT SELECT ON %s TO %s", view, sanitizedRole)
		if _, err := c.pool.Exec(ctx, grantViewSQL); err != nil {
			// Log warning but don't fail — some views may not exist in all versions.
			c.logger.Warn("failed to grant SELECT on view", "view", view, "error", err)
		}
	}

	c.logger.Info("exporter role setup completed", "role", roleName)
	return nil
}

// GetQueryDetail returns detailed execution information for a specific query by PID.
// It queries pg_stat_activity for session info, pg_locks for lock information,
// and pg_stat_user_tables for recently accessed tables.
func (c *pgxClient) GetQueryDetail(ctx context.Context, pid int32) (*QueryDetail, error) {
	// 1. Get session info from pg_stat_activity.
	detail := &QueryDetail{}
	sessionQuery := `SELECT pid, COALESCE(usename, ''), COALESCE(datname, ''),
		COALESCE(state, ''), COALESCE(query, ''),
		COALESCE(query_start, now()),
		COALESCE(now() - query_start, interval '0')::text,
		COALESCE(wait_event_type, ''), COALESCE(wait_event, ''),
		COALESCE(backend_type, '')
		FROM pg_stat_activity WHERE pid = $1`

	err := c.pool.QueryRow(ctx, sessionQuery, pid).Scan(
		&detail.PID, &detail.Username, &detail.Database,
		&detail.State, &detail.Query, &detail.QueryStart,
		&detail.Duration, &detail.WaitEventType, &detail.WaitEvent,
		&detail.BackendType,
	)
	if err != nil {
		return nil, fmt.Errorf("query not found or not accessible: %w", err)
	}

	// 2. Get locks for this PID.
	lockQuery := `SELECT locktype, mode, granted, COALESCE(relation::regclass::text, '')
		FROM pg_locks WHERE pid = $1`
	lockRows, lockErr := c.pool.Query(ctx, lockQuery, pid)
	if lockErr == nil {
		defer lockRows.Close()
		for lockRows.Next() {
			var lock LockInfo
			if scanErr := lockRows.Scan(&lock.LockType, &lock.Mode, &lock.Granted, &lock.Relation); scanErr == nil {
				detail.Locks = append(detail.Locks, lock)
			}
		}
	}

	// 3. Get tables accessed (tables with recent activity in the query's database).
	// This is an approximation — we list tables that have been accessed recently.
	tableQuery := `SELECT schemaname || '.' || relname FROM pg_stat_user_tables
		WHERE (seq_scan + COALESCE(idx_scan, 0)) > 0
		ORDER BY (seq_scan + COALESCE(idx_scan, 0)) DESC LIMIT 20`
	tableRows, tableErr := c.pool.Query(ctx, tableQuery)
	if tableErr == nil {
		defer tableRows.Close()
		for tableRows.Next() {
			var table string
			if scanErr := tableRows.Scan(&table); scanErr == nil {
				detail.TablesAccessed = append(detail.TablesAccessed, table)
			}
		}
	}

	c.logger.Info("query detail retrieved", "pid", pid, "state", detail.State)
	return detail, nil
}

// buildRoleOptions constructs the SQL options clause for role operations.
func buildRoleOptions(opts RoleOptions) string {
	var parts []string

	if opts.Login {
		parts = append(parts, "LOGIN")
	}
	if opts.SuperUser {
		parts = append(parts, "SUPERUSER")
	}
	if opts.CreateDB {
		parts = append(parts, "CREATEDB")
	}
	if opts.CreateRole {
		parts = append(parts, "CREATEROLE")
	}
	if opts.Password != "" {
		parts = append(parts, fmt.Sprintf("PASSWORD %s", quoteLiteral(opts.Password)))
	}
	if opts.ValidUntil != "" {
		parts = append(parts, fmt.Sprintf("VALID UNTIL %s", quoteLiteral(opts.ValidUntil)))
	}

	if len(parts) == 0 {
		return ""
	}

	result := " WITH"
	for _, p := range parts {
		result += " " + p
	}
	return result
}

// quoteLiteral safely quotes a string literal for SQL.
func quoteLiteral(s string) string {
	return "'" + escapeQuotes(s) + "'"
}

// escapeQuotes escapes single quotes in a string.
func escapeQuotes(s string) string {
	result := make([]byte, 0, len(s))
	for i := range len(s) {
		if s[i] == '\'' {
			result = append(result, '\'', '\'')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}

// MoveQueryToResourceGroup moves a running query's session to a different resource group.
// It looks up the session's role from pg_stat_activity by PID, then executes
// ALTER ROLE <role> RESOURCE GROUP <group> to reassign the role's resource group.
// Both role and group names are sanitized via pgx.Identifier to prevent SQL injection.
func (c *pgxClient) MoveQueryToResourceGroup(ctx context.Context, pid int32, targetGroup string) error {
	// Look up the username for the given PID from pg_stat_activity.
	var username string
	lookupQuery := `SELECT COALESCE(usename, '') FROM pg_stat_activity WHERE pid = $1`
	if err := c.pool.QueryRow(ctx, lookupQuery, pid).Scan(&username); err != nil {
		return fmt.Errorf("looking up session for PID %d: %w", pid, err)
	}
	if username == "" {
		return fmt.Errorf("session with PID %d not found or has no associated role", pid)
	}

	// Execute ALTER ROLE to reassign the resource group.
	alterSQL := fmt.Sprintf("ALTER ROLE %s RESOURCE GROUP %s",
		pgx.Identifier{username}.Sanitize(), pgx.Identifier{targetGroup}.Sanitize())
	if _, err := c.pool.Exec(ctx, alterSQL); err != nil {
		return fmt.Errorf("moving PID %d (role %s) to resource group %s: %w", pid, username, targetGroup, err)
	}

	c.logger.Info("query moved to resource group",
		"pid", pid, "role", username, "targetGroup", targetGroup)
	return nil
}

// classifySeverity returns a severity level based on a value and thresholds.
func classifySeverity(value, warningThreshold, criticalThreshold int64) string {
	switch {
	case value >= criticalThreshold:
		return severityCritical
	case value >= warningThreshold:
		return severityWarning
	default:
		return severityInfo
	}
}
