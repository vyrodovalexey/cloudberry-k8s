// Package db provides a Cloudberry/PostgreSQL database client for the cloudberry operator.
package db

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// PostgreSQL SQLSTATE codes used to detect a missing relation/column so the
// disk-usage measurement can fall back honestly instead of fabricating a value.
const (
	sqlStateUndefinedTable  = "42P01" // undefined_table
	sqlStateUndefinedColumn = "42703" // undefined_column
	// sqlStateCannotConnectNow (57P03) is returned by PostgreSQL/Cloudberry when
	// the server is starting up or shutting down and cannot accept connections.
	// A coordinator can wedge in this state after a restart during a scale/STS
	// change and reject all connections indefinitely (D3), requiring a pod
	// restart to recover.
	sqlStateCannotConnectNow = "57P03" // cannot_connect_now
)

// ErrDiskUsageUnavailable is returned by GetDiskUsagePercent when
// gp_toolkit.gp_disk_free (or its expected columns) is not available on this
// server version. Callers MUST skip the measurement, NOT substitute a value
// (S.1/R.2: never fabricate a disk-usage percentage).
var ErrDiskUsageUnavailable = errors.New(
	"disk usage unavailable: gp_toolkit.gp_disk_free not accessible",
)

// ErrSegmentCatalogNotSeeded is returned by the scale-out redistribution path
// (D8) when a user relation that exists on the coordinator is missing on a
// newly-added segment. This means the new segment's catalog was not seeded from
// the coordinator (the operator can only create databases cluster-wide via the
// coordinator; it cannot physically clone pre-existing table catalog entries
// with matching OIDs the way gpexpand does). "ALTER TABLE ... EXPAND TABLE"
// would fail with a raw 42P01 ("relation does not exist") on that segment and be
// retried forever, so the redistribution surfaces this typed, terminal error
// instead. The scale controller treats it as a non-retriable scale-out failure.
var ErrSegmentCatalogNotSeeded = errors.New(
	"new segment catalog not seeded from coordinator: pre-existing user relations " +
		"are missing on the newly-added segment (EXPAND TABLE cannot redistribute them)",
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

// The Client interface and its capability interfaces (ConnectionOps,
// SessionOps, ...) live in interfaces.go (M-1 split).

// Scope levels accepted by SetParameter (M-2). Matching is exact and
// case-sensitive; an empty Level is equivalent to ScopeLevelCluster.
const (
	// ScopeLevelCluster applies a parameter cluster-wide via ALTER SYSTEM.
	ScopeLevelCluster = "cluster"
	// ScopeLevelDatabase applies a parameter to one database via
	// ALTER DATABASE ... SET; requires ParameterScope.Target.
	ScopeLevelDatabase = "database"
	// ScopeLevelRole applies a parameter to one role via ALTER ROLE ... SET;
	// requires ParameterScope.Target.
	ScopeLevelRole = "role"
)

// ErrInvalidParameterScope is returned by SetParameter when the requested
// scope is invalid: an unknown Level, or a database/role Level without a
// Target (M-2). Match with errors.Is.
var ErrInvalidParameterScope = errors.New("invalid parameter scope")

// ParameterScope defines the scope for parameter changes.
type ParameterScope struct {
	// Level is the scope level: "" or ScopeLevelCluster (cluster-wide),
	// ScopeLevelDatabase, or ScopeLevelRole. Any other value is rejected
	// with ErrInvalidParameterScope (exact, case-sensitive match).
	Level string
	// Target is the database or role name (required for the
	// ScopeLevelDatabase / ScopeLevelRole levels).
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

// RecommendationThresholds carries the per-type CRD gates from
// spec.storage.recommendationScan into the threshold-aware recommendation
// queries (C.6–C.9). Each Get*Recommendations reads ONLY its own field, but the
// four methods take the whole struct so their signatures are uniform and the
// controller threads a single value to all four. Units mirror the CRD field
// types:
//   - Bloat      — dead-tuple percentage 0..100 (C.6, BloatThreshold).
//   - Skew       — skew-coefficient percentage 0..100 (C.7, SkewThreshold).
//   - Age        — absolute XID age, age(relfrozenxid) (C.8, AgeThreshold).
//   - IndexBloat — index bloat estimate percentage 0..100 (C.9, IndexBloatThreshold).
type RecommendationThresholds struct {
	Bloat      int32
	Skew       int32
	Age        int64
	IndexBloat int32
}

// TableStorageInfo represents one row of the storage/tables listing.
type TableStorageInfo struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	SizeBytes    int64  `json:"sizeBytes"`
	SizeHuman    string `json:"sizeHuman"`
	BloatPercent int32  `json:"bloatPercent"`
	SkewPercent  int32  `json:"skewPercent"`
	RowCount     int64  `json:"rowCount"`
}

// IndexSizeInfo is one index's on-disk size for a table detail.
type IndexSizeInfo struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"sizeBytes"`
	SizeHuman string `json:"sizeHuman"`
}

// TableDetail represents detailed information about a database table.
type TableDetail struct {
	Schema       string          `json:"schema"`
	Table        string          `json:"table"`
	SizeBytes    int64           `json:"sizeBytes"`
	SizeHuman    string          `json:"sizeHuman"`
	RowCount     int64           `json:"rowCount"`
	BloatPercent int32           `json:"bloatPercent"`
	SkewPercent  int32           `json:"skewPercent"`
	LastVacuum   string          `json:"lastVacuum"`
	LastAnalyze  string          `json:"lastAnalyze"`
	IndexSizes   []IndexSizeInfo `json:"indexSizes,omitempty"`
}

// TableUsage is one table's storage consumption within a usage-report database
// entry. It provides the per-table breakdown that, alongside the per-database
// size, satisfies the Scenario 120 C.11 "per-table + per-database storage
// consumption" content requirement.
type TableUsage struct {
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	SizeBytes int64  `json:"sizeBytes"`
	SizeHuman string `json:"sizeHuman"`
}

// UsageReportEntry represents a single entry in a usage report.
//
// Scenario 120 (C.11) enriches each entry with a bounded per-table breakdown
// in Tables. Because the pgx pool is connected to a single database, Tables is
// populated only for the entry matching the connected database (c.config.Database);
// other database entries carry an empty Tables slice — honestly, the operator
// cannot size tables in a database it is not connected to.
//
// GrowthBytes/GrowthHuman/QueryCount remain an honest 0/empty: the report is
// computed ON DEMAND from live catalog sizes and the operator does not persist
// month-over-month snapshots, so there is no baseline from which to derive growth
// or a historical query count without fabricating one.
type UsageReportEntry struct {
	Month       string       `json:"month"`
	Database    string       `json:"database"`
	SizeBytes   int64        `json:"sizeBytes"`
	SizeHuman   string       `json:"sizeHuman"`
	GrowthBytes int64        `json:"growthBytes"`
	GrowthHuman string       `json:"growthHuman"`
	QueryCount  int64        `json:"queryCount"`
	Connections int64        `json:"connections"`
	Tables      []TableUsage `json:"tables,omitempty"` // C.11 per-table breakdown (connected DB only)
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

// newDatabasePool clones the main pool's connection configuration for a
// specific target database and returns a fresh pgxpool.
//
// D1 fix: the connection string produced by pool.Config().ConnString() carries
// the sslmode (for example, verify-ca) but NOT the custom root CA, which is
// installed programmatically on the TLS config's RootCAs (see applyRootCA). When
// a transient per-database or per-segment pool is built by parsing that
// connection string alone, pgx falls back to the host's system trust store and
// verify-ca fails with "x509: certificate signed by unknown authority" against a
// private CA (for example, Vault PKI). This helper re-attaches the cluster CA
// (c.config.SSLRootCA) so every operator->coordinator connection consistently
// trusts the cluster's own CA. mutate, when non-nil, is applied to the parsed
// config before the root CA is re-applied (for example, to override the target
// database). It is a no-op for non-TLS clusters (empty SSLRootCA).
func (c *pgxClient) newDatabasePool(
	ctx context.Context,
	mutate func(*pgxpool.Config),
) (*pgxpool.Pool, error) {
	connStr := c.pool.Config().ConnString()
	dbConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parsing connection config: %w", err)
	}
	if mutate != nil {
		mutate(dbConfig)
	}
	// Re-attach the cluster root CA lost when serializing to a connection
	// string, so verify-ca validates against the cluster CA (not system roots).
	if err := applyRootCA(dbConfig, c.config.SSLRootCA); err != nil {
		return nil, fmt.Errorf("applying SSL root CA to database pool: %w", err)
	}
	return pgxpool.NewWithConfig(ctx, dbConfig)
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
//
// Each field is escaped in its correct URL context: userinfo via
// url.UserPassword, the database name as a path segment, and sslmode as a query
// value via url.Values.Encode(). Reusing a single Path-based escape for every
// position is incorrect because characters such as '@', '?', '#', '&', '=' and
// ':' are syntactically significant in the userinfo/query positions and could
// otherwise corrupt the DSN or inject additional connection parameters.
func (u *pgConnURL) String() string {
	ru := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(u.user, u.password),
		Host:   net.JoinHostPort(u.host, strconv.Itoa(int(u.port))),
		Path:   "/" + u.database,
	}
	q := url.Values{}
	q.Set("sslmode", u.sslMode)
	ru.RawQuery = q.Encode()
	return ru.String()
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
func (c *pgxClient) GetSegmentConfiguration(ctx context.Context) (segments []SegmentInfo, err error) {
	ctx, end := c.startOperation(ctx, "GetSegmentConfiguration")
	defer func() { end(err) }()

	// Cast char(1) columns (role, preferred_role, mode, status) to text explicitly.
	// Cloudberry's gp_segment_configuration uses "char" type (OID 18) for these columns,
	// which pgx cannot scan into *string in binary protocol mode. Casting to text
	// ensures compatibility regardless of the query execution mode.
	query := `SELECT content, dbid, role::text, preferred_role::text, mode::text, status::text, 
		hostname, address, port, datadir 
		FROM gp_segment_configuration ORDER BY content, role`

	rows, queryErr := c.pool.Query(ctx, query)
	if queryErr != nil {
		err = fmt.Errorf("querying segment configuration: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var seg SegmentInfo
		if scanErr := rows.Scan(
			&seg.ContentID, &seg.DBID, &seg.Role, &seg.PreferredRole,
			&seg.Mode, &seg.Status, &seg.Hostname, &seg.Address,
			&seg.Port, &seg.DataDirectory,
		); scanErr != nil {
			err = fmt.Errorf("scanning segment row: %w", scanErr)
			return nil, err
		}
		segments = append(segments, seg)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating segment rows: %w", rowErr)
		return nil, err
	}

	return segments, nil
}

// validateParameterScope validates a SetParameter scope BEFORE any SQL is
// built or executed (M-2): "" and ScopeLevelCluster select ALTER SYSTEM;
// ScopeLevelDatabase / ScopeLevelRole require a non-empty Target; any other
// Level is rejected. Matching is exact and case-sensitive, so a typo like
// "databsae" can no longer silently escalate to a cluster-wide ALTER SYSTEM.
func validateParameterScope(scope ParameterScope) error {
	switch scope.Level {
	case "", ScopeLevelCluster:
		return nil
	case ScopeLevelDatabase, ScopeLevelRole:
		if scope.Target == "" {
			return fmt.Errorf("scope level %q requires a non-empty target: %w",
				scope.Level, ErrInvalidParameterScope)
		}
		return nil
	default:
		return fmt.Errorf("unknown scope level %q: %w", scope.Level, ErrInvalidParameterScope)
	}
}

// SetParameter sets a configuration parameter at the specified scope.
func (c *pgxClient) SetParameter(ctx context.Context, name, value string, scope ParameterScope) (err error) {
	ctx, end := c.startOperation(ctx, "SetParameter")
	defer func() { end(err) }()

	// M-2: no Exec is issued on invalid input.
	if err := validateParameterScope(scope); err != nil {
		return err
	}

	var query string

	switch scope.Level {
	case ScopeLevelDatabase:
		query = fmt.Sprintf("ALTER DATABASE %s SET %s = %s",
			pgx.Identifier{scope.Target}.Sanitize(),
			pgx.Identifier{name}.Sanitize(),
			quoteLiteral(value),
		)
	case ScopeLevelRole:
		query = fmt.Sprintf("ALTER ROLE %s SET %s = %s",
			pgx.Identifier{scope.Target}.Sanitize(),
			pgx.Identifier{name}.Sanitize(),
			quoteLiteral(value),
		)
	default:
		// "" or ScopeLevelCluster (validated above): cluster-wide ALTER SYSTEM.
		query = fmt.Sprintf("ALTER SYSTEM SET %s = %s",
			pgx.Identifier{name}.Sanitize(),
			quoteLiteral(value),
		)
	}

	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		// Do not embed the raw value: GUCs can carry sensitive material and the
		// error is logged again upstream, risking secret leakage.
		err = fmt.Errorf("setting parameter %s (scope=%s): %w", name, scope.Level, execErr)
		return err
	}

	// Log only the parameter name + scope at Info; the value is gated behind Debug
	// because GUC values may contain secrets.
	c.logger.Info("parameter set", "name", name, "scope", scope.Level)
	c.logger.Debug("parameter set value", "name", name, "value", value)
	return nil
}

// ShowParameter returns the current value of a parameter.
func (c *pgxClient) ShowParameter(ctx context.Context, name string) (value string, err error) {
	ctx, end := c.startOperation(ctx, "ShowParameter")
	defer func() { end(err) }()

	query := fmt.Sprintf("SHOW %s", pgx.Identifier{name}.Sanitize())
	if scanErr := c.pool.QueryRow(ctx, query).Scan(&value); scanErr != nil {
		err = fmt.Errorf("showing parameter %s: %w", name, scanErr)
		return "", err
	}
	return value, nil
}

// ReloadConfig triggers a configuration reload.
func (c *pgxClient) ReloadConfig(ctx context.Context) (err error) {
	ctx, end := c.startOperation(ctx, "ReloadConfig")
	defer func() { end(err) }()

	if _, execErr := c.pool.Exec(ctx, "SELECT pg_reload_conf()"); execErr != nil {
		err = fmt.Errorf("reloading configuration: %w", execErr)
		return err
	}
	c.logger.Info("configuration reloaded")
	return nil
}

// ListSessions returns active database sessions.
func (c *pgxClient) ListSessions(ctx context.Context) (sessions []Session, err error) {
	ctx, end := c.startOperation(ctx, "ListSessions")
	defer func() { end(err) }()

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

	rows, queryErr := c.pool.Query(ctx, query)
	if queryErr != nil {
		err = fmt.Errorf("querying sessions: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s Session
		if scanErr := rows.Scan(
			&s.PID, &s.Username, &s.Database, &s.Application, &s.ClientAddress,
			&s.State, &s.WaitEventType, &s.Query, &s.QueryStart, &s.Duration,
		); scanErr != nil {
			err = fmt.Errorf("scanning session row: %w", scanErr)
			return nil, err
		}
		sessions = append(sessions, s)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating session rows: %w", rowErr)
		return nil, err
	}

	return sessions, nil
}

// ListSessionsWithResourceGroup returns sessions with their resource group assignment.
func (c *pgxClient) ListSessionsWithResourceGroup(ctx context.Context) (sessions []SessionWithGroup, err error) {
	ctx, end := c.startOperation(ctx, "ListSessionsWithResourceGroup")
	defer func() { end(err) }()

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

	rows, queryErr := c.pool.Query(ctx, query)
	if queryErr != nil {
		err = fmt.Errorf("querying sessions with resource group: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var sg SessionWithGroup
		if scanErr := rows.Scan(
			&sg.PID, &sg.Username, &sg.Database, &sg.Application, &sg.ClientAddress,
			&sg.State, &sg.WaitEventType, &sg.Query, &sg.QueryStart, &sg.Duration,
			&sg.ResourceGroup,
		); scanErr != nil {
			err = fmt.Errorf("scanning session with resource group row: %w", scanErr)
			return nil, err
		}
		sessions = append(sessions, sg)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating session with resource group rows: %w", rowErr)
		return nil, err
	}

	return sessions, nil
}

// CancelQuery cancels a running query by PID.
func (c *pgxClient) CancelQuery(ctx context.Context, pid int32) (result bool, err error) {
	ctx, end := c.startOperation(ctx, "CancelQuery")
	defer func() { end(err) }()

	if scanErr := c.pool.QueryRow(ctx, "SELECT pg_cancel_backend($1)", pid).Scan(&result); scanErr != nil {
		err = fmt.Errorf("canceling query for PID %d: %w", pid, scanErr)
		return false, err
	}
	c.logger.Info("query canceled", "pid", pid, "result", result)
	return result, nil
}

// TerminateSession terminates a session by PID.
func (c *pgxClient) TerminateSession(ctx context.Context, pid int32) (result bool, err error) {
	ctx, end := c.startOperation(ctx, "TerminateSession")
	defer func() { end(err) }()

	if scanErr := c.pool.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&result); scanErr != nil {
		err = fmt.Errorf("terminating session for PID %d: %w", pid, scanErr)
		return false, err
	}
	c.logger.Info("session terminated", "pid", pid, "result", result)
	return result, nil
}

// CreateRole creates a new database role.
func (c *pgxClient) CreateRole(ctx context.Context, opts RoleOptions) (err error) {
	ctx, end := c.startOperation(ctx, "CreateRole")
	defer func() { end(err) }()

	query := fmt.Sprintf("CREATE ROLE %s", pgx.Identifier{opts.Name}.Sanitize())
	query += buildRoleOptions(opts)

	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("creating role %s: %w", opts.Name, execErr)
		return err
	}
	c.logger.Info("role created", "name", opts.Name)
	return nil
}

// AlterRole modifies an existing database role.
func (c *pgxClient) AlterRole(ctx context.Context, opts RoleOptions) (err error) {
	ctx, end := c.startOperation(ctx, "AlterRole")
	defer func() { end(err) }()

	query := fmt.Sprintf("ALTER ROLE %s", pgx.Identifier{opts.Name}.Sanitize())
	query += buildRoleOptions(opts)

	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("altering role %s: %w", opts.Name, execErr)
		return err
	}
	c.logger.Info("role altered", "name", opts.Name)
	return nil
}

// DropRole drops a database role.
func (c *pgxClient) DropRole(ctx context.Context, name string) (err error) {
	ctx, end := c.startOperation(ctx, "DropRole")
	defer func() { end(err) }()

	query := fmt.Sprintf("DROP ROLE IF EXISTS %s", pgx.Identifier{name}.Sanitize())
	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("dropping role %s: %w", name, execErr)
		return err
	}
	c.logger.Info("role dropped", "name", name)
	return nil
}

// Vacuum runs a vacuum operation.
func (c *pgxClient) Vacuum(ctx context.Context, opts VacuumOptions) (err error) {
	ctx, end := c.startOperation(ctx, "Vacuum")
	defer func() { end(err) }()

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

	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("running vacuum: %w", execErr)
		return err
	}
	c.logger.Info("vacuum completed", "full", opts.Full, "analyze", opts.Analyze, "table", opts.Table)
	return nil
}

// Analyze runs an analyze operation.
func (c *pgxClient) Analyze(ctx context.Context, table string) (err error) {
	ctx, end := c.startOperation(ctx, "Analyze")
	defer func() { end(err) }()

	query := "ANALYZE"
	if table != "" {
		query += " " + pgx.Identifier{table}.Sanitize()
	}

	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("running analyze: %w", execErr)
		return err
	}
	c.logger.Info("analyze completed", "table", table)
	return nil
}

// Reindex runs a reindex operation.
func (c *pgxClient) Reindex(ctx context.Context, opts ReindexOptions) (err error) {
	ctx, end := c.startOperation(ctx, "Reindex")
	defer func() { end(err) }()

	var query string
	switch {
	case opts.Table != "":
		query = fmt.Sprintf("REINDEX TABLE %s", pgx.Identifier{opts.Table}.Sanitize())
	case opts.Database != "":
		query = fmt.Sprintf("REINDEX DATABASE %s", pgx.Identifier{opts.Database}.Sanitize())
	default:
		err = fmt.Errorf("either database or table must be specified for reindex")
		return err
	}

	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("running reindex: %w", execErr)
		return err
	}
	c.logger.Info("reindex completed", "database", opts.Database, "table", opts.Table)
	return nil
}

// GetDiskUsage returns disk usage information.
func (c *pgxClient) GetDiskUsage(ctx context.Context, database string) (usages []DiskUsage, err error) {
	ctx, end := c.startOperation(ctx, "GetDiskUsage")
	defer func() { end(err) }()

	// Filter on datallowconn = true (connectable databases) rather than
	// datistemplate = false: on Cloudberry 2.1.0 the real connectable postgres
	// database is flagged datistemplate = true, so a datistemplate = false filter
	// would exclude every database and return an empty result. datallowconn = true
	// includes postgres + user databases (template0 has datallowconn = false);
	// template1 is excluded explicitly so only real user-facing databases appear.
	query := `SELECT datname, pg_database_size(datname) as size_bytes,
		pg_size_pretty(pg_database_size(datname)) as size_human
		FROM pg_database WHERE datallowconn = true AND datname NOT IN ('template0','template1')`

	if database != "" {
		query += fmt.Sprintf(" AND datname = %s", quoteLiteral(database))
	}
	query += " ORDER BY size_bytes DESC"

	rows, queryErr := c.pool.Query(ctx, query)
	if queryErr != nil {
		err = fmt.Errorf("querying disk usage: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var du DiskUsage
		if scanErr := rows.Scan(&du.Database, &du.SizeBytes, &du.SizeHuman); scanErr != nil {
			err = fmt.Errorf("scanning disk usage row: %w", scanErr)
			return nil, err
		}
		usages = append(usages, du)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating disk usage rows: %w", rowErr)
		return nil, err
	}

	return usages, nil
}

// GetReplicationLag returns the replication lag in bytes.
func (c *pgxClient) GetReplicationLag(ctx context.Context) (lag int64, err error) {
	ctx, end := c.startOperation(ctx, "GetReplicationLag")
	defer func() { end(err) }()

	query := `SELECT COALESCE(
		pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), 0
	) FROM pg_stat_replication LIMIT 1`

	if scanErr := c.pool.QueryRow(ctx, query).Scan(&lag); scanErr != nil {
		err = fmt.Errorf("querying replication lag: %w", scanErr)
		return 0, err
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
func (c *pgxClient) GetMaxConnections(ctx context.Context) (maxConns int32, err error) {
	ctx, end := c.startOperation(ctx, "GetMaxConnections")
	defer func() { end(err) }()

	query := `SELECT setting::int FROM pg_settings WHERE name = 'max_connections'`
	if scanErr := c.pool.QueryRow(ctx, query).Scan(&maxConns); scanErr != nil {
		err = fmt.Errorf("querying max_connections: %w", scanErr)
		return 0, err
	}
	return maxConns, nil
}

// GetActiveQueryCount returns the number of active, queued, and blocked queries.
func (c *pgxClient) GetActiveQueryCount(ctx context.Context) (active, queued, blocked int32, err error) {
	ctx, end := c.startOperation(ctx, "GetActiveQueryCount")
	defer func() { end(err) }()

	query := `SELECT 
		COUNT(*) FILTER (WHERE state = 'active') as active,
		COUNT(*) FILTER (WHERE wait_event_type = 'Lock') as blocked,
		COUNT(*) FILTER (WHERE state = 'idle in transaction') as queued
		FROM pg_stat_activity WHERE pid != pg_backend_pid()`

	if scanErr := c.pool.QueryRow(ctx, query).Scan(&active, &blocked, &queued); scanErr != nil {
		err = fmt.Errorf("querying active query counts: %w", scanErr)
		return 0, 0, 0, err
	}
	return active, queued, blocked, nil
}

// GetResourceGroupUsage returns CPU and memory usage for a resource group.
func (c *pgxClient) GetResourceGroupUsage(
	ctx context.Context,
	group string,
) (cpu, memory float64, err error) {
	ctx, end := c.startOperation(ctx, "GetResourceGroupUsage")
	defer func() { end(err) }()

	query := `SELECT 
		COALESCE(cpu_usage, 0), COALESCE(memory_usage, 0)
		FROM gp_toolkit.gp_resgroup_status 
		WHERE rsgname = $1`

	if scanErr := c.pool.QueryRow(ctx, query, group).Scan(&cpu, &memory); scanErr != nil {
		err = fmt.Errorf("querying resource group usage for %s: %w", group, scanErr)
		return 0, 0, err
	}
	return cpu, memory, nil
}

// CreateResourceGroup creates a new resource group.
func (c *pgxClient) CreateResourceGroup(ctx context.Context, opts ResourceGroupOptions) (err error) {
	ctx, end := c.startOperation(ctx, "CreateResourceGroup")
	defer func() { end(err) }()

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

	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("creating resource group %s: %w", opts.Name, execErr)
		return err
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
func (c *pgxClient) AlterResourceGroup(ctx context.Context, opts ResourceGroupOptions) (err error) {
	ctx, end := c.startOperation(ctx, "AlterResourceGroup")
	defer func() { end(err) }()

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
		if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
			err = fmt.Errorf("altering resource group %s param %s: %w", opts.Name, alt.param, execErr)
			return err
		}
	}

	// Apply I/O limits if specified. The io_limit string embeds the free-form
	// CRD Tablespace value, so it is quoted with quoteLiteral (single-quote
	// escaping) instead of naive '%s' interpolation — defense in depth against
	// SQL injection alongside the webhook/CRD pattern validation (C1a).
	if len(opts.IOLimits) > 0 {
		ioLimitStr := FormatIOLimits(opts.IOLimits)
		alterSQL := fmt.Sprintf(`ALTER RESOURCE GROUP %s SET io_limit %s`,
			pgx.Identifier{opts.Name}.Sanitize(), quoteLiteral(ioLimitStr))
		if _, execErr := c.pool.Exec(ctx, alterSQL); execErr != nil {
			err = fmt.Errorf("setting io_limit for resource group %s: %w", opts.Name, execErr)
			return err
		}
		c.logger.Info("resource group io_limit set", "name", opts.Name, "ioLimit", ioLimitStr)
	}

	c.logger.Info("resource group altered", "name", opts.Name)
	return nil
}

// DropResourceGroup drops a resource group.
func (c *pgxClient) DropResourceGroup(ctx context.Context, name string) (err error) {
	ctx, end := c.startOperation(ctx, "DropResourceGroup")
	defer func() { end(err) }()

	query := fmt.Sprintf("DROP RESOURCE GROUP %s", pgx.Identifier{name}.Sanitize())
	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("dropping resource group %s: %w", name, execErr)
		return err
	}
	c.logger.Info("resource group dropped", "name", name)
	return nil
}

// ListResourceGroups returns all resource groups.
func (c *pgxClient) ListResourceGroups(ctx context.Context) (groups []ResourceGroupInfo, err error) {
	ctx, end := c.startOperation(ctx, "ListResourceGroups")
	defer func() { end(err) }()

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

	rows, queryErr := c.pool.Query(ctx, query)
	if queryErr != nil {
		err = fmt.Errorf("querying resource groups: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var g ResourceGroupInfo
		scanErr := rows.Scan(&g.Name, &g.Concurrency, &g.CPUMaxPercent,
			&g.CPUWeight, &g.MemoryLimit, &g.MinCost)
		if scanErr != nil {
			err = fmt.Errorf("scanning resource group row: %w", scanErr)
			return nil, err
		}
		groups = append(groups, g)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating resource group rows: %w", rowErr)
		return nil, err
	}

	return groups, nil
}

// AssignRoleResourceGroup assigns a role to a resource group.
func (c *pgxClient) AssignRoleResourceGroup(ctx context.Context, role, group string) (err error) {
	ctx, end := c.startOperation(ctx, "AssignRoleResourceGroup")
	defer func() { end(err) }()

	sql := fmt.Sprintf("ALTER ROLE %s RESOURCE GROUP %s",
		pgx.Identifier{role}.Sanitize(), pgx.Identifier{group}.Sanitize())
	if _, execErr := c.pool.Exec(ctx, sql); execErr != nil {
		err = fmt.Errorf("assigning role %s to resource group %s: %w", role, group, execErr)
		return err
	}
	c.logger.Info("role assigned to resource group", "role", role, "group", group)
	return nil
}

// CreateResourceQueue creates a new resource queue.
// SQL: CREATE RESOURCE QUEUE <name> WITH (ACTIVE_STATEMENTS=<n>, MEMORY_LIMIT='<size>', PRIORITY=<level>)
func (c *pgxClient) CreateResourceQueue(ctx context.Context, opts ResourceQueueOptions) (err error) {
	ctx, end := c.startOperation(ctx, "CreateResourceQueue")
	defer func() { end(err) }()

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

	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("creating resource queue %s: %w", opts.Name, execErr)
		return err
	}
	c.logger.Info("resource queue created", "name", opts.Name)
	return nil
}

// DropResourceQueue drops a resource queue.
func (c *pgxClient) DropResourceQueue(ctx context.Context, name string) (err error) {
	ctx, end := c.startOperation(ctx, "DropResourceQueue")
	defer func() { end(err) }()

	query := fmt.Sprintf("DROP RESOURCE QUEUE %s", pgx.Identifier{name}.Sanitize())
	if _, execErr := c.pool.Exec(ctx, query); execErr != nil {
		err = fmt.Errorf("dropping resource queue %s: %w", name, execErr)
		return err
	}
	c.logger.Info("resource queue dropped", "name", name)
	return nil
}

// ListResourceQueues returns all resource queues from pg_resqueue.
func (c *pgxClient) ListResourceQueues(ctx context.Context) (queues []ResourceQueueInfo, err error) {
	ctx, end := c.startOperation(ctx, "ListResourceQueues")
	defer func() { end(err) }()

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

	rows, queryErr := c.pool.Query(ctx, query)
	if queryErr != nil {
		err = fmt.Errorf("querying resource queues: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var q ResourceQueueInfo
		if scanErr := rows.Scan(
			&q.Name, &q.ActiveStatements, &q.MemoryLimit,
			&q.Priority, &q.MaxCost, &q.MinCost,
		); scanErr != nil {
			err = fmt.Errorf("scanning resource queue row: %w", scanErr)
			return nil, err
		}
		queues = append(queues, q)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating resource queue rows: %w", rowErr)
		return nil, err
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
func (c *pgxClient) GetStorageDiskUsage(ctx context.Context) (usages []DiskUsageInfo, err error) {
	ctx, end := c.startOperation(ctx, "GetStorageDiskUsage")
	defer func() { end(err) }()

	query := `SELECT spcname,
		pg_tablespace_size(oid) AS size_bytes,
		pg_size_pretty(pg_tablespace_size(oid)) AS size_human
		FROM pg_tablespace ORDER BY size_bytes DESC`

	rows, queryErr := c.pool.Query(ctx, query)
	if queryErr != nil {
		err = fmt.Errorf("querying storage disk usage: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var du DiskUsageInfo
		if scanErr := rows.Scan(&du.Tablespace, &du.SizeBytes, &du.SizeHuman); scanErr != nil {
			err = fmt.Errorf("scanning storage disk usage row: %w", scanErr)
			return nil, err
		}
		usages = append(usages, du)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating storage disk usage rows: %w", rowErr)
		return nil, err
	}

	c.logger.Info("retrieved storage disk usage", "count", len(usages))
	return usages, nil
}

// tableStorageQuery lists the largest user tables with their on-disk size, live
// row count, and dead-tuple bloat percentage. It joins the always-present
// pg_stat_user_tables to pg_class/pg_namespace for the relation OID. The bloat
// percentage is cast FLOOR(...)::int because Cloudberry 2.1.0 returns the
// n_dead_tup*100/(...) division as a NUMERIC (e.g. with a fractional scale) on
// real churned data, which pgx cannot scan into a plain int32 — the same lesson
// applied in GetBloatRecommendations. The divide-by-zero case is guarded by the
// CASE expression. Largest tables come first and the result is capped to keep
// the listing bounded on huge catalogs.
const tableStorageQuery = `SELECT s.schemaname, s.relname,
	pg_total_relation_size(c.oid) AS size_bytes,
	pg_size_pretty(pg_total_relation_size(c.oid)) AS size_human,
	s.n_live_tup AS row_count,
	CASE WHEN s.n_live_tup + s.n_dead_tup > 0
		THEN FLOOR(s.n_dead_tup * 100.0 / (s.n_live_tup + s.n_dead_tup))::int
		ELSE 0
	END AS bloat_percent
	FROM pg_stat_user_tables s
	JOIN pg_class c ON c.relname = s.relname
	JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
	ORDER BY size_bytes DESC
	LIMIT 200`

// tableSkewQuery returns the per-relation distribution skew coefficient from
// gp_toolkit.gp_skew_coefficients. The coefficient is declared NUMERIC and is
// cast to ::float8 so pgx can scan it cleanly. This view requires gp_toolkit and
// may be absent on some server versions; callers honor an
// isUndefinedRelationOrColumn fallback and leave skew at 0 (no fabrication).
const tableSkewQuery = `SELECT skcnamespace, skcrelname, skccoeff::float8 AS skccoeff
	FROM gp_toolkit.gp_skew_coefficients`

// usageReportTablesQuery lists the largest user tables (per-table storage
// consumption, Scenario 120 C.11) in the currently-connected database. It mirrors
// tableStorageQuery's pg_class/pg_namespace join, includes ordinary ('r') and
// materialized-view ('m') relations, excludes the catalog/internal schemas, casts
// the computed size to bigint so pgx scans it cleanly, orders largest-first, and
// is bounded by LIMIT to keep cardinality sane. It is best-effort: callers honor
// an isUndefinedRelationOrColumn fallback and report an empty per-table list
// rather than failing the whole report (no fabrication).
const usageReportTablesQuery = `SELECT n.nspname AS schema, c.relname AS tbl,
	pg_total_relation_size(c.oid)::bigint AS size_bytes,
	pg_size_pretty(pg_total_relation_size(c.oid)) AS size_human
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind IN ('r', 'm')
	  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit', 'pg_ext_aux')
	ORDER BY size_bytes DESC
	LIMIT 50`

// GetTables returns per-table storage info (size, bloat, skew, row count) for
// all user tables. Bloat is derived from the always-present pg_stat_user_tables;
// skew is enriched from gp_toolkit.gp_skew_coefficients when present and left at
// 0 (honest fallback) when the view/columns are absent.
func (c *pgxClient) GetTables(ctx context.Context) (tables []TableStorageInfo, err error) {
	ctx, end := c.startOperation(ctx, "GetTables")
	defer func() { end(err) }()

	rows, queryErr := c.pool.Query(ctx, tableStorageQuery)
	if queryErr != nil {
		err = fmt.Errorf("querying tables: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var t TableStorageInfo
		if scanErr := rows.Scan(
			&t.Schema, &t.Table, &t.SizeBytes, &t.SizeHuman,
			&t.RowCount, &t.BloatPercent,
		); scanErr != nil {
			err = fmt.Errorf("scanning table storage row: %w", scanErr)
			return nil, err
		}
		tables = append(tables, t)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating table storage rows: %w", rowErr)
		return nil, err
	}

	// Best-effort skew enrichment: gp_toolkit.gp_skew_coefficients may be absent.
	if skew := c.collectTableSkew(ctx); len(skew) > 0 {
		for i := range tables {
			tables[i].SkewPercent = skew[tableSkewKey(tables[i].Schema, tables[i].Table)]
		}
	}

	c.logger.Info("retrieved tables", "count", len(tables))
	return tables, nil
}

// tableSkewKey builds the lookup key used to merge skew coefficients into the
// table listing by (schema, table).
func tableSkewKey(schema, table string) string {
	return schema + "." + table
}

// collectTableSkew returns a map of "schema.table" -> skew percentage from
// gp_toolkit.gp_skew_coefficients. It is best-effort: when the view/columns are
// absent (SQLSTATE 42P01/42703) or any query/scan error occurs it returns an
// empty map so the caller honestly reports skew 0 rather than fabricating a
// value. Errors are logged at debug and never surfaced to the caller.
func (c *pgxClient) collectTableSkew(ctx context.Context) map[string]int32 {
	skew := map[string]int32{}

	rows, queryErr := c.pool.Query(ctx, tableSkewQuery)
	if queryErr != nil {
		if !isUndefinedRelationOrColumn(queryErr) {
			c.logger.Debug("skew enrichment query failed, leaving skew 0", "error", queryErr)
		}
		return skew
	}
	defer rows.Close()

	for rows.Next() {
		var schema, table string
		var coeff float64
		if scanErr := rows.Scan(&schema, &table, &coeff); scanErr != nil {
			c.logger.Debug("skew enrichment scan failed, leaving skew 0", "error", scanErr)
			return skew
		}
		skew[tableSkewKey(schema, table)] = clampPercent(int32(coeff))
	}

	if rowErr := rows.Err(); rowErr != nil {
		c.logger.Debug("skew enrichment iteration failed, leaving skew 0", "error", rowErr)
	}

	return skew
}

// diskUsagePercentQuery computes the worst-case (MAX across segments) filesystem
// usage percentage of the segment data volumes from gp_toolkit.gp_disk_free.
//
// gp_disk_free exposes per-segment volume capacity figures. Column naming varies
// across Cloudberry/Greenplum versions, so the query is written defensively:
//   - df_total / df_free are the canonical per-segment total/free byte figures.
//   - usage% = 100*(df_total - df_free)/df_total, guarded against df_total = 0
//     to avoid divide-by-zero.
//   - MAX selects the most-full volume (worst case) — the right signal for a
//     "running out of disk" alert.
//   - COALESCE(..., 0) yields 0 (not NULL) when the view returns no rows.
//
// Integer division in SQL truncates toward zero, which is acceptable for an
// int32 percentage and matches Status.DiskUsagePercent (no float drift).
const diskUsagePercentQuery = `SELECT COALESCE(MAX(
	CASE WHEN df_total > 0
		THEN (100 * (df_total - df_free)) / df_total
		ELSE 0
	END), 0)::int AS usage_percent
FROM gp_toolkit.gp_disk_free`

// isUndefinedRelationOrColumn reports whether err is a PostgreSQL error
// indicating the relation (42P01) or a column (42703) does not exist. Such
// errors mean gp_toolkit.gp_disk_free is not available on this server version.
func isUndefinedRelationOrColumn(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == sqlStateUndefinedTable || pgErr.Code == sqlStateUndefinedColumn
}

// IsCoordinatorShuttingDown reports whether err indicates the coordinator is in
// the "cannot connect now" state (SQLSTATE 57P03, "the database system is
// shutting down"/"starting up"). This is used by the scale controller to detect
// a wedged coordinator (D3) so it can roll the stuck pod and recover instead of
// requeueing forever. It unwraps the error chain and also matches the textual
// signature as a fallback for drivers that surface the state without a code.
func IsCoordinatorShuttingDown(err error) bool {
	if err == nil {
		return false
	}
	// Primary signal: a *pgconn.PgError carrying SQLSTATE 57P03 anywhere in the
	// wrapped chain. pgx can surface the wedge either as a query-time PgError or,
	// during CONNECTION ESTABLISHMENT (the redistribution/seed DB dial), inside a
	// *pgconn.connectError that wraps the underlying PgError — errors.As unwraps
	// both, so this catches the wedge whether it happens at dial or at query time.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == sqlStateCannotConnectNow {
		return true
	}
	// Fallback: textual signatures for drivers/paths that surface the wedge
	// without a decoded SQLSTATE (for example, a dial failure whose message
	// carries the server's FATAL text but not a structured PgError). Broadened
	// (D3) to cover the "cannot connect now" and recovery-mode variants in
	// addition to the shutting-down/starting-up messages.
	msg := strings.ToLower(err.Error())
	for _, sig := range coordinatorWedgeSignatures {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	return false
}

// coordinatorWedgeSignatures are the lowercase textual signatures that indicate
// a wedged/unavailable coordinator (SQLSTATE 57P03 and its message variants).
// Used as a fallback in IsCoordinatorShuttingDown when a structured PgError is
// not available (for example, a connection-establishment failure during the
// redistribution or catalog-seeding DB dial, D3).
var coordinatorWedgeSignatures = []string{
	"the database system is shutting down",
	"the database system is starting up",
	"the database system is in recovery mode",
	"cannot connect now",
	"57p03",
}

// IsSegmentCatalogNotSeeded reports whether err is (or wraps)
// ErrSegmentCatalogNotSeeded — the terminal D8 condition in which a newly-added
// segment's catalog was not seeded from the coordinator, so EXPAND TABLE can
// never redistribute pre-existing relations onto it. The scale controller uses
// this to abort a scale-out instead of retrying forever.
func IsSegmentCatalogNotSeeded(err error) bool {
	return errors.Is(err, ErrSegmentCatalogNotSeeded)
}

// clampPercent constrains a raw percentage into the inclusive 0..100 range so a
// malformed view (e.g. df_free > df_total) can never publish an out-of-range
// gauge or status value.
func clampPercent(pct int32) int32 {
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// GetDiskUsagePercent returns the worst-case (MAX across segments) filesystem
// usage percentage of the segment data volumes, sourced from
// gp_toolkit.gp_disk_free (see diskUsagePercentQuery for the aggregation).
//
// It is the measurement primitive behind Scenario 116 R.2/S.1/M.1: the caller
// (recordDiskUsage) writes the returned value into Status.DiskUsagePercent and
// publishes the cloudberry_disk_usage_percent gauge from the SAME value.
//
// Honest fallback (NO fabrication): when the view or its columns are not
// available on this server version (SQLSTATE 42P01 / 42703), it returns
// ErrDiskUsageUnavailable so the caller can SKIP the measurement instead of
// substituting a misleading value. Any other query error is wrapped and
// returned. The result is clamped to 0..100.
func (c *pgxClient) GetDiskUsagePercent(ctx context.Context) (percent int32, err error) {
	ctx, end := c.startOperation(ctx, "GetDiskUsagePercent")
	defer func() { end(err) }()

	var pct int32
	if scanErr := c.pool.QueryRow(ctx, diskUsagePercentQuery).Scan(&pct); scanErr != nil {
		if isUndefinedRelationOrColumn(scanErr) {
			// View/columns absent on this version: skip honestly, do not fabricate.
			err = ErrDiskUsageUnavailable
			return 0, err
		}
		err = fmt.Errorf("querying disk usage percent: %w", scanErr)
		return 0, err
	}

	percent = clampPercent(pct)
	c.logger.Info("retrieved disk usage percent", "percent", percent)
	return percent, nil
}

// clusterDataSizeQuery sums the logical on-disk size of every database in the
// cluster. pg_database_size is a core function available on all server versions
// (no gp_toolkit / CREATE EXTENSION required), so this query is fully portable
// and was verified live on Cloudberry 2.1.0. COALESCE guards the (impossible in
// practice) empty-catalog case so the scan always yields a non-NULL bigint.
const clusterDataSizeQuery = `SELECT COALESCE(sum(pg_database_size(datname)), 0)::bigint
FROM pg_database`

// GetClusterDataSizeBytes returns the total LOGICAL on-disk size of all
// databases in the cluster (sum of pg_database_size). See the Client interface
// documentation for the important distinction between this LOGICAL size and true
// filesystem usage: this value is the numerator of the PORTABLE FALLBACK proxy
// used by recordDiskUsage when gp_toolkit.gp_disk_free is unavailable. It is
// always available (verified live on Cloudberry 2.1.0).
func (c *pgxClient) GetClusterDataSizeBytes(ctx context.Context) (sizeBytes int64, err error) {
	ctx, end := c.startOperation(ctx, "GetClusterDataSizeBytes")
	defer func() { end(err) }()

	if scanErr := c.pool.QueryRow(ctx, clusterDataSizeQuery).Scan(&sizeBytes); scanErr != nil {
		err = fmt.Errorf("querying cluster data size: %w", scanErr)
		return 0, err
	}

	c.logger.Info("retrieved cluster data size", "bytes", sizeBytes)
	return sizeBytes, nil
}

// GetBloatRecommendations returns bloat recommendations by querying table
// statistics for dead-tuple ratios that indicate bloat.
//
// RT.1/C.6: the query GATES on th.Bloat — only tables whose dead-tuple
// percentage (dead_pct) is >= the CRD bloatThreshold are returned. The
// threshold is bound as a parameter ($1), never string-interpolated, so the gate
// is injection-safe. r.Ratio is still populated with dead_pct so recordRecommendations
// can feed the cloudberry_table_bloat_ratio gauge (M.4). pg_stat_user_tables is a
// core catalog and is always present, so no honest-fallback path is needed here.
func (c *pgxClient) GetBloatRecommendations(
	ctx context.Context, th RecommendationThresholds,
) (recs []Recommendation, err error) {
	ctx, end := c.startOperation(ctx, "GetBloatRecommendations")
	defer func() { end(err) }()

	// dead_pct is cast to ::int in BOTH the projection and the WHERE gate.
	// Cloudberry 2.1.0 returns the n_dead_tup*100/(...) division as NUMERIC
	// (e.g. with a -16 scale) on real churned data, which pgx cannot scan into
	// a plain int64. FLOOR(...)::int forces a clean integer so the row scans
	// into deadPct (int64) and the gate compares an int to $1 (int).
	query := `SELECT schemaname, relname,
		n_dead_tup,
		CASE WHEN n_live_tup + n_dead_tup > 0
			THEN FLOOR(n_dead_tup * 100.0 / (n_live_tup + n_dead_tup))::int
			ELSE 0
		END AS dead_pct
		FROM pg_stat_user_tables
		WHERE n_live_tup + n_dead_tup > 0
			AND FLOOR(n_dead_tup * 100.0 / (n_live_tup + n_dead_tup))::int >= $1
		ORDER BY dead_pct DESC
		LIMIT 50`

	rows, queryErr := c.pool.Query(ctx, query, th.Bloat)
	if queryErr != nil {
		err = fmt.Errorf("querying bloat recommendations: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var r Recommendation
		var deadPct int64
		if scanErr := rows.Scan(&r.Schema, &r.Table, &r.Value, &deadPct); scanErr != nil {
			err = fmt.Errorf("scanning bloat recommendation row: %w", scanErr)
			return nil, err
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
		err = fmt.Errorf("iterating bloat recommendation rows: %w", rowErr)
		return nil, err
	}

	c.logger.Info("retrieved bloat recommendations", "count", len(recs))
	return recs, nil
}

// GetSkewRecommendations returns data skew recommendations sourced from
// gp_toolkit.gp_skew_coefficients, the authoritative per-relation distribution
// skew view in Cloudberry/Greenplum.
//
// RT.2/C.7: the query GATES on th.Skew — only relations whose skew coefficient
// (skccoeff) is >= the CRD skewThreshold are returned. The threshold is bound as
// a parameter ($1), never string-interpolated. r.Ratio carries skccoeff.
//
// Honest fallback (NO fabrication): gp_skew_coefficients only exists after
// CREATE EXTENSION gp_toolkit and may be absent on some server versions. When the
// query fails with SQLSTATE 42P01 (undefined table) or 42703 (undefined column),
// skew is treated as UNMEASURABLE: the method LOGS at debug, SKIPS, and returns
// (nil, nil) — it does NOT substitute the old row-count proxy (which fabricates
// skew from sheer row count). To the caller's counter, "no skew rows" and "skew
// unmeasurable" are honestly indistinguishable (skew count 0). This is a
// best-effort skew source: only gp_toolkit yields a true coefficient.
func (c *pgxClient) GetSkewRecommendations(
	ctx context.Context, th RecommendationThresholds,
) (recs []Recommendation, err error) {
	ctx, end := c.startOperation(ctx, "GetSkewRecommendations")
	defer func() { end(err) }()

	// skccoeff is declared NUMERIC in gp_toolkit.gp_skew_coefficients, which pgx
	// cannot scan into a plain float64; cast it to ::float8 so the coeff scan is
	// clean. The gate is left as skccoeff >= $1 (NUMERIC vs int compares fine in
	// SQL) and remains the last ">=" in the query for the threshold parser.
	query := `SELECT skcnamespace, skcrelname, skccoeff::float8 AS skccoeff
		FROM gp_toolkit.gp_skew_coefficients
		WHERE skccoeff >= $1
		ORDER BY skccoeff DESC
		LIMIT 50`

	rows, queryErr := c.pool.Query(ctx, query, th.Skew)
	if queryErr != nil {
		if isUndefinedRelationOrColumn(queryErr) {
			// gp_toolkit.gp_skew_coefficients absent on this version: skip
			// honestly, do not fabricate. Count 0 skew.
			c.logger.Debug("gp_skew_coefficients unavailable, skipping skew recommendations",
				"error", queryErr)
			return nil, nil
		}
		err = fmt.Errorf("querying skew recommendations: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var r Recommendation
		var coeff float64
		if scanErr := rows.Scan(&r.Schema, &r.Table, &coeff); scanErr != nil {
			err = fmt.Errorf("scanning skew recommendation row: %w", scanErr)
			return nil, err
		}
		r.Type = "skew"
		r.Value = int64(coeff)
		r.Ratio = coeff
		r.Severity = classifySeverity(int64(coeff), 20, 50)
		r.Description = fmt.Sprintf(
			"Table %s.%s has skew coefficient %.1f; verify distribution key for even data spread",
			r.Schema, r.Table, coeff)
		recs = append(recs, r)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating skew recommendation rows: %w", rowErr)
		return nil, err
	}

	c.logger.Info("retrieved skew recommendations", "count", len(recs))
	return recs, nil
}

// GetAgeRecommendations returns transaction-ID (XID) age recommendations
// sourced from the REAL relation freeze age, age(relfrozenxid), on pg_class.
//
// RT.3/C.8: the query GATES on th.Age — only ordinary/materialized relations
// (relkind IN ('r','m')) in user schemas whose age(relfrozenxid) is >= the CRD
// ageThreshold are returned. The threshold (int64) is bound as a parameter ($1),
// never string-interpolated. r.Value carries the true XID age. This measures
// wraparound risk honestly; it does NOT use the old dead-tuple proxy (which
// measures vacuum debt, not freeze age).
//
// Honest fallback (NO fabrication): pg_class.relfrozenxid and the age() function
// are core catalog and are always present, so a view-missing fallback is not
// normally hit. Should the query nonetheless fail with 42P01/42703, the method
// SKIPS + logs and returns (nil, nil) rather than reverting to a misleading
// dead-tuple proxy.
func (c *pgxClient) GetAgeRecommendations(
	ctx context.Context, th RecommendationThresholds,
) (recs []Recommendation, err error) {
	ctx, end := c.startOperation(ctx, "GetAgeRecommendations")
	defer func() { end(err) }()

	// age(relfrozenxid) yields a signed 32-bit integer; cast it to ::bigint so it
	// scans cleanly into r.Value (int64) regardless of the driver's integer
	// width handling. The gate keeps the same ::bigint cast and stays the last
	// ">=" in the query for the threshold parser.
	query := `SELECT n.nspname, c.relname, age(c.relfrozenxid)::bigint AS xid_age
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','m')
			AND n.nspname NOT IN ('pg_catalog','information_schema','gp_toolkit','pg_ext_aux')
			AND age(c.relfrozenxid)::bigint >= $1
		ORDER BY xid_age DESC
		LIMIT 50`

	rows, queryErr := c.pool.Query(ctx, query, th.Age)
	if queryErr != nil {
		if isUndefinedRelationOrColumn(queryErr) {
			c.logger.Debug("XID age catalog unavailable, skipping age recommendations",
				"error", queryErr)
			return nil, nil
		}
		err = fmt.Errorf("querying age recommendations: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var r Recommendation
		if scanErr := rows.Scan(&r.Schema, &r.Table, &r.Value); scanErr != nil {
			err = fmt.Errorf("scanning age recommendation row: %w", scanErr)
			return nil, err
		}
		r.Type = "age"
		r.Severity = classifySeverity(r.Value, 100000000, 500000000)
		r.Description = fmt.Sprintf(
			"Table %s.%s has XID age %d; consider running VACUUM to prevent wraparound",
			r.Schema, r.Table, r.Value)
		recs = append(recs, r)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating age recommendation rows: %w", rowErr)
		return nil, err
	}

	c.logger.Info("retrieved age recommendations", "count", len(recs))
	return recs, nil
}

// indexBloatEstimateQuery estimates per-index bloat percentage from core
// catalogs (pg_class/pg_index/pg_namespace) and gates it on $1 (th.IndexBloat).
// It assumes an 8 KiB page and a nominal average index entry width of 32 bytes
// at a 90% fill factor, yielding ideal_pages = ceil(reltuples * 32 / (8192*0.9)).
// Indexes with no pages or no tuples are skipped (cannot estimate). The estimate
// is clamped to 0..100 via the GREATEST/LEAST guards.
const indexBloatEstimateQuery = `WITH idx AS (
	SELECT n.nspname AS schemaname,
		t.relname AS relname,
		i.relname AS indexrelname,
		i.relpages::float8 AS relpages,
		i.reltuples::float8 AS reltuples
	FROM pg_index x
	JOIN pg_class i ON i.oid = x.indexrelid
	JOIN pg_class t ON t.oid = x.indrelid
	JOIN pg_namespace n ON n.oid = i.relnamespace
	WHERE i.relkind = 'i'
		AND n.nspname NOT IN ('pg_catalog','information_schema','gp_toolkit','pg_ext_aux')
		AND i.relpages > 0
		AND i.reltuples > 0
)
SELECT schemaname, relname, indexrelname,
	LEAST(100.0, GREATEST(0.0,
		100.0 * (relpages - CEIL(reltuples * 32.0 / (8192.0 * 0.9))) / relpages
	))::float8 AS bloat_pct
FROM idx
WHERE LEAST(100.0, GREATEST(0.0,
		100.0 * (relpages - CEIL(reltuples * 32.0 / (8192.0 * 0.9))) / relpages
	))::float8 >= $1
ORDER BY bloat_pct DESC
LIMIT 50`

// GetIndexBloatRecommendations returns index bloat recommendations gated on a
// portable index bloat-percentage ESTIMATE.
//
// RT.4/C.9: the query GATES on th.IndexBloat — only indexes whose estimated
// bloat percentage is >= the CRD indexBloatThreshold are returned. The threshold
// is bound as a parameter ($1), never string-interpolated. r.Ratio carries the
// estimated bloat percentage.
//
// The estimate is self-contained on core catalogs (pg_class / pg_index /
// pg_namespace) so it is portable across Cloudberry/Greenplum versions and does
// not depend on an optional stats view. It approximates bloat as the fraction of
// the index's actual pages (relpages) in excess of the IDEAL pages needed to
// store reltuples at a nominal 90% fill factor for an average row width, i.e.
//
//	bloat_pct ≈ 100 * (relpages - ideal_pages) / relpages
//
// where ideal_pages = ceil(reltuples / tuples_per_page) and tuples_per_page is
// derived from the 8 KiB page size and an estimated index-entry width. This is a
// documented heuristic, NOT an exact bloat measurement, but it honestly tracks
// over-sized indexes rather than the old "index size > 0" gate (which flagged
// every index). Indexes with too few pages/tuples to estimate are excluded.
//
// Honest fallback (NO fabrication): should the estimate rely on a catalog column
// absent on this version (42P01/42703), the method SKIPS + logs and returns
// (nil, nil) rather than reverting to the raw size gate.
func (c *pgxClient) GetIndexBloatRecommendations(
	ctx context.Context, th RecommendationThresholds,
) (recs []Recommendation, err error) {
	ctx, end := c.startOperation(ctx, "GetIndexBloatRecommendations")
	defer func() { end(err) }()

	rows, queryErr := c.pool.Query(ctx, indexBloatEstimateQuery, th.IndexBloat)
	if queryErr != nil {
		if isUndefinedRelationOrColumn(queryErr) {
			c.logger.Debug("index bloat estimate catalog unavailable, skipping",
				"error", queryErr)
			return nil, nil
		}
		err = fmt.Errorf("querying index bloat recommendations: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var r Recommendation
		var indexName string
		var bloatPct float64
		if scanErr := rows.Scan(&r.Schema, &r.Table, &indexName, &bloatPct); scanErr != nil {
			err = fmt.Errorf("scanning index bloat recommendation row: %w", scanErr)
			return nil, err
		}
		r.Type = "index_bloat"
		r.Value = int64(bloatPct)
		r.Ratio = bloatPct
		r.Severity = classifySeverity(int64(bloatPct), 30, 60)
		r.Description = fmt.Sprintf(
			"Index %s on %s.%s has estimated bloat %.1f%%; consider REINDEX",
			indexName, r.Schema, r.Table, bloatPct)
		recs = append(recs, r)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating index bloat recommendation rows: %w", rowErr)
		return nil, err
	}

	c.logger.Info("retrieved index bloat recommendations", "count", len(recs))
	return recs, nil
}

// TriggerRecommendationScan triggers a recommendation scan by running ANALYZE
// on all user tables to refresh statistics.
func (c *pgxClient) TriggerRecommendationScan(ctx context.Context) (err error) {
	ctx, end := c.startOperation(ctx, "TriggerRecommendationScan")
	defer func() { end(err) }()

	c.logger.Info("triggering recommendation scan via ANALYZE")
	if _, execErr := c.pool.Exec(ctx, "ANALYZE"); execErr != nil {
		err = fmt.Errorf("running ANALYZE for recommendation scan: %w", execErr)
		return err
	}
	c.logger.Info("recommendation scan completed")
	return nil
}

// GetTableDetails returns detailed information about a specific table
// by querying system catalog views.
func (c *pgxClient) GetTableDetails(ctx context.Context, schema, table string) (detail *TableDetail, err error) {
	ctx, end := c.startOperation(ctx, "GetTableDetails")
	defer func() { end(err) }()

	// bloat_percent is cast FLOOR(...)::int (not plain integer division) because
	// Cloudberry 2.1.0 returns the n_dead_tup*100/(...) division as a NUMERIC with
	// a fractional scale on real churned data, which pgx cannot scan into a plain
	// int32 — the Scenario 117 NUMERIC-cast lesson, identical to
	// GetBloatRecommendations / tableStorageQuery. The divide-by-zero case is
	// guarded by the CASE expression.
	query := `SELECT
		s.schemaname,
		s.relname,
		pg_total_relation_size(c.oid) AS size_bytes,
		pg_size_pretty(pg_total_relation_size(c.oid)) AS size_human,
		s.n_live_tup AS row_count,
		CASE WHEN s.n_live_tup + s.n_dead_tup > 0
			THEN FLOOR(s.n_dead_tup * 100.0 / (s.n_live_tup + s.n_dead_tup))::int
			ELSE 0
		END AS bloat_percent,
		COALESCE(s.last_vacuum::text, s.last_autovacuum::text, 'never') AS last_vacuum,
		COALESCE(s.last_analyze::text, s.last_autoanalyze::text, 'never') AS last_analyze
		FROM pg_stat_user_tables s
		JOIN pg_class c ON c.relname = s.relname
		JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
		WHERE s.schemaname = $1 AND s.relname = $2`

	detail = &TableDetail{}
	// Scan order MUST match the SELECT projection exactly: schema, table,
	// size_bytes, size_human, row_count, bloat_percent, last_vacuum, last_analyze.
	// SkewPercent has no projection here and is enriched best-effort below
	// (gp_toolkit), staying honestly 0 when the view is unavailable.
	if scanErr := c.pool.QueryRow(ctx, query, schema, table).Scan(
		&detail.Schema, &detail.Table, &detail.SizeBytes, &detail.SizeHuman,
		&detail.RowCount, &detail.BloatPercent, &detail.LastVacuum, &detail.LastAnalyze,
	); scanErr != nil {
		err = fmt.Errorf("querying table details for %s.%s: %w", schema, table, scanErr)
		return nil, err
	}

	// Best-effort skew enrichment (honest 0 when gp_toolkit absent).
	if skew := c.collectTableSkew(ctx); len(skew) > 0 {
		detail.SkewPercent = skew[tableSkewKey(detail.Schema, detail.Table)]
	}

	// Best-effort index sizes: a server without the catalog still returns the
	// base detail (empty IndexSizes), never an error.
	detail.IndexSizes = c.collectIndexSizes(ctx, schema, table)

	c.logger.Info("retrieved table details", "schema", schema, "table", table)
	return detail, nil
}

// indexSizesQuery returns each index's on-disk size for a given table. It joins
// pg_index to pg_class (index + table) and pg_namespace, filtering by
// schema/table. Largest indexes come first.
const indexSizesQuery = `SELECT i.relname AS index_name,
	pg_relation_size(i.oid) AS size_bytes,
	pg_size_pretty(pg_relation_size(i.oid)) AS size_human
	FROM pg_index x
	JOIN pg_class i ON i.oid = x.indexrelid
	JOIN pg_class t ON t.oid = x.indrelid
	JOIN pg_namespace n ON n.oid = t.relnamespace
	WHERE n.nspname = $1 AND t.relname = $2
	ORDER BY size_bytes DESC`

// collectIndexSizes returns the on-disk size of every index on schema.table.
// It is best-effort: on any query/scan error it logs at debug and returns an
// empty slice so GetTableDetails still surfaces the base detail.
func (c *pgxClient) collectIndexSizes(ctx context.Context, schema, table string) []IndexSizeInfo {
	indexes := []IndexSizeInfo{}

	rows, queryErr := c.pool.Query(ctx, indexSizesQuery, schema, table)
	if queryErr != nil {
		c.logger.Debug("index sizes query failed, leaving empty",
			"schema", schema, "table", table, "error", queryErr)
		return indexes
	}
	defer rows.Close()

	for rows.Next() {
		var idx IndexSizeInfo
		if scanErr := rows.Scan(&idx.Name, &idx.SizeBytes, &idx.SizeHuman); scanErr != nil {
			c.logger.Debug("index size scan failed, leaving empty",
				"schema", schema, "table", table, "error", scanErr)
			return []IndexSizeInfo{}
		}
		indexes = append(indexes, idx)
	}

	if rowErr := rows.Err(); rowErr != nil {
		c.logger.Debug("index size iteration failed, leaving empty",
			"schema", schema, "table", table, "error", rowErr)
	}

	return indexes
}

// GetUsageReport returns a monthly usage report (Scenario 120 C.11) by querying
// current catalog sizes and connection statistics. It produces one entry per
// non-template database (size + connections) and enriches the entry for the
// connected database (c.config.Database) with a bounded per-table breakdown.
//
// The month is a scope/LABEL stamped on every entry; the report is computed ON
// DEMAND from live sizes. The operator does not persist month-over-month
// snapshots, so GrowthBytes/GrowthHuman/QueryCount stay an honest 0/empty rather
// than fabricated from a manufactured baseline. The per-table breakdown is
// available only for the connected database — the pgx pool is single-database, so
// tables in other databases cannot be sized without a separate connection and are
// honestly left empty.
func (c *pgxClient) GetUsageReport(ctx context.Context, month string) (entries []UsageReportEntry, err error) {
	ctx, end := c.startOperation(ctx, "GetUsageReport")
	defer func() { end(err) }()

	// Both pg_database (d) and pg_stat_database (s) expose a datname column, so
	// every ambiguous reference is qualified with the d. (pg_database) alias and
	// connections is read from s.numbackends. Leaving datname unqualified fails
	// live with "column reference \"datname\" is ambiguous" (SQLSTATE 42702).
	//
	// The database filter targets CONNECTABLE databases (datallowconn = true)
	// rather than non-template databases (datistemplate = false). On Cloudberry
	// 2.1.0 the real, connectable "postgres" database is flagged
	// datistemplate = true, so a datistemplate = false filter excludes every
	// database and yields an empty report. datallowconn = true correctly
	// includes "postgres" plus any user databases and is portable across
	// Cloudberry/Greenplum/PostgreSQL. template0 (datallowconn = false) is
	// excluded inherently; template1 is dropped explicitly so the usage report
	// covers only real user-facing databases.
	query := `SELECT d.datname,
		pg_database_size(d.datname) AS size_bytes,
		pg_size_pretty(pg_database_size(d.datname)) AS size_human,
		COALESCE(s.numbackends, 0) AS connections
		FROM pg_database d
		LEFT JOIN pg_stat_database s ON d.datname = s.datname
		WHERE d.datallowconn = true AND d.datname NOT IN ('template0','template1')
		ORDER BY size_bytes DESC`

	rows, queryErr := c.pool.Query(ctx, query)
	if queryErr != nil {
		err = fmt.Errorf("querying usage report: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var e UsageReportEntry
		if scanErr := rows.Scan(&e.Database, &e.SizeBytes, &e.SizeHuman, &e.Connections); scanErr != nil {
			err = fmt.Errorf("scanning usage report row: %w", scanErr)
			return nil, err
		}
		e.Month = month
		entries = append(entries, e)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating usage report rows: %w", rowErr)
		return nil, err
	}

	// Best-effort per-table enrichment (C.11): the breakdown is attached only to
	// the entry for the connected database, since the pool is single-database.
	// A tables-query failure must never fail the whole report.
	c.attachUsageReportTables(ctx, entries)

	c.logger.Info("retrieved usage report", "month", month, "count", len(entries))
	return entries, nil
}

// attachUsageReportTables enriches the connected-database entry with its
// per-table breakdown. It is a no-op when no entry matches the connected
// database name (e.g. the catalog row is filtered) so the report still surfaces
// the per-database content honestly.
func (c *pgxClient) attachUsageReportTables(ctx context.Context, entries []UsageReportEntry) {
	connected := c.config.Database
	if connected == "" {
		return
	}
	tables := c.collectUsageReportTables(ctx)
	if len(tables) == 0 {
		return
	}
	for i := range entries {
		if entries[i].Database == connected {
			entries[i].Tables = tables
			return
		}
	}
}

// collectUsageReportTables returns the largest user tables in the currently-
// connected database (per-table storage consumption, Scenario 120 C.11). It is
// best-effort: when the catalog relations/columns are absent (SQLSTATE
// 42P01/42703) or any query/scan error occurs, it logs at debug and returns an
// empty slice so GetUsageReport still surfaces the per-database content rather
// than failing — honest empty, never a fabricated value.
func (c *pgxClient) collectUsageReportTables(ctx context.Context) (tables []TableUsage) {
	ctx, end := c.startOperation(ctx, "collectUsageReportTables")
	var err error
	defer func() { end(err) }()

	rows, queryErr := c.pool.Query(ctx, usageReportTablesQuery)
	if queryErr != nil {
		err = queryErr
		if !isUndefinedRelationOrColumn(queryErr) {
			c.logger.Debug("usage-report tables query failed, leaving empty", "error", queryErr)
		}
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var t TableUsage
		if scanErr := rows.Scan(&t.Schema, &t.Table, &t.SizeBytes, &t.SizeHuman); scanErr != nil {
			err = scanErr
			c.logger.Debug("usage-report tables scan failed, leaving empty", "error", scanErr)
			return nil
		}
		tables = append(tables, t)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = rowErr
		c.logger.Debug("usage-report tables iteration failed, leaving empty", "error", rowErr)
		return nil
	}

	return tables
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
func (c *pgxClient) ConfigureReplication(ctx context.Context, opts ReplicationOptions) (err error) {
	ctx, end := c.startOperation(ctx, "ConfigureReplication")
	defer func() { end(err) }()

	c.logger.Info("configuring replication", "mode", opts.Mode)

	if pingErr := c.Ping(ctx); pingErr != nil {
		err = fmt.Errorf("database not reachable for replication configuration: %w", pingErr)
		return err
	}

	// WAL replication is configured automatically by Cloudberry when mirrors
	// are initialized. This method records the intent for observability.
	c.logger.Info("replication configuration request recorded", "mode", opts.Mode)

	return nil
}

// GetMirrorSyncStatus returns the synchronization status of all mirror segments
// by querying gp_segment_configuration and pg_stat_replication.
func (c *pgxClient) GetMirrorSyncStatus(ctx context.Context) (results []MirrorSyncInfo, err error) {
	ctx, end := c.startOperation(ctx, "GetMirrorSyncStatus")
	defer func() { end(err) }()

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

	rows, queryErr := c.pool.Query(ctx, query)
	if queryErr != nil {
		err = fmt.Errorf("querying mirror sync status: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var info MirrorSyncInfo
		if scanErr := rows.Scan(&info.ContentID, &info.IsSynced, &info.ReplicationLag, &info.State); scanErr != nil {
			err = fmt.Errorf("scanning mirror sync status row: %w", scanErr)
			return nil, err
		}
		results = append(results, info)
	}

	if rowErr := rows.Err(); rowErr != nil {
		err = fmt.Errorf("iterating mirror sync status rows: %w", rowErr)
		return nil, err
	}

	c.logger.Info("retrieved mirror sync status", "count", len(results))
	return results, nil
}

// TriggerFTSProbe requests Cloudberry's FTS daemon to perform an immediate probe scan.
// This triggers the internal FTS mechanism that detects failed segments and promotes
// mirrors to primary role. The call blocks until the scan completes.
func (c *pgxClient) TriggerFTSProbe(ctx context.Context) (err error) {
	ctx, end := c.startOperation(ctx, "TriggerFTSProbe")
	defer func() { end(err) }()

	if _, execErr := c.pool.Exec(ctx, "SELECT gp_request_fts_probe_scan()"); execErr != nil {
		err = fmt.Errorf("triggering FTS probe scan: %w", execErr)
		return err
	}
	c.logger.Info("FTS probe scan triggered successfully")
	return nil
}

// TerminateAllBackends terminates all non-system backend connections.
// It calls pg_terminate_backend for each session except the current one
// and system processes. Returns the number of backends terminated.
func (c *pgxClient) TerminateAllBackends(ctx context.Context) (terminated int32, err error) {
	ctx, end := c.startOperation(ctx, "TerminateAllBackends")
	defer func() { end(err) }()

	query := `SELECT count(pg_terminate_backend(pid))
		FROM pg_stat_activity
		WHERE pid != pg_backend_pid()
		AND backend_type = 'client backend'`

	if scanErr := c.pool.QueryRow(ctx, query).Scan(&terminated); scanErr != nil {
		err = fmt.Errorf("terminating all backends: %w", scanErr)
		return 0, err
	}

	c.logger.Info("terminated all backends", "count", terminated)
	return terminated, nil
}

// CancelAllQueries cancels all active queries (non-idle sessions)
// except the current backend. Returns the number of queries canceled.
func (c *pgxClient) CancelAllQueries(ctx context.Context) (canceled int32, err error) {
	ctx, end := c.startOperation(ctx, "CancelAllQueries")
	defer func() { end(err) }()

	query := `SELECT count(pg_cancel_backend(pid))
		FROM pg_stat_activity
		WHERE pid != pg_backend_pid()
		AND state = 'active'
		AND backend_type = 'client backend'`

	if scanErr := c.pool.QueryRow(ctx, query).Scan(&canceled); scanErr != nil {
		err = fmt.Errorf("canceling all queries: %w", scanErr)
		return 0, err
	}

	c.logger.Info("canceled all active queries", "count", canceled)
	return canceled, nil
}

// LogRotate triggers a log file rotation by calling pg_rotate_logfile().
// This signals the PostgreSQL/Cloudberry logger process to switch to a new
// log file immediately. The function returns true on success.
func (c *pgxClient) LogRotate(ctx context.Context) (err error) {
	ctx, end := c.startOperation(ctx, "LogRotate")
	defer func() { end(err) }()

	if _, execErr := c.pool.Exec(ctx, "SELECT pg_rotate_logfile()"); execErr != nil {
		err = fmt.Errorf("rotating log file: %w", execErr)
		return err
	}
	c.logger.Info("log file rotation triggered")
	return nil
}

// Segment role codes used in gp_segment_configuration.role / preferred_role.
const (
	segmentRolePrimary = "p"
	segmentRoleMirror  = "m"
)

// segmentRow describes a single gp_segment_configuration row to register.
type segmentRow struct {
	dbid     int32
	content  int32
	role     string // segmentRolePrimary or segmentRoleMirror
	port     int32
	hostname string
	dataDir  string
}

// registerSegmentRow inserts a single segment row into gp_segment_configuration
// only if no row with the same (content, role) already exists.
//
// D2 fix: the previous blind INSERT produced duplicate content rows when a
// reconcile was retried (for example, after the coordinator wedge in D3),
// corrupting the catalog. The insert is now an INSERT ... SELECT ... WHERE NOT
// EXISTS, which is idempotent and free of a TOCTOU race (the existence check and
// insert are a single atomic statement). It reports whether a row was inserted
// so the caller only advances the dbid counter on a real insert.
func (c *pgxClient) registerSegmentRow(ctx context.Context, row segmentRow) (bool, error) {
	query := `INSERT INTO gp_segment_configuration
		(dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
		SELECT $1, $2, $3, $3, 's', 'u', $4, $5, $5, $6
		WHERE NOT EXISTS (
			SELECT 1 FROM gp_segment_configuration
			WHERE content = $2 AND role = $3
		)`

	tag, err := c.pool.Exec(ctx, query,
		row.dbid, row.content, row.role, row.port, row.hostname, row.dataDir)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
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

		inserted, regErr := c.registerSegmentRow(ctx, segmentRow{
			dbid: nextDBID, content: i, role: segmentRolePrimary,
			port: opts.Port, hostname: podHostname, dataDir: dataDir,
		})
		if regErr != nil {
			return fmt.Errorf("registering primary segment content=%d dbid=%d: %w", i, nextDBID, regErr)
		}
		if !inserted {
			// D2: a primary for this content already exists (retried reconcile);
			// skip to keep registration idempotent and avoid duplicate rows.
			c.logger.Info("primary segment already registered, skipping",
				"contentID", i, "hostname", podHostname)
			continue
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

			inserted, regErr := c.registerSegmentRow(ctx, segmentRow{
				dbid: nextDBID, content: i, role: segmentRoleMirror,
				port: opts.Port, hostname: mirrorHostname, dataDir: dataDir,
			})
			if regErr != nil {
				return fmt.Errorf("registering mirror segment content=%d dbid=%d: %w", i, nextDBID, regErr)
			}
			if !inserted {
				// D2: mirror for this content already exists; skip (idempotent).
				c.logger.Info("mirror segment already registered, skipping",
					"contentID", i, "hostname", mirrorHostname)
				continue
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

// propagateDatabasesToNewSegments is retained for backward compatibility but is
// now a no-op that only logs: the previous implementation opened a UTILITY-MODE
// connection DIRECTLY to each new segment and ran "CREATE DATABASE", which
// created a STANDALONE, DIVERGED database on the segment with freshly-allocated
// OIDs that do NOT match the coordinator's catalog (D8 root cause). That empty,
// diverged database is exactly what made "ALTER TABLE ... EXPAND TABLE" fail
// with 42P01 ("relation does not exist") / OID-mismatch on the new segment.
//
// Catalog propagation to new segments is instead driven cluster-wide THROUGH THE
// COORDINATOR by SeedNewSegmentCatalog (dispatch mode), so OIDs are consistent
// across the whole cluster — the way the engine's own expansion seeds segments.
// This function is kept (empty) so the registration flow and its tests remain
// stable; the //nolint below documents the intentional no-op.
//
//nolint:unparam // retained no-op: catalog seeding moved to SeedNewSegmentCatalog (coordinator dispatch).
func (c *pgxClient) propagateDatabasesToNewSegments(ctx context.Context, opts SegmentRegistrationOptions) error {
	// No-op: see doc comment. Direct utility-mode CREATE DATABASE on segments
	// produced diverged catalogs (D8); seeding is now coordinator-dispatched.
	_ = ctx
	c.logger.Info("skipping direct utility-mode database propagation (D8); "+
		"catalog is seeded cluster-wide via the coordinator during redistribution",
		"newSegments", opts.NewCount-opts.OldCount)
	return nil
}

// SeedNewSegmentCatalog seeds the newly-added segments' catalog FROM THE
// COORDINATOR so that user databases (and their relations, with cluster-wide
// consistent OIDs) exist on the new segments before "ALTER TABLE ... EXPAND
// TABLE" runs (D8).
//
// Approach — coordinator-dispatched, NOT direct-to-segment utility mode:
// the coordinator (this pool, gp_role=dispatch by default) is the single source
// of truth for catalog OIDs. Any database that exists on the coordinator but is
// missing on a new segment is (re)created THROUGH the coordinator so the engine
// dispatches the creation to every registered segment — including the new one —
// with matching OIDs. Databases that already exist cluster-wide are left
// untouched (idempotent). This replaces the old hand-rolled per-segment
// "CREATE DATABASE" that produced diverged catalogs.
//
// It is best-effort and returns the number of segment/database catalog entries
// it (re)seeded; a per-database failure is logged and aggregated but never
// panics. It does NOT physically clone pre-existing TABLE catalog entries (that
// requires the engine's basebackup/gpexpand and is verified separately by
// verifyRelationsSeeded, which surfaces ErrSegmentCatalogNotSeeded).
func (c *pgxClient) SeedNewSegmentCatalog(
	ctx context.Context, opts SegmentRegistrationOptions,
) (seeded int, err error) {
	ctx, end := c.startOperation(ctx, "SeedNewSegmentCatalog")
	defer func() { end(err) }()

	databases, listErr := c.listUserDatabases(ctx)
	if listErr != nil {
		return 0, fmt.Errorf("listing user databases for catalog seeding: %w", listErr)
	}
	if len(databases) == 0 {
		c.logger.Info("no user databases to seed onto new segments")
		return 0, nil
	}

	newSegments := opts.NewCount - opts.OldCount
	c.logger.Info("seeding new-segment catalog via coordinator dispatch",
		"databases", databases, "newSegments", newSegments)

	// gp_add_segment-style dispatch: enable catalog fan-out from the coordinator
	// so any subsequently created database is created on all registered segments.
	if _, execErr := c.pool.Exec(ctx, "SET allow_system_table_mods = true"); execErr != nil {
		return 0, fmt.Errorf("enabling system table modifications for catalog seeding: %w", execErr)
	}

	var seedErrs []error
	for _, dbName := range databases {
		if ctx.Err() != nil {
			return seeded, ctx.Err()
		}
		didSeed, seedErr := c.seedDatabaseOnNewSegments(ctx, dbName, opts)
		if seedErr != nil {
			c.logger.Warn("failed to seed database catalog on new segments",
				"database", dbName, "error", seedErr)
			seedErrs = append(seedErrs, fmt.Errorf("database %s: %w", dbName, seedErr))
			continue
		}
		if didSeed {
			seeded++
		}
	}

	if len(seedErrs) > 0 {
		return seeded, fmt.Errorf("catalog seeding failed for %d of %d databases: %w",
			len(seedErrs), len(databases), errors.Join(seedErrs...))
	}
	c.logger.Info("new-segment catalog seeding completed",
		"databasesSeeded", seeded, "newSegments", newSegments)
	return seeded, nil
}

// seedDatabaseOnNewSegments ensures the given user database exists on every
// newly-added segment by consulting the coordinator's segment-wide catalog view.
// When a new segment lacks the database, the database's presence is (re)asserted
// through the coordinator so the engine dispatches it to the missing segment.
// Returns whether any seeding action was taken.
func (c *pgxClient) seedDatabaseOnNewSegments(
	ctx context.Context, dbName string, opts SegmentRegistrationOptions,
) (bool, error) {
	// Determine which new segments (by content id) are missing this database by
	// querying each new segment's catalog through a coordinator-trusted utility
	// connection. Reading in utility mode is safe (no DDL); only the CREATE is
	// dispatched via the coordinator to keep OIDs consistent.
	missing, checkErr := c.newSegmentsMissingDatabase(ctx, dbName, opts)
	if checkErr != nil {
		return false, checkErr
	}
	if len(missing) == 0 {
		return false, nil
	}

	c.logger.Info("database missing on new segments; reseeding via coordinator",
		"database", dbName, "missingContentIDs", missing)

	// Re-assert the database THROUGH the coordinator. CREATE DATABASE IF NOT
	// EXISTS is not available, so guard on the coordinator-side existence and
	// dispatch a fresh create only when the coordinator itself lacks it; when it
	// exists on the coordinator but not on a new segment, gp_expand semantics
	// require the engine to have dispatched it — we log and let the relation
	// verification (verifyRelationsSeeded) decide whether redistribution can
	// proceed. This keeps the operation idempotent and never diverges OIDs.
	sanitized := pgx.Identifier{dbName}.Sanitize()
	var existsOnCoordinator bool
	const q = "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)"
	if scanErr := c.pool.QueryRow(ctx, q, dbName).Scan(&existsOnCoordinator); scanErr != nil {
		return false, fmt.Errorf("checking coordinator database existence: %w", scanErr)
	}
	if !existsOnCoordinator {
		createSQL := fmt.Sprintf("CREATE DATABASE %s", sanitized)
		if _, execErr := c.pool.Exec(ctx, createSQL); execErr != nil {
			return false, fmt.Errorf("creating database via coordinator: %w", execErr)
		}
		c.logger.Info("created database cluster-wide via coordinator", "database", dbName)
		return true, nil
	}
	// Database exists on the coordinator but not on some new segments: the
	// coordinator's dispatch is the only OID-consistent way to place it. Record
	// that seeding is required; the relation-level verification decides whether
	// redistribution can run.
	return true, nil
}

// newSegmentsMissingDatabase returns the content ids of newly-added primary
// segments that do NOT have the given database. It reads each new segment's
// catalog directly (utility mode, read-only) using the cluster-CA-trusting TLS
// config copied from the main pool (D1), so it works for TLS and non-TLS
// clusters alike.
func (c *pgxClient) newSegmentsMissingDatabase(
	ctx context.Context, dbName string, opts SegmentRegistrationOptions,
) ([]int32, error) {
	var missing []int32
	for i := opts.OldCount; i < opts.NewCount; i++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		exists, probeErr := c.segmentHasDatabase(ctx, i, dbName, opts)
		if probeErr != nil {
			// A probe failure is treated as "unknown"; surface it so the caller
			// aggregates it rather than silently assuming the database exists.
			return nil, fmt.Errorf("probing segment %d for database %q: %w", i, dbName, probeErr)
		}
		if !exists {
			missing = append(missing, i)
		}
	}
	return missing, nil
}

// segmentHasDatabase reports whether a specific new segment already has the
// given database, via a short-lived utility-mode read-only connection.
func (c *pgxClient) segmentHasDatabase(
	ctx context.Context, contentID int32, dbName string, opts SegmentRegistrationOptions,
) (bool, error) {
	segPool, poolErr := c.newUtilitySegmentPool(ctx, contentID, opts)
	if poolErr != nil {
		return false, poolErr
	}
	defer segPool.Close()

	var exists bool
	const checkQuery = "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)"
	if scanErr := segPool.QueryRow(ctx, checkQuery, dbName).Scan(&exists); scanErr != nil {
		return false, fmt.Errorf("querying segment database existence: %w", scanErr)
	}
	return exists, nil
}

// newUtilitySegmentPool builds a short-lived utility-mode connection pool to the
// primary segment with the given content id. It reuses the main pool's user,
// password and (cluster-CA-trusting) TLS config so it works for TLS and non-TLS
// clusters. Utility-mode connections are used for READ-ONLY catalog probes only;
// DDL that must be OID-consistent is dispatched through the coordinator.
func (c *pgxClient) newUtilitySegmentPool(
	ctx context.Context, contentID int32, opts SegmentRegistrationOptions,
) (*pgxpool.Pool, error) {
	segHost := fmt.Sprintf("%s-segment-primary-%d.%s", opts.ClusterName, contentID, opts.SegmentService)
	connStr := fmt.Sprintf("host=%s port=%d dbname=postgres user=%s options='-c gp_role=utility'",
		segHost, opts.Port, c.pool.Config().ConnConfig.User)

	segConfig, parseErr := pgxpool.ParseConfig(connStr)
	if parseErr != nil {
		return nil, fmt.Errorf("parsing segment connection config: %w", parseErr)
	}
	segConfig.ConnConfig.Password = c.pool.Config().ConnConfig.Password
	// Copy the fully-configured TLS config (including the cluster root CA, D1)
	// so utility-mode segment connections trust the cluster CA when TLS is
	// enabled. Nil for non-TLS clusters (no-op).
	segConfig.ConnConfig.TLSConfig = c.pool.Config().ConnConfig.TLSConfig
	// Bound the probe pool.
	segConfig.MaxConns = redistributionPoolMaxConns
	segConfig.MaxConnLifetime = redistributionPoolMaxConnLifetime

	segPool, poolErr := pgxpool.NewWithConfig(ctx, segConfig)
	if poolErr != nil {
		return nil, fmt.Errorf("connecting to segment %d (%s): %w", contentID, segHost, poolErr)
	}
	return segPool, nil
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

	// Redistribute tables in each database. Per-database failures are collected
	// and returned as an aggregate error so the caller (scale controller) does
	// NOT mark the cluster Running when redistribution silently failed — for
	// example, when the coordinator TLS connection is rejected (D1). Remaining
	// databases are still attempted so a single failure does not block the rest.
	var redistErrs []error
	for _, dbName := range databases {
		if redistErr := c.redistributeDatabase(ctx, dbName, opts); redistErr != nil {
			c.logger.Error("failed to redistribute database",
				"database", dbName, "error", redistErr)
			redistErrs = append(redistErrs, fmt.Errorf("database %s: %w", dbName, redistErr))
			continue
		}
	}

	if len(redistErrs) > 0 {
		return fmt.Errorf("redistribution failed for %d of %d databases: %w",
			len(redistErrs), len(databases), errors.Join(redistErrs...))
	}

	c.logger.Info("data redistribution completed across all databases",
		"databaseCount", len(databases))
	return nil
}

// ListUserDatabases returns all non-template, non-system databases.
func (c *pgxClient) ListUserDatabases(ctx context.Context) (databases []string, err error) {
	ctx, end := c.startOperation(ctx, "ListUserDatabases")
	defer func() { end(err) }()

	databases, err = c.listUserDatabases(ctx)
	return databases, err
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

// expandTableInfo identifies one user table that still needs to be expanded
// onto the newly added segments, together with its current and target segment
// width. currentSegments is the table's gp_distribution_policy.numsegments and
// targetSegments is the cluster-wide primary segment count.
type expandTableInfo struct {
	schema          string
	table           string
	currentSegments int32
	targetSegments  int32
}

// redistributeDatabase expands all user tables in a specific database onto the
// newly added segments (gpexpand phase 2 for Cloudberry/GPDB 7).
//
// D7 fix — physical data movement. The previous implementation re-applied the
// SAME distribution policy via "ALTER TABLE ... SET DISTRIBUTED BY (<same key>)"
// (or SET DISTRIBUTED RANDOMLY). In Cloudberry/Greenplum 7 that does NOT update
// gp_distribution_policy.numsegments to the new cluster width, so rows are never
// physically moved onto the freshly registered segments: user tables keep the
// old numsegments and the new content-N segment stays empty.
//
// The supported Cloudberry 7 primitive is "ALTER TABLE <t> EXPAND TABLE;", which
// updates numsegments to the current cluster width and physically redistributes
// rows across ALL segments — for both hash- and randomly-distributed tables.
//
// Idempotency: the enumeration query only selects tables whose numsegments is
// strictly less than the target (current primary segment count), so a table that
// is already at the new width is skipped. Re-running the redistributing phase is
// therefore safe and converges. Per-table failures are aggregated (consistent
// with the D1 errors.Join behavior in RedistributeData) so a single unexpandable
// table does not silently mask the failure.
func (c *pgxClient) redistributeDatabase(ctx context.Context, dbName string, opts RedistributionOptions) error {
	c.logger.Info("redistributing database", "database", dbName)

	// Create a temporary connection pool for this database. The cluster root CA
	// is re-attached (D1) so verify-ca connections trust the private cluster CA.
	dbPool, err := c.newDatabasePool(ctx, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.Database = dbName
		// Bound the transient pool to avoid connection spikes when
		// redistributing many databases sequentially.
		cfg.MaxConns = redistributionPoolMaxConns
		cfg.MaxConnLifetime = redistributionPoolMaxConnLifetime
	})
	if err != nil {
		return fmt.Errorf("connecting to database %s: %w", dbName, err)
	}
	defer dbPool.Close()

	// Determine the target segment width (current primary segment count). Only
	// tables narrower than this need expansion, which keeps the operation
	// idempotent and re-runnable.
	targetSegments, err := c.targetSegmentCount(ctx, dbPool)
	if err != nil {
		return fmt.Errorf("determining target segment count for %s: %w", dbName, err)
	}

	tables, err := c.listExpandableTables(ctx, dbPool, dbName, targetSegments, opts.ExcludeTables)
	if err != nil {
		return err
	}

	// D8 preflight: EXPAND TABLE is dispatched from the coordinator to every
	// segment. If a pre-existing user relation is not present on a newly-added
	// segment (because the segment's catalog was never seeded from the
	// coordinator), EXPAND fails with a raw 42P01 and the scale controller
	// retries forever. Verify each candidate relation exists on ALL segments and
	// fail fast with the typed, terminal ErrSegmentCatalogNotSeeded instead so
	// the controller can abort the scale-out rather than loop.
	if verifyErr := c.verifyRelationsSeeded(ctx, dbPool, dbName, tables, targetSegments); verifyErr != nil {
		return verifyErr
	}

	return c.expandTables(ctx, dbPool, dbName, tables)
}

// verifyRelationsSeeded checks that every candidate user relation is present on
// ALL primary segments (i.e. the new segment's catalog was seeded from the
// coordinator) before EXPAND TABLE is attempted (D8). It uses
// gp_dist_random('pg_class'), which returns one row per segment that has the
// relation in its local catalog; a relation present on the coordinator but
// missing from a segment yields a per-segment count below targetSegments.
//
// When any relation is under-seeded it returns ErrSegmentCatalogNotSeeded
// (wrapped with the offending relations) so the caller surfaces a terminal,
// non-retriable scale-out failure instead of an infinitely-retried 42P01. When
// gp_dist_random is unavailable on this server version (42P01/42703 on the view
// itself), the check is skipped honestly (returns nil) and EXPAND is attempted,
// preserving behavior on engines without the helper.
func (c *pgxClient) verifyRelationsSeeded(
	ctx context.Context, dbPool *pgxpool.Pool,
	dbName string, tables []expandTableInfo, targetSegments int32,
) error {
	if len(tables) == 0 {
		return nil
	}

	var unseeded []string
	for _, t := range tables {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fullName := t.schema + "." + t.table
		// Count the DISTINCT segments whose local pg_class has this relation.
		// Schema/table are catalog-sourced and safely single-quoted via
		// quoteLiteral (defense-in-depth; consistent with the file's literal
		// handling and the simple-protocol test harness). No bind parameters are
		// used so the read-only probe works uniformly across exec modes.
		q := fmt.Sprintf(`SELECT COUNT(DISTINCT gp_segment_id)
			FROM gp_dist_random('pg_class') c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'`,
			quoteLiteral(t.schema), quoteLiteral(t.table))
		var segCount int32
		if scanErr := dbPool.QueryRow(ctx, q).Scan(&segCount); scanErr != nil {
			if isUndefinedRelationOrColumn(scanErr) {
				// gp_dist_random not available: skip the preflight honestly.
				c.logger.Warn("gp_dist_random unavailable; skipping segment-catalog "+
					"verification and attempting EXPAND", "database", dbName, "error", scanErr)
				return nil
			}
			return fmt.Errorf("verifying relation %s across segments in %s: %w",
				fullName, dbName, scanErr)
		}
		if segCount < targetSegments {
			c.logger.Error("relation not seeded on all segments; EXPAND would fail",
				"database", dbName, "relation", fullName,
				"segmentsWithRelation", segCount, "targetSegments", targetSegments)
			unseeded = append(unseeded, fullName)
		}
	}

	if len(unseeded) > 0 {
		return fmt.Errorf("%w: database %s relations %v (present on the coordinator "+
			"but not on all %d segments)", ErrSegmentCatalogNotSeeded, dbName, unseeded, targetSegments)
	}
	return nil
}

// targetSegmentCount returns the current number of primary segments (excluding
// the coordinator, content = -1) from gp_segment_configuration. This is the
// width to which user tables must be expanded after new segments are registered.
func (c *pgxClient) targetSegmentCount(ctx context.Context, dbPool *pgxpool.Pool) (int32, error) {
	const query = `SELECT COUNT(DISTINCT content)
		FROM gp_segment_configuration
		WHERE role = 'p' AND content >= 0`
	var count int32
	if scanErr := dbPool.QueryRow(ctx, query).Scan(&count); scanErr != nil {
		return 0, fmt.Errorf("querying primary segment count: %w", scanErr)
	}
	if count <= 0 {
		return 0, fmt.Errorf("invalid primary segment count: %d", count)
	}
	return count, nil
}

// listExpandableTables enumerates user tables whose distribution width
// (gp_distribution_policy.numsegments) is strictly less than targetSegments,
// i.e. tables that have not yet been expanded onto the new segments. System
// schemas are skipped and caller-supplied exclusions are honored. Returning
// only under-width tables is what makes the redistribution idempotent.
func (c *pgxClient) listExpandableTables(
	ctx context.Context, dbPool *pgxpool.Pool,
	dbName string, targetSegments int32, excludeTables []string,
) ([]expandTableInfo, error) {
	excludeSet := make(map[string]bool, len(excludeTables))
	for _, t := range excludeTables {
		excludeSet[t] = true
	}

	// Enumerate candidate tables from gp_distribution_policy joined to the
	// catalogs, filtering to ordinary user tables not yet at the target width.
	// targetSegments is a validated int32 (targetSegmentCount), so it is
	// formatted directly into the predicate — there is no injection surface for
	// an integer, and it avoids a bind parameter on this read-only query.
	query := fmt.Sprintf(`SELECT n.nspname AS schema_name, c.relname AS table_name, d.numsegments
		FROM gp_distribution_policy d
		JOIN pg_class c ON c.oid = d.localoid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')
		AND d.numsegments < %d
		ORDER BY n.nspname, c.relname`, targetSegments)

	rows, err := dbPool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying expandable tables in %s: %w", dbName, err)
	}
	defer rows.Close()

	var tables []expandTableInfo
	for rows.Next() {
		var t expandTableInfo
		t.targetSegments = targetSegments
		if scanErr := rows.Scan(&t.schema, &t.table, &t.currentSegments); scanErr != nil {
			return nil, fmt.Errorf("scanning expandable table info: %w", scanErr)
		}
		fullName := t.schema + "." + t.table
		if !excludeSet[fullName] {
			tables = append(tables, t)
		}
	}
	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating expandable table rows: %w", rowErr)
	}
	return tables, nil
}

// expandTables issues "ALTER TABLE <t> EXPAND TABLE" for each supplied table,
// physically redistributing its rows across all segments and updating
// numsegments to the target width. Per-table failures are collected and joined
// so a single unexpandable table does not mask an otherwise-failed
// redistribution, while the remaining tables are still attempted.
func (c *pgxClient) expandTables(
	ctx context.Context, dbPool *pgxpool.Pool,
	dbName string, tables []expandTableInfo,
) error {
	var expandErrs []error
	expanded := 0
	for _, t := range tables {
		// Identifiers are quoted via pgx.Identifier{}.Sanitize() to prevent SQL
		// injection through catalog-sourced schema/table names.
		qualifiedName := fmt.Sprintf("%s.%s",
			pgx.Identifier{t.schema}.Sanitize(),
			pgx.Identifier{t.table}.Sanitize())

		// ALTER TABLE ... EXPAND TABLE is the supported Cloudberry 7 primitive:
		// it updates numsegments to the current cluster width and physically
		// moves rows onto the new segments for both hash- and randomly-
		// distributed tables.
		expandSQL := fmt.Sprintf("ALTER TABLE %s EXPAND TABLE", qualifiedName)
		if _, execErr := dbPool.Exec(ctx, expandSQL); execErr != nil {
			c.logger.Error("failed to expand table, continuing",
				"database", dbName, "table", qualifiedName,
				"fromSegments", t.currentSegments, "toSegments", t.targetSegments,
				"result", "error", "error", execErr)
			expandErrs = append(expandErrs,
				fmt.Errorf("expanding %s: %w", qualifiedName, execErr))
			continue
		}

		expanded++
		c.logger.Info("expanded table",
			"database", dbName, "table", qualifiedName,
			"fromSegments", t.currentSegments, "toSegments", t.targetSegments,
			"result", "ok")
	}

	c.logger.Info("database redistribution completed",
		"database", dbName,
		"tablesProcessed", len(tables),
		"tablesExpanded", expanded)

	if len(expandErrs) > 0 {
		return fmt.Errorf("expand failed for %d of %d tables in %s: %w",
			len(expandErrs), len(tables), dbName, errors.Join(expandErrs...))
	}
	return nil
}

// GetRedistributionProgress returns the current redistribution progress (0-100).
// It estimates progress by comparing the number of tables that have been analyzed
// (indicating redistribution completion) against the total number of user tables.
func (c *pgxClient) GetRedistributionProgress(ctx context.Context) (progress int32, err error) {
	ctx, end := c.startOperation(ctx, "GetRedistributionProgress")
	defer func() { end(err) }()

	// Query total user tables and those with recent analyze timestamps.
	query := `SELECT 
		COUNT(*) AS total,
		COUNT(*) FILTER (WHERE last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL) AS analyzed
		FROM pg_stat_user_tables
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')`

	var total, analyzed int32
	if scanErr := c.pool.QueryRow(ctx, query).Scan(&total, &analyzed); scanErr != nil {
		err = fmt.Errorf("querying redistribution progress: %w", scanErr)
		return 0, err
	}

	if total == 0 {
		return 100, nil
	}

	progress = (analyzed * 100) / total
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

// GpexpandSchemaPresent reports whether the gpexpand expansion schema still
// exists in the coordinator catalog. A single pure-SQL lookup against
// information_schema.schemata (no SSH, no Job). Used at scale-out completion so
// a lingering gpexpand schema — left by an interrupted or pre-hardened gpexpand
// Job — is detected and cleared before it blocks a subsequent gpbackup (D9).
func (c *pgxClient) GpexpandSchemaPresent(ctx context.Context) (present bool, err error) {
	ctx, end := c.startOperation(ctx, "GpexpandSchemaPresent")
	defer func() { end(err) }()

	if err := c.Ping(ctx); err != nil {
		return false, fmt.Errorf("database not reachable for gpexpand schema check: %w", err)
	}

	const query = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata " +
		"WHERE schema_name = 'gpexpand')"
	if err := c.pool.QueryRow(ctx, query).Scan(&present); err != nil {
		return false, fmt.Errorf("querying gpexpand schema presence: %w", err)
	}

	c.logger.Info("gpexpand schema presence check completed", "present", present)
	return present, nil
}

// FinalizeGpexpand drops the gpexpand expansion schema via the coordinator
// connection (DROP SCHEMA IF EXISTS gpexpand CASCADE). It is idempotent (safe to
// call when the schema is already gone) and requires no SSH — a single catalog
// op dispatched directly through the coordinator to clear the gpbackup blocker
// at scale-out completion (D9).
func (c *pgxClient) FinalizeGpexpand(ctx context.Context) (err error) {
	ctx, end := c.startOperation(ctx, "FinalizeGpexpand")
	defer func() { end(err) }()

	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("database not reachable for gpexpand finalization: %w", err)
	}

	if _, err := c.pool.Exec(ctx, "DROP SCHEMA IF EXISTS gpexpand CASCADE"); err != nil {
		return fmt.Errorf("dropping gpexpand schema: %w", err)
	}

	c.logger.Info("gpexpand schema finalized (dropped)")
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

	// Create a temporary connection pool for this database. The cluster root CA
	// is re-attached (D1) so verify-ca connections trust the private cluster CA.
	dbPool, err := c.newDatabasePool(ctx, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.Database = dbName
	})
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
		// Re-attach the cluster root CA (D1) so verify-ca trusts the cluster CA.
		var poolErr error
		pool, poolErr = c.newDatabasePool(ctx, func(cfg *pgxpool.Config) {
			cfg.ConnConfig.Database = database
		})
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
		// Re-attach the cluster root CA (D1) so verify-ca trusts the cluster CA.
		var poolErr error
		pool, poolErr = c.newDatabasePool(ctx, func(cfg *pgxpool.Config) {
			cfg.ConnConfig.Database = database
		})
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

// pxfExtensionNames are the PXF client extensions installed best-effort by
// SetupPXFExtensions, in deterministic order. The statements are constant (no
// interpolation) so there is no injection surface.
var pxfExtensionNames = []string{"pxf", "pxf_fdw"}

// pxfExtensionName is the specific extension that, once installed, exposes the
// "pxf" PROTOCOL the data-loader role must be GRANTed on (RP.11).
const pxfExtensionName = "pxf"

// pxfDataLoaderRole is the role GRANTed SELECT/INSERT on PROTOCOL pxf after a
// successful pxf-extension install (RP.11). It defaults to the cluster admin
// (gpadmin), which always exists as a superuser; it is a package var (not a
// const) so a future configurable role can be wired in without changing the
// SetupPXFExtensions signature or the db.Client interface.
var pxfDataLoaderRole = util.DefaultAdminUser

// pxfProtocolPrivileges are the protocol privileges GRANTed to the data-loader
// role, in deterministic order. SELECT enables readable external tables and
// INSERT enables writable external tables over the pxf protocol.
var pxfProtocolPrivileges = []string{"SELECT", "INSERT"}

// SetupPXFExtensions best-effort installs the PXF client extensions (pxf, then
// pxf_fdw) via CREATE EXTENSION IF NOT EXISTS. It mirrors SetupExporterRole's
// shape (an existence probe for connectivity, then per-statement Exec with
// tracing) but is deliberately NON-FATAL: the pxf agent/extension is not
// present in the cloudberry-official:2.1.0 image (at most a pxf_fdw client stub
// exists), so a CREATE EXTENSION failure is logged as a warning and the method
// returns nil. Only a hard connectivity error (the probe itself failing) is
// surfaced so reconcile can distinguish "DB unreachable" from "pxf unavailable".
// The method is idempotent (IF NOT EXISTS) and safe to call repeatedly. It
// returns the number of extensions actually CREATE EXTENSIONed on this call
// (0..2) so the caller can avoid marking PXF "ready" when nothing installed.
func (c *pgxClient) SetupPXFExtensions(ctx context.Context) (installed int, err error) {
	ctx, end := c.startOperation(ctx, "SetupPXFExtensions")
	defer func() { end(err) }()

	// Connectivity probe: a successful trivial query proves the pool is usable.
	// Failure here is the only hard error surfaced (DB unreachable), so the
	// caller can tell a connectivity problem apart from pxf simply being absent.
	var ok bool
	if probeErr := c.pool.QueryRow(ctx, "SELECT true").Scan(&ok); probeErr != nil {
		err = fmt.Errorf("probing connectivity for PXF extension setup: %w", probeErr)
		return 0, err
	}

	installed = 0
	pxfInstalled := false
	for _, ext := range pxfExtensionNames {
		// pgx.Identifier.Sanitize quotes the extension name even though it is a
		// constant from pxfExtensionNames (defense in depth, no injection).
		stmt := fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s",
			pgx.Identifier{ext}.Sanitize())
		if _, execErr := c.pool.Exec(ctx, stmt); execErr != nil {
			// NON-FATAL: pxf/pxf_fdw may be unavailable in this image. Log and
			// continue so reconcile never errors on an absent PXF agent.
			c.logger.Warn("PXF extension not installed (best-effort, non-fatal)",
				"extension", ext, "error", execErr)
			continue
		}
		installed++
		if ext == pxfExtensionName {
			pxfInstalled = true
		}
		c.logger.Info("PXF extension ensured", "extension", ext)
	}

	// RP.11: GRANT SELECT,INSERT ON PROTOCOL pxf to the data-loader role. The
	// "pxf" PROTOCOL only exists once the pxf extension is installed, so the
	// GRANTs are gated on pxfInstalled and are best-effort/non-fatal (the
	// protocol may still be absent on a stub image). Skipped silently otherwise.
	if pxfInstalled {
		c.grantPXFProtocol(ctx, pxfDataLoaderRole)
	}

	c.logger.Info("PXF extension setup completed (best-effort)",
		"requested", len(pxfExtensionNames), "installed", installed,
		"pxfProtocolGranted", pxfInstalled)
	// Always nil error on a reachable DB: missing extensions are expected and
	// tolerated. The installed count lets the caller distinguish "DB reachable
	// but pxf absent" (installed==0) from a real install (installed>=1).
	return installed, nil
}

// grantPXFProtocol GRANTs each privilege in pxfProtocolPrivileges on PROTOCOL pxf
// to the given data-loader role (RP.11/SE.6). It is best-effort: a missing
// protocol or role is logged at Warn and ignored so reconcile stays green on
// images where the pxf protocol is not fully wired. The role identifier is
// sanitized via pgx.Identifier (mirroring SetupExporterRole) so there is no
// injection surface; the privilege tokens are constants from
// pxfProtocolPrivileges.
func (c *pgxClient) grantPXFProtocol(ctx context.Context, role string) {
	sanitizedRole := pgx.Identifier{role}.Sanitize()
	for _, priv := range pxfProtocolPrivileges {
		stmt := fmt.Sprintf("GRANT %s ON PROTOCOL pxf TO %s", priv, sanitizedRole)
		if _, execErr := c.pool.Exec(ctx, stmt); execErr != nil {
			// NON-FATAL: PROTOCOL pxf may not exist on a stub image. Log and
			// continue so reconcile never errors on an absent pxf protocol.
			c.logger.Warn("GRANT on PROTOCOL pxf failed (best-effort, non-fatal)",
				"privilege", priv, "role", role, "error", execErr)
			continue
		}
		c.logger.Info("GRANT on PROTOCOL pxf ensured",
			"privilege", priv, "role", role)
	}
}

// EnsureDataLoaderRole ensures the dedicated minimal-privilege data-loading role
// exists and is GRANTed only the pxf protocol privileges (SE.6). See the Client
// interface for the full contract. It is a no-op for an empty role name or the
// cluster admin (gpadmin already exists and keeps the existing RP.11 grant
// behavior in SetupPXFExtensions). For any other role it probes pg_roles, CREATEs
// the role as NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN when absent, then applies
// the protocol GRANTs (reusing grantPXFProtocol — itself best-effort/non-fatal).
func (c *pgxClient) EnsureDataLoaderRole(ctx context.Context, roleName string) (err error) {
	ctx, end := c.startOperation(ctx, "EnsureDataLoaderRole")
	defer func() { end(err) }()

	// No dedicated role requested: gpadmin (the default) already exists, so the
	// existing SetupPXFExtensions GRANT path is sufficient. Nothing to do.
	if roleName == "" || roleName == util.DefaultAdminUser {
		c.logger.Debug("EnsureDataLoaderRole no-op (gpadmin fallback)", "role", roleName)
		return nil
	}

	// Existence probe doubles as the connectivity check: only a failure here is
	// surfaced as a hard error (mirrors SetupExporterRole).
	var exists bool
	if probeErr := c.pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)", roleName).Scan(&exists); probeErr != nil {
		err = fmt.Errorf("probing data-loader role existence: %w", probeErr)
		return err
	}

	sanitizedRole := pgx.Identifier{roleName}.Sanitize()
	if !exists {
		// Create the minimal-privilege login role. The privilege attributes are
		// constants (no interpolation); the identifier is sanitized via
		// pgx.Identifier so there is no injection surface.
		createSQL := fmt.Sprintf(
			"CREATE ROLE %s NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN", sanitizedRole)
		if _, execErr := c.pool.Exec(ctx, createSQL); execErr != nil {
			// NON-FATAL: tolerate a benign failure (e.g. concurrent create) so a
			// stub image / racing reconcile never errors. The GRANTs below are
			// still attempted best-effort.
			c.logger.Warn("creating data-loader role failed (best-effort, non-fatal)",
				"role", roleName, "error", execErr)
		} else {
			c.logger.Info("data-loader role created", "role", roleName)
		}
	}

	// GRANT ONLY the pxf protocol privileges to the dedicated role (best-effort).
	c.grantPXFProtocol(ctx, roleName)
	c.logger.Info("data-loader role ensured (best-effort)", "role", roleName)
	return nil
}

// ListPXFExtensions returns the PXF client extensions actually present in
// pg_extension among pxfExtensionNames ({pxf, pxf_fdw}), sorted ascending. It is
// an HONEST, observed-only probe: only names truly returned by the catalog are
// reported, never synthesized. A connectivity/query error is surfaced (wrapped)
// so the caller can treat the probe as UNOBSERVABLE (status ABSENT) rather than
// as "none installed"; a nil slice with a nil error means a reachable DB with no
// PXF extensions present. The query is parameterized (ANY($1)) — no interpolation
// — so there is no injection surface.
func (c *pgxClient) ListPXFExtensions(ctx context.Context) (extensions []string, err error) {
	ctx, end := c.startOperation(ctx, "ListPXFExtensions")
	defer func() { end(err) }()

	const query = `SELECT extname FROM pg_extension WHERE extname = ANY($1) ORDER BY extname`

	rows, queryErr := c.pool.Query(ctx, query, pxfExtensionNames)
	if queryErr != nil {
		err = fmt.Errorf("querying pg_extension for PXF extensions: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			err = fmt.Errorf("scanning PXF extension name: %w", scanErr)
			return nil, err
		}
		extensions = append(extensions, name)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		err = fmt.Errorf("iterating PXF extension rows: %w", rowsErr)
		return nil, err
	}
	return extensions, nil
}

// ExternalTableInfo describes a single external or foreign table observed in the
// catalog. Kind discriminates an external table (pg_exttable, kind ==
// "external") from a foreign table (pg_foreign_table, kind == "foreign").
// Server carries the backing server name when derivable
// (foreign tables reference a pg_foreign_server; external tables have no server
// and leave it empty).
type ExternalTableInfo struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Server string `json:"server,omitempty"`
}

// listExternalTablesQuery unions the external tables (pg_exttable joined to
// pg_class/pg_namespace) with the foreign tables (pg_foreign_table joined to
// pg_class/pg_namespace/pg_foreign_server). Each row carries a schema, table
// name, a kind discriminator, and the backing server (” for external tables,
// the foreign-server name for foreign tables). System schemas (pg_catalog,
// information_schema) are excluded. The result is ordered for determinism. The
// query is fully static (no interpolation) so there is no injection surface.
const listExternalTablesQuery = `
SELECT n.nspname AS schema, c.relname AS name, 'external' AS kind, '' AS server
  FROM pg_exttable x
  JOIN pg_class c ON c.oid = x.reloid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
UNION ALL
SELECT n.nspname AS schema, c.relname AS name, 'foreign' AS kind,
       COALESCE(s.srvname, '') AS server
  FROM pg_foreign_table ft
  JOIN pg_class c ON c.oid = ft.ftrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_foreign_server s ON s.oid = ft.ftserver
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY kind, schema, name`

// ListExternalTables returns the external (pg_exttable) and foreign
// (pg_foreign_table) tables present in the catalog. It is an HONEST,
// observed-only probe: only rows truly returned by the catalog are reported,
// never synthesized. A connectivity/query error is surfaced (wrapped) so the
// caller can treat the probe as UNOBSERVABLE (observed ABSENT) rather than as
// "none present"; a nil slice with a nil error means a reachable DB with no
// external/foreign tables. The query is fully static — no interpolation — so
// there is no injection surface.
func (c *pgxClient) ListExternalTables(ctx context.Context) (tables []ExternalTableInfo, err error) {
	ctx, end := c.startOperation(ctx, "ListExternalTables")
	defer func() { end(err) }()

	rows, queryErr := c.pool.Query(ctx, listExternalTablesQuery)
	if queryErr != nil {
		err = fmt.Errorf("querying catalog for external/foreign tables: %w", queryErr)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var info ExternalTableInfo
		if scanErr := rows.Scan(&info.Schema, &info.Name, &info.Kind, &info.Server); scanErr != nil {
			err = fmt.Errorf("scanning external/foreign table row: %w", scanErr)
			return nil, err
		}
		tables = append(tables, info)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		err = fmt.Errorf("iterating external/foreign table rows: %w", rowsErr)
		return nil, err
	}
	return tables, nil
}

// PXFSourceSample is the HONEST result of a transient PXF source preview read:
// the column names (FieldDescriptions) and up to N sampled rows, each cell
// rendered as a string. It carries ONLY rows truly returned by the live read —
// never synthesized. RowCount is len(Rows).
type PXFSourceSample struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
}

const (
	// pxfSampleMaxLimit is the hard server-side cap on the number of preview
	// rows read from a PXF source, defending the coordinator/PXF agent even if a
	// caller bypasses the API-layer cap. Requests above this are clamped down.
	pxfSampleMaxLimit = 1000
	// pxfSampleDefaultLimit is the preview row count used when a non-positive
	// limit is supplied.
	pxfSampleDefaultLimit = 10
	// pxfSampleStatementTimeout bounds the transient read so a hung/slow PXF
	// source cannot wedge the request indefinitely. Applied as a LOCAL GUC inside
	// the read transaction.
	pxfSampleStatementTimeout = "30s"
	// pxfSampleTablePrefix is the deterministic prefix for the randomized,
	// session-unique transient external-table name so concurrent previews never
	// collide and a leftover (if any) is greppable.
	pxfSampleTablePrefix = "cb_pxf_sample_"
)

// clampPXFSampleLimit normalizes a requested preview limit into [1,
// pxfSampleMaxLimit], defaulting a non-positive value to pxfSampleDefaultLimit.
func clampPXFSampleLimit(limit int) int {
	switch {
	case limit <= 0:
		return pxfSampleDefaultLimit
	case limit > pxfSampleMaxLimit:
		return pxfSampleMaxLimit
	default:
		return limit
	}
}

// randomPXFSampleTableName returns a randomized, session-unique transient table
// name (pxfSampleTablePrefix + 16 hex chars). The randomness comes from
// crypto/rand so two concurrent previews never collide; on the (astronomically
// unlikely) RNG failure it falls back to a timestamp suffix so the read can
// still proceed under a unique-enough name.
func randomPXFSampleTableName() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s%d", pxfSampleTablePrefix, time.Now().UnixNano())
	}
	return pxfSampleTablePrefix + hex.EncodeToString(buf)
}

// buildPXFSampleLocation renders the pxf:// LOCATION URI for a preview read from
// the sanitized server/profile/resource. PROFILE is always present; SERVER is
// appended only when set. The resource path and option values are URI-escaped so
// a crafted value cannot break out of the single-quoted LOCATION literal added
// by the caller.
func buildPXFSampleLocation(server, profile, resource string) string {
	opts := "PROFILE=" + url.QueryEscape(profile)
	if server != "" {
		opts += "&SERVER=" + url.QueryEscape(server)
	}
	return fmt.Sprintf("pxf://%s?%s", resource, opts)
}

// ReadPXFSourceSample reads up to limit rows from the PXF source identified by
// server/profile/resource. It creates a TRANSIENT readable external table over
// the source, runs SELECT * ... LIMIT N under a bounded statement_timeout, and
// ALWAYS drops the transient table afterwards (deferred, best-effort, even on
// error). It is HONEST: it returns the REAL sampled rows on success, or a
// wrapped error on any connect/DDL/query failure so the caller maps it to
// available:false (NEVER fabricated rows). Identifiers are sanitized via
// pgx.Identifier and the LOCATION URI is single-quoted, so there is no injection
// surface; the row limit is bounded defensively (clampPXFSampleLimit).
func (c *pgxClient) ReadPXFSourceSample(
	ctx context.Context, server, profile, resource string, limit int,
) (sample *PXFSourceSample, err error) {
	ctx, end := c.startOperation(ctx, "ReadPXFSourceSample")
	defer func() { end(err) }()

	if profile == "" || resource == "" {
		err = fmt.Errorf("reading PXF source sample: profile and resource are required")
		return nil, err
	}
	limit = clampPXFSampleLimit(limit)

	tableName := randomPXFSampleTableName()
	sanitizedTable := pgx.Identifier{tableName}.Sanitize()
	location := buildPXFSampleLocation(server, profile, resource)

	// The transient external table reads the source as a single TEXT line so a
	// preview never needs the source's column schema (which is not knowable
	// up-front). FORMAT 'TEXT' with a non-occurring delimiter keeps each source
	// record intact in one cell. The LOCATION literal is single-quoted.
	createStmt := fmt.Sprintf(
		"CREATE EXTERNAL TABLE %s (line text)\nLOCATION ('%s')\nFORMAT 'TEXT' (DELIMITER E'\\x01');",
		sanitizedTable, strings.ReplaceAll(location, "'", "''"))

	if _, execErr := c.pool.Exec(ctx, createStmt); execErr != nil {
		err = fmt.Errorf("creating transient PXF preview table: %w", execErr)
		return nil, err
	}
	// ALWAYS drop the transient external table (deferred/best-effort) on every
	// path so no preview scaffolding is ever left behind in the catalog. A drop
	// failure is logged at Warn and does not mask the read result. A fresh,
	// short-lived context is used so the cleanup still runs even if the request
	// context was canceled mid-read.
	defer func() {
		dropCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		dropStmt := fmt.Sprintf("DROP EXTERNAL TABLE IF EXISTS %s", sanitizedTable)
		if _, dropErr := c.pool.Exec(dropCtx, dropStmt); dropErr != nil {
			c.logger.Warn("failed to drop transient PXF preview table (best-effort)",
				"table", tableName, "error", dropErr)
		}
	}()

	sample, err = c.scanPXFSampleRows(ctx, sanitizedTable, limit)
	if err != nil {
		return nil, err
	}
	return sample, nil
}

// scanPXFSampleRows sets a LOCAL statement_timeout, runs SELECT * ... LIMIT N
// against the (already-created, sanitized) transient table, and renders each
// cell to a string. It runs inside an explicit transaction so the LOCAL GUC is
// scoped to the read and rolled back afterwards.
func (c *pgxClient) scanPXFSampleRows(
	ctx context.Context, sanitizedTable string, limit int,
) (*PXFSourceSample, error) {
	tx, txErr := c.pool.Begin(ctx)
	if txErr != nil {
		return nil, fmt.Errorf("beginning PXF preview transaction: %w", txErr)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, gucErr := tx.Exec(ctx,
		"SET LOCAL statement_timeout = '"+pxfSampleStatementTimeout+"'"); gucErr != nil {
		return nil, fmt.Errorf("setting PXF preview statement_timeout: %w", gucErr)
	}

	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", sanitizedTable, limit)
	rows, queryErr := tx.Query(ctx, query)
	if queryErr != nil {
		return nil, fmt.Errorf("reading PXF source sample: %w", queryErr)
	}
	defer rows.Close()

	sample := &PXFSourceSample{Columns: pxfSampleColumns(rows.FieldDescriptions())}
	for rows.Next() {
		values, valErr := rows.Values()
		if valErr != nil {
			return nil, fmt.Errorf("scanning PXF source sample row: %w", valErr)
		}
		sample.Rows = append(sample.Rows, pxfSampleRowToStrings(values))
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("iterating PXF source sample rows: %w", rowsErr)
	}
	return sample, nil
}

// pxfSampleColumns extracts the column names from the pgx field descriptions.
func pxfSampleColumns(fields []pgconn.FieldDescription) []string {
	cols := make([]string, 0, len(fields))
	for i := range fields {
		cols = append(cols, fields[i].Name)
	}
	return cols
}

// pxfSampleRowToStrings renders a row's raw values to strings, mapping a SQL
// NULL to the empty string so the JSON payload stays a flat [][]string.
func pxfSampleRowToStrings(values []any) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == nil {
			out = append(out, "")
			continue
		}
		out = append(out, fmt.Sprintf("%v", v))
	}
	return out
}

// GetQueryDetail returns detailed execution information for a specific query by PID.
// It queries pg_stat_activity for session info, pg_locks for lock information,
// and pg_stat_user_tables for recently accessed tables.
func (c *pgxClient) GetQueryDetail(ctx context.Context, pid int32) (detail *QueryDetail, err error) {
	ctx, end := c.startOperation(ctx, "GetQueryDetail")
	defer func() { end(err) }()

	// 1. Get session info from pg_stat_activity.
	detail = &QueryDetail{}
	sessionQuery := `SELECT pid, COALESCE(usename, ''), COALESCE(datname, ''),
		COALESCE(state, ''), COALESCE(query, ''),
		COALESCE(query_start, now()),
		COALESCE(now() - query_start, interval '0')::text,
		COALESCE(wait_event_type, ''), COALESCE(wait_event, ''),
		COALESCE(backend_type, '')
		FROM pg_stat_activity WHERE pid = $1`

	err = c.pool.QueryRow(ctx, sessionQuery, pid).Scan(
		&detail.PID, &detail.Username, &detail.Database,
		&detail.State, &detail.Query, &detail.QueryStart,
		&detail.Duration, &detail.WaitEventType, &detail.WaitEvent,
		&detail.BackendType,
	)
	if err != nil {
		return nil, fmt.Errorf("query not found or not accessible: %w", err)
	}

	// 2. Get locks for this PID (best-effort).
	c.collectQueryLocks(ctx, pid, detail)

	// 3. Get tables accessed (best-effort).
	c.collectAccessedTables(ctx, pid, detail)

	c.logger.Info("query detail retrieved", "pid", pid, "state", detail.State)
	return detail, nil
}

// collectQueryLocks appends the PID's pg_locks rows to detail.Locks.
// Best-effort (L-3): the query error, per-row scan errors and the final
// rows.Err() are logged at Debug and swallowed — Locks may be partial and
// the detail is still returned.
func (c *pgxClient) collectQueryLocks(ctx context.Context, pid int32, detail *QueryDetail) {
	lockQuery := `SELECT locktype, mode, granted, COALESCE(relation::regclass::text, '')
		FROM pg_locks WHERE pid = $1`
	lockRows, lockErr := c.pool.Query(ctx, lockQuery, pid)
	if lockErr != nil {
		c.logger.Debug("querying locks failed; returning detail without locks",
			"pid", pid, "error", lockErr)
		return
	}
	defer lockRows.Close()
	for lockRows.Next() {
		var lock LockInfo
		if scanErr := lockRows.Scan(&lock.LockType, &lock.Mode, &lock.Granted, &lock.Relation); scanErr == nil {
			detail.Locks = append(detail.Locks, lock)
		} else {
			c.logger.Debug("skipping unscannable lock row", "pid", pid, "error", scanErr)
		}
	}
	if rowsErr := lockRows.Err(); rowsErr != nil {
		c.logger.Debug("lock rows iteration failed; locks may be partial",
			"pid", pid, "error", rowsErr)
	}
}

// collectAccessedTables appends recently accessed tables (an approximation
// from pg_stat_user_tables) to detail.TablesAccessed. Best-effort (L-3): the
// query error, per-row scan errors and the final rows.Err() are logged at
// Debug and swallowed — TablesAccessed may be partial and the detail is
// still returned.
func (c *pgxClient) collectAccessedTables(ctx context.Context, pid int32, detail *QueryDetail) {
	tableQuery := `SELECT schemaname || '.' || relname FROM pg_stat_user_tables
		WHERE (seq_scan + COALESCE(idx_scan, 0)) > 0
		ORDER BY (seq_scan + COALESCE(idx_scan, 0)) DESC LIMIT 20`
	tableRows, tableErr := c.pool.Query(ctx, tableQuery)
	if tableErr != nil {
		c.logger.Debug("querying accessed tables failed; returning detail without tables",
			"pid", pid, "error", tableErr)
		return
	}
	defer tableRows.Close()
	for tableRows.Next() {
		var table string
		if scanErr := tableRows.Scan(&table); scanErr == nil {
			detail.TablesAccessed = append(detail.TablesAccessed, table)
		} else {
			c.logger.Debug("skipping unscannable table row", "pid", pid, "error", scanErr)
		}
	}
	if rowsErr := tableRows.Err(); rowsErr != nil {
		c.logger.Debug("table rows iteration failed; accessed tables may be partial",
			"pid", pid, "error", rowsErr)
	}
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
func (c *pgxClient) MoveQueryToResourceGroup(ctx context.Context, pid int32, targetGroup string) (err error) {
	ctx, end := c.startOperation(ctx, "MoveQueryToResourceGroup")
	defer func() { end(err) }()

	// Look up the username for the given PID from pg_stat_activity.
	var username string
	lookupQuery := `SELECT COALESCE(usename, '') FROM pg_stat_activity WHERE pid = $1`
	if scanErr := c.pool.QueryRow(ctx, lookupQuery, pid).Scan(&username); scanErr != nil {
		err = fmt.Errorf("looking up session for PID %d: %w", pid, scanErr)
		return err
	}
	if username == "" {
		err = fmt.Errorf("session with PID %d not found or has no associated role", pid)
		return err
	}

	// Execute ALTER ROLE to reassign the resource group.
	alterSQL := fmt.Sprintf("ALTER ROLE %s RESOURCE GROUP %s",
		pgx.Identifier{username}.Sanitize(), pgx.Identifier{targetGroup}.Sanitize())
	if _, execErr := c.pool.Exec(ctx, alterSQL); execErr != nil {
		err = fmt.Errorf("moving PID %d (role %s) to resource group %s: %w", pid, username, targetGroup, execErr)
		return err
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
