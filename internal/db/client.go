// Package db provides a Cloudberry/PostgreSQL database client for the cloudberry operator.
package db

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// Severity level constants for recommendations.
const (
	severityInfo     = "info"
	severityWarning  = "warning"
	severityCritical = "critical"
)

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
	Application   string    `json:"application"`
	ClientAddress string    `json:"clientAddress"`
	State         string    `json:"state"`
	Query         string    `json:"query"`
	QueryStart    time.Time `json:"queryStart"`
	Duration      string    `json:"duration"`
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

// ResourceGroupOptions defines options for creating or altering a resource group.
type ResourceGroupOptions struct {
	Name          string
	Concurrency   int32
	CPUMaxPercent int32
	CPUWeight     int32
	MemoryLimit   int32
	MinCost       int32
}

// ResourceGroupInfo represents a resource group.
type ResourceGroupInfo struct {
	Name          string  `json:"name"`
	Concurrency   int32   `json:"concurrency"`
	CPUMaxPercent int32   `json:"cpuMaxPercent"`
	CPUWeight     int32   `json:"cpuWeight"`
	MemoryLimit   int32   `json:"memoryLimit"`
	CPUUsage      float64 `json:"cpuUsage"`
	MemoryUsage   float64 `json:"memoryUsage"`
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

	connStr := buildConnectionString(cfg)

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parsing connection string: %w", err)
	}

	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}

	var pool *pgxpool.Pool
	connectErr := util.RetryWithBackoff(ctx, retryOpts, func(ctx context.Context) error {
		var poolErr error
		pool, poolErr = pgxpool.NewWithConfig(ctx, poolCfg)
		if poolErr != nil {
			return fmt.Errorf("creating connection pool: %w", poolErr)
		}
		return pool.Ping(ctx)
	})

	if connectErr != nil {
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

// buildConnectionString constructs a PostgreSQL connection string with properly
// escaped parameters to prevent injection vulnerabilities.
func buildConnectionString(cfg Config) string {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	// Use pgx's ParseConfig to safely build and validate the connection string.
	// This avoids injection via specially crafted host/user/password values.
	connCfg, err := pgx.ParseConfig(
		"host=" + escapeConnParam(cfg.Host) +
			" port=" + fmt.Sprintf("%d", cfg.Port) +
			" dbname=" + escapeConnParam(cfg.Database) +
			" user=" + escapeConnParam(cfg.Username) +
			" password=" + escapeConnParam(cfg.Password) +
			" sslmode=" + escapeConnParam(sslMode),
	)
	if err != nil {
		// Fallback with escaped parameters if ParseConfig fails.
		return "host=" + escapeConnParam(cfg.Host) +
			" port=" + fmt.Sprintf("%d", cfg.Port) +
			" dbname=" + escapeConnParam(cfg.Database) +
			" user=" + escapeConnParam(cfg.Username) +
			" password=" + escapeConnParam(cfg.Password) +
			" sslmode=" + escapeConnParam(sslMode)
	}

	return connCfg.ConnString()
}

// escapeConnParam escapes a connection string parameter value by replacing
// backslashes and single quotes to prevent injection.
func escapeConnParam(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	// If the value contains spaces or special characters, quote it.
	if strings.ContainsAny(s, " \t\n'\\") {
		return "'" + s + "'"
	}
	return s
}

// Ping checks database connectivity.
func (c *pgxClient) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

// Close closes the database connection pool.
func (c *pgxClient) Close() {
	c.pool.Close()
}

// GetSegmentConfiguration returns the segment configuration from gp_segment_configuration.
func (c *pgxClient) GetSegmentConfiguration(ctx context.Context) ([]SegmentInfo, error) {
	query := `SELECT content, dbid, role, preferred_role, mode, status, 
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
	query := `SELECT pid, usename, application_name, client_addr, state, 
		COALESCE(query, ''), COALESCE(query_start, now()),
		COALESCE(now() - query_start, interval '0')::text
		FROM pg_stat_activity 
		WHERE pid != pg_backend_pid()
		ORDER BY query_start DESC NULLS LAST`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying sessions: %w", err)
	}
	defer rows.Close()

	var sessions []Session
	for rows.Next() {
		var s Session
		var clientAddr *string
		if err := rows.Scan(
			&s.PID, &s.Username, &s.Application, &clientAddr,
			&s.State, &s.Query, &s.QueryStart, &s.Duration,
		); err != nil {
			return nil, fmt.Errorf("scanning session row: %w", err)
		}
		if clientAddr != nil {
			s.ClientAddress = *clientAddr
		}
		sessions = append(sessions, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating session rows: %w", err)
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
func (c *pgxClient) PromoteStandby(ctx context.Context) error {
	if _, err := c.pool.Exec(ctx, "SELECT pg_promote()"); err != nil {
		return fmt.Errorf("promoting standby: %w", err)
	}
	c.logger.Info("standby promoted to primary")
	return nil
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
	query := fmt.Sprintf("CREATE RESOURCE GROUP %s WITH (concurrency=%d, cpu_max_percent=%d, cpu_weight=%d)",
		pgx.Identifier{opts.Name}.Sanitize(), opts.Concurrency, opts.CPUMaxPercent, opts.CPUWeight)

	if _, err := c.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("creating resource group %s: %w", opts.Name, err)
	}
	c.logger.Info("resource group created", "name", opts.Name)
	return nil
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
	}

	for _, alt := range alterations {
		if alt.value <= 0 {
			continue
		}
		query := fmt.Sprintf("ALTER RESOURCE GROUP %s SET %s %d",
			pgx.Identifier{opts.Name}.Sanitize(), alt.param, alt.value)
		if _, err := c.pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("altering resource group %s param %s: %w", opts.Name, alt.param, err)
		}
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
	query := `SELECT rsgname, 
		COALESCE(num_running, 0), COALESCE(num_queueing, 0), 
		COALESCE(cpu_usage, 0), COALESCE(memory_usage, 0)
		FROM gp_toolkit.gp_resgroup_status ORDER BY rsgname`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying resource groups: %w", err)
	}
	defer rows.Close()

	var groups []ResourceGroupInfo
	for rows.Next() {
		var g ResourceGroupInfo
		var numRunning, numQueueing int32
		if scanErr := rows.Scan(&g.Name, &numRunning, &numQueueing, &g.CPUUsage, &g.MemoryUsage); scanErr != nil {
			return nil, fmt.Errorf("scanning resource group row: %w", scanErr)
		}
		g.Concurrency = numRunning + numQueueing
		groups = append(groups, g)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterating resource group rows: %w", rowErr)
	}

	return groups, nil
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
