package db

import (
	"context"
	"io"
	"time"
)

// This file defines the db.Client capability interfaces (M-1): the former
// monolithic ~81-method Client interface is split into focused capability
// interfaces and recomposed below. The composed method set is EXACTLY
// identical to the previous monolith, so every existing implementation and
// mock keeps satisfying Client unchanged (structural typing). Consumers
// should accept the narrowest capability interface they need.

// ConnectionOps groups connection lifecycle operations.
type ConnectionOps interface {
	// Ping checks database connectivity.
	Ping(ctx context.Context) error
	// Close closes the database connection pool.
	Close()
}

// TopologyOps groups cluster topology inspection operations.
type TopologyOps interface {
	// GetSegmentConfiguration returns the segment configuration.
	GetSegmentConfiguration(ctx context.Context) ([]SegmentInfo, error)
}

// ConfigOps groups server configuration (GUC) operations.
type ConfigOps interface {
	// SetParameter sets a configuration parameter.
	SetParameter(ctx context.Context, name, value string, scope ParameterScope) error
	// ShowParameter returns the current value of a parameter.
	ShowParameter(ctx context.Context, name string) (string, error)
	// ReloadConfig triggers a configuration reload.
	ReloadConfig(ctx context.Context) error
}

// SessionOps groups session and query lifecycle operations.
type SessionOps interface {
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
	// TerminateAllBackends terminates all non-system backend connections.
	// It calls pg_terminate_backend for each session except the current one
	// and system processes. Returns the number of backends terminated.
	TerminateAllBackends(ctx context.Context) (int32, error)
	// CancelAllQueries cancels all active queries (non-idle sessions)
	// except the current backend. Returns the number of queries canceled.
	CancelAllQueries(ctx context.Context) (int32, error)
	// MoveQueryToResourceGroup moves a running query's session to a different resource group.
	// It looks up the session's role from pg_stat_activity and reassigns it via ALTER ROLE.
	MoveQueryToResourceGroup(ctx context.Context, pid int32, targetGroup string) error
}

// RoleOps groups database role management operations.
type RoleOps interface {
	// CreateRole creates a new database role.
	CreateRole(ctx context.Context, opts RoleOptions) error
	// AlterRole modifies an existing database role.
	AlterRole(ctx context.Context, opts RoleOptions) error
	// DropRole drops a database role.
	DropRole(ctx context.Context, name string) error
}

// MaintenanceOps groups database maintenance operations.
type MaintenanceOps interface {
	// Vacuum runs a vacuum operation.
	Vacuum(ctx context.Context, opts VacuumOptions) error
	// Analyze runs an analyze operation.
	Analyze(ctx context.Context, table string) error
	// Reindex runs a reindex operation.
	Reindex(ctx context.Context, opts ReindexOptions) error
	// LogRotate triggers a log file rotation by calling pg_rotate_logfile().
	// This signals the logger process to switch to a new log file immediately.
	LogRotate(ctx context.Context) error
}

// MonitoringOps groups live monitoring and inspection operations.
type MonitoringOps interface {
	// GetDiskUsage returns disk usage information.
	GetDiskUsage(ctx context.Context, database string) ([]DiskUsage, error)
	// GetReplicationLag returns the replication lag in bytes.
	GetReplicationLag(ctx context.Context) (int64, error)
	// GetActiveQueryCount returns the number of active, queued, and blocked queries.
	GetActiveQueryCount(ctx context.Context) (active, queued, blocked int32, err error)
	// GetMaxConnections returns the server's max_connections setting.
	GetMaxConnections(ctx context.Context) (int32, error)
	// GetResourceGroupUsage returns CPU and memory usage for a resource group.
	GetResourceGroupUsage(ctx context.Context, group string) (cpu, memory float64, err error)
	// GetQueryDetail returns detailed execution information for a specific query by PID.
	GetQueryDetail(ctx context.Context, pid int32) (*QueryDetail, error)
}

// WorkloadOps groups resource group and resource queue management operations.
type WorkloadOps interface {
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
}

// BackupOps groups backup and restore operations.
type BackupOps interface {
	// CreateBackup creates a new backup.
	CreateBackup(ctx context.Context, opts BackupOptions) (*BackupInfo, error)
	// RestoreBackup restores from a backup.
	RestoreBackup(ctx context.Context, opts RestoreOptions) error
	// ListBackups returns all available backups.
	ListBackups(ctx context.Context) ([]BackupInfo, error)
	// DeleteBackup deletes a backup by ID.
	DeleteBackup(ctx context.Context, id string) error
}

// DataLoadingOps groups data loading job operations.
type DataLoadingOps interface {
	// CreateDataLoadingJob creates a new data loading job.
	CreateDataLoadingJob(ctx context.Context, job DataLoadingJobConfig) error
	// StartDataLoadingJob starts a data loading job.
	StartDataLoadingJob(ctx context.Context, name string) error
	// StopDataLoadingJob stops a data loading job.
	StopDataLoadingJob(ctx context.Context, name string) error
	// ListDataLoadingJobs returns all data loading jobs.
	ListDataLoadingJobs(ctx context.Context) ([]DataLoadingJobStatus, error)
	// EnsureDataLoaderRole ensures the dedicated, minimal-privilege data-loading
	// database role exists and is GRANTed only the pxf protocol privileges
	// (SELECT/INSERT ON PROTOCOL pxf) — SE.6. The role is created (when absent)
	// as NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN via an existence-probe +
	// CREATE pattern (mirroring SetupExporterRole), then the protocol GRANTs are
	// applied. It is best-effort/NON-FATAL like SetupPXFExtensions: a missing
	// PROTOCOL pxf (stub image) is logged and tolerated. When roleName is empty
	// or the cluster admin (gpadmin) the method is a no-op — gpadmin already
	// exists and SetupPXFExtensions retains the existing RP.11 grant behavior.
	// Only a hard connectivity error (the existence probe failing) is surfaced.
	EnsureDataLoaderRole(ctx context.Context, roleName string) error
}

// StorageOps groups storage inspection and reporting operations.
type StorageOps interface {
	// GetStorageDiskUsage returns disk usage information per tablespace/segment.
	GetStorageDiskUsage(ctx context.Context) ([]DiskUsageInfo, error)
	// GetTables returns per-table storage info (size, bloat, skew, row count) for
	// all user tables. Best-effort: bloat is derived from dead-tuple percentage on
	// the always-present pg_stat_user_tables; skew is sourced from
	// gp_toolkit.gp_skew_coefficients when available and HONESTLY left 0 when the
	// view/columns are absent (no fabrication). A connectivity/query error is
	// surfaced so the handler can fall back to an empty list.
	GetTables(ctx context.Context) ([]TableStorageInfo, error)
	// GetDiskUsagePercent returns the worst-case (MAX across segments) filesystem
	// usage percentage of the segment data volumes, sourced from
	// gp_toolkit.gp_disk_free. It returns ErrDiskUsageUnavailable when the view or
	// its columns are not available so the caller can SKIP the measurement
	// (S.1/R.2: never fabricate a percentage).
	//
	// This is the PREFERRED, TRUE filesystem-usage path. When it returns
	// ErrDiskUsageUnavailable the caller should fall back to the portable
	// logical-capacity proxy built on GetClusterDataSizeBytes (see that method
	// and recordDiskUsage for the documented difference in what is measured).
	GetDiskUsagePercent(ctx context.Context) (int32, error)
	// GetClusterDataSizeBytes returns the total LOGICAL on-disk size of all
	// databases in the cluster, computed as
	// SELECT sum(pg_database_size(datname)) FROM pg_database. pg_database_size is
	// always available (no extension, no gp_toolkit dependency), so this method
	// works on every Cloudberry/Greenplum/PostgreSQL version — verified live on
	// Cloudberry 2.1.0.
	//
	// IMPORTANT — what this measures: this is the LOGICAL size of stored data
	// (the sum of database sizes), NOT the filesystem capacity/usage of the
	// underlying volumes. It is the numerator of the PORTABLE FALLBACK proxy used
	// by recordDiskUsage when gp_toolkit.gp_disk_free is unavailable: the
	// controller divides this by the CRD-provisioned PVC capacity to derive a
	// real, growing usage percentage. The gp_disk_free path (GetDiskUsagePercent)
	// remains the TRUE filesystem usage and is always preferred when present.
	GetClusterDataSizeBytes(ctx context.Context) (int64, error)
	// GetTableDetails returns detailed information about a specific table.
	GetTableDetails(ctx context.Context, schema, table string) (*TableDetail, error)
	// GetUsageReport returns a usage report for the given month: one entry per
	// non-template database (size + connections), each enriched with a bounded
	// per-table breakdown for the connected database only (Scenario 120 C.11).
	// The month is a scope/label; the report is computed on demand (no history).
	GetUsageReport(ctx context.Context, month string) ([]UsageReportEntry, error)
}

// RecommendationOps groups maintenance recommendation operations.
type RecommendationOps interface {
	// GetBloatRecommendations returns bloat recommendations gated on th.Bloat (C.6).
	GetBloatRecommendations(ctx context.Context, th RecommendationThresholds) ([]Recommendation, error)
	// GetSkewRecommendations returns data skew recommendations gated on th.Skew (C.7).
	GetSkewRecommendations(ctx context.Context, th RecommendationThresholds) ([]Recommendation, error)
	// GetAgeRecommendations returns XID age recommendations gated on th.Age (C.8).
	GetAgeRecommendations(ctx context.Context, th RecommendationThresholds) ([]Recommendation, error)
	// GetIndexBloatRecommendations returns index bloat recommendations gated on th.IndexBloat (C.9).
	GetIndexBloatRecommendations(ctx context.Context, th RecommendationThresholds) ([]Recommendation, error)
	// TriggerRecommendationScan triggers a recommendation scan.
	TriggerRecommendationScan(ctx context.Context) error
}

// HAOps groups high-availability and mirroring operations.
type HAOps interface {
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
	// PromoteStandby promotes the standby to primary.
	PromoteStandby(ctx context.Context) error
}

// ScaleOps groups scale-out / scale-in operations.
type ScaleOps interface {
	// RegisterNewSegments registers new primary and mirror segments in gp_segment_configuration.
	//
	// Deprecated (scale-out): the scale-out path no longer hand-registers
	// segments — the gpexpand coordinator-exec Job (builder.BuildGpexpandJob)
	// owns the gp_segment_configuration insert AND the physical segment-init
	// (basebackup from the coordinator template), which a raw catalog INSERT
	// cannot do. Pre-inserting a row makes gpexpand see the content as already
	// present and fail/double-register, so the controller MUST NOT call this
	// during scale-out. The method is retained for backward compatibility (the
	// db.Client interface and its mocks) and possible fallback tooling.
	RegisterNewSegments(ctx context.Context, opts SegmentRegistrationOptions) error
	// SeedNewSegmentCatalog seeds the newly-added segments' catalog from the
	// coordinator (dispatch mode). Returns the number of databases seeded.
	//
	// Deprecated (scale-out): superseded by the gpexpand Job — a database's
	// segment membership is FIXED at CREATE DATABASE time, so a
	// coordinator-dispatched seed can never provision a user database onto a
	// late-added segment (only gpexpand's segment-init does). Retained for
	// interface/mocks compatibility; not called on the scale-out path.
	SeedNewSegmentCatalog(ctx context.Context, opts SegmentRegistrationOptions) (int, error)
	// RedistributeData redistributes existing tables across all segments (including new ones).
	// This is the gpexpand equivalent for Cloudberry.
	RedistributeData(ctx context.Context, opts RedistributionOptions) error
	// GetRedistributionProgress returns the current redistribution progress (0-100).
	GetRedistributionProgress(ctx context.Context) (int32, error)
	// DeregisterSegments removes segment entries from gp_segment_configuration
	// for segments with content IDs >= newCount. This is called during scale-in
	// after data has been moved off the segments being removed.
	DeregisterSegments(ctx context.Context, newCount int32) error
	// GpexpandSchemaPresent reports whether the gpexpand expansion schema still
	// exists in the coordinator catalog. A lingering gpexpand schema (left by an
	// interrupted or pre-hardened gpexpand Job) blocks a subsequent gpbackup with
	// "expansion currently in process", so the operator checks this at scale-out
	// completion (D9). It is a single pure-SQL catalog lookup (no SSH, no Job).
	GpexpandSchemaPresent(ctx context.Context) (bool, error)
	// FinalizeGpexpand drops the gpexpand expansion schema via the coordinator
	// connection (DROP SCHEMA IF EXISTS gpexpand CASCADE). It is idempotent and
	// requires no SSH — a single catalog op the operator dispatches directly to
	// clear the gpbackup blocker at scale-out completion (D9).
	FinalizeGpexpand(ctx context.Context) error
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
}

// PXFOps groups PXF (external data) operations.
type PXFOps interface {
	// SetupPXFExtensions best-effort installs the PXF client extensions:
	// CREATE EXTENSION IF NOT EXISTS pxf, then pxf_fdw. Both statements are
	// NON-FATAL — the pxf agent/extension is absent in cloudberry-official:2.1.0
	// (only a pxf_fdw client stub may exist), so a CREATE EXTENSION failure is
	// logged as a warning and the method returns (count, nil). Only a hard
	// connectivity error (the existence probe itself failing) is surfaced.
	// Idempotent. The returned int is the number of extensions actually CREATE
	// EXTENSIONed on this call (0..2); callers use it to avoid marking PXF
	// "ready" when nothing was installed (e.g. DB in recovery / pxf absent).
	SetupPXFExtensions(ctx context.Context) (int, error)
	// ListPXFExtensions returns the PXF client extensions actually present in
	// pg_extension among {pxf, pxf_fdw}, sorted ascending. It is a LIVE,
	// observed-only probe: it never synthesizes names and returns only what the
	// catalog reports. A connectivity/query error is surfaced (not swallowed) so
	// the caller can treat an unobservable probe as ABSENT rather than as "none
	// installed". An empty (nil) slice with a nil error means a reachable DB with
	// no PXF extensions present.
	ListPXFExtensions(ctx context.Context) ([]string, error)
	// ListExternalTables returns the external tables (pg_exttable) and foreign
	// tables (pg_foreign_table) actually present in the catalog, each tagged with
	// its kind ("external" or "foreign") and, when derivable, its backing server.
	// It is a LIVE, observed-only probe: it never synthesizes rows and reports
	// only what the catalog returns. A connectivity/query error is surfaced (not
	// swallowed) so the caller can treat an unobservable probe as ABSENT rather
	// than as "none present". An empty (nil) slice with a nil error means a
	// reachable DB with no external/foreign tables defined.
	ListExternalTables(ctx context.Context) ([]ExternalTableInfo, error)
	// ReadPXFSourceSample reads up to limit rows from a PXF source (identified by
	// server/profile/resource) by creating a TRANSIENT readable external table,
	// running SELECT * ... LIMIT N under a bounded statement_timeout, and ALWAYS
	// dropping the transient table afterwards (deferred/best-effort). It is an
	// HONEST preview: it returns the REAL sampled rows on success, or a wrapped
	// error on any connect/DDL/query failure so the caller can treat the source
	// as unreachable (available:false) rather than fabricating rows.
	ReadPXFSourceSample(
		ctx context.Context, server, profile, resource string, limit int,
	) (*PXFSourceSample, error)
}

// QueryHistoryStore groups query history persistence and retrieval operations.
type QueryHistoryStore interface {
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
}

// ExporterSetupOps groups metrics-exporter provisioning operations.
type ExporterSetupOps interface {
	// SetupExporterRole creates the cloudberry_exporter database role with LOGIN privilege,
	// grants pg_monitor membership, and grants SELECT on monitoring views.
	SetupExporterRole(ctx context.Context, password string) error
}

// Client defines the interface for Cloudberry database operations. It is the
// composition of the capability interfaces above; its method set is exactly
// the former monolithic interface's, so all implementations and mocks remain
// source-compatible (M-1).
type Client interface {
	ConnectionOps
	TopologyOps
	ConfigOps
	SessionOps
	RoleOps
	MaintenanceOps
	MonitoringOps
	WorkloadOps
	BackupOps
	DataLoadingOps
	StorageOps
	RecommendationOps
	HAOps
	ScaleOps
	PXFOps
	QueryHistoryStore
	ExporterSetupOps
}

// Compile-time proof (non-test) that the production pgx-backed client
// implements the full composed Client interface — locks the M-1 refactor
// against an accidentally dropped or altered method.
var _ Client = (*pgxClient)(nil)
