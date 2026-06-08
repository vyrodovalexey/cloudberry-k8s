# Cloudberry Kubernetes Operator

A Kubernetes operator for managing the full lifecycle of [Cloudberry Database](https://cloudberry.apache.org/) clusters. Provides declarative cluster management, high availability, authentication, observability, and a companion CLI utility.

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [cloudberry-ctl CLI](#cloudberry-ctl-cli)
- [Development](#development)
- [Testing](#testing)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                         │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                 cloudberry-operator                        │  │
│  │                                                           │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐  │  │
│  │  │   Cluster    │ │     HA       │ │   Auth / Admin   │  │  │
│  │  │  Controller  │ │  Controller  │ │   Controllers    │  │  │
│  │  └──────┬───────┘ └──────┬───────┘ └────────┬─────────┘  │  │
│  │         └────────────────┼──────────────────┘             │  │
│  │                          │                                │  │
│  │  ┌───────────────────────┴─────────────────────────────┐  │  │
│  │  │           Reconciliation Engine                      │  │  │
│  │  │         (controller-runtime / kubebuilder)           │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  │                                                           │  │
│  │  ┌──────────────────────────────────────────────────────┐ │  │
│  │  │  REST API Server (:8090)                             │ │  │
│  │  │  Rate Limiter → Auth Middleware → Handlers           │ │  │
│  │  └──────────────────────────────────────────────────────┘ │  │
│  │                                                           │  │
│  │  ┌──────────┐  ┌───────────┐  ┌────────────────────────┐ │  │
│  │  │ Metrics  │  │ Telemetry │  │   Auth Middleware      │ │  │
│  │  │ (Prom)   │  │  (OTLP)   │  │ (bcrypt + OIDC/JWT)   │ │  │
│  │  └──────────┘  └───────────┘  └────────────────────────┘ │  │
│  │                                                           │  │
│  │  ┌──────────────────────────────────────────────────────┐ │  │
│  │  │  Cert Manager (Vault PKI / Self-Signed)             │ │  │
│  │  └──────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                  Cloudberry Cluster                        │  │
│  │  ┌──────────────┐  ┌──────────────┐                       │  │
│  │  │ Coordinator  │  │   Standby    │                       │  │
│  │  │ StatefulSet  │  │ StatefulSet  │                       │  │
│  │  └──────────────┘  └──────────────┘                       │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │            Segment StatefulSets                      │  │  │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │  │  │
│  │  │  │Primary 0 │  │Primary 1 │  │Primary N │          │  │  │
│  │  │  │Mirror  0 │  │Mirror  1 │  │Mirror  N │          │  │  │
│  │  │  └──────────┘  └──────────┘  └──────────┘          │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │    Vault     │  │   Keycloak   │  │   Observability    │    │
│  │  (optional)  │  │  (OIDC IdP)  │  │      Stack         │    │
│  └──────────────┘  └──────────────┘  └────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

The operator follows the standard Kubernetes reconciliation pattern: **Watch** resources for changes, **Diff** desired vs. actual state, **Act** to converge, **Update** status, and **Requeue** as needed.

## Features

**Cluster Lifecycle Management**
- Declarative cluster creation, updates, and deletion via `CloudberryCluster` CRD
- Cross-namespace cluster name uniqueness enforced by validating webhook
- Start, stop, and restart with multiple modes:
  - **Stop modes**: smart (wait for clients), fast (rollback transactions), immediate (abort connections)
  - **Start modes**: normal (all components), restricted (coordinator only), maintenance (utility mode)
  - **Restart**: stop + start with phase transitions (Running → Stopping → Initializing → Running)
- New cluster phases: `Stopped`, `Stopping`, `Restricted`, `Maintenance`
- Scale-out with automatic data redistribution (increase `segments.count` to add segments)
  - Pre-flight check blocks scaling when cluster is not in `Running` phase
  - 10-minute timeout with failure detection and `status.failedSegments` reporting
  - No automatic rollback on failure — manual intervention required
- Scale-in with PVC policy support (decrease `segments.count` to remove segments)
  - Scale-in by more than 50% requires `avsoft.io/confirm-scale-in=true` annotation (safety guard)
  - Confirmation annotation automatically cleaned up after successful completion
- Rolling upgrades with automatic rollback on failure
  - Phase-by-phase upgrade: mirrors → primaries → standby → coordinator → verify
  - 10-minute per-phase timeout with automatic rollback to previous image
  - Upgrade state tracked via `avsoft.io/upgrade` annotation
  - Pre-flight check blocks upgrades when cluster is not in `Running` phase
- Online PVC storage expansion for coordinator, standby, and segments (no shrink)
- Cluster deletion with configurable PVC policy (`Retain` or `Delete`) and event reporting
  - Backup-on-delete: optional pre-deletion backup Job when `backupOnDelete: true`
  - PVC events: `PVCsRetained` for Retain policy, `PVCsDeleted` for Delete policy
  - Deletion lifecycle events: `Deleting` → `BackupOnDelete` → `PVCsRetained`/`PVCsDeleted` → `Deleted`

**High Availability**
- Segment mirroring with group and spread layouts
- Enable/disable mirroring on existing clusters with state machine tracking (NotConfigured → Initializing → Syncing → InSync)
  - Pre-flight validation: cluster must be Running, sufficient nodes for layout
  - 30-minute timeout with MirroringDegraded on timeout
  - Webhook validation prevents enabling on non-Running clusters
  - Metrics: `cloudberry_mirroring_operations_total`, `cloudberry_replication_lag_bytes`
- Fault Tolerance Service (FTS) with configurable probe intervals, retries, and timeouts
- Automatic failover from primary to mirror segments via Cloudberry's internal FTS scan
  - FTS probe retries up to `FTSProbeRetries` times with `FTSProbeTimeout` per attempt
  - Detects primary segment failures and triggers mirror promotion
  - Emits `SegmentFailover` events per failed segment with promotion details
  - Increments `cloudberry_fts_failover_total` metric on failover
  - Cluster remains available during and after failover
  - Post-failover state: `MirroringDegraded` with `failedSegments` in status
- Coordinator standby with WAL streaming replication
- Incremental, full, and differential segment recovery
- Manual segment rebalancing with configurable skew threshold, parallelism, and table exclusion patterns
- Rebalance status API and CLI (`--status`, `--tables` flags)
- Data skew coefficient metric (`cloudberry_data_skew_coefficient`)

**Authentication & Authorization**
- Dual-mode authentication: Basic + OIDC (Keycloak)
- JWT validation with JWKS caching and role claim extraction
- Five-tier permission model: Self Only, Basic, Operator Basic, Operator, Admin
- `pg_hba.conf` management via CRD
- SSL/TLS support with configurable minimum TLS version
- Webhook TLS certificate management (Vault PKI or self-signed with automatic rotation)
- HashiCorp Vault integration for secrets management

**Observability**
- Prometheus metrics for cluster health, reconciliation, FTS, connections, scale operations, mirroring operations, and PVC sizes
- Reconciliation metrics: `cloudberry_reconcile_total`, `cloudberry_reconcile_errors_total`, `cloudberry_reconcile_duration_seconds` with cluster/namespace/result labels
- Operational metrics wired to real operations: `cloudberry_pvc_size_bytes` (PVC expansion), `cloudberry_redistribution_progress` (data redistribution), `cloudberry_backup_total` / `cloudberry_restore_total`, per-database `cloudberry_disk_usage_bytes`, and storage recommendation metrics (`cloudberry_recommendations_total`, `cloudberry_recommendation_scan_duration_seconds`)
- Maintenance metrics: `cloudberry_maintenance_operations_total` with cluster/namespace/operation/`result` (`started`, `success`, `failed`) labels
- Security metrics: `cloudberry_cert_rotation_total`, `cloudberry_cert_expiry_seconds`, `cloudberry_vault_operations_total`, `cloudberry_vault_operation_duration_seconds`, and `cloudberry_auth_attempts_total` (a missing/malformed `Authorization` header increments `{method="unknown",result="failure"}`)
- Admission and lifecycle metrics: `cloudberry_webhook_admission_total`, `cloudberry_upgrade_operations_total`, `cloudberry_rolling_restart_total`, `cloudberry_recovery_operations_total`
- Workload and query-history metrics wired through: slow queries, workload rule actions, active connections, and query-history insert/retention/size
- Exporter sidecars: `postgres-exporter` (port 9187) runs on both the coordinator and standby coordinator pods for monitoring continuity on promotion; `cloudberry-query-exporter` is coordinator-only (its cluster-global queries would otherwise duplicate metric series on a non-promoted standby); a per-segment `postgres-exporter` is available opt-in (default off) for both primary and mirror segments via the independent `queryMonitoring.exporters.postgresExporter.segments` and `queryMonitoring.exporters.postgresExporter.mirrors` flags for deep per-segment diagnostics; the `postgres-exporter` is Cloudberry-tailored (conditional resource-group query, disabled incompatible built-in collectors, recovery-safe WAL query) so scrapes run cleanly (`pg_exporter_last_scrape_error=0`) on coordinator, standby, and segments
- OpenTelemetry (OTLP) distributed tracing with gRPC/HTTP exporters
- Span error recording via `SetSpanError()` — sets error status and exception events on OTEL spans
- Structured logging (slog) with JSON output including cluster, namespace, controller, and reconcileID fields
- Structured error types with sentinel errors (`ErrNotFound`, `ErrInvalidInput`, `ErrRetryExhausted`) supporting `errors.Is()` classification
- Retry with exponential backoff for transient failures (configurable max retries, backoff, jitter)
- Webhook validation rejects invalid cluster specs at admission time (segments, OIDC, storage)
- Automatic pod deletion detection and recovery with degraded state reporting

**Security Hardening**
- SQL injection prevention with parameterized queries (pgx native config builder)
- SQL injection prevention in distribution key handling via `sanitizeDistKey()` helper
- SQL injection prevention in `updateNumsegments` with parameterized query
- Input validation on all API path parameters (SQL identifier regex)
- Port range validation in CRD types (1–65535)
- Recovery type validation (`incremental`, `full`, `differential` only)
- HTTP server timeouts (ReadTimeout, WriteTimeout, IdleTimeout) to prevent resource exhaustion
- Response body size limits in CLI client (10 MiB)
- URL encoding for all path parameters in CLI
- Rate limiter goroutine leak prevention with `sync.Once`-guarded shutdown
- Rate limiting for rebalance operations with inter-table delay and `dispatchRebalanceTables`
- DB connection pool leak prevention on retry failures
- Admin password persisted to K8s Secret (survives pod restarts)
- CLI password flag security warning (recommends env var)
- Webhook CA bundle injection with retry and exponential backoff
- Webhook cert rotation forces re-issuance on certificate-source mismatch (e.g. a stale self-signed cert while `certSource=vault-pki`) instead of keeping the stale cert until natural expiry
- Operator-to-cluster database TLS uses `sslmode=verify-ca` (CA chain validation against the cluster CA from the SSL cert Secret's `ca.crt`) when SSL is enabled with a `certSecret`
- Context cancellation checks in database propagation operations
- Goroutine leak prevention in idle daemon via `startOrUpdateIdleDaemon`
- Dependency vulnerability fix: upgraded `golang.org/x/net` (GO-2026-5026)
- Dependency security update: bumped `golang.org/x/crypto` to v0.52.0 (Go toolchain pinned to 1.26.4)

**Administration**
- Configuration management with automatic hot-reload vs rolling restart detection
  - Reload-safe parameters applied without pod restarts
  - Restart-required parameters (shared_buffers, max_connections, wal_level, etc.) trigger rolling restart
  - Rolling restart order: mirrors → primaries → standby → coordinator
  - Rolling restart state tracked via `avsoft.io/rolling-restart` annotation
- Cluster-wide, coordinator-only, per-database, and per-role parameters
- Maintenance operations via Kubernetes Jobs: vacuum, vacuum-analyze, vacuum-full, analyze, reindex, backup-on-delete
  - Jobs created with `BackoffLimit=1`, `TTLSecondsAfterFinished=3600`
  - `PGPASSWORD` sourced from admin password Secret
- Backup and restore to S3-compatible storage (AWS S3 / MinIO) via the `apache/cloudberry-backup` toolchain (`gpbackup`, `gprestore`, `gpbackup_s3_plugin`)
  - S3 credentials from a Kubernetes Secret or HashiCorp Vault (materialized to a Secret at reconcile time)
  - Full S3 config: bucket, folder, region, encryption, `forcePathStyle`, multipart tuning, retention, schedule (CronJob)
  - Live data cycle runs `gpbackup`/`gprestore` inside the coordinator pod (MPP coordinator→segment SSH dispatch); verified end-to-end by Scenario 71 for both Secret and Vault credential variants
  - Backup infrastructure verified by Scenario 72: toolchain image (`gpbackup`/`gprestore`/`gpbackup_s3_plugin`), backup RBAC (`cloudberry-backup-sa` + `cloudberry-backup-role`), the `<cluster>-backup-s3-config` ConfigMap, and the `jobTemplate` pod-template overrides (resources, nodeSelector, tolerations, serviceAccountName, backoffLimit, activeDeadlineSeconds, ttlSecondsAfterFinished)
  - On-demand backup with per-request `gpbackupOptions` verified by Scenario 73: `POST /clusters/{name}/backups` accepts compression level/type, jobs, with-stats, without-globals, include-schemas, and a `noCompression` override (emits `--no-compression` and ignores `--compression-level`); the on-demand request creates a Kubernetes Job directly (not via the CronJob)
  - Single-data-file backup + full-option restore verified by Scenario 74: a `singleDataFile`/`copyQueueSize` backup emits `--single-data-file --copy-queue-size 4` (no `--jobs`), requires `gpbackup_helper` on every segment, and writes one consolidated data file per segment; the full-option restore resolves `gprestore`'s mutual-exclusivity rules (include-table over include-schema, run-analyze over with-stats, `--copy-queue-size` instead of `--jobs` for single-data-file restores, and redirect-schema pre-creation) so `mydb_restored` is populated, objects land in the `restored` schema, and ANALYZE runs
  - Compression matrix (gzip vs zstd) verified by Scenario 75: two on-demand backups of the same data at `--compression-level 6` — one `--compression-type gzip`, one `--compression-type zstd` — both succeed and both restore cleanly into separate redirect DBs, with the on-disk sizes differing (zstd smaller). The `zstd` CLI now ships in the cluster image (`cloudberry-official:2.1.0`), required because `gpbackup` pipes segment `COPY` output through `zstd --compress`; data files are named `…_<oid>.gz` (gzip) vs `…_<oid>.zst` (zstd)
  - Scheduled backup via CronJob + status population verified by Scenario 76: setting `spec.backup.schedule` reconciles a CronJob `{cluster}-backup-schedule` (`ownerReferences` → the cluster, `concurrencyPolicy: Forbid`, `successfulJobsHistoryLimit`/`failedJobsHistoryLimit` = 3, pod `restartPolicy: Never`) that fires on schedule and spawns a Job; after the backup succeeds the operator populates `status.backup` (`lastBackupTime`, `lastBackupTimestamp` 14-digit, `lastBackupStatus`, `lastBackupType`, `lastBackupJobName`, `cronJobName`, and `backupHistory[]` with `size`+`duration`). The 14-digit `lastBackupTimestamp` is guaranteed (CronJob Jobs derive it from `CompletionTime` in UTC), and backup status is refreshed on the periodic reconcile even in steady state (unchanged spec generation)
  - Pre-backup health checks verified by Scenario 77: every backup Job's `pre-backup-check` init container blocks the backup when any of four checks fails — **77a** segments-up (`gp_segment_configuration` `status='d'`), **77b** long-running transaction (older than 1 hour), **77c** S3 reachability (a **fail-closed SigV4-signed HEAD** to `${S3_ENDPOINT}/${S3_BUCKET}` returns non-2xx/3xx — replacing the prior best-effort `aws s3 ls`), and **77d** local disk space (< 1 GiB free). On a fault the `gpbackup` container never starts, the operator sets `lastBackupStatus=Failed` and emits a de-duplicated `Warning`/`BackupFailed` Event (one per failed Job; restore failures excluded); healing the fault lets a fresh backup reach `Success`
  - Incremental backup lifecycle verified by Scenario 78: **78a** `gpbackup.incremental: true` (or a per-request `type=incremental`) always renders `--incremental --leaf-partition-data` once each on the Job *and* CronJob (leaf-partition-data forced even when unset; de-duplicated when set); **78b** a full → modify AO table → incremental WITHOUT `--from-timestamp` auto-forms against the most recent compatible backup (same bucket+folder) and `status.lastBackupType` becomes `incremental`; **78c** an explicit `--from-timestamp <full-ts>` (`gpbackupOptions.fromTimestamp`) pins the base. Each backup Job/CronJob is labelled `avsoft.io/backup-type` (`full|incremental`) and the operator derives `status.lastBackupType`/`backupHistory[].type` from the **Job's** label (spec fallback). **78d** restoring from the latest incremental validates the full set (full + all incrementals) and restores; a missing intermediate makes `gprestore` refuse (restore Job fails) — the operator records `lastBackupStatus=Failed` and emits a de-duplicated `Warning`/`RestoreFailed` Event (distinct from `BackupFailed`)
  - Retention cleanup (all policies) verified by Scenario 79: after **each successful backup** the operator creates one idempotent cleanup Job `<cluster>-cleanup-<ts>` (label `avsoft.io/backup-operation=cleanup`) that enforces all three policies via the **real** `gpbackman` CLI (`woblerr/gpbackman` — no `delete --keep-full`/`--older-than`; it ships v0.8.1 in `cloudberry-backup:2.1.0`). **79a** `fullCount` and **79b** `incrementalCount` enumerate `gpbackman backup-info --type full|incremental` and delete the oldest excess with `gpbackman backup-delete --timestamp <ts> --cascade` (re-enumerating after each delete so cascade neither over- nor under-counts); **79c** `maxAge` (`"30d"`) runs `gpbackman backup-clean --older-than-days 30 --cascade`. **79d** the cleanup script prints `RETENTION_DELETED=<n>` (also to `/dev/termination-log`); the operator patches `avsoft.io/backup-retention-deleted=<n>` onto the Succeeded cleanup Job and the metrics loop turns it into the counter `cloudberry_backup_retention_deleted_total` (incremented per deletion)
  - Post-restore validation verified by Scenario 80: after **each Succeeded restore** the operator creates one idempotent validation Job `<cluster>-validate-<ts>` (label `avsoft.io/backup-operation=validate`) whose script runs four checks — **80a** row-count compare of ACTUAL restored per-table counts against the EXPECTED counts captured from gpbackup history (passed via the restore Job's `avsoft.io/expected-row-counts` JSON annotation), emitting `ROW_COUNT_MATCH`/`ROW_COUNT_MISMATCH` and failing (`exit 1`) on any discrepancy (empty map → best-effort `ROW_COUNT_PROBE_SKIPPED`, no fail); **80b** an optional `ANALYZE` (`ANALYZE_OK`) when `backup.validation.runAnalyze` (falls back to `gprestore.runAnalyze`) refreshes planner stats; **80c** a must-pass invalid-index scan (`relkind='i' AND NOT indisvalid` → `exit 1`); and **80d** a configurable health-check query (`backup.validation.healthCheckQuery`, default `SELECT 1`). The operator records the terminal status into `cloudberry_restore_validation_total{cluster,namespace,result}` (`result=success|failed`) and emits a de-duplicated `Warning`/`ValidationFailed` Event on a Failed Job — **80e** a deliberate mismatch (data-only restore into a pre-populated table) is FLAGGED (Failed + Warning + `{result="failed"}`), and a failed validation does **not** retroactively fail the Succeeded restore
  - Local (PVC-backed) backup destination verified by Scenario 81: with `destination.type: local` the operator mounts the named PVC (`local.persistentVolumeClaim`) as the `backup-data` volume at `local.path` (default `/backups`) on the backup/restore Job and runs `gpbackup`/`gprestore` with `--backup-dir <path>` and **NO** S3 plugin — `backupDestinationArgs(cluster)` seeds `--backup-dir` for local (and `--plugin-config` for s3); `buildGpbackupArgs`/`buildGprestoreArgs`/`renderToolScript` are destination-aware so a local Job has **no** `--plugin-config`, **no** `s3-plugin-config` volume, **no** `/etc/gpbackup` render (the S3-config envsubst is skipped so the local Job does not crash under `set -euo pipefail`), and **no** `S3_*`/`AWS_*` env. The operator creates **no** S3 ConfigMap and **no** Vault/Secret S3 credentials for local; the pre-backup `df` disk-space check (Scenario 77d, < 1 GiB free blocks) and gpbackman retention (`--backup-dir <path>` instead of `--plugin-config`) are local-aware. **MPP note:** `gpbackup --backup-dir` writes per-segment backup sets on the coordinator and every segment host; an RWO PVC on one Job pod holds one set, so the live data cycle targets a segment-visible `--backup-dir` via coordinator-exec while the PVC mount proves the operator wiring
  - Backup security and encryption verified by Scenario 82: **82a** the `<cluster>-backup-s3-config` ConfigMap (`BuildBackupS3ConfigMap`) holds **only** `${...}` placeholders — no real credentials on disk/ConfigMap; creds reach the pod **only** as `valueFrom.secretKeyRef` env (`buildS3CredentialEnv`); **82b** `renderToolScript` runs `envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml` at runtime so the resolved config (with creds) exists **only** in the ephemeral pod filesystem (the ConfigMap stays placeholders-only); **82c** backup/restore/validate/cleanup Jobs run as `cloudberry-backup-sa` with **minimal RBAC** — Helm values `backup.rbac.scopeSecrets=true` + `backup.rbac.secretNames=[…]` scope `secrets:[get]` to a `resourceNames` allow-list so the SA is **denied** `get` on unrelated Secrets while still reading the backup-relevant ones (the S3 credential Secret + `<cluster>-ssh-keys`/`<cluster>-admin-password`/`<cluster>-backup-s3-vault-creds`); default `scopeSecrets=false` keeps namespace-wide get for backward compatibility (production should opt in, unioning per-cluster Secret names); **82d** `S3Destination.encryption` is an enum (`on|off`, default `on`, CRD-enforced) — `buildS3Env` sets `S3_ENCRYPTION` and the template line `encryption: ${S3_ENCRYPTION}` flips so the rendered config carries `encryption: on`/`off` (the S3 plugin TLS/SSL option; verified via the option, not literal HTTPS, in the HTTP-only test env); **82e** new `spec.backup.jobTemplate.imagePullSecrets` are applied to the backup Job pod spec (and restore/validate/cleanup/CronJob pods) via `applyJobTemplatePod` → `addImagePullSecrets`, so Jobs can pull from a private registry. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario82-security-encryption.yaml`; live cycle `test/e2e/scripts/scenario82-security-encryption.sh`
  - Backup failure handling verified by Scenario 83: backup Jobs are bounded by `spec.backup.jobTemplate.backoffLimit` (default `2`) and `activeDeadlineSeconds` (default `7200`), both seeded by `buildJobSpec` and overridable per cluster (the override reaches every backup/restore/cleanup/validation Job and the CronJob `jobTemplate`). **83a** a force-failure (unreachable/bad-creds S3, caught fast by the 77c pre-backup HEAD) retries up to `backoffLimit` (up to 3 pod attempts) and ends with the Job condition `reason=BackoffLimitExceeded`; **83b** a backup outliving a low `activeDeadlineSeconds` is killed by Kubernetes at the deadline (`reason=DeadlineExceeded`). The operator now classifies a Job as `Failed` when `Status.Failed > 0` **OR** it carries a terminal Failed condition (`jobHasFailedCondition` — `batchv1.JobFailed`/`ConditionTrue`), applied to `backupJobStatus`/`backupJobStatusCode`/`validationJobResult` after the Succeeded precedence — so a deadline-killed Job (failed-pod count may be `0`) is reliably recorded `Failed`. A Failed backup sets `status.backup.lastBackupStatus=Failed`, `cloudberry_backup_last_status=1`, and emits the de-duplicated `Warning`/`BackupFailed` Event (Scenario 77); a success sets the metric to `0`. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario83-backup-failure.yaml`; live cycle `test/e2e/scripts/scenario83-backup-failure.sh`
  - Prometheus backup/restore metrics verified by Scenario 84: the `gpbackup_exporter` is the **operator `/metrics` endpoint** — the operator derives nine metrics (namespace `cloudberry`) from the observed backup/restore/cleanup Jobs + their `avsoft.io/*` annotations and exposes them on `/metrics` (vmagent scrapes them into VictoriaMetrics via the `prometheus.io/scrape` annotations; the Grafana operator dashboard renders a panel for each). The nine: `backup_total{type,result}`, `backup_duration_seconds{type}`, `backup_size_bytes{timestamp}`, `backup_last_success_timestamp`, `backup_last_status` (`0=success, 1=failed, 2=in-progress`), `restore_total{result}`, `restore_duration_seconds`, `backup_retention_deleted_total`, and `backup_job_status{job,operation}` (`0=pending, 1=running, 2=succeeded, 3=failed`). The outcome label on `backup_total`/`restore_total` is **`result`** (`success`|`failed`), **not** `status` — query `{result="success"}`/`{result="failed"}` (not renamed in code; dashboards/PromQL across 76–83 use `result`). All nine were already wired across Scenarios 76–83 (`internal/metrics/metrics.go` `initBackupMetrics`; recorded by `recordLatestBackupMetrics`/`recordBackupJobMetrics`/`applyBackupJobToStatus` in `internal/controller/admin_controller.go`), so Scenario 84 is a verification/doc/dashboard scenario with no operator code change. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario84-metrics.yaml`; live cycle `test/e2e/scripts/scenario84-metrics.sh`
- Session management: list active sessions from `pg_stat_activity`, cancel queries via `pg_cancel_backend()`, terminate sessions via `pg_terminate_backend()` (with PID validation and graceful degradation when DB is unavailable)
- Resource group management: create, list, assign, and delete resource groups for workload isolation
  - Create groups with concurrency, CPU, and memory limits
  - Assign database roles to resource groups (`ALTER ROLE ... RESOURCE GROUP`)
  - Query live resource groups from the database with CRD spec fallback
- API admin password via `CLOUDBERRY_API_ADMIN_PASSWORD` env var or auto-generated (persisted to K8s Secret `cloudberry-operator-admin-password`)

**CLI Companion**
- `cloudberry-ctl` for imperative operations through the operator API
- Table, JSON, and YAML output formats with deterministic column ordering
- Shell completion for bash, zsh, and fish
- Environment variable and config file support (priority: CLI flag > env var > config file > default)
- Verbose mode (`-v`) for HTTP request/response debugging
- Response body size limit (10 MiB) and URL-encoded path parameters
- Stub commands return clear "not yet implemented" errors

## Quick Start

```bash
# 1. Install the operator via Helm
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system --create-namespace

# 2. Create a minimal Cloudberry cluster
kubectl apply -f - <<EOF
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: my-cluster
  namespace: cloudberry-test
spec:
  image: "postgres:16"
  coordinator:
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
    storage:
      size: 5Gi
  segments:
    count: 2
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
    storage:
      size: 5Gi
EOF

# 3. Check cluster status
kubectl get cloudberryclusters -n cloudberry-test

# 4. Use cloudberry-ctl for management
cloudberry-ctl cluster status --cluster my-cluster --namespace cloudberry-test
```

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Kubernetes | >= 1.26 |
| Helm | >= 3.x |
| Go (for building) | >= 1.26.4 |
| kubectl | >= 1.26 |

Optional:
- **Vault** for secrets management
- **Keycloak** for OIDC authentication
- **Prometheus** for metrics collection
- **OpenTelemetry Collector** for distributed tracing

## Installation

### Helm (Recommended)

```bash
# Create the operator namespace
kubectl create namespace cloudberry-system

# Install with default values
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system

# Install with custom values
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set operator.logLevel=debug \
  --set vault.enabled=true \
  --set vault.address=http://vault:8200

# Verify the installation
kubectl get pods -n cloudberry-system
```

### From Source

```bash
# Clone the repository
git clone https://github.com/cloudberry-contrib/cloudberry-k8s.git
cd cloudberry-k8s

# Build binaries
make build

# Build Docker images
make docker-build

# Deploy via Helm
make helm-install
```

See [docs/installation.md](docs/installation.md) for detailed installation instructions and configuration options.

## Usage

### Creating a Cluster

Apply a `CloudberryCluster` manifest:

```yaml
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: production-cluster
  namespace: cloudberry-prod
spec:
  image: "postgres:16"
  coordinator:
    resources:
      requests:
        cpu: "2"
        memory: 4Gi
    storage:
      storageClass: fast-ssd
      size: 50Gi
  standby:
    enabled: true
    storage:
      size: 50Gi
  segments:
    count: 8
    mirroring:
      enabled: true
      layout: spread
    storage:
      storageClass: fast-ssd
      size: 200Gi
    antiAffinity: required
  auth:
    basic:
      enabled: true
      adminUser: gpadmin
      adminPasswordSecret:
        name: cloudberry-admin-password
        key: password
  monitoring:
    enabled: true
    serviceMonitor: true
  deletionPolicy: Retain
```

### Managing Cluster Lifecycle

```bash
# Check status
cloudberry-ctl cluster status --cluster my-cluster

# Stop cluster (fast mode)
cloudberry-ctl cluster stop --cluster my-cluster --mode fast

# Start cluster
cloudberry-ctl cluster start --cluster my-cluster

# Restart cluster
cloudberry-ctl cluster restart --cluster my-cluster
```

### Scaling Operations

```bash
# Scale out by increasing segment count
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"count": 6}}}'

# Scale in by decreasing segment count
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"count": 4}}}'

# Scale-in >50% requires confirmation annotation
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/confirm-scale-in=true

# Monitor scale progress
cloudberry-ctl cluster scale-status --cluster my-cluster

# Check for failed segments after a scale-out failure
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.failedSegments}' | jq .

# Check scale events (blocked, failed, completed)
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -E 'Scale'
```

### Configuration Management

```bash
# View current parameters
cloudberry-ctl config get --cluster my-cluster

# Set a parameter
cloudberry-ctl config set --cluster my-cluster --param work_mem --value 256MB

# Reload configuration (no restart)
cloudberry-ctl config reload --cluster my-cluster
```

### High Availability Operations

```bash
# Check mirroring status
cloudberry-ctl ha mirroring status --cluster my-cluster

# Start incremental recovery
cloudberry-ctl ha recovery start --cluster my-cluster --type incremental

# Rebalance segments
cloudberry-ctl ha rebalance --cluster my-cluster

# Rebalance specific tables
cloudberry-ctl ha rebalance --cluster my-cluster --tables orders,customers

# Check rebalance status
cloudberry-ctl ha rebalance --cluster my-cluster --status

# Check standby status
cloudberry-ctl ha standby status --cluster my-cluster
```

See [docs/user-guide.md](docs/user-guide.md) for the complete user guide.

## Configuration

### Helm Chart Values

Key configuration options in `values.yaml`:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Operator replicas | `1` |
| `image.repository` | Operator image | `cloudberry-operator` |
| `operator.logLevel` | Log level | `info` |
| `operator.leaderElection` | Enable leader election | `true` |
| `operator.apiAddress` | REST API bind address | `:8090` |
| `operator.webhookEnabled` | Enable admission webhooks | `false` |
| `env.CLOUDBERRY_API_ADMIN_PASSWORD` | Admin password for the REST API (auto-generated and persisted to Secret if not set) | (generated) |
| `vault.enabled` | Enable Vault integration | `false` |
| `oidc.enabled` | Enable OIDC authentication | `false` |
| `telemetry.enabled` | Enable OTLP tracing | `false` |
| `telemetry.otlpInsecure` | Disable TLS for OTLP exporter | `false` |
| `metrics.enabled` | Enable Prometheus metrics | `true` |
| `webhook.enabled` | Enable admission webhooks | `true` |
| `webhook.certSource` | Certificate source (`self-signed` or `vault-pki`) | `self-signed` |

See [docs/installation.md](docs/installation.md) for the full values reference.

### CRD Configuration

The `CloudberryCluster` CRD supports configuration for:

- **Coordinator**: resources, storage (with online expansion), port, node selectors
- **Standby**: enable/disable, resources, storage (with online expansion)
- **Segments**: count, primaries per host, mirroring layout, anti-affinity, rebalance configuration, storage (with online expansion)
- **Authentication**: basic auth, OIDC, HBA rules, SSL/TLS
- **Configuration**: cluster-wide, coordinator-only, per-database, per-role parameters
- **High Availability**: FTS probe settings, checksums
- **Vault**: address, auth method, secret path
- **Monitoring**: metrics port, ServiceMonitor
- **Telemetry**: OTLP endpoint, protocol, sampling rate

See [docs/api-reference.md](docs/api-reference.md) for the complete API reference.

## cloudberry-ctl CLI

`cloudberry-ctl` provides imperative access to cluster management:

```bash
# Build from source
make build-ctl

# Show version
cloudberry-ctl version

# Shell completion
cloudberry-ctl completion bash > /etc/bash_completion.d/cloudberry-ctl
cloudberry-ctl completion zsh > "${fpath[1]}/_cloudberry-ctl"
```

See [docs/cloudberry-ctl.md](docs/cloudberry-ctl.md) for the full command reference.

## Development

### Project Structure

```
cloudberry-k8s/
├── api/v1alpha1/          # CRD Go types and generated code
├── cmd/
│   ├── operator/          # Operator entry point
│   └── cloudberry-ctl/    # CLI entry point
├── internal/
│   ├── api/               # REST API server with rate limiting
│   ├── auth/              # Authentication providers (bcrypt, OIDC/JWT)
│   ├── builder/           # Kubernetes resource builders
│   ├── certmanager/       # Webhook TLS cert lifecycle (Vault PKI / self-signed)
│   ├── config/            # Operator configuration
│   ├── controller/        # Reconciliation controllers
│   ├── ctl/               # Operator API client for cloudberry-ctl
│   ├── db/                # Database client (pgx) and client factory
│   ├── metrics/           # Prometheus metrics
│   ├── telemetry/         # OpenTelemetry tracing
│   ├── util/              # Shared utilities
│   ├── vault/             # Vault client
│   └── webhook/           # Admission webhooks
├── deploy/
│   ├── helm/              # Helm chart
│   └── docker/            # Docker-related files
├── test/
│   ├── e2e/               # End-to-end tests
│   ├── functional/        # Functional tests
│   ├── integration/       # Integration tests
│   ├── cases/             # Shared test cases
│   └── testutil/          # Test utilities
├── specifications/        # Design specifications
├── Dockerfile             # Operator container image
├── Dockerfile.ctl         # CLI container image
├── Makefile               # Build automation
└── .github/workflows/     # CI/CD pipelines
```

### Building

```bash
# Build everything
make build

# Build operator only
make build-operator

# Build CLI only
make build-ctl

# Build Docker images
make docker-build

# Generate CRD manifests and deepcopy
make generate
make manifests
```

### Code Quality

```bash
# Run linter
make lint

# Run go vet
make vet

# Format code
make fmt

# Run vulnerability check
make vuln
```

## Testing

```bash
# Unit tests
make test

# Unit tests with coverage report
make test-cover

# Functional tests
make test-functional

# Integration tests (requires Docker Compose test environment)
make test-env-up       # Start 9 services: Vault, Keycloak, MinIO, Kafka, RabbitMQ, VictoriaMetrics, Grafana, Tempo
make test-env-setup    # Configure services (Vault PKI, Keycloak realm, MinIO buckets, etc.)
make test-integration
make test-env-down     # Tear down

# End-to-end tests (requires Kubernetes cluster)
make test-e2e

# All tests
make test-all
```

**Test Data Loading** (prerequisite for scale/rebalance/performance tests):

```bash
# Load Scenario 7 test data (~1.45M rows, ~218 MB across 5 tables)
bash test/scenarios/scenario7_load_data.sh
```

Scenario 7 populates the `mydb` database with realistic test data including Pareto-skewed distributions and rebalance exclusion patterns. Run this before any performance, scale, or rebalance tests. See [docs/user-guide.md](docs/user-guide.md#test-data-setup) for details.

**Functional test scenarios** cover the full operator lifecycle: cluster bootstrap (1), config hot-reload and rolling restart (2), stop/start modes (3), maintenance operations (4), session management (5), resource groups (6), test data loading (7), scale-out (8), scale-in (9), rebalancing (10), scale-out failure (11), scale-in confirmation (12), PV expansion (13), cluster upgrade with rollback (14), error handling and observability (15), cluster deletion (16), mirroring enable/disable (19), automatic segment failover via FTS (20), bootstrap workload management via CRD (25), webhook validation negative tests for backup configuration (69a–69j), webhook defaults verification for backup configuration (70), full S3 backup configuration with Secret and Vault credential sources (71), backup infrastructure deployment (72), and on-demand backup with per-request gpbackup options incl. the `noCompression` override (73). See [docs/development.md](docs/development.md) for detailed test descriptions.

The project targets **90%+ unit test statement coverage** per package. Total coverage: **91.4%** with all 14 internal packages at 90%+. Key coverage: `internal/vault` at 99%, `internal/metrics` at 100%, `internal/api` at ~96%, `internal/db` at ~92%, `internal/certmanager` at ~93%, `internal/controller` at ~90.1%, `internal/auth` at ~97.6%, `internal/idle` at ~97%, `cmd/cloudberry-ctl` at ~91.6%, `cmd/operator` at ~30.0%. All **1,936 tests** pass (functional: 1,063, e2e: 833, integration: 38). See [docs/development.md](docs/development.md) for the full development and testing guide.

## Monitoring Quick Start

Deploy the monitoring stack (vmagent + OpenTelemetry Collector) alongside the operator:

```bash
# Deploy monitoring stack to Kubernetes
make monitoring-deploy

# Check monitoring status
make monitoring-status

# Remove monitoring stack
make monitoring-undeploy
```

Or deploy the operator with monitoring enabled via Helm:

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set metrics.enabled=true \
  --set serviceMonitor.enabled=true \
  --set telemetry.enabled=true \
  --set telemetry.otlpEndpoint=otel-collector:4317 \
  --set telemetry.otlpInsecure=true
```

Pre-built Grafana dashboards are available in the `monitoring/grafana/` directory. The `monitoring/grafana/cloudberry-operator.json` dashboard visualizes all operator metrics, including a **Security & Lifecycle** section covering certificate rotation and expiry, Vault operations, webhook admissions, upgrades, rolling restarts, and recovery.

## Deployment Status

The operator has been verified in production-like deployments:

- **Operator**: Deployed into `cloudberry-test` via `make helm-install-test` with Vault-PKI webhook certificates (CN issued by the Vault Root CA) using Vault Kubernetes auth, plus Keycloak OIDC. Vault Kubernetes auth is configured by `make test-env-setup` (`setup-vault-k8s-auth.sh`)
- **Cluster**: HA cluster (`scenario67`) with standby coordinator (`standbyReady`), segment mirroring (`InSync`), and Vault-PKI cluster TLS (the `scenario67-tls` Secret)
- **Exporters**: postgres-exporter on the coordinator, standby, and every segment primary and mirror, plus the coordinator-only cloudberry-query-exporter, producing metrics into VictoriaMetrics
- **Data**: ~100 MB of test data loaded into `mydb`
- **Backup/Restore**: Scenario 71 verified live for both credential variants — a real 100 MB `mydb` backup → S3 (MinIO, bucket `cloudberry-backups/backups`) → drop → restore cycle passes with matching row counts. Runs the MPP backup inside the coordinator pod (coordinator→segment SSH dispatch) via `test/e2e/scripts/scenario71-backup-restore.sh` for the `scenario71-secret` (Secret credentials) and `scenario71-vault` (Vault credentials) sample clusters
- **Backup Infrastructure**: Scenario 72 verified — toolchain image binaries (`gpbackup`/`gprestore`/`gpbackup_s3_plugin` in `cloudberry-backup:2.1.0`), backup RBAC (`cloudberry-backup-sa` + `cloudberry-backup-role`), the `<cluster>-backup-s3-config` ConfigMap, Job labels/namespace + env (`envsubst` → `/tmp/s3-config.yaml`), and the explicit `jobTemplate` overrides from `deploy/helm/cloudberry-operator/config/samples/scenario72-backup-infrastructure.yaml`
- **On-Demand Backup Options**: Scenario 73 verified — `POST /clusters/{name}/backups` renders per-request `gpbackupOptions` into the `gpbackup` invocation and creates a Job directly (not via the CronJob). 73a (standard options) yields `--compression-level 6 --compression-type zstd --jobs 4 --with-stats --without-globals --include-schema public --include-schema analytics`; 73b (`noCompression` override) yields `--no-compression` with `--compression-level` omitted. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario73-backup-options.yaml`
- **Single Data File + Full-Option Restore**: Scenario 74 verified live — a `gpbackupOptions{singleDataFile:true, copyQueueSize:4}` backup yields `--single-data-file --copy-queue-size 4` (no `--jobs`), requires `gpbackup_helper` on every segment, and writes one consolidated `gpbackup_<contentid>_<TS>.gz` per segment. The full-option restore resolves `gprestore`'s mutual-exclusivity rules — `--include-table` over `--include-schema`, `--run-analyze` over `--with-stats`, `--copy-queue-size` instead of `--jobs` for single-data-file restores, and redirect-schema pre-creation — so `mydb_restored` is populated, objects land in the `restored` schema, and ANALYZE runs (`pg_stats` populated). Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario74-single-data-file.yaml`; live cycle `test/e2e/scripts/scenario74-single-data-file.sh`
- **Compression Matrix (gzip vs zstd)**: Scenario 75 verified live — two on-demand backups of the same `public`-schema data at `--compression-level 6` (one `--compression-type gzip`, one `--compression-type zstd`) both succeed (2/2 tables) and both restore cleanly into separate redirect DBs (`mydb_gzip_restored` / `mydb_zstd_restored`, row counts `users=9533` / `orders=476625`). On-disk data-file totals differ as expected — gzip 4,204,206 B vs zstd 3,759,562 B (zstd smaller by 444,644 B, ~10.6%); data files are named `…_<oid>.gz` vs `…_<oid>.zst`. The `zstd` CLI now ships in the cluster image (`cloudberry-official:2.1.0`) — required because `gpbackup` pipes segment `COPY` output through `zstd --compress`. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario75-compression-matrix.yaml`; live cycle `test/e2e/scripts/scenario75-compression-matrix.sh`
- **Scheduled Backup + Status Population**: Scenario 76 verified live — `spec.backup.schedule` reconciles the CronJob `scenario76-backup-schedule` (`ownerReferences` → the cluster, `concurrencyPolicy: Forbid`, `successfulJobsHistoryLimit`/`failedJobsHistoryLimit` = 3, pod `restartPolicy: Never`) which fires (test override `*/2 * * * *`) and spawns a Job; after the backup succeeds the operator populates `status.backup` — `lastBackupTimestamp=20260607224409` (14-digit), `lastBackupStatus=Success`, `lastBackupType=full`, `cronJobName=scenario76-backup-schedule`, and `backupHistory[0]={timestamp, type:full, status:Success, size:4204206, duration:4s}`. The 14-digit timestamp is derived from the Job's `CompletionTime` (UTC) for CronJob Jobs, and backup status is refreshed on the periodic reconcile even in steady state. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario76-scheduled-backup.yaml`; live cycle `test/e2e/scripts/scenario76-scheduled-backup.sh`
- **Pre-Backup Health Checks**: Scenario 77 verified live — the backup Job's `pre-backup-check` init container blocks the backup when any of four checks fails: **77a** segments-up (`gp_segment_configuration` `status='d'` > 0), **77b** long-running transaction (non-idle txn older than 1 hour / `longRunningTxnThresholdSeconds=3600`), **77c** S3 reachability (a **fail-closed SigV4-signed HTTP HEAD** to `${S3_ENDPOINT}/${S3_BUCKET}`, path-style, blocks on non-2xx/3xx — `403`/`404`/timeout — replacing the prior best-effort `aws s3 ls`), and **77d** local disk space (`df` free < 1 GiB / `minBackupDiskFreeKB=1048576`). On a fault the `gpbackup` container never starts, the operator sets `lastBackupStatus=Failed` and emits a de-duplicated `Warning`/`BackupFailed` Event (one per failed Job; restore-operation failures excluded); healing the fault lets a fresh backup reach `Success`. Sample CRs `deploy/helm/cloudberry-operator/config/samples/scenario77-s3-prebackup.yaml` (S3 — 77a/77b/77c) and `scenario77-local-prebackup.yaml` (local — 77d); live cycle `test/e2e/scripts/scenario77-prebackup-checks.sh`
- **Incremental Backup Lifecycle**: Scenario 78 verified live — **78a** `gpbackup.incremental: true` (or per-request `type=incremental`) always renders `--incremental --leaf-partition-data` exactly once each on the backup Job *and* CronJob (`--leaf-partition-data` forced — gpbackup requires it for incrementals — even when `leafPartitionData` is unset; de-duplicated when set), via `appendIncrementalArgs`. **78b** a full → modify an append-optimized (AO) table → an incremental WITHOUT `--from-timestamp` auto-forms against the most recent compatible backup on the **same bucket+folder** (`/backups`), and `status.lastBackupType` becomes `incremental`. **78c** an explicit `--from-timestamp <full-ts>` (`gpbackupOptions.fromTimestamp` / `BackupJobOptions.FromTimestamp`) pins the base. Each backup Job/CronJob is labelled `avsoft.io/backup-type` (`util.LabelBackupType`, `full|incremental`) and the operator derives `status.lastBackupType` + `backupHistory[].type` from the **Job's** label (`backupTypeFromJob`, spec fallback). **78d** restoring from the latest incremental validates the full set (full + all incrementals) and restores; deleting an intermediate makes `gprestore` refuse (restore Job fails) — the operator records `lastBackupStatus=Failed` and emits a de-duplicated `Warning`/`RestoreFailed` Event (`api/v1alpha1.EventReasonRestoreFailed`, distinct from `BackupFailed`). Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario78-incremental-backup.yaml`; live cycle `test/e2e/scripts/scenario78-incremental-backup.sh`
- **Retention Cleanup (All Policies)**: Scenario 79 verified — with `retention.{fullCount:3, incrementalCount:10, maxAge:"30d"}` set, the operator creates one idempotent cleanup Job `<cluster>-cleanup-<latest-backup-ts>` (`util.RetentionCleanupJobName`, label `avsoft.io/backup-operation=cleanup`, owner-ref'd to the cluster) after each successful backup. The rendered `buildGpbackmanRetentionScript` (bash-3.2-safe, injection-safe) uses the **real** `gpbackman` CLI — **79a** `fullCount`/**79b** `incrementalCount` via `gpbackman backup-info --type full|incremental` + `gpbackman backup-delete --timestamp <oldest> --cascade` (re-enumerating after each delete so a cascaded delete neither over- nor under-counts), and **79c** `maxAge` via `gpbackman backup-clean --older-than-days <N> --cascade` (`parseMaxAgeDays("30d")=30`). There is **no** `gpbackman delete`/`--keep-full`/`--older-than` (those don't exist). **79d** the script emits `RETENTION_DELETED=<n>` (stdout + `/dev/termination-log`); `ensureRetentionCleanup`/`reconcileRetentionCleanupAnnotations` patches `avsoft.io/backup-retention-deleted=<n>` onto the Succeeded cleanup Job once, and the metrics loop increments `cloudberry_backup_retention_deleted_total`. `gpbackman` v0.8.1 ships in `cloudberry-backup:2.1.0` (`Dockerfile.cloudberry-backup`). Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario79-retention.yaml`; live cycle `test/e2e/scripts/scenario79-retention.sh`
- **Post-Restore Validation**: Scenario 80 verified — after each Succeeded restore the operator's `ensurePostRestoreValidation` creates one idempotent validation Job `<cluster>-validate-<ts>` (`util.PostRestoreValidationJobName`, label `avsoft.io/backup-operation=validate`, owner-ref'd to the cluster) whose rendered `postRestoreValidationScript` runs (injection-safe, `set -euo pipefail`): **80a** a per-table row-count compare of ACTUAL restored counts vs the EXPECTED counts captured from gpbackup history (`createValidationJob` reads the restore Job's `avsoft.io/expected-row-counts` JSON annotation into `ValidationJobOptions.ExpectedRowCounts`), emitting `ROW_COUNT_MATCH`/`ROW_COUNT_MISMATCH` and `exit 1` on any mismatch (empty map → best-effort `ROW_COUNT_PROBE_SKIPPED`, never fails); **80b** an optional database-wide `ANALYZE` (`ANALYZE_OK`) when `validationRunAnalyze` (`backup.validation.runAnalyze`, falling back to `gprestore.runAnalyze`); **80c** a must-pass invalid-index scan (`relkind='i' AND NOT i.indisvalid` → `exit 1`); and **80d** a configurable health-check query (`backup.validation.healthCheckQuery`, default `SELECT 1`). New CRD block `BackupSpec.Validation` (`BackupValidation{enabled, healthCheckQuery, runAnalyze}`). `observeValidationJobs`/`recordValidationOutcome` reads the validation Job's terminal status (de-duped via `avsoft.io/validation-recorded`), records `cloudberry_restore_validation_total{cluster,namespace,result}` (`result=success|failed`, `metrics.RecordRestoreValidation`), and on Failed emits a de-duplicated `Warning`/`ValidationFailed` Event (`api/v1alpha1.EventReasonValidationFailed`) — **80e** a deliberate mismatch (data-only restore into a pre-populated table) is FLAGGED (Failed + Warning + `{result="failed"}`) while the **restore** Job remains Succeeded (validation never retroactively fails the restore). Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario80-post-restore-validation.yaml`; live cycle `test/e2e/scripts/scenario80-post-restore-validation.sh`
- **Local Backup Destination**: Scenario 81 verified — with `destination.type: local` the operator mounts the named PVC (`local.persistentVolumeClaim`) as the `backup-data` volume at `local.path` (default `/backups`, `buildBackupVolumes`/`buildBackupVolumeMounts`) on the backup/restore Job and runs `gpbackup`/`gprestore` with `--backup-dir <path>` and **NO** S3 plugin. `backupDestinationArgs(cluster)` seeds the leading args (local → `--backup-dir <localBackupDir>`, s3/nil → `--plugin-config /tmp/s3-config.yaml`); `buildGpbackupArgs`/`buildGprestoreArgs` take the `cluster` and `renderToolScript` skips the `/etc/gpbackup` → `/tmp/s3-config.yaml` envsubst render for local (so the local Job does not crash under `set -euo pipefail`) — a local Job has **no** `--plugin-config`, **no** `s3-plugin-config` ConfigMap volume, **no** `/etc/gpbackup` mount, and **no** `S3_*`/`AWS_*` env (`buildBackupEnv`). The operator creates **no** S3 ConfigMap (`ensureBackupS3ConfigMap` no-ops via `BuildBackupS3ConfigMap` nil) and **no** Vault/Secret S3 credentials (`ensureBackupS3VaultCredentials` no-ops when `Destination.S3 == nil`). The pre-backup local disk-space check (`preBackupDestinationCheck` → `df -Pk`, `< minBackupDiskFreeKB = 1 GiB` blocks — Scenario 77d) and `buildGpbackmanRetentionScript` (`--backup-dir <path>` instead of `--plugin-config`) are local-aware. **MPP note:** `gpbackup --backup-dir` writes per-segment backup sets on the coordinator and every segment host, so a single RWO PVC on one Job pod holds one set — the Job-spec assertions prove the operator wiring (PVC mount + `--backup-dir` + no plugin) while the real `gpbackup`/`gprestore` data cycle runs via coordinator-exec into a segment-visible `--backup-dir`. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario81-local-destination.yaml`; live cycle `test/e2e/scripts/scenario81-local-destination.sh`
- **Backup Security and Encryption**: Scenario 82 verified — **82a** the `<cluster>-backup-s3-config` ConfigMap (`BuildBackupS3ConfigMap`) carries **only** `${...}` placeholders (region/endpoint/`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/bucket/folder/encryption/multipart) — no real credentials on disk/ConfigMap; creds reach the running pod **only** as `valueFrom.secretKeyRef` env (`buildS3CredentialEnv`, no literal `value:`). **82b** `renderToolScript` runs `envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml` at container runtime (ConfigMap mounted read-only at `/etc/gpbackup`), so the resolved config with credentials exists **only** in the ephemeral pod filesystem (never a ConfigMap/Secret/host; the ConfigMap stays placeholders-only). **82c** backup/restore/validate/cleanup Jobs run as `cloudberry-backup-sa` with **minimal RBAC**: Helm values `backup.rbac.scopeSecrets=true` + `backup.rbac.secretNames=[…]` render the `cloudberry-backup-role` `secrets:[get]` rule with a `resourceNames` allow-list (`deploy/helm/cloudberry-operator/templates/backup-rbac.yaml`), so a `get` on an **unrelated** Secret is **denied** while the backup-relevant Secrets (the S3 credential Secret + `<cluster>-ssh-keys`/`<cluster>-admin-password`/`<cluster>-backup-s3-vault-creds`) stay readable and backups still succeed. Default `scopeSecrets=false` keeps namespace-wide get for backward compatibility — production should set `scopeSecrets=true` and union all per-cluster Secret names (the SA/Role are namespace-fixed; kubelet-driven `secretKeyRef`/volume injection is unaffected). **82d** `S3Destination.encryption` is an enum (`on|off`, default `on`, CRD-enforced) — `buildS3Env` sets `S3_ENCRYPTION` and the template line `encryption: ${S3_ENCRYPTION}` flips so the rendered `/tmp/s3-config.yaml` carries `encryption: on`/`off` (the `gpbackup_s3_plugin` TLS/SSL option; asserted via the plugin option, not literal HTTPS, in the HTTP-only test env). **82e** new `spec.backup.jobTemplate.imagePullSecrets` (`BackupJobTemplate.ImagePullSecrets`) are applied to the backup Job pod spec — and the restore/validate/cleanup/CronJob pods — via `applyJobTemplatePod` → the shared `addImagePullSecrets` helper, so Jobs can pull from a private registry. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario82-security-encryption.yaml` (`scenario82-s3`, `encryption: on`, `jobTemplate.imagePullSecrets: [{name: regcred}]`, the `s3-credentials` + `unrelated-secret` Secrets); live cycle `test/e2e/scripts/scenario82-security-encryption.sh`
- **Backup Failure Handling**: Scenario 83 verified — backup Jobs are bounded by two Kubernetes Job guards seeded in `buildJobSpec` (`internal/builder/backup_builder.go`) and overridable per cluster via `spec.backup.jobTemplate`: `backoffLimit` (`defaultBackoffLimit=2`) and `activeDeadlineSeconds` (`defaultActiveDeadlineSeconds=7200`); the override reaches every backup/restore/cleanup/validation Job **and** the backup CronJob's `jobTemplate` (a nil field keeps the default). **83a** a force-failure (unreachable/bad-creds S3, failing fast at the 77c pre-backup SigV4 HEAD) retries up to `backoffLimit` (`Status.Failed` grows toward `backoffLimit+1` — up to 3 pod attempts) and ends with the Job condition `type=Failed reason=BackoffLimitExceeded`; **83b** a backup outliving a low `activeDeadlineSeconds` is killed by Kubernetes at the deadline with `type=Failed reason=DeadlineExceeded`. The operator's `jobHasFailedCondition(job)` (`internal/controller/admin_controller.go`) treats any Job carrying a `batchv1.JobFailed`/`ConditionTrue` condition as failed, so `backupJobStatus`/`backupJobStatusCode`/`validationJobResult` classify a Job `Failed` when `Status.Failed > 0` **OR** the Failed condition is set (after the Succeeded precedence) — reliably recording a deadline-killed Job (failed-pod count may be `0`) as `Failed`. `applyBackupJobToStatus` → `recordLatestBackupMetrics`/`emitBackupFailureEvent` then set `status.backup.lastBackupStatus=Failed`, `cloudberry_backup_last_status=1` (`0=success, 1=failed, 2=in-progress`), and the de-duplicated `Warning`/`BackupFailed` Event (Scenario 77); a successful backup sets the metric to `0`. The production default `activeDeadlineSeconds` stays the safe `7200` — the live deadline test materializes a per-run Job with `activeDeadlineSeconds: 5` + a long `sleep`. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario83-backup-failure.yaml`; live cycle `test/e2e/scripts/scenario83-backup-failure.sh`
- **Prometheus Backup/Restore Metrics**: Scenario 84 verified — the `gpbackup_exporter` is implemented as the **operator metric endpoint**: the operator derives **nine** backup/restore metrics (namespace `cloudberry`) from the observed backup/restore/cleanup Jobs + their `avsoft.io/*` annotations and history outcomes, and exposes them on its own Prometheus `/metrics` endpoint (no separate sidecar binary). vmagent scrapes the operator `/metrics` (via `prometheus.io/scrape` annotations on the operator Deployment/Service) into VictoriaMetrics, and the Grafana operator dashboard (`monitoring/grafana/cloudberry-operator.json`) renders a panel for each. The nine: `cloudberry_backup_total{cluster,namespace,type,result}`, `cloudberry_backup_duration_seconds{cluster,namespace,type}`, `cloudberry_backup_size_bytes{cluster,namespace,timestamp}`, `cloudberry_backup_last_success_timestamp{cluster,namespace}`, `cloudberry_backup_last_status{cluster,namespace}` (`0=success, 1=failed, 2=in-progress`), `cloudberry_restore_total{cluster,namespace,result}`, `cloudberry_restore_duration_seconds{cluster,namespace}`, `cloudberry_backup_retention_deleted_total{cluster,namespace}`, and `cloudberry_backup_job_status{cluster,namespace,job,operation}` (`0=pending, 1=running, 2=succeeded, 3=failed`). **The outcome label on `backup_total`/`restore_total` is `result` (`success`|`failed`), NOT `status`** — query `{result="success"}`/`{result="failed"}`; the label is not renamed in code (dashboards/PromQL across Scenarios 76–83 query `result`, and `RecordBackup`/`RecordRestore` lower-case the Job status). All nine were already defined + wired across Scenarios 76–83 (`internal/metrics/metrics.go` `initBackupMetrics`; recorded from observed Jobs by `recordLatestBackupMetrics`/`recordBackupJobMetrics`/`applyBackupJobToStatus` in `internal/controller/admin_controller.go`, with the per-Job code from `backupJobStatusCode` and the `type` from `backupTypeFromJob`), so Scenario 84 required **no** operator code change — it is a verification + documentation + dashboard scenario asserting all nine metrics in VictoriaMetrics over a full lifecycle (full + incremental backup, restore, retention cleanup, forced failure). Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario84-metrics.yaml`; functional `test/functional/scenario84_metrics_test.go`, integration `test/integration/scenario84_metrics_integration_test.go`, e2e `test/e2e/scenario84_metrics_e2e_test.go`, test-case catalog `Scenario84MetricCases` (`test/cases/test_cases.go`); live cycle `test/e2e/scripts/scenario84-metrics.sh`
- **Dashboards**: All Grafana dashboards in `monitoring/grafana/` (operator, exporters, node) reflecting live metrics; published via `make grafana-publish`
- **Monitoring**: vmagent (remote-writing to VictoriaMetrics), Vector (tailing `kubernetes_logs` to VictoriaLogs), OpenTelemetry Collector, and node-exporter deployed alongside the operator via `make monitoring-deploy`
- **Test Environment**: Docker Compose with 9 services (Vault, Keycloak, MinIO, Kafka, RabbitMQ, VictoriaMetrics, Grafana, Tempo)
- **Quality gates**: build PASS, `golangci-lint` 0 issues, `govulncheck` no vulnerabilities
- **Tests**: unit (91.4% coverage), functional, integration, and e2e (881 e2e cases) all PASS; performance smoke 900 requests / 0 errors
- **Coverage**: 91.4% overall project coverage

## Performance Characteristics

Based on performance testing (2026-05-19, 287,122 total requests, zero errors):

| Endpoint Type | p50 | p95 | p99 | Peak RPS |
|---------------|-----|-----|-----|----------|
| Health (`/healthz`, `/readyz`) | 2.7ms | 6.5ms | 10.6ms | 12,637 |
| API (authenticated, bcrypt) | 605ms | 794ms | 885ms | ~6 |

- **Health endpoints**: Sub-3ms p50 latency, 12,637 RPS peak throughput
- **API endpoints**: Latency dominated by bcrypt authentication (~100ms/request)
- **Stability**: Zero errors across all load conditions, stable 82MB memory footprint
- **Throughput ceiling**: Health endpoints scale linearly to 1,000 concurrent connections

See [test/performance/README.md](test/performance/README.md) for full test documentation and SLO targets.

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | System design, components, and data flows |
| [Installation](docs/installation.md) | Prerequisites, Helm installation, and configuration |
| [User Guide](docs/user-guide.md) | Creating clusters, lifecycle management, HA, auth |
| [API Reference](docs/api-reference.md) | REST API endpoints, schemas, and error codes |
| [cloudberry-ctl](docs/cloudberry-ctl.md) | CLI installation, configuration, and command reference |
| [Development](docs/development.md) | Development setup, building, testing, and contributing |

## Contributing

Contributions are welcome. To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes with tests
4. Run the full test suite (`make test && make lint`)
5. Commit your changes (`git commit -m 'Add my feature'`)
6. Push to the branch (`git push origin feature/my-feature`)
7. Open a Pull Request

### Guidelines

- Follow the existing code style and conventions
- Write unit tests for all new code (target 90%+ coverage)
- Update documentation for user-facing changes
- Use conventional commit messages
- Ensure `make lint` passes before submitting

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
