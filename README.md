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
- [Changelog](CHANGELOG.md)
- [Contributing](#contributing)
- [License](#license)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                         │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                 cloudberry-operator                       │  │
│  │                                                           │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐   │  │
│  │  │   Cluster    │ │     HA       │ │   Auth / Admin   │   │  │
│  │  │  Controller  │ │  Controller  │ │   Controllers    │   │  │
│  │  └──────┬───────┘ └──────┬───────┘ └────────┬─────────┘   │  │
│  │         └────────────────┼──────────────────┘             │  │
│  │                          │                                │  │
│  │  ┌───────────────────────┴─────────────────────────────┐  │  │
│  │  │           Reconciliation Engine                     │  │  │
│  │  │         (controller-runtime / kubebuilder)          │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  │                                                           │  │
│  │  ┌──────────────────────────────────────────────────────┐ │  │
│  │  │  REST API Server (:8090)                             │ │  │
│  │  │  Rate Limiter → Auth Middleware → Handlers           │ │  │
│  │  └──────────────────────────────────────────────────────┘ │  │
│  │                                                           │  │
│  │  ┌──────────┐  ┌───────────┐  ┌────────────────────────┐  │  │
│  │  │ Metrics  │  │ Telemetry │  │   Auth Middleware      │  │  │
│  │  │ (Prom)   │  │  (OTLP)   │  │ (bcrypt + OIDC/JWT)    │  │  │
│  │  └──────────┘  └───────────┘  └────────────────────────┘  │  │
│  │                                                           │  │
│  │  ┌──────────────────────────────────────────────────────┐ │  │
│  │  │  Cert Manager (Vault PKI / Self-Signed)              │ │  │
│  │  └──────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                  Cloudberry Cluster                       │  │
│  │  ┌──────────────┐  ┌──────────────┐                       │  │
│  │  │ Coordinator  │  │   Standby    │                       │  │
│  │  │ StatefulSet  │  │ StatefulSet  │                       │  │
│  │  └──────────────┘  └──────────────┘                       │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │            Segment StatefulSets                     │  │  │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐           │  │  │
│  │  │  │Primary 0 │  │Primary 1 │  │Primary N │           │  │  │
│  │  │  │Mirror  0 │  │Mirror  1 │  │Mirror  N │           │  │  │
│  │  │  └──────────┘  └──────────┘  └──────────┘           │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │
│  │    Vault     │  │   Keycloak   │  │   Observability    │     │
│  │  (optional)  │  │  (OIDC IdP)  │  │      Stack         │     │
│  └──────────────┘  └──────────────┘  └────────────────────┘     │
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
  - Backup-on-delete: optional pre-deletion backup Job when `backupOnDelete: true`. With `deletionPolicy: Delete`, the operator **waits for the backup Job to finish before deleting the PVCs** (bounded by a 30-minute timeout so deletion never wedges); terminal outcomes are recorded on `cloudberry_backup_on_delete_total{result="completed"|"failed"}`
  - PVC events: `PVCsRetained` for Retain policy, `PVCsDeleted` for Delete policy
  - Deletion lifecycle events: `Deleting` → `BackupOnDelete` → `BackupOnDeleteCompleted`/`BackupOnDeleteFailed` → `PVCsRetained`/`PVCsDeleted` → `Deleted`

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
- Manual standby activation (`avsoft.io/action=activate-standby` or `cloudberry-ctl ha standby activate`) **actually promotes** the standby coordinator via `pg_promote()` (`PromoteStandby`), with at-most-once semantics (the annotation is removed before promotion is attempted)
- Segment recovery (gprecoverseg-equivalent) is **not implemented yet**: the `avsoft.io/recovery` annotation is acknowledged and removed, an explicit `RecoveryNotImplemented` Warning event is emitted, and `cloudberry_recovery_operations_total` records `result="noop"` (never a fake `completed`)
- Manual segment rebalancing with configurable skew threshold, parallelism, and table exclusion patterns; when the in-database rebalance fails, the fallback rebalance Job is tracked to its terminal state (success records `rebalance`, failure records `rebalance-failed` on `cloudberry_scale_operations_total` — no fire-and-forget successes)
- Rebalance status API and CLI (`--status`, `--tables` flags)
- Data skew coefficient metric (`cloudberry_data_skew_coefficient`)

**Authentication & Authorization**
- Dual-mode authentication: Basic + OIDC (Keycloak)
- JWT validation with JWKS caching and role claim extraction
- Five-tier permission model: Self Only, Basic, Operator Basic, Operator, Admin
- `pg_hba.conf` management via CRD
- SSL/TLS support with configurable minimum TLS version
- Webhook TLS certificate management (Vault PKI or self-signed with automatic rotation)
- **Cluster TLS auto-issuance from Vault PKI**: when a cluster CR enables both `vault.enabled` and `auth.ssl.enabled` with a `certSecret` that does not exist, the operator issues the server certificate from the webhook's Vault PKI mount/role, creates a generic Secret (`tls.crt`/`tls.key`/`ca.crt`), and renews it at 2/3 lifetime — user-provided Secrets are never touched (events: `ClusterTLSIssued`/`ClusterTLSRenewed`/`ClusterTLSFailed`)
- **Cluster pods roll automatically on TLS certificate rotation**: the cluster controller stamps the `avsoft.io/tls-cert-checksum` annotation (checksum of the TLS Secret data) on the coordinator/standby/segment pod templates, so a rotated certificate rolls the pods exactly once — stable across no-op reconciles, preserved by reconcile paths that don't recompute it. **Upgrade note**: the first reconcile after upgrading to a version with this annotation triggers a one-time rolling restart of existing TLS-enabled cluster pods
- HashiCorp Vault integration for secrets management with token, Kubernetes, and AppRole auth methods (`CLOUDBERRY_VAULT_ROLE_ID`/`CLOUDBERRY_VAULT_SECRET_ID`); Vault token renewal and re-authentication are automatic (Vault `LifetimeWatcher` renews the login token in the background and re-authenticates on expiry); re-authentication is **generation-gated** (a reactive re-login after a 401/403 burst suppresses the redundant lifecycle re-login — no re-auth storms), and when both operator Vault and `webhook.certSource=vault-pki` are enabled the webhook PKI **reuses the shared Vault client** (single token lifecycle)
- **Vault KV-v2 logical paths**: `ReadSecret` takes the logical KV path (e.g. `secret/cloudberry/backup-s3`) and injects the `data/` request segment automatically for KV-v2 mounts — including under least-privilege policies granting only `<mount>/data/*` (a 403 on the verbatim path triggers a single fallback read, with no re-auth for path-shape 403s); explicit `secret/data/...` paths keep working (the webhook warns and suggests the logical form)
- Test users (`basic_user`/`opbasic_user`/`operator_user`) are **disabled by default** and only seeded when `CLOUDBERRY_ENABLE_TEST_USERS=true` (a WARN log is emitted when enabled — never enable in production, the credentials are publicly known)
- Configuration precedence (highest wins): environment variable > command-line flag > config file > default

**Observability**
- Prometheus metrics for cluster health, reconciliation, FTS, connections, scale operations, mirroring operations, and PVC sizes
- Reconciliation metrics: `cloudberry_reconcile_total`, `cloudberry_reconcile_errors_total`, `cloudberry_reconcile_duration_seconds` with cluster/namespace/result labels
- Operational metrics wired to real operations: `cloudberry_pvc_size_bytes` (PVC expansion), `cloudberry_redistribution_progress` (data redistribution), `cloudberry_backup_total` / `cloudberry_restore_total` (**transition-gated** — one increment per actual Job state change, so `rate()` and alerts are correct), per-database `cloudberry_disk_usage_bytes`, and storage recommendation metrics (`cloudberry_recommendations_total`, `cloudberry_recommendation_scan_duration_seconds`)
- Maintenance metrics: `cloudberry_maintenance_operations_total` with cluster/namespace/operation/`result` (`started`, `success`, `failed`) labels
- Security metrics: `cloudberry_cert_rotation_total`, `cloudberry_cert_expiry_seconds`, `cloudberry_vault_operations_total`, `cloudberry_vault_operation_duration_seconds`, and `cloudberry_auth_attempts_total` (a missing/malformed `Authorization` header increments `{method="unknown",result="failure"}`)
- Admission and lifecycle metrics: `cloudberry_webhook_admission_total`, `cloudberry_upgrade_operations_total`, `cloudberry_rolling_restart_total`, `cloudberry_recovery_operations_total`
- REST API server metrics: `cloudberry_api_requests_total` / `cloudberry_api_request_duration_seconds` (labelled by low-cardinality route **template**, never the raw path), `cloudberry_api_requests_in_flight` (panic-safe — decremented even when a handler panics), and `cloudberry_api_rate_limit_rejections_total`
- Database client metrics: `cloudberry_db_connect_total` / `cloudberry_db_connect_duration_seconds`, `cloudberry_db_query_duration_seconds{operation}`, and live pool gauges sampled per scrape (`cloudberry_db_pool_acquired_conns` / `_idle_conns` / `_max_conns`). 15 mutating/DDL `db.Client` methods record `cloudberry_db_query_duration_seconds` with their method name as the `operation` label (`SetParameter`, `ReloadConfig`, `CreateRole`, `AlterRole`, `DropRole`, `Vacuum`, `Analyze`, `Reindex`, `CreateResourceGroup`, `AlterResourceGroup`, `DropResourceGroup`, `AssignRoleResourceGroup`, `CreateResourceQueue`, `DropResourceQueue`, `MoveQueryToResourceGroup`); **22 read-path methods now record it too** (`GetSegmentConfiguration`, `GetMirrorSyncStatus`, `GetReplicationLag`, `GetActiveQueryCount`, `GetMaxConnections`, `GetResourceGroupUsage`, `ListSessionsWithResourceGroup`, `ListSessions`, `GetDiskUsage`, `GetStorageDiskUsage`, `ListResourceGroups`, `ListResourceQueues`, `CancelQuery`, `TriggerFTSProbe`, `ShowParameter`, `GetBloatRecommendations`, `GetSkewRecommendations`, `GetAgeRecommendations`, `GetIndexBloatRecommendations`, `GetTableDetails`, `GetUsageReport`, `GetRedistributionProgress`), completing the read/write symmetry
- Idle daemon and session metrics: `cloudberry_idle_daemon_up`, `cloudberry_idle_scan_failures_total`, `cloudberry_idle_reconnect_attempts_total`, `cloudberry_session_terminations_total`
- Controller operation metrics: `cloudberry_storage_expansions_total`, `cloudberry_backup_on_delete_total`, `cloudberry_scale_phase_duration_seconds{direction,phase}`, `cloudberry_cluster_cert_issuance_total`
- API business metrics: `cloudberry_migrate_operations_total`, `cloudberry_api_cluster_operations_total`, `cloudberry_log_stream_sessions_total` / `cloudberry_log_stream_bytes_total`, `cloudberry_oidc_discovery_total`, `cloudberry_auth_token_verify_duration_seconds`
- Request-side API/DDL counters (the request-side view, complementary to the controller-side outcome metrics): `cloudberry_api_cluster_lifecycle_requests_total{operation,result}` (cluster lifecycle/maintenance actions via the REST API — `operation` ∈ {`start`, `stop`, `restart`, `reload`, `activate-standby`, `rebalance`, `vacuum`, `analyze`, `reindex`, `config-update`}; `result` ∈ {`accepted`, `error`}), `cloudberry_api_workload_operations_total{kind,operation,result}` (workload-management DDL — `kind` ∈ {`resource_group`, `resource_queue`, `rule`}; `operation` ∈ {`create`, `update`, `delete`, `assign`}; `result` ∈ {`success`, `error`}), and `cloudberry_pxf_sync_total{cluster,namespace,result}` (PXF sync **request** outcomes — `result` ∈ {`success`, `error`} — separate from the honest `cloudberry_pxf_servers_changed_total` force-pair counter). The recovery API endpoint also records the request-side outcome on `cloudberry_recovery_operations_total` with `result` ∈ {`requested`, `error`} (alongside the controller-side `started`/`completed`/`failed`/`noop`)
- Control-plane setup metrics (honest outcome counters incremented only at the real outcome): `cloudberry_gpfdist_reconcile_total{cluster,namespace,operation,result}` (operator-side gpfdist provisioning reconcile outcome — `operation` ∈ {`pvc`, `deployment`, `service`, `delete`}; `result` ∈ {`success`, `error`}), `cloudberry_pxf_extension_setup_total{cluster,namespace,result}` (PXF client-extension setup attempt outcome — `result` ∈ {`installed`, `absent`, `error`}), `cloudberry_dataloader_role_setup_total{cluster,namespace,result}` (the outcome of the dedicated least-privilege data-loader role setup — `EnsureDataLoaderRole`, security control SE.6; `result` ∈ {`success`, `error`}), and `cloudberry_exporter_role_setup_total{cluster,namespace,result}` (the outcome of the monitoring exporter role provisioning — `setupExporterRole`'s DB round-trip in `admin_controller.go`; `result` ∈ {`success`, `error`}) — the latter two are the siblings of `cloudberry_pxf_extension_setup_total`, so all three best-effort role-setup DB round-trips are now metered (each previously only logged a `Warn` on failure)
- Storage-scan and OIDC outcome counters: `cloudberry_disk_usage_scan_total{cluster,namespace,result}` (disk-usage scan outcome recorded in the admin controller's `recordDiskUsage` — `result` ∈ {`success`, `error`, `skipped`}; `skipped` when `gp_toolkit.gp_disk_free` is unavailable on the server version, never a fabricated value), `cloudberry_recommendation_scan_total{cluster,namespace,result}` (storage-recommendation scan outcome recorded in `recordRecommendations` — `result` ∈ {`success`, `error`, `skipped`}, `skipped` when the DB is unavailable), and `cloudberry_oidc_userinfo_total{result}` (OIDC userinfo fetch outcome in `auth/oidc.go` — `result` ∈ {`success`, `error`})
- Query-exporter (`cmd/cloudberry-query-exporter`) self-observability: `cbexporter_collector_errors_total{collector}` and `cbexporter_collector_duration_seconds{collector}` — per-collector scrape error counter + duration, `collector` ∈ {`query_activity`, `resgroup_status`, `resgroup_iostats`, `spill_files`, `segment_health`, `dist_txns`, `table_skew`}. The unbounded `usename` label was **removed** from `cbexporter_queries_total` / `cbexporter_queries_slow_total` to bound metric cardinality
- Honest metric semantics: `cloudberry_connections_max` reports the **real** `max_connections` queried from the database; `cloudberry_pvc_size_bytes` is published in steady state on every reconcile (not only on expansion); `cloudberry_scale_operations_total` distinguishes `rebalance` from `rebalance-failed`
- Workload and query-history metrics wired through: slow queries, workload rule actions, active connections, and query-history insert/retention/size
- Exporter sidecars: `postgres-exporter` (port 9187) runs on both the coordinator and standby coordinator pods for monitoring continuity on promotion; `cloudberry-query-exporter` is coordinator-only (its cluster-global queries would otherwise duplicate metric series on a non-promoted standby); a per-segment `postgres-exporter` is available opt-in (default off) for both primary and mirror segments via the independent `queryMonitoring.exporters.postgresExporter.segments` and `queryMonitoring.exporters.postgresExporter.mirrors` flags for deep per-segment diagnostics; the `postgres-exporter` is Cloudberry-tailored (conditional resource-group query, disabled incompatible built-in collectors, recovery-safe WAL query) so scrapes run cleanly (`pg_exporter_last_scrape_error=0`) on coordinator, standby, and segments
- OpenTelemetry (OTLP) distributed tracing with gRPC/HTTP exporters; spans use **low-cardinality names** across namespaced families: API server spans renamed to the route template (`GET /api/v1alpha1/clusters/{name}` — never the raw path), `db.*` (per db.Client operation — including the 15 mutating/DDL spans `db.SetParameter`, `db.ReloadConfig`, `db.CreateRole`, `db.AlterRole`, `db.DropRole`, `db.Vacuum`, `db.Analyze`, `db.Reindex`, `db.CreateResourceGroup`, `db.AlterResourceGroup`, `db.DropResourceGroup`, `db.AssignRoleResourceGroup`, `db.CreateResourceQueue`, `db.DropResourceQueue`, `db.MoveQueryToResourceGroup`, plus the 22 read-path spans `db.GetSegmentConfiguration`, `db.GetMirrorSyncStatus`, `db.GetReplicationLag`, `db.GetActiveQueryCount`, `db.GetMaxConnections`, `db.GetResourceGroupUsage`, `db.ListSessionsWithResourceGroup`, `db.ListSessions`, `db.GetDiskUsage`, `db.GetStorageDiskUsage`, `db.ListResourceGroups`, `db.ListResourceQueues`, `db.CancelQuery`, `db.TriggerFTSProbe`, `db.ShowParameter`, `db.GetBloatRecommendations`, `db.GetSkewRecommendations`, `db.GetAgeRecommendations`, `db.GetIndexBloatRecommendations`, `db.GetTableDetails`, `db.GetUsageReport`, `db.GetRedistributionProgress`, plus the session/replication/maintenance/history spans `db.TerminateSession`, `db.ConfigureReplication`, `db.TerminateAllBackends`, `db.CancelAllQueries`, `db.LogRotate`, `db.TriggerRecommendationScan`, `db.GetQueryDetail`, `db.ListUserDatabases`, `db.EnsureQueryHistoryTable`, `db.InsertQueryHistory`, `db.GetQueryHistoryDetail` (each also recording `cloudberry_db_query_duration_seconds`) — completing the read/write symmetry), `controller.*` (per controller sub-operation, e.g. `controller.clusterTLS`, the cluster-controller sub-operation spans `controller.reconcileCoreResources` and `controller.reconcileStatefulSets`, plus the AdminReconciler sub-reconciler spans `controller.reconcilePxf`, `controller.reconcileDataLoading`, `controller.reconcileStorage`, `controller.reconcileResourceGroups`, `controller.ensureExporterCoreResources`, the cluster-controller sub-reconciler spans `controller.reconcileConfigMaps`, `controller.reconcileAdminSecret`, `controller.reconcileClusterSSHSecret`, `controller.reconcileServices`, `controller.reconcileCoordinator`, `controller.reconcileStandby`, `controller.reconcileSegments`, the HA/auth controller spans `controller.monitorStandby`, `controller.executeRebalanceViaDB`, `controller.handleStandbyActivation`, `controller.reconcileHBA`, and `controller.scanRecommendations.fetch` (with a bounded `rec_type` attribute)), `idle.*` (`idle.scan`), `auth.*` (`auth.authenticate`, `auth.oidc.verify`, `auth.oidc.userinfo`, `auth.basic.verify`), `webhook.*` (`webhook.validate`/`webhook.mutate`), `vault.watch.check` (the `SecretWatcher.checkForChanges` read, span-only — error status on a read failure), `certmanager.issueVaultPKICert` (the Vault PKI cert-issuance call, error status on failure), and `operator.*` startup spans (`operator.setupWebhookCerts`, `operator.injectCABundle`)
- Span error recording via `SetSpanError()` — sets error status and exception events on OTEL spans
- Structured logging (slog) with JSON output including cluster, namespace, controller, and reconcileID fields
- Structured error types with sentinel errors (`ErrNotFound`, `ErrInvalidInput`, `ErrRetryExhausted`) supporting `errors.Is()` classification
- Retry with exponential backoff for transient failures (configurable max retries, backoff, jitter)
- Webhook validation rejects invalid cluster specs at admission time (segments, OIDC, storage)
- Automatic pod deletion detection and recovery with degraded state reporting
- The REST API server is an **essential component**: an API startup/runtime failure shuts the operator down (non-zero exit) instead of leaving it running degraded without its API
- OIDC lazy discovery is **singleflight** with a 10-second per-attempt timeout (concurrent Bearer requests share one bounded discovery instead of piling up); per-request OIDC identity details (username/email/roles) are logged at Debug, not Info (PII hygiene)

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
  - `backup.image` is defaulted by the mutating webhook to the official backup image (`cloudberry-backup:2.1.0`) when unset; a backup-capable image **must** contain `kubectl` (coordinator-exec model) and the `gpbackup`/`gprestore` toolchain
  - Restores default to `--with-stats=false` (statistics restore is opt-in); with `withStats: true`, a `gprestore` exit code 2 (data restored, statistics failed) is treated as success-with-warning — `RestorePartial` Warning event and `cloudberry_restore_total{result="partial"}`
  - S3 credentials from a Kubernetes Secret or HashiCorp Vault: `vaultSecret.path` takes the **logical** KV path (e.g. `secret/cloudberry/backup-s3` — the operator injects `data/` for KV-v2 mounts, including under least-privilege `secret/data/*` policies; the explicit `secret/data/...` form still works with a webhook warning), materialized at reconcile time into the owner-referenced Secret `<cluster>-backup-s3-vault-creds` consumed via `secretKeyRef`; materialization failures emit a `Warning`/`BackupVaultCredentialsFailed` Event and mark the reconcile outcome `result="error"`
  - Full S3 config: bucket, folder, region, encryption, `forcePathStyle`, multipart tuning, retention, schedule (CronJob)
  - Live data cycle runs `gpbackup`/`gprestore` inside the coordinator pod (MPP coordinator→segment SSH dispatch); verified end-to-end by Scenario 71 for both Secret and Vault credential variants
  - Backup infrastructure verified by Scenario 72: toolchain image (`gpbackup`/`gprestore`/`gpbackup_s3_plugin`), backup RBAC (`cloudberry-backup-sa` + `cloudberry-backup-role`), the `<cluster>-backup-s3-config` ConfigMap, and the `jobTemplate` pod-template overrides (resources, nodeSelector, tolerations, serviceAccountName, backoffLimit, activeDeadlineSeconds, ttlSecondsAfterFinished)
  - On-demand backup with per-request `gpbackupOptions` verified by Scenario 73: `POST /clusters/{name}/backups` accepts compression level/type, jobs, with-stats, without-globals, include-schemas, and a `noCompression` override (emits `--no-compression` and ignores `--compression-level`); the on-demand request creates a Kubernetes Job directly (not via the CronJob). A non-empty `databases` array is **required** (`400 INVALID_REQUEST` otherwise — `gpbackup` hard-requires `--dbname`); scheduled CronJob backups, which carry no request body, default the target database to `postgres`
  - Single-data-file backup + full-option restore verified by Scenario 74: a `singleDataFile`/`copyQueueSize` backup emits `--single-data-file --copy-queue-size 4` (no `--jobs`), requires `gpbackup_helper` on every segment, and writes one consolidated data file per segment; the full-option restore resolves `gprestore`'s mutual-exclusivity rules (include-table over include-schema, run-analyze over with-stats, `--copy-queue-size` instead of `--jobs` for single-data-file restores, and redirect-schema pre-creation) so `mydb_restored` is populated, objects land in the `restored` schema, and ANALYZE runs
  - Compression matrix (gzip vs zstd) verified by Scenario 75: two on-demand backups of the same data at `--compression-level 6` — one `--compression-type gzip`, one `--compression-type zstd` — both succeed and both restore cleanly into separate redirect DBs, with the on-disk sizes differing (zstd smaller). The `zstd` CLI now ships in the cluster image (`cloudberry-official:2.1.0`), required because `gpbackup` pipes segment `COPY` output through `zstd --compress`; data files are named `…_<oid>.gz` (gzip) vs `…_<oid>.zst` (zstd)
  - Scheduled backup via CronJob + status population verified by Scenario 76: setting `spec.backup.schedule` reconciles a CronJob `{cluster}-backup-schedule` (`ownerReferences` → the cluster, `concurrencyPolicy: Forbid`, `successfulJobsHistoryLimit`/`failedJobsHistoryLimit` = 3, pod `restartPolicy: Never`) that fires on schedule and spawns a Job; after the backup succeeds the operator populates `status.backup` (`lastBackupTime`, `lastBackupTimestamp` 14-digit, `lastBackupStatus`, `lastBackupType`, `lastBackupJobName`, `cronJobName`, and `backupHistory[]` with `size`+`duration`). The 14-digit `lastBackupTimestamp` is guaranteed (CronJob Jobs derive it from `CompletionTime` in UTC), and backup status is refreshed on the periodic reconcile even in steady state (unchanged spec generation)
  - **Real gpbackup timestamp capture (restore-by-timestamp fix):** the operator now captures `gpbackup`'s **real** emitted `Backup Timestamp = <14-digit>` line from the backup Job (surfaced via `/dev/termination-log` and the new `avsoft.io/backup-timestamp` annotation) and records **that** as `status.lastBackupTimestamp`, instead of a pre-generated `time.Now()` value. Because `gpbackup` runs asynchronously inside the coordinator and assigns its own timestamp, the pre-generated value could **drift** from `gpbackup`'s real S3 object prefix, so a restore-by-timestamp directly against S3 previously failed with a `404`/`NotFound`; with the real timestamp recorded, restore-by-timestamp now resolves the correct S3 prefix. The change is **backward compatible** — when the annotation/marker is absent the operator falls back to the prior behaviour
  - Pre-backup health checks verified by Scenario 77: every backup Job's `pre-backup-check` init container blocks the backup when any of four checks fails — **77a** segments-up (`gp_segment_configuration` `status='d'`), **77b** long-running transaction (older than 1 hour), **77c** S3 reachability (a **fail-closed SigV4-signed HEAD** to `${S3_ENDPOINT}/${S3_BUCKET}` returns non-2xx/3xx — replacing the prior best-effort `aws s3 ls`), and **77d** local disk space (< 1 GiB free). On a fault the `gpbackup` container never starts, the operator sets `lastBackupStatus=Failed` and emits a de-duplicated `Warning`/`BackupFailed` Event (one per failed Job; restore failures excluded); healing the fault lets a fresh backup reach `Success`
  - Incremental backup lifecycle verified by Scenario 78: **78a** `gpbackup.incremental: true` (or a per-request `type=incremental`) always renders `--incremental --leaf-partition-data` once each on the Job *and* CronJob (leaf-partition-data forced even when unset; de-duplicated when set); **78b** a full → modify AO table → incremental WITHOUT `--from-timestamp` auto-forms against the most recent compatible backup (same bucket+folder) and `status.lastBackupType` becomes `incremental`; **78c** an explicit `--from-timestamp <full-ts>` (`gpbackupOptions.fromTimestamp`) pins the base. Each backup Job/CronJob is labelled `avsoft.io/backup-type` (`full|incremental`) and the operator derives `status.lastBackupType`/`backupHistory[].type` from the **Job's** label (spec fallback). **78d** restoring from the latest incremental validates the full set (full + all incrementals) and restores; a missing intermediate makes `gprestore` refuse (restore Job fails) — the operator records `lastBackupStatus=Failed` and emits a de-duplicated `Warning`/`RestoreFailed` Event (distinct from `BackupFailed`)
  - Retention cleanup (all policies) verified by Scenario 79: after **each successful backup** the operator creates one idempotent cleanup Job `<cluster>-cleanup-<ts>` (label `avsoft.io/backup-operation=cleanup`) that enforces all three policies via the **real** `gpbackman` CLI (`woblerr/gpbackman` — no `delete --keep-full`/`--older-than`; it ships v0.8.1 in `cloudberry-backup:2.1.0`). **79a** `fullCount` and **79b** `incrementalCount` enumerate `gpbackman backup-info --type full|incremental` and delete the oldest excess with `gpbackman backup-delete --timestamp <ts> --cascade` (re-enumerating after each delete so cascade neither over- nor under-counts); **79c** `maxAge` (`"30d"`) runs `gpbackman backup-clean --older-than-days 30 --cascade`. **79d** the cleanup script prints `RETENTION_DELETED=<n>` (also to `/dev/termination-log`); the operator patches `avsoft.io/backup-retention-deleted=<n>` onto the Succeeded cleanup Job and the metrics loop turns it into the counter `cloudberry_backup_retention_deleted_total` (incremented per deletion)
  - Post-restore validation verified by Scenario 80: after **each Succeeded restore** the operator creates one idempotent validation Job `<cluster>-validate-<ts>` (label `avsoft.io/backup-operation=validate`) whose script runs four checks — **80a** row-count compare of ACTUAL restored per-table counts against the EXPECTED counts captured from gpbackup history (passed via the restore Job's `avsoft.io/expected-row-counts` JSON annotation), emitting `ROW_COUNT_MATCH`/`ROW_COUNT_MISMATCH` and failing (`exit 1`) on any discrepancy (empty map → best-effort `ROW_COUNT_PROBE_SKIPPED`, no fail); **80b** an optional `ANALYZE` (`ANALYZE_OK`) when `backup.validation.runAnalyze` (falls back to `gprestore.runAnalyze`) refreshes planner stats; **80c** a must-pass invalid-index scan (`relkind='i' AND NOT indisvalid` → `exit 1`); and **80d** a configurable health-check query (`backup.validation.healthCheckQuery`, default `SELECT 1`). The operator records the terminal status into `cloudberry_restore_validation_total{cluster,namespace,result}` (`result=success|failed`) and emits a de-duplicated `Warning`/`ValidationFailed` Event on a Failed Job — **80e** a deliberate mismatch (data-only restore into a pre-populated table) is FLAGGED (Failed + Warning + `{result="failed"}`), and a failed validation does **not** retroactively fail the Succeeded restore
  - Local (PVC-backed) backup destination verified by Scenario 81: with `destination.type: local` the operator mounts the named PVC (`local.persistentVolumeClaim`) as the `backup-data` volume at `local.path` (default `/backups`) on the backup/restore Job and runs `gpbackup`/`gprestore` with `--backup-dir <path>` and **NO** S3 plugin — `backupDestinationArgs(cluster)` seeds `--backup-dir` for local (and `--plugin-config` for s3); `buildGpbackupArgs`/`buildGprestoreArgs`/`renderToolScript` are destination-aware so a local Job has **no** `--plugin-config`, **no** `s3-plugin-config` volume, **no** `/etc/gpbackup` render (the S3-config envsubst is skipped so the local Job does not crash under `set -euo pipefail`), and **no** `S3_*`/`AWS_*` env. The operator creates **no** S3 ConfigMap and **no** Vault/Secret S3 credentials for local; the pre-backup `df` disk-space check (Scenario 77d, < 1 GiB free blocks) and gpbackman retention (`--backup-dir <path>` instead of `--plugin-config`) are local-aware. **MPP note:** `gpbackup --backup-dir` writes per-segment backup sets on the coordinator and every segment host; an RWO PVC on one Job pod holds one set, so the live data cycle targets a segment-visible `--backup-dir` via coordinator-exec while the PVC mount proves the operator wiring
  - Backup security and encryption verified by Scenario 82: **82a** the `<cluster>-backup-s3-config` ConfigMap (`BuildBackupS3ConfigMap`) holds **only** `${...}` placeholders — no real credentials on disk/ConfigMap; creds reach the pod **only** as `valueFrom.secretKeyRef` env (`buildS3CredentialEnv`); **82b** `renderToolScript` runs `envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml` at runtime so the resolved config (with creds) exists **only** in the ephemeral pod filesystem (the ConfigMap stays placeholders-only); **82c** backup/restore/validate/cleanup Jobs run as `cloudberry-backup-sa` with **minimal RBAC** — Helm values `backup.rbac.scopeSecrets=true` + `backup.rbac.secretNames=[…]` scope `secrets:[get]` to a `resourceNames` allow-list so the SA is **denied** `get` on unrelated Secrets while still reading the backup-relevant ones (the S3 credential Secret + `<cluster>-ssh-keys`/`<cluster>-admin-password`/`<cluster>-backup-s3-vault-creds`); default `scopeSecrets=false` keeps namespace-wide get for backward compatibility (production should opt in, unioning per-cluster Secret names); **82d** `S3Destination.encryption` is an enum (`on|off`, default `on`, CRD-enforced) — `buildS3Env` sets `S3_ENCRYPTION` and the template line `encryption: ${S3_ENCRYPTION}` flips so the rendered config carries `encryption: on`/`off` (the S3 plugin TLS/SSL option; verified via the option, not literal HTTPS, in the HTTP-only test env); **82e** new `spec.backup.jobTemplate.imagePullSecrets` are applied to the backup Job pod spec (and restore/validate/cleanup/CronJob pods) via `applyJobTemplatePod` → `addImagePullSecrets`, so Jobs can pull from a private registry. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario82-security-encryption.yaml`; live cycle `test/e2e/scripts/scenario82-security-encryption.sh`
  - Backup failure handling verified by Scenario 83: backup Jobs are bounded by `spec.backup.jobTemplate.backoffLimit` (default `2`) and `activeDeadlineSeconds` (default `7200`), both seeded by `buildJobSpec` and overridable per cluster (the override reaches every backup/restore/cleanup/validation Job and the CronJob `jobTemplate`). **83a** a force-failure (unreachable/bad-creds S3, caught fast by the 77c pre-backup HEAD) retries up to `backoffLimit` (up to 3 pod attempts) and ends with the Job condition `reason=BackoffLimitExceeded`; **83b** a backup outliving a low `activeDeadlineSeconds` is killed by Kubernetes at the deadline (`reason=DeadlineExceeded`). The operator now classifies a Job as `Failed` when `Status.Failed > 0` **OR** it carries a terminal Failed condition (`jobHasFailedCondition` — `batchv1.JobFailed`/`ConditionTrue`), applied to `backupJobStatus`/`backupJobStatusCode`/`validationJobResult` after the Succeeded precedence — so a deadline-killed Job (failed-pod count may be `0`) is reliably recorded `Failed`. A Failed backup sets `status.backup.lastBackupStatus=Failed`, `cloudberry_backup_last_status=1`, and emits the de-duplicated `Warning`/`BackupFailed` Event (Scenario 77); a success sets the metric to `0`. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario83-backup-failure.yaml`; live cycle `test/e2e/scripts/scenario83-backup-failure.sh`
  - Prometheus backup/restore metrics verified by Scenario 84: the `gpbackup_exporter` is the **operator `/metrics` endpoint** — the operator derives nine metrics (namespace `cloudberry`) from the observed backup/restore/cleanup Jobs + their `avsoft.io/*` annotations and exposes them on `/metrics` (vmagent scrapes them into VictoriaMetrics via the `prometheus.io/scrape` annotations; the Grafana operator dashboard renders a panel for each). The nine: `backup_total{type,result}`, `backup_duration_seconds{type}`, `backup_size_bytes{timestamp}`, `backup_last_success_timestamp`, `backup_last_status` (`0=success, 1=failed, 2=in-progress`), `restore_total{result}`, `restore_duration_seconds`, `backup_retention_deleted_total`, and `backup_job_status{job,operation}` (`0=pending, 1=running, 2=succeeded, 3=failed`). The outcome label on `backup_total`/`restore_total` is **`result`** (`success`|`failed`), **not** `status` — query `{result="success"}`/`{result="failed"}` (not renamed in code; dashboards/PromQL across 76–83 use `result`); both counters are **transition-gated** (one increment per actual Job state change — no inflation from periodic no-op reconciles). All nine were already wired across Scenarios 76–83 (`internal/metrics/metrics.go` `initBackupMetrics`; recorded by `recordLatestBackupMetrics`/`recordBackupJobMetrics`/`applyBackupJobToStatus` in `internal/controller/admin_controller.go`), so Scenario 84 is a verification/doc/dashboard scenario with no operator code change. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario84-metrics.yaml`; live cycle `test/e2e/scripts/scenario84-metrics.sh`
  - All backup API endpoints verified by Scenario 85: the **seven** OIDC/JWT-authed backup/restore REST endpoints under `/api/v1alpha1/clusters/{name}/backups`, each with its own RBAC — **85a** `GET /backups` (Basic) lists the operator's recorded backup history (`status.backup.backupHistory`, derived from observed Jobs — **not** a live `gpbackman` query); **85b** `POST /backups` (Operator) creates a backup Job whose `gpbackup` args match `CreateBackupRequest.gpbackupOptions` (`mergeGpbackupOptions` → `buildGpbackupArgs`), **including the fix that `leafPartitionData` now emits `--leaf-partition-data` on FULL backups too, exactly once** (`appendLeafPartitionDataArgs` guarded on `!isEffectivelyIncremental` so the incremental force-pair is never duplicated); **85c** `GET /backups/{timestamp}` (Basic) returns the matching history entry (`400` non-14-digit, `404 BACKUP_NOT_FOUND` unknown); **85d** `DELETE /backups/{timestamp}` (Admin) creates a `gpbackman` cleanup Job; **85e** `POST /backups/{timestamp}/restore` (Admin) creates a restore Job whose `gprestore` args match `RestoreRequest.gprestoreOptions` (`dataOnly→--data-only`, `metadataOnly→--metadata-only`, `resizeCluster→--resize-cluster`, …) with `dataOnly`+`metadataOnly` rejected `400`, and include-schema/include-table + run-analyze/with-stats resolved to the more specific flag; **85f** `GET /backups/jobs` (Basic) lists backup/restore/cleanup Job statuses; **85g** `GET /backups/schedule` (Basic) returns the CronJob status + computed `nextScheduleTime`. Handlers in `internal/api/server.go` (`handle*Backup*`), DTOs/mapping in `internal/api/backup.go` (`buildBackupJobOptions`/`buildRestoreJobOptions`/`restoreOptionsConflict`). Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario85-api-endpoints.yaml`; live cycle `test/e2e/scripts/scenario85-api-endpoints.sh`
  - All backup CLI commands verified by Scenario 86: the **eleven** `cloudberry-ctl backup …` commands map to the operator backup REST API over an OIDC bearer token (`--operator-url`/`CLOUDBERRY_OPERATOR_URL`, `--auth-method oidc` + token via `--password`/`CLOUDBERRY_PASSWORD`) — **86a** `backup create` (all `gpbackupOptions` flags: `--type`, `--database`, `--compression-level`/`--compression-type`, `--jobs`, `--single-data-file`/`--copy-queue-size`, `--include-schema`/`--exclude-table`, `--incremental`/`--from-timestamp`/`--leaf-partition-data`, `--with-stats`, `--without-globals`) → `POST /backups`; **86b** `backup list` → `GET /backups`; **86c** `backup status --timestamp` → `GET /backups/{ts}`; **86d** `backup delete --timestamp` → `DELETE /backups/{ts}`; **86e** `backup restore --timestamp` (all `gprestoreOptions` flags incl. **`--resize-cluster`** — restores into a cluster with a different segment count) → `POST /backups/{ts}/restore`; **86f** `backup schedule` → `GET /backups/schedule`; **86g/h/i** `backup schedule set --cron`/`suspend`/`resume` → `PATCH /backups/schedule`; **86j** `backup jobs` → `GET /backups/jobs`; **86k** `backup jobs logs --job <name>` now **STREAMS** the Job's pod logs (`--follow`/`--tail`) via a **new** operator endpoint `GET /clusters/{name}/backups/jobs/{job}/logs` (Permission Basic, `text/plain`, `?follow`/`?tailLines`) that finds the Job's pod and streams its container logs — with a `kubectl logs` fallback when the endpoint is unavailable. The operator gained a typed Kubernetes clientset (injected via `Server.WithClientset`) to read pod logs; the CLI streams via `OperatorClient.GetStream`. Commands in `cmd/cloudberry-ctl/main.go` (`newBackup*`), endpoint `handleBackupJobLogs` in `internal/api/server.go`. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario86-cli-commands.yaml`; live cycle `test/e2e/scripts/scenario86-cli-commands.sh`
- Session management: list active sessions from `pg_stat_activity`, cancel queries via `pg_cancel_backend()`, terminate sessions via `pg_terminate_backend()` (with PID validation and graceful degradation when DB is unavailable)
- Resource group management: create, list, assign, and delete resource groups for workload isolation
  - Create groups with concurrency, CPU, and memory limits
  - Assign database roles to resource groups (`ALTER ROLE ... RESOURCE GROUP`)
  - Query live resource groups from the database with CRD spec fallback
- API admin password via `CLOUDBERRY_API_ADMIN_PASSWORD` env var or auto-generated (persisted to K8s Secret `cloudberry-operator-admin-password`)
- Data loading (PXF model) — **declarative contract + ingestion runtime implemented; native loads AND operator-driven `pxf://` loads row-count-verified**:
  - The full `dataLoading` CRD (PXF servers/extensions/custom connectors, `gpfdist`, `jobs[]` of `type: pxf|gpload`, `jobTemplate`) is accepted and persisted
  - Validating webhook enforces the `W.1`–`W.25` PXF + gpload rules (incl. the W.10 profile allowlist) **plus the W.10b write-capability rule** and applies 14 mutating-webhook defaults — gated on `dataLoading.enabled: true`, verified by Scenario 89 (W.1–W.16), Scenario 96 (W.10b), Scenario 99 (W.17), Scenario 101 (W.18–W.22 gpload field rules), Scenario 102 (W.23/W.24/W.23c custom-connector + streaming rules), and Scenario 103 (W.25 `loadMethod` + the W.17 fdw-read tweak)
  - **Ingestion runtime (Implemented):** for every enabled `dataLoading.jobs[]` entry the operator **creates and launches** a one-off `Job` (no `schedule`) or a `CronJob` (when `schedule` set), named `<cluster>-dataload-<job>` (container `dataload`, image `cluster.Spec.Image`, coordinator-exec `psql`). It generates the **external-table DDL** (`CREATE EXTERNAL TABLE (LIKE <target>) … LOCATION … FORMAT … LOG ERRORS`), runs `INSERT…SELECT` → harvests the `DATALOAD_ROWS` marker → `DROP` → `ANALYZE`, and best-effort-installs the PXF extensions (`CREATE EXTENSION pxf/pxf_fdw`, non-fatal). Verified by Scenario 92
  - **Per-type PXF server file-mapping + extensions + GRANTs (Implemented):** each PXF server type renders the correct `*-site.xml` set (**SL.1–6**) into the `<cluster>-pxf-servers` ConfigMap — `s3`→`s3-site.xml`; `hdfs`→`core-site.xml`+`hdfs-site.xml` always (+ optional `hive`/`hbase`/`mapred`/`yarn` site files, `config` map prefix-split `fs.*`→core/`dfs.*`→hdfs/…); `jdbc`→`jdbc-site.xml`; `hive`→`core`+`hive`; `hbase`→`core`+`hbase` — with `${PLACEHOLDER}` markers, never literal secrets. `SetupPXFExtensions` runs `CREATE EXTENSION IF NOT EXISTS pxf`/`pxf_fdw` (best-effort) and — only when `pxf` installed — `GRANT SELECT`/`INSERT ON PROTOCOL pxf TO "gpadmin"` (best-effort). Config sync across sidecars is structural (the shared `<cluster>-pxf-servers` ConfigMap — no explicit `pxf sync`). Verified by Scenario 93
  - **Live credential init-container (Implemented):** the `pxf-cred-init` init container `envsubst`-resolves the `${PLACEHOLDER}` site-XML templates against `credentialSecrets[]` into a **one-directory-per-server** `<server>/<file>.xml` layout in the shared emptyDir — **secrets never land in the ConfigMap**
  - **Rich status + 5 metrics (Implemented):** `status.dataLoading.jobs[].{lastRun,lastStatus,rowsLoaded,duration}` from real terminal Job status + the harvested marker, plus the emitted `cloudberry_data_loading_{job_status,job_last_success_timestamp,job_duration_seconds,rows_total,errors_total}` metrics (never synthesized)
  - **PXF sidecar + servers ConfigMap (Implemented):** enabling `dataLoading.pxf` (gated on `dataLoading.enabled && pxf.enabled && pxf.image != ""`) deploys a **PXF sidecar on segment-primary pods** (coordinator/standby/mirror untouched) and applies the `<cluster>-pxf-servers` ConfigMap; `pxf.logLevel`→`PXF_LOG_LEVEL`, `cloudberry_pxf_servers_configured` and `status.dataLoading.pxf.{configured,servers}` are populated. Verified by Scenario 91 (config) and Scenario 94 (sidecar deployment shape: container name `pxf`, env, port `5888`, liveness/readiness `HTTPGet /actuator/health:5888`, no `Command`/`Args` — the prepare/start/tail lifecycle is owned by the image entrypoint `hack/docker-entrypoint-pxf.sh`, and the probe path is the real PXF 2.1.0 Spring Boot actuator endpoint, **not** the legacy `/pxf/v15/Status` which 404s). The sidecar now also defines a **StartupProbe** (`HTTPGet /actuator/health:5888`, `periodSeconds=5`, `failureThreshold=24` → a ~120 s startup budget) plus a more tolerant liveness `timeoutSeconds`, so the slow ~50 s Spring Boot cold start no longer trips liveness into `CrashLoopBackOff`
  - **Native loads are real (row-count-verified):** Cloudberry's engine-native `gpfdist://`/`s3://` external-table protocols **load real data end-to-end** through the same Job machinery (e.g. **183,961 rows** from a staged CSV) — no PXF required (`file://` is admission-rejected for multi-segment gpload jobs by W.16)
  - **✅ `pxf://` execution is Implemented (row-count-verified):** the operator **generates, launches, and runs** the `pxf://` load Job end-to-end — an operator-driven `pxf://` load from MinIO S3 (via the PXF sidecar) loaded **183,961 rows** with credentials rendered automatically by the operator. It requires the **`cloudberry-pxf` sidecar image** (`Dockerfile.cloudberry-pxf`, from `apache/cloudberry-pxf`, `make docker-build-pxf`) + the **`pxf`/`pxf_fdw` extensions** in the DB image (`cloudberry-official-pxf`, `Dockerfile.cloudberry-official-pxf`, `make docker-build-official-pxf`); on a stock `cloudberry-official:2.1.0` only generation/launch holds. PXF Job generation/launch = Implemented; live `pxf://` execution = Implemented
  - **PXF CLI lifecycle (Implemented):** `cloudberry-ctl pxf status|restart|sync --cluster <name>` (and the matching `…/data-loading/pxf/{status,restart,sync}` REST routes) drive the operator-side PXF lifecycle — `pxf status` aggregates **honest** sidecar readiness from the segment-primary pods' real `pxf` container statuses (no synthetic health/exec/HTTP); `pxf restart` propagates by **patching the `<cluster>-segment-primary` StatefulSet restart-trigger annotation** so all segment pods **roll** and every sidecar restarts (a **pod ROLL — heavier** than an in-place sidecar restart), emitting `cloudberry_pxf_restart_total{cluster,namespace,result}`; `pxf sync` refreshes the `<cluster>-pxf-servers` ConfigMap and rolls the sidecars (the explicit, on-demand counterpart to the always-on structural sync). Only `status`/`restart`/`sync` are ctl commands — `pxf prepare`/`start`/`stop` are sidecar-local verbs run via `kubectl exec`. Verified by **Scenario 95**
  - **Object-store profiles & write-capability (Implemented — Scenario 96):** the object-store server **types** `gs`/`abfss`/`wasbs` (+ **Dell-ECS** = `s3` with a custom `fs.s3a.endpoint`, **MinIO** = `s3` with `fs.s3a.path.style.access=true`) join `s3`/`hdfs`/`jdbc`/`hbase`/`hive` (CRD enum `s3;hdfs;jdbc;hbase;hive;gs;abfss;wasbs`); all object-store types render into a single `<server>__s3-site.xml` (the profile scheme picks the connector at query time). The per-format **write-capability matrix** (`text`/`parquet`/`avro` writable; `json`/`orc`/`rc` read-only) is the single source of truth in `internal/pxfpolicy`, **enforced** by the webhook (W.10b — `mode: writable` + a read-only format is rejected) **and** re-checked by the builder, which emits `CREATE WRITABLE EXTERNAL TABLE … FORMATTER='pxfwritable_export'` for writable formats. Verified by **Scenario 96** (OS.1–OS.10 reads, CFG.1–CFG.8 object-store config-only, FF.1–FF.5 write matrix; cloud-only stores and ORC are config-only)
  - **Hadoop profiles & scheme-aware write-capability (Implemented/verified — Scenario 97):** the **HDFS** (`hdfs:text`/`parquet`/`avro`/`json`/`orc`/`SequenceFile`), **Hive** (`hive` auto-detect / `hive:text`/`hive:orc`/`hive:rc`), and **HBase** read profiles are validated, and for an `hdfs` server the operator renders `core-site.xml`+`hdfs-site.xml` (always) plus `<server>__hive-site.xml` (`hive.metastore.uris`) and `<server>__hbase-site.xml` (`hbase.zookeeper.quorum`). The write-capability predicate `pxfpolicy.IsProfileWritable` is now **scheme-aware**: `hdfs:text`/`parquet`/`avro`/`SequenceFile` are writable while `hdfs:json`/`hdfs:orc` are read-only, and **every `hive*` profile + `HBase` is read-only at the SCHEME level regardless of format** (`readOnlySchemes={hive,hbase}`) — so a `mode: writable` `hive:text` job is **rejected** even though `text` is a writable format (a policy fix: `IsProfileWritable` was previously a pure FORMAT predicate that wrongly admitted `hive:text` writable). Verified by **Scenario 97** (HP.1–6 HDFS reads, HV.1–4 Hive reads, HB.1 HBase read, SITE.1–4 site-file rendering, FF.6/FF.7 write edge, WRej.1–7 writable DENY matrix; parquet/avro/orc/SequenceFile/Hive-CTAS samples are config-only where not synthesizable)
  - **Filter pushdown, column projection & per-row error handling (Implemented/verified — Scenario 98):** `pxfJob.filterPushdown: true` → `FILTER_PUSHDOWN=true` and `pxfJob.columnProjection: true` → `PROJECT=true` in the `pxf://` LOCATION (both **default to `true`** in the mutating webhook; an explicit `false` is preserved), and `pxfJob.errorHandling.{segmentRejectLimit,segmentRejectLimitType (rows|percent, W.15-validated),logErrors}` → `[LOG ERRORS ]SEGMENT REJECT LIMIT <n> [ROWS|PERCENT]` on the READ external table (the writable export path correctly OMITS it). The operator emits the correct DDL option; the live PXF/engine performs the prune/tolerance. **Observability is honest:** `cloudberry_pxf_bytes_transferred_total` **stays Planned** (PXF 2.1.0's Spring Boot Actuator exposes no honest external-byte counter — fabricating one would break the metrics-honesty rule), so filter pushdown is **proven via real signals** — row-count reduction (`cloudberry_data_loading_rows_total` lower for a filtered job vs an unfiltered baseline), `EXPLAIN` (pushed filter / projected columns), and source-side query logs (JDBC/Hive `WHERE` predicate); per-row error handling is proven via the real `cloudberry_data_loading_job_status` (2=success / 3=failed) + `cloudberry_data_loading_errors_total` + `rows_total` (valid rows only). Verified by **Scenario 98** (FE.1–3 filter pushdown across object-store/JDBC/Hive, FE.4–5 wide parquet/ORC projection, FE.12a/b malformed-row tolerate/fail; ORC/Hive legs config-only where not synthesizable)
  - **Writable external tables / data export to S3, HDFS & JDBC (Implemented/verified — Scenario 99):** a `mode: writable` PXF job **exports** Cloudberry rows OUT to an external store via `CREATE WRITABLE EXTERNAL TABLE … FORMATTER='pxfwritable_export'` and a reversed `INSERT INTO <writable_ext> SELECT * FROM <targetTable>`. The DDL/export-script path is **profile-agnostic** (`internal/builder/dataload_builder.go`), so the SAME code exports to **S3 / object store** (FE.9/WE.1), **HDFS** (FE.10) and **JDBC** (FE.11 — rows land in the RDBMS, the strongest proof); `pxfpolicy.IsProfileWritable` admits `s3:`/`hdfs:` text/parquet/avro (+`hdfs:sequencefile`) and bare `jdbc`. The new **optional `pxfJob.sourceFilter`** WHERE predicate (valid only on `mode: writable`) exports a **filtered subset** — `… SELECT * FROM <target> WHERE region='us-east'` (emitted via a quoted heredoc so single quotes are safe; unset → byte-identical full export) — guarded by the new webhook rule **W.17** (a `sourceFilter` on a non-writable job, or one containing `;`/`--`/`/*`, is **rejected at admission**; the predicate is admin-authored trusted SQL, same boundary as `targetTable`). **Observability reuses the existing metrics (no new metric):** export is observed via `cloudberry_data_loading_rows_total` (the exported rowcount — the filtered export reports fewer rows) + `cloudberry_data_loading_job_status` (2=success / 3=failed); `cloudberry_pxf_bytes_transferred_total` and a first-class data-export Job kind stay **Planned** (`hdfs:parquet`/`avro` export may need `DATA_SCHEMA` — config-only). Verified by **Scenario 99** (FE.9/WE.1 S3, FE.10 HDFS, FE.11 JDBC, WE.2 correct-format gate, SF.1 filtered export, SF.2/SF.2b W.17 admission DENY; parquet/avro legs config-only)
  - **gpfdist Deployment + gpload control-file CSV load (Implemented/verified — Scenario 101):** when `dataLoading.gpfdist.enabled: true`, `reconcileGpfdist` deploys a gpfdist file-server runtime — the `<cluster>-gpfdist` **Deployment** (`gpfdist -d /data -p 8080 -l /var/log/gpfdist.log`; replicas honor `gpfdist.replicas`, default 1; image `gpfdist.image`, default `cloudberry-gpfdist:2.1.0`), a `<cluster>-gpfdist-data-pvc` **PVC** (RWO, 1Gi, mounted `/data`) and a `<cluster>-gpfdist-svc` **Service** (selector `avsoft.io/component=gpfdist` == pod labels, port 8080). A `type: gpload` job is **rerouted** from native external-table DDL to a **gpload control file** (`internal/builder/gpload_builder.go`, GL.1-GL.7): the operator renders a byte-stable control file (`gpfdist://<cluster>-gpfdist-svc:8080<glob>`, FORMAT/DELIMITER/HEADER/ENCODING, ERROR_LIMIT/LOG_ERRORS, OUTPUT TABLE/MODE insert|update|merge, PRELOAD TRUNCATE, SQL AFTER), delivers it via the per-job ConfigMap `<cluster>-gpload-<job>` mounted at `/etc/gpload`, and runs `gpload -f /etc/gpload/<job>.yml` in a `Job`/`CronJob`. New `gploadJob` fields (`inputSource{type:gpfdist|local,host,port}`, `delimiter`, `header`, `encoding`, `matchColumns`, `updateColumns`, `preload.truncate`, `postActions`; `mode`/`format` enums) drive the control file, guarded by webhook rules **W.18-W.22**. *(Two documented divergences from the spec's illustrative design: the per-cluster PVC name `<cluster>-gpfdist-data-pvc` rather than the literal `gpfdist-data-pvc`; the actual label domain `avsoft.io/component` rather than the illustrative `cloudberry.apache.org/component`.)* **Observability is honest:** gpload reuses the existing `cloudberry_data_loading_*` metrics (no new metric); gpfdist Deployment readiness is observed via `kubectl` (kube-state-metrics is absent in the test env); the 2 `cloudberry_gpfdist_*` metrics stay **Planned** (no scrapable endpoint). The gpfdist + gpload binaries are present in `cloudberry-official-pxf:2.1.0`; the thin `cloudberry-gpfdist:2.1.0` image is built via `make docker-build-gpfdist`. Verified by **Scenario 101**
  - **kafka-cdc continuous streaming via a custom connector (Implemented — Scenario 102; policy reversal scoped to custom connectors):** the `kafka` profile is **reinstated** as a **custom-connector** profile (built-in streaming stays out of scope — `isValidPxfProfile("kafka")` is still false). The model: a `dataLoading.pxf.servers[]` entry of the new **`custom`** type (CRD enum now `s3;hdfs;jdbc;hbase;hive;gs;abfss;wasbs;custom`; no forced config keys) + a matching `dataLoading.pxf.customConnectors[]` entry `{name, jarUrl}` + a `pxfJob` with `profile: kafka`. A new **`pxf-connector-init`** init container (`BuildPXFConnectorInitContainers`, wired after `pxf-cred-init` on segment-primary pods) downloads each `customConnectors[].jarUrl` into `/pxf/lib/custom/<name>.jar` on the sidecar classpath (`s3://`→`aws s3 cp` with the backup S3 creds + `AWS_S3_ENDPOINT`, `http(s)://`→`curl`). A `pxfJob.continuous: true` job runs as a **one-off long-running `Job`** (NOT a CronJob, even with no schedule; `ActiveDeadlineSeconds: nil` + `RestartPolicy: OnFailure` + `BackoffLimit: 6`) whose loader runs a streaming consume loop (`INSERT INTO <target> SELECT * FROM <ext>` per flush) until deleted; new `pxfJob` fields `continuous`/`batchSize`/`flushInterval` flow to the Job as `CBK_CONTINUOUS`/`CBK_BATCH_SIZE`/`CBK_FLUSH_INTERVAL`. Three webhook rules guard it: **W.23** (kafka/rabbitmq admitted only on a connector-backed `custom` server — bare kafka / kafka on a non-custom server still rejected), **W.24** (a `custom` server needs a matching `customConnectors[].name`), **W.23c** (`batchSize ≥ 1`, valid-duration `flushInterval`, `continuous` excludes `schedule`). **Observability is honest (no new metric):** kafka-cdc reuses `cloudberry_data_loading_*` — a continuous consumer's **steady state is `cloudberry_data_loading_job_status = Running`** (NOT Complete), `rows_total` best-effort per flush. **Live caveat:** end-to-end kafka→table row landing needs a REAL Kafka→PXF connector JAR (the staged one is a placeholder), so live row-landing is **config-only/documented** while the JAR download + mount + Job + DDL + streaming params are fully provable. Verified by **Scenario 102** (C.18, J.41–J.46, W.23/W.24/W.23c)
  - **FDW-based loading path (Implemented — Scenario 103):** a PXF job with the new **`pxfJob.loadMethod: fdw`** field (enum `external-table` (default) | `fdw`) loads via a **PERSISTENT** foreign-data-wrapper chain instead of the transient external table — `buildFDWDDL` emits `CREATE SERVER "foreign_<server>" FOREIGN DATA WRAPPER <scheme>_pxf_fdw` + `CREATE USER MAPPING FOR "gpadmin"` + `CREATE FOREIGN TABLE "foreign_<job>" (LIKE <target>)` (all `IF NOT EXISTS`, **never dropped**, directly queryable) then `INSERT INTO <target> SELECT * FROM "foreign_<job>" [WHERE <sourceFilter>]` + `ANALYZE`. The `FOREIGN DATA WRAPPER` is the **live-verified per-protocol** `pxf_fdw` wrapper registered in `cloudberry-official-pxf:2.1.0` (`SELECT fdwname FROM pg_foreign_data_wrapper`): `s3`→`s3_pxf_fdw`, `gs`→`gs_pxf_fdw`, `abfss`→`abfss_pxf_fdw`, `wasbs`→`wasbs_pxf_fdw`, `jdbc`→`jdbc_pxf_fdw`, `hdfs`→`hdfs_pxf_fdw`, `hive`→`hive_pxf_fdw`, `hbase`→`hbase_pxf_fdw` (generic fallback `pxf_fdw`; the `format` OPTION is omitted for bare `jdbc`/`hive`). The FDW path is **EQUIVALENT** to the external-table path (the same rows land). It is **read-only** — webhook **W.25** rejects `loadMethod: fdw` with `mode: writable`/`continuous: true` (and an unknown `loadMethod`); the **W.17** tweak allows `sourceFilter` on an fdw read. **Observability is honest (no new metric):** an FDW load reuses `cloudberry_data_loading_*`; the equivalence is proven by **equal row counts** (`count(events_ext) == count(events_fdw)`). Verified by **Scenario 103** (EX.5-EX.8, W.25, W.17 fdw-read; `cloudberry_pxf_*`/`cloudberry_gpfdist_*` stay Planned)
  - **Pre-load health checks (Implemented — Scenario 104):** before each data-loading Job the operator prepends a **`dataload-healthcheck` init container** (FIRST in the pod) on **both** the PXF/native (`buildDataLoadPodSpec`) and gpload (`buildGploadPodSpec`) Job pods; a non-zero check **blocks the load** → the Job fails. The five gated checks: **HC.1** PXF readiness (PXF jobs — a `psql` **DB-proxy** probe against the coordinator: `SELECT 1` / `pg_extension WHERE extname='pxf'` / `pxf_version()`; it is **NOT** a direct probe of the segment's localhost-only PXF sidecar, which the load pod cannot reach — the segment-pod sidecar liveness probe uses `/actuator/health`, while the legacy `/pxf/v15/Status` path 404s and is not used; the live proof is "stop PXF on a segment → the job fails"), **HC.2** target table exists (`to_regclass`, all jobs), **HC.3** object-store source connectivity (`curl --head ${AWS_S3_ENDPOINT}`; s3-family only, skipped for jdbc/hive/hbase/hdfs), **HC.4** gpfdist reachability (`curl http://<cluster>-gpfdist-svc:8080/`, gpload jobs when gpfdist enabled), **HC.5** scratch disk space (`df -Pk /dataload-scratch` ≥ `diskMinFreeMB`, all jobs). A `dataload-scratch` `emptyDir` (`SizeLimit` from `scratchSizeLimit`) is mounted at `/dataload-scratch` on both the init and main container. New CRD knob `dataLoading.healthChecks { enabled (default true; a nil block ⇒ on), diskMinFreeMB (default 64), scratchSizeLimit }`; `enabled: false` removes the init container + scratch volume. New controller Event **`DataLoadingHealthCheckFailed`** — a de-duplicated `Warning` Event emitted (`emitDataLoadHealthCheckFailureEvent`) when a data-load Job is observed Failed **and** the `dataload-healthcheck` init container terminated non-zero (honest attribution via the pod's `initContainerStatuses`; a main-container failure gets no HC event); restore the condition → the Job re-runs → init passes → the load proceeds. **Observability is honest (no new operator metric):** failures show via `cloudberry_data_loading_job_status=3` + `cloudberry_data_loading_errors_total` + the `DataLoadingHealthCheckFailed` Event + the NEW **kube-state-metrics** (`kube_job_status_failed{job_name=~".*-dataload-.*"}` / `kube_pod_init_container_status_*` / `kube_deployment_status_replicas_available`); `cloudberry_pxf_*`/`cloudberry_gpfdist_*` stay **Planned**. Verified by **Scenario 104** (HC.1-HC.5 + the init container + the knob + the Event)
  - **Live PXF health sub-status / DataLoadingStatus fields (Implemented — Scenario 105):** `status.dataLoading.pxf.status` (`Running`/`Stopped`/`Error`, **ABSENT** when unobservable) is derived **ONLY** from real segment-primary `pxf` container readiness (`ContainerStatuses`) aggregation (`util.PXFReadyCount`/`PXFStatusFromReadiness`) — **no exec, no live HTTP probe, no synthesized health**: all pxf containers ready→`Running`, some down→`Error` (degraded), none ready→`Stopped`, no pods observed→absent (a segment stop flips `Running → Error`/`Stopped`, restore → `Running`). `status.dataLoading.pxf.extensionsInstalled` lists `pxf`/`pxf_fdw` from a real read-only `pg_extension` probe (`db.Client.ListPXFExtensions`), **ABSENT (nil)** when the DB is unreachable or none are installed — **never synthesized**. Two HONEST gauges back them — `cloudberry_pxf_status` (0=Stopped/1=Running/2=Error) and `cloudberry_pxf_extensions_installed` (count) — **emitted only when observable**. S.2 (`pxf.servers`=len(servers)), S.4 (`activeJobs`) and S.5 (`jobs[]` name/lastRun/lastStatus/rowsLoaded/duration) remain honest. Verified by **Scenario 105**
  - **PXF server configuration update / delete observability (Implemented — Scenario 106):** SL.7/SL.8 mechanics (full-replacement reconcile of the `<cluster>-pxf-servers` ConfigMap) gain **honest observability**. Patching a server (e.g. `minio-warehouse`'s `fs.s3a.endpoint`, **SL.7**) regenerates only that server's `<server>__s3-site.xml` (others byte-identical); sidecars pick up on the next volume sync **or** an explicit `cloudberry-ctl pxf sync`, and reads use the **new** endpoint. Removing a server from `dataLoading.pxf.servers[]` (**SL.8**) drops its `<server>__*.xml` keys; external/foreign tables referencing it **fail** until recreated. On a **real** ConfigMap `Data` diff — and **only** then (never on a no-op sync or first create) — both the controller reconcile (`emitPXFServersChanged`) and the explicit `pxf sync` API path (`recordPXFServersChanged`) emit a **`PXFServersChanged`** event (message `PXF servers changed: added=[..] removed=[..] updated=[..]`) and increment the **`cloudberry_pxf_servers_changed_total{cluster,namespace}`** counter; the diff is computed by the shared, pure `util.DiffPXFServerNames`. Verified by **Scenario 106**
  - **All data-loading API endpoints P.1–P.15 (Implemented — Scenario 107):** the full data-loading REST surface now serves real data (`internal/api/dataloading.go`, wired by `registerDataLoadingRoutes`). The five **job mutations** flip from 501-stub → FULL: **P.8** `POST .../jobs` (Operator; `201`/`409 JOB_EXISTS`/`400` unknown server), **P.10** `PUT .../jobs/{job}` (Operator), **P.11** `DELETE .../jobs/{job}` (Admin; best-effort deletes the spawned Job), **P.12** `POST .../jobs/{job}/start` (Operator; creates a **REAL one-off `batchv1.Job`** → `202`/`409 JOB_ALREADY_RUNNING`), **P.13** `POST .../jobs/{job}/stop` (Operator; deletes the Job / suspends the CronJob → `202`/idempotent `200`). The **PXF servers CRUD** flips from Planned → FULL: **P.2** `GET .../pxf/servers[/{server}]` (Basic; REFERENCES only — no literal secrets), **P.3** `POST .../pxf/servers` (Operator; `201` returns the **rendered** `<server>__*.xml`/`409 SERVER_EXISTS`), **P.4** `PUT .../pxf/servers/{server}` (Operator), **P.5** `DELETE .../pxf/servers/{server}` (Admin; `409 SERVER_IN_USE` when a job references it — mirrors webhook W.9). **P.14** `GET .../jobs/{job}/logs` (Basic) **streams the REAL data-loading Job pod logs** (`?follow`/`?tailLines`; honest `501 LOGS_NOT_AVAILABLE` only when no clientset). **P.15** `GET .../external-tables` (Basic) returns `{observed, observedAvailable, expected}` — `observed` from a live `pg_exttable` + foreign-table probe (`db.Client.ListExternalTables`, **`null`**+`observedAvailable:false` when the DB is unreachable, **never synthesized**); `expected` is the spec-derived would-be set, clearly labeled. New error codes `SERVER_NOT_FOUND`/`SERVER_EXISTS`/`SERVER_IN_USE`/`JOB_EXISTS`/`JOB_ALREADY_RUNNING`. Permissions: Basic (read/status/logs/external-tables), Operator (create/update/start/stop/sync), Admin (delete). Verified by **Scenario 107**
  - **All data-loading / PXF CLI commands L.1–L.16 (Implemented — Scenario 108):** `cloudberry-ctl` now fully implements the data-loading / PXF CLI surface (`cmd/cloudberry-ctl/main.go`), wired to the Scenario 107 REST endpoints, plus **one new** server-side endpoint for test-read. **PXF servers CRUD** (NEW): **L.2** `pxf servers list` → `GET .../pxf/servers`, **L.3** `pxf servers create` (`--name --type --endpoint --bucket --credential-secret` repeatable `name[:key]`) → `POST .../pxf/servers`, **L.4** `pxf servers update [name] --endpoint` → `PUT .../pxf/servers/{name}` (via the new `runAPIPut` helper), **L.5** `pxf servers delete [name]` → `DELETE .../pxf/servers/{name}` (`409 SERVER_IN_USE`). **Enriched `data-loading jobs create`**: **L.9** `--type pxf` (`--name --server --profile --resource --target --schedule`; previously posted a nil body), **L.14** `--type gpload` (`--gpfdist-host --gpfdist-port --file-path --format`), **L.16** `--from-yaml <file>` (reads + unmarshals a full job, **precedence over flags**). **L.13** `data-loading jobs logs --job --follow --tail` (NEW) streams the Job pod logs via `OperatorClient.GetStream` with a **kubectl fallback** (mirrors `backup jobs logs` 86k). **L.15** `data-loading test-read` (`--job` OR `--server/--profile/--resource`, `--limit N` default 10 cap 1000) calls the **NEW** `GET .../data-loading/test-read` endpoint (`handleTestReadPXFSource`, Permission Basic, **no metric**) backed by the **NEW** `db.Client.ReadPXFSourceSample` (transient external table → `SELECT … LIMIT N` → **always DROP**); HONEST contract: prints **REAL rows**, or `available:false`/empty when the DB/source is unreachable — **never fabricated, never `500`**. Response shape `TestReadResponse {cluster, source{server,profile,resource}, limit, available, rowCount, columns, rows}`. Verified by **Scenario 108**
  - **All Prometheus metrics M.1–M.16 (Implemented, honesty-bounded — Scenario 109):** the metric catalog is closed out under a single rule — **every emitted metric traces to a REAL source; a metric with no honest source stays intentionally ABSENT and is NEVER synthesized.** Four flip Planned → Implemented: **M.1** `cloudberry_pxf_service_up{cluster,namespace,segment_host}` (the **per-segment** disaggregation of `cloudberry_pxf_status`, set from real per-segment-primary-pod `pxf` container readiness via `util.PXFReadyByHost` — `1` healthy / `0` on a killed segment, emitted only for observed hosts, never synthesized); **M.2** `cloudberry_pxf_requests_total` + **M.3** `cloudberry_pxf_request_duration_seconds` (the **real** `http_server_requests_seconds_count`/`_sum`/`_bucket` series scraped from the PXF Spring Boot Actuator `/actuator/prometheus`, enabled via `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE=health,prometheus` and picked up by a **dedicated vmagent scrape job** at `:5888` — a single pod annotation **cannot** cover both the pg-exporter `:9187` and the actuator `:5888`; **label-honesty caveat:** request count + latency are REAL, but the catalog's `server`/`profile`/`operation` labels are **NOT** honestly derivable from the actuator URI → downgraded to the actuator-native `uri`/`method`/`status`, never fabricated); and **M.10** `cloudberry_data_loading_bytes_total{cluster,namespace,job,source_type}` (from the real `DATALOAD_BYTES=<n>` marker the gpload script computes via `wc -c` for a **local gpload input source** — **omitted (honestly absent)** for external-table/pxf/FDW/continuous loads where no byte count is available). **M.6** `cloudberry_pxf_errors_total` is **FOLDED, not fabricated** — the honest error signals are `cloudberry_data_loading_errors_total{job}` (+`job_status=3`) and actuator non-2xx (`http_server_requests{status≥4xx}`); no synthetic typed counter is registered. The **honestly-absent (still Planned, NEVER fabricated)** metrics stay absent with documented rationale: **M.4** `cloudberry_pxf_bytes_transferred_total`, **M.5** `cloudberry_pxf_records_total` (record throughput observed instead via `cloudberry_data_loading_rows_total`), **M.7** `cloudberry_pxf_active_connections`, **M.15** `cloudberry_gpfdist_connections_active`, **M.16** `cloudberry_gpfdist_bytes_served_total` (no honest source in PXF 2.1.0 / gpfdist) — and the tests **assert their absence** (a NOT-emitted metric is a PASS). Verified by **Scenario 109**
  - **Complete webhook-validation negative matrix W.1–W.15 (Implemented — Scenario 110):** the **systematic** rejected-CR proof that **each** of the 15 data-loading webhook rules **(a) rejects** an otherwise-valid CR carrying exactly one violation, **(b) with a descriptive (field-path + reason) error**, **(c) and the rejected CR does NOT persist** (a follow-up `GET` is `NotFound`), plus a **CONTROL** (a fully-valid CR admits — no false-positive). No production code changed (all 15 rules were already in `internal/webhook/validating.go`); Scenario 110 adds the **rejection-source-per-rule** analysis across unit + functional + integration + e2e + perf layers: **11 WEBHOOK-enforced** (W.1, W.2, W.4, W.5, W.6, W.7, W.9, W.10, W.13, W.14 — the user sees our descriptive message on a live apply), **3 CRD-SCHEMA-enum** (W.3 server `type: ftp`, W.8 job `type: spark`, W.15 `segmentRejectLimitType: fraction` — the CRD OpenAPI `Enum` rejects at the apiserver **before** the webhook runs, with the webhook keeping the rule for **defense-in-depth**), and **2 BOTH** (W.11 `pxfJob.targetTable` / W.12 `gploadJob.targetTable` — an omitted key → CRD schema `required`, an empty-string value → webhook). Triggers: W.1 empty `pxf.image`; W.2 empty/dup server name; W.3 `type: ftp`; W.4 s3 missing endpoint/creds; W.5 jdbc missing driver/url; W.6 hdfs missing `fs.defaultFS`; W.7 empty/dup job name; W.8 `type: spark`; W.9 undefined `pxfJob.server`; W.10 `profile: s3:nonsense`; W.11/W.12 no `targetTable`; W.13 `schedule: "not a cron"`; W.14 partitioning column without range/interval; W.15 `segmentRejectLimitType: fraction`. Artifacts: `test/{cases,functional,integration,e2e,perf}/scenario110_*` + `internal/webhook/scenario110_validation_test.go`. Verified by **Scenario 110**
  - **Data-loading security controls SE.1–SE.6 / SL.6 (Implemented, honesty-bounded — Scenario 111):** the previously-Planned data-loading **Security** controls are now built, each classified **REAL** (proven) or **CONFIG-ONLY** (rendered config verified; a live handshake is **never faked**). **SE.6 dedicated minimal-privilege DB role (REAL):** `db.EnsureDataLoaderRole` creates `CREATE ROLE <role> NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN` granted **only** `SELECT,INSERT ON PROTOCOL pxf`, opt-in via the new `dataLoading.pxf.dataLoaderRole` (empty ⇒ `gpadmin`, the existing behavior; additive). **SE.4 Kerberos keytab from Secret (config-correct):** the new `dataLoading.pxf.servers[].kerberos{principal,keytabSecret{name,key},krb5ConfigMap,realm}` mounts the keytab Secret on the PXF sidecar + `pxf-cred-init` at `$PXF_BASE/keytabs/<server>/` and renders `hadoop.security.authentication=kerberos` + `pxf.service.kerberos.principal`/`.keytab` into `core-site.xml` (optional krb5.conf ConfigMap); the webhook requires principal+keytab when kerberos is set and rejects kerberos on non-hdfs/hive/hbase types. **HONESTY:** live *authenticated* Hadoop/Hive/HBase access is **CONFIG-ONLY** (the test env has **no KDC**; the operator never runs a live `kinit`). **SE.5 segment↔sidecar `localhost`-only (REAL):** `BuildPXFClusterNetworkPolicy` emits a NetworkPolicy for the segment-primary pods that does **not** allow cross-pod ingress to PXF `:5888` (same-pod localhost traffic is never subject to NetworkPolicy, so loads keep working — policy applied + load still succeeds). **SE.1/SL.6 init-container secret rendering (REAL):** `pxf-cred-init` resolves `${PLACEHOLDER}` site-XML from `credentialSecrets[]` into the ephemeral pod filesystem; **secrets never land in the ConfigMap**. **SE.2/SE.3 JDBC/S3 TLS passthrough (declarative):** JDBC URL/`ssl` params → `jdbc-site.xml`, `fs.s3a.connection.ssl.enabled=true` → `s3-site.xml`; a live encrypted handshake is asserted **only** when the source speaks TLS, otherwise **CONFIG-ONLY** — never faked. Verified by **Scenario 111**
  - **Data-loading disabled states DIS.1–DIS.3 (Implemented — Scenario 112):** the three "off" states are now honest and active. **DIS.1 — `dataLoading.enabled: false` TEARS DOWN (no longer a no-op):** `reconcileDataLoading` dispatches to `cleanupDataLoading`, which deletes the `<cluster>-pxf-servers` ConfigMap, the gpfdist Deployment/Service/PVC, all data-loading Jobs+CronJobs, the gpload control-file ConfigMaps, and the PXF NetworkPolicy, drops the PXF sidecar from the segment-primary StatefulSet (re-rendered without it), clears `Status.DataLoading`, sets `DataLoadingConfigured=False` reason `DataLoadingDisabled`, fires a one-shot `DataLoadingDisabled` event, and zeroes `cloudberry_data_loading_jobs_active`/`cloudberry_pxf_servers_configured`; the data-loading REST API reports `DATA_LOADING_NOT_ENABLED` (mutations `400`; list/get `200` disabled envelope; **DL-disabled precedence over `PXF_NOT_ENABLED`**); **re-enable → the idempotent reconcile redeploys everything**. **DIS.2 — `pxf.enabled: false` independence:** no PXF sidecars/extensions/ConfigMap (`pxfSidecarEnabled` gate + `ensurePxfServersConfigMap` delete-when-disabled) while gpload-type jobs still function. **DIS.3 — `gpfdist.enabled: false`:** the gpfdist Deployment/Service/PVC are GC'd, `inputSource.type: local` gpload jobs still work, and a gpfdist-source gpload job reports the missing dependency via the **HONEST RUNTIME** signal (gpload can't reach the absent host → Job Failed + `cloudberry_data_loading_errors_total` + `status=Failed`) — **HC.4 is skipped when gpfdist is disabled** (gated on `gpfdist.enabled`), so the signal is the runtime failure, NOT a fabricated pre-flight check. Verified by **Scenario 112**
  - **Planned / honestly absent (never fabricated):** the remaining PXF/gpfdist runtime — a first-class data-export Job kind, and the **6 honestly-absent metric families**: `cloudberry_pxf_bytes_transferred_total` (M.4 — a deliberate metrics-honesty hold: PXF has no honest external-byte counter, so filter pushdown is observed via row-count reduction + `EXPLAIN` + source logs instead — Scenario 98), `cloudberry_pxf_records_total` (M.5; substituted by `cloudberry_data_loading_rows_total`), `cloudberry_pxf_errors_total` (M.6 — **folded** into `cloudberry_data_loading_errors_total` + actuator non-2xx, not a synthetic metric), `cloudberry_pxf_active_connections` (M.7), and the 2 `cloudberry_gpfdist_*` metrics (M.15/M.16, no scrapable endpoint). **Scenario 109 flipped `cloudberry_pxf_service_up`, the actuator-passthrough `cloudberry_pxf_requests_total`/`cloudberry_pxf_request_duration_seconds`, and the conditional `cloudberry_data_loading_bytes_total` to Implemented** (see the Scenario 109 bullet above). (Config sync is structural via the shared ConfigMap; the **explicit** `cloudberry-ctl pxf sync` trigger is now **Implemented** — Scenario 95.) The job-mutation + `pxf/servers` CRUD + `jobs/{job}/logs` + `external-tables` **REST** routes are **Implemented (Scenario 107)**, and the matching **CLI** subcommands (`pxf servers …` CRUD, `data-loading jobs logs`, `data-loading test-read`, `--from-yaml`, gpload flags) are now **Implemented (Scenario 108)** — no L.1–L.16 CLI command remains planned. See [spec 12 §Implementation Status](specifications/12-data-loading-spec.md#implementation-status)

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
| `operator.enableTestUsers` | Seed well-known TEST users (`basic_user`/`opbasic_user`/`operator_user`). **Test suites only — never enable in production** | `false` |
| `vault.enabled` | Enable Vault integration | `false` |
| `vault.authMethod` | Vault auth method (`token`, `kubernetes`, `approle`) | `kubernetes` |
| `vault.roleID` / `vault.secretID` | AppRole credentials (`approle` auth method) | `""` |
| `oidc.enabled` | Enable OIDC authentication | `false` |
| `telemetry.enabled` | Enable OTLP tracing | `false` |
| `telemetry.otlpInsecure` | Disable TLS for OTLP exporter | `false` |
| `metrics.enabled` | Enable Prometheus metrics | `true` |
| `webhook.enabled` | Enable admission webhooks | `true` |
| `webhook.certSource` | Certificate source (`self-signed` or `vault-pki`) | `self-signed` |

Configuration precedence (highest wins): **environment variable > command-line flag > config file > default**. The environment always wins, even over an explicitly set flag.

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

**Functional test scenarios** cover the full operator lifecycle: cluster bootstrap (1), config hot-reload and rolling restart (2), stop/start modes (3), maintenance operations (4), session management (5), resource groups (6), test data loading (7), scale-out (8), scale-in (9), rebalancing (10), scale-out failure (11), scale-in confirmation (12), PV expansion (13), cluster upgrade with rollback (14), error handling and observability (15), cluster deletion (16), mirroring enable/disable (19), automatic segment failover via FTS (20), bootstrap workload management via CRD (25), webhook validation negative tests for backup configuration (69a–69j), webhook defaults verification for backup configuration (70), full S3 backup configuration with Secret and Vault credential sources (71), backup infrastructure deployment (72), on-demand backup with per-request gpbackup options incl. the `noCompression` override (73), PXF data-loading webhook validation negative tests (89, rules W.1–W.16 + W.10b), PXF data-loading webhook defaults (90), PXF full CRD configuration — segment-primary sidecar + servers ConfigMap rendering with logLevel→PXF_LOG_LEVEL propagation (91), the data-loading ingestion runtime — Job/CronJob generation + launch, external-table DDL → `INSERT…SELECT` → `DATALOAD_ROWS` marker harvest, rich status + 5 metrics, with row-count-verified native loads and operator-driven `pxf://` execution (92, row-count-verified: 183,961 rows from MinIO S3 via the PXF sidecar), PXF server ConfigMap / per-type file-mapping (SL.1–6) / `CREATE EXTENSION pxf`+`pxf_fdw` + `GRANT SELECT`/`INSERT ON PROTOCOL pxf TO gpadmin` / shared-ConfigMap sync (93), PXF sidecar deployment verification — the `pxf` container shape on the segment pod (94), and the PXF CLI lifecycle — `cloudberry-ctl pxf status|restart|sync` operator verbs (honest sidecar-readiness aggregation, restart via the segment-primary StatefulSet restart-trigger pod-roll, explicit ConfigMap-refresh sync, `cloudberry_pxf_restart_total`) plus the sidecar-local `pxf prepare/start/stop` exec verbs (95), and object-store profiles & format write-capability — the `gs`/`abfss`/`wasbs` (+ Dell-ECS / MinIO) object-store server types, the `internal/pxfpolicy` write-capability matrix enforced by the webhook (W.10b) and the builder, and the `pxfwritable_export` writable external-table DDL (96, OS.1–OS.10 / CFG.1–CFG.8 / FF.1–FF.5), and the Hadoop profiles (HDFS/Hive/HBase) with the now scheme-aware write-capability (all `hive*`/`HBase` read-only regardless of format) and the `hive-site.xml`/`hbase-site.xml` rendering (97, HP.1–6 / HV.1–4 / HB.1 / SITE.1–4 / FF.6/FF.7 / WRej.1–7), and filter pushdown / column projection / per-row error handling — the `FILTER_PUSHDOWN=true` / `PROJECT=true` DDL knobs (mutating-defaulted to `true`) and the `[LOG ERRORS ]SEGMENT REJECT LIMIT` clause, runtime-verified via row-count reduction + `EXPLAIN` + source query logs + job-status/errors (no fabricated `bytes_transferred` — it stays Planned) (98, FE.1–5 / FE.12a/b), and writable external tables / data export — `mode: writable` jobs that build `CREATE WRITABLE EXTERNAL TABLE … FORMATTER='pxfwritable_export'` and export Cloudberry rows OUT to **S3 / object store**, **HDFS** and **JDBC** (reversed `INSERT INTO <ext> SELECT * FROM <target>`), plus the new optional `pxfJob.sourceFilter` filtered export (`… WHERE region='us-east'`) guarded by webhook rule **W.17** (sourceFilter only on writable jobs; rejects `;`/`--`/`/*`), observed via `cloudberry_data_loading_rows_total`/`job_status` — no new metric (99, FE.9/WE.1 / FE.10 / FE.11 / WE.2 / SF.1 / SF.2), and gpfdist Deployment + gpload-csv — the gpfdist `Deployment`/`Service`/`PVC` (GP.2–GP.5) and the gpload control-file `Job`/`CronJob` (control file GL.1–GL.7 → `<cluster>-gpload-<job>` ConfigMap mounted at `/etc/gpload` → `gpload -f`), the new `gploadJob` fields and webhook rules W.18–W.22 (101), and kafka-cdc continuous streaming via a custom connector — the `kafka` profile reinstated as a custom-connector profile (`servers[].type: custom` + `customConnectors[]` + `pxfJob.profile: kafka`), the `pxf-connector-init` JAR-download init container (`/pxf/lib/custom`, C.18), the continuous one-off streaming Job (NOT a CronJob, J.43/J.46) with `continuous`/`batchSize`/`flushInterval` (→ `CBK_*` env), and webhook rules W.23/W.24/W.23c — observed via `cloudberry_data_loading_job_status=Running` steady state (no new metric; end-to-end row landing config-only with a placeholder JAR) (102), the FDW-based loading path — `pxfJob.loadMethod: fdw` builds a persistent `CREATE SERVER`/`USER MAPPING`/`FOREIGN TABLE` chain (per-protocol `pxf_fdw` wrapper) loaded by `INSERT…SELECT`, EQUIVALENT to the external-table path (equal row counts), guarded by W.25 + the W.17 fdw-read tweak (103), and the pre-load health checks — the `dataload-healthcheck` init container (FIRST on both PXF and gpload Job pods) running HC.1-HC.5 (PXF DB-proxy readiness, target table exists, object-store connectivity, gpfdist reachability, scratch disk space), the `dataLoading.healthChecks` knob, and the de-duplicated `DataLoadingHealthCheckFailed` Event, observed via `cloudberry_data_loading_job_status=3` + the new kube-state-metrics (no new operator metric) (104), and the DataLoadingStatus PXF fields — the live, honest `status.dataLoading.pxf.status` (`Running`/`Stopped`/`Error`, absent when unobservable) from real segment-primary `pxf` container readiness aggregation (S.1; segment-stop → `Error`/`Stopped`), `pxf.servers` count (S.2), `pxf.extensionsInstalled` from a real read-only `pg_extension` probe (S.3), `activeJobs` (S.4) and per-job `jobs[]` runtime fields (S.5), backed by the honest `cloudberry_pxf_status` / `cloudberry_pxf_extensions_installed` gauges (emitted only when observable) (105), and the PXF server configuration update / delete — patching a server endpoint regenerates only that server's `<server>__s3-site.xml` and reads use the new endpoint (SL.7), removing a server drops its `<server>__*.xml` keys so referencing tables fail until recreated (SL.8), with the honest `PXFServersChanged` event (message `added/removed/updated`) and `cloudberry_pxf_servers_changed_total{cluster,namespace}` counter fired by BOTH the reconcile and the `pxf sync` path **only on a real ConfigMap `Data` diff** — never on a no-op sync or first create (106), and all Prometheus metrics M.1–M.16 under the honesty rule — `cloudberry_pxf_service_up{segment_host}` from real per-segment `pxf` readiness (kill a segment → its series → 0), the actuator-passthrough `cloudberry_pxf_requests_total`/`cloudberry_pxf_request_duration_seconds` (real `http_server_requests_*` from `/actuator/prometheus` via a dedicated `:5888` vmagent job; `server/profile/operation` labels downgraded to actuator-native, never fabricated), the conditional `cloudberry_data_loading_bytes_total` (real `DATALOAD_BYTES` via `wc -c` on local gpload input; omitted otherwise), `cloudberry_data_loading_job_status` cycling 0→1→2→3, and the honestly-absent M.4/M.5/M.7/M.15/M.16 + the folded M.6 (a NOT-emitted metric is a PASS — never fabricated) (109), and the complete webhook-validation negative matrix W.1–W.15 — the systematic rejected-CR proof that each of the 15 data-loading webhook rules rejects an otherwise-valid CR carrying exactly one violation with a descriptive (field-path + reason) error and that the rejected CR does NOT persist (`GET` → NotFound), plus a CONTROL (a valid CR admits — no false-positive), recording the rejection source per rule — **11 webhook-enforced**, **3 CRD-schema-enum** (W.3 `type: ftp`, W.8 `type: spark`, W.15 `segmentRejectLimitType: fraction`; CRD `Enum` rejects at the apiserver before the webhook, which keeps the rule for defense-in-depth), **2 both** (W.11/W.12 `targetTable`: omitted-key→schema `required`, ``""``→webhook) — across unit + functional + integration + e2e + perf layers (no production change; all rules already in `internal/webhook/validating.go`) (110), and data-loading security controls SE.1–SE.6 / SL.6 — dedicated minimal-privilege DB role (`dataLoading.pxf.dataLoaderRole`, `NOSUPERUSER` + pxf-only grants, REAL), Kerberos keytab from Secret (`dataLoading.pxf.servers[].kerberos`, config-correct; live auth CONFIG-ONLY / no-KDC), segment↔sidecar `localhost`-only NetworkPolicy (REAL), `${...}` placeholder secret rendering + no-plaintext-in-ConfigMap (REAL), and JDBC/S3 TLS passthrough (declarative; live TLS CONFIG-ONLY unless the source speaks TLS) — no faked Kerberos/TLS handshake (111), and the data-loading disabled states DIS.1–DIS.3 — DIS.1 `dataLoading.enabled: false` now TEARS DOWN via `cleanupDataLoading` (deletes the `<cluster>-pxf-servers` ConfigMap, gpfdist Deployment/Service/PVC, all Jobs+CronJobs, gpload control-file ConfigMaps, the PXF NetworkPolicy; drops the segment-primary PXF sidecar; clears `Status.DataLoading`; condition `False`/`DataLoadingDisabled`; one-shot `DataLoadingDisabled` event; `cloudberry_data_loading_jobs_active`→0) with the data-loading API reporting `DATA_LOADING_NOT_ENABLED` (mutations `400`; list/get `200` disabled envelope; DL-disabled precedence over `PXF_NOT_ENABLED`) and re-enable redeploying everything idempotently; DIS.2 `pxf.enabled: false` independence (no PXF sidecars/extensions/ConfigMap; gpload jobs still work); DIS.3 `gpfdist.enabled: false` (gpfdist objects GC'd; local gpload jobs still work; a gpfdist-source job reports the missing dependency via the HONEST RUNTIME gpload failure — HC.4 is skipped when gpfdist is disabled, so it is the runtime Job-Failed signal, NOT a fabricated pre-flight check) (112). See [docs/development.md](docs/development.md) for detailed test descriptions.

The project **enforces 90%+ unit test statement coverage per package**. Goroutine-heavy packages (`internal/api`, `internal/controller`, `internal/idle`, `internal/vault`) run [goleak](https://github.com/uber-go/goleak) in their `TestMain` to fail the suite on leaked goroutines. Integration Scenario 89 verifies the backup artifact round-trip (upload → byte-for-byte download → retention delete) against the **real MinIO** object store from the Docker Compose environment, using the same bucket/folder layout and credentials the backup Jobs use. Total coverage: **91.4%** with all 14 internal packages at 90%+. Key coverage: `internal/vault` at 99%, `internal/metrics` at 100%, `internal/api` at ~96%, `internal/db` at ~92%, `internal/certmanager` at ~93%, `internal/controller` at ~90.1%, `internal/auth` at ~97.6%, `internal/idle` at ~97%, `cmd/cloudberry-ctl` at ~91.6%, `cmd/operator` at ~30.0%. All **1,936 tests** pass (functional: 1,063, e2e: 833, integration: 38). See [docs/development.md](docs/development.md) for the full development and testing guide.

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

Pre-built Grafana dashboards are available in the `monitoring/grafana/` directory — four dashboards: **operator** (`cloudberry-operator.json`), **exporters** (`cloudberry-exporters.json`), **node metrics** (`cloudberry-node-metrics.json`), and **OTel/telemetry** (`cloudberry-otel.json`). The operator dashboard visualizes all operator metrics — including the REST API panels (request rate/duration/in-flight/rate-limit rejections), DB connect/pool/query panels, idle-daemon health, and a **Security & Lifecycle** section covering certificate rotation and expiry, cluster TLS issuance, Vault operations, webhook admissions, upgrades, rolling restarts, and recovery. The operator dashboard gained **8 panels** for the request-side API/DDL metrics — `cloudberry_api_cluster_lifecycle_requests_total` (by operation/result), `cloudberry_api_workload_operations_total` (by kind/operation/result), and `cloudberry_pxf_sync_total` (by result) — plus a **DB-query-duration-by-operation p95** panel built on `cloudberry_db_query_duration_seconds_bucket`. It also gained a **"gpfdist, PXF Extension & Data-Loader Role Setup"** row covering the data-loading control-plane metric families — for each of `cloudberry_gpfdist_reconcile_total` (by operation/result), `cloudberry_pxf_extension_setup_total` (by result), and `cloudberry_dataloader_role_setup_total` (by result, **3 new panels** following the `cloudberry_pxf_extension_setup_total` pattern): a rate timeseries, a 1h-total stat, and an error stat. The **OTel/telemetry** dashboard (`cloudberry-otel.json`) renders Tempo traces for `service.name=cloudberry-operator`, otel-collector health (`otelcol_*`), and operator logs from VictoriaLogs. The test monitoring stack (Helm charts under `test/monitoring/`: vmagent, vector, otel-collector, node-exporter, kube-state-metrics) is deployed via `make monitoring-deploy`. **kube-state-metrics** (added in Scenario 104) feeds Kubernetes object-state metrics (`kube_job_status_failed`, `kube_pod_init_container_status_*`, `kube_deployment_status_replicas_available`) into VictoriaMetrics so pre-load health-check failures and gpfdist deployment readiness are observable in metrics (dashboard panels 274-276).

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
- **All Backup API Endpoints**: Scenario 85 verified — the **seven** OIDC/JWT-authenticated backup/restore REST endpoints under `/api/v1alpha1/clusters/{name}/backups`, each enforcing its own RBAC level via `withPermission` (`internal/api/server.go`): **85a** `GET /backups` (Basic) → `handleListBackups` lists the operator's recorded backup history (`status.backup.backupHistory`, the operator's view of `gpbackup` outcomes derived from observed Jobs — **not** a live `gpbackman` query); **85b** `POST /backups` (Operator) → `handleCreateBackup` creates a backup **Job** (not the CronJob) whose `gpbackup` args match `CreateBackupRequest.gpbackupOptions` (`buildBackupJobOptions`/`mergeGpbackupOptions` → `buildGpbackupArgs`) — **including the fix that `leafPartitionData` now emits `--leaf-partition-data` on FULL backups too, exactly once** (`appendLeafPartitionDataArgs` guarded on `!isEffectivelyIncremental` so the incremental force-pair `--incremental --leaf-partition-data` is never duplicated; net: full+false→none, full+true→one, incremental→one); **85c** `GET /backups/{timestamp}` (Basic) → `handleGetBackup` returns the matching `BackupHistoryEntry` (`400` non-14-digit, `404 BACKUP_NOT_FOUND` unknown); **85d** `DELETE /backups/{timestamp}` (Admin) → `handleDeleteBackup` creates a `gpbackman` cleanup Job (`backup-delete`, label `avsoft.io/backup-operation=cleanup`); **85e** `POST /backups/{timestamp}/restore` (Admin) → `handleRestoreBackup` creates a restore **Job** whose `gprestore` args match `RestoreRequest.gprestoreOptions` (`buildRestoreJobOptions`/`mergeGprestoreOptions` → `buildGprestoreArgs`: `dataOnly→--data-only`, `metadataOnly→--metadata-only`, `resizeCluster→--resize-cluster`, `redirectDb→--redirect-db`, …) with **mutual exclusivity** enforced — `dataOnly`+`metadataOnly` rejected `400` (`restoreOptionsConflict`), include-table wins over include-schema, run-analyze wins over with-stats; **85f** `GET /backups/jobs` (Basic) → `handleListBackupJobs` lists backup/restore/cleanup Job statuses (`succeeded|failed|running|pending`); **85g** `GET /backups/schedule` (Basic) → `handleGetBackupSchedule` returns the CronJob status + **computed** `nextScheduleTime` (`scheduled:false` when no CronJob). A missing cluster returns `404` for every endpoint; backup-not-enabled → `400 BACKUP_NOT_ENABLED`. DTOs + mapping in `internal/api/backup.go`; args in `internal/builder/backup_builder.go`. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario85-api-endpoints.yaml`; live cycle `test/e2e/scripts/scenario85-api-endpoints.sh`
- **All Backup CLI Commands**: Scenario 86 verified — the **eleven** `cloudberry-ctl backup …` commands (`cmd/cloudberry-ctl/main.go`, `newBackupCmd` → `newBackupCreateCmd`/`newBackupListCmd`/`newBackupStatusCmd`/`newBackupDeleteCmd`/`newBackupRestoreCmd`/`newBackupScheduleCmd`(+`set`/`suspend`/`resume`)/`newBackupJobsCmd`→`newBackupJobsLogsCmd`) drive the operator backup REST API over an OIDC bearer token (the CLI is pointed at the API via `--operator-url`/`CLOUDBERRY_OPERATOR_URL` and authenticates with `--auth-method oidc` + the token via `--password`/`CLOUDBERRY_PASSWORD`): **86a** `backup create` (`buildCreateBackupRequest` → all `gpbackupOptions` flags) → `POST /backups`; **86b** `backup list` → `GET /backups`; **86c** `backup status --timestamp` → `GET /backups/{ts}`; **86d** `backup delete --timestamp` → `DELETE /backups/{ts}`; **86e** `backup restore --timestamp` (`buildRestoreRequest` → all `gprestoreOptions` flags incl. **`--resize-cluster`**, which enables restoring into a cluster with a different segment count) → `POST /backups/{ts}/restore`; **86f** `backup schedule` → `GET /backups/schedule`; **86g/h/i** `backup schedule set --cron`/`suspend`/`resume` → `PATCH /backups/schedule` (`{schedule}` / `{suspend:true|false}`); **86j** `backup jobs` → `GET /backups/jobs`; **86k** `backup jobs logs --job <name>` now **STREAMS** the Job's pod logs (`--follow`/`--tail`) — a **new** operator endpoint `GET /clusters/{name}/backups/jobs/{job}/logs` (Permission Basic, `handleBackupJobLogs` in `internal/api/server.go`) finds the Job's most-recent pod (`findJobPod`/`mostRecentPodName`) and streams its container logs as `text/plain` (`?follow`/`?tailLines`), and the CLI copies the stream to stdout via `OperatorClient.GetStream` (`internal/ctl/client.go`) with a `kubectl logs` fallback when the endpoint is unavailable. The operator gained a typed Kubernetes clientset (injected via `Server.WithClientset`, wired from `mgr.GetConfig()`) because the controller-runtime client cannot stream pod logs. Sample CR `deploy/helm/cloudberry-operator/config/samples/scenario86-cli-commands.yaml`; live cycle `test/e2e/scripts/scenario86-cli-commands.sh`
- **Dashboards**: All Grafana dashboards in `monitoring/grafana/` (operator, exporters, node) reflecting live metrics; published via `make grafana-publish`
- **Monitoring**: vmagent (remote-writing to VictoriaMetrics), Vector (tailing `kubernetes_logs` to VictoriaLogs), OpenTelemetry Collector, node-exporter, and kube-state-metrics (Kubernetes object-state metrics — `kube_job_*`/`kube_pod_init_container_status_*`/`kube_deployment_status_replicas_available`, Scenario 104) deployed alongside the operator via `make monitoring-deploy`
- **Test Environment**: Docker Compose with 9 services (Vault, Keycloak, MinIO, Kafka, RabbitMQ, VictoriaMetrics, Grafana, Tempo)
- **Quality gates**: build PASS, `golangci-lint` 0 issues, `govulncheck` no vulnerabilities
- **Tests**: unit (91.4% coverage), functional, integration, and e2e (881 e2e cases) all PASS; performance smoke 900 requests / 0 errors
- **Coverage**: 91.4% overall project coverage

## Performance Characteristics

Current baseline (latest perf-test cycle): authenticated API throughput is **~7 RPS per client** — dominated by bcrypt password verification on every Basic-auth request — and health endpoints sustain **p99 < 10ms**. Note that the default API rate limit is **10 requests/minute per IP** (`api-rate-limit` / `CLOUDBERRY_API_RATE_LIMIT`; set `0` to disable), so performance testing requires raising or disabling the limit.

Earlier full load-test results (2026-05-19, 287,122 total requests, zero errors):

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
| [Changelog](CHANGELOG.md) | Notable changes, fixes, and observability additions (Keep a Changelog format) |

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
