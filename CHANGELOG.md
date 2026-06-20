# Changelog

All notable changes to the Cloudberry Kubernetes Operator are documented in this
file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Three new operator Prometheus metric families** (namespace `cloudberry`), all honest
  outcome counters:
  - `cloudberry_disk_usage_scan_total{cluster,namespace,result}` — disk-usage scan outcome,
    recorded in the admin controller's `recordDiskUsage`. `result` ∈ {`success`, `error`,
    `skipped`} (`skipped` when `gp_toolkit.gp_disk_free` is unavailable on the server
    version — never a fabricated value).
  - `cloudberry_recommendation_scan_total{cluster,namespace,result}` — storage
    recommendation scan outcome, recorded in `recordRecommendations`. `result` ∈
    {`success`, `error`, `skipped`} (`skipped` when the DB is unavailable).
  - `cloudberry_oidc_userinfo_total{result}` — OIDC userinfo fetch outcome in
    `auth/oidc.go`. `result` ∈ {`success`, `error`}.
- **Query-exporter self-observability** (`cmd/cloudberry-query-exporter`) — two new
  per-collector metric families: `cbexporter_collector_errors_total{collector}` (scrape
  error counter) and `cbexporter_collector_duration_seconds{collector}` (scrape duration),
  with `collector` ∈ {`query_activity`, `resgroup_status`, `resgroup_iostats`,
  `spill_files`, `segment_health`, `dist_txns`, `table_skew`}.
- **New OTEL spans** for finer-grained reconcile + auth + DB visibility:
  - cluster-controller sub-reconciler spans `controller.reconcileConfigMaps`,
    `controller.reconcileAdminSecret`, `controller.reconcileClusterSSHSecret`,
    `controller.reconcileServices`, `controller.reconcileCoordinator`,
    `controller.reconcileStandby`, `controller.reconcileSegments`;
  - HA/auth controller spans `controller.monitorStandby`,
    `controller.executeRebalanceViaDB`, `controller.handleStandbyActivation`,
    `controller.reconcileHBA`;
  - `auth.basic.verify` (basic-auth verification) and
    `controller.scanRecommendations.fetch` (with a bounded `rec_type` attribute);
  - DB-client `db.*` spans + `cloudberry_db_query_duration_seconds` added to
    `TerminateSession`, `ConfigureReplication`, `TerminateAllBackends`, `CancelAllQueries`,
    `LogRotate`, `TriggerRecommendationScan`, `GetQueryDetail`, `ListUserDatabases`,
    `EnsureQueryHistoryTable`, `InsertQueryHistory`, and `GetQueryHistoryDetail`.
- **Grafana dashboard panels** — `monitoring/grafana/cloudberry-operator.json` gained
  panels for the 3 new operator metrics (`cloudberry_disk_usage_scan_total`,
  `cloudberry_recommendation_scan_total`, `cloudberry_oidc_userinfo_total`);
  `monitoring/grafana/cloudberry-exporters.json` gained panels for the 2 new exporter
  metrics (`cbexporter_collector_errors_total`, `cbexporter_collector_duration_seconds`).
  The OTEL dashboard (`cloudberry-otel.json`) already covers `otelcol_*` / Tempo /
  VictoriaLogs and is unchanged.
- **Three new Prometheus metric families** (namespace `cloudberry`), all request-side
  counters complementary to the existing controller-side outcome metrics:
  - `cloudberry_api_cluster_lifecycle_requests_total{operation,result}` — cluster
    lifecycle/maintenance actions requested via the REST API, incremented at the
    `setClusterAnnotation`/`setMaintenanceAnnotation` choke points and
    `handleUpdateConfig`. `operation` ∈ {`start`, `stop`, `restart`, `reload`,
    `activate-standby`, `rebalance`, `vacuum`, `analyze`, `reindex`, `config-update`};
    `result` ∈ {`accepted`, `error`}.
  - `cloudberry_api_workload_operations_total{kind,operation,result}` —
    workload-management DDL requested via the API. `kind` ∈ {`resource_group`,
    `resource_queue`, `rule`}; `operation` ∈ {`create`, `update`, `delete`, `assign`};
    `result` ∈ {`success`, `error`}.
  - `cloudberry_pxf_sync_total{cluster,namespace,result}` — PXF sync **request**
    outcomes (separate from the honest `cloudberry_pxf_servers_changed_total`
    force-pair counter, which only fires on a real ConfigMap diff). `result` ∈
    {`success`, `error`}.
- **Recovery request-side dimension** — the recovery API endpoint now also records the
  request-side outcome on `cloudberry_recovery_operations_total` with `result` ∈
  {`requested`, `error`}, alongside the existing controller-side `started`/`completed`/
  `failed`/`noop` values.
- **15 mutating/DDL `db.Client` methods now record latency + spans** — `SetParameter`,
  `ReloadConfig`, `CreateRole`, `AlterRole`, `DropRole`, `Vacuum`, `Analyze`, `Reindex`,
  `CreateResourceGroup`, `AlterResourceGroup`, `DropResourceGroup`,
  `AssignRoleResourceGroup`, `CreateResourceQueue`, `DropResourceQueue`, and
  `MoveQueryToResourceGroup` emit `cloudberry_db_query_duration_seconds` with their
  method name as the `operation` label, plus a named `db.<Method>` OTEL span.
- **New OTEL child spans** on the AdminReconciler sub-reconcilers:
  `controller.reconcilePxf`, `controller.reconcileDataLoading`,
  `controller.reconcileStorage`, `controller.reconcileResourceGroups`, and
  `controller.ensureExporterCoreResources` (plus the 15 `db.<Method>` spans above).
- **Two new data-loading control-plane Prometheus metric families** (namespace
  `cloudberry`), both honest outcome counters incremented only at the real outcome
  (never on a no-op/skip):
  - `cloudberry_gpfdist_reconcile_total{cluster,namespace,operation,result}` —
    operator-side gpfdist provisioning reconcile outcome, incremented at the real
    Kubernetes create/update/delete outcomes in `reconcileGpfdist`/`ensureGpfdist*`.
    `operation` ∈ {`pvc`, `deployment`, `service`, `delete`}; `result` ∈ {`success`,
    `error`}.
  - `cloudberry_pxf_extension_setup_total{cluster,namespace,result}` — PXF
    client-extension setup attempt outcome (`setupPXFExtensions` DB round-trip).
    `result` ∈ {`installed` (≥1 extension created), `absent` (DB reachable but 0
    installed — e.g. `pxf` image-blocked / DB in recovery; **not** a failure), `error`
    (hard connectivity/setup failure)}.
- **Four more OTEL spans** — `controller.reconcileDataLoadingJobs`,
  `controller.reconcileGpfdist`, and `controller.setupPXFExtensions` (the admin/dataload
  controller sub-reconcilers), plus `util.patchStatefulSetRestartTrigger` (the shared
  StatefulSet rolling-restart primitive).
- **New data-loader-role-setup Prometheus metric family** (namespace `cloudberry`):
  - `cloudberry_dataloader_role_setup_total{cluster,namespace,result}` — the outcome of
    the dedicated least-privilege data-loader role setup (`EnsureDataLoaderRole`, security
    control SE.6) in `internal/controller/dataload_controller.go`, the sibling of
    `cloudberry_pxf_extension_setup_total`. `result` ∈ {`success`, `error`}. Previously
    this operation only logged a `Warn` on failure with no metric.
- **New exporter-role-setup Prometheus metric family** (namespace `cloudberry`):
  - `cloudberry_exporter_role_setup_total{cluster,namespace,result}` — the outcome of the
    monitoring exporter role provisioning (`setupExporterRole`'s DB round-trip) in
    `internal/controller/admin_controller.go`, the third sibling of
    `cloudberry_pxf_extension_setup_total` and `cloudberry_dataloader_role_setup_total`.
    `result` ∈ {`success`, `error`}; recorder method `RecordExporterRoleSetup`. Previously
    `setupExporterRole` only logged a `Warn` on failure with no metric; now all three
    best-effort role-setup DB round-trips are uniformly observable. Confirmed live
    (`result="success"`) after a cluster reconcile.
- **22 read-path `db.Client` methods now record latency + spans** — `GetSegmentConfiguration`,
  `GetMirrorSyncStatus`, `GetReplicationLag`, `GetActiveQueryCount`, `GetMaxConnections`,
  `GetResourceGroupUsage`, `ListSessionsWithResourceGroup`, `ListSessions`, `GetDiskUsage`,
  `GetStorageDiskUsage`, `ListResourceGroups`, `ListResourceQueues`, `CancelQuery`,
  `TriggerFTSProbe`, `ShowParameter`, `GetBloatRecommendations`, `GetSkewRecommendations`,
  `GetAgeRecommendations`, `GetIndexBloatRecommendations`, `GetTableDetails`,
  `GetUsageReport`, and `GetRedistributionProgress` are now wrapped with the
  `startOperation` helper, so each emits `cloudberry_db_query_duration_seconds` with its
  method name as the `operation` label plus a named `db.<Method>` OTEL span — completing
  the read-side symmetry with the 15 mutating/DDL methods that already did this.
- **More OTEL spans** — the 22 `db.<Method>` read spans above, plus
  `controller.reconcileCoreResources` and `controller.reconcileStatefulSets` (cluster
  controller sub-operations via `startControllerSpan`), `vault.watch.check`
  (`SecretWatcher.checkForChanges` — span-only, error status on a read failure; the vault
  read/error metric is already emitted by `ReadSecret`, so there is no double-count), and
  `certmanager.issueVaultPKICert` (wraps the Vault PKI `WriteSecretWithResponse` call,
  error status on failure).
- **CRD regeneration** — the committed CRD
  `deploy/helm/cloudberry-operator/crds/avsoft.io_cloudberryclusters.yaml` (the only CRD
  copy) was regenerated via `make manifests` to remove drift vs. the current `api/` types;
  the shipped schema now matches `api/v1alpha1` and `make manifests` is now idempotent.
- **Grafana dashboards** — the operator dashboard
  (`monitoring/grafana/cloudberry-operator.json`) gained **8 panels** covering the
  three request-side metric families plus a **DB-query-duration-by-operation p95** panel
  built on `cloudberry_db_query_duration_seconds_bucket`, and a new **"gpfdist, PXF
  Extension, Data-Loader & Exporter Role Setup"** row of panels (rate timeseries + 1h stat
  + error stat for each of `cloudberry_gpfdist_reconcile_total` and
  `cloudberry_pxf_extension_setup_total`, **3 panels** for
  `cloudberry_dataloader_role_setup_total`, and **3 new panels** (ids 308–310: "Exporter
  Role Setup Rate by Result" timeseries, "Exporter Role Setups (1h)" stat, "Exporter Role
  Errors (1h)" stat) for the new `cloudberry_exporter_role_setup_total` metric, all
  following the `cloudberry_dataloader_role_setup_total` panel pattern). Dashboards remain
  at 100 % `cloudberry_*` metric coverage. The OTEL dashboard
  (`monitoring/grafana/cloudberry-otel.json`) is unchanged — it covers Tempo traces,
  otel-collector health (`otelcol_*`), and VictoriaLogs.

### Fixed

- **Backup timestamp capture (restore-by-timestamp fix)** — the operator now captures
  `gpbackup`'s **real** emitted `Backup Timestamp = <14-digit>` from the backup Job
  (surfaced via `/dev/termination-log` and the new `avsoft.io/backup-timestamp`
  annotation) and records **that** as `status.lastBackupTimestamp`, instead of a
  pre-generated `time.Now()` value. Because `gpbackup` runs asynchronously inside the
  coordinator and assigns its own timestamp, the pre-generated value could drift from
  `gpbackup`'s real S3 object prefix, so restore-by-timestamp directly against S3
  previously failed with a `404`/`NotFound`; the recorded timestamp now matches the real
  S3 prefix and restore-by-timestamp resolves correctly. **Backward compatible** — falls
  back to the prior behaviour when the annotation/marker is absent. (Supersedes the
  earlier "Backup timestamp characteristic" note below, which described the pre-fix
  behaviour.)
- **ConfigMap annotation update now MERGES** — the ConfigMap annotation update path
  preserves third-party annotations (new `mergeAnnotations` helper) instead of
  overwriting them.
- **PXF sidecar StartupProbe** — the PXF sidecar now gets a StartupProbe (`HTTPGet
  /actuator/health:5888`, `periodSeconds=5`, `failureThreshold=24` → a ~120 s startup
  budget) and a more tolerant liveness `timeoutSeconds`, so the slow ~50 s Spring Boot
  cold start no longer trips liveness into `CrashLoopBackOff`.
- **Query-exporter metric cardinality** — the unbounded `usename` label was **removed**
  from `cbexporter_queries_total` / `cbexporter_queries_slow_total` to bound metric
  cardinality.
- **gpload bytes-measurement shell-quoting** — the `internal/builder` gpload
  bytes-measurement shell command (the `wc -c <file>` path) now uses shell-quoting
  (`shellQuote`, the `'\''` idiom) instead of SQL-literal quoting, fixing shell
  mis-parsing when a file path contains a single quote.
- **Data-loading HC.1 PXF readiness probe** — the pre-load health-check init
  container (`buildDataLoadHealthCheckScript` in `internal/builder/dataload_builder.go`)
  previously verified PXF readiness by calling a non-existent `pxf_version()` SQL
  function (PXF 2.1 ships no such function), so the `dataload-healthcheck` init
  container **ALWAYS failed** (`Init:Error`) for PXF s3 loads even with PXF correctly
  installed. The HC.1 probe now verifies a real PXF function exists via
  `SELECT 1 FROM pg_proc WHERE proname = 'pxf_read'`, proving PXF is actually usable.
- **Data-loading HC.3 external-source connectivity probe** — the HC.3 probe
  previously used `curl -fsS --head`, which fails on HTTP 400/403; because MinIO and
  many S3-compatible stores answer an unauthenticated HEAD/GET with 400/403, a
  reachable endpoint was wrongly reported "unreachable". The probe now captures the
  HTTP status code **without** `-f` and treats ANY HTTP response (1xx–5xx, i.e. the
  server answered) as reachable; only a true connection failure/timeout fails the
  check. Both HC.1/HC.3 fixes were verified live — the dataload Job now passes
  natively and completes (`DATALOAD_ROWS`, `status.dataLoading.jobs[].lastStatus=Succeeded`,
  `cloudberry_data_loading_job_status=2`) with no workarounds.
- **DSN credential URL-encoding** — connection-string credentials are now escaped via
  `url.UserPassword` (userinfo) and `url.Values` (query), preventing connection-string
  corruption / DSN parameter injection when credentials contain metacharacters
  (`@ / ? # & = :`).
- **`SetParameter` secret redaction** — the raw GUC value is no longer logged at `Info`
  or embedded in the returned error (moved to `Debug`), preventing secret leakage into
  logs (VictoriaLogs).
- **Cron day-of-week 7 = Sunday** — the cron parser now accepts day-of-week `7` as
  Sunday for Kubernetes/standard-cron parity; `"0 0 * * 7"` is equivalent to
  `"0 0 * * 0"`.
- **Data-loading status persistence** — the data-loading reconcile's intermediate
  `Status().Patch` (in `patchDataLoadingStatus`, used by `cleanupDataLoading`/
  `refreshDataLoadingStatusOnSteadyState`) round-tripped the cluster object and
  clobbered in-memory status (conditions, backup, workload) set by earlier
  sub-reconcilers before the single final `patchStatus`. Fixed by snapshotting
  `cluster.Status` before the intermediate patch and restoring it after, so the final
  `patchStatus` persists all sub-reconciler status atomically.
- **Migrate span error** — the migrate API span now records the real underlying error
  instead of a fabricated placeholder string.
- **`cancel`/`terminate` request-body bound** — the cancel/terminate API path now bounds
  the optional request body (`limitBody`) for parity with the other handlers.
- **X-Forwarded-For trim** — the `X-Forwarded-For` hop is now whitespace-trimmed for the
  rate-limit bucket key.

### Removed

- **Dead `GetClusterState` method and `ClusterState` type** — removed from the `db.Client`
  interface/implementation. They had no production caller and the method swallowed three
  sub-query errors (a latent false-`MirroringInSync` hazard). No behavioral change to
  anything in production.

### Notes

- **Backup timestamp characteristic (now resolved — see _Fixed_ above).** Previously
  `status.lastBackupTimestamp` (and `backupHistory[].timestamp`) was the
  **operator-assigned Job-creation timestamp**, which could **differ** from the `gpbackup`
  internal timestamp embedded in the S3 object paths (in the coordinator-exec model
  `gpbackup` runs asynchronously inside the coordinator and assigns its own timestamp), so
  restoring **by timestamp directly against S3** required the actual `gpbackup` timestamp or
  `gprestore` would report a `NotFound`. The current release captures `gpbackup`'s real
  emitted timestamp from the Job (via `/dev/termination-log` + the
  `avsoft.io/backup-timestamp` annotation) and records that, so `status.lastBackupTimestamp`
  now matches the S3 object path and restore-by-timestamp resolves the correct prefix.

### Verified

- **2026-06-20 — verification re-run (no new code changes; codebase already clean).**
  Full end-to-end acceptance against the live test environment confirming the
  prior-refactor features remain correct and documented. Operator deployed to local k8s
  (`cloudberry-test` ns) with Vault-PKI webhook certs (issuer `CN=Test Root CA`), Vault
  kubernetes-auth, Keycloak OIDC, and telemetry (`otelcol_receiver_accepted_spans > 0`).
  Cluster deployed with HA coordinator + standby, segment mirroring (`InSync`), Vault-PKI
  cluster TLS, `postgres-exporter` on coordinator/standby/every segment + mirror,
  `cloudberry-query-exporter` on the coordinator, and PXF sidecars. 144 MB `mydb`;
  operator-tracked `gpbackup` → S3 (MinIO via Vault creds) → `gprestore` verified
  row-for-row, **with the backup-timestamp fix confirmed end-to-end** (real `gpbackup`
  timestamp `20260620190719` captured into `status.lastBackupTimestamp` via the
  `avsoft.io/backup-timestamp` annotation; restore-by-timestamp resolved with no `404`).
  7 external writable + readable tables (`s3`/`hdfs` × text/parquet/avro + SequenceFile)
  each round-tripped 175k rows (~10 MB); the declarative `s3-csv-load` dataload Job
  succeeds **natively** (HC.1 `pg_proc pxf_read` + HC.3 status-code connectivity pass,
  `lastStatus=Succeeded`, `cloudberry_data_loading_job_status=2`). Monitoring stack in
  k8s — metrics in VictoriaMetrics, logs in VictoriaLogs, 4 Grafana dashboards (incl.
  OTEL) published at 100 % `cloudberry_*` metric coverage. Performance test: 347,869
  requests, 0 % errors, all SLOs met, and all 5 new observability metrics
  (`cloudberry_disk_usage_scan_total`, `cloudberry_recommendation_scan_total`,
  `cloudberry_oidc_userinfo_total`, `cbexporter_collector_errors_total`,
  `cbexporter_collector_duration_seconds`) reacted under load.
- End-to-end in the live test environment (post-refactor acceptance run): operator
  deployed to local k8s (`cloudberry-test` ns) with Vault-PKI webhook certs, Vault
  kubernetes-auth, and Keycloak OIDC; cluster deployed with HA coordinator + standby,
  segment mirroring (InSync), Vault-PKI cluster TLS, `postgres-exporter` on
  coordinator/standby/every segment + mirror, `cloudberry-query-exporter` on the
  coordinator, and PXF sidecars on segment primaries. 144 MB of data loaded into `mydb`;
  operator-tracked `gpbackup` → S3 (MinIO via Vault creds) → `gprestore` verified
  row-for-row (real `gpbackup` timestamp captured + restore-by-timestamp resolved). 7
  external writable + readable tables (`s3`:text/parquet/avro,
  `hdfs`:text/parquet/avro/SequenceFile) each ~10 MB round-tripped 175k rows. Monitoring
  stack (vmagent/vector/otel-collector) deployed to k8s — metrics in VictoriaMetrics, logs
  in VictoriaLogs, dashboards published to Grafana (including the new operator/exporter
  panels). Performance test: read endpoints 0 % errors, p99 within SLO, and the new
  metrics reacted under load.
- End-to-end in the live test environment: operator deployed with Vault-PKI webhook
  certs (issuer = Vault CA) + k8s auth + Keycloak OIDC; cluster deployed with HA
  coordinator + standby, group segment mirroring (InSync), Vault-PKI cluster TLS, PXF
  sidecar + postgres-exporter on every segment + mirror + cloudberry-query-exporter;
  456 MB `mydb` generated; `gpbackup` → MinIO (Vault S3 creds) → `gprestore` round-trip
  verified (row counts match); 7 PXF external writable tables (s3/hdfs × text/parquet/avro
  + hdfs:SequenceFile) — **6/7 full round-trip**; `hdfs:SequenceFile` write succeeds but
  read requires a custom Java `Writable` class on the PXF classpath (documented PXF 2.1.0
  limitation).
- End-to-end in the live test environment (Phase 3 refactor): operator deployed with
  Vault-PKI webhook certs + k8s auth + Keycloak OIDC; acceptance-test cluster Running with
  HA coordinator + standby, group segment mirroring (InSync), Vault-PKI cluster TLS, PXF
  sidecar + postgres-exporter on every segment + mirror + coordinator +
  cloudberry-query-exporter; 112 MB `mydb` generated; `gpbackup` → MinIO (Vault S3 creds) →
  `gprestore` round-trip verified (row counts match); 6 PXF external writable tables
  (s3/hdfs × text/parquet/avro) full round-trip at 150k rows each; the new metrics/spans
  (`cloudberry_dataloader_role_setup_total`, the 22 `db.<Method>` read spans +
  `cloudberry_db_query_duration_seconds`, and the new controller/vault/certmanager spans)
  confirmed populated in VictoriaMetrics under a ~500k-request perf test (0 % error rate);
  all dashboards published with 100 % `cloudberry_*` metric coverage.
- End-to-end in the live test environment (Phase 4 / iteration 2): operator redeployed with
  Vault-PKI webhook certs + k8s auth + Keycloak OIDC + telemetry (traces to
  otel-collector → Tempo: `Reconcile`/`vault.authenticate`/`operator.setupWebhookCerts`
  spans); acceptance-test cluster Running with HA coordinator + standby, group segment
  mirroring (InSync), Vault-PKI cluster TLS, PXF sidecar + postgres-exporter on every
  segment + mirror + coordinator + cloudberry-query-exporter; the new
  `cloudberry_exporter_role_setup_total` confirmed live (`result="success"`); 203 MB `mydb`
  generated; operator-tracked `gpbackup` → MinIO (Vault S3 creds) → `gprestore` round-trip
  verified (860K rows, row counts match, `status.lastBackupStatus=Success`); 6 PXF external
  writable tables (s3/hdfs × text/parquet/avro) full round-trip at 150k rows each with
  checksum-verified integrity; dashboards published at 100 % metric coverage; all
  unit/functional/integration/e2e suites green; lint 0, vuln 0.
