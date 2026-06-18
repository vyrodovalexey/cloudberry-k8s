# Changelog

All notable changes to the Cloudberry Kubernetes Operator are documented in this
file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

- **gpload bytes-measurement shell-quoting** — the `internal/builder` gpload
  bytes-measurement shell command (the `wc -c <file>` path) now uses shell-quoting
  (`shellQuote`, the `'\''` idiom) instead of SQL-literal quoting, fixing shell
  mis-parsing when a file path contains a single quote.
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

- **Backup timestamp characteristic** — `status.lastBackupTimestamp` (and
  `backupHistory[].timestamp`) is the **operator-assigned Job-creation timestamp**, which
  can **differ** from the `gpbackup` internal timestamp embedded in the S3 object paths. In
  the coordinator-exec model `gpbackup` runs asynchronously inside the coordinator and
  assigns its own timestamp. When restoring **by timestamp directly against S3** (outside
  the operator), the actual `gpbackup` timestamp — the one in the S3 object path — must be
  used, or `gprestore` will report a `NotFound`. This is an observed known characteristic,
  not a bug-fix.

### Verified

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
