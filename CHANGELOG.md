# Changelog

All notable changes to the Cloudberry Kubernetes Operator are documented in this
file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

> **Behavior change — backup-schedule cron day-field semantics (2026-07-12).**
> The operator's cron parser (`internal/cron`, used for backup-schedule
> next-run computation and webhook schedule validation) now follows the
> Vixie-cron **star rule** implemented by robfig/cron — the engine behind
> Kubernetes CronJobs: a day-of-month / day-of-week field is *unrestricted*
> only when its raw text starts with `*` (`*` or `*/n`). An explicit full
> range such as `1-31` or `0-6` is now treated as **restricted**, which flips
> the day matching to **OR** semantics between the two day fields. A schedule
> like `0 0 1-31 * 1` therefore now matches **every day** (day-of-month 1–31
> OR Monday) instead of Mondays only — matching what the Kubernetes CronJob
> controller actually executes. Previously such explicit full ranges were
> treated as unrestricted (AND semantics), so the operator's `nextScheduleTime`
> predictions could disagree with the real firing times. Review any schedule
> that spells out a full day range instead of `*`.

### Added

- **Four new Prometheus metric families** (namespace `cloudberry`) covering
  certificate-rotation health and Vault secret-watch freshness:
  - `cloudberry_webhook_ca_bundle_injection_total{result}` — webhook CA-bundle
    injection attempts (startup **and** post-rotation re-injection);
    `result` ∈ {`success`, `error`}. Recorded once per injection attempt
    (after retries) at both call sites.
  - `cloudberry_cert_rotation_check_errors_total{component}` — failed
    `NeedsRotation` checks in the background rotation loop; `component` is a
    bounded enum (currently `webhook`). Previously these failures were
    log-only.
  - `cloudberry_vault_watch_last_success_timestamp{path}` — Unix timestamp of
    the last successful poll of a watched Vault secret path; staleness is
    derived in PromQL as `time() - <gauge>`. A successful poll of an empty
    secret still counts as a success. `path` is bounded (the config-derived
    watched Vault paths).
  - `cloudberry_vault_watch_errors_total{path}` — failed polls per watched
    Vault secret path (the paired error counter; `ReadSecret` still meters
    the underlying vault operation, so there is no double count on
    `cloudberry_vault_operations_total`).
  All four use the shared result-label constants (`metrics.ResultSuccess` /
  `metrics.ResultError`), so the outcome vocabulary cannot drift between
  metric families.
- **Query-exporter `--log-level` flag + `LOG_LEVEL` env**
  (`cmd/cloudberry-query-exporter`) — the exporter's JSON logger level is now
  configurable (`debug`/`info`/`warn`/`error`, case-insensitive, default
  `info`), making `Debug` diagnostics reachable in the field. The environment
  variable takes precedence over the flag, matching the project-wide
  configuration precedence.
- **Query-exporter optional OTLP tracing** — new `--telemetry-enabled` /
  `--otlp-endpoint` flags with `TELEMETRY_ENABLED` / `OTLP_ENDPOINT`
  environment overrides (ENV > flag). **Disabled by default** — behavior is
  unchanged unless explicitly enabled. When enabled the exporter exports
  spans under `service.name=cloudberry-query-exporter` (gRPC OTLP): one
  `exporter.collect` span per collection cycle (error status when the cycle
  ends without a live database connection) and one `exporter.history.cleanup`
  span per retention-cleanup tick; tracer shutdown is bounded (5 s) and joined
  on all exit paths. No statement text is attached to spans (PII-safe).
- **New operator span `operator.certRotationCheck`** — one span per rotation
  tick carrying `needs_rotation`/`rotated` attributes and error status on
  check/rotate/inject failures, so a mid-run rotation months after startup
  stays diagnosable in the tracing backend.
- **Grafana: 8 new operator-dashboard panels** (Security & Lifecycle section
  of `monitoring/grafana/cloudberry-operator.json`, panel ids 326–333): "CA
  Bundle Injection Rate" / "CA Bundle Injection Errors (1h)", "Cert Rotation
  Check Errors Rate" / "Cert Rotation Check Errors (1h)", "Vault Watch
  Staleness" (`time() - cloudberry_vault_watch_last_success_timestamp`) /
  "Vault Watch Last Success", and "Vault Watch Errors Rate" / "Vault Watch
  Errors (1h)". The OTEL dashboard (`cloudberry-otel.json`) description now
  lists the new spans `operator.certRotationCheck`, `exporter.collect`, and
  `exporter.history.cleanup`.
- **CI coverage gate** — `.github/workflows/ci.yml` now fails the unit-test
  job when total statement coverage of `./internal/... ./cmd/... ./api/...`
  drops below **90 %**. Current total: **94.7 %**, with no package below 90 %.
- **CODEOWNERS for the PXF/gpfdist Dockerfiles** —
  `Dockerfile.cloudberry-pxf`, `Dockerfile.cloudberry-official-pxf`, and
  `Dockerfile.cloudberry-gpfdist` are now owned by the devops group like the
  other image Dockerfiles.
- **Performance-test report (2026-07-12)** —
  `test/performance/results/2026-07-12-perftest-report.md`: all SLOs pass and
  there is no regression vs the 2026-07-10 baseline after the cert-rotation
  and metrics changes (health p99 ~10 ms at 100 RPS with 0 errors; rate
  limiter enforcement unchanged).
- **Three new Prometheus metric families** making previously-silent failure paths
  observable:
  - `cloudberry_steady_state_refresh_errors_total{cluster,namespace,component}` —
    swallowed (non-fatal) errors on the admin controller's steady-state refresh paths,
    `component` ∈ {`backup`, `dataloading`, `storage`}. Refresh errors intentionally do
    not change reconcile result semantics; the counter makes the silent degradation
    alertable. The previously-swallowed cluster refetch error before status patches is
    now also logged (`refreshPhaseFromServer`).
  - `cloudberry_api_rate_limit_entries` — gauge of live per-client rate-limiter entries,
    sampled on every Prometheus scrape (each API server registers a provider on start and
    unregisters it on `Close`; multiple servers sharing one recorder are summed into the
    single process-wide gauge).
  - `cloudberry_query_exporter_history_errors_total{stage}` — query-exporter history
    pipeline failures, `stage` ∈ {`snapshot`, `insert`, `explain`}; incremented only on a
    real failure, never on skips or empty results.
- **`internal/dbschema` shared DDL package** — the `cloudberry_query_history` DDL now
  lives in a const-only leaf package consumed by both the operator db client
  (`internal/db/query_history.go`) and the standalone query exporter
  (`cmd/cloudberry-query-exporter/history.go`), replacing two hand-duplicated copies.
  Drift tests on both sides pin the consumers to the shared constant.
- **Grafana dashboard panels for the new metric families (8 panels)** — the operator
  dashboard (`monitoring/grafana/cloudberry-operator.json`) gained "Steady-State Refresh
  Errors" (rate timeseries + total stat) and "API Rate Limit Entries" (timeseries +
  current stat); the exporters dashboard (`monitoring/grafana/cloudberry-exporters.json`)
  gained "Query History Pipeline Errors" / "History Errors Total" and "Collector Errors
  Rate" / "Collector Scrape Duration". Dashboards cover all 168 project metric families
  (168/168).
- **CI on branch pushes** — `.github/workflows/ci.yml` now also triggers on pushes to
  `main` and `init` (verification jobs only: lint, govulncheck, unit/service tests, and
  the SonarCloud scan). Release/publish jobs (`build-release`, `docker-build-push*`,
  `trivy-scan`, `helm-package`) remain tag-gated (`v*`) and never fire on a branch push;
  PR-only jobs stay gated on `pull_request`.
- **Acceptance sample CR pins the TCP interconnect** —
  `deploy/helm/cloudberry-operator/config/samples/acceptance-cluster-deploy.yaml` now
  sets `config.parameters.gp_interconnect_type: tcp` with an in-file rationale: the
  default UDP interconnect (`udpifc`) is unreliable under kind / Docker Desktop
  networking (the VM NAT path drops or reorders UDP between pod sandboxes), stalling
  Motion traffic even with the NetworkPolicy admitting it. Note the GUC is applied from
  `postgresql.conf` at server start — changing it on a running cluster needs a pod
  restart to take effect.
- **Performance-test assets (2026-07-10)** — `test/performance/loads/`
  (health-baseline, api-read-throughput, rate-limiter-knee),
  `test/performance/ammo/api-read-acceptance.txt`, and the full report
  `test/performance/results/2026-07-10-perftest-report.md` (summarized in
  `test/performance/README.md`).
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

### Changed

- **Backup-schedule cron semantics: explicit full day ranges are now
  RESTRICTED (Vixie/robfig star-rule parity).** See the prominent
  **Behavior change** note at the top of this section — `internal/cron` now
  decides day-of-month/day-of-week restriction from the raw field text
  (`*`/`*/n` = unrestricted), so `"0 0 1-31 * 1"` fires every day (OR
  semantics) exactly as the Kubernetes CronJob controller executes it, and
  the operator's `nextScheduleTime` and webhook validation agree with
  reality.
- **Certificate rotation loop hardened** (`cmd/operator`):
  - **Leader-gated** — the loop blocks on `mgr.Elected()` before doing any
    work, so in a multi-replica deployment only the leader writes cert
    Secrets and webhook configurations. With leader election disabled,
    controller-runtime closes the elected channel as soon as the manager
    starts, preserving single-replica behavior.
  - **Jittered interval** — each tick waits the base 12 h interval plus a
    random jitter of up to 10 %, so multiple replicas de-synchronize instead
    of probing the cert Secret in lockstep.
  - **Pending-bundle retry on every tick** and per-tick
    `operator.certRotationCheck` span + outcome metrics (see *Fixed* /
    *Added* above). Failed rotation checks now also increment
    `cloudberry_cert_rotation_check_errors_total{component="webhook"}`.
- **Self-signed webhook certificates: the CA is now reused across rotations
  (leaf-only renewal).** The CA private key is persisted alongside the CA
  certificate in the webhook cert Secret (new `ca.key` key), and a rotation
  re-issues **only the serving (leaf) certificate** from the persisted CA as
  long as the CA parses, is the operator's own self-signed CA, is not past
  its own 2/3-lifetime rotation threshold, and its remaining validity covers
  the full validity of the new leaf (a leaf never outlives its issuer). With
  a reused CA, `ca.crt` — and therefore the injected webhook CA bundle —
  stays byte-identical across rotations. Legacy Secrets without `ca.key`
  keep working via the full-regeneration fallback (a missing `ca.key` is
  deliberately **not** a rotation trigger); the regeneration path starts
  persisting `ca.key`. Switching to `vault-pki` drops a stale `ca.key` from
  the Secret.
- **Union CA bundle during a CA cutover.** When a rotation replaces the CA
  (any source, including a Vault PKI issuing-CA change) and the previous CA
  is still time-valid, the bundle injected into the webhook configurations is
  the **union of the new and old CA**: during the kubelet Secret-propagation
  window the webhook pod may still serve the old leaf while the API server
  already trusts only the freshly injected bundle — including the old CA
  keeps admission working through the cutover race. The CA-reuse path
  (unchanged CA) returns the single CA as before.
- **Self-signed certificate issuance now rejects an empty `dnsNames` slice**
  with a clear error instead of panicking on the Common-Name access.
- **Disabled Vault client returns a typed error.** With Vault integration
  disabled, the no-op client's `ReadSecret` now returns
  `vault.ErrVaultDisabled` (match with `errors.Is`) instead of `(nil, nil)`,
  so an unguarded caller can no longer mistake the result for an
  existing-but-empty secret. Callers should keep guarding with
  `IsEnabled()`; guarded call sites are unaffected.
- **Vault `SecretWatcher` staleness observability.** `NewSecretWatcher`
  accepts an optional metrics recorder (variadic, matching the
  `certmanager.New` pattern) and emits the two per-path watch metrics above.
  The Vault client's operation recorder is now stored atomically, so
  `SetRecorder` is safe against the background token lifetime watcher
  reading it concurrently.
- **`db.Client` split into 17 capability interfaces** (`internal/db/interfaces.go`):
  `ConnectionOps`, `TopologyOps`, `ConfigOps`, `SessionOps`, `RoleOps`,
  `MaintenanceOps`, `MonitoringOps`, `WorkloadOps`, `BackupOps`,
  `DataLoadingOps`, `StorageOps`, `RecommendationOps`, `HAOps`, `ScaleOps`,
  `PXFOps`, `QueryHistoryStore`, and `ExporterSetupOps`. `Client` is now the
  composition (embedding) of all seventeen — the method set is unchanged, so
  the split is **source-compatible**; consumers can depend on just the
  capability they use (the idle daemon already consumes the narrow
  interfaces).
- **Query-exporter graceful shutdown joins the collect loop on every exit
  path** — the loop join (cancel + wait) is registered as a deferred closure
  immediately after the loop starts, so the HTTP-server error path joins the
  loop too, and the loop remains the single owner of the (possibly
  reconnected) database connection.
- **postgres-exporter `cloudberry_table_stats` is now catalog-only** — the custom
  per-table query reads `pg_class` + `pg_namespace` instead of `pg_stat_user_tables`. On
  Cloudberry `pg_stat_user_tables` is a **distributed** view (and the `dbsize` functions
  dispatch from the coordinator too), so every scrape previously triggered cluster-wide
  dispatch over the Motion/Interconnect layer — per-scrape cluster load, and a wedged
  sidecar whenever the interconnect was degraded. The catalog-only query reports
  **estimates**: `n_live_tup` from `GREATEST(reltuples, 0)` (clamping the PostgreSQL 14
  "never analyzed" `-1` sentinel) and a new `table_size_bytes` from
  `relpages * block_size`, both refreshed by `ANALYZE`/`VACUUM` and labelled "Estimated"
  in the HELP text. Tables **and materialized views** are covered
  (`relkind IN ('r','m')`), temp relations are excluded, and the per-table DML/scan
  counters (`seq_scan`, `n_tup_ins`, `n_dead_tup`, …) were **removed** — a sidecar
  exporter must never trigger cluster-wide dispatch per scrape. The Grafana
  exporters-dashboard table panels were reworked accordingly ("Top Tables by Estimated
  Size/Rows", "Estimated Size by Schema").
- **Backup lifecycle endpoints share the backup-enabled gate** —
  `DELETE /clusters/{name}/backups/{timestamp}` and
  `POST /clusters/{name}/backups/{timestamp}/restore` now return
  `400 BACKUP_NOT_ENABLED` when `spec.backup` is absent or disabled, consistent with the
  existing gate on `POST /backups` (shared `requireBackupEnabled` helper) — no cleanup or
  restore Job is ever built for a cluster whose backup destination/credentials are
  unconfigured. The restore reject is a client error, not a restore outcome:
  `cloudberry_restore_total{result="failed"}` is intentionally **not** incremented.
- **Optional JSON request bodies reject trailing data** — `decodeOptionalJSON` now
  requires EOF after the first JSON value, so trailing garbage (`{} {}`, `{}[]`,
  `{}garbage`) is rejected with `400` instead of silently ignored (it can mask
  truncated/concatenated payloads). `DisallowUnknownFields` is deliberately **deferred**:
  rejecting unknown fields would break lenient clients during version skew (e.g. a newer
  ctl sending future fields to an older operator) — revisit with an API versioning story.
- **Query-exporter history statements are deadline-bounded** — the history INSERT is
  bounded at 5 s (aligned with the collector query timeout) and the hourly retention
  DELETE at 60 s. Both target the **distributed** `cloudberry_query_history` table, so a
  degraded interconnect previously wedged the shared connection (and the whole collect
  loop) until `gp_interconnect_setup_timeout`; failures now fail fast and surface on
  `cloudberry_query_exporter_history_errors_total` instead, and an aborted cleanup simply
  retries on the next tick. The `resource_group` column is now inserted as an explicit
  empty literal — the session snapshot never captured it, and the always-empty struct
  field previously pretended otherwise (populating it from `rsgname` is a recorded
  follow-up).

### Fixed

- **HIGH (H-1): the webhook CA bundle is re-injected after every certificate
  rotation.** Previously the CA bundle was injected into the
  `ValidatingWebhookConfiguration`/`MutatingWebhookConfiguration` **only at
  operator startup**. An in-place rotation re-issues the serving certificate —
  and, for the self-signed source, could mint a fresh CA — so after the first
  rotation (typically ~2/3 of the cert lifetime, months into an operator's
  run) the API server rejected every admission call with `x509: certificate
  signed by unknown authority` until the operator was restarted. The rotation
  loop now captures the CA bundle returned by every successful rotation and
  re-injects it with the shared retry budget (5 retries, 1 s–30 s exponential
  backoff, 10 % jitter); when the retry budget is exhausted the bundle is kept
  as **pending** and re-attempted on every subsequent tick (once the Secret is
  rotated `NeedsRotation` reports false, so without this retry a transient
  API-server outage would strand the rotated CA forever). Both call sites
  (startup and rotation) record the outcome on the new
  `cloudberry_webhook_ca_bundle_injection_total{result}` counter.
- **`SetParameter` scope validation — a typo could silently escalate to
  `ALTER SYSTEM`.** `db.Client.SetParameter` previously routed any
  unrecognized `ParameterScope.Level` (e.g. `"databsae"`) through the default
  branch, executing a **cluster-wide** `ALTER SYSTEM` instead of the intended
  scoped statement. The scope is now validated before any SQL is built:
  `Level` must be `""`/`ScopeLevelCluster` (`"cluster"`),
  `ScopeLevelDatabase` (`"database"`), or `ScopeLevelRole` (`"role"`) — exact,
  case-sensitive match — and the database/role levels **require** a non-empty
  `Target`. Invalid scopes are rejected with the new sentinel
  `db.ErrInvalidParameterScope` (match with `errors.Is`); SQL for valid
  scopes is byte-identical to before.
- **Backup Job scripts: base64 decode failures now abort loudly (SC2155).**
  The generated coordinator-exec script exported its connection env as
  `export VAR=$(printf '%s' "$N" | base64 -d)`, which masks the pipeline's
  exit status under `set -euo pipefail` — a corrupt argument decoded to an
  **empty** value (e.g. empty `PGPASSWORD`) and the tool proceeded to a
  confusing downstream auth failure. Declaration and export are now split
  (`VAR=$(…); export VAR`), so a decode failure aborts the Job step with the
  real error.
- **`GetQueryDetail` no longer discards row errors silently** — lock/table
  collection keeps its best-effort semantics (partial `locks`/`tablesAccessed`
  still returned, never a hard failure), but query errors, per-row scan errors,
  and `rows.Err()` are now logged at Debug instead of being dropped.
- **CRITICAL: the cluster NetworkPolicy now admits the MPP interconnect** — the SE.5
  `<cluster>-pxf` NetworkPolicy (present whenever the PXF sidecar is enabled) makes the
  segment pods default-deny with only fixed service ports allowed, which silently dropped
  the dynamically-bound Motion/Interconnect listeners **between cluster pods**: any
  distributed query (JOIN, GROUP BY, multi-segment INSERT) hung until
  `gp_interconnect_setup_timeout`, and `gpbackup` hung at its first coordinator→segment
  SSH dispatch. The policy now carries a second, From-scoped ingress rule admitting —
  **only from same-namespace pods carrying this cluster's label** — TCP `1025–5887` +
  `5889–65535` (the range is split so cross-pod PXF `:5888` stays sealed), UDP
  `1025–65535` (single full range; PXF has no UDP listener), and TCP `22` for the MPP
  toolchain's coordinator→segment SSH (`gpbackup`/`gprestore`/`gpexpand`). Verified live:
  distributed queries and backup/restore complete with the policy applied.
- **HIGH: cluster deletion is handled before the action/lifecycle/generation gates** —
  `Reconcile` now checks `DeletionTimestamp` first. Previously a Terminating cluster in
  phase `Stopped` returned without removing the finalizer (stuck Terminating forever),
  and `Restricted`/`Maintenance` clusters requeued endlessly without ever reaching
  `handleDeletion`. Deletion always wins; clusters now delete correctly from every
  lifecycle phase.
- **Steady-state readiness refresh** — `status.segmentsReady` (and
  `coordinatorReady`/`standbyReady`) were frozen at their last-reconciled values while
  the spec generation was unchanged, so a segment restart in the Running steady state
  left a stale count (observed as a permanent 1/2 on a fully healthy cluster).
  Generation-unchanged reconciles now re-read the live StatefulSets and persist the
  counters when they drift (explicit MergePatch so `omitempty` zero values are not
  dropped).
- **Admin-password rotation nil-map guard** — rotating the admin password against a
  Secret persisted with no `data` no longer panics (`existing.Data` is initialized when
  nil, mirroring the AlreadyExists-race path in `createAdminPasswordSecret`).
- **Query-exporter shutdown joins the collect loop** — `run()` now cancels and **waits**
  for the collect loop before declaring the exporter stopped, and the loop is the single
  owner of the (possibly reconnected) database connection, closing whatever connection is
  current exactly once on exit. Previously `run()` closed the original connection
  pointer: a reconnected conn leaked, and shutdown could double-close a conn the loop had
  already closed.
- **Idle-daemon generation handover** — `Start()` now waits for the previous scan-loop
  generation's done channel to drain before spawning the next one, and each loop closes
  the done channel of **its own** generation (handed into the goroutine). Rapid
  Stop/Start cycles can no longer run two scan loops concurrently (racing
  `consecutiveFails` and the DB-client swap), double-close a done channel, or leave
  `Stop()` hanging.
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

### Security

- **The self-signed CA private key is persisted in the webhook cert Secret
  (`ca.key`).** Rationale: the same namespace-scoped Secret already holds the
  server private key (`tls.key`), so the blast radius of a Secret compromise
  is unchanged — and persisting the CA key is what enables leaf-only renewals
  that keep the injected CA bundle stable across rotations (part of the H-1
  hardening). The key is stored only for the self-signed source; the
  vault-pki issuing CA key never leaves Vault.
- **`io_limit` DDL injection hardening (defense in depth)** — the free-form
  `workload.resourceGroups[].ioLimits[].tablespace` CRD value is embedded in the rendered
  `ALTER RESOURCE GROUP … SET io_limit '…'` string. It is now (1) escaped with
  `quoteLiteral` in the db client, (2) validated by the webhook, and (3) constrained by a
  CRD validation pattern on `TablespaceIOLimitSpec.Tablespace` — all enforcing
  `^[A-Za-z_][A-Za-z0-9_]*$|^\*$` (a SQL identifier or the `*` wildcard). **Upgrade
  note:** a pre-existing cluster whose stored tablespace value does not match the pattern
  fails validation on its next update — rename the value to a plain SQL identifier (or
  `*`) before updating.
- **Query-exporter EXPLAIN sandbox** — plan collection re-plans query text captured from
  `pg_stat_activity` (arbitrary user SQL). It is now allowlist-gated: leading SQL
  comments (line and nested block) are stripped so a comment prefix cannot smuggle a
  statement past the check, only `SELECT`/`WITH` statement heads qualify, and a top-level
  semicolon followed by further statement content is rejected (protocol-independent
  multi-statement defense — under a simple-protocol DSN an embedded `ROLLBACK` could
  otherwise end the sandbox and let a trailing statement run outside it). The EXPLAIN
  itself runs inside `BEGIN READ ONLY` + `SET LOCAL statement_timeout`, with `ROLLBACK`
  executed on **all** paths (success, error, timeout) so the shared connection is never
  left inside a transaction.
- **401 responses no longer disclose auth configuration** — every API 401 now returns a
  generic body (`authentication required` / `authentication failed`) plus an RFC 7235
  `WWW-Authenticate` challenge advertising only the configured scheme(s)
  (`Basic realm="cloudberry"`, `Bearer`, or both). The concrete failure reason (missing
  header, unknown scheme, unconfigured provider) is logged and attached to the trace
  span, never returned to the client. The admin-password rotation handler likewise
  returns a generic 500 body instead of the raw Kubernetes error text (which is only
  logged).

### Known issues

- **Multi-writer status race (cluster vs. admin controller)** — both controllers persist
  status via MergePatch, so a concurrent write can briefly overwrite the other's fields
  (observed as a short-lived stale phase/counter). The status converges within one
  requeue (~30 s). Field-scoped status patches per controller are the recommended
  follow-up.
- **`gp_interconnect_type` is not in `restartRequiredParams`** — a live edit is
  classified reload-safe and only updates the ConfigMap, but the GUC is
  postmaster-scoped: it takes effect only after the pods restart. Trigger a rolling
  restart manually after changing it on a running cluster.
- **`gp_toolkit` views absent in this Cloudberry build** — collectors and queries that
  depend on `gp_toolkit` (resource-group iostats, skew, disk-free) degrade gracefully:
  they log a WARN and skip (or record `result="skipped"`), never fabricate values.
- **No exec-based `/metrics` probing on the operator image** — the operator runs on a
  distroless image (no shell, no curl), so `kubectl exec … curl localhost:8080/metrics`
  is impossible; use `kubectl port-forward` instead.
- **`DisallowUnknownFields` deferred** — unknown fields in API request bodies are still
  accepted (only trailing data after the JSON value is rejected) to keep older/newer
  clients interoperable during version skew.

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

- **2026-07-12 — performance re-run after the cert-rotation/metrics changes.**
  All SLOs pass, no regression vs the 2026-07-10 baseline
  (`test/performance/results/2026-07-12-perftest-report.md`): health
  endpoints p99 ≤ ~10 ms at 100 RPS (24,000 requests, 0 errors), authed API
  reads bcrypt-dominated as before, rate limiter knee exact, zero 5xx.
  Coverage after the accompanying unit-test phase: **94.7 %** total across
  `./internal/... ./cmd/... ./api/...` with no package below 90 % (enforced
  by the new CI gate).
- **2026-07-10 — full live acceptance (kind) after the reliability/security refactor.**
  Operator deployed with Vault-PKI webhook certs + Vault kubernetes-auth + Keycloak OIDC;
  `acceptance-test` cluster Running with HA coordinator + standby, 2+2 group segment
  mirroring (`InSync`), cluster TLS auto-issued from Vault PKI, and
  `gp_interconnect_type: tcp` (kind/Docker Desktop UDP unreliability — see the sample-CR
  note under _Added_). With the interconnect NetworkPolicy fix in place, operator-tracked
  `gpbackup` of a 234 MB `mydb` → MinIO (Vault-sourced S3 creds) → `gprestore` completed
  with the restore **and** post-restore validation Jobs `Succeeded` and row counts
  matching. PXF external writable + readable tables verified for `s3:text/parquet/avro`
  and `hdfs:text/parquet/avro` (~10 MB each, full write → read-back round-trip);
  `hdfs:SequenceFile` WRITE succeeds but READBACK fails with PXF's "No fields in record" —
  the documented PXF profile limitation (a custom Java `Writable` schema class on the PXF
  classpath is required), not an operator defect. Grafana dashboards published covering
  168/168 project metric families. Performance (report:
  `test/performance/results/2026-07-10-perftest-report.md`): health p99 10.4 ms at
  100 RPS with 0 errors, authenticated API p99 167 ms (bcrypt-dominated), the rate
  limiter enforced the configured 10 req/min exactly (fast ~39 ms 429 rejections;
  `cloudberry_api_rate_limit_rejections_total` and `cloudberry_api_rate_limit_entries`
  accurate under load), zero 5xx across 24,000+ requests, plus DB query baselines on the
  500k-row dataset.
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
