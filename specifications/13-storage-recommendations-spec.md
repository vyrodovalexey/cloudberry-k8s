# Specification 13: Storage Management & Recommendations

## Overview

This specification defines the storage management, recommendation scanning, and usage reporting
capabilities for the Cloudberry Kubernetes Operator. These features enable operators to monitor
disk usage, receive actionable recommendations for table maintenance, and generate monthly
resource usage reports.

## CRD Changes

### StorageManagementSpec (spec.storage)

Added to `CloudberryClusterSpec`:

```yaml
storage:
  diskMonitoring: true
  recommendationScan:
    enabled: true
    schedule: "0 3 * * 0"        # Weekly Sunday 3 AM
    bloatThreshold: 20            # Dead tuple % threshold
    skewThreshold: 50             # Skew coefficient %
    ageThreshold: 500000000       # XID age threshold
    indexBloatThreshold: 30       # Index bloat %
    scanDuration: "2h"            # Max scan duration
  usageReport:
    enabled: true
    monthly: true
```

### RecommendationScanSpec

| Field               | Type   | Default     | Description                          |
|---------------------|--------|-------------|--------------------------------------|
| enabled             | bool   | false       | Enable recommendation scanning       |
| schedule            | string | 0 3 * * 0   | Cron schedule for scans              |
| bloatThreshold      | int32  | 20          | Dead tuple percentage threshold      |
| skewThreshold       | int32  | 50          | Data skew coefficient threshold      |
| ageThreshold        | int64  | 500000000   | XID age threshold                    |
| indexBloatThreshold | int32  | 30          | Index bloat percentage threshold     |
| scanDuration        | string | 2h          | Maximum scan duration                |

### UsageReportSpec

| Field   | Type | Default | Description                    |
|---------|------|---------|--------------------------------|
| enabled | bool | false   | Enable usage reporting         |
| monthly | bool | false   | Generate monthly reports       |

When `enabled: true` (with `monthly: true`), the operator produces an ON-DEMAND
monthly usage report retrievable via API P.6 + the `cloudberry-ctl storage
usage-report` CLI (Scenario 120, C.11/C.13). Each report entry
(`db.UsageReportEntry`) carries per-database size + connections **and** a
`tables[]` per-table breakdown (`db.TableUsage{Schema, Table, SizeBytes,
SizeHuman}`) attached to the connected-database entry; see
[§API Response Shapes P.6](#p6--get-clustersnamestorageusage-report) and
[Scenario 120](#scenario-120--usage-reporting). The report is HONEST: there is no
persisted month-over-month history, so `growthBytes`/`queryCount` stay `0`. A
disabled report (`enabled: false`) is **unavailable** — P.6 returns
`usageReportEnabled: false` + empty entries.

### Status Fields

Added to `CloudberryClusterStatus`:

| Field                       | Type           | Description                                                                 |
|-----------------------------|----------------|-----------------------------------------------------------------------------|
| diskUsagePercent            | int32          | Current disk usage percentage. **Deliberately omits `omitempty`** so a `0` value is always serialized in the status MergePatch — the disabled-state reset (Scenario 122 C.2: `diskMonitoring` off → `0`) reliably persists to `0` and `kubectl get` shows `0` on a disabled cluster, rather than retaining a previous non-zero value. |
| recommendationCount         | int32          | Number of active recommendations. **Deliberately omits `omitempty`** so a `0` value is always serialized in the status MergePatch — the disabled-state reset (Scenario 122 C.4: `recommendationScan` disabled → `0`) reliably persists to `0` and `kubectl get` shows `0` on a disabled cluster, rather than retaining a previous non-zero value. |
| recommendationScanTruncated | bool           | `true` when the most recent scan hit the `scanDuration` cap (Scenario 118b; never sticky — reset `false` on every non-truncated scan) |
| lastRecommendationScanTime  | *metav1.Time   | Timestamp of the most recent recommendation scan (set each scan, capped or complete) |

## API Endpoints

> **Status: Implemented (verified live).** All six storage REST endpoints below
> carry stable IDs **P.1–P.6** and return REAL data (no static placeholders) from
> `internal/api/server.go` (the six handlers + their `collect*` helpers), backed
> by the DB methods on `internal/db/client.go`. The full request params + JSON
> response shapes are documented in [§API Response Shapes](#api-response-shapes)
> and the per-endpoint behavior in
> [Scenario 119](#scenario-119--all-api-endpoints). Reads require **Basic**
> permission; the scan POST requires **Operator**. Every read is best-effort: a
> DB-unavailable / NewClient / query error yields an HONEST empty payload with
> HTTP 200 (NOT a 500); a missing cluster is `404 CLUSTER_NOT_FOUND`. The `P.*`
> IDs are **storage-recommendations-scoped** and DISTINCT from the data-loading
> `P.*` family in [spec 12](12-data-loading-spec.md).

| ID  | Method | Path                                                  | Permission | Description                    |
|-----|--------|-------------------------------------------------------|------------|--------------------------------|
| P.1 | GET    | /clusters/{name}/storage/disk-usage                   | Basic      | Get disk usage info            |
| P.2 | GET    | /clusters/{name}/storage/tables                       | Basic      | List tables with storage info  |
| P.3 | GET    | /clusters/{name}/storage/tables/{schema}/{table}      | Basic      | Get table detail               |
| P.4 | GET    | /clusters/{name}/storage/recommendations              | Basic      | List recommendations           |
| P.5 | POST   | /clusters/{name}/storage/recommendations/scan         | Operator   | Trigger recommendation scan    |
| P.6 | GET    | /clusters/{name}/storage/usage-report                 | Basic      | Get usage report               |

### API Response Shapes

All paths are prefixed `/api/v1alpha1`. An optional `?namespace=<ns>` query
selects the cluster's namespace (omitted → search across namespaces). Sizes are
returned as both raw `sizeBytes` (int64) and a human string `sizeHuman`
(`pg_size_pretty`). Percentages (`bloatPercent`, `skewPercent`,
`diskUsagePercent`) are `int32` clamped `0..100`.

#### P.1 — `GET /clusters/{name}/storage/disk-usage`

Backed by `collectDiskUsage` (`GetDiskUsage`) + `collectStorageBreakdown`
(`GetStorageDiskUsage`).

```json
{
  "cluster": "my-cluster",
  "diskUsagePercent": 42,
  "diskUsage": [
    { "database": "mydb", "sizeBytes": 1048576, "sizeHuman": "1024 kB" }
  ],
  "diskUsageBySegment": [
    { "tablespace": "pg_default", "sizeBytes": 1048576, "sizeHuman": "1024 kB", "usagePercent": 42 }
  ]
}
```

- `diskUsagePercent` is sourced **only** from `status.diskUsagePercent` so the
  `P.1 == status.diskUsagePercent` (≡ Scenario 116 `M.1 == S.1`) invariant holds;
  `diskUsage` (per-database) and `diskUsageBySegment` (per-tablespace/segment) are
  purely additive and best-effort (empty when the DB is unreachable).

#### P.2 — `GET /clusters/{name}/storage/tables`

Backed by `collectTables` (`GetTables`).

```json
{
  "cluster": "my-cluster",
  "tables": [
    {
      "schema": "public",
      "table": "orders",
      "sizeBytes": 8388608,
      "sizeHuman": "8192 kB",
      "bloatPercent": 12,
      "skewPercent": 0,
      "rowCount": 100000
    }
  ],
  "total": 1
}
```

#### P.3 — `GET /clusters/{name}/storage/tables/{schema}/{table}`

Backed by `collectTableDetail` (`GetTableDetails`). On DB-unavailable / not-found
/ query error it returns the honest minimal `{schema, table}` shape (HTTP 200).

```json
{
  "schema": "public",
  "table": "orders",
  "sizeBytes": 8388608,
  "sizeHuman": "8192 kB",
  "rowCount": 100000,
  "bloatPercent": 12,
  "skewPercent": 0,
  "lastVacuum": "2026-06-01 03:00:00+00",
  "lastAnalyze": "2026-06-01 03:05:00+00",
  "indexSizes": [
    { "name": "orders_pkey", "sizeBytes": 1048576, "sizeHuman": "1024 kB" }
  ]
}
```

#### P.4 — `GET /clusters/{name}/storage/recommendations`

Backed by `collectRecommendations` (the four threshold-aware
`Get{Bloat,Skew,Age,IndexBloat}Recommendations`). `target` is `"schema.table"`.

```json
{
  "cluster": "my-cluster",
  "recommendations": [
    {
      "type": "bloat",
      "target": "public.orders",
      "value": 100000,
      "ratio": 25,
      "severity": "warning",
      "description": "table public.orders has 25% dead tuples"
    }
  ],
  "recommendationCount": 1,
  "total": 1
}
```

- `recommendationCount` is the LIVE total when the DB was reachable; when the DB
  is unreachable it falls back to the cached `status.recommendationCount`
  (honest cached value, never a fabricated count).

#### P.5 — `POST /clusters/{name}/storage/recommendations/scan`

Requires **Operator**. Gated on `spec.storage.recommendationScan.enabled`.

```json
{ "status": "scan initiated", "cluster": "my-cluster" }
```

- `202 Accepted` when enabled (each POST runs a best-effort scan that advances the
  `cloudberry_recommendation_scan_duration_seconds` count, independent of the
  cron schedule).
- `400 RECOMMENDATION_SCAN_NOT_ENABLED` when the scan is disabled/absent.

#### P.6 — `GET /clusters/{name}/storage/usage-report`

Backed by `collectUsageReport` (`GetUsageReport`). Optional `?month=YYYY-MM`.
**Soft-gated** on `spec.storage.usageReport.enabled` (a READ, so a disabled
report is `200` with an empty list + `usageReportEnabled:false`, NOT a `400`).

Each entry carries the per-database size/connections **and** a `tables[]`
per-table breakdown (Scenario 120 C.11). Because the pgx pool is single-database,
`tables[]` is attached only to the entry matching the connected database
(`c.config.Database`); other database entries carry an empty/omitted `tables`.
The breakdown is best-effort and bounded (`LIMIT 50`, largest-first): on an honest
fallback (`gp_catalog` relations/columns absent — SQLSTATE `42P01`/`42703`) or any
query/scan error it is empty rather than failing the whole report.
`growthBytes`/`growthHuman`/`queryCount` stay an honest `0`/empty — the report is
computed ON DEMAND from live catalog sizes with no persisted month-over-month
history to derive growth from (no fabrication).

```json
{
  "cluster": "my-cluster",
  "month": "2026-05",
  "entries": [
    {
      "month": "2026-05",
      "database": "mydb",
      "sizeBytes": 1048576,
      "sizeHuman": "1024 kB",
      "growthBytes": 0,
      "growthHuman": "",
      "queryCount": 0,
      "connections": 3,
      "tables": [
        { "schema": "public", "table": "orders", "sizeBytes": 8388608, "sizeHuman": "8192 kB" }
      ]
    }
  ],
  "total": 1,
  "usageReportEnabled": true
}
```

- `tables[]` is populated only on the connected-database entry; entries for other
  databases honestly omit it (the operator cannot size tables in a database it is
  not connected to). The breakdown comes from `collectUsageReportTables`
  (`pg_class`/`pg_namespace`, `relkind IN ('r','m')`, catalog/internal schemas
  excluded, `pg_total_relation_size`, `LIMIT 50`).

## CLI Commands

> **Status: Implemented (verified live).** The six `cloudberry-ctl storage`
> commands below carry stable IDs **L.1–L.6** and are thin clients over the
> Scenario 119/120 storage API endpoints **P.1–P.6**
> (`cmd/cloudberry-ctl/main.go::newStorageCmd`). The per-command behavior is
> documented in [Scenario 121](#scenario-121--all-cli-commands). These
> storage-recommendations `L.*` IDs are **DISTINCT** from the data-loading
> `L.1`–`L.16` CLI family ([spec 12](12-data-loading-spec.md) / Scenario 108);
> the two `L.*` namespaces do not overlap.

```
cloudberry-ctl storage disk-usage --cluster my-cluster
cloudberry-ctl storage tables list --cluster my-cluster
cloudberry-ctl storage tables detail --cluster my-cluster --schema public --table orders
cloudberry-ctl storage recommendations list --cluster my-cluster
cloudberry-ctl storage recommendations scan --cluster my-cluster
cloudberry-ctl storage usage-report --cluster my-cluster --month 2026-05
```

| ID  | Command                                  | API endpoint (method)                                      | Notes |
|-----|------------------------------------------|------------------------------------------------------------|-------|
| L.1 | `storage disk-usage`                     | `GET` P.1 `…/storage/disk-usage`                           | Prints disk usage. |
| L.2 | `storage tables list`                    | `GET` P.2 `…/storage/tables`                               | Lists tables with storage info. |
| L.3 | `storage tables detail`                  | `GET` P.3 `…/storage/tables/{schema}/{table}`             | Accepts `--schema`/`--table` flags AND legacy positional `[schema] [table]` args; flags win when set, else fall back to positional. A missing schema/table → clear usage error **before** any HTTP call (no half-built `/tables//` request). |
| L.4 | `storage recommendations list`           | `GET` P.4 `…/storage/recommendations`                     | Lists active recommendations. |
| L.5 | `storage recommendations scan`           | `POST` P.5 `…/storage/recommendations/scan`              | Triggers a best-effort scan. |
| L.6 | `storage usage-report`                   | `GET` P.6 `…/storage/usage-report?month=YYYY-MM`          | Optional `--month` threads through as `?month=` to select/label the reporting period. |

All six commands accept the shared `--cluster` (required) and `--namespace`
(optional, threaded as `?namespace=`) globals. L.3's
`--schema`/`--table` flags are the canonical form; the positional args are
retained for backward compatibility (`resolveTableDetail`).

## Prometheus Metrics

| Metric                                          | Type      | Labels                    | Description                        |
|-------------------------------------------------|-----------|---------------------------|------------------------------------|
| cloudberry_disk_usage_percent                   | Gauge     | cluster, namespace        | Disk usage percentage per cluster (Scenario 116: REAL measured value from `gp_toolkit.gp_disk_free`, matches `status.diskUsagePercent` — `M.1 == S.1`; was a static value) |
| cloudberry_recommendations_total                | Gauge     | cluster, namespace, type  | Recommendations by type (Scenario 117: carries REAL per-type data — `type=bloat\|skew\|age\|index_bloat` — from the four threshold-aware scans; set for ALL four types incl. `0`, and `M.2 == status.recommendationCount` summed over types) |
| cloudberry_recommendation_scan_duration_seconds | Histogram | cluster, namespace        | Scan duration in seconds (M.3 — observed over the four-type scan in `recordRecommendations`; under a truncated run reflects the **capped** run, not an unbounded scan) |
| cloudberry_recommendation_scan_truncated_total  | Counter   | cluster, namespace        | Incremented when a scan is **capped** at the configured `scanDuration` deadline (Scenario 118b, C.10) |
| cloudberry_table_bloat_ratio                    | Gauge     | cluster, namespace, table | Bloat ratio for top tables (Scenario 117: published from the SAME bloat scan as `cloudberry_recommendations_total{type=bloat}` — dead-tuple % per table, M.4) |
| cloudberry_recommendation_scan_cronjob          | Gauge     | cluster, namespace        | 1 when the recommendation-scan CronJob is ensured, 0 when removed |

## Validation Rules

> **Status: Implemented (webhook-authoritative).** The storage-recommendations
> validation rules below are stable-ID **W.1–W.5**, scoped to
> `spec.storage.recommendationScan` (`RecommendationScanSpec`). They are
> **DISTINCT** from the data-loading `W.1`–`W.25` rule family in
> [spec 12](12-data-loading-spec.md#webhook-validation); the two `W.*` namespaces
> do not overlap. The checks live in
> `internal/webhook/validating.go::validateStorageManagement` and are
> **gated on `recommendationScan.enabled: true`** — thresholds on a disabled
> recommendation scan are accepted (no-op). The checks are **webhook-authoritative**
> (no CRD `Minimum`/`Maximum` markers): static markers cannot honor the
> `enabled`-gate and would replace the descriptive, field-specific error message
> with a generic apiserver range error.

| Rule | Field (`spec.storage.recommendationScan.*`) | Constraint | Error message |
|------|---------------------------------------------|------------|---------------|
| W.1  | `bloatThreshold`      | between 0 and 100 | `storage.recommendationScan.bloatThreshold must be between 0 and 100, got <n>` |
| W.2  | `skewThreshold`       | between 0 and 100 | `storage.recommendationScan.skewThreshold must be between 0 and 100, got <n>` |
| W.3  | `indexBloatThreshold` | between 0 and 100 | `storage.recommendationScan.indexBloatThreshold must be between 0 and 100, got <n>` |
| W.4  | `ageThreshold`        | non-negative (`>= 0`) | `storage.recommendationScan.ageThreshold must be non-negative, got <n>` |
| W.5  | `scanDuration`        | when set, must parse as a Go duration (`time.ParseDuration`) | `storage.recommendationScan.scanDuration "<v>" must be a valid Go duration` |

W.5 is **defense-in-depth** for the controller's C.10 scan-duration cap: the
webhook only checks **parseability** (an empty `scanDuration` is accepted — the
controller falls back to the `10s` default), while the runtime clamp/fallback
policy lives in the controller's `resolveScanDuration`. Like W.1–W.4, W.5 is
**`enabled`-gated** (`validateStorageManagement` early-returns when the scan is
absent or `enabled: false`) and **webhook-authoritative** (no CRD marker — the
gate cannot be expressed statically).

A rejected CR is **not persisted** (a follow-up `GET` returns `NotFound`); denials
increment the shared admission counter
`cloudberry_webhook_admission_total{webhook="validating",result="denied"}`. The
systematic negative-test matrix is documented in
[Scenario 113](#scenario-113--validation-rules-negative-tests).

## Default Values (applied by mutating webhook)

> **Status: Implemented (webhook-authoritative).** The storage-recommendations
> default-injection rules below are stable-ID **D.1–D.6**, scoped to
> `spec.storage.recommendationScan` (`RecommendationScanSpec`). They are
> **DISTINCT** from the data-loading defaults `D.1`–`D.14` (Scenario 90,
> `setDataLoadingDefaults`); the two `D.*` namespaces do not overlap. The defaults
> live in `internal/webhook/mutating.go::setStorageManagementDefaults` (reached
> via the public `Default()` entrypoint) and are **gated on
> `recommendationScan.enabled: true`** — a default is injected only when the scan
> is enabled **and** the field is zero/empty. Explicit user-supplied values are
> always **preserved**. The defaults are **webhook-authoritative** (no CRD
> per-field `+kubebuilder:default` markers): static markers cannot honor the
> `enabled`-gate.

| Rule | Field (`spec.storage.recommendationScan.*`) | Default | Note |
|------|---------------------------------------------|---------|------|
| D.1  | `schedule`            | `0 3 * * 0` | Weekly Sunday 3 AM; injected only when empty |
| D.2  | `bloatThreshold`      | `20`        | Dead tuple % threshold; injected only when `0` |
| D.3  | `skewThreshold`       | `50`        | Skew coefficient %; injected only when `0` |
| D.4  | `ageThreshold`        | `500000000` | XID age threshold; injected only when `0` |
| D.5  | `indexBloatThreshold` | `30`        | Index bloat %; injected only when `0` |
| D.6  | `scanDuration`        | `2h`        | Max scan duration; injected only when empty |

All six defaults are **`enabled`-gated** and **preserve explicit user values**.
The systematic verification matrix is documented in
[Scenario 114](#scenario-114--mutating-webhook-defaults).

## Reconciliation

> **Status: Implemented (verified live).** The storage-recommendations
> reconciliation rules below are stable-ID **C.1, C.3, C.5, R.1, R.2, R.5** plus the
> disk-monitoring **S.1** (status) and **M.1** (metric), scoped to
> `spec.storage` (`StorageManagementSpec`) and exercised by
> `AdminReconciler.reconcileStorage()`
> (`internal/controller/admin_controller.go`). These `C.*`/`R.*` IDs are
> **storage-recommendations-scoped** and are distinct from any same-letter rule
> family in other specs. The scheduled CronJob is built by the NEW
> `BuildRecommendationScanCronJob`
> (`internal/builder/recommendation_scan_builder.go`) and ensured/GC'd by
> `ensureRecommendationScanCronJob` / `removeRecommendationScanCronJob`. Disk usage
> monitoring (**R.2, S.1, M.1**) is measured each reconcile by the NEW
> `recordDiskUsage` (mirrors `recordTableBloatRatios`: dbFactory client + 10s
> timeout + span, best-effort and non-fatal), backed by the NEW DB method
> `GetDiskUsagePercent` (`internal/db/client.go`).

| Rule | Field / trigger | Behavior |
|------|-----------------|----------|
| R.1 | `spec.storage.diskMonitoring` | `reconcileStorage()` **gates on `diskMonitoring: true`** and proceeds; it **early-returns (no-op)** when `spec.storage` is absent or `diskMonitoring` is false (no CronJob, no condition). |
| R.2 | `spec.storage.diskMonitoring` (measurement) | `reconcileStorage()` (and the steady-state `refreshStorageOnSteadyState`) **MEASURE** disk usage each reconcile via `recordDiskUsage` → `GetDiskUsagePercent()`, which queries `gp_toolkit.gp_disk_free` and returns the **MAX worst-case** `usage% = 100*(df_total-df_free)/df_total` (clamped `0..100`) across segment data volumes. When `gp_disk_free` is unavailable (SQLSTATE `42P01`/`42703`) it returns the sentinel `ErrDiskUsageUnavailable` — the controller **SKIPS without fabricating** a value (HONEST fallback; the prior status is not overwritten). |
| S.1 | `status.diskUsagePercent` | Populated with the **measured** value (no longer a static `0`); tracks growth as data grows (current value each reconcile, never sticky). |
| M.1 | `cloudberry_disk_usage_percent{cluster,namespace}` | The Prometheus gauge is published FROM THE SAME measured value as S.1, so **the metric MATCHES the status** (`M.1 == S.1` invariant by construction — the stale `Status.DiskUsagePercent` is never read to publish the gauge). |
| C.1 | `spec.storage.recommendationScan` (schedule) | The `recommendationScan` config is **accepted/parsed** when enabled — the schedule is read and carried to the CronJob; `RecommendationCount` is propagated into status. |
| C.3 | `spec.storage.recommendationScan` (thresholds) | The threshold set (`bloatThreshold`, `skewThreshold`, `ageThreshold`, `indexBloatThreshold`, `scanDuration`) is **accepted unchanged** and passed to the CronJob pod as env vars; per-table `cloudberry_table_bloat_ratio` gauges are published best-effort. |
| C.5 | `<cluster>-recommendation-scan` CronJob | A CronJob is **created** for the configured schedule (e.g. `0 3 * * 0`) via `BuildRecommendationScanCronJob` + `ensureRecommendationScanCronJob`, and **GC'd** (delete-if-exists) via `removeRecommendationScanCronJob` when the scan is disabled / has no schedule (nil-means-delete). The CronJob uses `ForbidConcurrent`, successful/failed history limits of `3`, `OwnerReferences` to the cluster, and a JobTemplate whose pod carries the scan thresholds + duration as env vars (HONEST scaffold: a read-only `SELECT 1` connectivity probe, not yet the real scan SQL). |
| C.10 | `spec.storage.recommendationScan.scanDuration` (cap) | `recordRecommendations` **CAPS** the whole scan at `scanDuration` via `resolveScanDuration` (empty/unparseable/`<= 0` → `10s` default; `> 24h` → clamp `24h`; otherwise the parsed value **verbatim**). A **single shared** `context.WithTimeout(ctx, resolveScanDuration(...))` bounds the TOTAL of all four `Get*` queries (ONE shared budget, NOT `4x` the cap). When the shared `dbCtx` hits its deadline mid-run the scan records **partial results** (only the per-type counts that completed; un-run types count `0`, **no fabrication**), sets `status.recommendationScanTruncated=true`, increments `cloudberry_recommendation_scan_truncated_total{cluster,namespace}`, and sets `status.lastRecommendationScanTime`. The M.3 duration metric reflects the **capped** run. The truncation flag is **never sticky** — set `false` on every non-truncated scan (the status field omits `omitempty`, so a prior `true` reliably clears via the status MergePatch). |
| C.11 | `spec.storage.usageReport` (report generation/content) | When `usageReport.enabled: true` (with `monthly: true`), the operator produces an **ON-DEMAND** monthly usage report (scoped/labeled by month — HONEST: no fabricated persisted history, so growth/queryCount stay `0` when no history) containing **per-table AND per-database** storage consumption. `GetUsageReport(ctx, month)` returns `[]db.UsageReportEntry{Month, Database, SizeBytes, SizeHuman, Connections, Tables:[]TableUsage{Schema,Table,SizeBytes,SizeHuman}}`. The per-table `Tables` breakdown is attached to the **CONNECTED-db** entry only (the pgx pool is single-database — honest), best-effort (empty on the honest `42P01`/`42703` fallback or any error), and bounded `LIMIT 50` (largest-first). |
| C.13 | `spec.storage.usageReport` (retrieval) | The report is **retrievable** via BOTH the API (P.6 `GET …/storage/usage-report` → `200 {cluster, month, entries:[…with tables…], total, usageReportEnabled}`; **Basic** perm; `?month=YYYY-MM` scopes/labels) AND the CLI (`cloudberry-ctl storage usage-report --cluster X [--namespace …] [--month YYYY-MM]`, the `--month` flag threading `?month=`). **Disabled** (`usageReport.enabled: false`, the Scenario 8 reference) → the report is **UNAVAILABLE** (`usageReportEnabled: false`). |
| C.2 | `spec.storage.diskMonitoring: false` (disabled-state reset) | When `diskMonitoring` is false (or `spec.storage` is absent), `reconcileStorage()` **does NOT measure** disk usage (the R.1 gate early-returns before `recordDiskUsage`). On this disabled path the operator **RESETS** the signal to an explicit disabled value via `clearStorageSignals`: `status.diskUsagePercent` is set to **`0`** and the `cloudberry_disk_usage_percent` gauge is published **`0`** (a `0` here is an explicit "monitoring off" signal, NOT "empty" — it is reset-on-disable, so a reader sees the feature is off rather than a frozen stale reading). The cleared status is persisted ONLY when there was an actual stale value (R6 anti-churn). The recommendation-scan CronJob is **GC'd** (owned by the steady-state path's `removeRecommendationScanCronJob`). **Re-enable** (`diskMonitoring: true`) → `recordDiskUsage` resumes and the metric/status repopulate from the live measurement (S.1/M.1). |
| C.4 | `spec.storage.recommendationScan.enabled: false` (disabled-state clear) | When the scan is disabled/absent, **no** scan CronJob is ensured (`ensureRecommendationScanCronJob`'s builder returns nil → delete-if-exists) and `recordRecommendations` does **NOT** run, so recommendations are **NOT produced**. The operator **CLEARS** the signal via `clearRecommendations`: `status.recommendationCount` is reset to **`0`** and `cloudberry_recommendations_total{type}` is published **`0`** for **ALL four** types (`bloat`/`skew`/`age`/`index_bloat`, reusing the canonical `recommendationTypes` slice so it cannot drift). The `status.recommendationScanTruncated` flag is also reset `false` (a disabled scan can never be truncated). The per-table `cloudberry_table_bloat_ratio{table}` gauge is **NOT cleared** — it is a per-table cardinality signal (M.4) the operator does not enumerate on disable, so there is no precise label set to zero (an HONEST, documented limitation; the CronJob GC + the count clear are the primary disabled signals). The explicit-trigger API `POST …/storage/recommendations/scan` returns **`400 RECOMMENDATION_SCAN_NOT_ENABLED`**. **Re-enable** (`recommendationScan.enabled: true`) → the CronJob + `recordRecommendations` resume and the counts/gauges repopulate. |
| C.12 | `spec.storage.usageReport.enabled: false` (disabled-state soft-gate) | The usage-report endpoint/CLI **report the feature disabled** rather than erroring: P.6 `GET …/storage/usage-report` is a **READ**, so a disabled report is a **soft-gate** — `200 {usageReportEnabled: false, entries: []}` (NOT a `400`; the `*_NOT_ENABLED` 400 is reserved for the mutating/action scan endpoint, C.4). The `cloudberry-ctl storage usage-report` CLI **inherits** the same payload. **Re-enable** (`usageReport.enabled: true`) → `entries` populate and `usageReportEnabled: true`. |
| R.5 | `StorageConfigured` status condition | On a successful storage reconcile the `StorageConfigured` condition is set to **`True`** with reason **`StorageReconciled`**. |
| R.3 | `spec.storage.recommendationScan` (scan) | When `recommendationScan.enabled: true`, `reconcileStorage()` (and steady-state `refreshStorageOnSteadyState`) calls `recordRecommendations`, which **PROCESSES** the scan config: it reads the four CRD thresholds into a `db.RecommendationThresholds{Bloat,Skew,Age,IndexBloat}` and runs all **FOUR** threshold-aware scans (`Get{Bloat,Skew,Age,IndexBloat}Recommendations`) in a single pass. Best-effort and non-fatal: a missing dbFactory / NewClient failure / single `Get*` error SKIPS that contribution without fabricating a count and without failing the reconcile. |
| R.4 | `status.recommendationCount` (invariant) | The status count equals the **CURRENT active count** — the sum across the four per-type counts from this scan, NOT the stale prior value. |
| S.2 | `status.recommendationCount` | Set to the current total active recommendation count (sum of the four per-type counts) and persisted via the end-of-reconcile status patch; tracks the live state each reconcile. |
| M.2 | `cloudberry_recommendations_total{cluster,namespace,type}` | The gauge is set for **EVERY** type (`bloat`, `skew`, `age`, `index_bloat`) on each scan, **including `0`** for absent/cleared types, so a cleared or out-of-threshold type resets to `0` rather than going stale. **`M.2 == count` invariant:** `status.recommendationCount` equals the sum over types of the value passed to `SetRecommendationsTotal{type}` — both derive from the SAME per-type counts in one pass (the stale status count is never read to publish the metric). |
| M.3 | `cloudberry_recommendation_scan_duration_seconds{cluster,namespace}` | The histogram is observed via `ObserveRecommendationScanDuration` over the elapsed time of the four-type scan loop in `recordRecommendations`. Under a truncated (C.10-capped) run it reflects the **capped** elapsed time, not an unbounded scan. |
| M.4 | `cloudberry_table_bloat_ratio{cluster,namespace,table}` | Published from the SAME bloat scan used for the `type=bloat` count (the bloat query runs ONCE per reconcile) — the top-N most-bloated tables' dead-tuple `%` (capped at 20 tables to bound cardinality). |

### Recommendation Types

> **Status: Implemented (verified live).** The four recommendation types
> **RT.1–RT.4** are scanned by `recordRecommendations`
> (`internal/controller/admin_controller.go`) via four THRESHOLD-AWARE DB queries
> on `internal/db/client.go`. Each query GATES on its own CRD threshold (bound as
> a query parameter `$1`, never string-interpolated, so the gate is
> injection-safe) and follows an **HONEST fallback** policy: when a required
> `gp_toolkit` view or catalog column is unavailable on the server version
> (SQLSTATE `42P01` undefined table / `42703` undefined column) the method SKIPS +
> logs at debug and returns no rows — that type counts `0` without **fabricating**
> a value. These `RT.*`/`C.*` IDs are **storage-recommendations-scoped**.

| Rule | Type (`cloudberry_recommendations_total{type}`) | Gate | Source | Honest fallback |
|------|--------------------------------------------------|------|--------|-----------------|
| RT.1 | `bloat`       | C.6 — dead-tuple `dead_pct >= bloatThreshold` | `pg_stat_user_tables` (core catalog, always present) | n/a — core catalog; also feeds `cloudberry_table_bloat_ratio` (M.4) |
| RT.2 | `skew`        | C.7 — `skccoeff >= skewThreshold` | `gp_toolkit.gp_skew_coefficients` | view absent (`42P01`/`42703`) → skip, count `0`, **no** row-count proxy |
| RT.3 | `age`         | C.8 — `age(relfrozenxid) >= ageThreshold` | `pg_class` / `pg_namespace` (`relkind IN ('r','m')`, user schemas) | core catalog; on `42P01`/`42703` → skip, count `0`, **no** dead-tuple proxy |
| RT.4 | `index_bloat` | C.9 — estimated `bloat_pct >= indexBloatThreshold` | `pg_class` / `pg_index` / `pg_namespace` portable estimate (8 KiB page, 32-byte entry, 90% fill) | on `42P01`/`42703` → skip, count `0`, **no** raw size gate |

Each type's gate uses ONLY its own CRD threshold field; the four are threaded
through a single `db.RecommendationThresholds{Bloat int32, Skew int32, Age int64,
IndexBloat int32}`. To the per-type counter, "no rows for this type" and "this
type unmeasurable" are honestly indistinguishable (count `0`).

The `ensureRecommendationScanCronJob` helper is called **unconditionally** from
`reconcileStorage`, so the create path and the enabled→disabled GC are both driven
every reconcile. It sets the **`cloudberry_recommendation_scan_cronjob`** gauge to
`1` when the CronJob is ensured and `0` when it is removed. A create/update error
surfaces as a reconcile error (no-false-positive control). The `usageReport`
config is accepted alongside the scan; reconcile completes with **no errors**.

## Scenarios

### Scenario 113 — Validation Rules (Negative Tests)

> **Status: Implemented (verified live).** The storage-recommendations
> validation rules **W.1–W.4** for `spec.storage.recommendationScan`
> (`RecommendationScanSpec`) are implemented in
> `internal/webhook/validating.go::validateStorageManagement`, gated on
> `recommendationScan.enabled: true`, and **webhook-authoritative** (no CRD
> `Minimum`/`Maximum` markers). The negative matrix below was **verified live**
> against the deployed Vault-PKI validating webhook: every invalid CR was
> rejected with the **exact** descriptive error and a follow-up `GET` returned
> `NotFound`; the boundary CR was created successfully. These `W.*` IDs are
> **DISTINCT** from the data-loading `W.1`–`W.25` family in
> [spec 12](12-data-loading-spec.md#webhook-validation).

Acceptance scenario (verbatim): *"For each of the 4 storage-recommendations
webhook rules W.1–W.4, apply an otherwise-valid CloudberryCluster with
`storage.recommendationScan.enabled: true` carrying EXACTLY ONE out-of-range
threshold and verify it is REJECTED with a DESCRIPTIVE (field-path + reason)
error that names the field AND the offending value, AND that the rejected CR
does NOT persist (a follow-up GET is NotFound). The boundary values
(bloat/skew/indexBloat = 0 and = 100; age = 0) must ADMIT (CONTROL — no
false-positive)."*

#### The 4 negative tests (one offending field each)

| Rule | Sub-case | Invalid input | Expected error | Source |
|------|----------|---------------|----------------|--------|
| W.1 | 113a | `bloatThreshold: 150` (and `-1`) | `storage.recommendationScan.bloatThreshold must be between 0 and 100, got 150` | WEBHOOK |
| W.2 | 113b | `skewThreshold: 101` | `storage.recommendationScan.skewThreshold must be between 0 and 100, got 101` | WEBHOOK |
| W.3 | 113c | `indexBloatThreshold: 200` | `storage.recommendationScan.indexBloatThreshold must be between 0 and 100, got 200` | WEBHOOK |
| W.4 | 113d | `ageThreshold: -5` | `storage.recommendationScan.ageThreshold must be non-negative, got -5` | WEBHOOK |

All four rules are **WEBHOOK-enforced** (not CRD-schema enums): the descriptive,
field-specific message is what the user sees on a live apply.

#### Boundary ACCEPTS (CONTROL — no false-positive)

The inclusive boundaries are valid and **admit**:

- `bloatThreshold`: `0` and `100`
- `skewThreshold`: `0` and `100`
- `indexBloatThreshold`: `0` and `100`
- `ageThreshold`: `0`

A CR at these boundaries is **created successfully** (verified live: the CR
persisted and a follow-up `GET` returned it).

#### Properties asserted by Scenario 113

- **Descriptive error** — every rejection names the offending field path **and**
  the offending value (`got <n>`).
- **NO-PERSIST** — a rejected CR never reaches etcd; a follow-up `GET` returns
  `NotFound` (verified live against the deployed Vault-PKI webhook).
- **CONTROL** — the boundary CR (bloat/skew/indexBloat = 0 and 100; age = 0)
  **admits** and persists, proving no false-positive.
- **`enabled`-gated** — out-of-range thresholds on a
  `recommendationScan.enabled: false` (or absent) spec are accepted (no-op); the
  rules run only when the scan is enabled.
- **Denied metric** — denials increment the shared admission counter
  `cloudberry_webhook_admission_total{webhook="validating",result="denied"}`
  (there is **no** dedicated per-rule storage metric).

#### Test layers

- **Unit** — validator-direct against `validateStorageManagement`
  (`internal/webhook/scenario113_validation_test.go`).
- **Functional** — admission via the validator over a base-valid CR with one
  violation (`test/functional/scenario113_storage_validation_test.go`).
- **Integration** — `test/integration/scenario113_storage_validation_test.go`.
- **E2E (live)** — `kubectl`/client `apply` → reject → `GET NotFound`, plus the
  boundary-CR create
  (`test/e2e/scenario113_storage_validation_e2e_test.go`).

#### Artifacts

- `internal/webhook/scenario113_validation_test.go` — the **unit** layer.
- `test/cases/scenario113_storage_validation_cases.go` — the per-rule catalog
  (the W.1–W.4 negative rows + the boundary CONTROL rows).
- `test/functional/scenario113_storage_validation_test.go` — the **functional**
  admission-entrypoint layer.
- `test/integration/scenario113_storage_validation_test.go` — the integration
  layer.
- `test/e2e/scenario113_storage_validation_e2e_test.go` — the **live** layer
  (reject + no-persist + boundary-create).

### Scenario 114 — Mutating Webhook Defaults

> **Status: Implemented (verified live).** The storage-recommendations
> default-injection rules **D.1–D.6** for `spec.storage.recommendationScan`
> (`RecommendationScanSpec`) are implemented in
> `internal/webhook/mutating.go::setStorageManagementDefaults` (reached via the
> public `Default()` entrypoint), gated on `recommendationScan.enabled: true`,
> and **webhook-authoritative** (no CRD per-field `+kubebuilder:default` markers —
> static markers cannot honor the `enabled`-gate). The behavior was **verified
> live** against the deployed Vault-PKI mutating webhook: a minimal spec
> (`enabled: true`, all other scan fields omitted) was applied and a follow-up
> `GET` returned the persisted object with **all six** defaults injected. These
> `D.*` IDs are **DISTINCT** from the data-loading defaults `D.1`–`D.14`
> (Scenario 90, `setDataLoadingDefaults`); the two `D.*` namespaces do not
> overlap.

Acceptance scenario (verbatim): *"Apply a spec with recommendationScan.enabled:
true and all other scan fields omitted; verify the persisted object has the
defaults injected (D.1 schedule '0 3 * * 0', D.2 bloatThreshold 20, D.3
skewThreshold 50, D.4 ageThreshold 500000000, D.5 indexBloatThreshold 30, D.6
scanDuration '2h'). Confirm defaults are applied only when
recommendationScan.enabled is true."*

#### The 6 injected defaults

| Rule | Field (`spec.storage.recommendationScan.*`) | Default | Injected when |
|------|---------------------------------------------|---------|---------------|
| D.1  | `schedule`            | `0 3 * * 0` | field empty (`""`) |
| D.2  | `bloatThreshold`      | `20`        | field zero (`0`) |
| D.3  | `skewThreshold`       | `50`        | field zero (`0`) |
| D.4  | `ageThreshold`        | `500000000` | field zero (`0`) |
| D.5  | `indexBloatThreshold` | `30`        | field zero (`0`) |
| D.6  | `scanDuration`        | `2h`        | field empty (`""`) |

All six defaults are **WEBHOOK-injected** (not CRD-schema `+kubebuilder:default`
markers): the `enabled`-gate is what a static marker cannot express.

#### Properties asserted by Scenario 114

- **`enabled`-gated** — defaults are injected **only** when
  `recommendationScan != nil && recommendationScan.enabled == true`. A
  `recommendationScan.enabled: false` (or absent) spec receives **no** injection;
  the omitted fields stay zero/empty.
- **Preservation (CONTROL — no overwrite)** — an explicit user value for any of
  the six fields is **never** overwritten; the default fills only a zero/empty
  field. Applying a spec that sets, e.g., `bloatThreshold: 5` keeps `5`, not the
  `20` default.
- **PERSIST** — after a minimal-spec apply, a follow-up `GET` returns the
  persisted object carrying all six defaults (verified live against the deployed
  Vault-PKI mutating webhook).
- **Allowed metric** — defaulting never denies admission; the mutating pass
  increments the shared admission counter
  `cloudberry_webhook_admission_total{webhook="mutating",result="allowed"}`
  (there is **no** dedicated per-field storage metric).

#### Test layers

- **Unit** — defaulter-direct against `setStorageManagementDefaults` via
  `Default()` (`internal/webhook/scenario114_defaults_test.go`).
- **Functional** — admission via the defaulter over a minimal enabled CR, plus
  the `enabled: false` no-op and the explicit-value preservation cases
  (`test/functional/scenario114_storage_defaults_test.go`).
- **Integration** — `test/integration/scenario114_storage_defaults_test.go`.
- **E2E (live)** — `kubectl`/client `apply` minimal → `GET` → assert all six
  defaults persisted, plus the `enabled: false` no-op
  (`test/e2e/scenario114_storage_defaults_e2e_test.go`).

#### Artifacts

- `internal/webhook/scenario114_defaults_test.go` — the **unit** layer.
- `test/cases/scenario114_storage_defaults_cases.go` — the per-rule catalog
  (the D.1–D.6 default rows + the `enabled`-gate and preservation CONTROL rows).
- `test/functional/scenario114_storage_defaults_test.go` — the **functional**
  admission-entrypoint layer.
- `test/integration/scenario114_storage_defaults_test.go` — the integration
  layer.
- `test/e2e/scenario114_storage_defaults_e2e_test.go` — the **live** layer
  (apply minimal + GET + all-six-persisted + `enabled: false` no-op).

### Scenario 115 — Enable Storage Management with Full Configuration

> **Status: Implemented (verified live).** The storage-recommendations
> reconciliation rules **C.1, C.3, C.5, R.1, R.5** for `spec.storage`
> (`StorageManagementSpec`) are implemented in
> `AdminReconciler.reconcileStorage()`
> (`internal/controller/admin_controller.go`), with the scheduled CronJob built by
> the NEW `BuildRecommendationScanCronJob`
> (`internal/builder/recommendation_scan_builder.go`) and ensured/GC'd by
> `ensureRecommendationScanCronJob` / `removeRecommendationScanCronJob`. The
> behavior was **verified live** against the deployed operator: applying the full
> storage block created the `<cluster>-recommendation-scan` CronJob and set the
> `StorageConfigured` condition to `True` with **no reconciliation errors**. These
> `C.*`/`R.*` IDs are **storage-recommendations-scoped**.

Acceptance scenario: *"Apply a CloudberryCluster carrying the full `spec.storage`
block (`diskMonitoring: true`, an enabled `recommendationScan` with schedule +
thresholds, and an enabled monthly `usageReport`) and verify reconcileStorage
proceeds past the diskMonitoring gate (R.1), accepts the recommendationScan
config (C.1/C.3), creates the `<cluster>-recommendation-scan` CronJob for the
configured schedule (C.5), and sets the `StorageConfigured` condition to True with
reason StorageReconciled (R.5) — with the usageReport accepted and no
reconciliation errors."*

#### CR spec (verbatim, full block)

```yaml
storage:
  diskMonitoring: true
  recommendationScan:
    enabled: true
    schedule: "0 3 * * 0"        # Weekly Sunday 3 AM
    bloatThreshold: 20
    skewThreshold: 50
    ageThreshold: 500000000
    indexBloatThreshold: 30
    scanDuration: "2h"
  usageReport:
    enabled: true
    monthly: true
```

#### Verification mapping

| Rule | Assertion |
|------|-----------|
| R.1 | `diskMonitoring: true` is detected and `reconcileStorage()` **proceeds** past the gate (it early-returns when `spec.storage` is nil or `diskMonitoring` is false — no CronJob, no condition). |
| C.1 | The `recommendationScan` config is **accepted/parsed** when enabled (schedule read; `RecommendationCount` propagated into status). |
| C.3 | The threshold set (`bloatThreshold: 20`, `skewThreshold: 50`, `ageThreshold: 500000000`, `indexBloatThreshold: 30`, `scanDuration: "2h"`) is **accepted unchanged** and passed to the CronJob pod as env vars. |
| C.5 | A CronJob `<cluster>-recommendation-scan` is **created** for the `0 3 * * 0` schedule via `BuildRecommendationScanCronJob` + `ensureRecommendationScanCronJob` (and **GC'd** via `removeRecommendationScanCronJob` when disabled): `ForbidConcurrent`, successful/failed history limits `= 3`, `OwnerReferences` to the cluster, and a JobTemplate pod carrying the scan thresholds as env vars (HONEST scaffold). |
| R.5 | The `StorageConfigured` status condition is set to **`True`** with reason **`StorageReconciled`**. |
| usageReport | The enabled monthly `usageReport` config is **accepted** alongside the scan; reconcile completes with **no errors**. |

#### CronJob + metric

- **CronJob** — `<cluster>-recommendation-scan` (name from
  `util.RecommendationScanCronJobName`), labelled `component: recommendation-scan`,
  schedule `0 3 * * 0`, `ConcurrencyPolicy: Forbid`,
  `SuccessfulJobsHistoryLimit` / `FailedJobsHistoryLimit` `= 3`, owner-referenced
  to the cluster. The JobTemplate pod runs a read-only `SELECT 1` connectivity
  probe (HONEST scaffold — NOT the real scan SQL) with `PGPASSWORD` sourced from
  the admin Secret and the scan thresholds + duration as env vars
  (`SCAN_BLOAT_THRESHOLD`, `SCAN_SKEW_THRESHOLD`, `SCAN_AGE_THRESHOLD`,
  `SCAN_INDEX_BLOAT_THRESHOLD`, `SCAN_DURATION`).
- **Metric** — `cloudberry_recommendation_scan_cronjob{cluster,namespace}` gauge
  is set to `1` when the CronJob is ensured and `0` when it is removed (driven by
  `ensureRecommendationScanCronJob`, called unconditionally every reconcile).

#### Test layers

- **Unit** — builder-direct against `BuildRecommendationScanCronJob`
  (`internal/builder/scenario115_recommendation_scan_builder_test.go`) and
  controller-direct against `reconcileStorage` /
  `ensureRecommendationScanCronJob` / `removeRecommendationScanCronJob`, incl. the
  R.1 gate, R.5 condition, C.5 create/GC, and the metric gauge
  (`internal/controller/scenario115_storage_management_test.go`).
- **Functional** — admission/reconcile over the full-block CR asserting the
  CronJob exists with the expected name + spec
  (`test/functional/scenario115_storage_management_test.go`).
- **Integration** — `test/integration/scenario115_storage_management_test.go`.
- **E2E (live)** — apply the full block → CronJob exists + `StorageConfigured`
  `= True` + no errors
  (`test/e2e/scenario115_storage_management_e2e_test.go`).

#### Artifacts

- `internal/builder/recommendation_scan_builder.go` — the NEW
  `BuildRecommendationScanCronJob` builder (C.5).
- `internal/controller/admin_controller.go` — `reconcileStorage` (R.1/C.1/C.3/R.5),
  `ensureRecommendationScanCronJob` / `removeRecommendationScanCronJob` (C.5 + metric).
- `internal/builder/scenario115_recommendation_scan_builder_test.go` — the builder
  **unit** layer.
- `internal/controller/scenario115_storage_management_test.go` — the controller
  **unit** layer.
- `test/cases/scenario115_storage_management_cases.go` — the per-rule catalog.
- `test/functional/scenario115_storage_management_test.go` — the **functional** layer.
- `test/integration/scenario115_storage_management_test.go` — the integration layer.
- `test/e2e/scenario115_storage_management_e2e_test.go` — the **live** layer
  (apply full block + CronJob exists + `StorageConfigured=True` + no errors).

### Scenario 116 — Disk Usage Monitoring (Status + Metric)

> **Status: Implemented (verified live).** The disk-usage monitoring rules
> **R.2, S.1, M.1** for `spec.storage.diskMonitoring` are implemented in
> `AdminReconciler.recordDiskUsage()`
> (`internal/controller/admin_controller.go`), wired into both
> `reconcileStorage()` (R.2) and the steady-state `refreshStorageOnSteadyState()`
> (steady-state growth tracking), and backed by the NEW DB method
> `GetDiskUsagePercent()` (`internal/db/client.go`). `recordDiskUsage` mirrors
> `recordTableBloatRatios` (best-effort, non-fatal, dbFactory client + 10s timeout
> + span). The behavior was **verified live** against the deployed
> operator/cluster: `status.diskUsagePercent` was populated with the measured
> value and the `cloudberry_disk_usage_percent` gauge matched it. These
> `R.*`/`S.*`/`M.*` IDs are **storage-recommendations-scoped**.

Acceptance scenario: *"With `spec.storage.diskMonitoring: true`, verify
reconcileStorage MEASURES disk usage each reconcile via `GetDiskUsagePercent()`
(R.2), populates `status.diskUsagePercent` with the measured value so it tracks
growth (S.1), and publishes the `cloudberry_disk_usage_percent` gauge from the
SAME measured value so the metric matches the status (M.1 == S.1). When
`gp_toolkit.gp_disk_free` is unavailable the controller SKIPS without fabricating
a value. Cross-check the metric against actual `df` filesystem usage on a segment
data volume."*

#### Verification mapping

| Rule | Assertion |
|------|-----------|
| R.2 | `reconcileStorage()` (and steady-state `refreshStorageOnSteadyState()`) call `recordDiskUsage`, which measures disk usage via `GetDiskUsagePercent()`. The query reads `gp_toolkit.gp_disk_free` and returns the **MAX worst-case** `usage% = 100*(df_total-df_free)/df_total` (clamped `0..100`, divide-by-zero guarded) across segment data volumes. **HONEST fallback:** when the view/columns are absent (SQLSTATE `42P01`/`42703`) it returns `ErrDiskUsageUnavailable` and the controller **SKIPS** — it does **not** fabricate a value or overwrite the prior status. |
| S.1 | `status.diskUsagePercent` is set to the **current measured value** (no longer a static `0`) and persisted via `patchStatus`; it **tracks growth** as data grows and is never sticky (each reconcile writes the current measurement). |
| M.1 | The `cloudberry_disk_usage_percent{cluster,namespace}` gauge is published FROM THE SAME local measured value (`r.metrics.SetDiskUsagePercent(...)`), so the metric **matches** the status. The `M.1 == S.1` invariant holds **by construction**: both derive from the single measured `pct`; the stale `Status.DiskUsagePercent` is never read to publish the gauge. |
| df cross-check | The published metric can be validated against actual `df` filesystem usage on a segment data volume (exercised at live deployment time). |

#### Measurement source + honest fallback

- **Source** — `GetDiskUsagePercent()` runs `diskUsagePercentQuery` against
  `gp_toolkit.gp_disk_free`, which exposes per-segment volume capacity figures
  (`df_total` / `df_free`). The query computes `100*(df_total-df_free)/df_total`
  per row (guarded against `df_total = 0`), takes `MAX` (the most-full volume —
  the right signal for a "running out of disk" alert), `COALESCE`s to `0` when no
  rows are returned, and the Go layer **clamps** the result to `0..100` so a
  malformed view can never publish an out-of-range value.
- **Honest fallback** — when `gp_toolkit.gp_disk_free` (or an expected column) is
  not available on this server version (SQLSTATE `42P01` undefined table / `42703`
  undefined column), `GetDiskUsagePercent` returns the sentinel
  `db.ErrDiskUsageUnavailable`. `recordDiskUsage` treats this (and any
  NewClient/missing-dbFactory failure) as a **SKIP**: it logs and returns
  **without** writing a value, so a transient DB outage never reports a misleading
  "disk empty" signal and the prior status is preserved.

#### Test layers

- **Unit** — DB-direct against `GetDiskUsagePercent` incl. the worst-case `MAX`,
  the `0..100` clamp, and the `ErrDiskUsageUnavailable` honest fallback
  (`internal/db/scenario116_disk_usage_test.go`); controller-direct against
  `recordDiskUsage` asserting S.1 (status populated) and the M.1 == S.1 invariant
  (metric published from the measured value, skip-on-error)
  (`internal/controller/scenario116_disk_usage_test.go`).
- **Functional** — `test/functional/scenario116_disk_usage_test.go`.
- **Integration** — `test/integration/scenario116_disk_usage_test.go`.
- **E2E (live)** — `status.diskUsagePercent` populated + `cloudberry_disk_usage_percent`
  gauge matches the status + `df` cross-check on a segment data volume
  (`test/e2e/scenario116_disk_usage_e2e_test.go`).

#### Artifacts

- `internal/db/client.go` — the NEW `GetDiskUsagePercent` / `diskUsagePercentQuery`
  and the `ErrDiskUsageUnavailable` sentinel (R.2 measurement primitive).
- `internal/controller/admin_controller.go` — `recordDiskUsage` (R.2/S.1/M.1),
  wired into `reconcileStorage` and `refreshStorageOnSteadyState`.
- `internal/db/scenario116_disk_usage_test.go` — the DB **unit** layer.
- `internal/controller/scenario116_disk_usage_test.go` — the controller **unit** layer.
- `test/cases/scenario116_disk_usage_cases.go` — the per-rule catalog.
- `test/functional/scenario116_disk_usage_test.go` — the **functional** layer.
- `test/integration/scenario116_disk_usage_test.go` — the integration layer.
- `test/e2e/scenario116_disk_usage_e2e_test.go` — the **live** layer
  (status populated + metric matches status + `df` cross-check).

### Scenario 117 — Recommendation Scan Across All Four Types

> **Status: Implemented (verified live).** The recommendation-scan rules
> **S.2, M.2, R.3, R.4, RT.1–RT.4, C.6–C.9, M.4** for
> `spec.storage.recommendationScan` are implemented in
> `AdminReconciler.recordRecommendations()`
> (`internal/controller/admin_controller.go`) — wired into both
> `reconcileStorage()` (R.3) and the steady-state `refreshStorageOnSteadyState()`
> — backed by the four NEW THRESHOLD-AWARE DB methods
> `Get{Bloat,Skew,Age,IndexBloat}Recommendations`
> (`internal/db/client.go`). The behavior was **verified live** against the
> deployed operator/cluster: per-type recommendations were triggered via DB
> fixtures and both the `cloudberry_recommendations_total{type}` gauges and
> `status.recommendationCount` were populated. These `S.*`/`M.*`/`R.*`/`RT.*`/`C.*`
> IDs are **storage-recommendations-scoped**.

Acceptance scenario: *"With `spec.storage.recommendationScan.enabled: true`,
verify reconcile runs all FOUR threshold-aware scans (bloat, skew, age,
index_bloat) gated on the CRD thresholds (R.3/C.6–C.9), sets
`cloudberry_recommendations_total{type}` for ALL four types including `0` (M.2),
sets `status.recommendationCount` to the sum of the per-type active counts so
`M.2 == count` (S.2/R.4), and publishes `cloudberry_table_bloat_ratio{table}`
from the bloat scan (M.4). A recommendation must CLEAR when its metric drops
below the threshold, and APPEAR/DISAPPEAR at the threshold boundary; an
unavailable `gp_toolkit` view must yield an HONEST `0`, never a fabricated
count."*

#### Precondition (DB state per type)

A live database whose state is shaped per type via DB fixtures: tables with a
controllable dead-tuple ratio (bloat), a controllable distribution-skew
coefficient (skew), a controllable `age(relfrozenxid)` (XID age), and indexes
with a controllable estimated bloat (index bloat). Each fixture can be nudged
across its CRD threshold to drive the APPEAR / CLEAR / BOUNDARY transitions.

#### The four sub-scenarios

| Sub | Type | Req IDs | Behavior |
|-----|------|---------|----------|
| 117a | Table bloat | RT.1 / C.6 / M.4 | Gate on `dead_pct >= bloatThreshold` (`pg_stat_user_tables`); the bloated table is recommended AND its dead-tuple `%` appears on `cloudberry_table_bloat_ratio{table}` (M.4); the recommendation **CLEARS** (and the per-type gauge resets toward `0`) when bloat drops below the threshold. |
| 117b | Data skew | RT.2 / C.7 | Gate on `skccoeff >= skewThreshold` via `gp_toolkit.gp_skew_coefficients`; the recommendation **APPEARS / DISAPPEARS** exactly at the `skewThreshold` boundary. |
| 117c | XID age | RT.3 / C.8 | Gate on `age(relfrozenxid) >= ageThreshold` (`pg_class`/`pg_namespace`, `relkind IN ('r','m')`, user schemas) — REAL freeze age, not a dead-tuple proxy. |
| 117d | Index bloat | RT.4 / C.9 | Gate on the portable estimated `bloat_pct >= indexBloatThreshold` (`pg_class`/`pg_index`/`pg_namespace`) — not the old "size > 0" gate. |

#### After-all assertions (one scan pass)

| Rule | Assertion |
|------|-----------|
| R.3 | `reconcileStorage()` PROCESSES the enabled `recommendationScan` config and runs all four scans through one `db.RecommendationThresholds`; best-effort and non-fatal (a `Get*` error skips that type without failing the reconcile or fabricating a count). |
| R.4 / S.2 | `status.recommendationCount == sum` of the four per-type active counts (the CURRENT total, never the stale prior value), persisted via the end-of-reconcile status patch. |
| M.2 | `cloudberry_recommendations_total{type}` is set for ALL four types incl. `0`, and the **`M.2 == count` invariant** holds: `status.recommendationCount` equals the sum over types of the value passed to `SetRecommendationsTotal{type}` (both from the SAME per-type counts in one pass). |
| M.4 | `cloudberry_table_bloat_ratio{table}` is published from the SAME bloat scan as the `type=bloat` count (one bloat query per reconcile), capped at the top-20 tables. |

#### Source + honest fallback

- **Source** — bloat reads `pg_stat_user_tables`; skew reads
  `gp_toolkit.gp_skew_coefficients` (`skccoeff`); age reads
  `age(relfrozenxid)` on `pg_class`; index bloat estimates from
  `pg_class`/`pg_index`/`pg_namespace`. Each query binds its CRD threshold as
  parameter `$1` (injection-safe), never string-interpolated.
- **Honest fallback (NO fabrication)** — when a `gp_toolkit` view or expected
  catalog column is unavailable (SQLSTATE `42P01`/`42703`), the affected `Get*`
  method SKIPS + logs at debug and returns no rows; that type counts `0`. The
  skew path explicitly does NOT substitute the old row-count proxy, age does NOT
  revert to a dead-tuple proxy, and index bloat does NOT revert to the raw
  size gate. "No rows" and "unmeasurable" are honestly indistinguishable (count
  `0`).

#### Test layers

- **Unit** — DB-direct against the four threshold-aware queries incl. the gate on
  each CRD threshold and the honest `42P01`/`42703` fallback
  (`internal/db/scenario117_recommendation_scan_test.go`); controller-direct
  against `recordRecommendations` asserting the four-type scan (R.3), the
  `count == sum` total (S.2/R.4), the per-type gauge incl. `0` and the
  `M.2 == count` invariant, the M.4 table-bloat publish, and the CLEAR/BOUNDARY
  transitions (`internal/controller/scenario117_recommendation_scan_test.go`).
- **Functional** — `test/functional/scenario117_recommendation_scan_test.go`.
- **Integration** — `test/integration/scenario117_recommendation_scan_test.go`.
- **E2E (live)** — per-type recommendations triggered via DB fixtures →
  `cloudberry_recommendations_total{type}` populated for all four types +
  `status.recommendationCount == sum` + `cloudberry_table_bloat_ratio{table}` +
  CLEAR/BOUNDARY behavior
  (`test/e2e/scenario117_recommendation_scan_e2e_test.go`).

#### Artifacts

- `internal/db/client.go` — the four NEW threshold-aware
  `Get{Bloat,Skew,Age,IndexBloat}Recommendations` methods and the
  `RecommendationThresholds` struct (RT.1–RT.4 / C.6–C.9 + honest fallback).
- `internal/controller/admin_controller.go` — `recordRecommendations`
  (R.3/R.4/S.2/M.2/M.4 + the `M.2 == count` invariant), wired into
  `reconcileStorage` and `refreshStorageOnSteadyState`.
- `internal/db/scenario117_recommendation_scan_test.go` — the DB **unit** layer.
- `internal/controller/scenario117_recommendation_scan_test.go` — the controller
  **unit** layer.
- `test/cases/scenario117_recommendation_scan_cases.go` — the per-rule catalog.
- `test/functional/scenario117_recommendation_scan_test.go` — the **functional** layer.
- `test/integration/scenario117_recommendation_scan_test.go` — the integration layer.
- `test/e2e/scenario117_recommendation_scan_e2e_test.go` — the **live** layer
  (per-type metrics + count populated + CLEAR/BOUNDARY).

### Scenario 118 — Scan Scheduling and Duration Limit

> **Status: Implemented (verified live).** The scan-scheduling and
> duration-limit rules **C.5, C.10, M.3** for `spec.storage.recommendationScan`
> (plus the new validation rule **W.5**) are implemented in
> `AdminReconciler.recordRecommendations()` / `resolveScanDuration()`
> (`internal/controller/admin_controller.go`), with the scheduled CronJob built by
> `BuildRecommendationScanCronJob` (Scenario 115,
> `internal/builder/recommendation_scan_builder.go`) and the duration cap enforced
> by a single shared `context.WithTimeout`. W.5 lives in
> `internal/webhook/validating.go::validateStorageManagement`. The behavior was
> **verified live** against the deployed operator/cluster: the
> `<cluster>-recommendation-scan` CronJob fired for the configured schedule and
> populated `cloudberry_recommendation_scan_duration_seconds`, and a tight
> `scanDuration` cap truncated the scan (status flag + counter + partial results).
> These `C.*`/`M.*`/`W.*` IDs are **storage-recommendations-scoped** (W.5 is
> **DISTINCT** from the data-loading `W.*` family in
> [spec 12](12-data-loading-spec.md#webhook-validation)).

Acceptance scenario: *"With `spec.storage.recommendationScan.enabled: true`,
verify the `<cluster>-recommendation-scan` CronJob is created for the configured
`schedule` (C.5/M.3) and the `cloudberry_recommendation_scan_duration_seconds`
histogram is populated after each scan (M.3); and verify that `recordRecommendations`
CAPS the scan at `scanDuration` via `resolveScanDuration` (C.10) — when the shared
context hits the deadline mid-run it records partial per-type counts (un-run types
count `0`, no fabrication), sets `status.recommendationScanTruncated=true`,
increments `cloudberry_recommendation_scan_truncated_total{cluster,namespace}`, and
sets `status.lastRecommendationScanTime`, with the M.3 duration metric reflecting
the capped run. The truncation flag is never sticky (set `false` on every
non-truncated scan). An invalid `scanDuration` is rejected by the webhook (W.5)."*

#### The two sub-scenarios

| Sub | Req IDs | Behavior |
|-----|---------|----------|
| 118a | C.5 / M.3 | **Schedule fires.** The recommendation-scan CronJob `<cluster>-recommendation-scan` is created for the configured `schedule` (via `BuildRecommendationScanCronJob` + `ensureRecommendationScanCronJob`, Scenario 115); after each scan in `recordRecommendations` the `cloudberry_recommendation_scan_duration_seconds` histogram is populated via `ObserveRecommendationScanDuration` (M.3). |
| 118b | C.10 | **`scanDuration` cap.** `recordRecommendations` bounds the WHOLE scan with a single shared `context.WithTimeout(ctx, resolveScanDuration(scan, logger))` budget across the four `Get*` queries. On a deadline-trip mid-run it records **partial results** (only the per-type counts that completed; un-run types count `0`, **no fabrication**), sets `status.recommendationScanTruncated=true`, increments `cloudberry_recommendation_scan_truncated_total{cluster,namespace}`, and sets `status.lastRecommendationScanTime`; the **M.3 duration metric reflects the capped run**. |

#### `resolveScanDuration` fallback / clamp / verbatim policy

`resolveScanDuration` parses `scanDuration` with `time.ParseDuration` and applies
a defensive policy (constants `defaultScanDuration = 10s`,
`maxScanDuration = 24h`):

| Input `scanDuration` | Resolved cap |
|----------------------|--------------|
| empty (`""`)         | `10s` default (preserves the historical hardcoded behavior) |
| unparseable          | `10s` default (and a `Warn` log) — defense-in-depth; W.5 normally rejects this upstream |
| `<= 0`               | `10s` default |
| `> 24h` (e.g. `2000h`) | clamped down to `24h` (ceiling guard) |
| otherwise            | the parsed value **verbatim** (a tiny `10ms` deterministically trips the deadline — no production floor) |

#### Never-sticky truncation flag

`status.recommendationScanTruncated` is set on **every** scan so it always
reflects the latest run. The field deliberately **omits `omitempty`**, so a
`false` value is always serialized in the status MergePatch — a previous `true`
reliably clears to `false` on the next non-truncated scan (the "never sticky"
guarantee, exercised by the controller test that seeds a sticky prior `true`).
`status.lastRecommendationScanTime` is set each scan (capped or complete).

#### New status fields + metric

- `status.recommendationScanTruncated` (`bool`) — `true` when the most recent
  scan hit the `scanDuration` cap.
- `status.lastRecommendationScanTime` (`*metav1.Time`) — timestamp of the most
  recent scan.
- `cloudberry_recommendation_scan_truncated_total{cluster,namespace}` (Counter) —
  incremented when a scan is capped.

#### W.5 — `scanDuration` must be a valid Go duration (webhook)

An invalid `scanDuration` is rejected with
`storage.recommendationScan.scanDuration "<v>" must be a valid Go duration` (in
`validateStorageManagement`; storage-recommendations **W.5**, distinct from the
data-loading `W.*` family). W.5 is **`enabled`-gated** and **webhook-authoritative**;
it only checks parseability (an empty value is accepted — the controller falls
back to the `10s` default), while the clamp/fallback policy lives in
`resolveScanDuration`.

#### Test layers

- **Unit** — controller-direct against `resolveScanDuration` (the
  fallback/clamp/verbatim table) and `recordRecommendations` (the C.10 cap →
  truncate + partial results + counter + `lastRecommendationScanTime` + the
  never-sticky reset, incl. the steady-state path)
  (`internal/controller/scenario118_scan_duration_test.go`); webhook-direct
  against W.5 (`internal/webhook/scenario118_validation_test.go`); metrics-direct
  against `IncRecommendationScanTruncated`
  (`internal/metrics/scenario118_metrics_test.go`).
- **Functional** — `test/functional/scenario118_scan_scheduling_test.go` (completed
  scan leaves the flag `false`; capped scan sets it `true` + sets
  `lastRecommendationScanTime`; `enabled: false` no-op).
- **Integration** — `test/integration/scenario118_scan_scheduling_test.go`.
- **E2E (live)** — `test/e2e/scenario118_scan_scheduling_e2e_test.go` (CronJob +
  M.3 populated; a tight `scanDuration` cap → `status.recommendationScanTruncated`
  + `cloudberry_recommendation_scan_truncated_total` increases).

#### Artifacts

- `internal/controller/admin_controller.go` — `resolveScanDuration` (C.10 policy)
  and `recordRecommendations` (C.10 cap + truncation signal + M.3 observe).
- `internal/webhook/validating.go` — `validateStorageManagement` (W.5).
- `internal/metrics/metrics.go` — `IncRecommendationScanTruncated` /
  `cloudberry_recommendation_scan_truncated_total`.
- `api/v1alpha1/types.go` — `RecommendationScanTruncated` /
  `LastRecommendationScanTime` status fields.
- `internal/controller/scenario118_scan_duration_test.go` — the controller
  **unit** layer.
- `internal/webhook/scenario118_validation_test.go` — the webhook **unit** layer.
- `internal/metrics/scenario118_metrics_test.go` — the metrics **unit** layer.
- `test/cases/scenario118_scan_scheduling_cases.go` — the per-rule catalog.
- `test/functional/scenario118_scan_scheduling_test.go` — the **functional** layer.
- `test/integration/scenario118_scan_scheduling_test.go` — the integration layer.
- `test/e2e/scenario118_scan_scheduling_e2e_test.go` — the **live** layer
  (CronJob + M.3 populated; cap → truncated flag + counter).

### Scenario 119 — All API Endpoints

> **Status: Implemented (verified live).** The six storage REST endpoints
> **P.1–P.6** are implemented in `internal/api/server.go` (the six handlers +
> their `collect*` helpers) and now return REAL data — backed by the DB methods
> on `internal/db/client.go` (`GetDiskUsage` / `GetStorageDiskUsage`, the new
> `GetTables`, the fixed `GetTableDetails` (+`IndexSizes`), the four
> threshold-aware `Get{Bloat,Skew,Age,IndexBloat}Recommendations`, and
> `GetUsageReport`). The behavior was **verified live** against the deployed
> operator API. The full request params + JSON response shapes are in
> [§API Response Shapes](#api-response-shapes). These `P.*` IDs are
> **storage-recommendations-scoped** and DISTINCT from the data-loading `P.*`
> family in [spec 12](12-data-loading-spec.md).

Acceptance scenario: *"For each of the six storage endpoints P.1–P.6, verify it
returns REAL data from the database (not a static placeholder): disk-usage
carries the per-database + per-segment breakdown with `diskUsagePercent` matching
`status.diskUsagePercent`; tables/table-detail carry live size/bloat/skew/row
data; recommendations carry the four threshold-aware types; the scan POST returns
202 when enabled and 400 when disabled; the usage-report is soft-gated on
`usageReport.enabled`. Reads require Basic, the scan requires Operator. Every read
is best-effort: a DB-unavailable / query error returns an HONEST empty payload
with HTTP 200, never a 500; a missing cluster returns 404."*

#### The six sub-scenarios

| Sub | Req ID | Endpoint | Behavior |
|-----|--------|----------|----------|
| 119a | P.1 | `GET …/storage/disk-usage` | `{cluster, diskUsagePercent, diskUsage:[per-db], diskUsageBySegment:[per-tablespace/segment]}`. `diskUsagePercent` is sourced ONLY from `status.diskUsagePercent` so **`P.1 == status.diskUsagePercent`** (≡ Scenario 116 `M.1 == S.1`); `diskUsage` (`collectDiskUsage` → `GetDiskUsage`) and `diskUsageBySegment` (`collectStorageBreakdown` → `GetStorageDiskUsage`) are purely additive and best-effort (honestly empty when unreachable). |
| 119b | P.2 | `GET …/storage/tables` | `{cluster, tables:[{schema,table,sizeBytes,sizeHuman,bloatPercent,skewPercent,rowCount}], total}` via the new `GetTables` (`pg_stat_user_tables` + `pg_total_relation_size`; bloat `FLOOR(n_dead_tup*100.0/(n_live_tup+n_dead_tup))::int`; skew best-effort from `gp_toolkit.gp_skew_coefficients` with an HONEST fallback to `0`). |
| 119c | P.3 | `GET …/storage/tables/{schema}/{table}` | `{schema,table,sizeBytes,sizeHuman,rowCount,bloatPercent,skewPercent,lastVacuum,lastAnalyze,indexSizes:[{name,sizeBytes,sizeHuman}]}` via `GetTableDetails`. **Bug fix:** bloat AND skew are now both populated (bloat via the `FLOOR(...)::int` NUMERIC-safe cast; skew enriched best-effort from `gp_toolkit`), and `IndexSizes` is added via `collectIndexSizes` (`pg_index`/`pg_class`/`pg_namespace`, largest first). DB-unavailable / not-found / query error → honest minimal `{schema, table}` (HTTP 200). |
| 119d | P.4 | `GET …/storage/recommendations` | `{cluster, recommendations:[{type,target:"schema.table",value,ratio,severity,description}], recommendationCount, total}` via `collectRecommendations` (the four threshold-aware `Get*`). `recommendationCount` is the LIVE total when the DB was reachable, else falls back to the cached `status.recommendationCount`. |
| 119e | P.5 | `POST …/storage/recommendations/scan` | `202 {status:"scan initiated", cluster}` when `recommendationScan.enabled` (each POST runs a best-effort scan that advances the `cloudberry_recommendation_scan_duration_seconds` count, **independent of the cron**); `400 RECOMMENDATION_SCAN_NOT_ENABLED` when disabled/absent. **Permission: Operator.** |
| 119f | P.6 | `GET …/storage/usage-report` | `{cluster, month, entries, total, usageReportEnabled}` via `collectUsageReport` (`GetUsageReport`). **Soft-gated:** disabled → `200` with `usageReportEnabled:false` + empty `entries` (a READ, so NOT a `400`); enabled → `entries` via `GetUsageReport` + `usageReportEnabled:true`. Optional `?month=YYYY-MM`. |

#### Auth model

- **Reads (P.1–P.4, P.6)** require **Basic** permission
  (`PermissionBasic`).
- **The scan POST (P.5)** requires **Operator** permission
  (`PermissionOperator`).

#### Best-effort / non-fatal (honest empty, NOT 500)

Every read collector mirrors `collectDiskUsage`'s best-effort/non-fatal handling:
a nil `dbFactory`, a `NewClient` error, or a query error yields an honest **empty**
payload with **HTTP 200**, never a `500`. A missing cluster is the only hard
error: `404 CLUSTER_NOT_FOUND`. The collectors never return `nil` slices (always
`[]`), and `collectTableDetail` returns `nil` so its handler falls back to the
minimal `{schema, table}` shape.

#### Honest sources + skew fallback

- **P.1** — `GetDiskUsage` (per-database `pg_database_size`) + `GetStorageDiskUsage`
  (per-tablespace/segment). `diskUsagePercent` is read ONLY from
  `status.diskUsagePercent` (the `P.1 == status` invariant).
- **P.2 / P.3** — `GetTables` / `GetTableDetails` read `pg_stat_user_tables` +
  `pg_total_relation_size`; bloat uses the `FLOOR(...)::int` NUMERIC-safe cast
  (the Scenario 117 lesson — Cloudberry 2.1.0 returns the division as a fractional
  NUMERIC that a plain `int` cast cannot scan). **Skew is best-effort:** it is
  enriched from `gp_toolkit.gp_skew_coefficients` and, when the view/columns are
  absent (SQLSTATE `42P01`/`42703`) or any query/scan error occurs, stays an
  **HONEST `0`** — never fabricated, never a row-count proxy. `IndexSizes` is also
  best-effort (empty when the catalog query fails).
- **P.4** — the four threshold-aware `Get*` (Scenario 117 sources + the same
  honest `42P01`/`42703` fallback).
- **P.6** — `GetUsageReport` (`pg_database_size` + `pg_stat_database`
  connections).

#### Test layers

- **Unit** — DB-direct against `GetTables` (incl. the skew fallback),
  `GetTableDetails` (the bloat/skew bug fix + the `IndexSizes` fallback), and
  `GetUsageReport` (`internal/db/scenario119_storage_api_test.go`); handler-direct
  against the six handlers incl. the auth model, the P.1==status invariant, the
  P.5 enabled/disabled split (and the per-POST duration-count advance), the P.6
  soft-disabled gate, and the best-effort honest-empty (200, not 500) paths
  (`internal/api/scenario119_storage_api_test.go`).
- **Functional** — `test/functional/scenario119_storage_api_test.go`.
- **Integration** — `test/integration/scenario119_storage_api_test.go`.
- **E2E (live)** — `test/e2e/scenario119_storage_api_e2e_test.go` (the six
  endpoints exercised against the deployed operator API: real data, the auth
  model, the 202/400 scan split, and the soft-gated usage-report).

#### Artifacts

- `internal/api/server.go` — the six handlers
  (`handleGetDiskUsage`, `handleListTables`, `handleGetTableDetail`,
  `handleListRecommendations`, `handleTriggerRecommendationScan`,
  `handleGetUsageReport`) + their `collect*` helpers
  (`collectDiskUsage`, `collectStorageBreakdown`, `collectTables`,
  `collectTableDetail`, `collectRecommendations`, `collectUsageReport`,
  `runRecommendationScan`).
- `internal/db/client.go` — `GetTables` (+ `collectTableSkew`),
  `GetTableDetails` (the bloat/skew fix + `collectIndexSizes`), and
  `GetUsageReport` (P.2/P.3/P.6 sources).
- `internal/db/scenario119_storage_api_test.go` — the DB **unit** layer.
- `internal/api/scenario119_storage_api_test.go` — the API handler **unit** layer.
- `test/cases/scenario119_storage_api_cases.go` — the per-endpoint catalog.
- `test/functional/scenario119_storage_api_test.go` — the **functional** layer.
- `test/integration/scenario119_storage_api_test.go` — the integration layer.
- `test/e2e/scenario119_storage_api_e2e_test.go` — the **live** layer
  (the six endpoints against the deployed operator API).

### Scenario 120 — Usage Reporting

> **Status: Implemented (verified live).** The usage-reporting rules **C.11**
> (report generation/content) and **C.13** (retrieval) for
> `spec.storage.usageReport` are implemented across the DB method
> `GetUsageReport` / `collectUsageReportTables` (`internal/db/client.go`), the API
> handler `handleGetUsageReport` / `collectUsageReport`
> (`internal/api/server.go`, endpoint **P.6**), and the
> `cloudberry-ctl storage usage-report` CLI command with the NEW `--month` flag
> (`cmd/cloudberry-ctl/main.go::newStorageCmd`). The behavior was **verified live**
> against the deployed operator API + CLI. These `C.*` IDs are
> **storage-recommendations-scoped**.

Acceptance scenario: *"With `spec.storage.usageReport.enabled: true` and
`monthly: true`, verify the operator produces an ON-DEMAND monthly usage report
(scoped/labeled by month) containing per-table AND per-database storage
consumption (C.11), retrievable via BOTH the API (P.6) and the CLI
`storage usage-report --month` (C.13). A disabled report
(`usageReport.enabled: false`) is UNAVAILABLE (`usageReportEnabled: false`). The
report is HONEST: no fabricated persisted history, so growth/queryCount stay `0`,
and the per-table breakdown falls back to empty on an honest `42P01`/`42703` /
query error rather than failing the report."*

#### C.11 — report generation + content model

- **On-demand, monthly scope.** `GetUsageReport(ctx, month)` is computed ON DEMAND
  from live catalog sizes; the optional `month` is carried verbatim onto every
  entry's `Month` field (scope/label only — there is no persisted month-over-month
  history).
- **Per-database + per-table content.** Each `db.UsageReportEntry` carries the
  per-database `SizeBytes`/`SizeHuman` (`pg_database_size`) + `Connections`
  (`pg_stat_database.numbackends`) **and** a `Tables []TableUsage{Schema, Table,
  SizeBytes, SizeHuman}` per-table breakdown.
- **Connected-db only (honest).** Because the pgx pool is single-database, the
  per-table `Tables` is attached **only** to the entry matching the connected
  database (`c.config.Database`) by `attachUsageReportTables`; other database
  entries honestly carry an empty/omitted `Tables` (the operator cannot size
  tables in a database it is not connected to).
- **Best-effort + bounded.** `collectUsageReportTables` runs
  `usageReportTablesQuery` (`pg_class`/`pg_namespace`, `relkind IN ('r','m')`,
  catalog/internal schemas — `pg_catalog`/`information_schema`/`gp_toolkit`/`pg_ext_aux`
  — excluded, `pg_total_relation_size`, largest-first, `LIMIT 50`). On an honest
  fallback (SQLSTATE `42P01` undefined table / `42703` undefined column) or any
  query/scan error it returns an **empty** slice (no fabrication) and the report
  still surfaces the per-database content.
- **Honest growth/queryCount.** `GrowthBytes`/`GrowthHuman`/`QueryCount` stay an
  honest `0`/empty — with no persisted baseline there is nothing to derive growth
  or a historical query count from.

#### C.13 — dual retrieval (API + CLI)

- **API P.6** — `GET /clusters/{name}/storage/usage-report` →
  `200 {cluster, month, entries:[…with tables…], total, usageReportEnabled}`
  (**Basic** permission). The optional `?month=YYYY-MM` scopes/labels the report.
  **Soft-gated:** disabled (`usageReport.enabled: false` or absent) → `200` with
  `usageReportEnabled: false` + empty `entries` (a READ, so NOT a `400`). Best-effort
  like the other P.* reads: a nil `dbFactory` / `NewClient` error / query error →
  honest empty `entries` with HTTP 200, never a 500; a missing cluster is
  `404 CLUSTER_NOT_FOUND`.
- **CLI** — `cloudberry-ctl storage usage-report --cluster X [--namespace …]
  [--month YYYY-MM]` GETs the P.6 endpoint. The NEW `--month` flag threads through
  as the `?month=` query param (encoded alongside `?namespace=` via a single
  `url.Values` builder); omitting it returns the current/unscoped report.

#### Disabled → unavailable

When `usageReport.enabled: false` (the Scenario 8 reference), the report is
**UNAVAILABLE**: P.6 returns `usageReportEnabled: false` with empty `entries`
(and the CLI surfaces the same payload). No report is fabricated.

#### Honest fallbacks (summary)

- **No persisted history** → `growthBytes`/`growthHuman`/`queryCount` = `0`/empty.
- **Single-db pool** → per-table `Tables` only on the connected-db entry.
- **`gp_catalog`/column absent (`42P01`/`42703`) or any tables-query error** →
  empty per-table breakdown, the per-database content still surfaces.
- **DB unreachable at the API layer** → honest empty `entries` (HTTP 200, not 500).

#### Test layers

- **Unit** — DB-direct against `GetUsageReport` / `collectUsageReportTables` incl.
  the per-table enrichment, the connected-db-only attachment, the `LIMIT 50`
  bound, and the honest `42P01`/`42703` / error fallback
  (`internal/db/scenario120_usage_report_test.go`); handler-direct against
  `handleGetUsageReport` incl. the soft-disabled gate, the `?month=` scope, the
  enriched `entries[].tables`, and the best-effort honest-empty (200, not 500)
  path (`internal/api/scenario120_usage_report_test.go`); CLI-direct against
  `newStorageCmd` incl. the `--month` flag threading `?month=`
  (`cmd/cloudberry-ctl/scenario120_usage_report_test.go`).
- **Functional** — `test/functional/scenario120_usage_report_test.go`.
- **Integration** — `test/integration/scenario120_usage_report_test.go`.
- **E2E (live)** — `test/e2e/scenario120_usage_report_e2e_test.go` (the enabled
  per-table + per-database report via API P.6 + CLI `--month`, and the
  disabled-unavailable behavior, against the deployed operator API + CLI).

#### Artifacts

- `internal/db/client.go` — `GetUsageReport` (C.11 report), `attachUsageReportTables`
  / `collectUsageReportTables` + `usageReportTablesQuery` (the per-table breakdown,
  connected-db only, `LIMIT 50`, honest fallback), and the `UsageReportEntry` /
  `TableUsage` types.
- `internal/api/server.go` — `handleGetUsageReport` / `collectUsageReport` (P.6,
  soft-gate + `?month=` + best-effort honest-empty).
- `cmd/cloudberry-ctl/main.go` — `newStorageCmd` (the `usage-report` command + the
  NEW `--month` flag threading `?month=`).
- `internal/db/scenario120_usage_report_test.go` — the DB **unit** layer.
- `internal/api/scenario120_usage_report_test.go` — the API handler **unit** layer.
- `cmd/cloudberry-ctl/scenario120_usage_report_test.go` — the CLI **unit** layer.
- `test/cases/scenario120_usage_report_cases.go` — the per-rule catalog.
- `test/functional/scenario120_usage_report_test.go` — the **functional** layer.
- `test/integration/scenario120_usage_report_test.go` — the integration layer.
- `test/e2e/scenario120_usage_report_e2e_test.go` — the **live** layer
  (enabled per-table + per-db report via API + CLI; disabled-unavailable).

### Scenario 121 — All CLI Commands

> **Status: Implemented (verified live).** The six storage CLI commands
> **L.1–L.6** are implemented in `cmd/cloudberry-ctl/main.go::newStorageCmd` as
> thin clients over the Scenario 119/120 storage API endpoints **P.1–P.6**. The
> behavior was **verified live** against the deployed operator API (all six
> commands run end-to-end). These storage-recommendations `L.*` IDs are
> **DISTINCT** from the data-loading `L.1`–`L.16` CLI family
> ([spec 12](12-data-loading-spec.md) / Scenario 108); the two `L.*` namespaces
> do not overlap.

Acceptance scenario: *"For each of the six storage CLI commands L.1–L.6, verify
it issues the correct request to its API endpoint P.1–P.6 and prints the
response: `storage disk-usage` (P.1), `storage tables list` (P.2),
`storage tables detail --schema public --table orders` (P.3),
`storage recommendations list` (P.4), `storage recommendations scan` (P.5,
POST), and `storage usage-report --month 2026-05` (P.6). L.3 must accept BOTH
the `--schema`/`--table` flags AND legacy positional args (flags win; a missing
schema/table is a clear usage error before any HTTP call). L.6's `--month` must
select the correct reporting period (the report is on-demand labeled by month —
honest, not a historical snapshot)."*

#### The six commands → endpoint mapping

| ID  | Command                          | Endpoint (method)                                   | Behavior |
|-----|----------------------------------|------------------------------------------------------|----------|
| L.1 | `storage disk-usage --cluster X` | `GET` P.1 `…/storage/disk-usage`                     | Prints disk usage (percent + per-database + per-segment breakdown). |
| L.2 | `storage tables list --cluster X` | `GET` P.2 `…/storage/tables`                        | Lists tables with size/bloat/skew/row-count storage info. |
| L.3 | `storage tables detail --cluster X --schema public --table orders` | `GET` P.3 `…/storage/tables/{schema}/{table}` | Prints table detail. Accepts the `--schema`/`--table` flags (canonical) AND legacy positional `[schema] [table]` args; flags take precedence when set, else fall back to positional via `resolveTableDetail`. A missing schema/table → clear usage error **before** any HTTP call (no half-built `/tables//` request). |
| L.4 | `storage recommendations list --cluster X` | `GET` P.4 `…/storage/recommendations`      | Lists active recommendations (the four threshold-aware types). |
| L.5 | `storage recommendations scan --cluster X` | `POST` P.5 `…/storage/recommendations/scan` | Triggers a best-effort recommendation scan (Operator perm at the API). |
| L.6 | `storage usage-report --cluster X --month 2026-05` | `GET` P.6 `…/storage/usage-report?month=YYYY-MM` | Prints the usage report for the selected month. The optional `--month` threads through as the `?month=` query param the API honors; omitting it returns the current/unscoped report. |

#### L.3 — `--schema`/`--table` flags + positional args

`storage tables detail` accepts the `--schema`/`--table` flags (the canonical
form per the [§CLI Commands](#cli-commands) table) AND the legacy positional
`[schema] [table]` args for backward compatibility. `resolveTableDetail` resolves
the pair with the flags taking precedence when non-empty; otherwise it falls back
to `args[0]=schema` / `args[1]=table`. When either resolves empty it returns the
clear usage error `schema and table are required (use --schema/--table or
positional args)` **BEFORE** any HTTP call is issued — so no malformed
`/tables//` request reaches the API.

#### L.6 — `--month` selects the reporting period (honest on-demand labeling)

`storage usage-report`'s optional `--month` flag scopes/labels the report: it is
encoded (alongside `--namespace`) through a single `url.Values` builder as the
`?month=` query param that the API already honors. The report is computed **ON
DEMAND** from live catalog sizes and the month is a label/scope only — HONEST:
there is no persisted month-over-month history, so the report is NOT a historical
snapshot (`growthBytes`/`queryCount` stay `0`; see
[Scenario 120](#scenario-120--usage-reporting) C.11). Omitting `--month` returns
the current/unscoped report.

#### Test layers

- **Unit** — CLI-direct against `newStorageCmd` (the six commands' paths, the L.3
  flags + positional resolution via `resolveTableDetail`, and the L.6 `--month`
  threading `?month=`) (`cmd/cloudberry-ctl/scenario121_storage_cli_test.go`).
- **Functional** — `test/functional/scenario121_storage_cli_test.go`.
- **Integration** — `test/integration/scenario121_storage_cli_test.go`.
- **E2E (live)** — `test/e2e/scenario121_storage_cli_e2e_test.go` (all six
  commands exercised against the deployed operator API).

#### Artifacts

- `cmd/cloudberry-ctl/main.go` — `newStorageCmd` (the six commands L.1–L.6, the
  L.3 `--schema`/`--table` flags) and `resolveTableDetail` (L.3 flag+positional
  resolution).
- `cmd/cloudberry-ctl/scenario121_storage_cli_test.go` — the CLI **unit** layer.
- `test/cases/scenario121_storage_cli_cases.go` — the per-command catalog.
- `test/functional/scenario121_storage_cli_test.go` — the **functional** layer.
- `test/integration/scenario121_storage_cli_test.go` — the integration layer.
- `test/e2e/scenario121_storage_cli_e2e_test.go` — the **live** layer
  (all six commands against the deployed operator API).

### Scenario 122 — Disabled States (C.2, C.4, C.12)

> **Status: Implemented (verified live).** The three storage disabled-state
> behaviors **C.2** (`diskMonitoring: false`), **C.4**
> (`recommendationScan.enabled: false`) and **C.12** (`usageReport.enabled:
> false`) — plus re-enablement — are implemented in
> `AdminReconciler.reconcileStorage()` / `refreshStorageOnSteadyState()` via the
> NEW `clearStorageSignals` / `clearRecommendations` helpers
> (`internal/controller/admin_controller.go`) and the soft-gated
> `handleGetUsageReport` (`internal/api/server.go`). The behavior was **verified
> live** against the deployed operator/cluster: each feature disabled → its signal
> reset; each feature re-enabled → reactivation. These `C.*` IDs are
> **storage-recommendations-scoped**.

Acceptance scenario: *"For each of the three storage features (disk monitoring,
recommendation scan, usage report), disable it and verify the operator resets the
corresponding signal to an explicit disabled state (NOT a frozen stale value);
then re-enable it and verify the signal reactivates. Disk monitoring off → no
measurement, `status.diskUsagePercent` + `cloudberry_disk_usage_percent` reset to
0, scan CronJob GC'd (C.2). Recommendation scan off → no CronJob/scan, no
recommendations, `status.recommendationCount` + `cloudberry_recommendations_total{type}`
reset to 0 for all four types, POST scan → 400 (C.4). Usage report off → API/CLI
soft-gate (`usageReportEnabled: false` + empty, 200 not 400) (C.12). Re-enabling
each feature restores its live signal."*

#### The three sub-scenarios

| Sub | Req ID | Feature flipped off | Disabled behavior | Re-enable reactivation |
|-----|--------|---------------------|-------------------|------------------------|
| 122a | C.2 | `spec.storage.diskMonitoring: false` | `reconcileStorage()` does **NOT** measure disk usage (the R.1 gate early-returns before `recordDiskUsage`). `status.diskUsagePercent` is **RESET to `0`** and the `cloudberry_disk_usage_percent` gauge is published **`0`** via `clearStorageSignals` (an explicit "monitoring off" signal — the operator resets-to-0 on disable, NOT a frozen stale reading; the live check asserts the gauge does not ADVANCE after the flip). The recommendation-scan CronJob is **GC'd** (owned by the steady-state `removeRecommendationScanCronJob`). The cleared status is persisted only when there was an actual stale value (R6 anti-churn). | `diskMonitoring: true` → `recordDiskUsage` resumes; `status.diskUsagePercent` + the gauge **repopulate** from the live measurement (S.1/M.1). |
| 122b | C.4 | `spec.storage.recommendationScan.enabled: false` | **No** scan CronJob is ensured (builder returns nil → delete-if-exists) and `recordRecommendations` does **NOT** run, so recommendations are **NOT produced**. `clearRecommendations` **RESETS** `status.recommendationCount` to **`0`** and publishes `cloudberry_recommendations_total{type}` **`0`** for **ALL four** types (`bloat`/`skew`/`age`/`index_bloat`, reusing the canonical `recommendationTypes` slice), and resets `status.recommendationScanTruncated` to `false`. The explicit-trigger `POST …/storage/recommendations/scan` returns **`400 RECOMMENDATION_SCAN_NOT_ENABLED`**; `GET …/storage/recommendations` remains a **live read**. | `recommendationScan.enabled: true` → the CronJob + `recordRecommendations` resume; counts/gauges **repopulate**. |
| 122c | C.12 | `spec.storage.usageReport.enabled: false` | The usage-report endpoint/CLI **report the feature disabled** rather than erroring. P.6 is a READ, so it is a **soft-gate** (API P.6): `200 {usageReportEnabled: false, entries: []}` — **NOT a `400`** (the `*_NOT_ENABLED` 400 is reserved for the mutating scan action, C.4). The `cloudberry-ctl storage usage-report` CLI **inherits** the same payload. | `usageReport.enabled: true` → `entries` populate and `usageReportEnabled: true`. |

#### Reset-to-0 design (explicit disabled signal)

On disable the operator deliberately **resets the signal to `0`** rather than
leaving the last measured value frozen, so `0` is an explicit "feature off"
signal a reader can act on (the gauge does not ADVANCE after the flip). Two
disabled paths converge on the same helpers so they behave identically:

- the **spec-driven** `reconcileStorage()` R.1 early-return (whole storage block
  off → `clearStorageSignals`; or `diskMonitoring` on but the scan disabled →
  `clearRecommendations`), and
- the **steady-state** `refreshStorageOnSteadyState()` storage-off block (same
  `clearStorageSignals` + CronJob GC).

To make the `0` reliably persist, `status.diskUsagePercent` and
`status.recommendationCount` had their `omitempty` JSON tag **REMOVED** — a `0`
value is always serialized in the status MergePatch, so a previous non-zero value
reliably clears to `0` and `kubectl get` shows `0` on a disabled cluster (without
`omitempty`, a `0` would be elided from the patch and the stale value would
linger). `clearStorageSignals` persists the cleared status ONLY when there was an
actual stale value (R6), so the never-configured common case does not churn.

#### Honest limitation — `cloudberry_table_bloat_ratio` is NOT cleared on disable

The per-table `cloudberry_table_bloat_ratio{cluster,namespace,table}` gauge (M.4)
is **NOT** cleared when the recommendation scan is disabled. It is a per-table
cardinality signal the operator does **not enumerate** on the disabled path (there
is no precise label set to zero), so it is deliberately left alone — an HONEST,
documented limitation. The CronJob GC and the on-disable count clear
(`status.recommendationCount` + `cloudberry_recommendations_total{type}` → `0`)
are the primary disabled-state signals.

#### Test layers

- **Unit** — controller-direct against `clearStorageSignals` /
  `clearRecommendations` and their wiring in `reconcileStorage` /
  `refreshStorageOnSteadyState`, asserting C.2 (disk reset to 0 + gauge 0 + CronJob
  GC), C.4 (count + all-four per-type gauges reset to 0, truncated flag cleared,
  table_bloat_ratio NOT cleared), and the re-enable reactivation
  (`internal/controller/scenario122_disabled_states_test.go`); handler-direct
  against the C.4 POST-scan `400 RECOMMENDATION_SCAN_NOT_ENABLED` and the C.12 P.6
  soft-gate (`200 {usageReportEnabled:false, entries:[]}`, not 400)
  (`internal/api/scenario122_disabled_states_test.go`).
- **Functional** — `test/functional/scenario122_disabled_states_test.go`.
- **Integration** — `test/integration/scenario122_disabled_states_test.go`.
- **E2E (live)** — each feature disabled → signal reset; re-enabled →
  reactivation (`test/e2e/scenario122_disabled_states_e2e_test.go`).

#### Artifacts

- `internal/controller/admin_controller.go` — `clearStorageSignals` (C.2 disk
  reset + C.4 clear, persisted only on a real stale value) and
  `clearRecommendations` (C.4 count + all-four per-type gauge reset; the
  `table_bloat_ratio` NOT-cleared limitation), wired into the `reconcileStorage`
  R.1 gate and the `refreshStorageOnSteadyState` storage-off block.
- `internal/api/server.go` — `handleGetUsageReport` (C.12 soft-gate) +
  `handleTriggerRecommendationScan` (C.4 `400 RECOMMENDATION_SCAN_NOT_ENABLED`).
- `api/v1alpha1/types.go` — `DiskUsagePercent` / `RecommendationCount` status
  fields with `omitempty` **removed** (so the disabled-state `0` reliably persists).
- `internal/controller/scenario122_disabled_states_test.go` — the controller
  **unit** layer.
- `internal/api/scenario122_disabled_states_test.go` — the API handler **unit** layer.
- `test/cases/scenario122_disabled_states_cases.go` — the per-feature catalog.
- `test/functional/scenario122_disabled_states_test.go` — the **functional** layer.
- `test/integration/scenario122_disabled_states_test.go` — the integration layer.
- `test/e2e/scenario122_disabled_states_e2e_test.go` — the **live** layer
  (each feature disabled → reset; re-enabled → reactivation).
