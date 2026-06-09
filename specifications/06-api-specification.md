# Cloudberry Operator - API Specification

**Version**: 1.0.0

---

## 1. Overview

The Cloudberry Operator exposes a REST API for programmatic access to cluster management operations. The API supports Basic and Bearer (JWT) authentication and follows RESTful conventions.

## 2. Base URL

```
https://{operator-service}:{port}/api/v1alpha1
```

Default port: 8090 (HTTP) or 8443 (HTTPS with TLS)

## 3. Authentication

### 3.1 Basic Authentication

```
Authorization: Basic base64(username:password)
```

### 3.2 Bearer Token (JWT)

```
Authorization: Bearer <JWT token>
```

### 3.3 Obtaining a Token

```bash
# Via Keycloak (password grant for testing)
curl -X POST http://keycloak:8090/realms/cloudberry/protocol/openid-connect/token \
  -d "grant_type=password" \
  -d "client_id=cloudberry-ctl" \
  -d "username=admin" \
  -d "password=adminpass"

# Via Keycloak (client_credentials for service accounts)
curl -X POST http://keycloak:8090/realms/cloudberry/protocol/openid-connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=cloudberry-operator" \
  -d "client_secret=<secret>"
```

## 4. API Endpoints

### 4.1 Cluster Management

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters | Basic | List all clusters |
| GET | /clusters/{name} | Basic | Get cluster details |
| POST | /clusters | Admin | Create cluster |
| PUT | /clusters/{name} | Operator | Update cluster |
| DELETE | /clusters/{name} | Admin | Delete cluster |
| GET | /clusters/{name}/status | Basic | Get cluster status |

### 4.2 Cluster Operations

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| POST | /clusters/{name}/start | Operator | Start cluster (modes: normal, restricted, maintenance) |
| POST | /clusters/{name}/stop | Operator | Stop cluster (modes: smart, fast, immediate) |
| POST | /clusters/{name}/restart | Operator | Restart cluster |
| POST | /clusters/{name}/reload | Operator | Reload configuration |

### 4.2.1 Scaling Operations

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| POST | /clusters/{name}/scale | Admin | Scale cluster segments |
| GET | /clusters/{name}/scale/status | Operator Basic | Get scale operation status |
| POST | /clusters/{name}/rebalance | Operator | Trigger data rebalancing |
| GET | /clusters/{name}/rebalance/status | Operator Basic | Get rebalance status |

### 4.2.2 Storage Operations

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| POST | /clusters/{name}/storage/expand | Admin | Expand PV storage |
| GET | /clusters/{name}/storage/disk-usage | Basic | Get disk usage |
| GET | /clusters/{name}/storage/pvcs | Basic | List PVC sizes and status |

### 4.3 Configuration

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/config | Operator Basic | Get configuration |
| PUT | /clusters/{name}/config | Operator | Update configuration |
| GET | /clusters/{name}/config/parameters | Operator Basic | List parameters |
| PUT | /clusters/{name}/config/parameters | Operator | Set parameters |
| GET | /clusters/{name}/config/hba | Operator Basic | Get HBA rules |
| PUT | /clusters/{name}/config/hba | Admin | Update HBA rules |

### 4.4 High Availability

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/segments | Basic | List segments |
| GET | /clusters/{name}/segments/{id} | Basic | Get segment details |
| GET | /clusters/{name}/mirroring | Basic | Get mirroring status |
| POST | /clusters/{name}/recovery | Operator | Start recovery |
| POST | /clusters/{name}/rebalance | Operator | Rebalance segments |
| GET | /clusters/{name}/standby | Basic | Get standby status |
| POST | /clusters/{name}/standby/activate | Admin | Activate standby |
| POST | /clusters/{name}/standby/reinitialize | Operator | Reinitialize standby |

### 4.5 Sessions

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/sessions | Operator Basic | List sessions |
| POST | /clusters/{name}/sessions/{pid}/cancel | Operator | Cancel query |
| DELETE | /clusters/{name}/sessions/{pid} | Operator | Terminate session |

### 4.6 Maintenance

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| POST | /clusters/{name}/maintenance/vacuum | Operator | Run vacuum |
| POST | /clusters/{name}/maintenance/analyze | Operator | Run analyze |
| POST | /clusters/{name}/maintenance/reindex | Operator | Run reindex |
| GET | /clusters/{name}/maintenance/jobs | Operator Basic | List maintenance jobs |

### 4.7 Authentication Management

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/auth/roles | Admin | List roles |
| POST | /clusters/{name}/auth/roles | Admin | Create role |
| PUT | /clusters/{name}/auth/roles/{role} | Admin | Update role |
| DELETE | /clusters/{name}/auth/roles/{role} | Admin | Delete role |
| POST | /clusters/{name}/auth/rotate-password | Admin | Rotate admin password |

### 4.8 Resource Group Management

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/workload/resource-groups | Basic | List resource groups |
| POST | /clusters/{name}/workload/resource-groups | Operator | Create resource group |
| DELETE | /clusters/{name}/workload/resource-groups/{group} | Operator | Delete resource group |
| POST | /clusters/{name}/workload/resource-groups/{group}/assign | Operator | Assign role to group |

### 4.9 Backup and Restore

All backup/restore endpoints are namespaced under `/clusters/{name}/backups`, are OIDC/JWT-authenticated (`withAuth`), and enforce per-endpoint RBAC (`withPermission`). A missing cluster returns `404 CLUSTER_NOT_FOUND`. These are the seven endpoints verified by **Scenario 85** (see §10).

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/backups | Basic | List backups from the operator's recorded backup history (`status.backup.backupHistory`) |
| POST | /clusters/{name}/backups | Operator | Create a backup (creates a Job directly) |
| GET | /clusters/{name}/backups/{timestamp} | Basic | Get details for a specific backup timestamp |
| DELETE | /clusters/{name}/backups/{timestamp} | Admin | Delete a backup (creates a `gpbackman` cleanup Job) |
| POST | /clusters/{name}/backups/{timestamp}/restore | Admin | Restore from a backup (creates a restore Job) |
| GET | /clusters/{name}/backups/jobs | Basic | List backup/restore/cleanup Job statuses |
| GET | /clusters/{name}/backups/jobs/{job}/logs | Basic | **Stream** the backup Job's pod logs as `text/plain` (Scenario 86k; `?follow`, `?tailLines`) |
| GET | /clusters/{name}/backups/schedule | Basic | Get CronJob status + computed `nextScheduleTime` |
| PATCH | /clusters/{name}/backups/schedule | Operator | Update `spec.backup.schedule` / suspend the CronJob (outside the Scenario 85 set) |
| POST | /clusters/{name}/migrate | Admin | Cross-cluster database migration (Scenario 87); creates a **single** coordinated migration Job — see §5.19 |

The full request schemas (`CreateBackupRequest.gpbackupOptions`, `RestoreRequest.gprestoreOptions`), the option → `gpbackup`/`gprestore` flag mapping, and the mutual-exclusivity rules are documented in §5.15–§5.18. The cross-cluster migration request/response is documented in §5.19.

### 4.10 Health and Metrics

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /healthz | None | Operator health check |
| GET | /readyz | None | Operator readiness check |
| GET | /metrics | None | Prometheus metrics |

## 5. Request/Response Schemas

### 5.1 Cluster Status Response

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "status": {
    "phase": "Running",
    "coordinatorReady": true,
    "standbyReady": true,
    "segmentsReady": 4,
    "segmentsTotal": 4,
    "mirroringStatus": "InSync",
    "clusterVersion": "7.7",
    "lastReconcileTime": "2026-05-11T18:00:00Z",
    "conditions": [
      {
        "type": "ClusterReady",
        "status": "True",
        "lastTransitionTime": "2026-05-11T17:55:00Z",
        "reason": "AllComponentsReady",
        "message": "All cluster components are running and healthy"
      }
    ]
  }
}
```

### 5.2 Start Cluster Request

```json
{
  "mode": "normal"  // "normal", "restricted", "maintenance"
}
```

### 5.3 Stop Cluster Request

```json
{
  "mode": "fast"  // "smart", "fast", "immediate"
}
```

### 5.4 Recovery Request

```json
{
  "type": "incremental",  // "incremental", "full", "differential"
  "targetSegments": [0, 1],  // optional, all failed if omitted
  "targetNode": "node-3",    // optional, recover in-place if omitted
  "parallelStreams": 4       // optional, for differential recovery
}
```

### 5.5 Configuration Update Request

```json
{
  "parameters": {
    "max_connections": "200",
    "work_mem": "128MB"
  },
  "coordinatorParameters": {
    "optimizer": "on"
  },
  "requiresRestart": false
}
```

### 5.6 HBA Rules Update Request

```json
{
  "rules": [
    {
      "type": "host",
      "database": "all",
      "user": "all",
      "address": "10.0.0.0/8",
      "method": "scram-sha-256"
    }
  ]
}
```

### 5.7 Scale Request

```json
{
  "segmentCount": 8,
  "confirmScaleIn": false
}
```

**Notes**:
- If `segmentCount` > current count: scale-out (add segments)
- If `segmentCount` < current count: scale-in (remove segments)
- `confirmScaleIn` must be `true` when reducing by more than 50%

### 5.8 Scale Status Response

```json
{
  "operation": "scale-out",
  "previousCount": 4,
  "targetCount": 8,
  "currentCount": 6,
  "phase": "redistributing",
  "redistributionProgress": 65,
  "startedAt": "2026-05-14T10:00:00Z",
  "estimatedCompletion": "2026-05-14T10:30:00Z"
}
```

### 5.9 Storage Expand Request

```json
{
  "component": "segments",
  "newSize": "500Gi"
}
```

**Valid `component` values**: `coordinator`, `standby`, `segments` (applies to all primary + mirror PVCs)

### 5.10 Storage Expand Response

```json
{
  "component": "segments",
  "previousSize": "200Gi",
  "newSize": "500Gi",
  "affectedPVCs": 8,
  "status": "expanding",
  "requiresRestart": false
}
```

### 5.11 Resource Group Create Request

```json
{
  "name": "analytics",
  "concurrency": 10,
  "cpuMaxPercent": 50,
  "memoryLimit": 30
}
```

### 5.12 Resource Group Assign Request

```json
{
  "role": "analyst"
}
```

### 5.13 Rebalance Request

```json
{
  "tables": ["public.orders", "public.customers"],
  "parallelism": 4,
  "excludeTables": ["audit_log", "temp_*"]
}
```

### 5.14 Session List Response

```json
{
  "sessions": [
    {
      "pid": 12345,
      "username": "analyst",
      "application": "psql",
      "clientAddress": "10.0.0.5",
      "state": "active",
      "query": "SELECT * FROM large_table",
      "queryStart": "2026-05-11T17:58:00Z",
      "duration": "2m30s"
    }
  ]
}
```

### 5.15 Create Backup Request (`POST /clusters/{name}/backups`)

**Permission**: `Operator`. **Pre-req**: `spec.backup.enabled: true` (else `400 BACKUP_NOT_ENABLED`). An on-demand backup creates a Kubernetes **Job directly** (it does **not** go through the scheduled CronJob). The per-request options are merged over the cluster's `backup.gpbackup` defaults and rendered into the `gpbackup` CLI invocation on the Job container.

```json
{
  "type": "full",
  "databases": ["mydb"],
  "gpbackupOptions": {
    "compressionLevel": 5,
    "compressionType": "zstd",
    "jobs": 4,
    "singleDataFile": true,
    "copyQueueSize": 8,
    "incremental": false,
    "fromTimestamp": "",
    "includeSchemas": ["public", "analytics"],
    "excludeTables": ["public.temp_data", "public.scratch"],
    "leafPartitionData": true,
    "withStats": true,
    "withoutGlobals": true,
    "noCompression": false
  }
}
```

- `type` — `full` (default) or `incremental`; any other value → `400 INVALID_REQUEST`.
- `databases` — each entry must be a valid identifier (else `400 INVALID_REQUEST`); the first DB drives `--dbname`.
- `gpbackupOptions` (`GpbackupOptionsRequest`) — the full field list is mapped in §5.16.

**Response `202`**: `{ "status": "backup started", "cluster": "...", "job": "...", "timestamp": "...", "type": "full" }`.

### 5.16 `gpbackupOptions` → `gpbackup` flag mapping

| Request field | gpbackup flag | Notes |
|---|---|---|
| `databases[0]` | `--dbname <db>` | First database only |
| `compressionLevel` (> 0) | `--compression-level <n>` | Skipped when `noCompression` |
| `compressionType` (`gzip`\|`zstd`) | `--compression-type <t>` | Skipped when `noCompression` |
| `noCompression` | `--no-compression` | **Precedence** over level/type |
| `singleDataFile` | `--single-data-file` | Mutually exclusive with `--jobs` |
| `copyQueueSize` (> 0) | `--copy-queue-size <n>` | Emitted **only** with `singleDataFile` |
| `jobs` (> 0) | `--jobs <n>` | Emitted **only** when **not** `singleDataFile` |
| `withStats` | `--with-stats` | |
| `withoutGlobals` | `--without-globals` | |
| `leafPartitionData` | `--leaf-partition-data` | **Emitted on FULL backups too**, exactly once (see note) |
| `incremental` (or `type=incremental`) | `--incremental --leaf-partition-data` | Leaf-partition-data forced **exactly once** for incrementals |
| `fromTimestamp` | `--from-timestamp <ts>` | Incremental only |
| `includeSchemas[]` | `--include-schema <s>` (repeated) | |
| `excludeTables[]` | `--exclude-table <t>` (repeated) | |

> **`leafPartitionData` on full backups.** `--leaf-partition-data` is valid and meaningful for **full** backups (it backs up leaf-partition data as separate files instead of the whole partitioned table). The operator now emits `--leaf-partition-data` for a full backup whenever `leafPartitionData: true`, and emits it **exactly once** — the incremental path (which force-pairs `--incremental --leaf-partition-data`) is guarded so the flag is never duplicated. Net behavior: full + `leafPartitionData:false` → none; full + `leafPartitionData:true` → exactly one; incremental (any value) → exactly one.

> **`noCompression` override.** When `noCompression: true`, the operator emits `--no-compression` and **omits** `--compression-level` / `--compression-type` even when those are also set — `--no-compression` takes precedence.

### 5.17 Restore Request (`POST /clusters/{name}/backups/{timestamp}/restore`)

**Permission**: `Admin`. The `{timestamp}` path value (or body `timestamp`; path preferred) must match `^\d{14}$` (else `400 INVALID_REQUEST`). The restore creates a Kubernetes **Job directly**.

```json
{
  "databases": ["mydb"],
  "gprestoreOptions": {
    "jobs": 4,
    "redirectDb": "mydb_restored",
    "redirectSchema": "restored",
    "createDb": true,
    "includeSchemas": ["public", "analytics"],
    "includeTables": ["public.users", "public.orders"],
    "excludeTables": ["public.audit"],
    "withGlobals": true,
    "withStats": true,
    "runAnalyze": true,
    "onErrorContinue": true,
    "dataOnly": true,
    "metadataOnly": false,
    "truncateTable": true,
    "resizeCluster": true
  }
}
```

**Response `202`**: `{ "status": "restore started", "cluster": "...", "job": "...", "timestamp": "..." }`.

### 5.18 `gprestoreOptions` → `gprestore` flag mapping

The leading args are destination-aware (S3 → `--plugin-config /tmp/s3-config.yaml`; local → `--backup-dir <path>`) plus `--timestamp <ts>`.

| Request field | gprestore flag | Notes |
|---|---|---|
| `timestamp` | `--timestamp <ts>` | From the path (preferred) or body |
| `jobs` (> 0) | `--jobs <n>` | |
| `redirectDb` | `--redirect-db <db>` | |
| `redirectSchema` | `--redirect-schema <s>` | |
| `createDb` | `--create-db` | |
| `includeTables[]` | `--include-table <t>` (repeated) | **Precedence**: emitted when both include\* set |
| `includeSchemas[]` | `--include-schema <s>` (repeated) | Emitted **only** when `includeTables` empty |
| `excludeTables[]` | `--exclude-table <t>` (repeated) | |
| `withGlobals` | `--with-globals` | |
| `runAnalyze` | `--run-analyze` | **Precedence** over `--with-stats` |
| `withStats` | `--with-stats` | Emitted **only** when **not** `runAnalyze` |
| `onErrorContinue` | `--on-error-continue` | |
| `dataOnly` | `--data-only` | Mutually exclusive with `metadataOnly` |
| `metadataOnly` | `--metadata-only` | Mutually exclusive with `dataOnly` |
| `truncateTable` | `--truncate-table` | |
| `resizeCluster` | `--resize-cluster` | |

**Mutual-exclusivity rules and their responses:**

1. **`dataOnly` + `metadataOnly`** — rejected at the handler with `400 INVALID_REQUEST` **before** the Job is built (`restoreOptionsConflict`); `gprestore` rejects the pair.
2. **`includeSchemas` vs `includeTables`** — when both are supplied the operator emits the more specific `--include-table` (table-level precedence) and **omits** `--include-schema`, keeping the `gprestore` invocation valid (no 400).
3. **`runAnalyze` vs `withStats`** — when both are supplied the operator emits `--run-analyze` and **omits** `--with-stats` (run-analyze precedence; no 400).

### 5.19 Cross-Cluster Migration Request (`POST /clusters/{name}/migrate`)

Migrates a database from a **source** cluster to a **target** cluster (Scenario 87, **Admin**). The path `{name}` identifies the source cluster (or set `sourceCluster` in the body). The operator creates a **single coordinated migration Job** `<source>-migration-<ts>` (label `avsoft.io/backup-operation=migrate`) that, under the coordinator-exec model: (1) execs `gpbackup` inside the **source** coordinator and **captures the real gpbackup timestamp**; (2) prepares the target DB on the **target** coordinator (`--truncate` → DROP+recreate empty; otherwise CREATE-if-absent); (3) execs `gprestore --timestamp <captured>` inside the **target** coordinator — it does **not** pass `--truncate-table`; and (4) runs post-migration validation (row-count probe + invalid-index scan + health check) on the target.

```json
POST /api/v1alpha1/clusters/src/migrate
{
  "sourceCluster": "src",
  "targetCluster": "dst",
  "database": "mydb",
  "tables": ["public.users", "public.orders"],
  "truncate": true,
  "redirectDb": "mydb",
  "redirectSchema": "",
  "jobs": 4
}
```

| Request field | Type | Description |
|---|---|---|
| `sourceCluster` | string | Source cluster name (defaults to the path `{name}`) |
| `targetCluster` | string | Target cluster name (**required**; must differ from source) |
| `database` | string | Source database to back up (`gpbackup --dbname`) |
| `tables[]` | string[] | Tables → repeated `--include-table` on both `gpbackup` and `gprestore` |
| `truncate` | bool | Clean target: DROP+recreate the target DB empty before restore (does **not** map to `--truncate-table`) |
| `redirectDb` | string | `gprestore --redirect-db` on the target |
| `redirectSchema` | string | `gprestore --redirect-schema` on the target |
| `jobs` | int32 | `gprestore --jobs` (restore parallelism) on the target |

**Requirements.** Both clusters must be backup-enabled with an **S3** destination sharing the **same bucket** (else `400 INVALID_REQUEST`); the migration backup and restore reference the **same bucket and folder** (the target reads the **source** cluster's S3 folder). A missing cluster returns `404 CLUSTER_NOT_FOUND`. The backup ServiceAccount needs `pods` + `pods/exec` RBAC (coordinator-exec model).

**202 Accepted response.** Because the migration runs as a **single** Job that performs all phases, the `backupJob`/`restoreJob`/`validationJob` fields all reference that one Job; `migrationJob` names it unambiguously:

```json
{
  "status": "migration started",
  "sourceCluster": "src",
  "targetCluster": "dst",
  "timestamp": "20260601020000",
  "migrationJob": "src-migration-20260601020000",
  "backupJob": "src-migration-20260601020000",
  "restoreJob": "src-migration-20260601020000",
  "validationJob": "src-migration-20260601020000"
}
```

> **Note.** `timestamp` is the operator-chosen value used **only to NAME the Job**; the actual `gpbackup`/`gprestore` timestamp is the one `gpbackup` generates at runtime and the Job captures (see [Backup & Restore Spec §Cross-Cluster Migration](11-backup-restore-spec.md#cross-cluster-migration)).

## 6. Error Handling

### 6.1 Error Response Format

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "Cluster 'my-cluster' not found in namespace 'cloudberry-test'",
    "details": {
      "cluster": "my-cluster",
      "namespace": "cloudberry-test"
    }
  }
}
```

### 6.2 Error Codes

| HTTP Status | Code | Description |
|-------------|------|-------------|
| 400 | INVALID_REQUEST | Malformed request body |
| 401 | UNAUTHORIZED | Missing or invalid credentials |
| 403 | FORBIDDEN | Insufficient permissions |
| 404 | CLUSTER_NOT_FOUND | Cluster does not exist |
| 404 | SEGMENT_NOT_FOUND | Segment does not exist |
| 404 | BACKUP_NOT_FOUND | Backup timestamp not found in the cluster's backup history |
| 400 | BACKUP_NOT_ENABLED | `spec.backup.enabled` is `false` (backup create rejected) |
| 409 | CONFLICT | Operation conflicts with current state |
| 422 | VALIDATION_ERROR | Request validation failed |
| 500 | INTERNAL_ERROR | Unexpected server error |
| 429 | RATE_LIMITED | Too many requests, retry after delay |
| 503 | SERVICE_UNAVAILABLE | Operator not ready |
| 503 | DB_UNAVAILABLE | Database connection unavailable |

### 6.3 Rate Limiting

- Default: 10 requests/minute per client IP (token bucket algorithm)
- Only trusts `X-Forwarded-For`/`X-Real-IP` from configured trusted proxies
- Configurable via operator configuration
- Returns `429 Too Many Requests` with `Retry-After` header
- Applied before authentication to prevent brute-force attacks

## 7. Pagination

For list endpoints:

```
GET /clusters/{name}/sessions?limit=50&offset=0
```

Response includes:
```json
{
  "items": [...],
  "total": 150,
  "limit": 50,
  "offset": 0
}
```

## 8. Webhook Endpoints

### 8.1 Validating Webhook

```
POST /validate-cloudberry-example-com-v1alpha1-cloudberrycluster
```

Validates CloudberryCluster CR before admission.

### 8.2 Mutating Webhook

```
POST /mutate-cloudberry-example-com-v1alpha1-cloudberrycluster
```

Sets defaults on CloudberryCluster CR.

## 9. OpenAPI/Swagger

The operator serves OpenAPI v3 specification at:
```
GET /openapi/v3
```

## 10. Scenario 85 — All Backup API Endpoints

**Scenario 85** verifies the **seven** backup/restore REST API endpoints end-to-end: routing, per-endpoint RBAC, the full request schemas, the option → `gpbackup`/`gprestore` flag mapping (§5.16, §5.18), and the negative/mutual-exclusivity responses. Every endpoint is OIDC/JWT-authenticated and a missing cluster returns `404 CLUSTER_NOT_FOUND`. The acceptance contract per sub-case (85a–85g):

- **85a — `GET /backups` (Basic).** Returns the operator's recorded backup history (`status.backup.backupHistory` — the operator's view of `gpbackup` outcomes derived from observed Jobs), **not** a live `gpbackman` query. Response `200`: `{ cluster, enabled, backups:[...], total, lastBackupTime, lastBackupTimestamp, lastBackupStatus }`. The boolean **`enabled`** field (`spec.backup != nil && spec.backup.enabled`) lets clients detect the backup-disabled state from the list endpoint without a `POST` probe (Scenario 88a — see §10.1); the status stays `200` and the pre-existing keys are unchanged for back-compat.

- **85b — `POST /backups` (Operator).** Creates a backup **Job** (label `avsoft.io/backup-operation=backup`) whose `gpbackup` args match the `CreateBackupRequest.gpbackupOptions` per §5.16, including `--leaf-partition-data` on a **full** backup when `leafPartitionData: true` (emitted exactly once). Negatives: invalid `type` / DB identifier / malformed JSON → `400`; backup not enabled → `400 BACKUP_NOT_ENABLED`; a Basic identity → `403`.

- **85c — `GET /backups/{timestamp}` (Basic).** Returns the matching `BackupHistoryEntry` from the recorded history. Negatives: a non-14-digit timestamp → `400 INVALID_REQUEST`; an unknown 14-digit timestamp → `404 BACKUP_NOT_FOUND`.

- **85d — `DELETE /backups/{timestamp}` (Admin).** Creates a `gpbackman` cleanup Job (`backup-delete`, label `avsoft.io/backup-operation=cleanup`) to remove the backup. Response `202`: `{ status:"deleted", cluster, job, timestamp }`. Negatives: invalid timestamp → `400`; Operator/Basic identity → `403`.

- **85e — `POST /backups/{timestamp}/restore` (Admin).** Creates a restore **Job** (label `avsoft.io/backup-operation=restore`) whose `gprestore` args match `RestoreRequest.gprestoreOptions` per §5.18 (e.g. `dataOnly→--data-only`, `metadataOnly→--metadata-only`, `resizeCluster→--resize-cluster`). Mutual exclusivity: `dataOnly`+`metadataOnly` → `400 INVALID_REQUEST`; include-schema vs include-table and run-analyze vs with-stats resolved in favor of the more specific flag (no 400). Negatives: invalid timestamp / DB / JSON → `400`; Operator/Basic identity → `403`.

- **85f — `GET /backups/jobs` (Basic).** Lists backup/restore/cleanup Job statuses for the cluster (status ∈ `succeeded|failed|running|pending`); unrelated Jobs are excluded. Response `200`: `{ cluster, jobs:[{ name, operation, status, startTime?, completionTime? }], total }`.

- **85g — `GET /backups/schedule` (Basic).** Returns the backup CronJob status with a **computed** `nextScheduleTime`, plus the boolean **`enabled`** field (`spec.backup != nil && spec.backup.enabled`). No CronJob → `200 { cluster, enabled, scheduled:false }`; CronJob present → `200 { cluster, enabled, scheduled:true, schedule, suspend, activeJobs, lastScheduleTime?, nextScheduleTime? }`. `enabled` distinguishes "backup disabled" from "enabled but no schedule" — both report `scheduled:false` (Scenario 88 — see §10.1).

The handlers live in `internal/api/server.go` (`handleListBackups`, `handleCreateBackup`, `handleGetBackup`, `handleDeleteBackup`, `handleRestoreBackup`, `handleListBackupJobs`, `handleGetBackupSchedule`); the DTOs and option mapping live in `internal/api/backup.go` (`buildBackupJobOptions`/`mergeGpbackupOptions`, `buildRestoreJobOptions`/`mergeGprestoreOptions`, `restoreOptionsConflict`); the args are rendered by `buildGpbackupArgs`/`buildGprestoreArgs` in `internal/builder/backup_builder.go`.

The scenario is driven by the sample CR `deploy/helm/cloudberry-operator/config/samples/scenario85-api-endpoints.yaml` and is covered by `test/functional/scenario85_api_endpoints_test.go`, `test/integration/scenario85_api_endpoints_integration_test.go`, `test/e2e/scenario85_api_endpoints_e2e_test.go`, the test-case catalog in `test/cases/scenario85_api_endpoints_cases.go`, and the live OIDC-authed verification script `test/e2e/scripts/scenario85-api-endpoints.sh`.

### 10.1 Scenario 88 — Backup Disabled / No Schedule (API surface)

**Scenario 88** covers how the backup API behaves when backup is **disabled** (`spec.backup.enabled: false`) or **enabled with an empty schedule** (`spec.backup.schedule: ""`). The behavior is detailed in the [Backup & Restore spec — Backup Disabled / No Schedule](11-backup-restore-spec.md#scenario-88--backup-disabled--no-schedule); the API contract is:

- **`POST /clusters/{name}/backups`** → `400 BACKUP_NOT_ENABLED` when `spec.backup` is nil or `spec.backup.enabled` is `false` (`handleCreateBackup` gating; see the error table in §6). When backup is enabled the request creates a Job directly (`202`) regardless of whether a schedule is set.

- **`GET /clusters/{name}/backups`** → `200` with a new boolean **`enabled`** field (`backupEnabled(cluster)` = `spec.backup != nil && spec.backup.enabled`). The field is added alongside the existing `cluster`, `backups`, `total`, `lastBackupTime`, `lastBackupTimestamp`, and `lastBackupStatus` keys — the status and existing keys are unchanged for back-compat. `enabled:false` surfaces the disabled state without needing a `POST` probe.

- **`GET /clusters/{name}/backups/schedule`** → `200` with both **`enabled`** and `scheduled`. When backup is **disabled** → `{ cluster, enabled:false, scheduled:false }`. When backup is **enabled with an empty schedule** → `{ cluster, enabled:true, scheduled:false }` (no CronJob, so on-demand-only). When a schedule is set and the CronJob exists → `{ cluster, enabled:true, scheduled:true, schedule, suspend, activeJobs, … }`. `enabled` is the field that distinguishes "disabled" from "enabled but unscheduled" since both report `scheduled:false`.

The `enabled` field is computed by the shared `backupEnabled` helper in `internal/api/server.go` (mirroring the `handleCreateBackup` `BACKUP_NOT_ENABLED` gate) and is keyed `responseKeyEnabled` (`"enabled"`) in both `handleListBackups` and `handleGetBackupSchedule`. The API behavior is covered by `internal/api/backup_test.go` (`TestHandleListBackups_EnabledField` and the schedule-endpoint cases) and the Scenario 88 functional/e2e suites.

## 11. Scenario 86 — Backup Job Log Streaming Endpoint

**Scenario 86** verifies that all eleven `cloudberry-ctl backup …` CLI commands map to the operator REST API (see [cloudberry-ctl §12](07-cloudberry-ctl-spec.md#12-scenario-86--all-backup-cli-commands)). Ten of the commands reuse the Scenario 85 endpoints above; the one **new** operator endpoint introduced by Scenario 86 (for `backup jobs logs --job <name>`, sub-case 86k) is documented here.

### 11.1 `GET /clusters/{name}/backups/jobs/{job}/logs`

Streams the logs of the pod backing a backup/restore/cleanup **Job** as plain text, so the CLI (and any client) can read backup output without `kubectl`.

| Property | Value |
|----------|-------|
| Method / Path | `GET /clusters/{name}/backups/jobs/{job}/logs` |
| Permission | **Basic** (`auth.PermissionBasic` — consistent with the other read-only backup endpoints) |
| Auth | OIDC/JWT bearer token (`withAuth`) |
| Success response | `200` with `Content-Type: text/plain; charset=utf-8`; the body **streams** the pod's container logs |
| Query params | `?follow=true` → stream as produced (flushed incrementally); `?tailLines=N` → only the last `N` lines; `?namespace=<ns>` → target namespace |

**Behavior.** `handleBackupJobLogs` (`internal/api/server.go`):

1. Validates the `{job}` path segment is a DNS-1123 name (else `400 INVALID_REQUEST`).
2. Requires a typed Kubernetes **clientset** (injected via `Server.WithClientset`, wired from `mgr.GetConfig()` in `cmd/operator/main.go`); when absent → `501 LOGS_NOT_AVAILABLE`. The controller-runtime `client.Client` cannot stream pod logs — only the typed clientset's `CoreV1().Pods(ns).GetLogs(...).Stream(ctx)` can.
3. Resolves the cluster (missing → `404 CLUSTER_NOT_FOUND`).
4. Finds the Job's most-recently-created pod via the `job-name` / `batch.kubernetes.io/job-name` label (`findJobPod` → `mostRecentPodName`). No pod found → `404 JOB_NOT_FOUND`.
5. Opens `clientset.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Follow, TailLines})` (`buildPodLogOptions` parses `?follow`/`?tailLines`), sets `Content-Type: text/plain; charset=utf-8`, writes `200`, and copies the stream to the response (flushing for `--follow`). A stream-open failure → `500 INTERNAL_ERROR`.

| Status | Condition |
|--------|-----------|
| `200` | Logs streamed (`text/plain`) |
| `400 INVALID_REQUEST` | Invalid job name |
| `404 CLUSTER_NOT_FOUND` | Cluster does not exist |
| `404 JOB_NOT_FOUND` | No pod found for the Job (e.g. already GC'd by `ttlSecondsAfterFinished`) |
| `500 INTERNAL_ERROR` | Failed to list pods or open the log stream |
| `501 LOGS_NOT_AVAILABLE` | Operator API has no Kubernetes clientset configured |

> **Route precedence.** The literal `/backups/jobs/{job}/logs` does not collide with `/backups/{timestamp}`: the `jobs` literal wins over the `{timestamp}` wildcard, and `{job}/logs` is a deeper path.

> **Finished-Job pod GC caveat.** Backup Jobs carry `ttlSecondsAfterFinished`; once a finished Job's pod is garbage-collected the endpoint returns `404 JOB_NOT_FOUND`. Stream from a recently created Job, or use `?tailLines` while the pod is still present.

The handler lives in `internal/api/server.go` (`handleBackupJobLogs`, `findJobPod`, `mostRecentPodName`, `streamPodLogs`, `buildPodLogOptions`); the clientset is injected by `Server.WithClientset`. The endpoint is covered by `internal/api/backup_joblogs_test.go`, `internal/api/backup_joblogs_scenario86_test.go`, `test/functional/scenario86_cli_commands_test.go`, `test/integration/scenario86_cli_commands_integration_test.go`, `test/e2e/scenario86_cli_commands_e2e_test.go`, and the live script `test/e2e/scripts/scenario86-cli-commands.sh`.
