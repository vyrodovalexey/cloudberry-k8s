# Data Architecture — Backup Artifacts & Flows

## 1. Where each artifact lives (target)

| Artifact | Location | Lives on | Notes |
|---|---|---|---|
| History DB `gpbackup_history.db` | `/data/pgdata/gpseg-1/` | coordinator RWO PVC | Default = `$COORDINATOR_DATA_DIRECTORY`. Reachable only in coordinator-exec model. |
| Backup metadata / TOC / report | `/data/backups/backups/<date>/<TS>/` | coordinator RWO PVC | Controlled by `--backup-dir /data/backups`. Small. |
| Per-segment working dirs / pipes | `/data/backups/...` on each segment | each segment RWO PVC | Created by MPP dispatch + `gpbackup_helper`. Was failing without exec model + backup-dir. |
| Bulk data files (compressed CSV) | MinIO `s3://cloudberry-backups/backups/...` | object store | Streamed by `gpbackup_s3_plugin`; NOT persisted on PVCs. |

## 2. Data flow diagram

```mermaid
flowchart LR
  subgraph COORD["coordinator-0 (/data PVC)"]
    H["gpbackup_history.db\n@ /data/pgdata/gpseg-1"]
    M["metadata/TOC/report\n@ /data/backups"]
  end
  subgraph SEGS["segment pods (/data PVC each)"]
    SD["per-segment dirs\n@ /data/backups"]
  end
  S3[("MinIO/S3\nbulk data files")]

  GB["gpbackup (exec on coordinator)"] --> H
  GB --> M
  GB -- MPP dispatch --> SD
  GB -- s3 plugin --> S3
  SD -- s3 plugin (helper) --> S3
```

## 3. ER-ish view of history DB (gpbackup-managed)

```mermaid
erDiagram
  BACKUP_HISTORY {
    string timestamp PK
    string database
    string backup_type
    bool   single_data_file
    string plugin
    string status
    int    bytes
  }
  RESTORE_PLAN {
    string timestamp FK
    string redirect_db
  }
  BACKUP_HISTORY ||--o{ RESTORE_PLAN : "restored as"
```

(Schema is owned by gpbackup; shown for orientation only.)

## 4. Event flow (backup → restore cycle, Scenario 71)

```mermaid
sequenceDiagram
  participant API
  participant CO as coordinator-0
  participant S3 as MinIO
  API->>CO: exec gpbackup (--backup-dir /data/backups --plugin-config s3)
  CO->>S3: data files
  CO->>CO: history+metadata on PVC
  API-->>API: record TS in CR status
  Note over CO: DROP DATABASE mydb (test clean step)
  API->>CO: exec gprestore (--timestamp TS --create-db --plugin-config s3)
  S3-->>CO: data files
  CO->>CO: recreate mydb, rebuild indexes
  API-->>API: verify row counts == baseline
```

## 5. Capacity / retention notes

- PVC growth from backups is bounded to **metadata/TOC/history** only (KB–MB),
  because data files go to S3. `/data/backups` should be pruned after each run
  (gpbackup cleans its working dirs; the operator may also clear stale TOC).
- Retention of the actual backup *sets* is enforced by the `gpbackman` cleanup
  Job against S3 + history (network-only; unaffected by this fix).
