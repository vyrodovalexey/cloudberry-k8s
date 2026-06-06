# Architecture — Cloudberry-K8s Backup/Restore (Target Design for Scenario 71)

## 1. Architecture Overview

### Architectural style

- **Kubernetes Operator** (controller + CRD `CloudberryCluster`) following a
  declarative reconcile loop.
- **Builder pattern** (`internal/builder`) constructs all Kubernetes objects
  (StatefulSets, Services, ConfigMaps, Secrets, Jobs, CronJobs) from the CR.
- **MPP database** (Apache Cloudberry) deployed as one StatefulSet per role:
  coordinator, standby, segment-primary, segment-mirror — each pod with its own
  **ReadWriteOnce** data PVC.
- Backups use the external **apache/cloudberry-backup** toolchain (`gpbackup`,
  `gprestore`, `gpbackup_helper`, `gpbackup_s3_plugin`, `gpbackman`).

### High-level component diagram (target)

```mermaid
flowchart TB
  subgraph CP["Operator (control plane)"]
    CTRL["CloudberryCluster controller\n+ Builder (internal/builder)"]
    API["REST API :8090\n(backup/restore endpoints)"]
  end

  subgraph CR["Cluster pods (cloudberry-official:2.1.0)"]
    COORD["coordinator-0\nPGDATA=/data/pgdata\nCOORDINATOR_DATA_DIRECTORY=/data/pgdata/gpseg-1\nGPHOME/bin: gpbackup,gprestore,gpbackup_helper,gpbackup_s3_plugin\nPVC data (RWO) @ /data\n+ /data/backups (backup-dir)"]
    STBY["standby-0\nPVC data (RWO)"]
    SP0["segment-primary-0\n/data/pgdata/gpseg0\n+ /data/backups"]
    SP1["segment-primary-1\n/data/pgdata/gpseg1\n+ /data/backups"]
    SM0["segment-mirror-0"]
    SM1["segment-mirror-1"]
  end

  subgraph EX["Detached Jobs (cloudberry-backup:2.1.0) — network-only"]
    CLEAN["gpbackman cleanup Job"]
    EXPO["gpbackup_exporter (optional)"]
  end

  MINIO[("MinIO / S3\nbucket: cloudberry-backups")]

  API --> CTRL
  CTRL -- "exec gpbackup/gprestore\n(--backup-dir /data/backups\n--plugin-config s3)" --> COORD
  COORD -- "MPP dispatch create-dirs +\nper-segment files (gpbackup_helper)" --> SP0
  COORD -- "MPP dispatch" --> SP1
  COORD -- "data files via s3 plugin" --> MINIO
  SP0 -- "data files via s3 plugin" --> MINIO
  SP1 -- "data files via s3 plugin" --> MINIO
  CLEAN -- "history/list/delete" --> MINIO
  EXPO -- "metrics" --> MINIO
```

### Entry points and request flow (target backup)

```mermaid
sequenceDiagram
  participant U as User/CLI
  participant API as Operator REST API :8090
  participant C as Controller/Builder
  participant K as kube-apiserver
  participant CO as coordinator-0 (cloudberry container)
  participant SEG as segment-primary-N
  participant S3 as MinIO/S3

  U->>API: POST /clusters/{n}/backups {type, databases}
  API->>C: create backup op (server-side timestamp TS)
  C->>K: render S3 ConfigMap + resolve creds Secret
  C->>CO: exec: render /tmp/s3-config.yaml; gpbackup --dbname mydb\n--backup-dir /data/backups --plugin-config /tmp/s3-config.yaml
  CO->>CO: write history DB + metadata under /data/pgdata/gpseg-1\n+ TOC under /data/backups
  CO->>SEG: MPP dispatch: create /data/backups subdir\n+ gpbackup_helper pipes
  SEG-->>S3: stream segment data files (s3 plugin)
  CO-->>S3: stream coordinator metadata/data (s3 plugin)
  CO-->>API: exit 0 (TS, size, duration)
  API-->>U: 202 {timestamp: TS}
```

## 2. Why the previous (failing) flow broke

```mermaid
flowchart LR
  subgraph BAD["FAILING standalone Job (cloudberry-backup:2.1.0)"]
    J["gpbackup pod\nmounts ONLY /etc/gpbackup (ConfigMap)\nNO /data/pgdata/gpseg-1\nNO segment data dirs"]
  end
  J -- "history DB write" --> X1["/data/pgdata/gpseg-1/gpbackup_history.db\n❌ no such file or directory"]
  J -- "MPP dispatch create-dirs" --> X2["segment local backup dirs\n❌ unable to create on N segments"]
```

Root cause: coordinator data dir + per-segment dirs live on **per-pod RWO PVCs**
that a separate Job pod cannot mount; the S3 plugin only moves data files, not
the history/metadata/per-segment dirs. (Full detail in
`architecture-findings.md`.)

## 3. Module / Package Map

| Package / file | Responsibility |
|---|---|
| `api/v1alpha1` | CRD types: `CloudberryCluster`, `BackupSpec`, `GpbackupOptions`, `GprestoreOptions`, S3 destination. |
| `internal/builder/builder.go` | Builds cluster StatefulSets/Services/ConfigMaps; defines data-dir layout (`PGDATA=/data/pgdata`, per-pod RWO PVC `data`). |
| `internal/builder/backup_builder.go` | Builds backup/restore/cleanup/validation Jobs, S3 ConfigMap, arg builders. **Target of the fix.** |
| `hack/docker-entrypoint-cloudberry.sh` | Cluster pod init: coordinator/segment data dirs, segment registration. **Add `/data/backups` here.** |
| `Dockerfile.cloudberry-official` | Cluster image; bundles gpbackup toolchain in `GPHOME/bin`. |
| `Dockerfile.cloudberry-backup` | Standalone toolchain image; keep for cleanup/exporter. |
| `internal/api` | REST API endpoints driving backup/restore. |

## 4. Domain Model (relevant slice)

```mermaid
classDiagram
  class CloudberryCluster {
    +Spec CloudberryClusterSpec
    +Status CloudberryClusterStatus
  }
  class BackupSpec {
    +bool Enabled
    +string Schedule
    +string Image
    +Destination
    +GpbackupOptions Gpbackup
    +GprestoreOptions Gprestore
    +Retention
    +JobTemplate
  }
  class Destination {
    +string Type  // s3|local
    +S3Destination S3
    +LocalDestination Local
  }
  class GpbackupOptions {
    +bool SingleDataFile
    +int Jobs
    +int CompressionLevel
    +string CompressionType
    +bool Incremental
    +bool WithStats
    // NEW (implicit): BackupDir defaults /data/backups
  }
  class BackupCoordinatorExecOp {
    +string Timestamp
    +string CoordinatorPod
    +[]string Args  // includes --backup-dir, --plugin-config
  }
  CloudberryCluster --> BackupSpec
  BackupSpec --> Destination
  BackupSpec --> GpbackupOptions
  BackupSpec ..> BackupCoordinatorExecOp : produces (target)
```

## 5. Deployment Topology (target)

```mermaid
flowchart TB
  subgraph ns["namespace cloudberry-test"]
    OP["cloudberry-operator (Deployment)\nREST :8090"]
    subgraph sts["StatefulSets (cloudberry-official:2.1.0)"]
      C0["coordinator-0\nPVC: data (RWO)\n/data/backups"]
      S0["standby-0\nPVC: data (RWO)"]
      P0["segment-primary-0\nPVC: data (RWO)"]
      P1["segment-primary-1\nPVC: data (RWO)"]
      M0["segment-mirror-0"]
      M1["segment-mirror-1"]
    end
    CM["ConfigMap: <cluster>-backup-s3-config"]
    SEC["Secret: backup-s3-credentials"]
    CLEAN["Job: gpbackman cleanup\n(cloudberry-backup:2.1.0)"]
  end
  MINIO[("MinIO svc minio:9000")]
  OP -- exec --> C0
  C0 --- P0
  C0 --- P1
  C0 --> MINIO
  P0 --> MINIO
  P1 --> MINIO
  CLEAN --> MINIO
  CM -. mounted/rendered .-> C0
  SEC -. env .-> C0
```

Key invariants:
- Each role pod owns its `data` RWO PVC (`builder.go:1170-1184`).
- `/data/backups` exists on **every** role pod (entrypoint change) so MPP
  create-dirs + `gpbackup_helper` pipes succeed locally.
- Bulk data goes to MinIO via the S3 plugin; only small metadata/TOC/history
  stay on PVCs.

See `dependency-map.md`, `api.md`, `data.md`, `security.md`, `requirement.md`.
