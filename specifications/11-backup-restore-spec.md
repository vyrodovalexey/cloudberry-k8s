# Specification 11: Backup and Restore

## Overview

This specification defines the backup and restore capabilities for Cloudberry Database clusters managed by the operator. It leverages [`apache/cloudberry-backup`](https://github.com/apache/cloudberry-backup) (`gpbackup`, `gprestore`, `gpbackup_s3_plugin`, `gpbackup_exporter`) as the underlying backup tooling. The operator creates Kubernetes **Jobs** for on-demand backup/restore operations and **CronJobs** for scheduled backups, providing a fully declarative, cloud-native backup lifecycle.

## Architecture

### Backup Toolchain

The operator relies on the following binaries from the `apache/cloudberry-backup` project (v2.1.0-incubating or later):

- **`gpbackup`** — parallel logical backup utility that produces metadata files and per-segment compressed CSV data files.
- **`gprestore`** — parallel logical restore utility that consumes `gpbackup` backup sets identified by timestamp.
- **`gpbackup_helper`** — helper binary deployed on every segment host, required when using `--single-data-file`.
- **`gpbackup_s3_plugin`** — S3-compatible storage plugin for streaming backup data to and from S3/MinIO endpoints.
- **`gpbackup_exporter`** — Prometheus exporter that collects metrics from the `gpbackup` history database.
  All binaries are bundled into a dedicated backup container image (e.g. `cloudberry-backup:2.1.0`) used by Jobs and CronJobs.

### Execution Model

The operator builds Kubernetes **Jobs** and **CronJobs** for scheduling backup/restore work, and never runs backup logic in the controller process itself.

| Action | Kubernetes Resource |
|---|---|
| Scheduled backup | `CronJob` → spawns `Job` on schedule |
| On-demand backup (API/CLI) | `Job` (created directly by the operator) |
| Restore from backup | `Job` (created directly by the operator) |
| Backup retention cleanup | `Job` (created by the operator or as a sidecar step in the CronJob) |

Jobs run in the same namespace as the `CloudberryCluster` and are labelled with the operator's own API group `avsoft.io` (the CRD group is `avsoft.io/v1alpha1`): `app.kubernetes.io/managed-by: cloudberry-operator`, `avsoft.io/cluster: <cluster-name>`, `avsoft.io/component: backup`, and `avsoft.io/backup-operation: backup|restore` (the value is the operation).

#### MPP Dispatch and the Coordinator-Exec Data Cycle

`gpbackup` is a Massively Parallel Processing (MPP) tool. The coordinator dispatches to **every** segment over SSH (port 22) to create per-segment backup directories and run `gpbackup_helper`/`gpbackup_s3_plugin`. With the S3 plugin, only the **DATA** files are streamed to S3 by each segment's `gpbackup_s3_plugin`, while the **metadata** files and the `gpbackup` **history database** are written to the coordinator data directory.

A standalone backup Job pod is **not** a real segment host in `gp_segment_configuration`. Its plugin-config distribution (the per-run `/tmp/<ts>_s3-config.yaml`) therefore never reaches the segments, so a live data cycle cannot complete from an isolated Job pod alone. The supported live backup/restore **data cycle** runs `gpbackup`/`gprestore` **inside the coordinator pod** (the *coordinator-exec* model). The coordinator pod is segment `-1`: it has the `GPHOME` toolchain, the coordinator data directory, and the shared SSH identity required to dispatch to the segments.

The operator still builds backup/restore Jobs and CronJobs for scheduling (the resources documented below). The live data cycle is exercised by the Scenario 71 orchestration script `test/e2e/scripts/scenario71-backup-restore.sh`, which supports `EXEC_MODE=coordinator` (default — runs `gpbackup`/`gprestore` inside the coordinator pod) and `EXEC_MODE=rest` (creates a standalone backup Job via the operator REST API, kept for reference/compat).

#### Passwordless Inter-Pod SSH

To enable `gpbackup`'s coordinator→segment dispatch, the operator generates **one** shared `gpadmin` `ed25519` keypair per cluster, stored in the Secret `<cluster>-ssh-keys`. The keypair is mounted read-only at `/etc/cloudberry/ssh` (mode `0444`) into every cluster pod. The container entrypoint installs it into `~/.ssh` with the correct permissions (`0600` private key, `0644` public key) and writes a silent SSH client config (`StrictHostKeyChecking no`, `UserKnownHostsFile /dev/null`, `LogLevel ERROR`) so remote command output is not polluted by login noise.

#### Container SSH/PAM Requirement

The cluster image (`Dockerfile.cloudberry-official`) ships a minimal, container-friendly `/etc/pam.d/sshd` (session uses `pam_unix` only). The stock RHEL session stack — `pam_namespace`/`pam_selinux`/`pam_loginuid` pulled in via `session include password-auth`/`postlogin` — fails `pam_open_session()` inside containers, which makes `sshd` log "PAM session not opened, exiting" and causes every remote SSH command to exit `254`. The minimal `pam.d/sshd` is therefore a hard requirement for MPP backup dispatch.

#### Backup Toolchain Compatibility

The cluster image must carry version-matched `gpbackup`/`gprestore`/`gpbackup_helper`/`gpbackup_s3_plugin` (`2.1.0-incubating`, built with the `gpbackup` segment-crash patches PATCH 1–7, including the `lib/pq` driver fix so `gprestore` links the `"postgres"` driver). A symlink `/usr/local/bin/gpbackup_s3_plugin → $GPHOME/bin/gpbackup_s3_plugin` ensures the plugin config's `executablepath` resolves on every pod (the coordinator sends `executablepath` to the segments over SSH, so the path must exist cluster-wide).

#### Resource Sizing

Under amd64 emulation the `gpbackup_s3_plugin` can be memory-heavy during metadata uploads. The Scenario 71 sample clusters use **1Gi** coordinator/segment memory limits to avoid OOM-killing the plugin mid-upload.

## CRD Specification

### BackupSpec

Added to `CloudberryClusterSpec`:

```yaml
backup:
  enabled: true
  schedule: "0 2 * * *"                   # Cron expression; empty = no CronJob
  retention:
    fullCount: 3                           # Full backups to retain
    incrementalCount: 10                   # Incremental backups per full backup
    maxAge: "30d"                          # Maximum backup age
  destination:
    type: s3                               # s3 or local
    s3:
      bucket: cloudberry-backups
      endpoint: "http://minio:9000"        # For MinIO; omit for AWS
      region: us-east-1
      folder: /backups                     # S3 folder (maps to gpbackup_s3_plugin `folder`)
      encryption: "on"                     # on|off — S3 plugin SSL encryption
      forcePathStyle: true                 # Required for MinIO
      credentialSecret:                      # Option A: credentials from a Kubernetes Secret
        name: backup-s3-credentials        # Secret containing aws_access_key_id / aws_secret_access_key
        accessKeyField: aws_access_key_id
        secretKeyField: aws_secret_access_key
      vaultSecret:                           # Option B (alternative): credentials from Vault (requires spec.vault.enabled)
        path: secret/data/cloudberry/backup-s3  # Vault KV path holding the S3 credentials
        accessKeyField: aws_access_key_id    # defaults to aws_access_key_id
        secretKeyField: aws_secret_access_key # defaults to aws_secret_access_key
      multipart:
        backupMaxConcurrentRequests: 4     # backup_max_concurrent_requests
        backupMultipartChunksize: "10MB"   # backup_multipart_chunksize
        restoreMaxConcurrentRequests: 4    # restore_max_concurrent_requests
        restoreMultipartChunksize: "10MB"  # restore_multipart_chunksize
    local:
      path: /backups                       # Maps to gpbackup --backup-dir
      persistentVolumeClaim: backup-pvc    # PVC mounted into the Job pod
  gpbackup:
    compressionLevel: 1                    # --compression-level (1-9, default 1)
    compressionType: gzip                  # --compression-type (gzip|zstd)
    singleDataFile: false                  # --single-data-file
    copyQueueSize: 1                       # --copy-queue-size (requires singleDataFile)
    jobs: 1                                # --jobs (parallel backup workers)
    incremental: false                     # --incremental
    leafPartitionData: false               # --leaf-partition-data
    withStats: true                        # --with-stats
    withoutGlobals: false                  # --without-globals
    noCompression: false                   # --no-compression (overrides compressionLevel)
  gprestore:
    jobs: 1                                # --jobs (parallel restore workers)
    createDb: false                        # --create-db
    withGlobals: false                     # --with-globals
    withStats: true                        # --with-stats
    runAnalyze: false                      # --run-analyze
    onErrorContinue: false                 # --on-error-continue
    truncateTable: false                   # --truncate-table
  jobTemplate:                             # Optional: pod template overrides for all backup/restore Jobs
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
      limits:
        cpu: "2"
        memory: "2Gi"
    nodeSelector: {}
    tolerations: []
    serviceAccountName: cloudberry-backup-sa
    backoffLimit: 2
    activeDeadlineSeconds: 7200            # 2-hour timeout
    ttlSecondsAfterFinished: 86400         # Cleanup finished Jobs after 24h
  image: "cloudberry-backup:2.1.0"         # Backup toolchain container image
```

### BackupStatus

Added to `CloudberryClusterStatus`:

```yaml
backup:
  lastBackupTime: "2026-05-19T02:00:00Z"
  lastBackupTimestamp: "20260519020000"     # gpbackup YYYYMMDDHHMMSS timestamp
  lastBackupStatus: Success                # Success | Failed | InProgress
  lastBackupType: full                     # full | incremental
  lastBackupJobName: mycluster-backup-20260519020000
  cronJobName: mycluster-backup-schedule
  backupHistory:                           # Recent backup entries (last N)
    - timestamp: "20260519020000"
      type: full
      status: Success
      size: "2.4Gi"
      duration: "5m32s"
    - timestamp: "20260518020000"
      type: incremental
      status: Success
      size: "128Mi"
      duration: "1m12s"
```

## Operator Reconciliation Logic

### CronJob for Scheduled Backups

When `backup.enabled: true` and `backup.schedule` is set, the operator creates/updates a CronJob:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: <cluster>-backup-schedule
  namespace: <namespace>
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    avsoft.io/cluster: <cluster>
    avsoft.io/component: backup
    avsoft.io/backup-operation: backup
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: <cluster>
spec:
  schedule: "0 2 * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 2
      activeDeadlineSeconds: 7200
      ttlSecondsAfterFinished: 86400
      template:
        spec:
          restartPolicy: Never
          serviceAccountName: cloudberry-backup-sa
          containers:
            - name: gpbackup
              image: cloudberry-backup:2.1.0
              command: ["/bin/bash", "-c"]
              args:
                - |
                  # Generate S3 plugin config from env vars
                  envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml
 
                  gpbackup --dbname ${CBDB_DATABASE} \
                    --plugin-config /tmp/s3-config.yaml \
                    --single-data-file \
                    --compression-level ${COMPRESSION_LEVEL} \
                    --compression-type ${COMPRESSION_TYPE} \
                    --jobs ${BACKUP_JOBS} \
                    --with-stats
              env:
                - name: CBDB_DATABASE
                  value: "mydb"
                - name: PGHOST
                  value: "<coordinator-service>"
                - name: PGPORT
                  value: "5432"
                - name: COMPRESSION_LEVEL
                  value: "1"
                - name: COMPRESSION_TYPE
                  value: "gzip"
                - name: BACKUP_JOBS
                  value: "1"
                - name: AWS_ACCESS_KEY_ID
                  valueFrom:
                    secretKeyRef:
                      name: backup-s3-credentials
                      key: aws_access_key_id
                - name: AWS_SECRET_ACCESS_KEY
                  valueFrom:
                    secretKeyRef:
                      name: backup-s3-credentials
                      key: aws_secret_access_key
              volumeMounts:
                - name: s3-plugin-config
                  mountPath: /etc/gpbackup
          volumes:
            - name: s3-plugin-config
              configMap:
                name: <cluster>-backup-s3-config
```

**Backup Job/CronJob container env.** The operator emits `CBDB_DATABASE`, `PGHOST`, `PGPORT`, `COMPRESSION_LEVEL`, `COMPRESSION_TYPE`, and `BACKUP_JOBS` on both the on-demand backup/restore Job and the scheduled backup CronJob containers. `COMPRESSION_LEVEL`, `COMPRESSION_TYPE`, and `BACKUP_JOBS` are taken from `backup.gpbackup` and **default to `1`, `gzip`, and `1`** respectively when unset. `CBDB_DATABASE` is the backup target database (it is **empty for the CronJob**, whose databases are resolved at runtime). `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are injected via `SecretKeyRef` to `backup-s3-credentials`. These env vars are **informational/inspectable**: the `gpbackup`/`gprestore` CLI invocation still passes `--dbname` / `--compression-level` / `--compression-type` / `--jobs` as explicit args.

### Job for On-Demand Backup

When a user triggers a backup via the API or CLI, the operator creates a one-off Job with the same pod spec, adding any override parameters (e.g. `--include-schema`, `--include-table`, `--incremental --from-timestamp`).

### Job for Restore

For restore operations, the operator creates a Job running `gprestore`:

```yaml
containers:
  - name: gprestore
    image: cloudberry-backup:2.1.0
    command: ["/bin/bash", "-c"]
    args:
      - |
        envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml
 
        gprestore --timestamp ${RESTORE_TIMESTAMP} \
          --plugin-config /tmp/s3-config.yaml \
          --include-schema ${INCLUDE_SCHEMAS} \
          --include-table ${INCLUDE_TABLES} \
          --redirect-db ${REDIRECT_DB} \
          --redirect-schema ${REDIRECT_SCHEMA} \
          --jobs ${RESTORE_JOBS} \
          --on-error-continue \
          --with-stats \
          --run-analyze
```

### S3 Plugin Configuration

The operator generates a ConfigMap containing the `gpbackup_s3_plugin` configuration template:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: <cluster>-backup-s3-config
data:
  s3-plugin-config.yaml.tpl: |
    executablepath: /usr/local/bin/gpbackup_s3_plugin
    options:
      region: ${S3_REGION}
      endpoint: ${S3_ENDPOINT}
      aws_access_key_id: ${AWS_ACCESS_KEY_ID}
      aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
      bucket: ${S3_BUCKET}
      folder: ${S3_FOLDER}
      encryption: ${S3_ENCRYPTION}
      backup_max_concurrent_requests: ${BACKUP_MAX_CONCURRENT_REQUESTS}
      backup_multipart_chunksize: ${BACKUP_MULTIPART_CHUNKSIZE}
      restore_max_concurrent_requests: ${RESTORE_MAX_CONCURRENT_REQUESTS}
      restore_multipart_chunksize: ${RESTORE_MULTIPART_CHUNKSIZE}
```

**Path-style addressing (`forcePathStyle`):** the `gpbackup_s3_plugin` derives path-style addressing automatically when a custom (non-AWS) `endpoint` is configured — which is the case for MinIO. The operator surfaces `S3_FORCE_PATH_STYLE` (from `destination.s3.forcePathStyle`) as an environment variable for explicitness and observability; setting `forcePathStyle: true` together with a custom `endpoint` is the supported way to back up to MinIO.

> **Note:** `gpbackup_s3_plugin` 2.1.0 does **not** accept the `aws_signature_version` option (it rejects an unknown config key). The operator's generated S3 plugin config therefore no longer emits `aws_signature_version`; path-style addressing is auto-derived for custom MinIO endpoints via `forcePathStyle`.

**S3 credential sources.** S3 credentials are provided by one of two mutually-exclusive sources on `destination.s3`:

- `credentialSecret` — references an existing Kubernetes Secret (`name`, `accessKeyField`, `secretKeyField`). The backup/restore Job's `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are injected via `SecretKeyRef`.
- `vaultSecret` — references a Vault KV path (`path`, `accessKeyField`, `secretKeyField`); requires `spec.vault.enabled: true`. At reconcile time the operator reads the Vault path and **materializes** a Kubernetes Secret named `<cluster>-backup-s3-vault-creds` (owner-referenced to the cluster) holding the credentials, which the Job then consumes via `SecretKeyRef`. Credentials are never embedded in the Job spec as plaintext.

See **Scenario 71 — Enable Backup with Full S3 Configuration** in the test scenarios, which exercises both credential sources against MinIO with the full S3 config (folder, encryption, forcePathStyle, multipart) and performs a live backup → clean → restore cycle. The live data cycle runs via the coordinator-exec model (see [MPP Dispatch and the Coordinator-Exec Data Cycle](#mpp-dispatch-and-the-coordinator-exec-data-cycle)) and is driven by `test/e2e/scripts/scenario71-backup-restore.sh` for both the Secret and Vault credential variants. A real 100MB `mydb` backup → S3 (MinIO) → drop → restore cycle passes with matching row counts for both variants.

### Scenario 72 — Backup Infrastructure Deployment

**Scenario 72** verifies the backup **infrastructure** the operator deploys for a cluster with backups enabled — the toolchain image, the backup RBAC, the S3 plugin ConfigMap, the Job labels/namespace, the Job container env, and the `jobTemplate` pod-template overrides. Six verifications are covered by tests plus live checks:

1. **Image binaries** — the `gpbackup`, `gprestore`, and `gpbackup_s3_plugin` binaries are present in the `cloudberry-backup:2.1.0` toolchain image (verified live via `docker run`; the Job container uses the configured image).
2. **RBAC** — the `cloudberry-backup-sa` ServiceAccount and the `cloudberry-backup-role` Role (`secrets` get, `configmaps` get, `events` create/patch) plus the RoleBinding. The backup Job references `cloudberry-backup-sa`.
3. **S3 ConfigMap** — the generated `{cluster}-backup-s3-config` ConfigMap carries `executablepath: /usr/local/bin/gpbackup_s3_plugin` plus the region/endpoint/credentials/bucket/folder/encryption placeholders and the four multipart placeholders, and **no** `aws_signature_version` key.
4. **Job labels/namespace** — the Job lives in the cluster namespace and carries `app.kubernetes.io/managed-by: cloudberry-operator`, `avsoft.io/cluster: <cluster>`, `avsoft.io/component: backup`, and `avsoft.io/backup-operation: backup`.
5. **Job env + envsubst** — the container carries `CBDB_DATABASE`, `PGHOST`, `PGPORT`, `COMPRESSION_LEVEL`, `COMPRESSION_TYPE`, and `BACKUP_JOBS` (AWS creds via `SecretKeyRef` to `backup-s3-credentials`) and runs `envsubst` to render `/tmp/s3-config.yaml`.
6. **jobTemplate overrides** — `resources`, `nodeSelector`, `tolerations`, `serviceAccountName`, `backoffLimit`, `activeDeadlineSeconds`, and `ttlSecondsAfterFinished` all propagate to the built Job.

The scenario is driven by the sample CR `deploy/helm/cloudberry-operator/config/samples/scenario72-backup-infrastructure.yaml` (full S3 destination with an explicit `jobTemplate`) and is covered by `test/functional/scenario72_backup_infrastructure_test.go` and `test/e2e/scenario72_backup_infrastructure_e2e_test.go`.

### Scenario 73 — On-Demand Backup with gpbackup Options

**Scenario 73** verifies that an on-demand backup (`POST /api/v1alpha1/clusters/{name}/backups`) creates a Kubernetes **Job DIRECTLY** (not via the scheduled CronJob) and renders the per-request `gpbackupOptions` into the `gpbackup` CLI invocation. The 73a/73b options are supplied **per-request at trigger time via REST** — they are **not** baked into the CR; the sample CR's cluster-level `backup.gpbackup` defaults are harmless and are overridden by the per-request options. Two sub-cases are verified:

- **73a — Standard options.** `compressionLevel=6`, `compressionType=zstd`, `jobs=4`, `withStats=true`, `withoutGlobals=true`, `includeSchemas=[public, analytics]`. The built Job's `gpbackup` container args contain `--compression-level 6 --compression-type zstd --jobs 4 --with-stats --without-globals --include-schema public --include-schema analytics` (one `--include-schema` per schema), and the operator returns a `Job` (not a `CronJob`).
- **73b — noCompression override.** `noCompression=true` together with `compressionLevel=6`. The args contain `--no-compression` and **omit** `--compression-level` (and `--compression-type`): the compression level is ignored, confirming `--no-compression` precedence.

The scenario is driven by the sample CR `deploy/helm/cloudberry-operator/config/samples/scenario73-backup-options.yaml` (full S3 destination; the 73a/73b gpbackup options are supplied per-request via REST, not in the CR) and is covered by `test/functional/scenario73_backup_options_test.go` and `test/e2e/scenario73_backup_options_e2e_test.go`.

### Retention Cleanup Job

After each successful backup, the operator creates a short-lived cleanup Job that runs `gpbackman` (from the `apache/cloudberry-backup` project) to enforce retention policy. The cleanup logic:

1. Lists all backups via `gpbackman` history database.
2. Deletes backups exceeding `retention.fullCount` or `retention.incrementalCount`.
3. Deletes backups older than `retention.maxAge`.
   Alternatively, cleanup can run as an `initContainer` or post-backup step in the same Job.

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/clusters/{name}/backups` | List all backups (queries `gpbackman` history) |
| POST | `/clusters/{name}/backups` | Create a new backup (creates a Job) |
| GET | `/clusters/{name}/backups/{timestamp}` | Get backup details by gpbackup timestamp |
| DELETE | `/clusters/{name}/backups/{timestamp}` | Delete a backup (creates a cleanup Job) |
| POST | `/clusters/{name}/backups/{timestamp}/restore` | Restore from backup (creates a restore Job) |
| GET | `/clusters/{name}/backups/jobs` | List backup/restore Job statuses |
| GET | `/clusters/{name}/backups/schedule` | Get CronJob status and next run time |

### Create Backup Request

`POST /clusters/{name}/backups` accepts an optional `gpbackupOptions` object (`GpbackupOptionsRequest`) carrying **per-request** gpbackup option overrides. An on-demand backup creates a Kubernetes **Job DIRECTLY** (it does **not** go through the scheduled CronJob). The supplied options are merged over the cluster's `backup.gpbackup` defaults and rendered into the `gpbackup` CLI invocation on the Job container.

```json
{
  "type": "full",
  "databases": ["mydb"],
  "gpbackupOptions": {
    "compressionLevel": 6,
    "compressionType": "zstd",
    "jobs": 4,
    "singleDataFile": true,
    "copyQueueSize": 4,
    "incremental": false,
    "fromTimestamp": "",
    "includeSchemas": ["public", "analytics"],
    "excludeTables": ["public.temp_data"],
    "leafPartitionData": false,
    "withStats": true,
    "withoutGlobals": false,
    "noCompression": false
  }
}
```

The `gpbackupOptions` fields map to `gpbackup` flags as follows:

| Request field | gpbackup flag |
|---|---|
| `compressionLevel` | `--compression-level` |
| `compressionType` (`gzip`\|`zstd`) | `--compression-type` |
| `jobs` | `--jobs` |
| `singleDataFile` | `--single-data-file` |
| `copyQueueSize` (requires `singleDataFile`) | `--copy-queue-size` |
| `incremental` | `--incremental` |
| `fromTimestamp` | `--from-timestamp` |
| `includeSchemas` (repeated) | one `--include-schema` per schema |
| `excludeTables` (repeated) | one `--exclude-table` per table |
| `leafPartitionData` | `--leaf-partition-data` |
| `withStats` | `--with-stats` |
| `withoutGlobals` | `--without-globals` |
| `noCompression` | `--no-compression` |

**`noCompression` override semantics.** When `noCompression: true`, the operator invokes `gpbackup` with `--no-compression` and the compression level/type are **ignored**: it emits `--no-compression` and does **not** emit `--compression-level` / `--compression-type`, producing an uncompressed backup — even if `compressionLevel` (or `compressionType`) is also set in the same request. `--no-compression` therefore takes precedence over the compression options.

### Restore Request

```json
{
  "timestamp": "20260519020000",
  "databases": ["mydb"],
  "gprestoreOptions": {
    "jobs": 4,
    "redirectDb": "mydb_restored",
    "redirectSchema": "restored",
    "createDb": true,
    "includeSchemas": ["public", "analytics"],
    "includeTables": ["public.users", "public.orders"],
    "excludeTables": [],
    "withGlobals": false,
    "withStats": true,
    "runAnalyze": true,
    "onErrorContinue": true,
    "dataOnly": false,
    "metadataOnly": false,
    "truncateTable": false,
    "resizeCluster": false
  }
}
```

The `gprestoreOptions` fields map to `gprestore` flags as follows:

| Request field | gprestore flag |
|---|---|
| `timestamp` | `--timestamp` |
| `jobs` | `--jobs` |
| `redirectDb` | `--redirect-db` |
| `redirectSchema` | `--redirect-schema` |
| `createDb` | `--create-db` |
| `includeSchemas` (repeated) | one `--include-schema` per schema |
| `includeTables` (repeated) | one `--include-table` per table |
| `excludeTables` (repeated) | one `--exclude-table` per table |
| `withGlobals` | `--with-globals` |
| `withStats` | `--with-stats` |
| `runAnalyze` | `--run-analyze` |
| `onErrorContinue` | `--on-error-continue` |
| `dataOnly` | `--data-only` |
| `metadataOnly` | `--metadata-only` |
| `truncateTable` | `--truncate-table` |
| `resizeCluster` | `--resize-cluster` |

**`include-schema` / `include-table` mutual exclusivity (restore).** `gprestore`
enforces that `--include-schema` and `--include-table` may **not** be specified
together (it aborts with *"The following flags may not be specified together:
include-schema, include-table, include-table-file"*). When a **restore** request
supplies **both** `includeSchemas` and `includeTables`, the operator emits the
more specific `--include-table` (one per table — **table-level precedence**) and
**omits** `--include-schema`, keeping the `gprestore` invocation valid. When only
one of the two is set, that filter is emitted as-is. `--exclude-table` is
unaffected and is always emitted when present. This rule applies to the
**restore** path only; the **backup** path (`gpbackup`) accepts `--include-schema`
and `--include-table` together and is unchanged.

**`run-analyze` / `with-stats` mutual exclusivity (restore).** `gprestore`
enforces that `--run-analyze` and `--with-stats` may **not** be specified
together (it aborts with *"The following flags may not be specified together:
run-analyze, with-stats"*). When a **restore** request supplies **both**
`runAnalyze=true` and `withStats=true`, the operator emits `--run-analyze` and
**omits** `--with-stats` — **run-analyze precedence**: recomputing planner
statistics via `ANALYZE` supersedes restoring the backed-up statistics, so the
fresher result wins and the `gprestore` invocation stays valid. When only one of
the two is set, that flag is emitted as-is. This rule applies to the **restore**
path only; the **backup** path (`gpbackup --with-stats`) is unaffected and
unchanged.

### Scenario 74 — Single Data File + Copy Queue + gpbackup_helper + Full-Option Restore

**Scenario 74** triggers an on-demand **single-data-file** backup
(`singleDataFile=true`, `copyQueueSize=4`; the args contain `--single-data-file`
and `--copy-queue-size 4` and **omit** `--jobs`, which `gpbackup` rejects with
`--single-data-file`) followed by a full-option **restore** supplied per-request:
`jobs=4`, `redirectDb=mydb_restored`, `redirectSchema=restored`, `createDb`,
`onErrorContinue` (and `withGlobals=false`, `truncateTable=false`, which are
**omitted**). The restore request supplies **both** `includeSchemas=[public,
analytics]` and `includeTables=[public.users, public.orders]`; per the
mutual-exclusivity rule above the operator emits `--include-table public.users
--include-table public.orders` (table-level precedence) and **omits**
`--include-schema`. The restore also supplies **both** `withStats=true` and
`runAnalyze=true`; per the run-analyze/with-stats mutual-exclusivity rule the
operator emits `--run-analyze` (run-analyze precedence — ANALYZE recomputes the
statistics and verifies "ANALYZE ran (stats present)") and **omits**
`--with-stats`. The operator returns a `Job` (not a `CronJob`) for both the
backup and the restore.

**Single-data-file backup mechanics.** `--single-data-file` requires the
`gpbackup_helper` binary on **every** segment host — it consolidates each
segment's per-table COPY streams into one file. `gpbackup_helper` ships in the
cluster image (`cloudberry-official:2.1.0`, at `$GPHOME/bin/gpbackup_helper`,
symlinked cluster-wide); the live script asserts its presence before backing up.
A single-data-file backup produces exactly **one consolidated data file per
segment** (`gpbackup_<contentid>_<TS>.gz`) instead of many per-table data files,
plus a per-segment `gpbackup_<contentid>_<TS>_toc.yaml` and the shared
coordinator metadata. `--copy-queue-size` controls how many concurrent COPY
streams feed that single file (it replaces `--jobs`, which is invalid in
single-data-file mode).

**`--copy-queue-size` for single-data-file restore.** `gprestore` **rejects
`--jobs`** when restoring a single-data-file backup (*"Cannot use jobs flag when
restoring backups with a single data file per segment"*). The parallelism flag
for a single-data-file restore is `--copy-queue-size`, so the live data cycle
maps the request's `jobs=4` to `--copy-queue-size 4`. (The REST builder, which
targets the general multi-file restore path, emits `--jobs` for `jobs`; the
single-data-file `--copy-queue-size` substitution is applied on the
single-data-file restore path exercised by the live script.)

**`--redirect-schema` pre-existence requirement.** `gprestore`'s
`--redirect-schema` requires the target schema to **already exist** in the
redirect database (*"Schema … to redirect into does not exist"*). `--create-db`
creates the database but **not** the redirect schema, so the operator/live flow
**pre-creates both** the redirect database (`mydb_restored`) and the redirect
schema (`restored`) before running `gprestore`, then restores into the existing
schema.

**Verified outcome.** `mydb_restored` is created and populated (row counts > 0);
the `public.users` / `public.orders` objects are redirected into the `restored`
schema; and ANALYZE ran (`pg_stats` has rows for the restored tables —
`pg_stats` presence is the reliable signal, since `last_analyze` may be NULL on
segments).

The scenario is driven by the sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario74-single-data-file.yaml`
and is covered by `test/functional/scenario74_single_data_file_test.go`,
`test/e2e/scenario74_single_data_file_e2e_test.go`,
`internal/api/scenario74_restore_test.go`, and the live data cycle
`test/e2e/scripts/scenario74-single-data-file.sh`.

### Scenario 75 — Compression Matrix (gzip vs zstd)

**Scenario 75** triggers two on-demand full backups of the **same** data that
differ **only** by compression algorithm — `gzip` and `zstd` — at the **same**
compression level (`6`), so the comparison is apples-to-apples (same level,
different codec). The operator builds a `Job` **directly** (not a `CronJob`) for
each on-demand backup and renders `--compression-type gzip|zstd
--compression-level 6` from the per-request `gpbackupOptions`; `zstd` is passed
through verbatim with no special-casing. The functional/e2e tests
(`test/functional/scenario75_compression_matrix_test.go`,
`internal/api/scenario75_compression_test.go`) assert the rendered **builder
args** (`--compression-type gzip`/`zstd`, `--compression-level 6`) and that the
gzip and zstd arg slices differ in **exactly one** arg (the compression-type
value).

**Compression matrix on substantial public-schema data.** The live data cycle
(`test/e2e/scripts/scenario75-compression-matrix.sh`) exercises the matrix on the
**substantial `public` schema** (`public.users` + `public.orders`, ~189 MB at the
default `DATA_TARGET_MB`): **both** the gzip and the zstd backup are scoped with
`--include-schema public` at `--compression-level 6`. Both backups complete
cleanly (2/2 tables), their on-disk data-file totals are both `> 0` and **differ**
(the size comparison reports both totals and which codec is smaller), and **both**
restore cleanly into their own redirect databases (`mydb_gzip_restored` /
`mydb_zstd_restored`) with row counts matching the public-schema baseline
(`users=9533`, `orders=476625`).

**Verified run.** In the live verification the gzip backup's total data-file bytes
were **4,204,206** (~4.01 MiB) and the zstd backup's were **3,759,562**
(~3.59 MiB) — **zstd smaller by 444,644 bytes (~10.6%)**, the expected outcome at
the same compression level. Both restored cleanly into `mydb_gzip_restored` /
`mydb_zstd_restored` with row counts equal to the baseline (`users=9533`,
`orders=476625`).

**`zstd` CLI image requirement (zstd-backup prerequisite).** zstd-compressed
backups **require** the `zstd` command-line tool in the cluster image. `gpbackup`
pipes each segment's `COPY` output through `zstd --compress`
(`COPY … TO PROGRAM 'zstd --compress -N -c | gpbackup_s3_plugin …'`); without the
`zstd` CLI on the segment hosts the pipe breaks with *"could not write to COPY
program: Broken pipe"* and the zstd backup fails. `Dockerfile.cloudberry-official`
therefore installs the `zstd` package (`gzip` is already present in the base
image), so `cloudberry-official:2.1.0` carries both codecs. This is a hard
prerequisite for `compressionType: zstd` backups (gzip needs no extra package).

**Multipart-stability operational note.** The live matrix runs the
`gpbackup_s3_plugin` with the **CR's** multipart settings — `backup_multipart_chunksize: 10MB`,
`backup_max_concurrent_requests: 4` (and the matching restore values) — rather than
the plugin's `500MB × 6` default, which is unstable under amd64 emulation. These
smaller multipart settings keep the plugin stable across both the gzip and zstd
backups and their restores.

`mydb` is generated with a realistic tiny `analytics.daily_totals` aggregate
table (365 rows) **in addition** to the substantial public tables, but because the
backups are scoped to `--include-schema public` the analytics table is simply
**not part of the backup set**. This deliberately keeps the tiny aggregate out of
the backups: a **whole-DB zstd** backup consistently fails **only** on
`analytics.daily_totals` with `pq: command error message: (2F000)` — a
`gpbackup_s3_plugin` + zstd **small-file pipe** edge case under amd64 emulation
(it is **not** zstd-missing — the `zstd` CLI ships in `cloudberry-official:2.1.0`,
and both `zstd --compress` and `COPY ... TO PROGRAM 'zstd -c'` on that table
succeed; the plugin's tiny pipe is the trigger). Scoping both codecs to the same
`public` schema uses the substantial comparable data the matrix needs while
sidestepping the emulation edge case.

**`gpbackup` data-file extensions differ by codec.** `gpbackup` names per-segment
data files by compression algorithm: **gzip** backups produce
`gpbackup_<contentid>_<TS>.gz` and **zstd** backups produce
`gpbackup_<contentid>_<TS>.zst`. The live size-measurement step therefore matches
**both** the `.gz` and `.zst` data-file extensions for the respective timestamps
(excluding the `*_toc.yaml` / `*_metadata.sql` / `*_config.yaml` / `*_report` /
`*_plugin_config.yaml` sidecars, which never carry those extensions) before
asserting that the gzip and zstd totals are both `> 0` and not equal.

The scenario is driven by the sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario75-compression-matrix.yaml`
and is covered by `test/functional/scenario75_compression_matrix_test.go`,
`test/e2e/scenario75_compression_matrix_e2e_test.go`,
`internal/api/scenario75_compression_test.go`, and the live data cycle
`test/e2e/scripts/scenario75-compression-matrix.sh`.

### Scenario 76 — Scheduled Backup via CronJob + Status Population

**Scenario 76** verifies the **scheduled** backup path: setting `spec.backup.schedule`
causes the operator to reconcile a **CronJob** that fires on schedule and spawns a
backup **Job**, after which the operator **populates the backup status** on the
`CloudberryCluster` from the completed Job. Unlike the on-demand scenarios (73–75),
which create a `Job` **directly**, Scenario 76 exercises the `CronJob → Job` mechanism.

**CronJob spec.** When `backup.enabled: true` and `backup.schedule` is set, the
operator creates/updates the CronJob `{cluster}-backup-schedule` (for this scenario,
`scenario76-backup-schedule`) with:

- `ownerReferences` → the `CloudberryCluster` (the CronJob is garbage-collected with the cluster);
- `concurrencyPolicy: Forbid` (a new run is skipped while a previous run is still active);
- `successfulJobsHistoryLimit: 3` and `failedJobsHistoryLimit: 3`;
- a `jobTemplate` whose pod `restartPolicy` is `Never`.

When the CronJob fires, Kubernetes spawns a Job named `{cluster}-backup-schedule-<hash>`
(the `<hash>` is Kubernetes' CronJob run suffix and does **not** embed a parseable
`gpbackup` timestamp — see the timestamp guarantee below).

**Status population.** After a backup Job **succeeds**, the operator populates on
`CloudberryClusterStatus.backup`:

- `lastBackupTime` — the backup completion time (RFC3339);
- `lastBackupTimestamp` — a **14-digit `YYYYMMDDHHMMSS`** value (see guarantee below);
- `lastBackupStatus: Success` (`Success | Failed | InProgress`);
- `lastBackupType: full` (`full | incremental`);
- `lastBackupJobName` — the name of the backup Job that produced the status;
- `cronJobName` — `{cluster}-backup-schedule`;
- `backupHistory[]` — recent backup entries, each carrying `timestamp`, `type`,
  `status`, **`size`**, and `duration`.

**`backupHistory` `size` field.** Each `backupHistory` entry now includes `size`,
derived (best-effort) from the backup Job's `avsoft.io/backup-size-bytes` annotation.
When the annotation is unavailable the field is left empty; it is no longer omitted
from history.

**14-digit timestamp guarantee.** `lastBackupTimestamp` is **always** a valid 14-digit
`YYYYMMDDHHMMSS` value. For on-demand Jobs (`{cluster}-backup-<TS>`) the operator keeps
the timestamp **embedded in the Job name**. For CronJob-spawned Jobs
(`{cluster}-backup-schedule-<hash>`), whose names do **not** embed a parseable
timestamp, the operator **derives** the timestamp from the Job's `CompletionTime`
(in **UTC**), so scheduled backups still report a well-formed 14-digit timestamp.

**Steady-state status refresh.** Backup status (`lastBackup*` and `backupHistory`) is
refreshed on the operator's **periodic reconcile** even when the cluster spec
**generation is unchanged** (steady state). The operator keeps the backup status up to
date from completed backup Jobs on an ongoing basis — not only when the spec changes.
This steady-state refresh is the key behavior that makes scheduled-backup status
population work: the CronJob's Job completes asynchronously (no spec change), and the
next periodic reconcile discovers it and updates the status.

**Execution model (live verification).** The CronJob firing and spawning a Job verifies
the **schedule mechanism** plus the CronJob spec (`Forbid`, `3/3` history,
`ownerReferences`, pod `restartPolicy: Never`). As with the other scenarios, a
standalone CronJob Job pod is **not** a segment host in `gp_segment_configuration`, so
the real `gpbackup` **data cycle** runs via the proven **coordinator-exec** path (see
[MPP Dispatch and the Coordinator-Exec Data Cycle](#mpp-dispatch-and-the-coordinator-exec-data-cycle));
status population is then verified from the resulting successful backup. For testing, a
near-future schedule (`*/2 * * * *`) is used so the CronJob fires within ~2 minutes; the
sample CR ships the production schedule `0 2 * * *`.

**Verified outcome.** A scheduled backup completes and the operator populates the status:
`lastBackupTimestamp=20260607224409` (14-digit), `lastBackupStatus=Success`,
`lastBackupType=full`, and `backupHistory[0]={timestamp, type: full, status: Success,
size: 4204206, duration: 4s}`. `cronJobName` is `scenario76-backup-schedule` and
`lastBackupJobName` matches the backup Job.

The scenario is driven by the sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario76-scheduled-backup.yaml`
(schedule `0 2 * * *`, overridden to `*/2 * * * *` by the live test) and is covered by
`test/functional/scenario76_scheduled_backup_test.go`,
`test/e2e/scenario76_scheduled_backup_e2e_test.go`, and the live data cycle
`test/e2e/scripts/scenario76-scheduled-backup.sh`.

## CLI Commands

```bash
# Backup operations (each creates a Kubernetes Job)
cloudberry-ctl backup create --cluster mycluster --database mydb \
  [--type full|incremental] \
  [--compression-level 6] [--compression-type zstd] \
  [--jobs 4] [--single-data-file] [--copy-queue-size 4] \
  [--include-schema public] [--exclude-table public.temp] \
  [--incremental] [--from-timestamp 20260518020000] \
  [--leaf-partition-data] [--with-stats] [--without-globals]
 
cloudberry-ctl backup list --cluster mycluster
cloudberry-ctl backup status --cluster mycluster --timestamp 20260519020000
cloudberry-ctl backup delete --cluster mycluster --timestamp 20260519020000
 
# Restore operations (each creates a Kubernetes Job)
cloudberry-ctl backup restore --cluster mycluster --timestamp 20260519020000 \
  [--redirect-db mydb_restored] [--redirect-schema restored] \
  [--create-db] \
  [--include-schema public] [--include-table public.users] \
  [--jobs 4] [--with-stats] [--run-analyze] \
  [--on-error-continue] [--truncate-table] [--resize-cluster]
 
# Schedule management
cloudberry-ctl backup schedule --cluster mycluster             # Show CronJob status
cloudberry-ctl backup schedule set --cluster mycluster --cron "0 3 * * *"
cloudberry-ctl backup schedule suspend --cluster mycluster
cloudberry-ctl backup schedule resume --cluster mycluster
 
# Job monitoring
cloudberry-ctl backup jobs --cluster mycluster                 # List all backup/restore Jobs
cloudberry-ctl backup jobs logs --cluster mycluster --job <job-name>
```

## Prometheus Metrics

The operator deploys `gpbackup_exporter` as a sidecar `Deployment` (or as part of the operator's metric endpoint) to expose the following metrics sourced from the `gpbackup` history database:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cloudberry_backup_total` | Counter | cluster, namespace, type, status | Total backup operations by type and result |
| `cloudberry_backup_duration_seconds` | Histogram | cluster, namespace, type | Backup duration distribution |
| `cloudberry_backup_size_bytes` | Gauge | cluster, namespace, timestamp | Backup size per timestamp |
| `cloudberry_backup_last_success_timestamp` | Gauge | cluster, namespace | Unix timestamp of last successful backup |
| `cloudberry_backup_last_status` | Gauge | cluster, namespace | 0=success, 1=failed, 2=in-progress |
| `cloudberry_restore_total` | Counter | cluster, namespace, status | Total restore operations |
| `cloudberry_restore_duration_seconds` | Histogram | cluster, namespace | Restore duration distribution |
| `cloudberry_backup_retention_deleted_total` | Counter | cluster, namespace | Backups deleted by retention policy |
| `cloudberry_backup_job_status` | Gauge | cluster, namespace, job, operation | Kubernetes Job status (0=pending, 1=running, 2=succeeded, 3=failed) |

## Webhook Validation

- `backup.destination.type` is required when `backup.enabled: true`
- `backup.destination.s3.bucket` is required when `destination.type: s3`
- For `destination.type: s3`, S3 credentials must be provided via **either** `backup.destination.s3.credentialSecret.name` **or** `backup.destination.s3.vaultSecret.path`. Rejection occurs only when **neither** is specified. (`vaultSecret.path` references a Vault KV path and requires `spec.vault.enabled: true` at runtime.)
- `backup.gpbackup.compressionLevel` must be between 1 and 9 (a value of `0` is rejected by the validator as an explicit invalid level; an omitted field is defaulted to `1` by the mutating webhook before validation)
- `backup.gpbackup.compressionType` must be `gzip` or `zstd`
- `backup.gpbackup.copyQueueSize` requires `singleDataFile: true`
- `backup.gpbackup.jobs` cannot be combined with `singleDataFile: true`
- `backup.gpbackup.incremental` requires `leafPartitionData: true`
- `backup.schedule` must be a valid cron expression when provided
- `backup.image` must be non-empty when `backup.enabled: true`

Each rejected request returns a descriptive webhook error naming the offending field, and the object is **not persisted**. See **Scenario 69 — Webhook Validation (All Rules)** in the test scenarios for the complete negative-test matrix (69a–69j).
## Webhook Defaults

- `gpbackup.compressionLevel`: `1`
- `gpbackup.compressionType`: `gzip`
- `gpbackup.jobs`: `1`
- `gpbackup.singleDataFile`: `false`
- `gpbackup.withStats`: `true`
- `gprestore.jobs`: `1`
- `gprestore.withStats`: `true`
- `retention.fullCount`: `3`
- `retention.maxAge`: `"30d"`
- `jobTemplate.backoffLimit`: `2`
- `jobTemplate.activeDeadlineSeconds`: `7200`
- `jobTemplate.ttlSecondsAfterFinished`: `86400`

Defaults are applied by the mutating admission webhook **only when `backup.enabled: true`** and only to fields the user left unset (explicit values are preserved). After admission, the persisted object reflects these defaults. See **Scenario 70 — Webhook Defaults** in the test scenarios, which applies a minimal backup spec (enabled, destination, image only) and verifies all twelve defaulted fields on the persisted object.

## Incremental Backup Support

`gpbackup` supports incremental backups that only capture changes to append-optimized tables since the last full or incremental backup. The operator manages incremental backup sets as follows:

1. When `gpbackup.incremental: true` in the CronJob spec, the operator configures `gpbackup --incremental --leaf-partition-data`.
2. `gpbackup` automatically locates the most recent compatible backup to form or extend a backup set.
3. The `--from-timestamp` flag can be supplied via the API/CLI to explicitly pin the base backup.
4. Restore of an incremental backup requires the full set (full + all incrementals). `gprestore` validates completeness before proceeding.
   Recommended schedule pattern:

```yaml
# Weekly full + daily incremental
backup:
  schedule: "0 2 * * *"
  gpbackup:
    incremental: true    # Daily runs produce incrementals
# A separate CronJob or manual trigger for weekly full:
# cloudberry-ctl backup create --cluster mycluster --database mydb --type full
```

## Cross-Cluster Migration

The operator supports parallel database migration between clusters by creating a coordinated pair of Jobs:

```bash
cloudberry-ctl migrate --source-cluster src --target-cluster dst \
  --database mydb \
  --tables "public.users,public.orders" \
  --truncate
```

This internally:

1. Creates a backup Job on the source cluster using `gpbackup --include-table ... --single-data-file --plugin-config ...`
2. Creates a restore Job on the target cluster using `gprestore --timestamp ... --redirect-db ... --plugin-config ... --truncate-table`
3. Both Jobs share the same S3 destination.
4. Post-migration validation Job runs row-count and checksum verification.
## Pre-Backup Health Checks

Every backup Job (on-demand and CronJob-spawned) carries a `pre-backup-check`
**init container** that the operator prepends to the pod spec
(`addPreBackupCheckInitContainer` in `internal/builder/backup_builder.go`). It
shares the backup image, environment, and volume mounts with the main `gpbackup`
container, so it connects to the coordinator and reaches the destination exactly
as the backup will. Init-container semantics make the check **blocking**: if any
sub-check exits non-zero the `gpbackup` container **never starts** and the Job
fails (after `backoffLimit` is exhausted).

The script (`preBackupCheckScript` / `preBackupDestinationCheck`) runs under
`set -euo pipefail` and performs **four** sub-checks, each of which **blocks**
(exits non-zero) on a fault:

| # | Check | Test | Threshold / constant | Blocks when |
|---|-------|------|----------------------|-------------|
| **77a** | Segments-up | `SELECT count(*) FROM gp_segment_configuration WHERE status='d'` | — | count `> 0` (any down segment) |
| **77b** | Long-running transaction | `pg_stat_activity` where `state <> 'idle'` and `now() - xact_start > interval` | `longRunningTxnThresholdSeconds = 3600` (1 hour) | count `> 0` (any txn older than the threshold) |
| **77c** | S3 reachability (S3 destinations) | fail-closed SigV4-signed HTTP **HEAD** to `${S3_ENDPOINT}/${S3_BUCKET}` (path-style) | `s3ReachabilityMaxTimeSeconds = 15` (curl `--max-time`) | response is **non-2xx/3xx** (403/404/`000`/connection refused) |
| **77d** | Local disk space (local destinations) | `df -Pk <backup-dir>` free KB | `minBackupDiskFreeKB = 1048576` KiB (**1 GiB**) | free space `< 1 GiB` |

### Sub-check details

- **77a — Segments-up.** A single down segment (`status='d'`) blocks the backup so
  `gpbackup`'s coordinator→segment dispatch is not attempted against an
  incomplete cluster. Heal by recovering the segment (`gprecoverseg`).

- **77b — Long-running transaction.** A transaction older than
  `longRunningTxnThresholdSeconds` (3600 s) in a non-idle state blocks the
  backup, since long-held transactions can stall `gpbackup`'s metadata locks.
  Heal by committing/rolling back or terminating the offending backend.

- **77c — S3 reachability (fail-closed SigV4 HEAD).** For S3 destinations the
  check issues a **SigV4-signed** HTTP `HEAD` against `${S3_ENDPOINT}/${S3_BUCKET}`
  using **path-style** addressing (MinIO-compatible) and the
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` already injected into the init
  container. The signing mirrors the `openssl`-HMAC SigV4 approach in
  `test/docker-compose/scripts/setup-minio.sh` (canonical headers
  `host;x-amz-content-sha256;x-amz-date`, signing region from `${S3_REGION}` and
  defaulting to `us-east-1`). A `2xx`/`3xx` response means the bucket is
  reachable; **any other outcome blocks the backup** — wrong credentials
  (`403`), a missing bucket (`404`), or an unreachable endpoint (`000` /
  connection refused, bounded by `curl --max-time 15` so a hung endpoint
  **fails closed** instead of stalling). This **replaces** the prior best-effort
  `aws s3 ls` probe, which never failed; S3 reachability is now a real blocking
  pre-condition.

- **77d — Local disk space.** For local (PVC-backed) destinations the check runs
  `df -Pk` against the backup directory and blocks the backup when free space is
  below `minBackupDiskFreeKB` (1048576 KiB = **1 GiB**), so a near-full PVC does
  not cause a partial backup. Heal by freeing space on the PVC.

The destination check (77c/77d) runs only for the matching `destination.type`:
when `backup` is nil or the destination type is unknown, `preBackupDestinationCheck`
returns an empty string (no destination check) while 77a/77b still apply.

### Failure-handling contract

When a backup-operation Job fails (a blocked pre-check or a failed backup
execution), the operator:

1. **Records `status.backup.lastBackupStatus = Failed`** and appends a
   `backupHistory[]` entry with `status=Failed` (see
   `applyBackupJobToStatus`), and sets the `cloudberry_backup_last_status` gauge
   to failed.
2. **Emits a Kubernetes Warning Event** with **type `Warning`**, reason
   **`BackupFailed`** (`api/v1alpha1.EventReasonBackupFailed`), `involvedObject`
   = the `CloudberryCluster`, naming the failed Job and its timestamp.

The Warning is **de-duplicated per failed Job**: `emitBackupFailureEvent`
(`internal/controller/admin_controller.go`) emits the event only on a real
**transition into `Failed`** for a given Job name — it captures the previous
`lastBackupStatus`/`lastBackupJobName` before overwriting and skips emission when
the same Job was already recorded as `Failed`. This means periodic reconciles of
an unchanged failed Job produce **exactly one** Warning, distinct failed Jobs
each produce their own Warning, and **restore-operation** Job failures are
**excluded** (Scenario 77 is backup-only). Healing the fault and triggering a
fresh backup lets all four checks pass and the Job reach `Success`, transitioning
`lastBackupStatus` back to `Success`.

### Scenario 77 — Pre-Backup Health Checks

**Scenario 77** verifies the `pre-backup-check` init container blocks the backup
when any of the four sub-checks (77a–77d) fails, records
`lastBackupStatus=Failed`, and emits the de-duplicated `Warning`/`BackupFailed`
Event; healing the fault then lets a fresh backup reach `Success`. Because the
checks split by destination type, the scenario uses **two** sample clusters:

- **`scenario77-s3`** — S3 (MinIO) destination, Secret credentials. Exercises
  **77a** (segments-up), **77b** (long-running txn), and **77c** (S3
  reachability — a SigV4 HEAD against a wrong bucket/creds returns non-2xx, so
  the init container blocks). Sample CR
  `deploy/helm/cloudberry-operator/config/samples/scenario77-s3-prebackup.yaml`.
- **`scenario77-local`** — local (PVC-backed) destination with a small ~2Gi
  backup PVC (`scenario77-backup-pvc`). Exercises **77d** (disk-fill below the
  1 GiB free threshold) and can re-run 77a/77b. Sample CR
  `deploy/helm/cloudberry-operator/config/samples/scenario77-local-prebackup.yaml`.

**Acceptance (per sub-check):** inject the fault → the init container exits
non-zero (the `gpbackup` container never starts) → the Job reaches Failed →
`status.lastBackupStatus=Failed` with a `backupHistory[]` `status=Failed` entry
→ a `Warning`/`BackupFailed` Event references the failed Job (emitted **once**
per failed Job) → heal the fault → a fresh on-demand backup passes all four
checks and reaches `Success` with `lastBackupStatus=Success`.

The scenario is covered by `test/functional/scenario77_prebackup_checks_test.go`,
`test/e2e/scenario77_prebackup_checks_e2e_test.go`,
`internal/controller/backup_event_scenario77_test.go`, and the live verification
script `test/e2e/scripts/scenario77-prebackup-checks.sh`.

## Post-Restore Validation

After a restore Job completes successfully, the operator optionally creates a validation Job that:

- Verifies row counts against backup metadata stored in the `gpbackup` history tables.
- Refreshes planner statistics (`ANALYZE`) — handled by `gprestore --run-analyze` when enabled.
- Scans for invalid objects (`SELECT * FROM pg_catalog.pg_class WHERE relkind = 'i' AND NOT indisvalid` on indexes).
- Confirms application connectivity by executing a configurable health-check query.
## RBAC Requirements

The operator creates a `ServiceAccount`, `Role`, and `RoleBinding` for backup Jobs:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: cloudberry-backup-sa
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: cloudberry-backup-role
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get"]
    resourceNames: ["backup-s3-credentials"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get"]
    resourceNames: ["<cluster>-backup-s3-config"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
```

## Security Considerations

- S3 credentials are stored in Kubernetes Secrets and injected as environment variables — never written to disk or ConfigMaps.
- The S3 plugin configuration template uses `envsubst` at runtime so credentials are resolved only inside the ephemeral Job pod.
- Backup Jobs run with a dedicated `ServiceAccount` scoped to the minimum required RBAC permissions.
- When `destination.s3.encryption: "on"` (default), all traffic to S3/MinIO uses TLS.
- The operator supports `imagePullSecrets` on the job template for private container registries.
