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

Every backup or restore action is executed as a Kubernetes **Job** (or **CronJob** for scheduled backups). The operator never runs backup logic in the controller process itself.

| Action | Kubernetes Resource |
|---|---|
| Scheduled backup | `CronJob` → spawns `Job` on schedule |
| On-demand backup (API/CLI) | `Job` (created directly by the operator) |
| Restore from backup | `Job` (created directly by the operator) |
| Backup retention cleanup | `Job` (created by the operator or as a sidecar step in the CronJob) |

Jobs run in the same namespace as the `CloudberryCluster` and are labelled with `app.kubernetes.io/managed-by: cloudberry-operator`, `cloudberry.apache.org/cluster: <cluster-name>`, and `cloudberry.apache.org/operation: backup|restore|cleanup`.

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
    cloudberry.apache.org/cluster: <cluster>
    cloudberry.apache.org/operation: backup
  ownerReferences:
    - apiVersion: cloudberry.apache.org/v1
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
    "includeSchemas": ["public", "analytics"],
    "excludeTables": ["public.temp_data"],
    "leafPartitionData": false,
    "withStats": true,
    "withoutGlobals": false
  }
}
```

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

The operator runs a lightweight init container in each backup Job that verifies:

- Cluster health: all segments are up (`SELECT * FROM gp_segment_configuration WHERE status = 'd'` returns zero rows).
- No long-running transactions that could block backup (checks `pg_stat_activity` for transactions older than a configurable threshold).
- For S3 destinations: validates credentials and bucket accessibility via a HEAD request.
- For local destinations: validates available disk space on the PVC.
  If any check fails, the init container exits non-zero and the Job does not proceed. The failure is recorded in backup status and emitted as a Kubernetes Event.

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
