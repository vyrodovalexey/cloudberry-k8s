# Specification 11: Backup and Restore

## Overview

This specification defines the backup and restore capabilities for Cloudberry Database clusters managed by the operator. It covers scheduled and on-demand backups, retention policies, S3/MinIO storage destinations, and selective restore operations.

## CRD Specification

### BackupSpec

Added to `CloudberryClusterSpec`:

```yaml
backup:
  enabled: true
  schedule: "0 2 * * *"          # Daily at 2 AM
  retention:
    fullCount: 3                  # Keep 3 full backups
    incrementalCount: 10          # Keep 10 incremental backups
    maxAge: "30d"                 # Maximum backup age
  destination:
    type: s3                      # s3 or local
    bucket: cloudberry-backups
    endpoint: "http://minio:9000" # For MinIO
    region: us-east-1
    path: /backups
    credentialSecret:
      name: backup-credentials
      key: credentials
    forcePathStyle: true          # Required for MinIO
  compression: 6                  # 0-9 compression level
  parallelism: 2                  # Parallel backup workers
  incremental: false              # Enable incremental backups
```

### BackupStatus

Added to `CloudberryClusterStatus`:

- `lastBackupTime` - Timestamp of the last backup
- `lastBackupStatus` - Status of the last backup (Success, Failed, InProgress)

### Retention Policy

- `fullCount` - Number of full backups to retain
- `incrementalCount` - Number of incremental backups to retain per full backup
- `maxAge` - Maximum age of backups (e.g., "30d", "90d")

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/clusters/{name}/backups` | List all backups |
| POST | `/clusters/{name}/backups` | Create a new backup |
| GET | `/clusters/{name}/backups/{id}` | Get backup details |
| DELETE | `/clusters/{name}/backups/{id}` | Delete a backup |
| POST | `/clusters/{name}/backups/{id}/restore` | Restore from backup |

### Create Backup Request

```json
{
  "type": "full",
  "compression": 6,
  "parallelism": 2
}
```

### Restore Request

```json
{
  "targetDatabase": "mydb",
  "schemas": ["public", "analytics"],
  "tables": ["users", "orders"]
}
```

## CLI Commands

```bash
# Backup operations
cloudberry-ctl backup create [--type full|incremental] [--compression 6]
cloudberry-ctl backup list
cloudberry-ctl backup delete <backup-id>
cloudberry-ctl backup restore <backup-id> [--target-db mydb] [--schemas public]
cloudberry-ctl backup status
cloudberry-ctl backup schedule
```

## Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_backup_total` | Counter | cluster, namespace, type, result | Total backup operations |
| `cloudberry_backup_duration_seconds` | Histogram | cluster, namespace | Backup duration |
| `cloudberry_backup_size_bytes` | Gauge | cluster, namespace | Last backup size |
| `cloudberry_restore_total` | Counter | cluster, namespace, result | Total restore operations |

## Webhook Validation

- `backup.destination.type` is required when backup is enabled
- `backup.destination.bucket` is required for S3 destinations
- `backup.compression` must be between 0 and 9

## Webhook Defaults

- `compression`: 6
- `parallelism`: 1
- `retention.fullCount`: 3
- `retention.maxAge`: "30d"

## Non-Parallel Backup

For small databases or schema-only exports, the operator supports non-parallel
logical backup via `pg_dump`:

```bash
cloudberry-ctl backup create --type non-parallel --format custom --compression 6
cloudberry-ctl backup create --type non-parallel --format plain --schema-only
```

Supported formats: `plain`, `custom`, `directory`, `tar`.

## Cross-Cluster Migration

The operator supports parallel database migration between clusters:

```bash
cloudberry-ctl migrate --source-cluster src --target-cluster dst \
  --database mydb --tables "public.users,public.orders" --truncate
```

Post-migration validation includes row-count and checksum verification.

## Pre-Backup Health Checks

Before running a backup, the operator verifies:
- Destination disk space is sufficient
- Cluster is healthy (all segments up)
- No long-running transactions that could block backup

## Post-Restore Validation

After restoring, the operator:
- Verifies row counts against backup metadata
- Refreshes planner statistics (`ANALYZE`)
- Scans for invalid objects
- Confirms application connectivity
