# Service & API Catalog — Backup/Restore

## REST API (operator, :8090)

| Method | Path | Description | Target-model change |
|---|---|---|---|
| GET | `/clusters/{name}/backups` | List backups (queries `gpbackman` history) | unchanged (network) |
| POST | `/clusters/{name}/backups` | Create backup; returns server-side TS | **now triggers coordinator-exec** instead of standalone Job |
| GET | `/clusters/{name}/backups/{ts}` | Backup details | unchanged |
| DELETE | `/clusters/{name}/backups/{ts}` | Delete backup (cleanup Job) | unchanged (network, `gpbackman`) |
| POST | `/clusters/{name}/backups/{ts}/restore` | Restore | **now triggers coordinator-exec** |
| GET | `/clusters/{name}/backups/jobs` | Job statuses | unchanged |
| GET | `/clusters/{name}/backups/schedule` | CronJob status | unchanged |

## Backup create (target exec command)

Request body (unchanged):
```json
{ "type": "full", "databases": ["mydb"] }
```

Operator action (target):
```bash
# rendered + exec'd INSIDE coordinator-0 (container: cloudberry)
envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml
gpbackup \
  --dbname mydb \
  --backup-dir /data/backups \
  --plugin-config /tmp/s3-config.yaml \
  --with-stats
```

## Restore (target exec command)

Request body (unchanged):
```json
{ "databases": ["mydb"], "gprestoreOptions": { "createDb": true } }
```

Operator action (target):
```bash
envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml
gprestore \
  --timestamp <TS> \
  --backup-dir /data/backups \
  --plugin-config /tmp/s3-config.yaml \
  --create-db
```

## Tool invocation contract (gpbackup storage plugin)

```mermaid
sequenceDiagram
  participant GB as gpbackup (coordinator)
  participant GH as gpbackup_helper (segment)
  participant PL as gpbackup_s3_plugin
  participant S3 as MinIO/S3
  GB->>PL: setup_plugin_for_backup(config)
  GB->>GB: write history DB @ /data/pgdata/gpseg-1
  GB->>GB: write metadata/TOC @ /data/backups
  GB->>GH: dispatch: create /data/backups/<seg> + open pipe
  GH->>PL: backup_data(stream)
  PL->>S3: PUT data file(s)
  GB->>PL: cleanup_plugin_for_backup
```

## Endpoints used by Scenario 71 e2e

- `POST /api/v1alpha1/clusters/<name>/backups?namespace=<ns>` → `{timestamp}`
- `POST /api/v1alpha1/clusters/<name>/backups/<ts>/restore?namespace=<ns>`

(See `test/e2e/scripts/scenario71-backup-restore.sh`.)
