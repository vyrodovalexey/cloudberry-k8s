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
| Backup retention cleanup | `Job` (created by the operator after each successful backup — `<cluster>-cleanup-<ts>`, runs `gpbackman`) |

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
    imagePullSecrets:                      # Optional: pull the backup image from a private registry
      - name: regcred                      # dockerconfigjson Secret; propagated to all backup Job pods
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

## Local Backup Destination

In addition to S3/MinIO, the operator supports a **local** (PVC-backed) backup destination end-to-end. With `destination.type: local` the backup/restore toolchain writes to a `gpbackup`/`gprestore` **`--backup-dir`** on a mounted PersistentVolumeClaim instead of streaming through the `gpbackup_s3_plugin`. The local path is **never** an S3 plugin path, so no S3 plugin config is rendered, no S3 ConfigMap is created, and no Vault/Secret S3 credentials are materialized.

### Local destination config block

```yaml
backup:
  enabled: true
  image: "cloudberry-backup:2.1.0"
  destination:
    type: local                              # s3 or local
    local:
      path: /backups                          # Maps to gpbackup/gprestore --backup-dir (default /backups)
      persistentVolumeClaim: backup-pvc       # PVC mounted into the Job pod at local.path
```

| Field | Maps to | Default | Meaning |
|-------|---------|---------|---------|
| `destination.type` | — | — | `local` selects the PVC/`--backup-dir` path; `s3` selects the plugin path. |
| `destination.local.path` | `gpbackup`/`gprestore` `--backup-dir <path>` | `/backups` | On-pod directory the backup set is written to / read from. Single source of truth for the args, the volume mount, and the retention script. |
| `destination.local.persistentVolumeClaim` | PVC volume `backup-data` mounted at `local.path` | — | The named PVC the backup/restore Job pod mounts read-write at `local.path`. |

### What the operator does for a local destination

- **PVC mount (`buildBackupVolumes` / `buildBackupVolumeMounts`).** When `destination.type: local` and `local.persistentVolumeClaim` is non-empty, the operator appends a `PersistentVolumeClaim` volume named **`backup-data`** (`ClaimName: <local.persistentVolumeClaim>`) and mounts it at `local.path` (default `/backups`) on the `gpbackup`/`gprestore` container. There is **no** `s3-plugin-config` ConfigMap volume and **no** `/etc/gpbackup` mount.
- **`--backup-dir`, no S3 plugin (`backupDestinationArgs`).** The `gpbackup`/`gprestore` args are seeded by `backupDestinationArgs(cluster)`: local → `["--backup-dir", <local.path|/backups>]`; s3 → `["--plugin-config", "/tmp/s3-config.yaml"]` (nil/unknown destinations default to the S3 leading args, keeping every existing S3 caller byte-identical). For local the args therefore contain `--backup-dir <path>` and **never** `--plugin-config` / `/tmp/s3-config.yaml`. `buildGpbackupArgs` / `buildGprestoreArgs` take the `cluster` and use this helper.
- **No S3 render in the tool script (`renderToolScript`).** For a local destination the tool script **skips** the `envsubst` block that renders `/etc/gpbackup/s3-plugin-config.yaml.tpl` → `/tmp/s3-config.yaml`. (A local Job has no S3 ConfigMap mounted at `/etc/gpbackup`, so reading the missing template under `set -euo pipefail` would otherwise abort the Job before `gpbackup` runs.) The `gpEnvPreamble` / `sshSetupPreamble` / plugin-path preambles are retained for both destinations — they are harmless and SSH is still required to reach the segments.
- **No S3 env, no S3 ConfigMap, no Vault S3 creds.** `buildBackupEnv` emits only `PG*`/database env for a non-S3 destination (no `S3_*` / `AWS_*` vars). `BuildBackupS3ConfigMap` returns nil for non-S3, so `ensureBackupS3ConfigMap` creates **no** `<cluster>-backup-s3-config` ConfigMap. `ensureBackupS3VaultCredentials` no-ops for local (`Destination.S3 == nil`), so **no** `<cluster>-backup-s3-vault-creds` Secret is created and no Vault read is attempted.
- **Local disk-space pre-backup check (`preBackupDestinationCheck`).** For local destinations the `pre-backup-check` init container runs `df -Pk <backup-dir>` and **blocks** the backup (`exit 1`) when free space is below `minBackupDiskFreeKB` (1048576 KiB = **1 GiB**) — Scenario 77d. S3 destinations instead get the SigV4 HEAD reachability check (77c); 77a/77b apply to both.
- **gpbackman retention with `--backup-dir` (`buildGpbackmanRetentionScript`).** For a local destination the retention cleanup script sets `DEST_FLAGS` to `--backup-dir <path>` (not `--plugin-config <rendered>`) and **skips** the S3 plugin-config render, so `gpbackman backup-delete` / `backup-clean` operate against the local backup directory. S3 retention is unchanged.

### MPP per-segment `--backup-dir` note

`gpbackup --backup-dir <dir>` is a Massively Parallel Processing operation: the coordinator dispatches over SSH to **every** segment host and each writes its **per-segment** backup set under `<dir>` on **that host** (the coordinator holds the coordinator/metadata set; each segment host holds its own data set). The operator mounts the named PVC at `local.path` **on the single backup/restore Job pod** and passes `--backup-dir <local.path>`; an RWO PVC mounted on one pod therefore holds **one** backup set, not the full cluster-wide per-segment fan-out.

This is a deliberate, documented model: the **operator behavior** (PVC mounted at `local.path`, `--backup-dir` in the args, no plugin/ConfigMap) is what the Job spec encodes and what the functional/integration/e2e tests assert. A real cluster-wide `gpbackup --backup-dir` requires the directory to exist and be writable on the coordinator **and** every segment host, so the live data cycle targets a segment-visible `--backup-dir` (e.g. one under each pod's writable `/tmp`) via the coordinator-exec model (see [MPP Dispatch and the Coordinator-Exec Data Cycle](#mpp-dispatch-and-the-coordinator-exec-data-cycle)), while the PVC mount is proven by a write/`ls` probe on the Job pod.

### Scenario 81 — Local Destination Backup/Restore

**Scenario 81** verifies the local (PVC-backed) destination backup/restore path described above: the backup/restore Job mounts the named PVC at `local.path`, `gpbackup`/`gprestore` run with `--backup-dir <path>` and **no** S3 plugin, no S3 ConfigMap/Vault credentials are created, and a real local backup → restore cycle round-trips with matching row counts. Acceptance is split per sub-case:

- **81a — Local backup Job spec.** For `destination.type: local`, the backup Job mounts the named PVC (`backup-pvc`) as the `backup-data` volume at `local.path` (`/backups`); the `gpbackup` container args contain `--backup-dir /backups` and **not** `--plugin-config` / `/tmp/s3-config.yaml`; there is **no** `s3-plugin-config` volume, **no** `/etc/gpbackup` mount, and **no** `S3_*`/`AWS_*` env; and the operator creates **no** `<cluster>-backup-s3-config` ConfigMap and **no** `<cluster>-backup-s3-vault-creds` Secret. The rendered tool script omits the `/etc/gpbackup` → `/tmp/s3-config.yaml` render, so the local Job does not crash under `set -euo pipefail`.

- **81b — PVC writable.** The `backup-pvc` PVC mounts read-write at `/backups`; a small write/`ls` probe on the mount confirms the operator's PVC wiring.

- **81c — Real local backup.** A real `gpbackup --backup-dir <dir>` (NO `--plugin-config`) completes (`Backup completed successfully`) and lands per-segment backup files under `<dir>/backups/<datestamp>/<TS>/` on the coordinator and every segment host (proving the `--backup-dir` toolchain end-to-end).

- **81d — Real local restore.** A real `gprestore --timestamp <TS> --backup-dir <dir> --create-db` (NO `--plugin-config`) completes (`Restore completed successfully`) into a fresh database with row counts equal to the source baseline.

**Live-run notes.** The sample CR uses a **single local destination** (`destination.type: local`, `local.path: /backups`, `local.persistentVolumeClaim: backup-pvc`) and declares the `backup-pvc` PVC (~5Gi; Scenario 78 lesson — do **not** fill hostpath PVCs) in the same manifest, so a single `kubectl apply` creates both. No `schedule` is set (on-demand). The Job-spec assertions (81a/81b) prove the **operator** behavior for the PVC/`--backup-dir`/no-plugin wiring; because a single RWO PVC on a standalone Job pod is not a segment host (see the MPP note above), the real `gpbackup`/`gprestore` data cycle (81c/81d) runs via the coordinator-exec path into a segment-visible `--backup-dir` (e.g. `/tmp/scenario81-backups`).

The scenario is driven by the sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario81-local-destination.yaml`
and is covered by `test/functional/scenario81_local_destination_test.go`,
`test/integration/scenario81_local_destination_integration_test.go`,
`test/e2e/scenario81_local_destination_e2e_test.go`, and the live verification script
`test/e2e/scripts/scenario81-local-destination.sh`.

### Scenario 82 — Security and Encryption

**Scenario 82** verifies the backup/restore security posture end-to-end: S3 credentials
never land on disk or in a ConfigMap, the S3 plugin config is resolved only inside the
ephemeral Job pod, the backup Jobs run as a dedicated minimal-RBAC ServiceAccount that
cannot read unrelated Secrets, the `encryption` plugin option flips with the CR field, and
`imagePullSecrets` propagate to the Job pod spec. The mechanics are detailed under
[Security and Encryption](#security-and-encryption); this section captures the acceptance
contract per sub-case:

- **82a — Credentials never on disk/ConfigMap.** The generated
  `<cluster>-backup-s3-config` ConfigMap's `s3-plugin-config.yaml.tpl` contains **only**
  `${...}` placeholders (`${S3_REGION}`, `${S3_ENDPOINT}`, `${AWS_ACCESS_KEY_ID}`,
  `${AWS_SECRET_ACCESS_KEY}`, `${S3_BUCKET}`, `${S3_FOLDER}`, `${S3_ENCRYPTION}`, and the
  multipart placeholders) and **no** real secret material. Credentials reach the running
  pod **only** as env via `valueFrom.secretKeyRef` (`AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` from the credential Secret) — no env entry carries a literal
  `value:` for these.

- **82b — Ephemeral `envsubst` resolution.** The rendered tool script runs
  `envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml` at container
  runtime (the ConfigMap is mounted read-only at `/etc/gpbackup`). The **resolved**
  `/tmp/s3-config.yaml` (with the real credentials substituted in) exists **only** in the
  ephemeral pod filesystem — it is never written to a ConfigMap, a Secret, or the host, and
  the ConfigMap template still shows `${...}` placeholders (82a holds).

- **82c — Dedicated ServiceAccount, minimal RBAC.** The backup/restore/validate/cleanup
  Jobs run as `cloudberry-backup-sa`. With `backup.rbac.scopeSecrets: true` and a
  `backup.rbac.secretNames` allow-list, the Role's `secrets: [get]` is scoped to those
  `resourceNames`, so the SA **can** get the backup-relevant Secrets (the credential
  Secret + the per-cluster `<cluster>-admin-password`, `<cluster>-ssh-keys`,
  `<cluster>-backup-s3-vault-creds`) but a `get` on an **unrelated** Secret is **denied**.
  Backups still succeed because the allow-list covers every Secret the Jobs consume. The
  default (`scopeSecrets: false`) keeps namespace-wide `get` for backward compatibility.

- **82d — Encryption on/off flips the plugin option.** `S3Destination.encryption` is an
  enum (`on|off`, default `on`). `buildS3Env` sets `S3_ENCRYPTION` from it and the ConfigMap
  template's `encryption: ${S3_ENCRYPTION}` line flips accordingly, so the rendered
  `/tmp/s3-config.yaml` contains `encryption: on` (the S3 plugin's TLS/SSL option) or
  `encryption: off`. The CRD enum rejects any other value. (In the HTTP-only test
  environment this is asserted via the **plugin option**, not literal HTTPS.)

- **82e — `imagePullSecrets` propagate.** `spec.backup.jobTemplate.imagePullSecrets` are
  applied to the backup Job pod spec (and to restore / post-restore-validation / cleanup /
  CronJob pods — they all flow through `applyJobTemplatePod`), so the Jobs can pull the
  backup image from a private registry.

**Live-run notes.** The sample CR uses a **single S3 (MinIO) destination**
(`scenario82-s3`, HA coordinator+standby, segment mirroring, Vault-PKI TLS) with
`encryption: "on"`, a `jobTemplate.imagePullSecrets` entry, the `s3-credentials` credential
Secret, and an `unrelated-secret` used by the 82c deny test. The operator chart is installed
with `backup.rbac.scopeSecrets: true` and a `secretNames` allow-list covering
`scenario82-s3-admin-password`, `scenario82-s3-ssh-keys`,
`scenario82-s3-backup-s3-vault-creds`, and `s3-credentials`. The 82c deny is verified with
`kubectl auth can-i` and an in-pod SA-token probe; 82d is flipped live with
`kubectl patch …{"encryption":"off"}`; 82e is read from
`.spec.template.spec.imagePullSecrets[*].name`. As with the other scenarios, real
`gpbackup`/`gprestore` runs via the coordinator-exec path while the Job-spec/ConfigMap
assertions prove the operator behavior.

The scenario is driven by the sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario82-security-encryption.yaml`
and is covered by `test/functional/scenario82_security_encryption_test.go`,
`test/integration/scenario82_security_encryption_integration_test.go`,
`test/e2e/scenario82_security_encryption_e2e_test.go`, and the live verification script
`test/e2e/scripts/scenario82-security-encryption.sh`.

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

After **each successful backup**, the operator creates a short-lived **cleanup Job** that
runs **`gpbackman`** (the [`woblerr/gpbackman`](https://github.com/woblerr/gpbackman)
backup-management CLI, provided by the backup image — see
[gpbackman in the backup image](#gpbackman-in-the-backup-image)) to enforce **all three**
retention policies: `retention.fullCount`, `retention.incrementalCount`, and
`retention.maxAge`.

> **Real gpbackman CLI (no `delete --keep-full`).** Earlier drafts of this spec referenced
> an invalid `gpbackman delete --older-than … --keep-full N` invocation. `gpbackman` has
> **no** `delete` subcommand and **no** count flags (`--keep-full` / `--older-than`). The
> real commands the cleanup Job uses are:
>
> | Command | Purpose |
> |---|---|
> | `gpbackman backup-info --type full\|incremental --history-db <db>` | List backups (TIMESTAMP / TYPE / STATUS), newest first |
> | `gpbackman backup-delete --timestamp <ts> --cascade --plugin-config <cfg> --history-db <db>` | Delete a specific backup (and, with `--cascade`, its dependent incrementals) |
> | `gpbackman backup-clean --older-than-days <N> --cascade --plugin-config <cfg> --history-db <db>` | Time-based retention — delete every backup older than `<N>` whole days |
>
> `gpbackman` has **no native count-based retention**, so the operator implements `fullCount`
> / `incrementalCount` by enumerating with `backup-info` and deleting the oldest excess with
> `backup-delete`.

#### Cleanup script (`buildGpbackmanRetentionScript`)

`BuildRetentionCleanupJob(cluster, timestamp)`
(`internal/builder/backup_builder.go`) renders a self-contained POSIX-sh /
bash-3.2-safe script (no associative arrays; every interpolated value is
single-quoted, so there is no injection surface) into the cleanup container's
`args[0]`. The script renders the S3 plugin config, points `gpbackman` at the
coordinator history DB (`--history-db <COORDINATOR_DATA_DIRECTORY>/gpbackup_history.db`),
and enforces each configured policy:

- **`fullCount` (count-based, full).** `gpbackman backup-info --type full` lists the
  **Success** full timestamps newest-first. The script deletes the **oldest excess**
  beyond `fullCount` with `gpbackman backup-delete --timestamp <oldest> --cascade`,
  **re-enumerating** the current Success timestamps before each delete so a cascaded
  full-delete (which takes its dependent incrementals with it) neither over- nor
  under-counts; it stops once the retained set is within the limit.

- **`incrementalCount` (count-based, incremental).** The same loop with
  `backup-info --type incremental` / `backup-delete --timestamp <oldest> --cascade`.
  **Cascade caveat:** deleting an incremental with `--cascade` also removes the
  incrementals that depend on it, so the script re-enumerates the **current** Success
  incrementals after every delete and stops as soon as the retained count is `≤
  incrementalCount`. The reported deletion count reflects the **actual** number of
  backups removed (including any that disappeared via cascade), so the metric stays
  accurate.

- **`maxAge` (time-based).** `parseMaxAgeDays` converts the `maxAge` expression into a
  whole number of days (`"30d"` → `30`, `"4w"` → `28`, the Go duration `"720h"` → `30`,
  `"25h"` → `1`, a bare `"30"` → `30`; a positive sub-day duration rounds up to `1`,
  empty/unparseable skips the step). The script then runs
  `gpbackman backup-clean --older-than-days <N> --cascade`, counting the deletions by
  comparing the total Success backups before and after.

The script maintains a `DELETED` counter, prints the marker `RETENTION_DELETED=<n>` to
stdout, and writes `<n>` to the container's `terminationMessagePath`
(`/dev/termination-log`); the container's `terminationMessagePolicy` is
`FallbackToLogsOnError` so the count is recoverable from the pod log if the file write is
missed. The script always exits `0`.

#### Cleanup Job placement and idempotency

`ensureRetentionCleanup(ctx, cluster)` (`internal/controller/admin_controller.go`) is
called from `reconcileBackup` after the backup status is refreshed. It is a **no-op** when
backup is disabled or **no** retention policy is set (`fullCount`, `incrementalCount`, and
`maxAge` all empty/zero). Otherwise it finds the **newest Succeeded** backup-operation Job
(label `avsoft.io/backup-operation=backup`, `Succeeded > 0`), derives its 14-digit
timestamp, and creates **one** cleanup Job named `<cluster>-cleanup-<latest-backup-ts>`
(`util.RetentionCleanupJobName`). Because the name is keyed off the latest **successful
backup** timestamp, a **Get-before-Create** makes creation idempotent — cleanup runs
**exactly once per successful backup**, and a rerun never produces a duplicate Job. The
cleanup Job carries the label `avsoft.io/backup-operation=cleanup`
(`util.BackupOperationCleanup`) and is owner-referenced to the cluster.

#### Deletion count → metric flow

The cleanup Job pod cannot self-annotate without extra RBAC, so the deletion count reaches
the metric via the operator:

1. The cleanup script emits `RETENTION_DELETED=<n>` to stdout **and** to
   `/dev/termination-log`.
2. After the cleanup Job reaches **Succeeded**,
   `reconcileRetentionCleanupAnnotations` reads `<n>` from the terminated cleanup pod's
   container message (parsing the `RETENTION_DELETED=` marker) and patches the Job
   annotation **`avsoft.io/backup-retention-deleted=<n>`**
   (`util.AnnotationBackupRetentionDeleted`) — **once** (idempotent: it skips Jobs that
   already carry the annotation).
3. The existing metrics loop (`recordBackupJobMetrics`) reads the annotation from each
   **Succeeded** cleanup Job and calls `metrics.RecordBackupRetentionDeleted(cluster,
   namespace, n)`, which increments the counter
   **`cloudberry_backup_retention_deleted_total{cluster,namespace}`** by `<n>`.

Each successive backup yields a distinct cleanup Job (one per latest successful timestamp),
so the counter accrues a separate delta per backup. Assert the metric as a **monotonic
delta**, never an absolute value.

#### gpbackman in the backup image

`gpbackman` is built from `woblerr/gpbackman` (pinned `GPBACKMAN_VERSION`, default
`v0.8.1`) into `/usr/local/bin/gpbackman` in `Dockerfile.cloudberry-backup` (it links
`mattn/go-sqlite3`, so it is a CGO build). The binary is present, executable, and on
`PATH` in `cloudberry-backup:2.1.0` alongside `gpbackup`/`gprestore`/`gpbackup_helper`/
`gpbackup_s3_plugin`; `gpbackman --version` is smoke-tested at build time. The cleanup Job
uses this image, so its `gpbackman backup-info` / `backup-delete` / `backup-clean`
invocations resolve at runtime.

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

`gpbackup` supports incremental backups that only capture changes to append-optimized
(AO) tables since the last full or incremental backup. The operator manages incremental
backup sets as follows:

1. When `gpbackup.incremental: true` in the spec (or an on-demand request sets
   `type=incremental`), the operator **always** renders
   `gpbackup --incremental --leaf-partition-data`.
2. `gpbackup` automatically locates the most recent compatible backup to form or extend a
   backup set.
3. The `--from-timestamp` flag can be supplied via the API/CLI to explicitly pin the base
   backup.
4. Restore of an incremental backup requires the full set (full + all incrementals).
   `gprestore` validates completeness before proceeding and **refuses** when an
   intermediate incremental is missing.
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

### Forced `--incremental --leaf-partition-data` pairing (78a)

`gpbackup` **requires** `--leaf-partition-data` for incremental backups. The operator
therefore treats the two flags as an inseparable pair: whenever an incremental is
effective the rendered backup Job/CronJob args **always** include
`--incremental --leaf-partition-data`, emitted **exactly once** each — even when
`leafPartitionData` is left unset, and **de-duplicated** when `leafPartitionData: true`
is also given explicitly. Full backups are unaffected (the branch runs for incrementals
only). The pairing is applied by `appendIncrementalArgs` in
`internal/builder/backup_builder.go`, which both `BuildBackupJob` and
`BuildBackupCronJob` route through, so on-demand Jobs and scheduled CronJobs render
identical incremental args.

An incremental is "effective" when **either** the cluster spec sets
`gpbackup.incremental: true`, **or** the per-request options set `type=incremental`
(API `POST …/backups` with `type: incremental`), **or** the per-request
`gpbackupOptions.incremental` is true — resolved by the `effectiveBackupType` helper.

### Auto-base discovery vs. pinned base (78b / 78c)

- **Auto-base (no `--from-timestamp`, 78b).** Running an incremental **without**
  `--from-timestamp` lets `gpbackup` auto-form the incremental against the **most recent
  compatible backup** already present on the destination. This is why every full and
  incremental in a set **must share the same bucket + folder** — gpbackup's auto-base
  discovery reads the backup history on the destination. After a full backup, modifying
  an AO table and running an incremental with no pin produces an incremental keyed to that
  full; `status.backup.lastBackupType` becomes `incremental`.
- **Pinned base (`--from-timestamp <full-ts>`, 78c).** Supplying an explicit
  `--from-timestamp` (via `gpbackupOptions.fromTimestamp` on the API, or
  `BackupJobOptions.FromTimestamp` in the builder, surfaced as `--from-timestamp <ts>`)
  **pins** the incremental to that base, even when a more-recent compatible backup exists.

### The `avsoft.io/backup-type` label and status derivation (78b)

Each backup Job and CronJob is labelled with **`avsoft.io/backup-type`**
(`util.LabelBackupType`), valued `full` or `incremental` from the *effective* type:

- `BuildBackupJob` sets the label from `effectiveBackupType(cluster, opts)` — so a
  per-request `type=incremental` Job is labelled `incremental` **even when the cluster
  spec is full**.
- `BuildBackupCronJob` sets the label from `effectiveBackupType(cluster, nil)` (spec
  only), and stamps it on the CronJob, its `jobTemplate`, and the pod template, so
  CronJob-spawned Jobs inherit it.

The operator then **derives status from the Job's label, not the spec**:
`applyBackupJobToStatus` calls `backupTypeFromJob(job, cluster)`
(`internal/controller/admin_controller.go`), which reads the Job's
`avsoft.io/backup-type` label and **falls back to the spec**
(`backupTypeFromLabels(cluster)`) only when the label is absent. Both
`status.backup.lastBackupType` **and** the appended `backupHistory[].type` are sourced
this way, so a per-Job incremental reports `lastBackupType=incremental` even against a
`full` spec, and CronJob-spawned Jobs report the right type robustly.

### Incremental restore completeness + RestoreFailed event (78d)

Restoring from the **latest** incremental requires the **entire chain** —
`full → inc1 → … → incN` — to be present. `gprestore` validates the set's completeness
natively: with the full chain present it restores; if an **intermediate** incremental is
missing it reports the incremental set is incomplete and **refuses** (the restore Job
exits non-zero / `status.Failed > 0`). The operator makes **no** change to gprestore's
native completeness algorithm.

On a failed **restore-operation** Job the operator now:

1. Records `status.backup.lastBackupStatus = Failed` and a `backupHistory[]` entry with
   `status=Failed`, and records the failed restore metric (`RecordRestore(…, failed)`).
2. Emits a Kubernetes **Warning** Event with reason **`RestoreFailed`**
   (`api/v1alpha1.EventReasonRestoreFailed = "RestoreFailed"`) via
   `emitRestoreFailureEvent` — **de-duplicated per failed Job** (fires only on a real
   transition into `Failed` for that Job name, so periodic reconciles of an unchanged
   failed restore Job produce **exactly one** Warning).

`RestoreFailed` is intentionally **distinct** from the backup-only `BackupFailed` reason:
restore-operation failures emit `RestoreFailed` and **never** `BackupFailed`, so
Scenario 77's backup-only semantics (its test asserts zero `BackupFailed` for restore)
stay intact while the 78d refuse-on-incomplete case is surfaced.

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

### Scenario 78 — Incremental Backup Lifecycle

**Scenario 78** verifies the full incremental backup lifecycle — flag wiring,
auto-base discovery, a pinned base, status derivation from the Job's
`avsoft.io/backup-type` label, and incremental-restore completeness (including the
refuse-on-missing-intermediate path and the new `RestoreFailed` Warning). The
mechanics are described in detail under
[Incremental Backup Support](#incremental-backup-support); this section captures the
acceptance contract per sub-case:

- **78a — Incremental flag wiring.** With `gpbackup.incremental: true` (or a per-request
  `type=incremental`), the rendered backup **Job** *and* **CronJob** args contain
  `--incremental --leaf-partition-data` **exactly once each** — leaf-partition-data is
  **forced** (it is required by gpbackup for incrementals) even when `leafPartitionData`
  is unset, and **de-duplicated** when `leafPartitionData: true` is also set.

- **78b — Auto-locate base.** A FULL backup → modify an AO table → an INCREMENTAL backup
  **without** `--from-timestamp` auto-forms against the most recent compatible backup on
  the **same bucket+folder**; `status.backup.lastBackupType` becomes `incremental` and
  `backupHistory[0].type` is `incremental`, **derived from the Job's
  `avsoft.io/backup-type` label** (so a spec-`full` cluster running a per-Job incremental
  still reports `incremental`).

- **78c — Pinned base.** An INCREMENTAL with explicit
  `--from-timestamp <full-ts>` (API `gpbackupOptions.fromTimestamp` /
  `BackupJobOptions.FromTimestamp`) is pinned to that base even when a more-recent
  compatible backup exists; status/history type are `incremental`.

- **78d — Restore completeness.** Restoring from the **latest** incremental validates the
  full set (full + all incrementals) and restores when complete; deleting an
  **intermediate** incremental and retrying makes `gprestore` report the set incomplete
  and **refuse** (the restore Job fails). The operator records `lastBackupStatus=Failed`
  for the restore and emits a de-duplicated `Warning`/`RestoreFailed` Event — and
  **never** a `BackupFailed` Event for a restore (Scenario 77 stays backup-only).

**Live-run notes.** The sample CR uses a **single S3 (MinIO) destination** with
`incremental: true`, the **same `folder: /backups`** for the full and every incremental
(required for 78b auto-base discovery), `retention.incrementalCount`, and a data load
that creates **≥1 append-optimized table** (e.g.
`public.events_ao WITH (appendonly=true, orientation=row)`) — incrementals are only
meaningful for AO tables, and 78b/78c modify that table to yield real deltas. As with the
other scenarios, real `gpbackup`/`gprestore` runs via the coordinator-exec path; the
Job/CronJob incremental **args** are verified from the rendered spec.

The scenario is driven by the sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario78-incremental-backup.yaml`
and is covered by `test/functional/scenario78_incremental_backup_test.go`,
`test/integration/scenario78_incremental_backup_integration_test.go`,
`test/e2e/scenario78_incremental_backup_e2e_test.go`,
`internal/builder/backup_builder_scenario78_test.go`,
`internal/controller/backup_event_scenario78_test.go`, and the live data cycle
`test/e2e/scripts/scenario78-incremental-backup.sh`.

### Scenario 79 — Retention Cleanup, All Policies

**Scenario 79** verifies the gpbackman-driven retention cleanup lifecycle described under
[Retention Cleanup Job](#retention-cleanup-job): with **all three** retention policies set
(`fullCount: 3`, `incrementalCount: 10`, `maxAge: "30d"`), the operator creates a single
idempotent cleanup Job after each successful backup that enforces every policy via the real
`gpbackman` CLI and feeds its deletion count into
`cloudberry_backup_retention_deleted_total`. The mechanics are described in detail above;
this section captures the acceptance contract per sub-case:

- **79a — `fullCount` retention.** With `fullCount=3` and **4** full backups present, the
  cleanup script enumerates `gpbackman backup-info --type full` (Success, newest-first) and
  deletes the **oldest** full with
  `gpbackman backup-delete --timestamp <oldest> --cascade` (excess `= 4 − 3 = 1`), retaining
  the newest **3**. No `--type incremental` delete and no `backup-clean` step run when those
  policies are not binding. `RETENTION_DELETED=1` → metric `+= 1`.

- **79b — `incrementalCount` retention.** With `incrementalCount=10`, a full + **11**
  incrementals, the script enumerates `gpbackman backup-info --type incremental` and deletes
  the **oldest excess** (`11 − 10 = 1`) with `backup-delete --timestamp <oldest> --cascade`,
  **re-enumerating** the current Success incrementals after each delete so a cascaded delete
  neither over- nor under-counts; the loop stops once `≤ 10` incrementals remain. The full
  (`≤ fullCount`) is untouched. The reported count reflects the **actual** backups removed
  (including any taken by cascade); metric `+=` that count.

- **79c — `maxAge` retention.** With `maxAge="30d"` and a backup whose `gpbackup_history.db`
  timestamp is older than 30 days (the live test ages a history row in SQLite), the script
  runs `gpbackman backup-clean --older-than-days 30 --cascade` (`parseMaxAgeDays("30d") =
  30`), removing the aged backup (cascade removes its dependents) while a fresh backup is
  retained. `RETENTION_DELETED` counts the difference; metric `+= 1`.

- **79d — Cleanup placement + metric.** After the newest **Succeeded** backup (timestamp
  `T`), `reconcileBackup → ensureRetentionCleanup` creates exactly one cleanup Job
  `<cluster>-cleanup-T`; a rerun creates **no** duplicate (Get-before-Create idempotent).
  When the cleanup Job Succeeds, the operator reads `RETENTION_DELETED=<n>` from the
  terminated pod and patches `avsoft.io/backup-retention-deleted=<n>` **once**; the metrics
  loop then increments `cloudberry_backup_retention_deleted_total` by `<n>`. Retention
  all-zero/empty ⇒ **no** cleanup Job; no Succeeded backup ⇒ **no** cleanup Job.

**Live-run notes.** The sample CR uses a **single S3 (MinIO) destination** with the
**same `folder: /backups`** for fulls and every incremental (so `gpbackman` enumerates one
backup history on the destination), `retention.{fullCount: 3, incrementalCount: 10, maxAge:
"30d"}`, `gpbackup.incremental: true` (for the 79b chain), and **no** `schedule`
(on-demand). As with the other scenarios, the standalone cleanup Job pod is not the
coordinator; live deletions are exercised via the coordinator-exec path against the real
`gpbackup_history.db`, while the cleanup Job's rendered **script/args** are asserted from
the spec. The metric is asserted as a **monotonic delta** in VictoriaMetrics.

The scenario is driven by the sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario79-retention.yaml` and is covered by
`test/functional/scenario79_retention_test.go`,
`test/integration/scenario79_retention_integration_test.go`,
`test/e2e/scenario79_retention_e2e_test.go`, and the live verification script
`test/e2e/scripts/scenario79-retention.sh`.

## Post-Restore Validation

After a restore Job reaches **Succeeded**, the operator creates a single idempotent
**validation Job** (`util.PostRestoreValidationJobName(cluster, ts)`, label
`avsoft.io/backup-operation=validate`, owner-ref'd to the cluster) that runs the rendered
`postRestoreValidationScript` in the backup image. The script executes four checks in order
— optional `ANALYZE`, row-count compare, invalid-index scan, health-check query — and exits
non-zero (the Job **Fails**) when a **must-pass** check fails:

1. **Row-count compare vs gpbackup history (headline).** The operator captures the EXPECTED
   per-table row counts from the **gpbackup history metadata** of the restored timestamp and
   passes them to the validation Job via the restore Job's
   **`avsoft.io/expected-row-counts`** annotation (a JSON object of fully-qualified
   `schema.table` → count). For each expected table the script runs
   `psql -tA -c "SELECT count(*) FROM <table>"`, compares the ACTUAL restored count against
   the expected one, and emits a parsable marker — `ROW_COUNT_MATCH table=<t> count=<a>` on
   a match, or `ROW_COUNT_MISMATCH table=<t> expected=<e> actual=<a>` (to stderr) on a
   discrepancy. **Any** mismatch makes the Job exit 1 (a **must-pass** check). This also
   catches the data-only-into-prepopulated case where `actual > expected`. When the expected
   map is **empty** (no history counts available), the script falls back to a best-effort
   total-table probe that emits `ROW_COUNT_PROBE_SKIPPED` and **never fails**.

2. **Run-analyze (planner-stats refresh).** When run-analyze is enabled the script runs a
   database-wide `ANALYZE` to refresh planner statistics **before** the row-count compare and
   emits `ANALYZE_OK`. Because the user explicitly asked for fresh stats, a failing `ANALYZE`
   aborts the Job (`set -euo pipefail`). Run-analyze is driven from
   `backup.validation.runAnalyze` and falls back to the restore's `gprestore.runAnalyze`
   intent when the validation flag is unset.

3. **Invalid-index scan (must-pass).** The script scans
   `SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON c.oid = i.indexrelid WHERE c.relkind='i' AND NOT i.indisvalid`;
   any invalid index makes the Job exit 1.

4. **Health-check query.** A configurable query (`backup.validation.healthCheckQuery`,
   default `SELECT 1`) confirms application connectivity. It runs last and is best-effort
   (it never fails validation on its own).

### Validation config (`backup.validation`)

Validation is configured by the optional `BackupSpec.Validation` block
(`api/v1alpha1.BackupValidation`). When the block is unset the operator uses defaults
(validation **enabled**, `SELECT 1` health-check, run-analyze inherited from
`gprestore.runAnalyze`). Expected per-table row counts are computed by the **operator** (from
gpbackup history metadata) and are NOT user-facing.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `validation.enabled` | `*bool` | `true` (when unset) | Whether post-restore validation Jobs are created. Set `false` to disable. |
| `validation.healthCheckQuery` | `string` | `SELECT 1` | Connectivity health-check query the validation Job runs. |
| `validation.runAnalyze` | `bool` | `false` (falls back to `gprestore.runAnalyze`) | When `true`, the validation Job runs a database-wide `ANALYZE` to refresh planner stats. |

```yaml
spec:
  backup:
    validation:
      enabled: true
      healthCheckQuery: "SELECT 1"
      runAnalyze: true
```

### Validation result surfacing

The operator observes the validation Job's **terminal status** (`observeValidationJobs`) and
records the outcome **exactly once** (gated on the `avsoft.io/validation-recorded`
annotation, so the metric and Event do not storm on periodic reconciles):

- **Metric** — `cloudberry_restore_validation_total{cluster,namespace,result}` (CounterVec,
  `result ∈ {success, failed}`) is incremented from the validation Job's terminal status —
  `success` when the Job Succeeds, `failed` when it Fails.
- **Event** — on a **Failed** validation Job the operator emits a de-duplicated
  `Warning`/`ValidationFailed` Event (`api/v1alpha1.EventReasonValidationFailed =
  "ValidationFailed"`), mirroring the Scenario 77/78 transition de-dup.
- **Restore is not failed by validation.** Validation runs **post-restore**; a Failed
  validation surfaces a Warning + the `failed` metric but does **not** retroactively alter
  the restore status — the restore Job remains Succeeded.

### Scenario 80 — Post-Restore Validation

**Scenario 80** verifies the post-restore validation lifecycle described above — the
operator creates a validation Job for each Succeeded restore, the four checks run, and the
terminal outcome drives the `cloudberry_restore_validation_total{result}` metric and the
`ValidationFailed` Warning. The mechanics are described in detail above; this section
captures the acceptance contract per sub-case:

- **80a — Row-count match vs gpbackup history.** With the EXPECTED per-table counts captured
  from gpbackup history (passed via `avsoft.io/expected-row-counts`) and a restore into a
  **fresh** DB, every table's actual count equals its expected count → the script emits
  `ROW_COUNT_MATCH` per table, no `ROW_COUNT_MISMATCH`, the Job **Succeeds**, and
  `cloudberry_restore_validation_total{result="success"}` increments. No Warning Event.

- **80b — Run-analyze refreshes planner stats.** With `validation.runAnalyze: true` (or the
  restore's `gprestore.runAnalyze`), the script runs `ANALYZE` before the row-count compare
  and emits `ANALYZE_OK`; planner stats are refreshed and contribute to the overall pass.

- **80c — Invalid-index scan (must-pass).** A restored DB with all indexes valid passes; a
  DB with ≥1 invalid index (`relkind='i' AND NOT indisvalid`) makes the Job exit 1 → Failed
  → `{result="failed"}` + `ValidationFailed` Warning.

- **80d — Configurable health-check query.** The default `SELECT 1` (or a custom
  `validation.healthCheckQuery`) confirms connectivity; the Job Succeeds and increments
  `{result="success"}`.

- **80e — Deliberate mismatch FLAGGED (headline).** A target table is **pre-populated with
  extra rows** and the restore is performed **data-only** into it, so `actual > expected`.
  The script emits `ROW_COUNT_MISMATCH table=<t> expected=<e> actual=<a>` and exits 1 → the
  validation Job **Fails**, `cloudberry_restore_validation_total{result="failed"}`
  increments, and a `Warning`/`ValidationFailed` Event references the failed Job. The
  **restore** Job remains Succeeded (validation does not retroactively fail the restore).

**Live-run notes.** The validation Job pod is not the coordinator; its `psql` reaches the
coordinator via the Job's `buildBackupEnv`/`PGHOST=<cluster>-coordinator` and the
`PGDATABASE` set from the restored database. Expected counts are anchored on the source-DB
counts captured at backup time (deterministic) and cross-checked against the gpbackup
report/toc. As with the other scenarios, real `gpbackup`/`gprestore` runs via the
coordinator-exec path while the validation Job's rendered **script/args** are asserted from
the spec.

The scenario is driven by the sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario80-post-restore-validation.yaml`
and is covered by `test/functional/scenario80_post_restore_validation_test.go`,
`test/integration/scenario80_post_restore_validation_integration_test.go`,
`test/e2e/scenario80_post_restore_validation_e2e_test.go`,
`internal/builder/backup_builder_scenario80_test.go`,
`internal/controller/validation_scenario80_test.go`, and the live verification script
`test/e2e/scripts/scenario80-post-restore-validation.sh`.

## Backup Failure Handling

Backup (and restore/validate/cleanup) Jobs are governed by two Kubernetes Job-level
guards that bound how a failing backup retries and how long it may run before it is
forcibly killed. Both are seeded with defaults by `buildJobSpec`
(`internal/builder/backup_builder.go`) and are overridable per cluster via
`spec.backup.jobTemplate`:

| Field | `spec.backup.jobTemplate` override | Default | Maps to | Effect |
|-------|-----------------------------------|---------|---------|--------|
| `backoffLimit` | `jobTemplate.backoffLimit` (`*int32`) | `2` | `JobSpec.BackoffLimit` | Number of pod retries before the Job is marked terminal `Failed`. With the default `2`, a persistently failing backup makes **up to 3** pod attempts before Kubernetes sets the Job condition `type=Failed reason=BackoffLimitExceeded`. |
| `activeDeadlineSeconds` | `jobTemplate.activeDeadlineSeconds` (`*int64`) | `7200` (2 h) | `JobSpec.ActiveDeadlineSeconds` | Wall-clock deadline for the whole Job. When the deadline is reached Kubernetes **kills** the Job and sets the condition `type=Failed reason=DeadlineExceeded`. |

`buildJobSpec` seeds `backoff=defaultBackoffLimit` (`2`) and
`deadline=defaultActiveDeadlineSeconds` (`7200`) and, when a `jobTemplate` is present,
overrides each from `tmpl.BackoffLimit` / `tmpl.ActiveDeadlineSeconds` (a nil field keeps
the default). Because every backup/restore/cleanup/validation Job and the scheduled
CronJob's `jobTemplate` route through `buildJobSpec`, the override reaches **every** Job
spec uniformly.

### Failure-detection contract

The operator classifies a Job as **Failed** when **either**:

- `job.Status.Failed > 0` (the failed-**pod** count), **or**
- the Job carries a terminal **Failed condition** — a `batchv1.JobFailed` condition with
  `status == corev1.ConditionTrue` (e.g. `reason=DeadlineExceeded` or
  `reason=BackoffLimitExceeded`), detected by the helper
  `jobHasFailedCondition(job)` (`internal/controller/admin_controller.go`).

This combined check is applied in `backupJobStatus` (→ `"Failed"`),
`backupJobStatusCode` (→ `3`), and `validationJobResult`, **after** the Succeeded
precedence branch (a Job with `Succeeded > 0` still reads `Success`). The condition-based
arm is what makes a **deadline-killed** Job (or a **backoffLimit-exhausted** Job) reliably
recorded as `Failed` even when the failed-pod count is `0` — on some clusters a
deadline-killed Job presents with `Status.Failed == 0` and only the `DeadlineExceeded`
condition set, which the prior pod-count-only mapping would have mis-read as
`InProgress`.

```go
// classification precedence (backupJobStatus / backupJobStatusCode)
switch {
case job.Status.Succeeded > 0:                            // success wins
    return backupStatusSuccess // "Success" / 2
case job.Status.Failed > 0 || jobHasFailedCondition(job): // Status.Failed>0 OR Failed condition
    return backupStatusFailed  // "Failed"  / 3
// ... running / pending unchanged
}
```

### Resulting status, metric, and event

When the latest observed backup-operation Job is classified `Failed`,
`applyBackupJobToStatus` → `recordLatestBackupMetrics` / `emitBackupFailureEvent`:

1. Sets **`status.backup.lastBackupStatus = Failed`** and appends a `backupHistory[]`
   entry with `status=Failed`.
2. Records **`cloudberry_backup_last_status{cluster,namespace} = 1`** (the gauge's
   `0=success, 1=failed, 2=in-progress`); a subsequent successful backup sets it back to
   `0`.
3. Emits the de-duplicated **`Warning`/`BackupFailed`** Event (Scenario 77 — one per
   failed Job, fired only on a real transition into `Failed`; restore-operation failures
   emit `RestoreFailed` instead, see Scenario 78).

### Deadline behavior

A backup Job whose work outlives its `activeDeadlineSeconds` is **killed by Kubernetes at
the deadline** (not by the operator) and marked with the Job condition
`type=Failed reason=DeadlineExceeded`. Via the failure-detection contract above it is
recorded as `Failed` — `lastBackupStatus=Failed`, `cloudberry_backup_last_status=1`, and a
`BackupFailed` Warning — exactly like a backoffLimit-exhausted Job. The production default
deadline is the safe `7200` (2 h); a deliberately **low** `activeDeadlineSeconds` (e.g.
`5`) on a long-running command is the deterministic way to exercise the deadline-kill path.

### Scenario 83 — Backup Failure Handling

**Scenario 83** verifies that a backup which **cannot succeed** is bounded, retried, and
recorded as `Failed` end-to-end — both the force-failure (`backoffLimit`) path and the
`activeDeadlineSeconds` deadline-kill path — and that the failure surfaces as
`status.backup.lastBackupStatus=Failed`, `cloudberry_backup_last_status=1`, and the
de-duplicated `BackupFailed` Warning. The mechanics are described in detail above; this
section captures the acceptance contract per sub-case:

- **83a — Force-failure / backoffLimit exhaustion.** A backup Job pointed at an
  **unreachable** S3 endpoint (or with **bad creds**) fails fast at the
  `pre-backup-check` S3-reachability HEAD (Scenario 77c) and **retries up to
  `backoffLimit` (2)** — up to **3** pod attempts (`Status.Failed` grows toward
  `backoffLimit+1`) — before reaching the terminal Job condition
  `type=Failed reason=BackoffLimitExceeded`. The operator records
  `lastBackupStatus=Failed`, sets `cloudberry_backup_last_status=1`, and emits **one**
  `Warning`/`BackupFailed` Event.

- **83b — `activeDeadlineSeconds` deadline kill.** A backup Job with a **low**
  `activeDeadlineSeconds` (e.g. `5`) running a long command is **killed by Kubernetes at
  the deadline** with the Job condition `type=Failed reason=DeadlineExceeded` (its
  failed-pod count may be `0` — the `jobHasFailedCondition` arm classifies it `Failed`
  anyway). The operator records `lastBackupStatus=Failed` and
  `cloudberry_backup_last_status=1`.

- **83c — Builder defaults + `jobTemplate` override.** With **no** `jobTemplate`, the
  built backup Job carries `*BackoffLimit == 2` and `*ActiveDeadlineSeconds == 7200`
  (and the CronJob's `jobTemplate` carries `*BackoffLimit == 2`); with
  `jobTemplate.{backoffLimit: 2, activeDeadlineSeconds: 5}` the override reaches the
  materialized Job/CronJob spec. Restore / post-restore-validation / cleanup Jobs route
  through the same `buildJobSpec`, so the override propagates uniformly; a `jobTemplate`
  with a nil field retains the default.

- **83d — Status detection.** `backupJobStatus`/`backupJobStatusCode` return `Failed`/`3`
  for `Status.Failed > 0` **and** for a `JobFailed` condition (`DeadlineExceeded` or
  `BackoffLimitExceeded`) even when `Status.Failed == 0`; `Succeeded` precedence is
  preserved; in-progress/pending are unchanged when no failure signal is present;
  `recordLatestBackupMetrics` maps `Failed → cloudberry_backup_last_status=1`,
  `Success → 0`, otherwise `2`.

**Live-run notes.** The sample CR uses a **single S3 (MinIO) destination**
(`scenario83-s3`, HA coordinator+standby, segment mirroring, Vault-PKI TLS) with an
explicit `jobTemplate.backoffLimit: 2` and the **production-safe** default
`activeDeadlineSeconds` (`7200`). The force-failure (83a) live path points a backup Job at
a **bad/unreachable** S3 endpoint so the pre-backup S3 HEAD fails fast and `backoffLimit`
drives the retries; the deadline (83b) live path materializes a **per-run** operator-shaped
backup Job with `activeDeadlineSeconds: 5` + a long `sleep` (so the **production cluster's**
deadline is never lowered). A real healthy backup is run **last** so the steady-state
`cloudberry_backup_last_status` converges to `0` ("all backups successful") while the
discrete failure/deadline Jobs remain the intended Scenario 83 failures. As with the other
scenarios, real `gpbackup` runs via the coordinator-exec path while the Job-spec/condition
assertions prove the operator behavior.

The scenario is driven by the sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario83-backup-failure.yaml`
and is covered by `test/functional/scenario83_backup_failure_test.go`,
`test/integration/scenario83_backup_failure_integration_test.go`,
`test/e2e/scenario83_backup_failure_e2e_test.go`, and the live verification script
`test/e2e/scripts/scenario83-backup-failure.sh`.

## RBAC Requirements

The operator chart (`backup.rbac.create: true`) creates a `ServiceAccount`, `Role`, and
`RoleBinding` for backup Jobs in the release namespace
(`deploy/helm/cloudberry-operator/templates/backup-rbac.yaml`). The ServiceAccount name
`cloudberry-backup-sa` is namespace-fixed and matches `util.BackupServiceAccountName`, so
the Job builder can reference it:

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
    # resourceNames rendered ONLY when backup.rbac.scopeSecrets=true (see below)
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
```

### Scoped `secrets: get` (`scopeSecrets` / `secretNames`)

By default the `secrets: [get]` rule is **namespace-wide** (no `resourceNames`), so the SA
can `get` any Secret in the namespace. This is backward-compatible but coarse. Two Helm
values harden it to **minimal RBAC** (Scenario 82c):

| Value | Default | Effect |
|-------|---------|--------|
| `backup.rbac.scopeSecrets` | `false` | When `true` (and `secretNames` is non-empty), the `secrets: [get]` rule is scoped to a `resourceNames` allow-list, so the SA cannot `get` unrelated Secrets. |
| `backup.rbac.secretNames` | `[backup-s3-credentials]` | The explicit allow-list of Secret names the SA may `get` when `scopeSecrets: true`. |

When scoped, the allow-list **must** include **every** Secret the
backup/restore/validate/cleanup Jobs consume, or backups break. For a cluster named
`<cluster>` the Jobs read:

| Secret | Source | Used for |
|--------|--------|----------|
| your `destination.s3.credentialSecret.name` (e.g. `backup-s3-credentials`) | user-named (dynamic) | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` |
| `<cluster>-backup-s3-vault-creds` | `util.BackupS3VaultCredentialsSecretName` (deterministic) | Vault-rendered S3 credentials |
| `<cluster>-admin-password` | `util.AdminPasswordSecretName` | `PGPASSWORD` |
| `<cluster>-ssh-keys` | `util.ClusterSSHSecretName` | passwordless inter-pod SSH identity |
| `<cluster>-tls` | user-named `auth.ssl.certSecret.name` | TLS — only if a future mount needs it (the backup Job does **not** mount TLS today; include optionally) |

> **Multi-cluster note.** The SA and Role are **namespace-fixed**, but the consumed Secret
> names are **per-cluster**. With multiple clusters in one namespace, the `secretNames`
> allow-list must **union** all their per-cluster Secret names.

> **Scoping does not break env/volume injection.** Pod `secretKeyRef` env and volume mounts
> are resolved by the **kubelet** (using its own credentials), not by the backup SA's API
> token, so scoping `secrets: get` restricts only the SA's own API `get` calls — it does
> **not** affect the Job's injected credentials. **Production deployments should set
> `scopeSecrets: true`** and list the credential Secret plus the per-cluster
> `<cluster>-ssh-keys`, `<cluster>-admin-password`, `<cluster>-tls`, and
> `<cluster>-backup-s3-vault-creds` Secret names.

## Security and Encryption

The backup/restore path is designed so credentials never persist outside Kubernetes Secrets,
the SA holds only minimal RBAC, S3 traffic is encrypted, and private registries are
supported. Scenario 82 verifies all five properties.

### Credentials never on disk or in a ConfigMap (82a)

`BuildBackupS3ConfigMap` emits the `<cluster>-backup-s3-config` ConfigMap whose
`s3-plugin-config.yaml.tpl` `options:` values are **all** `${...}` placeholders
(`${S3_REGION}`, `${S3_ENDPOINT}`, `${AWS_ACCESS_KEY_ID}`, `${AWS_SECRET_ACCESS_KEY}`,
`${S3_BUCKET}`, `${S3_FOLDER}`, `${S3_ENCRYPTION}`, and the multipart placeholders) — **no**
real credential material is ever written into the ConfigMap. The actual S3 credentials are
stored only in a Kubernetes Secret and reach the running pod **only** as environment
variables via `valueFrom.secretKeyRef` (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`); no
env entry carries a literal `value:` for these.

### Ephemeral `envsubst` rendering of `/tmp/s3-config.yaml` (82b)

The ConfigMap is mounted **read-only** at `/etc/gpbackup`. At container runtime the rendered
tool script (`renderToolScript`) runs
`envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml` (with a POSIX
`eval`+heredoc fallback) so the credentials are resolved **only inside the ephemeral Job
pod**. The resolved `/tmp/s3-config.yaml` (with the real credentials substituted in) lives
solely in the pod's ephemeral filesystem — it is never written to a ConfigMap, a Secret, or
the host, and the source ConfigMap template still shows `${...}` placeholders.

### Dedicated ServiceAccount with minimal RBAC (82c)

Backup/restore/validate/cleanup Jobs run as the dedicated `cloudberry-backup-sa`
ServiceAccount with the minimal Role described under
[RBAC Requirements](#rbac-requirements). When `backup.rbac.scopeSecrets: true` and a
`backup.rbac.secretNames` allow-list are set, the SA's `secrets: [get]` is scoped to those
`resourceNames`, so a `get` on an **unrelated** Secret is **denied** while the
backup-relevant Secrets (the credential Secret + `<cluster>-ssh-keys`,
`<cluster>-admin-password`, `<cluster>-backup-s3-vault-creds`) remain readable and backups
still succeed. The default (`scopeSecrets: false`) keeps namespace-wide `get` for backward
compatibility; production deployments should enable scoping (see the
[scoped `secrets: get`](#scoped-secrets-get-scopesecrets--secretnames) table for the full
allow-list).

### TLS / encryption plugin option (82d)

`S3Destination.encryption` is an enum (`on|off`, default `on`; enforced by the CRD). The
operator's `buildS3Env` sets the `S3_ENCRYPTION` env var from it, and the ConfigMap
template's `encryption: ${S3_ENCRYPTION}` line flips accordingly, so the rendered
`/tmp/s3-config.yaml` carries `encryption: on` (the `gpbackup_s3_plugin` TLS/SSL option) or
`encryption: off`. With `encryption: "on"` all traffic to S3/MinIO uses TLS. (Against an
HTTP-only test endpoint this is verified via the **plugin option**, not literal HTTPS; any
value other than `on`/`off` is rejected by the CRD enum.)

### `imagePullSecrets` for private registries (82e)

`spec.backup.jobTemplate.imagePullSecrets` (`[]ImagePullSecret`) are applied to the backup
Job pod spec by `applyJobTemplatePod` (via the shared `addImagePullSecrets` helper), and the
same template flows through restore, post-restore-validation, cleanup, and CronJob pods — so
every backup Job can pull the toolchain image from a private container registry.

```yaml
spec:
  backup:
    jobTemplate:
      imagePullSecrets:
        - name: regcred           # dockerconfigjson Secret for the private registry
```
