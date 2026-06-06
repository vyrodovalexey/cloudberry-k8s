# Requirements — Working gpbackup → S3 → gprestore Cycle (Scenario 71)

## Functional requirements

- **FR1** A user-triggered FULL backup of `mydb` to MinIO must complete with the
  S3 plugin streaming data files to the bucket and the gpbackup history/metadata
  persisted.
- **FR2** `gpbackup` must successfully create per-segment backup directories on
  all segment instances (no "Unable to create backup directories" error).
- **FR3** The gpbackup history DB must be writable (no
  `gpbackup_history.db: no such file or directory`).
- **FR4** A restore of the same timestamp (with `--create-db`) must recreate
  `mydb` and reproduce per-table row counts equal to the pre-backup baseline.
- **FR5** Retention cleanup (`gpbackman`) and the optional exporter continue to
  work as detached, network-only Jobs.

## Non-functional / constraints

- **NFR1 (smallest change):** prefer no new image binaries and no RWX storage.
- **NFR2 (topology):** each role pod keeps its own RWO `data` PVC; no shared
  cross-pod volume.
- **NFR3 (security):** S3 credentials remain in Secrets, injected as env; no
  plaintext in specs; S3 traffic TLS.
- **NFR4 (compat):** preserve existing CRD fields, REST API shapes, and
  Scenario 71 e2e script behavior.

## Derived technical requirements (the fix)

- **TR1 — Execution model:** run `gpbackup`/`gprestore` **inside the coordinator
  container** (exec model), not in a standalone Job pod, so history/metadata land
  on the real coordinator data dir and segment dispatch uses the live MPP path.
- **TR2 — Backup dir:** pass `--backup-dir /data/backups` for backup and restore.
- **TR3 — Per-pod backup dir:** `/data/backups` must exist (0700, gpadmin) on
  coordinator + standby + every segment pod (entrypoint change).
- **TR4 — Plugin/creds in exec:** render the S3 plugin config and inject `AWS_*`
  env into the coordinator exec session (reuse existing ConfigMap/Secret
  builders).
- **TR5 — RBAC:** grant the operator SA `pods/exec` for the coordinator pod.
- **TR6 — Keep standalone image** for `gpbackman` cleanup and `gpbackup_exporter`.
- **TR7 — Optional tuning:** support `--single-data-file` (uses the
  already-bundled `gpbackup_helper`) without making it the correctness mechanism.

## Out of scope

- Rewriting the standalone Job image.
- Introducing RWX/NFS storage.
- Changing the MPP segment topology or PVC access modes.

## Acceptance

`test/e2e/scripts/scenario71-backup-restore.sh` passes for both `secret` and
`vault` variants: backup Job/op completes, MinIO bucket contains objects for the
timestamp, restore completes, and `dim`/`events` row counts match baseline.
