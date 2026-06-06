# Architecture Findings — Scenario 71 Live Backup Failure (gpbackup → S3 → gprestore)

> Scope: root-cause analysis and the smallest viable fix for the live
> backup-execution failure blocking **Scenario 71**. **Analysis only — no code
> is changed here.** This document is the design the `go-development` agent will
> implement.

---

## 0. TL;DR

`gpbackup` is an **MPP coordinator-dispatched tool**. Even with the
`gpbackup_s3_plugin` (which only streams *data files* to S3), `gpbackup` still
needs, on the **coordinator host** and on **every segment host**, a writable
on-disk *backup directory* derived from each segment's
`COORDINATOR_DATA_DIRECTORY` / `datadir`. It writes its **history DB** and
**metadata** under the coordinator data dir, and it dispatches **per-segment
directory creation + per-segment file writes** to each segment backend.

The operator runs the backup in a **standalone Job pod** that mounts only the
S3-config ConfigMap. It does **not** mount the coordinator data dir, and the
segment pods have no writable backup dir reachable at the path gpbackup expects.
Therefore both failure lines are inevitable:

```
[CRITICAL]: Unable to create backup directories on 3 segments.
[ERROR]: Unable to update history database. Error:
         open /data/pgdata/gpseg-1/gpbackup_history.db: no such file or directory
```

**Root cause is architectural, not a flag bug.** A standalone Job pod cannot host
a coordinator-dispatched MPP backup. The fix is **not code-only** for the
standalone-Job model; the standalone model is fundamentally incompatible with
`gpbackup`'s execution semantics.

**Recommended fix (smallest viable): run the backup/restore *inside the
coordinator pod* via `kubectl exec`-equivalent (a Job whose pod targets the
coordinator StatefulSet pod is NOT enough — the data dir is on the coordinator's
RWO PVC). Use the `gpbackup_s3_plugin` for data, and add `--backup-dir` pointing
at a path that exists on the coordinator and every segment.** Because each pod
already has its own writable PVC at `/data` and the official image already ships
`gpbackup_helper` in `GPHOME/bin`, the minimal working topology is:

1. Execute `gpbackup`/`gprestore` **in the coordinator container** (exec model),
   not in a detached pod, so the history DB + metadata land on the coordinator's
   real `/data/pgdata/gpseg-1`.
2. Add **`--backup-dir /data/backups`** (a directory that exists, and is
   writable, on the coordinator **and** every segment pod under their own
   `/data` PVC) so the per-segment metadata/`gpbackup_helper` files have a home.
3. Keep `--plugin-config` for the S3 plugin so the bulk **data files** stream to
   MinIO and are not persisted on the PVCs.

This keeps the binary work in the official image (which has `gpbackup`,
`gprestore`, `gpbackup_helper`, `gpbackup_s3_plugin` in `GPHOME/bin`) and
removes the broken standalone `cloudberry-backup:2.1.0` Job pod from the live
data path.

---

## 1. Confirmed Root Cause (with file/line + binary/image map)

### 1.1 The history-DB path `/data/pgdata/gpseg-1` is the coordinator data dir

- The official image sets `COORDINATOR_DATA_DIRECTORY=/data/pgdata/gpseg-1`
  (`Dockerfile.cloudberry-official:140,152`) and `PGDATA=/data/pgdata`
  (`:147`). The entrypoint derives `COORDINATOR_DATA_DIR="${PGDATA}/gpseg-1"`
  (`hack/docker-entrypoint-cloudberry.sh:33`).
- `gpbackup` writes its history DB to `$COORDINATOR_DATA_DIRECTORY/gpbackup_history.db`
  and its backup *report/metadata* under the coordinator backup dir by default.
  With no `--backup-dir`, the default base is the coordinator data dir.
- The backup **Job pod** is built from `cloudberry-backup:2.1.0`
  (`backup_builder.go:22,150-155,623`) and mounts **only** the S3-config
  ConfigMap at `/etc/gpbackup` (`backup_builder.go:906-939, 942-969`). It has
  **no** `/data/pgdata/gpseg-1`. Hence
  `open /data/pgdata/gpseg-1/gpbackup_history.db: no such file or directory`.

### 1.2 "Unable to create backup directories on N segments" is MPP dispatch

- `gpbackup` queries `gp_segment_configuration` for each segment's `datadir`
  and instructs the segment backends (and `gpbackup_helper`) to create a backup
  subdirectory under each segment's data dir (or under `--backup-dir` if given).
  The cluster registers segment datadirs as `/data/pgdata/gpseg0`,
  `/data/pgdata/gpseg1`, … (`hack/docker-entrypoint-cloudberry.sh:508,515,529,536`).
- The S3 plugin **does not remove this requirement**: the plugin is invoked for
  *data-file transfer* only; gpbackup still creates per-segment local
  directories for staging/metadata/pipes (especially with `gpbackup_helper`).
  Because the segment pods have no writable backup directory at the dispatched
  path *and the dispatch reaches them via the live MPP connection from the
  coordinator*, the create-dirs step fails for the segment instances.
- "3 segments" = the instances gpbackup counted (coordinator content -1 plus the
  primary segments in this 2-primary topology, or the set it dispatched to);
  the exact count is incidental — the point is **per-instance local dirs are
  required and absent**.

### 1.3 Binary → image map (verified)

| Binary | `cloudberry-backup:2.1.0` (Job pod) | `cloudberry-official:2.1.0` (cluster pods) |
|---|---|---|
| `gpbackup` | ✅ `/usr/local/bin` (`Dockerfile.cloudberry-backup:245`) | ✅ `GPHOME/bin` (`Dockerfile.cloudberry-official:96`) |
| `gprestore` | ✅ `/usr/local/bin` (`:246`) | ✅ `GPHOME/bin` (`:97`) |
| `gpbackup_helper` | ✅ `/usr/local/bin` (`:247`) | ✅ `GPHOME/bin` (`:98`) |
| `gpbackup_s3_plugin` | ✅ `/usr/local/bin` (`:248`) | ✅ `GPHOME/bin` (`:99`) |
| `psql`/libpq | ✅ (postgresql pkg `:235`) | ✅ (from RPM, under `GPHOME/bin`) |

**Key fact:** the official cluster image **already bundles `gpbackup_helper` and
all four tools** in `GPHOME/bin` (`Dockerfile.cloudberry-official:95-103`). So
running the backup *inside the coordinator container* needs **no image change**.

### 1.4 Topology facts that make the standalone Job impossible

- Coordinator/standby/segment pods each have their **own RWO PVC** named `data`
  mounted at `/data` (`builder.go:1170-1184`, `AccessModes:[ReadWriteOnce]`).
- There is **no shared/backup volume** on the cluster pods — `buildVolumes`
  only mounts `config` (ConfigMap) and optional TLS (`builder.go:1118-1160`).
- The coordinator's `/data/pgdata/gpseg-1` lives on a **ReadWriteOnce** PVC bound
  to `scenario71-secret-coordinator-0`. It **cannot** be co-mounted by a separate
  Job pod on a different node, and even on the same node RWO + StatefulSet PVC
  ownership makes this fragile/unsupported.

> **Conclusion:** the data gpbackup must write (history DB, metadata, per-segment
> dirs) lives on per-pod RWO PVCs the standalone Job pod cannot reach. This is an
> **architecture/execution-model defect**, not a missing CLI flag.

---

## 2. Findings

> **[SEVERITY: critical]** — Standalone backup Job pod cannot host an MPP gpbackup
>
> **Description:** `gpbackup` is coordinator-dispatched and writes its history DB
> + metadata to the coordinator data dir (`/data/pgdata/gpseg-1`) and dispatches
> per-segment local-dir creation to each segment. The Job pod
> (`cloudberry-backup:2.1.0`) mounts only the S3-config ConfigMap
> (`backup_builder.go:906-969`) and has none of these paths. The coordinator and
> each segment use **separate RWO PVCs** (`builder.go:1176`) that the Job pod
> cannot mount. The S3 plugin only moves *data files*; it does not eliminate the
> local history/metadata/per-segment-dir requirement.
> **Recommendation:** Stop running the live backup in a detached pod. Run
> `gpbackup`/`gprestore` **inside the coordinator container** (exec model). The
> official image already has all four binaries in `GPHOME/bin`
> (`Dockerfile.cloudberry-official:95-103`), so the history DB + metadata land on
> the real coordinator data dir and segment dispatch reaches live segment
> backends over the existing MPP connection.

> **[SEVERITY: critical]** — Missing `--backup-dir`; per-segment dirs default to
> each data dir
>
> **Description:** Neither `buildGpbackupArgs` nor `buildGprestoreArgs`
> (`backup_builder.go:166-195, 252-282`) ever passes `--backup-dir`. With the S3
> plugin, gpbackup still needs a writable local directory on the coordinator and
> every segment for metadata, segment TOC/report files, and `gpbackup_helper`
> named pipes. Defaulting to each instance's data dir is brittle (perms, mixing
> backup artifacts into PGDATA) and is the surface the segment "create
> directories" failure manifests on.
> **Recommendation:** Pass `--backup-dir /data/backups` (a path that exists and
> is writable on the coordinator **and** every segment under their own `/data`
> PVC). Create `/data/backups` in the entrypoint (or an init step) for every
> role so the dispatched create-dirs succeeds locally on each pod.

> **[SEVERITY: high]** — Pre-backup checks and env wiring assume the wrong pod
>
> **Description:** The pre-backup init container, S3 env, and `PGHOST` wiring
> (`backup_builder.go:645-714, 758-814`) are all built around the standalone Job
> pod connecting to the coordinator over the network. In the exec model the
> backup runs *on* the coordinator, so `PGHOST` becomes the local socket / the
> tool relies on the coordinator's own `gp_segment_configuration`, and the
> destination/health checks should run in-pod.
> **Recommendation:** Re-target the pre-backup checks and env to the exec
> context; keep the S3 ConfigMap/credentials, but inject S3 env into the
> coordinator exec session rather than the Job pod.

> **[SEVERITY: medium]** — Spec 11 over-promises the standalone-Job model
>
> **Description:** Spec 11 §Execution Model (`11-backup-restore-spec.md:20-31`)
> asserts every backup runs as a detached Job and "never runs backup logic in the
> controller process". The detached-Job shape is incompatible with gpbackup MPP
> semantics in this per-pod-RWO-PVC topology. The spec already *notes*
> `gpbackup_helper` must be on every segment host (`:15`) and references
> `--backup-dir` for local (`:70`) but never wires `--backup-dir` for the S3
> path.
> **Recommendation:** Update Spec 11 to define the **coordinator-exec execution
> model** for live data backups (the operator opens an exec stream into the
> coordinator pod, or creates a Job that `kubectl exec`s into it), document the
> mandatory `--backup-dir` shared path, and clarify that the standalone Job image
> is only for metadata/cleanup tooling (`gpbackman`) that does not require the
> data dirs.

> **[SEVERITY: low]** — `cloudberry-backup` image still useful for cleanup/exporter
>
> **Description:** The standalone image is fine for `gpbackman` retention cleanup
> and the `gpbackup_exporter` (they talk to S3 + history over the network, not to
> segment data dirs). Only the *live backup/restore data path* must move to exec.
> **Recommendation:** Keep `cloudberry-backup:2.1.0` for cleanup/exporter Jobs;
> route only `BuildBackupJob`/`BuildRestoreJob` data operations through the
> coordinator-exec model.

---

## 3. Why the S3 plugin does NOT save the standalone model

`gpbackup_s3_plugin` implements the gpbackup *storage-plugin contract*
(`setup_plugin_for_backup`, `backup_data`, `backup_file`, …). It is invoked by
gpbackup/`gpbackup_helper` to **ship data files** to S3 instead of leaving them
on local disk. It does **not** relocate:

- the **history DB** (`$COORDINATOR_DATA_DIRECTORY/gpbackup_history.db`),
- the **metadata/TOC/report** files (under the coordinator backup dir or
  `--backup-dir`),
- the **per-segment working directories** that gpbackup creates on each segment
  (used for staging, the `gpbackup_helper` named pipes, and per-segment TOCs).

So even a *perfect* S3 plugin run still requires writable local dirs on the
coordinator and every segment — exactly what the standalone Job pod lacks.

---

## 4. Smallest Viable Change — Decision

### Options evaluated

| Option | Verdict | Why |
|---|---|---|
| (a) `--backup-dir` on a writable path mounted in the **Job** pod | ❌ insufficient alone | The path must also be **the same writable path on every segment and the coordinator**, reachable by their backends. A volume mounted in the Job pod is invisible to the segment/coordinator containers. Solves nothing for segment dispatch. |
| (b) `--single-data-file` + `gpbackup_helper` + S3 plugin | ⚠️ necessary-ish, not sufficient | `gpbackup_helper` **is** present in the official image (`Dockerfile.cloudberry-official:98`) but it runs **on the segment hosts** invoked by the **coordinator** — i.e., it only works when the backup is dispatched from inside the running cluster. It does not make a *standalone* Job pod work, and it still needs a local `--backup-dir` for pipes/TOC. Adopt `--single-data-file` as a tuning option, not as the fix. |
| (c) Shared **RWX** PVC mounted by coordinator+segments+job | ❌ rejected | Requires every cluster pod to mount a new RWX volume (NFS/CephFS class), changes the StatefulSet pod spec for all roles, and is operationally heavy. Most clusters provision RWO block storage; mandating RWX is a large, risky change for a logical backup that already streams data to S3. |
| (d) Run backup **inside the coordinator pod** (exec/sidecar) | ✅ **chosen** | The coordinator pod already has the data dir, all four tools in `GPHOME/bin`, the live MPP connection to segments, and per-pod writable `/data`. `gpbackup` dispatches segment work over its normal MPP path. Smallest change to make a real cycle work; no RWX, no image rebuild. |

### Chosen design (Option d + a writable per-pod `--backup-dir`)

**Execution model:** the operator runs `gpbackup`/`gprestore` **in the
coordinator container** of `scenario71-secret-coordinator-0`. Two equivalent
implementations (pick per operator conventions):

1. **Exec-from-controller / exec-Job:** a small Job (using a `kubectl`/client-go
   image, or the operator itself) that opens an exec stream into the coordinator
   pod and runs the tool there. The tool inherits the coordinator's `GPHOME`,
   `COORDINATOR_DATA_DIRECTORY`, and live cluster.
2. **Backup-as-exec via the API server:** the operator's existing backup endpoint
   triggers an exec into the coordinator (the API already drives the cycle in
   `scenario71-backup-restore.sh`).

**Arguments (added by the builder for the exec command):**

```
gpbackup \
  --dbname mydb \
  --backup-dir /data/backups \
  --plugin-config /tmp/s3-config.yaml \
  [--single-data-file] \         # optional tuning; uses gpbackup_helper on segments
  --with-stats ...

gprestore \
  --timestamp <TS> \
  --backup-dir /data/backups \
  --plugin-config /tmp/s3-config.yaml \
  [--create-db] ...
```

- `--backup-dir /data/backups` lives on each pod's **own** `/data` RWO PVC, so
  every segment/coordinator backend can create its local subdir there.
- The S3 plugin still streams the bulk **data files** to MinIO, so `/data/backups`
  only holds small metadata/TOC/report + history — negligible PVC growth.

**Required supporting changes:**

- **Entrypoint / init:** create `/data/backups` (owned `gpadmin:gpadmin`, mode
  0700) for **every** role in `hack/docker-entrypoint-cloudberry.sh` (alongside
  the `mkdir -p "${PGDATA}"` at `:629`), so the dispatched create-dirs succeeds
  on coordinator + every segment. **No image binary change needed.**
- **S3 plugin config:** render `/tmp/s3-config.yaml` inside the coordinator exec
  session (reuse the existing ConfigMap template + envsubst from
  `backup_builder.go:320-343, 366-388`), or mount the S3-config ConfigMap into
  the coordinator pod.
- **Credentials:** inject `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` into the
  exec session from the same Secret the builder already resolves
  (`backup_builder.go:879-903`).

### Is the fix code-only?

- **Builder (Go):** yes — change `BuildBackupJob`/`BuildRestoreJob` to emit a
  coordinator-exec command (or have the controller/API exec into the coordinator)
  and add `--backup-dir /data/backups` to `buildGpbackupArgs`/`buildGprestoreArgs`.
- **Image change:** **NOT required** for binaries (official image already has
  `gpbackup_helper` + all tools). **A tiny entrypoint change** is required to
  `mkdir /data/backups` per role — that is a script change in
  `hack/docker-entrypoint-cloudberry.sh`, not a new binary. (Alternatively the
  exec command itself can `mkdir -p /data/backups` on the coordinator, and rely
  on gpbackup to create per-segment subdirs — but pre-creating on all roles is
  the robust choice.)
- **Shared PVC:** **NOT required.**

---

## 5. Spec update implied

Update `specifications/11-backup-restore-spec.md`:

1. **§Execution Model** — replace "every backup runs as a detached Job" with the
   **coordinator-exec** model for live data backup/restore; keep detached Jobs
   only for `gpbackman` cleanup and the `gpbackup_exporter`.
2. **§S3 Plugin / args** — document mandatory `--backup-dir /data/backups` for
   both S3 and local destinations, and state that the S3 plugin handles only
   data-file transfer (history/metadata/per-segment dirs remain local).
3. **§Toolchain** — note that the **official cluster image** carries the gpbackup
   toolchain in `GPHOME/bin`, which is what enables the exec model.
4. **§Entrypoint** — document that `/data/backups` is created per role.

---

## 6. Concrete handoff to `go-development`

1. `hack/docker-entrypoint-cloudberry.sh`: after `mkdir -p "${PGDATA}"` (`:629`),
   add `mkdir -p /data/backups && chmod 700 /data/backups` for **all** roles
   (coordinator, standby, primary, mirror).
2. `internal/builder/backup_builder.go`:
   - In `buildGpbackupArgs` (`:166`) and `buildGprestoreArgs` (`:252`), inject
     `--backup-dir /data/backups` (make the path a const, default `/data/backups`).
   - Change `BuildBackupJob` (`:439`) and `BuildRestoreJob` (`:468`) to produce a
     **coordinator-exec** operation instead of a standalone `cloudberry-backup`
     pod for the data path: either (i) a Job that execs into
     `<cluster>-coordinator-0` via client-go/kubectl, or (ii) wire the controller/
     API to exec the rendered script in the coordinator container `cloudberry`.
   - Render the S3 plugin config + S3 env **into the exec session** (reuse
     `renderToolScript`, `BuildBackupS3ConfigMap`, `buildS3Env`).
3. Keep `BuildRetentionCleanupJob` (`:579`) on `cloudberry-backup:2.1.0`
   (network-only; unaffected).
4. Update Spec 11 per §5.

See `architecture.md`, `api.md`, `data.md`, `security.md`, `dependency-map.md`,
and `requirement.md` in this folder for the full target design.
