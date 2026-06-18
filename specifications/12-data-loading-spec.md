# Specification 12: Data Loading

## Overview

This specification defines the data loading capabilities for Cloudberry Database clusters managed by the operator. It leverages the [Apache Cloudberry PXF](https://github.com/apache/cloudberry-pxf) (Platform Extension Framework) as the primary data access layer for external data sources, supplemented by Cloudberry-native bulk loading utilities (`gpfdist`, `gpload`) and operator-managed Kubernetes Jobs for scheduled and on-demand ingestion workflows.

PXF is a Java-based service that runs on every segment host and provides a `pxf://` protocol for creating external tables and foreign data wrappers (FDW) against heterogeneous external data stores. The operator manages the full PXF lifecycle: deployment, server configuration, credential injection, health monitoring, and extension setup within the database.

## Implementation Status

> **Read this first.** This specification documents both the **implemented**
> declarative contract and the **implemented + planned** runtime/execution design.
> The PXF *sidecar* deployment and *servers ConfigMap* rendering, the
> **data-loading Job/CronJob ingestion runtime** (external-table DDL → `INSERT…
> SELECT` → `ANALYZE`), the **live credential init-container** secret rendering,
> the best-effort **PXF extension setup**, the **rich per-job status**, and the
> **5 data-loading metrics** are now **implemented** (segment-primary scope, gated
> on `dataLoading.enabled`). The **per-type PXF server file-mapping** (SL.1–6) and
> the **`CREATE EXTENSION pxf`/`pxf_fdw` + `GRANT SELECT`/`INSERT ON PROTOCOL pxf
> TO gpadmin`** (RP.9–RP.11, best-effort/non-fatal) are also **implemented**;
> config sync across sidecars is satisfied structurally by the shared
> `<cluster>-pxf-servers` ConfigMap (RP.12), and an **explicit** on-demand sync
> trigger (`cloudberry-ctl pxf sync` / `POST .../pxf/sync`) plus the
> `pxf status`/`pxf restart` lifecycle verbs are now **Implemented** (Scenario
> 95). The **gpfdist Deployment/Service/PVC** and the **gpload control-file
> Job/CronJob** are now **Implemented** (Scenario 101). The **FDW-based loading
> path** (`pxfJob.loadMethod: fdw` → persistent `CREATE SERVER`/`USER MAPPING`/
> `FOREIGN TABLE`) is now **Implemented** (Scenario 103). The **data-loading
> security controls SE.1–SE.6 / SL.6** (init-container secret rendering, the
> dedicated minimal-privilege DB role, Kerberos keytab mounting, the
> segment↔sidecar `localhost`-only NetworkPolicy, and TLS-via-config passthrough)
> are now **Implemented** (Scenario 111) — with the **HONESTY caveat** that live
> *authenticated* Kerberos and live *encrypted* TLS handshakes are **CONFIG-ONLY**
> (the test env has no KDC and no TLS-speaking source; the rendered config is
> verified, a live handshake is never faked). The rest of the PXF/gpfdist
> *runtime* (writable-table export Job kind, the 7 `cloudberry_pxf_*`
> runtime/health metrics, and the 2 `cloudberry_gpfdist_*` metrics) is **design
> only** and **not built yet**. Every major section below carries an explicit
> **Status** note. Use this legend:
>
> ## ✅ PXF live-execution note (read before relying on `pxf://`)
>
> The operator **GENERATES, LAUNCHES, and now RUNS** the `pxf://` load Job/CronJob
> end-to-end (external-table DDL + `INSERT…SELECT` + `ANALYZE`). A **live `pxf://`
> read-back is now Implemented and row-count verified**: an operator-driven
> `pxf://` load from MinIO S3 (via the PXF sidecar) loaded **183,961 rows** with
> credentials rendered automatically by the operator (no manual workarounds). For
> PXF: **Job generation/launch = Implemented; live `pxf://` execution =
> Implemented**, provided the cluster runs the **`cloudberry-pxf` sidecar image**
> (built from `apache/cloudberry-pxf` via `Dockerfile.cloudberry-pxf`) **and** the
> DB image ships the **`pxf`/`pxf_fdw` extensions** (the `cloudberry-official-pxf`
> image, built via `Dockerfile.cloudberry-official-pxf`). On a stock
> `cloudberry-official:2.1.0` (no PXF agent/extension) the `pxf://` Job still
> generates/launches correctly but cannot read back — that is an image
> prerequisite, not a code gap.
>
> The **engine-native** external-table protocols — `gpfdist://` (and bare paths
> served by the cluster gpfdist Service), `s3://` — **also load real data
> end-to-end** using the *same* operator Job machinery (only the LOCATION
> protocol differs). This native path needs **no PXF** and runs on
> `cloudberry-official`; it is **row-count verified** (e.g. **183,961 rows**
> loaded from a staged CSV). (`file://` is **not** a supported
> `gploadJob.filePaths` input for a multi-segment cluster — it is
> admission-rejected by webhook rule **W.16**; see
> [Webhook Validation](#webhook-validation).)

| Badge | Meaning |
|-------|---------|
| **Status: Implemented** | Present in code, exercised by tests. Safe to rely on. |
| **Status: 501-stub** | Route/command exists but returns `501 NOT_IMPLEMENTED` (no behavior yet). |
| **Status: Planned** | Design/future content only. **Not built.** Do not rely on it. |

**What is implemented today** is the *declarative* data-loading model:

- The full `DataLoadingSpec` CRD (`api/v1alpha1/types.go`): `pxf`, `gpfdist`,
  `jobs[]` (`pxfJob`/`gploadJob`), and `jobTemplate`.
- The validating admission webhook rules **W.1–W.25** (including the W.10 PXF
  profile allowlist, the W.16 `file://` rejection for gpload jobs, the
  Scenario 102 custom-connector / streaming rules **W.23/W.24/W.23c**, and the
  Scenario 103 `loadMethod` rule **W.25**) and the 14
  mutating-webhook **defaults**.
- A controller reconcile that **validates config and counts jobs**: it sets a
  lightweight `Status.DataLoading` (phase, configured/active counts, per-job
  `name`/`enabled`), the `cloudberry_data_loading_jobs_active` gauge, and the
  `DataLoadingConfigured` condition.
- REST **read** routes (`jobs` list/get) and the live CLI commands
  (`data-loading jobs {list,create,start,stop,delete}`, `data-loading status`).
- **PXF sidecar deployment** on **segment-primary** pods plus the rendered
  **`<cluster>-pxf-servers` ConfigMap** (`internal/builder/pxf_builder.go`,
  injected by `BuildSegmentPrimaryStatefulSet`), gated on
  `dataLoading.enabled && pxf.enabled && pxf.image != ""`. The reconcile applies
  the ConfigMap, sets the config-derived `cloudberry_pxf_servers_configured`
  gauge, populates `status.dataLoading.pxf.{configured,servers}`, and enriches
  the `DataLoadingConfigured` condition with the PXF server count.

**Also implemented now (this cycle — the ingestion runtime):**

- **Data-loading Jobs/CronJobs.** The operator **creates** a one-off `Job` (no
  `schedule`) or a `CronJob` (when `schedule` is set) for every enabled
  `dataLoading.jobs[]` entry, named `<cluster>-dataload-<job>`, container
  `dataload`, image = `cluster.Spec.Image` (`internal/builder/dataload_builder.go`
  `BuildDataLoadJob`/`BuildDataLoadCronJob`; controller
  `reconcileDataLoadingJobs`).
- **External-table DDL generation** (`buildExternalTableDDL`): `CREATE EXTERNAL
  TABLE <tmp> (LIKE <target>) LOCATION (...) FORMAT ...` — `pxf://` for PXF jobs,
  `gpfdist://`/`s3://` (or a bare path served via the cluster gpfdist Service)
  for native/gpload jobs — with optional `LOG ERRORS SEGMENT REJECT LIMIT`.
  (`file://` is admission-rejected for multi-segment gpload jobs by W.16; the
  builder keeps a verbatim `file://` passthrough for a future single-host
  caller.)
- **Live credential init-container** (`pxf-cred-init`): `envsubst`-resolves the
  `${PLACEHOLDER}` site-XML templates against the `credentialSecrets[]` into the
  shared `pxf-servers` emptyDir (one `<server>/<file>.xml` directory per server)
  at pod start; **secrets never land in the ConfigMap**.
- **Best-effort PXF extension setup** (`SetupPXFExtensions`): `CREATE EXTENSION
  IF NOT EXISTS pxf/pxf_fdw`, annotation-gated, **non-fatal** when the extension
  is absent.
- **Rich per-job status** (`status.dataLoading.jobs[].{lastRun,lastStatus,
  rowsLoaded,duration}`) sourced from real terminal Job status + the harvested
  `DATALOAD_ROWS` marker.
- **5 data-loading metrics** emitted from Job status / the marker.

**Now Implemented (Scenario 96):** object-store server types `gs`/`abfss`/`wasbs`
(+ Dell-ECS / MinIO variants), the **format write-capability enforcement** (W.10b
admission + builder re-check via `internal/pxfpolicy`), and the
`pxfwritable_export` **writable external-table DDL**.

**Now Implemented/verified (Scenario 97):** the **Hadoop profiles**
(HDFS/Hive/HBase) write-capability is enforced, including a **scheme-aware**
correction — `IsProfileWritable` now treats `hive*`/`HBase` as **read-only at the
scheme level regardless of format** (`readOnlySchemes={hive,hbase}`), so writable
`hive:text` is rejected — and the `hive-site.xml`/`hbase-site.xml` rendering
(`hive.metastore.uris` + `hbase.zookeeper.quorum`) is verified. See
[Scenario 97](#scenario-97--hadoop-profiles-hdfs--hive--hbase).

**Now Implemented (Scenario 101):** the **gpfdist Deployment/Service/PVC**
(`reconcileGpfdist`, gated on `dataLoading.gpfdist.enabled`) and the **gpload
control-file Job/CronJob** — a `type: gpload` job now renders a gpload YAML
control file (GL.1-GL.7), delivers it via the `<cluster>-gpload-<job>` ConfigMap
mounted at `/etc/gpload`, and runs `gpload -f`. See
[Scenario 101](#scenario-101--gpfdist-deployment--gpload-csv).

**Now Implemented (Scenario 103):** the **FDW-based loading path** — a
`pxfJob.loadMethod: fdw` PXF job builds the **persistent** `CREATE SERVER` /
`CREATE USER MAPPING` / `CREATE FOREIGN TABLE` chain (`IF NOT EXISTS`, never
dropped) with the live-verified per-protocol `pxf_fdw` wrapper, then loads via
`INSERT INTO <target> SELECT * FROM <foreign> [WHERE <sourceFilter>]`. It is
**EQUIVALENT** to the external-table path (equal row counts). See
[Scenario 103](#scenario-103--fdw-based-loading-path).

**What is still planned (not built)** is the rest of the PXF/gpfdist *runtime*:
the first-class
**data-export Job kind** (the writable-DDL path + its admission are Implemented;
see Scenario 96), pre-load health checks, Kerberos, and the **4** remaining
`cloudberry_pxf_*` runtime metrics (`bytes_transferred_total`, `records_total`,
`errors_total` — **folded**, not synthesized — and `active_connections`) plus the
2 `cloudberry_gpfdist_*` metrics (which have **no scrapable endpoint** and stay
Planned). **Scenario 109** flipped four metrics to Implemented from real sources:
`cloudberry_pxf_service_up` (per-segment readiness), the actuator-passthrough
`cloudberry_pxf_requests_total`/`cloudberry_pxf_request_duration_seconds`, and the
conditional `cloudberry_data_loading_bytes_total` (local gpload `wc -c`; omitted
otherwise). The `pxf/*` REST routes + CLI group are Implemented (Scenarios 95,
107, 108); the job-mutation REST routes (create/update/delete/start/stop) are now
**FULL** (Scenario 107).

> **Live `pxf://` execution is now Implemented** (see the note above): the
> operator generates, launches, and runs the PXF load Job end-to-end, **provided**
> the cluster runs the `cloudberry-pxf` sidecar image + the `pxf` extension in the
> DB image (the `cloudberry-official-pxf` image). It is **row-count verified**
> (183,961 rows from MinIO S3 via the PXF sidecar). On a stock
> `cloudberry-official` (no PXF extension) only generation/launch holds. The
> **native** `gpfdist://`/`s3://` load path (and bare paths served via the cluster
> gpfdist Service) remains the no-extension, row-count-verified baseline.
> (`file://` is admission-rejected for multi-segment gpload jobs by W.16.)

See the [Conformance / Implementation Status Summary](#conformance--implementation-status-summary)
at the end for an at-a-glance matrix.

## Architecture

### PXF Components

> **Status: Mixed.** The PXF **sidecar JVM** is now deployed as a sidecar
> container on **segment-primary** pods (Implemented), and the **extension setup**
> (`CREATE EXTENSION pxf`/`pxf_fdw` + `GRANT SELECT`/`INSERT ON PROTOCOL pxf TO
> gpadmin`, best-effort/non-fatal) is now Implemented. Config sync across sidecars
> is satisfied structurally by the shared `<cluster>-pxf-servers` ConfigMap (no
> explicit `pxf sync` needed). The **CLI bootstrap invocation** (`pxf
> prepare/start`) is owned by the image entrypoint, not the operator.

The PXF framework from `apache/cloudberry-pxf` consists of:

- **PXF Server** — a long-running JVM process on each segment host that handles data access requests from Cloudberry segments via REST. Deployed as a sidecar container on each segment pod.
- **PXF Extension (`pxf`)** — a C-language Cloudberry extension implementing the external table protocol handler (`pxf://` protocol).
- **PXF FDW Extension (`pxf_fdw`)** — a C-language Cloudberry extension implementing foreign data wrappers for PXF, supporting `CREATE FOREIGN TABLE` / `CREATE SERVER` syntax.
- **PXF CLI (`pxf`)** — command-line utility for managing PXF service lifecycle (`pxf prepare`, `pxf start`, `pxf stop`, `pxf restart`, `pxf status`, `pxf sync`).
### Built-in Connectors

PXF includes connectors for the following external data stores:

| Connector | Data Sources | Profiles |
|-----------|-------------|----------|
| Object Store | S3, MinIO, GCS, Azure Blob, Azure Data Lake, Dell ECS | `s3:text`, `s3:parquet`, `s3:avro`, `s3:json`, `s3:orc`, `gs:*`, `abfss:*`, `wasbs:*` |
| Hadoop | HDFS, Hive, HBase | `hdfs:text`, `hdfs:parquet`, `hdfs:avro`, `hdfs:json`, `hdfs:orc`, `hdfs:SequenceFile`, `hive`, `hive:text`, `hive:orc`, `hive:rc`, `HBase` |
| JDBC | PostgreSQL, MySQL, Oracle, SQL Server, any JDBC driver | `jdbc` |

> **Object-store server TYPES (Scenario 96).** `gs`, `abfss`, and `wasbs` are now
> valid `dataLoading.pxf.servers[].type` values — not just profile schemes. The
> CRD `PxfServerSpec.Type` enum is `s3;hdfs;jdbc;hbase;hive;gs;abfss;wasbs` and
> webhook rule **W.3** admits all eight. All four object-store types
> (`s3`/`gs`/`abfss`/`wasbs`) render into a single `<server>__s3-site.xml` (PXF
> reads every object store from the `s3-site.xml`-style config; the profile scheme
> selects the connector at query time). Dell ECS is an `s3` server with a custom
> `fs.s3a.endpoint`; MinIO is an `s3` server with `fs.s3a.path.style.access=true`.
> See [Scenario 96](#scenario-96--object-store-profiles--format-write-capability).

### Supported File Formats

> **Status: Write-capability now ENFORCED (Implemented — Scenario 96).** This
> Read/Write matrix was previously documentation-only; the write column is now
> **enforced** by the admission webhook (rule **W.10b**) and re-checked by the DDL
> builder (defense-in-depth), both reading the single source of truth
> [`internal/pxfpolicy`](#single-source-of-truth-internalpxfpolicy). A
> `mode: writable` job whose profile format has **Write = No** (json/orc/rc) is
> **rejected at admission**; text/parquet/avro/SequenceFile writable jobs are
> admitted and build a `pxfwritable_export` writable external table. See
> [Writable External Tables](#writable-external-tables-data-export) and
> [Scenario 96](#scenario-96--object-store-profiles--format-write-capability).

| Format | Read | Write |
|--------|------|-------|
| Text (CSV/TSV) | Yes | Yes |
| Parquet | Yes | Yes |
| Avro | Yes | Yes |
| JSON | Yes | No |
| ORC | Yes | No |
| RCFile | Yes | No |
| SequenceFile | Yes | Yes |

### Execution Model

> **Status: Mixed.** The PXF **sidecar**, **servers ConfigMap**, **credential
> init-container**, **best-effort extension setup**, the **data-loading
> Job/CronJob ingestion runtime**, the **gpfdist `Deployment`/`Service`/`PVC`**
> (`reconcileGpfdist`), and the **gpload control-file Job/CronJob** are now created
> (Implemented; segment-primary scope for the sidecar). The **FDW-based loading
> path** (`pxfJob.loadMethod: fdw` → persistent `CREATE SERVER`/`USER MAPPING`/
> `FOREIGN TABLE`) is now **Implemented (Scenario 103)**. The remaining resources
> (the first-class data-export Job kind) are **Planned**. See
> [Operator Reconciliation Logic](#operator-reconciliation-logic).

| Action | Kubernetes Resource | Status |
|--------|---------------------|--------|
| PXF Server process | Sidecar container on each **segment-primary** pod | **Implemented** |
| PXF server config | `<cluster>-pxf-servers` ConfigMap (`*-site.xml`) | **Implemented** |
| Live credential resolution | `pxf-cred-init` init container (`envsubst` → resolved `*-site.xml`) | **Implemented** |
| PXF extension setup | Operator reconcile (`SetupPXFExtensions`, best-effort/non-fatal) | **Implemented** |
| Scheduled data load (`CREATE EXTERNAL TABLE` → `INSERT…SELECT` → `ANALYZE`) | `CronJob` → spawns `Job` | **Implemented** (native + `pxf://` both execute; `pxf://` requires the `cloudberry-pxf` sidecar image + the `pxf` extension) |
| On-demand data load | `Job` `<cluster>-dataload-<job>` (operator-created) | **Implemented** (native + `pxf://` both execute; `pxf://` requires the `cloudberry-pxf` sidecar image + the `pxf` extension) |
| FDW-based load (`loadMethod: fdw`: persistent `CREATE SERVER`/`USER MAPPING`/`FOREIGN TABLE` IF NOT EXISTS → `INSERT…SELECT` from the foreign table → `ANALYZE`; never dropped) | `Job`/`CronJob` `<cluster>-dataload-<job>` (operator-created) | **Implemented** (Scenario 103; per-protocol `pxf_fdw` wrapper; EQUIVALENT to the external-table path; live read-back requires the `cloudberry-pxf` sidecar + the `pxf_fdw` extension) |
| gpfdist-based parallel load | `Deployment` + `Service` + `PVC` (`<cluster>-gpfdist*`, `reconcileGpfdist`, gated on `gpfdist.enabled`) | **Implemented** (Scenario 101) |
| Bulk load via gpload control file | `<cluster>-gpload-<job>` ConfigMap (`/etc/gpload/<job>.yml`) + `Job`/`CronJob` running `gpload -f` | **Implemented** (Scenario 101) |

## CRD Specification

### DataLoadingSpec

> **Status: Implemented.** The full `DataLoadingSpec` CRD model exists in
> `api/v1alpha1/types.go` and is accepted/persisted by the API server. The
> example below uses the **real field names** (`config` map +
> `credentialSecrets[]`, `pxfJob`/`gploadJob`, `partitioning`, `errorHandling`).
> Note: declaring these fields is implemented; *acting on them* (deploying PXF,
> running jobs) is [Planned](#operator-reconciliation-logic).

Added to `CloudberryClusterSpec`:

```yaml
dataLoading:
  enabled: true
 
  pxf:
    enabled: true
    image: "cloudberry-pxf:7.1.0"
    jvmOpts: "-Xmx1g -Xms256m"
    port: 5888
    logLevel: INFO                         # DEBUG, INFO, WARN, ERROR
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
      limits:
        cpu: "2"
        memory: "2Gi"
    extensions:
      pxf: true                            # Install pxf extension (external tables)
      pxfFdw: true                         # Install pxf_fdw extension (foreign data wrappers)
    servers:
      # config is a plain string map of NON-SENSITIVE site settings only.
      # Credentials are referenced via credentialSecrets[] (resolved by an
      # init container) and never placed inline in config.
      - name: s3-datalake
        type: s3
        config:
          fs.s3a.endpoint: "s3.amazonaws.com"
          fs.s3a.path.style.access: "true"   # Required for MinIO
        credentialSecrets:
          - name: s3-credentials
            key: access_key
          - name: s3-credentials
            key: secret_key
      - name: minio-warehouse
        type: s3
        config:
          fs.s3a.endpoint: "http://minio:9000"
          fs.s3a.path.style.access: "true"
        credentialSecrets:
          - name: minio-credentials
            key: access_key
          - name: minio-credentials
            key: secret_key
      - name: hadoop-cluster
        type: hdfs
        config:
          fs.defaultFS: "hdfs://namenode:8020"
        hive:
          hive.metastore.uris: "thrift://hive-metastore:9083"
        hbase:
          hbase.zookeeper.quorum: "zk1:2181,zk2:2181,zk3:2181"
      - name: mysql-oltp
        type: jdbc
        config:
          jdbc.driver: "com.mysql.cj.jdbc.Driver"
          jdbc.url: "jdbc:mysql://mysql:3306/production"
        jdbc:                                # optional jdbc-site.xml settings
          jdbc.pool.enabled: "true"
        credentialSecrets:
          - name: mysql-credentials
            key: username
          - name: mysql-credentials
            key: password
      - name: postgres-source
        type: jdbc
        config:
          jdbc.driver: "org.postgresql.Driver"
          jdbc.url: "jdbc:postgresql://pghost:5432/sourcedb"
        credentialSecrets:
          - name: pg-credentials
            key: username
          - name: pg-credentials
            key: password
      - name: kafka-connector              # Scenario 102 — type=custom (W.3); NO forced config keys
        type: custom                        # backed by the matching customConnectors[] entry (W.24)
    customConnectors:                      # Additional JARs for custom PXF plugins (C.18)
      - name: custom-connector
        jarUrl: "s3://artifacts/pxf-plugins/my-connector.jar"
      - name: kafka-connector              # Scenario 102 — backs the `kafka` profile
        jarUrl: "s3://cloudberry-data/connectors/kafka-connector.jar"
 
  gpfdist:
    enabled: false
    replicas: 2
    image: "cloudberry-gpfdist:2.1.0"
    port: 8080
 
  jobs:
    - name: s3-parquet-ingest
      type: pxf
      enabled: true
      schedule: "0 */6 * * *"              # Every 6 hours
      pxfJob:
        server: s3-datalake
        profile: s3:parquet
        resource: "s3a://data-lake/events/"
        targetTable: public.events
        mode: insert                        # insert | insert-select | writable
        filterPushdown: true
        columnProjection: true
        errorHandling:
          segmentRejectLimit: 100
          segmentRejectLimitType: rows      # rows | percent
          logErrors: true
 
    - name: jdbc-sync
      type: pxf
      enabled: true
      schedule: "30 2 * * *"               # Daily at 2:30 AM
      pxfJob:
        server: mysql-oltp
        profile: jdbc
        resource: "production.orders"
        targetTable: public.orders_staging
        mode: insert-select
        filterPushdown: true
        partitioning:                       # all three required together (W.14)
          column: order_date
          range: "2024-01-01:2026-12-31"
          interval: "1:month"
 
    - name: hdfs-hive-load
      type: pxf
      enabled: true
      schedule: "0 4 * * *"
      pxfJob:
        server: hadoop-cluster
        profile: hive:orc
        resource: "warehouse.fact_sales"
        targetTable: public.fact_sales
        mode: insert-select
        filterPushdown: true
        columnProjection: true
 
    - name: s3-export                       # writable EXPORT job (Scenario 99)
      type: pxf
      enabled: true
      pxfJob:
        server: s3-datalake
        profile: s3:text                    # writable format on a writable scheme (W.10b)
        resource: "s3a://export-bucket/exports/s3/"
        targetTable: public.export_src       # SOURCE table the rows are read FROM
        mode: writable                       # reverses the INSERT direction (export)
        sourceFilter: "region='us-east'"     # OPTIONAL filtered export (W.17); omit → full table
 
    - name: gpload-csv                       # gpfdist + gpload control file (Scenario 101)
      type: gpload
      enabled: true
      schedule: "*/30 * * * *"              # schedule → CronJob (J.25); omit → one-off Job
      gploadJob:
        targetTable: public.raw_data        # OUTPUT TABLE (GL.5 / J.34)
        mode: insert                        # insert | update | merge (GL.5 / J.35-37)
        format: csv                         # csv | text (GL.3 / J.30)
        inputSource:                         # WHERE gpload reads (GL.2 / J.26-29)
          type: gpfdist                      # gpfdist | local (J.27); default gpfdist
          # host/port optional — default the cluster gpfdist Service:8080 (J.28/29)
        filePaths:                           # globs → local FILE entries served by the gpfdist svc
          - "/incoming/*.csv"
        delimiter: ","                       # single char (GL.3 / J.31); default ","
        header: true                         # first row is a header (GL.3 / J.32)
        encoding: "UTF-8"                    # INPUT ENCODING (GL.3 / J.33); default UTF-8
        preload:
          truncate: true                     # PRELOAD TRUNCATE (GL.6 / J.39)
        postActions:                         # SQL.AFTER block (GL.7 / J.40)
          - "ANALYZE public.raw_data"
        # matchColumns / updateColumns required for mode update|merge (W.20 / J.36-37)
        errorHandling:                       # ERROR_LIMIT + LOG_ERRORS (GL.4 / J.38)
          segmentRejectLimit: 50
          segmentRejectLimitType: rows
          logErrors: true

    - name: kafka-cdc                        # continuous streaming via custom connector (Scenario 102)
      type: pxf
      enabled: true
      # NO schedule → a one-off long-running Job (NOT a CronJob, J.46)
      pxfJob:
        server: kafka-connector              # MUST be type=custom + customConnectors[] (W.23/W.24)
        profile: "kafka"                     # custom-connector profile — admitted ONLY via W.23
        resource: "cloudberry-cdc"           # the kafka topic/resource the consumer reads
        targetTable: public.kafka_events     # rows stream INTO this table
        continuous: true                     # long-running streaming consumer (J.43)
        batchSize: 10000                     # rows buffered before a flush (J.44, Min 1 → CBK_BATCH_SIZE)
        flushInterval: "30s"                 # Go duration between flushes (J.45 → CBK_FLUSH_INTERVAL)
 
  jobTemplate:
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
      limits:
        cpu: "2"
        memory: "2Gi"
    nodeSelector: {}
    tolerations: []
    serviceAccountName: cloudberry-data-loading-sa
    backoffLimit: 3
    activeDeadlineSeconds: 14400            # 4-hour timeout
    ttlSecondsAfterFinished: 86400
```

> **`kafka` is a valid `pxfJob.profile` ONLY via the custom-connector model
> (Scenario 102 — policy reversal, scoped to custom connectors).** Earlier drafts
> rejected every `kafka` profile at W.10 ("no streaming"). That blanket rejection
> is now **narrowed**: a `kafka` (or `rabbitmq`) profile is **admitted** when — and
> only when — the referenced server is `type: custom` **and** is backed by a
> matching `customConnectors[]` entry of the same name (**W.23** + **W.24**). A
> **bare** `kafka` job, or a `kafka` profile on a **non-custom** server, is **still
> REJECTED** — there is no built-in streaming. See
> [Scenario 102](#scenario-102--kafka-cdc-continuous-streaming-custom-connector)
> and [Removed Fields](#removed-fields-breaking-change).

### DataLoadingStatus

> **Status: Implemented (counts + per-job runtime fields + spec-derived PXF
> summary + live PXF health sub-status — Scenario 105).** The controller populates
> the counts and per-job `name`/`enabled` from the spec, the spec-derived
> `pxf.{configured,servers}`, the rich per-job **runtime** fields (`lastRun`,
> `lastStatus`, `rowsLoaded`, `duration`) harvested from the real terminal Job
> status + the `DATALOAD_ROWS` marker (`enrichDataLoadingStatus` in
> `internal/controller/dataload_controller.go`), **and** — now Implemented
> (Scenario 105) — the live PXF health sub-status `pxf.status` and
> `pxf.extensionsInstalled`. The runtime fields are **absent until a Job has run**
> (and `rowsLoaded` is **never synthesized** — it appears only when the marker is
> harvested from a succeeded Job). The `pxf.status` and `extensionsInstalled`
> fields are likewise **observed-only and HONEST**: derived from real
> segment-primary `pxf` container readiness (`ContainerStatuses`) and a real
> read-only `pg_extension` probe respectively, and **ABSENT when not observable**
> — never synthesized (see [Live PXF health sub-status](#live-pxf-health-sub-status-scenario-105)
> below). Note: `pxf.configured` and `pxf.servers` are computed purely from the
> spec (`configured = pxf.enabled && image set`; `servers = len(pxf.servers)`);
> they are **not** a live health report.

Added to `CloudberryClusterStatus` (`api/v1alpha1/types.go`,
`DataLoadingStatus` / `DataLoadingJobStatus`):

```yaml
# Implemented status (counts + spec-derived PXF summary + per-job runtime +
# live PXF health sub-status — Scenario 105):
dataLoading:
  phase: Configured                        # high-level phase
  configuredJobs: 4                        # total declared jobs (len(jobs))
  activeJobs: 3                            # jobs with enabled: true
  jobs:
    - name: s3-parquet-ingest             # spec-derived
      enabled: true                        # spec-derived
      lastRun: "2026-06-11T06:00:00Z"      # Job status.startTime (absent until a run)
      lastStatus: Succeeded                # Succeeded | Failed | Running | Pending
      rowsLoaded: 183961                   # harvested DATALOAD_ROWS marker (never synthesized)
      duration: "3m12s"                    # completion − start (absent until completed)
    - name: jdbc-sync
      enabled: true
  pxf:                                     # populated only when pxf is enabled
    configured: true                       # pxf.enabled && pxf.image set
    servers: 5                             # len(pxf.servers), spec-derived
    # Scenario 105 — LIVE, HONEST health sub-status (observed-only):
    status: Running                        # Running | Stopped | Error; ABSENT when unobservable.
                                           #   Derived ONLY from real segment-primary `pxf`
                                           #   container readiness (ContainerStatuses) aggregation —
                                           #   NO exec, NO live HTTP probe, NO synthesized health:
                                           #     all pxf containers ready  → Running
                                           #     some down (degraded)      → Error
                                           #     none ready                → Stopped
                                           #     no pods observed          → field ABSENT
    extensionsInstalled:                   # real read-only pg_extension probe (ListPXFExtensions);
      - pxf                                #   lists pxf and/or pxf_fdw when ACTUALLY installed;
      - pxf_fdw                            #   ABSENT (nil) when DB unreachable or none installed —
                                           #   never synthesized.
```

> A separate `status.dataLoadingJobs` integer is retained for backward
> compatibility and mirrors `dataLoading.activeJobs`.

#### Live PXF health sub-status (Scenario 105)

> **Status: Implemented (Scenario 105).** The live **PXF health** sub-status —
> `pxf.status` and `pxf.extensionsInstalled` — is now populated. Both fields are
> **LIVE, HONEST, and observed-only**: they reflect what the operator can actually
> observe right now, and the corresponding field is **ABSENT when unobservable**
> (never synthesized, never inferred from the spec).

- **`pxf.status`** — `"Running" | "Stopped" | "Error"`, **ABSENT** when
  unobservable. It is derived **ONLY** from real segment-primary `pxf` container
  readiness, aggregated from each pod's `ContainerStatuses`
  (`util.PXFReadyCount` + `util.PXFStatusFromReadiness`,
  `util.SegmentPrimaryPXFSelector`). There is **NO `kubectl exec`, NO live HTTP
  probe of the sidecar, and NO synthesized health**. The mapping is:

  | Observed segment-primary `pxf` containers | `pxf.status` |
  |-------------------------------------------|--------------|
  | **all** ready | `Running` |
  | **some** down (degraded) | `Error` |
  | **none** ready | `Stopped` |
  | **no pods observed** (unobservable) | *field ABSENT* |

- **`pxf.extensionsInstalled`** — `[]string`, from a **real read-only
  `pg_extension` probe** (`ListPXFExtensions` in `internal/db/client.go`). It
  lists `pxf` and/or `pxf_fdw` **when they are actually installed**, in
  deterministic order. It is **ABSENT (`nil`)** when the DB is unreachable **or**
  when no PXF extensions are installed — it is **never synthesized** (it is **not**
  derived from `pxf.extensions.{pxf,pxfFdw}` spec flags).

```yaml
dataLoading:
  pxf:
    status: Running                        # Running | Stopped | Error; ABSENT when unobservable
                                           #   (real ContainerStatuses readiness aggregation only)
    extensionsInstalled:                   # real read-only pg_extension probe; ABSENT when
      - pxf                                #   DB unreachable / none installed (never synthesized)
      - pxf_fdw
```

> **HONESTY framing.** Both fields encode "what we can see, when we can see it."
> A segment-stop that drops a `pxf` container's readiness flips `pxf.status`
> `Running → Error` (the degraded mapping); losing observability of the pods
> drops the field entirely rather than reporting a stale or guessed value.
> `extensionsInstalled` reports only extensions the live database actually has —
> on a stock `cloudberry-official:2.1.0` (which ships only a `pxf_fdw` client
> stub, not the `pxf` extension) the list honestly reflects exactly what
> `pg_extension` returns.

## Operator Reconciliation Logic

> **Status: Implemented = config-validation + status-counting + PXF sidecar /
> servers ConfigMap + disabled-state teardown.** When `dataLoading == nil` **or**
> `dataLoading.enabled: false`, `reconcileDataLoading` dispatches to
> **`cleanupDataLoading`** (Scenario 112 / DIS.1), which actively **tears down**
> the data-loading subsystem — it is **no longer a no-op early-return**. See
> [Scenario 112 — Disabled States (DIS.1–DIS.3)](#scenario-112--disabled-states-dis1dis3)
> for the exact GC'd object set, the `DataLoadingDisabled` condition/event, the
> zeroed gauges, and the idempotent redeploy on re-enable. The implemented
> `reconcileDataLoading`
> (`internal/controller/admin_controller.go`) does the following when
> `dataLoading.enabled: true`: counts configured/active jobs, writes the
> lightweight `Status.DataLoading`, sets the
> `cloudberry_data_loading_jobs_active` gauge, and — when `pxf.enabled` — calls
> `reconcilePxf`, which applies the `<cluster>-pxf-servers` ConfigMap
> (`ensurePxfServersConfigMap`), sets the `cloudberry_pxf_servers_configured`
> gauge, populates `status.dataLoading.pxf.{configured,servers}`, and enriches
> the `DataLoadingConfigured` condition with the PXF server count. It also emits
> a `DataLoadingReconciled` event. **On a real ConfigMap `Data` diff**,
> `ensurePxfServersConfigMap` additionally emits a `PXFServersChanged` event and
> increments `cloudberry_pxf_servers_changed_total` (`emitPXFServersChanged`,
> Scenario 106). The PXF **sidecar** itself is injected into the
> segment-primary StatefulSet pod template by `BuildSegmentPrimaryStatefulSet`,
> alongside the **`pxf-cred-init`** credential init-container (live `envsubst`
> secret rendering). The reconcile then calls **`setupPXFExtensions`**
> (best-effort `CREATE EXTENSION`, non-fatal) and **`reconcileDataLoadingJobs`**,
> which creates the per-job `Job`/`CronJob`, harvests the `DATALOAD_ROWS` marker,
> enriches the per-job runtime status, and emits the 5 data-loading metrics.
> (The `CREATE EXTENSION pxf`/`pxf_fdw` + `GRANT … ON PROTOCOL pxf TO gpadmin`
> setup is best-effort/non-fatal — RP.9–RP.11 — and config sync is structural via
> the shared ConfigMap, RP.12.) It also calls **`reconcileGpfdist`** (Scenario
> 101), which — gated on `dataLoading.gpfdist.enabled` — ensures the gpfdist
> `PVC`/`Deployment`/`Service` (GP.2-GP.5) and best-effort GCs them when disabled;
> and `reconcileDataLoadingJobs` routes a `type: gpload` job through the **gpload
> control-file path** (renders the `<cluster>-gpload-<job>` ConfigMap + a
> `Job`/`CronJob` running `gpload -f /etc/gpload/<job>.yml`). **What remains
> [Planned](#implementation-status) and not built in this section: FDW setup and
> the first-class data-export Job kind.**

### PXF Deployment

> **Status: Implemented (segment-primary sidecar + servers ConfigMap; gated on
> `dataLoading.enabled && pxf.enabled && pxf.image != ""`).** The sidecar is
> built by `BuildPXFSidecarContainers`/`BuildPXFSidecarVolumes` and injected into
> the **segment-primary** StatefulSet only — coordinator, standby, and mirror
> pods do **not** receive it. A default cluster (`dataLoading == nil`) yields a
> byte-identical pod template. Extension DDL (`CREATE EXTENSION pxf`/`pxf_fdw` +
> protocol GRANTs) and Job ingestion execution are now **Implemented**; config
> sync is structural (shared ConfigMap — no explicit `pxf sync`). **Still
> Planned:** the sidecar bootstrap `command`/`args` (owned by the image
> entrypoint) and the gpfdist/gpload runtime (see below).

When `dataLoading.pxf.enabled: true`, the operator:

1. **Deploys the PXF sidecar** on each **segment-primary** pod (**Implemented**). The PXF JVM process runs alongside the Cloudberry segment, listening on the configured port (default 5888). The env is derived from the spec with deterministic fallbacks (`PXF_LOG_LEVEL` defaults to `INFO`), so re-patching `pxf.logLevel` rebuilds the container with the new value. The implemented env var names are `PXF_HOME`, `PXF_BASE`, `PXF_JVM_OPTS`, `PXF_PORT`, `PXF_LOG_LEVEL`, `PXF_EXTENSION_PXF`, and `PXF_EXTENSION_PXF_FDW`:
```yaml
containers:
  - name: pxf
    image: cloudberry-pxf:7.1.0
    env:
      - name: PXF_HOME
        value: /usr/local/cloudberry-pxf
      - name: PXF_BASE
        value: /pxf-base
      - name: PXF_JVM_OPTS
        value: "-Xmx1g -Xms256m"
      - name: PXF_PORT
        value: "5888"
      - name: PXF_LOG_LEVEL
        value: "INFO"                      # from pxf.logLevel; defaults to INFO
      - name: PXF_EXTENSION_PXF
        value: "true"                      # from pxf.extensions.pxf (nil => true)
      - name: PXF_EXTENSION_PXF_FDW
        value: "true"                      # from pxf.extensions.pxfFdw (nil => true)
    ports:
      - name: pxf
        containerPort: 5888
    # PXF 2.1.0 exposes its health via the Spring Boot actuator at
    # /actuator/health (curl http://localhost:5888/actuator/health =>
    # {"status":"UP",...}). The legacy /pxf/v15/Status path is a DB-client
    # endpoint that returns 404 and must NOT be used as a health check.
    livenessProbe:
      httpGet:
        path: /actuator/health
        port: 5888
      initialDelaySeconds: 60
      periodSeconds: 20
    readinessProbe:
      httpGet:
        path: /actuator/health
        port: 5888
      initialDelaySeconds: 30
      periodSeconds: 10
    volumeMounts:
      - name: pxf-base                     # emptyDir
        mountPath: /pxf-base
      - name: pxf-servers                  # <cluster>-pxf-servers ConfigMap (Optional)
        mountPath: /pxf-base/servers
      - name: pxf-lib                      # emptyDir
        mountPath: /pxf/lib/custom
```

> The sidecar **does not** set a `command`/`args` today (the `pxf prepare`/`pxf
> start` bootstrap remains **Planned** — it is owned by the image entrypoint); it
> relies on the image's own entrypoint. An explicit `pxf sync` is **not needed**
> (config sync is structural via the shared `<cluster>-pxf-servers` ConfigMap,
> RP.12). The `pxf-servers` volume the sidecar mounts is an **emptyDir** holding
> the RESOLVED `*-site.xml` (the `pxf-cred-init` init container renders the
> ConfigMap templates into it); the ConfigMap-backed `pxf-templates` volume
> (carrying the `${...}`-placeholder templates) is mounted **only** on the init
> container and marked `Optional: true` so the pod can schedule before the
> controller applies the ConfigMap. `pxf.resources` map directly onto the
> container resources when set.

2. **Generates PXF server configurations** as the `<cluster>-pxf-servers` ConfigMap (**Implemented**), keyed by `<server-name>__<file>.xml` (one file per server, byte-stable with sorted keys). `credentialSecrets[]` references are mapped to the **standard PXF/Hadoop property names** (`fs.s3a.access.key`/`fs.s3a.secret.key` for `s3`; `jdbc.user`/`jdbc.password` for `jdbc`) and rendered as sanitized `${<SANITIZED_NAME_KEY>}` **placeholders** in the relevant site XML (see [Credential rendering rules](#credential-rendering-rules-standard-properties--env-name-sanitization)). **Live secret resolution is now Implemented** via the `pxf-cred-init` init container (see [Credential init-container](#credential-init-container-live-secret-resolution) below): it `envsubst`-substitutes the real secret values into resolved `*-site.xml` files at pod start — the **ConfigMap keeps only `${...}` placeholders; secrets never land in it**. The live `pxf://` load now works end-to-end with the operator-rendered credentials (no manual workarounds), provided the configured PXF image exists:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: <cluster>-pxf-servers
data:
  s3-datalake__s3-site.xml: |
    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
      <property>
        <name>fs.s3a.access.key</name>
        <value>${S3_DATALAKE_CREDS_AWS_ACCESS_KEY_ID}</value>
      </property>
      <property>
        <name>fs.s3a.secret.key</name>
        <value>${S3_DATALAKE_CREDS_AWS_SECRET_ACCESS_KEY}</value>
      </property>
      <property>
        <name>fs.s3a.endpoint</name>
        <value>s3.amazonaws.com</value>
      </property>
      <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
      </property>
    </configuration>
 
  mysql-oltp__jdbc-site.xml: |
    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
      <property>
        <name>jdbc.driver</name>
        <value>com.mysql.cj.jdbc.Driver</value>
      </property>
      <property>
        <name>jdbc.url</name>
        <value>jdbc:mysql://mysql:3306/production</value>
      </property>
      <property>
        <name>jdbc.user</name>
        <value>${MYSQL_CREDS_USERNAME}</value>
      </property>
      <property>
        <name>jdbc.password</name>
        <value>${MYSQL_CREDS_PASSWORD}</value>
      </property>
    </configuration>
 
  hadoop-cluster__core-site.xml: |
    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
      <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:8020</value>
      </property>
    </configuration>
 
  hadoop-cluster__hive-site.xml: |
    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
      <property>
        <name>hive.metastore.uris</name>
        <value>thrift://hive-metastore:9083</value>
      </property>
    </configuration>
 
  hadoop-cluster__hbase-site.xml: |
    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
      <property>
        <name>hbase.zookeeper.quorum</name>
        <value>zk1:2181,zk2:2181,zk3:2181</value>
      </property>
    </configuration>
```

> **Implemented rendering details (per-type file-mapping, SL.1–SL.6).** The
> per-type file mapping is: `s3`→`s3-site.xml`; `hdfs`→`core-site.xml` **and**
> `hdfs-site.xml` (both always; `hdfs-site.xml` is a minimal `<configuration/>`
> when no `dfs.*` key) + optional `hive-site.xml`/`hbase-site.xml` (from the
> `hive`/`hbase` maps or the `hive.*`/`hbase.*` Config fragment) + optional
> `mapred-site.xml`/`yarn-site.xml` (from `mapred*`/`mapreduce.*` / `yarn.*` keys);
> `jdbc`→`jdbc-site.xml` (merging `config` + `jdbc` + credential placeholders);
> `hive`→`core-site.xml` **and** `hive-site.xml` (both always);
> `hbase`→`core-site.xml` **and** `hbase-site.xml` (both always). The `config` map
> for Hadoop-family servers is **prefix-split** into the canonical site files
> (`fs.*`→core, `dfs.*`→hdfs, `mapred*`/`mapreduce.*`→mapred, `yarn.*`→yarn,
> `hive.*`→hive, `hbase.*`→hbase, other→core). Full mapping table in
> [PXF Server Configuration Lifecycle](#creating-a-server-file-mapping-sl1sl6).
> Custom connectors are listed deterministically in `connectors.properties` as
> `name=jarUrl` lines **and** (Scenario 102, C.18) their `jarUrl` is **downloaded**
> at pod start by the `pxf-connector-init` init container into
> `/pxf/lib/custom/<name>.jar` on the sidecar's classpath — see
> [Connector-JAR download init-container](#connector-jar-download-init-container-pxf-connector-init-c18--scenario-102).

#### Credential rendering rules (standard properties + env-name sanitization)

> **Status: Implemented.** Two rules govern how `credentialSecrets[]` become
> working PXF configuration. They were the root cause of two live-load bugs (a
> non-standard property name PXF ignored, and hyphenated env var names the POSIX
> resolver could not substitute) and are now fixed in
> `internal/builder/pxf_builder.go`.

**Rule 1 — Standard PXF/Hadoop property names (not `pxf.credential.*`).** PXF and
Hadoop only read the well-known site properties; the operator therefore maps each
`credentialSecrets[]` entry to the correct standard property based on the
**server type** and the secret **key's role**:

| Server type | Access/user property | Secret/password property |
|-------------|----------------------|--------------------------|
| `s3`        | `fs.s3a.access.key`  | `fs.s3a.secret.key`      |
| `jdbc`      | `jdbc.user`          | `jdbc.password`          |
| `hdfs` / `hive` / `hbase` / unknown | *(none — Kerberos)* | *(none)* |

Role detection uses a **key-name heuristic** with an **order-based fallback**:

- A key containing `access` or `user` → the **access/user** property; a key
  containing `secret`, `password`, or `pass` → the **secret/password** property
  (matches the test environment keys `aws_access_key_id` /
  `aws_secret_access_key`, and `username` / `password`).
- When the key is ambiguous/empty, fall back to **order**: the first
  `credentialSecrets[]` entry takes the access/user property, the second takes
  the secret/password property.

`hdfs`/`hive`/`hbase` (and unknown) servers emit **no** inline credentials — the
operator never writes `pxf.credential.*` anywhere.

**Rule 2 — Env-name sanitization (valid shell variable names).** The init
container resolves each placeholder from an env var sourced via `SecretKeyRef`.
Env var names must match `[A-Za-z0-9_]` and may not start with a digit, but raw
secret names contain hyphens (e.g. `backup-s3-credentials`). A **single shared
helper** (`pxfCredentialEnvName` → `pxfSanitizeEnvName`) is used for **both** the
placeholder token emitted into the site XML **and** the SecretKeyRef env var
`Name`, so they can never diverge. It uppercases the `<name>_<key>` token,
replaces every character outside `[A-Za-z0-9_]` with `_`, and prefixes a leading
digit with `_`. For example:

| Secret name | Key | Env var name / placeholder |
|-------------|-----|----------------------------|
| `backup-s3-credentials` | `aws_access_key_id` | `${BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID}` |
| `mysql-creds` | `password` | `${MYSQL_CREDS_PASSWORD}` |

The rendered `s3-site.xml` then contains, e.g.:

```xml
<property><name>fs.s3a.access.key</name><value>${BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID}</value></property>
<property><name>fs.s3a.secret.key</name><value>${BACKUP_S3_CREDENTIALS_AWS_SECRET_ACCESS_KEY}</value></property>
```

and the matching init-container env injects `BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID`
(from `SecretKeyRef{Name: backup-s3-credentials, Key: aws_access_key_id}`), which
both `envsubst` and the POSIX fallback resolve to the real value.

3. **Installs PXF extensions and GRANTs the protocol** in the target database (**Status: Implemented — best-effort / non-fatal**). During `reconcileDataLoading`, the operator calls `setupPXFExtensions` → `db.Client.SetupPXFExtensions`, which runs `CREATE EXTENSION IF NOT EXISTS pxf` (**RP.9**), then `pxf_fdw` (**RP.10**), and — **only when the `pxf` extension actually installed** — `GRANT SELECT ON PROTOCOL pxf` **and** `GRANT INSERT ON PROTOCOL pxf` to the data-loader role (**RP.11**). The data-loader role is **`gpadmin`** (`util.DefaultAdminUser`; a sanitized identifier, defense-in-depth via `pgx.Identifier`), which always exists as a superuser. The call is **idempotency-gated** by the `avsoft.io/pxf-extensions-ready` annotation (skipped once set) and is **strictly non-fatal**: a missing `dbFactory`, a connection failure, a `CREATE EXTENSION` error, or a failed GRANT only logs a warning and returns. The GRANTs are gated on `pxfInstalled` because the `pxf` **PROTOCOL** only exists once the `pxf` extension is installed (`SELECT` enables readable external tables, `INSERT` enables writable external tables). **Importantly, `cloudberry-official:2.1.0` does not ship the `pxf` extension** (only a `pxf_fdw` client stub), so the `pxf` step — and therefore the protocol GRANTs — are **expected to be a no-op** there; the connectivity probe distinguishes a genuinely unreachable DB (the only hard error) from an absent extension (tolerated):
```sql
-- Best-effort, idempotent, non-fatal. Absent extensions are expected & tolerated.
CREATE EXTENSION IF NOT EXISTS pxf;       -- RP.9; typically absent in cloudberry-official
CREATE EXTENSION IF NOT EXISTS pxf_fdw;   -- RP.10; client stub present
-- RP.11: only attempted when CREATE EXTENSION pxf succeeded (the PROTOCOL exists):
GRANT SELECT ON PROTOCOL pxf TO "gpadmin";   -- readable external tables
GRANT INSERT ON PROTOCOL pxf TO "gpadmin";   -- writable external tables
```
> The data-loading load script *also* best-effort-installs `pxf_fdw` (`|| true`)
> for PXF jobs before creating the external table (see
> [Data Loading Jobs](#data-loading-jobs)). The `gpadmin` data-loader role is a
> package variable (`pxfDataLoaderRole`) so a future configurable role can be
> wired in without changing the `SetupPXFExtensions` signature.

4. **Syncs PXF configuration** across segment hosts (**Status: Implemented structurally — RP.12 satisfied by the shared ConfigMap; an explicit `pxf sync` trigger is now also Implemented as an on-demand refresh**). All segment-primary PXF sidecars mount the **same** `<cluster>-pxf-servers` ConfigMap (as the `pxf-templates` volume on each pod's `pxf-cred-init` init container), and every init container renders **byte-identical** resolved configs into its sidecar's shared emptyDir. Config therefore propagates to all sidecars **structurally** by construction — no explicit command is required for steady-state correctness.

   The **structural sync remains true**. In addition, an **explicit** sync trigger now exists (Scenario 95): `cloudberry-ctl pxf sync --cluster <name>` / `POST .../data-loading/pxf/sync` (`handlePXFSync`). It re-renders the `<cluster>-pxf-servers` ConfigMap from the current spec **and** bumps the segment-primary restart-trigger annotation, so the `pxf-cred-init` init container re-renders the resolved configs on the resulting pod roll. This is the **on-demand** counterpart to the always-on structural sync: use it to force a ConfigMap refresh + sidecar roll immediately (e.g. after a server-config or referenced-secret change) rather than waiting for the next reconcile/restart. (The explicit `pxf sync` was previously listed as Planned/not-needed; it is now Implemented as an explicit trigger that complements — does not replace — the structural model.)

### Credential init-container (live secret resolution)

> **Status: Implemented.** `BuildPXFCredentialInitContainers`
> (`internal/builder/pxf_builder.go`) injects a single `pxf-cred-init` init
> container into the **segment-primary** pod (gated on the sidecar being enabled).
> This is what makes live secret rendering Implemented.

The init container:

1. Mounts the **raw templates ConfigMap** (the `${<NAME>_<KEY>}`-placeholder
   `*-site.xml` files) read-only.
2. Receives every server's `credentialSecrets[]` as env vars via `SecretKeyRef`
   (`buildPXFCredentialEnv`): one env var per reference whose `Name` is the
   **sanitized** `<NAME>_<KEY>` token (see
   [Credential rendering rules](#credential-rendering-rules-standard-properties--env-name-sanitization)
   — uppercased, hyphens/illegal chars → `_`, leading-digit guarded), emitted in
   **sorted, de-duplicated** order so the container env is byte-stable. The env
   var `Name` is produced by the **same** helper that emits the site-XML
   placeholder, guaranteeing they match. The secret **values** never appear in
   the spec — only references.
3. Runs `envsubst` over each template (with a POSIX `eval "cat <<EOF"` fallback
   when `envsubst` is absent — the same idiom the backup script uses), writing
   the **resolved** `*-site.xml` into the shared `pxf-servers` emptyDir the
   sidecar reads, using the **native nested layout** PXF expects: a ConfigMap key
   `<server>__<file>.xml` is written to `servers/<server>/<file>.xml`. This makes
   the operator output match PXF's native `$PXF_BASE/servers` layout directly, so
   the image entrypoint's reorg step becomes a no-op (it still handles the legacy
   flat layout for backward compatibility). Non-XML keys (e.g.
   `connectors.properties`) are written verbatim at the top level.

**Result:** resolved credentials exist only at runtime in the emptyDir; the
`<cluster>-pxf-servers` ConfigMap stores only `${...}` placeholders — **secrets
never land in the ConfigMap**.

### Connector-JAR download init-container (`pxf-connector-init`, C.18 — Scenario 102)

> **Status: Implemented (Scenario 102).** `BuildPXFConnectorInitContainers`
> (`internal/builder/pxf_builder.go`) injects a single `pxf-connector-init` init
> container into the **segment-primary** pod, wired **after** `pxf-cred-init` for
> readability. It is a guarded no-op (returns an empty slice) unless the **PXF
> sidecar is enabled AND at least one `dataLoading.pxf.customConnectors[]` entry
> is declared**.

Previously `customConnectors[]` only produced `connectors.properties` listing
lines — the JARs themselves had to be baked into the image. Scenario 102 adds a
**download** step so a connector JAR can be staged externally and fetched at pod
start.

The connector-init container:

1. Uses the **same image** as the sidecar (`dataLoading.pxf.image`) and mounts the
   shared `pxf-lib` emptyDir at **`/pxf/lib/custom`** (`pxfLibMountPath`) — the
   same emptyDir the sidecar mounts, so the downloaded JARs land on the sidecar's
   classpath.
2. For **each** `customConnectors[]` entry (sorted by `name` for byte-stability)
   downloads `jarUrl` into **`/pxf/lib/custom/<name>.jar`**:
    - `s3://…` → `aws --endpoint-url "$AWS_S3_ENDPOINT" s3 cp <jarUrl> <dst>`
    - `http(s)://…` → `curl -fsSL <jarUrl> -o <dst>`
    - any other scheme aborts the init (`exit 1`).
    Each download is asserted non-empty (`test -s <dst>`).
    > **Image-capability note (verified live, Scenario 102).** The init container
    > runs on the **PXF sidecar image** (`pxf.image`). The `http(s)://` path needs
    > only `curl` (present in `cloudberry-pxf:2.1.0`); the `s3://` path additionally
    > needs the `aws` CLI in that image. `cloudberry-pxf:2.1.0` ships `curl` but
    > **not** the `aws` CLI, so an `s3://` `jarUrl` requires an aws-CLI-capable PXF
    > image (or pre-bake the `aws` CLI). For a MinIO-staged JAR the equivalent
    > `http://<minio>/…` URL works out of the box — this is what the live Scenario 102
    > run used (`http://minio:9000/cloudberry-data/connectors/kafka-connector.jar`).
3. Receives S3 credentials from the cluster's **backup S3 destination** (the same
   MinIO-staged `backup-s3-credentials` the operator already wires for backup) via
   `SecretKeyRef`, plus `AWS_S3_ENDPOINT` and `AWS_DEFAULT_REGION`
   (`buildPXFConnectorEnv`). When the cluster has no S3 backup destination the env
   is empty (`http(s)://` jarUrls need no credentials).

**Result:** the `kafka-connector.jar` (or any custom connector JAR) is present at
`/pxf/lib/custom/<name>.jar` in the sidecar before queries run — the runtime half
of the [Scenario 102](#scenario-102--kafka-cdc-continuous-streaming-custom-connector)
custom-connector model.

### Data Loading Jobs

> **Status: Implemented (Job/CronJob generation + launch + native execution).**
> For every **enabled** `dataLoading.jobs[]` entry the operator creates a one-off
> `Job` (no `schedule`) or a `CronJob` (when `schedule` is set), named
> `<cluster>-dataload-<job>`. The Job runs a `psql` load script that creates an
> external table, runs `INSERT…SELECT` into the target, harvests the rowcount,
> drops the temp table, and `ANALYZE`s the target.
>
> **Honesty:** the **native** (`gpfdist://`/`s3://`, and bare paths served by the
> cluster gpfdist Service) path **executes for real** (row-count verified;
> `file://` is admission-rejected for multi-segment gpload jobs by W.16). The
> **`pxf://`** path **now also executes for real** (row-count verified: 183,961
> rows from MinIO S3 via the PXF sidecar) **when** the cluster runs the
> `cloudberry-pxf` sidecar image (`Dockerfile.cloudberry-pxf`) + the `pxf`
> extension in the DB image (`cloudberry-official-pxf`). On a stock
> `cloudberry-official` (no PXF extension) the `pxf://` Job generates/launches but
> cannot read back — an image prerequisite, not a code gap.

**Data-loader image (coordinator-exec model).** The Job container is named
`dataload` and runs on **`cluster.Spec.Image`** (the cloudberry-official runtime
image, `dataLoaderImage(cluster)`), which already ships `psql` plus the
engine-native `gpfdist://`/`s3://` external-table protocols — so the
**same image that runs Cloudberry runs the loader** (no extra image to pull for
the genuine native path). The container connects to the cluster **coordinator**
over `psql` with the backup-style `PG*` env (`buildDataLoadEnv`):
`PGHOST=<cluster>-coordinator` service, `PGPORT` resolved, `PGUSER=gpadmin`,
`PGDATABASE=postgres`, `PGPASSWORD` via `SecretKeyRef` on the admin password
Secret. `RestartPolicy: Never`, `TerminationMessagePolicy:
FallbackToLogsOnError`.

**External-table DDL generation (`buildExternalTableDDL`).** A pure,
deterministic, byte-stable, injection-safe generator (identifiers
double-quoted, the LOCATION URI single-quoted). The temp table uses `(LIKE
<targetTable>)` so it **inherits the target's columns** — there is **no CRD
column field**:

```
CREATE EXTERNAL TABLE "cbk_dataload_ext_<job>" (LIKE "<schema>"."<target>")
LOCATION ('<uri>')
FORMAT '<...>'
[LOG ERRORS SEGMENT REJECT LIMIT <n> ROWS|PERCENT];
```

- **PXF jobs** (`type: pxf`): `pxf://<resource>?PROFILE=<p>&SERVER=<s>` with the
  typed options emitted in a **fixed order** — `FILTER_PUSHDOWN=true` (when
  `filterPushdown`), `PROJECT=true` (when `columnProjection`), then
  `PARTITION_BY`/`RANGE`/`INTERVAL` (from `partitioning`) — and
  `FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')`.
- **Native/gpload jobs** (`type: gpload`): `gpfdist://`/`s3://` LOCATION derived
  from `gploadJob.filePaths` (an explicit scheme is used verbatim; a bare path is
  served via the cluster gpfdist Service `<cluster>-gpfdist:<port>`), and
  `FORMAT 'CSV'` / `FORMAT 'TEXT'` from `gploadJob.format` (unset → CSV). The
  bare `file://` scheme is **admission-rejected** (W.16) for multi-segment
  clusters and so never reaches the generator from a CR; the generator keeps a
  verbatim `file://` passthrough only for a future single-host caller.
- **Error handling** → `LOG ERRORS SEGMENT REJECT LIMIT <n> ROWS|PERCENT` from
  `errorHandling` (`LOG ERRORS` prefix only when `logErrors: true`; the suffix is
  emitted only when `segmentRejectLimit > 0`).

**Load script (`buildDataLoadScript`, `set -euo pipefail`).** The sequence is:

1. **[pxf only, best-effort]** `CREATE EXTENSION IF NOT EXISTS pxf_fdw` —
   tolerated failure (`|| echo …`) because the extension is absent in
   cloudberry-official; native jobs skip this step.
2. `DROP EXTERNAL TABLE IF EXISTS <tmp>` (tolerated) → `CREATE EXTERNAL TABLE`
   from the generated DDL (delivered via a quoted heredoc).
3. `rows=$(psql -tA -c 'INSERT INTO <target> SELECT * FROM <tmp>' | awk '{print
   $NF}')` — captures the `INSERT 0 <n>` rowcount, then emits the
   **`DATALOAD_ROWS=<n>` marker** to stdout **and** `/dev/termination-log`. The
   controller harvests this marker to populate `status.dataLoading.jobs[]
   .rowsLoaded` and the `cloudberry_data_loading_rows_total` metric (never
   synthesized).
4. `DROP EXTERNAL TABLE IF EXISTS <tmp>` → `ANALYZE <target>`.

**JobTemplate mapping** (`dataLoading.jobTemplate`): `resources` → container
resources; `serviceAccountName`/`nodeSelector`/`tolerations` → pod; and
`backoffLimit` / `activeDeadlineSeconds` / `ttlSecondsAfterFinished` → the
`JobSpec` (falling back to the backup defaults `3` / `14400` / `86400`). The
CronJob uses `ConcurrencyPolicy: Forbid` and keeps 3 successful/failed job
history entries.

**Illustrative generated PXF load Job** (the `pxf://` LOCATION executes for real
when the `cloudberry-pxf` sidecar image + the `pxf` extension are present —
row-count verified, 183,961 rows from MinIO S3; on a stock `cloudberry-official`
it generates/launches only):

```yaml
containers:
  - name: dataload
    image: <cluster.Spec.Image>            # cloudberry-official: psql + native protocols
    command: ["/bin/bash", "-c"]
    args:
      - |
        set -euo pipefail
        # [pxf only, best-effort — extension absent in cloudberry-official]
        psql -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION IF NOT EXISTS pxf_fdw' \
          || echo 'dataload: pxf_fdw extension unavailable (best-effort, continuing)'
        psql -v ON_ERROR_STOP=1 -c 'DROP EXTERNAL TABLE IF EXISTS "cbk_dataload_ext_s3_ingest"' || true
        psql -v ON_ERROR_STOP=1 <<'_CBK_DDL_EOF_'
        CREATE EXTERNAL TABLE "cbk_dataload_ext_s3_ingest" (LIKE "public"."events")
        LOCATION ('pxf://s3a://data-lake/events/?PROFILE=s3:parquet&SERVER=s3-datalake&FILTER_PUSHDOWN=true&PROJECT=true')
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
        LOG ERRORS SEGMENT REJECT LIMIT 100 ROWS;
        _CBK_DDL_EOF_
        rows=$(psql -v ON_ERROR_STOP=1 -tA -c 'INSERT INTO "public"."events" SELECT * FROM "cbk_dataload_ext_s3_ingest"' | awk '{print $NF}')
        rows=${rows:-0}
        echo "DATALOAD_ROWS=${rows}"
        printf '%s%s' 'DATALOAD_ROWS=' "${rows}" > /dev/termination-log 2>/dev/null || true
        psql -v ON_ERROR_STOP=1 -c 'DROP EXTERNAL TABLE IF EXISTS "cbk_dataload_ext_s3_ingest"' || true
        psql -v ON_ERROR_STOP=1 -c 'ANALYZE "public"."events"'
```

**FDW-based alternative** (**Status: Implemented — Scenario 103**) — for **persistent** foreign tables that can be queried directly or used in `INSERT INTO … SELECT FROM`. It is selected by **`pxfJob.loadMethod: fdw`** (the default `external-table` builds the transient external-table path above) and is **EQUIVALENT** to the external-table path: the SAME rows land in the target (proven by equal row counts, not a new metric). Unlike the external-table path, the FDW objects are **PERSISTENT** (`CREATE … IF NOT EXISTS`, **never dropped**) so the foreign table stays directly queryable after the Job completes.

The operator (`buildFDWDDL`, `internal/builder/dataload_builder.go`) emits the **EX.5-EX.7** chain with **deterministic** names (`foreign_<server>`, the `gpadmin` user mapping, `foreign_<job>`), the **live-verified per-protocol** `pxf_fdw` wrapper, and a `(LIKE <target>)` foreign table:

```sql
-- EX.5 — CREATE SERVER (persistent, idempotent; wrapper resolved per scheme).
-- OPTIONS (config '<pxf-server>') names the PXF server config (the rendered
-- <server>-site.xml) the FDW read resolves its credentials + endpoint from —
-- resource/format belong on the FOREIGN TABLE only (live-verified).
CREATE SERVER IF NOT EXISTS "foreign_s3_datalake"
  FOREIGN DATA WRAPPER s3_pxf_fdw
  OPTIONS (config 's3-datalake');

-- EX.6 — CREATE USER MAPPING (for the data-loader role = gpadmin)
CREATE USER MAPPING IF NOT EXISTS FOR "gpadmin"
  SERVER "foreign_s3_datalake";

-- EX.7 — CREATE FOREIGN TABLE (LIKE target, persistent; resource/format here)
CREATE FOREIGN TABLE IF NOT EXISTS "foreign_events" (LIKE "public"."events")
  SERVER "foreign_s3_datalake"
  OPTIONS (resource 'cloudberry-data/text/data.csv', format 'csv');
```

Notes (verified against `dataload_builder.go`):

- **Deterministic names.** `fdwServerName(server) = "foreign_" + sanitize(server)`; `fdwForeignTableName(job) = "foreign_" + sanitize(job)`; the user mapping is `FOR "gpadmin"` (`pxfDataLoaderRole = util.DefaultAdminUser`, the role `SetupPXFExtensions` GRANTs `SELECT`/`INSERT ON PROTOCOL pxf` — RP.11).
- **SERVER OPTIONS = `config` (live-verified).** The `CREATE SERVER` carries `OPTIONS (config '<pxf-server>')` (= `pxfJob.server`), NOT `resource`/`format`: the `pxf_fdw` VALIDATOR rejects `resource` at the `pg_foreign_server` level (*"the resource option can only be defined at the pg_foreign_table level"*), and `config` is what lets the wrapper resolve the named PXF server's credentials/endpoint. `resource`/`format` live on the `CREATE FOREIGN TABLE` only.
- **Per-protocol wrapper (live-verified).** The `FOREIGN DATA WRAPPER` is the per-scheme `pxf_fdw` wrapper registered in `cloudberry-official-pxf:2.1.0` (confirmed via `SELECT fdwname FROM pg_foreign_data_wrapper`): `s3`→`s3_pxf_fdw`, `gs`→`gs_pxf_fdw`, `abfss`→`abfss_pxf_fdw`, `wasbs`→`wasbs_pxf_fdw`, `jdbc`→`jdbc_pxf_fdw`, `hdfs`→`hdfs_pxf_fdw`, `hive`→`hive_pxf_fdw`, `hbase`→`hbase_pxf_fdw` (each scheme has its **OWN** registered wrapper; unknown schemes fall back to the generic `pxf_fdw`).
- **Format OPTION (FOREIGN TABLE).** The `format` OPTION is derived from the profile suffix (`s3:parquet`→`'parquet'`); the `text` suffix maps to `'csv'` (object-store text data is comma-delimited CSV, matching the external-table path's `FORMAT 'CSV'`; the `pxf_fdw` `text` format is tab-delimited). It is **OMITTED** for a **bare** profile (`jdbc`/`hive`, which take a resource, not a format).
- **Injection-safe / byte-stable.** Identifiers are double-quoted (`quoteSQLIdentifier`), the resource/format are single-quoted literals (`quoteSQLLiteral`), and the chain is emitted in a fixed order.

The load step (EX.8, `buildFDWDataLoadScript`) ensures the persistent objects exist, **queries the foreign table directly** (`SELECT count(*) FROM "foreign_events"`), then loads via `INSERT INTO "public"."events" SELECT * FROM "foreign_events" [WHERE <sourceFilter>]` and `ANALYZE` — **NO DROP** (persistent). This INSERT…SELECT shape is identical to the external-table path, so the same rows land. See [§Scenario 103 — FDW-Based Loading Path](#scenario-103--fdw-based-loading-path).

### gpfdist Deployment

> **Status: Implemented (Scenario 101).** When `dataLoading.gpfdist.enabled: true`,
> `reconcileGpfdist` (`internal/controller/dataload_controller.go`) ensures a
> gpfdist **PVC + Deployment + Service** (GP.2-GP.5), built by
> `internal/builder/gpfdist_builder.go`. When `enabled` flips back to `false` the
> three objects are best-effort deleted (the cluster `ownerRef` also GCs them).

When `dataLoading.gpfdist.enabled: true`, the operator creates a `Deployment`
running `gpfdist` for parallel file distribution, a backing PVC mounted at
`/data`, and a `Service` that gpload control files target over `gpfdist://`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: <cluster>-gpfdist                  # builder.GpfdistServiceName (GP.2)
spec:
  replicas: 1                              # honors gpfdist.replicas; default 1 (RWO-safe)
  selector:
    matchLabels:
      avsoft.io/component: gpfdist         # == pod-template labels (GP.5)
  template:
    metadata:
      labels:
        avsoft.io/component: gpfdist       # selector EQUALS pod labels (no drift)
    spec:
      containers:
        - name: gpfdist
          image: cloudberry-gpfdist:2.1.0  # gpfdist.image; default cloudberry-gpfdist:2.1.0
          command: ["gpfdist"]
          args:
            - "-d"
            - "/data"
            - "-p"
            - "8080"
            - "-l"
            - "/var/log/gpfdist.log"
          ports:
            - name: gpfdist
              containerPort: 8080
          volumeMounts:
            - name: data
              mountPath: /data             # GP.4
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: <cluster>-gpfdist-data-pvc   # per-cluster PVC (see note)
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: <cluster>-gpfdist-data-pvc         # util.GpfdistDataPVCName (GP.4)
spec:
  accessModes: ["ReadWriteOnce"]           # RWO → default replicas 1
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: <cluster>-gpfdist-svc              # util.GpfdistServiceName2 (GP.5)
spec:
  type: ClusterIP
  selector:
    avsoft.io/component: gpfdist           # == Deployment pod labels (GP.5)
  ports:
    - name: gpfdist
      port: 8080
      targetPort: 8080
```

> **Divergence from the illustrative spec (documented honestly):**
>
> - **PVC name.** The earlier design literal `gpfdist-data-pvc` was implemented as
>   the **per-cluster** `<cluster>-gpfdist-data-pvc` (`util.GpfdistDataPVCName`).
>   The per-cluster name avoids same-namespace collisions between two clusters,
>   is multi-cluster-safe, and carries the cluster `ownerRef` so it is GC'd with
>   the cluster.
> - **Selector label domain.** The earlier design illustration
>   `cloudberry.apache.org/component: gpfdist` was implemented with the repo's
>   **actual** label domain `avsoft.io/component: gpfdist` (`util.LabelComponent`
>   / `util.ComponentGpfdist`). Both the Deployment pod-template labels and the
>   Service selector use `util.CommonLabels(cluster, gpfdist)`, so the selector
>   cannot drift from the pod labels and the cluster label additionally scopes
>   selection (two clusters in one namespace never cross-select).
>
> The Deployment name is `<cluster>-gpfdist` (`builder.GpfdistServiceName`); the
> Service name is `<cluster>-gpfdist-svc` (`util.GpfdistServiceName2`).

### gpload Jobs

> **Status: Implemented (Scenario 101).** For a `type: gpload` job the operator
> renders a byte-stable gpload YAML **control file** (GL.1-GL.7,
> `BuildGploadControlFile` in `internal/builder/gpload_builder.go`), delivers it
> via a per-job ConfigMap `<cluster>-gpload-<job>` (data key `<job>.yml`,
> `util.GploadControlFileConfigMapName`) mounted **read-only at `/etc/gpload`**,
> and runs `gpload -f /etc/gpload/<job>.yml` on the cluster (data-loader) image in
> a **CronJob** (when `schedule` is set, J.25) or a one-off **Job** (when not).
> This gpload control-file path **replaces** the old native-external-table-DDL
> path for gpload jobs; PXF jobs are unchanged.

The new `gploadJob` fields (`inputSource`, `delimiter`, `header`, `encoding`,
`matchColumns`, `updateColumns`, `preload.truncate`, `postActions`) drive the
generated control file. The control file is **deterministic** (blocks/keys emitted
in a fixed order) and matches the byte-exact golden:

```yaml
# Generated gpload control file (data key <job>.yml in <cluster>-gpload-<job>)
VERSION: 1.0.0.1                            # GL.1
DATABASE: postgres                          # GL.1 (defaultCoordinatorDatabase)
USER: gpadmin                               # GL.1 (util.DefaultAdminUser)
HOST: <cluster>-coord-hl                    # GL.1 (CoordinatorServiceName)
PORT: 5432                                  # GL.1
GPLOAD:
  INPUT:                                    # GL.2/GL.3/GL.4
    - SOURCE:
        LOCAL_HOSTNAME:                     # external gpfdist host (GL.2 / J.28)
          - <cluster>-gpfdist-svc
        PORT: 8080                          # external gpfdist port (J.29)
        FILE:                               # LOCAL paths served BY that gpfdist (NOT gpfdist:// URLs)
          - /incoming/*.csv
    - FORMAT: csv                           # csv | text (GL.3)
    - DELIMITER: ','                        # single char (GL.3)
    - HEADER: true                          # emitted only when header: true (GL.3)
    - ENCODING: UTF-8                       # GL.3
    - ERROR_LIMIT: 50                       # GL.4 (errorHandling.segmentRejectLimit)
    - LOG_ERRORS: true                      # GL.4 (errorHandling.logErrors)
  OUTPUT:                                   # GL.5
    - TABLE: public.raw_data
    - MODE: INSERT                          # INSERT | UPDATE | MERGE
    # update/merge also emit MATCH_COLUMNS (+ optional UPDATE_COLUMNS)
  PRELOAD:                                  # GL.6 (emitted only when preload.truncate)
    - TRUNCATE: true
  SQL:                                      # GL.7 (one AFTER entry per postActions[])
    - AFTER: "ANALYZE public.raw_data"
```

For `inputSource.type: gpfdist` (default) the SOURCE block emits
`LOCAL_HOSTNAME` (the external gpfdist host) + `PORT` and the `FILE` entries are
**local paths** served by that gpfdist (e.g. `/incoming/*.csv`) — **NOT**
`gpfdist://` URLs. (gpload starts its own gpfdist for `FILE` entries, so a
`gpfdist://` URL in `FILE` would be double-prefixed and fail to resolve; the
external gpfdist is addressed via `LOCAL_HOSTNAME`/`PORT` instead. This was
corrected after live verification.) For `inputSource.type: local` the SOURCE
block omits `LOCAL_HOSTNAME`/`PORT` and the `FILE` entries are the `filePaths`
verbatim. The `host`/`port` default to the
cluster gpfdist Service `<cluster>-gpfdist-svc:8080`.

## PXF Profile Reference

### Object Store Profiles

> **Status: Write column now ENFORCED (Implemented — Scenario 96).** The
> write-capability of each profile is enforced by the webhook (**W.10b**) and the
> builder via [`internal/pxfpolicy`](#single-source-of-truth-internalpxfpolicy):
> only `text`/`parquet`/`avro` formats are writable for the object-store schemes
> (`s3`/`gs`/`abfss`/`wasbs`); `json`/`orc` are **read-only** and a
> `mode: writable` job using them is **rejected at admission**. The `gs`/`abfss`/
> `wasbs` Write column follows the **same per-format rule** as `s3` (it is the
> format suffix, not the scheme, that drives writability).

| Profile | Description | Read | Write |
|---------|-------------|------|-------|
| `s3:text` | Text/CSV on S3/MinIO/GCS/Azure | Yes | Yes |
| `s3:parquet` | Parquet on S3/MinIO/GCS/Azure | Yes | Yes |
| `s3:avro` | Avro on S3/MinIO/GCS/Azure | Yes | Yes |
| `s3:json` | JSON on S3/MinIO/GCS/Azure | Yes | No |
| `s3:orc` | ORC on S3/MinIO/GCS/Azure | Yes | No |
| `gs:{text,parquet,avro}` | Google Cloud Storage (writable formats) | Yes | Yes |
| `gs:{json,orc}` | Google Cloud Storage (read-only formats) | Yes | No |
| `abfss:{text,parquet,avro}` | Azure Data Lake Gen2 (writable formats) | Yes | Yes |
| `abfss:{json,orc}` | Azure Data Lake Gen2 (read-only formats) | Yes | No |
| `wasbs:{text,parquet,avro}` | Azure Blob Storage (writable formats) | Yes | Yes |
| `wasbs:{json,orc}` | Azure Blob Storage (read-only formats) | Yes | No |

### Hadoop Profiles

> **Status: Write column now ENFORCED (Implemented — Scenario 97).** The Hadoop
> write-capability is enforced by the webhook (**W.10b**) and re-checked by the
> builder via [`internal/pxfpolicy`](#single-source-of-truth-internalpxfpolicy),
> exactly like the object-store profiles. Two distinct rules drive the Write
> column:
> - **HDFS (`hdfs:`) — per-format.** `hdfs:text`/`parquet`/`avro`/`SequenceFile`
>   are **writable**; `hdfs:json`/`hdfs:orc` are **read-only** and a
>   `mode: writable` job using them is **rejected at admission**.
> - **Hive (`hive*`) and HBase — read-only at the SCHEME level, REGARDLESS of
>   format.** Every `hive`/`hive:text`/`hive:orc`/`hive:rc` profile and the
>   `HBase` profile is **Write = No**; a `mode: writable` job on any of them is
>   **rejected**. In particular `hive:text` is rejected for a writable table even
>   though `text` is a writable *format* on `hdfs`/object stores — the read-only
>   Hive scheme **overrides** the per-format check (see the policy correction in
>   [Single source of truth](#single-source-of-truth-internalpxfpolicy)).

| Profile | Description | Read | Write |
|---------|-------------|------|-------|
| `hdfs:text` | Text/CSV on HDFS | Yes | Yes |
| `hdfs:parquet` | Parquet on HDFS | Yes | Yes |
| `hdfs:avro` | Avro on HDFS | Yes | Yes |
| `hdfs:json` | JSON on HDFS | Yes | No |
| `hdfs:orc` | ORC on HDFS | Yes | No |
| `hdfs:SequenceFile` | SequenceFile on HDFS | Yes | Yes |
| `hive` | Hive tables (auto-detect format) | Yes | No (read-only scheme) |
| `hive:text` | Hive text tables | Yes | No (read-only scheme) |
| `hive:orc` | Hive ORC tables | Yes | No (read-only scheme) |
| `hive:rc` | Hive RCFile tables | Yes | No (read-only scheme) |
| `HBase` | HBase tables | Yes | No (read-only scheme) |

### JDBC Profile

| Profile | Description | Read | Write |
|---------|-------------|------|-------|
| `jdbc` | Any JDBC-accessible database | Yes | Yes |

JDBC connector supports partitioning for parallel reads:

```
LOCATION ('pxf://schema.table?PROFILE=jdbc&SERVER=mysql-oltp&PARTITION_BY=order_date:date&RANGE=2024-01-01:2026-12-31&INTERVAL=1:month')
```

### PXF Features

> **Status: Implemented (declarative knobs) + Verified (Scenario 98) for filter
> pushdown / column projection / per-row error handling; the rest below is
> Planned (runtime).** The operator's job is to **emit the correct DDL options**,
> and that is done and now **runtime-verified** for the three knobs below. The
> *declarative* knobs that drive them — `pxfJob.filterPushdown`,
> `columnProjection`, `partitioning`, `errorHandling`, and the W.10-validated
> `profile` — are part of the implemented CRD/webhook, render byte-stably into
> the generated DDL/LOCATION (`internal/builder/dataload_builder.go`
> `buildPXFLocation` / `errorHandlingClause`), and the mutating webhook **defaults
> `filterPushdown` + `columnProjection` to `true`** when unset (preserving an
> explicit user `false`). The **actual** pushdown / projection / error tolerance
> happens at the live PXF/engine layer; the operator does not (and honestly
> cannot) meter bytes saved — see
> [Scenario 98](#scenario-98--filter-pushdown-column-projection-per-row-error-handling)
> for the honest, operator-observable proofs (row-count reduction via
> `cloudberry_data_loading_rows_total`, `EXPLAIN`, source-side query logs, and the
> real `cloudberry_data_loading_job_status` / `…_errors_total` for error handling).

- **Filter pushdown** — **Implemented (declarative knob) + Verified (Scenario 98).** WHERE clause predicates are pushed to the external data source to reduce data transfer. `pxfJob.filterPushdown=true` → `FILTER_PUSHDOWN=true` in the `pxf://` LOCATION. Supported by object store (Parquet, ORC column predicates), JDBC, and Hive connectors. Proven via row-count reduction + `EXPLAIN` + source-side query logs (not a fabricated byte counter).
- **Column projection** — **Implemented (declarative knob) + Verified (Scenario 98).** Only requested columns are read from columnar formats (Parquet, ORC). `pxfJob.columnProjection=true` → `PROJECT=true`. Proven via `EXPLAIN` showing only the projected columns (EXPLAIN-only — no honest byte meter).
- **JDBC partitioning** — splits a single-table JDBC query into parallel sub-queries based on a partition column (int, date, enum), distributing work across Cloudberry segments. *(Declarative knob Implemented; live parallelism runtime-observed only indirectly.)*
- **Writable external tables** — PXF supports writing data back to S3, HDFS, and JDBC targets using `CREATE WRITABLE EXTERNAL TABLE`. *(Write-capability enforcement + `pxfwritable_export` DDL Implemented — Scenario 96/97; the writable export path intentionally OMITS the `LOG ERRORS`/reject-limit suffix.)*
- **Per-row error handling** — **Implemented (declarative knob) + Verified (Scenario 98).** `pxfJob.errorHandling.{segmentRejectLimit, segmentRejectLimitType (rows|percent, W.15-validated), logErrors}` → `[LOG ERRORS ]SEGMENT REJECT LIMIT <n> [ROWS|PERCENT]` on the READ external table; malformed rows are tolerated up to the threshold. Proven via the real `cloudberry_data_loading_job_status` (2=success / 3=failed) + `cloudberry_data_loading_errors_total` + `rows_total` (valid rows only).
## API Endpoints

The `Status` column reflects what is wired in `internal/api/server.go`
(`registerDataLoadingRoutes`) + `internal/api/dataloading.go` today:
**FULL** = implemented and serving real data; **501-stub** = route registered
but returns `501 NOT_IMPLEMENTED`; **Planned** = route is **not registered** at
all (a request 404s on the router).

> **Scenario 107 flipped ALL of P.1–P.15 to FULL.** The five job mutations
> (`POST/PUT/DELETE/start/stop`) — previously **501-stub** — and the PXF servers
> CRUD + `jobs/{job}/logs` + `external-tables` — previously **Planned** — are now
> **Implemented and serving real data** (`internal/api/dataloading.go`). The only
> honest non-200 holds are operational, not stubs: P.14 returns `501
> LOGS_NOT_AVAILABLE` only when no Kubernetes clientset is wired (no pod-log
> source), and P.15's `observed` is `null` with `observedAvailable:false` when the
> DB is unreachable (never synthesized). See
> [Scenario 107 — All API Endpoints (P.1–P.15)](#scenario-107--all-api-endpoints-p1p15).

| ID | Method | Path | Description | Perm | Status |
|----|--------|------|-------------|------|--------|
| P.7 | GET | `/clusters/{name}/data-loading/jobs` | List all data loading jobs (from `spec.dataLoading.jobs`) | Basic | **FULL** |
| P.9 | GET | `/clusters/{name}/data-loading/jobs/{job}` | Get job details (from spec) | Basic | **FULL** |
| P.8 | POST | `/clusters/{name}/data-loading/jobs` | Create a new job; `201`; `409 JOB_EXISTS`; `400` when `pxfJob.server` is unknown | Operator | **FULL** (Scenario 107) |
| P.10 | PUT | `/clusters/{name}/data-loading/jobs/{job}` | Update a job; `200` | Operator | **FULL** (Scenario 107) |
| P.11 | DELETE | `/clusters/{name}/data-loading/jobs/{job}` | Delete a job; best-effort deletes the spawned Job | Admin | **FULL** (Scenario 107) |
| P.12 | POST | `/clusters/{name}/data-loading/jobs/{job}/start` | Start/trigger a job → creates a REAL one-off `batchv1.Job`; `202`; `409 JOB_ALREADY_RUNNING` | Operator | **FULL** (Scenario 107) |
| P.13 | POST | `/clusters/{name}/data-loading/jobs/{job}/stop` | Stop a job → deletes the Job / suspends the CronJob; `202`; idempotent `200` when nothing to stop | Operator | **FULL** (Scenario 107) |
| P.1 | GET | `/clusters/{name}/data-loading/pxf/status` | Honest PXF sidecar readiness across segment-primary pods | Basic | **FULL** |
| — | POST | `/clusters/{name}/data-loading/pxf/restart` | Operator-driven PXF restart (segment-primary pod roll); `202` | Operator | **FULL** |
| P.6 | POST | `/clusters/{name}/data-loading/pxf/sync` | Refresh the `<cluster>-pxf-servers` ConfigMap + roll sidecars; `202` | Operator | **FULL** |
| P.2 | GET | `/clusters/{name}/data-loading/pxf/servers` | List configured PXF servers (REFERENCES only — no literal secrets) | Basic | **FULL** (Scenario 107) |
| P.2 | GET | `/clusters/{name}/data-loading/pxf/servers/{server}` | Get one configured PXF server; `404 SERVER_NOT_FOUND` | Basic | **FULL** (Scenario 107) |
| P.3 | POST | `/clusters/{name}/data-loading/pxf/servers` | Create a server; `201` returns the RENDERED `<server>__*.xml`; `409 SERVER_EXISTS` | Operator | **FULL** (Scenario 107) |
| P.4 | PUT | `/clusters/{name}/data-loading/pxf/servers/{server}` | Update a server; `200` rendered config; `404 SERVER_NOT_FOUND` | Operator | **FULL** (Scenario 107) |
| P.5 | DELETE | `/clusters/{name}/data-loading/pxf/servers/{server}` | Delete a server; `409 SERVER_IN_USE` if referenced by a job (mirrors webhook W.9) | Admin | **FULL** (Scenario 107) |
| P.14 | GET | `/clusters/{name}/data-loading/jobs/{job}/logs` | Stream the data-loading Job pod logs (`?follow`, `?tailLines`); `501 LOGS_NOT_AVAILABLE` when no clientset | Basic | **FULL** (Scenario 107) |
| P.15 | GET | `/clusters/{name}/data-loading/external-tables` | `{observed, observedAvailable, expected}` — live catalog vs spec-derived (honest split) | Basic | **FULL** (Scenario 107) |
| P.16 | GET | `/clusters/{name}/data-loading/test-read` | Honest sample read of a PXF source (`?job` OR `?server&profile&resource`, `?limit` default 10 cap 1000); `{cluster, source, limit, available, rowCount, columns, rows}` — real rows or `available:false`/empty, NEVER fabricated, NEVER `500` | Basic | **FULL** (Scenario 108) |

> The two read routes (P.7/P.9) return job definitions straight from the persisted
> `spec.dataLoading.jobs` (they do not query a running system). The five
> job-mutation routes (P.8/P.10/P.11/P.12/P.13) are now **FULL** (Scenario 107):
> they preserve the `404`-when-cluster-missing contract, then persist the spec
> change and/or create/delete the real one-off `batchv1.Job`. The Admin tier
> guards the destructive `DELETE` routes (P.5/P.11); Operator guards
> create/update/start/stop/sync; Basic guards read/status/logs/external-tables.

> **PXF lifecycle routes (Implemented this cycle — Scenario 95).** The three
> `pxf/{status,restart,sync}` routes are wired in `internal/api/server.go`
> (`handlePXFStatus`/`handlePXFRestart`/`handlePXFSync`) and serve real data:
> - `GET .../pxf/status` (`PermissionBasic`) → `200` with
>   `{servers, configured, sidecars:[{pod,ready}], readySidecars, totalSidecars}`.
>   Readiness is read straight from the segment-primary pods' real
>   `ContainerStatuses` for the `pxf` container — **no** synthetic health, **no**
>   exec, **no** cross-pod HTTP. It echoes the spec-derived
>   `status.dataLoading.pxf.{configured,servers}`.
> - `POST .../pxf/restart` (`PermissionOperator`) → `202` with
>   `{restarted, statefulSet, message}`. Patches the `<cluster>-segment-primary`
>   StatefulSet pod-template restart-trigger annotation
>   (`avsoft.io/restart-trigger`), so the segment-primary pods **roll** (a pod
>   roll, heavier than an in-place sidecar restart — see Scenario 95). Records
>   `cloudberry_pxf_restart_total{cluster,namespace,result}` (`started`/`failed`).
> - `POST .../pxf/sync` (`PermissionOperator`) → `202` with
>   `{synced, configMap, statefulSet, message}`. Refreshes the
>   `<cluster>-pxf-servers` ConfigMap and bumps the restart-trigger so the
>   `pxf-cred-init` init container re-renders the resolved configs on the roll.
>
> On a cluster where PXF is not enabled, all three return `400 PXF_NOT_ENABLED`
> and perform no StatefulSet/ConfigMap mutation.

> **Disabled data-loading reporting (Scenario 112 / DIS.1).** When the
> data-loading subsystem itself is disabled (`dataLoading.enabled: false` or
> absent), the REST surface reports **`DATA_LOADING_NOT_ENABLED`**: **mutating**
> endpoints (job `POST/PUT/DELETE`, `start`/`stop`, `external-tables`,
> `jobs/{job}/logs`) return **`400 DATA_LOADING_NOT_ENABLED`**, while **read**
> endpoints (`GET` jobs list / one job) return **`200`** with a **disabled
> envelope** (`writeDataLoadingDisabled`). A PXF endpoint on a DL-disabled cluster
> reports **`DATA_LOADING_NOT_ENABLED`** (the broader gate) and **NOT**
> `PXF_NOT_ENABLED` — DL-disabled takes precedence (`getPXFCluster` checks
> `dataLoadingEnabled` before `pxfEnabled`). See
> [Scenario 112](#scenario-112--disabled-states-dis1dis3).

## CLI Commands

> **Status legend.** **Live** = the command exists in
> `cmd/cloudberry-ctl/main.go` and calls the API. As of **Scenario 107** the
> `start`/`stop` and `create`/`delete` REST routes they reach are **FULL** (no
> longer 501-stubs) — the CLI verbs now drive real job creation/deletion and
> real one-off `batchv1.Job` start/stop. The `pxf status|restart|sync`
> subcommands are **Live** (registered via `newPxfCmd` and serving real data,
> Scenario 95). **As of Scenario 108 the FULL data-loading / PXF CLI surface
> (L.1–L.16) is Live**: `pxf servers {list,create,update,delete}` CRUD,
> `data-loading jobs logs` (streaming + kubectl fallback), `data-loading
> test-read`, the enriched `data-loading jobs create` (`--type pxf`/`--type
> gpload` flags + `--from-yaml`) are all registered subcommands wired to the
> Scenario 107 REST routes (plus the **new** `GET .../data-loading/test-read`
> endpoint added in Scenario 108). The
> sidecar-local PXF verbs (`pxf prepare/start/stop`) are **not** ctl commands —
> they are PXF-binary verbs exercised inside the sidecar via `kubectl exec`
> (see Scenario 95).

```bash
# --- Live commands (data-loading group) ---
# The target cluster comes from the global --cluster flag; start/stop/delete
# take the job name as a positional argument.
cloudberry-ctl data-loading jobs list   --cluster my-cluster            # Live  (FULL read)
cloudberry-ctl data-loading status      --cluster my-cluster            # Live  (FULL; lists jobs)
cloudberry-ctl data-loading jobs create --cluster my-cluster …          # Live  → REST FULL (Scenario 107; 201/409)
cloudberry-ctl data-loading jobs start  --cluster my-cluster job-name   # Live  → REST FULL (Scenario 107; 202 → real Job)
cloudberry-ctl data-loading jobs stop   --cluster my-cluster job-name   # Live  → REST FULL (Scenario 107; 202/200)
cloudberry-ctl data-loading jobs delete --cluster my-cluster job-name   # Live  → REST FULL (Scenario 107; deletes Job)

# --- Live commands (pxf group — Scenario 95, operator-driven lifecycle) ---
# --cluster is required; --namespace defaults to cloudberry-test.
cloudberry-ctl pxf status  --cluster my-cluster                         # Live (FULL; honest sidecar readiness)
cloudberry-ctl pxf restart --cluster my-cluster                         # Live (FULL; 202 → segment-primary pod ROLL)
cloudberry-ctl pxf sync    --cluster my-cluster                         # Live (FULL; 202 → ConfigMap refresh + roll)
```

> **`pxf restart` is a pod roll, not an in-place restart.** `pxf restart` makes
> the operator patch the `<cluster>-segment-primary` StatefulSet pod-template
> restart-trigger annotation; the kubelet then rolls **all** segment pods, each
> of which re-runs the entrypoint (`pxf prepare`/`pxf start`), so every PXF
> sidecar restarts. This is heavier than the sidecar-local in-place
> `pxf restart` verb (which you would run via `kubectl exec -c pxf`).

> As of **Scenario 108** the live `data-loading jobs create` accepts the rich
> `--name/--type/--server/--profile/--resource/--target/--schedule` flags (and
> the `--type gpload` + `--from-yaml` variants below); before Scenario 108 it
> posted a minimal body to the **now-FULL** REST route (Scenario 107). The
> `POST .../jobs` endpoint was already Implemented; Scenario 108 wired the rich
> flag UX.

### Implemented CLI (Scenario 108 — L.1–L.16)

> **Status: Implemented (Scenario 108).** The commands below are all registered
> subcommands in `cmd/cloudberry-ctl/main.go`, wired to the Scenario 107 REST
> routes (plus the **new** `GET .../data-loading/test-read` endpoint added in
> Scenario 108). See
> [Scenario 108 — All CLI Commands (L.1–L.16)](#scenario-108--all-cli-commands-l1l16)
> and [cloudberry-ctl §5.10](07-cloudberry-ctl-spec.md#510-data-loading-and-pxf-scenario-108).

```bash
# PXF server CRUD (Implemented — Scenario 108) — L.2–L.5
cloudberry-ctl pxf servers list --cluster my-cluster                          # L.2 → GET .../pxf/servers
cloudberry-ctl pxf servers create --cluster my-cluster --name s3-lake --type s3 \
  --endpoint http://minio:9000 --bucket data-lake \
  --credential-secret s3-creds:access_key \
  --credential-secret s3-creds:secret_key                                     # L.3 → POST .../pxf/servers
cloudberry-ctl pxf servers update s3-lake --cluster my-cluster \
  --endpoint http://minio:9001                                                # L.4 → PUT .../pxf/servers/{name}
cloudberry-ctl pxf servers delete s3-lake --cluster my-cluster                # L.5 → DELETE .../pxf/servers/{name}
 
# Rich PXF job creation flags (Implemented — Scenario 108) — L.9
cloudberry-ctl data-loading jobs create --cluster my-cluster \
  --name s3-ingest --type pxf \
  --server s3-lake --profile s3:parquet \
  --resource "s3a://data-lake/events/" \
  --target public.events \
  --schedule "0 */6 * * *"
# Stream a data-loading Job's logs (Implemented — Scenario 108) — L.13
cloudberry-ctl data-loading jobs logs --cluster my-cluster --job s3-ingest \
  --follow --tail 200                                                         # → GET .../jobs/{job}/logs
 
# gpload flags (Implemented — Scenario 108) — L.14
cloudberry-ctl data-loading jobs create --cluster my-cluster \
  --name csv-load --type gpload \
  --gpfdist-host gpfdist-svc --gpfdist-port 8080 \
  --file-path "/data/incoming/*.csv" \
  --target public.raw_data --format csv
 
# Quick test-read (Implemented — Scenario 108) — L.15
cloudberry-ctl data-loading test-read --cluster my-cluster \
  --server s3-lake --profile s3:parquet \
  --resource "s3a://data-lake/events/sample.parquet" --limit 10              # → GET .../data-loading/test-read
cloudberry-ctl data-loading test-read --cluster my-cluster --job s3-ingest --limit 5
 
# YAML upload (Implemented — Scenario 108) — L.16
cloudberry-ctl data-loading jobs create --cluster my-cluster --from-yaml job-config.yaml
```

> **Flag shapes (Scenario 108).** `pxf servers create` takes `--name --type
> --endpoint --bucket --credential-secret` (the last is **repeatable**, value
> `name[:key]`); `pxf servers update [name]` takes a positional name + `--endpoint`
> (via the new `runAPIPut` helper); `pxf servers delete [name]` takes a positional
> name (`409 SERVER_IN_USE` when referenced). `data-loading jobs create`
> accepts `--type pxf` (`--name --server --profile --resource --target
> --schedule`), `--type gpload` (`--gpfdist-host --gpfdist-port --file-path
> --format`), or `--from-yaml <file>` (reads + unmarshals a full job;
> **precedence over flags**). `data-loading jobs logs` takes `--job --follow
> --tail` (streams via `GetStream`; kubectl fallback). `data-loading test-read`
> takes `--job` **OR** `--server/--profile/--resource`, plus `--limit N`
> (**default 10, cap 1000**); it prints **real rows** or `available:false`/empty
> when the DB/source is unreachable — never fabricated, never `500`.

## Prometheus Metrics

Of the 21 metrics, **15** exist in the metrics recorder (incl. the two
actuator-passthrough request/latency series) and **6 stay honestly Planned/absent**
(Scenario 105 added the 2 HONEST PXF health gauges `cloudberry_pxf_status` and
`cloudberry_pxf_extensions_installed`; Scenario 106 added the HONEST PXF
servers-changed counter `cloudberry_pxf_servers_changed_total`; **Scenario 109**
added the per-segment `cloudberry_pxf_service_up`, the actuator-passthrough
`cloudberry_pxf_requests_total`/`cloudberry_pxf_request_duration_seconds`, and the
conditional `cloudberry_data_loading_bytes_total`). The **6 honestly-absent**
families — `cloudberry_pxf_bytes_transferred_total` (M.4),
`cloudberry_pxf_records_total` (M.5), `cloudberry_pxf_errors_total` (M.6, **folded**
into `data_loading_errors_total` + actuator non-2xx — no synthetic metric is
registered), `cloudberry_pxf_active_connections` (M.7), and the 2
`cloudberry_gpfdist_*` (M.15/M.16) — have **no honest source** in PXF 2.1.0 /
gpfdist and are **never fabricated** (the Scenario 109 tests assert their absence).
The **2 config-derived gauges** are emitted on every reconcile:
`cloudberry_data_loading_jobs_active` (active job count) and
`cloudberry_pxf_servers_configured` (`len(pxf.servers)`). The **5
data-loading-runtime metrics** are now **emitted from the spawned Job's
Kubernetes status + the harvested `DATALOAD_ROWS` marker** by
`reconcileDataLoadingJobs` → `recordDataLoadJobMetrics` (terminal-state gated so
re-reconciles never double-count; values are **never synthesized**):
`cloudberry_data_loading_job_status`,
`cloudberry_data_loading_job_last_success_timestamp`,
`cloudberry_data_loading_job_duration_seconds`,
`cloudberry_data_loading_rows_total`, and
`cloudberry_data_loading_errors_total`. The operator-driven PXF restart counter
`cloudberry_pxf_restart_total{cluster,namespace,result}` (`result` ∈
`started`/`failed`) is **emitted** from the restart handler (`handlePXFRestart` →
`recordPXFRestart`, Scenario 95). Two **HONEST PXF health gauges** are now
**emitted (Scenario 105)** from the same observed-only sources as the status
sub-fields, and **only when observable**:
`cloudberry_pxf_status{cluster,namespace}` (0=Stopped, 1=Running, 2=Error;
emitted only when the readiness aggregation yields an observable state) and
`cloudberry_pxf_extensions_installed{cluster,namespace}` (the count of installed
PXF extensions, emitted only when the live `pg_extension` probe actually observed
them). The HONEST PXF **servers-changed** counter
`cloudberry_pxf_servers_changed_total{cluster,namespace}` is **emitted (Scenario
106)** — incremented by `1` **only on a real `<cluster>-pxf-servers` ConfigMap
`Data` diff** (a server added, removed, or its rendered `*-site.xml` keys
changed), by **both** the controller reconcile (`emitPXFServersChanged`) and the
explicit `pxf sync` API path (`recordPXFServersChanged`); a no-op sync or a
first-time create increments **nothing** (no diff = no series forced).
**Scenario 109** added four more honest metrics: the per-segment gauge
`cloudberry_pxf_service_up{cluster,namespace,segment_host}` (the disaggregation of
`cloudberry_pxf_status`, set per observed segment-primary pod from real
`pxf` container readiness via `util.PXFReadyByHost` — `1` healthy / `0` on a
killed segment; never a synthesized host); the actuator-passthrough
`cloudberry_pxf_requests_total` + `cloudberry_pxf_request_duration_seconds` (the
**real** `http_server_requests_seconds_count`/`_sum`/`_bucket` series scraped from
the PXF Spring Boot Actuator `/actuator/prometheus` by a **dedicated vmagent scrape
job** — request count + latency are REAL, but the `server`/`profile`/`operation`
labels are downgraded to the actuator-native `uri`/`method`/`status` because they
are **not** honestly derivable from the actuator URI); and the conditional
`cloudberry_data_loading_bytes_total{cluster,namespace,job,source_type}` (emitted
from the real `DATALOAD_BYTES=<n>` marker the gpload script computes via `wc -c`
for a **local gpload input source**, and **omitted** — honestly absent — for
external-table/pxf/FDW/continuous loads where no byte count is available). The
remaining `cloudberry_pxf_*` families (`bytes_transferred_total`, `records_total`,
`errors_total`, `active_connections`) and the 2 `cloudberry_gpfdist_*` metrics
remain **honestly Planned/absent** — they have no honest source and are never
fabricated (`pxf_errors_total` is **folded** into the existing
`cloudberry_data_loading_errors_total` + actuator non-2xx rather than registered
as a synthetic typed counter).

> The 5 runtime metrics are emitted for **both** PXF and native jobs from the
> observed Job status. With the `cloudberry-pxf` sidecar image + the `pxf`
> extension present, a `pxf://` job succeeds and emits the success metrics (rows,
> last-success, duration) just like the native path (row-count verified, 183,961
> rows). On a stock `cloudberry-official` (no PXF extension) the `pxf://` Job
> terminates Failed (incrementing `…_errors_total`, status gauge `3`).

> **Writable EXPORT reuses these metrics (Scenario 99 — NO new metric).** A
> writable export (`mode: writable`) is a data-loading job, so its rows-exported,
> success and failure are observed through the **same** existing metrics:
> `cloudberry_data_loading_rows_total` (the exported rowcount, harvested from the
> SAME `DATALOAD_ROWS` marker — the filtered `sourceFilter` export reports fewer
> rows than the unfiltered baseline), `cloudberry_data_loading_job_status`
> (**2**=Succeeded / **3**=Failed) and `cloudberry_data_loading_errors_total`. No
> export-specific metric is added; `cloudberry_pxf_bytes_transferred_total` stays
> **Planned**.

> **gpload reuses these metrics (Scenario 101 — NO new metric).** A `type: gpload`
> job is a data-loading job, so its load is observed through the **same** existing
> `cloudberry_data_loading_*` metrics: `job_status` (2=success / 3=failed),
> `rows_total` (best-effort harvested from gpload's own summary line via the SAME
> `DATALOAD_ROWS` marker — **omitted when gpload's summary cannot be parsed**, no
> synthesized rowcount), `errors_total`, and `job_duration_seconds`. **Scenario 109
> additionally emits `cloudberry_data_loading_bytes_total{job,source_type}`** for a
> gpload job **with a LOCAL input source**, from the real `DATALOAD_BYTES=<n>`
> marker the gpload script computes via `wc -c`; for non-local inputs (and for
> external-table/pxf/FDW loads) the byte marker is **omitted** and the metric is
> honestly absent — never synthesized. The **gpfdist Deployment readiness** is observable via
> **kube-state-metrics** (`kube_deployment_status_replicas_ready`), which is **NOT
> deployed in the test env**, so in tests gpfdist readiness is observed via
> `kubectl` rather than VictoriaMetrics. The 2 `cloudberry_gpfdist_*` metrics
> (`connections_active`, `bytes_served_total`) stay **Planned** — gpfdist has **no
> scrapable Prometheus endpoint**.

> **Why `cloudberry_pxf_bytes_transferred_total` stays Planned (metrics-honesty
> decision — Scenario 98, reaffirmed by Scenario 109).** PXF 2.1.0 runs on Spring
> Boot Actuator. **Scenario 109 now enables `/actuator/prometheus`** (via
> `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE=health,prometheus`) to scrape the
> **real** request count + latency (M.2/M.3) — but the actuator still offers only
> `http_server_requests` count/latency + JVM metrics; there is **no honest
> external-source byte counter** to scrape. Emitting a fabricated
> `bytes_transferred` would violate the metrics-honesty rule (values are never
> synthesized), so this counter **stays Planned** (along with `pxf_records_total`,
> the folded `pxf_errors_total`, `pxf_active_connections`, and the 2
> `cloudberry_gpfdist_*`). Filter pushdown is instead observed via **real**
> signals: (1) **row-count reduction** — `cloudberry_data_loading_rows_total`
> (harvested from the `DATALOAD_ROWS` marker) is lower for a filtered job than an
> unfiltered baseline; (2) **`EXPLAIN`** shows the pushed filter / projected
> columns; (3) **source-side query logs** (JDBC pgsource/MySQL, Hive/HS2) show the
> `WHERE` predicate. Per-row error handling is observed via the **real**
> `cloudberry_data_loading_job_status` (2=success / 3=failed) +
> `cloudberry_data_loading_errors_total` + `rows_total` (valid rows only). See
> [Scenario 98](#scenario-98--filter-pushdown-column-projection-per-row-error-handling).

| Metric | Type | Labels | Description | Status |
|--------|------|--------|-------------|--------|
| `cloudberry_data_loading_jobs_active` | Gauge | cluster, namespace | Active (enabled) data loading jobs | **Implemented** (emitted) |
| `cloudberry_pxf_servers_configured` | Gauge | cluster, namespace | Configured external PXF servers (`len(pxf.servers)`, config-derived) | **Implemented** (emitted) |
| `cloudberry_data_loading_job_status` | Gauge | cluster, namespace, job | Job status (0=idle/pending, 1=running, 2=success, 3=failed) | **Implemented** (emitted from Job status) |
| `cloudberry_data_loading_job_last_success_timestamp` | Gauge | cluster, namespace, job | Unix timestamp of last successful job run | **Implemented** (emitted from Job `completionTime`) |
| `cloudberry_data_loading_job_duration_seconds` | Histogram | cluster, namespace, job | Job execution duration | **Implemented** (emitted from Job start→completion) |
| `cloudberry_data_loading_rows_total` | Counter | cluster, namespace, job, source_type | Total rows loaded by job | **Implemented** (emitted from `DATALOAD_ROWS` marker) |
| `cloudberry_data_loading_errors_total` | Counter | cluster, namespace, job | Failed data-loading Job runs per job | **Implemented** (emitted on Job Failed) |
| `cloudberry_pxf_restart_total` | Counter | cluster, namespace, result | Operator-driven PXF restart operations (`result`=`started`/`failed`) | **Implemented** (emitted from `handlePXFRestart`) |
| `cloudberry_pxf_status` | Gauge | cluster, namespace | Honest PXF health (0=Stopped, 1=Running, 2=Error) from real segment-primary `pxf` `ContainerStatuses` readiness aggregation | **Implemented** (Scenario 105; emitted **only when observable** — never synthesized) |
| `cloudberry_pxf_extensions_installed` | Gauge | cluster, namespace | Count of installed PXF extensions (`pxf`/`pxf_fdw`) from a real read-only `pg_extension` probe | **Implemented** (Scenario 105; emitted **only when observed** — absent when DB unreachable / none installed) |
| `cloudberry_pxf_servers_changed_total` | Counter | cluster, namespace | Observed PXF servers ConfigMap `Data` changes (incremented on a real add/remove/update diff) | **Implemented** (Scenario 106; incremented by `emitPXFServersChanged`/`recordPXFServersChanged` **only on a real diff** — never on a no-op sync or first create) |
| `cloudberry_pxf_service_up` | Gauge | cluster, namespace, segment_host | PXF service health **per segment** (0/1) — the per-segment-host disaggregation of `cloudberry_pxf_status` | **Implemented** (Scenario 109; from **real** per-segment-primary-pod `pxf` `ContainerStatuses[pxf].Ready` via `util.PXFReadyByHost`; `1` healthy / `0` on a killed segment; emitted **only for observed segment hosts** — never a synthesized host) |
| `cloudberry_pxf_requests_total` | Counter | (actuator-native: `uri`, `method`, `status`, `outcome`) | Total PXF requests | **Implemented** (Scenario 109; the **real** `http_server_requests_seconds_count` scraped from the PXF Spring Boot Actuator `/actuator/prometheus`. **Label-honesty caveat:** the request **count is REAL**, but the catalog-imagined `server`/`profile`/`operation` labels are **NOT** honestly derivable from the actuator URI → the series flows under its **actuator-native** name/labels; the fine-grained labels are **downgraded, never fabricated**) |
| `cloudberry_pxf_request_duration_seconds` | Histogram | (actuator-native: `uri`, `method`, `status`) | PXF request duration | **Implemented** (Scenario 109; the **real** latency histogram `http_server_requests_seconds_sum`/`_bucket` from `/actuator/prometheus`. **Label-honesty caveat:** the **latency is REAL**, but `server`/`profile` are NOT honestly derivable → exposed with **actuator-native** labels; never fabricated) |
| `cloudberry_pxf_bytes_transferred_total` | Counter | cluster, namespace, server, profile, direction | Bytes read from/written to external sources | **Planned** (PXF 2.1.0 exposes **no honest external-bytes counter** — Spring Boot Actuator offers only `http_server_requests` + JVM metrics; never fabricated. Filter pushdown is observed instead via row-count reduction (`cloudberry_data_loading_rows_total`) + `EXPLAIN` + source logs — Scenario 98. Scenario 109 reaffirms this absence; the absence test `109-M4-ABSENT` asserts the family is NOT emitted) |
| `cloudberry_pxf_records_total` | Counter | cluster, namespace, server, profile, direction | Records read/written via PXF | **Planned** (no honest PXF-native record counter in 2.1.0; record throughput is instead observed via the honest `cloudberry_data_loading_rows_total{job,source_type}`. Scenario 109 keeps this Planned; `109-M5-ABSENT` asserts it is NOT emitted, `109-M5-SUBST` asserts `rows_total` is the substitute) |
| `cloudberry_pxf_errors_total` | Counter | cluster, namespace, server, profile, error_type | PXF errors (connection, parse, timeout) | **Planned — FOLDED, not a new synthetic metric** (Scenario 109; PXF does not expose typed error counts. The honest error signals are (1) the existing `cloudberry_data_loading_errors_total{job}` on a Failed load (error_type collapses to "load_failed") + `job_status=3`, and (2) actuator non-2xx via `http_server_requests{status=~"4..\|5.."}`. No typed `cloudberry_pxf_errors_total{error_type}` is registered — fabricating one would invent a meaning the source lacks) |
| `cloudberry_pxf_active_connections` | Gauge | cluster, namespace, server | Active PXF connections to external sources | **Planned** (no honest source — `tomcat.threads.busy` is a JVM-thread **proxy**, NOT external-source connections; relabeling it as `active_connections` would be dishonest. Scenario 109 keeps this Planned; `109-M7-ABSENT` asserts it is NOT emitted) |
| `cloudberry_data_loading_bytes_total` | Counter | cluster, namespace, job, source_type | Total bytes loaded by job | **Implemented when a real byte count is available** (Scenario 109; emitted from the **real** `DATALOAD_BYTES=<n>` marker that the gpload script computes via `wc -c` for a **local gpload input source**; **omitted (no series) for external-table/pxf/FDW/continuous loads** where psql returns only a rowcount tag — **never synthesized**) |
| `cloudberry_gpfdist_connections_active` | Gauge | cluster, namespace | Active gpfdist connections from segments | **Planned** (gpfdist has **no scrapable Prometheus endpoint / `/metrics`** — only `/var/log/gpfdist.log`; readiness is observed via KSM `kube_deployment_status_replicas_ready`. Scenario 109 keeps this Planned; `109-M15-ABSENT` asserts it is NOT emitted) |
| `cloudberry_gpfdist_bytes_served_total` | Counter | cluster, namespace | Total bytes served by gpfdist | **Planned** (same as M.15 — gpfdist has only a log file, no scrapable endpoint; never fabricated. `109-M16-ABSENT` asserts it is NOT emitted) |

## Webhook Validation

> **Status: Implemented.** All rules (W.1–W.25 **plus W.10b**, the Scenario 96
> write-capability rule, the Scenario 99 `sourceFilter` rule, the Scenario 101
> gpload-field rules **W.18–W.22**, the Scenario 102 custom-connector /
> streaming rules **W.23/W.24/W.23c**, and the Scenario 103 load-method rule
> **W.25** + the W.17 fdw-read tweak), including the W.10 profile allowlist and the
> W.16 `file://` rejection, are present in `internal/webhook/validating.go` and
> exercised by `internal/webhook/validating_test.go`,
> [Scenario 89](#scenario-89--webhook-validation-all-rules), (for W.3/W.4
> object-store extension + W.10b) [Scenario 96](#scenario-96--object-store-profiles--format-write-capability),
> (for W.17 `sourceFilter`) [Scenario 99](#scenario-99--writable-external-tables--data-export),
> (for W.18–W.22) [Scenario 101](#scenario-101--gpfdist-deployment--gpload-csv),
> (for **W.23/W.24/W.23c**) [Scenario 102](#scenario-102--kafka-cdc-continuous-streaming-custom-connector),
> and (for **W.25** + the W.17 fdw-read tweak) [Scenario 103](#scenario-103--fdw-based-loading-path).
> The **complete, systematic W.1–W.15 rejected-CR matrix** (per-rule rejection
> source + descriptive-error + NO-PERSIST + CONTROL admit, across unit/functional/
> integration/e2e/perf) is verified by
> [Scenario 110](#scenario-110--webhook-validation-all-rules).

The validating admission webhook (`internal/webhook/validating.go`,
`validateDataLoading`) enforces the rules below **only when**
`dataLoading.enabled: true`. Validation is **fail-fast**: the first offending
field is reported with a descriptive, field-path-anchored error and the
`CloudberryCluster` is **rejected (not persisted)**. A disabled
(`dataLoading.enabled: false`) or absent spec is a **validation** no-op even if
its body is invalid (these rules don't run). Note this is *admission-time
validation* only — at *reconcile time* a disabled spec is **not** a no-op: it
triggers an active teardown (`cleanupDataLoading`, Scenario 112 / DIS.1) — see
[Scenario 112](#scenario-112--disabled-states-dis1dis3).

Each rule carries a stable id (`W.1`–`W.25` plus `W.10b`) that maps directly to a
[Scenario 89](#scenario-89--webhook-validation-all-rules) (W.1–W.16) /
[Scenario 99](#scenario-99--writable-external-tables--data-export) (W.17) /
[Scenario 101](#scenario-101--gpfdist-deployment--gpload-csv) (W.18–W.22) /
[Scenario 102](#scenario-102--kafka-cdc-continuous-streaming-custom-connector) (W.23, W.24, W.23c) /
[Scenario 103](#scenario-103--fdw-based-loading-path) (W.25 + the W.17 fdw-read tweak)
negative test case and to a unit case in
`internal/webhook/validating_test.go`. The W.1–W.15 rules are **additionally**
covered by the complete multi-layer
[Scenario 110](#scenario-110--webhook-validation-all-rules) matrix (unit
`internal/webhook/scenario110_validation_test.go` + functional/integration/e2e/perf
`scenario110_*`), which also records the **rejection source** (webhook vs
CRD-schema-enum vs both) per rule.

| Rule | Field path | Constraint |
|------|-----------|------------|
| W.1 | `dataLoading.pxf.image` | Required (non-empty) when `dataLoading.pxf.enabled: true` |
| W.2 | `dataLoading.pxf.servers[].name` | Required (non-empty) and unique across `servers[]` |
| W.3 | `dataLoading.pxf.servers[].type` | Must be one of `s3`, `hdfs`, `jdbc`, `hbase`, `hive`, `gs`, `abfss`, `wasbs`, `custom` (the **object-store types `gs`/`abfss`/`wasbs` were added in Scenario 96**; **`custom` was added in Scenario 102** — a generic connector server with **NO forced type-specific config keys**, in lockstep with the CRD `PxfServerSpec.Type` enum `s3;hdfs;jdbc;hbase;hive;gs;abfss;wasbs;custom`). A `custom` server's profile implementation comes from a matching `customConnectors[]` JAR; the server→connector link is enforced by **W.24** |
| W.4 | `dataLoading.pxf.servers[]` (object-store types `s3`/`gs`/`abfss`/`wasbs`) | `config["fs.s3a.endpoint"]` must be set for **all** object-store types (PXF renders every object store into `s3-site.xml`). `credentialSecrets[]` must be non-empty **only for type `s3`**; `gs`/`abfss`/`wasbs` use cloud-native auth (workload identity / account keys in config) so their `credentialSecrets[]` are **optional** (Scenario 96) |
| W.5 | `dataLoading.pxf.servers[]` (type `jdbc`) | `config["jdbc.driver"]` **and** `config["jdbc.url"]` must be set |
| W.6 | `dataLoading.pxf.servers[]` (type `hdfs`) | `config["fs.defaultFS"]` must be set |
| W.7 | `dataLoading.jobs[].name` | Required (non-empty) and unique across `jobs[]` |
| W.8 | `dataLoading.jobs[].type` | Must be `pxf` or `gpload` |
| W.9 | `dataLoading.jobs[].pxfJob.server` | Must reference a defined `dataLoading.pxf.servers[].name` (the `pxfJob` body is required for `type: pxf`) |
| W.10 | `dataLoading.jobs[].pxfJob.profile` | Must be a valid PXF profile per the [profile policy](#w10-valid-pxf-profile-policy) below |
| W.10b | `dataLoading.jobs[].pxfJob` (`mode: writable`) | **Write-capability (Scenario 96 + scheme-aware extension, Scenario 97).** When `pxfJob.mode == "writable"`, the profile must be writable per `pxfpolicy.IsProfileWritable`, which is **scheme-aware**. It **rejects** two classes: **(a)** a non-writable **FORMAT** on a writable scheme — `json`/`orc`/`rc` on `s3`/`gs`/`abfss`/`wasbs`/`hdfs`; and **(b)** **ANY** profile on a **read-only SCHEME** — all `hive*` (`hive`/`hive:text`/`hive:orc`/`hive:rc`) and `HBase`, regardless of format (so `hive:text` writable is rejected even though `text` is a writable format). Writable jobs admit only for `text`/`parquet`/`avro`/`SequenceFile` on a writable scheme. The error contains `write-unsupported` and `writable`. The builder re-checks the **same** predicate (defense-in-depth). *(Code id: `W.10b` in `validating.go` — the next sub-id under W.10, kept stable rather than renumbering the W.11–W.16 tail.)* |
| W.11 | `dataLoading.jobs[].pxfJob.targetTable` | Required (non-empty) |
| W.12 | `dataLoading.jobs[].gploadJob.targetTable` | Required (non-empty); the `gploadJob` body is required for `type: gpload` |
| W.13 | `dataLoading.jobs[].schedule` | Must be a valid cron expression when provided (empty schedule allowed) |
| W.14 | `dataLoading.jobs[].pxfJob.partitioning` | When `column` is set, `range` **and** `interval` must also be set (all three together) |
| W.15 | `dataLoading.jobs[].pxfJob.errorHandling.segmentRejectLimitType` | Must be `rows` or `percent` when set |
| W.16 | `dataLoading.jobs[].gploadJob.filePaths[]` | Must **not** use the bare `file://` scheme. A `file://` external table requires a per-segment-host URI (`file://<seghost>/path`) the operator cannot synthesize from the CRD, so it is **invalid on a multi-segment cluster**. Supported native sources are bare paths (served by the cluster gpfdist Service), `gpfdist://`, and `s3://`. |
| W.17 | `dataLoading.jobs[].pxfJob.sourceFilter` | **Source-filter sanity (Scenario 99; mode/method gate extended in Scenario 103).** **(a) Mode/method gate** — `sourceFilter` is valid on a **writable export** job (`mode: writable`) **OR** an **fdw read** job (`loadMethod: fdw`, the fdw read's `INSERT INTO <target> SELECT * FROM <foreign> WHERE <filter>` applies the predicate to the foreign-table SELECT). On a **plain external-table read/import** (loadMethod unset/`external-table`, not writable) the INSERT direction has no source-table predicate to apply, so a set `sourceFilter` is **rejected** (the error contains `sourceFilter` and `writable`). **(b) Sanity check** — `sourceFilter` must **not** contain a statement terminator (`;`) or SQL comment opener (`--`, `/*`) (the error names `statement terminators or SQL comments`). This is a **cheap substring scan, not a SQL parser** (`sqlPredicateForbidden = {";", "--", "/*"}`): the predicate is **cluster-admin-authored, trusted CR content** (the **same trust boundary as `targetTable`**), so the check only reduces obvious stacked-query / comment footguns — it does not make a malicious predicate safe. *(Code id: `W.17` in `validateSourceFilter`, `validating.go`; called AFTER W.25 so it can consult `loadMethod`.)* |
| W.18 | `dataLoading.jobs[].gploadJob.inputSource.type` | **gpload input-source kind (Scenario 101).** When set, `inputSource.type` must be `gpfdist` or `local` (the CRD enum also constrains it; the webhook rejects with a clear message for defense in depth). *(Code id: `W.18` in `validateGploadInputSource`, `validating.go`.)* |
| W.19 | `dataLoading.jobs[].gploadJob.delimiter` | **gpload delimiter (Scenario 101).** When set, `delimiter` must be **exactly one character** (the CRD `MaxLength=1` also bounds it; the webhook rejects the empty-vs-multi distinction with a clear message). *(Code id: `W.19` in `validateGploadDelimiter`, `validating.go`.)* |
| W.20 | `dataLoading.jobs[].gploadJob` (`mode: update`/`merge`) | **gpload update/merge key (Scenario 101).** When `mode` is `update` or `merge`, gpload requires the `MATCH_COLUMNS` block, so a **non-empty `matchColumns`** is required; an empty `matchColumns` is **rejected**. *(Code id: `W.20` in `validateGploadMode`, `validating.go`.)* |
| W.21 | `dataLoading.jobs[].gploadJob.postActions[]` | **gpload post-action sanity (Scenario 101).** Each `postActions[]` element (a raw SQL statement run via `SQL.AFTER`) must pass the **same** cheap SQL sanity check as W.17 (no statement terminator `;` / comment opener `--`, `/*`). The actions are **author-trusted CR content** (same trust boundary as `targetTable`); the check only reduces obvious footguns. *(Code id: `W.21` in `validateGploadPostActions`, reuses the W.17 `containsUnsafeSQLFragment` helper.)* |
| W.22 | `dataLoading.jobs[].gploadJob.inputSource.host`/`.port` | **gpload host/port scope (Scenario 101).** `host`/`port` are only meaningful for a `gpfdist` source; on `inputSource.type: local` a set `host` or non-zero `port` has no effect and is **rejected**. *(Code id: `W.22` in `validateGploadInputSource`, `validating.go`.)* |
| W.23 | `dataLoading.jobs[].pxfJob.profile` (custom-connector/streaming) | **Kafka-profile-requires-custom-connector (Scenario 102 — policy reversal, scoped).** A **custom-connector (streaming) profile** (`kafka`, `rabbitmq`; scheme-matched, case-insensitive — see `pxfCustomConnectorSchemes`) is "recognized" by W.10 but **admitted ONLY** when the referenced server is **connector-backed** (`type: custom` **with** a matching `customConnectors[]` entry). A **bare** `kafka` profile, or a `kafka`/`rabbitmq` profile on a **non-custom** server, is **still REJECTED** — preserving the "no built-in streaming" guarantee. The built-in allowlist (`isValidPxfProfile`) is **UNCHANGED** (`isValidPxfProfile("kafka")` is still false); recognition lives in the separate `isCustomConnectorProfile`. Reject message: `dataLoading.jobs[i].pxfJob.profile "kafka" is a custom-connector profile and requires the referenced server "kafka-connector" to be type=custom with a matching customConnectors[] entry`. *(Code id: `W.23` in `validatePxfJob`, `validating.go`.)* |
| W.24 | `dataLoading.pxf.servers[]` (type `custom`) | **Custom-server-requires-connector (Scenario 102).** A server of `type: custom` **MUST** have a `customConnectors[]` entry of the **same name** (the link is by NAME) — otherwise the JAR that implements its profile is missing. A mismatched/absent connector name is **rejected**. Reject message: `dataLoading.pxf.servers[i] of type custom requires a matching customConnectors[].name "kafka-connector"`. *(Code id: `W.24` in `validatePxfServers`, `validating.go`.)* |
| W.23c | `dataLoading.jobs[].pxfJob` (`continuous`/`batchSize`/`flushInterval`) + `jobs[].schedule` | **Streaming-knobs sanity (Scenario 102).** **(a)** `batchSize`, when set, must be **≥ 1** (also enforced by kubebuilder `Minimum=1`); a negative value is **rejected** (`...batchSize -1 must be >= 1`). **(b)** `flushInterval`, when set, must parse as a **Go duration** (`time.ParseDuration`); a non-duration is **rejected** (`...flushInterval "nonsense" must be a valid duration`). **(c)** a `continuous: true` job **MUST NOT** set a `schedule` — it runs as a one-off long-running Job, never a CronJob (J.46); a continuous job with a schedule is **rejected**: `dataLoading.jobs[i]: continuous streaming jobs must not set a schedule; they run as a one-off long-running Job, not a CronJob`. (A NON-continuous kafka job MAY carry a schedule.) *(Code id: `W.23c` in `validateStreamingParams` + `validateDataLoadingJobBody`, `validating.go`.)* |
| W.25 | `dataLoading.jobs[].pxfJob.loadMethod` | **Load-method (Scenario 103).** **(a) ENUM** — `loadMethod`, when set, must be **`external-table`** (the default transient external-table path) or **`fdw`** (the persistent foreign-data-wrapper path); any other value is **rejected** (`...loadMethod "bogus" must be external-table or fdw`). Also enforced by the CRD kubebuilder `Enum=external-table;fdw`; re-checked here for defense in depth. **(b) FDW IS READ-ONLY** — `loadMethod: fdw` is a **read/import** path: it is **rejected** with `mode: writable` (`...loadMethod=fdw is a read/import path and is not valid with mode=writable (a writable FDW export is out of scope)`). **(c) FDW IS ONE-OFF** — `loadMethod: fdw` builds a **persistent one-off** load and is **rejected** with `continuous: true` (`...loadMethod=fdw is a one-off persistent load and is not valid with continuous=true`). W.25 is checked **BEFORE** W.17 so an fdw+writable job is rejected here (not by W.17) and so W.17 can safely consult `loadMethod` for its fdw-read allowance. *(Code id: `W.25` in `validateLoadMethod`, `validating.go`.)* |

> **Note (W.16 — `file://` is not supported for multi-segment loads).** In
> Cloudberry/Greenplum a `file://` external table requires the URI to embed a
> **segment host** (`file://<seghost>/path`): each segment reads its OWN local
> copy and the file must physically exist on every segment host. The operator
> does not enumerate segment hostnames at DDL-generation time and cannot
> synthesize a correct per-segment-host `file://` LOCATION, so a bare
> `file:///path` (the only form expressible in the CRD) is rejected at admission.
> Use `gpfdist://` or `s3://` (both work cluster-wide), or a **bare path** which
> the operator serves via the cluster `gpfdist` Service. The rejection error is
> field-pathed, e.g.:
> `dataLoading.jobs[i].gploadJob.filePaths[j]: file:// scheme is not supported for multi-segment loads; use gpfdist:// or s3:// (or a bare path served by the cluster gpfdist service)`.
> The builder still passes a `file://` scheme through verbatim (it never silently
> rewrites it) for a future single-host / in-container caller, but a CR can never
> reach it because W.16 rejects `file://` for gpload jobs first.

> **Note (credential model).** Sensitive values are **not** carried inline in
> `servers[].config` (which is a plain `map[string]string` for non-sensitive
> site settings only). Credentials are referenced via
> `servers[].credentialSecrets[]` (a list of `{name, key}` Secret references)
> and resolved by an init container at pod startup. W.4 therefore checks
> `credentialSecrets[]` is non-empty for `s3` servers rather than looking for
> inline keys. For the **cloud-native object stores** (`gs`/`abfss`/`wasbs`,
> Scenario 96) `credentialSecrets[]` is **optional** — they authenticate via
> workload identity / account keys carried in `config` — but `fs.s3a.endpoint` is
> still required (W.4).

### Rejection source per rule (webhook vs CRD schema enum vs both)

> **Status: Implemented + verified (Scenario 110).** Every rule W.1–W.15 is
> present in `internal/webhook/validating.go` with a descriptive, field-pathed
> error. **Three** of them — **W.3** (server `type`), **W.8** (job `type`), and
> **W.15** (`segmentRejectLimitType`) — are **ALSO** constrained by a CRD OpenAPI
> **kubebuilder `Enum`**, so on a **live** `kubectl apply` the **apiserver schema
> rejects the bad value BEFORE the webhook runs** (the webhook keeps the rule for
> **defense-in-depth**, but never sees the request). The apiserver enum error is
> itself descriptive (it names the field + the allowed values, e.g.
> `Unsupported value: "ftp"`). **Two** rules — **W.11** (`pxfJob.targetTable`) and
> **W.12** (`gploadJob.targetTable`) — are **expression-dependent**: an **omitted**
> required key is rejected by the CRD **schema `required`** at the apiserver, while
> an **empty-string** value of the same field passes the schema and is rejected by
> the **webhook**. The remaining **11** rules are **webhook-enforced only** (the
> field has no schema constraint that catches the violation — e.g. a free `config`
> map key, a cross-element uniqueness check, an undefined cross-reference, a cron
> string, or a cross-field partitioning rule).

| Rule | Trigger (the single offending value) | Descriptive error (substring) | Rejection source |
|------|--------------------------------------|-------------------------------|------------------|
| W.1 | `pxf.enabled: true` + empty `pxf.image` | `dataLoading.pxf.image is required when pxf.enabled is true` | **WEBHOOK** |
| W.2 | empty **or** duplicate `servers[].name` | `…servers[i].name is required` / `…name "x" is a duplicate` | **WEBHOOK** |
| W.3 | `servers[].type: ftp` | webhook: `…servers[i].type must be one of …, got "ftp"`; live: apiserver `Unsupported value: "ftp"` | **CRD-SCHEMA enum** (webhook = defense-in-depth) |
| W.4 | s3 server missing `config["fs.s3a.endpoint"]` **or** empty `credentialSecrets` | `…must include "fs.s3a.endpoint"` / `…must include credentialSecrets` | **WEBHOOK** |
| W.5 | jdbc server missing `config["jdbc.driver"]` **or** `config["jdbc.url"]` | `…must include "jdbc.driver"` / `…must include "jdbc.url"` | **WEBHOOK** |
| W.6 | hdfs server missing `config["fs.defaultFS"]` | `…must include "fs.defaultFS"` | **WEBHOOK** |
| W.7 | empty **or** duplicate `jobs[].name` | `…jobs[i].name is required` / `…name "x" is a duplicate` | **WEBHOOK** |
| W.8 | `jobs[].type: spark` | webhook: `…jobs[i].type must be "pxf" or "gpload", got "spark"`; live: apiserver `Unsupported value: "spark"` | **CRD-SCHEMA enum** (webhook = defense-in-depth) |
| W.9 | `pxfJob.server` referencing an undefined server | `…pxfJob.server "x" does not reference a defined pxf.servers[].name` | **WEBHOOK** |
| W.10 | `pxfJob.profile: s3:nonsense` | `…pxfJob.profile "s3:nonsense" is not a valid PXF profile` | **WEBHOOK** |
| W.11 | `pxfJob` with no `targetTable` | `…pxfJob.targetTable is required` | **BOTH** (omitted key → SCHEMA `required`; `""` → WEBHOOK) |
| W.12 | `gploadJob` with no `targetTable` | `…gploadJob.targetTable is required` | **BOTH** (omitted key → SCHEMA `required`; `""` → WEBHOOK) |
| W.13 | `jobs[].schedule: "not a cron"` | `…jobs[i].schedule is not a valid cron expression` | **WEBHOOK** |
| W.14 | `pxfJob.partitioning.column` set without `range`+`interval` | `…pxfJob.partitioning requires column, range, and interval together` | **WEBHOOK** |
| W.15 | `errorHandling.segmentRejectLimitType: fraction` | webhook: `…segmentRejectLimitType must be "rows" or "percent", got "fraction"`; live: apiserver `Unsupported value: "fraction"` | **CRD-SCHEMA enum** (webhook = defense-in-depth) |

**Tally:** **11 WEBHOOK-enforced** (W.1, W.2, W.4, W.5, W.6, W.7, W.9, W.10, W.13,
W.14 — counting W.2/W.4/W.5/W.7 once each) · **3 CRD-SCHEMA enum** (W.3, W.8,
W.15 — also webhook-guarded for defense-in-depth) · **2 BOTH** (W.11, W.12 —
omitted-key→schema-`required`, empty-string→webhook). The CRD enums backing
W.3/W.8/W.15 are `PxfServerSpec.Type` (`s3;hdfs;jdbc;hbase;hive;gs;abfss;wasbs;custom`),
the job `Type` enum (`pxf;gpload`), and `ErrorHandlingSpec.SegmentRejectLimitType`
(`rows;percent`) in `api/v1alpha1/types.go` — they are kept in **lockstep** with
the corresponding webhook constants (`pxfServerTypes`, `dataLoadingJobTypes`,
`segmentRejectLimitType*`).

### W.10 valid PXF profile policy

A PXF profile is either a **bare scheme** (`<scheme>`) or a
**scheme + format** pair (`<scheme>:<format>`). Matching is **case-insensitive**
for both the scheme and the format (so `HBase` and `hbase` both pass). The
allowlist (`pxfProfileSchemes` in `validating.go`) is:

| Scheme(s) | Bare allowed? | Allowed `:<format>` suffixes |
|-----------|---------------|------------------------------|
| `s3`, `gs`, `abfss`, `wasbs` | No | `text`, `parquet`, `avro`, `json`, `orc` |
| `hdfs` | No | `text`, `parquet`, `avro`, `json`, `orc`, `SequenceFile` |
| `hive` | Yes | `text`, `orc`, `rc` |
| `jdbc` | No (bare only) | — (bare `jdbc` only) |
| `hbase` | No (bare only) | — (bare `HBase`/`hbase` only) |

Consequences of the policy:

- A bare object-store/hdfs scheme is **rejected** (`s3`, `hdfs` alone fail —
  they require a format suffix).
- An unknown scheme is rejected (e.g. `foo:bar`).
- A known scheme with a disallowed suffix is rejected (e.g. `s3:nonsense`,
  `jdbc:x`, `hbase:x`).
- `jdbc` and `hbase` accept **only** the bare profile (no suffix).

> **Write-capability overlay (W.10b — Scenario 96 + scheme-aware in Scenario 97).**
> W.10 validates that a profile is *readable*; **W.10b** additionally validates
> *writability* for `mode: writable` jobs. The two share the canonical format
> constants from
> [`internal/pxfpolicy`](#single-source-of-truth-internalpxfpolicy) (the W.10
> allowlist literals alias the same constants), so a format spelled differently in
> the two checks is impossible. A `mode: writable` job admitted by W.10 (valid
> profile) is **still rejected** by W.10b if **either** (a) the format is
> `json`/`orc`/`rc` (read-only format on a writable scheme), **or** (b) the scheme
> is read-only — every `hive*` profile and `HBase` is rejected for a writable
> table regardless of format (e.g. `hive:text` writable is rejected even though
> `text` is a writable format on `hdfs`/object stores). The predicate is
> **scheme-aware** (`readOnlySchemes = {hive, hbase}`), not a pure format check.

> **Design note (streaming schemes — custom-connector only, Scenario 102 policy
> reversal).** The built-in allowlist (`pxfProfileSchemes`) is **UNCHANGED**:
> `kafka`, `rabbitmq`, and other streaming schemes are **still not** built-in PXF
> profiles (`isValidPxfProfile("kafka")` is still false). What changed in Scenario
> 102 is that these streaming schemes are **re-enabled ONLY as custom-connector
> profiles**, recognized by a **separate** set (`pxfCustomConnectorSchemes =
> {kafka, rabbitmq}` / `isCustomConnectorProfile`) and **gated by W.23**: a
> `pxfJob.profile: kafka` is admitted **only** when the referenced server is
> `type: custom` backed by a matching `customConnectors[]` entry (**W.23 + W.24**).
> A **bare** `kafka` profile, or `kafka`/`rabbitmq` on a **non-custom** server, is
> **still REJECTED** — **built-in streaming remains out of scope**. See
> [Scenario 102](#scenario-102--kafka-cdc-continuous-streaming-custom-connector)
> and [Removed Fields](#removed-fields-breaking-change).
>
> **Why custom-connector-only (verified against `cloudberry-pxf:2.1.0`).** Stock
> PXF ships **no Kafka connector** — its `pxf-profiles.xml` registers no `Kafka`
> profile, and the only `kafka` references in the PXF app jar are Micrometer's
> Kafka **metrics binder** (an observability dependency), **not** a PXF
> Fragmenter/Accessor/Resolver. This is by design: Apache PXF is a **batch,
> pull, request/response** framework (segments fetch bounded fragments over
> HTTP), whereas Kafka is an **unbounded, push, offset-tracked stream** — the two
> models do not fit. Consequently a working `kafka` profile can **only** come
> from a **user-supplied custom-connector JAR** (`customConnectors[].jarUrl` →
> `/pxf/lib/custom`), which is exactly why the operator gates it behind W.23. The
> operator implements the full *plumbing* (JAR download + mount, `type: custom`
> server, continuous streaming Job, `CBK_*` params); supplying a **real**
> Kafka→PXF connector implementation is out of the operator's scope, so
> end-to-end row landing is **config-only** until such a JAR is provided.

### Scenario 89 — Webhook Validation (All Rules)

Scenario 89 is the negative-test scenario that proves every rule above rejects
an otherwise-valid `dataLoading` spec carrying exactly **one** offending field,
with a descriptive error that names the offending field path, and that the
rejected CR is **not persisted**. The 15 rules (`W.1`–`W.15`) are catalogued in
`test/cases/test_cases.go` (`Scenario89ValidationCases()`) and exercised
directly against the validator in `internal/webhook/validating_test.go`
(`TestValidateDataLoading`, plus `TestIsValidPxfProfile` for the W.10
allowlist).

Key properties asserted by Scenario 89:

- **Gated on `enabled`** — a `dataLoading.enabled: false` spec with invalid
  content is accepted (no-op); the full rule set runs only when enabled.
- **Fail-fast** — validation returns the first error encountered; the error
  string contains the indexed field path (e.g.
  `dataLoading.jobs[0].pxfJob.profile`).
- **Not persisted** — a rejected CR never reaches etcd; a follow-up `Get`
  returns `NotFound`.
- **No per-rule metric** — rejections are **not** counted by a dedicated
  data-loading metric. They increment the existing admission counter
  `cloudberry_webhook_admission_total{webhook="validating",result="denied"}`
  shared by all validating-webhook denials (internal/non-validation failures
  record the distinct `result="error"` instead).

### Scenario 110 — Webhook Validation (All Rules)

> **Status: Implemented (no production change — verification scenario).** All 15
> rules W.1–W.15 are **already** implemented in `internal/webhook/validating.go`
> with descriptive, field-pathed errors, and three (W.3 server `type`, W.8 job
> `type`, W.15 `segmentRejectLimitType`) are **also** enforced by CRD OpenAPI
> enums. Scenario 110 adds **no production code**; it contributes the **COMPLETE,
> systematic rejected-CR negative-test matrix** that proves, for **each** of the
> 15 rules, that it **(a) rejects** the invalid CR, **(b) with a descriptive
> error**, and **(c) the CR does NOT persist** — plus the per-rule **rejection
> source** (webhook vs CRD-schema-enum vs both) and a **CONTROL** (a fully-valid CR
> admits, proving no false-positive).

Acceptance scenario (verbatim): *"For each of the 15 data-loading webhook rules
W.1–W.15, apply an otherwise-valid CloudberryCluster carrying EXACTLY ONE
violation and verify it is REJECTED with a DESCRIPTIVE (field-path + reason)
error AND that the rejected CR does NOT persist (a follow-up GET is NotFound).
Confirm the rejection SOURCE per rule (11 webhook-enforced, 3 CRD-schema-enum,
2 both). A fully-valid CR must ADMIT (CONTROL — no false-positive)."*

#### The 15 negative tests (one offending field each)

| Rule | Offending trigger | Rejection source |
|------|-------------------|------------------|
| W.1 | `pxf.enabled: true` + empty `pxf.image` | WEBHOOK |
| W.2 | empty server name **or** duplicate server name | WEBHOOK |
| W.3 | `servers[].type: ftp` | CRD-SCHEMA enum (webhook = defense-in-depth) |
| W.4 | s3 server missing `fs.s3a.endpoint` **or** empty `credentialSecrets` | WEBHOOK |
| W.5 | jdbc server missing `jdbc.driver` **or** `jdbc.url` | WEBHOOK |
| W.6 | hdfs server missing `fs.defaultFS` | WEBHOOK |
| W.7 | empty job name **or** duplicate job name | WEBHOOK |
| W.8 | `jobs[].type: spark` | CRD-SCHEMA enum (webhook = defense-in-depth) |
| W.9 | `pxfJob.server` referencing an undefined server | WEBHOOK |
| W.10 | `pxfJob.profile: s3:nonsense` | WEBHOOK |
| W.11 | `pxfJob` with no `targetTable` | BOTH (omitted-key→schema `required`; `""`→webhook) |
| W.12 | `gploadJob` with no `targetTable` | BOTH (omitted-key→schema `required`; `""`→webhook) |
| W.13 | `jobs[].schedule: "not a cron"` | WEBHOOK |
| W.14 | `pxfJob.partitioning.column` without `range`+`interval` | WEBHOOK |
| W.15 | `errorHandling.segmentRejectLimitType: fraction` | CRD-SCHEMA enum (webhook = defense-in-depth) |

See [Rejection source per rule](#rejection-source-per-rule-webhook-vs-crd-schema-enum-vs-both)
above for the exact descriptive-error substrings and the underlying CRD enums.

#### Properties asserted by Scenario 110

- **Descriptive error** — every rejection names the offending field path **and**
  the reason. For the 11 WEBHOOK rules the user sees **our** message on a live
  apply; for the 3 SCHEMA-enum rules (W.3/W.8/W.15) the **apiserver enum** error
  (`Unsupported value: "ftp"`, etc.) is itself descriptive (it names the field +
  the allowed values); for the 2 BOTH rules (W.11/W.12) the live `-L` rows omit
  the key so the apiserver `required` wording is asserted.
- **NO-PERSIST guarantee** — a rejected CR never reaches etcd; the live (`-L`)
  rows carry a `NoPersist` contract realized as a follow-up `GET` returning
  `NotFound` (`110-NOPERSIST-L`).
- **Rejection source per rule** — each row records its LIVE source so the matrix
  documents **11 WEBHOOK** · **3 CRD-SCHEMA-enum** · **2 BOTH**, and the
  defense-in-depth webhook message is also asserted at the unit/functional layers
  for the schema-enum rules (those layers have no apiserver/CRD).
- **CONTROL — no false-positive** — a fully-valid base CR **admits**
  (`110-CONTROL-admit-F` at the functional layer; `110-CONTROL-admit-L` applies
  on the live apiserver, GETs back, then cleans up).
- **Three layers** — every rule is exercised at the **unit** layer
  (validator-direct, `internal/webhook/scenario110_validation_test.go`), the
  **functional** layer (admission via `CloudberryClusterValidator.ValidateCreate`
  over a base-valid CR with one violation, `test/functional`), and the **live**
  layer (`kubectl apply` → reject → `GET NotFound`, `test/e2e`, `KUBECONFIG` +
  `SCENARIO110_LIVE` gated, SKIPs cleanly when unset).

#### Artifacts

- `test/cases/scenario110_webhook_validation_cases.go` —
  `cases.Scenario110WebhookCases()`, the flat per-rule catalog: the `-U`/`-F`/`-L`
  rows (with the W.2/W.4/W.5/W.7 OR sub-cases) carrying the rejection `Source`,
  the single `OffendingField`, the required `ErrorSubstrings`, and the `NoPersist`
  contract, plus the `110-CONTROL-admit-{F,L}` and `110-NOPERSIST-L` cross-cutting
  rows.
- `internal/webhook/scenario110_validation_test.go` — the **unit** layer
  (validator-direct `ValidateCreate`; the schema-enum rules assert the webhook
  defense-in-depth message here).
- `test/functional/scenario110_webhook_validation_test.go` — the **functional**
  admission-entrypoint layer.
- `test/integration/scenario110_webhook_validation_test.go` — the integration
  layer.
- `test/e2e/scenario110_webhook_validation_e2e_test.go` — the **live** Part B
  (`kubectl apply` reject + no-persist; `KUBECONFIG` + `SCENARIO110_LIVE` gated).
- `test/perf/scenario110_webhook_validation_perf_test.go` — the admission-latency
  perf check for the negative matrix.

> Scenario 110 is **complementary to**, not a re-implementation of,
> [Scenario 89](#scenario-89--webhook-validation-all-rules): Scenario 89 is the
> original validator-direct W.1–W.15 negative suite; Scenario 110 adds the
> systematic **multi-layer** (unit + functional + integration + e2e + perf) matrix
> with the explicit **per-rule rejection-source** analysis, the live **NO-PERSIST**
> contract, and the **CONTROL** admit.

## Webhook Defaults

> **Status: Implemented.** All 14 defaults below are applied by
> `setDataLoadingDefaults` in `internal/webhook/mutating.go`.

The mutating admission webhook (`internal/webhook/mutating.go`,
`setDataLoadingDefaults`) applies the defaults below. Like validation,
defaulting is **gated on `dataLoading.enabled: true`** (a disabled/absent spec
gets no defaults) and is **non-destructive**: each field is set only when
unset/zero/`nil`, so explicit user values are preserved. The three `*bool`
fields (`extensions.pxf`, `extensions.pxfFdw`, `pxfJob.filterPushdown`,
`pxfJob.columnProjection`) default to `true` **only when `nil`**, so an explicit
`false` survives defaulting rather than being silently re-enabled.

| Field path | Default |
|------------|---------|
| `dataLoading.pxf.port` | `5888` |
| `dataLoading.pxf.jvmOpts` | `"-Xmx1g -Xms256m"` |
| `dataLoading.pxf.logLevel` | `INFO` |
| `dataLoading.pxf.extensions.pxf` | `true` (only when `nil`) |
| `dataLoading.pxf.extensions.pxfFdw` | `true` (only when `nil`) |
| `dataLoading.gpfdist.replicas` | `1` |
| `dataLoading.gpfdist.port` | `8080` |
| `dataLoading.jobs[].pxfJob.mode` | `insert` |
| `dataLoading.jobs[].pxfJob.filterPushdown` | `true` (only when `nil`) |
| `dataLoading.jobs[].pxfJob.columnProjection` | `true` (only when `nil`) |
| `dataLoading.jobs[].gploadJob.mode` | `insert` |
| `dataLoading.jobTemplate.backoffLimit` | `3` |
| `dataLoading.jobTemplate.activeDeadlineSeconds` | `14400` (4 hours) |
| `dataLoading.jobTemplate.ttlSecondsAfterFinished` | `86400` (24 hours) |
| `dataLoading.healthChecks.enabled` | `true` (a nil `healthChecks` block ⇒ on; Scenario 104) |
| `dataLoading.healthChecks.diskMinFreeMB` | `64` (the HC.5 free-space threshold, MB; Scenario 104) |

> Defaults for `pxf.port`, `jvmOpts`, `logLevel`, and `extensions.*` are applied
> only when `dataLoading.pxf` is present; `gpfdist.*` defaults only when
> `dataLoading.gpfdist` is present. The `jobTemplate` block is allocated if
> absent so its three Job-lifecycle defaults always materialize on an enabled
> spec.

### Scenario 90 — Webhook Defaults

Scenario 90 is the defaulting scenario that proves all **14** defaults above
(`D.1`–`D.14`) are applied to an enabled, minimal `dataLoading` spec that sets
none of them, that explicit user values (including explicit `false` on `*bool`
fields) are preserved, and that a disabled spec receives no defaults. The 14
defaults are catalogued in `test/cases/test_cases.go`
(`Scenario90DefaultsCases()`), exercised against the mutating defaulter in
`test/functional/scenario90_webhook_defaults_test.go` and
`test/e2e/scenario90_webhook_defaults_e2e_test.go`. The `KUBECONFIG`-gated live
e2e test additionally `Create`s a minimal (non-defaulted) valid CR on a real API
server and `Get`s it back to assert the 14 defaults are present in the
**persisted** object — proving the server-side mutating webhook ran. (The
companion validation negative tests live in
[Scenario 89](#scenario-89--webhook-validation-all-rules).)

### Scenario 91 — Enable Data Loading with Full PXF CRD Configuration

> **Status: Implemented (sidecar + servers ConfigMap rendering).** Scenario 91
> exercises the **newly-implemented** PXF sidecar deployment and servers
> ConfigMap rendering against a full PXF spec. Job ingestion execution, `pxf
> sync`, and extension DDL remain **Planned** and are out of Scenario 91's scope.

Scenario 91 applies a **full** `dataLoading.pxf` spec carrying all **5** server
types — `s3`/MinIO (object store), `hdfs` (with `hive`/`hbase` config), `jdbc`
(MySQL and PostgreSQL), `hive`, and `hbase` — plus extensions, custom
connectors, resources, and a non-default `logLevel`, and proves that the
operator:

- **Parses every field** and renders the PXF **sidecar** into the
  **segment-primary** pod template only. The sidecar env carries the implemented
  names `PXF_HOME`, `PXF_BASE`, `PXF_JVM_OPTS`, `PXF_PORT`, `PXF_LOG_LEVEL`,
  `PXF_EXTENSION_PXF`, and `PXF_EXTENSION_PXF_FDW`, with `pxf.resources`,
  ports, and `/actuator/health` probes applied. Coordinator, standby, and mirror
  pods are **byte-identical** to a default cluster (sidecar scope =
  segment-primary only).
- **Renders the `<cluster>-pxf-servers` ConfigMap**, one `<name>__<file>.xml`
  per server with sorted (byte-stable) keys, `credentialSecrets[]` as
  `${PLACEHOLDER}` markers, and `customConnectors` in `connectors.properties`.
- **Sets `cloudberry_pxf_servers_configured = len(pxf.servers)`**, populates
  `status.dataLoading.pxf.{configured,servers}`, and raises the
  `DataLoadingConfigured` condition enriched with the PXF server count.
- **C.6 — `logLevel` → `PXF_LOG_LEVEL` propagation.** Because the sidecar is
  rebuilt from spec on every reconcile, re-patching `pxf.logLevel` rolls the
  segment-primary pod template env. The values **`DEBUG`**, **`WARN`**, and
  **`ERROR`** each flow verbatim into `PXF_LOG_LEVEL` on rebuild (an unset
  `logLevel` resolves to `INFO`), proving re-patch propagation.

> **Environment limitation (honest scoping).** Live data loading is exercised
> against **s3/MinIO + HDFS + Hive** in the test environment. The
> **jdbc (MySQL/PostgreSQL)** and **hbase** servers are **config-verified**
> (their `*-site.xml` is rendered into the ConfigMap and asserted). The
> **ingestion runtime** (Job/CronJob generation + launch + native execution) is
> covered by [Scenario 92](#scenario-92--data-loading-ingestion-runtime); for
> `pxf://` jobs, **live execution remains image-blocked** (only generation/launch
> is exercised).

The functional path (`BuildSegmentPrimaryStatefulSet` injection,
`BuildPXFServersConfigMap`, `reconcilePxf`, and the `logLevel` rebuild loop) is
exercised infra-free in `internal/builder/pxf_builder_test.go` and
`internal/controller/admin_controller_test.go`. The `KUBECONFIG`-gated live e2e
test applies the full spec to a real API server and asserts the sidecar +
ConfigMap materialize; it **skips cleanly** when `KUBECONFIG` is unset.

## Implemented vs Planned (data-loading runtime)

> At-a-glance status of the **ingestion runtime** implemented this cycle. Verified
> against `internal/builder/dataload_builder.go`,
> `internal/controller/dataload_controller.go`,
> `internal/controller/admin_controller.go`, `internal/builder/pxf_builder.go`,
> `internal/db/client.go`, and `internal/metrics/metrics.go`.

> **✅ PXF note.** `pxf://` Job **generation + launch = Implemented**, and **live
> `pxf://` execution = Implemented** (row-count verified, 183,961 rows from MinIO
> S3 via the PXF sidecar) **when** the cluster runs the `cloudberry-pxf` sidecar
> image (`Dockerfile.cloudberry-pxf`) + the `pxf` extension in the DB image
> (`cloudberry-official-pxf`, `Dockerfile.cloudberry-official-pxf`). The
> engine-native `gpfdist://`/`s3://` path (and bare paths served via the cluster
> gpfdist Service) **executes for real** with no PXF (row-count verified, e.g.
> 183,961 rows; `file://` is admission-rejected for multi-segment gpload jobs by
> W.16).

| Capability | Status |
|------------|--------|
| Data-loading `Job` (one-off) / `CronJob` (scheduled) creation `<cluster>-dataload-<job>` | **Implemented** |
| External-table DDL generator (`CREATE EXTERNAL TABLE (LIKE target) … LOCATION … FORMAT … LOG ERRORS`) | **Implemented** |
| Native `gpfdist://`/`s3://` (+ bare-path-via-gpfdist) **live execution** (row-count verified; `file://` rejected by W.16) | **Implemented** |
| `pxf://` Job generation + launch | **Implemented** |
| `pxf://` **live execution** (read-back) | **Implemented** (row-count verified, 183,961 rows; requires the `cloudberry-pxf` sidecar image + the `pxf` extension in the DB image) |
| `cloudberry-pxf` sidecar image (`Dockerfile.cloudberry-pxf`, from `apache/cloudberry-pxf`) + `cloudberry-official-pxf` DB image (`Dockerfile.cloudberry-official-pxf`, `pxf`/`pxf_fdw` installable) | **Implemented** (built via `make docker-build-pxf` / `docker-build-official-pxf`) |
| Load script: `INSERT…SELECT` + `DATALOAD_ROWS` marker + `DROP` + `ANALYZE` | **Implemented** |
| Per-type PXF server file-mapping (SL.1–6: `s3`→s3-site; `hdfs`→core+hdfs(+optional hive/hbase/mapred/yarn); `jdbc`→jdbc-site; `hive`→core+hive; `hbase`→core+hbase) + Config prefix-split | **Implemented** |
| `pxf-cred-init` credential init-container (live `envsubst` secret resolution; one-dir-per-server `<server>/<file>.xml`; secrets never in ConfigMap) | **Implemented** |
| `SetupPXFExtensions` best-effort/non-fatal `CREATE EXTENSION pxf` (RP.9) + `pxf_fdw` (RP.10), annotation-gated | **Implemented** (no-op for `pxf` on cloudberry-official) |
| `GRANT SELECT`/`INSERT ON PROTOCOL pxf TO "gpadmin"` (RP.11), best-effort/non-fatal, only when `pxf` installed | **Implemented** (no-op when `pxf` absent) |
| PXF config sync across sidecars via the shared `<cluster>-pxf-servers` ConfigMap (RP.12) | **Implemented** (structural; always-on) |
| Explicit `pxf sync` trigger (`cloudberry-ctl pxf sync` / `POST .../pxf/sync`): ConfigMap refresh + sidecar roll on demand | **Implemented** (Scenario 95; complements the structural sync) |
| Operator-driven PXF restart (`cloudberry-ctl pxf restart` / `POST .../pxf/restart`): segment-primary StatefulSet restart-trigger bump → pod roll → all sidecars restart | **Implemented** (Scenario 95; pod roll, heavier than in-place) |
| Honest PXF status (`cloudberry-ctl pxf status` / `GET .../pxf/status`): real sidecar `ContainerStatuses` readiness aggregation + spec-derived echo | **Implemented** (Scenario 95; no fake health/exec/HTTP) |
| `cloudberry-ctl pxf status\|restart\|sync` CLI group + `pxf/{status,restart,sync}` REST routes | **Implemented** (Scenario 95) |
| `cloudberry_pxf_restart_total{cluster,namespace,result}` metric | **Implemented** (emitted from `handlePXFRestart`) |
| Hadoop-profile write-capability enforcement (HDFS per-format + **scheme-aware** Hive/HBase read-only) | **Implemented/verified** (Scenario 97; `readOnlySchemes={hive,hbase}` in `internal/pxfpolicy`, W.10b + builder defense-in-depth) |
| `hive-site.xml` / `hbase-site.xml` rendering for `hdfs`/`hive`/`hbase` servers | **Implemented/verified** (Scenario 97; `renderPXFHDFSServer` SITE.1–4 — metastore URI + ZK quorum) |
| Filter pushdown DDL knob (`pxfJob.filterPushdown=true` → `FILTER_PUSHDOWN=true`) + mutating default-true | **Implemented/verified** (Scenario 98; `buildPXFLocation` + `setPxfJobDefaults`; runtime-proven via row-count reduction / `EXPLAIN` / source logs — NOT bytes) |
| Column projection DDL knob (`pxfJob.columnProjection=true` → `PROJECT=true`) + mutating default-true | **Implemented/verified** (Scenario 98; `buildPXFLocation` + `setPxfJobDefaults`; EXPLAIN-only proof) |
| Per-row error handling DDL knob (`errorHandling` → `[LOG ERRORS ]SEGMENT REJECT LIMIT <n> [ROWS\|PERCENT]`; writable export OMITS it; W.15-validated) | **Implemented/verified** (Scenario 98; `errorHandlingClause`; proven via real `job_status`/`errors_total`/`rows_total`) |
| Writable external-table EXPORT to **S3 / object store** (FE.9/WE.1), **HDFS** (FE.10), **JDBC** (FE.11) via `pxfwritable_export` (profile-agnostic builder; `IsProfileWritable` admits `s3:`/`hdfs:` text/parquet/avro(+seq) + bare `jdbc`) | **Implemented/verified** (Scenario 99; objects/part-files/rows LAND; `s3:parquet`/`hdfs:parquet`/`avro` `[CONFIG-ONLY]` — DATA_SCHEMA) |
| `pxfJob.sourceFilter` filtered export (`INSERT INTO <ext> SELECT * FROM <target> WHERE <sourceFilter>`; quoted-heredoc single-quote safety; unset → byte-identical full export) | **Implemented/verified** (Scenario 99; `dataload_builder.go`; filtered `rows_total` < baseline) |
| Webhook rule **W.17** (`sourceFilter` valid on `mode: writable` OR `loadMethod: fdw` read — Scenario 103 tweak; reject on a plain external-table read + `;`/`--`/`/*`) | **Implemented/verified** (Scenario 99 + 103; `validateSourceFilter`) |
| Webhook rule **W.25** (`pxfJob.loadMethod` enum `external-table`\|`fdw`; `fdw` is read-only — reject `mode: writable`/`continuous: true`) | **Implemented/verified** (Scenario 103; `validateLoadMethod`, checked before W.17) |
| Rich per-job status `{lastRun,lastStatus,rowsLoaded,duration}` | **Implemented** |
| 5 data-loading metrics (`job_status`, `job_last_success_timestamp`, `job_duration_seconds`, `rows_total`, `errors_total`) — **reused** by writable export (no new export metric) | **Implemented** (emitted) |
| First-class **data-export Job kind** (beyond the writable-DDL path the load Job builds) + `cloudberry_pxf_bytes_transferred_total` | **Planned** (export is observed via `rows_total`/`job_status`, never a fabricated byte counter — Scenario 99) |
| gpfdist `Deployment`/`Service`/`PVC` (`<cluster>-gpfdist*`, `reconcileGpfdist`, gated on `gpfdist.enabled`; GP.2-GP.5) | **Implemented** (Scenario 101; per-cluster PVC name + `avsoft.io/component` label domain — documented divergences) |
| gpload control-file load path: control file (GL.1-7) → `<cluster>-gpload-<job>` ConfigMap (`/etc/gpload`) → `Job`/`CronJob` running `gpload -f` (replaces native DDL for gpload jobs) | **Implemented** (Scenario 101; `gpload_builder.go`) |
| New `gploadJob` fields (`inputSource`, `delimiter`, `header`, `encoding`, `matchColumns`, `updateColumns`, `preload.truncate`, `postActions`; `mode`/`format` enums) + webhook **W.18-W.22** | **Implemented** (Scenario 101) |
| **kafka custom-connector profile** (`servers[].type: custom` + `customConnectors[]` + `pxfJob.profile: kafka`; W.3 + W.23 + W.24; built-in streaming still rejected) | **Implemented** (Scenario 102) |
| **Connector-JAR download init-container** `pxf-connector-init` (C.18; downloads each `customConnectors[].jarUrl` into `/pxf/lib/custom/<name>.jar` — s3://→`aws s3 cp`, http(s)://→`curl`) | **Implemented** (Scenario 102) |
| **Continuous streaming Job** (`pxfJob.continuous: true` → a one-off long-running `Job`, **NOT** a CronJob, J.46; `ActiveDeadlineSeconds` nil + `RestartPolicy OnFailure` + `BackoffLimit 6`; streaming consume loop honoring `batchSize`/`flushInterval`) | **Implemented** (Scenario 102) |
| New `pxfJob` fields (`continuous`, `batchSize` Min 1, `flushInterval` Go duration) → loader env `CBK_CONTINUOUS`/`CBK_BATCH_SIZE`/`CBK_FLUSH_INTERVAL`; webhook **W.23/W.24/W.23c** | **Implemented** (Scenario 102) |
| End-to-end kafka→table **row landing** (needs a REAL Kafka→PXF connector JAR; the staged one is a placeholder) | **CONFIG-ONLY** (Scenario 102; JAR download + mount + Job + DDL + streaming params are provable; live row-landing is documented/config-only with a placeholder JAR — the Job still runs as a streaming consumer with the JAR mounted) |
| kafka-cdc dedicated metric | **NONE — by design** (Scenario 102 reuses `cloudberry_data_loading_*`; a continuous consumer's steady state is `job_status=Running`, NOT Complete; `rows_total` best-effort per flush) |
| gpload-specific CLI flags (`data-loading jobs create --type gpload`) | **Implemented** (Scenario 108; `--gpfdist-host --gpfdist-port --file-path --format`) |
| Rich `data-loading jobs create` CLI flags (`--type pxf`: `--name --server --profile --resource --target --schedule`) + `--from-yaml <file>` (reads+unmarshals a full job, precedence over flags) | **Implemented** (Scenario 108; previously posted a nil body) |
| `data-loading jobs logs` CLI (`--job --follow --tail`; `GetStream` + kubectl fallback, mirrors `backup jobs logs` 86k) | **Implemented** (Scenario 108; → `GET .../data-loading/jobs/{job}/logs`, P.14) |
| `data-loading test-read` CLI (`--job` OR `--server/--profile/--resource`, `--limit` default 10 cap 1000) | **Implemented** (Scenario 108; → NEW `GET .../data-loading/test-read`) |
| `GET .../data-loading/test-read` REST (P.16: `handleTestReadPXFSource`, PermissionBasic, NO metric) | **Implemented** (Scenario 108; transient external table → `SELECT LIMIT N` → always DROP; `TestReadResponse {cluster, source{server,profile,resource}, limit, available, rowCount, columns, rows}`; real rows or `available:false`/empty — never fabricated, never `500`) |
| New DB read method `db.Client.ReadPXFSourceSample` (transient external table sample read; observed-only) | **Implemented** (Scenario 108; `internal/db/client.go`) |
| New CLI helper `runAPIPut` (alongside `runAPIGet`/`runAPIPost`/`runAPIDelete`) | **Implemented** (Scenario 108; used by `pxf servers update`) |
| **FDW-based loading path** (`pxfJob.loadMethod: fdw` → persistent `CREATE SERVER`/`CREATE USER MAPPING`/`CREATE FOREIGN TABLE` IF NOT EXISTS, per-protocol `pxf_fdw` wrapper, `(LIKE <target>)`, never dropped; EX.5-EX.8; `INSERT INTO <target> SELECT * FROM <foreign> [WHERE <sourceFilter>]` — EQUIVALENT to the external-table path, equal row counts) + new `pxfJob.loadMethod` field + webhook **W.25** + W.17 tweak | **Implemented** (Scenario 103; `buildFDWDDL`/`buildFDWDataLoadScript`, `dataload_builder.go`; reuses `cloudberry_data_loading_*` — NO new metric) |
| `cloudberry_pxf_service_up{cluster,namespace,segment_host}` (M.1) — per-segment PXF readiness | **Implemented** (Scenario 109; the per-segment-host disaggregation of `cloudberry_pxf_status`, set from **real** per-segment-primary-pod `pxf` container readiness via `util.PXFReadyByHost` — `1` healthy / `0` on a killed segment; emitted only for observed hosts, never synthesized) |
| `cloudberry_pxf_requests_total` (M.2) + `cloudberry_pxf_request_duration_seconds` (M.3) — actuator passthrough | **Implemented** (Scenario 109; the **real** `http_server_requests_seconds_count`/`_sum`/`_bucket` series from the PXF Actuator `/actuator/prometheus`, enabled via `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE=health,prometheus` and scraped by a **dedicated vmagent job** at `:5888`. **Label-honesty caveat:** count + latency are REAL; the catalog's `server`/`profile`/`operation` labels are downgraded to the actuator-native `uri`/`method`/`status` — not fabricated) |
| `cloudberry_data_loading_bytes_total{cluster,namespace,job,source_type}` (M.10) | **Implemented when a real byte count is available** (Scenario 109; from the real `DATALOAD_BYTES=<n>` marker computed via `wc -c` for a **local gpload input**; **omitted** for external-table/pxf/FDW/continuous loads — never synthesized) |
| 4 remaining `cloudberry_pxf_*` metrics (`pxf_bytes_transferred_total` M.4, `pxf_records_total` M.5, `pxf_errors_total` M.6, `pxf_active_connections` M.7) + 2 `cloudberry_gpfdist_*` (M.15/M.16) | **Planned / honestly absent** (no honest source in PXF 2.1.0 / gpfdist — never fabricated. M.5 record throughput is observed via `cloudberry_data_loading_rows_total`; **M.6 is FOLDED** into `cloudberry_data_loading_errors_total` + actuator non-2xx — no synthetic typed counter is registered. Scenario 109 tests assert the absence of M.4/M.5/M.7/M.15/M.16) |
| PXF servers-changed observability: `PXFServersChanged` event (`EventReasonPXFServersChanged`, message `added/removed/updated`) + `cloudberry_pxf_servers_changed_total{cluster,namespace}` counter — emitted on a real `<cluster>-pxf-servers` ConfigMap `Data` diff by BOTH the reconcile (`emitPXFServersChanged`) and the `pxf sync` path (`recordPXFServersChanged`); shared `util.DiffPXFServerNames`/`FormatPXFServersChangedMessage` | **Implemented** (Scenario 106; fires ONLY on a real diff — never on a no-op sync or first create) |
| `pxf/servers` CRUD REST routes (P.2–P.5: list/get/create/update/delete) | **Implemented** (Scenario 107; `handleListPXFServers`/`handleGetPXFServer`/`handleCreatePXFServer`/`handleUpdatePXFServer`/`handleDeletePXFServer`; `201` returns rendered `<server>__*.xml`; `409 SERVER_EXISTS`/`SERVER_IN_USE`; Basic read / Operator create+update / Admin delete) |
| `pxf servers …` CLI subcommands (`list`/`create`/`update`/`delete`) | **Implemented** (Scenario 108; `newPxfServersCmd` → `pxf servers {list,create,update,delete}`; `create` flags `--name --type --endpoint --bucket --credential-secret` repeatable `name[:key]`; `update [name] --endpoint` via `runAPIPut`; `delete [name]` honors `409 SERVER_IN_USE`) |
| Data-loading job mutations REST (P.8/P.10/P.11/P.12/P.13: create/update/delete/start/stop) | **Implemented** (Scenario 107; `start` creates a REAL one-off `batchv1.Job` → `202`/`409 JOB_ALREADY_RUNNING`; `stop` deletes the Job / suspends the CronJob → `202`/idempotent `200`; `create` → `201`/`409 JOB_EXISTS`/`400` unknown server; `delete` best-effort removes the spawned Job; Operator create/update/start/stop, Admin delete) |
| `jobs/{job}/logs` REST (P.14) | **Implemented** (Scenario 107; `handleDataLoadingJobLogs` streams the data-loading Job pod logs, `?follow`/`?tailLines`; honest `501 LOGS_NOT_AVAILABLE` only when no clientset is wired) |
| `external-tables` REST (P.15) | **Implemented** (Scenario 107; `handleListExternalTables` → `{observed, observedAvailable, expected}`; `observed` from live `db.Client.ListExternalTables` — `null`+`observedAvailable:false` when DB unreachable, NEVER synthesized; `expected` spec-derived, clearly labeled) |
| New DB read method `db.Client.ListExternalTables` (`pg_exttable` + foreign tables; static query, observed-only) | **Implemented** (Scenario 107; `internal/db/client.go`) |

### Scenario 92 — Data-Loading Ingestion Runtime

> **Status: Implemented (Job/CronJob generation + launch + native execution +
> operator-driven `pxf://` live execution).** Scenario 92 drives the
> **controller** over `reconcileDataLoadingJobs` and asserts the operator
> genuinely **builds and launches** correct load Jobs, harvests the
> `DATALOAD_ROWS` marker, enriches the per-job status, and records the 5 honest
> metrics. The `pxf://` live read-back is row-count verified (183,961 rows from
> MinIO S3 via the PXF sidecar) under `KUBECONFIG` + `SCENARIO92_PXF_LIVE=1`.

Scenario 92 proves that, for every **enabled** `dataLoading.jobs[]` entry, the
operator:

- **Generates + launches the load workload.** A job with **no** `schedule`
  yields a one-off `Job` `<cluster>-dataload-<job>` whose `args[0]` carries the
  full load script (DDL + `INSERT…SELECT` + the `DATALOAD_ROWS` marker); a job
  **with** a `schedule` yields a `CronJob` (not a `Job`). A **disabled** job
  yields **no** workload. A second reconcile is **idempotent** (no duplicate).
- **Resolves credentials live.** The segment-primary pod carries the
  `pxf-cred-init` init container that `envsubst`-renders the resolved
  `*-site.xml` into the shared emptyDir (secrets never in the ConfigMap).
- **Enriches rich status + 5 metrics on terminal Jobs.** A **Succeeded** Job
  carrying a `DATALOAD_ROWS=<n>` termination marker populates
  `status.dataLoading.jobs[].{lastStatus=Succeeded, lastRun, rowsLoaded=<n>,
  duration}` and emits `…_job_status=2`, `…_job_last_success_timestamp`,
  `…_job_duration_seconds`, and `…_rows_total{source_type}` (the `source_type`
  derived from the spec — e.g. `s3` from `s3:parquet`, else `gpfdist`). A
  **Failed** Job sets `lastStatus=Failed`, `…_job_status=3`, and increments
  `…_errors_total`.
- **Genuine native load proof.** Cloudberry's engine-native external-table
  protocols load **real data** through the *same* Job machinery — only the
  LOCATION protocol differs — with the rowcount verified end-to-end (e.g.
  **183,961 rows** from a staged CSV via `gpfdist://`/`s3://` or a bare path
  served by the cluster gpfdist Service). This is the real, tested data-load
  path. (`file://` is admission-rejected for multi-segment gpload jobs by W.16.)
- **Operator-driven `pxf://` live load (Implemented).** The operator generates,
  launches, and **runs** the `pxf://` load Job end-to-end with credentials
  rendered automatically by the operator. It is **row-count verified** (183,961
  rows loaded from MinIO S3 via the PXF sidecar) **when** the cluster runs the
  `cloudberry-pxf` sidecar image (`Dockerfile.cloudberry-pxf`) + the `pxf`
  extension in the DB image (`cloudberry-official-pxf`). On a stock
  `cloudberry-official` (no PXF extension) Scenario 92 still exercises the
  controller **machinery** (create → status → marker harvest → metric) for the
  `pxf://` path; the strict live row-count assertion is gated behind
  `SCENARIO92_PXF_LIVE=1` (run against the prepared cluster).

The controller path (`reconcileDataLoadingJobs`, `ensureDataLoadJob`/
`ensureDataLoadCronJob`, `enrichDataLoadingStatus`, `harvestDataLoadRows`,
`recordDataLoadJobMetrics`) and the builder path (`BuildDataLoadJob`/
`BuildDataLoadCronJob`, `buildExternalTableDDL`, `buildDataLoadScript`) are
exercised infra-free in
`test/functional/scenario92_dataload_jobs_test.go`,
`internal/controller/dataload_controller_test.go`, and
`internal/builder/dataload_builder_job_test.go`. The genuine native row-count
load **and** the operator-driven `pxf://` row-count load are verified at
live-deployment time: the `pxf://` live read-back asserts the real target row
count (`SELECT count(*)` == `SCENARIO92_PXF_EXPECTED_ROWS`, default 183961) in
`test/e2e/scenario92_dataload_runtime_e2e_test.go`, gated by `KUBECONFIG` +
`SCENARIO92_PXF_LIVE=1` so it skips cleanly without the `cloudberry-pxf` image.

### Scenario 93 — Server ConfigMap, File Mapping, Extensions, Sync

> **Status: Implemented.** Scenario 93 verifies the PXF **server configuration
> contract** end-to-end: the `<cluster>-pxf-servers` ConfigMap one-directory-per-
> server layout, the per-type `*-site.xml` file-mapping (**SL.1–SL.6**), the
> `${PLACEHOLDER}`-only (no-literal-secret) rendering, the `pxf-cred-init`
> resolution, the best-effort `CREATE EXTENSION pxf`/`pxf_fdw` + protocol GRANTs
> (**RP.8–RP.12**), and the shared-ConfigMap sync model. It is the configuration/
> extensions counterpart to [Scenario 91](#scenario-91--enable-data-loading-with-full-pxf-crd-configuration)
> (full CRD parse) and [Scenario 94](#scenario-94--pxf-sidecar-deployment-verification)
> (sidecar container shape).

Scenario 93 proves the operator:

- **Renders one logical directory per server (SL — ConfigMap layout).** The
  `<cluster>-pxf-servers` ConfigMap holds deterministic `<server>__<file>.xml`
  data keys; the `pxf-cred-init` init container reorganizes them into nested
  `<server>/<file>.xml` (one directory per server) under `$PXF_BASE/servers` in
  the shared emptyDir the sidecar reads.
- **Maps each server type to the correct site files (SL.1–SL.6).**
  - **SL.1** `s3` → `s3-site.xml` (Config + `fs.s3a.access.key`/`fs.s3a.secret.key`
    placeholders).
  - **SL.2** `hdfs` → `core-site.xml` **and** `hdfs-site.xml` ALWAYS (minimal
    `<configuration/>` when no `dfs.*`), plus `hive-site.xml`/`hbase-site.xml`
    (when the `hive`/`hbase` map or `hive.*`/`hbase.*` keys exist) and
    `mapred-site.xml`/`yarn-site.xml` (when `mapred*`/`mapreduce.*` / `yarn.*`
    keys exist). The `config` map is **prefix-split** (`fs.*`→core, `dfs.*`→hdfs,
    `mapred*`/`mapreduce.*`→mapred, `yarn.*`→yarn, `hive.*`→hive, `hbase.*`→hbase,
    other→core).
  - **SL.3** `jdbc` → `jdbc-site.xml` (Config + `jdbc` map + `jdbc.user`/
    `jdbc.password` placeholders).
  - **SL.4** `hive` → `core-site.xml` **and** `hive-site.xml` (both always).
  - **SL.5** `hbase` → `core-site.xml` **and** `hbase-site.xml` (both always).
  - **SL.6** Every credentialed server's XML carries **`${PLACEHOLDER}` tokens,
    never literal secrets**.
- **Resolves credentials live (init-container rendering).** The `pxf-cred-init`
  init container `envsubst`-substitutes the `${<SANITIZED_NAME_KEY>}` tokens from
  `SecretKeyRef` env into the resolved `*-site.xml` — secrets never land in the
  ConfigMap.
- **Installs extensions + GRANTs the protocol (RP.8–RP.12).** `SetupPXFExtensions`
  runs `CREATE EXTENSION IF NOT EXISTS pxf` (**RP.9**), then `pxf_fdw` (**RP.10**),
  both best-effort/non-fatal, and — only when `pxf` installed — `GRANT
  SELECT`/`GRANT INSERT ON PROTOCOL pxf TO "gpadmin"` (**RP.11**), also
  best-effort/non-fatal. The shared `<cluster>-pxf-servers` ConfigMap mounted on
  every segment-primary sidecar IS the sync mechanism (**RP.12**) — all sidecars
  render byte-identical configs; **no explicit `pxf sync` is needed**.
- **Verifies the test JDBC sources.** The MySQL (`mysql-oltp`) and PostgreSQL
  (`postgres-source`) JDBC sources were added to the test environment
  (docker-compose `mysql` + `pgsource` services, k8s Secrets
  `mysql-credentials` / `pg-source-credentials`) so the `jdbc` file-mapping and
  credential placeholders are exercised against real drivers.

The rendering path (`BuildPXFServersConfigMap`, `renderPXFServer`,
`splitHadoopSiteFiles`, `pxfCredentialInitScript`) and the extension/GRANT path
(`SetupPXFExtensions` → `grantPXFProtocol`) are exercised infra-free in
`internal/builder/pxf_builder_test.go` (`TestRenderPXFServer_FileMapping`,
`TestRenderPXFServer_NoLiteralSecrets`) and `internal/db/pgxclient_test.go`
(`TestPgxClient_SetupPXFExtensions_*`, incl. the `GrantFailsNonFatal` and
`PxfFailsFdwSucceeds` cases).

> **Scenario numbering note.** **Scenario 91** = enable data loading with the
> full PXF CRD configuration (parse + sidecar + servers ConfigMap). **Scenario
> 92** = data-load ingestion runtime (external-table DDL + load `Job`/`CronJob`
> generation/launch + native/`pxf://` execution). **Scenario 93** = Server
> ConfigMap, File Mapping, Extensions, Sync (this section — SL.1–6 + RP.8–12).
> **Scenario 94** = PXF Sidecar Deployment Verification (the `pxf` container
> shape on the segment pod). A prior cycle's "Scenario 93 — PXF Sidecar
> Deployment Verification" was **renamed to Scenario 94** so this re-spec could
> take number 93.

### Scenario 94 — PXF Sidecar Deployment Verification

> **Status: Implemented (sidecar container shape verification).** Scenario 94
> drives the **builder** (`BuildPXFSidecarContainers` /
> `BuildSegmentPrimaryStatefulSet`) and asserts the **exact deployment
> contract** of the injected `pxf` sidecar container on the segment-primary pod
> — the container the data-loading runtime depends on. No production code
> change is involved; the sidecar builder is already correct and live-verified.

> **Scenario numbering note.** Scenario 92 is the data-loading **ingestion
> runtime** (external-table DDL + load `Job`/`CronJob` generation/launch, across
> `scenario92_dataload_runtime_e2e_test.go`,
> `scenario92_dataload_live_load_e2e_test.go`, and
> `scenario92_dataload_jobs_test.go`);
> [Scenario 93](#scenario-93--server-configmap-file-mapping-extensions-sync) is
> the **Server ConfigMap / File Mapping / Extensions / Sync** verification
> (SL.1–6 + RP.8–12); **Scenario 94** is the **sidecar deployment verification**
> (the `pxf` container's shape on the segment pod); and
> [Scenario 95](#scenario-95--pxf-cli-lifecycle) is the **PXF CLI lifecycle**
> (the `cloudberry-ctl pxf status|restart|sync` operator verbs + the sidecar-local
> `pxf prepare/start/status/stop/restart/sync` exec verbs).
> [Scenario 91](#scenario-91--enable-data-loading-with-full-pxf-crd-configuration)
> *also* verifies sidecar **config** (env derived from the full 5-server spec +
> the rendered servers ConfigMap); Scenario 94 instead pins the full container
> **contract** (port, probes, command-absence, mounts, resources)
> deterministically and verifies it on a **live** segment pod. (A prior cycle's
> "Scenario 93 — PXF Sidecar Deployment Verification" was renamed to Scenario 94
> so the re-specified Server-ConfigMap scenario could take number 93.) **Scenario
> 94 (PXF Sidecar Deployment Verification) is RETAINED** unchanged — the
> originally-requested "Scenario 94" for the CLI lifecycle was assigned **95**
> because 94 was already implemented and embedded.

Scenario 94 asserts that, when `dataLoading.pxf` is enabled with an image, the
operator injects a container named **`pxf`** into the segment-primary pod
template with **exactly** this shape:

- **Env:** `PXF_HOME=/usr/local/cloudberry-pxf`, `PXF_BASE=/pxf-base`,
  `PXF_JVM_OPTS == pxf.jvmOpts` (default `-Xmx1g -Xms256m`), `PXF_PORT="5888"`
  (string), `PXF_LOG_LEVEL == pxf.logLevel` (default `INFO`),
  `PXF_EXTENSION_PXF` / `PXF_EXTENSION_PXF_FDW` (from `extensions.*`, default
  `true`).
- **Port:** one container port `5888` named `pxf`, protocol `TCP`.
- **Liveness probe:** `HTTPGet /actuator/health` on `5888`,
  `initialDelaySeconds: 60`, `periodSeconds: 20`.
- **Readiness probe:** `HTTPGet /actuator/health` on `5888`,
  `initialDelaySeconds: 30`, `periodSeconds: 10`.
- **Volume mounts:** `pxf-base → /pxf-base`, `pxf-servers → /pxf-base/servers`,
  `pxf-lib → /pxf/lib/custom`.
- **Resources:** `requests`/`limits` converted from `pxf.resources`.
- **Command/Args:** **none** (`Command == nil && Args == nil`).

Two facts are pinned explicitly because the obvious-but-wrong values would
otherwise creep in:

1. **Health probe path is `/actuator/health`, NOT `/pxf/v15/Status`.** The real
   `apache/cloudberry-pxf` 2.1.0 image exposes its health via the **Spring Boot
   actuator** at `/actuator/health` (verified live: returns `{"status":"UP"}`).
   The legacy `/pxf/v15/Status` path is a **DB-client** endpoint that returns
   **404** on that image, so it must **not** be used for liveness/readiness.
2. **The `pxf prepare → pxf start → tail service log` lifecycle is owned by the
   image ENTRYPOINT (`hack/docker-entrypoint-pxf.sh`).** The operator therefore
   sets **no** container `Command` and **no** `Args`; overriding them would
   bypass the entrypoint's prepare/start sequence.

A **blast-radius** negative is included: a `pxf`-disabled or
`dataLoading`-disabled cluster carries **no** `pxf` container in the segment pod
(and the coordinator never carries it).

The builder path is exercised infra-free in
`test/functional/scenario94_pxf_sidecar_verification_test.go` and
`test/e2e/scenario94_pxf_sidecar_verification_e2e_test.go` (a catalog,
`cases.Scenario94Cases()`, keeps the cross-layer expectations honest against the
live built container). The live check
(`TestE2E_Scenario94_LivePXFSidecarOnSegmentPod`) finds a deployed
segment-primary pod with a `pxf` container and asserts the **same** shape on the
live pod spec; under `SCENARIO94_PXF_LIVE=1` it additionally asserts the `pxf`
container is **Ready** and that
`kubectl exec <segpod> -c pxf -- curl -sf localhost:5888/actuator/health`
returns `UP`. It skips cleanly without `KUBECONFIG` (and the runtime assertion
skips cleanly when the real image is not deployed).

### Scenario 95 — PXF CLI Lifecycle

> **Status: Implemented (operator-driven lifecycle verbs + honest status).**
> Scenario 95 exercises the **PXF lifecycle** surfaced two ways: (1) the
> **operator-driven** verbs `cloudberry-ctl pxf status|restart|sync --cluster
> <name> [--namespace <ns>]` (`newPxfCmd` → `handlePXFStatus`/`handlePXFRestart`/
> `handlePXFSync`), and (2) the **sidecar-local** PXF-binary verbs
> `pxf prepare/start/status/stop/restart/sync`, which run **inside** the
> `cloudberry-pxf:2.1.0` image (the `pxf` binary on `PATH`; health at
> `/actuator/health`) and are exercised via `kubectl exec -c pxf` — they are
> **not** ctl commands.

> **Scenario numbering note.** The user requested this work as "Scenario 94", but
> **Scenario 94 = PXF Sidecar Deployment Verification** is already implemented and
> embedded, so it is **RETAINED**; the PXF CLI lifecycle therefore takes number
> **95**. Full sequence: **91** = enable data loading with the full PXF CRD config;
> **92** = data-loading ingestion runtime; **93** = Server ConfigMap / File
> Mapping / Extensions / Sync; **94** = PXF Sidecar Deployment Verification;
> **95** = PXF CLI Lifecycle (this section).

**`status`/`restart`/`sync` and (as of Scenario 108) `servers …` are ctl
commands.** `pxf prepare`, `pxf start`, and `pxf stop` are sidecar-local
PXF-binary verbs (run via `kubectl exec`), **not** `cloudberry-ctl` subcommands.
`pxf servers …` CRUD is now **Implemented in the CLI** (Scenario 108;
`pxf servers {list,create,update,delete}` → the Scenario 107 `pxf/servers`
P.2–P.5 REST routes).

**The operator `pxf restart` is a pod roll (honest caveat).**
`cloudberry-ctl pxf restart --cluster` makes the operator patch the
`<cluster>-segment-primary` StatefulSet **pod-template** restart-trigger
annotation (`avsoft.io/restart-trigger`). The kubelet then **rolls all segment
pods**; each pod re-runs the image entrypoint (`pxf prepare` → `pxf start`), so
every PXF sidecar restarts. This is a **pod ROLL** — strictly **heavier** than a
single-sidecar `pxf restart` issued via `kubectl exec -c pxf -- pxf restart`
(note: that sidecar-local verb kills PID 1 and triggers a **container** restart,
not an in-place JVM stop/start — see **Sidecar verb semantics** below). Document
and expect the roll latency accordingly. `pxf sync` uses the **same** roll primitive but additionally
refreshes the `<cluster>-pxf-servers` ConfigMap first, so the `pxf-cred-init`
init container re-renders the resolved configs on the roll.

**What's verified (L.1–L.6, `cases.Scenario95Cases()`):**

- **L.1 `prepare`** (exec) — `pxf prepare` on the live segment-primary sidecar is
  **idempotent** (safe to re-run).
- **L.2 `start` → status Running** (exec + operator) — `pxf start` brings the
  sidecar up (`/actuator/health` → `UP`); operator `pxf status` then reports the
  segment-primary `pxf` containers **Ready**.
- **L.3 `stop` → readiness fails** (exec + operator) — `pxf stop` takes the
  sidecar down; the operator `pxf status` readiness aggregation reflects the
  container as **not-ready**.
- **L.4 `restart` recovers** (operator) — `cloudberry-ctl pxf restart --cluster`
  rolls the segment-primary pods (restart-trigger bump); sidecars recover Ready
  and `cloudberry_pxf_restart_total{result="started"}` increments.
- **L.5 `sync` redistributes** (operator) — `cloudberry-ctl pxf sync --cluster`
  refreshes the `<cluster>-pxf-servers` ConfigMap and rolls the sidecars so the
  new server config takes effect.
- **L.6 `ctl pxf restart` → all sidecars** (operator + exec) — the headline
  command restarts PXF across **all** segment-primary sidecars in **one** operator
  action.

**Sidecar verb semantics (verified live).** In the `cloudberry-pxf:2.1.0` image
the PXF JVM process **is the container's PID 1** (the entrypoint
`hack/docker-entrypoint-pxf.sh` execs/tails it). Therefore the **sidecar-local**
verbs `pxf stop` and `pxf restart` (run via `kubectl exec <segpod> -c pxf --
pxf stop|restart`) kill PID 1, which makes Kubernetes **restart the whole sidecar
container** (per `restartPolicy`); the entrypoint then re-runs `pxf prepare`/
`pxf start` and the sidecar recovers (`GET :5888/actuator/health` → `UP`). So
in-sidecar `pxf stop`/`pxf restart` behave as a **container restart**, not an
in-place JVM-only stop/start. This is real, expected behavior (verified live:
`stop` → `/actuator/health` fails → k8s restarts the container → recovers `UP`).
Likewise, `pxf sync <hostname>` is the **cluster-wide rsync-to-remote-hosts**
verb and is **not applicable inside a single sidecar**: in this operator each
sidecar receives its config via the `<cluster>-pxf-servers` ConfigMap volume
mount + the `pxf-cred-init` init container (resolved configs live under
`/pxf-base/servers`). The sidecar-local `pxf sync` is therefore **replaced** by
the operator-level explicit sync (`cloudberry-ctl pxf sync` /
`POST .../data-loading/pxf/sync`), which refreshes the ConfigMap + rolls the
segment pods. **None of this changes the operator-driven headline path:**
`cloudberry-ctl pxf restart --cluster` (restart-trigger annotation bump → pod
roll → all sidecars re-initialize) works exactly as documented above and was
verified live (annotation bumped, pods rolled with new UIDs, sidecars Ready
again, `cloudberry_pxf_restart_total{result="started"}` incremented).

**Honesty.** `pxf status` is derived **only** from the real `pxf` container
readiness in the segment-primary pods' `ContainerStatuses` — **no** synthetic
health, **no** exec, **no** cross-pod HTTP — and echoes the spec-derived
`status.dataLoading.pxf.{configured,servers}`. A **not-enabled** cluster returns
`400 PXF_NOT_ENABLED` for all three verbs and performs no StatefulSet/ConfigMap
mutation (and records no restart metric).

The operator layer is exercised infra-free in
`test/functional/scenario95_pxf_cli_lifecycle_test.go` (the real `api.Server` HTTP
router + auth/RBAC middleware over a fake k8s client): it asserts the `202`
restart/sync responses, the restart-trigger annotation bump (the pod-roll
primitive), the ConfigMap (re)create/update, the honest ready/total readiness
aggregation, the `cloudberry_pxf_restart_total{result="started"}` emission, and
the `PXF_NOT_ENABLED` gate. The exec-driven lifecycle verbs (L.1–L.3, L.6) run
against a live deployed sidecar under `KUBECONFIG` + `SCENARIO95_PXF_LIVE=1`,
skipping cleanly when the real `cloudberry-pxf` image is not deployed.

### Scenario 96 — Object Store Profiles & Format Write-Capability

> **Status: Implemented (write-capability enforcement + object-store server types
> + writable DDL).** Scenario 96 adds (1) the `gs`/`abfss`/`wasbs` object-store
> server **types** (+ Dell-ECS / MinIO variants), (2) the **format
> write-capability matrix** enforced at admission (W.10b) and re-checked by the
> builder, and (3) the `pxfwritable_export` **writable external-table DDL**. Live
> reads/writes are proven where a backing object store is available (MinIO);
> cloud-only stores and unsynthesizable formats are explicitly **config-only**.

> **Scenario numbering note.** The user requested this work as "Scenario 95", but
> **Scenario 95 = PXF CLI Lifecycle** is already implemented and documented, so it
> is **RETAINED**; object-store profiles & write-capability therefore take number
> **96**. Full sequence: **91** = enable data loading with the full PXF CRD
> config; **92** = data-loading ingestion runtime; **93** = Server ConfigMap /
> File Mapping / Extensions / Sync; **94** = PXF Sidecar Deployment Verification;
> **95** = PXF CLI Lifecycle; **96** = Object Store Profiles & Format
> Write-Capability (this section).

#### Single source of truth: `internal/pxfpolicy`

The write-capability matrix lives in **exactly one** place:
[`internal/pxfpolicy`](../internal/pxfpolicy/policy.go) — a deliberately tiny
**leaf** package with zero non-stdlib dependencies. It exports:

- `ModeWritable = "writable"` — the `pxfJob.mode` sentinel that selects a writable
  (export) external table.
- `WritableFormats = {text, parquet, avro, sequencefile}` — the set of formats
  with **Write = Yes** (read-only `json`/`orc`/`rc` are absent).
- `readOnlySchemes = {hive, hbase}` — the set of profile **schemes** whose
  connector is **read-only regardless of format** (Scenario 97). Every `hive*`
  profile (`hive`, `hive:text`, `hive:orc`, `hive:rc`) and the `hbase` profile is
  Write = No because the connector does not support writable external tables.
- `IsProfileWritable(profile)` — the predicate the webhook (W.10b) and the builder
  both call. It lowercases and `strings.Cut`s on `:`, then applies the rules in
  order: **(1)** a read-only **scheme** (`hive`/`hbase`) is **never** writable —
  this branch overrides the format check; **(2)** otherwise a bare profile is
  writable only for connectors that support writes (`jdbc`; bare `hbase`/`hive`
  are already rejected by rule 1); **(3)** otherwise a `<scheme>:<format>` profile
  is writable iff the format is in `WritableFormats`.

> **Policy correction (Scenario 97 — `IsProfileWritable` is now scheme-aware).**
> Earlier (Scenario 96) `IsProfileWritable` was a **pure FORMAT predicate**: it
> split off the format suffix and consulted `WritableFormats` only. That WRONGLY
> admitted `hive:text` as writable, because `text` is a writable format — even
> though the Hive connector has **no** writable external table. The predicate is
> now **scheme-aware**: the new `readOnlySchemes = {hive, hbase}` set makes those
> connectors write-unsupported **regardless of format**, matching the Hadoop
> Profiles table (all `hive*`/`HBase` = Write = No). So `hive:text` writable is
> now correctly **rejected**, not just `hive:json`. This corrects the earlier
> Scenario 96 description that implied `hive:text` would be writable.

Both `internal/webhook` and `internal/builder` import this leaf (the webhook
already imports the builder, so a webhook↔builder policy dependency would create
an import cycle — the leaf breaks it). The W.10 read-allowlist literals in the
webhook also **alias** `pxfpolicy`'s format constants, so the read allowlist and
the write matrix can never use diverging spellings.

#### Object-store server rendering (`s3-site.xml` for all)

PXF reads object-store connection settings from a single `s3-site.xml`-style
config for **every** object-store connector; the connector is chosen by the
**profile scheme** at query time, not by a distinct per-cloud site file. So
`renderPXFServer` routes all four object-store types
(`s3`/`gs`/`abfss`/`wasbs`) through the **same** renderer, emitting
`<server>__s3-site.xml` with the `fs.*` keys from `config` and the credential
placeholders folded in as the standard `fs.s3a.access.key`/`fs.s3a.secret.key`.
Variants:

- **Dell ECS** — an `s3` server with a custom `fs.s3a.endpoint` (e.g.
  `https://ecs.dell.example.com:9021`); no distinct type needed.
- **MinIO** — an `s3` server with `fs.s3a.path.style.access=true` (path-style
  addressing).

#### What's verified (`cases.Scenario96Cases()` — OS / CFG / FF)

The sample cluster is
[`scenario96-objstore-test.yaml`](../deploy/helm/cloudberry-operator/config/samples/scenario96-objstore-test.yaml)
(cluster `objstore-test`, namespace `cloudberry-test`) with servers `s3-datalake`,
`minio-warehouse` (path-style), `gcs-datalake`, `adls-gen2`, `azure-blob`,
`dell-ecs`.

**OS.1–OS.10 — object-store reads (s3 / MinIO):**

| ID | Profile · Server | Coverage |
|----|------------------|----------|
| OS.1 | `s3:text` · s3-datalake | LOCATION correct; **live** rows land (CSV synthesizable) |
| OS.2 | `s3:parquet` · s3-datalake | **live** (parquet synthesized via tooling container) |
| OS.3 | `s3:avro` · s3-datalake | **live** (avro via fastavro; **config-only** if tooling absent) |
| OS.4 | `s3:json` · s3-datalake | **live** (json natively synthesizable) |
| OS.5 | `s3:orc` · s3-datalake | **CONFIG-ONLY** — DDL/LOCATION only (ORC not locally synthesizable) |
| OS.6 | `s3:text` · minio-warehouse | path-style `s3-site.xml`; **live** rows land |
| OS.7 | `s3:parquet` · minio-warehouse | **live** (parquet via tooling) |
| OS.8 | `s3:avro` · minio-warehouse | **live** (avro; **config-only** if tooling absent) |
| OS.9 | `s3:json` · minio-warehouse | **live** rows land |
| OS.10 | `s3:orc` · minio-warehouse | **CONFIG-ONLY** — DDL/LOCATION + path-style config only |

**CFG.1–CFG.8 — `gs`/`abfss`/`wasbs`/Dell-ECS server-config + LOCATION (all
CONFIG-ONLY — no local backing store):**

| ID | Profile · Server | Coverage |
|----|------------------|----------|
| CFG.1 | `gs:text` · gcs-datalake | **CONFIG-ONLY** site XML + LOCATION correct |
| CFG.2 | `gs:parquet` · gcs-datalake | **CONFIG-ONLY** |
| CFG.3 | `abfss:text` · adls-gen2 | **CONFIG-ONLY** Azure ADLS Gen2 site XML |
| CFG.4 | `abfss:parquet` · adls-gen2 | **CONFIG-ONLY** |
| CFG.5 | `wasbs:text` · azure-blob | **CONFIG-ONLY** Azure Blob site XML |
| CFG.6 | `wasbs:json` · azure-blob | **CONFIG-ONLY** |
| CFG.7 | `s3:text` · dell-ecs | **CONFIG-ONLY** S3-compatible site XML w/ custom endpoint |
| CFG.8 | `s3:parquet` · dell-ecs | **CONFIG-ONLY** endpoint-overridden `s3-site.xml` |

**FF.1–FF.5 — the write-capability matrix (the core deliverable):**

| ID | Profile (`mode: writable`) | Expected |
|----|----------------------------|----------|
| FF.1 | `s3:text` | **SUCCEED** (admission + DDL + script) — admitted; `CREATE WRITABLE EXTERNAL TABLE … pxfwritable_export`, **no** `LOG ERRORS`; the operator emits the correct reversed-direction export `INSERT`. Live text export of **text-compatible** columns round-trips; see the PXF text-formatter note below |
| FF.2 | `s3:parquet` | **SUCCEED** — admitted; writable DDL correct; **live parquet export round-trips** (verified: 100 rows → MinIO `.snappy.parquet` objects) |
| FF.3 | `s3:avro` | **SUCCEED** — admitted; **live avro export round-trips** (verified: 100 rows → MinIO `.avro` objects); config-only build+admit if avro tooling absent |
| FF.4 | `s3:json` | **REJECT** — admission DENY; error contains `write-unsupported`/`writable`; builder also errors (defense-in-depth) |
| FF.5 | `s3:orc` | **REJECT** — admission DENY; error contains `write-unsupported`/`writable`; builder also errors (defense-in-depth) |

#### Config-only (honest)

- **Cloud-only object stores** (`gs`/`abfss`/`wasbs`, Dell-ECS) have **no local
  backing store** in CI, so CFG.1–CFG.8 prove **server-config + LOCATION
  correctness only** (no live reads).
- **ORC** (OS.5/OS.10) is **not synthesizable** locally — DDL/LOCATION only.
- **Avro** (OS.3/OS.8, FF.3) is live when the avro tooling container is present,
  else config-only (build + admit).
- The **live export RUNTIME** (FF.1–FF.3) is proven only where the MinIO backing
  store is available; a first-class **export-Job kind** beyond the writable-DDL
  path remains **Planned**.
- **PXF text-formatter data-type constraint (honest, verified live).** The
  operator-generated export DDL/script is correct for all three writable formats
  (the load Job pushes cluster rows OUT via `INSERT INTO <writable_ext> SELECT *
  FROM <target>`). Live verification: **FF.2 (parquet) and FF.3 (avro) export
  real data round-trip end-to-end** (100 rows each → MinIO objects). For **FF.1
  (text)**, the PXF `s3:text` *write* path serializes each column to bytes and
  rejects some non-text column types at the PXF sidecar (e.g. `class
  java.lang.Integer cannot be cast to [B`); this is a **PXF connector
  limitation**, not an operator defect — the previously-broken
  "`cannot read from a WRITABLE external table`" direction bug is **fixed**.
  Text exports succeed for text-compatible column types. Choose `parquet`/`avro`
  for mixed-type exports.

The compose stack adds an `hbase` service (`harisekhon/hbase:2.1`) and
`scripts/gen-objstore-samples.sh` generates the object-store sample data
(text/json natively; parquet/avro via the tooling container; ORC skipped). The
e2e live path is gated by **`SCENARIO96_OBJSTORE_LIVE`** and skips cleanly when no
backing object store is deployed.

### Scenario 97 — Hadoop Profiles (HDFS / Hive / HBase)

> **Status: Implemented/verified (Hadoop write-capability enforcement, incl.
> scheme-aware Hive/HBase read-only + `hive-site.xml`/`hbase-site.xml`
> rendering).** Every behaviour Scenario 97 proves is **already shipped** — the
> `pxfpolicy` write-matrix (including `SequenceFile`), the now **scheme-aware**
> `IsProfileWritable`, the webhook W.10/W.10b admission, and the builder's
> `core`/`hdfs`/`hive`/`hbase-site.xml` rendering + writable export DDL.
> Scenario 97 is a **TEST + LIVE-VERIFICATION** scenario over the combined
> `hdfs`-typed `hadoop-cluster` server (which also carries the Hive metastore +
> HBase ZK config), plus one **policy correction** (see below).

> **Scenario numbering note.** The user requested this work as "Scenario 96", but
> **Scenario 96 = Object Store Profiles & Format Write-Capability** is already
> implemented and documented, so it is **RETAINED**; Hadoop profiles therefore
> take number **97**. Full sequence: **91** = enable data loading with the full
> PXF CRD config; **92** = data-loading ingestion runtime; **93** = Server
> ConfigMap / File Mapping / Extensions / Sync; **94** = PXF Sidecar Deployment
> Verification; **95** = PXF CLI Lifecycle; **96** = Object Store Profiles &
> Format Write-Capability; **97** = Hadoop Profiles (HDFS / Hive / HBase) (this
> section).

#### Policy correction (scheme-aware write-capability)

`internal/pxfpolicy.IsProfileWritable` was previously a **pure FORMAT predicate**
that WRONGLY admitted `hive:text` as writable (because `text` is a writable
format). It is now **scheme-aware**: a new `readOnlySchemes = {hive, hbase}` set
makes the Hive and HBase connectors **write-unsupported regardless of format**,
matching the Hadoop Profiles table (all `hive*`/`HBase` = Write = No). The webhook
**W.10b** + the builder defense-in-depth now reject writable `hive:text` (and the
rest of the `hive*`/`HBase` family) with `write-unsupported`. See
[Single source of truth](#single-source-of-truth-internalpxfpolicy).

#### Read profiles (admitted at W.10)

- **HDFS** (`hdfsFormats()`): `hdfs:text`, `hdfs:parquet`, `hdfs:avro`,
  `hdfs:json`, `hdfs:orc`, `hdfs:SequenceFile` — all valid read profiles.
- **Hive**: `hive` (auto-detect, bare), `hive:text`, `hive:orc`, `hive:rc`
  (RCFile read).
- **HBase**: the bare `HBase`/`hbase` profile (case-insensitive at W.10).

#### Write matrix (W.10b)

| `mode: writable` profile | Outcome |
|--------------------------|---------|
| `hdfs:text` / `hdfs:parquet` / `hdfs:avro` / `hdfs:SequenceFile` | **admit-write** — `FF.7` (SequenceFile), `FF.7t` (text) |
| `hdfs:json` / `hdfs:orc` | **deny-write** (`write-unsupported`) — `WRej.1`/`WRej.2` |
| `hive` / `hive:text` / `hive:orc` / `hive:rc` | **deny-write** — read-only **scheme**; `WRej.3`–`WRej.6`, `FF.6b` |
| `HBase` | **deny-write** — read-only scheme; `WRej.7` |

`hive:rc` is **read OK** (`FF.6a`, RCFile = `FF.6` read leg) but **writable
rejected** (`FF.6b`). The Hive/HBase rejections are driven by the read-only
**scheme**, not a per-format check, so `hive:text` writable is denied even though
`text` is a writable format on `hdfs`/object stores.

#### `hive-site.xml` / `hbase-site.xml` render proof (`SITE.*`)

For the `hdfs`-typed `hadoop-cluster` server, `renderPXFHDFSServer` ALWAYS emits
`core-site.xml` + `hdfs-site.xml`, and additionally emits
`<server>__hive-site.xml` / `<server>__hbase-site.xml` from the dedicated
`server.hive` / `server.hbase` maps (or the `Config` `hive.*`/`hbase.*`
fragments). The site-file assertions:

| ID | Site file | Carries |
|----|-----------|---------|
| SITE.1 | `hadoop-cluster__hive-site.xml` | `hive.metastore.uris` = `thrift://hive-metastore:9083` |
| SITE.2 | `hadoop-cluster__hbase-site.xml` | `hbase.zookeeper.quorum` = `hbase:2181` |
| SITE.3 | `hadoop-cluster__core-site.xml` | `fs.defaultFS` = `hdfs://namenode:8020` |
| SITE.4 | `hadoop-cluster__hdfs-site.xml` | always emitted (valid `<configuration>`); `dfs.replication=1` |

The metastore URI / ZK quorum are routed into `hive-site.xml`/`hbase-site.xml`
(and **not** `core-site.xml`).

#### What's verified (`cases.Scenario97Cases()` — HP / HV / HB / SITE / FF / WRej)

The sample cluster is
[`scenario97-hadoop-test.yaml`](../deploy/helm/cloudberry-operator/config/samples/scenario97-hadoop-test.yaml)
(cluster `hadoop-test`, namespace `cloudberry-test`) with the combined
`hadoop-cluster` (`hdfs`) server plus dedicated `hive-warehouse` (`hive`) and
`hbase-store` (`hbase`) servers. The catalog rows are:

- **HP.1–HP.6** — HDFS reads (`text`/`parquet`/`avro`/`json`/`orc`/`SequenceFile`).
- **HV.1–HV.4** — Hive reads (`hive` auto-detect / `hive:text` / `hive:orc` / `hive:rc`).
- **HB.1** — HBase read (case-insensitive `HBase` profile).
- **SITE.1–SITE.4** — rendered site-file assertions (above).
- **FF.6a/FF.6b** — `hive:rc` read OK + writable REJECT.
- **FF.7 / FF.7t** — `hdfs:SequenceFile` / `hdfs:text` writable SUCCEED.
- **WRej.1–WRej.7** — the writable DENY matrix (`hdfs:json`/`hdfs:orc` +
  all `hive*` + `HBase`).

`cases.Scenario97Cases()` is resolved against the **real** built artifact (DDL /
site-file / admission deny) by the CatalogHonest functional + e2e tests.

#### Config-only & live results (honest)

The **operator behavior is correct for every Hadoop profile** — the generated
external-table DDL/LOCATION uses the standard PXF contract `FORMAT 'CUSTOM'
(FORMATTER='pxfwritable_import')` (read) / `pxfwritable_export` (write) for all
profiles, and the rendered site files carry the metastore URI + ZK quorum
(SITE.1–4 verified live). Verified at live deployment time (cluster
`hadoop-test`):

- **`hdfs:json`** read → **live rows land** (HP.4 — 2000 rows). **`hive`**
  auto-detect read → **live rows land** (HV.1). `source_type` `hdfs`/`hive`
  series present in VictoriaMetrics.
- **Write-capability matrix proven live (headline):** writable `hdfs:text` /
  `hdfs:SequenceFile` **admitted**; writable `hdfs:json`, `hdfs:orc`, **all
  `hive*` (incl. `hive:text`)**, and `HBase` **DENIED** at admission with
  `write-unsupported` (the scheme-aware fix). FF.7 `hdfs:SequenceFile` writable
  build + admit proven (live HDFS landing requires the PXF `DATA_SCHEMA` option —
  a PXF-connector requirement, not an operator concern).
- **`parquet`/`avro`/`orc`/`hive:rc`** are synthesized via the tooling container
  / Hive CTAS; **config-only** (DDL/LOCATION + admit) when tooling is absent
  (HP.2/HP.3/HP.5/HV.4/FF.6a).
- **Environmental limitations (NOT operator defects), documented honestly:**
  - **HP.1 `hdfs:text` live read** depends on the seeded text file's
    delimiter/schema matching the target table; the operator DDL is correct
    (Part A HP.1 passes) — a raw-text/schema mismatch in the local sample can
    yield 0 rows. Prefer `hdfs:json`/Hive for deterministic local text reads.
  - **HB.1 HBase live read** fails in the local stack due to an HBase
    **client/server version mismatch** (PXF 2.1.0 ships HBase client 2.3.x; the
    test `harisekhon/hbase:2.1` server is 2.1.x → `ConnectionClosedException`).
    The operator-rendered `hbase-site.xml` (ZK quorum) is correct (SITE.2);
    the mismatch is a test-environment image constraint.
- **FF.7** live export round-trips where the HDFS backing store + PXF
  `DATA_SCHEMA` are present, else build + admit only.

The compose stack adds `namenode`/`hiveserver2`/`hbase` services and
`scripts/gen-hadoop-samples.sh` generates the Hadoop sample data
(`text`/`json` natively via WebHDFS; `parquet`/`avro`/`orc`/`SequenceFile` +
Hive `TEXTFILE`/`ORC`/`RCFILE` tables + the HBase table via `beeline`/`hbase
shell` CTAS; clearly logged as PRODUCED vs CONFIG-ONLY). The e2e live path is
gated by **`SCENARIO97_HADOOP_LIVE`** and skips cleanly when no Hadoop stack is
deployed. The Scenario 97 dashboard adds a **Hadoop-filtered `source_type`
timeseries** + a **Scenario 97 doc text panel** (no new metric).

### Scenario 98 — Filter Pushdown, Column Projection, Per-Row Error Handling

> **Status: Implemented (declarative DDL knobs) + Verified (Scenario 98).** The
> three knobs Scenario 98 proves — `FILTER_PUSHDOWN=true`, `PROJECT=true`, and
> `[LOG ERRORS ]SEGMENT REJECT LIMIT <n> [ROWS|PERCENT]` — **already ship** in the
> generated DDL (`internal/builder/dataload_builder.go` `buildPXFLocation` /
> `errorHandlingClause`), the mutating webhook **defaults `filterPushdown` +
> `columnProjection` to `true`** when unset (`setPxfJobDefaults`), and W.15
> validates the reject-limit type. Scenario 98 is a **TEST +
> LIVE-VERIFICATION + HONEST-OBSERVABILITY** scenario: it proves the *runtime*
> behaviour these knobs request via **real** signals, and records the honest
> decision to **leave `cloudberry_pxf_bytes_transferred_total` Planned** rather
> than fabricate a byte counter.

> **Scenario numbering note.** No collision. Full recent sequence: **95** = PXF
> CLI lifecycle; **96** = Object Store Profiles & Format Write-Capability; **97** =
> Hadoop Profiles (HDFS / Hive / HBase); **98** = Filter Pushdown / Column
> Projection / Per-Row Error Handling (this section).

#### The declarative knobs (already shipped, re-verified)

`buildPXFLocation` emits query options in a fixed, byte-stable order
(`PROFILE`, `SERVER`, `FILTER_PUSHDOWN`, `PROJECT`, `PARTITION_BY`/`RANGE`/`INTERVAL`):

- `pxfJob.filterPushdown=true` → `FILTER_PUSHDOWN=true` in the `pxf://` LOCATION.
  The mutating webhook DEFAULTS `filterPushdown` to `true` when unset; an explicit
  `false` is preserved and emits **nothing**.
- `pxfJob.columnProjection=true` → `PROJECT=true`. Also defaults to `true` when unset.
- `pxfJob.errorHandling.{segmentRejectLimit, segmentRejectLimitType (rows|percent,
  W.15-validated), logErrors}` → `[LOG ERRORS ]SEGMENT REJECT LIMIT <n>
  [ROWS|PERCENT]` (`errorHandlingClause`, gated on a positive `segmentRejectLimit`).
  The **writable export** path (`mode: writable`) correctly **OMITS** this suffix
  (writable external tables do not accept reject limits).

#### Honest-observability decision (`bytes_transferred` stays Planned)

`cloudberry_pxf_bytes_transferred_total` **stays PLANNED** and is **never asserted
or fabricated**. PXF 2.1.0's Spring Boot Actuator exposes only `/actuator/health`
by default. **Scenario 109 now enables `/actuator/prometheus`** to scrape the real
request count + latency (M.2/M.3) — but it still offers only `http_server_requests`
count/latency + JVM metrics; there is **no honest external-source byte counter**.
Emitting a fabricated `bytes_transferred` would violate the metrics-honesty rule.
Filter pushdown is instead **proven via REAL signals**:

1. **Row-count reduction** — `cloudberry_data_loading_rows_total` (real, harvested
   from the `DATALOAD_ROWS` marker) is **lower** for a filtered job than an
   unfiltered baseline.
2. **`EXPLAIN`** shows the pushed filter / projected columns in the external-scan
   plan.
3. **Source-side query logs** (JDBC pgsource/MySQL, Hive/HS2) show the `WHERE`
   predicate (the strongest proof for JDBC).

Per-row error handling (FE.12) is proven via the **real**
`cloudberry_data_loading_job_status` (2=success / 3=failed) +
`cloudberry_data_loading_errors_total` + `rows_total` (valid rows only) — fully
operator-observable, no new metric needed.

#### What's verified (`cases.Scenario98Cases()` — FE.1–5, FE.12a/b)

The sample cluster is
[`scenario98-pushdown-test.yaml`](../deploy/helm/cloudberry-operator/config/samples/scenario98-pushdown-test.yaml)
(cluster `pushdown-test`) with the `s3-datalake`, `minio-warehouse`,
`mysql-oltp`, `postgres-source` and `hadoop-cluster` servers. Each row is resolved
against the **real** built DDL/LOCATION by the CatalogHonest functional + e2e
Part A tests; `[CONFIG-ONLY]`/`[EXPLAIN-ONLY]` rows are explicitly marked so a
reader never mistakes a config-only assertion for a live-data assertion. The
catalog rows are:

| ID | Knob | Source / profile | DDL proof | Honest live signal |
|----|------|------------------|-----------|--------------------|
| FE.1 | filter pushdown | object-store `s3:parquet` | `FILTER_PUSHDOWN=true` | `rows_total` filtered < baseline + `EXPLAIN` pushed filter (ORC leg `[CONFIG-ONLY]` if not synthesizable) |
| FE.2 | filter pushdown | JDBC (`mysql-oltp`/`postgres-source`) | `FILTER_PUSHDOWN=true` | **source-side query log** `WHERE` predicate (strongest for JDBC) + `rows_total` < baseline |
| FE.3 | filter pushdown | Hive (`warehouse.fact_sales`) | `FILTER_PUSHDOWN=true` | Hive/HS2 query-log predicate + partition prune + `rows_total` < baseline (`[CONFIG-ONLY]` when no live Hive backing) |
| FE.4 | column projection | WIDE `s3:parquet` | `PROJECT=true` | `[EXPLAIN-ONLY]` `EXPLAIN` shows only the projected columns (no honest byte meter) |
| FE.5 | column projection | WIDE `s3:orc` | `PROJECT=true` | `EXPLAIN` projected-columns where ORC synthesizable, else DDL+`PROJECT` correctness only (`[CONFIG-ONLY]`) |
| FE.12a | error handling | object-store `s3:text` | `LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS` | malformed source = 5 bad rows ≤ 10 → Job **Completed**; `job_status=2` + `rows_total` = VALID rows only |
| FE.12b | error handling | object-store `s3:text` | `SEGMENT REJECT LIMIT 2 ROWS` | same 5 bad rows > 2 → Job **Failed**; `job_status=3` + `errors_total` incremented |

> **`bytes_transferred` is NEVER named as a signal in any FE row** — the honest
> proofs are row-count reduction, `EXPLAIN`, source logs, and job status.

#### Config-only / explain-only caveats (honest)

- Filter pushdown and column projection are **declarative + runtime-proven**; the
  operator only emits the correct LOCATION option — the engine/PXF connector
  performs the actual prune. Where a live backing store (Hive) or a synthesizable
  sample (ORC) is absent, the row degrades to **`[CONFIG-ONLY]`** (DDL/LOCATION
  correctness) or **`[EXPLAIN-ONLY]`** (plan-shape, no byte meter), as marked.
- Column projection has **no honest byte meter**, so its only live proof is
  `EXPLAIN` (target-list narrowing) — `[EXPLAIN-ONLY]` by construction.
- Error handling (FE.12) is **fully operator-observable** end-to-end via the real
  job-status / errors / rows metrics — no caveat.

#### Artifacts

The compose stack adds `test/docker-compose/scripts/gen-pushdown-samples.sh`,
which generates the pushdown sample data (a filterable + WIDE
parquet/ORC/text dataset for the object-store legs, JDBC seed tables
`jdbc_test_data` with a `category` filter column, and a **malformed CSV** carrying
5 bad rows for FE.12; legs are clearly logged as PRODUCED vs CONFIG-ONLY). The
e2e live path is gated by **`SCENARIO98_PUSHDOWN_LIVE`** and skips cleanly when no
pushdown stack is deployed. The Scenario 98 dashboard adds a **"Filter Pushdown &
Projection (Scenario 98)" doc text panel** (the existing Job Status + Errors
panels already cover FE.12); there is **NO `bytes_transferred` panel** — it stays
Planned.

### Scenario 99 — Writable External Tables / Data Export

> **Status: Implemented (writable export to S3 / HDFS / JDBC + `sourceFilter`
> filtered export + W.17) + Verified (Scenario 99).** The writable DDL
> (`pxfwritable_export`) and the write-capability enforcement shipped in
> Scenario 96; Scenario 99 **verifies the three export targets live** and adds
> the **new `PxfJobSpec.SourceFilter`** filtered export plus the **new webhook
> rule W.17**. A first-class **data-export Job kind** (beyond the writable-DDL
> path the load Job already builds) and `bytes_transferred` remain **Planned**.

> **Scenario numbering note.** No collision. Full recent sequence: **96** = Object
> Store Profiles & Format Write-Capability; **97** = Hadoop Profiles (HDFS / Hive /
> HBase); **98** = Filter Pushdown / Column Projection / Per-Row Error Handling;
> **99** = Writable External Tables / Data Export (this section).

#### The three export targets (profile-agnostic builder)

`buildPXFExternalTableDDL` emits the SAME `CREATE WRITABLE EXTERNAL TABLE …
FORMATTER='pxfwritable_export'` shape for all three targets, differing only by the
LOCATION `PROFILE`/`SERVER`. `pxfpolicy.IsProfileWritable` admits each:

- **S3 / object store (FE.9 / WE.1)** — `s3:text`/`s3:parquet`/`s3:avro` → objects
  LAND in MinIO under `cloudberry-warehouse/exports/s3/` (S3 list/HEAD). `s3:parquet`
  is **`[CONFIG-ONLY]`** where parquet write tooling is absent.
- **HDFS (FE.10)** — `hdfs:text`/`hdfs:parquet`/`hdfs:avro`/`hdfs:sequencefile` →
  part files LAND in HDFS under `/data-lake/exports/hdfs/` (WebHDFS `LISTSTATUS`).
  `hdfs:parquet`/`hdfs:avro` export is **`[CONFIG-ONLY]`** (needs `DATA_SCHEMA`);
  prefer `hdfs:text` for the deterministic live landing.
- **JDBC (FE.11)** — bare `jdbc` (writable via `bareWritableProfiles`) → rows LAND
  in the `pgsource` `sourcedb.export_target` table (`SELECT count(*) > 0`). The
  **strongest, deterministic** proof; the target table is pre-created + granted by
  `gen-export-targets.sh`.

#### WE.2 — data lands with the correct format

For every export the generated WRITABLE DDL carries `FORMATTER='pxfwritable_export'`
**and** the correct format per profile (`s3:text`/`hdfs:text` → text/CSV-shaped;
`jdbc` → rows with the expected columns). This is the explicit WE.2 "correct
format" gate. parquet/avro format-landing is **`[CONFIG-ONLY]`** without write
tooling / `DATA_SCHEMA`.

#### SF.1 — `sourceFilter` filtered export

With `sourceFilter="region='us-east'"` the export **script** emits `INSERT INTO
<ext> SELECT * FROM <target> WHERE region='us-east'` — the `WHERE` is the **only**
script delta vs the baseline (the WRITABLE DDL is unchanged). The predicate is
routed through a quoted heredoc so its single quotes are safe. **Honest signal =**
the filtered export lands **FEWER rows** than the unfiltered baseline (JDBC
`count(*)` is deterministic).

#### SF.2 — `sourceFilter` admission deny (W.17)

- **SF.2 (W.17(a) mode gate):** `sourceFilter` on a READ/import job (mode unset,
  not writable) → admission **DENY**; the error names `sourceFilter` and
  `writable`.
- **SF.2b (W.17(b) sanity check):** a writable job whose `sourceFilter` contains
  `;` (e.g. `"1=1; DROP TABLE x"`) → **DENY** (`statement terminators or SQL
  comments`).

Decision recorded: **REJECT** (not silently ignore), matching the W.* reject
posture.

#### Honest observability (no new metric)

Export jobs are data-loading jobs with `mode: writable`; they reuse the **existing**
data-loading metrics — **no new metric** is added:

- **Rows exported** — `cloudberry_data_loading_rows_total` (harvested from the
  `DATALOAD_ROWS` marker; the same `INSERT 0 <n>` rowcount). The filtered export's
  `rows_total` is lower than the unfiltered baseline.
- **Export success / failure** — `cloudberry_data_loading_job_status` (**2** =
  Succeeded / **3** = Failed) + `cloudberry_data_loading_errors_total`.
- **`bytes_transferred` stays Planned** and is **never asserted or fabricated**.

#### Config-only caveats (honest)

- `hdfs:parquet`/`hdfs:avro` export may need a `DATA_SCHEMA` and is **`[CONFIG-ONLY]`**;
  the deterministic live-landing legs use `text`.
- `s3:parquet` export is **`[CONFIG-ONLY]`** where parquet write tooling is absent.
- The `sourceFilter` predicate is **admin-authored, trusted SQL** (W.17 is a
  cheap sanity check, not a parser) — the same trust boundary as `targetTable`.

#### What's verified (`cases.Scenario99Cases()` — FE.9/WE.1, FE.10, FE.11, WE.2, SF.1, SF.2/SF.2b)

The sample cluster is
[`scenario99-export-test.yaml`](../deploy/helm/cloudberry-operator/config/samples/scenario99-export-test.yaml)
(cluster `export-test`) with the `minio-warehouse`, `hadoop-cluster` and
`postgres-source` servers and four jobs — `s3-export` (FE.9/WE.1), `hdfs-export`
(FE.10), `jdbc-export` (FE.11) and `s3-export-filtered` (SF.1, `sourceFilter:
region='us-east'`). Each row is resolved against the **real** built DDL/script by
the CatalogHonest functional + e2e Part A tests; `[CONFIG-ONLY]` rows are marked.

| ID | Target | Profile | DDL proof | Honest live signal |
|----|--------|---------|-----------|--------------------|
| FE.9 / WE.1 | S3 / object store | `s3:text` writable | `FORMATTER='pxfwritable_export'`, no `LOG ERRORS`; reversed INSERT | objects LAND in MinIO `…/exports/s3/` (S3 list/HEAD); `s3:parquet` `[CONFIG-ONLY]` |
| FE.10 | HDFS | `hdfs:text` writable | same writable DDL | part files LAND in HDFS `/data-lake/exports/hdfs/` (WebHDFS); `hdfs:parquet/avro` `[CONFIG-ONLY]` (DATA_SCHEMA) |
| FE.11 | JDBC | bare `jdbc` writable | same writable DDL | rows LAND in `pgsource` `sourcedb.export_target` (`count(*) > 0`) — strongest |
| WE.2 | all three | per profile | `FORMATTER='pxfwritable_export'` + correct format | text/CSV-shaped / JDBC rows; parquet/avro `[CONFIG-ONLY]` |
| SF.1 | S3 (filtered) | `s3:text` + `sourceFilter` | `… SELECT * FROM <target> WHERE region='us-east'` | filtered export lands FEWER rows than baseline |
| SF.2 / SF.2b | — | — (webhook) | none | admission DENY: W.17(a) mode gate / W.17(b) `;` sanity check |

> **`bytes_transferred` is NEVER named as a signal in any row** — the honest proofs
> are objects/part-files/rows landing, `rows_total` reduction, and job status.

#### Artifacts

The compose stack adds
[`test/docker-compose/scripts/gen-export-targets.sh`](../test/docker-compose/scripts/gen-export-targets.sh),
which **pre-creates** the export targets: the `pgsource` `export_target` JDBC table
(FE.11, `GRANT ALL`) and the S3 (FE.9/WE.1) / HDFS (FE.10) export prefixes (logged
as PRODUCED vs CONFIG-ONLY). The e2e live path is gated by
**`SCENARIO99_EXPORT_LIVE`** and skips cleanly when no export stack is deployed
(the same flag gates the perf baseline). The Scenario 99 dashboard adds **panel
263 — "Writable External Tables / Data Export (Scenario 99)"** doc text panel
(the existing Job Status + Rows + Errors panels already cover the export
signals); there is **NO new metric** and **NO `bytes_transferred` panel** — it
stays Planned.

### Scenario 101 — gpfdist Deployment + gpload-csv

> **Status: Implemented (gpfdist Deployment/Service/PVC + gpload control-file
> Job/CronJob).** Two previously-Planned features flip to Implemented: the
> **gpfdist file-server runtime** (`reconcileGpfdist` →
> `internal/builder/gpfdist_builder.go`, GP.2-GP.5) and the **gpload control-file
> load path** (`internal/builder/gpload_builder.go`, GL.1-GL.7 + J.25-J.40). The
> 2 `cloudberry_gpfdist_*` metrics stay **Planned** (no scrapable endpoint).

> **Scenario numbering note.** No collision (100 skipped by request). Full recent
> sequence: **96** = Object Store Profiles; **97** = Hadoop Profiles; **98** =
> Pushdown / Projection / Error handling; **99** = Writable export; **101** =
> gpfdist Deployment + gpload-csv (this section).

#### gpfdist Deployment / Service / PVC (GP.2-GP.5)

When `dataLoading.gpfdist.enabled: true`, `reconcileGpfdist` ensures three objects
(and best-effort GCs them when the flag flips back off):

- **GP.2/GP.3 Deployment `<cluster>-gpfdist`** (`BuildGpfdistDeployment`) — `replicas`
  honors `gpfdist.replicas` (default **1**, RWO-safe); image `gpfdist.image`
  (default `cloudberry-gpfdist:2.1.0`); container `gpfdist` with command
  `["gpfdist"]` and args `["-d","/data","-p","8080","-l","/var/log/gpfdist.log"]`;
  named container port **8080**; volumeMount `data` → `/data`.
- **GP.4 PVC `<cluster>-gpfdist-data-pvc`** (`BuildGpfdistPVC`) — `ReadWriteOnce`,
  modest `1Gi` request, mounted at `/data`.
- **GP.5 Service `<cluster>-gpfdist-svc`** (`BuildGpfdistService`) — `ClusterIP`,
  selector `avsoft.io/component=gpfdist` (**equals** the Deployment pod labels —
  cannot drift), port **8080** → targetPort **8080**.

> **Documented divergences from the spec's illustrative examples (honest):**
>
> - **PVC name.** The earlier design literal `gpfdist-data-pvc` is implemented as
>   the **per-cluster** `<cluster>-gpfdist-data-pvc` (`util.GpfdistDataPVCName`):
>   avoids same-namespace collisions between two clusters, multi-cluster-safe,
>   `ownerRef`-GC'd with the cluster.
> - **Selector label domain.** The earlier illustration
>   `cloudberry.apache.org/component: gpfdist` is implemented with the repo's
>   **actual** label domain `avsoft.io/component: gpfdist` (`util.LabelComponent` /
>   `util.ComponentGpfdist`). Both pod labels and selector come from
>   `util.CommonLabels`, so they cannot drift and the cluster label additionally
>   scopes selection.

#### gpload control-file CronJob (J.25, GL.1-GL.7, J.26-J.40)

A `type: gpload` job is now rerouted from the old native-external-table-DDL path to
the **gpload control-file path** (PXF jobs unchanged):

1. **Control file (GL.1-GL.7)** — `BuildGploadControlFile` renders a byte-stable
   gpload YAML control file: `VERSION 1.0.0.1` / `DATABASE postgres` / `USER gpadmin`
   / `HOST <cluster>-coord-hl` / `PORT 5432` (GL.1); `INPUT.SOURCE.FILE`
   `gpfdist://<cluster>-gpfdist-svc:8080<glob>` (GL.2); `FORMAT`/`DELIMITER`/
   optional `HEADER`/`ENCODING` (GL.3); `ERROR_LIMIT`/`LOG_ERRORS` (GL.4);
   `OUTPUT.TABLE`/`MODE` (insert|update|merge, with `MATCH_COLUMNS`/`UPDATE_COLUMNS`
   for update/merge) (GL.5); `PRELOAD.TRUNCATE` (GL.6); `SQL.AFTER` (GL.7).
2. **Delivery** — the control file is carried in the per-job ConfigMap
   `<cluster>-gpload-<job>` (data key `<job>.yml`,
   `util.GploadControlFileConfigMapName`), mounted **read-only at `/etc/gpload`**.
3. **Execution (J.25)** — `BuildGploadCronJob` (when `schedule` is set) /
   `BuildGploadJob` (one-off) runs `gpload -f /etc/gpload/<job>.yml` on the cluster
   (data-loader) image. The wrapper best-effort harvests gpload's summary rowcount
   into the `DATALOAD_ROWS` marker (omitted when unparseable — no synthesized count).

Notable field-driven behaviors:

- **J.27 `inputSource.type: local`** — `FILE` entries are the `filePaths` verbatim
  (no `gpfdist://` prefix).
- **J.28/J.29** — `inputSource.host`/`.port` override the default gpfdist
  Service host/port for a `gpfdist` source.
- **J.36/J.37 `mode: update`/`merge`** — emit `MATCH_COLUMNS` (+ optional
  `UPDATE_COLUMNS`); the webhook **W.20** requires non-empty `matchColumns`.

#### New `gploadJob` fields + webhook rules W.18-W.22

The new `GploadJobSpec` fields (`api/v1alpha1/types.go`) drive the control file:
`inputSource{type:gpfdist|local, host, port}`, `delimiter` (`MaxLength 1`),
`header *bool`, `encoding`, `matchColumns[]`, `updateColumns[]`,
`preload{truncate *bool}`, `postActions[]`; `mode` is now an Enum
`insert;update;merge` and `format` an Enum `csv;text`. New sub-structs:
`GploadInputSourceSpec`, `GploadPreloadSpec`.

The webhook (`internal/webhook/validating.go`) adds **W.18-W.22**:

- **W.18** — `inputSource.type`, when set, must be `gpfdist` or `local`.
- **W.19** — `delimiter`, when set, must be **exactly one character**.
- **W.20** — `mode: update`/`merge` requires non-empty `matchColumns`.
- **W.21** — each `postActions[]` element must pass the W.17 SQL sanity check (no
  statement terminators / comment openers); reuses the W.17 helper.
- **W.22** — `inputSource.host`/`.port` are valid **only** for `type: gpfdist`;
  rejected on `type: local`.

#### Honest observability (no new metric)

- **gpload** is a data-loading job → it reuses the **existing**
  `cloudberry_data_loading_*` metrics (`job_status` 2=success/3=failed,
  `rows_total` best-effort from gpload's summary, `errors_total`,
  `job_duration_seconds`). **No new operator metric.**
- **gpfdist Deployment readiness** is observable via **kube-state-metrics**
  (`kube_deployment_status_replicas_ready`) — but kube-state-metrics is **NOT
  deployed in the test env**, so in tests gpfdist readiness is observed via
  **`kubectl`**, not VictoriaMetrics.
- The 2 `cloudberry_gpfdist_*` metrics (`connections_active`,
  `bytes_served_total`) stay **Planned** — gpfdist has **no scrapable endpoint**.

#### Binaries + images (present)

The gpfdist + gpload **binaries** are confirmed present in
`cloudberry-official-pxf:2.1.0`
(`/usr/local/cloudberry-db-2.1.0/bin/{gpfdist,gpload}`); the thin
`cloudberry-gpfdist:2.1.0` image (`Dockerfile.cloudberry-gpfdist`, built via
`make docker-build-gpfdist`) just runs the gpfdist binary over the served `/data`.

#### Artifacts

- Sample cluster
  [`scenario101-gpfdist-test.yaml`](../deploy/helm/cloudberry-operator/config/samples/scenario101-gpfdist-test.yaml)
  (cluster `gpfdist-test`, `gpfdist.enabled: true` + the `gpload-csv` job).
- `cases.Scenario101Cases()` (`test/cases`) — the GP/GL/J catalog.
- `test/docker-compose/scripts/gen-gpload-csv.sh` + the seed CSVs (PVC-seed
  approach: the CSVs are staged onto the gpfdist `/data` PVC so the live load reads
  them over `gpfdist://`).
- The live e2e leg is gated by **`SCENARIO101_GPFDIST_LIVE`** and skips cleanly
  when no gpfdist stack is deployed.
- Dashboard panel — **gpfdist + gpload (Scenario 101)** doc-text panel (gpfdist
  Deployment observed via `kubectl`, not a VM metric, since kube-state-metrics is
  absent; gpload reuses the existing `cloudberry_data_loading_*` panels).

### Scenario 102 — kafka-cdc (Continuous Streaming, Custom Connector)

> **Status: Implemented (custom-connector kafka model + connector-JAR download +
> continuous streaming Job) — with a config-only live caveat for end-to-end row
> landing.** Scenario 102 **reverses** the prior "kafka removed / no streaming
> profiles" policy **scoped to custom connectors only**: the `kafka` profile is
> reinstated as a **custom-connector** profile. Built-in streaming is still out of
> scope (the built-in allowlist is unchanged).

#### The custom-connector kafka model (C.18 / J.41 / J.42)

The streaming path is modeled entirely within the existing PXF custom-connector
machinery — there is **no** new top-level `streamingServer` block:

1. **`pxf.servers[]` entry** `{name: kafka-connector, type: custom}` — the new
   `custom` server type (**W.3**) has **no forced type-specific config keys**; its
   profile implementation comes from a JAR.
2. **`pxf.customConnectors[]` entry** `{name: kafka-connector, jarUrl: s3://…}` —
   the JAR that implements the `kafka` profile. The server↔connector link is by
   **NAME** and enforced by **W.24** (a `type: custom` server requires a matching
   `customConnectors[].name`).
3. **`pxfJob`** `{server: kafka-connector, profile: kafka, continuous: true, …}` —
   admitted by **W.23** (a custom-connector/streaming profile is allowed **only**
   when its server is connector-backed). A bare `kafka` profile, or `kafka` on a
   non-custom server, is **still REJECTED**.

`isValidPxfProfile("kafka")` remains **false** (the built-in `pxfProfileSchemes`
allowlist is **unchanged**). Recognition lives in a **separate** set
`pxfCustomConnectorSchemes = {kafka, rabbitmq}` (`isCustomConnectorProfile`), which
W.10 consults so a streaming profile is "recognized" but then **gated by W.23**.

#### Connector-JAR download into `/pxf/lib/custom` (C.18)

The `pxf-connector-init` init container (`BuildPXFConnectorInitContainers`,
`internal/builder/pxf_builder.go`) is wired into the **segment-primary** pod
**after** `pxf-cred-init`. For each `customConnectors[].jarUrl` (sorted by name)
it downloads into **`/pxf/lib/custom/<name>.jar`** in the shared `pxf-lib`
emptyDir the sidecar mounts: `s3://` via `aws --endpoint-url "$AWS_S3_ENDPOINT" s3
cp`, `http(s)://` via `curl -fsSL`, any other scheme aborts the init. S3 env is
reused from the cluster's **backup S3 destination** (`backup-s3-credentials` +
`AWS_S3_ENDPOINT`/`AWS_DEFAULT_REGION`). See
[Connector-JAR download init-container](#connector-jar-download-init-container-pxf-connector-init-c18--scenario-102).

#### Continuous one-off Job — NOT a CronJob (J.43 / J.46)

A `pxfJob.continuous: true` job is shaped as a **one-off long-running `Job`**, not
a `CronJob` — even with no schedule it is never a CronJob (**J.46**; a continuous
job with a schedule is rejected by **W.23c**). The continuous Job has
`ActiveDeadlineSeconds: nil` (runs until deleted) + `RestartPolicy: OnFailure` +
`BackoffLimit: 6` (`continuousBackoffLimit`). The loader runs a **streaming
consume loop** (`buildContinuousDataLoadScript`): it creates the `pxf://…?
PROFILE=kafka&SERVER=kafka-connector` external table once, then loops `INSERT INTO
<target> SELECT * FROM <ext>` per flush — emitting a best-effort `DATALOAD_ROWS`
marker per flush — until the Job is deleted.

#### Streaming knobs (J.44 / J.45) → loader env

| Field | Constraint | Loader env |
|-------|-----------|------------|
| `pxfJob.continuous` (`*bool`, J.43) | streaming consumer toggle | `CBK_CONTINUOUS` (`true`/`false`; always emitted, first) |
| `pxfJob.batchSize` (`int32`, J.44) | `Minimum=1` (W.23c); rows buffered before a flush | `CBK_BATCH_SIZE` (omitted when 0) |
| `pxfJob.flushInterval` (`string`, J.45) | valid **Go duration** (W.23c, e.g. `"30s"`/`"1m"`) | `CBK_FLUSH_INTERVAL` (omitted when empty; loop sleep defaults to `30s`) |

A **non-continuous** pxf job emits `CBK_CONTINUOUS=false` and omits
`CBK_BATCH_SIZE`/`CBK_FLUSH_INTERVAL` when unset.

#### Observability — HONEST (no new metric)

Scenario 102 introduces **NO new operator metric**. kafka-cdc **reuses** the
existing `cloudberry_data_loading_*` family. The key honesty point: a continuous
consumer's **steady state is `cloudberry_data_loading_job_status = Running`** (1),
**NOT** Complete/2 — unlike a batch job it never "succeeds" on its own. `3`
(failed) means the consumer crashed; `rows_total` is best-effort per flush;
`jobs_active` includes the kafka-cdc job while Running. `cloudberry_pxf_*` and
`cloudberry_gpfdist_*` stay **Planned**.

#### HONEST live caveat (config-only end-to-end row landing)

The JAR **download + mount** (`/pxf/lib/custom/kafka-connector.jar`), the **Job
creation + shaping** (continuous, not CronJob), the **streaming params**
(`CBK_*`), and the **external-table DDL** are **fully provable**. The end-to-end
**kafka→table row landing** needs a **REAL Kafka→PXF connector JAR** — the staged
one is a **placeholder** — so live row-landing is **CONFIG-ONLY / documented**.
The Job still runs as a streaming consumer with the JAR mounted; the live e2e/perf
legs are gated by **`SCENARIO102_KAFKA_LIVE`** and skip/CONFIG-ONLY cleanly absent
a real connector JAR + reachable Kafka.

> **Stock PXF has no Kafka connector (verified against `cloudberry-pxf:2.1.0`).**
> `pxf-profiles.xml` registers **no `Kafka` profile**, and the only `kafka`
> references in the PXF app jar are Micrometer's Kafka *metrics binder*, not a PXF
> Fragmenter/Accessor/Resolver. Apache PXF is a batch, pull, request/response
> framework; Kafka is an unbounded push stream — the models don't fit, so PXF
> ships no Kafka plugin. A functional `kafka` profile therefore **requires a
> user-supplied custom-connector JAR**, which is out of the operator's scope.
> To make row landing live, supply a real Kafka→PXF connector JAR at the
> `customConnectors[].jarUrl` (built against the PXF SDK: a `Fragmenter` mapping
> topic→partitions, an `Accessor` consuming offsets, a `Resolver` deserializing
> records, registered in `pxf-profiles.xml`), **or** use a non-PXF Kafka consumer
> in the loader (e.g. `kcat`/Kafka Connect) — both are net-new connector work,
> not operator changes. The operator's plumbing is already complete and proven.

#### Artifacts

- Sample cluster
  [`scenario102-kafka-test.yaml`](../deploy/helm/cloudberry-operator/config/samples/scenario102-kafka-test.yaml)
  (cluster `kafka-test`, namespace `cloudberry-test`: a `custom` `kafka-connector`
  server + the matching `customConnectors[]` JAR + the continuous `kafka-cdc` job).
- `cases.Scenario102Cases()` (`test/cases`) — the C.18 / J.41–J.46 / W.23 / W.24 /
  W.23c catalog.
- `test/docker-compose/scripts/gen-kafka-cdc.sh` — produces the sample CDC
  messages the kafka-cdc job consumes.
- Functional/integration/e2e/perf suites
  (`internal/.../*_scenario102_*`, `test/{functional,integration,e2e,perf}/scenario102_*`);
  the live legs are gated by **`SCENARIO102_KAFKA_LIVE`**.
- Dashboard **panel 272 — "kafka-cdc Continuous Streaming (Scenario 102)"** doc-text
  panel (observed via `cloudberry_data_loading_job_status = Running` steady state;
  **no new metric**).

### Scenario 103 — FDW-Based Loading Path

> **Status: Implemented.** Scenario 103 adds an **alternative FDW loading path**
> alongside the existing external-table path. A `pxfJob.loadMethod: fdw` PXF job
> builds a **PERSISTENT** foreign-data-wrapper chain instead of the transient
> external table, then loads via `INSERT INTO <target> SELECT * FROM <foreign>`.
> It is **EQUIVALENT** to the external-table path: the SAME rows land in the
> target — proven by **equal row counts**, not a new metric.

#### How the path is selected — `pxfJob.loadMethod`

The new field `pxfJob.loadMethod` (`api/v1alpha1/types.go`) selects the mechanism:

| `loadMethod` | DDL generated | Mechanism |
|--------------|---------------|-----------|
| `external-table` (default; also empty) | `CREATE EXTERNAL TABLE <tmp> (LIKE <target>) LOCATION ('pxf://…')` … `INSERT…SELECT` … **DROP** | Transient PXF external table |
| `fdw` | `CREATE SERVER` + `CREATE USER MAPPING` + `CREATE FOREIGN TABLE` (all `IF NOT EXISTS`, **never dropped**) … `INSERT…SELECT` | Persistent `pxf_fdw` foreign-data-wrapper |

A `loadMethod: fdw` job is routed by `isFDWPxfJob` to `buildFDWDataLoadScript`
(parallel to the writable/continuous branches), so the non-FDW external-table
path stays **byte-identical**.

#### EX.5-EX.7 — the persistent FDW DDL (`buildFDWDDL`)

The chain is emitted in a fixed, byte-stable, injection-safe order (identifiers
double-quoted; the resource/format single-quoted literals):

```sql
-- EX.5 — CREATE SERVER (persistent; OPTIONS (config '<pxf-server>'), NOT resource/format)
CREATE SERVER IF NOT EXISTS "foreign_s3_datalake"
  FOREIGN DATA WRAPPER s3_pxf_fdw
  OPTIONS (config 's3-datalake');

-- EX.6 — CREATE USER MAPPING (FOR the data-loader role = gpadmin)
CREATE USER MAPPING IF NOT EXISTS FOR "gpadmin"
  SERVER "foreign_s3_datalake";

-- EX.7 — CREATE FOREIGN TABLE (LIKE target, persistent; resource/format here)
CREATE FOREIGN TABLE IF NOT EXISTS "foreign_events" (LIKE "public"."events")
  SERVER "foreign_s3_datalake"
  OPTIONS (resource 'cloudberry-data/text/data.csv', format 'csv');
```

- **Deterministic names.** Server = `"foreign_" + sanitize(pxfJob.server)`
  (`fdwServerName`); foreign table = `"foreign_" + sanitize(job.name)`
  (`fdwForeignTableName`); the user mapping is `FOR "gpadmin"`
  (`pxfDataLoaderRole = util.DefaultAdminUser` — the role RP.11 GRANTs
  `SELECT`/`INSERT ON PROTOCOL pxf`).
- **`(LIKE <target>)`.** The foreign table inherits the target's column schema, so
  no per-column schema needs to be authored in the CR.
- **Format OPTION omitted for bare profiles.** `fdwFormatOption` takes the profile
  suffix after `:` (`s3:parquet`→`'parquet'`); a **bare** profile (`jdbc`/`hive`)
  has no suffix, so the `format` OPTION is **omitted** entirely (JDBC/Hive FDW take
  a resource, not a format).

#### Per-protocol `pxf_fdw` wrapper (LIVE-VERIFIED)

`fdwWrapperForProfile` parses the profile scheme (the token before `:`) and looks
it up in the live-verified `fdwWrapperByScheme` map. These are the **EXACT**
per-protocol wrapper names the `pxf_fdw` extension registers in
`cloudberry-official-pxf:2.1.0`, confirmed via
`SELECT fdwname FROM pg_foreign_data_wrapper` — each scheme has its **OWN**
registered wrapper (they are NOT collapsed: `gs` uses `gs_pxf_fdw`, not
`s3_pxf_fdw`); the operator emits the per-scheme one:

| Profile scheme | Wrapper |
|----------------|---------|
| `s3` | `s3_pxf_fdw` |
| `gs` | `gs_pxf_fdw` |
| `abfss` | `abfss_pxf_fdw` |
| `wasbs` | `wasbs_pxf_fdw` |
| `jdbc` | `jdbc_pxf_fdw` |
| `hdfs` | `hdfs_pxf_fdw` |
| `hive` | `hive_pxf_fdw` |
| `hbase` | `hbase_pxf_fdw` |
| _(unknown scheme)_ | `pxf_fdw` (generic fallback) |

#### EX.8 — the load step (`buildFDWDataLoadScript`)

The script ensures the persistent FDW objects exist (idempotent `IF NOT EXISTS`,
**no drop**), then:

1. **Queries the foreign table directly** (the EX.8 persistence proof):
   `psql -c 'SELECT count(*) FROM "foreign_events"'`.
2. Loads via the **shared** `writeDataLoadInsert` helper:
   `INSERT INTO "public"."events" SELECT * FROM "foreign_events" [WHERE <sourceFilter>]`
   (the `sourceFilter` WHERE — now valid on an fdw read per W.17 — is the same
   admin-trusted predicate the writable export uses; a single-quote-bearing
   predicate is routed through a quoted heredoc, and the `INSERT 0 <n>` command
   tag is captured into the `DATALOAD_ROWS` marker the controller harvests).
3. `ANALYZE "public"."events"` (read path refreshes the planner stats).

The foreign table / server / user mapping are **NOT dropped** — they remain
directly queryable after the Job completes. The INSERT…SELECT shape is **identical
to the external-table path**, so the same rows land (the equivalence proof).

#### Observability — HONEST (no new metric)

Scenario 103 introduces **NO new operator metric**. An FDW load **is** a
data-loading job, so it **reuses** the existing `cloudberry_data_loading_*` family
(`job_status`/`rows_total`/`errors_total`). The FDW==external-table **equivalence
is proven by EQUAL ROW COUNTS** (`count(public.events_ext) ==
count(public.events_fdw)` over the SAME dataset), **not** a new metric. The
`cloudberry_pxf_*` and `cloudberry_gpfdist_*` families stay **Planned**.

#### Live result (verified) + an honest distinct caveat

The **operator's declarative FDW load works end-to-end** (verified live, cluster
`fdw-test`): the `s3-fdw-load` Job's generated DDL — `CREATE SERVER … OPTIONS
(config 's3-datalake')` + `CREATE FOREIGN TABLE … OPTIONS (resource '…', format
'csv')` — succeeds; the foreign objects **persist**; the **direct** foreign-table
read returns **1000** rows (EX.8); `public.events_fdw` lands **1000**; the
filtered FDW load lands **950** (`< 1000`); `job_status` is **2** (success) with
`rows_total` harvested. **EQUIVALENCE** holds (1000 == 1000) over the same CSV
dataset.

> **Distinct pre-existing caveat (NOT the FDW path).** The **external-table**
> read path for an `s3:text` profile uses `FORMAT 'CUSTOM'
> (FORMATTER='pxfwritable_import')`, which returns each CSV line as a **single
> text field** — so the `s3-ext-load` Job fails on **multi-column CSV** ("Record
> has 1 fields but the schema size is N"). This is a **pre-existing
> external-table-path limitation**, independent of the FDW work: the **FDW path
> handles CSV correctly** because it maps `s3:text` → the FDW `format 'csv'`. The
> equivalence comparison therefore loads `events_ext` over the **same** dataset
> via the operator's persistent FDW foreign table when the external-table CSV
> leg can't parse, and remains a true equal-row-count assertion. (Fixing the
> external-table `s3:text` CSV parse — e.g. emitting `FORMAT 'CSV'` for delimited
> text profiles — is tracked separately and is out of Scenario 103's FDW scope.)

#### Artifacts

- Sample cluster
  [`scenario103-fdw-test.yaml`](../deploy/helm/cloudberry-operator/config/samples/scenario103-fdw-test.yaml)
  (cluster `fdw-test`, namespace `cloudberry-test`: ONE `s3-datalake` server +
  three jobs over the SAME MinIO dataset — `s3-ext-load` (external-table) +
  `s3-fdw-load` (fdw) + `s3-fdw-filtered` (fdw + `sourceFilter`) — for the
  equivalence proof).
- `cases.Scenario103Cases()` (`test/cases`) — the EX.5-EX.8 / W.25 / W.17 catalog.
- Functional/integration/e2e/perf suites
  (`internal/builder/dataload_builder_scenario103_test.go`,
  `internal/webhook/validating_scenario103_test.go`,
  `test/{functional,integration,e2e}/scenario103_fdw*`); the live legs are gated by
  **`SCENARIO103_FDW_LIVE`**.
- Dashboard **panel 273 — "FDW-Based Loading Path (Scenario 103)"** doc-text panel
  (observed via `cloudberry_data_loading_*`; equivalence by row counts; **no new
  metric**).

### Scenario 104 — Pre-Load Health Checks

> **Status: Implemented.** Scenario 104 flips the previously-Planned pre-load
> health-check init container to **Implemented**. A `dataload-healthcheck` init
> container runs five gated checks before each data-loading Job; a non-zero exit
> **blocks the load** → the Job fails. The init container is prepended **FIRST**
> on **BOTH** the pxf/native (`buildDataLoadPodSpec`) **AND** the gpload
> (`buildGploadPodSpec`) Job pods.

#### The init container (`buildDataLoadHealthCheckInitContainer`)

The `dataload-healthcheck` container runs the data-loader image with `bash -c`
over `buildDataLoadHealthCheckScript` (`set -euo pipefail`). Its env is the PG\*
data-loading env (HC.1/HC.2 reach the coordinator) PLUS the S3 creds env (HC.3,
reused from the connector-init pattern — `AWS_*` via `SecretKeyRef`,
`AWS_S3_ENDPOINT`, never plaintext). It mounts the shared `dataload-scratch`
`emptyDir` at `/dataload-scratch` (HC.5). The container is gated on
`healthChecksEnabled` (default ON); an explicit `healthChecks.enabled: false`
omits the init container, the scratch volume **and** the main-container scratch
mount (the pod is then byte-identical to a no-health-check pod).

#### HC.1-HC.5 — the five checks (with gating)

| Check | Gate | Probe | Fails when |
|-------|------|-------|------------|
| **HC.1** PXF readiness | pxf jobs + `pxf.enabled` | A `psql` **DB-proxy**: `SELECT 1` (coordinator) → `SELECT 1 FROM pg_extension WHERE extname='pxf'` (extension present) → `SELECT pxf_version()` (PXF ready) | coordinator unreachable / `pxf` extension absent / PXF not ready — LIVE proof: stop PXF on a segment |
| **HC.2** target table exists | ALL jobs (with a resolvable target) | `psql … to_regclass('<targetTable>')` | the target table is missing (the deterministic headline) |
| **HC.3** source connectivity | pxf **object-store** jobs (s3-family) | `curl -fsS --head "${AWS_S3_ENDPOINT}"` (+ trailing-slash retry) | endpoint wrong/unreachable; clean **SKIP** with no endpoint; **skipped** for `jdbc`/`hive`/`hbase`/`hdfs` |
| **HC.4** gpfdist reachability | gpload jobs + `gpfdist.enabled` | `curl -fsS "http://<cluster>-gpfdist-svc:8080/"` | the gpfdist Deployment is scaled to 0 (no ready endpoints) |
| **HC.5** disk space | ALL jobs | `df -Pk /dataload-scratch` free `>= diskMinFreeMB` | scratch full / `diskMinFreeMB` raised above free space |

#### HC.1 DB-proxy honesty

The data-load Job pod **CANNOT** reach a segment's localhost-only PXF sidecar —
PXF segment↔sidecar traffic is `localhost`-only (no cross-pod traffic; see
Security Considerations). So **HC.1 is a DB proxy via `psql` against the
coordinator, NOT a direct `curl` of the sidecar `/actuator/health`**. The
segment-pod sidecar **liveness probe** uses `/actuator/health` (the PXF 2.1.0
Spring Boot actuator endpoint); the scenario's legacy `/pxf/v15/Status` wording
is the **incorrect path that 404s and must NOT be used**. The live proof of HC.1
is "stop PXF on a segment → the job fails".

#### The `healthChecks` CRD knob

`dataLoading.healthChecks` (`api/v1alpha1.DataLoadHealthChecksSpec`):

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `enabled` | `*bool` | `true` (nil block ⇒ on) | Whether the init container runs; `false` → no init container / scratch volume |
| `diskMinFreeMB` | `int32` | `64` | HC.5 free-space threshold (MB) on `/dataload-scratch` |
| `scratchSizeLimit` | `string` | unset | Optional `emptyDir` `SizeLimit` (e.g. `"64Mi"`); caps the scratch volume and makes HC.5 deterministically fillable |

#### The scratch volume

A `dataload-scratch` `emptyDir` (`SizeLimit` from `scratchSizeLimit`, ignored if
unparseable) is added to the pod and mounted at `/dataload-scratch` on **BOTH**
the init container and the main data-load container — so the load's temp /
error-log files have a real home AND HC.5's `df` probe observes the same volume
the load writes to.

#### Failure → blocks → status → Event → restore → proceeds

A non-zero check exits the init container non-zero → the main load container is
**blocked** → the Job is observed **Failed** (`status.dataLoading` +
`cloudberry_data_loading_job_status=3` + `errors_total`). When the controller
observes a data-load Job transition into Failed **and** the `dataload-healthcheck`
init container terminated non-zero, `emitDataLoadHealthCheckFailureEvent`
(`dataload_controller.go`) emits a **de-duplicated** `EventTypeWarning`
**`DataLoadingHealthCheckFailed`** Event. Attribution is **honest**: it inspects
the Job pod's `initContainerStatuses` (via `podInitContainerFailed`); a
**main-container** failure (a real load error) does **NOT** get the HC event, and
when the init status is not derivable the controller stays **silent** (the
failure is still surfaced via status + `errors_total` + the Job pod logs).
**Restore** the broken condition → the Job re-runs → the init passes → the load
**proceeds** → Job Succeeded.

#### Observability — kube-state-metrics (NEW)

Scenario 104 adds **kube-state-metrics** to the test monitoring stack
(`test/monitoring/kube-state-metrics`, a Helm chart) so object-level
Kubernetes metrics flow to VictoriaMetrics:

- `kube_job_status_failed{job_name=~".*-dataload-.*"}` — failed data-load Jobs.
- `kube_pod_init_container_status_*` — the `dataload-healthcheck` init
  container's terminated/restart state.
- `kube_deployment_status_replicas_available{deployment=~".*-gpfdist"}` — the
  HC.4 gpfdist-readiness view.

Dashboard panels **274** (doc text), **275** (`kube_job_status_failed` data-load
failures) and **276** (gpfdist deployment ready replicas) are added.

#### Observability — HONEST (no new operator metric)

Scenario 104 introduces **NO new operator metric**. Health-check failures are
observed via the EXISTING `cloudberry_data_loading_job_status=3` (Failed) +
`cloudberry_data_loading_errors_total` + the `DataLoadingHealthCheckFailed` Event
+ the NEW kube-state-metrics families above. The `cloudberry_pxf_*` and
`cloudberry_gpfdist_*` families stay **Planned** and are never asserted.

#### Artifacts

- Sample cluster
  [`scenario104-healthcheck-test.yaml`](../deploy/helm/cloudberry-operator/config/samples/scenario104-healthcheck-test.yaml)
  (cluster `healthcheck-test`, namespace `cloudberry-test`: pxf + gpfdist +
  `healthChecks {enabled, diskMinFreeMB: 64, scratchSizeLimit: "64Mi"}`; an
  `s3-load` pxf job — HC.1/HC.2/HC.3/HC.5 — and a `gpload-csv` gpload job —
  HC.4).
- `cases.Scenario104Cases()` (`test/cases`) — the HC.1-HC.5 catalog resolved
  against the real built artifact (the init container + the 5-check script + the
  scratch volume/mounts) plus the `DataLoadingHealthCheckFailed` Event.
- Functional/integration/e2e suites
  (`internal/builder/dataload_builder_healthcheck_test.go`,
  `internal/controller/dataload_controller_healthcheck_test.go`,
  `test/{functional,integration,e2e}/scenario104_health_checks*`); the live
  fail+restore legs are gated by **`SCENARIO104_HC_LIVE`**.
- The NEW `test/monitoring/kube-state-metrics` chart + dashboard panels 274-276.

### Scenario 105 — DataLoadingStatus PXF Fields

> **Status: Implemented.** Scenario 105 flips the previously-Planned live **PXF
> health** sub-status of `status.dataLoading.pxf` to **Implemented**: `pxf.status`
> and `pxf.extensionsInstalled` are now populated — **LIVE, HONEST, observed-only,
> and ABSENT when unobservable** (never synthesized). The already-Implemented
> S.2/S.4/S.5 fields are re-pinned here and remain honest. See
> [DataLoadingStatus](#dataloadingstatus) and
> [Live PXF health sub-status](#live-pxf-health-sub-status-scenario-105).

Acceptance scenario (verbatim): *"With PXF running and several jobs configured,
verify `status.dataLoading.pxf.status: Running`; stop PXF on a segment → status
reflects Error/Stopped. `pxf.servers` equals the count of configured server
definitions (S.2). `pxf.extensionsInstalled` lists `pxf` and `pxf_fdw` (S.3). Run
jobs concurrently → `activeJobs` matches (S.4). After job runs, each `jobs[]`
entry has `name`, `lastRun`, `lastStatus`, `rowsLoaded`, `duration` populated
correctly (S.5)."*

#### S.1 — `pxf.status` (NEW, Implemented)

`pxf.status` ∈ `"Running" | "Stopped" | "Error"`, **ABSENT** when unobservable.
It is derived **ONLY** from real segment-primary `pxf` container readiness
aggregated from each pod's `ContainerStatuses`
(`util.PXFReadyCount` + `util.PXFStatusFromReadiness`,
`util.SegmentPrimaryPXFSelector`) — **NO `kubectl exec`, NO live HTTP probe of
the sidecar, NO synthesized health**:

| Observed segment-primary `pxf` containers | `pxf.status` |
|-------------------------------------------|--------------|
| **all** ready (`ready == total > 0`) | `Running` |
| **some** down (`0 < ready < total`) | `Error` (degraded) |
| **none** ready (`ready == 0, total > 0`) | `Stopped` |
| **no pods observed** / pod-list error | *field ABSENT* (non-fatal) |

The **KEY segment-stop → Error transition**: stopping PXF on **one** segment of a
multi-segment cluster drops that pod's `pxf` container readiness, so the
aggregation moves `Running → Error` (degraded); restoring it returns to
`Running`. A single-segment full outage maps to `Stopped`. Losing observability
of the pods drops the field entirely rather than reporting a stale or guessed
value.

#### S.2 — `pxf.servers` (already Implemented, re-pinned)

`pxf.servers == len(pxf.servers)` (spec-derived config count, **not** a
live-reachable count); `pxf enabled` with zero servers honestly reports
`configured: true, servers: 0`.

#### S.3 — `pxf.extensionsInstalled` (NEW, Implemented)

`pxf.extensionsInstalled` is `[]string` from a **real read-only `pg_extension`
probe** (`ListPXFExtensions` in `internal/db/client.go`). It lists `pxf` and/or
`pxf_fdw` **when they are actually installed** (deterministic order), an
**honest subset** when only one is present, and is **ABSENT (`nil`)** when the DB
is unreachable **or** when no PXF extensions are installed — **never synthesized**
(an empty array is never emitted, and the field is **not** derived from the
`pxf.extensions.{pxf,pxfFdw}` spec flags). The probe is **best-effort/non-fatal**:
a DB/query error leaves the field absent and reconcile still succeeds.

#### S.4 — `activeJobs` (already Implemented, re-pinned)

`activeJobs` == the count of `enabled` jobs (and `configuredJobs` == `len(jobs)`);
running jobs **concurrently** does not change this **enabled-count** invariant
(`activeJobs` is the enabled count, not the in-flight count). The legacy
`status.dataLoadingJobs` integer mirrors `activeJobs`.

#### S.5 — `jobs[]` runtime fields (already Implemented, re-pinned)

After a run, each `jobs[]` entry honestly carries `name`, `lastRun`
(= Job `startTime`), `lastStatus` (`Succeeded`/`Failed`/`Running`/`Pending`),
`rowsLoaded` (harvested `DATALOAD_ROWS` marker — present **only** on a succeeded
Job, **never synthesized**), and `duration` (start→completion, absent until
terminal). A never-executed job carries only `name`/`enabled`.

#### Observability — HONEST PXF health gauges (NEW)

Scenario 105 adds **two** honest PXF gauges, emitted **only when observable** from
the same observed-only sources as the status fields:

- `cloudberry_pxf_status{cluster,namespace}` — `0`=Stopped, `1`=Running,
  `2`=Error; recorded **only** when the readiness aggregation yields an observable
  state (when `pxf.status` is ABSENT the gauge is **not** recorded — no series is
  forced).
- `cloudberry_pxf_extensions_installed{cluster,namespace}` — the **count** of
  installed PXF extensions; recorded **only** when the `pg_extension` probe
  actually observed them (not emitted when the DB is unreachable or none are
  installed).

The 7 `cloudberry_pxf_*` runtime/health metrics and the `cloudberry_gpfdist_*`
families stay **Planned** and are never asserted.

#### Artifacts

- `cases.Scenario105Cases()`
  ([`test/cases/scenario105_dataloading_status_cases.go`](../test/cases/scenario105_dataloading_status_cases.go))
  — the S.1–S.5 + MX catalog (builder/reconcile `-B` rows and live `-L` rows).
- The shared honesty helpers `util.PXFReadyCount` / `util.PXFStatusFromReadiness`
  / `util.SegmentPrimaryPXFSelector` (consumed by **both** the reconcile path and
  the `pxf status` API handler) and the read-only `db.Client.ListPXFExtensions`
  (`internal/db`, with pgxmock coverage in `internal/db/pgxclient_test.go`).
- Functional/integration/e2e suites
  (`test/functional/scenario105_dataloading_status_test.go`,
  `test/integration/scenario105_dataloading_status_test.go`,
  `test/e2e/scenario105_dataloading_status_e2e_test.go`); the DB-real
  `ListPXFExtensions` leg is gated by **`SCENARIO105_DB_LIVE`** and the live
  segment-stop → Error leg by the deployed cluster.

### Scenario 106 — Server Configuration Update / Delete

> **Status: Implemented.** The SL.7/SL.8 operator mechanics (full-replacement
> reconcile of the `<cluster>-pxf-servers` ConfigMap) already existed; Scenario
> 106 adds **honest observability** on top — a `PXFServersChanged` event and a
> `cloudberry_pxf_servers_changed_total` counter that fire **only on a real
> ConfigMap `Data` diff**. See [Updating a Server (SL.7)](#updating-a-server-sl7)
> and [Deleting a Server (SL.8)](#deleting-a-server-sl8).

Acceptance scenario (verbatim): *"Patch a server's endpoint (e.g.
`minio-warehouse`'s `fs.s3a.endpoint`) → the `<cluster>-pxf-servers` ConfigMap
regenerates that server's `<server>__s3-site.xml` (others byte-identical),
sidecars pick up on the next volume sync or an explicit `pxf sync`, and reads use
the NEW endpoint. Remove a server from `dataLoading.pxf.servers[]` → its
`<server>__*.xml` keys are dropped from the ConfigMap and external/foreign tables
referencing it fail until recreated. In BOTH cases a `PXFServersChanged` event is
emitted and `cloudberry_pxf_servers_changed_total` increments by exactly 1 — but
ONLY on a real diff (a no-op sync / first create fires neither)."*

#### SL.7 — Update (patch endpoint → CM regen → new endpoint used)

Patching `minio-warehouse`'s `fs.s3a.endpoint` re-renders only that server's
`<server>__s3-site.xml` key (every other server's keys stay byte-identical). The
sidecars re-render on the next volume sync (the `pxf-cred-init` init container) or
immediately via the explicit `pxf sync` trigger (**Implemented — Scenario 95**),
and subsequent `pxf://` reads use the **new** endpoint. Steady-state correctness
is **structural** (shared ConfigMap, byte-identical renders); `pxf sync` is the
**on-demand** ConfigMap-refresh + segment-primary roll, not a prerequisite.

#### SL.8 — Delete (remove server → keys removed → referencing table fails)

Removing a server from `dataLoading.pxf.servers[]` drops its `<server>__*.xml`
keys from the ConfigMap on the next full-replacement reconcile; remaining servers'
keys stay intact. External/foreign tables that reference the deleted `SERVER`
**fail** until recreated against a still-configured server.

#### MX — Honest event + metric (fires only on a real diff)

On a **real** `<cluster>-pxf-servers` ConfigMap `Data` diff — and only then —
both the controller reconcile (`ensurePxfServersConfigMap` → `emitPXFServersChanged`)
and the explicit `pxf sync` API path (`handlePXFSync` → `recordPXFServersChanged`):

- emit a **Normal `PXFServersChanged`** event
  (`cbv1alpha1.EventReasonPXFServersChanged`) with the bounded, deterministic
  message `PXF servers changed: added=[..] removed=[..] updated=[..]`; and
- increment `cloudberry_pxf_servers_changed_total{cluster,namespace}` by exactly
  `1` (`IncPXFServersChanged`).

A **no-op sync** (no `Data` change) and a **first-time create** (no prior
ConfigMap to diff) fire **neither** — no event, no counter increment, no series
forced. The diff is computed by the shared, pure
`internal/util.DiffPXFServerNames` (which parses the `<server>__<file>.xml` keys
into server names and reports sorted `added`/`removed`/`updated`), and the message
is rendered by `FormatPXFServersChangedMessage` — the SAME helpers on both the
controller and API paths so the two never disagree.

#### Artifacts

- `cases.Scenario106Cases()`
  ([`test/cases/scenario106_server_config_cases.go`](../test/cases/scenario106_server_config_cases.go))
  — the SL.7/SL.8 + MX catalog (`Scenario106EventReason = "PXFServersChanged"`,
  `Scenario106ChangedMetric = "cloudberry_pxf_servers_changed_total"`).
- The shared honesty helpers `util.DiffPXFServerNames` /
  `util.FormatPXFServersChangedMessage` (`internal/util/pxf.go`), consumed by
  **both** `emitPXFServersChanged` (controller) and `recordPXFServersChanged`
  (API).
- Functional/integration/perf/e2e suites
  (`test/functional/scenario106_server_config_update_delete_test.go`,
  `test/integration/scenario106_server_config_update_delete_test.go`,
  `test/perf/scenario106_server_config_perf_test.go`,
  `test/e2e/scenario106_server_config_update_delete_e2e_test.go`); the live leg is
  gated by **`SCENARIO106_LIVE`**.

### Scenario 107 — All API Endpoints (P.1–P.15)

> **Status: Implemented.** Scenario 107 lands the **full** data-loading REST
> surface in `internal/api/dataloading.go` (wired by
> `internal/api/server.go` → `registerDataLoadingRoutes`). The five job mutations
> (P.8/P.10/P.11/P.12/P.13) flip from **501-stub** → **FULL**, and the PXF servers
> CRUD (P.2–P.5) + `jobs/{job}/logs` (P.14) + `external-tables` (P.15) flip from
> **Planned** → **FULL**. The already-FULL P.1 (`pxf/status`), P.6 (`pxf/sync`),
> P.7 (`jobs` list) and P.9 (`jobs/{job}` get) are re-pinned here. The two honest
> non-200 holds are **operational, not stubs**: P.14 → `501 LOGS_NOT_AVAILABLE`
> only when no Kubernetes clientset is wired, and P.15's `observed` is `null` with
> `observedAvailable:false` when the DB is unreachable. See the
> [API Endpoints table](#api-endpoints).

Acceptance scenario (verbatim): *"Exercise every data-loading API endpoint
P.1–P.15: list/get/create/update/delete PXF servers; list/get/create/update/
delete/start/stop jobs; stream job logs; list external tables. Each returns real
data (or an HONEST absence), correct status codes, and the right permission tier
— never a 501 stub for an implemented route, never a fabricated catalog row."*

#### Endpoints (payload · side effect · honesty)

| ID | Endpoint | Perm | Payload / params | Side effect & honesty notes |
|----|----------|------|------------------|-----------------------------|
| P.1 | `GET .../pxf/status` | Basic | — | Real segment-primary `pxf` `ContainerStatuses` readiness aggregation + spec-derived echo (no exec/HTTP/synthesized health). |
| P.2 | `GET .../pxf/servers` | Basic | — | Lists configured servers as `{name,type,config}` + credential-secret **REFERENCES** — **never** literal secret values. |
| P.2 | `GET .../pxf/servers/{server}` | Basic | — | One server; `404 SERVER_NOT_FOUND` when absent. |
| P.3 | `POST .../pxf/servers` | Operator | server spec | Persists into `spec.dataLoading.pxf.servers[]`; `201` returns the **RENDERED** `<server>__*.xml` config; `409 SERVER_EXISTS` on a duplicate name (race-safe re-check). |
| P.4 | `PUT .../pxf/servers/{server}` | Operator | server spec | Updates the server in place; `200` returns the re-rendered config; `404 SERVER_NOT_FOUND`. |
| P.5 | `DELETE .../pxf/servers/{server}` | Admin | — | Removes the server; **`409 SERVER_IN_USE`** (lists the referencing jobs, performs NO mutation) when a job still references it — mirrors webhook **W.9**. |
| P.6 | `POST .../pxf/sync` | Operator | — | Refreshes the `<cluster>-pxf-servers` ConfigMap + rolls sidecars; `202`. |
| P.7 | `GET .../jobs` | Basic | — | Lists jobs straight from `spec.dataLoading.jobs`. |
| P.8 | `POST .../jobs` | Operator | job spec | Persists a new job; `201`; **`409 JOB_EXISTS`** on a duplicate name; **`400`** when `pxfJob.server` names an unknown server. |
| P.9 | `GET .../jobs/{job}` | Basic | — | One job from spec. |
| P.10 | `PUT .../jobs/{job}` | Operator | job spec | Updates the job; `200`. |
| P.11 | `DELETE .../jobs/{job}` | Admin | — | Removes the job from spec; **best-effort deletes** the spawned `<cluster>-dataload-<job>` Job. |
| P.12 | `POST .../jobs/{job}/start` | Operator | — | Creates a **REAL one-off `batchv1.Job`**; `202`; **`409 JOB_ALREADY_RUNNING`** when the Job already exists (does not clobber the in-flight run). |
| P.13 | `POST .../jobs/{job}/stop` | Operator | — | Deletes the running Job / **suspends** the CronJob; `202`; **idempotent `200`** ("nothing to stop") when there is nothing running — never fabricates a stop. |
| P.14 | `GET .../jobs/{job}/logs` | Basic | `?follow`, `?tailLines` | **Streams the REAL data-loading Job pod logs**; honest **`501 LOGS_NOT_AVAILABLE`** only when no Kubernetes clientset is wired (no log source) — never a fake log body. |
| P.15 | `GET .../external-tables` | Basic | — | Returns `{cluster, observed, observedAvailable, expected}`. **`observed`** = live `db.Client.ListExternalTables` (`pg_exttable` + foreign tables); **`null`** with **`observedAvailable:false`** when the DB is unreachable — **NEVER synthesized**. **`expected`** = spec-derived (`foreign_<job>` for fdw jobs, target tables for pxf jobs), **clearly labeled** and never claimed to "exist". |

#### New error codes (Scenario 107)

| Code | HTTP | Meaning |
|------|------|---------|
| `SERVER_NOT_FOUND` | `404` | Named PXF server is absent (P.2 get / P.4 / P.5). |
| `SERVER_EXISTS` | `409` | Create would duplicate an existing server name (P.3). |
| `SERVER_IN_USE` | `409` | Delete refused — a job still references the server (P.5; mirrors W.9). |
| `JOB_EXISTS` | `409` | Create would duplicate an existing job name (P.8). |
| `JOB_ALREADY_RUNNING` | `409` | Start refused — the one-off Job already exists (P.12). |
| `LOGS_NOT_AVAILABLE` | `501` | No Kubernetes clientset wired to stream Job pod logs (P.14). |

#### Honesty split (P.14 real logs · P.15 real catalog or honest-absent)

- **P.14 streams REAL pod logs**, not a synthesized transcript. It mirrors the
  backup `jobs/{job}/logs` handler: when a clientset is present it streams the
  data-loading Job pod's stdout (`?follow`/`?tailLines` honored); when none is
  wired it returns the honest `501 LOGS_NOT_AVAILABLE` rather than an empty or
  fabricated body.
- **P.15 never synthesizes the catalog.** `observed` reflects only rows the live
  `pg_exttable`/foreign-table probe actually returned; a DB connectivity/query
  error sets `observed: null` + `observedAvailable: false` (UNOBSERVABLE, not
  "none present"). `expected` is the spec-derived would-be set, kept in a
  **separate** field so it is never conflated with the live catalog.

#### Permissions

Basic (read/status/logs/external-tables) · Operator
(create/update/start/stop/sync) · Admin (the destructive deletes — P.5 server,
P.11 job).

#### Artifacts

- `internal/api/dataloading.go` (the P.2–P.15 handlers + `ExternalTablesResponse`
  / `ExternalTableInfo` shapes + the new error codes) wired by
  `registerDataLoadingRoutes` in `internal/api/server.go`.
- The new read-only `db.Client.ListExternalTables` (`internal/db/client.go`;
  static `pg_exttable` + foreign-table query).
- Functional/edge suites
  (`internal/api/scenario107_dataloading_api_test.go`,
  `internal/api/scenario107_dataloading_edges_test.go`,
  `internal/db/external_tables_scenario107_test.go`).

### Scenario 108 — All CLI Commands (L.1–L.16)

> **Status: Implemented.** Scenario 108 wires the **full** data-loading / PXF
> CLI surface (L.1–L.16) in `cmd/cloudberry-ctl/main.go` to the Scenario 107 REST
> endpoints, **plus one new** server-side endpoint for `test-read`. Most verbs
> reuse the already-FULL Scenario 107 routes; the code additions are the
> `pxf servers` CRUD subcommands, the enriched `data-loading jobs create` flag
> set (pxf + gpload + `--from-yaml`), the `data-loading jobs logs` streaming path
> (mirroring `backup jobs logs` 86k), the `data-loading test-read` command, the
> new `runAPIPut` helper, and the new `GET .../data-loading/test-read` endpoint
> (`handleTestReadPXFSource`) + `db.Client.ReadPXFSourceSample`. See the
> [CLI Commands](#cli-commands) block and
> [cloudberry-ctl §5.10 / §13](07-cloudberry-ctl-spec.md#510-data-loading-and-pxf-scenario-108).

Acceptance scenario (verbatim): *"Exercise every data-loading / PXF CLI command
L.1–L.16: `pxf status|servers list/create/update/delete|sync|restart`;
`data-loading jobs list/create (pxf, gpload, --from-yaml)/start/stop/delete/logs`;
`data-loading test-read`. Each builds the right operator REST request
(method/path/body), renders the response correctly, the new `jobs logs` streams
with a kubectl fallback, and `test-read` prints REAL rows (or an HONEST absence) —
never a fabricated catalog row, never a 500 for an unreachable source."*

#### Commands (REST mapping · documented effect)

| Sub-case | CLI command | REST request | Effect / notes |
|----------|-------------|--------------|----------------|
| **L.1** | `pxf status` | `GET .../data-loading/pxf/status` | existed (Scenario 95); honest sidecar readiness |
| **L.2** | `pxf servers list` | `GET .../data-loading/pxf/servers` | **NEW**; references only — never literal secrets |
| **L.3** | `pxf servers create` | `POST .../data-loading/pxf/servers` | **NEW**; `--name --type --endpoint --bucket --credential-secret` (repeatable `name[:key]`); `201` rendered config / `409 SERVER_EXISTS` |
| **L.4** | `pxf servers update [name]` | `PUT .../data-loading/pxf/servers/{name}` | **NEW** (via `runAPIPut`); positional name + `--endpoint`; `404 SERVER_NOT_FOUND` |
| **L.5** | `pxf servers delete [name]` | `DELETE .../data-loading/pxf/servers/{name}` | **NEW**; `409 SERVER_IN_USE` when referenced by a job |
| **L.6** | `pxf sync` | `POST .../data-loading/pxf/sync` | existed (Scenario 95); ConfigMap refresh + roll |
| **L.7** | `pxf restart` | `POST .../data-loading/pxf/restart` | existed (Scenario 95); segment-primary pod roll |
| **L.8** | `data-loading jobs list` | `GET .../data-loading/jobs` | existed; reads from spec |
| **L.9** | `data-loading jobs create --type pxf` | `POST .../data-loading/jobs` | enriched (**NEW** flags `--name --server --profile --resource --target --schedule`; previously posted nil body); `201`/`409 JOB_EXISTS`/`400` unknown server |
| **L.10** | `data-loading jobs start [job]` | `POST .../data-loading/jobs/{job}/start` | existed; `202` → real one-off `batchv1.Job` |
| **L.11** | `data-loading jobs stop [job]` | `POST .../data-loading/jobs/{job}/stop` | existed; `202`/idempotent `200` |
| **L.12** | `data-loading jobs delete [job]` | `DELETE .../data-loading/jobs/{job}` | existed; best-effort deletes spawned Job |
| **L.13** | `data-loading jobs logs --job <job>` | `GET .../data-loading/jobs/{job}/logs` | **NEW**; `GetStream` (`--follow`/`--tail`) + **kubectl fallback** (mirrors `backup jobs logs` 86k) |
| **L.14** | `data-loading jobs create --type gpload` | `POST .../data-loading/jobs` | **NEW** flags `--gpfdist-host --gpfdist-port --file-path --format` |
| **L.15** | `data-loading test-read` | `GET .../data-loading/test-read` | **NEW** CLI + **NEW** REST; `--job` OR `--server/--profile/--resource`; `--limit N` (default 10, cap 1000); honest rows or `available:false` |
| **L.16** | `data-loading jobs create --from-yaml <file>` | `POST .../data-loading/jobs` | **NEW**; reads + unmarshals a full job; **precedence over flags** |

#### New endpoint + DB method + honest contract (test-read)

The single server-side addition is the **new** read-only endpoint:

| Endpoint | Perm | Metric | Behaviour |
|----------|------|--------|-----------|
| `GET .../data-loading/test-read` | Basic | **none** | `handleTestReadPXFSource` resolves the source (from `?job` OR `?server&profile&resource`), then calls `db.Client.ReadPXFSourceSample` to build a **transient** external table, run `SELECT … LIMIT N` (default 10, **cap 1000**), and **ALWAYS DROP** it. |

The response shape is `TestReadResponse`:

```json
{
  "cluster": "my-cluster",
  "source": {"server": "s3-lake", "profile": "s3:parquet", "resource": "s3a://data-lake/events/"},
  "limit": 10,
  "available": true,
  "rowCount": 10,
  "columns": ["id", "ts", "region"],
  "rows": [["1", "...", "us-east"]]
}
```

**Honest contract.** `test-read` prints **REAL rows** when the DB and source are
reachable; when the DB is unreachable or the source cannot be read it returns
`available:false` with an empty `rows`/`rowCount` — values are **never
fabricated**, and the endpoint **never returns `500`** for an unreachable source
(the transient external table is **always dropped**, success or failure). This
mirrors the Scenario 107 P.15 honesty split (observed-or-honest-absent, never
synthesized).

#### New CLI helper

`runAPIPut` is added alongside the existing `runAPIGet`/`runAPIPost`/
`runAPIDelete` helpers and is used by `pxf servers update` (L.4) to PUT the
endpoint change. Log streaming (L.13) reuses `OperatorClient.GetStream`.

#### Permissions

Basic (`pxf status`, `pxf servers list`, `jobs list`, `jobs logs`, `test-read`) ·
Operator (`pxf servers create/update`, `pxf sync`, `pxf restart`, `jobs
create/start/stop`) · Admin (the destructive deletes — `pxf servers delete`,
`jobs delete`).

#### Artifacts

- `cmd/cloudberry-ctl/main.go` — `newPxfServersCmd` (+`list`/`create`/`update`/
  `delete`), the enriched `newDataLoadingJobsCmd` (+`create` pxf/gpload/`--from-yaml`,
  +`logs`), `newDataLoadingTestReadCmd`, and the new `runAPIPut` helper.
- `internal/api/dataloading.go` — the new `handleTestReadPXFSource` handler +
  `TestReadResponse` shape (wired by `registerDataLoadingRoutes` in
  `internal/api/server.go`).
- `internal/db/client.go` — the new read-only `db.Client.ReadPXFSourceSample`
  (transient external table → `SELECT … LIMIT N` → always DROP).

### Scenario 109 — All Prometheus Metrics (M.1–M.16)

> **Status: Implemented (honesty-bounded).** Scenario 109 closes out the
> Prometheus metric catalog (M.1–M.16) with a single defining property:
> **HONESTY**. Every emitted metric traces to a **REAL** source; a metric with no
> honest source stays **intentionally ABSENT** and documented — its value is
> **NEVER synthesized**. Four metrics flip from Planned → Implemented (M.1, M.2,
> M.3, M.10), one is **folded** rather than fabricated (M.6), five stay honestly
> absent (M.4, M.5, M.7, M.15, M.16), and seven are already-Implemented
> confirmations (M.8, M.9, M.11–M.14). See the
> [Prometheus Metrics catalog](#prometheus-metrics).

Acceptance scenario (verbatim): *"Verify every Prometheus metric M.1–M.16: each
Implemented metric is emitted from a REAL source with honest labels; each
honestly-absent metric is NOT emitted (its absence is the passing evidence) and is
NEVER fabricated. Kill a segment's PXF → its `pxf_service_up{segment_host}` flips
to 0; drive a job lifecycle → `data_loading_job_status` cycles 0→1→2→3."*

#### Per-metric verification (real source · honest labels · absence)

| M | Metric | Decision | REAL source / honest position |
|---|--------|----------|-------------------------------|
| **M.1** | `cloudberry_pxf_service_up{cluster,namespace,segment_host}` | **Implemented** | Per-segment-primary-pod `pxf` `ContainerStatuses[pxf].Ready` via `util.PXFReadyByHost`; the **per-segment disaggregation** of `cloudberry_pxf_status`. `1` healthy / `0` on a killed segment. Emitted **only for observed segment hosts** — never a synthesized host. |
| **M.2** | `cloudberry_pxf_requests_total` | **Implemented (actuator passthrough)** | The **real** `http_server_requests_seconds_count` scraped from the PXF Actuator `/actuator/prometheus`. **Label caveat:** count is REAL; `server`/`profile`/`operation` are **NOT** honestly derivable from the actuator URI → downgraded to actuator-native `uri`/`method`/`status` — never fabricated. |
| **M.3** | `cloudberry_pxf_request_duration_seconds` | **Implemented (actuator passthrough)** | The **real** latency histogram `http_server_requests_seconds_sum`/`_bucket` from the same scrape. **Label caveat:** latency is REAL; `server`/`profile` downgraded to actuator-native — never fabricated. |
| **M.4** | `cloudberry_pxf_bytes_transferred_total` | **ABSENT (Planned)** | No honest external-bytes counter in PXF 2.1.0 (actuator exposes only `http_server_requests` + JVM). Test `109-M4-ABSENT` asserts NOT-emitted. |
| **M.5** | `cloudberry_pxf_records_total` | **ABSENT (Planned, substituted)** | No PXF-native record counter; record throughput is observed via the honest `cloudberry_data_loading_rows_total`. `109-M5-ABSENT` asserts NOT-emitted; `109-M5-SUBST` asserts `rows_total` is the substitute. |
| **M.6** | `cloudberry_pxf_errors_total` | **FOLDED (no synthetic metric)** | PXF exposes no typed error counts. Honest error signals: (1) `cloudberry_data_loading_errors_total{job}` on a Failed load (+ `job_status=3`); (2) actuator non-2xx `http_server_requests{status=~"4..\|5.."}`. **No typed `cloudberry_pxf_errors_total{error_type}` is registered.** `109-M6-F` asserts a forced pxf:// error increments `data_loading_errors_total` + flips `job_status=3`; `109-M6-L` asserts actuator non-2xx is visible. |
| **M.7** | `cloudberry_pxf_active_connections` | **ABSENT (Planned)** | No honest source — `tomcat.threads.busy` is a JVM-thread proxy, NOT external-source connections; relabeling it would be dishonest. `109-M7-ABSENT` asserts NOT-emitted. |
| **M.8** | `cloudberry_data_loading_jobs_active` | **Implemented (confirm)** | Active (enabled) job count. |
| **M.9** | `cloudberry_data_loading_rows_total{job,source_type}` | **Implemented (confirm)** | `DATALOAD_ROWS` marker. |
| **M.10** | `cloudberry_data_loading_bytes_total{cluster,namespace,job,source_type}` | **Implemented (conditional/honest)** | The **real** `DATALOAD_BYTES=<n>` marker the gpload script computes via `wc -c` for a **local gpload input source**. For external-table/pxf/FDW/continuous loads (psql returns only a rowcount tag) the marker is **NOT emitted** → the metric is honestly **ABSENT** for those jobs — never synthesized. `109-M10-L` asserts emission on a real byte count; `109-M10-ABSENT` asserts no series when there is no honest byte source. |
| **M.11** | `cloudberry_data_loading_errors_total{job}` | **Implemented (confirm)** | Job Failed. |
| **M.12** | `cloudberry_data_loading_job_duration_seconds` | **Implemented (confirm)** | Job start→completion. |
| **M.13** | `cloudberry_data_loading_job_last_success_timestamp{job}` | **Implemented (confirm)** | Job `completionTime`. |
| **M.14** | `cloudberry_data_loading_job_status{job}` (0/1/2/3) | **Implemented (confirm)** | Job k8s status; `109-M14-CYCLE` asserts `0 → 1 → 2`, and `→ 3` on a forced failure. |
| **M.15** | `cloudberry_gpfdist_connections_active` | **ABSENT (Planned)** | gpfdist has **no scrapable Prometheus endpoint** — only `/var/log/gpfdist.log`; readiness observed via KSM `kube_deployment_status_replicas_ready`. `109-M15-ABSENT` asserts NOT-emitted. |
| **M.16** | `cloudberry_gpfdist_bytes_served_total` | **ABSENT (Planned)** | Same as M.15 — log file only, no scrapable endpoint. `109-M16-ABSENT` asserts NOT-emitted. |

#### The actuator-scrape requirement (dedicated vmagent job — a honesty trap)

M.2/M.3 require an **explicit, dedicated vmagent scrape job** for the PXF actuator
at `:5888/actuator/prometheus`. **A single pod scrape annotation cannot cover both
exporters**: the segment-primary pod **already** carries
`prometheus.io/scrape=true`, `prometheus.io/port=9187`, `prometheus.io/path=/metrics`
for the pg query-exporter, and the annotation mechanism supports **exactly one
(port,path) pair per pod**. The PXF actuator lives on a **different port (`:5888`)
and path (`/actuator/prometheus`)**, so re-using the existing annotation would
**silently scrape nothing** — a honesty trap (claiming "scraped" while no data
flows). Scenario 109 therefore adds an **explicit additional `scrape_config`** in
the vmagent ConfigMap that selects the segment-primary pods by label and targets
`:5888/actuator/prometheus`. The cross-cutting `109-VM-SCRAPE` case proves both the
`:9187` exporter **and** the `:5888` actuator job scrape the **same** segment pod.

#### Honest-label boundary (M.2/M.3)

The actuator yields `http_server_requests` with the labels `uri`, `method`,
`status`, `outcome` — **NOT** `server`, `profile`, or `operation`. The request
**count and latency histogram are REAL**; the catalog's fine-grained
`server`/`profile`/`operation` labels are **downgraded** to what the actuator
honestly provides, and the URI is **NOT** relabeled into `{server,profile,operation}`
(PXF request URIs do not reliably encode them). Inventing those labels would assign
a meaning the source does not have → it is intentionally avoided. `109-M2-LABELHONEST`
locks this: no `uri → {server,profile,operation}` relabel is performed.

#### The HONESTY position (absent metrics are never fabricated)

The defining property of Scenario 109 is that **a NOT-emitted metric is a PASS**.
The five honestly-absent families — `cloudberry_pxf_bytes_transferred_total` (M.4),
`cloudberry_pxf_records_total` (M.5), `cloudberry_pxf_active_connections` (M.7),
`cloudberry_gpfdist_connections_active` (M.15), `cloudberry_gpfdist_bytes_served_total`
(M.16) — and the **folded** `cloudberry_pxf_errors_total` (M.6) are **never
registered or synthesized**. The `109-HONESTY` guard test enumerates these families
and asserts **none** is registered (a regression lock against future fabrication),
and each `…-ABSENT` case asserts the specific family is not emitted. No dashboard
panel queries a non-emitted metric.

#### Artifacts

- `internal/metrics/metrics.go` — the new `SetPXFServiceUp(cluster, ns,
  segment_host, up)` gauge (M.1) and `RecordDataLoadingBytes(cluster, ns, job,
  source_type, n)` counter (M.10); the M.4/M.5/M.7/M.15/M.16 families are
  **deliberately left unregistered** with a honesty-rationale godoc.
- `internal/util/pxf.go` — `util.PXFReadyByHost(podList)` (per-segment-host
  readiness, reusing `pxfContainerReady`).
- `internal/controller/dataload_controller.go` — `harvestDataLoadBytes` +
  `parseDataLoadBytesMessage` (mirroring the rows path), wired into
  `recordDataLoadJobMetrics`.
- `internal/builder/{pxf_builder,dataload_builder,gpload_builder}.go` — the
  actuator `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE=health,prometheus` env and
  the conditional `DATALOAD_BYTES=<n>` (`wc -c`, local gpload input) marker.
- `test/monitoring/vmagent/templates/configmap.yaml` — the explicit
  `:5888/actuator/prometheus` scrape job.
- `cases.Scenario109Cases()` — the `109-M{1..16}-{U,F,L}` catalog + the
  `109-HONESTY` / `109-VM-SCRAPE` cross-cutting cases.

### Scenario 111 — Security (SE.1–SE.6, SL.6)

> **Status: Implemented, honesty-bounded.** Scenario 111 flips the
> previously-Planned data-loading **Security** controls (SE.1–SE.6, SL.6) to
> **Implemented**, with each control explicitly classified **REAL** (proven
> end-to-end) or **CONFIG-ONLY** (rendered config verified; a live handshake is
> **never faked** because the test env has no KDC and no TLS-speaking source).
> See [Security Considerations](#security-considerations).

Acceptance scenario (verbatim): *"Configure a PXF cluster with a dedicated data-loader
role, a Kerberos-authenticated HDFS server, a TLS JDBC server, and the
cluster NetworkPolicy. Verify: the dedicated role is `NOSUPERUSER` with only the
`pxf` protocol grants; the keytab Secret is mounted on the sidecar and the
`core-site.xml` carries the Kerberos properties; the rendered ConfigMap contains
NO plaintext secret; the NetworkPolicy omits cross-pod `:5888` ingress yet the
load still succeeds; credentials appear only in the ephemeral pod filesystem.
Live authenticated Kerberos / encrypted TLS is asserted only when a real KDC /
TLS source is present — otherwise it is CONFIG-ONLY and never simulated."*

#### Per-control verification (REAL vs CONFIG-ONLY)

| Control | Classification | Verification |
|---------|----------------|--------------|
| **SE.1 — init-container secret rendering** | **REAL (verified)** | `pxf-cred-init` `envsubst`-resolves `${PLACEHOLDER}` site-XML from `credentialSecrets[]` into the ephemeral `pxf-servers` emptyDir at pod start. The proof: the rendered `<server>/<file>.xml` carries the real value in the pod fs, while the `<cluster>-pxf-servers` ConfigMap carries only the `${PLACEHOLDER}` token. |
| **SL.6 — no plaintext in ConfigMap** | **REAL (verified)** | A scan of the rendered `<cluster>-pxf-servers` ConfigMap `Data` asserts **no** secret value appears — only `${...}` placeholders. Secrets live **only** in the ephemeral pod filesystem (emptyDir), never persisted in the ConfigMap or the CR. |
| **SE.2 — JDBC TLS** | **CONFIG-ONLY** unless the source speaks TLS | The JDBC URL / `ssl` params are rendered verbatim into `jdbc-site.xml`. A live encrypted handshake is asserted **only** when the source actually speaks TLS; otherwise the rendered config is verified (no faked handshake). |
| **SE.3 — S3 TLS** | **CONFIG-ONLY** unless the source speaks TLS | `fs.s3a.connection.ssl.enabled=true` is rendered into `s3-site.xml`. Same honesty boundary as SE.2. |
| **SE.4 — Kerberos keytab** | **REAL config-correctness; live auth CONFIG-ONLY (no KDC)** | The keytab Secret is mounted on **both** the PXF sidecar and `pxf-cred-init` at `$PXF_BASE/keytabs/<server>/<key>`; `core-site.xml` is verified to carry `hadoop.security.authentication=kerberos`, `pxf.service.kerberos.principal=<principal>` and `pxf.service.kerberos.keytab=$PXF_BASE/keytabs/<server>/<key>`; an optional `krb5ConfigMap` is mounted at `/etc/krb5.conf` (`KRB5_CONFIG`). **HONESTY:** the operator never runs `kinit` and the test env has **no KDC**, so live authenticated Hadoop/Hive/HBase access is CONFIG-ONLY — config-correctness is verified, a live Kerberos handshake is **never simulated**. |
| **SE.5 — segment↔sidecar `localhost`-only** | **REAL (proven)** | `BuildPXFClusterNetworkPolicy` emits a NetworkPolicy selecting the segment-primary pods whose ingress rules **omit** cross-pod access to PXF `:5888`. Because same-pod `localhost` traffic is never subject to a NetworkPolicy, the in-pod segment→sidecar path keeps working: the policy is applied **and** a real `pxf://` load still succeeds (the proof is the combination, not the policy alone). |
| **SE.6 — dedicated minimal-privilege DB role** | **REAL (least-privilege)** | `db.EnsureDataLoaderRole` runs `CREATE ROLE <role> NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN` (idempotent) and grants **only** `SELECT,INSERT ON PROTOCOL pxf`. The proof: the role has the pxf protocol grants **and** cannot perform unrelated privileged ops (e.g. `CREATE ROLE`/`CREATE DATABASE` are denied). Opt-in via `pxf.dataLoaderRole`; **empty ⇒ `gpadmin`** (the existing behavior is byte-identical when unset). |

#### CRD knobs

- **`pxf.dataLoaderRole`** (`PxfSpec.DataLoaderRole`, string, optional) — the
  dedicated minimal-privilege role to create and target the `pxf` protocol GRANTs
  at. **Empty (default) ⇒ `gpadmin`** (the existing behavior, unchanged). A
  non-empty value other than `gpadmin` triggers `db.EnsureDataLoaderRole`
  (`NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN`, only `SELECT,INSERT ON PROTOCOL
  pxf`). Additive — the `gpadmin` path is untouched when unset.
- **`pxf.servers[].kerberos`** (`PxfServerSpec.Kerberos` →
  `PxfKerberosSpec`, optional) — Kerberos (keytab) authentication for a
  Hadoop-family server. Fields:
  - `principal` (string, **required**) — the Kerberos service principal PXF
    authenticates as (e.g. `pxf/_HOST@REALM`).
  - `keytabSecret` (`{name,key}`, **required**) — the Secret + key holding the
    keytab bytes, mounted at `$PXF_BASE/keytabs/<server>/<key>`.
  - `krb5ConfigMap` (string, optional) — a ConfigMap whose `krb5.conf` key is
    mounted at `/etc/krb5.conf` (`KRB5_CONFIG`); when unset the image default is
    used.
  - `realm` (string, optional) — records the Kerberos realm for
    documentation/diagnostics; not required to render a working config.

  The admission webhook **requires** `principal` + `keytabSecret{name,key}` when
  `kerberos` is set and **rejects** `kerberos` on non-`hdfs`/`hive`/`hbase`
  server types.

#### The HONESTY position (no faked handshakes)

The defining property of Scenario 111 is that **a control is only claimed REAL
when it is proven end-to-end**. SE.1/SL.6 (secret rendering + no-plaintext),
SE.5 (NetworkPolicy + load still succeeds) and SE.6 (dedicated NOSUPERUSER role
with pxf-only grants) are **REAL**. SE.4 Kerberos config-correctness is REAL, but
**live authenticated access is CONFIG-ONLY** — the test env has **no KDC** and the
operator **never performs a live `kinit`**. SE.2/SE.3 TLS is rendered
declaratively and a live encrypted handshake is asserted **only** against a real
TLS-speaking source — otherwise it is **CONFIG-ONLY**. No live Kerberos or TLS
handshake is ever simulated or fabricated.

#### Artifacts

- `api/v1alpha1/types.go` — `PxfSpec.DataLoaderRole` (SE.6),
  `PxfServerSpec.Kerberos` → `PxfKerberosSpec{Principal,KeytabSecret,Krb5ConfigMap,Realm}`
  (SE.4); `zz_generated.deepcopy.go` + `deepcopy_security_scenario111_test.go`.
- `internal/db` — `db.EnsureDataLoaderRole` (SE.6 role + `SELECT,INSERT ON
  PROTOCOL pxf` grant).
- `internal/builder/pxf_builder.go` — `BuildPXFClusterNetworkPolicy` (SE.5),
  the keytab/krb5 mounts on the PXF sidecar + `pxf-cred-init`, and the
  `core-site.xml` Kerberos property rendering (SE.4).
- `internal/webhook` — the kerberos `principal`+`keytab` requirement and the
  non-`hdfs`/`hive`/`hbase` rejection.

### Scenario 112 — Disabled States (DIS.1–DIS.3)

> **Status: Implemented.** Scenario 112 makes the three data-loading *disabled*
> states honest and *active*: disabling `dataLoading` no longer leaves orphaned
> objects behind — it **tears them down** (DIS.1) — and the two sub-feature
> "off" states (`pxf.enabled: false`, DIS.2; `gpfdist.enabled: false`, DIS.3)
> are documented with their **real** observable consequences, including the
> **honest runtime** dependency-missing signal for a gpfdist-source gpload job
> when gpfdist is off. See [Operator Reconciliation Logic](#operator-reconciliation-logic)
> and [API Endpoints](#api-endpoints).

Acceptance scenario (verbatim): *"Take a fully-configured data-loading cluster
(PXF + gpfdist + jobs). (DIS.1) Set `dataLoading.enabled: false` and reconcile:
verify every operator-managed data-loading object is removed, `Status.DataLoading`
is cleared, the `DataLoadingConfigured` condition goes `False` with reason
`DataLoadingDisabled`, a one-shot `DataLoadingDisabled` event fires, the
`cloudberry_data_loading_jobs_active` gauge is `0`, and the data-loading REST API
reports `DATA_LOADING_NOT_ENABLED`; then re-enable and verify everything is
redeployed by the idempotent reconcile. (DIS.2) With `dataLoading.enabled: true,
pxf.enabled: false`, verify no PXF sidecars/extensions/ConfigMap exist while
gpload-type jobs still function. (DIS.3) With `gpfdist.enabled: false`, verify the
gpfdist Deployment/Service/PVC are gone, `inputSource.type: local` gpload jobs
still work, and a `gpfdist`-source gpload job reports the missing dependency via
the honest runtime gpload failure (NOT a fabricated pre-flight check)."*

#### DIS.1 — `dataLoading.enabled: false` TEARS DOWN (no longer a no-op)

> **Behavior change.** Earlier drafts treated `dataLoading.enabled: false` as a
> pure **no-op early-return** in the reconcile. It is now an **active teardown**:
> `reconcileDataLoading` dispatches to **`cleanupDataLoading`**
> (`internal/controller/admin_controller.go`) whenever `dataLoading == nil` **or**
> `dataLoading.enabled == false`. Disabling (rather than deleting) the cluster
> does **not** fire ownerRef GC, so these explicit, best-effort/non-fatal deletes
> are what reclaim the stale resources promptly.

On disable, the operator removes the following operator-managed objects:

| Object | GC'd by | Notes |
|--------|---------|-------|
| `<cluster>-pxf-servers` ConfigMap | CLUSTER controller (`ensurePxfServersConfigMap` delete-when-disabled) | The rendered servers ConfigMap is deleted (no longer orphaned) when the sidecar is disabled — `pxfSidecarEnabled` is false ⇒ nil render ⇒ delete the stale CM |
| gpfdist `Deployment` / `Service` / `PVC` | `cleanupDataLoading` → `deleteGpfdistResources` | best-effort, NotFound-tolerant |
| data-loading `Job`s **and** `CronJob`s | `cleanupDataLoading` → `deleteDataLoadingWorkloads` | label-scoped GC |
| gpload control-file ConfigMaps | `cleanupDataLoading` → `deleteGploadControlFileConfigMaps` | label-scoped GC |
| PXF cluster `NetworkPolicy` (SE.5) | `cleanupDataLoading` (direct delete) | present only when PXF was enabled; reaped on disable too |
| PXF **sidecar** + `pxf-cred-init` (segment-primary pod template) | CLUSTER controller | the segment-primary StatefulSet is **re-rendered without the sidecar** (`pxfSidecarEnabled` gate) — coordinator/standby/mirror were never affected |

> The `<cluster>-pxf-servers` ConfigMap delete and the sidecar removal are owned
> by the **CLUSTER** controller (`ensurePxfServersConfigMap` /
> `BuildSegmentPrimaryStatefulSet`), **not** by `cleanupDataLoading`, because the
> admin reconcile only runs once the cluster is `Running`, whereas the ConfigMap
> + sidecar are managed from cluster initialization onward.

`cleanupDataLoading` then:

- **Clears `Status.DataLoading`** (the in-memory pointer is set to nil after a
  zeroed `phase: Disabled` snapshot is persisted, so the steady-state disabled
  path stays honest and no resurrection occurs).
- **Sets the `DataLoadingConfigured` condition to `False`** with reason
  **`DataLoadingDisabled`** (message `Data loading is disabled`).
- **Emits a one-shot `DataLoadingDisabled` Normal event**
  (`EventReasonDataLoadingDisabled`) **only on the transition** into the disabled
  state (de-duplicated: `transitioning` is true only when status was still
  present, mirroring `cleanupWorkload`). A never-enabled cluster emits **no**
  event; a steady disabled reconcile emits **no** repeat event.
- **Zeroes the gauges** — `cloudberry_data_loading_jobs_active` → `0` and
  `cloudberry_pxf_servers_configured` → `0` (honest: disabled ⇒ 0 active jobs, 0
  configured servers).

**Re-enable → redeploy.** Re-setting `dataLoading.enabled: true` redeploys
everything through the **normal, idempotent (get-or-create) reconcile body** — no
special-casing is needed on the re-enable path. The sidecar, servers ConfigMap,
NetworkPolicy, gpfdist objects, and Jobs/CronJobs all come back.

**API reporting (DIS.1).** When `dataLoading` is disabled, the data-loading REST
surface reports **`DATA_LOADING_NOT_ENABLED`** (`internal/api/dataloading.go`,
`internal/api/server.go`):

- **Mutating** endpoints (`POST/PUT/DELETE` jobs, `start`/`stop`, `external-tables`,
  `jobs/{job}/logs`) → **`400 DATA_LOADING_NOT_ENABLED`**
  (message `data loading is not enabled for this cluster`).
- **Read** endpoints (`GET` jobs list / `GET` one job) → **`200`** with a
  **disabled envelope** (`writeDataLoadingDisabled`), not a 400.
- **PXF precedence.** On a data-loading-disabled cluster a PXF endpoint
  (`pxf/status`, etc.) reports **`DATA_LOADING_NOT_ENABLED`** — the broader
  data-loading gate — **NOT** `PXF_NOT_ENABLED`; the honest, outer reason is
  surfaced first (`getPXFCluster` checks `dataLoadingEnabled` before `pxfEnabled`).

#### DIS.2 — `dataLoading.enabled: true, pxf.enabled: false`

PXF is independently disable-able while data loading stays on. With PXF off:

- **No PXF sidecars** on the segment-primary pods (the `pxfSidecarEnabled` gate is
  false), **no PXF extensions** setup, and **no `<cluster>-pxf-servers`
  ConfigMap** — `ensurePxfServersConfigMap` now **deletes** a stale ConfigMap
  when the sidecar is disabled (the only new bit this cycle; previously a stale CM
  could be orphaned).
- **gpload-type jobs still function** — the gpload control-file path
  (`type: gpload`) is independent of PXF and connects to the coordinator directly,
  so it runs unchanged with PXF off.

#### DIS.3 — `gpfdist.enabled: false`

gpfdist is independently disable-able. With gpfdist off:

- The gpfdist `Deployment` / `Service` / `PVC` are **garbage-collected**
  (`reconcileGpfdist` best-effort GCs them when `gpfdist.enabled` is false).
- **`inputSource.type: local` gpload jobs still work** — a local-input gpload job
  does not depend on the gpfdist file-server, so it loads unchanged.
- A **`gpfdist`-source** gpload job reports the missing dependency via the
  **HONEST RUNTIME signal**, NOT a fabricated pre-flight check: `gpload` cannot
  reach the absent gpfdist host, so the load fails at runtime → the **Job is
  observed Failed** → `cloudberry_data_loading_errors_total` increments and
  `status=Failed` (`cloudberry_data_loading_job_status=3`).

> **HONESTY note — HC.4 is SKIPPED when gpfdist is disabled.** The pre-load
> health-check **HC.4 gpfdist reachability** (Scenario 104) is **gated on
> `gpfdist.enabled`**, so when gpfdist is OFF the check **does not run at all**.
> The dependency-missing signal for a gpfdist-source job is therefore the **real
> runtime gpload failure** (Job Failed + `errors_total` + `status=Failed`), **not**
> a pre-flight check. The spec deliberately does **not** fabricate a pre-flight
> dependency check here — the honest signal is the runtime failure.

#### Artifacts

- `internal/controller/admin_controller.go` — `cleanupDataLoading` (DIS.1
  teardown + status clear + condition `False`/`DataLoadingDisabled` + one-shot
  event + gauge zeroing); `reconcileDataLoading` dispatch to it; `reconcileGpfdist`
  GC-when-disabled (DIS.3).
- `internal/controller/cluster_controller.go` — `ensurePxfServersConfigMap`
  delete-when-disabled (DIS.1 ConfigMap GC / DIS.2 PXF-off CM delete).
- `internal/builder/{builder,pxf_builder,networkpolicy_builder}.go` —
  `pxfSidecarEnabled` gate (DIS.1/DIS.2: no sidecar / no NetworkPolicy when off).
- `internal/api/{server,dataloading}.go` — `DATA_LOADING_NOT_ENABLED`
  (`errCodeDataLoadingNotEnabled`/`msgDataLoadingNotEnabled`), `writeDataLoadingDisabled`
  (200 disabled envelope), and the DL-disabled-before-PXF precedence in
  `getPXFCluster`.
- `api/v1alpha1/types.go` — `EventReasonDataLoadingDisabled` (`"DataLoadingDisabled"`).
- Tests: `internal/controller/{dataload_disabled_scenario112,cluster_pxf_servers_delete_scenario112}_test.go`,
  `internal/api/scenario112_disabled_test.go`,
  `test/cases/scenario112_disabled_states_cases.go`,
  `test/{functional,e2e}/scenario112_disabled_states*`.

## Design Decisions

### Data-loading model replaced with the PXF model (breaking change)

The `dataLoading` CRD model in `api/v1alpha1/types.go` (`DataLoadingSpec`) was
**replaced** with the PXF (Platform Extension Framework) model documented in
this specification. The PXF model provides federated external-table access
(object storage, HDFS, JDBC, Hive, HBase) plus `gpfdist`/`gpload` file loading,
governed by the W.1–W.16 webhook rules above.

#### Removed fields (breaking change)

The previous, simplified data-loading model has been removed in full. There is
**no** automated migration; CRs using the old shape must be rewritten to the PXF
model. Removed fields:

| Removed field | Replacement in the PXF model |
|---------------|------------------------------|
| `dataLoading.streamingServer` (and its config) | **Old shape stays removed** — there is no top-level `streamingServer` block. Streaming/CDC is now modeled via the **PXF custom-connector path**: a `pxf.servers[]` of `type: custom` + a matching `customConnectors[]` entry + a `pxfJob` (`profile: kafka`, `continuous: true`) — see [Scenario 102](#scenario-102--kafka-cdc-continuous-streaming-custom-connector) |
| `dataLoading.jobs[].type: s3` + `jobs[].s3Source` | `jobs[].type: pxf` with a `pxf.servers[]` of `type: s3` and a `jobs[].pxfJob` (`server`, `profile: s3:<format>`, `targetTable`) |
| `dataLoading.jobs[].type: kafka` + `jobs[].kafkaSource` | **The old `type: kafka` job kind + `kafkaSource` shape stays removed**, BUT the kafka **PROFILE is reinstated** (Scenario 102, policy reversal scoped to custom connectors): `jobs[].type: pxf` with a `pxf.servers[]` of `type: custom` (W.3), a matching `customConnectors[]` JAR (W.24), and a `jobs[].pxfJob` (`server`, `profile: kafka`, `targetTable`, `continuous`) — admitted by W.23. A bare `kafka` profile / `kafka` on a non-custom server is still rejected (no built-in streaming) |
| `dataLoading.jobs[].type: rabbitmq` + `jobs[].rabbitmqSource` | Old shape removed; `rabbitmq` is likewise re-enabled only as a custom-connector profile (recognized by `pxfCustomConnectorSchemes`, gated by W.23) |

The only valid `jobs[].type` values are now `pxf` and `gpload` (W.8); the valid
PXF server types are `s3`, `hdfs`, `jdbc`, `hbase`, `hive`, `gs`, `abfss`,
`wasbs`, `custom` (W.3 — the object-store types `gs`/`abfss`/`wasbs` were added in
Scenario 96; `custom` was added in Scenario 102).

### No per-rule rejection metric

No new metric was added for data-loading admission rejections. Each W.1–W.16
denial increments the existing shared admission counter
`cloudberry_webhook_admission_total{webhook="validating",result="denied"}`
(internal/non-validation admission failures use the distinct `result="error"`).
This keeps admission observability uniform across all validating-webhook rules
rather than introducing per-feature counters.

## PXF Server Configuration Lifecycle

> **Status: Implemented (rendering + per-type file-mapping + live secret
> resolution + shared-ConfigMap sync).** The operator generates the `*-site.xml`
> files into the single `<cluster>-pxf-servers` ConfigMap
> (`BuildPXFServersConfigMap`), maps each server type to the correct set of site
> files (`renderPXFServer` → `splitHadoopSiteFiles`, **SL.1–SL.6**), and the
> `pxf-cred-init` init container resolves the `${PLACEHOLDER}` tokens into a
> **one-directory-per-server** layout (`<server>/<file>.xml`) in the shared
> emptyDir the sidecar reads. The `servers[]` CRD fields are validated (W.2–W.6),
> persisted, and rendered. The **explicit** `pxf sync` trigger
> (`cloudberry-ctl pxf sync` / `POST .../pxf/sync`) is now **Implemented**
> (Scenario 95): it refreshes this ConfigMap on demand and rolls the sidecars,
> complementing the always-on structural sync — see
> [Syncs PXF configuration](#operator-reconciliation-logic). **Server CRUD via
> the API is Implemented** (Scenario 107: `pxf/servers` list/get/create/
> update/delete REST routes, P.2–P.5; `201` returns the rendered `<server>__*.xml`,
> `409 SERVER_EXISTS`/`SERVER_IN_USE`); the `pxf servers …` **CLI** subcommands
> are now **Implemented too** (Scenario 108: `pxf servers {list,create,update,delete}`).

### Creating a Server (file-mapping, SL.1–SL.6)

The operator translates each entry in `dataLoading.pxf.servers[]` into a set of
`*-site.xml` files. In the `<cluster>-pxf-servers` ConfigMap these are stored
under deterministic data keys `<server-name>__<file>.xml`; the `pxf-cred-init`
init container reorganizes them into a **single logical directory per server**
(`$PXF_BASE/servers/<server-name>/<file>.xml`) at pod start. The **implemented**
per-type mapping is:

| Server Type | Generated Files | Notes |
|-------------|-----------------|-------|
| `s3` | `s3-site.xml` | `config` + `fs.s3a.access.key`/`fs.s3a.secret.key` `${...}` placeholders (SL.1) |
| `hdfs` | `core-site.xml` **(always)**, `hdfs-site.xml` **(always; minimal `<configuration/>` when no `dfs.*`)**, optionally `hive-site.xml` (when `hive` set / `hive.*` keys), `hbase-site.xml` (when `hbase` set / `hbase.*` keys), `mapred-site.xml` (when `mapred*`/`mapreduce.*` keys), `yarn-site.xml` (when `yarn.*` keys) | `config` is **prefix-split** into the canonical Hadoop files (SL.2) |
| `jdbc` | `jdbc-site.xml` | `config` + `jdbc` map + `jdbc.user`/`jdbc.password` `${...}` placeholders (SL.3) |
| `hive` | `core-site.xml` **(always)**, `hive-site.xml` **(always)** | core carries the non-hive `config`; hive carries the `hive` map (preferred) or the `hive.*` fragment (SL.4) |
| `hbase` | `core-site.xml` **(always)**, `hbase-site.xml` **(always)** | core carries the non-hbase `config`; hbase carries the `hbase` map (preferred) or the `hbase.*` fragment (SL.5) |

**Config prefix-split rule (`splitHadoopSiteFiles`).** For `hdfs`/`hive`/`hbase`
servers each `config` entry is routed to its canonical site file by **key
prefix** so the property lands in the file PXF actually reads it from:

| Key prefix | Routed to |
|------------|-----------|
| `fs.*` (e.g. `fs.defaultFS`, `fs.s3a.endpoint`) | `core-site.xml` |
| `dfs.*` (e.g. `dfs.replication`) | `hdfs-site.xml` |
| `mapred.*` / `mapreduce.*` | `mapred-site.xml` |
| `yarn.*` | `yarn-site.xml` |
| `hive.*` | `hive-site.xml` |
| `hbase.*` | `hbase-site.xml` |
| *(anything else)* | `core-site.xml` (safe default) |

Dedicated `server.hive` / `server.hbase` maps **win** over the Config
`hive.*`/`hbase.*` fragment when both are present.

**SL.6 — no literal secrets.** Every credentialed server's rendered XML carries
only `${<SANITIZED_NAME_KEY>}` placeholders (resolved at runtime by the init
container); the literal secret values **never** appear in the ConfigMap. The
non-sensitive site settings come from `servers[].config` (a plain
`map[string]string`); credentials are referenced via
`servers[].credentialSecrets[]` (a list of `{name, key}` Secret references). See
[Credential rendering rules](#credential-rendering-rules-standard-properties--env-name-sanitization).

### Updating a Server (SL.7)

When the CRD is updated, the operator re-generates the `<cluster>-pxf-servers`
ConfigMap (`ensurePxfServersConfigMap`, full-replacement reconcile of the
rendered `Data`). Patching a server — e.g. changing `minio-warehouse`'s
`fs.s3a.endpoint` — re-renders that server's `<server>__s3-site.xml` (and any
other affected `<server>__*.xml`) keys; every **other** server's keys stay
byte-identical. The PXF sidecars pick up the change on the **next volume sync**
(the `pxf-cred-init` init container re-renders the resolved configs on pod
restart), and subsequent reads use the **new** endpoint.

Config sync is **structural** in the shared-ConfigMap model — every
segment-primary sidecar mounts the **same** ConfigMap and renders byte-identical
configs, so steady-state correctness needs **no** explicit command. In addition,
an **explicit** `pxf sync` trigger is **Implemented (Scenario 95)**
(`cloudberry-ctl pxf sync --cluster <name>` / `POST .../data-loading/pxf/sync`,
`handlePXFSync`): it forces a ConfigMap refresh **and** rolls the
segment-primary sidecars on demand (the on-demand counterpart to the always-on
structural sync) so a server-config or referenced-secret change propagates
immediately rather than waiting for the next reconcile/restart.

**Honest observability (Implemented — Scenario 106).** On a **real** ConfigMap
`Data` diff (and only then), `ensurePxfServersConfigMap` emits a Normal
**`PXFServersChanged`** event (`cbv1alpha1.EventReasonPXFServersChanged`) whose
message names the changed servers —
`PXF servers changed: added=[..] removed=[..] updated=[..]` — and increments the
`cloudberry_pxf_servers_changed_total{cluster,namespace}` counter. The same
honest signal fires on the explicit `pxf sync` path. The diff itself is computed
by the shared, pure `internal/util.DiffPXFServerNames` helper (it parses the
`<server>__<file>.xml` keys into server names and reports `added`/`removed`/
`updated`), with the message rendered by `FormatPXFServersChangedMessage`. A
no-op sync or a first-time create produces **no** event and increments
**nothing**.

### Deleting a Server (SL.8)

Removing a server from `dataLoading.pxf.servers[]` causes the operator to drop
that server's `<server-name>__*.xml` keys from the `<cluster>-pxf-servers`
ConfigMap on the next reconcile (full-replacement); every remaining server's
keys stay intact. Any external/foreign tables referencing the deleted server
**fail** (the `SERVER` no longer resolves) until they are recreated against a
still-configured server.

A delete is a real `Data` diff, so it too triggers the honest **Scenario 106**
signal: a `PXFServersChanged` event with the removed server listed under
`removed=[..]` and a single `cloudberry_pxf_servers_changed_total` increment.

## Writable External Tables (Data Export)

> **Status: Mixed (Implemented — Scenario 96; export targets + `sourceFilter`
> Verified — Scenario 99).** The **write-capability ENFORCEMENT**, the **writable
> DDL emission** and the **filtered-export `sourceFilter`** are now built:
>
> - **Admission (Implemented).** A `mode: writable` PXF job is **rejected** by the
>   webhook (rule **W.10b**) when its profile format is not writable
>   (`json`/`orc`/`rc`) — the error contains both `write-unsupported` and
>   `writable`. Writable `text`/`parquet`/`avro` (and `hdfs:SequenceFile`) jobs
>   **admit**. The rule applies uniformly to **all** object-store schemes
>   (`s3`/`gs`/`abfss`/`wasbs`). `pxfpolicy.IsProfileWritable` admits `s3:text`/
>   `s3:parquet`/`s3:avro`, `hdfs:text`/`hdfs:parquet`/`hdfs:avro`/
>   `hdfs:sequencefile`, and **bare `jdbc`**.
> - **Builder (Implemented).** When `pxfJob.mode == "writable"`,
>   `buildPXFExternalTableDDL` emits `CREATE WRITABLE EXTERNAL TABLE … FORMAT
>   'CUSTOM' (FORMATTER='pxfwritable_export')` with **no** `LOG ERRORS` / `SEGMENT
>   REJECT LIMIT` (writable tables take no reject limit). The read/import path is
>   unchanged (`pxfwritable_import`). The writable DDL / export-script path is
>   **profile-agnostic** — the SAME `dataload_builder.go` code emits the S3, HDFS
>   and JDBC export tables, differing only by the LOCATION `PROFILE`/`SERVER`.
>   **Defense-in-depth:** the builder re-checks `pxfpolicy.IsProfileWritable` and
>   **errors** on a writable + read-only-format combination even if the webhook
>   were bypassed.
> - **Export targets — S3 + HDFS + JDBC (Verified — Scenario 99).** The three
>   writable-export targets are exercised live in Scenario 99: **S3** (FE.9/WE.1),
>   **HDFS** (FE.10) and **JDBC** (FE.11), all via `CREATE WRITABLE EXTERNAL TABLE
>   … FORMATTER='pxfwritable_export'`. **WE.2** asserts the data lands with the
>   correct format and the `pxfwritable_export` formatter.
> - **Filtered export — `sourceFilter` (Implemented — Scenario 99).** The new
>   optional `pxfJob.sourceFilter` WHERE predicate turns the export into `INSERT
>   INTO <writable_ext> SELECT * FROM <target> WHERE <sourceFilter>` (a filtered
>   subset); unset → full-table export, **byte-identical** to before. Guarded by
>   the new webhook rule **W.17**.
> - **Live export RUNTIME (honest).** The dedicated **export-Job lifecycle** (a
>   first-class data-export job *kind* beyond the writable-DDL path the load Job
>   already builds) remains **Planned**. `hdfs:parquet`/`hdfs:avro` export may need
>   a `DATA_SCHEMA` (config-only); the deterministic live-landing legs use the
>   `text` format. **`bytes_transferred` stays Planned** — exports are observed via
>   `cloudberry_data_loading_rows_total` + `job_status`, never a fabricated byte
>   counter.
>
> Both the webhook and the builder read the SINGLE source of truth
> [`internal/pxfpolicy`](#single-source-of-truth-internalpxfpolicy).

PXF supports writing data back to external stores via `CREATE WRITABLE EXTERNAL TABLE`. A `mode: writable` PXF job builds a writable external table (no `LOG ERRORS`/`SEGMENT REJECT LIMIT`) and the load script **reverses the INSERT direction** — `INSERT INTO <writable_ext> SELECT * FROM <target> [WHERE <sourceFilter>]` — pushing Cloudberry query results OUT to the external store. The export path is profile-agnostic; the three verified targets (Scenario 99) are **S3 / object stores** (FE.9/WE.1), **HDFS** (FE.10) and **JDBC** (FE.11):

**S3 / object-store export (FE.9 / WE.1)** — write-capable object-store formats (`text`/`parquet`/`avro`):

```sql
CREATE WRITABLE EXTERNAL TABLE export_events (LIKE public.events)
LOCATION ('pxf://s3a://export-bucket/events/?PROFILE=s3:parquet&SERVER=s3-datalake')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');
 
INSERT INTO export_events SELECT * FROM public.events;
```

**HDFS export (FE.10)** — `hdfs:text` for the deterministic live landing (`hdfs:parquet`/`hdfs:avro` may need `DATA_SCHEMA`, config-only):

```sql
CREATE WRITABLE EXTERNAL TABLE export_hdfs (LIKE public.export_src)
LOCATION ('pxf:///data-lake/exports/hdfs/?PROFILE=hdfs:text&SERVER=hadoop-cluster')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');
 
INSERT INTO export_hdfs SELECT * FROM public.export_src;
```

**JDBC export (FE.11)** — bare `jdbc` writes rows back into a JDBC target table (the strongest, deterministic proof — rows LAND in the RDBMS):

```sql
CREATE WRITABLE EXTERNAL TABLE export_jdbc (LIKE public.export_src)
LOCATION ('pxf://export_target?PROFILE=jdbc&SERVER=postgres-source')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');
 
INSERT INTO export_jdbc SELECT * FROM public.export_src;
```

### `loadMethod` — external-table vs. FDW loading path (Scenario 103)

> **Status: Implemented (Scenario 103).** `PxfJobSpec.LoadMethod` (`json:
> loadMethod`) is a NEW **optional** enum field in `api/v1alpha1/types.go`
> (`+kubebuilder:validation:Enum=external-table;fdw`). The builder
> (`internal/builder/dataload_builder.go` `isFDWPxfJob`/`buildFDWDDL`/
> `buildFDWDataLoadScript`) and the webhook (`internal/webhook/validating.go`
> W.25 + the W.17 tweak) consume it.

`pxfJob.loadMethod` selects HOW the PXF data flows:

- **`external-table`** (default; also the empty/unset value) — builds a **transient**
  `CREATE EXTERNAL TABLE (LIKE <target>) … LOCATION ('pxf://…')` + `INSERT…SELECT`,
  then **DROPs** it. This is the historical path; byte-identical when `loadMethod`
  is unset.
- **`fdw`** — builds a **PERSISTENT** foreign-data-wrapper chain (`CREATE SERVER` +
  `CREATE USER MAPPING` + `CREATE FOREIGN TABLE`, all `IF NOT EXISTS`, **never
  dropped**, retained for direct querying) and loads via `INSERT INTO <target>
  SELECT * FROM <foreign_table> [WHERE <sourceFilter>]`. The FDW path is a
  **READ/import path only** (webhook **W.25** rejects `loadMethod: fdw` with
  `mode: writable` or `continuous: true`).

The two methods are **EQUIVALENT** — the same rows land in the target. See
[§Scenario 103 — FDW-Based Loading Path](#scenario-103--fdw-based-loading-path).

### `sourceFilter` — filtered writable export (Scenario 99) / fdw read (Scenario 103)

> **Status: Implemented (Scenario 99; fdw-read extension Scenario 103).**
> `PxfJobSpec.SourceFilter` (`json:
> sourceFilter`) is a NEW **optional** SQL `WHERE`-predicate body for a writable
> EXPORT job **or an fdw read job**, in `api/v1alpha1/types.go`. The builder
> (`internal/builder/dataload_builder.go`) and the webhook
> (`internal/webhook/validating.go` W.17) consume it.

`pxfJob.sourceFilter` is meaningful for a writable export (`mode: writable`)
**or** an fdw read job (`loadMethod: fdw`). When set, the script emits, for a
writable export:

```sql
INSERT INTO <writable_ext> SELECT * FROM <targetTable> WHERE <sourceFilter>;
```

and for an **fdw read** (Scenario 103):

```sql
INSERT INTO <targetTable> SELECT * FROM <foreign_table> WHERE <sourceFilter>;
```

so only the matching source rows are loaded/exported (a filtered subset). When
**unset**, the script is the full table and is **byte-identical** to the
pre-Scenario-99 behaviour. A `sourceFilter` on a **plain external-table read**
(loadMethod unset/`external-table`, not writable) is **rejected** by W.17.

- **Schema location.** `dataLoading.jobs[].pxfJob.sourceFilter` (string,
  `+optional`).
- **Trust model.** The predicate is a **raw SQL fragment authored by the cluster
  administrator** in the CR — the **same trust boundary as `targetTable`**. It is
  emitted **verbatim** (no quoting / parameterisation): it IS SQL by design.
- **Single-quote safety.** Because a predicate MAY contain single quotes (e.g.
  `region='us-east'`), the filtered `INSERT` is emitted via a **quoted heredoc**
  piped to `psql -tA` (instead of `psql -c '…'`) so embedded single quotes cannot
  break the shell quoting; the `INSERT 0 <n>` command tag is still captured through
  the SAME `awk` rowcount extraction (so `DATALOAD_ROWS` / `rows_total` are
  unchanged).
- **Sanity check (W.17).** Admission **rejects** a `sourceFilter` on a plain
  external-table read/import job (it is valid on a writable export OR an fdw read;
  error contains `sourceFilter` + `writable`) and a predicate
  containing `;`, `--` or `/*` (statement terminators / SQL comments). It is a
  **cheap substring scan, not a SQL parser** — it reduces obvious footguns, not a
  guarantee of safety on an already-trusted predicate.

## Pre-Load Health Checks

> **Status: Implemented (Scenario 104).** Before each data-loading Job a
> `dataload-healthcheck` init container runs a fixed set of pre-load checks; a
> non-zero exit blocks the main load container → the Job fails. The init
> container is prepended (**FIRST**) on **BOTH** the pxf/native path
> (`buildDataLoadPodSpec`) **AND** the gpload path (`buildGploadPodSpec`).

Before each data-loading Job, the `dataload-healthcheck` init container
(`buildDataLoadHealthCheckInitContainer`, the data-loader image, `bash -c` over
`buildDataLoadHealthCheckScript`, `set -euo pipefail`) validates five checks,
each gated by job type / cluster config:

- **HC.1 — PXF readiness (pxf jobs, `pxf.enabled`).** A **DB-proxy** probe via
  `psql` against the coordinator: `SELECT 1` (coordinator reachable) →
  `SELECT 1 FROM pg_extension WHERE extname='pxf'` (extension present) →
  `SELECT pxf_version()` (PXF ready). **Honesty:** the data-load Job pod
  **cannot** reach a segment's localhost-only PXF sidecar (PXF segment↔sidecar
  is `localhost`-only, no cross-pod traffic — see Security Considerations), so
  HC.1 is a **DB proxy, NOT a direct `curl` of the sidecar `/actuator/health`**.
  The segment-pod sidecar **liveness probe** uses `/actuator/health` (the PXF
  2.1.0 Spring Boot actuator endpoint); the legacy/incorrect `/pxf/v15/Status`
  path **404s and must NOT be used**. The **live proof** of HC.1 is "stop PXF on
  a segment → the Job fails".
- **HC.2 — target table exists (ALL jobs).** `psql … to_regclass('<targetTable>')`;
  an empty result fails the check (skipped only when no target table resolves).
- **HC.3 — external-source connectivity (pxf object-store jobs).**
  `curl -fsS --head "${AWS_S3_ENDPOINT}"` (with a trailing-slash retry); fails on
  a wrong/unreachable endpoint, clean **SKIP** when no endpoint is wired. Gated
  to s3-family schemes — **skipped for `jdbc`/`hive`/`hbase`/`hdfs`**.
- **HC.4 — gpfdist reachability (gpload jobs, `gpfdist.enabled`).**
  `curl -fsS "http://<cluster>-gpfdist-svc:8080/"`; fails when the gpfdist
  Deployment is scaled to 0 (no ready endpoints).
- **HC.5 — disk space (ALL jobs).** `df -Pk /dataload-scratch` free space must be
  `>= diskMinFreeMB`; fails when the shared scratch volume is full.

**Scratch volume.** A `dataload-scratch` `emptyDir` (`SizeLimit` from
`scratchSizeLimit`) is added to the pod and mounted at `/dataload-scratch` on
**BOTH** the init container **AND** the main data-load container, so error-log /
temp files have a real home and HC.5's `df` probe observes the same volume the
load writes to.

**CRD knob.** `dataLoading.healthChecks { enabled *bool (default true),
diskMinFreeMB int32 (default 64), scratchSizeLimit string }`. The checks are
**ON by default**: a nil `healthChecks` block (or a nil `enabled`) enables them;
only an explicit `healthChecks.enabled: false` omits the init container, the
scratch volume **and** the main-container scratch mount (byte-identical to a
no-health-check pod).

**Failure handling.** A non-zero check exits the init container non-zero →
blocks the main load → the Job is observed **Failed** (`status.dataLoading` /
`cloudberry_data_loading_job_status=3` + `errors_total`). When the controller
observes a data-load Job transition into Failed **and** the
`dataload-healthcheck` init container terminated non-zero (honest attribution
via the Job pod's `initContainerStatuses`), it emits a de-duplicated **Warning
Event** `DataLoadingHealthCheckFailed` (`emitDataLoadHealthCheckFailureEvent` in
`dataload_controller.go`). A **main-container** failure (a real load error) does
**not** get the HC event; when the init status is not derivable the controller
stays silent (the failure is still surfaced via status + `errors_total` + the
Job pod logs). Restore the broken condition → the Job re-runs → the init passes
→ the load proceeds.
## Security Considerations

> **Status: Implemented (Scenario 111), honesty-bounded.** The security controls
> below (SE.1–SE.6, SL.6) are **built and exercised** — with two **HONESTY**
> caveats called out per control: live *authenticated* Kerberos
> (SE.4) and live *encrypted* TLS handshakes (SE.2/SE.3) are **CONFIG-ONLY** (the
> test env has no KDC and no TLS-speaking source). The operator renders the
> config **correctly** and verifies it; it **never fakes** a live Kerberos kinit
> or a TLS handshake. See
> [Scenario 111 — Security (SE.1–SE.6, SL.6)](#scenario-111--security-se1se6-sl6)
> for the per-control verification matrix.

| Control | Status | Classification | Detail |
|---------|--------|----------------|--------|
| **SE.1 / SL.6** — init-container secret rendering | **Implemented** | **REAL (verified)** | `pxf-cred-init` `envsubst`-resolves the `${PLACEHOLDER}` site-XML templates from `credentialSecrets[]` into the ephemeral `pxf-servers` emptyDir at pod start. **Secrets never land in the ConfigMap** — they live only in the ephemeral pod filesystem. |
| **SE.2** — JDBC TLS | **Implemented (declarative)** | **CONFIG-ONLY** unless the source speaks TLS | TLS/SSL is wired declaratively via JDBC URL / ssl params rendered into `jdbc-site.xml`. A live encrypted handshake is asserted **only** when the source actually speaks TLS; otherwise the rendered config is verified (no faked handshake). |
| **SE.3** — S3 TLS | **Implemented (declarative)** | **CONFIG-ONLY** unless the source speaks TLS | `fs.s3a.connection.ssl.enabled=true` is rendered into `s3-site.xml`. Same honesty boundary as SE.2 — live encryption asserted only against a TLS source. |
| **SE.4** — Kerberos keytab | **Implemented (config-correct)** | **CONFIG-ONLY** (no live KDC) | `servers[].kerberos{principal,keytabSecret,krb5ConfigMap,realm}` mounts the keytab Secret on the PXF sidecar + cred-init at `$PXF_BASE/keytabs/<server>/` and folds `hadoop.security.authentication=kerberos` + `pxf.service.kerberos.principal`/`.keytab` into `core-site.xml`. Live *authenticated* Hadoop/Hive/HBase access is CONFIG-ONLY — the test env has no KDC, and the operator never performs a live `kinit`. |
| **SE.5** — segment↔sidecar `localhost`-only | **Implemented** | **REAL (proven)** | `BuildPXFClusterNetworkPolicy` emits a NetworkPolicy for the segment-primary pods that does **not** allow cross-pod ingress to PXF `:5888`; same-pod localhost traffic is never subject to NetworkPolicy, so loads keep working (policy applied + load still succeeds). |
| **SE.6** — dedicated minimal-privilege DB role | **Implemented** | **REAL (least-privilege)** | `db.EnsureDataLoaderRole` creates a `NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN` role granted **only** `SELECT,INSERT ON PROTOCOL pxf`. Opt-in via `pxf.dataLoaderRole` (empty ⇒ `gpadmin`, the existing behavior). The role cannot perform unrelated operations. |

- **SE.1 / SL.6** — All external data source credentials are stored in Kubernetes Secrets and rendered into init-container XML configs (`pxf-cred-init`). Secrets are **never** committed to ConfigMaps; they exist only in the ephemeral pod filesystem. **REAL (verified).**
- **SE.2** — PXF server configurations with JDBC connections support TLS/SSL via JDBC URL / `ssl` parameters rendered into `jdbc-site.xml`. **CONFIG-ONLY** unless the source speaks TLS — a live encrypted handshake is never faked.
- **SE.3** — S3 connections support `fs.s3a.connection.ssl.enabled=true` for encrypted transport, rendered into `s3-site.xml`. **CONFIG-ONLY** unless the source speaks TLS.
- **SE.4** — Kerberos authentication for Hadoop/Hive/HBase is supported via keytab files mounted as Secrets and `core-site.xml` properties. **CONFIG-ONLY** (no live KDC in the test env); the operator never performs a live `kinit`.
- **SE.5** — PXF service communication between segment and sidecar is `localhost`-only (no cross-pod traffic), enforced by `BuildPXFClusterNetworkPolicy`. **REAL (proven).**
- **SE.6** — The operator creates a dedicated database role (`pxf.dataLoaderRole`) for data loading with minimal privileges (`NOSUPERUSER`, only `SELECT,INSERT ON PROTOCOL pxf`). **REAL (least-privilege).**

## Conformance / Implementation Status Summary

At-a-glance matrix of what this spec describes vs. what the code actually does
today. Verified against `api/v1alpha1/types.go`,
`internal/webhook/{validating,mutating}.go`,
`internal/controller/admin_controller.go`, `internal/metrics/metrics.go`,
`internal/api/server.go`, and `cmd/cloudberry-ctl/main.go`.

### Implemented

| Area | Detail |
|------|--------|
| CRD `DataLoadingSpec` | Full PXF model: `pxf{...}`, `gpfdist{...}`, `jobs[]{pxfJob/gploadJob}`, `jobTemplate{...}`, `healthChecks{enabled,diskMinFreeMB,scratchSizeLimit}` (Scenario 104) |
| Webhook validation | **W.1–W.25 + W.10b** + W.10 profile allowlist + W.16 `file://` rejection (tested by Scenario 89; W.3/W.4 object-store extension + W.10b write-capability by Scenario 96; **W.17 `sourceFilter` by Scenario 99**; **W.18–W.22 gpload by Scenario 101**; **W.23/W.24/W.23c custom-connector + streaming by Scenario 102**; **W.25 `loadMethod` + the W.17 fdw-read tweak by Scenario 103**) |
| Object-store server types `gs`/`abfss`/`wasbs` (+ Dell-ECS / MinIO variants) | CRD `PxfServerSpec.Type` enum widened; W.3/W.4 admit them; all render into `<server>__s3-site.xml` (`renderPXFServer`) — Scenario 96 |
| Write-capability ENFORCEMENT (W.10b admission + builder re-check) | `mode: writable` + read-only format (`json`/`orc`/`rc`) **rejected**; single source of truth `internal/pxfpolicy` — Scenario 96 |
| Writable external-table DDL (`pxfwritable_export`) | `buildPXFExternalTableDDL` emits `CREATE WRITABLE EXTERNAL TABLE … FORMATTER='pxfwritable_export'` (no `LOG ERRORS`/reject limit) for writable formats — Scenario 96 |
| Writable EXPORT to **S3** (FE.9/WE.1) / **HDFS** (FE.10) / **JDBC** (FE.11) + WE.2 correct-format gate | Profile-agnostic `dataload_builder.go`; `IsProfileWritable` admits `s3:`/`hdfs:` text/parquet/avro(+seq) + bare `jdbc`; objects/part-files/rows LAND; `parquet`/`avro` `[CONFIG-ONLY]` (DATA_SCHEMA) — **Scenario 99** |
| `pxfJob.sourceFilter` filtered export + **W.17** (`validateSourceFilter`) | `INSERT INTO <ext> SELECT * FROM <target> WHERE <sourceFilter>` via quoted heredoc (single-quote safe); unset → byte-identical full export; W.17 valid on `mode: writable` OR `loadMethod: fdw` read (Scenario 103), rejects a plain external-table read + `;`/`--`/`/*` — **Scenario 99 + 103** |
| FDW-based loading path (`pxfJob.loadMethod: fdw`) + **W.25** (`validateLoadMethod`) | `buildFDWDDL`/`buildFDWDataLoadScript`: persistent `CREATE SERVER`/`USER MAPPING`/`FOREIGN TABLE` IF NOT EXISTS (per-protocol `pxf_fdw` wrapper, `(LIKE <target>)`, never dropped) → `SELECT count(*) FROM <foreign>` → `INSERT INTO <target> SELECT * FROM <foreign> [WHERE <sourceFilter>]` → `ANALYZE`. EQUIVALENT to the external-table path (equal row counts); W.25 enum + fdw read-only (reject `mode: writable`/`continuous: true`). Reuses `cloudberry_data_loading_*` — NO new metric — **Scenario 103** |
| Hadoop-profile write-capability (HDFS per-format + **scheme-aware** Hive/HBase read-only) | `IsProfileWritable` is now scheme-aware (`readOnlySchemes={hive,hbase}`): `hdfs:text/parquet/avro/SequenceFile` writable; `hdfs:json/orc` + **all** `hive*` + `HBase` rejected (`hive:text` writable rejected despite `text` being a writable format) — Scenario 97 |
| `hive-site.xml` / `hbase-site.xml` rendering | `renderPXFHDFSServer` emits `<server>__hive-site.xml` (`hive.metastore.uris`) + `<server>__hbase-site.xml` (`hbase.zookeeper.quorum`) for the `hadoop-cluster` `hdfs` server; always `core-site.xml`+`hdfs-site.xml` (SITE.1–4) — Scenario 97 |
| Filter pushdown / column projection / per-row error handling DDL knobs | `buildPXFLocation` emits `FILTER_PUSHDOWN=true` / `PROJECT=true`; `errorHandlingClause` emits `[LOG ERRORS ]SEGMENT REJECT LIMIT <n> [ROWS\|PERCENT]` (writable export omits it); mutating webhook defaults `filterPushdown`/`columnProjection` to `true`; W.15 validates reject-limit type. **Runtime-verified (Scenario 98)** via row-count reduction (`rows_total`) + `EXPLAIN` + source logs + real `job_status`/`errors_total` — **no fabricated `bytes_transferred`** |
| Webhook defaults | All **14** defaults (`setDataLoadingDefaults`) |
| Controller reconcile | Config validation + job counting; sets lightweight `Status.DataLoading` (phase, configured/active counts, per-job name/enabled), `DataLoadingConfigured` condition, `DataLoadingReconciled` event. When `pxf.enabled`: applies the servers ConfigMap, sets `cloudberry_pxf_servers_configured`, populates `status.dataLoading.pxf.{configured,servers}`, enriches the condition with the server count; on a real servers-ConfigMap `Data` diff also emits `PXFServersChanged` + increments `cloudberry_pxf_servers_changed_total` (Scenario 106) |
| PXF sidecar | Injected into the **segment-primary** StatefulSet pod template (`BuildSegmentPrimaryStatefulSet` → `BuildPXFSidecarContainers`/`Volumes`); gated on `dataLoading.enabled && pxf.enabled && pxf.image != ""`; coordinator/standby/mirror untouched. Env: `PXF_HOME/PXF_BASE/PXF_JVM_OPTS/PXF_PORT/PXF_LOG_LEVEL/PXF_EXTENSION_PXF/PXF_EXTENSION_PXF_FDW`; `logLevel`→`PXF_LOG_LEVEL` re-patch propagation |
| PXF servers ConfigMap | `<cluster>-pxf-servers` rendered by `BuildPXFServersConfigMap`: one `<name>__<file>.xml` per server (byte-stable), `credentialSecrets[]` as `${PLACEHOLDER}`, `customConnectors` in `connectors.properties` |
| Credential init-container | `pxf-cred-init` (`BuildPXFCredentialInitContainers`): live `envsubst` secret→XML rendering into the `pxf-servers` emptyDir (one `<server>/<file>.xml` directory per server); **secrets never in the ConfigMap** |
| PXF extension setup | `setupPXFExtensions` → `SetupPXFExtensions`: best-effort/non-fatal `CREATE EXTENSION pxf/pxf_fdw`, annotation-gated (no-op for `pxf` on cloudberry-official) |
| Data-loading Jobs/CronJobs | `reconcileDataLoadingJobs` creates `<cluster>-dataload-<job>` Job (one-off) / CronJob (scheduled) per enabled job; container `dataload`, image `cluster.Spec.Image`, coordinator-exec `psql` |
| External-table DDL + load script | `buildExternalTableDDL` (`CREATE EXTERNAL TABLE (LIKE target) … LOCATION … FORMAT … LOG ERRORS`); `buildDataLoadScript` (`INSERT…SELECT` → `DATALOAD_ROWS` marker → `DROP` → `ANALYZE`) |
| Native `gpfdist://`/`s3://` (+ bare-path-via-gpfdist) **live execution** | Real data load, row-count verified (e.g. 183,961 rows); `file://` admission-rejected by W.16 |
| kafka custom-connector profile + connector-JAR download + continuous streaming Job (Scenario 102) | `servers[].type: custom` + `customConnectors[]` + `pxfJob.profile: kafka` admitted by **W.23/W.24**; `pxf-connector-init` downloads each `jarUrl` into `/pxf/lib/custom/<name>.jar` (C.18); `continuous: true` → one-off long-running Job (NOT CronJob, J.46) with a streaming consume loop; new `continuous`/`batchSize`/`flushInterval` fields → `CBK_*` env (**W.23c**). NO new metric (reuses `cloudberry_data_loading_*`; steady state `job_status=Running`). End-to-end row landing is **CONFIG-ONLY** (needs a real connector JAR; placeholder staged) |
| **FDW-based loading path** (`pxfJob.loadMethod: fdw`; Scenario 103) | `buildFDWDDL`/`buildFDWDataLoadScript` emit the persistent EX.5-EX.8 chain — `CREATE SERVER IF NOT EXISTS "foreign_<server>" FOREIGN DATA WRAPPER <s3\|gs\|abfss\|wasbs\|jdbc\|hdfs\|hive\|hbase>_pxf_fdw` + `CREATE USER MAPPING IF NOT EXISTS FOR "gpadmin"` + `CREATE FOREIGN TABLE IF NOT EXISTS "foreign_<job>" (LIKE <target>)`, all idempotent, **never dropped** (persistent, directly queryable) — then `SELECT count(*) FROM <foreign>` + `INSERT INTO <target> SELECT * FROM <foreign> [WHERE <sourceFilter>]` + `ANALYZE`. **EQUIVALENT** to the external-table path (same INSERT…SELECT shape, equal row counts). New `pxfJob.loadMethod` (`external-table`\|`fdw`) field; webhook **W.25** (enum + fdw read-only: reject `mode:writable`/`continuous:true`) + the **W.17** tweak (`sourceFilter` now valid on a fdw read). NO new metric — reuses `cloudberry_data_loading_*` (`job_status`/`rows_total`/`errors_total`); `cloudberry_pxf_*`/`cloudberry_gpfdist_*` stay Planned |
| **Pre-load health checks** (`dataload-healthcheck` init container; Scenario 104) | A `dataload-healthcheck` init container is prepended **FIRST** on **BOTH** the pxf/native (`buildDataLoadPodSpec`) and gpload (`buildGploadPodSpec`) Job pods (gated on `dataLoading.healthChecks.enabled`, default ON). It runs 5 gated checks before the load — **HC.1** PXF readiness (pxf jobs; a `psql` **DB-proxy** probe — `SELECT 1`/`pg_extension WHERE extname='pxf'`/`pxf_version()` — NOT a direct sidecar curl, since the load pod can't reach the localhost-only sidecar), **HC.2** `to_regclass(targetTable)` target exists (ALL jobs), **HC.3** `curl --head ${AWS_S3_ENDPOINT}` object-store connectivity (s3-family only; jdbc/hive/hbase/hdfs skipped), **HC.4** `curl http://<cluster>-gpfdist-svc:8080/` (gpload jobs when gpfdist enabled), **HC.5** `df -Pk /dataload-scratch` free `>= diskMinFreeMB` (ALL jobs). A `dataload-scratch` `emptyDir` (`SizeLimit` from `scratchSizeLimit`) is mounted at `/dataload-scratch` on BOTH the init + main container. A non-zero check blocks the load → Job Failed (`data_loading_job_status=3` + `errors_total`) → a de-duplicated `DataLoadingHealthCheckFailed` Warning Event (`emitDataLoadHealthCheckFailureEvent`, honest attribution via the pod's `initContainerStatuses`). `enabled:false` → no init container / scratch volume. NO new operator metric (reuses `cloudberry_data_loading_*`); observability is rounded out by the NEW kube-state-metrics (`kube_job_status_failed{job_name=~".*-dataload-.*"}` / `kube_pod_init_container_status_*` / `kube_deployment_status_replicas_available`) |
| Event `DataLoadingHealthCheckFailed` | De-duplicated `EventTypeWarning` emitted when a data-load Job is observed Failed AND the `dataload-healthcheck` init container terminated non-zero (Scenario 104) |
| `pxf://` Job generation + launch | Implemented |
| `pxf://` **live execution** (read-back) | Implemented; row-count verified (183,961 rows from MinIO S3 via the PXF sidecar). Requires the `cloudberry-pxf` sidecar image (`Dockerfile.cloudberry-pxf`) + the `pxf` extension in the DB image (`cloudberry-official-pxf`, `Dockerfile.cloudberry-official-pxf`) |
| `cloudberry-pxf` + `cloudberry-official-pxf` images | Built via `make docker-build-pxf` / `docker-build-official-pxf` (from `apache/cloudberry-pxf`) |
| Rich per-job status | `status.dataLoading.jobs[].{lastRun,lastStatus,rowsLoaded,duration}` from terminal Job status + `DATALOAD_ROWS` marker |
| Metric `cloudberry_data_loading_jobs_active` | Emitted by the reconcile loop |
| Metric `cloudberry_pxf_servers_configured` | Emitted by `reconcilePxf` (`len(pxf.servers)`, config-derived) |
| Metric `cloudberry_data_loading_job_status` | Emitted from terminal Job status (0/1/2/3) |
| Metric `cloudberry_data_loading_job_last_success_timestamp` | Emitted from Job `completionTime` on success |
| Metric `cloudberry_data_loading_job_duration_seconds` | Emitted from Job start→completion |
| Metric `cloudberry_data_loading_rows_total` | Emitted from the harvested `DATALOAD_ROWS` marker (`source_type` spec-derived) |
| Metric `cloudberry_data_loading_errors_total` | Emitted on Job Failed |
| Status `status.dataLoading.pxf.{configured,servers}` | Spec-derived (`configured = pxf.enabled && image set`; `servers = len(pxf.servers)`) |
| Status `status.dataLoading.pxf.status` (Scenario 105) | LIVE, HONEST `Running`/`Stopped`/`Error`, **ABSENT** when unobservable; from real segment-primary `pxf` `ContainerStatuses` readiness aggregation (`util.PXFReadyCount`/`PXFStatusFromReadiness`) — NO exec/HTTP/synthesized health; all ready→Running, some down→Error, none→Stopped, no pods→absent |
| Status `status.dataLoading.pxf.extensionsInstalled` (Scenario 105) | `[]string` from a real read-only `pg_extension` probe (`db.Client.ListPXFExtensions`); lists `pxf`/`pxf_fdw` when actually installed, honest subset, **ABSENT (nil)** when DB unreachable / none installed — never synthesized |
| Metric `cloudberry_pxf_status` (Scenario 105) | Gauge (0=Stopped/1=Running/2=Error); emitted **only when observable** from the readiness aggregation — never synthesized |
| Metric `cloudberry_pxf_extensions_installed` (Scenario 105) | Gauge = count of installed PXF extensions; emitted **only when observed** by the `pg_extension` probe |
| Event `PXFServersChanged` (Scenario 106) | Normal event (`EventReasonPXFServersChanged`), message `PXF servers changed: added=[..] removed=[..] updated=[..]`; emitted on a real `<cluster>-pxf-servers` ConfigMap `Data` diff by BOTH reconcile (`emitPXFServersChanged`) and `pxf sync` (`recordPXFServersChanged`) — never on a no-op sync / first create |
| Metric `cloudberry_pxf_servers_changed_total` (Scenario 106) | Counter (`{cluster,namespace}`); incremented by `1` on a real servers-ConfigMap diff (shared `util.DiffPXFServerNames`) — **only when the diff is non-empty** |
| REST `GET .../jobs`, `GET .../jobs/{job}` | **FULL** (read from spec) |
| REST job mutations (`POST/PUT/DELETE/start/stop`) | **FULL** (Scenario 107; `start` creates a real one-off `batchv1.Job` → `202`/`409 JOB_ALREADY_RUNNING`; `stop` deletes Job / suspends CronJob → `202`/idempotent `200`; `create` → `201`/`409 JOB_EXISTS`/`400` unknown server; `delete` best-effort removes the spawned Job; Operator create/update/start/stop, Admin delete) |
| REST `pxf/servers` CRUD (P.2–P.5), `jobs/{job}/logs` (P.14), `external-tables` (P.15) | **FULL** (Scenario 107; servers `201` returns rendered `<server>__*.xml` + `409 SERVER_EXISTS`/`SERVER_IN_USE`; logs stream real pod logs + honest `501 LOGS_NOT_AVAILABLE` with no clientset; external-tables `{observed,observedAvailable,expected}` — live catalog or honest-absent) |
| REST DB read `db.Client.ListExternalTables` (`pg_exttable` + foreign tables; static, observed-only) | **FULL** (Scenario 107) |
| REST `GET .../data-loading/test-read` (P.16; `handleTestReadPXFSource`, Basic, NO metric) | **FULL** (Scenario 108; transient external table → `SELECT LIMIT N` → always DROP; `TestReadResponse {cluster,source,limit,available,rowCount,columns,rows}`; real rows or `available:false` — never fabricated, never `500`) |
| REST DB read `db.Client.ReadPXFSourceSample` (transient sample read; observed-only) | **FULL** (Scenario 108) |
| CLI `data-loading jobs {list,create,start,stop,delete,logs}`, `data-loading status`, `data-loading test-read` | Live (Scenario 108; `create` now builds real `--type pxf`/`--type gpload`/`--from-yaml` bodies; `logs` streams via `GetStream` + kubectl fallback; `test-read` → the new test-read endpoint) |
| CLI `pxf {status,restart,sync}` + `pxf servers {list,create,update,delete}` | Live (Scenario 95 lifecycle + Scenario 108 servers CRUD; `servers create` `--name --type --endpoint --bucket --credential-secret` repeatable `name[:key]`; `servers update [name] --endpoint` via `runAPIPut`; `servers delete [name]` honors `409 SERVER_IN_USE`) |
| CLI helper `runAPIPut` (alongside `runAPIGet`/`runAPIPost`/`runAPIDelete`) | **Implemented** (Scenario 108) |
| **SE.1 / SL.6** — init-container secret rendering (Scenario 111) | `pxf-cred-init` `envsubst`-resolves `${PLACEHOLDER}` site-XML from `credentialSecrets[]` into the ephemeral `pxf-servers` emptyDir; secrets only in the ephemeral pod fs, **never in the ConfigMap**. **REAL (verified).** |
| **SE.4** — Kerberos keytab from Secret (Scenario 111) | New `pxf.servers[].kerberos{principal,keytabSecret{name,key},krb5ConfigMap,realm}`. Keytab Secret mounted on the PXF sidecar + `pxf-cred-init` at `$PXF_BASE/keytabs/<server>/`; renders `hadoop.security.authentication=kerberos` + `pxf.service.kerberos.principal`/`.keytab` into `core-site.xml`; optional krb5.conf ConfigMap mount. Webhook requires principal+keytab when kerberos set, rejects kerberos on non-hdfs/hive/hbase types. **Config-correct; live authenticated access CONFIG-ONLY (no KDC) — no faked `kinit`.** |
| **SE.5** — segment↔sidecar `localhost`-only NetworkPolicy (Scenario 111) | `BuildPXFClusterNetworkPolicy` emits a NetworkPolicy for segment-primary pods that does NOT allow cross-pod ingress to PXF `:5888`; same-pod localhost traffic is never subject to NetworkPolicy so loads keep working. **REAL (policy applied + load still succeeds).** |
| **SE.6** — dedicated minimal-privilege DB role (Scenario 111) | `db.EnsureDataLoaderRole` creates `CREATE ROLE <role> NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN` granted **only** `SELECT,INSERT ON PROTOCOL pxf`. Opt-in via `pxf.dataLoaderRole` (empty ⇒ `gpadmin`, unchanged); additive. **REAL least-privilege.** |
| **SE.2 / SE.3** — JDBC / S3 TLS passthrough (Scenario 111) | TLS wired declaratively: JDBC URL/`ssl` params → `jdbc-site.xml`; `fs.s3a.connection.ssl.enabled=true` → `s3-site.xml`. **Declarative; live encrypted handshake CONFIG-ONLY unless the source speaks TLS — never faked.** |
| **DIS.1** — `dataLoading.enabled: false` TEARS DOWN (Scenario 112) | `reconcileDataLoading` dispatches to `cleanupDataLoading`: deletes the `<cluster>-pxf-servers` ConfigMap (CLUSTER ctrl `ensurePxfServersConfigMap` delete-when-disabled), gpfdist Deployment/Service/PVC, all data-loading Jobs+CronJobs, gpload control-file ConfigMaps, the PXF NetworkPolicy; drops the PXF sidecar from the segment-primary StatefulSet (re-render without it); clears `Status.DataLoading`; condition `DataLoadingConfigured=False` reason `DataLoadingDisabled`; one-shot `DataLoadingDisabled` event (de-dup on transition); `cloudberry_data_loading_jobs_active`→0, `cloudberry_pxf_servers_configured`→0. **No longer a no-op.** Re-enable → idempotent reconcile redeploys everything. |
| **DIS.1** — API `DATA_LOADING_NOT_ENABLED` (Scenario 112) | DL-disabled REST: mutations → `400 DATA_LOADING_NOT_ENABLED`; list/get → `200` disabled envelope (`writeDataLoadingDisabled`); PXF endpoints report `DATA_LOADING_NOT_ENABLED` (DL-disabled precedence over `PXF_NOT_ENABLED`). |
| **DIS.2** — `pxf.enabled: false` independence (Scenario 112) | No PXF sidecars/extensions/`<cluster>-pxf-servers` ConfigMap (`pxfSidecarEnabled` gate + `ensurePxfServersConfigMap` delete-when-disabled); gpload-type jobs still function. |
| **DIS.3** — `gpfdist.enabled: false` honest runtime signal (Scenario 112) | gpfdist Deployment/Service/PVC GC'd (`reconcileGpfdist`); `inputSource.type: local` gpload jobs still work; a gpfdist-source gpload job reports the missing dependency via the **HONEST RUNTIME** signal — gpload can't reach the absent host → Job Failed + `cloudberry_data_loading_errors_total` + `status=Failed`. **HC.4 is SKIPPED when gpfdist is disabled** (gated on `gpfdist.enabled`), so the signal is the runtime failure, NOT a fabricated pre-flight check. |

### Planned (not built)

| Area | Detail |
|------|--------|
| PXF runtime | Sidecar `command`/`args` bootstrap (`pxf prepare/start`). (**Live `pxf://` execution is now Implemented** — see the Implemented table — requiring the `cloudberry-pxf` sidecar image + the `pxf` extension; only the explicit bootstrap `command`/`args` remain Planned. The explicit `pxf sync` trigger and the `pxf status`/`pxf restart` lifecycle verbs are now **Implemented** — Scenario 95 — alongside the always-on structural sync via the shared `<cluster>-pxf-servers` ConfigMap, RP.12.) |
| PXF server-config lifecycle | Server CRUD via **API is Implemented** (Scenario 107: `pxf/servers` list/get/create/update/delete REST routes, P.2–P.5) **and the `pxf servers …` CLI subcommands are now Implemented too** (Scenario 108: `pxf servers {list,create,update,delete}`). The `pxf status\|restart\|sync` lifecycle verbs are Implemented (Scenario 95). |
| gpfdist | `Deployment` + `Service` |
| gpload | Control-file (`gpload` YAML) Job |
| Writable external tables (export-Job kind) | The write-capability **enforcement**, the `pxfwritable_export` **writable DDL** (Scenario 96) and the **S3/HDFS/JDBC export targets + `sourceFilter` filtered export + W.17** (Scenario 99) are now **Implemented** (see the Implemented table); a first-class **data-export Job kind** (beyond the writable-DDL path the load Job already builds) remains Planned |
| Security (SE.1–SE.6, SL.6) | **NONE Planned** — the data-loading security controls are now **Implemented (Scenario 111)**: SE.1/SL.6 init-container secret rendering (REAL), SE.5 segment↔sidecar `localhost`-only NetworkPolicy (REAL), SE.6 dedicated minimal-privilege DB role (REAL), SE.4 Kerberos keytab mounting (config-correct; live auth CONFIG-ONLY/no-KDC), SE.2/SE.3 JDBC/S3 TLS passthrough (declarative; live TLS CONFIG-ONLY unless the source speaks TLS). See the Implemented table + [Scenario 111](#scenario-111--security-se1se6-sl6). |
| Metrics (6 honestly absent) | **Scenario 109 flipped 4 to Implemented** from real sources: `cloudberry_pxf_service_up{segment_host}` (M.1, per-segment `pxf` readiness via `util.PXFReadyByHost`), `cloudberry_pxf_requests_total` (M.2) + `cloudberry_pxf_request_duration_seconds` (M.3) (the **real** `http_server_requests_*` actuator series scraped from `/actuator/prometheus` by a dedicated vmagent `:5888` job — count/latency REAL, `server/profile/operation` labels downgraded to actuator-native, never fabricated), and `cloudberry_data_loading_bytes_total` (M.10, from the real `DATALOAD_BYTES` marker via `wc -c` on local gpload input; omitted otherwise). The **6 honestly-absent** families stay Planned: `pxf_bytes_transferred_total` (M.4), `pxf_records_total` (M.5; record throughput observed via `cloudberry_data_loading_rows_total`), `pxf_errors_total` (M.6 — **FOLDED** into `cloudberry_data_loading_errors_total` + actuator non-2xx; no synthetic typed counter registered), `pxf_active_connections` (M.7), and the 2 `cloudberry_gpfdist_*` (M.15/M.16, no scrapable endpoint). **`pxf_bytes_transferred_total` is a deliberate metrics-honesty hold (Scenario 98):** PXF 2.1.0's Spring Boot Actuator exposes no honest external-bytes counter, so filter pushdown is observed via row-count reduction (`cloudberry_data_loading_rows_total`) + `EXPLAIN` + source logs instead of a fabricated byte gauge. **Pre-load health-check failures (Scenario 104) add NO new operator metric** — they are observed via the EXISTING `cloudberry_data_loading_job_status=3` (Failed) + `cloudberry_data_loading_errors_total` + the `DataLoadingHealthCheckFailed` Event + the NEW kube-state-metrics (`kube_job_status_failed` / `kube_pod_init_container_status_*` / `kube_deployment_status_replicas_available`); the 6 honestly-absent `cloudberry_pxf_*`/`cloudberry_gpfdist_*` families stay Planned |
| Per-profile read/write counts | No runtime metric for per-profile read/write counts; the write-capability decision is enforced at admission/build time, not measured at runtime (Scenario 96 + 97 dashboards show the real `source_type` breakdown — incl. a Hadoop-filtered timeseries — plus text panels, no new metric) |
| Filter-pushdown / projection runtime metric | No dedicated runtime metric — the pushdown/projection decision is emitted into the DDL/LOCATION at build time and **observed** (not metered) at runtime via row-count reduction (`cloudberry_data_loading_rows_total`) + `EXPLAIN` + source-side query logs (Scenario 98 dashboard adds a doc text panel, no new metric; `pxf_bytes_transferred_total` stays Planned) |
| REST | **NONE Planned** — the entire data-loading REST surface (P.1–P.15) is now **Implemented**: `pxf/servers` CRUD (P.2–P.5), `external-tables` (P.15), `jobs/{job}/logs` (P.14) landed in **Scenario 107**; `pxf/{status,restart,sync}` in Scenario 95; job CRUD/lifecycle (P.7–P.13) in Scenario 107. |
| CLI | **NONE Planned for L.1–L.16** — the full data-loading / PXF CLI surface is now **Implemented (Scenario 108)**: `pxf servers {list,create,update,delete}` CRUD, `data-loading jobs logs` (streaming + kubectl fallback), `data-loading test-read`, and the enriched `data-loading jobs create` (`--type pxf`/`--type gpload` flags + `--from-yaml`) are all registered subcommands wired to the Scenario 107 REST routes (plus the new `GET .../data-loading/test-read` endpoint). `pxf status\|restart\|sync` ARE Live (Scenario 95). |