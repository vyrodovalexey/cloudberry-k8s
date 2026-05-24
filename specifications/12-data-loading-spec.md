# Specification 12: Data Loading

## Overview

This specification defines the data loading capabilities for Cloudberry Database clusters managed by the operator. It leverages the [Apache Cloudberry PXF](https://github.com/apache/cloudberry-pxf) (Platform Extension Framework) as the primary data access layer for external data sources, supplemented by Cloudberry-native bulk loading utilities (`gpfdist`, `gpload`) and operator-managed Kubernetes Jobs for scheduled and on-demand ingestion workflows.

PXF is a Java-based service that runs on every segment host and provides a `pxf://` protocol for creating external tables and foreign data wrappers (FDW) against heterogeneous external data stores. The operator manages the full PXF lifecycle: deployment, server configuration, credential injection, health monitoring, and extension setup within the database.

## Architecture

### PXF Components

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

### Supported File Formats

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

| Action | Kubernetes Resource |
|--------|---------------------|
| PXF Server process | Sidecar container on each segment pod |
| PXF extension setup | Init container or operator reconcile (SQL DDL) |
| Scheduled data load (INSERT INTO ... SELECT FROM pxf) | `CronJob` → spawns `Job` |
| On-demand data load | `Job` (created by operator via API/CLI) |
| gpfdist-based parallel load | `Deployment` (gpfdist server) + `Job` (gpload) |
| Bulk load via gpload | `Job` |

## CRD Specification

### DataLoadingSpec

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
      - name: s3-datalake
        type: s3
        config:
          fs.s3a.access.key:
            secretKeyRef:
              name: s3-credentials
              key: access_key
          fs.s3a.secret.key:
            secretKeyRef:
              name: s3-credentials
              key: secret_key
          fs.s3a.endpoint: "s3.amazonaws.com"
          fs.s3a.path.style.access: "true"   # Required for MinIO
      - name: minio-warehouse
        type: s3
        config:
          fs.s3a.access.key:
            secretKeyRef:
              name: minio-credentials
              key: access_key
          fs.s3a.secret.key:
            secretKeyRef:
              name: minio-credentials
              key: secret_key
          fs.s3a.endpoint: "http://minio:9000"
          fs.s3a.path.style.access: "true"
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
          jdbc.user:
            secretKeyRef:
              name: mysql-credentials
              key: username
          jdbc.password:
            secretKeyRef:
              name: mysql-credentials
              key: password
        driverJar: "/pxf/lib/mysql-connector-java-8.0.33.jar"
      - name: postgres-source
        type: jdbc
        config:
          jdbc.driver: "org.postgresql.Driver"
          jdbc.url: "jdbc:postgresql://pghost:5432/sourcedb"
          jdbc.user:
            secretKeyRef:
              name: pg-credentials
              key: username
          jdbc.password:
            secretKeyRef:
              name: pg-credentials
              key: password
    customConnectors:                      # Additional JARs for custom PXF plugins
      - name: custom-connector
        jarUrl: "s3://artifacts/pxf-plugins/my-connector.jar"
 
  gpfdist:
    enabled: false
    replicas: 2
    image: "cloudberry-gpfdist:2.1.0"
    port: 8080
    dataDirectory: /data
    persistentVolumeClaim: gpfdist-data-pvc
    resources:
      requests:
        cpu: "250m"
        memory: "256Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
 
  jobs:
    - name: s3-parquet-ingest
      type: pxf
      enabled: true
      schedule: "0 */6 * * *"              # Every 6 hours
      pxfJob:
        server: s3-datalake
        profile: s3:parquet
        resource: "s3a://data-lake/events/year={{.Year}}/month={{.Month}}/"
        targetTable: public.events
        mode: insert                        # insert | insert-select | writable
        columns: "event_id, event_type, payload, created_at"
        filterPushdown: true
        columnProjection: true
        customOptions:
          MAP_BY_POSITION: "true"
        errorHandling:
          segmentRejectLimit: 100
          segmentRejectLimitType: rows      # rows | percent
          logErrors: true
        postActions:
          - analyze
          - notify
 
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
        partitioning:
          column: order_date
          range: "2024-01-01:2026-12-31"
          interval: "1:month"
        postActions:
          - analyze
          - sql: "CALL public.merge_orders_staging()"
 
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
 
    - name: gpload-csv
      type: gpload
      enabled: true
      schedule: "*/30 * * * *"
      gploadJob:
        inputSource:
          type: gpfdist                     # gpfdist | local
          servers:
            - host: gpfdist-svc
              port: 8080
          filePaths:
            - "/data/incoming/*.csv"
        format: csv
        delimiter: ","
        header: true
        encoding: "UTF-8"
        targetTable: public.raw_data
        mode: insert                        # insert | update | merge
        errorHandling:
          segmentRejectLimit: 50
          segmentRejectLimitType: rows
          logErrors: true
        preActions:
          - sql: "TRUNCATE public.raw_data_staging"
        postActions:
          - analyze
          - sql: "INSERT INTO public.raw_data SELECT * FROM public.raw_data_staging"
 
    - name: kafka-cdc
      type: pxf
      enabled: true
      pxfJob:
        server: kafka-connector             # Custom PXF connector
        profile: kafka
        resource: "cloudberry-cdc-topic"
        targetTable: public.cdc_events
        mode: insert
        continuous: true                    # Long-running streaming job
        batchSize: 10000
        flushInterval: "30s"
 
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

### DataLoadingStatus

Added to `CloudberryClusterStatus`:

```yaml
dataLoading:
  pxf:
    status: Running                        # Running | Stopped | Error
    servers: 4                             # Configured PXF server definitions
    extensionsInstalled:
      - pxf
      - pxf_fdw
  activeJobs: 3
  jobs:
    - name: s3-parquet-ingest
      lastRun: "2026-05-19T06:00:00Z"
      lastStatus: Success
      rowsLoaded: 1482937
      duration: "3m12s"
    - name: jdbc-sync
      lastRun: "2026-05-19T02:30:00Z"
      lastStatus: Success
      rowsLoaded: 52481
      duration: "1m45s"
```

## Operator Reconciliation Logic

### PXF Deployment

When `dataLoading.pxf.enabled: true`, the operator:

1. **Deploys PXF sidecar** on each segment pod. The PXF JVM process runs alongside the Cloudberry segment, listening on the configured port (default 5888):
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
        value: "INFO"
    ports:
      - name: pxf
        containerPort: 5888
    command: ["/bin/bash", "-c"]
    args:
      - |
        pxf prepare
        pxf start
        # Keep container alive, PXF runs as background service
        tail -f ${PXF_BASE}/logs/pxf-service.log
    livenessProbe:
      httpGet:
        path: /pxf/v15/Status
        port: 5888
      initialDelaySeconds: 30
      periodSeconds: 30
    readinessProbe:
      httpGet:
        path: /pxf/v15/Status
        port: 5888
      initialDelaySeconds: 15
      periodSeconds: 10
    volumeMounts:
      - name: pxf-base
        mountPath: /pxf-base
      - name: pxf-servers
        mountPath: /pxf-base/servers
      - name: pxf-lib
        mountPath: /pxf/lib/custom
```

2. **Generates PXF server configurations** as a ConfigMap (one directory per server, containing `*-site.xml` files). Secrets are resolved at pod startup via an init container that renders templates:
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
        <value>${S3_DATALAKE_ACCESS_KEY}</value>
      </property>
      <property>
        <name>fs.s3a.secret.key</name>
        <value>${S3_DATALAKE_SECRET_KEY}</value>
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
        <value>${MYSQL_OLTP_USER}</value>
      </property>
      <property>
        <name>jdbc.password</name>
        <value>${MYSQL_OLTP_PASSWORD}</value>
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

3. **Installs PXF extensions** in the target database via an operator-managed SQL migration step:
```sql
-- Install external table protocol handler
CREATE EXTENSION IF NOT EXISTS pxf;
 
-- Install FDW support
CREATE EXTENSION IF NOT EXISTS pxf_fdw;
 
-- Grant PXF protocol usage to application roles
GRANT SELECT ON PROTOCOL pxf TO data_loader_role;
GRANT INSERT ON PROTOCOL pxf TO data_loader_role;
```

4. **Syncs PXF configuration** across segment hosts. After updating server configs, the operator executes `pxf sync` (equivalent to copying `$PXF_BASE/servers` to all segment hosts) which is handled automatically because all PXF sidecars mount the same ConfigMap volume.
### Data Loading Jobs

For each job in `dataLoading.jobs`, the operator creates either a CronJob (if `schedule` is set) or a one-off Job (for on-demand triggers).

**PXF-type job** — runs SQL that reads from a PXF external table and inserts into the target:

```yaml
containers:
  - name: data-loader
    image: cloudberry-toolbox:2.1.0
    command: ["/bin/bash", "-c"]
    args:
      - |
        # Create temporary external table
        psql -h ${COORDINATOR_HOST} -p 5432 -d ${DATABASE} -c "
          CREATE EXTERNAL TABLE tmp_pxf_load_${JOB_NAME} (
            ${COLUMN_DEFINITIONS}
          )
          LOCATION ('pxf://${RESOURCE}?PROFILE=${PROFILE}&SERVER=${SERVER}${CUSTOM_OPTIONS}')
          FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
          ${ERROR_HANDLING};
        "
 
        # Load data
        psql -h ${COORDINATOR_HOST} -p 5432 -d ${DATABASE} -c "
          INSERT INTO ${TARGET_TABLE} (${COLUMNS})
          SELECT ${COLUMNS} FROM tmp_pxf_load_${JOB_NAME};
        "
 
        # Cleanup
        psql -h ${COORDINATOR_HOST} -p 5432 -d ${DATABASE} -c "
          DROP EXTERNAL TABLE IF EXISTS tmp_pxf_load_${JOB_NAME};
        "
 
        # Post-actions
        psql -h ${COORDINATOR_HOST} -p 5432 -d ${DATABASE} -c "ANALYZE ${TARGET_TABLE};"
```

**FDW-based alternative** — for persistent foreign tables that can be queried directly or used in `INSERT INTO ... SELECT FROM`:

```sql
-- Create FDW server
CREATE SERVER s3_datalake
  FOREIGN DATA WRAPPER s3_pxf_fdw
  OPTIONS (resource 's3a://data-lake/events/', format 'parquet');
 
-- Create user mapping
CREATE USER MAPPING FOR data_loader
  SERVER s3_datalake;
 
-- Create foreign table
CREATE FOREIGN TABLE foreign_events (
  event_id int,
  event_type text,
  payload jsonb,
  created_at timestamp
) SERVER s3_datalake
  OPTIONS (resource 's3a://data-lake/events/', format 'parquet');
```

### gpfdist Deployment

When `dataLoading.gpfdist.enabled: true`, the operator creates a `Deployment` running `gpfdist` instances for parallel file distribution:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: <cluster>-gpfdist
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: gpfdist
          image: cloudberry-gpfdist:2.1.0
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
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: gpfdist-data-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: <cluster>-gpfdist-svc
spec:
  selector:
    cloudberry.apache.org/component: gpfdist
  ports:
    - port: 8080
      targetPort: 8080
```

### gpload Jobs

For `gpload`-type jobs, the operator generates a `gpload` YAML control file and executes it in a Job:

```yaml
# Generated gpload control file
VERSION: 1.0.0.1
DATABASE: mydb
USER: gpadmin
HOST: coordinator-svc
PORT: 5432
GPLOAD:
  INPUT:
    - SOURCE:
        FILE:
          - gpfdist://gpfdist-svc:8080/incoming/*.csv
    - FORMAT: csv
    - DELIMITER: ','
    - HEADER: true
    - ENCODING: UTF-8
    - ERROR_LIMIT: 50
    - LOG_ERRORS: true
  OUTPUT:
    - TABLE: public.raw_data
    - MODE: INSERT
  PRELOAD:
    - TRUNCATE: true
  SQL:
    - AFTER: "ANALYZE public.raw_data"
```

## PXF Profile Reference

### Object Store Profiles

| Profile | Description | Read | Write |
|---------|-------------|------|-------|
| `s3:text` | Text/CSV on S3/MinIO/GCS/Azure | Yes | Yes |
| `s3:parquet` | Parquet on S3/MinIO/GCS/Azure | Yes | Yes |
| `s3:avro` | Avro on S3/MinIO/GCS/Azure | Yes | Yes |
| `s3:json` | JSON on S3/MinIO/GCS/Azure | Yes | No |
| `s3:orc` | ORC on S3/MinIO/GCS/Azure | Yes | No |
| `gs:*` | Google Cloud Storage (same format variants) | Yes | Yes |
| `abfss:*` | Azure Data Lake Gen2 | Yes | Yes |
| `wasbs:*` | Azure Blob Storage | Yes | Yes |

### Hadoop Profiles

| Profile | Description | Read | Write |
|---------|-------------|------|-------|
| `hdfs:text` | Text/CSV on HDFS | Yes | Yes |
| `hdfs:parquet` | Parquet on HDFS | Yes | Yes |
| `hdfs:avro` | Avro on HDFS | Yes | Yes |
| `hdfs:json` | JSON on HDFS | Yes | No |
| `hdfs:orc` | ORC on HDFS | Yes | No |
| `hdfs:SequenceFile` | SequenceFile on HDFS | Yes | Yes |
| `hive` | Hive tables (auto-detect format) | Yes | No |
| `hive:text` | Hive text tables | Yes | No |
| `hive:orc` | Hive ORC tables | Yes | No |
| `hive:rc` | Hive RCFile tables | Yes | No |
| `HBase` | HBase tables | Yes | No |

### JDBC Profile

| Profile | Description | Read | Write |
|---------|-------------|------|-------|
| `jdbc` | Any JDBC-accessible database | Yes | Yes |

JDBC connector supports partitioning for parallel reads:

```
LOCATION ('pxf://schema.table?PROFILE=jdbc&SERVER=mysql-oltp&PARTITION_BY=order_date:date&RANGE=2024-01-01:2026-12-31&INTERVAL=1:month')
```

### PXF Features

- **Filter pushdown** — WHERE clause predicates are pushed to the external data source to reduce data transfer. Supported by object store (Parquet, ORC column predicates), JDBC, and Hive connectors.
- **Column projection** — only requested columns are read from columnar formats (Parquet, ORC). Reduces I/O significantly.
- **JDBC partitioning** — splits a single-table JDBC query into parallel sub-queries based on a partition column (int, date, enum), distributing work across Cloudberry segments.
- **Writable external tables** — PXF supports writing data back to S3, HDFS, and JDBC targets using `CREATE WRITABLE EXTERNAL TABLE`.
- **Per-row error handling** — `LOG ERRORS SEGMENT REJECT LIMIT` allows tolerating malformed rows up to a threshold.
## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/clusters/{name}/data-loading/pxf/status` | PXF service health across segments |
| GET | `/clusters/{name}/data-loading/pxf/servers` | List configured PXF servers |
| POST | `/clusters/{name}/data-loading/pxf/servers` | Create a PXF server configuration |
| PUT | `/clusters/{name}/data-loading/pxf/servers/{server}` | Update a PXF server configuration |
| DELETE | `/clusters/{name}/data-loading/pxf/servers/{server}` | Delete a PXF server configuration |
| POST | `/clusters/{name}/data-loading/pxf/sync` | Sync PXF config across segments |
| GET | `/clusters/{name}/data-loading/jobs` | List all data loading jobs |
| POST | `/clusters/{name}/data-loading/jobs` | Create a new job |
| GET | `/clusters/{name}/data-loading/jobs/{job}` | Get job details |
| PUT | `/clusters/{name}/data-loading/jobs/{job}` | Update a job |
| DELETE | `/clusters/{name}/data-loading/jobs/{job}` | Delete a job |
| POST | `/clusters/{name}/data-loading/jobs/{job}/start` | Start/trigger a job |
| POST | `/clusters/{name}/data-loading/jobs/{job}/stop` | Stop a job |
| GET | `/clusters/{name}/data-loading/jobs/{job}/logs` | Get job execution logs |
| GET | `/clusters/{name}/data-loading/external-tables` | List PXF external/foreign tables |

## CLI Commands

```bash
# PXF management
cloudberry-ctl pxf status --cluster my-cluster
cloudberry-ctl pxf servers list --cluster my-cluster
cloudberry-ctl pxf servers create --cluster my-cluster --name s3-lake --type s3 \
  --endpoint http://minio:9000 --bucket data-lake --credential-secret s3-creds
cloudberry-ctl pxf servers update --cluster my-cluster --name s3-lake --endpoint http://minio:9001
cloudberry-ctl pxf servers delete --cluster my-cluster --name s3-lake
cloudberry-ctl pxf sync --cluster my-cluster
cloudberry-ctl pxf restart --cluster my-cluster
 
# Data loading jobs
cloudberry-ctl data-loading jobs list --cluster my-cluster
cloudberry-ctl data-loading jobs create --cluster my-cluster \
  --name s3-ingest --type pxf \
  --server s3-lake --profile s3:parquet \
  --resource "s3a://data-lake/events/" \
  --target-table public.events \
  --schedule "0 */6 * * *"
cloudberry-ctl data-loading jobs start --cluster my-cluster --job s3-ingest
cloudberry-ctl data-loading jobs stop --cluster my-cluster --job s3-ingest
cloudberry-ctl data-loading jobs delete --cluster my-cluster --job s3-ingest
cloudberry-ctl data-loading jobs logs --cluster my-cluster --job s3-ingest
 
# gpload jobs
cloudberry-ctl data-loading jobs create --cluster my-cluster \
  --name csv-load --type gpload \
  --gpfdist-host gpfdist-svc --gpfdist-port 8080 \
  --file-path "/data/incoming/*.csv" \
  --target-table public.raw_data --format csv
 
# Quick test: read from PXF external source
cloudberry-ctl data-loading test-read --cluster my-cluster \
  --server s3-lake --profile s3:parquet \
  --resource "s3a://data-lake/events/sample.parquet" --limit 10
 
# YAML upload (for complex job definitions)
cloudberry-ctl data-loading jobs create --cluster my-cluster --from-yaml job-config.yaml
```

## Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_pxf_service_up` | Gauge | cluster, namespace, segment_host | PXF service health per segment (0/1) |
| `cloudberry_pxf_requests_total` | Counter | cluster, namespace, server, profile, operation | Total PXF requests (read/write) |
| `cloudberry_pxf_request_duration_seconds` | Histogram | cluster, namespace, server, profile | PXF request duration |
| `cloudberry_pxf_bytes_transferred_total` | Counter | cluster, namespace, server, profile, direction | Bytes read from/written to external sources |
| `cloudberry_pxf_records_total` | Counter | cluster, namespace, server, profile, direction | Records read/written via PXF |
| `cloudberry_pxf_errors_total` | Counter | cluster, namespace, server, profile, error_type | PXF errors (connection, parse, timeout) |
| `cloudberry_pxf_active_connections` | Gauge | cluster, namespace, server | Active PXF connections to external sources |
| `cloudberry_data_loading_jobs_active` | Gauge | cluster, namespace | Active data loading jobs |
| `cloudberry_data_loading_rows_total` | Counter | cluster, namespace, job, source_type | Total rows loaded by job |
| `cloudberry_data_loading_bytes_total` | Counter | cluster, namespace, job, source_type | Total bytes loaded by job |
| `cloudberry_data_loading_errors_total` | Counter | cluster, namespace, job | Rejected/error rows per job |
| `cloudberry_data_loading_job_duration_seconds` | Histogram | cluster, namespace, job | Job execution duration |
| `cloudberry_data_loading_job_last_success_timestamp` | Gauge | cluster, namespace, job | Unix timestamp of last successful job run |
| `cloudberry_data_loading_job_status` | Gauge | cluster, namespace, job | Job status (0=idle, 1=running, 2=success, 3=failed) |
| `cloudberry_gpfdist_connections_active` | Gauge | cluster, namespace | Active gpfdist connections from segments |
| `cloudberry_gpfdist_bytes_served_total` | Counter | cluster, namespace | Total bytes served by gpfdist |

## Webhook Validation

- `dataLoading.pxf.image` is required when `pxf.enabled: true`
- `dataLoading.pxf.servers[].name` must be unique and non-empty
- `dataLoading.pxf.servers[].type` must be `s3`, `hdfs`, `jdbc`, `hbase`, or `hive`
- `dataLoading.pxf.servers[]` with `type: s3` must include `fs.s3a.endpoint` and credential references
- `dataLoading.pxf.servers[]` with `type: jdbc` must include `jdbc.driver` and `jdbc.url`
- `dataLoading.pxf.servers[]` with `type: hdfs` must include `fs.defaultFS`
- `dataLoading.jobs[].name` is required and must be unique
- `dataLoading.jobs[].type` must be `pxf` or `gpload`
- `dataLoading.jobs[].pxfJob.server` must reference a defined PXF server name
- `dataLoading.jobs[].pxfJob.profile` must be a valid PXF profile
- `dataLoading.jobs[].pxfJob.targetTable` is required
- `dataLoading.jobs[].gploadJob.targetTable` is required
- `dataLoading.jobs[].schedule` must be a valid cron expression when provided
- `dataLoading.jobs[].pxfJob.partitioning` requires `column`, `range`, and `interval` together
- `dataLoading.jobs[].pxfJob.errorHandling.segmentRejectLimitType` must be `rows` or `percent`
## Webhook Defaults

- `pxf.port`: `5888`
- `pxf.jvmOpts`: `"-Xmx1g -Xms256m"`
- `pxf.logLevel`: `INFO`
- `pxf.extensions.pxf`: `true`
- `pxf.extensions.pxfFdw`: `true`
- `gpfdist.replicas`: `1`
- `gpfdist.port`: `8080`
- `jobs[].pxfJob.mode`: `insert`
- `jobs[].pxfJob.filterPushdown`: `true`
- `jobs[].pxfJob.columnProjection`: `true`
- `jobs[].gploadJob.mode`: `insert`
- `jobTemplate.backoffLimit`: `3`
- `jobTemplate.activeDeadlineSeconds`: `14400`
- `jobTemplate.ttlSecondsAfterFinished`: `86400`
## PXF Server Configuration Lifecycle

### Creating a Server

The operator translates each entry in `dataLoading.pxf.servers[]` into a directory under `$PXF_BASE/servers/<server-name>/` containing the appropriate XML configuration files. The mapping is:

| Server Type | Generated Files |
|-------------|----------------|
| `s3` | `s3-site.xml` |
| `hdfs` | `core-site.xml`, `hdfs-site.xml`, optionally `hive-site.xml`, `hbase-site.xml`, `mapred-site.xml`, `yarn-site.xml` |
| `jdbc` | `jdbc-site.xml` |
| `hive` | `core-site.xml`, `hive-site.xml` |
| `hbase` | `core-site.xml`, `hbase-site.xml` |

Credential references (`secretKeyRef`) are resolved by an init container at pod startup, substituting environment variable placeholders in the XML templates. The actual secrets are never stored in ConfigMaps.

### Updating a Server

When the CRD is updated, the operator re-generates the ConfigMap, and the PXF sidecars pick up the changes on the next volume sync (or an explicit `pxf sync` trigger).

### Deleting a Server

Removing a server from `dataLoading.pxf.servers[]` causes the operator to remove the corresponding directory from the ConfigMap. Any external/foreign tables referencing the deleted server will fail until recreated.

## Writable External Tables (Data Export)

PXF supports writing data back to external stores via `CREATE WRITABLE EXTERNAL TABLE`. The operator can manage export jobs that write Cloudberry query results to S3, HDFS, or JDBC targets:

```sql
CREATE WRITABLE EXTERNAL TABLE export_events (
  event_id int,
  event_type text,
  payload jsonb,
  created_at timestamp
)
LOCATION ('pxf://s3a://export-bucket/events/?PROFILE=s3:parquet&SERVER=s3-datalake')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');
 
INSERT INTO export_events SELECT * FROM public.events WHERE created_at > '2026-01-01';
```

## Pre-Load Health Checks

Before each data loading Job, an init container validates:

- PXF service is healthy on all segments (`pxf status` or HTTP health check on `/pxf/v15/Status`)
- Target table exists and is accessible
- PXF server configuration is valid (connectivity test to external source)
- For gpfdist jobs: gpfdist pods are running and reachable
- Sufficient disk space for temporary/error log files
## Security Considerations

- All external data source credentials are stored in Kubernetes Secrets and injected as environment variables or init-container-rendered XML configs. Secrets are never committed to ConfigMaps.
- PXF server configurations with JDBC connections support TLS/SSL via JDBC URL parameters.
- S3 connections support `fs.s3a.connection.ssl.enabled=true` for encrypted transport.
- Kerberos authentication for Hadoop/Hive/HBase is supported via keytab files mounted as Secrets.
- PXF service communication between segment and sidecar is `localhost`-only (no cross-pod traffic).
- The operator creates a dedicated database role for data loading with minimal privileges.