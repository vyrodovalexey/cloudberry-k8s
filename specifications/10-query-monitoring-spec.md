# Cloudberry Operator - Query Monitoring & Analysis Specification

**Version**: 2.8.0
**API Group**: avsoft.io
 
---

## 1. Overview

Query monitoring provides live query observation, plan inspection, history search, and on-demand plan analysis. The operator exposes query monitoring capabilities through the CRD configuration, REST API, and cloudberry-ctl CLI.

The operator deploys a set of Prometheus exporters as sidecar containers and companion Deployments to collect metrics from Cloudberry system catalogs, segment hosts, and resource management subsystems.

## 2. CRD Schema

### 2.1 QueryMonitoringSpec

```yaml
spec:
  queryMonitoring:
    enabled: true
    historyRetention: "30d"
    samplingInterval: 5
    guestAccess: false
    planCollection: true
    slowQueryThreshold: "1000ms"
    exporters:
      postgresExporter:
        enabled: true
        image: "prometheuscommunity/postgres-exporter:0.16.0"
        port: 9187
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
          limits:
            cpu: "500m"
            memory: "256Mi"
      nodeExporter:
        enabled: true
        image: "prom/node-exporter:1.8.2"
        port: 9100
      cloudberryQueryExporter:
        enabled: true
        image: "cloudberry-query-exporter:1.0.0"
        port: 9188
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
          limits:
            cpu: "500m"
            memory: "256Mi"
      serviceMonitor:
        enabled: true
        namespace: ""                      # Empty = same namespace as cluster
        interval: "15s"
        scrapeTimeout: "10s"
        labels: {}                         # Additional labels for ServiceMonitor
      prometheusRule:
        enabled: true
        namespace: ""
        labels: {}
```

### 2.2 Configuration Fields

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| enabled | bool | Enable query monitoring | false |
| historyRetention | string | History retention period | "30d" |
| samplingInterval | int32 | Sampling interval in seconds | 5 |
| guestAccess | bool | Allow unauthenticated read-only access to guest-enabled endpoints (see §8.5) | false |
| planCollection | bool | Collect visual query plans | true |
| slowQueryThreshold | string | Threshold for slow query logging | "1000ms" |

### 2.3 Disabling Query Monitoring

When `spec.queryMonitoring.enabled` is set to `false` (or the `queryMonitoring` section is omitted entirely), all monitoring-specific API endpoints return an HTTP 200 response with the following body:

```json
{
  "monitoringEnabled": false,
  "message": "query monitoring is not enabled for this cluster"
}
```

**Affected endpoints** (return the disabled response):
- `GET /queries` — Query monitoring overview
- `GET /queries/active` — Active query counts
- `GET /queries/history` — Query history
- `GET /queries/history/{qid}` — Query history detail
- `POST /queries/history/export` — Export query history
- `GET /queries/monitor/state` — Monitor pause/resume state
- `POST /queries/monitor/pause` — Pause monitor
- `GET /metrics/exporters` — Exporter health

**Unaffected endpoints** (continue to function normally):
- `GET /sessions` — Session listing
- `POST /sessions/{pid}/cancel` — Cancel session query
- `DELETE /sessions/{pid}` — Terminate session
- `POST /queries/plan-check` — Plan analysis
- `POST /queries/{pid}/cancel` — Cancel query by PID
- `POST /queries/{pid}/move` — Move query to resource group
- `GET /queries/{pid}` — Query detail by PID

The operator also records a `cloudberry_monitoring_disabled_access_total` Prometheus metric each time a monitoring endpoint is accessed while monitoring is disabled, labeled by `cluster` and `namespace`.

### 2.4 Status Fields

Added to CloudberryClusterStatus:

- `activeQueries` (int32) — Currently running queries
- `queuedQueries` (int32) — Queries waiting in queue
- `blockedQueries` (int32) — Queries blocked by locks

These fields are **updated from the database** on every admin-controller reconcile cycle by querying `pg_stat_activity`:

```sql
SELECT 
  COUNT(*) FILTER (WHERE state = 'active') as active,
  COUNT(*) FILTER (WHERE wait_event_type = 'Lock') as blocked,
  COUNT(*) FILTER (WHERE state = 'idle in transaction') as queued
FROM pg_stat_activity WHERE pid != pg_backend_pid()
```

The values are then pushed to Prometheus metrics (`cloudberry_active_queries`, `cloudberry_queued_queries`, `cloudberry_blocked_queries`) and patched to the CR status subresource. When the DB is unavailable, the update is skipped (non-fatal).

## 3. Exporter Architecture

The operator deploys three complementary exporters to cover all metric layers. Each exporter runs as a sidecar container (on the coordinator pod) or as a DaemonSet (on segment hosts), and exposes a `/metrics` endpoint for Prometheus scraping.

### 3.1 postgres_exporter (Coordinator Sidecar)

**Image**: `prometheuscommunity/postgres-exporter` (prometheus-community)

Runs on the **coordinator pod** as a sidecar. Deployed with `--disable-default-metrics` because Cloudberry (like Greenplum) uses a non-standard PostgreSQL fork; the default built-in collectors produce errors on MPP-specific catalog differences. All metrics are defined via a custom `queries.yaml` ConfigMap.

The `--disable-default-metrics` flag removes all built-in metrics and uses only queries supplied in the `queries.yaml` file, as explicitly documented by the project for Greenplum/Cloudberry compatibility.

**Connection**: connects to the coordinator via a Unix socket or `localhost:5432` using a dedicated `cloudberry_exporter` database role with `pg_monitor` membership.

**Deployment**:

```yaml
containers:
  - name: postgres-exporter
    image: prometheuscommunity/postgres-exporter:0.16.0
    args:
      - "--disable-default-metrics"
      - "--extend.query-path=/etc/postgres-exporter/queries.yaml"
      - "--auto-discover-databases"
      - "--web.listen-address=:9187"
    env:
      - name: DATA_SOURCE_NAME
        valueFrom:
          secretKeyRef:
            name: <cluster>-exporter-credentials
            key: dsn
    ports:
      - name: pg-exporter
        containerPort: 9187
    volumeMounts:
      - name: exporter-queries
        mountPath: /etc/postgres-exporter
volumes:
  - name: exporter-queries
    configMap:
      name: <cluster>-exporter-queries
```

### 3.2 cloudberry-query-exporter (Coordinator Sidecar)

**Image**: operator-provided `cloudberry-query-exporter`

A lightweight custom Go exporter (built and maintained as part of the operator project) that queries Cloudberry-specific system views at the configured `samplingInterval` and exposes them as Prometheus metrics. It handles views and functions that `postgres_exporter` cannot easily model (multi-row resource group status pivots, real-time CPU/memory per-query breakdowns, spill file aggregation).

Runs as a second sidecar on the coordinator pod, connecting over the same Unix socket.

**Deployment**:

```yaml
containers:
  - name: cloudberry-query-exporter
    image: cloudberry-query-exporter:1.0.0
    args:
      - "--listen-address=:9188"
      - "--sampling-interval=5s"
      - "--slow-query-threshold=1000ms"
    env:
      - name: DATA_SOURCE_NAME
        valueFrom:
          secretKeyRef:
            name: <cluster>-exporter-credentials
            key: dsn
    ports:
      - name: cbdb-exporter
        containerPort: 9188
```

### 3.3 node_exporter (Segment Host DaemonSet)

**Image**: `prom/node-exporter`

Deployed as a **DaemonSet** across all Cloudberry segment hosts to collect OS-level metrics (CPU, memory, disk I/O, network, filesystem). These metrics correlate with per-query resource consumption at the host level.

The DaemonSet is labelled and selected via `cloudberry.apache.org/cluster: <cluster-name>` so that only nodes running Cloudberry segments are targeted.

**Deployment**:

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: <cluster>-node-exporter
  labels:
    cloudberry.apache.org/cluster: <cluster>
    cloudberry.apache.org/component: node-exporter
spec:
  selector:
    matchLabels:
      cloudberry.apache.org/cluster: <cluster>
      cloudberry.apache.org/component: node-exporter
  template:
    spec:
      hostPID: true
      hostNetwork: true
      containers:
        - name: node-exporter
          image: prom/node-exporter:1.8.2
          args:
            - "--path.rootfs=/host"
            - "--web.listen-address=:9100"
            - "--collector.filesystem.mount-points-exclude=^/(dev|proc|sys|var/lib/docker|run)($|/)"
          ports:
            - name: metrics
              containerPort: 9100
              hostPort: 9100
          volumeMounts:
            - name: rootfs
              mountPath: /host
              readOnly: true
      volumes:
        - name: rootfs
          hostPath:
            path: /
```

### 3.4 ServiceMonitor and PrometheusRule

When `exporters.serviceMonitor.enabled: true`, the operator creates `ServiceMonitor` resources for all three exporters so that a Prometheus Operator stack discovers them automatically:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: <cluster>-query-metrics
  labels:
    cloudberry.apache.org/cluster: <cluster>
spec:
  selector:
    matchLabels:
      cloudberry.apache.org/cluster: <cluster>
  endpoints:
    - port: pg-exporter
      interval: "15s"
      path: /metrics
    - port: cbdb-exporter
      interval: "15s"
      path: /metrics
    - port: node-metrics
      interval: "15s"
      path: /metrics
```

When `exporters.prometheusRule.enabled: true`, the operator creates a `PrometheusRule` containing alerting rules (see Section 7).

## 4. Exporter Custom Queries ConfigMap

The operator generates a ConfigMap `<cluster>-exporter-queries` containing SQL queries for `postgres_exporter`. Each query maps to a Cloudberry system catalog view.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: <cluster>-exporter-queries
data:
  queries.yaml: |
    # ── Connection Metrics ──────────────────────────────────────
    cloudberry_connections:
      query: |
        SELECT datname,
               usename,
               state,
               COALESCE(application_name, '') AS application_name,
               COUNT(*) AS count
        FROM pg_stat_activity
        WHERE backend_type = 'client backend'
        GROUP BY datname, usename, state, application_name
      metrics:
        - datname:
            usage: "LABEL"
            description: "Database name"
        - usename:
            usage: "LABEL"
            description: "Username"
        - state:
            usage: "LABEL"
            description: "Connection state"
        - application_name:
            usage: "LABEL"
            description: "Application name"
        - count:
            usage: "GAUGE"
            description: "Number of connections"
 
    cloudberry_connections_max:
      query: "SELECT setting::int AS max_connections FROM pg_settings WHERE name = 'max_connections'"
      metrics:
        - max_connections:
            usage: "GAUGE"
            description: "Maximum allowed connections"
 
    # ── Database Statistics ─────────────────────────────────────
    cloudberry_database_stats:
      query: |
        SELECT datname,
               numbackends,
               xact_commit,
               xact_rollback,
               blks_read,
               blks_hit,
               tup_returned,
               tup_fetched,
               tup_inserted,
               tup_updated,
               tup_deleted,
               conflicts,
               temp_files,
               temp_bytes,
               deadlocks
        FROM pg_stat_database
        WHERE datname NOT IN ('template0', 'template1')
      metrics:
        - datname:
            usage: "LABEL"
            description: "Database name"
        - numbackends:
            usage: "GAUGE"
            description: "Active backends"
        - xact_commit:
            usage: "COUNTER"
            description: "Transactions committed"
        - xact_rollback:
            usage: "COUNTER"
            description: "Transactions rolled back"
        - blks_read:
            usage: "COUNTER"
            description: "Disk blocks read"
        - blks_hit:
            usage: "COUNTER"
            description: "Buffer cache hits"
        - tup_returned:
            usage: "COUNTER"
            description: "Rows returned by queries"
        - tup_fetched:
            usage: "COUNTER"
            description: "Rows fetched by queries"
        - tup_inserted:
            usage: "COUNTER"
            description: "Rows inserted"
        - tup_updated:
            usage: "COUNTER"
            description: "Rows updated"
        - tup_deleted:
            usage: "COUNTER"
            description: "Rows deleted"
        - conflicts:
            usage: "COUNTER"
            description: "Queries cancelled due to conflicts"
        - temp_files:
            usage: "COUNTER"
            description: "Temp files created"
        - temp_bytes:
            usage: "COUNTER"
            description: "Temp bytes written"
        - deadlocks:
            usage: "COUNTER"
            description: "Deadlocks detected"
 
    # ── Segment Health ──────────────────────────────────────────
    cloudberry_segment_status:
      query: |
        SELECT hostname,
               role,
               preferred_role,
               status,
               mode,
               COUNT(*) AS count
        FROM gp_segment_configuration
        GROUP BY hostname, role, preferred_role, status, mode
      metrics:
        - hostname:
            usage: "LABEL"
            description: "Segment hostname"
        - role:
            usage: "LABEL"
            description: "Current role (p=primary, m=mirror)"
        - preferred_role:
            usage: "LABEL"
            description: "Preferred role"
        - status:
            usage: "LABEL"
            description: "Status (u=up, d=down)"
        - mode:
            usage: "LABEL"
            description: "Sync mode (s=synced, n=not synced)"
        - count:
            usage: "GAUGE"
            description: "Number of segments in this state"
 
    cloudberry_segments_down:
      query: |
        SELECT COUNT(*) AS down_count
        FROM gp_segment_configuration
        WHERE status = 'd'
      metrics:
        - down_count:
            usage: "GAUGE"
            description: "Number of segments currently down"
 
    # ── Lock Metrics ────────────────────────────────────────────
    cloudberry_locks:
      query: |
        SELECT l.mode,
               l.locktype,
               CASE WHEN l.granted THEN 'granted' ELSE 'waiting' END AS state,
               COUNT(*) AS count
        FROM pg_locks l
        GROUP BY l.mode, l.locktype, state
      metrics:
        - mode:
            usage: "LABEL"
            description: "Lock mode"
        - locktype:
            usage: "LABEL"
            description: "Lock type"
        - state:
            usage: "LABEL"
            description: "Lock state (granted/waiting)"
        - count:
            usage: "GAUGE"
            description: "Number of locks"
 
    # ── Table Statistics ────────────────────────────────────────
    cloudberry_table_stats:
      query: |
        SELECT schemaname,
               relname,
               seq_scan,
               seq_tup_read,
               idx_scan,
               idx_tup_fetch,
               n_tup_ins,
               n_tup_upd,
               n_tup_del,
               n_live_tup,
               n_dead_tup,
               last_vacuum,
               last_autovacuum,
               last_analyze,
               last_autoanalyze
        FROM pg_stat_all_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')
        ORDER BY n_live_tup DESC
        LIMIT 100
      metrics:
        - schemaname:
            usage: "LABEL"
            description: "Schema name"
        - relname:
            usage: "LABEL"
            description: "Table name"
        - seq_scan:
            usage: "COUNTER"
            description: "Sequential scans"
        - seq_tup_read:
            usage: "COUNTER"
            description: "Rows read by sequential scans"
        - idx_scan:
            usage: "COUNTER"
            description: "Index scans"
        - idx_tup_fetch:
            usage: "COUNTER"
            description: "Rows fetched by index scans"
        - n_tup_ins:
            usage: "COUNTER"
            description: "Rows inserted"
        - n_tup_upd:
            usage: "COUNTER"
            description: "Rows updated"
        - n_tup_del:
            usage: "COUNTER"
            description: "Rows deleted"
        - n_live_tup:
            usage: "GAUGE"
            description: "Estimated live rows"
        - n_dead_tup:
            usage: "GAUGE"
            description: "Estimated dead rows"
 
    # ── WAL ─────────────────────────────────────────────────────
    cloudberry_wal:
      query: |
        SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') AS wal_bytes_total
      metrics:
        - wal_bytes_total:
            usage: "COUNTER"
            description: "Total WAL bytes generated"
 
    # ── Replication ─────────────────────────────────────────────
    cloudberry_replication:
      query: |
        SELECT client_addr,
               application_name,
               state,
               sent_lsn - write_lsn AS write_lag_bytes,
               sent_lsn - flush_lsn AS flush_lag_bytes,
               sent_lsn - replay_lsn AS replay_lag_bytes
        FROM pg_stat_replication
      metrics:
        - client_addr:
            usage: "LABEL"
            description: "Replica address"
        - application_name:
            usage: "LABEL"
            description: "Application name"
        - state:
            usage: "LABEL"
            description: "Replication state"
        - write_lag_bytes:
            usage: "GAUGE"
            description: "Write lag in bytes"
        - flush_lag_bytes:
            usage: "GAUGE"
            description: "Flush lag in bytes"
        - replay_lag_bytes:
            usage: "GAUGE"
            description: "Replay lag in bytes"
```

## 5. Cloudberry-Specific Metrics (cloudberry-query-exporter)

The custom `cloudberry-query-exporter` queries Cloudberry-specific system views that require pivoting or aggregation beyond what `postgres_exporter` can model with flat queries. It exposes the following metrics:

### 5.1 Query Activity Metrics

Source: `pg_stat_activity`, `gp_stat_activity`

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_queries_active` | Gauge | cluster, namespace | Currently running queries (state='active') |
| `cloudberry_queries_idle` | Gauge | cluster, namespace | Idle connections |
| `cloudberry_queries_idle_in_transaction` | Gauge | cluster, namespace | Connections idle in transaction |
| `cloudberry_queries_queued` | Gauge | cluster, namespace | Queries waiting in resource group queue |
| `cloudberry_queries_blocked` | Gauge | cluster, namespace | Queries blocked by locks (waiting=true) |
| `cloudberry_queries_total` | Counter | cluster, namespace, datname, usename, state | Total queries observed by state |
| `cloudberry_queries_slow_total` | Counter | cluster, namespace, datname, usename | Queries exceeding slowQueryThreshold |
| `cloudberry_query_duration_seconds` | Histogram | cluster, namespace, datname, usename | Distribution of query durations |
| `cloudberry_query_max_duration_seconds` | Gauge | cluster, namespace | Duration of the longest currently running query |
| `cloudberry_queries_cancelled_total` | Counter | cluster, namespace, reason | Queries cancelled (by user, timeout, resource limit) |

### 5.2 Resource Group Metrics

Source: `gp_toolkit.gp_resgroup_status`, `gp_toolkit.gp_resgroup_status_per_host`, `gp_toolkit.gp_resgroup_config`

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_resgroup_running_queries` | Gauge | cluster, namespace, rsgname | Running queries in resource group |
| `cloudberry_resgroup_queued_queries` | Gauge | cluster, namespace, rsgname | Queued queries in resource group |
| `cloudberry_resgroup_executed_total` | Counter | cluster, namespace, rsgname | Total queries executed by resource group |
| `cloudberry_resgroup_queue_duration_seconds_total` | Counter | cluster, namespace, rsgname | Total time queries spent in queue |
| `cloudberry_resgroup_cpu_usage_percent` | Gauge | cluster, namespace, rsgname, hostname | Real-time CPU core usage by resource group per host |
| `cloudberry_resgroup_memory_usage_bytes` | Gauge | cluster, namespace, rsgname, hostname | Memory used by resource group per host |
| `cloudberry_resgroup_memory_available_bytes` | Gauge | cluster, namespace, rsgname, hostname | Memory available to resource group per host |
| `cloudberry_resgroup_memory_quota_used_bytes` | Gauge | cluster, namespace, rsgname, hostname | Quota memory used by resource group per host |
| `cloudberry_resgroup_memory_shared_used_bytes` | Gauge | cluster, namespace, rsgname, hostname | Shared memory used by resource group per host |

### 5.3 Resource Group I/O Metrics

Source: `gp_toolkit.gp_resgroup_iostats_per_host`

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_resgroup_io_read_bytes_per_sec` | Gauge | cluster, namespace, rsgname, hostname, tablespace | Read bytes/sec by resource group |
| `cloudberry_resgroup_io_write_bytes_per_sec` | Gauge | cluster, namespace, rsgname, hostname, tablespace | Write bytes/sec by resource group |
| `cloudberry_resgroup_io_read_ops_per_sec` | Gauge | cluster, namespace, rsgname, hostname, tablespace | Read IOPS by resource group |
| `cloudberry_resgroup_io_write_ops_per_sec` | Gauge | cluster, namespace, rsgname, hostname, tablespace | Write IOPS by resource group |

### 5.4 Spill File Metrics

Source: `gp_toolkit.gp_workfile_usage_per_query`, `gp_toolkit.gp_workfile_usage_per_segment`

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_spill_files_active` | Gauge | cluster, namespace | Active spill files across all segments |
| `cloudberry_spill_files_bytes` | Gauge | cluster, namespace | Total spill file size in bytes |
| `cloudberry_spill_files_per_segment` | Gauge | cluster, namespace, segment_id, hostname | Spill files per segment |
| `cloudberry_spill_files_per_query` | Gauge | cluster, namespace, datname, pid | Spill files per query |

### 5.5 Segment Health Metrics

Source: `gp_segment_configuration`, FTS (Fault Tolerance Service)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_segments_total` | Gauge | cluster, namespace, role | Total segments by role (primary/mirror) |
| `cloudberry_segments_up` | Gauge | cluster, namespace, role | Segments in 'up' status |
| `cloudberry_segments_down` | Gauge | cluster, namespace, role | Segments in 'down' status |
| `cloudberry_segments_not_synced` | Gauge | cluster, namespace | Segments not in synced mode |
| `cloudberry_segments_not_preferred_role` | Gauge | cluster, namespace | Segments not running in preferred role |
| `cloudberry_cluster_uptime_seconds` | Gauge | cluster, namespace | Coordinator uptime since postmaster start |

### 5.6 Distributed Transaction Metrics

Source: `pg_stat_activity`, `gp_distributed_log`

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_distributed_transactions_active` | Gauge | cluster, namespace | Active distributed transactions |
| `cloudberry_distributed_transactions_committed_total` | Counter | cluster, namespace | Committed distributed transactions |
| `cloudberry_distributed_transactions_aborted_total` | Counter | cluster, namespace | Aborted distributed transactions |
| `cloudberry_oldest_transaction_age_seconds` | Gauge | cluster, namespace | Age of the oldest running transaction |

### 5.7 Data Distribution / Skew Metrics

Source: `gp_toolkit.gp_skew_coefficients`, periodic sampling

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_table_skew_coefficient` | Gauge | cluster, namespace, schemaname, tablename | Data distribution skew coefficient (0=uniform, 1=maximally skewed) |
| `cloudberry_query_cpu_skew_percent` | Gauge | cluster, namespace, pid | CPU skew for a running query across segments |

### 5.8 Query History Metrics

Source: Operator API server, `cloudberry-query-exporter` sidecar

These metrics track the operational health of the query history subsystem. They are registered by the operator's Prometheus recorder (namespace: `cloudberry`).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_query_history_total` | Counter | cluster, namespace | Total queries recorded in the history table |
| `cloudberry_query_history_search_duration_seconds` | Histogram | cluster, namespace | Duration of history search operations (buckets: 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s) |
| `cloudberry_query_history_export_total` | Counter | cluster, namespace, format | Total history export operations (format label: `csv`) |
| `cloudberry_query_history_retention_deleted_total` | Counter | cluster, namespace | Total entries deleted by retention cleanup |
| `cloudberry_query_history_size_bytes` | Gauge | cluster, namespace | Current estimated size of the query history table in bytes |

## 6. Prometheus Metrics Summary (All Sources)

### 6.1 postgres_exporter Metrics

| Metric | Type | Labels | Source View |
|--------|------|--------|-------------|
| `cloudberry_connections` | Gauge | datname, usename, state, application_name | `pg_stat_activity` |
| `cloudberry_connections_max` | Gauge | — | `pg_settings` |
| `cloudberry_database_stats_numbackends` | Gauge | datname | `pg_stat_database` |
| `cloudberry_database_stats_xact_commit` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_xact_rollback` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_blks_read` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_blks_hit` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_tup_returned` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_tup_fetched` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_tup_inserted` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_tup_updated` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_tup_deleted` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_temp_files` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_temp_bytes` | Counter | datname | `pg_stat_database` |
| `cloudberry_database_stats_deadlocks` | Counter | datname | `pg_stat_database` |
| `cloudberry_segment_status` | Gauge | hostname, role, preferred_role, status, mode | `gp_segment_configuration` |
| `cloudberry_segments_down_count` | Gauge | — | `gp_segment_configuration` |
| `cloudberry_locks` | Gauge | mode, locktype, state | `pg_locks` |
| `cloudberry_table_stats_*` | Counter/Gauge | schemaname, relname | `pg_stat_all_tables` |
| `cloudberry_wal_bytes_total` | Counter | — | `pg_current_wal_lsn()` |
| `cloudberry_replication_*` | Gauge | client_addr, application_name, state | `pg_stat_replication` |

### 6.2 cloudberry-query-exporter Metrics

All metrics from Section 5 (query activity, resource groups, resource group I/O, spill files, segment health, distributed transactions, data skew).

### 6.3 node_exporter Metrics (Segment Hosts)

Standard `node_exporter` metrics prefixed with `node_`. Key metrics used in Cloudberry dashboards and alerts:

| Metric | Description |
|--------|-------------|
| `node_cpu_seconds_total` | CPU time by mode (user, system, idle, iowait) |
| `node_memory_MemTotal_bytes` | Total memory |
| `node_memory_MemAvailable_bytes` | Available memory |
| `node_disk_read_bytes_total` | Disk read bytes |
| `node_disk_written_bytes_total` | Disk written bytes |
| `node_disk_io_time_seconds_total` | Disk I/O time |
| `node_filesystem_avail_bytes` | Available filesystem space |
| `node_filesystem_size_bytes` | Total filesystem size |
| `node_network_receive_bytes_total` | Network bytes received |
| `node_network_transmit_bytes_total` | Network bytes transmitted |
| `node_load1`, `node_load5`, `node_load15` | System load averages |

## 7. Alerting Rules (PrometheusRule)

The operator creates a `PrometheusRule` with pre-configured alerts:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: <cluster>-query-alerts
  labels:
    cloudberry.apache.org/cluster: <cluster>
spec:
  groups:
    - name: cloudberry-segment-health
      rules:
        - alert: CloudberrySegmentDown
          expr: cloudberry_segments_down > 0
          for: 1m
          labels:
            severity: critical
          annotations:
            summary: "Cloudberry segment(s) down on {{ $labels.cluster }}"
            description: "{{ $value }} segment(s) are in down status."
 
        - alert: CloudberrySegmentNotSynced
          expr: cloudberry_segments_not_synced > 0
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Cloudberry segment(s) not synced on {{ $labels.cluster }}"
 
        - alert: CloudberrySegmentNotPreferredRole
          expr: cloudberry_segments_not_preferred_role > 0
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Segment(s) not running in preferred role on {{ $labels.cluster }}"
 
    - name: cloudberry-query-performance
      rules:
        - alert: CloudberrySlowQueryRate
          expr: rate(cloudberry_queries_slow_total[5m]) > 0.1
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "High slow query rate on {{ $labels.cluster }}"
 
        - alert: CloudberryLongRunningQuery
          expr: cloudberry_query_max_duration_seconds > 3600
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Query running longer than 1 hour on {{ $labels.cluster }}"
 
        - alert: CloudberryHighQueuedQueries
          expr: cloudberry_queries_queued > 50
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "High number of queued queries on {{ $labels.cluster }}"
 
        - alert: CloudberryBlockedQueries
          expr: cloudberry_queries_blocked > 10
          for: 2m
          labels:
            severity: warning
          annotations:
            summary: "Multiple queries blocked by locks on {{ $labels.cluster }}"
 
        - alert: CloudberryDeadlockDetected
          expr: increase(cloudberry_database_stats_deadlocks[5m]) > 0
          labels:
            severity: warning
          annotations:
            summary: "Deadlock detected on {{ $labels.cluster }}/{{ $labels.datname }}"
 
    - name: cloudberry-resource-groups
      rules:
        - alert: CloudberryResourceGroupQueueSaturation
          expr: >
            cloudberry_resgroup_queued_queries
            / on(rsgname)
            group_left cloudberry_resgroup_config_concurrency > 0.8
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Resource group {{ $labels.rsgname }} queue near saturation"
 
    - name: cloudberry-connections
      rules:
        - alert: CloudberryConnectionPoolExhaustion
          expr: >
            sum(cloudberry_connections) / cloudberry_connections_max > 0.85
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Connection pool over 85% utilized on {{ $labels.cluster }}"
 
    - name: cloudberry-disk
      rules:
        - alert: CloudberryHighSpillFileUsage
          expr: cloudberry_spill_files_bytes > 10737418240
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Spill files exceed 10GB on {{ $labels.cluster }}"
 
        - alert: CloudberrySegmentDiskFull
          expr: >
            (node_filesystem_avail_bytes{mountpoint=~"/data.*"} / node_filesystem_size_bytes{mountpoint=~"/data.*"}) < 0.1
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "Segment host disk less than 10% free on {{ $labels.hostname }}"
 
    - name: cloudberry-host-resources
      rules:
        - alert: CloudberryHostHighCPU
          expr: >
            (1 - avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[5m]))) > 0.9
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Segment host CPU above 90% on {{ $labels.instance }}"
 
        - alert: CloudberryHostHighMemory
          expr: >
            (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) > 0.9
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Segment host memory above 90% on {{ $labels.instance }}"
```

## 8. Capabilities

### 8.1 Live Query Monitor

- List running, queued, and blocked queries
- Show query ID, status, user, database, workload, times, spill files, CPU
- Cancel running queries with reason message
- Reassign queries to different resource groups
- Filter by status (running/queued/blocked)
- Advanced search by time, database, resource group, user, tags
### 8.2 Query Details

- Execution metrics (CPU, memory, spill, disk I/O, locks)
- Real-time plan with progress animation
- Top slice metrics (CPU, memory, disk I/O)
- Inner queries of function calls
- Textual EXPLAIN plan
- Accessed tables list
### 8.3 Query History

- Browse completed queries with charts
- Advanced search with regex/wildcard
- Export to CSV
- Historical query details with plan

#### 8.3.1 History Storage

Cloudberry does not persist completed query history natively. The operator maintains a custom `cloudberry_query_history` table in the coordinator database, created automatically by the `cloudberry-query-exporter` sidecar on startup.

**Table Schema**:

```sql
CREATE TABLE IF NOT EXISTS cloudberry_query_history (
    id               BIGSERIAL PRIMARY KEY,
    query_id         TEXT NOT NULL,
    pid              INTEGER NOT NULL,
    username         TEXT NOT NULL,
    database_name    TEXT NOT NULL,
    query_text       TEXT NOT NULL,
    query_start      TIMESTAMPTZ NOT NULL,
    query_end        TIMESTAMPTZ NOT NULL,
    duration_ms      DOUBLE PRECISION NOT NULL,
    state            TEXT NOT NULL,
    rows_affected    BIGINT DEFAULT 0,
    cpu_time_ms      DOUBLE PRECISION DEFAULT 0,
    memory_bytes     BIGINT DEFAULT 0,
    spill_bytes      BIGINT DEFAULT 0,
    disk_read_bytes  BIGINT DEFAULT 0,
    disk_write_bytes BIGINT DEFAULT 0,
    wait_events      TEXT DEFAULT '',
    resource_group   TEXT DEFAULT '',
    explain_plan     TEXT DEFAULT '',
    error_message    TEXT DEFAULT '',
    created_at       TIMESTAMPTZ DEFAULT NOW()
) DISTRIBUTED BY (id);
```

**Indexes**:

| Index | Column | Purpose |
|-------|--------|---------|
| `idx_query_history_start` | `query_start` | Time-range queries and ORDER BY |
| `idx_query_history_user` | `username` | User-based filtering |
| `idx_query_history_db` | `database_name` | Database-based filtering |

**Column Reference**:

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Auto-incrementing primary key |
| `query_id` | TEXT | Unique identifier, format: `q-{pid}-{query_start_unix_nano}` |
| `pid` | INTEGER | Backend process ID at execution time |
| `username` | TEXT | Database user who ran the query |
| `database_name` | TEXT | Target database |
| `query_text` | TEXT | Full SQL text |
| `query_start` | TIMESTAMPTZ | Query start timestamp |
| `query_end` | TIMESTAMPTZ | Query completion timestamp |
| `duration_ms` | DOUBLE PRECISION | Execution duration in milliseconds |
| `state` | TEXT | Terminal state: `completed`, `cancelled`, or `error` |
| `rows_affected` | BIGINT | Number of rows affected |
| `cpu_time_ms` | DOUBLE PRECISION | CPU time consumed |
| `memory_bytes` | BIGINT | Peak memory usage |
| `spill_bytes` | BIGINT | Bytes spilled to disk |
| `disk_read_bytes` | BIGINT | Disk read bytes |
| `disk_write_bytes` | BIGINT | Disk write bytes |
| `wait_events` | TEXT | Wait event types observed during execution |
| `resource_group` | TEXT | Resource group the query ran in |
| `explain_plan` | TEXT | Saved EXPLAIN plan (when collected) |
| `error_message` | TEXT | Error message if the query failed |
| `created_at` | TIMESTAMPTZ | Row insertion timestamp |

#### 8.3.2 History Collection

The `cloudberry-query-exporter` sidecar collects completed queries using a **PID tracking** strategy:

1. **Snapshot**: On each sampling interval, the collector queries `pg_stat_activity` for all client backend sessions (excluding its own PID).
2. **Compare**: The current PID set is compared against the previous cycle's PID set.
3. **Detect completion**: PIDs present in the previous cycle but absent in the current cycle indicate completed queries.
4. **Filter**: Only sessions that were in `active` state with non-empty query text are recorded. Idle sessions are ignored.
5. **Insert**: Completed queries are inserted into the `cloudberry_query_history` table with a generated `query_id` of format `q-{pid}-{query_start_unix_nano}`.

**Session snapshot query**:

```sql
SELECT pid, COALESCE(usename, ''), COALESCE(datname, ''),
    COALESCE(query, ''), COALESCE(query_start, now()),
    COALESCE(state, ''), COALESCE(wait_event_type, '')
FROM pg_stat_activity
WHERE backend_type = 'client backend'
  AND pid != pg_backend_pid()
  AND usename IS NOT NULL
```

#### 8.3.3 EXPLAIN Plan Collection

When `planCollection: true` is set in the CRD, the collector automatically captures EXPLAIN plans for slow queries:

- **Trigger condition**: Query duration exceeds the `slowQueryThreshold` (default `1000ms`).
- **Plan format**: `EXPLAIN (FORMAT TEXT) <query>` — text format for readability and storage efficiency.
- **Timeout**: EXPLAIN execution uses a 2-second timeout to prevent blocking.
- **Excluded commands**: DDL and utility commands are skipped. The following prefixes are excluded: `CREATE`, `ALTER`, `DROP`, `COPY`, `GRANT`, `REVOKE`, `SET`, `RESET`, `VACUUM`, `ANALYZE`, `REINDEX`.
- **Failure handling**: If EXPLAIN fails (e.g., temporary tables no longer exist), the plan field is left empty. Collection failures are logged at DEBUG level and do not affect history insertion.

**When `planCollection: false`**:

- The `--plan-collection` flag is **omitted** from the `cloudberry-query-exporter` container args.
- Query history entries will have an **empty** `explainPlan` field.
- All other monitoring functionality (active query counts, history collection, slow query detection) continues to operate normally.
- The exporter still collects query metadata (duration, user, database, resource group) — only EXPLAIN plan capture is disabled.
- This is useful in environments where running EXPLAIN on production queries is undesirable due to performance concerns or security policies.

#### 8.3.4 Retention

Automatic cleanup is driven by the `historyRetention` CRD field (default `"30d"`). The `cloudberry-query-exporter` sidecar periodically deletes entries older than the retention period:

```sql
DELETE FROM cloudberry_query_history WHERE created_at < $1
```

Where `$1` is `NOW() - historyRetention`. The number of deleted rows is logged and recorded in the `cloudberry_query_history_retention_deleted_total` Prometheus metric.

#### 8.3.5 Search Capabilities

The query history search supports the following filter parameters (all filters are AND-combined):

| Parameter | Type | Description |
|-----------|------|-------------|
| `pattern` | string | Search pattern applied to `query_text` |
| `patternType` | string | `regex` (default) or `wildcard` |
| `user` | string | Exact match on `username` |
| `database` | string | Exact match on `database_name` |
| `resourceGroup` | string | Exact match on `resource_group` |
| `state` | string | Exact match on `state` (`completed`, `cancelled`, `error`) |
| `minDuration` | float | Minimum `duration_ms` threshold |
| `since` | string | Start of time range (RFC3339 or Go duration, e.g. `24h`) |
| `until` | string | End of time range (RFC3339) |
| `limit` | int | Page size (default: 50, max: 100) |
| `offset` | int | Pagination offset (default: 0) |

**Pattern matching**:

- **Regex** (`patternType=regex`): Uses PostgreSQL `~` operator. Patterns are validated with Go's `regexp.Compile` before execution to prevent ReDoS attacks.
- **Wildcard** (`patternType=wildcard`): Converts to SQL `LIKE` pattern — `*` maps to `%`, `?` maps to `_`. Existing `%` and `_` characters in the pattern are escaped.

#### 8.3.6 CSV Export

The export endpoint streams query history as CSV directly to the HTTP response, avoiding in-memory buffering of the full result set. No pagination limits are applied to exports — all matching rows are included.

**CSV columns** (in order):

| Column | Source Field |
|--------|-------------|
| `query_id` | `query_id` |
| `username` | `username` |
| `database` | `database_name` |
| `query_text` | `query_text` |
| `start_time` | `query_start` (RFC3339) |
| `end_time` | `query_end` (RFC3339) |
| `duration_ms` | `duration_ms` |
| `rows_affected` | `rows_affected` |
| `cpu_time_ms` | `cpu_time_ms` |
| `memory_bytes` | `memory_bytes` |
| `spill_bytes` | `spill_bytes` |
| `state` | `state` |

**Response headers**:

```
Content-Type: text/csv
Content-Disposition: attachment; filename="query-history.csv"
```
### 8.4 Plan Analysis

The plan analysis subsystem provides a **static plan checker** that analyzes PostgreSQL/Cloudberry `EXPLAIN ANALYZE` output without requiring a database connection. The analysis is purely text-based — it parses the plan text into a tree of `PlanNode` structs and applies a set of detection rules to identify performance issues with actionable recommendations.

The implementation lives in `internal/planchecker/` and consists of three components:

- **parser.go** — Parses EXPLAIN ANALYZE text output into a tree of `PlanNode` structs. Supports standard PostgreSQL plan nodes and Cloudberry-specific motion nodes (Gather Motion, Redistribute Motion, Broadcast Motion). Uses regex-based extraction for node types, costs, actual times, filters, sort methods, hash conditions, index conditions, and join filters.
- **checker.go** — Applies six detection rules to each node in the plan tree and collects identified issues. Entry point: `CheckPlan(planText string) (*PlanCheckResult, error)`.
- **types.go** — Defines `PlanNode`, `PlanIssue`, `PlanCheckResult`, and `PlanCheckRequest` types, along with severity levels, category constants, and threshold constants.

#### 8.4.1 Detection Rules

The plan checker applies the following rules to every node in the parsed plan tree:

| # | Rule | Category | Trigger Condition | Severity | Recommendation |
|---|------|----------|-------------------|----------|----------------|
| 1 | Sequential Scan on Large Table | `sequential_scan` | Node type starts with `Seq Scan` AND `actualRows > 10,000` | warning | Create an index on the table (includes filter condition if present) |
| 2 | Row Estimate Mismatch | `row_estimate_mismatch` | `\|planRows - actualRows\| / max(planRows, 1) > 10.0` (node must have actual data from ANALYZE) | warning | Run `ANALYZE` on the tables involved to update statistics |
| 3 | Sort Spill to Disk | `sort_spill` | `SortSpaceType == "Disk"` | warning | Increase `work_mem` (reports current sort disk usage in kB) |
| 4 | Nested Loop with High Rows | `nested_loop_high_rows` | Node type starts with `Nested Loop` AND `actualRows * max(actualLoops, 1) > 100,000` | warning | Consider Hash Join or Merge Join instead |
| 5 | Excessive Filter Rows Removed | `excessive_filter_rows` | `RowsRemoved > 10 * max(ActualRows, 1)` AND `RowsRemoved > 1,000` | warning | Add an index on the filter column |
| 6 | High-Cost Node | `high_cost_node` | `TotalCost > 10,000` | info | Review the node for optimization opportunities |

#### 8.4.2 API Endpoint

**POST /clusters/{name}/queries/plan-check**

Permission: **Basic** (read-only, no database connection required).

**Request Body** (`Content-Type: application/json`):

```json
{
  "planText": "Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..431.00 rows=1000 width=36) (actual time=1.234..5.678 rows=50000 loops=1)\n  ->  Seq Scan on orders  (cost=0.00..431.00 rows=1000 width=36) (actual time=0.100..3.456 rows=50000 loops=1)\n        Filter: (total > 100)\n        Rows Removed by Filter: 150000\nExecution Time: 6.789 ms"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `planText` | string | Yes | Raw EXPLAIN ANALYZE text output |

**Response** (`200 OK`):

```json
{
  "issues": [
    {
      "severity": "warning",
      "category": "sequential_scan",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Sequential scan on orders returned 50000 rows",
      "recommendation": "Consider creating an index on orders for filter condition (total > 100)",
      "details": {
        "actualRows": 50000,
        "filter": "(total > 100)",
        "totalCost": 431.00
      }
    },
    {
      "severity": "warning",
      "category": "row_estimate_mismatch",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Row estimate mismatch on orders: estimated 1000 rows, actual 50000 rows (49x off)",
      "recommendation": "Run ANALYZE on the tables involved to update statistics",
      "details": {
        "planRows": 1000,
        "actualRows": 50000,
        "ratio": 49.0
      }
    },
    {
      "severity": "warning",
      "category": "excessive_filter_rows",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Filter removed 3x more rows than returned (150000 removed vs 50000 returned)",
      "recommendation": "Filter removed 3x more rows than returned; consider adding index on filter column",
      "details": {
        "rowsRemoved": 150000,
        "actualRows": 50000,
        "ratio": 3,
        "filter": "(total > 100)"
      }
    }
  ],
  "summary": "Found 3 performance issues: 3 warning(s)",
  "totalNodes": 2,
  "executionTime": 6.789
}
```

**Response Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `issues` | array | List of identified performance issues |
| `issues[].severity` | string | `"warning"` or `"info"` |
| `issues[].category` | string | Issue category (see detection rules table) |
| `issues[].nodeType` | string | Plan node type where the issue was found |
| `issues[].relation` | string | Table name (if applicable) |
| `issues[].description` | string | Human-readable description of the issue |
| `issues[].recommendation` | string | Actionable recommendation to resolve the issue |
| `issues[].details` | object | Additional details (varies by category) |
| `summary` | string | Human-readable summary (e.g., "Found 3 performance issues: 2 warning(s), 1 info") |
| `totalNodes` | int | Total number of plan nodes parsed |
| `executionTime` | float | Total execution time from the plan footer (ms), 0 if not present |

**Error Responses**:

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `INVALID_REQUEST` | Empty or missing `planText` field |
| 400 | `INVALID_REQUEST` | Plan text could not be parsed |
| 404 | `CLUSTER_NOT_FOUND` | Cluster does not exist |

#### 8.4.3 CLI Command

```bash
# Analyze a plan from a file
cloudberry-ctl queries plan-check --cluster my-cluster -f explain.txt

# Analyze a plan from stdin
cat explain.txt | cloudberry-ctl queries plan-check --cluster my-cluster -f -

# JSON output
cloudberry-ctl queries plan-check --cluster my-cluster -f explain.txt -o json
```

**Flags**:

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `-f`, `--file` | string | Yes | Path to EXPLAIN ANALYZE output file (use `-` for stdin) |
| `--cluster` | string | Yes | Target cluster name |
| `-o`, `--output` | string | No | Output format: `table` (default), `json`, `yaml` |

**Output (table)**:

```
SEVERITY  CATEGORY               NODE TYPE     RELATION  DESCRIPTION
warning   sequential_scan        Seq Scan      orders    Sequential scan on orders returned 50000 rows
warning   row_estimate_mismatch  Seq Scan      orders    Row estimate mismatch on orders: estimated 1000 rows, actual 50000 rows (49x off)
warning   excessive_filter_rows  Seq Scan      orders    Filter removed 3x more rows than returned (150000 removed vs 50000 returned)

Summary: Found 3 performance issues: 3 warning(s)
Total nodes: 2 | Execution time: 6.789 ms
```

#### 8.4.4 Prometheus Metrics

The following metrics are registered by the operator API server when plan check requests are processed:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_plan_check_total` | Counter | cluster, namespace | Total number of plan check operations performed |
| `cloudberry_plan_check_duration_seconds` | Histogram | cluster, namespace | Duration of plan check operations (buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s) |
| `cloudberry_plan_check_issues_total` | Counter | cluster, namespace, category, severity | Total number of issues detected, by category and severity |

#### 8.4.5 Example Input/Output

**Input** (saved as `explain.txt`):

```
Sort  (cost=15000.00..15250.00 rows=100000 width=44) (actual time=120.000..145.000 rows=100000 loops=1)
  Sort Key: created_at
  Sort Method: external merge  Disk: 8192kB
  ->  Nested Loop  (cost=0.00..12000.00 rows=100000 width=44) (actual time=0.050..80.000 rows=200000 loops=1)
        ->  Seq Scan on orders  (cost=0.00..5000.00 rows=100 width=36) (actual time=0.010..10.000 rows=50000 loops=1)
              Filter: (status = 'pending')
              Rows Removed by Filter: 950000
        ->  Index Scan using idx_items_order_id on items  (cost=0.00..70.00 rows=1000 width=8) (actual time=0.001..0.010 rows=4 loops=50000)
              Index Cond: (order_id = orders.id)
Execution Time: 150.000 ms
```

**Output**:

```json
{
  "issues": [
    {
      "severity": "warning",
      "category": "sort_spill",
      "nodeType": "Sort",
      "relation": "",
      "description": "Sort spilled to disk using 8192kB",
      "recommendation": "Increase work_mem (current sort used 8192kB on disk)",
      "details": { "sortMethod": "external merge", "sortSpaceUsed": 8192, "sortSpaceType": "Disk" }
    },
    {
      "severity": "info",
      "category": "high_cost_node",
      "nodeType": "Sort",
      "relation": "",
      "description": "High-cost node Sort (cost=15250.00)",
      "recommendation": "High-cost node (15250); review for optimization",
      "details": { "totalCost": 15250.00 }
    },
    {
      "severity": "warning",
      "category": "nested_loop_high_rows",
      "nodeType": "Nested Loop",
      "relation": "",
      "description": "Nested loop processed 200000 total rows (200000 rows x 1 loops)",
      "recommendation": "Consider Hash Join or Merge Join; nested loop processed 200000 rows",
      "details": { "actualRows": 200000, "actualLoops": 1, "totalRows": 200000 }
    },
    {
      "severity": "info",
      "category": "high_cost_node",
      "nodeType": "Nested Loop",
      "relation": "",
      "description": "High-cost node Nested Loop (cost=12000.00)",
      "recommendation": "High-cost node (12000); review for optimization",
      "details": { "totalCost": 12000.00 }
    },
    {
      "severity": "warning",
      "category": "sequential_scan",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Sequential scan on orders returned 50000 rows",
      "recommendation": "Consider creating an index on orders for filter condition (status = 'pending')",
      "details": { "actualRows": 50000, "filter": "(status = 'pending')", "totalCost": 5000.00 }
    },
    {
      "severity": "warning",
      "category": "row_estimate_mismatch",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Row estimate mismatch on orders: estimated 100 rows, actual 50000 rows (499x off)",
      "recommendation": "Run ANALYZE on the tables involved to update statistics",
      "details": { "planRows": 100, "actualRows": 50000, "ratio": 499.0 }
    },
    {
      "severity": "warning",
      "category": "excessive_filter_rows",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Filter removed 19x more rows than returned (950000 removed vs 50000 returned)",
      "recommendation": "Filter removed 19x more rows than returned; consider adding index on filter column",
      "details": { "rowsRemoved": 950000, "actualRows": 50000, "ratio": 19, "filter": "(status = 'pending')" }
    }
  ],
  "summary": "Found 7 performance issues: 5 warning(s), 2 info",
  "totalNodes": 4,
  "executionTime": 150.0
}
```

### 8.5 Guest Access

When `guestAccess: true` is set in the `queryMonitoring` CRD spec, the operator allows unauthenticated read-only access to specific endpoints. Guest access is designed for monitoring dashboards and status pages that need to display cluster health without requiring credentials.

#### 8.5.1 Guest Access Behavior

- **Guest identity**: Unauthenticated requests receive a guest identity with `PermissionBasic` and username `guest`.
- **Read-only methods only**: Guest access is restricted to `GET`, `HEAD`, and `OPTIONS` methods. All `POST`, `PUT`, and `DELETE` requests require authentication regardless of the `guestAccess` setting.
- **Per-cluster setting**: Guest access is evaluated per-cluster by looking up the `CloudberryCluster` CR and checking `spec.queryMonitoring.guestAccess`.
- **Authenticated requests take priority**: When an `Authorization` header is present, the normal authentication flow is used even if guest access is enabled.

#### 8.5.2 Guest-Enabled Endpoints

Only endpoints registered with `withGuestAuth` support guest access. Currently:

| Endpoint | Permission Required | Guest Access |
|----------|-------------------|--------------|
| `GET /clusters/{name}/queries/active` | Basic | Allowed (guest has Basic) |
| `GET /clusters/{name}/metrics/exporters` | Basic | Allowed (guest has Basic) |
| `GET /clusters/{name}/queries` | Operator Basic | Denied (guest has Basic, returns 403) |

All other endpoints use `withAuth` and return `401 Unauthorized` for unauthenticated requests.

#### 8.5.3 Permission Levels

The operator enforces a hierarchical permission model. Each endpoint requires a minimum permission level:

| Level | Value | Description |
|-------|-------|-------------|
| Self Only | 0 | View own queries and sessions |
| Basic | 1 | View cluster state, active queries, exporter health |
| Operator Basic | 2 | View all sessions, configurations, query history |
| Operator | 3 | Cancel queries, move queries, cluster operations |
| Admin | 4 | Full access — create/delete clusters, activate standby |

A user with a higher permission level can access all endpoints that require a lower level. For example, an `Operator` user can access `Basic` and `Operator Basic` endpoints.

#### 8.5.4 Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_guest_access_total` | Counter | cluster, namespace, result | Guest access attempts (result: `allowed` or `denied`) |

## 9. API Endpoints

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/queries | Operator Basic | List active queries |
| GET | /clusters/{name}/queries/active | Basic | Get active query count |
| GET | /clusters/{name}/queries/{qid} | Operator Basic | Get query details |
| POST | /clusters/{name}/queries/{qid}/cancel | Operator | Cancel query |
| POST | /clusters/{name}/queries/{qid}/move | Operator | Move query to resource group |
| GET | /clusters/{name}/queries/history | Operator Basic | Search query history |
| GET | /clusters/{name}/queries/history/{qid} | Operator Basic | Get historical query details |
| POST | /clusters/{name}/queries/history/export | Operator Basic | Export history to CSV |
| POST | /clusters/{name}/queries/plan-check | Basic | Run static plan checker |
| POST | /clusters/{name}/queries/export | Operator Basic | Export active queries to CSV |
| GET | /clusters/{name}/metrics/exporters | Basic | List exporter health status |

### 9.1 Query History API Details

#### GET /clusters/{name}/queries/history

Search query history with optional filters and pagination.

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `namespace` | string | No | Cluster namespace |
| `pattern` | string | No | Search pattern for query text |
| `patternType` | string | No | `regex` (default) or `wildcard` |
| `user` | string | No | Filter by username |
| `database` | string | No | Filter by database name |
| `resourceGroup` | string | No | Filter by resource group |
| `state` | string | No | Filter by state (`completed`, `cancelled`, `error`) |
| `minDuration` | float | No | Minimum duration in milliseconds |
| `since` | string | No | Start time — RFC3339 or Go duration (e.g. `24h`, `30m`) |
| `until` | string | No | End time — RFC3339 format |
| `limit` | int | No | Page size (default: 50, max: 100) |
| `offset` | int | No | Pagination offset (default: 0) |

**Response** (`200 OK`):

```json
{
  "items": [
    {
      "id": 42,
      "queryId": "q-1234-1716984000000000000",
      "pid": 1234,
      "username": "analyst",
      "databaseName": "warehouse",
      "queryText": "SELECT * FROM orders WHERE created_at > '2026-01-01'",
      "queryStart": "2026-05-29T10:00:00Z",
      "queryEnd": "2026-05-29T10:00:02.5Z",
      "durationMs": 2500.00,
      "state": "completed",
      "rowsAffected": 15000,
      "cpuTimeMs": 1800.50,
      "memoryBytes": 67108864,
      "spillBytes": 0,
      "diskReadBytes": 134217728,
      "diskWriteBytes": 0,
      "waitEvents": "",
      "resourceGroup": "default_group",
      "createdAt": "2026-05-29T10:00:02.5Z"
    }
  ],
  "total": 156,
  "limit": 50,
  "offset": 0
}
```

**Error Responses**:

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `INVALID_REQUEST` | Invalid filter parameter (bad regex, negative limit, etc.) |
| 404 | `CLUSTER_NOT_FOUND` | Cluster does not exist |
| 503 | `DB_UNAVAILABLE` | Cannot connect to coordinator database |

#### GET /clusters/{name}/queries/history/{qid}

Return detailed information for a specific historical query, including the EXPLAIN plan if collected.

**Path Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Cluster name |
| `qid` | string | Query ID (e.g. `q-1234-1716984000000000000`) |

**Response** (`200 OK`):

```json
{
  "id": 42,
  "queryId": "q-1234-1716984000000000000",
  "pid": 1234,
  "username": "analyst",
  "databaseName": "warehouse",
  "queryText": "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.total > 1000",
  "queryStart": "2026-05-29T10:00:00Z",
  "queryEnd": "2026-05-29T10:00:05.2Z",
  "durationMs": 5200.00,
  "state": "completed",
  "rowsAffected": 3200,
  "cpuTimeMs": 4100.25,
  "memoryBytes": 134217728,
  "spillBytes": 268435456,
  "diskReadBytes": 536870912,
  "diskWriteBytes": 134217728,
  "waitEvents": "IO",
  "resourceGroup": "analytics_group",
  "explainPlan": "Gather Motion 4:1  (slice1; segments: 4)\n  ->  Hash Join\n        Hash Cond: (o.customer_id = c.id)\n        ->  Seq Scan on orders o\n              Filter: (total > 1000)\n        ->  Hash\n              ->  Broadcast Motion 4:4  (slice2; segments: 4)\n                    ->  Seq Scan on customers c",
  "errorMessage": "",
  "createdAt": "2026-05-29T10:00:05.2Z"
}
```

**Error Responses**:

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `INVALID_REQUEST` | Query ID is empty |
| 404 | `QUERY_NOT_FOUND` | No history entry with the given query ID |
| 503 | `DB_NOT_AVAILABLE` | Database connection not configured |
| 503 | `DB_CONNECTION_FAILED` | Failed to connect to coordinator database |

#### POST /clusters/{name}/queries/history/export

Export query history as a CSV file. Accepts an optional JSON body to filter the exported data. All matching rows are exported (no pagination limits).

**Request Body** (optional, `Content-Type: application/json`):

```json
{
  "pattern": "SELECT.*FROM orders",
  "patternType": "regex",
  "user": "analyst",
  "database": "warehouse",
  "resourceGroup": "analytics_group",
  "state": "completed",
  "since": "24h",
  "until": "2026-05-29T23:59:59Z"
}
```

All fields are optional. When the body is empty or omitted, all history entries are exported.

**Response** (`200 OK`):

```
Content-Type: text/csv
Content-Disposition: attachment; filename="query-history.csv"

query_id,username,database,query_text,start_time,end_time,duration_ms,rows_affected,cpu_time_ms,memory_bytes,spill_bytes,state
q-1234-1716984000000000000,analyst,warehouse,"SELECT * FROM orders WHERE created_at > '2026-01-01'",2026-05-29T10:00:00Z,2026-05-29T10:00:02.5Z,2500.00,15000,1800.50,67108864,0,completed
```

**Error Responses**:

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `CLUSTER_NOT_FOUND` | Cluster does not exist |
| 503 | `DB_NOT_AVAILABLE` | Database connection not configured |
| 503 | `DB_CONNECTION_FAILED` | Failed to connect to coordinator database |

> **Note**: Once CSV streaming begins (HTTP 200 headers sent), errors during row iteration cannot be communicated via HTTP status codes. Partial CSV output may result if the database connection drops mid-export.

### 9.2 Query Cancel API Details

#### POST /clusters/{name}/queries/{pid}/cancel

Cancel a running query by PID. Executes `pg_cancel_backend()` on the coordinator database. The session remains connected but the current query is interrupted.

**Path Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Cluster name |
| `pid` | int | PostgreSQL backend process ID of the query to cancel |

**Request Body** (optional, `Content-Type: application/json`):

```json
{
  "reason": "Query running too long"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `reason` | string | No | Human-readable reason for cancellation (logged for audit) |

**Response** (`200 OK`):

```json
{
  "pid": 1234,
  "canceled": true,
  "reason": "Query running too long"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `pid` | int | The backend process ID that was targeted |
| `canceled` | bool | `true` if `pg_cancel_backend()` returned true; `false` if PID not found or query already completed |
| `reason` | string | The cancellation reason (echoed back when provided) |

**Prometheus Metric**: `cloudberry_query_cancel_total` counter is incremented on each successful cancel request.

**Error Responses**:

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `INVALID_REQUEST` | PID is zero, negative, or non-numeric |
| 404 | `CLUSTER_NOT_FOUND` | Cluster does not exist |
| 503 | `DB_UNAVAILABLE` | Cannot connect to coordinator database |
| 500 | `INTERNAL_ERROR` | `pg_cancel_backend()` execution failed |

### 9.3 Query Move API Details

#### POST /clusters/{name}/queries/{pid}/move

Move a running query to a different resource group. Executes `ALTER ROLE <user> RESOURCE GROUP <target_group>` on the coordinator to reassign the user's resource group, which affects the running query's resource allocation.

**Path Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Cluster name |
| `pid` | int | PostgreSQL backend process ID of the query to move |

**Request Body** (`Content-Type: application/json`):

```json
{
  "targetGroup": "etl"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `targetGroup` | string | Yes | Name of the target resource group to move the query into |

**Response** (`200 OK`):

```json
{
  "pid": 1234,
  "moved": true,
  "targetGroup": "etl",
  "previousGroup": "default_group"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `pid` | int | The backend process ID that was targeted |
| `moved` | bool | `true` if the resource group reassignment succeeded |
| `targetGroup` | string | The target resource group name |
| `previousGroup` | string | The resource group the query was previously in |

**Prometheus Metric**: `cloudberry_query_move_total` counter is incremented on each successful move request.

**Error Responses**:

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `INVALID_REQUEST` | PID is zero, negative, or non-numeric; or `targetGroup` is empty |
| 404 | `CLUSTER_NOT_FOUND` | Cluster does not exist |
| 404 | `RESOURCE_GROUP_NOT_FOUND` | Target resource group does not exist |
| 503 | `DB_UNAVAILABLE` | Cannot connect to coordinator database |
| 500 | `INTERNAL_ERROR` | Resource group reassignment failed |

### 9.4 Exporter Health API Details

#### GET /clusters/{name}/metrics/exporters

List the health status of all Prometheus exporters deployed for the cluster. This endpoint checks the `/metrics` endpoint of each exporter sidecar and reports their availability, last scrape time, and error state.

**Path Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Cluster name |

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `namespace` | string | No | Cluster namespace |

**Response** (`200 OK`):

```json
{
  "exporters": [
    {
      "name": "postgres-exporter",
      "type": "postgres_exporter",
      "port": 9187,
      "healthy": true,
      "lastScrape": "2026-05-30T10:00:15Z",
      "scrapeInterval": "15s",
      "metricsCount": 142,
      "errorMessage": ""
    },
    {
      "name": "cloudberry-query-exporter",
      "type": "cloudberry_query_exporter",
      "port": 9188,
      "healthy": true,
      "lastScrape": "2026-05-30T10:00:15Z",
      "scrapeInterval": "15s",
      "metricsCount": 87,
      "errorMessage": ""
    },
    {
      "name": "node-exporter",
      "type": "node_exporter",
      "port": 9100,
      "healthy": true,
      "lastScrape": "2026-05-30T10:00:15Z",
      "scrapeInterval": "15s",
      "metricsCount": 256,
      "errorMessage": ""
    }
  ],
  "total": 3,
  "healthyCount": 3
}
```

| Field | Type | Description |
|-------|------|-------------|
| `exporters` | array | List of exporter health entries |
| `exporters[].name` | string | Exporter container name |
| `exporters[].type` | string | Exporter type identifier |
| `exporters[].port` | int | Metrics port number |
| `exporters[].healthy` | bool | `true` if the exporter's `/metrics` endpoint is reachable |
| `exporters[].lastScrape` | string | ISO 8601 timestamp of the last successful scrape |
| `exporters[].scrapeInterval` | string | Configured scrape interval |
| `exporters[].metricsCount` | int | Number of metric families exposed |
| `exporters[].errorMessage` | string | Error message if the exporter is unhealthy (empty when healthy) |
| `total` | int | Total number of exporters |
| `healthyCount` | int | Number of healthy exporters |

**Prometheus Metric**: `cloudberry_exporter_health_check_total` counter is incremented on each health check request.

**Error Responses**:

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `CLUSTER_NOT_FOUND` | Cluster does not exist |
| 500 | `INTERNAL_ERROR` | Failed to check exporter health |

## 10. CLI Commands

```bash
cloudberry-ctl queries list --cluster my-cluster
cloudberry-ctl queries list --cluster my-cluster --status running
cloudberry-ctl queries detail --cluster my-cluster --query-id 12345
cloudberry-ctl queries cancel --cluster my-cluster --query-id 12345 --reason "Too long"
cloudberry-ctl queries move --cluster my-cluster --query-id 12345 --target-group etl
cloudberry-ctl queries export --cluster my-cluster --format csv
cloudberry-ctl queries export --cluster my-cluster --format csv -O active-queries.csv
cloudberry-ctl queries history list --cluster my-cluster --last 24h
cloudberry-ctl queries history list --cluster my-cluster --user analyst --database mydb
cloudberry-ctl queries history list --cluster my-cluster --pattern "SELECT.*FROM orders" --pattern-type regex
cloudberry-ctl queries history list --cluster my-cluster --pattern "SELECT * FROM users*" --pattern-type wildcard
cloudberry-ctl queries history list --cluster my-cluster --state completed --min-duration 5000
cloudberry-ctl queries history --cluster my-cluster --last 24h --export csv
cloudberry-ctl queries history detail --cluster my-cluster --query-id q-1234-1716984000000000000
cloudberry-ctl queries history export --cluster my-cluster -O queries.csv
cloudberry-ctl queries history export --cluster my-cluster --last 24h --user analyst -O filtered.csv
cloudberry-ctl queries plan-check --cluster my-cluster -f explain.txt
```

## 11. Session Management (Scenario 59 — Live Query Monitor)

### 11.1 Active Sessions

The operator exposes session listing and management:

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/sessions | Operator Basic | List active sessions |
| POST | /clusters/{name}/sessions/{pid}/cancel | Operator | Cancel session query |
| DELETE | /clusters/{name}/sessions/{pid} | Operator | Terminate session |

### 11.2 Session Filtering (59a + 59d)

The `GET /sessions` endpoint supports query parameters for filtering:

| Parameter | Values | Description |
|-----------|--------|-------------|
| `status` | `running`, `queued`, `blocked`, `idle` | Filter by session state |
| `database` | database name | Filter by database |
| `user` | username | Filter by user |
| `resource_group` | group name | Filter by resource group |
| `since` | Go duration (e.g. `5m`, `1h`) | Sessions started within this window |

Filters are AND-combined. When no filters are provided, all sessions are returned.

### 11.3 Cancel with Reason (59b)

The `POST /sessions/{pid}/cancel` endpoint accepts an optional JSON body:

```json
{"reason": "Too long running query"}
```

When provided, the reason is included in the response and logged for audit purposes.

### 11.4 Resource Group Reassignment (59c)

Use `POST /clusters/{name}/workload/resource-groups/{group}/assign` with body `{"role": "username"}` to reassign a user's resource group, affecting all their running queries.

### 11.5 Query Details (Scenario 60)

The `GET /clusters/{name}/queries/{pid}` endpoint returns detailed execution information for a specific query:

```json
{
  "pid": 1234,
  "username": "analyst",
  "database": "mydb",
  "state": "active",
  "query": "SELECT * FROM large_table JOIN dim_table ON ...",
  "queryStart": "2026-05-27T12:00:00Z",
  "duration": "00:00:30",
  "waitEventType": "",
  "waitEvent": "",
  "backendType": "client backend",
  "locks": [
    {"lockType": "relation", "mode": "AccessShareLock", "granted": true, "relation": "large_table"},
    {"lockType": "relation", "mode": "AccessShareLock", "granted": true, "relation": "dim_table"}
  ],
  "tablesAccessed": ["public.large_table", "public.dim_table"]
}
```

The response includes:
- **Execution metrics**: PID, state, query text, duration, backend type, wait events
- **Lock information**: All locks held or awaited by the query (type, mode, granted status, relation)
- **Tables accessed**: List of tables with recent activity in the query's database

Permission: `OperatorBasic` (read-only access to query details).

### 11.2 CSV Export

Query monitor and history results can be exported to CSV:

```bash
cloudberry-ctl queries export --cluster my-cluster --format csv -o queries.csv
cloudberry-ctl queries history --cluster my-cluster --last 24h --export csv
```

### 11.3 Pause/Resume Monitor

The query monitor supports pause/resume to snapshot a moment in time. While paused, a "stale data" indicator is shown and no new data is fetched.

#### API Endpoints

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `POST` | `/clusters/{name}/queries/monitor/pause` | Operator | Pause the query monitor |
| `POST` | `/clusters/{name}/queries/monitor/resume` | Operator | Resume the query monitor |
| `GET` | `/clusters/{name}/queries/monitor/state` | Basic | Get current monitor state |

#### Pause Request

```
POST /api/v1alpha1/clusters/{name}/queries/monitor/pause?namespace={ns}
Authorization: Basic <credentials>
```

**Response (200 OK):**

```json
{
  "status": "paused",
  "pausedAt": "2026-05-30T10:00:00Z",
  "message": "Query monitor paused"
}
```

When already paused, returns:

```json
{
  "status": "paused",
  "pausedAt": "2026-05-30T10:00:00Z",
  "message": "Query monitor is already paused"
}
```

#### Resume Request

```
POST /api/v1alpha1/clusters/{name}/queries/monitor/resume?namespace={ns}
Authorization: Basic <credentials>
```

**Response (200 OK):**

```json
{
  "status": "resumed",
  "message": "Query monitor resumed"
}
```

Resuming when not paused succeeds idempotently.

#### Get Monitor State

```
GET /api/v1alpha1/clusters/{name}/queries/monitor/state?namespace={ns}
Authorization: Basic <credentials>
```

**Response (200 OK) — not paused:**

```json
{
  "paused": false,
  "stale": false
}
```

**Response (200 OK) — paused:**

```json
{
  "paused": true,
  "stale": true,
  "pausedAt": "2026-05-30T10:00:00Z"
}
```

#### Behavior

- **Snapshot**: When paused, the operator takes a snapshot of the current query data (activeQueries, queuedQueries, blockedQueries, config). Subsequent requests to `/queries` and `/queries/active` return the cached snapshot.
- **Stale flag**: While paused, all query monitoring responses include `"stale": true` and `"pausedAt": "<RFC3339 timestamp>"` to indicate the data is not live.
- **Resume**: Removes the cached snapshot. Subsequent requests return fresh data from the cluster status without stale indicators.
- **Idempotent**: Pausing when already paused returns success with "already paused" message. Resuming when not paused returns success.

#### Permission Levels

| Operation | Required Permission |
|-----------|-------------------|
| Pause monitor | Operator |
| Resume monitor | Operator |
| Get monitor state | Basic |

#### CLI Commands

```bash
# Pause the query monitor
cloudberry-ctl queries monitor pause --cluster my-cluster

# Resume the query monitor
cloudberry-ctl queries monitor resume --cluster my-cluster

# Check monitor state
cloudberry-ctl queries monitor state --cluster my-cluster
```

#### Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_monitor_pause_total` | counter | `cluster`, `namespace` | Total monitor pause events |
| `cloudberry_monitor_resume_total` | counter | `cluster`, `namespace` | Total monitor resume events |

### 11.5 Exporter Health Monitoring

The `GET /clusters/{name}/metrics/exporters` endpoint provides a unified view of all Prometheus exporter sidecars deployed for a cluster. This enables operators to quickly verify that metric collection is functioning correctly without manually checking individual exporter pods.

**Use cases**:

- **Pre-flight check**: Verify all exporters are healthy before investigating metric gaps
- **Alerting integration**: Build alerts on `cloudberry_exporter_health_check_total` to detect exporter failures
- **Dashboard integration**: Display exporter health status in the Grafana "Query Operations" row

**Exporter types checked**:

| Exporter | Container Name | Port | Source |
|----------|---------------|------|--------|
| postgres_exporter | `postgres-exporter` | 9187 | Coordinator sidecar |
| cloudberry_query_exporter | `cloudberry-query-exporter` | 9188 | Coordinator sidecar |
| node_exporter | `node-exporter` | 9100 | Segment host DaemonSet |

**Health check mechanism**: The operator performs an HTTP GET to each exporter's `/metrics` endpoint with a 5-second timeout. An exporter is considered healthy if the endpoint returns HTTP 200 and the response body contains at least one valid Prometheus metric line.

**CLI command**:

```bash
cloudberry-ctl metrics exporters --cluster my-cluster
```

**Output (table)**:

```
NAME                         TYPE                        PORT  HEALTHY  LAST SCRAPE           METRICS
postgres-exporter            postgres_exporter           9187  true     2026-05-30T10:00:15Z  142
cloudberry-query-exporter    cloudberry_query_exporter   9188  true     2026-05-30T10:00:15Z  87
node-exporter                node_exporter               9100  true     2026-05-30T10:00:15Z  256
```

## 12. RBAC for Exporters

The operator creates a dedicated PostgreSQL role for exporters:

```sql
CREATE ROLE cloudberry_exporter WITH LOGIN PASSWORD '...' IN ROLE pg_monitor;
GRANT SELECT ON gp_segment_configuration TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_resgroup_status TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_resgroup_status_per_host TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_resgroup_iostats_per_host TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_resgroup_config TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_workfile_usage_per_query TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_workfile_usage_per_segment TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_skew_coefficients TO cloudberry_exporter;
```

The password is stored in a Kubernetes Secret `<cluster>-exporter-credentials` and mounted as the `DATA_SOURCE_NAME` environment variable.

## 13. cloudberry-query-exporter Component

The `cloudberry-query-exporter` is a Go application maintained as part of the operator project at `cmd/cloudberry-query-exporter/`. It is built as a separate Docker image (`Dockerfile.cloudberry-query-exporter`) and deployed as a sidecar container on the coordinator pod.

See `specifications/14-exporter-deployment-spec.md` for full details on the exporter deployment architecture, retry mechanism, and CI integration.

## 14. Implementation Notes

### 14.1 Resource Creation Order

The exporter credentials Secret and queries ConfigMap are created by the **cluster-controller** before the coordinator StatefulSet, ensuring sidecar containers have access to the DSN and queries on first start. The Secret and ConfigMap volume references use `optional: true` as a safety net.

### 14.2 DB Role Retry Mechanism

The exporter DB role setup uses a 10-second context timeout and retries on every admin-controller reconcile cycle (every ~10 seconds) until the role is created. Success is tracked via the `avsoft.io/exporter-role-ready` annotation.

### 14.3 Node Exporter

The node-exporter is deployed via a dedicated Helm chart at `test/monitoring/node-exporter/`, not managed by the operator. This avoids coupling the operator to host-level DaemonSet management.

### 14.4 ServiceMonitor and PrometheusRule

The operator does not create ServiceMonitor or PrometheusRule resources. Metric discovery relies on `prometheus.io/*` pod annotations, which are compatible with vmagent, Prometheus, and other scrape-based collectors without requiring the Prometheus Operator CRDs.
