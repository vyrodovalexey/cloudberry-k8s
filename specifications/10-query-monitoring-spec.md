# Cloudberry Operator - Query Monitoring & Analysis Specification

**Version**: 2.0.0
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
| guestAccess | bool | Allow unauthenticated read-only access | false |
| planCollection | bool | Collect visual query plans | true |
| slowQueryThreshold | string | Threshold for slow query logging | "1000ms" |

### 2.3 Status Fields

Added to CloudberryClusterStatus:

- `activeQueries` (int32) — Currently running queries
- `queuedQueries` (int32) — Queries waiting in queue
- `blockedQueries` (int32) — Queries blocked by locks
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
### 8.4 Plan Analysis

- Static plan checker for EXPLAIN ANALYZE output
- Performance issue identification
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
| GET | /clusters/{name}/metrics/exporters | Basic | List exporter health status |

## 10. CLI Commands

```bash
cloudberry-ctl queries list --cluster my-cluster
cloudberry-ctl queries list --cluster my-cluster --status running
cloudberry-ctl queries detail --cluster my-cluster --query-id 12345
cloudberry-ctl queries cancel --cluster my-cluster --query-id 12345 --reason "Too long"
cloudberry-ctl queries move --cluster my-cluster --query-id 12345 --target-group etl
cloudberry-ctl queries history --cluster my-cluster --last 24h
cloudberry-ctl queries history --cluster my-cluster --user analyst --database mydb
cloudberry-ctl queries plan-check --cluster my-cluster -f explain.txt
```

## 11. Session Management

### 11.1 Active Sessions

The operator exposes session listing and management:

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/sessions | Operator Basic | List active sessions |
| POST | /clusters/{name}/sessions/{pid}/cancel | Operator | Cancel session query |
| POST | /clusters/{name}/sessions/{pid}/terminate | Operator | Terminate session |

### 11.2 CSV Export

Query monitor and history results can be exported to CSV:

```bash
cloudberry-ctl queries export --cluster my-cluster --format csv -o queries.csv
cloudberry-ctl queries history --cluster my-cluster --last 24h --export csv
```

### 11.3 Pause/Resume Monitor

The query monitor supports pause/resume to snapshot a moment in time. While paused, a "stale data" indicator is shown and no new data is fetched.

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
