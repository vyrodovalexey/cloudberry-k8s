# Cloudberry Operator — Exporter Deployment Specification

**Version**: 1.1.0
**API Group**: avsoft.io

---

## 1. Overview

The operator deploys Prometheus metric exporters alongside Cloudberry Database clusters to enable observability via VictoriaMetrics or any Prometheus-compatible backend. Three exporter types are supported:

| Exporter | Deployment | Port | Purpose |
|----------|-----------|------|---------|
| postgres-exporter | Coordinator sidecar | 9187 | PostgreSQL/Cloudberry catalog metrics |
| cloudberry-query-exporter | Coordinator sidecar | 9188 | Query activity, resource groups, slow queries |
| node-exporter | Helm chart (DaemonSet) | 9100 | Host-level CPU, memory, disk, network |

## 2. Component: cloudberry-query-exporter

A lightweight Go application maintained as part of the operator project at `cmd/cloudberry-query-exporter/`.

### 2.1 Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--listen-address` | `:9188` | HTTP listen address for metrics |
| `--sampling-interval` | `5s` | Interval between metric collection cycles |
| `--slow-query-threshold` | `1000ms` | Duration threshold for slow query classification |

### 2.2 Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATA_SOURCE_NAME` | No (optional) | PostgreSQL connection DSN. When empty, exporter starts in degraded mode (`up=0`) and retries reading the env var on each collection cycle. |

### 2.3 Metrics Exported

#### Core Metrics
| Metric | Type | Description |
|--------|------|-------------|
| `cloudberry_active_queries` | Gauge | Currently running queries |
| `cloudberry_idle_sessions` | Gauge | Idle database sessions |
| `cloudberry_slow_queries` | Gauge | Queries exceeding the slow threshold |
| `cloudberry_total_connections` | Gauge | Total database connections |
| `cloudberry_query_exporter_up` | Gauge | 1 if DB connection works, 0 otherwise |

#### Query Activity (57a)
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_queries_idle_in_transaction` | Gauge | — | Sessions in idle-in-transaction state |
| `cloudberry_queries_blocked` | Gauge | — | Queries blocked by locks |
| `cloudberry_queries_total` | Counter | datname, usename, state | Total queries by state |
| `cloudberry_queries_slow_total` | Counter | datname, usename | Slow queries exceeding threshold |
| `cloudberry_query_duration_seconds` | Histogram | — | Query duration distribution |
| `cloudberry_query_max_duration_seconds` | Gauge | — | Longest running query duration |
| `cloudberry_queries_canceled_total` | Counter | reason | Canceled queries by reason |

#### Resource Groups (57b)
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_resgroup_running_queries` | Gauge | rsgname | Running queries per group |
| `cloudberry_resgroup_queued_queries` | Gauge | rsgname | Queued queries per group |
| `cloudberry_resgroup_executed_total` | Counter | rsgname | Total executed queries |
| `cloudberry_resgroup_queue_duration_seconds_total` | Counter | rsgname | Total queue wait time |
| `cloudberry_resgroup_cpu_usage_percent` | Gauge | rsgname, hostname | CPU usage per host |
| `cloudberry_resgroup_memory_usage_bytes` | Gauge | rsgname, hostname | Memory used |
| `cloudberry_resgroup_memory_available_bytes` | Gauge | rsgname, hostname | Memory available |
| `cloudberry_resgroup_memory_quota_used_bytes` | Gauge | rsgname, hostname | Quota memory used |
| `cloudberry_resgroup_memory_shared_used_bytes` | Gauge | rsgname, hostname | Shared memory used |

#### Resource Group I/O (57c)
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_resgroup_io_read_bytes_per_sec` | Gauge | rsgname, hostname, tablespace | Read throughput |
| `cloudberry_resgroup_io_write_bytes_per_sec` | Gauge | rsgname, hostname, tablespace | Write throughput |
| `cloudberry_resgroup_io_read_ops_per_sec` | Gauge | rsgname, hostname, tablespace | Read IOPS |
| `cloudberry_resgroup_io_write_ops_per_sec` | Gauge | rsgname, hostname, tablespace | Write IOPS |

#### Spill Files (57d)
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_spill_files_active` | Gauge | — | Active spill files |
| `cloudberry_spill_files_bytes` | Gauge | — | Total spill file bytes |
| `cloudberry_spill_files_per_segment` | Gauge | segment_id, hostname | Spill bytes per segment |
| `cloudberry_spill_files_per_query` | Gauge | datname, pid | Spill bytes per query |

#### Segment Health (57e)
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_segments_total` | Gauge | role | Segments by role (primary/mirror) |
| `cloudberry_segments_up` | Gauge | — | Segments with status=u |
| `cloudberry_segments_down` | Gauge | — | Segments with status=d |
| `cloudberry_segments_not_synced` | Gauge | — | Segments with mode=n |
| `cloudberry_segments_not_preferred_role` | Gauge | — | Segments not in preferred role |
| `cloudberry_cluster_uptime_seconds` | Gauge | — | Cluster uptime |

#### Distributed Transactions (57f)
| Metric | Type | Description |
|--------|------|-------------|
| `cloudberry_distributed_transactions_active` | Gauge | Active distributed transactions |
| `cloudberry_distributed_transactions_committed_total` | Counter | Committed transactions |
| `cloudberry_distributed_transactions_aborted_total` | Counter | Aborted transactions |
| `cloudberry_oldest_transaction_age_seconds` | Gauge | Age of oldest open transaction |

#### Data Distribution (57g)
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_table_skew_coefficient` | Gauge | schemaname, tablename | Data skew coefficient (0=even, 1=all on one segment) |
| `cloudberry_query_exporter_scrape_duration_seconds` | Histogram | Duration of metric collection |

### 2.4 Resilience

- Starts without a DSN (serves `/health` and `/metrics` with `up=0`)
- Reconnects with exponential backoff (1s initial, 30s max, 10% jitter)
- Never crashes on DB unavailability
- Graceful shutdown on SIGTERM/SIGINT

### 2.5 Docker Image

- Dockerfile: `Dockerfile.cloudberry-query-exporter`
- Base: `gcr.io/distroless/static-debian12:nonroot`
- Image tag: `cloudberry-query-exporter:1.0.0`
- User: `65532:65532` (non-root)

## 3. Operator Reconciliation Flow

### 3.1 Cluster Controller (before StatefulSet creation)

When `queryMonitoring.enabled=true` with `exporters` configured:

1. **Create exporter credentials Secret** (`{cluster}-exporter-credentials`) with `password` and `dsn` keys. The DSN password is URL-encoded to handle special characters.
2. **Create exporter queries ConfigMap** (`{cluster}-exporter-queries`) with `queries.yaml` for postgres-exporter custom queries.

These are created before the coordinator StatefulSet so sidecar containers can reference them via env vars and volume mounts.

### 3.2 Coordinator StatefulSet Sidecar Injection

The `BuildCoordinatorStatefulSet` method injects sidecar containers when exporters are enabled:

- Appends `postgres-exporter` and/or `cloudberry-query-exporter` containers
- Appends `exporter-queries` volume (optional ConfigMap)
- Adds `prometheus.io/scrape=true`, `prometheus.io/port=9187`, `prometheus.io/path=/metrics` annotations to the pod template

Secret and ConfigMap references use `optional: true` so the pod can start before the admin-controller creates them.

### 3.3 Admin Controller (after cluster reaches Running)

1. **Ensure exporter credentials Secret** — create or update
2. **Ensure exporter queries ConfigMap** — create or update
3. **Ensure exporter Service** (`{cluster}-exporter-metrics`) — ClusterIP service exposing ports 9187 and 9188
4. **Setup exporter DB role** — create `cloudberry_exporter` role with LOGIN, pg_monitor membership, and SELECT grants on 8 monitoring views. Uses a 10-second context timeout. On failure, logs a warning and retries on the next reconcile cycle.
5. **Track role readiness** — sets `avsoft.io/exporter-role-ready=true` annotation on success. The admin-controller requeues every 10 seconds until this annotation is set.

### 3.4 Retry Mechanism

The admin-controller skips full reconciliation when `ObservedGeneration == Generation` UNLESS the exporter role is not ready. This ensures:

- Normal reconciliation is skipped when nothing changed (efficient)
- DB role setup retries every ~10 seconds until the DB accepts connections
- Once the role is created, the annotation prevents further retries

## 4. Exporter RBAC

```sql
CREATE ROLE cloudberry_exporter WITH LOGIN PASSWORD '...';
GRANT pg_monitor TO cloudberry_exporter;
GRANT SELECT ON gp_segment_configuration TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_resgroup_status TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_resgroup_status_per_host TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_resgroup_iostats_per_host TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_resgroup_config TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_workfile_usage_per_query TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_workfile_usage_per_segment TO cloudberry_exporter;
GRANT SELECT ON gp_toolkit.gp_skew_coefficients TO cloudberry_exporter;
```

Password stored in Secret `{cluster}-exporter-credentials`. Both exporters use this Secret for `DATA_SOURCE_NAME`.

## 5. Custom Queries (postgres_exporter)

The operator generates a `queries.yaml` ConfigMap with 10 custom query definitions:

| Query Name | Labels | Metrics | Source |
|---|---|---|---|
| `cloudberry_segment_status` | hostname, role, preferred_role, status, mode | count (GAUGE) | `gp_segment_configuration` |
| `cloudberry_segments_down` | — | down_count (GAUGE) | `gp_segment_configuration` |
| `cloudberry_replication` | client_addr, application_name, state | write_lag_bytes, flush_lag_bytes, replay_lag_bytes (GAUGE) | `pg_stat_replication` |
| `cloudberry_connections` | datname, usename, state, application_name | count (GAUGE) | `pg_stat_activity` |
| `cloudberry_connections_max` | — | max_connections (GAUGE) | `pg_settings` |
| `cloudberry_resgroup_status` | rsgname | running_queries, queued_queries (GAUGE), executed_total, queue_duration_seconds_total (COUNTER) | `gp_toolkit.gp_resgroup_status` |
| `cloudberry_database_stats` | datname | 14 metrics: numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, conflicts, temp_files, temp_bytes, deadlocks | `pg_stat_database` |
| `cloudberry_locks` | mode, locktype, state | count (GAUGE) | `pg_locks` |
| `cloudberry_table_stats` | schemaname, relname | seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del (COUNTER), n_live_tup, n_dead_tup (GAUGE) | `pg_stat_user_tables` (LIMIT 100) |
| `cloudberry_wal` | — | wal_bytes_total (COUNTER) | `pg_current_wal_lsn()` |

## 6. Node Exporter

Deployed via a dedicated Helm chart at `test/monitoring/node-exporter/` (not managed by the operator). The chart creates a DaemonSet with:

- `hostPID: true`, `hostNetwork: true`
- Args: `--path.rootfs=/host`, `--web.listen-address=:9100`, `--collector.filesystem.mount-points-exclude=...`
- `hostPort: 9100`
- Volume: rootfs hostPath `/` mounted read-only at `/host`
- Prometheus scrape annotations for vmagent discovery

### 6.1 Required Metric Families (Scenario 58)

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 1 | `node_cpu_seconds_total` | Counter | CPU time by mode (user, system, idle, iowait, irq, nice, softirq, steal) |
| 2 | `node_memory_MemTotal_bytes` | Gauge | Total physical memory |
| 3 | `node_memory_MemAvailable_bytes` | Gauge | Available memory |
| 4 | `node_disk_read_bytes_total` | Counter | Disk read bytes by device |
| 5 | `node_disk_written_bytes_total` | Counter | Disk written bytes by device |
| 6 | `node_disk_io_time_seconds_total` | Counter | Disk I/O time by device |
| 7 | `node_filesystem_avail_bytes` | Gauge | Available filesystem space |
| 8 | `node_filesystem_size_bytes` | Gauge | Total filesystem size |
| 9 | `node_network_receive_bytes_total` | Counter | Network received bytes by interface |
| 10 | `node_network_transmit_bytes_total` | Counter | Network transmitted bytes by interface |
| 11 | `node_load1` / `node_load5` / `node_load15` | Gauge | System load averages |

## 6. Metrics Pipeline

```
postgres-exporter (9187) ──┐
                           ├── vmagent (scrape via annotations) ──> VictoriaMetrics
cloudberry-query-exporter (9188) ──┤
                           │
node-exporter (9100) ──────┘
```

The vmagent discovers pods via `kubernetes_sd_configs` with `prometheus.io/scrape=true` annotation filtering.

## 7. Build and CI

### 7.1 Makefile Targets

| Target | Description |
|--------|-------------|
| `docker-build-query-exporter` | Build cloudberry-query-exporter Docker image |
| `docker-build-all` | Build all images (operator, ctl, cloudberry, query-exporter) |
| `monitoring-deploy` | Deploy vmagent + otel-collector + node-exporter to k8s |

### 7.2 GitHub Actions

The `cloudberry-query-exporter` image is included in:
- `docker-build` job (PR: build without push)
- `docker-build-push` job (tag: build, push, sign)
- `trivy-scan` job (tag: vulnerability scanning)
