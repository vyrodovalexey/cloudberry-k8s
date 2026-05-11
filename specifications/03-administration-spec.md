# Cloudberry Operator - Administration Specification

**Version**: 1.0.0

---

## 1. Overview

The Administration specification covers the operator's capabilities for managing the Cloudberry cluster lifecycle, configuration, monitoring, and maintenance operations. These capabilities are exposed through the CloudberryCluster CRD and the cloudberry-ctl CLI.

## 2. Cluster Lifecycle Operations

### 2.1 Initialize Cluster

**Trigger**: CloudberryCluster CR creation

**Process**:
1. Validate CR spec (webhook validation)
2. Create namespace-scoped RBAC resources
3. Create ConfigMaps for postgresql.conf, pg_hba.conf
4. Create Secrets for admin password, monitoring role
5. Create headless Services (coordinator, standby, segments)
6. Create coordinator StatefulSet with init container
7. Wait for coordinator to be ready
8. If standby enabled, create standby StatefulSet
9. Create segment StatefulSets (primaries + mirrors)
10. Run cluster initialization (gpinitsystem equivalent)
11. Apply configuration parameters
12. Update CR status to `Running`

**Init Container Responsibilities**:
- Initialize data directory if empty
- Set up coordinator catalog
- Create monitoring database and role
- Apply initial pg_hba.conf

**Status Transitions**: `Pending` -> `Initializing` -> `Running`

### 2.2 Start Cluster

**Trigger**: CR annotation `avsoft.io/action: start`

**Modes**:
- **Normal**: Start all coordinator and segment processes
- **Restricted**: Start with `superuser_reserved_connections` only (annotation value: `start-restricted`)
- **Maintenance**: Start coordinator only in utility mode (annotation value: `start-maintenance`)

**Process**:
1. Scale StatefulSets to desired replicas
2. Wait for pods to be ready
3. Verify database connectivity
4. Update status

### 2.3 Stop Cluster

**Trigger**: CR annotation `avsoft.io/action: stop`

**Modes**:
- **Smart** (default): Wait for clients to disconnect
- **Fast**: Rollback active transactions, disconnect clients
- **Immediate**: Abort all connections immediately

**Process**:
1. Execute stop command on coordinator
2. Scale StatefulSets to 0
3. Update status to `Stopped`

### 2.4 Restart Cluster

**Trigger**: CR annotation `avsoft.io/action: restart`

**Process**:
1. Stop cluster (fast mode)
2. Start cluster (normal mode)
3. Update status

### 2.5 Upgrade Cluster

**Trigger**: Change to `spec.version` or `spec.image`

**Process**:
1. Pre-flight checks (cluster healthy, disk space, replication lag)
2. Update mirror StatefulSets first (rolling)
3. Update primary segment StatefulSets (rolling)
4. Update standby coordinator
5. Update coordinator
6. Run version-specific upgrade hooks
7. Verify cluster health
8. Update status

**Rollback**: If any step fails health check, revert to previous image

### 2.6 Remove Cluster

**Trigger**: CloudberryCluster CR deletion

**Process**:
1. Finalizer blocks immediate deletion
2. If `backupOnDelete: true`, trigger backup
3. Stop cluster gracefully
4. Delete StatefulSets, Services, ConfigMaps
5. If `deletionPolicy: Delete`, delete PVCs
6. If `deletionPolicy: Retain`, leave PVCs
7. Remove finalizer
8. CR is garbage collected

## 3. Configuration Management

### 3.1 Cluster-Wide Parameters

**Source**: `spec.config.parameters`

**Process**:
1. Render parameters into ConfigMap `{cluster}-postgresql-conf`
2. Mount ConfigMap into coordinator and segment pods
3. Determine if parameter requires restart
4. If reload-safe: signal running processes (pg_reload_conf)
5. If restart-required: orchestrate rolling restart
6. Update status condition `ConfigApplied`

**Supported Parameter Categories**:
- Connection and authentication (max_connections, listen_addresses, port)
- Memory (shared_buffers, work_mem, maintenance_work_mem, statement_mem, gp_vmem_protect_limit)
- Query tuning (optimizer, enable_seqscan, enable_indexscan, enable_hashjoin, default_statistics_target)
- WAL (wal_level, max_wal_size, checkpoint_completion_target)
- Replication (max_wal_senders, wal_keep_size, synchronous_commit)
- Logging (log_min_messages, log_min_duration_statement, log_connections, log_disconnections)
- Interconnect (gp_interconnect_type, gp_max_packet_size)
- Greenplum-specific (gp_enable_global_deadlock_detector, gp_autostats_mode)

### 3.2 Coordinator-Only Parameters

**Source**: `spec.config.coordinatorParameters`

Applied only to the coordinator instance, not segments.

### 3.3 Per-Database Parameters

**Source**: `spec.config.databaseParameters`

```yaml
databaseParameters:
  mydb:
    work_mem: "256MB"
    default_statistics_target: "150"
```

Applied via `ALTER DATABASE ... SET ...`

### 3.4 Per-Role Parameters

**Source**: `spec.config.roleParameters`

```yaml
roleParameters:
  analyst:
    work_mem: "512MB"
    statement_mem: "1GB"
```

Applied via `ALTER ROLE ... SET ...`

### 3.5 Configuration Reload

**Trigger**: Change to `spec.config` without restart-required parameters

**Process**:
1. Update ConfigMap
2. Execute `SELECT pg_reload_conf()` on coordinator
3. Verify parameter values via `SHOW`
4. Update status

## 4. Monitoring Operations

### 4.1 Cluster State Inspection

The operator continuously monitors and reports:

| Metric | Source | Status Field |
|--------|--------|-------------|
| Cluster phase | Reconciliation state | `status.phase` |
| Coordinator readiness | Pod health + DB connectivity | `status.coordinatorReady` |
| Standby readiness | Pod health + replication lag | `status.standbyReady` |
| Segment readiness | Pod health + segment status | `status.segmentsReady` |
| Mirroring status | gp_segment_configuration | `status.mirroringStatus` |
| Failed segments | gp_segment_configuration | `status.failedSegments` |

### 4.2 Prometheus Metrics

| Metric Name | Type | Description |
|------------|------|-------------|
| `cloudberry_cluster_info` | Gauge | Cluster metadata (version, segments, phase) |
| `cloudberry_coordinator_up` | Gauge | Coordinator availability (0/1) |
| `cloudberry_standby_up` | Gauge | Standby availability (0/1) |
| `cloudberry_segments_ready` | Gauge | Number of ready segments |
| `cloudberry_segments_total` | Gauge | Total number of segments |
| `cloudberry_segments_failed` | Gauge | Number of failed segments |
| `cloudberry_mirroring_in_sync` | Gauge | Mirroring sync status (0/1) |
| `cloudberry_reconcile_total` | Counter | Total reconciliation count |
| `cloudberry_reconcile_errors_total` | Counter | Reconciliation error count |
| `cloudberry_reconcile_duration_seconds` | Histogram | Reconciliation duration |
| `cloudberry_config_reload_total` | Counter | Configuration reload count |
| `cloudberry_connections_active` | Gauge | Active database connections |
| `cloudberry_connections_max` | Gauge | Maximum allowed connections |
| `cloudberry_disk_usage_bytes` | Gauge | Disk usage per database |

### 4.3 Structured Logging

All operator logs use structured format (slog) with fields:
- `cluster`: Cluster name
- `namespace`: Namespace
- `controller`: Controller name
- `reconcileID`: Unique reconciliation ID
- `operation`: Current operation
- `duration`: Operation duration

Log levels:
- `DEBUG`: Detailed reconciliation steps
- `INFO`: State changes, operations completed
- `WARN`: Degraded conditions, retries
- `ERROR`: Failed operations, unrecoverable errors

## 5. Maintenance Operations

### 5.1 Vacuum

**Trigger**: CR annotation `avsoft.io/maintenance: vacuum`

**Options** (via annotation values):
- `vacuum` - Regular vacuum
- `vacuum-analyze` - Vacuum with statistics refresh
- `vacuum-full` - Full vacuum (requires exclusive lock)

**Implementation**: Create a Kubernetes Job that connects to the coordinator and executes the vacuum command.

### 5.2 Analyze

**Trigger**: CR annotation `avsoft.io/maintenance: analyze`

Refreshes planner statistics on all tables or specified tables.

### 5.3 Reindex

**Trigger**: CR annotation `avsoft.io/maintenance: reindex`

Rebuilds indexes to recover from corruption or bloat.

### 5.4 Log Rotation

**Trigger**: Automatic (daily) or CR annotation

Rotates, archives, and compresses server log files.

## 6. Session Management

### 6.1 Via cloudberry-ctl

```bash
# List active sessions
cloudberry-ctl sessions list --cluster my-cluster

# Cancel a running query
cloudberry-ctl sessions cancel-query --cluster my-cluster --pid 12345

# Terminate a session
cloudberry-ctl sessions terminate --cluster my-cluster --pid 12345
```

### 6.2 Via Operator API

The operator exposes a webhook endpoint for session management operations that require immediate action.

## 7. Resource Management

### 7.1 Resource Groups

Managed via `spec.config.parameters` for global settings and cloudberry-ctl for group-level operations:

```bash
# Create resource group
cloudberry-ctl resource-group create --cluster my-cluster \
  --name analytics --concurrency 10 --cpu-max-percent 50 --memory-limit 30

# Assign role to resource group
cloudberry-ctl resource-group assign --cluster my-cluster \
  --group analytics --role analyst
```

### 7.2 Resource Queues

Alternative resource management via SQL:

```bash
cloudberry-ctl resource-queue create --cluster my-cluster \
  --name myqueue --active-statements 10 --memory-limit 2GB --priority HIGH
```

## 8. Error Handling

All administration operations follow this error handling pattern:

1. **Validate** inputs before execution
2. **Log** operation start with context
3. **Execute** with timeout (context.Context)
4. **Retry** transient failures with exponential backoff
5. **Report** status via CR conditions
6. **Alert** via Prometheus metrics on failure
7. **Trace** operation span via OTLP
