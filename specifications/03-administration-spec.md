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

### 2.5 Scale-Out (Add Segments)

**Trigger**: Increase `spec.segments.count` in the CloudberryCluster CR

**Example**: Scale from 4 to 8 primary segments

```yaml
spec:
  segments:
    count: 8          # was 4
    mirroring:
      enabled: true
      layout: group
```

**Process**:
1. Operator detects `spec.segments.count` increased
2. Pre-flight check: cluster must be in `Running` phase
   - If not in `Running` phase, the operator emits a `ScaleOutBlocked` warning event and skips the operation (retries on next reconcile)
3. Set `avsoft.io/scale-started` annotation with the current timestamp (RFC 3339)
4. Set status phase to `Scaling`
5. Set `DataRedistribution` condition with reason `ScaleOutStarted`
6. Emit `ScaleOutStarted` event
7. Update primary segment StatefulSet replicas from old count to new count
8. If mirroring enabled, update mirror segment StatefulSet replicas
9. Create redistribution Job via `BuildMaintenanceJob(cluster, "redistribute", timestamp)`
10. Set `DataRedistribution` condition to `InProgress`
11. On each reconcile while in `Scaling` phase, `checkScaleProgress()` verifies all StatefulSets are ready
12. When all pods are ready:
    - Set phase back to `Running`
    - Update `segmentsReady`, `segmentsTotal`
    - Set `DataRedistribution` condition to `Completed`
    - Emit `ScaleOutCompleted` event
    - Record `cloudberry_scale_operations_total{operation="scale-out"}` metric
    - Remove `avsoft.io/scale-started` annotation

**Scale Timeout** (10 minutes):
- The `avsoft.io/scale-started` annotation tracks when the scale operation began
- On each reconcile, `checkScaleProgress()` checks whether the timeout (10 minutes) has elapsed
- If the timeout is exceeded and pods are still not ready, `handleScaleFailure()` is invoked

**Rebalancing After Scale-Out**:
- Data redistribution runs as a background Job
- Progress tracked via `status.conditions` with type `DataRedistribution`
- Condition values: `InProgress`, `Completed`, `Failed`
- Redistribution can be monitored via:
  ```bash
  cloudberry-ctl cluster scale-status --cluster my-cluster
  ```
- Tables are redistributed one at a time to minimize impact
- Redistribution respects resource limits (configurable parallelism)

**Failure Handling**: If new segments are not ready after the 10-minute timeout, the operator:
1. Identifies unready segments from both primary and mirror StatefulSets
2. Populates `status.failedSegments` with details (contentID, hostname, role, status)
3. Sets condition `ScaleOutFailed` = `True` with reason `SegmentsNotReady` and a message including the count and timeout
4. Emits warning event `ScaleOutFailed`
5. Removes the `avsoft.io/scale-started` annotation
6. Does **NOT** automatically roll back — the cluster stays in `Scaling` phase
7. Manual intervention is required to resolve (fix the underlying issue, then the operator resumes on next reconcile)

### 2.6 Scale-In (Remove Segments)

**Trigger**: Decrease `spec.segments.count` in the CloudberryCluster CR

**Example**: Scale from 8 to 4 primary segments

```yaml
spec:
  segments:
    count: 4          # was 8
```

**Process**:
1. Operator detects `spec.segments.count` decreased
2. Pre-flight check: cluster must be in `Running` phase
   - If not in `Running` phase, the operator emits a `ScaleInBlocked` warning event and skips the operation (retries on next reconcile)
3. Safety check: if new count < 50% of current count, require `avsoft.io/confirm-scale-in=true` annotation
   - If missing, emit `ScaleInBlocked` warning event and skip
4. Set `avsoft.io/scale-started` annotation with the current timestamp
5. Set status phase to `Scaling`
6. Additional pre-flight checks:
   - New segment count must be >= 1
   - Sufficient capacity on remaining segments for redistributed data
4. Run data redistribution to move data OFF segments being removed:
   - Redistribute all tables to use only the first N segments
   - This is a potentially long-running operation tracked via Job
5. Wait for redistribution to complete
6. Verify no data remains on segments being removed
7. Deregister removed segments from `gp_segment_configuration`
8. If mirroring enabled, scale down mirror StatefulSet first
9. Scale down primary segment StatefulSet
10. Handle PVCs based on `deletionPolicy`:
    - `Retain`: PVCs for removed segments are kept (can be manually cleaned)
    - `Delete`: PVCs for removed segments are deleted
11. Update status: `segmentsReady`, `segmentsTotal`, phase back to `Running`
12. Record event `ScaleInCompleted`

**Safety Constraints**:
- Scale-in is blocked if redistribution would exceed available disk space
- Scale-in is blocked during active backup or recovery operations
- Minimum segment count is 1
- Scale-in by more than 50% of current count requires explicit confirmation annotation:
  ```yaml
  annotations:
    avsoft.io/confirm-scale-in: "true"
  ```

**Rebalancing During Scale-In**:
- Data must be fully redistributed BEFORE segments are removed
- Redistribution progress tracked via `status.conditions` type `DataRedistribution`
- If redistribution fails, scale-in is aborted and cluster remains at current size
- Failed redistribution emits event `ScaleInRedistributionFailed`

### 2.7 Segment Rebalancing

**Trigger**: CR annotation `avsoft.io/action: rebalance` or automatic after scale operations

**Purpose**: Redistribute data evenly across all segments to eliminate skew

**Process**:
1. Analyze current data distribution using `gp_toolkit.gp_skew_coefficients`
2. Identify tables with significant skew (above configurable threshold)
3. Create a redistribution plan
4. Execute redistribution as a background Job:
   - `ALTER TABLE ... SET DISTRIBUTED BY (...)` for hash-distributed tables
   - `ALTER TABLE ... SET DISTRIBUTED RANDOMLY` for randomly distributed tables
5. Track progress via status condition `DataRedistribution`
6. Verify distribution after completion
7. Update metrics: `cloudberry_data_skew_coefficient`

**Configuration**:
```yaml
spec:
  segments:
    rebalance:
      skewThreshold: 10        # percentage skew to trigger rebalance
      parallelism: 2           # number of tables to redistribute concurrently
      excludeTables:            # tables to skip during rebalance
        - "audit_log"
        - "temp_*"
```

**CLI**:
```bash
# Trigger manual rebalance
cloudberry-ctl ha rebalance --cluster my-cluster

# Check rebalance status
cloudberry-ctl ha rebalance --cluster my-cluster --status

# Rebalance specific tables
cloudberry-ctl ha rebalance --cluster my-cluster --tables public.orders,public.customers
```

### 2.8 Extend Persistent Volumes

**Trigger**: Increase `spec.coordinator.storage.size`, `spec.standby.storage.size`, or `spec.segments.storage.size` in the CloudberryCluster CR

**Example**: Extend coordinator storage from 5Gi to 20Gi

```yaml
spec:
  coordinator:
    storage:
      size: "20Gi"    # was "5Gi"
```

**Process**:
1. Operator detects storage size increase in the CR spec
2. Pre-flight checks:
   - New size must be greater than current size (shrinking is not supported)
   - PVC must exist (missing PVCs are skipped without error)
   - **StorageClass pre-flight check** via `storageClassSupportsExpansion()`:
     a. Read the StorageClass name from `pvc.spec.storageClassName` or the legacy `volume.beta.kubernetes.io/storage-class` annotation
     b. If no StorageClass is specified (default SC), allow the expansion attempt
     c. Look up the StorageClass in the Kubernetes API
     d. If the StorageClass has `allowVolumeExpansion: true`, allow the expansion
     e. If the StorageClass has `allowVolumeExpansion: false` or `nil`, **block** the expansion
     f. If the StorageClass is not found, **block** the expansion with a descriptive reason
     g. On transient API errors, **allow** the expansion (fail-open)
3. For each affected PVC that passes the pre-flight check:
   a. Patch the PVC `spec.resources.requests.storage` to the new size
   b. Wait for the PVC to report the new capacity in `status.capacity.storage`
   c. Some StorageClasses require a pod restart for the filesystem to be resized
4. If pod restart is required:
   - Perform rolling restart of affected StatefulSet (same order as config rolling restart)
   - Mirrors first, then primaries, then standby, then coordinator
5. Verify filesystem size inside pods matches the new PVC size
6. Update status condition `StorageExpanded` with details
7. Record event `StorageExpanded`

**StorageClass Blocking Behavior**:
- When expansion is blocked by the StorageClass check, the operator logs a WARN with the PVC name, StorageClass name, reason, and current/desired sizes
- **No error is returned** — the reconciliation continues normally for other components
- The PVC remains at its current size
- No `StorageExpanded` event or condition is emitted for blocked PVCs
- This is a non-fatal, informational warning — the operator does not retry the blocked expansion until the next spec change

**Scope of Expansion**:

| Field | Affects |
|-------|---------|
| `spec.coordinator.storage.size` | Coordinator PVC |
| `spec.standby.storage.size` | Standby coordinator PVC |
| `spec.segments.storage.size` | ALL primary segment PVCs AND all mirror segment PVCs |

**Constraints**:
- Volume shrinking is NOT supported (Kubernetes limitation)
- The StorageClass must have `allowVolumeExpansion: true` — the operator performs a pre-flight check via `storageClassSupportsExpansion()` and blocks expansion if the field is `false`, `nil`, or the StorageClass is not found
- When no StorageClass is specified on the PVC (cluster default), the operator allows the expansion attempt since it cannot determine the default StorageClass's capabilities
- On transient StorageClass lookup errors, the operator allows the expansion attempt (fail-open) rather than permanently blocking
- Some cloud providers require the pod to be restarted for online expansion
- Expansion of segment storage applies to ALL segments uniformly (individual segment expansion is not supported)
- **Docker Desktop limitation**: The `hostpath` provisioner does not implement volume expansion at the storage layer. The PVC metadata is updated but the underlying volume does not resize

**Monitoring**:
- `cloudberry_disk_usage_bytes` metric tracks actual usage
- `cloudberry_disk_usage_percent` metric tracks usage percentage
- Alerts can be configured on usage percentage to trigger proactive expansion

**CLI**:
```bash
# Check current storage usage
cloudberry-ctl inspect disk-usage --cluster my-cluster

# View PVC sizes
cloudberry-ctl storage disk-usage --cluster my-cluster
```

**Example: Extend all segment storage**:
```yaml
spec:
  segments:
    storage:
      size: "50Gi"    # was "20Gi"
```

This patches all segment PVCs (both primary and mirror) to 50Gi.

### 2.9 Upgrade Cluster

**Trigger**: Change to `spec.version` (when `spec.version` differs from `status.clusterVersion`) or presence of the `avsoft.io/upgrade` annotation (in-progress upgrade)

**Pre-flight Check**: Cluster must be in `Running` phase. If not, the operator emits an `UpgradeBlocked` warning event and skips the operation (retries on next reconcile).

**Process**:
1. Operator detects `spec.version != status.clusterVersion` via `isUpgradeNeeded()`
2. Pre-flight check: cluster must be in `Running` phase
   - If not in `Running` phase, emit `UpgradeBlocked` warning event and skip
3. Capture current image from the coordinator StatefulSet via `getCurrentImage()`
4. Store rollback state in the `avsoft.io/upgrade` annotation as JSON (see Upgrade Annotation Format below)
5. Set status phase to `Updating`
6. Emit `UpgradeStarted` event
7. Process upgrade phases in order (least critical first):
   - **Phase 1 — Mirrors**: Update mirror segment StatefulSet image. Wait for all mirror pods to be ready. Skip if mirroring is not enabled
   - **Phase 2 — Primaries**: Update primary segment StatefulSet image. Wait for all primary pods to be ready
   - **Phase 3 — Standby**: Update standby coordinator StatefulSet image. Wait for standby pod to be ready. Skip if standby is not enabled
   - **Phase 4 — Coordinator**: Update coordinator StatefulSet image. Wait for coordinator pod to be ready
   - **Phase 5 — Verify**: Post-upgrade health check — verify coordinator and primary segments are ready via `verifyUpgrade()`
8. On successful verification:
   - Set phase back to `Running`
   - Update `status.clusterVersion` to the new version
   - Set `UpgradeCompleted` condition to `True` with reason `UpgradeSucceeded`
   - Emit `UpgradeCompleted` event
   - Remove `avsoft.io/upgrade` annotation

**Upgrade Order**: mirrors → primaries → standby → coordinator (least critical first, coordinator last)

**Phase Advancement**: Each phase calls `upgradePhase()`, which updates the StatefulSet image via `updateStatefulSetImage()`, checks readiness via `isStatefulSetReady()`, and advances to the next phase via `advanceUpgradePhase()` when ready. If the StatefulSet is not yet ready, the reconciler requeues after 5 seconds.

**Upgrade Annotation Format** (`avsoft.io/upgrade`):

```json
{
  "previousImage": "postgres:16",
  "previousVersion": "7.1.0",
  "phase": "primaries",
  "startedAt": "2026-05-15T10:00:00Z",
  "phaseStartedAt": "2026-05-15T10:01:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `previousImage` | string | Container image before the upgrade (for rollback) |
| `previousVersion` | string | `status.clusterVersion` before the upgrade (for rollback) |
| `phase` | string | Current upgrade phase: `mirrors`, `primaries`, `standby`, `coordinator`, `verify` |
| `startedAt` | string | RFC 3339 timestamp when the upgrade was initiated |
| `phaseStartedAt` | string | RFC 3339 timestamp when the current phase started (reset on each phase transition) |

**Rollback Behavior**:

Each upgrade phase has a **10-minute timeout**. On each reconciliation, `continueUpgrade()` checks whether the current phase has exceeded the timeout by comparing `time.Since(phaseStartedAt)` against `upgradePhaseTimeout` (10 minutes).

If the timeout is exceeded:
1. `rollbackUpgrade()` reverts **ALL** StatefulSets (mirrors, primaries, standby, coordinator) to the `previousImage` stored in the upgrade annotation
2. Sets phase back to `Running`
3. Restores `status.clusterVersion` to the `previousVersion`
4. Sets `UpgradeFailed` condition to `True` with reason `RolledBack` and a message describing which phase timed out
5. Emits `UpgradeRollback` warning event
6. Removes the `avsoft.io/upgrade` annotation

**Status Transitions**: `Running` → `Updating` → `Running` (success) or `Running` → `Updating` → `Running` (rollback)

**Events**:

| Event | Type | Description |
|-------|------|-------------|
| `UpgradeStarted` | Normal | Upgrade initiated with previous and new version |
| `UpgradeCompleted` | Normal | Upgrade completed successfully |
| `UpgradeBlocked` | Warning | Upgrade blocked because cluster is not in `Running` phase |
| `UpgradeRollback` | Warning | Upgrade rolled back due to phase timeout |

**Conditions**:

| Condition | Status | Reason | Description |
|-----------|--------|--------|-------------|
| `UpgradeCompleted` | `True` | `UpgradeSucceeded` | Upgrade completed successfully |
| `UpgradeFailed` | `True` | `RolledBack` | Upgrade failed and was rolled back |

### 2.10 Remove Cluster

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
| Data redistribution | gpexpand / redistribution Job | `status.conditions[DataRedistribution]` |
| Storage expansion | PVC status | `status.conditions[StorageExpanded]` |

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
| `cloudberry_disk_usage_percent` | Gauge | Disk usage percentage |
| `cloudberry_data_skew_coefficient` | Gauge | Data distribution skew coefficient |
| `cloudberry_scale_operations_total` | Counter | Total scale-out/scale-in operations |
| `cloudberry_redistribution_progress` | Gauge | Data redistribution progress (0-100%) |
| `cloudberry_pvc_size_bytes` | Gauge | PVC requested size per component |

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
