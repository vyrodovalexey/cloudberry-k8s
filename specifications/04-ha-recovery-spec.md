# Cloudberry Operator - High Availability & Recovery Specification

**Version**: 1.0.0

---

## 1. Overview

This specification covers the operator's high availability and recovery capabilities, including segment mirroring, fault detection, automatic failover, coordinator standby management, and recovery operations.

## 2. Segment Mirroring

### 2.1 Mirroring Configuration

**Source**: `spec.segments.mirroring`

```yaml
segments:
  count: 8
  primariesPerHost: 2
  mirroring:
    enabled: true
    layout: group  # group | spread
```

### 2.2 Group Mirroring Layout

All mirrors for one host's primary segments are placed on one other host.

**Operator Implementation**:
- StatefulSet for primaries: `{cluster}-segment-primary`
- StatefulSet for mirrors: `{cluster}-segment-mirror`
- Pod anti-affinity ensures primary and mirror never share a node
- Mirror placement follows group algorithm:
  - Host N's mirrors go to Host (N+1) % total_hosts

### 2.3 Spread Mirroring Layout

Each host's mirrors are distributed across multiple remaining hosts.

**Operator Implementation**:
- Requires: number of hosts > primariesPerHost
- Mirror placement follows spread algorithm:
  - Host N's mirror M goes to Host (N + M + 1) % total_hosts
- Pod topology spread constraints enforce distribution

### 2.4 Adding Mirrors to Existing Cluster

**Trigger**: Change `spec.segments.mirroring.enabled` from `false` to `true`

**Process**:
1. Validate sufficient nodes for mirror placement
2. Create mirror StatefulSet
3. Initialize mirror segments from primaries
4. Start WAL replication
5. Wait for mirrors to sync
6. Update status `mirroringStatus: InSync`

### 2.5 Transaction Log Replication

- Continuous WAL streaming from primary to mirror
- Synchronous or asynchronous mode (configurable)
- Replication lag monitoring via Prometheus metrics

## 3. Fault Tolerance Service (FTS)

### 3.1 Configuration

**Source**: `spec.ha`

```yaml
ha:
  ftsProbeInterval: 60    # seconds between probes
  ftsProbeTimeout: 20     # seconds to wait for response
  ftsProbeRetries: 5      # retries before marking down
  checksums: true          # storage-layer checksums
```

### 3.2 Probe Mechanism

The operator implements FTS probing:

1. **Probe Loop**: Every `ftsProbeInterval` seconds
2. **Health Check**: TCP connection + SQL ping to each primary segment
3. **Failure Detection**: If probe fails after `ftsProbeRetries` attempts with `ftsProbeTimeout` per attempt
4. **Action**: Mark segment as down, promote mirror

### 3.3 Automatic Failover

**Trigger**: FTS detects primary segment failure

**Process**:
1. Mark primary segment as `Down` in status
2. Promote mirror to primary role
3. Update `gp_segment_configuration` equivalent
4. Continue query processing on promoted mirror
5. Emit event: `SegmentFailover`
6. Update Prometheus metric: `cloudberry_segments_failed`
7. Update CR status: `failedSegments` list

### 3.4 Prometheus Metrics for FTS

| Metric | Type | Description |
|--------|------|-------------|
| `cloudberry_fts_probe_total` | Counter | Total FTS probes |
| `cloudberry_fts_probe_failures_total` | Counter | Failed FTS probes |
| `cloudberry_fts_probe_duration_seconds` | Histogram | Probe duration |
| `cloudberry_fts_failover_total` | Counter | Total failovers |
| `cloudberry_segment_status` | Gauge | Per-segment status (1=up, 0=down) |
| `cloudberry_replication_lag_bytes` | Gauge | Replication lag per segment |

## 4. Segment Recovery

### 4.1 Incremental Recovery

**Trigger**: `cloudberry-ctl recovery incremental --cluster my-cluster`

Or CR annotation: `avsoft.io/recovery: incremental`

**Process**:
1. Identify failed segments from CR status
2. For each failed segment:
   a. Connect to the live mirror
   b. Copy only WAL changes missed during downtime
   c. Start the recovered segment
   d. Verify replication sync
3. Update CR status

**Use Case**: Segment was down briefly, data is intact.

### 4.2 Full Recovery

**Trigger**: `cloudberry-ctl recovery full --cluster my-cluster`

Or CR annotation: `avsoft.io/recovery: full`

**Process**:
1. Identify failed segments
2. For each failed segment:
   a. Delete existing data on failed segment PVC
   b. Copy all data from live mirror (pg_basebackup equivalent)
   c. Start the recovered segment
   d. Verify replication sync
3. Update CR status

**Use Case**: Segment data is corrupted.

### 4.3 Differential Recovery

**Trigger**: `cloudberry-ctl recovery differential --cluster my-cluster`

**Process**:
1. Identify failed segments
2. For each failed segment:
   a. Sync only file-level differences (rsync equivalent)
   b. Support parallel copy streams
   c. Start the recovered segment
   d. Verify replication sync
3. Update CR status

**Use Case**: Large segments where minimizing data transfer is important.

### 4.4 Recovery to Different Host

**Trigger**: `cloudberry-ctl recovery --cluster my-cluster --target-node node-3`

**Process**:
1. Validate target node has sufficient resources
2. Create new PVC on target node
3. Copy data from live mirror to new PVC
4. Update segment configuration
5. Start recovered segment on new node
6. Verify replication sync

### 4.5 Rebalancing After Recovery

**Trigger**: `cloudberry-ctl rebalance --cluster my-cluster`

Or CR annotation: `avsoft.io/action: rebalance`

**Process**:
1. Identify segments where mirror is acting as primary
2. For each such segment:
   a. Ensure original primary is recovered and synced
   b. Demote current primary (was mirror) back to mirror role
   c. Promote original primary back to primary role
3. Verify all segments are in preferred roles
4. Update CR status

**Selective Rebalance**:
```bash
cloudberry-ctl rebalance --cluster my-cluster --content-ids 0,1,2
```

## 5. Coordinator Standby

### 5.1 Deploy Standby

**Trigger**: Set `spec.standby.enabled: true`

**Process**:
1. Create standby StatefulSet `{cluster}-coordinator-standby`
2. Create standby PVC
3. Initialize standby from coordinator (pg_basebackup)
4. Configure WAL streaming replication
5. Verify standby is receiving WAL
6. Update status: `standbyReady: true`

### 5.2 WAL Replication to Standby

- Continuous WAL streaming from coordinator to standby
- Standby replays WAL to maintain current state
- Replication lag monitored via:
  - `cloudberry_standby_replication_lag_bytes` (Prometheus)
  - `status.conditions[StandbyReady]` (CR status)

### 5.3 Activate Standby on Coordinator Failure

**Trigger**: `cloudberry-ctl standby activate --cluster my-cluster`

Or CR annotation: `avsoft.io/action: activate-standby`

**Note**: Activation is NOT automatic - requires explicit administrator action.

**Process**:
1. Verify coordinator is truly unavailable
2. Promote standby to primary coordinator
3. Update Services to point to new coordinator
4. Reconstruct state from replicated WAL
5. Update CR status: `coordinatorReady: true` (pointing to former standby)
6. Emit event: `CoordinatorFailover`

### 5.4 Reinitialize Standby After Failover

**Trigger**: `cloudberry-ctl standby reinitialize --cluster my-cluster`

**Process**:
1. Repair or replace original coordinator node
2. Initialize new standby on original host
3. Configure WAL streaming from new coordinator
4. Verify standby sync
5. Update status

### 5.5 Restore Original Roles

**Trigger**: `cloudberry-ctl standby restore-roles --cluster my-cluster`

**Process**:
1. Stop cluster
2. Swap coordinator and standby roles
3. Reinitialize standby on secondary host
4. Start cluster
5. Verify health

## 6. Coordinator Mirroring Status

### 6.1 Status Reporting

The operator reports coordinator mirroring status in:

- CR status condition: `StandbyReady`
- Prometheus metric: `cloudberry_standby_up`
- Prometheus metric: `cloudberry_standby_replication_lag_bytes`
- Kubernetes events on state changes

### 6.2 Health Checks

| Check | Frequency | Action on Failure |
|-------|-----------|-------------------|
| Standby pod health | Every 10s (K8s probe) | Restart pod |
| WAL replication active | Every `ftsProbeInterval` | Alert, update status |
| Replication lag | Every `ftsProbeInterval` | Alert if exceeds threshold |

## 7. Storage-Layer Checksums

**Source**: `spec.ha.checksums`

When enabled:
- Block-level checksums for heap and append-optimized storage
- Detects data corruption on disk
- Configured at cluster initialization time
- Cannot be changed after initialization

## 8. Network Redundancy

### 8.1 Interconnect Configuration

Managed via `spec.config.parameters`:

```yaml
config:
  parameters:
    gp_interconnect_type: "udpifc"  # or "tcp"
    gp_max_packet_size: "8192"
```

### 8.2 Pod Network Policies

The operator creates NetworkPolicies to:
- Allow coordinator <-> segment communication
- Allow segment <-> segment communication (interconnect)
- Allow coordinator <-> standby replication
- Allow primary <-> mirror replication
- Allow external client access to coordinator only

## 9. Recovery Kubernetes Jobs

Recovery operations are implemented as Kubernetes Jobs:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: {cluster}-recovery-{timestamp}
  namespace: {namespace}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    avsoft.io/cluster: {cluster}
    avsoft.io/operation: recovery
spec:
  backoffLimit: 3
  activeDeadlineSeconds: 3600
  template:
    spec:
      containers:
        - name: recovery
          image: {cluster-image}
          command: ["/recovery-entrypoint.sh"]
          env:
            - name: RECOVERY_TYPE
              value: "incremental"  # or full, differential
            - name: TARGET_SEGMENTS
              value: "0,1"
      restartPolicy: Never
```

## 10. Event Types

| Event | Type | Reason | Description |
|-------|------|--------|-------------|
| SegmentFailover | Warning | SegmentDown | Primary segment failed, mirror promoted |
| SegmentRecovered | Normal | SegmentRecovered | Failed segment recovered |
| SegmentsRebalanced | Normal | Rebalanced | Segment roles restored to preferred |
| CoordinatorFailover | Warning | CoordinatorDown | Coordinator failed, standby activated |
| StandbyInitialized | Normal | StandbyReady | Standby coordinator initialized |
| MirroringDegraded | Warning | MirroringDegraded | One or more mirrors out of sync |
| MirroringRestored | Normal | MirroringInSync | All mirrors back in sync |
| RecoveryStarted | Normal | RecoveryStarted | Recovery operation initiated |
| RecoveryCompleted | Normal | RecoveryCompleted | Recovery operation completed |
| RecoveryFailed | Warning | RecoveryFailed | Recovery operation failed |
