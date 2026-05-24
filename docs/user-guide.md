# User Guide

This guide covers day-to-day operations for managing Cloudberry Database clusters with the Cloudberry Operator.

## Table of Contents

- [Creating a CloudberryCluster](#creating-a-cloudberrycluster)
- [Cluster Name Uniqueness](#cluster-name-uniqueness)
- [Managing Cluster Lifecycle](#managing-cluster-lifecycle)
  - [Cluster Phases](#cluster-phases)
  - [Starting a Cluster](#starting-a-cluster)
  - [Stopping a Cluster](#stopping-a-cluster)
  - [Restarting a Cluster](#restarting-a-cluster)
  - [Phase Transitions](#phase-transitions)
  - [Action Annotations Reference](#action-annotations-reference)
  - [Upgrading a Cluster](#upgrading-a-cluster)
    - [Upgrade Process](#upgrade-process)
    - [Monitoring Upgrade Progress](#monitoring-upgrade-progress)
    - [Checking Upgrade Status](#checking-upgrade-status)
    - [Automatic Rollback](#automatic-rollback)
    - [Upgrade Events](#upgrade-events)
    - [Troubleshooting Upgrades](#troubleshooting-upgrades)
  - [Deleting a Cluster](#deleting-a-cluster)
    - [Deletion Policy](#deletion-policy)
    - [Backup on Delete](#backup-on-delete)
    - [Deletion Flow](#deletion-flow)
    - [Deletion Events](#deletion-events)
    - [Monitoring Deletion](#monitoring-deletion)
    - [No Finalizer Behavior](#no-finalizer-behavior)
- [Configuration Management](#configuration-management)
  - [Hot-Reload vs Rolling Restart](#hot-reload-vs-rolling-restart)
  - [Restart-Required Parameters](#restart-required-parameters)
  - [Rolling Restart Behavior](#rolling-restart-behavior)
- [Authentication Setup](#authentication-setup)
  - [Default HBA Rules](#default-hba-rules)
  - [OIDC Redirect Protection](#oidc-redirect-protection)
  - [OIDC Full Flow Setup with Keycloak](#oidc-full-flow-setup-with-keycloak)
  - [Dual-Mode Authentication (Basic + OIDC)](#dual-mode-authentication-basic--oidc)
  - [SSL/TLS Configuration](#ssltls-configuration)
- [Webhook Certificate Setup](#webhook-certificate-setup)
- [High Availability](#high-availability)
  - [Automatic Segment Failover](#automatic-segment-failover)
    - [How FTS Detects Failures](#how-fts-detects-failures)
    - [What Happens During Automatic Failover](#what-happens-during-automatic-failover)
    - [Monitoring Failover](#monitoring-failover)
    - [Post-Failover State](#post-failover-state)
    - [Recovering After Failover](#recovering-after-failover)
    - [Troubleshooting Failover Issues](#troubleshooting-failover-issues)
  - [Enable Mirroring on Existing Cluster](#enable-mirroring-on-existing-cluster)
    - [Prerequisites](#prerequisites)
    - [Enabling Mirroring](#enabling-mirroring-1)
    - [Monitoring Mirroring Progress](#monitoring-mirroring-progress)
    - [Status Transitions](#status-transitions)
    - [Mirroring Events](#mirroring-events)
    - [Mirroring Metrics](#mirroring-metrics)
    - [Troubleshooting Mirroring Enable](#troubleshooting-mirroring-enable)
  - [Disable Mirroring](#disable-mirroring)
    - [Implications of Disabling Mirroring](#implications-of-disabling-mirroring)
    - [Disabling Mirroring](#disabling-mirroring)
    - [PVC Cleanup Behavior](#pvc-cleanup-behavior)
- [Maintenance Operations](#maintenance-operations)
  - [Maintenance Jobs](#maintenance-jobs)
  - [Maintenance Annotations](#maintenance-annotations)
- [Session Management](#session-management)
  - [Listing Sessions](#listing-sessions)
  - [Canceling a Query](#canceling-a-query)
  - [Terminating a Session](#terminating-a-session)
  - [Graceful Degradation](#graceful-degradation)
  - [Error Handling](#error-handling)
- [Resource Group Management](#resource-group-management)
  - [Creating a Resource Group](#creating-a-resource-group)
  - [Listing Resource Groups](#listing-resource-groups)
  - [Assigning a Role to a Resource Group](#assigning-a-role-to-a-resource-group)
  - [Deleting a Resource Group](#deleting-a-resource-group)
- [Declarative Workload Management](#declarative-workload-management)
  - [Enabling Workload Management](#enabling-workload-management)
  - [Resource Group Reconciliation](#resource-group-reconciliation)
  - [Workload Rules ConfigMap](#workload-rules-configmap)
  - [Idle Session Rules ConfigMap](#idle-session-rules-configmap)
  - [WorkloadConfigured Condition](#workloadconfigured-condition)
  - [DB Unavailable Fallback](#db-unavailable-fallback)
- [Storage Expansion](#storage-expansion)
  - [StorageClass Requirements](#storageclass-requirements)
  - [Expansion Scopes](#expansion-scopes)
  - [Safety Constraints](#safety-constraints)
  - [Monitoring Storage Expansion](#monitoring-storage-expansion)
  - [Storage Expansion Events and Conditions](#storage-expansion-events-and-conditions)
  - [Storage Expansion Metrics](#storage-expansion-metrics)
- [Scaling Operations](#scaling-operations)
  - [Scaling Out (Adding Segments)](#scaling-out-adding-segments)
  - [Scaling In (Removing Segments)](#scaling-in-removing-segments)
  - [Scale-Out Failure Handling](#scale-out-failure-handling)
  - [Phase Transitions During Scaling](#phase-transitions-during-scaling)
  - [Monitoring Scale Progress](#monitoring-scale-progress)
  - [Data Redistribution](#data-redistribution)
  - [Scale Metrics](#scale-metrics)
- [Segment Rebalancing](#segment-rebalancing)
  - [Rebalance Configuration](#rebalance-configuration)
  - [Triggering a Rebalance](#triggering-a-rebalance)
  - [Monitoring Rebalance Status](#monitoring-rebalance-status)
  - [Rebalance Events and Conditions](#rebalance-events-and-conditions)
  - [Rebalance Metrics](#rebalance-metrics)
- [Test Data Setup](#test-data-setup)
  - [Test Data Schema](#test-data-schema)
  - [Pareto Skew Pattern](#pareto-skew-pattern)
  - [Rebalance Exclusion Patterns](#rebalance-exclusion-patterns)
  - [Loading Test Data](#loading-test-data)
- [Error Handling and Observability](#error-handling-and-observability)
  - [Structured Error Types](#structured-error-types)
  - [Retry with Exponential Backoff](#retry-with-exponential-backoff)
  - [Reconciliation Metrics](#reconciliation-metrics)
  - [Telemetry Spans](#telemetry-spans)
  - [Structured Logging](#structured-logging)
  - [Webhook Validation Errors](#webhook-validation-errors)
  - [Pod Deletion Recovery](#pod-deletion-recovery)
- [Inspection Commands](#inspection-commands)
- [Auditing](#auditing)
  - [Connection Auditing](#connection-auditing)
  - [Statement Auditing](#statement-auditing)
  - [Operator Audit Log](#operator-audit-log)
- [Monitoring and Observability](#monitoring-and-observability)

## Creating a CloudberryCluster

### Minimal Cluster

The simplest cluster requires only an image, coordinator storage, and segment configuration:

```yaml
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: minimal-cluster
  namespace: cloudberry-test
spec:
  image: "postgres:16"
  coordinator:
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
    storage:
      size: 5Gi
  segments:
    count: 2
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
    storage:
      size: 5Gi
```

```bash
kubectl apply -f minimal-cluster.yaml
```

The operator applies defaults for all unspecified fields:
- **Image**: `postgres:16` (or specify your preferred PostgreSQL-compatible image)
- **Coordinator port**: `5432` (all components, including segments, use this port). Port values are validated to be in the range 1–65535
- **Storage class**: cluster default (no `storageClass` required)
- **Basic auth**: enabled with `gpadmin` user
- **Deletion policy**: `Retain`

> **Note**: The init container uses `busybox:1.36` to prepare the data directory. The main container receives `PGDATA=/data/pgdata` and `POSTGRES_PASSWORD` (via `SecretKeyRef` from the auto-created admin password Secret) environment variables.

### Production Cluster

A production-ready cluster with HA, authentication, and monitoring:

```yaml
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: production-cluster
  namespace: cloudberry-prod
spec:
  image: "postgres:16"

  coordinator:
    resources:
      requests:
        cpu: "2"
        memory: 4Gi
      limits:
        cpu: "4"
        memory: 8Gi
    storage:
      storageClass: fast-ssd
      size: 50Gi

  standby:
    enabled: true
    resources:
      requests:
        cpu: "2"
        memory: 4Gi
    storage:
      storageClass: fast-ssd
      size: 50Gi

  segments:
    count: 8
    primariesPerHost: 2
    mirroring:
      enabled: true
      layout: spread
    resources:
      requests:
        cpu: "4"
        memory: 16Gi
      limits:
        cpu: "8"
        memory: 32Gi
    storage:
      storageClass: fast-ssd
      size: 200Gi
    antiAffinity: required

  auth:
    basic:
      enabled: true
      adminUser: gpadmin
      adminPasswordSecret:
        name: cloudberry-admin-password
        key: password
    oidc:
      enabled: true
      issuerURL: https://keycloak.auth-system/realms/cloudberry
      clientID: cloudberry-operator
      clientSecret:
        secretRef:
          name: oidc-client-secret
          key: client-secret
      roleMapping:
        admin: Admin
        operator: Operator
        user: Basic
        reader: "Self Only"
    hbaRules:
      - type: local
        database: all
        user: gpadmin
        method: trust
      - type: host
        database: all
        user: all
        address: "10.0.0.0/8"
        method: scram-sha-256
      - type: hostssl
        database: all
        user: all
        address: "0.0.0.0/0"
        method: scram-sha-256
    ssl:
      enabled: true
      certSecret:
        name: cloudberry-tls
      minTLSVersion: "1.2"

  config:
    parameters:
      max_connections: "200"
      shared_buffers: "2GB"
      work_mem: "128MB"
      maintenance_work_mem: "512MB"
      wal_level: "replica"
    coordinatorParameters:
      optimizer: "on"

  ha:
    ftsProbeInterval: 30
    ftsProbeTimeout: 10
    ftsProbeRetries: 3
    checksums: true

  monitoring:
    enabled: true
    metricsPort: 9187
    serviceMonitor: true

  telemetry:
    enabled: true
    otlpEndpoint: otel-collector:4317
    otlpProtocol: grpc

  deletionPolicy: Retain
  backupOnDelete: true
```

## Cluster Name Uniqueness

CloudberryCluster names must be **unique across all namespaces**. The validating webhook rejects creation of a cluster if another cluster with the same name already exists in any namespace.

```bash
# This succeeds:
kubectl apply -f - <<EOF
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: my-cluster
  namespace: team-a
spec:
  image: "postgres:16"
  coordinator:
    storage: { size: 5Gi }
  segments:
    count: 2
    storage: { size: 5Gi }
EOF

# This is rejected — "my-cluster" already exists in namespace "team-a":
kubectl apply -f - <<EOF
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: my-cluster
  namespace: team-b
spec:
  image: "postgres:16"
  coordinator:
    storage: { size: 5Gi }
  segments:
    count: 2
    storage: { size: 5Gi }
EOF
# Error: CloudberryCluster with name "my-cluster" already exists in namespace "team-a"
```

This constraint prevents naming collisions in cross-namespace service discovery and ensures cluster names can serve as unique identifiers across the entire Kubernetes cluster.

> **Note**: This validation requires the admission webhook to be enabled (`webhook.enabled=true`). If webhooks are disabled, duplicate names are not prevented.

### Checking Cluster Status

```bash
# Quick status via kubectl
kubectl get cloudberryclusters -n cloudberry-test

# Output:
# NAME                 PHASE     SEGMENTS   AGE
# production-cluster   Running   8          1h

# Detailed status
kubectl describe cloudberrycluster production-cluster -n cloudberry-test

# Status via cloudberry-ctl (communicates with the operator REST API)
cloudberry-ctl cluster status --cluster production-cluster

# JSON output
cloudberry-ctl cluster status --cluster production-cluster --output json
```

> **Note**: All `cloudberry-ctl` commands communicate with the operator REST API server (default port `:8090`). The CLI uses the `internal/ctl.OperatorClient` to make authenticated HTTP calls. Ensure the operator is running and accessible before using CLI commands.

> **Tip**: Use the `--verbose` (`-v`) flag to debug connectivity or authentication issues. Verbose mode logs HTTP request/response details. Configuration priority is: CLI flag > environment variable > config file > default. See [cloudberry-ctl reference](cloudberry-ctl.md) for details.

## Managing Cluster Lifecycle

### Cluster Phases

The cluster progresses through several phases during its lifecycle:

| Phase | Description |
|-------|-------------|
| `Pending` | Cluster resource created, waiting for initialization |
| `Initializing` | StatefulSets and Services are being created |
| `Running` | All components are running and healthy |
| `Updating` | Cluster spec changed, resources are being updated |
| `Scaling` | Segment count is being changed |
| `Stopping` | Cluster is shutting down (scale-down in progress) |
| `Stopped` | All pods are scaled to zero |
| `Restricted` | Coordinator only, superuser connections only |
| `Maintenance` | Coordinator only, utility mode |
| `Failed` | An error occurred during reconciliation |
| `Deleting` | Cluster is being deleted |

### Starting a Cluster

```bash
# Normal start — all components (coordinator, standby, primaries, mirrors)
cloudberry-ctl cluster start --cluster my-cluster

# Restricted start — coordinator only, superuser connections only
cloudberry-ctl cluster start --cluster my-cluster --mode restricted

# Maintenance start — coordinator only in utility mode
cloudberry-ctl cluster start --cluster my-cluster --mode maintenance
```

Or via annotation:

```bash
# Normal start
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/action=start

# Restricted start
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/action=start-restricted

# Maintenance start
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/action=start-maintenance
```

When starting from `Stopped`:
- **Normal start** (`start`): Full reconciliation restores all StatefulSets. Phase transitions: `Stopped` → `Initializing` → `Running` (all 10 pods for a typical cluster with coordinator + standby + 4 primaries + 4 mirrors).
- **Restricted start** (`start-restricted`): Only the coordinator StatefulSet is scaled up. Phase transitions: `Stopped` → `Restricted` (coordinator only).
- **Maintenance start** (`start-maintenance`): Only the coordinator StatefulSet is scaled up in utility mode. Phase transitions: `Stopped` → `Maintenance` (coordinator only).

### Stopping a Cluster

```bash
# Smart stop — wait for clients to disconnect (default)
cloudberry-ctl cluster stop --cluster my-cluster

# Fast stop — rollback active transactions, disconnect clients
cloudberry-ctl cluster stop --cluster my-cluster --mode fast

# Immediate stop — abort all connections immediately
cloudberry-ctl cluster stop --cluster my-cluster --mode immediate
```

Or via annotation:

```bash
# Smart stop (default)
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/action=stop

# Fast stop
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/action=stop-fast

# Immediate stop
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/action=stop-immediate
```

The operator scales down StatefulSets in a safe order: **mirrors → primaries → standby → coordinator**. The phase transitions through `Stopping` → `Stopped` (0 pods).

The operator emits the following events during stop:
- `Stopping` — scale-down initiated
- `Stopped` — all pods are down

### Restarting a Cluster

```bash
cloudberry-ctl cluster restart --cluster my-cluster
```

Or via annotation:

```bash
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/action=restart
```

A restart performs a stop followed by a full start. Phase transitions: `Running` → `Stopping` → `Initializing` → `Running`. The operator emits `Restarting` and `Restarted` events.

### Phase Transitions

```
                    ┌──────────┐
                    │ Pending  │
                    └────┬─────┘
                         │
                    ┌────▼──────────┐
                    │ Initializing  │◄──────────────────────┐
                    └────┬──────────┘                       │
                         │                                  │
                    ┌────▼─────┐    stop/stop-fast/     ┌───┴────┐
                    │ Running  │───stop-immediate──────▶│Stopping│
                    └────┬─────┘                        └───┬────┘
                         │                                  │
                         │ restart                     ┌────▼────┐
                         └────────────────────────────▶│ Stopped │
                                                       └────┬────┘
                                                            │
                                    ┌───────────────────────┼───────────────┐
                                    │ start                 │               │
                               ┌────▼──────────┐    ┌──────▼─────┐  ┌──────▼──────────┐
                               │ Initializing  │    │ Restricted │  │  Maintenance    │
                               │  → Running    │    │            │  │                 │
                               └───────────────┘    └────────────┘  └─────────────────┘
```

### Action Annotations Reference

All lifecycle actions are triggered by setting the `avsoft.io/action` annotation on the `CloudberryCluster` resource. The operator processes the annotation and removes it after handling.

| Annotation Value | Description | Resulting Phase |
|-----------------|-------------|-----------------|
| `start` | Normal start — all components | `Running` |
| `start-restricted` | Coordinator only, superuser connections | `Restricted` |
| `start-maintenance` | Coordinator only, utility mode | `Maintenance` |
| `stop` | Smart stop — wait for clients | `Stopped` |
| `stop-fast` | Fast stop — rollback transactions | `Stopped` |
| `stop-immediate` | Immediate stop — abort connections | `Stopped` |
| `restart` | Stop then start | `Running` |
| `rebalance` | Rebalance segment roles | `Running` |
| `activate-standby` | Promote standby to coordinator | `Running` |

> **Note**: Action annotations are checked **before** the generation-based skip logic. This ensures that lifecycle actions (which do not change the CRD generation) are always processed.

> **Retry on failure**: Action annotations are removed **after** successful processing. If the action handler fails (e.g., due to a transient error), the annotation remains on the resource and the action is retried on the next reconciliation cycle. This ensures that failed actions are not silently lost.

### Upgrading a Cluster

To upgrade the database version, update `spec.version` and `spec.image`:

```bash
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge -p '
  {"spec": {"version": "7.2.0", "image": "postgres:17"}}'
```

Or update the CRD manifest directly:

```yaml
spec:
  version: "7.2.0"    # was "7.1.0"
  image: "postgres:17"  # was "postgres:16"
```

The operator detects the upgrade when `spec.version` differs from `status.clusterVersion` and performs a phase-by-phase rolling upgrade.

#### Upgrade Process

1. **Pre-flight check**: The cluster must be in `Running` phase. If not, the operator emits an `UpgradeBlocked` warning event and retries on the next reconciliation
2. **State capture**: The operator saves the current image and version in the `avsoft.io/upgrade` annotation for rollback
3. **Phase transition**: The cluster phase changes to `Updating`
4. **Rolling upgrade** (least critical components first):
   - **Mirrors** — Mirror segment StatefulSet image is updated; waits for all mirror pods to be ready
   - **Primaries** — Primary segment StatefulSet image is updated; waits for all primary pods to be ready
   - **Standby** — Standby coordinator StatefulSet image is updated; waits for the standby pod to be ready (skipped if standby is not enabled)
   - **Coordinator** — Coordinator StatefulSet image is updated; waits for the coordinator pod to be ready
5. **Verification**: Post-upgrade health check confirms the coordinator and primary segments are ready
6. **Completion**: Phase returns to `Running`, `status.clusterVersion` is updated, and the `UpgradeCompleted` event is emitted

#### Monitoring Upgrade Progress

```bash
# Watch the cluster phase
kubectl get cloudberrycluster my-cluster -n cloudberry-test -w

# Check the upgrade annotation for current phase
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.metadata.annotations.avsoft\.io/upgrade}' | jq .

# Watch upgrade events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -i upgrade
```

**Example upgrade annotation** (during the primaries phase):

```json
{
  "previousImage": "postgres:16",
  "previousVersion": "7.1.0",
  "phase": "primaries",
  "startedAt": "2026-05-15T10:00:00Z",
  "phaseStartedAt": "2026-05-15T10:01:00Z"
}
```

#### Checking Upgrade Status

```bash
# Check if upgrade completed
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.conditions[?(@.type=="UpgradeCompleted")]}' | jq .

# Check if upgrade failed
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.conditions[?(@.type=="UpgradeFailed")]}' | jq .

# Verify the new cluster version
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.clusterVersion}'
```

#### Automatic Rollback

Each upgrade phase has a **10-minute timeout**. If a phase does not complete within this window (e.g., new pods fail to become ready), the operator automatically rolls back:

1. **ALL** StatefulSets (mirrors, primaries, standby, coordinator) are reverted to the previous image
2. The cluster phase returns to `Running` with the old version
3. The `UpgradeFailed` condition is set with reason `RolledBack`
4. An `UpgradeRollback` warning event is emitted

```bash
# After a rollback, check the UpgradeFailed condition
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.conditions[?(@.type=="UpgradeFailed")]}' | jq .
```

**Example output:**

```json
{
  "type": "UpgradeFailed",
  "status": "True",
  "reason": "RolledBack",
  "message": "phase \"coordinator\" timed out after 10m0s",
  "lastTransitionTime": "2026-05-15T10:12:00Z"
}
```

#### Upgrade Events

| Event | Type | Description |
|-------|------|-------------|
| `UpgradeStarted` | Normal | Upgrade initiated (includes previous and new version) |
| `UpgradeCompleted` | Normal | Upgrade completed successfully |
| `UpgradeBlocked` | Warning | Upgrade blocked — cluster not in `Running` phase |
| `UpgradeRollback` | Warning | Upgrade rolled back due to phase timeout |

```bash
# View all upgrade-related events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -E 'Upgrade'
```

#### Troubleshooting Upgrades

If an upgrade fails and rolls back:

1. **Check why pods failed to become ready**:

   ```bash
   kubectl get pods -n cloudberry-test -l avsoft.io/cluster=my-cluster | grep -v Running
   kubectl describe pod <failing-pod> -n cloudberry-test
   kubectl logs <failing-pod> -n cloudberry-test
   ```

2. **Common causes**:

   | Cause | Symptoms | Resolution |
   |-------|----------|------------|
   | Invalid image | Pods in `ImagePullBackOff` | Fix the image name/tag and retry |
   | Incompatible version | Pods crash-looping | Check database compatibility and use a supported version |
   | Insufficient resources | Pods stuck in `Pending` | Increase resource limits or add nodes |

3. **Retry the upgrade** after fixing the issue by patching `spec.version` and `spec.image` again

### Deleting a Cluster

```bash
# Delete via cloudberry-ctl (requires confirmation)
cloudberry-ctl cluster delete --cluster my-cluster --confirm

# Delete via kubectl
kubectl delete cloudberrycluster my-cluster -n cloudberry-test
```

When you delete a `CloudberryCluster`, the operator's finalizer intercepts the deletion and performs cleanup before the resource is removed. The cluster phase transitions from its current state to `Deleting` during this process.

#### Deletion Policy

The `deletionPolicy` field controls what happens to PVCs when the cluster is deleted:

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `Retain` (default) | PVCs are preserved after deletion | Data recovery, audit compliance, debugging |
| `Delete` | All cluster PVCs are automatically deleted | Cost savings, ephemeral environments |

Configure the deletion policy in the CRD:

```yaml
spec:
  deletionPolicy: Retain    # or "Delete"
  backupOnDelete: true       # optional: trigger backup before deletion
```

#### Backup on Delete

When `backupOnDelete: true`, the operator creates a maintenance Job to perform a backup before proceeding with deletion. This ensures you have a recovery point even when deleting a cluster.

```yaml
spec:
  deletionPolicy: Retain
  backupOnDelete: true
```

The backup Job:
- **Name**: `{cluster}-maintenance-{timestamp}` with `backup-on-delete` in the name
- **Operation**: `backup-on-delete` (maps to `gpbackup` in production Cloudberry)
- **Labels**: `avsoft.io/cluster={cluster}`, `avsoft.io/operation=backup-on-delete`
- **Properties**: Same as other maintenance Jobs (`BackoffLimit=1`, `TTLSecondsAfterFinished=3600`)

#### Deletion Flow

The operator processes deletion in the following order:

1. **Phase transition**: Sets the cluster phase to `Deleting` and emits a `Deleting` event
2. **Backup** (if `backupOnDelete: true`): Creates a backup maintenance Job and emits a `BackupOnDelete` event
3. **PVC cleanup**: Based on the `deletionPolicy`:
   - **Retain**: PVCs are preserved; emits a `PVCsRetained` event
   - **Delete**: All cluster PVCs are deleted; emits a `PVCsDeleted` event
4. **Finalizer removal**: Removes the `avsoft.io/finalizer` finalizer, allowing Kubernetes to complete the deletion
5. **Completion**: Emits a `Deleted` event

#### Deletion Events

| Event | Type | Description |
|-------|------|-------------|
| `Deleting` | Normal | Cluster deletion initiated, phase set to `Deleting` |
| `BackupOnDelete` | Normal | Backup triggered before deletion (when `backupOnDelete: true`) |
| `PVCsRetained` | Normal | PVCs preserved (when `deletionPolicy: Retain`) |
| `PVCsDeleted` | Normal | All PVCs deleted (when `deletionPolicy: Delete`) |
| `Deleted` | Normal | Cluster deletion completed, finalizer removed |

```bash
# Watch deletion events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -E 'Deleting|BackupOnDelete|PVCs|Deleted'
```

#### Monitoring Deletion

```bash
# Watch the cluster phase during deletion
kubectl get cloudberrycluster my-cluster -n cloudberry-test -w

# Check if backup Job was created
kubectl get jobs -n cloudberry-test \
  -l avsoft.io/cluster=my-cluster,avsoft.io/operation=backup-on-delete

# Verify PVC state after deletion
kubectl get pvc -n cloudberry-test -l avsoft.io/cluster=my-cluster
```

#### No Finalizer Behavior

If the cluster does not have a finalizer (e.g., it was removed manually), Kubernetes deletes the resource immediately without invoking the operator's deletion logic. No backup is performed, no PVC cleanup occurs, and no deletion events are emitted.

> **Note**: The operator automatically adds a finalizer when creating a cluster. Removing the finalizer manually bypasses all deletion safeguards.

## Configuration Management

### Viewing Parameters

```bash
# All parameters
cloudberry-ctl config get --cluster my-cluster

# Specific parameter
cloudberry-ctl config get --cluster my-cluster --param max_connections
```

### Setting Parameters

```bash
# Cluster-wide parameter
cloudberry-ctl config set --cluster my-cluster \
  --param work_mem --value 256MB

# Coordinator-only parameter
cloudberry-ctl config set --cluster my-cluster \
  --param optimizer --value on --coordinator-only

# Per-database parameter
cloudberry-ctl config set --cluster my-cluster \
  --param work_mem --value 512MB --database mydb

# Per-role parameter
cloudberry-ctl config set --cluster my-cluster \
  --param statement_mem --value 1GB --role analyst
```

Or update the CRD directly:

```yaml
spec:
  config:
    parameters:
      max_connections: "200"
      shared_buffers: "2GB"
      work_mem: "128MB"
    coordinatorParameters:
      optimizer: "on"
    databaseParameters:
      mydb:
        work_mem: "512MB"
    roleParameters:
      analyst:
        statement_mem: "1GB"
```

### Hot-Reload vs Rolling Restart

The operator automatically classifies parameter changes into two categories:

**Reload-safe parameters** (PostgreSQL context = `sighup`): These parameters take effect after a configuration reload without restarting pods. Examples include `log_min_messages`, `work_mem`, `statement_timeout`, and `log_statement`.

When you change a reload-safe parameter:
1. The operator updates the `{cluster}-postgresql-conf` ConfigMap
2. No pods are restarted
3. The `ConfigApplied` condition is set to `True` with reason `ConfigReloaded`
4. A `ConfigReloaded` event is emitted
5. The `cloudberry_config_reload_total` metric is incremented

**Restart-required parameters** (PostgreSQL context = `postmaster`): These parameters require a server restart to take effect. Changing them triggers an automatic rolling restart.

When you change a restart-required parameter:
1. The operator updates the `{cluster}-postgresql-conf` ConfigMap
2. A rolling restart is triggered automatically
3. The `ConfigApplied` condition is set to `False` with reason `RestartRequired`
4. A `RollingRestartStarted` event is emitted
5. After the rolling restart completes, `ConfigApplied` is set to `True` with reason `ConfigAppliedAfterRestart`
6. A `RollingRestartCompleted` event is emitted

### Restart-Required Parameters

The following parameters require a server restart:

| Parameter | Description |
|-----------|-------------|
| `shared_buffers` | Shared memory for caching |
| `max_connections` | Maximum concurrent connections |
| `max_prepared_transactions` | Maximum prepared transactions |
| `max_worker_processes` | Maximum background workers |
| `max_wal_senders` | Maximum WAL sender processes |
| `wal_level` | WAL logging level |
| `wal_buffers` | WAL buffer size |
| `huge_pages` | Huge pages usage |
| `shared_preload_libraries` | Preloaded shared libraries |
| `max_locks_per_transaction` | Maximum locks per transaction |
| `max_files_per_process` | Maximum open files per process |
| `port` | Listening port |
| `superuser_reserved_connections` | Reserved superuser connections |
| `unix_socket_directories` | Unix socket directories |
| `listen_addresses` | Listen addresses |
| `bonjour` | Bonjour service discovery |
| `ssl` | SSL/TLS mode |

All other parameters are reload-safe and do not require a restart.

### Rolling Restart Behavior

When a rolling restart is triggered (by changing restart-required parameters), the operator restarts pods in a safe order to minimize downtime:

1. **Mirrors** — Mirror segments are restarted first (lowest impact)
2. **Primaries** — Primary segments are restarted next
3. **Standby** — The standby coordinator is restarted
4. **Coordinator** — The coordinator is restarted last

The rolling restart state is tracked via the `avsoft.io/rolling-restart` annotation, which contains a JSON payload:

```json
{
  "phase": "primaries",
  "startedAt": "2026-05-14T10:00:00Z",
  "restartParams": ["shared_buffers", "max_connections"]
}
```

The `phase` field progresses through: `mirrors` → `primaries` → `standby` → `coordinator` → `completed`.

### Reloading Configuration

For parameters that do not require a restart:

```bash
cloudberry-ctl config reload --cluster my-cluster
```

The operator automatically detects whether a parameter change requires a restart or can be applied via reload. Restart-required changes trigger a rolling restart.

### Resetting Parameters

```bash
cloudberry-ctl config reset --cluster my-cluster --param work_mem
```

### Managing HBA Rules

```bash
# List current rules
cloudberry-ctl config hba list --cluster my-cluster

# Update rules from file
cloudberry-ctl config hba update --cluster my-cluster -f hba-rules.yaml

# View change history
cloudberry-ctl config hba history --cluster my-cluster
```

Example `hba-rules.yaml`:

```yaml
rules:
  - type: local
    database: all
    user: gpadmin
    method: trust
  - type: host
    database: all
    user: all
    address: "10.0.0.0/8"
    method: scram-sha-256
  - type: host
    database: all
    user: all
    address: "0.0.0.0/0"
    method: reject
```

## Authentication Setup

### Default HBA Rules

When you deploy a `CloudberryCluster` without specifying `auth.hbaRules`, the operator automatically generates a secure set of default `pg_hba.conf` rules:

```
local   all   gpadmin                 trust
local   all   all                     scram-sha-256
host    all   gpadmin   127.0.0.1/32  trust
host    all   all       0.0.0.0/0     scram-sha-256
host    replication  all  0.0.0.0/0   scram-sha-256
```

These defaults provide a balance of convenience and security:

| Connection | User | Auth Method | Behavior |
|------------|------|-------------|----------|
| Local (Unix socket) | `gpadmin` | `trust` | No password required — enables operator-internal management |
| Local (Unix socket) | All other users | `scram-sha-256` | Password required |
| Remote (`127.0.0.1`) | `gpadmin` | `trust` | Localhost loopback trusted for admin |
| Remote (any IP) | All users | `scram-sha-256` | Password required for all remote connections |
| Replication (any IP) | All users | `scram-sha-256` | Password required for streaming replication |

The same defaults are generated when `hbaRules` is set to an empty array (`hbaRules: []`).

**Overriding defaults**: When you provide explicit `hbaRules` in the CRD spec, the defaults are replaced entirely. Only your custom rules appear in the generated `pg_hba.conf`. For example:

```yaml
spec:
  auth:
    hbaRules:
      - type: local
        database: all
        user: gpadmin
        method: trust
      - type: host
        database: all
        user: all
        address: "10.0.0.0/8"
        method: scram-sha-256
      - type: host
        database: all
        user: all
        address: "0.0.0.0/0"
        method: reject
```

> **Note**: The default HBA rules behavior is verified by Scenario 45 (see `test/functional/scenario45_hba_defaults_test.go` and `test/e2e/scenario45_hba_defaults_e2e_test.go`).

### Basic Authentication

Basic auth is enabled by default. The operator uses **bcrypt** for password hashing, providing strong protection against brute-force attacks.

**Automatic admin password creation**: The operator automatically creates an admin password Secret (`{cluster}-admin-password`) if one does not exist when the cluster is created. The password is injected into the coordinator pod via a `SecretKeyRef` environment variable.

**Custom admin password**: To use a specific password, create the Secret before deploying the cluster:

```bash
kubectl create secret generic cloudberry-admin-password \
  -n cloudberry-test \
  --from-literal=password='your-secure-password'
```

Reference it in the CRD:

```yaml
spec:
  auth:
    basic:
      enabled: true
      adminUser: gpadmin
      adminPasswordSecret:
        name: cloudberry-admin-password
        key: password
```

> **Note**: Passwords are stored as bcrypt hashes internally. The `InMemoryCredentialStore` used by the API server hashes passwords with `bcrypt.DefaultCost` (10). Passwords longer than 72 bytes are truncated by bcrypt — keep admin passwords under this limit.

### Password Rotation

```bash
cloudberry-ctl auth rotate-password --cluster my-cluster
```

This updates the Kubernetes Secret, database role password, and Vault secret (if enabled).

### OIDC Redirect Protection

The OIDC provider's HTTP client limits redirects to 5 hops during OIDC discovery and token exchange. This prevents infinite redirect loops when the identity provider misconfigures its endpoints. If you encounter `stopped after 5 redirects` errors, verify that the `issuerURL` is correct and the OIDC provider's `.well-known/openid-configuration` endpoint is accessible without excessive redirects.

### OIDC Authentication (Keycloak)

1. **Configure Keycloak** with a realm, client, and roles:
   - Realm: `cloudberry`
   - Client: `cloudberry-operator` (confidential)
   - Roles: `admin`, `operator`, `operator-basic`, `user`, `reader`

2. **Create the client secret**:

```bash
kubectl create secret generic oidc-client-secret \
  -n cloudberry-test \
  --from-literal=client-secret='your-oidc-secret'
```

3. **Configure the CRD**:

```yaml
spec:
  auth:
    oidc:
      enabled: true
      issuerURL: https://keycloak.auth-system/realms/cloudberry
      clientID: cloudberry-operator
      clientSecret:
        secretRef:
          name: oidc-client-secret
          key: client-secret
      scopes:
        - openid
        - profile
        - email
      roleClaimPath: "realm_access.roles"
      roleClaimSource: id_token
      roleMatchMode: exact
      roleMapping:
        admin: Admin
        operator: Operator
        operator-basic: "Operator Basic"
        user: Basic
        reader: "Self Only"
      pkce: true
      allowLocalSignIn: true
```

4. **Authenticate with cloudberry-ctl**:

```bash
# OIDC login (opens browser)
cloudberry-ctl auth login --cluster my-cluster

# Basic auth login
cloudberry-ctl auth login --cluster my-cluster --basic --username admin

# Check auth status
cloudberry-ctl auth status --cluster my-cluster
```

### OIDC Full Flow Setup with Keycloak

This section provides step-by-step instructions for setting up a complete OIDC authentication flow with Keycloak, including per-user role mapping to all 5 permission levels and service account support. This setup was verified end-to-end on a real cluster (Scenario 41).

#### Step 1: Configure the Keycloak Realm

Create a Keycloak realm with the required clients, roles, and users:

```bash
# Create the realm (e.g., "test" or "cloudberry")
# Set the Frontend URL to match the operator's issuerURL
# Example: http://host.docker.internal:8090 (for Docker Desktop)
```

**Realm settings:**

| Setting | Value | Notes |
|---------|-------|-------|
| Realm name | `test` (or `cloudberry`) | Must match the path in `issuerURL` |
| Frontend URL | `http://host.docker.internal:8090` | Must match the operator's `issuerURL` so the `iss` claim in tokens is correct |

#### Step 2: Create the Keycloak Client

Create a confidential client for the operator:

| Setting | Value |
|---------|-------|
| Client ID | `cloudberry-operator` |
| Client Protocol | `openid-connect` |
| Access Type | `confidential` |
| Service Accounts Enabled | `true` |
| Direct Access Grants Enabled | `true` |

**Critical: Add an audience mapper** to include `cloudberry-operator` in the `aud` claim of issued tokens. Without this, JWT audience validation fails with `401 Unauthorized`.

1. Go to the client's **Mappers** tab
2. Create a new mapper:
   - Name: `audience-mapper`
   - Mapper Type: `Audience`
   - Included Client Audience: `cloudberry-operator`
   - Add to ID token: `ON`
   - Add to access token: `ON`

#### Step 3: Create Realm Roles

Create 5 realm roles that map to the operator's permission levels:

| Realm Role | Maps To | Permission Level |
|------------|---------|-----------------|
| `admin` | Admin | Full access — user management, security config, cluster lifecycle |
| `operator` | Operator | Cluster operations — start/stop, config changes, maintenance |
| `operator-basic` | Operator Basic | Basic operations — view all sessions, view configurations |
| `user` | Basic | View cluster state — read cluster status, view dashboards |
| `reader` | Self Only | View own queries and sessions only |

#### Step 4: Create Test Users

Create users and assign one role to each:

| Username | Email | Realm Role |
|----------|-------|------------|
| `admin-user` | `admin-user@test.local` | `admin` |
| `operator-user` | `operator-user@test.local` | `operator` |
| `opbasic-user` | `opbasic-user@test.local` | `operator-basic` |
| `basic-user` | `basic-user@test.local` | `user` |
| `reader-user` | `reader-user@test.local` | `reader` |

Set a password for each user (e.g., `password`) with "Temporary" set to `OFF`.

#### Step 5: Assign Roles to the Service Account

For service account (client_credentials) support, assign the `admin` role to the service account:

1. Go to the client's **Service Account Roles** tab
2. Add the `admin` realm role

#### Step 6: Create the Client Secret in Kubernetes

```bash
# Get the client secret from Keycloak (Client → Credentials tab)
kubectl create secret generic oidc-client-secret \
  -n cloudberry-test \
  --from-literal=client-secret='<your-keycloak-client-secret>'
```

#### Step 7: Configure the Cluster CR

```yaml
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: my-cluster
  namespace: cloudberry-test
spec:
  image: "cloudberrydb/cloudberry:2.1.0"
  coordinator:
    storage:
      size: "5Gi"
  segments:
    count: 2
    storage:
      size: "5Gi"
  auth:
    basic:
      enabled: true
      adminUser: gpadmin
    oidc:
      enabled: true
      issuerURL: http://host.docker.internal:8090/realms/test
      clientID: cloudberry-operator
      clientSecret:
        secretRef:
          name: oidc-client-secret
          key: client-secret
      scopes: [openid, profile, email]
      roleClaimPath: "realm_access.roles"
      roleClaimSource: id_token
      roleMatchMode: exact
      roleMapping:
        admin: Admin
        operator: Operator
        operator-basic: "Operator Basic"
        user: Basic
        reader: "Self Only"
      pkce: true
      allowLocalSignIn: true
```

#### Step 8: Verify the Setup

**Obtain a token for each user** using the password grant:

```bash
# Get a token for admin-user
TOKEN=$(curl -s -X POST \
  "http://host.docker.internal:8090/realms/test/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=cloudberry-operator" \
  -d "client_secret=<your-client-secret>" \
  -d "username=admin-user" \
  -d "password=password" \
  | jq -r '.access_token')

# Call the operator API with the OIDC token
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/api/v1alpha1/clusters
```

**Obtain a service account token** using client_credentials:

```bash
SA_TOKEN=$(curl -s -X POST \
  "http://host.docker.internal:8090/realms/test/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=cloudberry-operator" \
  -d "client_secret=<your-client-secret>" \
  | jq -r '.access_token')

curl -H "Authorization: Bearer $SA_TOKEN" \
  http://localhost:8090/api/v1alpha1/clusters
```

**Verify Basic auth still works** alongside OIDC:

```bash
curl -u gpadmin:admin-password \
  http://localhost:8090/api/v1alpha1/clusters
```

#### Expected Results

Each user should receive the correct permission level based on their Keycloak realm role:

| User | Role | Expected Permission | HTTP Status |
|------|------|---------------------|-------------|
| `admin-user` | `admin` | Admin | 200 |
| `operator-user` | `operator` | Operator | 200 |
| `opbasic-user` | `operator-basic` | Operator Basic | 200 |
| `basic-user` | `user` | Basic | 200 |
| `reader-user` | `reader` | Self Only | 200 (list own), 403 (admin ops) |
| Service account | `admin` | Admin | 200 |
| Basic auth (gpadmin) | — | Admin | 200 |

**Operator log entries** confirm the authentication flow:

```
"OIDC auth succeeded" username=admin-user email=admin-user@test.local roles=[admin] permission=Admin
"OIDC auth succeeded" username=service-account-cloudberry-operator roles=[admin] permission=Admin
"basic auth succeeded" username=gpadmin permission=Admin
```

#### Troubleshooting

| Symptom | Cause | Resolution |
|---------|-------|------------|
| `401 Unauthorized` on all Bearer requests | Audience mapper missing | Add `oidc-audience-mapper` to the Keycloak client that includes `cloudberry-operator` in the `aud` claim |
| `401 Unauthorized` with "issuer mismatch" | Frontend URL mismatch | Set the Keycloak realm's Frontend URL to match the operator's `issuerURL` exactly |
| Token accepted but wrong permission | Role not assigned | Verify the user has the correct realm role assigned in Keycloak |
| Service account gets Self Only | Service account role missing | Assign the desired realm role to the client's service account (Service Account Roles tab) |
| `stopped after 5 redirects` | Incorrect issuer URL | Verify the `issuerURL` is correct and the `.well-known/openid-configuration` endpoint is accessible |

> **Note**: The OIDC full flow is verified by Scenario 41. See `test/functional/scenario41_oidc_full_flow_test.go` and `test/e2e/scenario41_oidc_full_flow_e2e_test.go` for the full test suite.

### Dual-Mode Authentication (Basic + OIDC)

The operator supports running both Basic and OIDC authentication simultaneously. When both providers are enabled, the auth middleware inspects the `Authorization` header to route each request to the correct provider:

| Header Prefix | Provider | Identity.AuthMethod |
|---------------|----------|---------------------|
| `Basic ...` | Basic auth provider | `basic` |
| `Bearer ...` | OIDC/JWT provider | `oidc` |
| *(missing)* | — (rejected) | — |
| Other (e.g., `Digest`) | — (rejected) | — |

Requests without a recognized `Authorization` header receive a `401 Unauthorized` response in JSON format.

**Enabling dual-mode auth**: Set both `auth.basic.enabled: true` and `auth.oidc.enabled: true` in the cluster spec:

```yaml
spec:
  auth:
    basic:
      enabled: true
      adminUser: gpadmin
      adminPasswordSecret:
        name: cloudberry-admin-password
        key: password
    oidc:
      enabled: true
      issuerURL: https://keycloak.auth-system/realms/cloudberry
      clientID: cloudberry-operator
      clientSecret:
        secretRef:
          name: oidc-client-secret
          key: client-secret
      roleMapping:
        admin: Admin
        operator: Operator
        operator-basic: "Operator Basic"
        user: Basic
        reader: "Self Only"
```

**How it works**:

1. The middleware extracts the `Authorization` header from each incoming request
2. If the header starts with `Basic `, the request is routed to the Basic auth provider, which validates credentials against the in-memory credential store (backed by the admin password Secret and database roles)
3. If the header starts with `Bearer `, the request is routed to the OIDC provider, which validates the JWT signature, issuer, audience, and expiry, then maps role claims to permission levels
4. Both providers return an `Identity` object with the authenticated user's `Username`, `AuthMethod` (`"basic"` or `"oidc"`), and `PermissionLevel`
5. The `Identity` is stored in the request context for downstream permission enforcement

**Permission levels** are resolved independently by each provider:

- **Basic auth**: Permission is determined by the credential store entry (admin user gets `Admin`, database roles are mapped based on role membership)
- **OIDC auth**: Permission is determined by mapping JWT role claims through the `roleMapping` configuration

Both providers share the same permission hierarchy: `Self Only` < `Basic` < `Operator Basic` < `Operator` < `Admin`. Permission enforcement middleware works identically regardless of which provider authenticated the request.

**Keycloak configuration requirements**: For OIDC to work end-to-end with a real Keycloak instance, you must configure the following:

1. **Audience mapper**: Add a protocol mapper (type: `oidc-audience-mapper`) to the Keycloak realm that includes the operator's `clientID` in the `aud` claim of issued tokens. Without this, JWT audience validation fails with `401 Unauthorized`.
2. **Frontend URL**: Set the Keycloak realm's `frontendUrl` to match the operator's configured `issuerURL`. This ensures the `iss` claim in issued tokens matches what the operator expects during JWT validation.
3. **Role assignment**: Assign appropriate realm roles (e.g., `admin`, `operator`, `user`, `reader`) to service accounts and users. These roles must correspond to the `roleMapping` entries in the cluster CR.

**Real-cluster verification results** (10/10 PASS):

| # | Test | HTTP Status | Result |
|---|------|-------------|--------|
| 1 | Basic Auth (valid admin) → routed to Basic provider | 200 | PASS |
| 2 | Basic Auth (invalid password) | 401 | PASS |
| 3 | No Auth Header | 401 | PASS |
| 4 | Bearer Auth (Keycloak service account JWT) → routed to OIDC provider | 200 | PASS |
| 5 | Bearer Auth (Keycloak user password-grant JWT) → routed to OIDC provider | 200 | PASS |
| 6 | Unsupported Auth Type (Digest) | 401 | PASS |
| 7 | Health /healthz (no auth) | 200 | PASS |
| 8 | Health /readyz (no auth) | 200 | PASS |
| 9 | Bearer Auth (invalid token) | 401 | PASS |
| 10 | Dual-auth cluster CR phase = Running | Running | PASS |

> **Note**: Dual-mode auth behavior is verified by Scenario 38. See `test/functional/scenario38_dual_auth_test.go` and `test/e2e/scenario38_dual_auth_e2e_test.go` for the full test suite.

### SSL/TLS Configuration

The operator supports SSL/TLS encryption for PostgreSQL connections. When SSL is enabled, the operator configures `postgresql.conf` with SSL parameters and mounts TLS certificates into all StatefulSets (coordinator, standby, primary segments, and mirror segments).

Two certificate sources are supported:

| Source | Description | Use Case |
|--------|-------------|----------|
| **Kubernetes Secret** | TLS certificates stored in a `kubernetes.io/tls` Secret | Standard deployments, manual cert management |
| **Vault PKI** | Certificates issued by HashiCorp Vault PKI engine | Production environments with automated cert lifecycle |

#### K8s Secret Source

1. **Create a TLS Secret** with the server certificate, private key, and CA certificate:

```bash
# Option A: Using kubectl create secret tls (tls.crt and tls.key only)
kubectl create secret tls cloudberry-tls \
  -n cloudberry-test \
  --cert=server.crt \
  --key=server.key

# Option B: Add the CA certificate to the Secret
kubectl create secret generic cloudberry-tls \
  -n cloudberry-test \
  --from-file=tls.crt=server.crt \
  --from-file=tls.key=server.key \
  --from-file=ca.crt=ca.crt
```

2. **Enable SSL in the CRD** with the cert Secret reference and minimum TLS version:

```yaml
spec:
  auth:
    ssl:
      enabled: true
      certSecret:
        name: cloudberry-tls
      minTLSVersion: "1.2"
```

3. **Use `hostssl` HBA rules** to require SSL for remote connections:

```yaml
spec:
  auth:
    ssl:
      enabled: true
      certSecret:
        name: cloudberry-tls
      minTLSVersion: "1.2"
    hbaRules:
      - type: hostssl
        database: all
        user: all
        address: "0.0.0.0/0"
        method: scram-sha-256
      - type: local
        database: all
        user: gpadmin
        method: trust
```

When SSL is enabled, the operator generates the following `postgresql.conf` settings:

```
ssl = on
ssl_cert_file = '/tls/tls.crt'
ssl_key_file = '/tls/tls.key'
ssl_ca_file = '/tls/ca.crt'
ssl_min_protocol_version = 'TLSv1.2'
```

The TLS certificates are made available at `/tls` on all StatefulSets (coordinator, standby, primary segments, and mirror segments) using a two-volume approach with an init container.

**TLS key permissions handling**: PostgreSQL requires the private key file (`tls.key`) to have permissions `0600` (owner read/write only). Kubernetes Secret volumes mount files as symlinks with `0777` permissions, which PostgreSQL rejects. To solve this, the operator uses the following approach:

1. The TLS Secret is mounted at `/tls-secret` (read-only)
2. An EmptyDir volume is mounted at `/tls`
3. An `init-tls` init container copies the certificate files from `/tls-secret` to `/tls` with correct permissions:
   - `tls.key`: `0600` (owner read/write only)
   - `tls.crt` and `ca.crt`: `0644` (world readable)
   - All files owned by `gpadmin:gpadmin` (UID 1000)

This is handled automatically by the operator — no manual configuration is required.

**Supported `minTLSVersion` values:**

| Value | PostgreSQL Setting | Description |
|-------|-------------------|-------------|
| `"1.2"` | `TLSv1.2` | TLS 1.2 minimum (recommended) |
| `"1.3"` | `TLSv1.3` | TLS 1.3 minimum (strongest) |

#### Vault PKI Source

For production environments, use Vault's PKI secrets engine to issue and automatically rotate TLS certificates:

```yaml
spec:
  auth:
    ssl:
      enabled: true
      certSecret:
        name: cloudberry-vault-tls
      minTLSVersion: "1.2"
  vault:
    enabled: true
    address: http://vault:8200
    authMethod: token
    secretPath: secret/data/cloudberry
```

Configure the webhook certificate source to use Vault PKI in your Helm values:

```yaml
webhook:
  enabled: true
  certSource: vault-pki
  vaultPKI:
    mountPath: pki
    role: cloudberry-operator
```

Vault PKI issues certificates with the following fields:

| Field | Description |
|-------|-------------|
| `certificate` | PEM-encoded server certificate |
| `private_key` | PEM-encoded server private key |
| `issuing_ca` | PEM-encoded CA certificate |
| `serial_number` | Certificate serial number |

**Certificate rotation**: Certificates are automatically rotated when 2/3 of their lifetime has elapsed. The rotation check runs every 12 hours. After rotation, the new CA bundle is injected into the webhook configurations.

#### Verifying SSL Configuration

```bash
# Check the postgresql.conf ConfigMap for SSL settings
kubectl get configmap <cluster>-postgresql-conf -n cloudberry-test \
  -o jsonpath='{.data.postgresql\.conf}' | grep ssl

# Verify the TLS volume is mounted on the coordinator StatefulSet
kubectl get statefulset <cluster>-coordinator -n cloudberry-test \
  -o jsonpath='{.spec.template.spec.volumes[?(@.name=="tls")]}'

# Verify the TLS Secret exists
kubectl get secret cloudberry-tls -n cloudberry-test

# Check the pg_hba.conf for hostssl rules
kubectl get configmap <cluster>-pg-hba-conf -n cloudberry-test \
  -o jsonpath='{.data.pg_hba\.conf}' | grep hostssl
```

> **Note**: SSL/TLS configuration is verified by Scenario 47. See `test/functional/scenario47_ssl_tls_test.go` and `test/e2e/scenario47_ssl_tls_e2e_test.go` for the full test suite.

### Role Management

```bash
# Basic auth login
cloudberry-ctl auth login --cluster my-cluster --basic --username admin

# Check auth status
cloudberry-ctl auth status --cluster my-cluster
```

> **Security note**: Avoid passing passwords via the `--password` CLI flag, as they may be visible in shell history and process listings. Use the `CLOUDBERRY_PASSWORD` environment variable instead:
>
> ```bash
> export CLOUDBERRY_PASSWORD='your-secure-password'
> cloudberry-ctl auth login --cluster my-cluster --basic --username admin
> ```

## Webhook Certificate Setup

The operator's admission webhooks (validating and mutating) require TLS certificates. The operator manages these certificates automatically using one of two strategies: self-signed generation or Vault PKI issuance.

### Self-Signed Certificates (Default)

No configuration is needed. The operator generates a self-signed CA and server certificate on startup, stores them in a Kubernetes Secret, and injects the CA bundle into the webhook configurations.

**Certificate properties:**

| Component | Algorithm | Validity | Constraints |
|-----------|-----------|----------|-------------|
| CA certificate | ECDSA P-256 | 10 years | CA:TRUE, pathlen:0 |
| Server certificate | ECDSA P-256 | 1 year | CA:FALSE |

The server certificate includes the following SANs:
- `{service}.{namespace}.svc`
- `{service}.{namespace}.svc.cluster.local`

**Automatic rotation**: Certificates are checked for rotation every 12 hours and automatically rotated when 2/3 of their lifetime has elapsed. After rotation, the new CA bundle is re-injected into both validating and mutating webhook configurations.

**Helm auto-generation**: When using the Helm chart, the following values are auto-generated if left empty:
- `certSecretName`: `{release}-webhook-certs`
- `serviceName`: `{release}-webhook`
- `caBundle`: Left empty to trigger runtime injection by the operator

### Vault PKI Certificates

For production environments, use Vault's PKI secrets engine for trusted certificate issuance:

```yaml
# In your Helm values
webhook:
  enabled: true
  certSource: vault-pki
  vaultPKI:
    mountPath: pki
    role: cloudberry-operator

vault:
  enabled: true
  address: http://vault:8200
```

The operator authenticates to Vault using the configured auth method (token, kubernetes, or approle) and requests certificates from `{vaultPKI.mountPath}/issue/{vaultPKI.role}`.

**Certificate request parameters:**

| Parameter | Value |
|-----------|-------|
| Common Name | `{webhookServiceName}.{namespace}.svc` |
| SANs | `{webhookServiceName}.{namespace}.svc`, `{webhookServiceName}.{namespace}.svc.cluster.local` |
| Format | PEM |
| TTL | From Vault role configuration |

Ensure the Vault PKI role allows issuing certificates for the webhook service DNS names.

#### Kubernetes Auth for Vault PKI (Production)

For production deployments, use Kubernetes service account authentication instead of static tokens. The operator exchanges its service account JWT for a Vault token via the `auth/kubernetes/login` endpoint.

**Step 1: Configure the Vault Kubernetes auth backend**

```bash
# Enable the Kubernetes auth method in Vault
vault auth enable kubernetes

# Create a dedicated service account for Vault auth
kubectl create serviceaccount vault-auth -n cloudberry-system

# Grant the service account permission to validate tokens
kubectl create clusterrolebinding vault-auth-delegator \
  --clusterrole=system:auth-delegator \
  --serviceaccount=cloudberry-system:vault-auth

# Get the service account token and CA cert
SA_SECRET=$(kubectl get sa vault-auth -n cloudberry-system \
  -o jsonpath='{.secrets[0].name}')
SA_JWT=$(kubectl create token vault-auth -n cloudberry-system)
K8S_CA=$(kubectl config view --raw --minify --flatten \
  -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d)

# Configure Vault with the Kubernetes API details
vault write auth/kubernetes/config \
  kubernetes_host="https://kubernetes.docker.internal:6443" \
  token_reviewer_jwt="$SA_JWT" \
  kubernetes_ca_cert="$K8S_CA" \
  disable_iss_validation=true \
  disable_local_ca_jwt=true
```

> **Docker Desktop hostname requirement**: You must use `kubernetes.docker.internal` as the `kubernetes_host` (not `host.docker.internal`). The Kubernetes API server certificate only includes `kubernetes.docker.internal` as a SAN. Using `host.docker.internal` causes TLS verification failures during TokenReview API calls, and the operator fails to authenticate with Vault.

**Step 2: Create a Vault role for the operator**

```bash
vault write auth/kubernetes/role/cloudberry-operator \
  bound_service_account_names=cloudberry-operator \
  bound_service_account_namespaces=cloudberry-system \
  policies=default,cloudberry-pki \
  ttl=1h
```

**Step 3: Create a Vault PKI role**

```bash
vault write pki/roles/cloudberry-operator \
  allow_any_name=true \
  max_ttl=720h
```

**Step 4: Deploy the operator with Kubernetes auth**

```yaml
# In your Helm values
vault:
  enabled: true
  address: http://vault:8200
  authMethod: kubernetes
  authPath: auth/kubernetes
  role: cloudberry-operator

webhook:
  enabled: true
  certSource: vault-pki
  vaultPKI:
    mountPath: pki
    role: cloudberry-operator
```

**Verification**: After deployment, check the operator logs for successful Kubernetes auth:

```bash
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.msg | test("authenticated|vault|kubernetes"))'

# Expected log entries:
# {"msg": "authenticated with vault using kubernetes method", "role": "cloudberry-operator"}
# {"msg": "webhook certificates ensured", "certSource": "vault-pki"}
```

**Environment variables** set by the Helm chart for Kubernetes auth:

| Variable | Value |
|----------|-------|
| `CLOUDBERRY_VAULT_AUTH_METHOD` | `kubernetes` |
| `CLOUDBERRY_VAULT_AUTH_PATH` | `auth/kubernetes` |
| `CLOUDBERRY_VAULT_ROLE` | `cloudberry-operator` |
| `CLOUDBERRY_VAULT_ADDRESS` | `http://vault:8200` |

**Environment variables** injected into the operator pod:

| Variable | Description |
|----------|-------------|
| `CLOUDBERRY_WEBHOOK_CERT_SOURCE` | Certificate source (`self-signed` or `vault-pki`) |
| `CLOUDBERRY_WEBHOOK_CERT_SECRET_NAME` | Secret name for storing certificates |
| `CLOUDBERRY_WEBHOOK_SERVICE_NAME` | Webhook service name for SAN generation |
| `CLOUDBERRY_WEBHOOK_VAULT_PKI_MOUNT` | Vault PKI mount path (vault-pki only) |
| `CLOUDBERRY_WEBHOOK_VAULT_PKI_ROLE` | Vault PKI role name (vault-pki only) |

### Verifying Webhook Certificates

```bash
# Check the certificate Secret
kubectl get secret -n cloudberry-system -l app.kubernetes.io/component=webhook-certs

# Verify the Secret contains all required keys
kubectl get secret <release>-webhook-certs -n cloudberry-system \
  -o jsonpath='{.data}' | jq 'keys'
# Expected: ["ca.crt", "tls.crt", "tls.key"]

# Inspect the server certificate
kubectl get secret <release>-webhook-certs -n cloudberry-system \
  -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -text -noout

# Verify the webhook configuration has a CA bundle
kubectl get validatingwebhookconfigurations -o jsonpath='{.items[*].webhooks[*].clientConfig.caBundle}' | head -c 50

# Check the CA certificate properties
kubectl get secret <release>-webhook-certs -n cloudberry-system \
  -o jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -text -noout
```

### Troubleshooting Webhook Certificates

If webhook calls fail with TLS errors:

1. **Check that the certificate Secret exists and contains valid data:**

   ```bash
   kubectl get secret <release>-webhook-certs -n cloudberry-system \
     -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -text -noout
   ```

   Verify the certificate has not expired and the SANs match the webhook service name.

2. **Verify the CA bundle in the webhook configuration matches the CA in the Secret:**

   ```bash
   kubectl get validatingwebhookconfiguration <release>-validating \
     -o jsonpath='{.webhooks[0].clientConfig.caBundle}' | base64 -d | openssl x509 -text -noout
   ```

3. **If using Vault PKI**, ensure the Vault server is reachable and the PKI role is properly configured:

   ```bash
   # Check operator logs for Vault connection errors
   kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
     jq 'select(.msg | test("vault|cert|webhook"))'
   ```

4. **Check that environment variables are set correctly** (common issue when Vault PKI is configured):

   ```bash
   kubectl get deployment -n cloudberry-system cloudberry-operator \
     -o jsonpath='{.spec.template.spec.containers[0].env[*]}' | jq .
   ```

   All `CLOUDBERRY_WEBHOOK_*` and `CLOUDBERRY_VAULT_*` variables must be populated. If they are empty, verify the Helm values and check that viper defaults are configured (see Bug Fix 2 below).

5. **Known issues fixed in Scenario 48:**

   - **Vault client nil error**: If the operator logs show "vault client is not enabled" when using `certSource=vault-pki`, ensure you are running a version that includes the vault client wiring fix in `setupWebhookCerts()`.
   - **Empty environment variables**: If `CLOUDBERRY_VAULT_ADDRESS` or `CLOUDBERRY_VAULT_TOKEN` are empty despite being set in Helm values, ensure you are running a version that includes the missing viper defaults fix in `internal/config/config.go`.

> **Note**: Webhook certificate management is verified by Scenario 48. See `test/functional/scenario48_webhook_certs_test.go` and `test/e2e/scenario48_webhook_certs_e2e_test.go` for the full test suite.

## High Availability

### Segment Mirroring

#### Checking Mirroring Status

```bash
cloudberry-ctl ha mirroring status --cluster my-cluster
```

#### Enabling Mirroring

```bash
# Enable with group layout (default)
cloudberry-ctl ha mirroring enable --cluster my-cluster

# Enable with spread layout
cloudberry-ctl ha mirroring enable --cluster my-cluster --layout spread
```

Or update the CRD:

```yaml
spec:
  segments:
    mirroring:
      enabled: true
      layout: spread  # or "group"
```

**Group layout**: All mirrors for one host's primaries go to one other host. Simpler but a single host failure loses all its mirrors.

**Spread layout**: Mirrors are distributed across multiple hosts. Better fault tolerance but requires more hosts than `primariesPerHost`.

### Automatic Segment Failover

When segment mirroring is enabled, the operator automatically detects primary segment failures and triggers Cloudberry's internal failover mechanism to promote mirrors to the primary role. The cluster remains available during and after failover.

> **Prerequisite**: Automatic failover requires mirroring to be enabled (`spec.segments.mirroring.enabled: true`). Without mirroring, failed primary segments are reported in `status.failedSegments` but no automatic promotion occurs.

#### How FTS Detects Failures

The Fault Tolerance Service (FTS) probe runs on every HA reconciliation cycle (controlled by `ha.ftsProbeInterval`). For each probe cycle, the operator:

1. Connects to the coordinator database via the `DBClientFactory`
2. Queries `gp_segment_configuration` to retrieve the status of all segments
3. If the query fails, retries up to `FTSProbeRetries` times with `FTSProbeTimeout` per attempt
4. Analyzes the returned segment statuses — any segment with status `d` (down) is flagged

**Retry behavior**: Each probe attempt uses a dedicated context with the configured timeout. If an attempt fails, the operator logs a warning and retries. If all attempts fail, the probe reports an error and retries on the next reconciliation cycle.

**Default FTS settings and detection timeline**:

| Setting | Default | Description |
|---------|---------|-------------|
| `ftsProbeInterval` | `60s` | Seconds between FTS probe cycles |
| `ftsProbeTimeout` | `20s` | Timeout per probe attempt |
| `ftsProbeRetries` | `5` | Number of retry attempts before declaring failure |

With default settings, a segment failure is detected within approximately **60 seconds** (one probe interval). With aggressive settings (`ftsProbeInterval=5`, `ftsProbeTimeout=5`, `ftsProbeRetries=3`), detection can occur in approximately **15 seconds**.

Configure FTS settings in the CRD:

```yaml
spec:
  ha:
    ftsProbeInterval: 10   # probe every 10 seconds
    ftsProbeTimeout: 5     # 5 seconds per attempt
    ftsProbeRetries: 3     # 3 retries before marking down
```

Or via CLI:

```bash
cloudberry-ctl ha fts configure --cluster my-cluster \
  --probe-interval 10 \
  --probe-timeout 5 \
  --probe-retries 3
```

#### What Happens During Automatic Failover

When the FTS probe detects one or more primary segments as down and mirroring is enabled, the operator performs the following steps:

1. **Trigger Cloudberry FTS scan**: The operator calls `TriggerFTSProbe()` on the coordinator database, which initiates Cloudberry's internal failover mechanism. Cloudberry promotes the corresponding mirror segment to the primary role
2. **Verify promotion**: The operator re-reads `gp_segment_configuration` to confirm the mirror has been promoted. It checks whether a different DBID now holds the primary role for the affected content ID
3. **Emit events**: A `SegmentFailover` warning event is emitted for each failed primary segment, including the content ID, original primary hostname, and new primary hostname (if promotion succeeded)
4. **Update metrics**: The `cloudberry_fts_failover_total` counter is incremented once per failover event. Per-segment status metrics are updated via `SetSegmentStatus()`
5. **Update status**: `status.failedSegments` is populated with the details of each failed segment, and `status.mirroringStatus` transitions to `MirroringDegraded`

> **Note**: If the `TriggerFTSProbe()` call fails (e.g., coordinator is unreachable), the operator still emits `SegmentFailover` events and updates status based on the originally detected failures. The failover is retried on the next probe cycle.

#### Monitoring Failover

**Kubernetes events**:

```bash
# Watch for failover events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -E 'SegmentFailover|MirroringDegraded'
```

| Event | Type | Description |
|-------|------|-------------|
| `SegmentFailover` | Warning | Primary segment failed; includes content ID, original and new primary hostnames |
| `MirroringDegraded` | Warning | One or more segments are down; includes count of failed segments |

**Prometheus metrics**:

```promql
# Total failover events
cloudberry_fts_failover_total{cluster="my-cluster"}

# Total FTS probes by result (success, failure, degraded)
cloudberry_fts_probe_total{cluster="my-cluster"}

# Number of currently failed segments
cloudberry_segments_failed{cluster="my-cluster"}

# Mirroring sync status (0 = degraded, 1 = in sync)
cloudberry_mirroring_in_sync{cluster="my-cluster"}

# Replication lag per segment (bytes)
cloudberry_replication_lag_bytes{cluster="my-cluster"}
```

**Cluster status**:

```bash
# Check mirroring status
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.mirroringStatus}'
# Output: Degraded

# Check failed segments
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.failedSegments}' | jq .

# Check via CLI
cloudberry-ctl ha mirroring status --cluster my-cluster
```

#### Post-Failover State

After a successful failover:

- **`status.mirroringStatus`**: Transitions to `MirroringDegraded` — the cluster is operational but running without full redundancy for the affected segments
- **`status.failedSegments`**: Contains the list of segments that failed, with their content ID, hostname, role, and status
- **Cluster availability**: The cluster remains available for reads and writes. The promoted mirror now serves as the primary for the affected content ID
- **Segment roles**: The original primary is marked as down (`d`), and the former mirror is now the primary (`p`). There is no mirror for the affected content ID until recovery is performed

**Example `status.failedSegments`**:

```json
[
  {
    "contentID": 0,
    "hostname": "my-cluster-segment-primary-0",
    "role": "p",
    "status": "d"
  }
]
```

#### Recovering After Failover

After failover, you should recover the failed segment to restore full redundancy. Use the recovery annotation or CLI:

```bash
# Incremental recovery (preferred when data is intact)
cloudberry-ctl ha recovery start --cluster my-cluster --type incremental

# Full recovery (when data is corrupted)
cloudberry-ctl ha recovery start --cluster my-cluster --type full
```

Or via annotation:

```bash
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/recovery=incremental
```

After recovery completes, rebalance segments to restore original roles:

```bash
cloudberry-ctl ha rebalance --cluster my-cluster
```

Once all segments are healthy, `status.mirroringStatus` returns to `InSync`, `status.failedSegments` is cleared, and the `cloudberry_mirroring_in_sync` metric returns to `1`.

#### Troubleshooting Failover Issues

| Symptom | Possible Cause | Resolution |
|---------|---------------|------------|
| No `SegmentFailover` events | Mirroring not enabled | Enable mirroring: `spec.segments.mirroring.enabled: true` |
| `SegmentFailover` event but mirror not promoted | Cloudberry FTS scan failed | Check coordinator logs; the operator retries on the next probe cycle |
| `MirroringDegraded` persists after recovery | Recovery not completed | Run `cloudberry-ctl ha recovery status --cluster my-cluster` to check progress |
| `failedSegments` not clearing | Segment pod still down | Check pod status: `kubectl get pods -l avsoft.io/cluster=my-cluster` |
| FTS probe errors in operator logs | Database connection issues | Verify coordinator is reachable; check `DBClientFactory` configuration |
| Slow detection time | Probe interval too high | Reduce `ftsProbeInterval` (e.g., to 10s) for faster detection |

```bash
# Check operator logs for FTS probe details
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.msg | test("FTS|failover|segment.*down"))'

# Check segment pod status
kubectl get pods -n cloudberry-test -l avsoft.io/cluster=my-cluster,avsoft.io/component=segment-primary

# Verify mirroring configuration
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.spec.segments.mirroring}'
```

### Enable Mirroring on Existing Cluster

You can enable segment mirroring on an existing cluster that was created without mirroring. The operator creates mirror StatefulSets, initializes mirrors from primaries via WAL replication, and transitions the mirroring status through a well-defined state machine.

#### Prerequisites

Before enabling mirroring, ensure:

- **Cluster is in `Running` phase** — The operator blocks mirroring enable on clusters in any other phase (e.g., `Stopped`, `Initializing`, `Scaling`). The webhook rejects the patch if the cluster is not `Running`.
- **Sufficient nodes for the layout** — For `group` layout, you need at least 2 hosts. For `spread` layout, you need more hosts than `primariesPerHost`. The operator validates node count and emits a `MirroringEnableBlocked` event if nodes are insufficient.
- **Mirroring status is `NotConfigured`** — The operator only initiates mirroring enable when `status.mirroringStatus` is `NotConfigured` and no mirror StatefulSet exists.

#### Enabling Mirroring

Patch the cluster CR to set `spec.segments.mirroring.enabled: true`:

```bash
# Enable mirroring with group layout (default)
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"mirroring": {"enabled": true}}}}'

# Enable mirroring with spread layout
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"mirroring": {"enabled": true, "layout": "spread"}}}}'
```

Or update the CRD manifest directly:

```yaml
spec:
  segments:
    mirroring:
      enabled: true
      layout: spread  # or "group" (default)
```

Or via CLI:

```bash
cloudberry-ctl ha mirroring enable --cluster my-cluster
cloudberry-ctl ha mirroring enable --cluster my-cluster --layout spread
```

The operator automatically:
1. Validates the cluster is in `Running` phase and has sufficient nodes
2. Sets the cluster phase to `Updating`
3. Creates the mirror segment StatefulSet with the same replica count as the primary StatefulSet
4. Sets `status.mirroringStatus` to `Initializing`
5. Initiates WAL replication from primaries to mirrors via the DB client
6. Transitions `status.mirroringStatus` to `Syncing` as data synchronization progresses
7. Monitors replication lag and transitions to `InSync` when all mirrors are fully synchronized
8. Returns the cluster phase to `Running`

#### Monitoring Mirroring Progress

```bash
# Watch the cluster status during mirroring enable
kubectl get cloudberrycluster my-cluster -n cloudberry-test -w

# Check mirroring status
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.mirroringStatus}'

# Check the MirroringHealthy condition
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.conditions[?(@.type=="MirroringHealthy")]}' | jq .

# Watch mirroring events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -i mirroring

# Check mirror StatefulSet readiness
kubectl get statefulsets -n cloudberry-test -l avsoft.io/cluster=my-cluster

# Via CLI
cloudberry-ctl ha mirroring status --cluster my-cluster

# Via API
curl -u admin:password \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/mirroring
```

#### Status Transitions

The mirroring status progresses through the following states during enable:

```
NotConfigured → Initializing → Syncing → InSync
```

| Status | Description |
|--------|-------------|
| `NotConfigured` | Mirroring is not set up. Starting state before enable. |
| `Initializing` | Mirror StatefulSet created, mirrors are being initialized from primaries. The operator creates the mirror pods and begins WAL replication setup via the DB client. |
| `Syncing` | Mirrors are actively synchronizing data from primaries. Replication lag is decreasing. The operator monitors `cloudberry_replication_lag_bytes` during this phase. |
| `InSync` | All mirrors are fully synchronized with their primaries. Mirroring enable is complete. |
| `Degraded` | Set if the mirroring enable operation times out after 30 minutes. Requires manual investigation. |

**Expected timeline**: The time to complete mirroring enable depends on the data volume. For a typical cluster with moderate data, expect:
- `Initializing` → `Syncing`: 1–5 minutes (mirror pod creation and WAL setup)
- `Syncing` → `InSync`: Depends on data volume (WAL replay and catch-up)
- **Timeout**: 30 minutes. If mirrors do not reach `InSync` within this window, the status transitions to `Degraded`.

#### Mirroring Events

| Event | Type | Description |
|-------|------|-------------|
| `MirroringEnabled` | Normal | Mirroring enable initiated — mirror StatefulSet created |
| `MirroringInitializing` | Normal | Mirror initialization in progress |
| `MirroringInSync` | Normal | All mirrors synchronized — mirroring enable complete |
| `MirroringDegraded` | Warning | Mirroring enable timed out after 30 minutes |
| `MirroringEnableBlocked` | Warning | Mirroring enable blocked — cluster not in `Running` phase or insufficient nodes |

```bash
# View mirroring events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -E 'Mirroring'
```

#### Mirroring Metrics

The operator exposes the following metrics for mirroring operations:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_mirroring_operations_total` | Counter | `cluster`, `namespace`, `operation` | Total mirroring enable/disable operations. `operation` is `enable` or `disable` |
| `cloudberry_replication_lag_bytes` | Gauge | `cluster`, `namespace`, `segment` | Replication lag in bytes per segment. Decreases during `Syncing` phase, reaches 0 at `InSync` |

```promql
# Total mirroring enable operations
cloudberry_mirroring_operations_total{operation="enable"}

# Total mirroring disable operations
cloudberry_mirroring_operations_total{operation="disable"}

# Replication lag for all segments (should approach 0 during sync)
cloudberry_replication_lag_bytes{cluster="my-cluster"}
```

#### Troubleshooting Mirroring Enable

If mirroring enable fails or times out:

1. **Check why mirror pods are not ready**:

   ```bash
   kubectl get pods -n cloudberry-test -l avsoft.io/cluster=my-cluster,avsoft.io/component=segment-mirror
   kubectl describe pod <mirror-pod> -n cloudberry-test
   kubectl logs <mirror-pod> -n cloudberry-test
   ```

2. **Common causes and fixes**:

   | Cause | Symptoms | Resolution |
   |-------|----------|------------|
   | Insufficient nodes | Mirror pods stuck in `Pending` | Add nodes to satisfy anti-affinity and layout requirements |
   | Insufficient storage | PVC provisioning failure | Check StorageClass and available storage capacity |
   | Cluster not Running | `MirroringEnableBlocked` event | Wait for cluster to reach `Running` phase, then retry |
   | DB client error | Mirroring stuck in `Initializing` | Check operator logs for database connection errors |
   | Timeout (30 min) | Status transitions to `Degraded` | Check replication lag metrics, investigate slow WAL replay |

3. **After fixing the issue**, the operator automatically detects mirror readiness on the next reconciliation cycle and completes the transition to `InSync`.

### Disable Mirroring

You can disable mirroring on a cluster that currently has mirroring enabled. The operator removes the mirror StatefulSet and updates the mirroring status.

#### Implications of Disabling Mirroring

> **Warning**: Disabling mirroring reduces data protection. Without mirrors, a primary segment failure results in data unavailability until recovery is performed. Consider the following before disabling:

- **No automatic failover** — Primary segment failures require manual recovery (incremental, full, or differential)
- **Reduced fault tolerance** — The cluster can no longer survive a single host failure without data loss
- **No rollback** — Re-enabling mirroring requires a full mirror initialization, which takes time proportional to data volume

#### Disabling Mirroring

Patch the cluster CR to set `spec.segments.mirroring.enabled: false`:

```bash
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"mirroring": {"enabled": false}}}}'
```

Or update the CRD manifest directly:

```yaml
spec:
  segments:
    mirroring:
      enabled: false
```

The operator automatically:
1. Validates the cluster is in `Running` phase
2. Scales down and deletes the mirror segment StatefulSet
3. Sets `status.mirroringStatus` to `NotConfigured`
4. Emits a `MirroringDisabled` event
5. Records the `cloudberry_mirroring_operations_total{operation="disable"}` metric

#### PVC Cleanup Behavior

When mirroring is disabled, the behavior of mirror PVCs depends on the cluster's `deletionPolicy`:

| Policy | Behavior |
|--------|----------|
| `Retain` (default) | Mirror PVCs are preserved after the mirror StatefulSet is deleted. You can manually clean them up later or use them for data recovery. |
| `Delete` | Mirror PVCs are automatically deleted when the mirror StatefulSet is removed. |

```bash
# Check for orphaned mirror PVCs after disabling mirroring
kubectl get pvc -n cloudberry-test -l avsoft.io/cluster=my-cluster,avsoft.io/component=segment-mirror

# Manually delete orphaned mirror PVCs (if using Retain policy)
kubectl delete pvc -n cloudberry-test -l avsoft.io/cluster=my-cluster,avsoft.io/component=segment-mirror
```

### Coordinator Standby

#### Enabling Standby

```yaml
spec:
  standby:
    enabled: true
    resources:
      requests:
        cpu: "2"
        memory: 4Gi
    storage:
      storageClass: fast-ssd
      size: 50Gi
```

#### Checking Standby Status

```bash
cloudberry-ctl ha standby status --cluster my-cluster
```

#### Activating Standby (Coordinator Failover)

> **Important**: Standby activation is a manual operation. It is not triggered automatically to prevent split-brain scenarios.

```bash
cloudberry-ctl ha standby activate --cluster my-cluster --confirm
```

This promotes the standby to primary coordinator, updates Services to point to the new coordinator, and reconstructs state from replicated WAL.

#### Reinitializing Standby After Failover

After activating the standby, reinitialize a new standby:

```bash
cloudberry-ctl ha standby reinitialize --cluster my-cluster
```

#### Restoring Original Roles

To swap the coordinator and standby back to their original roles:

```bash
cloudberry-ctl ha standby restore-roles --cluster my-cluster
```

### Segment Recovery

When a primary segment fails, the operator automatically promotes its mirror. To recover the failed segment:

#### Incremental Recovery

Use when the segment was down briefly and data is intact:

```bash
cloudberry-ctl ha recovery start --cluster my-cluster --type incremental
```

#### Full Recovery

Use when segment data is corrupted:

```bash
cloudberry-ctl ha recovery start --cluster my-cluster --type full
```

#### Differential Recovery

Use for large segments where minimizing data transfer is important:

```bash
cloudberry-ctl ha recovery start --cluster my-cluster \
  --type differential --parallel 4
```

> **Recovery type validation**: The `--type` flag accepts only `incremental`, `full`, or `differential`. Any other value is rejected by the API with a `400 INVALID_REQUEST` error. This validation prevents typos and invalid recovery modes from being silently accepted.

#### Recovery to a Different Node

```bash
cloudberry-ctl ha recovery start --cluster my-cluster \
  --target-node node-3
```

#### Checking Recovery Status

```bash
cloudberry-ctl ha recovery status --cluster my-cluster
```

### Rebalancing Segments

After recovery, segments may not be in their preferred roles (a mirror may be acting as primary). Rebalance restores original roles:

```bash
# Rebalance all segments
cloudberry-ctl ha rebalance --cluster my-cluster

# Rebalance specific segments
cloudberry-ctl ha rebalance --cluster my-cluster --content-ids 0,1,2
```

### FTS Configuration

Tune the Fault Tolerance Service probe settings:

```bash
cloudberry-ctl ha fts configure --cluster my-cluster \
  --probe-interval 30 \
  --probe-timeout 10 \
  --probe-retries 3
```

Or in the CRD:

```yaml
spec:
  ha:
    ftsProbeInterval: 30   # seconds between probes
    ftsProbeTimeout: 10    # seconds to wait for response
    ftsProbeRetries: 3     # retries before marking down
    checksums: true        # storage-layer checksums
```

## Maintenance Operations

### Vacuum

```bash
# Regular vacuum on all tables
cloudberry-ctl maintenance vacuum --cluster my-cluster

# Vacuum a specific table
cloudberry-ctl maintenance vacuum --cluster my-cluster --table public.large_table

# Vacuum with statistics refresh
cloudberry-ctl maintenance vacuum --cluster my-cluster --analyze

# Full vacuum (requires exclusive lock)
cloudberry-ctl maintenance vacuum --cluster my-cluster --full
```

### Analyze

Refresh planner statistics:

```bash
# All tables
cloudberry-ctl maintenance analyze --cluster my-cluster

# Specific table
cloudberry-ctl maintenance analyze --cluster my-cluster --table public.large_table
```

### Reindex

Rebuild indexes:

```bash
# All indexes in a database
cloudberry-ctl maintenance reindex --cluster my-cluster --database mydb

# Specific table
cloudberry-ctl maintenance reindex --cluster my-cluster --table public.large_table
```

### Catalog Check

```bash
cloudberry-ctl maintenance check-catalog --cluster my-cluster --database mydb
```

### Maintenance Jobs

The operator creates Kubernetes `batchv1.Job` resources for maintenance operations. Each Job runs a `psql` command that connects to the coordinator service and executes the requested operation.

**Job properties:**
- **BackoffLimit**: `1` (retry once on failure)
- **TTLSecondsAfterFinished**: `3600` (auto-cleanup after 1 hour)
- **RestartPolicy**: `Never`
- **Authentication**: `PGPASSWORD` sourced from the cluster's admin password Secret

**Supported operations:**

| Operation | Annotation Value | SQL Command |
|-----------|-----------------|-------------|
| Vacuum | `vacuum` | `VACUUM` |
| Vacuum + Analyze | `vacuum-analyze` | `VACUUM ANALYZE` |
| Full Vacuum | `vacuum-full` | `VACUUM FULL` |
| Analyze | `analyze` | `ANALYZE` |
| Reindex | `reindex` | `REINDEX DATABASE` |
| Redistribute | `redistribute` | `ANALYZE` (maps to `gpexpand` in production Cloudberry) |
| Rebalance | `rebalance` | `ANALYZE` (maps to `gpexpand` redistribution in production Cloudberry) |
| Backup on Delete | `backup-on-delete` | `SELECT 1` (maps to `gpbackup` in production Cloudberry) |

The `redistribute` operation is created automatically during scale-out operations. The `rebalance` operation is created when a manual rebalance is triggered via annotation or API. The `backup-on-delete` operation is created automatically during cluster deletion when `backupOnDelete: true`. Unknown operations emit a `MaintenanceUnknown` warning event and are not executed.

**Events:**
- `MaintenanceStarted` (Normal) — Job created with the job name in the message

### Maintenance Annotations

You can trigger maintenance operations directly via annotations:

```bash
# Trigger a vacuum
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/maintenance=vacuum

# Trigger vacuum with analyze
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/maintenance=vacuum-analyze

# Trigger a full vacuum
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/maintenance=vacuum-full

# Trigger analyze
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/maintenance=analyze

# Trigger reindex
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/maintenance=reindex
```

The operator removes the annotation after creating the Job. You can monitor Job status with:

```bash
kubectl get jobs -n cloudberry-test -l avsoft.io/cluster=my-cluster,avsoft.io/operation=maintenance
```

## Session Management

The operator provides real-time session management by querying `pg_stat_activity` on the cluster's coordinator database. Session operations use the `DBClientFactory` to create short-lived database connections, execute the requested operation, and close the connection.

### Listing Sessions

```bash
# List all active sessions
cloudberry-ctl sessions list --cluster my-cluster

# Filter by state
cloudberry-ctl sessions list --cluster my-cluster --state active

# Filter by user
cloudberry-ctl sessions list --cluster my-cluster --user analyst
```

**Example output (JSON):**

```json
{
  "sessions": [
    {
      "pid": 1234,
      "username": "gpadmin",
      "application": "psql",
      "clientAddress": "10.0.0.1",
      "state": "active",
      "query": "SELECT * FROM orders",
      "queryStart": "2026-05-14T10:00:00Z",
      "duration": "00:05:30"
    },
    {
      "pid": 5678,
      "username": "appuser",
      "application": "pgbench",
      "clientAddress": "10.0.0.2",
      "state": "idle",
      "query": "INSERT INTO logs VALUES ($1)",
      "queryStart": "2026-05-14T09:50:00Z",
      "duration": "00:15:30"
    }
  ],
  "total": 2
}
```

**Session fields:**

| Field | Type | Description |
|-------|------|-------------|
| `pid` | int | PostgreSQL backend process ID |
| `username` | string | Database user running the session |
| `application` | string | Application name (e.g., `psql`, `pgbench`) |
| `clientAddress` | string | Client IP address |
| `state` | string | Session state (`active`, `idle`, `idle in transaction`, etc.) |
| `query` | string | Current or last executed query |
| `queryStart` | string | ISO 8601 timestamp when the current query started |
| `duration` | string | Elapsed time of the current query (e.g., `00:05:30`) |

### Canceling a Query

Cancel a running query without terminating the session. This calls `pg_cancel_backend()` on the coordinator:

```bash
cloudberry-ctl sessions cancel-query --cluster my-cluster 12345
```

**Example output (JSON):**

```json
{
  "pid": 12345,
  "canceled": true
}
```

The `canceled` field indicates whether `pg_cancel_backend()` returned `true`. A value of `false` means the PID was not found or the query had already completed.

### Terminating a Session

Terminate a session entirely, disconnecting the client. This calls `pg_terminate_backend()` on the coordinator:

```bash
cloudberry-ctl sessions terminate --cluster my-cluster 12345
```

**Example output (JSON):**

```json
{
  "pid": 12345,
  "terminated": true
}
```

The `terminated` field indicates whether `pg_terminate_backend()` returned `true`. A value of `false` means the PID was not found or the session had already ended.

### Graceful Degradation

When the database connection is not available (e.g., the `DBClientFactory` is not configured), the list sessions endpoint returns an empty result with an informational message instead of an error:

```json
{
  "sessions": [],
  "total": 0,
  "message": "database connection not available"
}
```

### Error Handling

| Scenario | HTTP Status | Error Code | Description |
|----------|-------------|------------|-------------|
| Invalid PID (zero, negative, non-numeric) | 400 | `INVALID_REQUEST` | PID must be a positive integer |
| Cluster not found | 404 | `CLUSTER_NOT_FOUND` | The specified cluster does not exist |
| Database connection failed | 503 | `DB_UNAVAILABLE` | Cannot connect to the cluster's database |
| Query execution failed | 500 | `INTERNAL_ERROR` | The database query or operation failed |

> **PID validation**: The PID argument must be a positive integer. The API rejects PIDs that are zero, negative, or non-numeric with a `400 Bad Request` error (`INVALID_REQUEST: PID must be a positive integer`).

## Resource Group Management

Resource groups allow you to control how database resources (CPU, memory, concurrency) are allocated across different workloads and roles. You can create resource groups with specific limits, assign database roles to them, and manage their lifecycle through the CLI or REST API.

Resource group operations execute SQL commands directly on the Cloudberry coordinator via the `DBClientFactory`. When the database connection is not available, create operations return a `201` response with a `"pending"` message, and list operations fall back to the CRD spec.

### Creating a Resource Group

Create a resource group with concurrency, CPU, and memory limits:

```bash
cloudberry-ctl resource-group create --cluster my-cluster \
  --name analytics --concurrency 10 --cpu-max-percent 50 --memory-limit 30
```

**Flags:**

| Flag | Type | Description |
|------|------|-------------|
| `--name` | string | Resource group name (required) |
| `--concurrency` | int | Maximum number of concurrent transactions |
| `--cpu-max-percent` | int | Maximum CPU usage percentage (0–100) |
| `--memory-limit` | int | Memory limit percentage (0–100) |

**Response (JSON):**

```json
{
  "name": "analytics",
  "concurrency": 10,
  "cpuMaxPercent": 50,
  "memoryLimit": 30,
  "status": "created"
}
```

The underlying SQL executed on the coordinator is:

```sql
CREATE RESOURCE GROUP analytics WITH (concurrency=10, cpu_max_percent=50, memory_limit=30);
```

### Listing Resource Groups

List all resource groups in the cluster:

```bash
cloudberry-ctl resource-group list --cluster my-cluster
```

**Response (JSON):**

```json
{
  "resourceGroups": [
    {
      "name": "analytics",
      "concurrency": 10,
      "cpuMaxPercent": 50,
      "memoryLimit": 30
    }
  ],
  "total": 1
}
```

When a database connection is available, resource groups are queried from `gp_toolkit.gp_resgroup_status`. When the database is unavailable, the endpoint falls back to the resource groups defined in the CRD spec.

### Assigning a Role to a Resource Group

Assign a database role to a resource group to enforce its resource limits on that role's queries:

```bash
cloudberry-ctl resource-group assign --cluster my-cluster \
  --group analytics --role analyst
```

**Flags:**

| Flag | Type | Description |
|------|------|-------------|
| `--group` | string | Resource group name (required) |
| `--role` | string | Database role to assign (required) |

**Response (JSON):**

```json
{
  "group": "analytics",
  "role": "analyst",
  "status": "assigned"
}
```

The underlying SQL executed on the coordinator is:

```sql
ALTER ROLE analyst RESOURCE GROUP analytics;
```

### Deleting a Resource Group

Delete a resource group from the cluster:

```bash
cloudberry-ctl resource-group delete --cluster my-cluster --name analytics
```

**Response (JSON):**

```json
{
  "group": "analytics",
  "status": "deleted"
}
```

The underlying SQL executed on the coordinator is:

```sql
DROP RESOURCE GROUP analytics;
```

> **Note**: You cannot delete a resource group that has roles assigned to it. Reassign or drop the roles first.

## Declarative Workload Management

The operator supports fully declarative workload management through the `spec.workload` section of the `CloudberryCluster` CRD. When workload management is enabled, the operator automatically reconciles resource groups, workload rules, and idle session rules from the CRD spec into the database and ConfigMaps.

### Enabling Workload Management

Enable workload management by setting `spec.workload.enabled: true` and defining resource groups, rules, and idle rules:

```yaml
spec:
  workload:
    enabled: true
    resourceGroups:
      - name: analytics
        concurrency: 10
        cpuMaxPercent: 50
        cpuWeight: 100
        memoryLimit: 30
        minCost: 500
      - name: etl
        concurrency: 5
        cpuMaxPercent: 30
        cpuWeight: 50
        memoryLimit: 20
    rules:
      - name: cancel-long-queries
        enabled: true
        resourceGroup: analytics
        action: cancel
        thresholdType: running_time
        threshold: "3600"
        priority: 1
      - name: move-heavy-queries
        enabled: true
        queryTag: heavy
        action: move
        moveTarget: etl
        thresholdType: spill_size
        threshold: "1073741824"
        priority: 2
    idleRules:
      - name: terminate-idle-analytics
        enabled: true
        resourceGroup: analytics
        idleTimeout: "30m"
        excludeInTransaction: true
        terminateMessage: "Session terminated due to inactivity"
```

### Resource Group Reconciliation

The operator diffs desired (CRD spec) vs actual (database) resource groups on every reconciliation cycle:

1. **Creates** resource groups that are in the spec but not in the database via `CREATE RESOURCE GROUP`
2. **Alters** resource groups whose parameters have changed via `ALTER RESOURCE GROUP`
3. **Drops** resource groups that are in the database but not in the spec via `DROP RESOURCE GROUP`

The reconciliation is idempotent — running it multiple times with the same spec produces no additional changes. Resource groups that already match the desired state are left untouched.

```bash
# View the current workload configuration
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.spec.workload}' | jq .

# Check the WorkloadConfigured condition
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.conditions[?(@.type=="WorkloadConfigured")]}' | jq .
```

### Workload Rules ConfigMap

Workload rules from `spec.workload.rules` are serialized to JSON and stored in a ConfigMap named `{cluster}-workload-rules` under the `rules.json` key. The ConfigMap has owner references to the cluster for automatic garbage collection.

```bash
# View the workload rules ConfigMap
kubectl get configmap my-cluster-workload-rules -n cloudberry-test -o yaml

# View just the rules
kubectl get configmap my-cluster-workload-rules -n cloudberry-test \
  -o jsonpath='{.data.rules\.json}' | jq .
```

**ConfigMap labels:**
- `app.kubernetes.io/managed-by=cloudberry-operator`
- `app.kubernetes.io/component=workload-rules`
- `app.kubernetes.io/instance={cluster}`

### Idle Session Rules ConfigMap

Idle session rules from `spec.workload.idleRules` are stored in the same `{cluster}-workload-rules` ConfigMap under the `idle-rules.json` key:

```bash
# View idle session rules
kubectl get configmap my-cluster-workload-rules -n cloudberry-test \
  -o jsonpath='{.data.idle-rules\.json}' | jq .
```

### WorkloadConfigured Condition

The operator sets the `WorkloadConfigured` status condition to report the state of workload reconciliation:

| Status | Reason | Description |
|--------|--------|-------------|
| `True` | `WorkloadReconciled` | All resource groups, workload rules, and idle rules reconciled successfully |
| `False` | `DBUnavailable` | Database connection unavailable — resource groups not reconciled to the database |

```bash
# Check the condition
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.conditions[?(@.type=="WorkloadConfigured")]}' | jq .
```

**Example condition (success):**

```json
{
  "type": "WorkloadConfigured",
  "status": "True",
  "reason": "WorkloadReconciled",
  "message": "Workload management reconciled successfully",
  "lastTransitionTime": "2026-05-18T10:00:00Z"
}
```

### DB Unavailable Fallback

When the database is unavailable (e.g., coordinator is down or the DB client factory is not configured), the operator falls back gracefully:

- Sets `WorkloadConfigured=False` with reason `DBUnavailable`
- Includes the error details in the condition message
- Does **not** fail the overall reconciliation — other reconciliation steps continue normally
- Retries on the next reconciliation cycle

This ensures that workload configuration is eventually consistent once the database becomes available.

## Storage Expansion

The operator supports online expansion of persistent volume claims (PVCs) for all cluster components. When you increase a storage size in the `CloudberryCluster` spec, the operator detects the change during reconciliation and patches the corresponding PVCs to the new size.

> **Prerequisite**: Your `StorageClass` must support volume expansion (`allowVolumeExpansion: true`). Without this, the operator blocks the PVC patch entirely and logs a warning. See [StorageClass Requirements](#storageclass-requirements) below.

### StorageClass Requirements

The operator performs a **pre-flight StorageClass check** before expanding any PVC. The StorageClass referenced by the PVC must have `allowVolumeExpansion: true` for the expansion to proceed.

#### How the Check Works

When `expandPVCIfNeeded()` detects that a PVC needs resizing, it calls `storageClassSupportsExpansion()` which:

1. Reads the StorageClass name from the PVC's `spec.storageClassName` field
2. Falls back to the legacy `volume.beta.kubernetes.io/storage-class` annotation if the field is not set
3. Looks up the StorageClass in the Kubernetes API
4. Checks the `allowVolumeExpansion` field:
   - **`true`** — expansion proceeds normally
   - **`false` or `nil`** — expansion is **blocked**; a warning is logged and the PVC remains unchanged
   - **StorageClass not found** — expansion is **blocked** with a descriptive reason
5. If **no StorageClass is specified** (the PVC uses the cluster default), the expansion is **allowed** (the operator cannot determine the default StorageClass's capabilities)
6. On **transient API errors**, the expansion is **allowed** rather than blocked (fail-open to avoid permanently blocking legitimate expansions)

#### Checking Your StorageClass

Verify that your StorageClass supports volume expansion:

```bash
# Check a specific StorageClass
kubectl get storageclass <name> -o jsonpath='{.allowVolumeExpansion}'

# List all StorageClasses with their expansion support
kubectl get storageclass -o custom-columns=NAME:.metadata.name,EXPANSION:.allowVolumeExpansion
```

#### Enabling Volume Expansion

If your StorageClass does not support expansion, you can enable it:

```bash
kubectl patch storageclass <name> -p '{"allowVolumeExpansion": true}'
```

> **Note**: Not all storage provisioners support volume expansion even when `allowVolumeExpansion` is set to `true`. Check your storage provider's documentation to confirm support.

#### What Happens When Expansion Is Blocked

When the StorageClass does not support expansion, the operator:

1. Logs a **WARN**-level message with the PVC name, StorageClass name, reason, and the current and desired sizes
2. **Skips** the PVC patch — the PVC remains at its current size
3. **Does not return an error** — reconciliation continues normally for other components
4. **Does not emit** a `StorageExpanded` event or set the `StorageExpanded` condition

**Example log output:**

```json
{
  "level": "WARN",
  "msg": "PVC expansion blocked by StorageClass",
  "pvc": "data-my-cluster-coordinator-0",
  "storageClass": "standard",
  "reason": "StorageClass \"standard\" does not allow volume expansion",
  "currentSize": "5Gi",
  "desiredSize": "10Gi"
}
```

This is a non-fatal condition. To resolve it, either enable `allowVolumeExpansion` on the StorageClass or migrate to a StorageClass that supports expansion.

#### Docker Desktop / hostpath Limitation

The Docker Desktop `hostpath` provisioner does **not** actually implement volume expansion at the storage layer, even when `allowVolumeExpansion: true` is set on the StorageClass. The operator patches the PVC's `spec.resources.requests.storage` to the new size, but the underlying volume on disk does not resize.

This means:
- The PVC metadata shows the new size
- The actual available disk space inside the container remains unchanged
- This limitation applies only to local development with Docker Desktop

For production environments, use a storage provisioner that fully supports volume expansion (e.g., AWS EBS, GCE PD, Azure Disk, Ceph RBD).

### Expansion Scopes

Storage expansion operates independently on three scopes. You can expand one scope without affecting the others.

#### Coordinator Storage

Expand the coordinator's PVC by increasing `spec.coordinator.storage.size`:

```yaml
spec:
  coordinator:
    storage:
      size: 10Gi    # was 5Gi
```

Or via `kubectl patch`:

```bash
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"coordinator": {"storage": {"size": "10Gi"}}}}'
```

The operator patches the single coordinator PVC (`data-{cluster}-coordinator-0`). Standby and segment PVCs remain unchanged.

#### Standby Storage

Expand the standby coordinator's PVC by increasing `spec.standby.storage.size`:

```yaml
spec:
  standby:
    enabled: true
    storage:
      size: 10Gi    # was 5Gi
```

Or via `kubectl patch`:

```bash
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"standby": {"storage": {"size": "10Gi"}}}}'
```

The operator patches the single standby PVC (`data-{cluster}-standby-0`). Coordinator and segment PVCs remain unchanged.

> **Note**: Standby expansion is skipped if `spec.standby.enabled` is `false` or `spec.standby.storage` is not set.

#### Segment Storage

Expand all segment PVCs by increasing `spec.segments.storage.size`:

```yaml
spec:
  segments:
    count: 4
    mirroring:
      enabled: true
    storage:
      size: 10Gi    # was 5Gi
```

Or via `kubectl patch`:

```bash
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"storage": {"size": "10Gi"}}}}'
```

The operator patches **all** primary segment PVCs (`data-{cluster}-segment-primary-0` through `data-{cluster}-segment-primary-N`) and, if mirroring is enabled, all mirror segment PVCs (`data-{cluster}-segment-mirror-0` through `data-{cluster}-segment-mirror-N`).

**Example**: A cluster with 3 segments and mirroring enabled has 6 segment PVCs. Increasing `segments.storage.size` from `5Gi` to `10Gi` expands all 6 PVCs. The coordinator PVC remains unchanged.

### Safety Constraints

**No shrink allowed**: The operator only expands PVCs — it never shrinks them. If the desired size is less than or equal to the current PVC size, the expansion is silently skipped. This prevents accidental data loss from reducing volume sizes.

**PVC not found is skipped**: If a PVC does not yet exist (e.g., during initial cluster creation before StatefulSets have created their PVCs), the expansion is skipped without error. The PVC is created at the correct size by the StatefulSet's `volumeClaimTemplate`.

**StorageClass requirement**: The underlying `StorageClass` must have `allowVolumeExpansion: true`. Verify your StorageClass supports expansion:

```bash
kubectl get storageclass -o custom-columns=NAME:.metadata.name,EXPANSION:.allowVolumeExpansion
```

**No downtime**: PVC expansion is an online operation — pods do not need to restart. However, some storage providers may require the pod to be restarted for the filesystem to recognize the new size. Check your storage provider's documentation.

### Monitoring Storage Expansion

#### Via CLI

```bash
# List all PVCs for a cluster with their current sizes
cloudberry-ctl cluster pvcs --cluster my-cluster
```

#### Via API

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/storage/pvcs?namespace=cloudberry-test"
```

**Response (200 OK):**

```json
{
  "pvcs": [
    {
      "name": "data-my-cluster-coordinator-0",
      "component": "coordinator",
      "size": "10Gi",
      "phase": "Bound"
    },
    {
      "name": "data-my-cluster-standby-0",
      "component": "standby",
      "size": "10Gi",
      "phase": "Bound"
    },
    {
      "name": "data-my-cluster-segment-primary-0",
      "component": "segment-primary",
      "size": "10Gi",
      "phase": "Bound"
    },
    {
      "name": "data-my-cluster-segment-mirror-0",
      "component": "segment-mirror",
      "size": "10Gi",
      "phase": "Bound"
    }
  ],
  "total": 4
}
```

#### Via kubectl

```bash
# Check PVC sizes directly
kubectl get pvc -n cloudberry-test -l avsoft.io/cluster=my-cluster \
  -o custom-columns=NAME:.metadata.name,SIZE:.spec.resources.requests.storage,STATUS:.status.phase

# Check the StorageExpanded condition
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.conditions[?(@.type=="StorageExpanded")]}'

# Watch storage expansion events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep StorageExpanded
```

### Storage Expansion Events and Conditions

The operator emits the following event when PVCs are expanded:

| Event | Type | Description |
|-------|------|-------------|
| `StorageExpanded` | Normal | PVC storage expanded successfully |

The `StorageExpanded` status condition tracks whether PVCs have been expanded:

| Condition | Status | Reason | Description |
|-----------|--------|--------|-------------|
| `StorageExpanded` | `True` | `PVCsExpanded` | Persistent volume claims expanded to new sizes |

**Example condition:**

```json
{
  "type": "StorageExpanded",
  "status": "True",
  "reason": "PVCsExpanded",
  "message": "Persistent volume claims expanded to new sizes",
  "lastTransitionTime": "2026-05-15T10:00:00Z"
}
```

### Storage Expansion Metrics

The operator exposes a Prometheus metric for PVC sizes:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_pvc_size_bytes` | Gauge | `cluster`, `namespace`, `component` | PVC size in bytes for each component |

```promql
# Coordinator PVC size
cloudberry_pvc_size_bytes{cluster="my-cluster", component="coordinator"}

# All segment PVC sizes
cloudberry_pvc_size_bytes{cluster="my-cluster", component=~"segment-.*"}

# Total storage across all components
sum(cloudberry_pvc_size_bytes{cluster="my-cluster"})
```

## Scaling Operations

The operator supports scaling a Cloudberry cluster by changing the segment count. When you modify `spec.segments.count`, the operator detects the difference between the desired and actual StatefulSet replicas, transitions the cluster to the `Scaling` phase, updates both primary and mirror StatefulSets, creates a data redistribution Job, and transitions back to `Running` once all pods reach the desired replica count. Both scale-out (adding segments) and scale-in (removing segments) are supported.

### Scaling Out (Adding Segments)

To scale out a cluster, patch the `segments.count` field in the CRD:

```bash
# Scale from 4 to 6 segments
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"count": 6}}}'
```

Or update the CRD manifest directly:

```yaml
spec:
  segments:
    count: 6    # was 4
```

The operator automatically:
1. Detects the scale-out in `reconcileSegments()` by comparing `spec.segments.count` against the current StatefulSet replicas
2. Sets the cluster phase to `Scaling`
3. Updates the primary segment StatefulSet replicas to the new count
4. Updates the mirror segment StatefulSet replicas (if mirroring is enabled)
5. Creates a redistribution Job to rebalance data across the new segments
6. Monitors pod readiness and transitions back to `Running` when all pods are ready

**Example**: Scaling a mirrored cluster from 4 to 6 segments:
- Before: 10 pods (1 coordinator + 1 standby + 4 primaries + 4 mirrors)
- After: 14 pods (1 coordinator + 1 standby + 6 primaries + 6 mirrors)

### Scaling In (Removing Segments)

To scale in a cluster, decrease the `segments.count` field in the CRD:

```bash
# Scale from 6 to 4 segments
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"count": 4}}}'
```

Or update the CRD manifest directly:

```yaml
spec:
  segments:
    count: 4    # was 6
```

The operator automatically:
1. Detects the scale-in in `reconcileSegments()` by comparing `spec.segments.count` against the current StatefulSet replicas
2. Sets the cluster phase to `Scaling`
3. Creates a redistribution Job to move data off the segments being removed
4. Scales down the mirror segment StatefulSet first (if mirroring is enabled)
5. Scales down the primary segment StatefulSet
6. Handles PVC cleanup based on the `deletionPolicy`
7. Transitions back to `Running` when all StatefulSets reach the desired replica count

**Example**: Scaling a mirrored cluster from 6 to 4 segments:
- Before: 14 pods (1 coordinator + 1 standby + 6 primaries + 6 mirrors)
- After: 10 pods (1 coordinator + 1 standby + 4 primaries + 4 mirrors)

#### PVC Behavior During Scale-In

The `deletionPolicy` controls what happens to PVCs for removed segments:

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `Retain` (default) | PVCs for removed segments are preserved | Data recovery, audit compliance |
| `Delete` | PVCs for removed segments are automatically deleted | Cost savings, ephemeral environments |

**Retain policy example** (scaling from 6 → 4 segments):
- Mirror and primary StatefulSets scale from 6 to 4 replicas
- PVCs for segments 4 and 5 remain in the namespace
- Total PVCs remain at 16 (12 active + 4 orphaned)
- Orphaned PVCs can be manually cleaned up later

**Delete policy example** (scaling from 6 → 4 segments):
- Mirror and primary StatefulSets scale from 6 to 4 replicas
- PVCs for segments 4 and 5 are automatically deleted by `cleanupOrphanedPVCs()`
- Total PVCs decrease from 16 to 12

#### 50% Confirmation Requirement

Scale-in operations that reduce the segment count by more than 50% require an explicit confirmation annotation. This safety mechanism prevents accidental large-scale reductions that could cause significant data movement and potential service disruption.

**How it works**: The operator calculates `newCount / currentCount`. If the ratio is less than 0.5 (i.e., more than 50% of segments are being removed), the operation is blocked unless the `avsoft.io/confirm-scale-in=true` annotation is present on the cluster resource.

**Example: Scaling from 8 to 3 segments (62.5% reduction)**

```bash
# Step 1: This is BLOCKED — scaling from 8 to 3 is a 62.5% reduction (>50%)
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"count": 3}}}'

# The operator emits a ScaleInBlocked warning event:
#   ScaleInBlocked — "Scale-in from 8 to 3 requires annotation avsoft.io/confirm-scale-in=true"
# The cluster phase stays Running, and no StatefulSets are modified.

# Step 2: Add the confirmation annotation
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/confirm-scale-in=true

# Step 3: Now the scale-in proceeds on the next reconciliation
# The operator detects the annotation and allows the scale-in:
#   - Phase transitions: Running → Scaling → Running
#   - Events: ScaleInStarted, ScaleInCompleted
#   - Primary and mirror StatefulSets scale from 8 to 3 replicas
```

After the scale-in completes successfully, the operator **automatically removes** the `avsoft.io/confirm-scale-in` annotation along with the `avsoft.io/scale-started` annotation. You do not need to clean up the annotation manually.

**Boundary behavior**:

| From | To | Reduction | Blocked? | Reason |
|------|----|-----------|----------|--------|
| 8 | 3 | 62.5% | **Yes** | Exceeds 50% threshold |
| 10 | 4 | 60% | **Yes** | Exceeds 50% threshold |
| 8 | 4 | 50% (exactly) | **No** | Check uses strict less-than (`< 0.5`) |
| 6 | 4 | 33% | **No** | Within 50% threshold |

> **Note**: The confirmation annotation is checked only when the new count is less than 50% of the current count. Scale-in operations at or within the 50% threshold proceed without confirmation. The annotation has no effect on scale-out operations.

### Scale-Out Failure Handling

The operator includes pre-flight checks, timeout detection, and failure reporting for scale operations. These mechanisms prevent scaling in unsafe states and surface failures for manual resolution.

#### Pre-Flight Blocking

Scale-out and scale-in operations require the cluster to be in the `Running` phase. If the cluster is in any other phase (e.g., `Initializing`, `Stopped`, `Scaling`), the operator blocks the operation and emits a warning event:

```bash
# Check events for blocked scale operations
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -E 'ScaleOutBlocked|ScaleInBlocked'
```

| Event | Type | Trigger |
|-------|------|---------|
| `ScaleOutBlocked` | Warning | Scale-out attempted when cluster is not in `Running` phase |
| `ScaleInBlocked` | Warning | Scale-in attempted when cluster is not in `Running` phase, or >50% reduction without confirmation |

The operator does not return an error for blocked operations — it skips the scale and retries on the next reconciliation cycle. Once the cluster reaches the `Running` phase, the pending scale operation proceeds automatically.

#### Timeout and Failure Detection

When a scale operation starts, the operator sets the `avsoft.io/scale-started` annotation with the current timestamp. On each reconciliation, `checkScaleProgress()` checks whether the operation has exceeded the **10-minute timeout**.

If the timeout elapses and not all segment pods are ready, the operator invokes `handleScaleFailure()`:

1. Identifies which primary and mirror segments are not ready
2. Populates `status.failedSegments` with details for each unready segment
3. Sets the `ScaleOutFailed` condition to `True` with reason `SegmentsNotReady`
4. Emits a `ScaleOutFailed` warning event
5. Removes the `avsoft.io/scale-started` annotation
6. The cluster **stays in the `Scaling` phase** — no automatic rollback

#### Checking Failed Segments

After a scale failure, inspect the cluster status to see which segments failed:

```bash
# View failed segments in the cluster status
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.failedSegments}' | jq .
```

**Example output:**

```json
[
  {
    "contentID": 4,
    "hostname": "my-cluster-segment-primary-4",
    "role": "primary",
    "status": "NotReady"
  },
  {
    "contentID": 5,
    "hostname": "my-cluster-segment-primary-5",
    "role": "primary",
    "status": "NotReady"
  }
]
```

Check the `ScaleOutFailed` condition for summary information:

```bash
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.conditions[?(@.type=="ScaleOutFailed")]}' | jq .
```

**Example output:**

```json
{
  "type": "ScaleOutFailed",
  "status": "True",
  "reason": "SegmentsNotReady",
  "message": "Scale-out failed: 2 segments not ready after 10m0s",
  "lastTransitionTime": "2026-05-15T10:15:00Z"
}
```

#### Manual Recovery Steps

Since the operator does not automatically roll back a failed scale operation, you must resolve the issue manually:

1. **Diagnose the failure** — Check why the new segment pods are not ready:

   ```bash
   # Check pod status
   kubectl get pods -n cloudberry-test -l avsoft.io/cluster=my-cluster | grep -v Running

   # Describe a failing pod for events and conditions
   kubectl describe pod my-cluster-segment-primary-4 -n cloudberry-test

   # Check pod logs
   kubectl logs my-cluster-segment-primary-4 -n cloudberry-test
   ```

2. **Common causes and fixes**:

   | Cause | Symptoms | Resolution |
   |-------|----------|------------|
   | Insufficient node resources | Pod stuck in `Pending` | Add nodes or increase resource quotas |
   | PVC provisioning failure | Pod stuck in `Pending` with PVC events | Check StorageClass and available storage |
   | Image pull failure | Pod in `ImagePullBackOff` | Verify image name and registry access |
   | Readiness probe failure | Pod in `Running` but not `Ready` | Check database initialization logs |
   | Node affinity/anti-affinity | Pod stuck in `Pending` | Adjust anti-affinity rules or add nodes |

3. **After fixing the underlying issue**, the operator automatically detects that the pods become ready on the next reconciliation cycle and transitions the cluster back to `Running`.

4. **If you need to revert** the scale operation (scale back to the original count), update `spec.segments.count` back to the previous value:

   ```bash
   # Revert from 6 back to 4 segments
   kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
     -p '{"spec": {"segments": {"count": 4}}}'
   ```

   > **Note**: Reverting a failed scale-out triggers a scale-in operation, which requires the cluster to be in `Running` phase. If the cluster is stuck in `Scaling` phase due to the failure, you may need to manually resolve the pod issues first.

### Phase Transitions During Scaling

```
┌─────────┐    segments.count    ┌─────────┐    all pods ready    ┌─────────┐
│ Running │───── changed ───────▶│ Scaling │────── complete ─────▶│ Running │
└─────────┘                      └─────────┘                      └─────────┘
```

During scaling (both scale-out and scale-in):
- The cluster phase changes from `Running` to `Scaling`
- A `DataRedistribution` condition is set with reason `ScaleOutStarted` or `ScaleInStarted`
- A `ScaleOutStarted` or `ScaleInStarted` event is emitted
- When all segment StatefulSets reach the desired replica count, the phase returns to `Running`
- A `ScaleOutCompleted` or `ScaleInCompleted` event is emitted
- The `DataRedistribution` condition is updated with reason `Completed`
- For scale-in with `deletionPolicy=Delete`, orphaned PVCs are cleaned up at completion

### Monitoring Scale Progress

#### Via CLI

```bash
# Check scale operation status
cloudberry-ctl cluster scale-status --cluster my-cluster
```

**Output (JSON):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "scaling": true,
  "phase": "Scaling",
  "segmentsReady": 4,
  "segmentsTotal": 6,
  "redistribution": {
    "status": "True",
    "reason": "InProgress",
    "message": "Data redistribution in progress"
  }
}
```

After scaling completes:

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "scaling": false,
  "phase": "Running",
  "segmentsReady": 6,
  "segmentsTotal": 6,
  "redistribution": {
    "status": "True",
    "reason": "Completed",
    "message": "Data redistribution completed"
  }
}
```

#### Via API

```bash
curl -u admin:password \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/scale/status
```

See [API Reference — Scale Status](api-reference.md#scale-status) for the full response schema.

#### Via kubectl

```bash
# Watch cluster phase
kubectl get cloudberrycluster my-cluster -n cloudberry-test -w

# Check events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep Scale

# Check StatefulSet readiness
kubectl get statefulsets -n cloudberry-test -l avsoft.io/cluster=my-cluster
```

### Data Redistribution

When a scale operation is initiated, the operator creates a redistribution Job. For scale-out, the Job rebalances data across the new segments. For scale-in, the Job moves data off the segments being removed. The Job uses the `redistribute` maintenance operation, which runs an `ANALYZE` command on the coordinator (in a production Cloudberry deployment, this maps to `gpexpand` redistribution).

**Job properties:**
- **Name**: `{cluster}-maintenance-{timestamp}`
- **Operation**: `redistribute`
- **BackoffLimit**: `1`
- **TTLSecondsAfterFinished**: `3600`
- **Authentication**: `PGPASSWORD` from the cluster's admin password Secret

Monitor the redistribution Job:

```bash
kubectl get jobs -n cloudberry-test \
  -l avsoft.io/cluster=my-cluster,avsoft.io/operation=maintenance
```

### Scale Metrics

The operator exposes Prometheus metrics for scale operations:

| Metric | Type | Description |
|--------|------|-------------|
| `cloudberry_scale_operations_total` | Counter | Total number of scale operations (labels: `cluster`, `namespace`, `operation`) |
| `cloudberry_redistribution_progress` | Gauge | Data redistribution progress from 0.0 to 1.0 (labels: `cluster`, `namespace`) |

The `operation` label distinguishes between `scale-out` and `scale-in` operations:

```promql
# Total scale-out operations
cloudberry_scale_operations_total{operation="scale-out"}

# Total scale-in operations
cloudberry_scale_operations_total{operation="scale-in"}
```

These metrics complement the existing segment metrics (`cloudberry_segments_total`, `cloudberry_segments_ready`) to provide full visibility into scaling operations.

## Segment Rebalancing

After recovery or failover events, segments may not be in their preferred roles — a mirror may be acting as primary. The rebalance operation redistributes data across segments to restore optimal data placement. The operator supports configurable rebalance with skew thresholds, parallelism control, and table exclusion patterns.

### Rebalance Configuration

Configure rebalance behavior in the `spec.segments.rebalance` section of the `CloudberryCluster` CRD:

```yaml
spec:
  segments:
    count: 4
    rebalance:
      skewThreshold: 10       # Percentage skew threshold (default: 10)
      parallelism: 2           # Concurrent table redistributions (default: 2)
      excludeTables:           # Tables to skip during rebalance
        - audit_log
        - "temp_*"
```

**Configuration fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `skewThreshold` | int | `10` | Percentage of data skew that triggers rebalance. A value of `10` means rebalance activates when any segment holds 10% more data than the average |
| `parallelism` | int | `2` | Number of tables to redistribute concurrently. Higher values speed up rebalance but increase cluster load |
| `excludeTables` | string[] | `[]` | Tables to skip during rebalance. Supports exact names (`audit_log`) and glob patterns (`temp_*`) |

> **Note**: If `spec.segments.rebalance` is not set, the operator uses default values (`skewThreshold=10`, `parallelism=2`, no excluded tables).

### Triggering a Rebalance

You can trigger a rebalance operation in three ways:

#### Via CLI

```bash
# Rebalance all segments (uses configured or default settings)
cloudberry-ctl ha rebalance --cluster my-cluster

# Rebalance specific tables only
cloudberry-ctl ha rebalance --cluster my-cluster --tables orders,customers,logs

# Check rebalance status
cloudberry-ctl ha rebalance --cluster my-cluster --status
```

#### Via Annotation

```bash
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/action=rebalance
```

The operator processes the annotation, creates a rebalance Job, and removes the annotation after handling.

#### Via API

```bash
# Trigger rebalance
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/rebalance

# Trigger rebalance for specific tables
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/rebalance \
  -H "Content-Type: application/json" \
  -d '{"tables": ["orders", "customers", "logs"]}'
```

### Monitoring Rebalance Status

#### Via CLI

```bash
cloudberry-ctl ha rebalance --cluster my-cluster --status
```

**Output (JSON):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "config": {
    "skewThreshold": 10,
    "parallelism": 2,
    "excludeTables": ["audit_log", "temp_*"]
  },
  "redistribution": {
    "status": "True",
    "reason": "RebalanceCompleted",
    "message": "Rebalance completed successfully",
    "lastTransition": "2026-05-14T10:05:00Z"
  }
}
```

#### Via API

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/rebalance/status"
```

See [API Reference — Rebalance Status](api-reference.md#rebalance-status) for the full response schema.

#### Via kubectl

```bash
# Check the DataRedistribution condition
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.conditions[?(@.type=="DataRedistribution")]}'

# Watch events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -i rebalance

# Check the rebalance Job
kubectl get jobs -n cloudberry-test \
  -l avsoft.io/cluster=my-cluster,avsoft.io/operation=maintenance
```

### Rebalance Events and Conditions

The operator emits the following events during rebalance:

| Event | Type | Description |
|-------|------|-------------|
| `RebalanceStarted` | Normal | Rebalance operation initiated with configuration details |
| `RebalanceCompleted` | Normal | Rebalance operation completed successfully |

The `DataRedistribution` status condition tracks rebalance progress:

| Reason | Status | Description |
|--------|--------|-------------|
| `RebalanceStarted` | `True` | Rebalance is in progress |
| `RebalanceCompleted` | `True` | Rebalance finished successfully |

**Example condition:**

```json
{
  "type": "DataRedistribution",
  "status": "True",
  "reason": "RebalanceCompleted",
  "message": "Rebalance completed successfully",
  "lastTransitionTime": "2026-05-14T10:05:00Z"
}
```

### Rebalance Metrics

The operator exposes the following metrics related to rebalance operations:

| Metric | Type | Description |
|--------|------|-------------|
| `cloudberry_scale_operations_total{operation="rebalance"}` | Counter | Total number of rebalance operations completed |
| `cloudberry_data_skew_coefficient` | Gauge | Current data skew coefficient across segments (labels: `cluster`, `namespace`) |

```promql
# Total rebalance operations for a cluster
cloudberry_scale_operations_total{operation="rebalance", cluster="my-cluster"}

# Current data skew coefficient
cloudberry_data_skew_coefficient{cluster="my-cluster"}
```

## Test Data Setup

The project includes a data loading scenario (Scenario 7) that populates a realistic test dataset for validating scale, rebalance, and performance operations. The dataset uses the `mydb` database and creates five tables with different distribution strategies, intentional data skew, and exclusion patterns.

### Test Data Schema

| Table | Rows | Approx. Size | Distribution | Description |
|-------|------|-------------|-------------|-------------|
| `orders` | 1,000,000 | 101 MB | hash (`customer_id`) | 500K from Scenario 6 + 500K Pareto-skewed |
| `logs` | 200,000 | 56 MB | random | Application log entries with JSONB metadata |
| `customers` | 100,000 | 17 MB | hash (`id`) | Pre-existing from Scenario 6 |
| `audit_log` | 100,000 | 25 MB | hash (`id`) | Excluded from rebalance (`exclude_from_rebalance=true`) |
| `temp_staging` | 50,000 | 12 MB | hash (`id`) | Matches `temp_*` exclusion pattern |

**Total**: ~1,450,000 rows, ~218 MB, 16 indexes across all tables.

Distribution metadata is stored via `COMMENT ON TABLE`, which encodes the distribution type, key, and exclusion flags. For example:

```sql
COMMENT ON TABLE orders IS 'distribution=hash, key=customer_id';
COMMENT ON TABLE logs IS 'distribution=random';
COMMENT ON TABLE audit_log IS 'distribution=hash, key=id, exclude_from_rebalance=true';
COMMENT ON TABLE temp_staging IS 'distribution=hash, key=id, temporary_staging=true';
```

The `analyst` role receives `SELECT` on all tables and `USAGE` on all sequences.

### Pareto Skew Pattern

The `orders` table uses a Pareto (80/20) distribution to create measurable data skew for rebalance testing:

- **80%** of the 500K new orders target the first 20,000 customers (IDs 1–20,000)
- **20%** of the 500K new orders target the remaining 80,000 customers (IDs 20,001–100,000)

This produces a realistic skew where a small fraction of distribution keys hold a disproportionate share of the data. Use this pattern to verify that:

1. The `inspect skew` command correctly detects uneven data distribution
2. Rebalance operations redistribute data across segments
3. Query performance degrades predictably on skewed tables

```sql
-- Pareto distribution logic (from scenario7_load_data.sql)
INSERT INTO orders (customer_id, amount, status)
SELECT
    CASE WHEN random() < 0.8
        THEN (random() * 19999 + 1)::int          -- 80% to first 20K customers
        ELSE (random() * 79999 + 20001)::int       -- 20% to remaining 80K
    END,
    (random() * 5000 + 1)::numeric(10,2),
    CASE (random() * 4)::int
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'completed'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'cancelled'
        ELSE 'returned'
    END
FROM generate_series(1, 500000);
```

### Rebalance Exclusion Patterns

Two tables are configured to be excluded from rebalance operations:

**1. `audit_log` — Explicit exclusion flag**

The `audit_log` table has `exclude_from_rebalance=true` in its distribution comment. Rebalance operations should skip this table entirely, preserving its current data placement. This is useful for compliance or audit tables where data locality must remain stable.

```sql
COMMENT ON TABLE audit_log IS 'distribution=hash, key=id, exclude_from_rebalance=true';
```

**2. `temp_staging` — Name pattern exclusion**

The `temp_staging` table matches the `temp_*` wildcard exclusion pattern. Any table whose name starts with `temp_` is automatically excluded from rebalance. This prevents unnecessary data movement for transient staging tables.

```sql
COMMENT ON TABLE temp_staging IS 'distribution=hash, key=id, temporary_staging=true';
```

When implementing or testing rebalance logic, verify that:
- Tables with `exclude_from_rebalance=true` are not moved
- Tables matching the `temp_*` pattern are not moved
- Only `orders`, `logs`, and `customers` participate in rebalance

### Loading Test Data

Run the data loading script against a running Cloudberry cluster:

```bash
# Load test data (uses default namespace and cluster name)
bash test/scenarios/scenario7_load_data.sh

# Override namespace and cluster name
NAMESPACE=my-ns CLUSTER=my-cluster bash test/scenarios/scenario7_load_data.sh
```

The script performs the following steps:

1. Copies `test/scenarios/scenario7_load_data.sql` to the coordinator pod via `kubectl cp`
2. Executes the SQL via `psql -U gpadmin -d mydb`
3. Verifies the results by printing table sizes, row counts, index counts, and total database size

> **Prerequisite**: Scenarios 1–6 must have been run first. The script expects the `mydb` database, `customers` table (100K rows), `orders` table (500K rows), and `analyst` role to already exist.

## Error Handling and Observability

The operator provides comprehensive error handling, retry mechanisms, and observability features to ensure reliable cluster management and easy troubleshooting.

### Structured Error Types

The operator uses a hierarchy of typed errors that support `errors.Is()` and `errors.As()` for programmatic error handling. Each error type wraps a sentinel error for easy classification:

| Error Type | Sentinel | Description |
|------------|----------|-------------|
| `ReconcileError` | (wraps inner error) | Error during reconciliation — includes the operation name and underlying cause |
| `ClusterNotFoundError` | `ErrNotFound` | Cluster resource not found in the specified namespace |
| `ValidationError` | `ErrInvalidInput` | Input validation failure — includes the field name and constraint message |
| `AuthenticationError` | `ErrUnauthorized` | Authentication failure — includes the auth method and reason |
| `PermissionDeniedError` | `ErrForbidden` | Authorization failure — includes the user, operation, and required permission |
| `SegmentNotFoundError` | `ErrNotFound` | Segment with the specified content ID not found |

**Example: Checking error types in Go**

```go
import "github.com/cloudberry-contrib/cloudberry-k8s/internal/util"

err := reconcileCluster(ctx, cluster)
if errors.Is(err, util.ErrNotFound) {
    // Handle missing resource
} else if errors.Is(err, util.ErrInvalidInput) {
    // Handle validation failure
} else if errors.Is(err, util.ErrRetryExhausted) {
    // All retry attempts failed
}
```

### Retry with Exponential Backoff

The operator uses `RetryWithBackoff()` for transient failure recovery. All retryable operations (database connections, Vault calls, API requests) use this mechanism.

**Retry behavior:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MaxRetries` | `5` | Maximum number of retry attempts after the initial call |
| `InitialBackoff` | `1s` | Wait time before the first retry |
| `MaxBackoff` | `30s` | Maximum wait time between retries |
| `Multiplier` | `2.0` | Backoff multiplier (exponential growth) |
| `JitterFraction` | `0.1` | Random jitter added to prevent thundering herd (0.0–1.0) |

**Key behaviors:**

- **Exponential growth**: Each retry waits `previous × multiplier`, capped at `MaxBackoff`
- **Context-aware**: Respects `context.Context` cancellation and deadlines — stops retrying immediately when the context expires
- **Sentinel error**: Returns `ErrRetryExhausted` (wrapped with the last error) when all attempts fail
- **Jitter**: Adds randomized jitter to prevent synchronized retries across multiple operator instances

**Example retry timeline** (with defaults):

```
Attempt 1: immediate
Attempt 2: ~1s wait
Attempt 3: ~2s wait
Attempt 4: ~4s wait
Attempt 5: ~8s wait
Attempt 6: ~16s wait
→ ErrRetryExhausted if all fail
```

### Reconciliation Metrics

The operator records metrics for every reconciliation cycle, providing visibility into operator health and performance.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_reconcile_total` | Counter | `cluster`, `namespace`, `result` | Total reconciliation count. `result` is `success` or `error` |
| `cloudberry_reconcile_errors_total` | Counter | `cluster`, `namespace` | Total reconciliation errors (incremented when `result=error`) |
| `cloudberry_reconcile_duration_seconds` | Histogram | `cluster`, `namespace` | Time spent in each reconciliation cycle |

**Useful PromQL queries:**

```promql
# Reconciliation error rate (last 5 minutes)
rate(cloudberry_reconcile_errors_total[5m])

# Average reconciliation duration
rate(cloudberry_reconcile_duration_seconds_sum[5m])
  / rate(cloudberry_reconcile_duration_seconds_count[5m])

# Success ratio
sum(rate(cloudberry_reconcile_total{result="success"}[5m]))
  / sum(rate(cloudberry_reconcile_total[5m]))
```

### Telemetry Spans

When OpenTelemetry tracing is enabled, the operator creates spans for reconciliation loops and records errors on those spans. This provides distributed tracing visibility into operator behavior.

**Error recording on spans:**

- `SetSpanError(span, err)` sets the span status to `codes.Error` and records an `exception` event with the error message
- Safe to call with `nil` error — no status change occurs
- Error spans appear in your tracing backend (Jaeger, Tempo, etc.) with error indicators

**Span attributes include:**

- Cluster name and namespace
- Reconciliation result (success/error)
- Duration
- Error details (when applicable)

### Structured Logging

The operator uses Go's `slog` package for structured JSON logging. Every log entry includes contextual fields for filtering and correlation:

```json
{
  "level": "ERROR",
  "msg": "reconciliation failed",
  "cluster": "my-cluster",
  "namespace": "cloudberry-test",
  "controller": "cluster-controller",
  "reconcileID": "abc-123",
  "error": "reconcile error during \"reconciling coordinator\": connection refused",
  "duration": "1.234s"
}
```

**Filtering logs by error type:**

```bash
# All reconciliation errors
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.level == "ERROR")'

# Errors for a specific cluster
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.level == "ERROR" and .cluster == "my-cluster")'

# All warnings (including blocked operations, degraded state)
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.level == "WARN")'
```

### Webhook Validation Errors

The validating admission webhook rejects invalid `CloudberryCluster` resources at creation time, preventing misconfigured clusters from entering the system:

| Validation | Error Message |
|------------|---------------|
| `segments.count < 1` | `segments.count must be >= 1, got 0` |
| OIDC enabled without `issuerURL` | `auth.oidc.issuerURL is required when OIDC is enabled` |
| OIDC enabled without `clientID` | `auth.oidc.clientID is required when OIDC is enabled` |
| Missing coordinator storage | `coordinator.storage.size is required` |
| Missing segment storage | `segments.storage.size is required` |
| Duplicate cluster name | `CloudberryCluster with name "X" already exists in namespace "Y"` |

**Example: Webhook rejection**

```bash
$ kubectl apply -f invalid-cluster.yaml
Error from server: error when creating "invalid-cluster.yaml":
  admission webhook "validate.cloudberrycluster.avsoft.io" denied the request:
  segments.count must be >= 1, got 0
```

### Pod Deletion Recovery

The operator automatically detects and recovers from pod deletions during normal operation:

1. **Detection**: During reconciliation, the operator compares `StatefulSet.Status.ReadyReplicas` against the expected count. When `segmentsReady < segmentsTotal`, the cluster is in a degraded state
2. **Status update**: The operator updates `status.segmentsReady` to reflect the actual ready count
3. **Recovery**: Kubernetes automatically recreates deleted StatefulSet pods. On the next reconciliation cycle, the operator detects the recovered pods and updates the status back to healthy
4. **No manual intervention**: The entire detection-recovery cycle is automatic

```bash
# Check for degraded segments
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.segmentsReady}/{.status.segmentsTotal}'

# Watch recovery in real time
kubectl get cloudberrycluster my-cluster -n cloudberry-test -w
```

## Inspection Commands

```bash
# Disk usage
cloudberry-ctl inspect disk-usage --cluster my-cluster
cloudberry-ctl inspect disk-usage --cluster my-cluster --database mydb

# Data distribution skew
cloudberry-ctl inspect skew --cluster my-cluster --table public.large_table

# Table bloat
cloudberry-ctl inspect bloat --cluster my-cluster

# Missing statistics
cloudberry-ctl inspect missing-stats --cluster my-cluster

# Server logs
cloudberry-ctl inspect logs --cluster my-cluster --severity ERROR --last 1h
```

## Auditing

The operator provides comprehensive auditing across three categories: PostgreSQL connection auditing, statement auditing, and operator-level audit logging.

### Connection Auditing

Enable connection auditing to log client connections and disconnections in PostgreSQL:

```yaml
spec:
  config:
    parameters:
      log_connections: "on"
      log_disconnections: "on"
```

These parameters are rendered into `postgresql.conf` and take effect after a configuration reload.

### Statement Auditing

Enable statement auditing to log SQL statements and their durations:

```yaml
spec:
  config:
    parameters:
      log_statement: "ddl"                # none, ddl, mod, all
      log_min_duration_statement: "1000"   # log statements taking > 1000ms
      log_duration: "on"                   # log duration of all statements
```

### Operator Audit Log

The operator logs all authentication, authorization, and administrative events as structured JSON. These logs are written to the operator's standard output and can be collected by your log aggregation system.

**Authentication success** -- logged when a user successfully authenticates:

```json
{"level":"INFO","msg":"basic auth succeeded","username":"admin","method":"basic","source_ip":"192.168.1.1:12345","permission":"Admin"}
```

**Authentication failure** -- logged when authentication fails:

```json
{"level":"WARN","msg":"authentication failed","method":"basic","error":"invalid credentials","remote_addr":"192.168.1.100:12345"}
```

**Permission denied** -- logged when an authenticated user attempts an operation they lack permission for:

```json
{"level":"WARN","msg":"permission denied","username":"viewer","method":"basic","source_ip":"192.168.1.1:12345","required_permission":"Admin","actual_permission":"Basic","path":"/api/v1alpha1/clusters","http_method":"POST"}
```

**Config changed** -- logged when a user updates cluster configuration:

```json
{"level":"INFO","msg":"config changed","cluster":"my-cluster","username":"admin","method":"basic","source_ip":"192.168.1.1:12345"}
```

**Role management** -- logged when a role is assigned to a resource group:

```json
{"level":"INFO","msg":"role assigned to resource group","cluster":"my-cluster","group":"analytics","role":"analyst","username":"admin","method":"basic","source_ip":"192.168.1.1:12345"}
```

**Filtering audit logs:**

```bash
# View all authentication events
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.msg == "basic auth succeeded" or .msg == "authentication failed")'

# View all permission denied events
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.msg == "permission denied")'

# View all config change events
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.msg == "config changed")'

# View all audit events for a specific user
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.username == "admin")'
```

## Monitoring and Observability

### Prometheus Metrics

The operator exposes metrics at the `/metrics` endpoint. Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
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
| `cloudberry_connections_max` | Gauge | Maximum configured connections |
| `cloudberry_fts_probe_total` | Counter | Total FTS probes |
| `cloudberry_fts_failover_total` | Counter | Total failovers |
| `cloudberry_replication_lag_bytes` | Gauge | Replication lag per segment |
| `cloudberry_mirroring_operations_total` | Counter | Total mirroring enable/disable operations (labels: `operation` = `enable`, `disable`) |
| `cloudberry_connections_active` | Gauge | Active database connections |
| `cloudberry_scale_operations_total` | Counter | Total scale operations (labels: `operation` = `scale-out`, `scale-in`, `rebalance`) |
| `cloudberry_redistribution_progress` | Gauge | Data redistribution progress (0.0–1.0) |
| `cloudberry_data_skew_coefficient` | Gauge | Data skew coefficient across segments |
| `cloudberry_pvc_size_bytes` | Gauge | PVC size in bytes (labels: `cluster`, `namespace`, `component`) |
| `cloudberry_resource_group_cpu_usage` | Gauge | CPU usage per resource group (labels: `cluster`, `namespace`, `group`) |
| `cloudberry_resource_group_memory_usage` | Gauge | Memory usage per resource group (labels: `cluster`, `namespace`, `group`) |

### ServiceMonitor

Enable the Prometheus Operator ServiceMonitor in the Helm values:

```yaml
serviceMonitor:
  enabled: true
  interval: 30s
  labels:
    release: prometheus
```

### OpenTelemetry Tracing

Enable OTLP tracing in the CRD:

```yaml
spec:
  telemetry:
    enabled: true
    otlpEndpoint: otel-collector:4317
    otlpProtocol: grpc
    samplingRate: 0.5
```

Traces include spans for reconciliation loops, API requests, database operations, and Vault interactions.

### Structured Logging

Operator logs use structured JSON format with standard fields:

```bash
# View operator logs
kubectl logs -n cloudberry-system deployment/cloudberry-operator

# Filter by cluster
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.cluster == "my-cluster")'

# Filter by level
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.level == "ERROR")'
```

### API Rate Limiting

The operator REST API enforces per-IP rate limiting to protect against abuse and brute-force attacks:

- **Default limit**: 10 requests per minute per client IP
- **Algorithm**: Token bucket with automatic refill
- **Scope**: Applied to all authenticated endpoints (health checks are exempt)
- **IP identification**: Uses `RemoteAddr` by default. Proxy headers (`X-Forwarded-For`, `X-Real-IP`) are only trusted when the request comes from a configured trusted proxy CIDR range. This prevents clients from spoofing forwarded headers to bypass rate limiting
- **Response**: When the limit is exceeded, the API returns `429 Too Many Requests` with a `Retry-After` header

#### Trusted Proxies

By default, the rate limiter does **not** trust any proxy headers — it uses only the direct connection's `RemoteAddr` for client IP identification. If the operator runs behind a load balancer or reverse proxy, configure trusted proxy CIDR ranges so the rate limiter correctly identifies client IPs:

```yaml
# Example: trust the cluster's internal pod network
# Configure via operator startup options
trustedProxies:
  - "10.0.0.0/8"
  - "172.16.0.0/12"
```

When a request arrives from an IP within a trusted proxy range, the rate limiter reads the client IP from the `X-Forwarded-For` header (first IP in the chain) or `X-Real-IP` header. Invalid CIDR strings are logged as warnings and ignored.

If you encounter rate limiting with `cloudberry-ctl`, wait for the `Retry-After` period before retrying. For automation scripts, implement exponential backoff when receiving 429 responses.

```bash
# Example: 429 response
# HTTP/1.1 429 Too Many Requests
# Retry-After: 7
# {"error":{"code":"RATE_LIMITED","message":"too many requests, please retry later"}}
```

### Kubernetes Events

The operator emits events for significant state changes:

```bash
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp'
```

Key event types:

| Event | Type | Description |
|-------|------|-------------|
| `Stopping` | Normal | Cluster stop initiated |
| `Stopped` | Normal | Cluster fully stopped (0 pods) |
| `Starting` | Normal | Cluster start initiated |
| `Started` | Normal | Cluster fully started |
| `Restarting` | Normal | Cluster restart initiated |
| `Restarted` | Normal | Cluster restart completed |
| `ConfigReloaded` | Normal | Configuration reloaded without restart |
| `RollingRestartStarted` | Normal | Rolling restart initiated for restart-required params |
| `RollingRestartCompleted` | Normal | Rolling restart completed |
| `MaintenanceStarted` | Normal | Maintenance Job created |
| `MaintenanceUnknown` | Warning | Unknown maintenance operation requested |
| `ScaleOutStarted` | Normal | Scale-out operation initiated |
| `ScaleOutCompleted` | Normal | Scale-out operation completed |
| `ScaleInStarted` | Normal | Scale-in operation initiated |
| `ScaleInCompleted` | Normal | Scale-in operation completed |
| `ScaleOutBlocked` | Warning | Scale-out blocked (cluster not in Running phase) |
| `ScaleOutFailed` | Warning | Scale-out failed (segments not ready after timeout) |
| `ScaleInBlocked` | Warning | Scale-in blocked (cluster not in Running phase, or >50% reduction without confirmation) |
| `RebalanceStarted` | Normal | Segment rebalance initiated with configuration details |
| `RebalanceCompleted` | Normal | Segment rebalance completed successfully |
| `StorageExpanded` | Normal | PVC storage expanded successfully |
| `UpgradeStarted` | Normal | Cluster upgrade initiated (includes previous and new version) |
| `UpgradeCompleted` | Normal | Cluster upgrade completed successfully |
| `UpgradeBlocked` | Warning | Upgrade blocked — cluster not in `Running` phase |
| `UpgradeRollback` | Warning | Upgrade rolled back due to phase timeout |
| `Deleting` | Normal | Cluster deletion initiated, phase set to `Deleting` |
| `BackupOnDelete` | Normal | Backup triggered before deletion (when `backupOnDelete: true`) |
| `PVCsRetained` | Normal | PVCs preserved after deletion (when `deletionPolicy: Retain`) |
| `PVCsDeleted` | Normal | All PVCs deleted after deletion (when `deletionPolicy: Delete`) |
| `Deleted` | Normal | Cluster deletion completed, finalizer removed |
| `SegmentFailover` | Warning | Primary segment failed, mirror promoted |
| `SegmentRecovered` | Normal | Failed segment recovered |
| `SegmentsRebalanced` | Normal | Segment roles restored |
| `CoordinatorFailover` | Warning | Coordinator failed, standby activated |
| `StandbyInitialized` | Normal | Standby coordinator initialized |
| `MirroringEnabled` | Normal | Mirroring enable initiated — mirror StatefulSet created |
| `MirroringDisabled` | Normal | Mirroring disabled — mirror StatefulSet deleted |
| `MirroringInitializing` | Normal | Mirror initialization in progress |
| `MirroringInSync` | Normal | All mirrors synchronized — mirroring enable complete |
| `MirroringDegraded` | Warning | One or more mirrors out of sync, or mirroring enable timed out |
| `MirroringRestored` | Normal | All mirrors back in sync |
| `RecoveryStarted` | Normal | Recovery operation initiated |
| `RecoveryCompleted` | Normal | Recovery operation completed |
| `RecoveryFailed` | Warning | Recovery operation failed |
| `WorkloadReconciled` | Normal | Workload management reconciled successfully |
| `AuthReconciled` | Normal | Authentication configuration reconciled |
| `OIDCValidationFailed` | Warning | OIDC validation failed (with details) |
| `OIDCConfigured` | Normal | OIDC authentication properly configured |

### Vault Integration

Enable Vault for centralized secrets management:

```yaml
spec:
  vault:
    enabled: true
    address: http://vault:8200
    authMethod: kubernetes    # token, kubernetes, or approle
    authPath: auth/kubernetes  # auth mount path (method-specific)
    role: cloudberry-operator
    secretPath: secret/data/cloudberry
```

#### Authentication Methods

The operator supports three Vault authentication methods:

| Method | Configuration | Use Case |
|--------|--------------|----------|
| `token` | Static Vault token passed via `VAULT_TOKEN` env var or config | Development, CI/CD |
| `kubernetes` | Kubernetes service account JWT exchanged for Vault token via `auth/kubernetes/login` | Production (recommended) |
| `approle` | AppRole `role_id` and `secret_id` used to obtain a client token via `auth/approle/login` | Automation, CI/CD pipelines |

**Token auth** (development):

```yaml
spec:
  vault:
    enabled: true
    address: http://vault:8200
    authMethod: token
    secretPath: secret/data/cloudberry
```

**Kubernetes auth** (production):

```yaml
spec:
  vault:
    enabled: true
    address: http://vault:8200
    authMethod: kubernetes
    authPath: auth/kubernetes
    role: cloudberry-operator
    secretPath: secret/data/cloudberry
```

Kubernetes auth requires a Vault Kubernetes auth backend configured with the correct API server hostname and a dedicated service account for token review. See [Kubernetes Auth for Vault PKI](#kubernetes-auth-for-vault-pki-production) for the full setup procedure.

> **Docker Desktop**: Use `kubernetes_host: https://kubernetes.docker.internal:6443` when configuring the Vault Kubernetes auth backend. The Kubernetes API server certificate only includes `kubernetes.docker.internal` as a SAN — using `host.docker.internal` causes TLS verification failures.

**AppRole auth** (automation):

```yaml
spec:
  vault:
    enabled: true
    address: http://vault:8200
    authMethod: approle
    authPath: auth/approle
    role: cloudberry-operator
    secretPath: secret/data/cloudberry
```

#### KV Secret Paths

Vault stores cluster secrets at the following KV v2 paths under the configured `secretPath`:

| Path | Contents | Description |
|------|----------|-------------|
| `secret/data/cloudberry/admin-password` | `username`, `password` | Admin database password |
| `secret/data/cloudberry/oidc-secret` | `client_id`, `client_secret` | OIDC client credentials |
| `secret/data/cloudberry/monitoring-password` | `username`, `password` | Monitoring role password |
| `secret/data/cloudberry/tls` | `ca_cert`, `tls_cert`, `tls_key` | TLS certificates (optional, alternative to K8s TLS Secrets) |

#### Secret Rotation Watch

The operator includes a `SecretWatcher` that periodically polls Vault secrets and detects changes via SHA-256 hash comparison:

1. On each poll interval, the watcher reads the secret from Vault
2. Computes a SHA-256 hash of the secret data
3. Compares the new hash against the previously stored hash
4. If the hash differs, invokes the registered `onChange` callback
5. The callback updates the corresponding Kubernetes Secret and reloads affected components (e.g., database password rotation, TLS certificate reload)

This mechanism ensures that secrets updated directly in Vault are automatically propagated to the cluster without manual intervention.

#### Connection Retry Configuration

The Vault client uses exponential backoff with jitter for connection retries:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MaxRetries` | `5` | Maximum retry attempts after the initial call |
| `InitialBackoff` | `1s` | Wait time before the first retry |
| `MaxBackoff` | `30s` | Maximum wait time between retries |
| `Multiplier` | `2.0` | Backoff multiplier (exponential growth) |
| `JitterFraction` | `0.1` | Random jitter to prevent thundering herd |

**Example retry timeline**:

```
Attempt 1: immediate
Attempt 2: ~1s wait
Attempt 3: ~2s wait
Attempt 4: ~4s wait
Attempt 5: ~8s wait
→ Error if all attempts fail
```

> **Note**: Vault integration is comprehensively verified by Scenario 46. See `test/functional/scenario46_vault_integration_test.go` and `test/e2e/scenario46_vault_integration_e2e_test.go` for the full test suite covering all 3 auth methods, all 4 KV paths, secret rotation watch, and retry configuration.

## Performance Tuning

### Performance Characteristics

Based on performance testing (2026-05-19), the operator exhibits the following characteristics:

| Endpoint Type | p50 Latency | p95 Latency | p99 Latency | Peak RPS | Error Rate |
|---------------|-------------|-------------|-------------|----------|------------|
| Health (`/healthz`, `/readyz`) | 2.7ms | 6.5ms | 10.6ms | 12,637 | 0% |
| API (authenticated) | 605ms | 794ms | 885ms | ~6 | 0% |

**Key observations:**
- Health endpoints are extremely fast (sub-3ms p50) and handle over 12,000 RPS
- API endpoint latency is dominated by bcrypt password verification (~100ms per request at cost factor 10)
- The operator maintains zero errors across 287,000+ requests under all load conditions
- Memory remains stable at ~82MB resident with no growth observed during sustained load

### Latency Breakdown (API Endpoints)

| Component | Contribution | Percentage |
|-----------|-------------|------------|
| bcrypt auth (cost 10) | ~100ms/req | ~80% |
| Kubernetes API call | ~20-30ms/req | ~16% |
| HTTP/JSON overhead | ~5ms/req | ~4% |

### Tuning Recommendations

**Rate Limiting**: The default rate limit is 10 requests per minute per IP. For environments with high API usage, increase the limit:

```bash
# Set via environment variable on the operator
CLOUDBERRY_API_RATE_LIMIT=1000
```

**bcrypt Cost Factor**: The default bcrypt cost factor is 10. For development environments where latency is more important than security, you can reduce it. For production, consider implementing JWT token-based authentication after initial login to amortize the bcrypt cost.

**Monitoring Stack**: Deploy the monitoring stack for real-time visibility into operator performance:

```bash
# Deploy vmagent + otel-collector via Makefile
make monitoring-deploy

# Check status
make monitoring-status
```

**Key metrics to monitor for performance:**

```promql
# Reconciliation duration (should be < 5s for healthy clusters)
histogram_quantile(0.95, rate(cloudberry_reconcile_duration_seconds_bucket[5m]))

# Reconciliation error rate (should be 0 for healthy clusters)
rate(cloudberry_reconcile_errors_total[5m])

# Active database connections
cloudberry_connections_active

# FTS probe success rate
rate(cloudberry_fts_probe_total{result="success"}[5m])
  / rate(cloudberry_fts_probe_total[5m])
```

### Monitoring Stack Deployment

The operator integrates with VictoriaMetrics and OpenTelemetry for full observability:

```bash
# Deploy monitoring stack to Kubernetes
make monitoring-deploy

# Or deploy alongside the operator with Helm
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set metrics.enabled=true \
  --set serviceMonitor.enabled=true \
  --set telemetry.enabled=true \
  --set telemetry.otlpEndpoint=otel-collector:4317 \
  --set telemetry.otlpInsecure=true

# Check monitoring status
make monitoring-status

# Remove monitoring stack
make monitoring-undeploy
```

Pre-built Grafana dashboards are available in the `monitoring/grafana/` directory.
