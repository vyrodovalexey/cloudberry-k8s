# User Guide

This guide covers day-to-day operations for managing Cloudberry Database clusters with the Cloudberry Operator.

## Table of Contents

- [Creating a CloudberryCluster](#creating-a-cloudberrycluster)
- [Managing Cluster Lifecycle](#managing-cluster-lifecycle)
- [Configuration Management](#configuration-management)
- [Authentication Setup](#authentication-setup)
- [High Availability](#high-availability)
- [Maintenance Operations](#maintenance-operations)
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
- **Coordinator port**: `5432` (all components, including segments, use this port)
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

## Managing Cluster Lifecycle

### Starting a Cluster

```bash
# Normal start — all components
cloudberry-ctl cluster start --cluster my-cluster

# Restricted start — superuser connections only
cloudberry-ctl cluster start --cluster my-cluster --mode restricted

# Maintenance start — coordinator only in utility mode
cloudberry-ctl cluster start --cluster my-cluster --mode maintenance
```

Or via annotation:

```bash
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/action=start
```

### Stopping a Cluster

```bash
# Smart stop — wait for clients to disconnect (default)
cloudberry-ctl cluster stop --cluster my-cluster

# Fast stop — rollback active transactions, disconnect clients
cloudberry-ctl cluster stop --cluster my-cluster --mode fast

# Immediate stop — abort all connections immediately
cloudberry-ctl cluster stop --cluster my-cluster --mode immediate
```

### Restarting a Cluster

```bash
cloudberry-ctl cluster restart --cluster my-cluster
```

This performs a fast stop followed by a normal start.

### Upgrading a Cluster

To upgrade the database version, update the `spec.image` field:

```bash
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge -p '
  {"spec": {"image": "postgres:17"}}'
```

The operator performs a rolling upgrade:
1. Pre-flight checks (cluster healthy, disk space)
2. Update mirror StatefulSets (rolling)
3. Update primary segment StatefulSets (rolling)
4. Update standby coordinator
5. Update coordinator
6. Verify cluster health

If any step fails the health check, the operator reverts to the previous image.

### Deleting a Cluster

```bash
# Delete via cloudberry-ctl (requires confirmation)
cloudberry-ctl cluster delete --cluster my-cluster --confirm

# Delete via kubectl
kubectl delete cloudberrycluster my-cluster -n cloudberry-test
```

The deletion behavior depends on the `deletionPolicy`:
- **Retain** (default): PVCs are preserved after deletion
- **Delete**: PVCs are removed along with the cluster

If `backupOnDelete: true`, the operator triggers a backup before deletion.

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

### SSL/TLS Configuration

1. **Create a TLS secret**:

```bash
kubectl create secret tls cloudberry-tls \
  -n cloudberry-test \
  --cert=server.crt \
  --key=server.key
```

2. **Enable SSL in the CRD**:

```yaml
spec:
  auth:
    ssl:
      enabled: true
      certSecret:
        name: cloudberry-tls
      minTLSVersion: "1.2"
```

### Role Management

```bash
# List roles
cloudberry-ctl auth roles list --cluster my-cluster

# Create a role
cloudberry-ctl auth roles create --cluster my-cluster \
  --name analyst --login --password mypass

# Update a role
cloudberry-ctl auth roles update --cluster my-cluster \
  --name analyst --valid-until "2026-12-31"

# Delete a role
cloudberry-ctl auth roles delete --cluster my-cluster --name analyst
```

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

### Session Management

```bash
# List active sessions
cloudberry-ctl sessions list --cluster my-cluster

# Filter by state
cloudberry-ctl sessions list --cluster my-cluster --state active

# Filter by user
cloudberry-ctl sessions list --cluster my-cluster --user analyst

# Cancel a running query
cloudberry-ctl sessions cancel-query --cluster my-cluster --pid 12345

# Terminate a session
cloudberry-ctl sessions terminate --cluster my-cluster --pid 12345
```

### Inspection Commands

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
| `cloudberry_fts_probe_total` | Counter | Total FTS probes |
| `cloudberry_fts_failover_total` | Counter | Total failovers |
| `cloudberry_replication_lag_bytes` | Gauge | Replication lag per segment |
| `cloudberry_connections_active` | Gauge | Active database connections |

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
- **Response**: When the limit is exceeded, the API returns `429 Too Many Requests` with a `Retry-After` header

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
| `SegmentFailover` | Warning | Primary segment failed, mirror promoted |
| `SegmentRecovered` | Normal | Failed segment recovered |
| `SegmentsRebalanced` | Normal | Segment roles restored |
| `CoordinatorFailover` | Warning | Coordinator failed, standby activated |
| `StandbyInitialized` | Normal | Standby coordinator initialized |
| `MirroringDegraded` | Warning | One or more mirrors out of sync |
| `MirroringRestored` | Normal | All mirrors back in sync |
| `RecoveryStarted` | Normal | Recovery operation initiated |
| `RecoveryCompleted` | Normal | Recovery operation completed |
| `RecoveryFailed` | Warning | Recovery operation failed |

### Vault Integration

Enable Vault for centralized secrets management:

```yaml
spec:
  vault:
    enabled: true
    address: http://vault:8200
    authMethod: kubernetes
    role: cloudberry-operator
    secretPath: secret/data/cloudberry
```

Vault stores:
- Admin password at `secret/data/cloudberry/admin-password`
- OIDC client secret at `secret/data/cloudberry/oidc-secret`
- Monitoring password at `secret/data/cloudberry/monitoring-password`
- TLS certificates at `secret/data/cloudberry/tls` (optional)

The operator periodically polls Vault for secret changes and automatically updates Kubernetes Secrets and reloads affected components.
