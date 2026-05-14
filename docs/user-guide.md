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
- [Configuration Management](#configuration-management)
  - [Hot-Reload vs Rolling Restart](#hot-reload-vs-rolling-restart)
  - [Restart-Required Parameters](#restart-required-parameters)
  - [Rolling Restart Behavior](#rolling-restart-behavior)
- [Authentication Setup](#authentication-setup)
- [Webhook Certificate Setup](#webhook-certificate-setup)
- [High Availability](#high-availability)
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
- [Inspection Commands](#inspection-commands)
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

## Webhook Certificate Setup

The operator's admission webhooks require TLS certificates. The operator manages these certificates automatically using one of two strategies.

### Self-Signed Certificates (Default)

No configuration is needed. The operator generates a self-signed CA and server certificate on startup, stores them in a Kubernetes Secret, and injects the CA bundle into the webhook configurations.

Certificates are checked for rotation every 12 hours and automatically rotated when 2/3 of their lifetime has elapsed.

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

Ensure the Vault PKI role allows issuing certificates for the webhook service DNS names.

### Verifying Webhook Certificates

```bash
# Check the certificate Secret
kubectl get secret -n cloudberry-system -l app.kubernetes.io/component=webhook-certs

# Verify the webhook configuration has a CA bundle
kubectl get validatingwebhookconfigurations -o jsonpath='{.items[*].webhooks[*].clientConfig.caBundle}' | head -c 50
```

### Troubleshooting Webhook Certificates

If webhook calls fail with TLS errors:

1. Check that the certificate Secret exists and contains valid data:
   ```bash
   kubectl get secret <release>-webhook-certs -n cloudberry-system -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -text -noout
   ```

2. Verify the CA bundle in the webhook configuration matches the CA in the Secret:
   ```bash
   kubectl get validatingwebhookconfiguration <release>-validating -o jsonpath='{.webhooks[0].clientConfig.caBundle}' | base64 -d | openssl x509 -text -noout
   ```

3. If using Vault PKI, ensure the Vault server is reachable and the PKI role is properly configured.

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

Unknown operations emit a `MaintenanceUnknown` warning event and are not executed.

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
