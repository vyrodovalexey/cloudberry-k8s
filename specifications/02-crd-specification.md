# Cloudberry Operator - CRD Specification

**Version**: 1.0.0
**API Group**: avsoft.io
**API Version**: v1alpha1

---

## 1. CloudberryCluster CRD

### 1.1 Full Schema

```yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: cloudberryclusters.avsoft.io
spec:
  group: avsoft.io
  versions:
    - name: v1alpha1
      served: true
      storage: true
      subresources:
        status: {}
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              required: [coordinator, segments]
              properties:
                # --- Cluster Identity ---
                version:
                  type: string
                  description: "Cloudberry DB version"
                  default: "7.7"
                image:
                  type: string
                  description: "Container image for Cloudberry DB"
                  default: "cloudberrydb/cloudberry:7.7"
                imagePullPolicy:
                  type: string
                  enum: [Always, IfNotPresent, Never]
                  default: IfNotPresent
                imagePullSecrets:
                  type: array
                  items:
                    type: object
                    properties:
                      name:
                        type: string

                # --- Coordinator ---
                coordinator:
                  type: object
                  required: [storage]
                  properties:
                    replicas:
                      type: integer
                      minimum: 1
                      maximum: 1
                      default: 1
                    resources:
                      type: object
                      properties:
                        requests:
                          type: object
                          properties:
                            cpu: { type: string }
                            memory: { type: string }
                        limits:
                          type: object
                          properties:
                            cpu: { type: string }
                            memory: { type: string }
                    storage:
                      type: object
                      required: [size]
                      properties:
                        storageClass:
                          type: string
                        size:
                          type: string
                          default: "10Gi"
                    nodeSelector:
                      type: object
                      additionalProperties:
                        type: string
                    tolerations:
                      type: array
                      items:
                        type: object
                    port:
                      type: integer
                      default: 5432

                # --- Standby Coordinator ---
                standby:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                      default: false
                    resources:
                      type: object
                      properties:
                        requests:
                          type: object
                          properties:
                            cpu: { type: string }
                            memory: { type: string }
                        limits:
                          type: object
                          properties:
                            cpu: { type: string }
                            memory: { type: string }
                    storage:
                      type: object
                      properties:
                        storageClass:
                          type: string
                        size:
                          type: string
                          default: "10Gi"
                    nodeSelector:
                      type: object
                      additionalProperties:
                        type: string

                # --- Segments ---
                segments:
                  type: object
                  required: [count, storage]
                  properties:
                    count:
                      type: integer
                      minimum: 1
                      description: "Total number of primary segments"
                    primariesPerHost:
                      type: integer
                      minimum: 1
                      default: 2
                    mirroring:
                      type: object
                      properties:
                        enabled:
                          type: boolean
                          default: true
                        layout:
                          type: string
                          enum: [group, spread]
                          default: group
                    resources:
                      type: object
                      properties:
                        requests:
                          type: object
                          properties:
                            cpu: { type: string }
                            memory: { type: string }
                        limits:
                          type: object
                          properties:
                            cpu: { type: string }
                            memory: { type: string }
                    storage:
                      type: object
                      required: [size]
                      properties:
                        storageClass:
                          type: string
                        size:
                          type: string
                          default: "20Gi"
                    nodeSelector:
                      type: object
                      additionalProperties:
                        type: string
                    tolerations:
                      type: array
                      items:
                        type: object
                    antiAffinity:
                      type: string
                      enum: [preferred, required]
                      default: preferred
                    rebalance:
                      type: object
                      description: "Segment rebalancing configuration"
                      properties:
                        skewThreshold:
                          type: integer
                          default: 10
                          description: "Percentage skew threshold to trigger rebalance"
                        parallelism:
                          type: integer
                          default: 2
                          description: "Number of tables to redistribute concurrently"
                        excludeTables:
                          type: array
                          items:
                            type: string
                          description: "Tables to skip during rebalance (supports glob patterns)"

                # --- Authentication & Authorization ---
                auth:
                  type: object
                  properties:
                    basic:
                      type: object
                      properties:
                        enabled:
                          type: boolean
                          default: true
                        adminUser:
                          type: string
                          default: "gpadmin"
                        adminPasswordSecret:
                          type: object
                          properties:
                            name: { type: string }
                            key: { type: string }
                    oidc:
                      type: object
                      description: >
                        OIDC/JWT authentication configuration. When both basic and OIDC are enabled,
                        the auth middleware routes requests based on the Authorization header prefix:
                        "Basic ..." → basic provider, "Bearer ..." → OIDC provider. The operator
                        initializes the OIDC provider in startAPIServer() when cfg.OIDC.Enabled is
                        true, with graceful fallback to Basic-only auth if OIDC initialization fails.
                        Dual-mode behavior is verified by Scenario 38 against a real Keycloak instance
                        (test/functional/scenario38_dual_auth_test.go). Note: Keycloak requires an
                        audience mapper for the clientID and a matching frontendUrl for the issuerURL.
                      properties:
                        enabled:
                          type: boolean
                          default: false
                        issuerURL:
                          type: string
                        clientID:
                          type: string
                        clientSecret:
                          type: object
                          properties:
                            secretRef:
                              type: object
                              properties:
                                name: { type: string }
                                key: { type: string }
                        scopes:
                          type: array
                          items:
                            type: string
                          default: ["openid", "profile", "email"]
                        roleClaimPath:
                          type: string
                          default: "realm_access.roles"
                        roleClaimSource:
                          type: string
                          enum: [id_token, userinfo]
                          default: id_token
                        roleMatchMode:
                          type: string
                          enum: [exact, suffix, prefix, contains]
                          default: exact
                        roleMapping:
                          type: object
                          additionalProperties:
                            type: string
                          description: "Map IdP roles to permission levels"
                        pkce:
                          type: boolean
                          default: true
                        allowLocalSignIn:
                          type: boolean
                          default: true
                    hbaRules:
                      type: array
                      description: >
                        Host-based authentication rules for pg_hba.conf. When omitted or empty,
                        the operator generates secure default rules (see specification 05, section 6.3).
                        Default behavior is verified by Scenario 45 (test/functional/scenario45_hba_defaults_test.go).
                      items:
                        type: object
                        required: [type, database, user, method]
                        properties:
                          type:
                            type: string
                            enum: [local, host, hostssl, hostnossl]
                          database:
                            type: string
                          user:
                            type: string
                          address:
                            type: string
                          method:
                            type: string
                            enum: [trust, reject, md5, scram-sha-256, password, ident, peer, gss, ldap, cert, pam, radius]
                          options:
                            type: string
                    ssl:
                      type: object
                      properties:
                        enabled:
                          type: boolean
                          default: false
                        certSecret:
                          type: object
                          properties:
                            name: { type: string }
                        minTLSVersion:
                          type: string
                          enum: ["1.2", "1.3"]
                          default: "1.2"

                # --- Configuration ---
                config:
                  type: object
                  properties:
                    parameters:
                      type: object
                      additionalProperties:
                        type: string
                      description: "Cluster-wide postgresql.conf parameters"
                    coordinatorParameters:
                      type: object
                      additionalProperties:
                        type: string
                      description: "Coordinator-only parameters"
                    databaseParameters:
                      type: object
                      additionalProperties:
                        type: object
                        additionalProperties:
                          type: string
                      description: "Per-database parameters"
                    roleParameters:
                      type: object
                      additionalProperties:
                        type: object
                        additionalProperties:
                          type: string
                      description: "Per-role parameters"

                # --- High Availability ---
                ha:
                  type: object
                  properties:
                    ftsProbeInterval:
                      type: integer
                      default: 60
                      description: "FTS probe interval in seconds"
                    ftsProbeTimeout:
                      type: integer
                      default: 20
                      description: "Per-attempt timeout in seconds for FTS probe calls to GetSegmentConfiguration. Each retry attempt uses a fresh context with this deadline. Used by probeSegmentConfigWithRetries in the HA controller."
                    ftsProbeRetries:
                      type: integer
                      default: 5
                      description: "Maximum number of retry attempts for GetSegmentConfiguration during FTS probe. Each failed attempt increments fts_probe_failures_total. After all retries are exhausted, the probe cycle fails."
                    checksums:
                      type: boolean
                      default: true
                      description: "Enable storage-layer checksums"

                # --- Vault Integration ---
                # Comprehensive Vault integration is verified by Scenario 46
                # (test/functional/scenario46_vault_integration_test.go,
                #  test/e2e/scenario46_vault_integration_e2e_test.go).
                # Scenario 46 validates all 3 auth methods (token, kubernetes, approle),
                # all 4 KV secret paths (admin-password, oidc-secret, monitoring-password, tls),
                # secret rotation watch via hash comparison, and connection retry with
                # exponential backoff (MaxRetries=5, InitialBackoff=1s, MaxBackoff=30s).
                vault:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                      default: false
                    address:
                      type: string
                    authMethod:
                      type: string
                      enum: [token, kubernetes, approle]
                      default: kubernetes
                    authPath:
                      type: string
                    role:
                      type: string
                    secretPath:
                      type: string
                    tlsSecret:
                      type: object
                      properties:
                        name: { type: string }

                # --- Monitoring ---
                monitoring:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                      default: true
                    metricsPort:
                      type: integer
                      default: 9187
                    serviceMonitor:
                      type: boolean
                      default: false

                # --- Telemetry ---
                telemetry:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                      default: false
                    otlpEndpoint:
                      type: string
                    otlpProtocol:
                      type: string
                      enum: [grpc, http]
                      default: grpc
                    samplingRate:
                      type: number
                      default: 1.0

                # --- Deletion Policy ---
                deletionPolicy:
                  type: string
                  enum: [Retain, Delete]
                  default: Retain
                  description: "PV reclaim policy on cluster deletion"
                backupOnDelete:
                  type: boolean
                  default: false

            # --- Status ---
            status:
              type: object
              properties:
                phase:
                  type: string
                  enum: [Pending, Initializing, Running, Updating, Scaling, Failed, Deleting, Stopped, Stopping, Restricted, Maintenance]
                coordinatorReady:
                  type: boolean
                standbyReady:
                  type: boolean
                segmentsReady:
                  type: integer
                segmentsTotal:
                  type: integer
                mirroringStatus:
                  type: string
                  enum: [NotConfigured, Initializing, InSync, Syncing, Degraded, Down]
                clusterVersion:
                  type: string
                lastReconcileTime:
                  type: string
                  format: date-time
                lastConfigChangeTime:
                  type: string
                  format: date-time
                observedGeneration:
                  type: integer
                conditions:
                  type: array
                  items:
                    type: object
                    properties:
                      type: { type: string }
                      status: { type: string }
                      lastTransitionTime: { type: string, format: date-time }
                      reason: { type: string }
                      message: { type: string }
                previousSegmentCount:
                  type: integer
                  description: "Previous segment count before scale operation (for tracking)"
                redistributionProgress:
                  type: integer
                  description: "Data redistribution progress percentage (0-100)"
                failedSegments:
                  type: array
                  items:
                    type: object
                    properties:
                      contentID: { type: integer }
                      hostname: { type: string }
                      role: { type: string }
                      status: { type: string }
```

### 1.2 Status Conditions

| Condition Type | Description |
|---------------|-------------|
| `ClusterReady` | All components are running and healthy |
| `CoordinatorReady` | Coordinator pod is running and accepting connections |
| `StandbyReady` | Standby coordinator is synced and ready |
| `SegmentsReady` | All segment pods are running |
| `MirroringHealthy` | All mirrors are in sync |
| `AuthConfigured` | Authentication is properly configured |
| `ConfigApplied` | All configuration parameters are applied |
| `VaultConnected` | Vault connection is established (if enabled) |
| `DataRedistribution` | Data redistribution status (InProgress, Completed, Failed) |
| `StorageExpanded` | PV expansion status |
| `ScaleOutCompleted` | Scale-out operation completed successfully |
| `ScaleInCompleted` | Scale-in operation completed successfully |

### 1.3 Validation Rules

1. `segments.count` must be >= 1
2. If `segments.mirroring.layout` is `spread`, segment host count must exceed `primariesPerHost`
3. If `auth.oidc.enabled` is true, `issuerURL` and `clientID` are required
4. If `vault.enabled` is true, `address` is required
5. `config.parameters` keys must be valid Cloudberry configuration parameter names
6. `deletionPolicy` defaults to `Retain` for safety
7. Scale-in by more than 50% of current segment count requires confirmation annotation `avsoft.io/confirm-scale-in: "true"`
8. Storage size can only be increased, never decreased (Kubernetes PVC limitation)
9. Storage expansion requires the StorageClass to have `allowVolumeExpansion: true`
10. **Cross-namespace name uniqueness**: CloudberryCluster names must be unique across all namespaces. This is enforced via a validating admission webhook that checks for duplicate `metadata.name` values across namespaces on CREATE operations. The webhook prevents naming collisions that could cause resource conflicts in shared infrastructure (e.g., shared storage, DNS, monitoring labels).

### 1.4 Webhook Validation Rules for Duplicate Detection

The validating webhook enforces cross-namespace uniqueness:

- **On CREATE**: List all CloudberryCluster resources across all namespaces. If any existing cluster has the same `metadata.name`, reject the request with an error message indicating the conflicting namespace.
- **On UPDATE**: No cross-namespace check needed (name is immutable on update).
- **Failure policy**: Configurable via `webhook.failurePolicy` (default: `Fail`). In environments where the webhook may be unavailable, set to `Ignore` to avoid blocking cluster creation.

```go
// Webhook duplicate detection logic
func (v *CloudberryClusterValidator) validateNameUniqueness(ctx context.Context, cluster *v1alpha1.CloudberryCluster) error {
    var clusterList v1alpha1.CloudberryClusterList
    if err := v.client.List(ctx, &clusterList); err != nil {
        return fmt.Errorf("failed to list clusters: %w", err)
    }
    for _, existing := range clusterList.Items {
        if existing.Name == cluster.Name && existing.Namespace != cluster.Namespace {
            return fmt.Errorf("CloudberryCluster %q already exists in namespace %q; names must be unique across namespaces",
                cluster.Name, existing.Namespace)
        }
    }
    return nil
}
```

### 1.5 Mirroring Status Values

| Status | Description |
|--------|-------------|
| `NotConfigured` | Mirroring is not enabled (`spec.segments.mirroring.enabled: false`) |
| `Initializing` | Mirrors are being initialized from primaries (base backup in progress) |
| `Syncing` | Mirrors are catching up via WAL streaming replication |
| `InSync` | All mirrors are fully synchronized with their primaries |
| `Degraded` | One or more mirrors are out of sync or initialization timed out |
| `Down` | Mirroring is completely down |

The `Initializing` status is set when mirroring is first enabled on an existing cluster. It indicates the base-backup phase before WAL streaming begins.

### 1.6 Event Reasons

#### 1.6.1 Mirroring Operations

| Event Reason | Event Type | Description |
|-------------|------------|-------------|
| `MirroringEnabled` | Normal | Mirroring enable initiated with the specified layout |
| `MirroringDisabled` | Warning/Normal | Mirroring disable initiated (warning) or completed (normal) |
| `MirroringInitializing` | Normal | Mirror StatefulSet created, base backup in progress |
| `MirroringInSync` | Normal | All mirrors synchronized after enable operation |
| `MirroringFailed` | Warning | Mirroring operation failed (validation error, timeout, or runtime error) |

These events are emitted by the cluster controller during mirroring enable/disable operations and provide visibility into the operation progress.

#### 1.6.2 Segment Failover

| Event Reason | Event Type | Description |
|-------------|------------|-------------|
| `SegmentFailover` | Warning | A primary segment has failed and automatic failover has been triggered. Emitted per failed primary segment with details including contentID, original hostname, and new primary hostname (if mirror promotion was verified) or "mirror promotion pending" status. |
| `SegmentFailoverCompleted` | Normal | A segment failover has completed and been verified. The mirror has been successfully promoted to primary role. |

These events are emitted by the HA controller during automatic failover via `handleFailover()`. The failover is triggered when the FTS probe detects primary segments with `status != "u"` while mirroring is enabled. The controller calls `gp_request_fts_probe_scan()` to trigger Cloudberry's internal FTS daemon for mirror promotion, then verifies the result.

### 1.7 Annotations

| Annotation | Description | Managed By |
|-----------|-------------|------------|
| `avsoft.io/mirroring-state` | JSON-encoded state of an in-progress mirroring enable operation. Contains `phase`, `startedAt`, `phaseStartedAt`, and `layout` fields. Automatically removed on completion or timeout. | Controller |
| `avsoft.io/failover-state` | Tracks in-progress automatic failover state. Set by the HA controller when segment failover is detected and `handleFailover()` is invoked. Used to coordinate failover lifecycle and prevent duplicate failover actions. | Controller |
| `avsoft.io/recovery` | Recovery type to trigger (`incremental`, `full`, `differential`) | User |
| `avsoft.io/action` | Cluster action to trigger (`rebalance`, `activate-standby`) | User |
| `avsoft.io/confirm-scale-in` | Confirmation for large scale-in operations (`"true"`) | User |

**Mirroring State Annotation Schema**:

```json
{
  "phase": "creating-sts",
  "startedAt": "2026-05-15T10:00:00Z",
  "phaseStartedAt": "2026-05-15T10:00:00Z",
  "layout": "group"
}
```

Phase values: `creating-sts` → `initializing` → `syncing` → `completed`.

### 1.8 Day-2 Operations: Mirroring Enable/Disable

Mirroring can be enabled or disabled on an existing cluster as a day-2 operation by patching `spec.segments.mirroring.enabled`.

**Enable Mirroring**:
- Requires cluster in `Running` phase
- Requires sufficient segment count for the chosen layout
- Transitions: `status.phase` → `Updating`, `status.mirroringStatus` → `Initializing` → `Syncing` → `InSync`
- 30-minute timeout; on timeout `mirroringStatus` → `Degraded`

**Disable Mirroring**:
- Requires cluster in `Running` phase
- Deletes mirror StatefulSet and optionally cleans up PVCs (based on `deletionPolicy`)
- Transitions: `status.mirroringStatus` → `NotConfigured`
- Cluster phase remains `Running` throughout

See specification 04 (HA & Recovery), sections 2.4 and 2.5 for full implementation details.

### 1.9 Webhook Validation for Mirroring

The validating webhook enforces the following rules for mirroring transitions on UPDATE:

1. **Enabling mirroring**: Cluster must be in `Running` phase. Segment count must satisfy layout requirements (group: `count >= 2 * primariesPerHost`, spread: `count > primariesPerHost`).
2. **Disabling mirroring**: Allowed from any Running state without additional restrictions.
3. **Layout change while enabled**: Rejected. You must disable mirroring before changing the layout.
4. **Spread layout warning**: If enabling spread mirroring with marginal segment count (`count <= primariesPerHost + 1`), the webhook admits the request but returns a warning about limited fault tolerance.

## 2. Sample Manifests

### 2.1 Minimal Cluster

```yaml
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: minimal-cluster
  namespace: cloudberry-test
spec:
  coordinator:
    storage:
      size: 5Gi
  segments:
    count: 2
    storage:
      size: 10Gi
```

### 2.2 Production Cluster with HA and OIDC

```yaml
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: production-cluster
  namespace: cloudberry-test
spec:
  version: "7.7"
  image: cloudberrydb/cloudberry:7.7
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
      issuerURL: http://keycloak:8090/realms/cloudberry
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
      pkce: true
      allowLocalSignIn: true
    hbaRules:
      - type: local
        database: all
        user: gpadmin
        method: trust
      - type: host
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
      log_connections: "on"
      log_disconnections: "on"
    coordinatorParameters:
      optimizer: "on"
  ha:
    ftsProbeInterval: 30
    ftsProbeTimeout: 10
    ftsProbeRetries: 3
    checksums: true
  vault:
    enabled: true
    address: http://vault:8200
    authMethod: kubernetes
    role: cloudberry-operator
    secretPath: secret/data/cloudberry
  monitoring:
    enabled: true
    metricsPort: 9187
    serviceMonitor: true
  telemetry:
    enabled: true
    otlpEndpoint: tempo:4317
    otlpProtocol: grpc
  deletionPolicy: Retain
  backupOnDelete: true
```

### 2.3 Scale-Out Example

```yaml
# Scale from 4 to 8 segments with rebalance configuration
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: production-cluster
  namespace: cloudberry-test
spec:
  segments:
    count: 8          # increased from 4
    mirroring:
      enabled: true
      layout: group
    storage:
      storageClass: fast-ssd
      size: 200Gi
    rebalance:
      skewThreshold: 10
      parallelism: 4
      excludeTables:
        - "audit_log"
        - "temp_*"
```

### 2.4 Storage Expansion Example

```yaml
# Expand coordinator storage from 50Gi to 100Gi
# and segment storage from 200Gi to 500Gi
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: production-cluster
  namespace: cloudberry-test
spec:
  coordinator:
    storage:
      storageClass: fast-ssd
      size: 100Gi       # increased from 50Gi
  standby:
    enabled: true
    storage:
      storageClass: fast-ssd
      size: 100Gi       # increased from 50Gi
  segments:
    count: 8
    storage:
      storageClass: fast-ssd
      size: 500Gi       # increased from 200Gi — applies to ALL primary and mirror PVCs
```

### 2.5 Enable Mirroring on Existing Cluster

```yaml
# Enable group mirroring on a running cluster without mirrors
# Patch: kubectl patch cloudberrycluster my-cluster --type=merge -p '...'
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: my-cluster
  namespace: cloudberry-test
spec:
  segments:
    count: 8
    primariesPerHost: 2
    mirroring:
      enabled: true        # changed from false to true
      layout: group
    storage:
      storageClass: fast-ssd
      size: 200Gi
```

**kubectl patch example**:

```bash
# Enable mirroring with group layout
kubectl patch cloudberrycluster my-cluster -n cloudberry-test \
  --type=merge \
  -p '{"spec":{"segments":{"mirroring":{"enabled":true,"layout":"group"}}}}'

# Monitor progress
kubectl get cloudberrycluster my-cluster -n cloudberry-test -w
```

**Expected status progression**:

```
NAME         PHASE      MIRRORING
my-cluster   Updating   Initializing
my-cluster   Updating   Syncing
my-cluster   Running    InSync
```

### 2.6 Disable Mirroring on Existing Cluster

```bash
# Disable mirroring
kubectl patch cloudberrycluster my-cluster -n cloudberry-test \
  --type=merge \
  -p '{"spec":{"segments":{"mirroring":{"enabled":false}}}}'
```

**Expected status progression**:

```
NAME         PHASE      MIRRORING
my-cluster   Running    NotConfigured
```

**Note**: If `spec.deletionPolicy` is `Delete`, mirror PVCs are cleaned up automatically. If `Retain` (default), PVCs are preserved for potential re-enable.

## 3. Printer Columns

```yaml
additionalPrinterColumns:
  - name: Phase
    type: string
    jsonPath: .status.phase
  - name: Version
    type: string
    jsonPath: .spec.version
  - name: Segments
    type: string
    jsonPath: .status.segmentsReady
  - name: Mirroring
    type: string
    jsonPath: .status.mirroringStatus
  - name: Age
    type: date
    jsonPath: .metadata.creationTimestamp
```
