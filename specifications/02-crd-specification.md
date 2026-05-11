# Cloudberry Operator - CRD Specification

**Version**: 1.0.0
**API Group**: cloudberry.example.com
**API Version**: v1alpha1

---

## 1. CloudberryCluster CRD

### 1.1 Full Schema

```yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: cloudberryclusters.cloudberry.example.com
spec:
  group: cloudberry.example.com
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
                      description: "FTS probe timeout in seconds"
                    ftsProbeRetries:
                      type: integer
                      default: 5
                      description: "FTS probe retry count"
                    checksums:
                      type: boolean
                      default: true
                      description: "Enable storage-layer checksums"

                # --- Vault Integration ---
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
                  enum: [Pending, Initializing, Running, Updating, Scaling, Failed, Deleting]
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
                  enum: [NotConfigured, InSync, Syncing, Degraded, Down]
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

### 1.3 Validation Rules

1. `segments.count` must be >= 1
2. If `segments.mirroring.layout` is `spread`, segment host count must exceed `primariesPerHost`
3. If `auth.oidc.enabled` is true, `issuerURL` and `clientID` are required
4. If `vault.enabled` is true, `address` is required
5. `config.parameters` keys must be valid Cloudberry configuration parameter names
6. `deletionPolicy` defaults to `Retain` for safety

## 2. Sample Manifests

### 2.1 Minimal Cluster

```yaml
apiVersion: cloudberry.example.com/v1alpha1
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
apiVersion: cloudberry.example.com/v1alpha1
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
