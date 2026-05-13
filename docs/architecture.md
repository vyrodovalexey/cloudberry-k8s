# Architecture

This document describes the system architecture of the Cloudberry Kubernetes Operator, including component design, controller reconciliation flows, authentication architecture, and high availability design.

## Table of Contents

- [System Overview](#system-overview)
- [Component Overview](#component-overview)
- [CRD Design](#crd-design)
- [Controller Reconciliation Flow](#controller-reconciliation-flow)
- [Authentication Architecture](#authentication-architecture)
- [High Availability Design](#high-availability-design)
- [Observability Architecture](#observability-architecture)
- [REST API Server Architecture](#rest-api-server-architecture)
  - [Rate Limiter](#rate-limiter)
  - [Trusted Proxies](#trusted-proxies)
- [DBClientFactory Pattern](#dbclientfactory-pattern)
- [Status Update Pattern](#status-update-pattern)
- [Webhook Certificate Manager](#webhook-certificate-manager)
- [Design Principles](#design-principles)

## System Overview

The Cloudberry Operator is a Kubernetes operator built with [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) that manages the full lifecycle of Cloudberry Database clusters. It uses the standard Kubernetes reconciliation pattern to converge actual cluster state toward the desired state declared in `CloudberryCluster` custom resources.

The operator runs two server components:
1. **Controller Manager** — Watches `CloudberryCluster` resources and reconciles desired state
2. **REST API Server** — Provides programmatic access for `cloudberry-ctl` and external integrations

```
┌─────────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                         │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                 cloudberry-operator                        │  │
│  │                                                           │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐  │  │
│  │  │   Cluster    │ │     HA       │ │   Auth / Admin   │  │  │
│  │  │  Controller  │ │  Controller  │ │   Controllers    │  │  │
│  │  └──────┬───────┘ └──────┬───────┘ └────────┬─────────┘  │  │
│  │         └────────────────┼──────────────────┘             │  │
│  │                          │                                │  │
│  │  ┌───────────────────────┴─────────────────────────────┐  │  │
│  │  │           Reconciliation Engine                      │  │  │
│  │  │         (controller-runtime / kubebuilder)           │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  │                                                           │  │
│  │  ┌──────────────────────────────────────────────────────┐ │  │
│  │  │              REST API Server (:8090)                  │ │  │
│  │  │  ┌──────────┐  ┌──────────────┐  ┌───────────────┐  │ │  │
│  │  │  │  Rate    │  │     Auth     │  │   Handlers    │  │ │  │
│  │  │  │ Limiter  │──│  Middleware  │──│  (CRUD, ops)  │  │ │  │
│  │  │  └──────────┘  └──────────────┘  └───────────────┘  │ │  │
│  │  └──────────────────────────────────────────────────────┘ │  │
│  │                                                           │  │
│  │  ┌──────────┐  ┌───────────┐  ┌────────────────────────┐ │  │
│  │  │ Metrics  │  │ Telemetry │  │   Auth Middleware      │ │  │
│  │  │ (Prom)   │  │  (OTLP)   │  │  (Basic + OIDC/JWT)   │ │  │
│  │  └──────────┘  └───────────┘  └────────────────────────┘ │  │
│  │                                                           │  │
│  │  ┌──────────────────────────────────────────────────────┐ │  │
│  │  │  DB Client Factory  │  Webhooks (conditional)       │ │  │
│  │  └──────────────────────────────────────────────────────┘ │  │
│  │                                                           │  │
│  │  ┌──────────────────────────────────────────────────────┐ │  │
│  │  │  Cert Manager (Vault PKI / Self-Signed)             │ │  │
│  │  └──────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                  Cloudberry Cluster                        │  │
│  │  ┌──────────────┐  ┌──────────────┐                       │  │
│  │  │ Coordinator  │  │   Standby    │                       │  │
│  │  │ StatefulSet  │  │ StatefulSet  │  (conditionally       │  │
│  │  └──────────────┘  └──────────────┘   created)            │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │            Segment StatefulSets                      │  │  │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │  │  │
│  │  │  │Primary 0 │  │Primary 1 │  │Primary N │          │  │  │
│  │  │  │Mirror  0 │  │Mirror  1 │  │Mirror  N │          │  │  │
│  │  │  └──────────┘  └──────────┘  └──────────┘          │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │    Vault     │  │   Keycloak   │  │   Observability    │    │
│  │  (optional)  │  │  (OIDC IdP)  │  │      Stack         │    │
│  └──────────────┘  └──────────────┘  └────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Component Overview

### Operator Components

| Component | Package | Responsibility |
|-----------|---------|----------------|
| **Cluster Controller** | `internal/controller` | Full cluster lifecycle: create, update, scale, delete StatefulSets, Services, ConfigMaps |
| **HA Controller** | `internal/controller` | FTS probing, automatic failover, mirroring status, standby management, recovery orchestration |
| **Auth Controller** | `internal/controller` | `pg_hba.conf` management, OIDC configuration, TLS secrets, password rotation |
| **Admin Controller** | `internal/controller` | Configuration parameters, maintenance operations (vacuum, analyze, reindex) |
| **API Server** | `internal/api` | REST API for programmatic cluster management, with per-IP rate limiting and body size limits |
| **Rate Limiter** | `internal/api` | Per-IP token bucket rate limiter protecting API endpoints from abuse |
| **Auth Middleware** | `internal/auth` | Basic (bcrypt) and OIDC/JWT authentication, permission enforcement |
| **Resource Builder** | `internal/builder` | Pure functions that construct Kubernetes resources from CRD spec |
| **DB Client Factory** | `internal/db` | Creates database clients from cluster connection information, resolving service endpoints and credentials from Kubernetes Secrets |
| **DB Client** | `internal/db` | Cloudberry/PostgreSQL database operations via pgx with real SQL queries |
| **CLI Client** | `internal/ctl` | HTTP client for `cloudberry-ctl` to communicate with the operator REST API |
| **Vault Client** | `internal/vault` | HashiCorp Vault integration for secrets management |
| **Metrics** | `internal/metrics` | Prometheus metrics registration and recording |
| **Telemetry** | `internal/telemetry` | OpenTelemetry tracing setup and span helpers |
| **Webhooks** | `internal/webhook` | Validating and mutating admission webhooks (including cross-namespace duplicate detection) |
| **Cert Manager** | `internal/certmanager` | Webhook TLS certificate lifecycle: issuance, storage, and rotation via Vault PKI or self-signed CA |

### Managed Kubernetes Resources

For each `CloudberryCluster`, the operator creates and manages:

| Resource | Name Pattern | Purpose |
|----------|-------------|---------|
| StatefulSet | `{cluster}-coordinator` | Coordinator node |
| StatefulSet | `{cluster}-coordinator-standby` | Standby coordinator (created only when `standby.enabled: true`) |
| StatefulSet | `{cluster}-segment-primary` | Primary segment nodes |
| StatefulSet | `{cluster}-segment-mirror` | Mirror segment nodes (if mirroring enabled) |
| Service | `{cluster}-coordinator` | Headless service for coordinator |
| Service | `{cluster}-coordinator-standby` | Headless service for standby (created only when standby is enabled) |
| Service | `{cluster}-segments` | Headless service for segments |
| Service | `{cluster}-client` | ClusterIP service for client access |
| ConfigMap | `{cluster}-postgresql-conf` | PostgreSQL configuration |
| ConfigMap | `{cluster}-pg-hba-conf` | Host-based authentication rules |
| Secret | `{cluster}-admin-password` | Admin credentials (auto-created by operator if not present) |
| Job | `{cluster}-recovery-{timestamp}` | Recovery operations |
| Job | `{cluster}-maintenance-{timestamp}` | Maintenance operations |
| NetworkPolicy | `{cluster}-network-policy` | Network access rules |

### Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Go | 1.26+ |
| Operator Framework | controller-runtime | v0.24+ |
| CLI Framework | cobra + viper | latest |
| OIDC | go-oidc/v3 + oauth2 | latest |
| Database Driver | pgx/v5 | latest |
| Vault Client | vault/api | latest |
| Metrics | prometheus/client_golang | latest |
| Tracing | opentelemetry-go | latest |
| Testing | testify | latest |

## CRD Design

### CloudberryCluster Resource

The `CloudberryCluster` CRD (`avsoft.io/v1alpha1`) is the primary API surface. It follows the Kubernetes convention of separating desired state (`.spec`) from observed state (`.status`).

```
CloudberryCluster
├── spec
│   ├── version              # Cloudberry DB version (default: "7.7")
│   ├── image                # Container image
│   ├── coordinator          # Coordinator node config
│   │   ├── resources        # CPU/memory requests and limits
│   │   ├── storage          # PVC size and storage class
│   │   └── port             # Listening port (default: 5432)
│   ├── standby              # Standby coordinator config
│   │   ├── enabled          # Enable/disable standby
│   │   ├── resources        # CPU/memory
│   │   └── storage          # PVC config
│   ├── segments             # Segment nodes config
│   │   ├── count            # Number of primary segments
│   │   ├── primariesPerHost # Segments per host (default: 2)
│   │   ├── mirroring        # Mirror config (enabled, layout)
│   │   ├── resources        # CPU/memory
│   │   ├── storage          # PVC config
│   │   └── antiAffinity     # preferred or required
│   ├── auth                 # Authentication config
│   │   ├── basic            # Basic auth (admin user, password secret)
│   │   ├── oidc             # OIDC config (issuer, client, role mapping)
│   │   ├── hbaRules         # pg_hba.conf rules
│   │   └── ssl              # TLS config
│   ├── config               # Database parameters
│   │   ├── parameters       # Cluster-wide params
│   │   ├── coordinatorParameters
│   │   ├── databaseParameters
│   │   └── roleParameters
│   ├── ha                   # HA config (FTS probe settings)
│   ├── vault                # Vault integration
│   ├── monitoring           # Prometheus metrics config
│   ├── telemetry            # OTLP tracing config
│   └── deletionPolicy       # Retain or Delete PVCs
└── status
    ├── phase                # Pending/Initializing/Running/Failed/Deleting
    ├── coordinatorReady     # Coordinator health
    ├── standbyReady         # Standby health
    ├── segmentsReady        # Ready segment count
    ├── segmentsTotal        # Total segment count
    ├── mirroringStatus      # NotConfigured/InSync/Degraded/Down
    ├── conditions           # Standard Kubernetes conditions
    └── failedSegments       # List of failed segments
```

### Status Conditions

| Condition | Description |
|-----------|-------------|
| `ClusterReady` | All components are running and healthy |
| `CoordinatorReady` | Coordinator pod is running and accepting connections |
| `StandbyReady` | Standby coordinator is synced and ready |
| `SegmentsReady` | All segment pods are running |
| `MirroringHealthy` | All mirrors are in sync |
| `AuthConfigured` | Authentication is properly configured |
| `ConfigApplied` | All configuration parameters are applied |
| `VaultConnected` | Vault connection is established (if enabled) |

### Printer Columns

When you run `kubectl get cloudberryclusters`, the output includes:

```
NAME              PHASE     VERSION   SEGMENTS   MIRRORING   AGE
my-cluster        Running   7.7       4          InSync      2h
```

## Controller Reconciliation Flow

### Cluster Controller

The Cluster Controller is the primary reconciler. It manages the full lifecycle of a Cloudberry cluster.

```
                    ┌──────────────────┐
                    │  Watch Event     │
                    │  (CR change)     │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Fetch CR        │
                    │  (Get cluster)   │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
              ┌─────┤  Deleted?        ├─────┐
              │ Yes └──────────────────┘ No  │
              │                              │
     ┌────────▼─────────┐          ┌────────▼─────────┐
     │  Handle Deletion  │          │  Ensure Finalizer │
     │  - Backup if set  │          └────────┬─────────┘
     │  - Delete PVCs    │                   │
     │  - Remove finalizer│         ┌────────▼─────────┐
     └──────────────────┘          │  Action Annotation?│
                                   └───┬───────────┬───┘
                                  Yes  │           │ No
                              ┌────────▼───┐  ┌───▼────────────┐
                              │Handle Action│  │ Reconcile      │
                              │start/stop/  │  │ - ConfigMaps   │
                              │restart      │  │ - Services     │
                              └────────────┘  │ - Coordinator  │
                                              │ - Standby      │
                                              │ - Segments     │
                                              │ - Update Status│
                                              └───┬────────────┘
                                                  │
                                         ┌────────▼─────────┐
                                         │  Requeue (30s)    │
                                         └──────────────────┘
```

**Key behaviors:**
- Uses a **finalizer** (`avsoft.io/finalizer`) to ensure cleanup before deletion
- Supports **annotation-based actions** for start, stop, restart operations
- Implements **create-or-update** pattern for idempotent resource management
- **Requeues** every 30 seconds for periodic health checks (10 seconds on error)
- Emits **Kubernetes events** for state transitions
- Records **Prometheus metrics** for reconciliation duration and results
- Uses **`Status().Patch()` with MergePatch** for all status updates (see [Status Update Pattern](#status-update-pattern) below)

### HA Controller

The HA Controller manages fault tolerance and recovery:

```
┌─────────────────────────────────────────────────┐
│                 HA Controller                     │
│                                                   │
│  ┌─────────────────────────────────────────────┐ │
│  │  FTS Probe Loop (every ftsProbeInterval)    │ │
│  │                                             │ │
│  │  For each primary segment:                  │ │
│  │    1. TCP connection check                  │ │
│  │    2. SQL ping (SELECT 1)                   │ │
│  │    3. If fails after retries → failover     │ │
│  └─────────────────────────────────────────────┘ │
│                                                   │
│  ┌─────────────────────────────────────────────┐ │
│  │  Failover Handler                           │ │
│  │                                             │ │
│  │  1. Mark primary as Down                    │ │
│  │  2. Promote mirror to primary               │ │
│  │  3. Update segment configuration            │ │
│  │  4. Emit SegmentFailover event              │ │
│  │  5. Update metrics and CR status            │ │
│  └─────────────────────────────────────────────┘ │
│                                                   │
│  ┌─────────────────────────────────────────────┐ │
│  │  Recovery Orchestrator                      │ │
│  │                                             │ │
│  │  Handles recovery annotations:              │ │
│  │  - incremental: WAL-based recovery          │ │
│  │  - full: pg_basebackup from mirror          │ │
│  │  - differential: rsync-based recovery       │ │
│  │  Creates Kubernetes Jobs for execution      │ │
│  │  Uses MergePatch for annotation removal     │ │
│  └─────────────────────────────────────────────┘ │
│                                                   │
│  ┌─────────────────────────────────────────────┐ │
│  │  Standby Manager                            │ │
│  │                                             │ │
│  │  - Monitor standby health                   │ │
│  │  - Track replication lag                    │ │
│  │  - Handle activate-standby annotation       │ │
│  │  - Handle reinitialize-standby              │ │
│  └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

### Auth Controller

Manages authentication configuration:

1. Renders `pg_hba.conf` rules from CRD spec into a ConfigMap
2. Generates default HBA rules when none are specified
3. Validates OIDC settings (issuer URL reachable, client ID valid)
4. Manages TLS certificate mounting and PostgreSQL SSL parameters
5. Syncs admin password from Kubernetes Secret or Vault to the database
6. Checks `ObservedGeneration` to skip reconciliation when only status has changed, reducing unnecessary work
7. Emits Kubernetes events for auth configuration changes:
   - `AuthReconciled` (Normal) — authentication configuration reconciled successfully
   - `OIDCValidationFailed` (Warning) — OIDC validation failed with details
   - `OIDCConfigured` (Normal) — OIDC authentication is properly configured

### Admin Controller

Manages configuration and maintenance:

1. Detects parameter changes via hash comparison
2. Determines if parameters require restart or can be reloaded
3. Applies parameters at cluster, coordinator, database, and role levels
4. Creates Kubernetes Jobs for maintenance operations (vacuum, analyze, reindex)
5. Monitors Job completion and cleans up finished Jobs
6. Performs a single consolidated status update per reconciliation cycle to reduce API server load
7. Uses `MergePatch` for annotation removal to avoid race conditions with concurrent updates

## Authentication Architecture

The operator API supports dual-mode authentication:

```
┌──────────────────────────────────────────────────────┐
│                  Incoming Request                      │
│                                                       │
│  Authorization: Basic base64(user:pass)               │
│  -- OR --                                             │
│  Authorization: Bearer <JWT token>                    │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│               Auth Middleware Chain                    │
│                                                       │
│  1. Extract Authorization header                      │
│  2. Detect auth type (Basic vs Bearer)                │
│  3. Route to appropriate provider                     │
│                                                       │
│  ┌────────────────┐    ┌───────────────────────────┐ │
│  │  Basic Auth    │    │  OIDC/JWT Auth            │ │
│  │  Provider      │    │  Provider                 │ │
│  │                │    │                           │ │
│  │  - Validate    │    │  - Verify JWT signature   │ │
│  │    credentials │    │  - Check issuer/audience  │ │
│  │  - Check admin │    │  - Check expiry           │ │
│  │    secret      │    │  - Extract role claims    │ │
│  │  - Check DB    │    │  - Map roles → perms      │ │
│  │    roles       │    │                           │ │
│  └───────┬────────┘    └─────────────┬─────────────┘ │
│          └──────────┬────────────────┘                │
│                     ▼                                 │
│  ┌──────────────────────────────────────────────────┐│
│  │          Permission Resolver                      ││
│  │                                                   ││
│  │  Determine effective permission level:             ││
│  │  Self Only → Basic → Operator Basic →             ││
│  │  Operator → Admin                                 ││
│  └──────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────┘
```

### Permission Levels

| Level | Description | Example Capabilities |
|-------|-------------|---------------------|
| **Self Only** | View own queries and sessions | Cancel own queries |
| **Basic** | View cluster state | Read cluster status, list databases |
| **Operator Basic** | Basic operations | View all sessions, view configurations |
| **Operator** | Cluster operations | Start/stop, config changes, maintenance |
| **Admin** | Full access | User management, security config, delete cluster |

### OIDC Flow

1. Operator discovers OIDC configuration from `{issuerURL}/.well-known/openid-configuration`
2. Caches JWKS (JSON Web Key Set) and refreshes every 5 minutes
3. On each request, validates JWT signature, issuer, audience, and expiry
4. Extracts role claims from configurable JSON path (e.g., `realm_access.roles`)
5. Maps IdP roles to permission levels using the `roleMapping` configuration
6. Supports exact, suffix, prefix, and contains role matching modes

## High Availability Design

### Segment Mirroring

The operator supports two mirroring layouts:

**Group Mirroring**: All mirrors for one host's primary segments are placed on one other host.
```
Host 0: Primary 0, Primary 1  →  Mirrors on Host 1
Host 1: Primary 2, Primary 3  →  Mirrors on Host 2
Host 2: Primary 4, Primary 5  →  Mirrors on Host 0
```

**Spread Mirroring**: Each host's mirrors are distributed across multiple remaining hosts.
```
Host 0: Primary 0, Primary 1  →  Mirror 0 on Host 1, Mirror 1 on Host 2
Host 1: Primary 2, Primary 3  →  Mirror 2 on Host 2, Mirror 3 on Host 0
Host 2: Primary 4, Primary 5  →  Mirror 4 on Host 0, Mirror 5 on Host 1
```

Spread mirroring provides better fault tolerance but requires more hosts than `primariesPerHost`.

### Fault Tolerance Service (FTS)

```
┌─────────────────────────────────────────────┐
│              FTS Probe Loop                  │
│                                             │
│  Every ftsProbeInterval seconds:            │
│                                             │
│  ┌─────────────────────────────────────┐    │
│  │  For each primary segment:          │    │
│  │                                     │    │
│  │  ┌──────────┐    ┌──────────────┐   │    │
│  │  │ TCP Check│───▶│ SQL Ping     │   │    │
│  │  └──────────┘    └──────┬───────┘   │    │
│  │                         │           │    │
│  │              ┌──────────▼────────┐  │    │
│  │              │  Success?         │  │    │
│  │              └───┬──────────┬────┘  │    │
│  │             Yes  │          │ No    │    │
│  │                  │   ┌──────▼────┐  │    │
│  │                  │   │ Retry     │  │    │
│  │                  │   │ (N times) │  │    │
│  │                  │   └──────┬────┘  │    │
│  │                  │          │ Fail  │    │
│  │                  │   ┌──────▼────┐  │    │
│  │                  │   │ FAILOVER  │  │    │
│  │                  │   │ Promote   │  │    │
│  │                  │   │ mirror    │  │    │
│  │                  │   └───────────┘  │    │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
```

### Coordinator Standby

The standby coordinator maintains a hot copy of the coordinator via WAL streaming replication:

- **Deployment**: Separate StatefulSet with its own PVC
- **Replication**: Continuous WAL streaming from coordinator
- **Activation**: Manual only (requires explicit administrator action via annotation or CLI)
- **Monitoring**: Replication lag tracked via Prometheus metrics

Standby activation is intentionally **not automatic** to prevent split-brain scenarios.

### Recovery Operations

| Type | Method | Use Case |
|------|--------|----------|
| **Incremental** | WAL replay | Brief downtime, data intact |
| **Full** | pg_basebackup from mirror | Data corruption |
| **Differential** | rsync-based file sync | Large segments, minimize transfer |

Recovery operations run as Kubernetes Jobs with configurable backoff and timeout.

## Observability Architecture

### Metrics (Prometheus)

The operator exposes metrics at the `/metrics` endpoint:

- **Cluster metrics**: `cloudberry_cluster_info`, `cloudberry_coordinator_up`, `cloudberry_standby_up`
- **Segment metrics**: `cloudberry_segments_ready`, `cloudberry_segments_total`, `cloudberry_segments_failed`
- **Mirroring metrics**: `cloudberry_mirroring_in_sync`
- **Reconciliation metrics**: `cloudberry_reconcile_total`, `cloudberry_reconcile_errors_total`, `cloudberry_reconcile_duration_seconds`
- **FTS metrics**: `cloudberry_fts_probe_total`, `cloudberry_fts_failover_total`, `cloudberry_replication_lag_bytes`
- **Connection metrics**: `cloudberry_connections_active`, `cloudberry_connections_max`

### Tracing (OpenTelemetry)

When telemetry is enabled, the operator emits OTLP traces for:

- Reconciliation loops (one span per reconciliation)
- API request handling
- Database operations
- Vault operations

Supports both gRPC and HTTP OTLP exporters with configurable sampling rate.

### Structured Logging

All logs use Go's `slog` package with standard fields:

```json
{
  "level": "INFO",
  "msg": "reconciliation completed",
  "cluster": "my-cluster",
  "namespace": "cloudberry-test",
  "controller": "cluster-controller",
  "reconcileID": "abc-123",
  "duration": "1.234s"
}
```

## REST API Server Architecture

The operator embeds a REST API server that starts alongside the controller manager. The API server provides programmatic access for `cloudberry-ctl` and external integrations.

### Request Pipeline

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Client  │───▶│  Rate Limiter│───▶│  Auth        │───▶│  Permission  │
│  Request │    │  (per-IP     │    │  Middleware   │    │  Check       │
│          │    │   token      │    │  (Basic/JWT) │    │              │
│          │    │   bucket)    │    │              │    │              │
└──────────┘    └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
                       │ 429               │ 401               │ 403
                       │ if exceeded       │ if invalid        │ if denied
                                                               │
                                                      ┌────────▼───────┐
                                                      │   Handler      │
                                                      │  (validates    │
                                                      │   input, body  │
                                                      │   size limit,  │
                                                      │   DNS-1123     │
                                                      │   names)       │
                                                      └────────────────┘
```

### Rate Limiter

The API uses a per-IP token bucket rate limiter (`internal/api/ratelimit.go`):

- **Algorithm**: Token bucket with automatic refill based on elapsed time
- **Default limit**: 10 requests per minute per IP
- **IP extraction**: Uses `RemoteAddr` by default. `X-Forwarded-For` and `X-Real-IP` headers are only trusted when the direct connection comes from a configured trusted proxy (see [Trusted Proxies](#trusted-proxies) below). This prevents header spoofing attacks where untrusted clients forge forwarded headers to bypass rate limiting
- **Cleanup**: Background goroutine removes inactive entries every 5 minutes to prevent memory leaks
- **Graceful shutdown**: The `Stop()` method terminates the background cleanup goroutine, preventing goroutine leaks when the rate limiter is no longer needed. The API server calls `Stop()` during shutdown
- **Response**: Returns `429 Too Many Requests` with a `Retry-After` header when the limit is exceeded

Rate limiting is applied **before** authentication to protect against brute-force credential attacks.

#### Trusted Proxies

By default, the rate limiter uses only `RemoteAddr` for client IP identification. To trust proxy headers (`X-Forwarded-For`, `X-Real-IP`), configure trusted proxy CIDR ranges via the `WithTrustedProxies` option:

```go
rateLimiter := api.NewRateLimiter(10, time.Minute, logger,
    api.WithTrustedProxies([]string{"10.0.0.0/8", "172.16.0.0/12"}),
)
```

When a request arrives from an IP within a trusted proxy range, the rate limiter reads the client IP from `X-Forwarded-For` (first IP in the chain) or `X-Real-IP`. Invalid CIDR strings are logged as warnings and skipped.

### Input Validation

- **Body size limit**: All request bodies are limited to 1 MiB via `http.MaxBytesReader`
- **Name validation**: Cluster and namespace names are validated against DNS-1123 subdomain format
- **Security headers**: All responses include `X-Content-Type-Options`, `X-Frame-Options`, `Strict-Transport-Security`, and other security headers

### API Server Lifecycle

The API server starts in a background goroutine from the operator `main()` function. It listens on the address configured by `APIAddress` (default `:8090`). On context cancellation, the server performs a graceful shutdown with a 5-second timeout using `context.Background()` to ensure the shutdown completes even when the parent context is already canceled. During shutdown, the rate limiter's `Stop()` method is called to terminate the background cleanup goroutine and prevent goroutine leaks.

## DBClientFactory Pattern

The `DBClientFactory` (`internal/db/factory.go`) solves the problem of creating database connections for each managed cluster. Controllers do not create database clients directly; instead, they use the factory.

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   Controller     │────▶│  DBClientFactory  │────▶│   DB Client      │
│  (HA, Admin)     │     │                   │     │   (pgx)          │
└──────────────────┘     └────────┬──────────┘     └──────────────────┘
                                  │
                         ┌────────▼──────────┐
                         │  Resolves:         │
                         │  - Coordinator     │
                         │    service host    │
                         │  - Port from spec  │
                         │  - Admin password  │
                         │    from K8s Secret │
                         │  - Username from   │
                         │    spec or default │
                         └───────────────────┘
```

**Key behaviors:**
- Reads the admin password from the cluster's `{cluster}-admin-password` Secret
- Resolves the coordinator service endpoint as `{cluster}-coordinator.{namespace}.svc`
- Configures retry options with exponential backoff
- Returns a `Client` interface for testability

## Status Update Pattern

All controllers use `Status().Patch()` with `MergePatchType` instead of `Status().Update()` to prevent status clobbering between concurrent controllers. When multiple controllers (Cluster, HA, Auth, Admin) reconcile the same `CloudberryCluster` simultaneously, a full status update from one controller can overwrite fields set by another.

### `patchStatus()`

The standard status patch function serializes the entire `cluster.Status` struct and applies it as a MergePatch:

```go
func patchStatus(ctx context.Context, c client.Client, cluster *CloudberryCluster) error {
    statusPatch, _ := json.Marshal(map[string]interface{}{
        "status": cluster.Status,
    })
    return c.Status().Patch(ctx, cluster, client.RawPatch(types.MergePatchType, statusPatch))
}
```

### `patchFTSStatus()`

The FTS probe results require special handling because the `FailedSegments` field uses `omitempty` in its JSON tag. With `json.Marshal`, an empty slice is omitted entirely, and MergePatch treats omitted fields as "no change" — meaning previously failed segments would never be cleared.

`patchFTSStatus()` manually constructs the patch JSON to always include `failedSegments`, even when empty:

```go
statusMap := map[string]interface{}{
    "mirroringStatus": cluster.Status.MirroringStatus,
}
if len(failedSegments) == 0 {
    statusMap["failedSegments"] = []interface{}{} // explicit empty array
} else {
    statusMap["failedSegments"] = failedSegments
}
```

This ensures that when all segments recover, the `failedSegments` list is properly cleared in the status.

## Webhook Certificate Manager

The `internal/certmanager` package manages TLS certificates for the admission webhook server. It supports two certificate sources with automatic rotation.

```
┌─────────────────────────────────────────────────────────────┐
│                  Cert Manager                                │
│                                                              │
│  ┌────────────────────┐    ┌─────────────────────────────┐  │
│  │  Vault PKI         │    │  Self-Signed (fallback)     │  │
│  │  (preferred)       │    │                             │  │
│  │                    │    │  - Generates ECDSA P-256    │  │
│  │  - Issues certs    │    │    CA + server key pairs    │  │
│  │    via PKI engine  │    │  - CA valid for 10 years    │  │
│  │  - Configurable    │    │  - Server cert validity     │  │
│  │    mount path and  │    │    configurable (default    │  │
│  │    role            │    │    1 year)                   │  │
│  └────────┬───────────┘    └──────────────┬──────────────┘  │
│           └──────────┬───────────────────┘                   │
│                      ▼                                       │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  Kubernetes Secret (TLS type)                         │   │
│  │  - ca.crt   (CA certificate PEM)                      │   │
│  │  - tls.crt  (server certificate PEM)                  │   │
│  │  - tls.key  (server private key PEM)                  │   │
│  └───────────────────────────────────────────────────────┘   │
│                      │                                       │
│                      ▼                                       │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  CA Bundle Injection                                  │   │
│  │  → ValidatingWebhookConfiguration.caBundle            │   │
│  │  → MutatingWebhookConfiguration.caBundle              │   │
│  └───────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Certificate Lifecycle

1. **Startup**: `EnsureCertificates()` checks if a valid certificate Secret exists
2. **Issuance**: If no Secret exists or the certificate is invalid, new certificates are generated using the configured source
3. **Storage**: Certificates are stored in a Kubernetes Secret of type `kubernetes.io/tls`
4. **Rotation check**: `NeedsRotation()` is called periodically (every 12 hours). Rotation triggers when **2/3 of the certificate lifetime** has elapsed
5. **CA bundle injection**: The returned CA bundle (PEM) is injected into the `ValidatingWebhookConfiguration` and `MutatingWebhookConfiguration` resources

### DNS SANs

Server certificates include the following Subject Alternative Names:

- `{serviceName}.{namespace}.svc`
- `{serviceName}.{namespace}.svc.cluster.local`

### Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `CertSource` | Certificate source (`vault-pki` or `self-signed`) | `self-signed` |
| `ServiceName` | Webhook service name for DNS SANs | — |
| `SecretName` | Kubernetes Secret name for certificate storage | — |
| `VaultPKIMountPath` | Vault PKI engine mount path | `pki` |
| `VaultPKIRole` | Vault PKI role name | `cloudberry-operator` |
| `CertValidityDuration` | Certificate validity period | `365d` |

## Design Principles

1. **Declarative over Imperative** — All state is expressed in CRDs; the operator converges toward the declared state
2. **Idempotent Reconciliation** — The same input always produces the same output; reconciliation is safe to retry
3. **Graceful Degradation** — The operator continues functioning if optional services (Vault, Keycloak) are unavailable
4. **Least Privilege** — Operator RBAC is scoped to the minimum required permissions
5. **Observable** — Every operation emits metrics, traces, and structured logs
6. **Testable** — All external dependencies are behind interfaces for mocking
7. **Configurable** — Environment variables override flags; sensible defaults for everything
