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
- [Error Handling Patterns](#error-handling-patterns)
  - [Error Type Hierarchy](#error-type-hierarchy)
  - [Retry with Exponential Backoff](#retry-with-exponential-backoff)
  - [Reconciliation Error Flow](#reconciliation-error-flow)
  - [Webhook Validation](#webhook-validation)
  - [Pod Deletion Recovery](#pod-deletion-recovery)
- [REST API Server Architecture](#rest-api-server-architecture)
  - [Rate Limiter](#rate-limiter)
  - [Trusted Proxies](#trusted-proxies)
  - [HTTP Server Timeouts](#http-server-timeouts)
- [DBClientFactory Pattern](#dbclientfactory-pattern)
- [Upgrade Flow](#upgrade-lifecycle)
- [Status Update Pattern](#status-update-pattern)
- [Webhook Certificate Manager](#webhook-certificate-manager)
  - [Vault PKI Certificate Issuance](#vault-pki-certificate-issuance)
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
| **Resource Builder** | `internal/builder` | Pure functions that construct Kubernetes resources from CRD spec. Builder methods return `(*Resource, error)` to surface configuration errors early |
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
    ├── phase                # Pending/Initializing/Running/Stopping/Stopped/Restricted/Maintenance/Failed/Deleting
    ├── coordinatorReady     # Coordinator health
    ├── standbyReady         # Standby health
    ├── segmentsReady        # Ready segment count
    ├── segmentsTotal        # Total segment count
    ├── mirroringStatus      # NotConfigured/InSync/Degraded/Down
    ├── conditions           # Standard Kubernetes conditions
    └── failedSegments       # List of failed segments
```

### Status Phases

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

### Status Conditions

| Condition | Description |
|-----------|-------------|
| `ClusterReady` | All components are running and healthy |
| `CoordinatorReady` | Coordinator pod is running and accepting connections |
| `StandbyReady` | Standby coordinator is synced and ready |
| `SegmentsReady` | All segment pods are running |
| `MirroringHealthy` | All mirrors are in sync |
| `AuthConfigured` | Authentication is properly configured |
| `ConfigApplied` | All configuration parameters are applied. Reason values: `ConfigReloaded`, `RestartRequired`, `ConfigAppliedAfterRestart` |
| `ScaleOutFailed` | Scale-out operation failed. Reason: `SegmentsNotReady` — segments did not become ready within the 10-minute timeout |
| `UpgradeCompleted` | Cluster upgrade completed successfully. Reason: `UpgradeSucceeded` — all phases passed and verification succeeded |
| `UpgradeFailed` | Cluster upgrade failed and was rolled back. Reason: `RolledBack` — a phase timed out after 10 minutes |
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
- Checks **action annotations before generation skip** — annotations don't change the CRD generation, so they must be processed before the `ObservedGeneration` check
- Handles **lifecycle phases** (`Stopped`, `Stopping`, `Restricted`, `Maintenance`) that short-circuit normal reconciliation when no action annotation is pending
- Implements **create-or-update** pattern for idempotent resource management
- **Requeues** every 30 seconds for periodic health checks (10 seconds on error, 5 seconds during stopping)
- Emits **Kubernetes events** for state transitions (Stopping, Stopped, Starting, Started, Restarting, Restarted)
- Records **Prometheus metrics** for reconciliation duration and results via `recordMetricsSnapshot()`
- Uses **`Status().Patch()` with MergePatch** for all status updates (see [Status Update Pattern](#status-update-pattern) below)

**Scale-out lifecycle:**
- **Detection**: `reconcileSegments()` compares `spec.segments.count` against the current primary StatefulSet's `spec.replicas`. If the desired count exceeds the current count **and** `currentCount > 0` (guard against false scale detection during restarts), it delegates to `handleScaleOut()`.
- **Pre-flight check**: `handleScaleOut()` verifies the cluster is in `Running` phase. If not, it emits a `ScaleOutBlocked` warning event and returns without error (retries on next reconcile).
- **`handleScaleOut()`**: Sets the `avsoft.io/scale-started` annotation with the current timestamp, sets phase to `Scaling`, updates primary and mirror StatefulSet replicas, creates a redistribution Job via `BuildMaintenanceJob(cluster, "redistribute", timestamp)`, and sets the `DataRedistribution` condition.
- **`checkScaleProgress()`**: Called on each reconciliation when the cluster is in `Scaling` phase. Uses `allSegmentStatefulSetsReady()` to verify that both primary and mirror StatefulSets have reached the desired replica count. When ready, transitions the cluster to `Running`, emits `ScaleOutCompleted`, records the `cloudberry_scale_operations_total` metric, and removes the `avsoft.io/scale-started` annotation.
- **Timeout detection**: `checkScaleProgress()` reads the `avsoft.io/scale-started` annotation and checks if the elapsed time exceeds `scaleTimeout` (10 minutes). If the timeout is exceeded, it delegates to `handleScaleFailure()`.
- **`handleScaleFailure()`**: Identifies unready segments from both primary and mirror StatefulSets, populates `status.failedSegments` with details (contentID, hostname, role, status), sets the `ScaleOutFailed` condition to `True` with reason `SegmentsNotReady`, emits a `ScaleOutFailed` warning event, and removes the `avsoft.io/scale-started` annotation. The cluster **stays in `Scaling` phase** — no automatic rollback.
- **Events**: `ScaleOutStarted` (when scaling begins), `ScaleOutCompleted` (when all pods are ready), `ScaleOutBlocked` (when cluster is not in Running phase), `ScaleOutFailed` (when timeout is exceeded).
- **Metrics**: `cloudberry_scale_operations_total` (counter), `cloudberry_redistribution_progress` (gauge).

**Scale-in lifecycle:**
- **Detection**: `reconcileSegments()` compares `spec.segments.count` against the current primary StatefulSet's `spec.replicas`. If the desired count is less than the current count **and** `currentCount > 0` (guard against false scale detection during restarts), it delegates to `handleScaleIn()`.
- **Pre-flight check**: `handleScaleIn()` verifies the cluster is in `Running` phase. If not, it emits a `ScaleInBlocked` warning event and returns without error (retries on next reconcile).
- **Safety check**: If the new count is less than 50% of the current count, `handleScaleIn()` requires the `avsoft.io/confirm-scale-in=true` annotation. Without it, a `ScaleInBlocked` warning event is emitted and the operation is skipped.
- **`handleScaleIn()`**: Sets the `avsoft.io/scale-started` annotation with the current timestamp, sets phase to `Scaling`, creates a redistribution Job to move data off segments being removed, scales down the mirror StatefulSet first (if mirroring is enabled), then scales down the primary StatefulSet, and sets the `DataRedistribution` condition.
- **`checkScaleProgress()`**: Detects whether the completed scaling was a scale-in (by comparing `spec.segments.count < status.segmentsTotal`). For scale-in, it calls `cleanupOrphanedPVCs()` when `deletionPolicy=Delete`, emits `ScaleInCompleted`, records `cloudberry_scale_operations_total{operation="scale-in"}`, and removes the `avsoft.io/scale-started` annotation.
- **`cleanupOrphanedPVCs()`**: Iterates over segment indices starting from the new count and deletes PVCs for both primary and mirror components. PVCs are named `data-{stsName}-{index}`. The function stops when a PVC is not found (no more orphans).
- **Events**: `ScaleInStarted` (when scaling begins), `ScaleInCompleted` (when all pods are ready), `ScaleInBlocked` (when cluster is not in Running phase, or >50% reduction lacks confirmation).
- **Metrics**: `cloudberry_scale_operations_total{operation="scale-in"}` (counter).

```
┌─────────────────────────────────────────────────────────────────┐
│                    Scale-Out Flow                                │
│                                                                  │
│  reconcileSegments()                                             │
│    │                                                             │
│    ├── Get existing primary StatefulSet                          │
│    ├── Compare spec.segments.count vs sts.spec.replicas          │
│    ├── Guard: currentCount > 0 (prevent false scale on restart)  │
│    │                                                             │
│    └── If desired > current → handleScaleOut()                   │
│         │                                                        │
│         ├── Pre-flight: cluster.Status.Phase == Running?          │
│         │   └── No → emit ScaleOutBlocked, return (retry later)  │
│         │                                                        │
│         ├── Set avsoft.io/scale-started annotation (timestamp)   │
│         ├── Set phase = Scaling                                  │
│         ├── Set DataRedistribution condition (ScaleOutStarted)   │
│         ├── Emit ScaleOutStarted event                           │
│         ├── Update primary StatefulSet replicas                  │
│         ├── Update mirror StatefulSet replicas (if mirroring)    │
│         ├── Create redistribution Job                            │
│         └── Set DataRedistribution condition (InProgress)        │
│                                                                  │
│  checkScaleProgress() — called when phase == Scaling             │
│    │                                                             │
│    ├── allSegmentStatefulSetsReady()?                             │
│    │   ├── Yes → transition to Running                           │
│    │   │         ├── Set phase = Running                         │
│    │   │         ├── Update segmentsReady/segmentsTotal          │
│    │   │         ├── Set DataRedistribution (Completed)          │
│    │   │         ├── Emit ScaleOutCompleted event                │
│    │   │         ├── Record scale_operations_total metric        │
│    │   │         └── Remove avsoft.io/scale-started annotation   │
│    │   │                                                         │
│    │   └── No  → check timeout                                   │
│    │              ├── time.Since(scale-started) > 10m?            │
│    │              │   └── Yes → handleScaleFailure()              │
│    │              │              ├── Identify unready segments    │
│    │              │              ├── Populate failedSegments      │
│    │              │              ├── Set ScaleOutFailed=True      │
│    │              │              │   (reason=SegmentsNotReady)    │
│    │              │              ├── Emit ScaleOutFailed event    │
│    │              │              ├── Remove scale-started ann.    │
│    │              │              └── Stay in Scaling (no rollback)│
│    │              └── No  → requeue after 5s                     │
└─────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    Scale-In Flow                                  │
│                                                                  │
│  reconcileSegments()                                             │
│    │                                                             │
│    ├── Get existing primary StatefulSet                          │
│    ├── Compare spec.segments.count vs sts.spec.replicas          │
│    ├── Guard: currentCount > 0 (prevent false scale on restart)  │
│    │                                                             │
│    └── If desired < current → handleScaleIn()                    │
│         │                                                        │
│         ├── Pre-flight: cluster.Status.Phase == Running?          │
│         │   └── No → emit ScaleInBlocked, return (retry later)   │
│         │                                                        │
│         ├── Safety check: newCount < 50% of oldCount?            │
│         │   └── Yes → require avsoft.io/confirm-scale-in=true    │
│         │              └── Missing → emit ScaleInBlocked, return │
│         │                                                        │
│         ├── Set avsoft.io/scale-started annotation (timestamp)   │
│         ├── Set phase = Scaling                                  │
│         ├── Set DataRedistribution condition (ScaleInStarted)    │
│         ├── Emit ScaleInStarted event                            │
│         ├── Create redistribution Job (move data off segments)   │
│         ├── Scale down mirror StatefulSet (mirrors first)        │
│         ├── Scale down primary StatefulSet                       │
│         └── Set DataRedistribution condition (InProgress)        │
│                                                                  │
│  checkScaleProgress() — called when phase == Scaling             │
│    │                                                             │
│    ├── allSegmentStatefulSetsReady()?                             │
│    │   ├── No  → check timeout (same as scale-out)               │
│    │   └── Yes → determine scale-in vs scale-out                 │
│    │              │                                               │
│    │              └── If scale-in (desired < previous total):     │
│    │                   ├── If deletionPolicy=Delete:              │
│    │                   │   └── cleanupOrphanedPVCs()              │
│    │                   │       └── Delete PVCs for indices         │
│    │                   │           [newCount..oldCount-1]          │
│    │                   ├── Set phase = Running                    │
│    │                   ├── Update segmentsReady/segmentsTotal     │
│    │                   ├── Set DataRedistribution (Completed)     │
│    │                   ├── Emit ScaleInCompleted event            │
│    │                   ├── Record scale_operations_total{scale-in}│
│    │                   └── Remove avsoft.io/scale-started ann.    │
└─────────────────────────────────────────────────────────────────┘
```

**Upgrade lifecycle:**
- **Detection**: `isUpgradeNeeded()` checks whether `spec.version != status.clusterVersion` or the `avsoft.io/upgrade` annotation is present (in-progress upgrade).
- **Pre-flight check**: `handleUpgrade()` verifies the cluster is in `Running` phase. If not, it emits an `UpgradeBlocked` warning event and returns without error (retries on next reconcile).
- **`handleUpgrade()`**: Captures the current image from the coordinator StatefulSet via `getCurrentImage()`, stores rollback state (previousImage, previousVersion, phase, startedAt, phaseStartedAt) in the `avsoft.io/upgrade` annotation as JSON, sets phase to `Updating`, emits `UpgradeStarted` event, and delegates to `continueUpgrade()`.
- **`continueUpgrade()`**: Parses the upgrade state from the annotation, checks for phase timeout (10 minutes per phase), and dispatches to the appropriate phase handler. Phases progress in order: mirrors → primaries → standby → coordinator → verify.
- **`upgradePhase()`**: Generic phase handler that updates the StatefulSet image via `updateStatefulSetImage()`, checks readiness via `isStatefulSetReady()`, and advances to the next phase via `advanceUpgradePhase()` when ready. Skips the phase if the component is not enabled (e.g., mirroring disabled, standby disabled).
- **`verifyUpgrade()`**: Post-upgrade health check that confirms the coordinator and primary segments are ready. On success, delegates to `completeUpgrade()`.
- **`completeUpgrade()`**: Sets phase to `Running`, updates `status.clusterVersion` to the new version, sets `UpgradeCompleted` condition to `True`, removes the `avsoft.io/upgrade` annotation, and emits `UpgradeCompleted` event.
- **`rollbackUpgrade()`**: Triggered when a phase exceeds the 10-minute timeout. Reverts ALL StatefulSets (mirrors, primaries, standby, coordinator) to the `previousImage` via `revertStatefulSetImage()`. Sets phase to `Running`, restores `status.clusterVersion` to `previousVersion`, sets `UpgradeFailed` condition to `True` with reason `RolledBack`, removes the `avsoft.io/upgrade` annotation, and emits `UpgradeRollback` warning event.
- **Events**: `UpgradeStarted` (when upgrade begins), `UpgradeCompleted` (when all phases pass verification), `UpgradeBlocked` (when cluster is not in Running phase), `UpgradeRollback` (when a phase times out and rollback occurs).
- **Conditions**: `UpgradeCompleted` (True/UpgradeSucceeded), `UpgradeFailed` (True/RolledBack).

```
┌─────────────────────────────────────────────────────────────────┐
│                    Upgrade Flow                                   │
│                                                                   │
│  isUpgradeNeeded()                                                │
│    │                                                              │
│    ├── Check avsoft.io/upgrade annotation (in-progress?)          │
│    └── Check spec.version != status.clusterVersion                │
│                                                                   │
│  handleUpgrade()                                                  │
│    │                                                              │
│    ├── Pre-flight: cluster.Status.Phase == Running?               │
│    │   └── No → emit UpgradeBlocked, return (retry later)        │
│    │                                                              │
│    ├── Capture current image from coordinator StatefulSet         │
│    ├── Store state in avsoft.io/upgrade annotation (JSON)         │
│    ├── Set phase = Updating                                       │
│    ├── Emit UpgradeStarted event                                  │
│    └── Delegate to continueUpgrade()                              │
│                                                                   │
│  continueUpgrade() — called when avsoft.io/upgrade is present     │
│    │                                                              │
│    ├── Parse upgrade state from annotation                        │
│    ├── Check phase timeout: time.Since(phaseStartedAt) > 10m?     │
│    │   └── Yes → rollbackUpgrade()                                │
│    │              ├── Revert ALL StatefulSets to previousImage     │
│    │              ├── Set phase = Running                          │
│    │              ├── Restore clusterVersion = previousVersion     │
│    │              ├── Set UpgradeFailed=True (reason=RolledBack)   │
│    │              ├── Remove avsoft.io/upgrade annotation          │
│    │              └── Emit UpgradeRollback event                   │
│    │                                                              │
│    └── Dispatch by phase:                                         │
│         ├── mirrors     → upgradePhase(mirror STS, next=primaries)│
│         ├── primaries   → upgradePhase(primary STS, next=standby) │
│         ├── standby     → upgradePhase(standby STS, next=coord)   │
│         ├── coordinator → upgradePhase(coord STS, next=verify)    │
│         └── verify      → verifyUpgrade()                         │
│                            ├── Coordinator ready?                  │
│                            ├── Primaries ready?                    │
│                            └── Yes → completeUpgrade()             │
│                                       ├── Set phase = Running     │
│                                       ├── Update clusterVersion   │
│                                       ├── Set UpgradeCompleted    │
│                                       ├── Remove annotation       │
│                                       └── Emit UpgradeCompleted   │
│                                                                   │
│  upgradePhase(stsName, componentEnabled, nextPhase)               │
│    │                                                              │
│    ├── Component not enabled? → skip, advance to nextPhase        │
│    ├── Update StatefulSet image via updateStatefulSetImage()       │
│    ├── StatefulSet ready? → advance to nextPhase                  │
│    └── Not ready → requeue after 5s                               │
└─────────────────────────────────────────────────────────────────┘
```

**Stop/Start/Restart lifecycle:**
- **Stop** (`stop`, `stop-fast`, `stop-immediate`): Scales down StatefulSets in order: mirrors → primaries → standby → coordinator. Uses `scaleStatefulSet()` to set replicas to 0. Phase: `Running` → `Stopping` → `Stopped`.
- **Start** (`start`, `start-restricted`, `start-maintenance`): For normal start, triggers full reconciliation to restore all StatefulSets. For restricted/maintenance, scales up only the coordinator. Phase: `Stopped` → `Initializing`/`Restricted`/`Maintenance` → `Running`.
- **Restart** (`restart`): Performs a stop followed by a start. Phase: `Running` → `Stopping` → `Initializing` → `Running`.

**Storage expansion lifecycle:**
- **Detection**: `reconcileStorageExpansion()` compares `spec.*.storage.size` against actual PVC sizes for coordinator, standby, and segments.
- **`expandPVCIfNeeded()`**: For each PVC, compares the desired size against the current PVC size using `resource.Quantity.Cmp()`. If the desired size is larger, it calls `storageClassSupportsExpansion()` before patching the PVC.
- **`storageClassSupportsExpansion()`**: Pre-flight check that verifies the PVC's StorageClass allows volume expansion. Returns `(allowed bool, reason string)`.

```
┌─────────────────────────────────────────────────────────────────┐
│              Storage Expansion Flow                               │
│                                                                   │
│  reconcileStorageExpansion()                                      │
│    │                                                              │
│    ├── For coordinator PVC:                                       │
│    │     └── expandPVCIfNeeded(coordPVC, desiredSize)             │
│    ├── For standby PVC (if enabled):                              │
│    │     └── expandPVCIfNeeded(standbyPVC, desiredSize)           │
│    └── For each segment PVC (primary + mirror):                   │
│          └── expandPVCIfNeeded(segmentPVC, desiredSize)           │
│                                                                   │
│  expandPVCIfNeeded(pvc, desiredSize)                              │
│    │                                                              │
│    ├── PVC not found? → skip (no error)                           │
│    ├── desiredSize ≤ currentSize? → skip (no shrink)              │
│    │                                                              │
│    └── storageClassSupportsExpansion(pvc)                         │
│         │                                                         │
│         ├── Read StorageClass name from:                          │
│         │   1. pvc.spec.storageClassName                          │
│         │   2. volume.beta.kubernetes.io/storage-class annotation │
│         │                                                         │
│         ├── No StorageClass specified (default SC)?               │
│         │   └── allowed=true (cannot determine default SC caps)   │
│         │                                                         │
│         ├── Lookup StorageClass from API                          │
│         │   ├── Not found → allowed=false, reason="not found"     │
│         │   └── Transient error → allowed=true (fail-open)        │
│         │                                                         │
│         └── Check allowVolumeExpansion field                      │
│             ├── true  → allowed=true                              │
│             ├── false → allowed=false, reason="does not allow"    │
│             └── nil   → allowed=false, reason="does not allow"    │
│                                                                   │
│    If allowed:                                                    │
│      └── Patch PVC spec.resources.requests.storage                │
│                                                                   │
│    If blocked:                                                    │
│      └── Log WARN with PVC name, SC name, reason,                │
│          current size, desired size                                │
│          (no error returned — reconciliation continues)           │
│                                                                   │
│  After all PVCs processed:                                        │
│    ├── If any PVC expanded:                                       │
│    │   ├── Set StorageExpanded condition (True/PVCsExpanded)      │
│    │   ├── Emit StorageExpanded event                             │
│    │   └── Record cloudberry_pvc_size_bytes metric                │
│    └── If no PVCs expanded: no condition/event changes            │
└─────────────────────────────────────────────────────────────────┘
```

All metrics are registered with `ctrlmetrics.Registry` (controller-runtime's metrics registry) to ensure they are exposed on the `/metrics` endpoint.

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

Manages configuration, rolling restarts, and maintenance:

1. Detects parameter changes via hash comparison
2. Classifies changed parameters using `restartRequiredParams` map (shared_buffers, max_connections, wal_level, etc.)
3. **Reload-safe changes**: Updates ConfigMap, sets `ConfigApplied=True/ConfigReloaded`, emits `ConfigReloaded` event, increments `cloudberry_config_reload_total`
4. **Restart-required changes**: Updates ConfigMap, triggers rolling restart via `triggerRollingRestart()`, sets `ConfigApplied=False/RestartRequired`
5. **Rolling restart state machine**: Tracked via `avsoft.io/rolling-restart` annotation with JSON state (`phase`, `startedAt`, `restartParams`). Phases: mirrors → primaries → standby → coordinator → completed. Uses `continueRollingRestart()` and `restartStatefulSet()` to progress through phases.
6. Creates Kubernetes `batchv1.Job` resources for maintenance operations via `BuildMaintenanceJob()`:
   - Supported operations: `vacuum`, `vacuum-analyze`, `vacuum-full`, `analyze`, `reindex`
   - Job properties: `BackoffLimit=1`, `TTLSecondsAfterFinished=3600`, `RestartPolicy=Never`
   - `PGPASSWORD` sourced from admin password Secret
   - Unknown operations emit `MaintenanceUnknown` warning event
   - Emits `MaintenanceStarted` event with job name
7. Monitors Job completion and cleans up finished Jobs
8. Performs a single consolidated status update per reconciliation cycle to reduce API server load
9. Uses `MergePatch` for annotation removal to avoid race conditions with concurrent updates

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

The operator exposes metrics at the `/metrics` endpoint. All custom metrics are registered with `ctrlmetrics.Registry` (controller-runtime's built-in Prometheus registry), which ensures they are served alongside standard controller-runtime metrics on the same `/metrics` endpoint.

- **Cluster metrics**: `cloudberry_cluster_info`, `cloudberry_coordinator_up`, `cloudberry_standby_up`
- **Segment metrics**: `cloudberry_segments_ready`, `cloudberry_segments_total`, `cloudberry_segments_failed`
- **Mirroring metrics**: `cloudberry_mirroring_in_sync`
- **Reconciliation metrics**: `cloudberry_reconcile_total`, `cloudberry_reconcile_errors_total`, `cloudberry_reconcile_duration_seconds`
- **Configuration metrics**: `cloudberry_config_reload_total`
- **FTS metrics**: `cloudberry_fts_probe_total`, `cloudberry_fts_failover_total`, `cloudberry_replication_lag_bytes`
- **Connection metrics**: `cloudberry_connections_active`, `cloudberry_connections_max`
- **Scale metrics**: `cloudberry_scale_operations_total`, `cloudberry_redistribution_progress`

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

## Error Handling Patterns

The operator uses a layered error handling strategy that combines structured error types, retry with exponential backoff, and observability integration to ensure reliable reconciliation and easy troubleshooting.

### Error Type Hierarchy

All custom errors in `internal/util/errors.go` follow a consistent pattern: each typed error wraps a sentinel error via `Unwrap()`, enabling `errors.Is()` for programmatic classification.

```
Sentinel Errors (for errors.Is matching)
├── ErrNotFound           ← ClusterNotFoundError, SegmentNotFoundError
├── ErrInvalidInput       ← ValidationError
├── ErrUnauthorized       ← AuthenticationError
├── ErrForbidden          ← PermissionDeniedError
├── ErrRetryExhausted     ← returned by RetryWithBackoff
├── ErrTimeout
├── ErrConnectionFailed
└── ErrAlreadyExists

Wrapper Errors (preserve inner error chain)
└── ReconcileError        ← wraps any error with operation context
```

**Design principles:**

- **Sentinel errors** (`ErrNotFound`, `ErrInvalidInput`, etc.) enable callers to classify errors without type assertions
- **Typed errors** (`ClusterNotFoundError`, `ValidationError`, etc.) carry structured context (cluster name, field name, etc.) for logging and API responses
- **`ReconcileError`** wraps any error with the operation name, preserving the full error chain for `errors.Is()` and `errors.As()`
- **`ErrRetryExhausted`** is returned by `RetryWithBackoff()` when all attempts fail, wrapping the last error from the final attempt

### Retry with Exponential Backoff

The `RetryWithBackoff()` function in `internal/util/retry.go` provides a generic retry mechanism used throughout the operator for transient failure recovery.

```
┌─────────────────────────────────────────────────────────────────┐
│                  RetryWithBackoff Flow                            │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │  For attempt = 0 to MaxRetries:                          │    │
│  │                                                          │    │
│  │  1. Check context.Err() → return if canceled/expired     │    │
│  │  2. Call fn(ctx)                                         │    │
│  │     ├── nil → return nil (success)                       │    │
│  │     └── error → continue to backoff                      │    │
│  │  3. Calculate backoff:                                   │    │
│  │     sleep = min(initialBackoff × multiplier^attempt,     │    │
│  │                  maxBackoff)                              │    │
│  │     sleep += jitter(sleep × jitterFraction)              │    │
│  │  4. select:                                              │    │
│  │     ├── ctx.Done() → return "context canceled"           │    │
│  │     └── time.After(sleep) → next attempt                 │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                   │
│  All attempts exhausted → return ErrRetryExhausted + lastErr     │
└─────────────────────────────────────────────────────────────────┘
```

**Key behaviors:**

- **Context-first**: Checks `ctx.Err()` before each attempt and during backoff sleep via `select`
- **Exponential growth**: Backoff doubles each attempt (configurable multiplier), capped at `MaxBackoff`
- **Jitter**: Adds randomized jitter (`JitterFraction × backoff × rand`) to prevent thundering herd
- **Error wrapping**: Returns `fmt.Errorf("%w: after N attempts: %w", ErrRetryExhausted, lastErr)` — both sentinels are matchable via `errors.Is()`

### Reconciliation Error Flow

When a reconciliation cycle encounters an error, the operator follows this flow:

```
┌──────────────────┐
│  Reconcile()     │
│  starts timer    │
└────────┬─────────┘
         │
    ┌────▼────────────┐
    │  Execute logic   │
    └────┬────────┬───┘
    success       error
         │            │
    ┌────▼────┐  ┌────▼──────────────────────────┐
    │ Record  │  │ Record metrics:                │
    │ metrics:│  │   RecordReconcile(             │
    │ success │  │     cluster, ns, "error", dur) │
    │         │  │ Set span error:                │
    └────┬────┘  │   SetSpanError(span, err)      │
         │       │ Log structured error:           │
         │       │   slog.Error("reconciliation    │
         │       │     failed", "cluster", name,   │
         │       │     "error", err)               │
         │       └────────────────────────────────┘
         │
    ┌────▼────────────────┐
    │  Requeue (30s/10s)  │
    │  30s on success     │
    │  10s on error       │
    └─────────────────────┘
```

**Metrics recording**: Every reconciliation cycle calls `RecordReconcile(cluster, namespace, result, duration)` where `result` is `"success"` or `"error"`. This populates:
- `cloudberry_reconcile_total` (counter with `result` label)
- `cloudberry_reconcile_errors_total` (counter, incremented only on errors)
- `cloudberry_reconcile_duration_seconds` (histogram)

**Telemetry spans**: When OTLP tracing is enabled, `SetSpanError(span, err)` sets the span status to `codes.Error` and records an `exception` event. The function is nil-safe — calling it with `nil` error is a no-op.

**Structured logging**: All reconciliation errors are logged with `slog` including `cluster`, `namespace`, `controller`, `reconcileID`, `error`, and `duration` fields.

### Webhook Validation

The validating admission webhook (`internal/webhook/validating.go`) rejects invalid `CloudberryCluster` resources before they enter the system. Validation runs synchronously during the Kubernetes API admission phase.

**Validation chain:**

1. `segments.count >= 1` — prevents zero-segment clusters
2. `coordinator.storage.size` required — prevents clusters without coordinator storage
3. `segments.storage.size` required — prevents clusters without segment storage
4. OIDC: `issuerURL` required when `oidc.enabled=true`
5. OIDC: `clientID` required when `oidc.enabled=true`
6. Cross-namespace duplicate name check via `checkDuplicateName()`

### Pod Deletion Recovery

The operator detects and recovers from pod deletions automatically through the standard reconciliation loop:

1. **Detection**: `recordMetricsSnapshot()` reads `StatefulSet.Status.ReadyReplicas` and updates `status.segmentsReady`. When `segmentsReady < segmentsTotal`, the cluster is degraded
2. **Kubernetes recovery**: The StatefulSet controller automatically recreates deleted pods
3. **Operator recovery**: On the next reconciliation cycle (requeued every 30s), the operator detects the restored pods and updates `status.segmentsReady` back to `segmentsTotal`
4. **No phase change**: Pod deletion does not change the cluster phase — the cluster stays in `Running` with degraded segment counts

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

### HTTP Server Timeouts

The API server configures explicit timeouts on the `http.Server` to prevent resource exhaustion from slow or malicious clients:

| Timeout | Value | Purpose |
|---------|-------|---------|
| `ReadTimeout` | 30s | Maximum duration for reading the entire request, including the body |
| `WriteTimeout` | 60s | Maximum duration before timing out writes of the response |
| `IdleTimeout` | 120s | Maximum time to wait for the next request when keep-alives are enabled |

### API Server Lifecycle

The API server starts in a background goroutine from the operator `main()` function. It listens on the address configured by `APIAddress` (default `:8090`). On context cancellation, the server performs a graceful shutdown with a 5-second timeout using `context.Background()` to ensure the shutdown completes even when the parent context is already canceled. During shutdown, the rate limiter's `Stop()` method is called to terminate the background cleanup goroutine and prevent goroutine leaks.

## DBClientFactory Pattern

The `DBClientFactory` interface is defined in `internal/db/factory.go` as a shared interface. Previously, duplicate interface definitions existed in both the controller and API server packages. The interface was extracted to the `internal/db` package to serve as the single source of truth, eliminating duplication and ensuring consistent signatures across consumers.

Both controllers and the API server use the factory instead of creating database clients directly.

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   Controller     │────▶│  DBClientFactory  │────▶│   DB Client      │
│  (HA, Admin)     │     │                   │     │   (pgx)          │
└──────────────────┘     └────────┬──────────┘     └──────────────────┘
                                  │
┌──────────────────┐              │
│   API Server     │──────────────┘
│  (Sessions)      │
└──────────────────┘     ┌───────────────────┐
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

### API Server Integration

The API server receives a `DBClientFactory` at startup (injected from `cmd/operator/main.go`). The factory is used by session management handlers to create short-lived database connections:

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│  HTTP Client │────▶│  API Server  │────▶│  DBClientFactory  │────▶│  PostgreSQL  │
│  (ctl/curl)  │     │  (handler)   │     │                   │     │  Coordinator │
└──────────────┘     └──────┬───────┘     └───────────────────┘     └──────────────┘
                            │
                   ┌────────▼────────┐
                   │  Session Flow:   │
                   │  1. Resolve      │
                   │     cluster CR   │
                   │  2. Create DB    │
                   │     client       │
                   │  3. Execute SQL  │
                   │     (pg_stat_    │
                   │      activity,   │
                   │      pg_cancel/  │
                   │      terminate_  │
                   │      backend)    │
                   │  4. Close client │
                   └─────────────────┘
```

**Session operation flow:**

1. The API handler receives a request (e.g., `GET /clusters/{name}/sessions`)
2. The handler resolves the `CloudberryCluster` CR from the Kubernetes API
3. If no `DBClientFactory` is configured, the handler returns a graceful degradation response (empty sessions with a `"database connection not available"` message)
4. The handler calls `dbFactory.NewClient()` to create a short-lived database connection to the cluster's coordinator
5. If the connection fails, the handler returns `503 DB_UNAVAILABLE`
6. The handler executes the database operation:
   - **List sessions**: Queries `pg_stat_activity` for active sessions
   - **Cancel query**: Calls `pg_cancel_backend(pid)` to cancel a running query
   - **Terminate session**: Calls `pg_terminate_backend(pid)` to terminate a session
7. The database client is closed via `defer dbClient.Close()`
8. The handler returns the result as JSON

The `DBClientFactory` interface is defined once in `internal/db/factory.go` and imported by both the controller and API server packages. This eliminates the previous duplication where identical interfaces existed in `internal/controller/ha_controller.go` and `internal/api/server.go`.

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
│  │    using           │    │  - Server cert validity     │  │
│  │    WriteSecret     │    │    configurable (default    │  │
│  │    (write op)      │    │    1 year)                   │  │
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

### Vault PKI Certificate Issuance

The Vault PKI strategy issues certificates by calling `WriteSecretWithResponse()` on the Vault PKI issue endpoint (e.g., `pki/issue/cloudberry-operator`). This is a write operation that generates a new certificate — using `ReadSecret()` would be incorrect since PKI issuance requires a POST/PUT request. The response contains the certificate, private key, and CA chain, which are stored in the Kubernetes Secret.

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
