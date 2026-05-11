# Cloudberry Operator - Overview Specification

**Version**: 1.0.0
**API Group**: avsoft.io
**API Version**: v1alpha1

---

## 1. Purpose

The Cloudberry Operator is a Kubernetes operator that manages the full lifecycle of Cloudberry Database clusters on Kubernetes. It provides declarative management of cluster installation, configuration, high availability, authentication, authorization, and day-to-day administration through Custom Resource Definitions (CRDs).

## 2. Goals

1. **Declarative Cluster Management** - Define Cloudberry clusters as Kubernetes custom resources
2. **Automated Lifecycle** - Install, upgrade, scale, configure, and remove clusters automatically
3. **High Availability** - Segment mirroring, coordinator standby, automatic failover
4. **Security** - Basic and OIDC authentication, RBAC, TLS, Vault integration
5. **Observability** - Prometheus metrics, OTLP tracing, structured logging
6. **CLI Companion** - `cloudberry-ctl` utility for imperative operations through the operator

## 3. Architecture

### 3.1 Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                         │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              cloudberry-operator                       │   │
│  │  ┌────────────┐ ┌────────────┐ ┌──────────────────┐  │   │
│  │  │  Cluster   │ │    HA      │ │  Auth/AuthZ      │  │   │
│  │  │ Controller │ │ Controller │ │  Controller      │  │   │
│  │  └─────┬──────┘ └─────┬──────┘ └────────┬─────────┘  │   │
│  │        │               │                  │            │   │
│  │  ┌─────┴───────────────┴──────────────────┴─────────┐ │   │
│  │  │            Reconciliation Engine                   │ │   │
│  │  │  (controller-runtime / kubebuilder)                │ │   │
│  │  └───────────────────┬────────────────────────────────┘ │   │
│  │                      │                                   │   │
│  │  ┌──────────┐ ┌──────┴──────┐ ┌────────────────────┐   │   │
│  │  │ Metrics  │ │ Telemetry   │ │ Auth Middleware     │   │   │
│  │  │(Prom)    │ │ (OTLP)      │ │ (Basic+OIDC)       │   │   │
│  │  └──────────┘ └─────────────┘ └────────────────────┘   │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Cloudberry Cluster                        │   │
│  │  ┌─────────────┐  ┌──────────────┐                   │   │
│  │  │ Coordinator │  │   Standby    │                   │   │
│  │  │ StatefulSet │  │  StatefulSet │                   │   │
│  │  └─────────────┘  └──────────────┘                   │   │
│  │  ┌─────────────────────────────────────────────────┐ │   │
│  │  │          Segment StatefulSets                     │ │   │
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐           │ │   │
│  │  │  │Primary 0│ │Primary 1│ │Primary N│           │ │   │
│  │  │  │Mirror  0│ │Mirror  1│ │Mirror  N│           │ │   │
│  │  │  └─────────┘ └─────────┘ └─────────┘           │ │   │
│  │  └─────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   Vault      │  │  Keycloak    │  │  Observability   │  │
│  │  (optional)  │  │  (OIDC IdP)  │  │  Stack           │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Reconciliation Loop

The operator follows the standard Kubernetes reconciliation pattern:

1. **Watch** - Monitor CloudberryCluster and related resources for changes
2. **Diff** - Compare desired state (CR spec) with actual state (K8s resources + DB state)
3. **Act** - Create, update, or delete resources to converge to desired state
4. **Status** - Update CR status subresource with current state
5. **Requeue** - Schedule next reconciliation if needed

### 3.3 Controller Responsibilities

| Controller | Watches | Manages |
|-----------|---------|---------|
| ClusterController | CloudberryCluster | StatefulSets, Services, ConfigMaps, Secrets, PVCs |
| HAController | CloudberryCluster (HA section) | Mirroring config, FTS settings, standby lifecycle |
| AuthController | CloudberryCluster (Auth section) | pg_hba.conf ConfigMap, OIDC config, TLS secrets |
| AdminController | CloudberryCluster (Config section) | Parameter ConfigMaps, maintenance jobs |

### 3.4 Operator Lifecycle

#### Installation
```bash
# Via Helm
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system --create-namespace

# CRDs are installed as part of the Helm chart
```

#### Upgrade
```bash
helm upgrade cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system
```

#### Uninstall
```bash
# Operator removal (clusters remain)
helm uninstall cloudberry-operator --namespace cloudberry-system

# Full cleanup (including CRDs - DESTRUCTIVE)
helm uninstall cloudberry-operator --namespace cloudberry-system
kubectl delete crd cloudberryclusters.avsoft.io
```

## 4. Design Principles

1. **Declarative over Imperative** - All state expressed in CRDs
2. **Idempotent Reconciliation** - Same input always produces same output
3. **Graceful Degradation** - Operator continues functioning if optional services (Vault, Keycloak) are unavailable
4. **Least Privilege** - Operator RBAC scoped to minimum required permissions
5. **Observable** - Every operation emits metrics, traces, and structured logs
6. **Testable** - All external dependencies behind interfaces for mocking
7. **Configurable** - ENV variables override flags; sensible defaults for everything

## 5. Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Go | 1.23+ |
| Operator Framework | controller-runtime | v0.19+ |
| CLI Framework | cobra + viper | latest |
| OIDC | go-oidc/v3 + oauth2 | latest |
| Database Driver | pgx/v5 | latest |
| Vault Client | vault/api/v2 | latest |
| Metrics | prometheus/client_golang | latest |
| Tracing | opentelemetry-go | latest |
| Testing | testify + gomock | latest |
| Linting | golangci-lint | (existing config) |

## 6. Namespace Convention

- **Operator namespace**: `cloudberry-system`
- **Test namespace**: `cloudberry-test`
- **Cluster namespace**: User-defined (defaults to same as CR namespace)
