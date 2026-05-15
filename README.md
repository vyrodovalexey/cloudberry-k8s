# Cloudberry Kubernetes Operator

A Kubernetes operator for managing the full lifecycle of [Cloudberry Database](https://cloudberry.apache.org/) clusters. Provides declarative cluster management, high availability, authentication, observability, and a companion CLI utility.

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [cloudberry-ctl CLI](#cloudberry-ctl-cli)
- [Development](#development)
- [Testing](#testing)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Architecture

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
│  │  │  REST API Server (:8090)                             │ │  │
│  │  │  Rate Limiter → Auth Middleware → Handlers           │ │  │
│  │  └──────────────────────────────────────────────────────┘ │  │
│  │                                                           │  │
│  │  ┌──────────┐  ┌───────────┐  ┌────────────────────────┐ │  │
│  │  │ Metrics  │  │ Telemetry │  │   Auth Middleware      │ │  │
│  │  │ (Prom)   │  │  (OTLP)   │  │ (bcrypt + OIDC/JWT)   │ │  │
│  │  └──────────┘  └───────────┘  └────────────────────────┘ │  │
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
│  │  │ StatefulSet  │  │ StatefulSet  │                       │  │
│  │  └──────────────┘  └──────────────┘                       │  │
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

The operator follows the standard Kubernetes reconciliation pattern: **Watch** resources for changes, **Diff** desired vs. actual state, **Act** to converge, **Update** status, and **Requeue** as needed.

## Features

**Cluster Lifecycle Management**
- Declarative cluster creation, updates, and deletion via `CloudberryCluster` CRD
- Cross-namespace cluster name uniqueness enforced by validating webhook
- Start, stop, and restart with multiple modes:
  - **Stop modes**: smart (wait for clients), fast (rollback transactions), immediate (abort connections)
  - **Start modes**: normal (all components), restricted (coordinator only), maintenance (utility mode)
  - **Restart**: stop + start with phase transitions (Running → Stopping → Initializing → Running)
- New cluster phases: `Stopped`, `Stopping`, `Restricted`, `Maintenance`
- Scale-out with automatic data redistribution (increase `segments.count` to add segments)
  - Pre-flight check blocks scaling when cluster is not in `Running` phase
  - 10-minute timeout with failure detection and `status.failedSegments` reporting
  - No automatic rollback on failure — manual intervention required
- Scale-in with PVC policy support (decrease `segments.count` to remove segments)
  - Scale-in by more than 50% requires `avsoft.io/confirm-scale-in=true` annotation (safety guard)
  - Confirmation annotation automatically cleaned up after successful completion
- Rolling upgrades with automatic rollback on failure
  - Phase-by-phase upgrade: mirrors → primaries → standby → coordinator → verify
  - 10-minute per-phase timeout with automatic rollback to previous image
  - Upgrade state tracked via `avsoft.io/upgrade` annotation
  - Pre-flight check blocks upgrades when cluster is not in `Running` phase
- Online PVC storage expansion for coordinator, standby, and segments (no shrink)
- Cluster deletion with configurable PVC policy (`Retain` or `Delete`) and event reporting
  - Backup-on-delete: optional pre-deletion backup Job when `backupOnDelete: true`
  - PVC events: `PVCsRetained` for Retain policy, `PVCsDeleted` for Delete policy
  - Deletion lifecycle events: `Deleting` → `BackupOnDelete` → `PVCsRetained`/`PVCsDeleted` → `Deleted`

**High Availability**
- Segment mirroring with group and spread layouts
- Fault Tolerance Service (FTS) with configurable probe intervals
- Automatic failover from primary to mirror segments
- Coordinator standby with WAL streaming replication
- Incremental, full, and differential segment recovery
- Manual segment rebalancing with configurable skew threshold, parallelism, and table exclusion patterns
- Rebalance status API and CLI (`--status`, `--tables` flags)
- Data skew coefficient metric (`cloudberry_data_skew_coefficient`)

**Authentication & Authorization**
- Dual-mode authentication: Basic + OIDC (Keycloak)
- JWT validation with JWKS caching and role claim extraction
- Five-tier permission model: Self Only, Basic, Operator Basic, Operator, Admin
- `pg_hba.conf` management via CRD
- SSL/TLS support with configurable minimum TLS version
- Webhook TLS certificate management (Vault PKI or self-signed with automatic rotation)
- HashiCorp Vault integration for secrets management

**Observability**
- Prometheus metrics for cluster health, reconciliation, FTS, connections, scale operations, and PVC sizes
- Reconciliation metrics: `cloudberry_reconcile_total`, `cloudberry_reconcile_errors_total`, `cloudberry_reconcile_duration_seconds` with cluster/namespace/result labels
- OpenTelemetry (OTLP) distributed tracing with gRPC/HTTP exporters
- Span error recording via `SetSpanError()` — sets error status and exception events on OTEL spans
- Structured logging (slog) with JSON output including cluster, namespace, controller, and reconcileID fields
- Structured error types with sentinel errors (`ErrNotFound`, `ErrInvalidInput`, `ErrRetryExhausted`) supporting `errors.Is()` classification
- Retry with exponential backoff for transient failures (configurable max retries, backoff, jitter)
- Webhook validation rejects invalid cluster specs at admission time (segments, OIDC, storage)
- Automatic pod deletion detection and recovery with degraded state reporting

**Security Hardening**
- SQL injection prevention with parameterized queries (pgx native config builder)
- HTTP server timeouts (ReadTimeout, WriteTimeout, IdleTimeout) to prevent resource exhaustion
- Response body size limits in CLI client (10 MiB)
- URL encoding for all path parameters in CLI
- Rate limiter goroutine leak prevention with `sync.Once`-guarded shutdown
- DB connection pool leak prevention on retry failures

**Administration**
- Configuration management with automatic hot-reload vs rolling restart detection
  - Reload-safe parameters applied without pod restarts
  - Restart-required parameters (shared_buffers, max_connections, wal_level, etc.) trigger rolling restart
  - Rolling restart order: mirrors → primaries → standby → coordinator
  - Rolling restart state tracked via `avsoft.io/rolling-restart` annotation
- Cluster-wide, coordinator-only, per-database, and per-role parameters
- Maintenance operations via Kubernetes Jobs: vacuum, vacuum-analyze, vacuum-full, analyze, reindex, backup-on-delete
  - Jobs created with `BackoffLimit=1`, `TTLSecondsAfterFinished=3600`
  - `PGPASSWORD` sourced from admin password Secret
- Session management: list active sessions from `pg_stat_activity`, cancel queries via `pg_cancel_backend()`, terminate sessions via `pg_terminate_backend()` (with PID validation and graceful degradation when DB is unavailable)
- Resource group management: create, list, assign, and delete resource groups for workload isolation
  - Create groups with concurrency, CPU, and memory limits
  - Assign database roles to resource groups (`ALTER ROLE ... RESOURCE GROUP`)
  - Query live resource groups from the database with CRD spec fallback
- API admin password via `CLOUDBERRY_API_ADMIN_PASSWORD` env var or auto-generated

**CLI Companion**
- `cloudberry-ctl` for imperative operations through the operator API
- Table, JSON, and YAML output formats with deterministic column ordering
- Shell completion for bash, zsh, and fish
- Environment variable and config file support (priority: CLI flag > env var > config file > default)
- Verbose mode (`-v`) for HTTP request/response debugging
- Response body size limit (10 MiB) and URL-encoded path parameters
- Stub commands return clear "not yet implemented" errors

## Quick Start

```bash
# 1. Install the operator via Helm
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system --create-namespace

# 2. Create a minimal Cloudberry cluster
kubectl apply -f - <<EOF
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: my-cluster
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
EOF

# 3. Check cluster status
kubectl get cloudberryclusters -n cloudberry-test

# 4. Use cloudberry-ctl for management
cloudberry-ctl cluster status --cluster my-cluster --namespace cloudberry-test
```

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Kubernetes | >= 1.26 |
| Helm | >= 3.x |
| Go (for building) | >= 1.26.3 |
| kubectl | >= 1.26 |

Optional:
- **Vault** for secrets management
- **Keycloak** for OIDC authentication
- **Prometheus** for metrics collection
- **OpenTelemetry Collector** for distributed tracing

## Installation

### Helm (Recommended)

```bash
# Create the operator namespace
kubectl create namespace cloudberry-system

# Install with default values
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system

# Install with custom values
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set operator.logLevel=debug \
  --set vault.enabled=true \
  --set vault.address=http://vault:8200

# Verify the installation
kubectl get pods -n cloudberry-system
```

### From Source

```bash
# Clone the repository
git clone https://github.com/cloudberry-contrib/cloudberry-k8s.git
cd cloudberry-k8s

# Build binaries
make build

# Build Docker images
make docker-build

# Deploy via Helm
make helm-install
```

See [docs/installation.md](docs/installation.md) for detailed installation instructions and configuration options.

## Usage

### Creating a Cluster

Apply a `CloudberryCluster` manifest:

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
    storage:
      storageClass: fast-ssd
      size: 50Gi
  standby:
    enabled: true
    storage:
      size: 50Gi
  segments:
    count: 8
    mirroring:
      enabled: true
      layout: spread
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
  monitoring:
    enabled: true
    serviceMonitor: true
  deletionPolicy: Retain
```

### Managing Cluster Lifecycle

```bash
# Check status
cloudberry-ctl cluster status --cluster my-cluster

# Stop cluster (fast mode)
cloudberry-ctl cluster stop --cluster my-cluster --mode fast

# Start cluster
cloudberry-ctl cluster start --cluster my-cluster

# Restart cluster
cloudberry-ctl cluster restart --cluster my-cluster
```

### Scaling Operations

```bash
# Scale out by increasing segment count
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"count": 6}}}'

# Scale in by decreasing segment count
kubectl patch cloudberrycluster my-cluster -n cloudberry-test --type merge \
  -p '{"spec": {"segments": {"count": 4}}}'

# Scale-in >50% requires confirmation annotation
kubectl annotate cloudberrycluster my-cluster -n cloudberry-test \
  avsoft.io/confirm-scale-in=true

# Monitor scale progress
cloudberry-ctl cluster scale-status --cluster my-cluster

# Check for failed segments after a scale-out failure
kubectl get cloudberrycluster my-cluster -n cloudberry-test \
  -o jsonpath='{.status.failedSegments}' | jq .

# Check scale events (blocked, failed, completed)
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' | grep -E 'Scale'
```

### Configuration Management

```bash
# View current parameters
cloudberry-ctl config get --cluster my-cluster

# Set a parameter
cloudberry-ctl config set --cluster my-cluster --param work_mem --value 256MB

# Reload configuration (no restart)
cloudberry-ctl config reload --cluster my-cluster
```

### High Availability Operations

```bash
# Check mirroring status
cloudberry-ctl ha mirroring status --cluster my-cluster

# Start incremental recovery
cloudberry-ctl ha recovery start --cluster my-cluster --type incremental

# Rebalance segments
cloudberry-ctl ha rebalance --cluster my-cluster

# Rebalance specific tables
cloudberry-ctl ha rebalance --cluster my-cluster --tables orders,customers

# Check rebalance status
cloudberry-ctl ha rebalance --cluster my-cluster --status

# Check standby status
cloudberry-ctl ha standby status --cluster my-cluster
```

See [docs/user-guide.md](docs/user-guide.md) for the complete user guide.

## Configuration

### Helm Chart Values

Key configuration options in `values.yaml`:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Operator replicas | `1` |
| `image.repository` | Operator image | `cloudberry-operator` |
| `operator.logLevel` | Log level | `info` |
| `operator.leaderElection` | Enable leader election | `true` |
| `operator.apiAddress` | REST API bind address | `:8090` |
| `operator.webhookEnabled` | Enable admission webhooks | `false` |
| `env.CLOUDBERRY_API_ADMIN_PASSWORD` | Admin password for the REST API (auto-generated if not set) | (generated) |
| `vault.enabled` | Enable Vault integration | `false` |
| `oidc.enabled` | Enable OIDC authentication | `false` |
| `telemetry.enabled` | Enable OTLP tracing | `false` |
| `telemetry.otlpInsecure` | Disable TLS for OTLP exporter | `false` |
| `metrics.enabled` | Enable Prometheus metrics | `true` |
| `webhook.enabled` | Enable admission webhooks | `true` |
| `webhook.certSource` | Certificate source (`self-signed` or `vault-pki`) | `self-signed` |

See [docs/installation.md](docs/installation.md) for the full values reference.

### CRD Configuration

The `CloudberryCluster` CRD supports configuration for:

- **Coordinator**: resources, storage (with online expansion), port, node selectors
- **Standby**: enable/disable, resources, storage (with online expansion)
- **Segments**: count, primaries per host, mirroring layout, anti-affinity, rebalance configuration, storage (with online expansion)
- **Authentication**: basic auth, OIDC, HBA rules, SSL/TLS
- **Configuration**: cluster-wide, coordinator-only, per-database, per-role parameters
- **High Availability**: FTS probe settings, checksums
- **Vault**: address, auth method, secret path
- **Monitoring**: metrics port, ServiceMonitor
- **Telemetry**: OTLP endpoint, protocol, sampling rate

See [docs/api-reference.md](docs/api-reference.md) for the complete API reference.

## cloudberry-ctl CLI

`cloudberry-ctl` provides imperative access to cluster management:

```bash
# Build from source
make build-ctl

# Show version
cloudberry-ctl version

# Shell completion
cloudberry-ctl completion bash > /etc/bash_completion.d/cloudberry-ctl
cloudberry-ctl completion zsh > "${fpath[1]}/_cloudberry-ctl"
```

See [docs/cloudberry-ctl.md](docs/cloudberry-ctl.md) for the full command reference.

## Development

### Project Structure

```
cloudberry-k8s/
├── api/v1alpha1/          # CRD Go types and generated code
├── cmd/
│   ├── operator/          # Operator entry point
│   └── cloudberry-ctl/    # CLI entry point
├── internal/
│   ├── api/               # REST API server with rate limiting
│   ├── auth/              # Authentication providers (bcrypt, OIDC/JWT)
│   ├── builder/           # Kubernetes resource builders
│   ├── certmanager/       # Webhook TLS cert lifecycle (Vault PKI / self-signed)
│   ├── config/            # Operator configuration
│   ├── controller/        # Reconciliation controllers
│   ├── ctl/               # Operator API client for cloudberry-ctl
│   ├── db/                # Database client (pgx) and client factory
│   ├── metrics/           # Prometheus metrics
│   ├── telemetry/         # OpenTelemetry tracing
│   ├── util/              # Shared utilities
│   ├── vault/             # Vault client
│   └── webhook/           # Admission webhooks
├── deploy/
│   ├── helm/              # Helm chart
│   └── docker/            # Docker-related files
├── test/
│   ├── e2e/               # End-to-end tests
│   ├── functional/        # Functional tests
│   ├── integration/       # Integration tests
│   ├── cases/             # Shared test cases
│   └── testutil/          # Test utilities
├── specifications/        # Design specifications
├── Dockerfile             # Operator container image
├── Dockerfile.ctl         # CLI container image
├── Makefile               # Build automation
└── .github/workflows/     # CI/CD pipelines
```

### Building

```bash
# Build everything
make build

# Build operator only
make build-operator

# Build CLI only
make build-ctl

# Build Docker images
make docker-build

# Generate CRD manifests and deepcopy
make generate
make manifests
```

### Code Quality

```bash
# Run linter
make lint

# Run go vet
make vet

# Format code
make fmt

# Run vulnerability check
make vuln
```

## Testing

```bash
# Unit tests
make test

# Unit tests with coverage report
make test-cover

# Functional tests
make test-functional

# Integration tests (requires Docker Compose test environment)
make test-env-up       # Start 9 services: Vault, Keycloak, MinIO, Kafka, RabbitMQ, VictoriaMetrics, Grafana, Tempo
make test-env-setup    # Configure services (Vault PKI, Keycloak realm, MinIO buckets, etc.)
make test-integration
make test-env-down     # Tear down

# End-to-end tests (requires Kubernetes cluster)
make test-e2e

# All tests
make test-all
```

**Test Data Loading** (prerequisite for scale/rebalance/performance tests):

```bash
# Load Scenario 7 test data (~1.45M rows, ~218 MB across 5 tables)
bash test/scenarios/scenario7_load_data.sh
```

Scenario 7 populates the `mydb` database with realistic test data including Pareto-skewed distributions and rebalance exclusion patterns. Run this before any performance, scale, or rebalance tests. See [docs/user-guide.md](docs/user-guide.md#test-data-setup) for details.

The project targets **90%+ unit test statement coverage** per package. Key coverage: `internal/vault` at 99%, `internal/metrics` at 100%, `internal/db` at 93%, `internal/certmanager` at ~90%, `internal/controller` at ~83%. See [docs/development.md](docs/development.md) for the full development and testing guide.

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | System design, components, and data flows |
| [Installation](docs/installation.md) | Prerequisites, Helm installation, and configuration |
| [User Guide](docs/user-guide.md) | Creating clusters, lifecycle management, HA, auth |
| [API Reference](docs/api-reference.md) | REST API endpoints, schemas, and error codes |
| [cloudberry-ctl](docs/cloudberry-ctl.md) | CLI installation, configuration, and command reference |
| [Development](docs/development.md) | Development setup, building, testing, and contributing |

## Contributing

Contributions are welcome. To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes with tests
4. Run the full test suite (`make test && make lint`)
5. Commit your changes (`git commit -m 'Add my feature'`)
6. Push to the branch (`git push origin feature/my-feature`)
7. Open a Pull Request

### Guidelines

- Follow the existing code style and conventions
- Write unit tests for all new code (target 90%+ coverage)
- Update documentation for user-facing changes
- Use conventional commit messages
- Ensure `make lint` passes before submitting

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
