# Development Guide

This guide covers setting up a development environment, building the project, running tests, and contributing to the Cloudberry Kubernetes Operator.

## Table of Contents

- [Development Environment Setup](#development-environment-setup)
- [Project Structure](#project-structure)
- [Building](#building)
- [Testing](#testing)
- [Code Review Findings and Fixes](#code-review-findings-and-fixes)
  - [Test Coverage Requirements](#test-coverage-requirements)
  - [Performance Testing](#performance-testing)
  - [Running REST API Performance Tests](#running-rest-api-performance-tests)
- [Monitoring Stack Makefile Targets](#monitoring-stack-makefile-targets)
- [Idle Daemon Reconnection Mechanism](#idle-daemon-reconnection-mechanism)
- [Shared DB Client Pattern in Admin Controller](#shared-db-client-pattern-in-admin-controller)
- [Context-Aware Rebalance Goroutine Management](#context-aware-rebalance-goroutine-management)
- [Code Style and Linting](#code-style-and-linting)
- [Code Generation](#code-generation)
- [Adding New Features](#adding-new-features)
- [Debugging](#debugging)

## Development Environment Setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Go | 1.26+ | Build and test |
| Docker | 20+ | Container images, test environment |
| kubectl | 1.26+ | Kubernetes interaction |
| Helm | 3.x | Chart deployment |
| golangci-lint | latest | Code linting |
| controller-gen | v0.17+ | CRD and deepcopy generation |

### Install Development Tools

```bash
# Install golangci-lint
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8

# Install controller-gen
go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.3

# Install govulncheck
go install golang.org/x/vuln/cmd/govulncheck@latest
```

### Clone and Build

```bash
git clone https://github.com/cloudberry-contrib/cloudberry-k8s.git
cd cloudberry-k8s

# Download dependencies
go mod download

# Verify the build
make build

# Run linter
make lint

# Run tests
make test
```

### Local Kubernetes Cluster

For local development, use kind or minikube:

```bash
# Create a kind cluster
kind create cluster --name cloudberry-dev

# Build the operator image and load it into kind
make docker-build
kind load docker-image cloudberry-operator:latest --name cloudberry-dev

# Install the operator via Helm
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system --create-namespace

# Create the test namespace
kubectl create namespace cloudberry-test

# Deploy the sample cluster (uses postgres:16, 2 segments, minimal resources)
kubectl apply -f deploy/helm/cloudberry-operator/config/samples/cloudberrycluster-sample.yaml

# Verify pods are running
kubectl get pods -n cloudberry-test
# Expected output:
# NAME                                    READY   STATUS    RESTARTS   AGE
# cloudberry-sample-coordinator-0         1/1     Running   0          60s
# cloudberry-sample-segment-primary-0     1/1     Running   0          60s
# cloudberry-sample-segment-primary-1     1/1     Running   0          60s

# View cluster status
kubectl get cloudberryclusters -n cloudberry-test
```

The sample CR (`cloudberrycluster-sample.yaml`) uses `postgres:16` with reduced resources suitable for local development: 100m CPU / 256Mi memory requests, 5Gi storage, and 2 segments with no standby or mirroring.

### Docker Compose Test Environment

The project includes a Docker Compose setup with 9 services for integration testing:

| Service | Port(s) | Purpose |
|---------|---------|---------|
| Vault | 8200 | PKI for mTLS certificates, secrets management |
| Keycloak | 8090/8091 | OIDC provider for authentication testing |
| MinIO | 9000/9001 | S3-compatible storage |
| Kafka | 9094 | Event streaming (KRaft mode) |
| RabbitMQ | 5672/15672 | Message queue |
| VictoriaMetrics | 8428 | Metrics storage |
| Grafana | 3000 | Dashboards |
| Tempo | 3200/4317/4318 | Distributed tracing (OTLP receivers) |
| Keycloak DB | (internal) | PostgreSQL backend for Keycloak |

```bash
# Start all test services
make test-env-up
# or: docker compose -f test/docker-compose/docker-compose.yml up -d

# Run setup scripts (configures Vault PKI, Keycloak realm, MinIO buckets, Kafka topics, RabbitMQ queues, Hive/HDFS warehouse)
make test-env-setup

# Run integration tests
make test-integration

# Tear down
make test-env-down
```

**Setup order:**

1. `docker compose up -d` — start all services
2. Wait for Vault and Keycloak to be ready (health checks)
3. `scripts/setup-vault.sh` — configures PKI engine, issues certificates
4. `scripts/setup-vault-k8s-auth.sh` — configures Vault Kubernetes auth + PKI for the operator (required before deploying with `webhook.certSource=vault-pki`)
5. `scripts/setup-keycloak.sh` — creates realm, clients for service-to-service auth
6. `scripts/setup-minio.sh` — creates test buckets
7. `scripts/setup-kafka.sh` — creates test topics
8. `scripts/setup-rabbitmq.sh` — creates test queues
9. `scripts/setup-hive-hdfs.sh` — creates the HDFS warehouse directories and a sample Hive table (for PXF `hdfs:*` / `hive:*` data-loading tests)

The setup scripts (`test/docker-compose/scripts/`) configure:
- **Vault**: Enables the PKI secrets engine, creates policies and Kubernetes auth roles
- **Vault Kubernetes auth** (`setup-vault-k8s-auth.sh`): Enables `auth/kubernetes`; creates a token-reviewer ServiceAccount (`system:auth-delegator`) plus a long-lived token Secret in `cloudberry-test`; configures `auth/kubernetes` with `kubernetes_host=https://kubernetes.docker.internal:6443`; creates the `cloudberry-operator` Vault policy (`pki/issue`, `pki/sign`, `pki/cert/ca` read, `secret/data/cloudberry*` read), the `auth/kubernetes/role/cloudberry-operator` role (bound to SA `cloudberry-operator` in `cloudberry-test`), the PKI role `pki/roles/cloudberry-operator`, and a placeholder KV secret at `secret/data/cloudberry`. The script is idempotent and wired into `make test-env-setup`.

> **Vault Kubernetes Auth (docker-desktop) — `kubernetes.docker.internal` gotcha**: `setup-vault-k8s-auth.sh` must point Vault at `kubernetes_host=https://kubernetes.docker.internal:6443`, **not** `host.docker.internal`. The Docker Desktop API-server serving certificate only includes `kubernetes.docker.internal` in its SANs; using `host.docker.internal` makes Vault's `TokenReview` TLS hostname verification fail and operator login returns `403 permission denied`.
- **Keycloak**: Creates the `cloudberry` realm, `cloudberry-operator` client, and test users with roles
- **MinIO**: Creates S3-compatible test buckets for backup testing
- **Kafka**: Creates test topics for event streaming
- **RabbitMQ**: Creates test queues for message processing
- **Hive + HDFS**: Single-node HDFS (NameNode `hdfs://namenode:8020`, DataNode) plus a Hive Metastore (`thrift://hive-metastore:9083`, Postgres-backed) and HiveServer2 (`jdbc:hive2://127.0.0.1:10000`). Endpoints match the PXF server config in `specifications/12-data-loading-spec.md`; `setup-hive-hdfs.sh` creates `/user/hive/warehouse` + `/data-lake` in HDFS and seeds a sample `warehouse.fact_sales` ORC table for PXF `hdfs:*` / `hive:*` external-table tests

### Monitoring Stack Deployment

The project includes monitoring configurations in the `monitoring/` directory, Helm charts under `test/monitoring/`, and the Docker Compose test environment:

- **Grafana dashboards**: Pre-built dashboards for operator metrics in `monitoring/grafana/`. They cover all exported metrics — operator metrics, the cloudberry-query-exporter resource-group/IO/spill/skew metrics, and the postgres-exporter custom SQL metrics. Publish them with `make grafana-publish`
- **vmagent** (`test/monitoring/vmagent`): VictoriaMetrics agent that scrapes Prometheus-compatible metrics and remote-writes to VictoriaMetrics (`host.docker.internal:8428`)
- **vector** (`test/monitoring/vector`): Vector tails the `kubernetes_logs` source and ships logs to VictoriaLogs (`host.docker.internal:9428`)
- **otel-collector** (`test/monitoring/otel-collector`): OpenTelemetry Collector for distributed tracing (repo chart; renders service as `otel-collector`)
- **node-exporter** (`test/monitoring/node-exporter`): Node-level metrics
- **kube-state-metrics** (`test/monitoring/kube-state-metrics`): Kubernetes object-state metrics (`kube_job_*`, `kube_pod_init_container_status_*`, `kube_deployment_status_replicas_available`) scraped into VictoriaMetrics — added in Scenario 104 so pre-load health-check failures (failed data-load Jobs / failed `dataload-healthcheck` init containers / gpfdist deployment readiness) are observable in metrics, not just `kubectl`

**Local development (Docker Compose):**

The Docker Compose test environment includes VictoriaMetrics (port 8428), Grafana (port 3000), and Tempo (ports 3200/4317/4318) pre-configured:

```bash
# Start the full test environment including monitoring services
make test-env-up

# Access Grafana at http://localhost:3000
# Access VictoriaMetrics at http://localhost:8428
# OTLP receivers at localhost:4317 (gRPC) and localhost:4318 (HTTP)
```

**Kubernetes deployment:**

To deploy the monitoring stack (vmagent + vector + otel-collector + node-exporter) into the `cloudberry-test` namespace alongside the operator:

```bash
# Deploy the vmagent/vector/otel-collector/node-exporter charts to cloudberry-test
make monitoring-deploy

# Check status / remove
make monitoring-status
make monitoring-undeploy

# Publish the Grafana dashboards to the test-environment Grafana
make grafana-publish
```

Or deploy the operator itself with metrics and telemetry enabled:

```bash
# Deploy the operator with metrics and telemetry enabled
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set metrics.enabled=true \
  --set serviceMonitor.enabled=true \
  --set telemetry.enabled=true \
  --set telemetry.otlpEndpoint=otel-collector:4317 \
  --set telemetry.otlpInsecure=true
```

## Project Structure

```
cloudberry-k8s/
├── api/
│   └── v1alpha1/
│       ├── doc.go                    # Package documentation
│       ├── groupversion_info.go      # SchemeBuilder and GroupVersion
│       ├── types.go                  # CRD Go types (CloudberryCluster)
│       ├── types_test.go             # Type tests
│       └── zz_generated.deepcopy.go  # Generated DeepCopy methods
│
├── cmd/
│   ├── operator/
│   │   └── main.go                   # Operator entry point
│   └── cloudberry-ctl/
│       └── main.go                   # CLI entry point
│
├── internal/
│   ├── api/
│   │   ├── server.go                 # REST API server, routes, input validation
│   │   ├── ratelimit.go              # Per-IP token bucket rate limiter
│   │   ├── ratelimit_test.go         # Rate limiter tests
│   │   └── server_test.go            # API server tests
│   │
│   ├── auth/
│   │   ├── types.go                  # Identity, Provider, PermissionLevel
│   │   ├── basic.go                  # Basic auth provider
│   │   ├── basic_test.go             # Basic auth tests
│   │   ├── oidc.go                   # OIDC/JWT auth provider
│   │   ├── oidc_test.go              # OIDC tests
│   │   ├── middleware.go             # Auth and permission middleware
│   │   └── middleware_test.go        # Middleware tests
│   │
│   ├── builder/
│   │   ├── builder.go                # K8s resource builders
│   │   └── builder_test.go           # Builder tests
│   │
│   ├── config/
│   │   ├── config.go                 # Operator configuration
│   │   └── config_test.go            # Config tests
│   │
│   ├── controller/
│   │   ├── cluster_controller.go     # Cluster lifecycle reconciler
│   │   ├── cluster_controller_test.go
│   │   ├── ha_controller.go          # HA reconciler (FTS, failover)
│   │   ├── ha_controller_test.go
│   │   ├── auth_controller.go        # Auth config reconciler
│   │   ├── auth_controller_test.go
│   │   ├── admin_controller.go       # Admin/config reconciler
│   │   └── admin_controller_test.go
│   │
│   ├── ctl/
│   │   ├── client.go                 # Operator API HTTP client for cloudberry-ctl
│   │   ├── client_test.go            # Client tests
│   │   ├── output.go                 # Output formatting (table, JSON, YAML)
│   │   └── output_test.go            # Output tests
│   │
│   ├── db/
│   │   ├── client.go                 # Database client (pgx) with real SQL queries
│   │   ├── factory.go                # DBClientFactory — creates clients from cluster info
│   │   ├── factory_test.go           # Factory tests
│   │   └── client_test.go            # DB client tests
│   │
│   ├── metrics/
│   │   ├── metrics.go                # Prometheus metrics
│   │   └── metrics_test.go           # Metrics tests
│   │
│   ├── telemetry/
│   │   ├── telemetry.go              # OpenTelemetry tracing
│   │   └── telemetry_test.go         # Telemetry tests
│   │
│   ├── util/
│   │   ├── conditions.go             # K8s condition helpers
│   │   ├── constants.go              # Shared constants
│   │   ├── errors.go                 # Custom error types
│   │   ├── hash.go                   # Hash computation
│   │   ├── logging.go                # Structured logging
│   │   ├── names.go                  # Resource name builders
│   │   ├── ptr.go                    # Pointer helpers
│   │   ├── retry.go                  # Retry with backoff
│   │   ├── strings.go                # String utilities
│   │   └── *_test.go                 # Tests for each file
│   │
│   ├── vault/
│   │   ├── vault.go                  # Vault client
│   │   └── vault_test.go             # Vault tests
│   │
│   ├── certmanager/
│   │   ├── certmanager.go            # Certificate manager interface and lifecycle
│   │   ├── certmanager_test.go       # Certificate manager tests
│   │   ├── selfsigned.go             # Self-signed CA and server cert generation
│   │   ├── selfsigned_test.go        # Self-signed cert tests
│   │   └── vaultpki.go              # Vault PKI certificate issuance
│   │
│   └── webhook/
│       ├── validating.go             # Validating admission webhook (with cross-namespace duplicate detection)
│       ├── validating_test.go
│       ├── mutating.go               # Mutating admission webhook
│       └── mutating_test.go
│
├── deploy/
│   ├── helm/
│   │   └── cloudberry-operator/      # Helm chart
│   │       ├── Chart.yaml
│   │       ├── values.yaml
│   │       ├── values.schema.json
│   │       ├── crds/                 # CRD manifests
│   │       ├── templates/            # K8s resource templates
│   │       └── config/samples/       # Sample CRs
│   └── docker/
│
├── test/
│   ├── e2e/                          # End-to-end tests
│   │   ├── suite_test.go
│   │   ├── cluster_e2e_test.go
│   │   ├── ha_e2e_test.go
│   │   ├── auth_e2e_test.go
│   │   ├── scenario49_ctl_auth_e2e_test.go
│   │   ├── scenario50_auditing_e2e_test.go
│   │   ├── scenario51_security_headers_e2e_test.go
│   │   └── scenario52_negative_edge_cases_e2e_test.go
│   ├── functional/                   # Functional tests
│   │   ├── cluster_lifecycle_test.go
│   │   ├── config_management_test.go
│   │   ├── ha_operations_test.go
│   │   ├── auth_config_test.go
│   │   ├── maintenance_test.go
│   │   ├── scenario5_session_management_test.go
│   │   ├── scenario6_resource_management_test.go
│   │   ├── scenario7_load_data_test.go
│   │   ├── scenario9_scalein_test.go
│   │   ├── scenario12_scalein_confirmation_test.go
│   │   ├── scenario13_pv_expansion_test.go
│   │   ├── scenario15_error_handling_test.go
│   │   ├── scenario16_deletion_test.go
│   │   ├── scenario19_enable_mirroring_test.go
│   │   ├── scenario20_automatic_failover_test.go
│   │   ├── scenario25_bootstrap_workload_test.go
│   │   ├── scenario49_ctl_auth_test.go
│   │   ├── scenario50_auditing_test.go
│   │   ├── scenario51_security_headers_test.go
│   │   ├── scenario52_negative_edge_cases_test.go
│   │   └── webhook_test.go
│   ├── scenarios/                    # SQL/shell scripts for test scenarios
│   │   ├── scenario7_load_data.sql   # Test data loading (5 tables, ~1.45M rows)
│   │   └── scenario7_load_data.sh    # Runner script (kubectl cp + psql)
│   ├── integration/                  # Integration tests
│   │   ├── api_integration_test.go
│   │   ├── auth_flow_test.go
│   │   ├── keycloak_integration_test.go
│   │   └── vault_integration_test.go
│   ├── cases/
│   │   └── test_cases.go             # Shared test case definitions
│   └── testutil/
│       ├── env.go                    # Test environment helpers
│       ├── fixtures.go               # Test fixtures
│       ├── k8s.go                    # K8s test helpers
│       ├── keycloak.go               # Keycloak test helpers
│       └── vault.go                  # Vault test helpers
│
├── specifications/                   # Design specifications
├── .github/workflows/                # CI/CD pipelines
├── Dockerfile                        # Operator container image
├── Dockerfile.ctl                    # CLI container image
├── Makefile                          # Build automation
├── .golangci.yml                     # Linter configuration
├── go.mod                            # Go module definition
└── go.sum                            # Dependency checksums
```

### Package Responsibilities

| Package | Responsibility | Dependencies |
|---------|---------------|-------------|
| `api/v1alpha1` | CRD types, validation markers, deepcopy | k8s.io/apimachinery |
| `internal/config` | Operator configuration loading | viper |
| `internal/util` | Shared utilities (retry, names, conditions) | — |
| `internal/metrics` | Prometheus metrics registration and `NoopRecorder` for testing | prometheus/client_golang |
| `internal/telemetry` | OTLP tracing setup | opentelemetry-go |
| `internal/vault` | Vault client with retry | vault/api, internal/util |
| `internal/auth` | Auth providers and middleware | go-oidc, internal/vault |
| `internal/ctl` | Operator API HTTP client for CLI | net/http |
| `internal/db` | Database operations, client factory, and shared `DBClientFactory` interface | pgx/v5 |
| `internal/builder` | K8s resource construction | api/v1alpha1, internal/util |
| `internal/controller` | Reconciliation controllers | All internal packages |
| `internal/api` | REST API server | internal/auth, internal/metrics |
| `internal/certmanager` | Webhook and cluster TLS cert lifecycle (Vault PKI / self-signed) | internal/vault, k8s client |
| `internal/webhook` | Admission webhooks (with cross-namespace duplicate detection) | api/v1alpha1 |
| `internal/cron` | Single shared 5-field cron engine (next-run computation for the API, schedule validation for the webhook) | — |
| `internal/httpjson` | Shared JSON response / error-envelope encoder (`{"error":{"code","message"}}`) used by the API server and auth middleware | — |
| `internal/idle` | Idle-session daemon (scan loop, reconnection, observability) | internal/db, internal/metrics |

### Key Internal Helpers

The codebase uses several internal helper functions that are important to understand when contributing:

| Helper | Package | Purpose |
|--------|---------|---------|
| `resolvePort()` | `internal/builder` | Returns the coordinator port from the cluster spec, falling back to the default (5432). Used by all resource builder functions to avoid duplicating port resolution logic |
| `sanitizeDistKey()` | `internal/db` | Validates comma-separated distribution key column names against a SQL identifier regex. Prevents SQL injection in `CREATE TABLE ... DISTRIBUTED BY` and `ALTER TABLE ... SET DISTRIBUTED BY` statements |
| `notImplemented()` | `cmd/cloudberry-ctl` | Returns a standardized `"command %q is not yet implemented"` error for stub CLI commands. All unimplemented commands use this helper to provide consistent error messages |
| `removeAnnotationPatch()` | `internal/controller` | Removes an annotation from a cluster using a `MergePatch` instead of a full update. This avoids race conditions when multiple controllers modify the same resource concurrently |
| `patchStatus()` | `internal/controller` | Patches the status subresource using `Status().Patch()` with `MergePatchType`. Prevents status clobbering between concurrent controllers |
| `patchFTSStatus()` | `internal/controller` | Patches FTS-related status fields with a manually constructed MergePatch. Handles `omitempty` on `FailedSegments` by always including the field explicitly, even when empty |
| `checkDuplicateName()` | `internal/webhook` | Lists all `CloudberryCluster` resources across namespaces and rejects creation if the same name exists in a different namespace |
| `buildConnectionString()` | `internal/db` | Constructs a PostgreSQL connection string using the pgx native config builder (not manual string escaping). Returns an error (instead of falling back to a default) if the connection string cannot be parsed |
| `GenerateRandomPassword()` | `internal/util` | Generates a cryptographically secure random password including special characters (`!@#$%^&*()-_=+`). Used for auto-generated admin passwords |
| `NewNoopRecorder()` | `internal/metrics` | Creates a `NoopRecorder` instance — a no-op implementation of the `Recorder` interface where all methods do nothing. Used in unit tests where metric recording is not needed, avoiding nil pointer dereferences without requiring a full Prometheus registry |

## Building

### Build Commands

```bash
# Build both operator and CLI
make build

# Build operator only
make build-operator

# Build CLI only
make build-ctl

# Build Docker images (operator + CLI)
make docker-build

# Build all Docker images (operator, CLI, and Cloudberry DB)
make docker-build-all

# Build operator Docker image only
make docker-build-operator

# Build CLI Docker image only
make docker-build-ctl

# Build Apache Cloudberry database image (compiles from source)
make docker-build-cloudberry

# Push Docker images
make docker-push
```

### Building the Cloudberry DB Image

The project includes `Dockerfile.cloudberry`, a multi-stage build that compiles Apache Cloudberry 2.1.0 from source on Rocky Linux 9.6. The resulting image (`cloudberrydb/cloudberry:2.1.0`) is used by the operator to run real Cloudberry clusters.

```bash
# Build the Cloudberry DB image (takes 15-30 minutes on first build)
make docker-build-cloudberry

# The image is tagged as cloudberrydb/cloudberry:2.1.0
docker images | grep cloudberry
```

**Image details:**
- **Base**: Rocky Linux 9.6 (build stage) / Rocky Linux 9.6-minimal (runtime stage)
- **Source**: Apache Cloudberry 2.1.0-incubating
- **Platforms**: linux/amd64, linux/arm64
- **User**: `gpadmin` (non-root)
- **Entrypoint**: `hack/docker-entrypoint-cloudberry.sh`

**Entrypoint script** (`hack/docker-entrypoint-cloudberry.sh`):

The entrypoint handles initialization and startup for all Cloudberry roles:

| Role | `CLOUDBERRY_ROLE` | Behavior |
|------|-------------------|----------|
| Coordinator | `coordinator` | Runs in `dispatch` mode (`gp_role=dispatch`). Registers segments in `gp_segment_configuration` on first startup. Sets up streaming replication to standby. |
| Standby | `standby` | Connects to coordinator via `pg_basebackup` for initial sync, then runs as a streaming replica. |
| Primary segment | `primary` | Runs in `execute` mode (`gp_role=execute`). Content ID and DB ID are derived from the StatefulSet pod ordinal. |
| Mirror segment | `mirror` | Connects to the corresponding primary via `pg_basebackup`, then runs as a streaming replica of that primary. |

Key environment variables consumed by the entrypoint:

| Variable | Description | Default |
|----------|-------------|---------|
| `PGDATA` | Data directory path | `/data/pgdata` |
| `POSTGRES_PASSWORD` | Admin password for gpadmin | (required) |
| `CLOUDBERRY_ROLE` | Role: coordinator, standby, primary, mirror | `coordinator` |
| `CLOUDBERRY_CONTENT_ID` | Segment content ID (-1 for coordinator) | Derived from pod name |
| `CLOUDBERRY_COORDINATOR_HOST` | Hostname of the coordinator | `localhost` |
| `CLOUDBERRY_SEGMENT_PORT` | Port for this segment | `5432` |
| `CLOUDBERRY_DB_ID` | Database ID for this segment | Derived from pod name |
| `CLOUDBERRY_SEGMENT_COUNT` | Total number of primary segments | `4` |

**Segment registration**: On coordinator first startup, the entrypoint inserts rows into `gp_segment_configuration` for all segments (primaries and mirrors). This populates the Cloudberry catalog so the coordinator knows about all segments in the cluster.

### Build Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VERSION` | Version string (injected via ldflags) | Git tag or `dev` |
| `COMMIT` | Git commit hash (injected via ldflags) | `git rev-parse --short HEAD` |
| `BUILD_DATE` | Build timestamp (injected via ldflags) | UTC ISO 8601 |
| `IMG_OPERATOR` | Operator image name | `cloudberry-operator:latest` |
| `IMG_CTL` | CLI image name | `cloudberry-ctl:latest` |
| `CGO_ENABLED` | Enable CGO | `0` |
| `GOOS` | Target OS | Current OS |
| `GOARCH` | Target architecture | Current arch |

Version strings are injected at build time using Go ldflags:

```bash
-ldflags="-s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE)"
```

This ensures `cloudberry-ctl version` and operator startup logs display the correct build information.

> **Note**: The `Dockerfile.ctl` uses `-X main.version` (not `-X main.appVersion`) to match the Go variable declaration in `cmd/cloudberry-ctl/main.go`. This was corrected during the 2026-05-19 refactoring session.

```bash
# Cross-compile for Linux
GOOS=linux GOARCH=amd64 make build

# Build with custom version
VERSION=v0.2.0 make build

# Build with custom image name
IMG_OPERATOR=myregistry/cloudberry-operator:v0.2.0 make docker-build-operator
```

### Dockerfile

The operator uses a multi-stage Dockerfile:

1. **Builder stage**: `golang:1.26.4-alpine`, compiles with `-trimpath` and `-ldflags="-s -w -X main.version=... -X main.commit=... -X main.buildDate=..."`
2. **Runtime stage**: `gcr.io/distroless/static-debian12:nonroot` (minimal, non-root)

The final image is under 100MB and runs as user `65532` (nonroot). Version information is injected via build arguments (`VERSION`, `COMMIT`, `BUILD_DATE`) passed through Docker build args.

## Testing

### Test Strategy

The project uses four levels of testing:

| Level | Location | Tag | What It Tests |
|-------|----------|-----|---------------|
| **Unit** | `*_test.go` alongside source | (none) | Individual functions and methods |
| **Functional** | `test/functional/` | `functional` | Controller behavior with fake K8s client |
| **Integration** | `test/integration/` | `integration` | Real Vault, Keycloak, database connections |
| **E2E** | `test/e2e/` | `e2e` | Full operator in a real K8s cluster |

### Running Tests

```bash
# Unit tests
make test

# Unit tests with coverage report
make test-cover

# Functional tests
make test-functional

# Integration tests (requires Docker Compose services)
make test-env-up
make test-env-setup
make test-integration
make test-env-down

# End-to-end tests (requires K8s cluster with operator deployed)
make test-e2e

# All tests
make test-all
```

### Controller Test Scenarios

The controller tests cover four comprehensive scenarios that validate the operator's core functionality:

#### Scenario 1 — Full Cluster Bootstrap

Tests the complete cluster creation flow with all features enabled:

- **Setup**: Coordinator + standby + 4 primary segments + 4 mirrors, OIDC (Keycloak), Vault integration, all 4 config layers (cluster, coordinator, database, role)
- **Webhook validation**: Negative test verifying `segments.count=0` is rejected
- **Resources verified**: ConfigMaps (`postgresql.conf`, `pg_hba.conf`), Secrets, headless Services, StatefulSets with init containers, OrderedReady pod management
- **Status assertions**: `phase=Running`, `coordinatorReady=true`, `standbyReady=true`, `segmentsReady=4`, `mirroringStatus=InSync`
- **Metrics verified**: `cloudberry_cluster_info`, `cloudberry_coordinator_up`, `cloudberry_standby_up`, `cloudberry_segments_ready/total`, `cloudberry_mirroring_in_sync`, `cloudberry_connections_max`
- **Logging verified**: Structured JSON logging with `cluster`, `namespace`, `controller` fields

#### Scenario 2 — Configuration Hot-Reload and Rolling Restart

Tests the configuration change classification and rolling restart state machine:

- **Phase A (Reload-safe)**: Change `log_min_messages` → ConfigMap updated, no pod restarts, `ConfigApplied=True/ConfigReloaded`
- **Phase B (Restart-required)**: Change `shared_buffers` and `max_connections` → ConfigMap updated, rolling restart triggered
- **Rolling restart order**: mirrors → primaries → standby → coordinator
- **Parameter classification**: Validates the `restartRequiredParams` map
- **Status conditions**: `ConfigApplied=False/RestartRequired` during restart, `ConfigApplied=True/ConfigAppliedAfterRestart` after
- **Events verified**: `ConfigReloaded`, `RollingRestartStarted`, `RollingRestartCompleted`
- **Annotation tracking**: `avsoft.io/rolling-restart` with JSON state
- **Metrics verified**: `cloudberry_config_reload_total` incremented

#### Scenario 3 — Stop / Start Modes

Tests all cluster lifecycle transitions:

- **3a**: Smart stop (`stop`) → `Stopped` (0 pods) → Normal start (`start`) → `Running` (10 pods)
- **3b**: Fast stop (`stop-fast`) → `Stopped` → Restricted start (`start-restricted`) → `Restricted` (coordinator only)
- **3c**: Immediate stop (`stop-immediate`) → `Stopped` → Maintenance start (`start-maintenance`) → `Maintenance` (coordinator only)
- **3d**: Restart (`restart`) → `Stopping` → `Initializing` → `Running`
- **Scale-down order**: mirrors → primaries → standby → coordinator
- **Scale-up**: Full reconciliation restores all StatefulSets
- **Events verified**: `Stopping`, `Stopped`, `Starting`, `Started`, `Restarting`, `Restarted`
- **Annotation handling**: Action annotations checked BEFORE generation skip

#### Scenario 4 — Maintenance Operations

Tests the maintenance Job creation pipeline:

- **Builder method**: `BuildMaintenanceJob` added to `ResourceBuilder` interface
- **Job creation**: Creates `batchv1.Job` with `psql` command connecting to coordinator
- **Operations tested**: `vacuum`, `vacuum-analyze`, `vacuum-full`, `analyze`, `reindex`
- **Job properties**: `BackoffLimit=1`, `TTLSecondsAfterFinished=3600`, `RestartPolicy=Never`
- **Authentication**: `PGPASSWORD` from admin password Secret
- **Error handling**: Unknown operations emit `MaintenanceUnknown` warning event
- **Events verified**: `MaintenanceStarted` with job name

#### Scenario 5 — Session Management

Tests the session management API endpoints (list sessions, cancel query, terminate session) via the API server handlers with a mock `DBClientFactory`:

- **List sessions**: Verifies that `handleListSessions` queries `pg_stat_activity` via `dbClient.ListSessions()` and returns session data (PID, username, application, clientAddress, state, query, queryStart, duration)
- **Cancel query**: Verifies that `handleCancelQuery` calls `pg_cancel_backend()` via `dbClient.CancelQuery()` and returns the result
- **Terminate session**: Verifies that `handleTerminateSession` calls `pg_terminate_backend()` via `dbClient.TerminateSession()` and returns the result
- **PID validation**: Invalid PIDs (zero, negative, non-numeric) return `400 Bad Request` with `INVALID_REQUEST` error code
- **Graceful degradation**: When no `DBClientFactory` is configured, list returns empty sessions with `"database connection not available"` message
- **DB connection errors**: When `dbFactory.NewClient()` fails, returns `503 DB_UNAVAILABLE`
- **Query errors**: When the database operation fails, returns `500 INTERNAL_ERROR`
- **Cluster not found**: When the cluster does not exist, returns `404 CLUSTER_NOT_FOUND`
- **Client lifecycle**: Verifies that the database client is closed after each request via `defer dbClient.Close()`
- **12 test cases** covering all success paths, error paths, and edge cases

#### Scenario 6 — Resource Management

Tests the resource group management API endpoints (create, list, assign, delete) via the API server handlers with a mock `DBClientFactory`:

- **Create resource group**: Verifies that `handleCreateResourceGroup` calls `dbClient.CreateResourceGroup()` with the correct `ResourceGroupOptions` (name, concurrency, cpuMaxPercent, memoryLimit) and returns `201 Created` with the group details
- **Assign role to resource group**: Verifies that `handleAssignResourceGroup` calls `dbClient.AssignRoleResourceGroup()` with the correct role and group name, and returns `200 OK` with assignment confirmation
- **List resource groups**: Verifies that `handleListResourceGroups` queries `dbClient.ListResourceGroups()` and returns the full list with `total` count. Falls back to CRD spec when the database is unavailable
- **Delete resource group**: Verifies that `handleDeleteResourceGroup` calls `dbClient.DropResourceGroup()` with the correct group name and returns `200 OK` with deletion confirmation
- **Graceful degradation**: When no `DBClientFactory` is configured, create returns `201` with a `"pending"` message
- **Validation errors**: Empty resource group name returns `400 INVALID_REQUEST`; empty role on assign returns `400 INVALID_REQUEST`
- **Database errors**: When `CreateResourceGroup`, `DropResourceGroup`, or `AssignRoleResourceGroup` fails, returns `500 INTERNAL_ERROR`
- **Cluster not found**: When the cluster does not exist, returns `404 CLUSTER_NOT_FOUND`
- **Client lifecycle**: Verifies that the database client is closed after each request via `defer dbClient.Close()`
- **11 test cases** covering all success paths, error paths, and edge cases

#### Scenario 7 — Load Data for Subsequent Scenarios

Prepares a realistic test dataset in the `mydb` database for use by subsequent scenarios (scale, rebalance, performance). The scenario creates tables with different distribution strategies, loads data with intentional skew, and sets up exclusion patterns.

- **SQL script**: `test/scenarios/scenario7_load_data.sql`
- **Shell runner**: `test/scenarios/scenario7_load_data.sh`
- **Functional tests**: `test/functional/scenario7_load_data_test.go`

**Data loaded:**

| Table | Rows | Distribution | Notes |
|-------|------|-------------|-------|
| `orders` | 1,000,000 | hash (`customer_id`) | 500K existing + 500K Pareto-skewed (80% to 20% of customers) |
| `logs` | 200,000 | random | Log entries with JSONB metadata |
| `customers` | 100,000 | hash (`id`) | Pre-existing from Scenario 6 |
| `audit_log` | 100,000 | hash (`id`) | Marked `exclude_from_rebalance=true` |
| `temp_staging` | 50,000 | hash (`id`) | Matches `temp_*` exclusion pattern |

**Total**: ~1,450,000 rows, ~218 MB, 16 indexes

**How to run:**

```bash
# Run the data loading script against a running cluster
bash test/scenarios/scenario7_load_data.sh

# Override namespace and cluster name
NAMESPACE=my-ns CLUSTER=my-cluster bash test/scenarios/scenario7_load_data.sh
```

The shell script copies the SQL file to the coordinator pod via `kubectl cp`, executes it with `psql -U gpadmin -d mydb`, and verifies table sizes, row counts, index counts, and total database size.

**Functional tests (8 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario7_DataSchemaDefinition` | Table references, required columns, and indexes in the SQL script |
| `TestScenario7_TableDistributionComments` | `COMMENT ON TABLE` metadata for distribution type, key, and exclusion flags |
| `TestScenario7_ExpectedTableCount` | Exactly 5 tables defined; `ANALYZE` called for each |
| `TestScenario7_SkewedDataDistribution` | Pareto distribution logic: `random() < 0.8` split, 20K/80K customer targeting |
| `TestScenario7_SQLScriptStructure` | Database connection (`\c mydb`), `GRANT` statements, `CREATE TABLE IF NOT EXISTS`, index count |
| `TestScenario7_InsertRowCounts` | `generate_series` counts: 500K orders, 200K logs, 100K audit, 50K staging |
| `TestScenario7_TempStagingExclusion` | `temporary_staging=true` flag and `temp_` prefix pattern |
| `TestScenario7_ShellScriptExists` | Shell script structure: shebang, strict mode, `kubectl cp`/`exec`, verification queries |

#### Scenario 8 — Scale-Out with Mirroring

Tests the scale-out flow for a mirrored cluster, including scale detection, StatefulSet updates, redistribution Job creation, and phase transitions.

- **Scale detection**: `reconcileSegments()` compares `spec.segments.count` against the current primary StatefulSet replicas. When the desired count exceeds the current count, `handleScaleOut()` is invoked
- **Phase transitions**: `Running` → `Scaling` → `Running` (verified via status assertions)
- **StatefulSet updates**: Primary StatefulSet replicas updated from 4 to 6; mirror StatefulSet replicas updated from 4 to 6
- **Pod count**: Total pods increase from 10 to 14 (6 primary + 6 mirror + coordinator + standby)
- **Redistribution Job**: A `{cluster}-maintenance-{timestamp}` Job is created with the `redistribute` operation
- **Events verified**: `ScaleOutStarted` (emitted when scaling begins), `ScaleOutCompleted` (emitted when all pods are ready)
- **Conditions verified**: `DataRedistribution` condition transitions through `ScaleOutStarted` → `InProgress` → `Completed`
- **Metrics verified**: `cloudberry_segments_total=6`, `cloudberry_segments_ready=6`, `cloudberry_scale_operations_total=1`
- **Scale status API**: `GET /clusters/{name}/scale/status` returns scaling state, segment readiness, and redistribution condition
- **CLI command**: `cloudberry-ctl cluster scale-status --cluster <name>` calls the scale status API
- **No-op test**: Verifying that patching `segments.count` to the same value does not trigger a scale-out or emit `ScaleOutStarted`
- **Functional tests**: `test/functional/scenario8_scaleout_test.go`

**Live verification results** (from a running cluster):
- Patched `segments.count` from 4 to 6
- Phase: `Running` → `Scaling` → `Running` (40 seconds)
- Primary StatefulSet: 4/4 → 6/6
- Mirror StatefulSet: 4/4 → 6/6
- Total pods: 10 → 14

#### Scenario 9 — Scale-In with Both PVC Policies

Tests the scale-in flow for a mirrored cluster, including safety checks, StatefulSet scale-down, PVC cleanup, and phase transitions.

- **Scale detection**: `reconcileSegments()` compares `spec.segments.count` against the current primary StatefulSet replicas. When the desired count is less than the current count, `handleScaleIn()` is invoked
- **Safety check**: Scale-in by more than 50% requires the `avsoft.io/confirm-scale-in=true` annotation. Without it, a `ScaleInBlocked` warning event is emitted and the operation is skipped
- **Phase transitions**: `Running` → `Scaling` → `Running` (verified via status assertions)
- **StatefulSet updates**: Mirror StatefulSet scaled down first, then primary StatefulSet (mirrors first for safety)
- **PVC behavior (Retain)**: PVCs for removed segments are preserved; total PVC count remains unchanged
- **PVC behavior (Delete)**: `cleanupOrphanedPVCs()` deletes PVCs for removed segments; total PVC count decreases
- **Redistribution Job**: A `{cluster}-maintenance-{timestamp}` Job is created with the `redistribute` operation to move data off segments being removed
- **Events verified**: `ScaleInStarted` (when scaling begins), `ScaleInCompleted` (when all pods are ready), `ScaleInBlocked` (when >50% reduction lacks confirmation)
- **Conditions verified**: `DataRedistribution` condition transitions through `ScaleInStarted` → `InProgress` → `Completed`
- **Metrics verified**: `cloudberry_scale_operations_total{operation="scale-in"}=1`
- **Functional tests**: `test/functional/scenario9_scalein_test.go`

**Live verification results** (from a running cluster):
- Scenario 9a (Retain policy): Scaled from 6 → 4 segments
  - Phase: `Running` → `Scaling` → `Running` (5 seconds)
  - Mirror StatefulSet: 6 → 4, Primary StatefulSet: 6 → 4
  - PVCs for segments 4, 5 preserved — 16 PVCs remain
  - Events: `ScaleInStarted`, `ScaleInCompleted`
  - Metrics: `scale_operations_total{scale-in}=1`

**Functional tests (5 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario9a_ScaleInRetain` | Scale-in with Retain policy: PVCs preserved for removed segments |
| `TestScenario9b_ScaleInDelete` | Scale-in with Delete policy: PVCs deleted for removed segments |
| `TestScenario9_ScaleInBlockedWithout50PercentConfirmation` | Scale-in >50% blocked without confirmation annotation |
| `TestScenario9_ScaleInWithConfirmationProceeds` | Scale-in >50% proceeds with `avsoft.io/confirm-scale-in=true` |
| `TestScenario9_ScaleMetricsRecorded` | `cloudberry_scale_operations_total{operation="scale-in"}` incremented |

#### Scenario 10 — Manual Segment Rebalancing

Tests the manual segment rebalancing flow, including rebalance configuration, Job creation, status API, CLI flags, events, conditions, and metrics.

- **RebalanceSpec**: `spec.segments.rebalance` with `skewThreshold`, `parallelism`, and `excludeTables` fields added to the CRD
- **handleRebalance()**: Full implementation in the HA controller — reads rebalance config from the cluster spec (with defaults: `skewThreshold=10`, `parallelism=2`), creates a maintenance Job with the `rebalance` operation, sets `DataRedistribution` conditions, and emits events
- **Annotation trigger**: Setting `avsoft.io/action=rebalance` triggers `handleRebalance()`, which removes the annotation via MergePatch
- **Rebalance Job**: Created via `BuildMaintenanceJob(cluster, "rebalance", timestamp)` — uses the `rebalance` entry in the maintenance SQL map (maps to `ANALYZE` in test mode, `gpexpand` redistribution in production Cloudberry)
- **Status API**: `GET /clusters/{name}/rebalance/status` returns the rebalance configuration and `DataRedistribution` condition
- **CLI flags**: `cloudberry-ctl ha rebalance --status` queries the status API; `--tables` sends a table list in the POST body
- **Events verified**: `RebalanceStarted` (with threshold and parallelism in message), `RebalanceCompleted`
- **Conditions verified**: `DataRedistribution` transitions through `RebalanceStarted` → `RebalanceCompleted`
- **Metrics verified**: `cloudberry_scale_operations_total{operation="rebalance"}` incremented; `cloudberry_data_skew_coefficient` gauge available
- **Default config test**: When `spec.segments.rebalance` is nil, defaults (`skewThreshold=10`, `parallelism=2`, no excludeTables) are applied
- **Functional tests**: `test/functional/scenario10_rebalance_test.go`

**Live verification results** (from a running cluster):
- Rebalance config set: `skewThreshold=10`, `parallelism=2`, `excludeTables=[audit_log, temp_*]`
- Annotation `avsoft.io/action=rebalance` triggered rebalance
- Job created and completed
- Events: `RebalanceStarted`, `RebalanceCompleted`
- DataRedistribution condition: `RebalanceCompleted`
- Metric: `cloudberry_scale_operations_total{operation="rebalance"}=1`
- API status endpoint returns rebalance config and condition

**Functional tests (5 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario10a_RebalanceViaAnnotation` | Annotation triggers rebalance: Job created, events emitted, conditions set, annotation removed |
| `TestScenario10b_RebalanceStatusAPI` | `GET /rebalance/status` returns config and DataRedistribution condition |
| `TestScenario10c_RebalanceSpecificTables` | `POST /rebalance` with tables body sets the rebalance annotation |
| `TestScenario10_RebalanceMetrics` | `RecordScaleOperation("rebalance")` called after rebalance completes |
| `TestScenario10_DefaultRebalanceConfig` | Nil `RebalanceSpec` uses defaults (threshold=10, parallelism=2) |

#### Scenario 11 — Scale-Out Failure and Rollback

Tests the scale-out failure handling flow, including pre-flight blocking, timeout detection, failure reporting, and the guard against false scale detection during restarts.

- **Pre-flight blocking (scale-out)**: When the cluster is not in `Running` phase, `handleScaleOut()` emits a `ScaleOutBlocked` warning event and skips the operation. The operator retries on the next reconciliation cycle
- **Pre-flight blocking (scale-in)**: When the cluster is not in `Running` phase, `handleScaleIn()` emits a `ScaleInBlocked` warning event and skips the operation
- **Scale timeout**: The `avsoft.io/scale-started` annotation tracks the operation start time. `checkScaleProgress()` detects when the elapsed time exceeds the 10-minute timeout
- **`handleScaleFailure()`**: Identifies unready segments from both primary and mirror StatefulSets, populates `status.failedSegments` with contentID, hostname, role, and status, sets the `ScaleOutFailed` condition to `True` with reason `SegmentsNotReady`, emits a `ScaleOutFailed` warning event, and removes the `avsoft.io/scale-started` annotation
- **No automatic rollback**: The cluster stays in `Scaling` phase after failure — manual intervention is required
- **Guard against false scale detection**: The `currentCount > 0` check in `reconcileSegments()` prevents false scale-out/scale-in detection when StatefulSets are being created during initial cluster bootstrap or restart
- **Annotation cleanup on success**: The `avsoft.io/scale-started` annotation is removed after a successful scale completion
- **Failed segments with mirroring**: When mirroring is enabled, both primary and mirror unready segments are reported in `status.failedSegments`
- **Functional tests**: `test/functional/scenario11_scaleout_failure_test.go`

**Events:**

| Event | Type | Description |
|-------|------|-------------|
| `ScaleOutBlocked` | Warning | Scale-out blocked because cluster is not in `Running` phase |
| `ScaleInBlocked` | Warning | Scale-in blocked because cluster is not in `Running` phase |
| `ScaleOutFailed` | Warning | Scale-out failed — segments not ready after 10-minute timeout |

**Conditions:**

| Condition | Status | Reason | Description |
|-----------|--------|--------|-------------|
| `ScaleOutFailed` | `True` | `SegmentsNotReady` | Scale-out failed with count and timeout info |

**Functional tests (7 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario11a_ScaleOutBlockedWhenNotRunning` | Scale-out blocked when cluster is in `Initializing` phase; `ScaleOutBlocked` event emitted |
| `TestScenario11b_ScaleOutTimeoutAndFailure` | Scale timeout triggers `handleScaleFailure()`; `ScaleOutFailed` condition and event; `failedSegments` populated |
| `TestScenario11c_ScaleInBlockedWhenNotRunning` | Scale-in blocked when cluster is not in `Running` phase; `ScaleInBlocked` event emitted |
| `TestScenario11d_ScaleStartedAnnotationCleanup` | `avsoft.io/scale-started` annotation removed after successful scale completion |
| `TestScenario11e_ScaleFailureWithMirroring` | Both primary and mirror unready segments reported in `failedSegments` |
| `TestScenario11f_FalseScaleDetectionGuard` | `currentCount > 0` guard prevents false scale detection during restarts |
| `TestScenario11g_NoAutoRollback` | Cluster stays in `Scaling` phase after failure — no automatic rollback |

#### Scenario 12 — Scale-In >50% Confirmation Requirement

Tests the safety mechanism that blocks scale-in operations reducing the segment count by more than 50%, requiring an explicit `avsoft.io/confirm-scale-in=true` annotation to proceed. Also verifies that the confirmation annotation is cleaned up after successful scale-in completion.

- **Confirmation check**: `handleScaleIn()` calculates `newCount / currentCount`. If the ratio is less than 0.5 (i.e., more than 50% reduction), the operation is blocked unless the `avsoft.io/confirm-scale-in=true` annotation is present
- **Blocked behavior**: When blocked, a `ScaleInBlocked` warning event is emitted with a message referencing the required annotation. The cluster phase stays `Running`, StatefulSet replicas remain unchanged, and no redistribution Job is created
- **Confirmed behavior**: With the annotation present, scale-in proceeds normally — phase transitions to `Scaling`, StatefulSets are updated, a redistribution Job is created, and `DataRedistribution` condition is set to `InProgress`
- **Annotation cleanup**: After successful scale-in completion, `completeScaleOperation()` calls `finaliseScaleIn()` → `cleanupScaleAnnotations()`, which removes both `avsoft.io/confirm-scale-in` and `avsoft.io/scale-started` annotations via MergePatch
- **Boundary test (exactly 50%)**: Scaling from 8→4 (exactly 50%) is NOT blocked — the check uses strict less-than (`< 0.5`), so 50% reductions proceed without confirmation
- **Boundary test (just over 50%)**: Scaling from 10→4 (60% reduction) IS blocked without the confirmation annotation
- **Refactored helpers**: `checkScaleProgress()` was refactored to extract `completeScaleOperation()`, `finaliseScaleIn()`, and `cleanupScaleAnnotations()` for reduced cyclomatic complexity
- **Functional tests**: `test/functional/scenario12_scalein_confirmation_test.go`

**Live verification results** (from a running cluster):
- 12a: Scale 8→3 without confirmation → `ScaleInBlocked` warning event, phase stays `Running`
- 12b: Scale 8→3 with `avsoft.io/confirm-scale-in=true` → proceeds, `ScaleInStarted`, `ScaleInCompleted`, segments=3

**Events:**

| Event | Type | Description |
|-------|------|-------------|
| `ScaleInBlocked` | Warning | Scale-in >50% blocked — annotation `avsoft.io/confirm-scale-in=true` required |
| `ScaleInStarted` | Normal | Scale-in proceeds after confirmation |
| `ScaleInCompleted` | Normal | Scale-in completed, confirmation annotation cleaned up |

**Functional tests (5 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario12a_ScaleInBlockedWithout50PercentConfirmation` | 8→3 (62.5% reduction) blocked without confirmation: `ScaleInBlocked` event, phase stays `Running`, StatefulSets unchanged, no Job created |
| `TestScenario12b_ScaleInProceedsWithConfirmation` | 8→3 proceeds with `avsoft.io/confirm-scale-in=true`: phase → `Scaling`, StatefulSets updated to 3, redistribution Job created, `DataRedistribution` condition set |
| `TestScenario12_ScaleInCompletionCleansConfirmation` | After scale-in completes: `confirm-scale-in` and `scale-started` annotations removed, phase → `Running`, `ScaleInCompleted` event emitted |
| `TestScenario12_ExactlyAt50PercentNotBlocked` | 8→4 (exactly 50%) NOT blocked: phase → `Scaling`, `ScaleInStarted` emitted, no `ScaleInBlocked` event |
| `TestScenario12_JustOver50PercentBlocked` | 10→4 (60% reduction) blocked without confirmation: `ScaleInBlocked` event, phase stays `Running`, StatefulSets unchanged |

#### Scenario 13 — Extend Persistent Volumes

Tests the online PVC expansion flow for coordinator, standby, and segment storage, including safety constraints (no shrink, PVC not found), StorageClass pre-flight checks, and the PVC listing API.

- **`reconcileStorageExpansion()`**: Detects PVC size increases by comparing `spec.*.storage.size` against actual PVC sizes. Patches PVCs for coordinator, standby, and segments independently
- **`expandPVCIfNeeded()`**: Compares desired vs current PVC size using `resource.Quantity.Cmp()`. Calls `storageClassSupportsExpansion()` before patching. Patches the PVC if the desired size is larger and the StorageClass allows it. Returns `(false, nil)` if the PVC is not found or the desired size is not larger (no shrink)
- **`storageClassSupportsExpansion()`**: Pre-flight check that looks up the StorageClass referenced by the PVC (via `spec.storageClassName` or the legacy `volume.beta.kubernetes.io/storage-class` annotation). Blocks expansion if `allowVolumeExpansion` is `false` or `nil`, or if the StorageClass is not found. Allows expansion when no StorageClass is specified (default SC) or on transient errors (fail-open). When blocked, logs a WARN with PVC name, StorageClass name, reason, and current/desired sizes — no error is returned
- **Three scopes**: Coordinator (single PVC), standby (single PVC), segments (all primary + mirror PVCs)
- **Safety**: Shrink requests are silently skipped (desired ≤ current). Missing PVCs are skipped without error. StorageClass without `allowVolumeExpansion: true` blocks expansion with a warning
- **PVC listing API**: `GET /clusters/{name}/storage/pvcs` lists all cluster PVCs with sizes, component labels, and binding status
- **Metric**: `cloudberry_pvc_size_bytes` gauge with `cluster`, `namespace`, `component` labels via `SetPVCSizeBytes()`
- **Condition**: `StorageExpanded` set to `True` with reason `PVCsExpanded` when any PVC is expanded
- **Event**: `StorageExpanded` (Normal) emitted when PVCs are expanded successfully
- **Functional tests**: `test/functional/scenario13_pv_expansion_test.go`

**Functional tests (8 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario13a_CoordinatorStorageExpansion` | Coordinator PVC expanded from 5Gi→10Gi; segment PVCs unchanged; `StorageExpanded` condition and event emitted |
| `TestScenario13b_StandbyStorageExpansion` | Standby PVC expanded from 5Gi→10Gi; coordinator and segment PVCs unchanged |
| `TestScenario13c_SegmentStorageExpansion` | All 6 segment PVCs (3 primary + 3 mirror) expanded from 5Gi→10Gi; coordinator PVC unchanged; `StorageExpanded` event emitted |
| `TestScenario13_NoExpansionWhenSizeUnchanged` | No PVCs modified when storage sizes match; no `StorageExpanded` event |
| `TestScenario13_NoShrinkAllowed` | PVCs remain at 10Gi when spec requests 3Gi (shrink); no `StorageExpanded` event |
| `TestScenario13_PVCNotFoundSkipped` | Reconciliation succeeds when PVCs don't exist; no `StorageExpanded` event |
| `TestScenario13_BlockedByStorageClass` | PVC with `allowVolumeExpansion=false` StorageClass → expansion blocked, PVC stays at original 5Gi size, no `StorageExpanded` event emitted, reconciliation succeeds without error |
| `TestScenario13_AllowedByStorageClass` | PVC with `allowVolumeExpansion=true` StorageClass → expansion proceeds, PVC expanded to 10Gi, `StorageExpanded` event emitted |

#### Scenario 14 — Cluster Upgrade with Rollback

Tests the cluster upgrade flow, including phase-by-phase image updates, post-upgrade verification, automatic rollback on timeout, pre-flight blocking, and no-op detection when the version is unchanged.

- **Upgrade detection**: `isUpgradeNeeded()` checks whether `spec.version != status.clusterVersion` or the `avsoft.io/upgrade` annotation is present
- **Pre-flight blocking**: When the cluster is not in `Running` phase, `handleUpgrade()` emits an `UpgradeBlocked` warning event and skips the operation (retries on next reconcile)
- **`handleUpgrade()`**: Captures the current image from the coordinator StatefulSet, stores rollback state (previousImage, previousVersion, phase, startedAt, phaseStartedAt) in the `avsoft.io/upgrade` annotation as JSON, sets phase to `Updating`, and emits `UpgradeStarted` event
- **Upgrade order**: mirrors → primaries → standby → coordinator → verify (least critical first)
- **`upgradePhase()`**: Generic phase handler that updates the StatefulSet image, checks readiness, and advances to the next phase. Skips phases for disabled components (mirroring, standby)
- **`verifyUpgrade()`**: Post-upgrade health check confirming coordinator and primary segments are ready
- **`completeUpgrade()`**: Sets phase to `Running`, updates `status.clusterVersion`, sets `UpgradeCompleted` condition, removes the upgrade annotation, and emits `UpgradeCompleted` event
- **Rollback**: Each phase has a 10-minute timeout. On timeout, `rollbackUpgrade()` reverts ALL StatefulSets to the previous image, restores the old version, sets `UpgradeFailed` condition with reason `RolledBack`, and emits `UpgradeRollback` warning event
- **No-op detection**: When `spec.version == status.clusterVersion` and no upgrade annotation exists, no upgrade is triggered
- **Functional tests**: `test/functional/scenario14_upgrade_test.go`

**Events:**

| Event | Type | Description |
|-------|------|-------------|
| `UpgradeStarted` | Normal | Upgrade initiated with previous and new version |
| `UpgradeCompleted` | Normal | Upgrade completed successfully |
| `UpgradeBlocked` | Warning | Upgrade blocked — cluster not in `Running` phase |
| `UpgradeRollback` | Warning | Upgrade rolled back due to phase timeout |

**Conditions:**

| Condition | Status | Reason | Description |
|-----------|--------|--------|-------------|
| `UpgradeCompleted` | `True` | `UpgradeSucceeded` | Upgrade completed successfully |
| `UpgradeFailed` | `True` | `RolledBack` | Upgrade failed and was rolled back |

**Functional tests (4 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario14_UpgradeHappyPath` | Full upgrade flow: `Running` → `Updating` → `Running`, all StatefulSets updated to new image, `UpgradeStarted` and `UpgradeCompleted` events, upgrade annotation set and removed, `clusterVersion` updated |
| `TestScenario14_UpgradeRollback` | Timeout triggers rollback: all StatefulSets reverted to old image, phase returns to `Running`, `clusterVersion` restored, `UpgradeFailed` condition set with reason `RolledBack`, `UpgradeRollback` event emitted, upgrade annotation removed |
| `TestScenario14_UpgradeBlockedWhenNotRunning` | Upgrade blocked when cluster is in `Stopped` phase: no upgrade annotation set, phase remains `Stopped` |
| `TestScenario14_NoUpgradeWhenVersionUnchanged` | No upgrade triggered when `spec.version == status.clusterVersion`: phase does not change to `Updating`, no `UpgradeStarted` event |

#### Scenario 15 — Error Handling, Retry, and Observability

Tests the error handling, retry with exponential backoff, metrics recording, telemetry spans, structured logging, and structured error types across the operator.

- **Webhook validation**: Rejects invalid parameters — `segments.count=0`, OIDC without `issuerURL`, OIDC without `clientID`, missing coordinator storage, missing segment storage
- **Reconcile error metrics**: `TrackingMetricsRecorder` verifies that `RecordReconcile(result="error")` is called on failures with the correct cluster name, namespace, and positive duration
- **Reconcile success metrics**: `RecordReconcile(result="success")` is called with positive duration after a healthy reconciliation
- **Retry with exponential backoff**: `RetryWithBackoff()` tested for fail-then-succeed, retry exhaustion (`ErrRetryExhausted`), context cancellation, exponential timing verification, and deadline expiry during backoff
- **Telemetry spans**: `SetSpanError()` records error status (`codes.Error`) and an `exception` event on OpenTelemetry spans. Nil error is safe (no error status set)
- **Structured error logging**: slog output captured via `bytes.Buffer` — verifies `cluster`, `namespace`, cluster name, namespace value, and `reconciliation` messages are present
- **Reconcile total and duration**: Multiple reconciliation cycles tracked correctly — `RecordReconcile` called at least once per cycle with positive duration
- **Context timeout handling**: `RetryWithBackoff` respects context timeout, handles pre-canceled context immediately, and propagates `context.DeadlineExceeded`
- **Pod deletion recovery**: Detects degraded state (`segmentsReady < segmentsTotal`) when a segment pod is deleted, then recovers to `Running` with `segmentsReady == segmentsTotal` when the pod returns
- **Prometheus metrics**: `cloudberry_reconcile_total`, `cloudberry_reconcile_errors_total`, and `cloudberry_reconcile_duration_seconds` are all registered and populated via `NewPrometheusRecorder` with a dedicated `prometheus.Registry`
- **Structured errors**: Error wrapping verified with `errors.Is()` for `ReconcileError` (wraps inner error), `ErrRetryExhausted` (from exhausted retries), `ValidationError` (wraps `ErrInvalidInput`), and `ClusterNotFoundError` (wraps `ErrNotFound`)

**Key test infrastructure:**

| Component | Purpose |
|-----------|---------|
| `TrackingMetricsRecorder` | Implements `metrics.Recorder`; captures `RecordReconcile` and `RecordScaleOperation` calls for assertion |
| `tracetest.InMemoryExporter` | In-memory OTEL span exporter for verifying span status, events, and error recording |
| `bytes.Buffer` + `util.NewLogger` | Captures structured log output for content verification |
| `webhookValidatorAdapter` | Replicates webhook validation logic for functional tests without importing internal webhook package |

**Functional tests (22 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario15_WebhookRejectsInvalidParams/segments_count_zero` | `segments.count=0` rejected with "must be >= 1" |
| `TestScenario15_WebhookRejectsInvalidParams/oidc_without_issuer_url` | OIDC without `issuerURL` rejected |
| `TestScenario15_WebhookRejectsInvalidParams/oidc_without_client_id` | OIDC without `clientID` rejected |
| `TestScenario15_ReconcileErrorMetrics` | `RecordReconcile(result="error")` called with correct cluster, namespace, positive duration |
| `TestScenario15_ReconcileSuccessMetrics` | `RecordReconcile(result="success")` called with correct cluster, namespace, positive duration |
| `TestScenario15_RetryWithExponentialBackoff/fails_then_succeeds` | 3 failures then success — 4 total attempts |
| `TestScenario15_RetryWithExponentialBackoff/always_fails_exhausts_retries` | All retries exhausted — `ErrRetryExhausted` returned |
| `TestScenario15_RetryWithExponentialBackoff/context_cancellation` | Pre-canceled context returns immediately |
| `TestScenario15_RetryWithExponentialBackoff/exponential_backoff_timing` | Second interval > half of first interval (exponential growth) |
| `TestScenario15_RetryWithExponentialBackoff/context_deadline_during_backoff` | Deadline exceeded during backoff sleep — fewer than max attempts |
| `TestScenario15_TelemetrySpanOnError` | Span has `codes.Error` status and `exception` event after `SetSpanError` |
| `TestScenario15_StructuredErrorLogging` | Log output contains `cluster`, `namespace`, cluster name, namespace value, `reconciliation` |
| `TestScenario15_ReconcileTotalAndDuration` | 3 reconciliation cycles produce ≥3 `RecordReconcile` calls with positive duration |
| `TestScenario15_ContextTimeoutHandling/retry_respects_timeout` | 50ms timeout stops retries with 100ms backoff |
| `TestScenario15_ContextTimeoutHandling/pre_canceled_context` | Pre-canceled context — function never called |
| `TestScenario15_ContextTimeoutHandling/deadline_exceeded_propagation` | Expired context propagated correctly |
| `TestScenario15_PodDeletionRecovery` | Degraded → recovered: `segmentsReady < segmentsTotal` then `segmentsReady == segmentsTotal` |
| `TestScenario15_PrometheusMetricsRecording/record_reconcile_success` | `cloudberry_reconcile_total` and `cloudberry_reconcile_duration_seconds` present in registry |
| `TestScenario15_PrometheusMetricsRecording/record_reconcile_error` | `cloudberry_reconcile_errors_total` present in registry |
| `TestScenario15_SetSpanErrorNilSafe` | `SetSpanError(span, nil)` does not set error status |
| `TestScenario15_WebhookValidatesStorage/missing_coordinator_storage` | Missing `coordinator.storage.size` rejected |
| `TestScenario15_WebhookValidatesStorage/missing_segment_storage` | Missing `segments.storage.size` rejected |
| `TestScenario15_StructuredErrors/reconcile_error_wrapping` | `ReconcileError` wraps inner error, `errors.Is` works |
| `TestScenario15_StructuredErrors/retry_exhausted_wrapping` | Exhausted retries wrap `ErrRetryExhausted` |
| `TestScenario15_StructuredErrors/validation_error` | `ValidationError` wraps `ErrInvalidInput` |
| `TestScenario15_StructuredErrors/cluster_not_found_error` | `ClusterNotFoundError` wraps `ErrNotFound` |

#### Scenario 16 — Cluster Deletion with Both Policies

Tests the cluster deletion flow with both `Retain` and `Delete` PVC policies, including backup-on-delete support, PVC event reporting, and phase transitions.

- **Deletion with Retain policy**: When `deletionPolicy=Retain` and `backupOnDelete=true`, the operator sets the phase to `Deleting`, creates a `backup-on-delete` maintenance Job, preserves all PVCs, emits `BackupOnDelete` and `PVCsRetained` events, removes the finalizer, and emits the `Deleted` event
- **Deletion with Delete policy**: When `deletionPolicy=Delete`, the operator deletes all cluster PVCs via `deletePVCs()`, emits the `PVCsDeleted` event, removes the finalizer, and emits the `Deleted` event. No `BackupOnDelete` or `PVCsRetained` events are emitted
- **No finalizer skips deletion**: When the cluster has no finalizer, the reconciler returns immediately without emitting any events. The cluster is deleted by Kubernetes garbage collection
- **Backup Job creation**: When `backupOnDelete=true`, a `batchv1.Job` is created with `backup-on-delete` in the name, the `avsoft.io/cluster` label set to the cluster name, and the `avsoft.io/operation` label set to `backup-on-delete`. The Job uses `BuildMaintenanceJob()` with the `backup-on-delete` operation (maps to `SELECT 1` in test mode, `gpbackup` in production Cloudberry)
- **Phase transition**: The cluster phase transitions from `Running` → `Deleting` during deletion. The `Deleting` event confirms the transition occurred. After finalizer removal, the cluster is deleted by Kubernetes
- **Functional tests**: `test/functional/scenario16_deletion_test.go`

**Live verification results** (from a running cluster):
- 16a (Retain + backupOnDelete): Cluster deleted, backup job created, 18 PVCs retained. Events: `Deleting` → `BackupOnDelete` → `PVCsRetained` → `Deleted`
- 16b (Delete): Cluster deleted, PVCs deleted (Terminating). Events: `Deleting` → `PVCsDeleted` → `Deleted`

**Events:**

| Event | Type | Description |
|-------|------|-------------|
| `Deleting` | Normal | Cluster deletion initiated |
| `BackupOnDelete` | Normal | Backup triggered before cluster deletion (when `backupOnDelete=true`) |
| `PVCsRetained` | Normal | PVCs preserved (when `deletionPolicy=Retain`) |
| `PVCsDeleted` | Normal | All PVCs deleted (when `deletionPolicy=Delete`) |
| `Deleted` | Normal | Cluster deletion completed, finalizer removed |

**Functional tests (5 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario16a_DeleteWithRetainPolicy` | Retain + backupOnDelete: PVCs preserved (3 remain), backup Job created, events `Deleting` → `BackupOnDelete` → `PVCsRetained` → `Deleted`, no `PVCsDeleted` event |
| `TestScenario16b_DeleteWithDeletePolicy` | Delete policy: PVCs deleted (0 remain), events `Deleting` → `PVCsDeleted` → `Deleted`, no `BackupOnDelete` or `PVCsRetained` events |
| `TestScenario16_NoFinalizerSkipsDeletion` | No finalizer: reconciler returns immediately, no requeue, no events emitted |
| `TestScenario16_BackupJobCreated` | backupOnDelete=true: Job with `backup-on-delete` in name created, correct `avsoft.io/cluster` and `avsoft.io/operation` labels |
| `TestScenario16_DeletionPhaseTransition` | Phase transition: `Running` → `Deleting` confirmed by `Deleting` event, cluster deleted after finalizer removal, `Deleted` event emitted |

#### Scenario 19 — Enable/Disable Mirroring on Existing Cluster

Tests enabling and disabling segment mirroring on an existing running cluster, including pre-flight validation, state machine transitions, DB client interactions, timeout handling, and webhook validation.

- **Pre-flight validation**: Mirroring enable requires the cluster to be in `Running` phase. The webhook rejects the patch if the cluster is not running. The operator also validates that the segment count is sufficient for the requested layout (e.g., group layout requires at least 2 segments)
- **Enable flow**: `handleMirroringEnable()` creates the mirror StatefulSet, sets `status.mirroringStatus` to `Initializing`, initiates WAL replication via the DB client, and emits `MirroringEnabled` event
- **Status transitions**: `NotConfigured` → `Initializing` → `Syncing` → `InSync`. Each transition is verified via status assertions
- **Phase transitions**: `Running` → `Updating` → `Running` during mirroring enable
- **Condition updates**: `MirroringHealthy` condition set with reason `MirroringInitializing` during enable, then `True` after completion
- **Mirror StatefulSet**: Created with the same replica count as the primary StatefulSet. Verified via StatefulSet lookup
- **Replication lag**: DB client `SetReplicationLag` metric populated during `Syncing` phase, decreases to 0 at `InSync`
- **WAL replication**: DB client `InitializeMirrors()` called to set up streaming replication from primaries to mirrors
- **Completion**: `completeMirroringEnable()` sets `status.mirroringStatus` to `InSync`, phase to `Running`, emits `MirroringInSync` event
- **Data verification**: DB client confirms mirror data matches primary data after synchronization
- **DB error handling**: When the DB client returns an error during mirror initialization, the operator logs the error and retries on the next reconciliation
- **Timeout**: 30-minute timeout. If mirrors do not reach `InSync` within this window, `status.mirroringStatus` transitions to `Degraded` and a `MirroringDegraded` warning event is emitted
- **Disable flow**: `handleMirroringDisable()` deletes the mirror StatefulSet, sets `status.mirroringStatus` to `NotConfigured`, handles PVC cleanup based on `deletionPolicy`, and emits `MirroringDisabled` event
- **PVC cleanup (Retain)**: Mirror PVCs preserved after disable
- **PVC cleanup (Delete)**: Mirror PVCs deleted after disable
- **Idempotency**: Enabling mirroring twice does not create duplicate StatefulSets
- **Webhook validation**: Enabling mirroring on a non-Running cluster is rejected. Changing layout while mirroring is enabled is rejected
- **Metrics**: `cloudberry_mirroring_operations_total{operation="enable"}` and `cloudberry_mirroring_operations_total{operation="disable"}` incremented
- **Functional tests**: `test/functional/scenario19_enable_mirroring_test.go`

**Events:**

| Event | Type | Description |
|-------|------|-------------|
| `MirroringEnabled` | Normal | Mirroring enable initiated — mirror StatefulSet created |
| `MirroringInitializing` | Normal | Mirror initialization in progress |
| `MirroringInSync` | Normal | All mirrors synchronized — mirroring enable complete |
| `MirroringDegraded` | Warning | Mirroring enable timed out after 30 minutes |
| `MirroringDisabled` | Normal | Mirroring disabled — mirror StatefulSet deleted |

**Functional tests (17 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestEnableMirroring_ValidatesNodeCount` | Insufficient segments for layout → blocked with event |
| `TestEnableMirroring_RequiresRunningPhase` | Non-Running cluster → blocked, no mirror StatefulSet created |
| `TestEnableMirroring_CreatesMirrorStatefulSet` | Mirror StatefulSet created with correct name and labels |
| `TestEnableMirroring_MirrorSTSMatchesPrimaryCount` | Mirror replica count matches primary replica count |
| `TestEnableMirroring_StatusTransitions` | `NotConfigured` → `Initializing` verified via status |
| `TestEnableMirroring_PhaseTransitions` | `Running` → `Updating` during enable |
| `TestEnableMirroring_ConditionUpdates` | `MirroringHealthy` condition set with `MirroringInitializing` reason |
| `TestEnableMirroring_ReplicationLagDecreases` | `SetReplicationLag` metric called during sync |
| `TestEnableMirroring_WALReplicationStarts` | DB client `InitializeMirrors()` called |
| `TestEnableMirroring_CompletesSuccessfully` | Full flow: `NotConfigured` → `InSync`, phase → `Running`, `MirroringInSync` event |
| `TestEnableMirroring_DataMatchesPrimaries` | DB client verifies mirror data matches primaries |
| `TestEnableMirroring_HandlesDBError` | DB error during init → logged, retried on next reconcile |
| `TestEnableMirroring_HandlesTimeout` | 30-minute timeout → `Degraded` status, `MirroringDegraded` event |
| `TestDisableMirroring_DeletesMirrorSTS` | Mirror StatefulSet deleted, status → `NotConfigured` |
| `TestDisableMirroring_PVCRetainPolicy` | Mirror PVCs preserved with Retain policy |
| `TestDisableMirroring_PVCDeletePolicy` | Mirror PVCs deleted with Delete policy |
| `TestEnableMirroring_Idempotent` | Second enable does not create duplicate StatefulSet |
| `TestWebhook_MirroringEnableOnRunning` | Webhook allows enable on Running cluster |
| `TestWebhook_MirroringEnableOnNonRunning` | Webhook rejects enable on non-Running cluster |
| `TestWebhook_MirroringLayoutChangeRejected` | Webhook rejects layout change while mirroring is enabled |

#### Scenario 20 — Automatic Segment Failover via FTS

Tests the automatic segment failover flow via the Fault Tolerance Service (FTS), including probe retry mechanism, failure detection, Cloudberry internal FTS scan triggering, mirror promotion verification, event emission, metric recording, and edge cases.

- **FTS probe retry**: `probeSegmentConfigWithRetries()` retries `GetSegmentConfiguration()` up to `FTSProbeRetries` times with `FTSProbeTimeout` per attempt. Each attempt uses a dedicated `context.WithTimeout`. On success after retry, logs "FTS probe succeeded after retry"
- **Failure detection**: `analyzeSegments()` iterates over segment configuration, identifies segments with status `d` (down), and builds `failedSegments` and `failedPrimaries` lists. Coordinator entries (contentID < 0) are skipped
- **Automatic failover**: When failed primaries are detected and mirroring is enabled, `handleFailover()` calls `dbClient.TriggerFTSProbe()` to initiate Cloudberry's internal FTS scan, which promotes mirrors to primary role
- **Promotion verification**: After triggering failover, the operator re-reads `gp_segment_configuration` and verifies that a different DBID now holds the primary role for the affected content ID
- **SegmentFailover event**: Emitted per failed primary segment with content ID, original primary hostname, and new primary hostname (if promotion succeeded)
- **FTS failover metric**: `cloudberry_fts_failover_total` incremented once per failover cycle (not per segment)
- **Status updates**: `status.failedSegments` populated with failed segment details; `status.mirroringStatus` set to `MirroringDegraded`; `MirroringDegraded` warning event emitted
- **Resilience**: If `TriggerFTSProbe()` fails, the operator still emits events and updates status based on originally detected failures. If re-read fails, events are emitted for the originally detected failures
- **Mirroring disabled**: When mirroring is not enabled, failed primaries are reported but no failover is triggered and no `SegmentFailover` events are emitted
- **Multiple primaries down**: Handles simultaneous failure of multiple primary segments, emitting `SegmentFailover` events for each
- **Cluster availability**: The cluster remains available during and after failover — the promoted mirror serves as the new primary
- **Subsequent reconciliation**: After failover, subsequent reconciliation cycles succeed and correctly report the post-failover state
- **Functional tests**: `test/functional/scenario20_automatic_failover_test.go`

**Events:**

| Event | Type | Description |
|-------|------|-------------|
| `SegmentFailover` | Warning | Primary segment failed; mirror promotion triggered or pending |
| `MirroringDegraded` | Warning | One or more segments are down |

**Metrics:**

| Metric | Description |
|--------|-------------|
| `cloudberry_fts_failover_total` | Incremented once per failover cycle |
| `cloudberry_fts_probe_total{result=failure}` | Incremented per failed probe attempt |
| `cloudberry_fts_probe_total{result=degraded}` | Recorded when probe succeeds but segments are down |
| `cloudberry_segments_failed` | Set to the count of failed segments |

**Functional tests (16 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestDetection_FTSProbeFailsForKilledSegment` | Primary down → `MirroringDegraded`, `failedSegments` populated |
| `TestDetection_RetriesOccurUpToFTSProbeRetries` | Probe retries on failure, succeeds on Nth attempt → `InSync` |
| `TestDetection_AllRetriesExhausted_ProbeFailureRecorded` | All retries fail → probe failure metric recorded |
| `TestDetection_ProbeTimeoutRespected` | Per-attempt timeout is applied via `context.WithTimeout` |
| `TestFailover_MirrorPromotedToPrimary` | After failover, mirror DBID holds primary role for affected contentID |
| `TestFailover_SegmentConfigurationUpdated` | Post-failover segment config reflects role swap |
| `TestFailover_SegmentFailoverEventEmitted` | `SegmentFailover` event emitted with content ID and hostnames |
| `TestFailover_FTSFailoverMetricIncrements` | `cloudberry_fts_failover_total` incremented |
| `TestFailover_SegmentStatusDropsToZero` | Per-segment status metric set to `false` for failed segment |
| `TestFailover_SegmentsFailedIncrements` | `cloudberry_segments_failed` gauge set to count of failures |
| `TestFailover_FailedSegmentsListUpdated` | `status.failedSegments` contains contentID, hostname, role, status |
| `TestFailover_ClusterRemainsAvailable` | Cluster phase stays `Running` during failover |
| `TestFailover_SubsequentReconcileSucceeds` | Post-failover reconciliation succeeds with updated state |
| `TestFailover_MultiplePrimariesDown` | Two primaries down → both reported, events for each |
| `TestFailover_TriggerFTSProbeError` | `TriggerFTSProbe` fails → events still emitted, status still updated |
| `TestFailover_MirroringDisabled_NoFailover` | No mirroring → no `SegmentFailover` events, no failover triggered |
| `TestFailover_AllHealthy_NoFailover` | All segments healthy → `InSync`, no failover, `failedSegments` empty |

#### Scenario 25 — Bootstrap Workload Management via CRD

Tests the full workload bootstrap flow with a mock DB client, verifying resource group CRUD operations, ConfigMap creation for workload/idle rules, and fallback behavior when the database is unavailable.

- **Resource group creation**: When the CRD spec contains resource groups that do not exist in the database, the operator creates them via `dbClient.CreateResourceGroup()` with the correct attributes (concurrency, cpuMaxPercent, cpuWeight, memoryLimit, minCost)
- **Resource group update (alter)**: When a resource group exists in the database but has different parameters than the CRD spec, the operator alters it via `dbClient.AlterResourceGroup()`. Groups with matching parameters are left untouched
- **Resource group removal (drop)**: When a resource group exists in the database but is not in the CRD spec, the operator drops it via `dbClient.DropResourceGroup()`
- **Workload rules ConfigMap**: Workload rules from `spec.workload.rules` are serialized to JSON and stored in a `{cluster}-workload-rules` ConfigMap under the `rules.json` key. Rules include cancel, move, and log actions with threshold types and priorities
- **Idle session rules ConfigMap**: Idle session rules from `spec.workload.idleRules` are serialized to JSON and stored in the same ConfigMap under the `idle-rules.json` key. Rules include idle timeout, excludeInTransaction, and custom terminate messages
- **Full bootstrap**: All components (resource groups, workload rules, idle rules, metrics) are reconciled in a single pass with `WorkloadConfigured=True/WorkloadReconciled` condition
- **DB unavailable fallback**: When the DB client factory returns an error, the operator sets `WorkloadConfigured=False/DBUnavailable` without failing the overall reconciliation. When `dbFactory` is nil (unit tests), falls back to condition-only mode
- **Metrics**: Resource group CPU and memory usage metrics updated from actual DB values after successful reconciliation
- **Functional tests**: `test/functional/scenario25_bootstrap_workload_test.go`
- **Example CR**: `test/examples/scenario25-bootstrap-workload.yaml`

**Conditions:**

| Condition | Status | Reason | Description |
|-----------|--------|--------|-------------|
| `WorkloadConfigured` | `True` | `WorkloadReconciled` | All resource groups, workload rules, and idle rules reconciled successfully |
| `WorkloadConfigured` | `False` | `DBUnavailable` | Database connection unavailable — resource groups not reconciled |

**Functional tests (7 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestScenario25a_BootstrapResourceGroups_CreatesInDB` | Two resource groups (analytics, etl) created with correct parameters; `WorkloadConfigured=True/WorkloadReconciled` condition set |
| `TestScenario25b_BootstrapWorkloadRules_CreatesConfigMap` | ConfigMap created with `rules.json` containing 2 workload rules (cancel, move) with correct action, thresholdType, threshold, priority |
| `TestScenario25c_BootstrapIdleRules_CreatesConfigMap` | ConfigMap created with `idle-rules.json` containing 1 idle session rule with correct idleTimeout, excludeInTransaction, terminateMessage |
| `TestScenario25d_FullBootstrap_AllComponents` | Full bootstrap: 2 resource groups created, ConfigMap with both `rules.json` and `idle-rules.json`, `WorkloadConfigured=True` condition |
| `TestScenario25e_ResourceGroupUpdate_AltersInDB` | Existing resource group with different parameters triggers `ALTER RESOURCE GROUP` (not CREATE); parameters updated correctly |
| `TestScenario25f_ResourceGroupRemoval_DropsFromDB` | Orphaned resource group (in DB but not in spec) triggers `DROP RESOURCE GROUP`; matching groups are not altered or created |
| `TestScenario25g_DBUnavailable_FallsBackToConditionOnly` | DB factory error → `WorkloadConfigured=False/DBUnavailable` with error message; reconciliation succeeds without error |

#### Scenario 38 — Dual-Mode Auth Infrastructure Bootstrap

Tests that when a `CloudberryCluster` is deployed with both basic and OIDC authentication enabled, the operator's auth middleware correctly routes requests to the appropriate provider based on the `Authorization` header, and both providers return correct `Identity` objects with proper `AuthMethod` and `PermissionLevel`.

- **Dual-mode routing**: `Authorization: Basic ...` → routed to Basic provider (`Identity.AuthMethod="basic"`); `Authorization: Bearer ...` → routed to OIDC provider (`Identity.AuthMethod="oidc"`)
- **Provider interface compliance**: Both `BasicAuthProvider` and `OIDCProvider` implement the `Provider` interface; `Type()` returns `"basic"` and `"oidc"` respectively
- **Permission resolver**: All 5 permission levels verified via basic auth — `Admin`, `Operator`, `Operator Basic`, `Basic`, `Self Only`
- **Missing auth header**: Returns `401 Unauthorized` with JSON error body `{"error": {"code": "UNAUTHORIZED"}}`
- **Unsupported auth type**: `Digest`, etc. return `401 Unauthorized` with JSON error body
- **Sequential routing**: Multiple sequential requests with alternating auth types are correctly routed without cross-contamination
- **CR spec reflection**: Cluster CR with both `auth.basic` and `auth.oidc` persists correctly and the API server operates with both providers active
- **Error response format**: All 401 responses use proper JSON format
- **Test case catalog**: `DualAuthCase` type and `DualAuthCases()` function in `test/cases/test_cases.go` (9 cases)
- **Example CR**: `test/examples/scenario38-dual-auth.yaml`
- **Functional tests**: `test/functional/scenario38_dual_auth_test.go`
- **E2E tests**: `test/e2e/scenario38_dual_auth_e2e_test.go`

**Bug fix — OIDC provider wiring in `startAPIServer()`**:

During real-cluster testing, a critical bug was discovered in `cmd/operator/main.go`: `startAPIServer()` passed `nil` for the OIDC provider, meaning Bearer token auth was never available even when OIDC was configured via Helm values. The fix adds OIDC provider initialization when `cfg.OIDC.Enabled` is true, with default role mapping (`admin`→Admin, `operator`→Operator, `operator-basic`→"Operator Basic", `user`→Basic, `reader`→"Self Only"), default `RoleClaimPath: "realm_access.roles"`, and `RoleMatchMode: "exact"`. OIDC initialization failure is handled gracefully (logs warning, continues with Basic-only auth).

**Real-cluster verification results** (10/10 PASS):

| # | Test | HTTP Status | Result |
|---|------|-------------|--------|
| 1 | Basic Auth (valid admin) → routed to Basic provider | 200 | PASS |
| 2 | Basic Auth (invalid password) | 401 | PASS |
| 3 | No Auth Header | 401 | PASS |
| 4 | Bearer Auth (REAL Keycloak service account JWT) → routed to OIDC provider | 200 | PASS |
| 5 | Bearer Auth (REAL Keycloak user password-grant JWT) → routed to OIDC provider | 200 | PASS |
| 6 | Unsupported Auth Type (Digest) | 401 | PASS |
| 7 | Health /healthz (no auth) | 200 | PASS |
| 8 | Health /readyz (no auth) | 200 | PASS |
| 9 | Bearer Auth (invalid token) | 401 | PASS |
| 10 | Dual-auth cluster CR phase = Running | Running | PASS |

**Operator log evidence**:

- Basic: `"basic auth succeeded", username: "admin", permission: "Admin"`
- OIDC (service account): `"OIDC auth succeeded", username: "service-account-cloudberry-operator", roles: ["admin"], permission: "Admin"`
- OIDC (user): `"OIDC auth succeeded", username: "testuser", email: "testuser@test.local", roles: ["admin"], permission: "Admin"`

**Keycloak configuration requirements**:

1. **Audience mapper**: Keycloak realm must have an `oidc-audience-mapper` that includes the operator's `clientID` in the `aud` claim
2. **Frontend URL**: Keycloak realm `frontendUrl` must match the operator's configured `issuerURL` (so the `iss` claim matches)
3. **Role assignment**: Service accounts and users must have appropriate realm roles assigned (e.g., `admin`, `operator`)

**Functional tests (18 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestFunctional_Scenario38_BothProvidersActive` | Middleware created with both basic and OIDC providers simultaneously |
| `TestFunctional_Scenario38_BasicAuthRouting` | `Authorization: Basic ...` routed to basic provider; `AuthMethod="basic"`, correct username and permission |
| `TestFunctional_Scenario38_BearerAuthRouting` | `Authorization: Bearer ...` routed to OIDC provider; `AuthMethod="oidc"`, correct username and permission |
| `TestFunctional_Scenario38_BasicProviderType` | `BasicAuthProvider.Type()` returns `"basic"` |
| `TestFunctional_Scenario38_OIDCProviderType` | `OIDCProvider.Type()` returns `"oidc"` |
| `TestFunctional_Scenario38_PermissionResolver_Admin` | Basic auth admin → `PermissionAdmin` |
| `TestFunctional_Scenario38_PermissionResolver_Operator` | Basic auth operator → `PermissionOperator` |
| `TestFunctional_Scenario38_PermissionResolver_Basic` | Basic auth viewer → `PermissionBasic` |
| `TestFunctional_Scenario38_PermissionResolver_SelfOnly` | Basic auth reader → `PermissionSelfOnly` |
| `TestFunctional_Scenario38_MissingAuthHeader` | No `Authorization` header → 401 with `UNAUTHORIZED` JSON error |
| `TestFunctional_Scenario38_UnsupportedAuthType` | `Digest` auth type → 401 with `UNAUTHORIZED` JSON error |
| `TestFunctional_Scenario38_SimultaneousProviders_DifferentUsers` | Sequential Basic/Bearer/Basic/none requests correctly routed |
| `TestFunctional_Scenario38_DualAuthCases` | 9 cases from `DualAuthCases()` catalog executed |

**E2E tests (20 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestE2E_Scenario38_DualAuth_BothProvidersSimultaneous` | API server routes Basic and Bearer requests correctly (3 subtests) |
| `TestE2E_Scenario38_DualAuth_BasicAuthIdentity` | Basic auth identity has correct `Username`, `AuthMethod`, `Permission` |
| `TestE2E_Scenario38_DualAuth_OIDCAuthIdentity` | OIDC auth identity has correct `Username`, `AuthMethod`, `Permission`, `Roles` |
| `TestE2E_Scenario38_DualAuth_PermissionMatrix` | All 5 permission levels verified with `Permission.String()` (5 subtests) |
| `TestE2E_Scenario38_DualAuth_ProviderInterfaceCompliance` | Both providers implement `auth.Provider`; `Type()` and `Authenticate()` return correct values |
| `TestE2E_Scenario38_DualAuth_CRSpecReflected` | Cluster CR persists dual auth config; API server works with both providers |
| `TestE2E_Scenario38_DualAuth_ErrorResponseFormat` | 401 responses have JSON format with `{"error": {"code": "UNAUTHORIZED"}}` (2 subtests) |
| `TestE2E_Scenario38_DualAuth_CasesCatalog` | 9 cases from `DualAuthCases()` catalog executed in E2E context |

```bash
# Run dual-mode auth functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario38

# Run dual-mode auth E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario38
```

#### Scenario 39 — Basic Authentication Flow

Tests the basic authentication flow end-to-end, including admin user validation (correct/wrong password, missing/malformed headers, timing attack prevention, no password leakage in logs) and DB role validation (unknown users, multiple users with different permission levels).

- **Admin auth (39a)**: Valid admin credentials produce an `Identity` with `Username="admin"`, `Permission=Admin`, `AuthMethod="basic"`. Wrong password returns 401. Missing `Authorization` header returns 401. Malformed Basic headers (invalid base64, empty, no space, Digest) return 401
- **No password in logs**: After authentication, log output contains the username but never the password. Verified by capturing `slog` output and asserting the password string is absent
- **Timing attack prevention**: When a user is not found in the credential store, the provider performs a bcrypt comparison against a dummy hash to ensure constant-time behavior. Verified by measuring that the user-not-found path takes > 1ms
- **DB role validation (39b)**: Unknown users not in the `InMemoryCredentialStore` receive 401 with "invalid credentials" error. Multiple users with different permission levels (Admin, Operator, Operator Basic, Basic, Self Only) all authenticate correctly with the expected permission
- **Provider interface compliance**: `BasicAuthProvider.Type()` returns `"basic"`. All `Identity` fields verified: Username, Permission, AuthMethod set; Email, Groups, Roles, TokenExpiry empty/nil for basic auth
- **Error response format**: All 401 responses use JSON format with `{"error": {"code": "UNAUTHORIZED", "message": "..."}}`
- **Security headers**: Responses include `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Strict-Transport-Security`, and other security headers
- **API server integration**: Basic auth middleware integrates correctly with the API server — authenticated requests succeed, unauthenticated requests return 401
- **Test case catalog**: `BasicAuthFlowCase` type and `BasicAuthFlowCases()` function in `test/cases/test_cases.go` (8 cases)
- **Example CR**: `test/examples/scenario39-basic-auth.yaml`
- **Functional tests**: `test/functional/scenario39_basic_auth_test.go`
- **E2E tests**: `test/e2e/scenario39_basic_auth_e2e_test.go`

**Known limitation**: The current implementation uses `InMemoryCredentialStore` with only the admin user. Database role validation via SQL query to the coordinator is specified but not implemented. Unknown users get 401 with timing-attack-safe dummy hash comparison.

**Real-cluster verification results (6/6 PASS)**:

Operator deployed with webhooks (vault-PKI, k8s auth) + OIDC + Basic auth.

| # | Test | HTTP | Result |
|---|------|------|--------|
| 39a-valid | admin with correct password (from K8s Secret) | 200 | ✅ Identity: username=admin, permission=Admin, AuthMethod=basic |
| 39a-wrong | admin with wrong password | 401 | ✅ |
| 39a-noleak | Password NOT in operator logs | N/A | ✅ Only username logged |
| 39a-missing | No auth header | 401 | ✅ |
| 39a-malformed | Malformed Basic header | 401 | ✅ |
| 39b-unknown | Unknown user 'analyst' (not in credential store) | 401 | ✅ |

Data operations: mydb created, 50 rows inserted, SELECT works.

**Functional tests (13 test methods, 31 sub-tests):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario39a_AdminAuth_CorrectPassword` | Valid admin credentials → Identity with Admin permission and AuthMethod="basic" |
| `TestFunctional_Scenario39a_AdminAuth_WrongPassword` | Wrong admin password → 401 via middleware |
| `TestFunctional_Scenario39a_AdminAuth_NoPasswordInLogs` | Password never in log output; username IS logged for audit |
| `TestFunctional_Scenario39a_AdminAuth_TimingAttack` | Unknown user path takes non-trivial time (bcrypt dummy hash) |
| `TestFunctional_Scenario39a_AdminAuth_MissingHeader` | No Authorization header → 401 |
| `TestFunctional_Scenario39a_AdminAuth_MalformedHeader` | 4 malformed headers (invalid base64, empty, no space, Digest) → 401 |
| `TestFunctional_Scenario39b_DBRole_NotInStore` | Unknown user → 401 with "invalid credentials" |
| `TestFunctional_Scenario39b_DBRole_MultipleUsers` | All 5 permission levels verified (Admin, Operator, Operator Basic, Basic, Self Only) |
| `TestFunctional_Scenario39_BasicAuthFlowCases` | 8 cases from `BasicAuthFlowCases()` catalog |
| `TestFunctional_Scenario39_ProviderType` | `BasicAuthProvider.Type()` returns `"basic"` |
| `TestFunctional_Scenario39_IdentityFields` | All Identity fields verified (set and unset) |
| `TestFunctional_Scenario39_MiddlewareWithAPIServer` | API server integration: authenticated → 200, unauthenticated → 401, wrong password → 401 |
| `TestFunctional_Scenario39_ErrorResponseJSON` | 401 response is JSON with `{"error": {"code": "UNAUTHORIZED"}}` |

**E2E tests (6 test methods, 22 sub-tests):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario39_AdminAuth_FullFlow` | Full admin auth lifecycle: valid → 200, invalid → 401, missing → 401 |
| `TestE2E_Scenario39_PermissionLevels` | All 5 permission levels verified with `Permission.String()` and `AuthMethod` |
| `TestE2E_Scenario39_SecurityHeaders` | Security headers present on success and failure responses |
| `TestE2E_Scenario39_ErrorResponseFormat` | JSON error format for missing header, wrong password, unsupported auth type |
| `TestE2E_Scenario39_ClusterCRWithBasicAuth` | Cluster CR with basic auth config persists; API server works |
| `TestE2E_Scenario39_BasicAuthFlowCases` | 8 cases from `BasicAuthFlowCases()` catalog in E2E context |

```bash
# Run basic auth flow functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario39

# Run basic auth flow E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario39
```

#### Scenario 40 — Password Rotation

Tests the admin password rotation lifecycle, including K8s Secret creation, password priority resolution (env var > K8s Secret > generated), API-driven rotation via `POST /api/v1alpha1/auth/rotate-password`, CLI command `cloudberry-ctl auth rotate-password`, immediate in-memory credential update (no restart needed), and Vault secret watcher change detection.

- **Admin Secret creation**: Cluster controller creates an admin password Secret with `managed-by` label when one does not exist. Existing user-provided Secrets are not overwritten
- **Password priority resolution**: `CLOUDBERRY_API_ADMIN_PASSWORD` env var takes priority over K8s Secret. When neither is set, a cryptographically secure random password is generated via `util.GenerateRandomPassword()`. Two generated passwords are always different
- **API-driven rotation**: `POST /api/v1alpha1/auth/rotate-password` (requires Admin permission) generates a new random password, updates the K8s Secret `cloudberry-operator-admin-password`, updates the in-memory credential store immediately (no restart needed), records the `cloudberry_password_rotation_total` Prometheus metric, and returns `{"status": "rotated", "message": "Admin password rotated successfully"}`. The new password is NOT returned in the response (security)
- **CLI command**: `cloudberry-ctl auth rotate-password --cluster <name>` calls the API endpoint and prints a success/failure message
- **New password works immediately**: After API rotation, the new password authenticates via Basic auth (HTTP 200) without operator restart
- **Old password fails immediately**: After API rotation, the old password returns HTTP 401 without operator restart
- **Vault SecretWatcher**: `SecretWatcher` detects hash change on the Vault secret path and invokes the `onChange` callback with updated data
- **Test case catalog**: `PasswordRotationCase` type and `PasswordRotationCases()` function in `test/cases/test_cases.go` (5 cases)
- **Example CR**: `test/examples/scenario40-password-rotation.yaml`
- **Functional tests**: `test/functional/scenario40_password_rotation_test.go`
- **E2E tests**: `test/e2e/scenario40_password_rotation_e2e_test.go`

**Known limitations**:

1. DB role password update not implemented — only the operator API admin password is rotated
2. Vault secret sync is manual — `SecretWatcher` exists but is not wired into the automatic rotation pipeline

**Files changed for API-driven rotation**:

| File | Change |
|------|--------|
| `internal/api/server.go` | New `POST /auth/rotate-password` endpoint + `handleRotatePassword` handler |
| `internal/metrics/metrics.go` | New `cloudberry_password_rotation_total` counter metric |
| `cmd/operator/main.go` | Pass `credStore` to API server |
| `cmd/cloudberry-ctl/main.go` | Implement `rotate-password` CLI command |
| `internal/ctl/client.go` | `RotatePasswordPath()` helper |
| `internal/api/server_test.go` | 6 new unit tests for rotate-password |
| `test/functional/scenario1_full_bootstrap_test.go` | Mock fix for credStore parameter |

**Real-cluster verification results (11/11 PASS)**:

Full test environment running (Vault, Keycloak, VictoriaMetrics, MinIO, Kafka, RabbitMQ).

| # | Step | Result |
|---|------|--------|
| 1 | K8s Secret exists | ✅ |
| 2 | Current password works (HTTP 200) | ✅ |
| 3 | API rotate-password returns `{"status":"rotated"}` | ✅ |
| 4 | New password differs from old in K8s Secret | ✅ |
| 5 | New password works IMMEDIATELY (HTTP 200, no restart) | ✅ |
| 6 | Old password FAILS IMMEDIATELY (HTTP 401) | ✅ |
| 7 | Password NOT in operator logs | ✅ |
| 8 | Vault secret updated consistently | ✅ |
| 9 | `cloudberry-ctl auth rotate-password` succeeds | ✅ |
| 10 | Password rotated again by ctl | ✅ |
| 11 | Data ops work (100 rows in mydb) | ✅ |

**Functional tests (10 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario40_AdminSecret_Created` | Cluster controller creates admin password Secret with `managed-by` label |
| `TestFunctional_Scenario40_AdminSecret_NotOverwritten` | Existing user-provided Secret is not overwritten |
| `TestFunctional_Scenario40_OperatorPassword_FromSecret` | Operator reads password from K8s Secret when no env var is set |
| `TestFunctional_Scenario40_OperatorPassword_FromEnvVar` | Env var takes priority over K8s Secret |
| `TestFunctional_Scenario40_OperatorPassword_Generated` | Random password generated when neither env var nor Secret exists |
| `TestFunctional_Scenario40_SecretUpdate_NewPassword` | K8s Secret updated with new password value |
| `TestFunctional_Scenario40_BasicAuth_WithNewPassword` | New password authenticates after rotation (HTTP 200) |
| `TestFunctional_Scenario40_BasicAuth_OldPasswordFails` | Old password fails after rotation (HTTP 401) |
| `TestFunctional_Scenario40_VaultSecretWatcher_DetectsChange` | Vault `SecretWatcher` detects hash change, invokes `onChange` callback |
| `TestFunctional_Scenario40_PasswordRotationCases` | All 5 cases from `PasswordRotationCases()` catalog (5 sub-tests) |

**E2E tests (6 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario40_AdminSecretCreated` | Admin password Secret created with `managed-by` label |
| `TestE2E_Scenario40_PasswordChange_NewWorks` | New password authenticates through full API stack after rotation |
| `TestE2E_Scenario40_PasswordChange_OldFails` | Old password returns HTTP 401 after rotation |
| `TestE2E_Scenario40_VaultWatcher_DetectsChange` | Vault `SecretWatcher` detects change in E2E context |
| `TestE2E_Scenario40_ClusterCRAccepted` | Cluster CR with basic auth config persists; API server works |
| `TestE2E_Scenario40_PasswordRotationCases` | All 5 cases from `PasswordRotationCases()` catalog in E2E context |

```bash
# Run password rotation functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario40

# Run password rotation E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario40
```

#### Scenario 41 — OIDC Full Flow with Keycloak

Tests the complete OIDC authentication flow end-to-end, including OIDC provider initialization, JWT verification, role extraction from nested `realm_access.roles` claims, role-to-permission mapping for all 5 permission levels, standard claim extraction, dual-mode auth (Basic + OIDC), service account (client_credentials) flow, and all role match modes.

- **OIDC provider initialization**: `NewOIDCProvider()` fetches `.well-known/openid-configuration` and JWKS from the issuer. Validates that `IssuerURL` and `ClientID` are required. Unreachable issuers return an error
- **JWT verification**: Valid tokens succeed, invalid tokens fail, expired tokens fail, wrong audience fails, missing bearer token fails
- **Role extraction**: Roles extracted from nested `realm_access.roles` claim path. Single role, multiple roles, no roles, and missing `realm_access` all handled correctly
- **Role-to-permission mapping**: All 5 levels verified — `admin`→Admin, `operator`→Operator, `operator-basic`→"Operator Basic", `user`→Basic, `reader`→"Self Only". When multiple roles are present, the highest permission wins. Unknown roles default to Self Only
- **Claim extraction**: `sub` sets Username, `email` sets Email, `preferred_username` overrides `sub` when present
- **Dual-mode auth (allowLocalSignIn)**: Basic and OIDC providers work simultaneously. Sequential requests with alternating auth types are correctly routed without cross-contamination
- **Service account (client_credentials)**: Token with `azp` claim and no `preferred_username` accepted; `sub` used as username
- **Role match modes**: All 4 modes verified — `exact`, `suffix`, `prefix`, `contains` — with positive and negative test cases
- **Test case catalog**: `OIDCFlowCase` type and `OIDCFlowCases()` function in `test/cases/test_cases.go` (5 cases)
- **Example CR**: `test/examples/scenario41-oidc-full-flow.yaml`
- **Functional tests**: `test/functional/scenario41_oidc_full_flow_test.go`
- **E2E tests**: `test/e2e/scenario41_oidc_full_flow_e2e_test.go`

**Real-cluster verification results (7/7 PASS)**:

The operator was deployed with Vault-PKI webhook certs (Kubernetes auth to Vault), OIDC enabled (`issuerURL=http://host.docker.internal:8090/realms/test`, `clientID=cloudberry-operator`), and basic auth enabled (`allowLocalSignIn`).

| # | Test | HTTP | Permission | Result |
|---|------|------|------------|--------|
| 1 | admin-user (role=admin) via OIDC Bearer | 200 | Admin | ✅ |
| 2 | operator-user (role=operator) via OIDC Bearer | 200 | Operator | ✅ |
| 3 | opbasic-user (role=operator-basic) via OIDC Bearer | 200 | Operator Basic | ✅ |
| 4 | basic-user (role=user) via OIDC Bearer | 200 | Basic | ✅ |
| 5 | reader-user (role=reader) via OIDC Bearer | 403 | Self Only | ✅ |
| 6 | Basic auth alongside OIDC (allowLocalSignIn) | 200 | Admin | ✅ |
| 7 | Service account (client_credentials) via OIDC Bearer | 200 | Admin | ✅ |

**Operator log evidence**:

- `username=admin-user email=admin-user@test.local roles=[admin] permission=Admin`
- `username=operator-user email=operator-user@test.local roles=[operator] permission=Operator`
- `username=opbasic-user email=opbasic-user@test.local roles=[operator-basic] permission=Operator Basic`
- `username=basic-user email=basic-user@test.local roles=[user] permission=Basic`
- `username=reader-user email=reader-user@test.local roles=[reader] permission=Self Only`
- `username=service-account-cloudberry-operator roles=[admin] permission=Admin`

**Functional tests (8 test methods, 37 sub-tests):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario41_OIDCProviderInit` | Provider init with mock discovery, OAuth2 config, missing issuer/client ID, unreachable issuer |
| `TestFunctional_Scenario41_JWTVerification` | Valid, invalid, expired, wrong-audience, and missing bearer tokens |
| `TestFunctional_Scenario41_RoleExtraction` | Single role, multiple roles, no roles, missing `realm_access` |
| `TestFunctional_Scenario41_RoleMapping_AllLevels` | All 5 mappings, multiple roles (highest wins), unknown role defaults |
| `TestFunctional_Scenario41_ClaimExtraction` | `sub`, `email`, `preferred_username` override, all claims together |
| `TestFunctional_Scenario41_AllowLocalSignIn` | Basic alongside OIDC, sequential routing, no auth returns 401 |
| `TestFunctional_Scenario41_MatchModes` | All 4 match modes with positive and negative cases (8 sub-tests) |
| `TestFunctional_Scenario41_OIDCFlowCases` | 5 cases from `OIDCFlowCases()` catalog |

**E2E tests (6 test methods, 16 sub-tests):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario41_OIDCProviderInit` | OIDC provider initialization with mock discovery |
| `TestE2E_Scenario41_PerUserAuth` | 5 users with different roles authenticated with correct permissions |
| `TestE2E_Scenario41_AllowLocalSignIn` | Dual-mode auth: basic succeeds, OIDC succeeds, no auth fails, interleaved requests |
| `TestE2E_Scenario41_ServiceAccount` | Service account token accepted, `sub` used as username |
| `TestE2E_Scenario41_ClusterCRWithOIDC` | Cluster CR with OIDC config persists, API server works with both auth methods |
| `TestE2E_Scenario41_OIDCFlowCases` | 5 cases from `OIDCFlowCases()` catalog in E2E context |

```bash
# Run OIDC full flow functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario41

# Run OIDC full flow E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario41
```

#### Scenario 42 — Role Claim Source and Match Modes

Tests the `roleClaimSource` and `roleMatchMode` configuration fields, verifying that the OIDC provider correctly extracts roles from the configured source and applies the configured match mode when mapping roles to permission levels.

- **Role claim source (id_token)**: Roles extracted from the ID token's `realm_access.roles` claim. Single role, multiple roles, and no roles all handled correctly
- **Role claim source (userinfo)**: Configuration value accepted but not implemented — `Authenticate()` always reads from ID token claims (known limitation)
- **Match mode (exact)**: Token role must match the mapping key exactly. `admin` matches `admin`, but `super-admin` does not
- **Match mode (suffix)**: Token role must end with the mapping key. `org-admin` matches `admin`, but `admin-team` does not
- **Match mode (prefix)**: Token role must start with the mapping key. `admin-team` matches `admin`, but `super-admin` does not
- **Match mode (contains)**: Token role must contain the mapping key as a substring. `super-admin-user` matches `admin`, but `reader` does not
- **resolvePermission integration**: All 4 match modes verified with positive and negative cases across exact, suffix, prefix, and contains modes (12 sub-tests)
- **Test case catalog**: `RoleClaimCase` type and `RoleClaimCases()` function in `test/cases/test_cases.go` (10 cases)
- **Example CR**: `test/examples/scenario42-role-claim-modes.yaml`
- **Functional tests**: `test/functional/scenario42_role_claim_modes_test.go`
- **E2E tests**: `test/e2e/scenario42_role_claim_modes_e2e_test.go`

**Known limitations**:

1. `roleClaimSource: userinfo` is configured but not implemented — `Authenticate()` always reads from ID token claims
2. `roleMatchMode` is hardcoded to `"exact"` in `cmd/operator/main.go` — not configurable via Helm/env vars
3. Match modes (suffix, prefix, contains) work correctly in the code but can only be tested on a real cluster by modifying the operator source
4. Keycloak 26.x requires `firstName` and `lastName` on users for password grant to work ("Account is not fully set up" error without them)

**Real-cluster verification results (6/6 PASS)**:

Operator deployed with `roleMatchMode=exact` (hardcoded default), `roleClaimSource=id_token`.

| # | Test | Role | Match Mode | HTTP | Permission | Result |
|---|------|------|------------|------|------------|--------|
| 42a | admin-user | admin | id_token source | 200 | Admin | ✅ |
| 42c-match | exact-admin-user | admin | exact | 200 | Admin | ✅ |
| 42c-nomatch | super-admin-user | super-admin | exact | 403 | Self Only | ✅ |
| 42d-exact | org-admin-user | org-admin | exact (no match) | 403 | Self Only | ✅ |
| 42e-exact | admin-team-user | admin-team | exact (no match) | 403 | Self Only | ✅ |
| 42f-exact | super-admin-role-user | super-admin-user | exact (no match) | 403 | Self Only | ✅ |

**Functional tests (12 test methods, 37 sub-tests):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario42a_IDToken_RolesFromClaims` | Admin role extracted from ID token, multiple roles, no roles defaults to Self Only |
| `TestFunctional_Scenario42b_UserInfo_ConfigField` | UserInfo config accepted, still reads ID token claims, default source is id_token |
| `TestFunctional_Scenario42c_Exact_Match` | Exact match: `admin` matches `admin` → Admin |
| `TestFunctional_Scenario42c_Exact_NoMatch` | Exact no-match: `admin` does not match `super-admin` → Self Only |
| `TestFunctional_Scenario42d_Suffix_Match` | Suffix match: `admin` matches `org-admin` → Admin |
| `TestFunctional_Scenario42d_Suffix_NoMatch` | Suffix no-match: `admin` does not match `admin-team` → Self Only |
| `TestFunctional_Scenario42e_Prefix_Match` | Prefix match: `admin` matches `admin-team` → Admin |
| `TestFunctional_Scenario42e_Prefix_NoMatch` | Prefix no-match: `admin` does not match `super-admin` → Self Only |
| `TestFunctional_Scenario42f_Contains_Match` | Contains match: `admin` matches `super-admin-user` → Admin |
| `TestFunctional_Scenario42f_Contains_NoMatch` | Contains no-match: `admin` does not match `reader` → Self Only |
| `TestFunctional_Scenario42_ResolvePermission_AllModes` | All 4 match modes with 3 cases each (12 sub-tests) |
| `TestFunctional_Scenario42_RoleClaimCases` | All 10 cases from `RoleClaimCases()` catalog |

**E2E tests (7 suite methods, 17 sub-tests):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario42_ExactMatch_AdminRole` | Exact match with admin role → Admin permission |
| `TestE2E_Scenario42_ExactMatch_NoMatch` | Exact match with non-matching role → Self Only |
| `TestE2E_Scenario42_SuffixMatch` | Suffix match: `org-admin` matches `admin` pattern → Admin |
| `TestE2E_Scenario42_PrefixMatch` | Prefix match: `admin-team` matches `admin` pattern → Admin |
| `TestE2E_Scenario42_ContainsMatch` | Contains match: `super-admin-user` matches `admin` pattern → Admin |
| `TestE2E_Scenario42_ClusterCRWithRoleConfig` | Cluster CR with role claim config persists correctly in K8s |
| `TestE2E_Scenario42_RoleClaimCases` | All 10 cases from `RoleClaimCases()` catalog in E2E context |

```bash
# Run role claim modes functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario42

# Run role claim modes E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario42
```

#### Scenario 43 — Full Permission Matrix Verification

Tests the complete API permission matrix by verifying every endpoint against all five permission levels (Admin, Operator, OperatorBasic, Basic, SelfOnly). The full 5-user × 57-endpoint matrix (285 permission checks) is verified in automated functional tests using `api.NewServer()` with `httptest`.

- **Permission enforcement**: Each of the 57 API endpoints is tested against all 5 users. Users with sufficient permission receive a non-401/403 response; users below the required level receive `403 Forbidden` with JSON error body `{"error": {"code": "FORBIDDEN", "message": "insufficient permissions..."}}`
- **Unauthenticated requests**: All API endpoints return `401 Unauthorized` with `{"error": {"code": "UNAUTHORIZED"}}` when no credentials are provided
- **Health endpoints bypass auth**: `/healthz` and `/readyz` return 200 without authentication
- **Security headers on 403**: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Cache-Control: no-store`, `Strict-Transport-Security: max-age=31536000`
- **Forbidden response format**: 403 responses include the required permission level in the error message (e.g., "requires Operator Basic")
- **Test case catalog**: `PermissionMatrixCase` type and `PermissionMatrixCases()` function in `test/cases/test_cases.go` (57 cases)
- **Example CR**: `test/examples/scenario43-permission-matrix.yaml`
- **Functional tests**: `test/functional/scenario43_permission_matrix_test.go`
- **E2E tests**: `test/e2e/scenario43_permission_matrix_e2e_test.go`

**Permission level requirements by endpoint category:**

| Category | Required Level | Endpoint Count |
|----------|---------------|---------------|
| Read-only cluster state | Basic | 24 |
| Config and sessions viewing | OperatorBasic | 2 |
| Cluster operations (mutations) | Operator | 24 |
| Destructive / high-impact | Admin | 7 |

**Real-cluster verification results (12/12 PASS)**:

Operator deployed with self-signed webhook certs + OIDC (OIDC unavailable due to Docker Desktop networking). Basic auth tested.

| # | Test | HTTP | Result |
|---|------|------|--------|
| 43a-1 | Admin GET /clusters | not 401/403 | PASS |
| 43a-2 | Admin GET /status | not 401/403 | PASS |
| 43a-3 | Admin GET /config | not 401/403 | PASS |
| 43a-4 | Admin GET /sessions | not 401/403 | PASS |
| 43a-5 | Admin POST /start | not 401/403 | PASS |
| 43a-6 | Admin POST /vacuum | not 401/403 | PASS |
| 43a-7 | Admin DELETE /cluster | not 401/403 | PASS |
| 43b | No auth → 401 | 401 | PASS |
| 43c | Wrong password → 401 | 401 | PASS |
| 43d | Unknown user → 401 | 401 | PASS |
| 43e-1 | /healthz no auth → 200 | 200 | PASS |
| 43e-2 | /readyz no auth → 200 | 200 | PASS |

Data operations: mydb created, 50 rows inserted, SELECT works.

**Known limitation**: OIDC-based permission testing on a real cluster requires Keycloak reachable from k8s pods. In Docker Desktop, `host.docker.internal` resolves but connection is refused. The full OIDC permission matrix was verified in Scenario 41.

**Functional tests (10 suite methods):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario43a_Admin_AllOperationsSucceed` | Admin user accesses all endpoints without 401/403 |
| `TestFunctional_Scenario43b_Operator_AllowedAndDenied` | Operator allowed on operator-level, denied on admin-only with 403 |
| `TestFunctional_Scenario43c_OperatorBasic_AllowedAndDenied` | OperatorBasic allowed on config/sessions, denied on operator operations |
| `TestFunctional_Scenario43d_Basic_AllowedAndDenied` | Basic allowed on read-only state, denied on config/sessions and operator ops |
| `TestFunctional_Scenario43e_SelfOnly_AllowedAndDenied` | SelfOnly: health endpoints 200, all API endpoints 403 |
| `TestFunctional_Scenario43_PermissionMatrixCases` | All 57 cases × 5 users = 285 permission checks |
| `TestFunctional_Scenario43_UnauthenticatedDenied` | Unauthenticated → 401 with `UNAUTHORIZED` JSON error |
| `TestFunctional_Scenario43_HealthEndpointsNoAuth` | `/healthz` and `/readyz` return 200 without auth |
| `TestFunctional_Scenario43_ForbiddenResponseFormat` | 403 JSON format with required permission level in message |
| `TestFunctional_Scenario43_SecurityHeadersOnForbidden` | Security headers present on 403 responses |

**E2E tests (8 suite methods):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario43a_Admin_AllOperationsSucceed` | Admin accesses all endpoints without 401/403 |
| `TestE2E_Scenario43b_Operator_AllowedAndDenied` | Operator allowed/denied with correct 403 JSON error |
| `TestE2E_Scenario43c_OperatorBasic_AllowedAndDenied` | OperatorBasic allowed/denied boundaries |
| `TestE2E_Scenario43d_Basic_AllowedAndDenied` | Basic allowed/denied boundaries |
| `TestE2E_Scenario43e_SelfOnly_AllowedAndDenied` | SelfOnly: health 200, all API 403 |
| `TestE2E_Scenario43_PermissionMatrixCases` | Full 57 × 5 = 285 permission checks from catalog |
| `TestE2E_Scenario43_UnauthenticatedDenied` | Unauthenticated → 401 with JSON error |
| `TestE2E_Scenario43_SecurityHeadersOnForbidden` | Security headers on 403 responses |

```bash
# Run permission matrix functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario43

# Run permission matrix E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario43
```

#### Scenario 44 — pg_hba.conf Custom Rules

Tests that when a `CloudberryCluster` is deployed with explicit `hbaRules` in the spec, the operator generates a `pg_hba.conf` ConfigMap containing exactly the specified custom rules, excludes all default rules, preserves rule ordering, tracks configuration changes via a hash annotation, and supports live updates without pod restarts.

- **Custom rule generation**: When `spec.auth.hbaRules` contains explicit rules, the Auth Reconciler generates a `pg_hba.conf` ConfigMap containing only those rules. Default rules are not generated
- **Rule ordering**: Custom rules appear in the same order as specified in the CRD `hbaRules` array
- **Default exclusion**: Default-only rules (`local all all scram-sha-256`, `host gpadmin 127.0.0.1/32 trust`, `host replication all 0.0.0.0/0 scram-sha-256`) are absent when custom rules are specified
- **Config hash annotation**: The ConfigMap has an `avsoft.io/config-hash` annotation with a non-empty SHA hash
- **Live update**: Patching `spec.auth.hbaRules` triggers a new reconciliation that updates the ConfigMap content and changes the config hash annotation. Old rules are removed and new rules are added
- **Rule types**: Supports `local`, `host`, `hostssl`, and `hostnossl` connection types with all authentication methods (`trust`, `scram-sha-256`, `md5`, `reject`, `peer`, `ldap`)
- **Rule options**: HBA rules with additional options (e.g., LDAP server configuration) are rendered correctly
- **Test case catalog**: `HBACustomRuleCase` type and `HBACustomRuleCases()` function in `test/cases/test_cases.go` (5 cases)
- **Example CR**: `test/examples/scenario44-hba-custom-rules.yaml`
- **Functional tests**: `test/functional/scenario44_hba_custom_rules_test.go`
- **E2E tests**: `test/e2e/scenario44_hba_custom_rules_e2e_test.go`

**CR spec used:**

```yaml
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
    - type: hostssl
      database: all
      user: all
      address: "192.168.0.0/16"
      method: scram-sha-256
    - type: host
      database: all
      user: all
      address: "0.0.0.0/0"
      method: reject
```

**Real-cluster verification results (13/13 PASS)**:

| # | Test | Result |
|---|------|--------|
| 1 | ConfigMap `scenario44-hba-custom-pg-hba-conf` exists | ✅ |
| 2 | `local all gpadmin trust` rule present | ✅ |
| 3 | `host all all 10.0.0.0/8 scram-sha-256` rule present | ✅ |
| 4 | `hostssl all all 192.168.0.0/16 scram-sha-256` rule present | ✅ |
| 5 | `host all all 0.0.0.0/0 reject` rule present | ✅ |
| 6 | No default rules (127.0.0.1/32 absent) | ✅ |
| 7 | `avsoft.io/config-hash` annotation present | ✅ |
| 8 | Config volume in StatefulSet | ✅ |
| 9 | Coordinator pod Running | ✅ |
| 10 | TCP from 127.0.0.1 blocked (reject rule active) | ✅ |
| 11 | Hash changed after HBA update (7c09d696→1abc07f9) | ✅ |
| 12 | New rule (172.16.0.0/12) added after patch | ✅ |
| 13 | analyst user rule present after patch | ✅ |

**Functional tests (16 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario44_CustomRules_ConfigMapCreated` | ConfigMap created with all 4 custom rules; exactly 4 rule lines |
| `TestFunctional_Scenario44_CustomRules_RuleOrder` | Rules appear in CRD-specified order: local < host scram < hostssl < host reject |
| `TestFunctional_Scenario44_CustomRules_HashAnnotation` | `avsoft.io/config-hash` annotation present and non-empty |
| `TestFunctional_Scenario44_CustomRules_NoDefaults` | Default-only rules absent when custom rules are set |
| `TestFunctional_Scenario44_CustomRules_LocalTrust` | `local all gpadmin trust` rule present |
| `TestFunctional_Scenario44_CustomRules_HostScram` | `host all all 10.0.0.0/8 scram-sha-256` rule present |
| `TestFunctional_Scenario44_CustomRules_HostSSL` | `hostssl all all 192.168.0.0/16 scram-sha-256` rule present |
| `TestFunctional_Scenario44_CustomRules_HostReject` | `host all all 0.0.0.0/0 reject` rule present |
| `TestFunctional_Scenario44_UpdateRules_ConfigMapUpdated` | Updated rules present; old rules absent; hash changed |
| `TestFunctional_Scenario44_UpdateRules_HashChanged` | Config hash annotation changes after HBA rules update |
| `TestFunctional_Scenario44_HBACustomRuleCases` | All 5 cases from `HBACustomRuleCases()` catalog (5 sub-tests) |

**E2E tests (10 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario44_CustomRules_ConfigMap` | ConfigMap created with 4 rules and hash annotation |
| `TestE2E_Scenario44_CustomRules_AllRulesPresent` | All 4 custom rules present; default-only rules absent |
| `TestE2E_Scenario44_UpdateRules` | Rules updated, old rules removed, hash changed, rule count updated |
| `TestE2E_Scenario44_ClusterCRAccepted` | Cluster CR with custom HBA rules accepted; 4 rules preserved in spec |
| `TestE2E_Scenario44_HBACustomRuleCases` | All 5 cases from `HBACustomRuleCases()` catalog (5 sub-tests) |

```bash
# Run custom HBA rules functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario44

# Run custom HBA rules E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario44
```

#### Scenario 45 — Default HBA Rules with Real Cloudberry Cluster

Tests that when a `CloudberryCluster` is deployed with no `hbaRules` in the spec, the operator generates the correct default `pg_hba.conf` rules. Also verifies that custom rules override defaults, empty rules trigger defaults, rule ordering is correct, and the ConfigMap has proper ownership metadata.

- **Default rule generation**: When `spec.auth.hbaRules` is omitted or empty, the Auth Reconciler generates a `pg_hba.conf` ConfigMap containing exactly 5 default rules: local trust for gpadmin, local scram-sha-256 for all users, host trust for gpadmin from localhost, host scram-sha-256 for all users, and host replication scram-sha-256
- **Rule ordering**: Local rules appear before host rules in the generated `pg_hba.conf`
- **Custom override**: When explicit `hbaRules` are provided, defaults are not generated — only custom rules appear
- **Empty slice**: An empty `hbaRules: []` triggers default rule generation (same behavior as omitted)
- **Behavioral verification**: Each connection type (local gpadmin, local other, host gpadmin localhost, host any, replication) maps to the correct authentication method
- **ConfigMap ownership**: The generated ConfigMap has proper labels and a config hash annotation
- **Test case catalog**: `HBADefaultRuleCase` type and `HBADefaultRuleCases()` function in `test/cases/test_cases.go`
- **Example CR**: `test/examples/scenario45-hba-defaults.yaml`
- **Functional tests**: `test/functional/scenario45_hba_defaults_test.go`
- **E2E tests**: `test/e2e/scenario45_hba_defaults_e2e_test.go`

**Expected default rules:**

```
local   all   gpadmin                 trust
local   all   all                     scram-sha-256
host    all   gpadmin   127.0.0.1/32  trust
host    all   all       0.0.0.0/0     scram-sha-256
host    replication  all  0.0.0.0/0   scram-sha-256
```

**Behavioral verification matrix:**

| Connection Type | User | Source | Auth Method | Password Required |
|----------------|------|--------|-------------|-------------------|
| `local` | `gpadmin` | Unix socket | `trust` | No |
| `local` | Any other user | Unix socket | `scram-sha-256` | Yes |
| `host` | `gpadmin` | `127.0.0.1/32` | `trust` | No |
| `host` | Any user | `0.0.0.0/0` | `scram-sha-256` | Yes |
| `host` (replication) | Any user | `0.0.0.0/0` | `scram-sha-256` | Yes |

**Functional tests (11 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestFunctional_Scenario45_NoHBARules_GeneratesDefaults` | No hbaRules → all 5 default lines present in ConfigMap |
| `TestFunctional_Scenario45_DefaultRuleOrder` | Local rules appear before host rules |
| `TestFunctional_Scenario45_ReplicationRulePresent` | Replication rule present in defaults |
| `TestFunctional_Scenario45_GpadminTrustLocal` | Local gpadmin uses trust |
| `TestFunctional_Scenario45_AllUsersScramLocal` | Local all users use scram-sha-256 |
| `TestFunctional_Scenario45_GpadminTrustLocalhost` | Host gpadmin from 127.0.0.1/32 uses trust |
| `TestFunctional_Scenario45_AllUsersScramRemote` | Host all users from 0.0.0.0/0 use scram-sha-256 |
| `TestFunctional_Scenario45_ReplicationScram` | Host replication from 0.0.0.0/0 uses scram-sha-256 |
| `TestFunctional_Scenario45_CustomRulesOverrideDefaults` | Custom rules replace defaults entirely |
| `TestFunctional_Scenario45_BehavioralVerification` | All 5 connection types map to correct auth methods; exactly 5 rule lines |
| `TestFunctional_Scenario45_EmptyHBARules_GeneratesDefaults` | Empty hbaRules slice triggers default generation |

**E2E tests (5 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestE2E_Scenario45_HBADefaults_NoRulesGeneratesDefaults` | Full reconciliation with no hbaRules → 5 default rules, exactly 5 lines |
| `TestE2E_Scenario45_HBADefaults_BehavioralVerification` | All connection types verified; rule ordering confirmed |
| `TestE2E_Scenario45_HBADefaults_CustomRulesOverride` | Custom rules present, defaults excluded |
| `TestE2E_Scenario45_HBADefaults_EmptyRulesGeneratesDefaults` | Empty slice → defaults generated |
| `TestE2E_Scenario45_HBADefaults_ConfigMapOwnership` | ConfigMap name, labels, and config hash annotation verified |

#### Scenario 46 — Vault Integration (All Auth Methods + Secrets)

Tests the operator's Vault integration across all authentication methods, secret paths, secret rotation, and connection retry behavior. Verified against a real running Kubernetes cluster with a real HashiCorp Vault instance.

- **Token auth (46a, 46b)**: Operator authenticates to Vault using a static token and reads secrets from all 4 KV paths (`admin-password`, `oidc-secret`, `monitoring-password`, `tls`). API returns HTTP 200 for all paths. Sub-scenario 46b explicitly tests the static token path in Vault dev mode
- **AppRole auth (46c)**: AppRole enabled in Vault, role created, `role_id` and `secret_id` obtained, login successful with client token returned via `auth/approle/login`
- **Secret rotation watch (46d)**: Admin password updated directly in Vault. `SecretWatcher` detects change via SHA-256 hash comparison. `onChange` callback invoked, confirming the rotation mechanism works end-to-end
- **Connection retry (46e)**: Validates `DefaultRetryOptions` configuration: `MaxRetries=5`, `InitialBackoff=1s`, `MaxBackoff=30s`, `Multiplier=2.0`, `JitterFraction=0.1`
- **KV secret paths**: All 4 paths verified — `secret/data/cloudberry/admin-password`, `secret/data/cloudberry/oidc-secret`, `secret/data/cloudberry/monitoring-password`, `secret/data/cloudberry/tls`
- **Mock Vault server**: Functional tests use a mock Vault HTTP server (`httptest.Server`) that handles token auth, AppRole login, KV v2 reads/writes, and secret versioning
- **Test case catalog**: `VaultIntegrationCase` type and `VaultIntegrationCases()` function in `test/cases/test_cases.go` (5 cases)
- **Example CR**: `test/examples/scenario46-vault-integration.yaml`
- **Functional tests**: `test/functional/scenario46_vault_integration_test.go`
- **E2E tests**: `test/e2e/scenario46_vault_integration_e2e_test.go`

**Real-cluster verification results**:

- Operator deployed with Vault token auth + webhooks + vault-PKI
- Scenario 1 cluster deployed, 10 pods running, 2000 rows inserted and queried
- All 4 Vault KV paths readable via token auth
- AppRole login successful with client token returned
- Secret rotation detected via hash comparison, `onChange` callback invoked
- Retry configuration confirmed

**Functional tests (9 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestFunctional_Scenario46_TokenAuth_ReadAllSecrets` | Token auth reads all 4 KV paths; each secret contains expected keys |
| `TestFunctional_Scenario46_TokenAuth_DevMode` | Static token in dev mode authenticates and reads all 4 paths |
| `TestFunctional_Scenario46_AppRoleAuth` | AppRole login returns client token; authenticated client reads secrets |
| `TestFunctional_Scenario46_SecretRotationWatch` | SecretWatcher detects hash change after secret update; onChange callback invoked |
| `TestFunctional_Scenario46_ConnectionRetry` | RetryWithBackoff retries failing operations with exponential backoff |
| `TestFunctional_Scenario46_DefaultRetryOptions` | DefaultRetryOptions match expected values (MaxRetries=5, etc.) |
| `TestFunctional_Scenario46_VaultClientCreation` | Vault client created with correct config for each auth method |
| `TestFunctional_Scenario46_SecretWriteAndRead` | Write secret to Vault, read it back, verify data matches |
| `TestFunctional_Scenario46_CasesCatalog` | All 5 cases from `VaultIntegrationCases()` catalog executed |

**E2E tests (10 test cases, 9 PASS, 1 SKIP):**

| Test | What It Verifies |
|------|-----------------|
| `TestE2E_Scenario46_TokenAuth` | Real Vault token auth, write and read all 4 KV paths |
| `TestE2E_Scenario46_TokenAuth_AllPaths` | All 4 secret paths readable with correct data |
| `TestE2E_Scenario46_AppRoleAuth` | Real AppRole login with role_id and secret_id |
| `TestE2E_Scenario46_SecretRotation` | Real secret update detected by SecretWatcher |
| `TestE2E_Scenario46_RetryConfig` | DefaultRetryOptions confirmed against real Vault |
| `TestE2E_Scenario46_VaultHealth` | Vault health endpoint returns initialized and unsealed |
| `TestE2E_Scenario46_KVEngineEnabled` | KV v2 engine mounted at `secret/` |
| `TestE2E_Scenario46_ClusterCRWithVault` | Cluster CR with Vault config persists correctly |
| `TestE2E_Scenario46_CasesCatalog` | All 5 cases from `VaultIntegrationCases()` executed in E2E context |
| `TestE2E_Scenario46_PKICertIssuance` | SKIP — PKI cert issuance when Vault PKI role is not configured |

```bash
# Run Vault integration functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario46

# Run Vault integration E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario46
```

#### Scenario 47 — SSL/TLS Configuration Verification

Tests the operator's SSL/TLS configuration across two certificate sources: Kubernetes Secrets (47a) and Vault PKI (47b). Verifies `postgresql.conf` SSL settings, TLS volume mounting on StatefulSets, `hostssl` HBA rules, Vault PKI certificate issuance, certificate rotation at 2/3 lifetime, and self-signed certificate generation.

- **47a — K8s Secret source**: Deploys a cluster with `auth.ssl.enabled: true`, `certSecret.name: cloudberry-tls`, `minTLSVersion: "1.2"`. Verifies `postgresql.conf` contains all 5 SSL settings (`ssl = on`, `ssl_cert_file`, `ssl_key_file`, `ssl_ca_file`, `ssl_min_protocol_version`). Verifies TLS volume sourced from the cert Secret is mounted at `/tls` (read-only) on all StatefulSets. Verifies `hostssl` HBA rules are rendered correctly in `pg_hba.conf`. Tests both TLS 1.2 and 1.3 minimum versions. Verifies SSL disabled produces no SSL settings
- **47b — Vault PKI source**: Tests certificate issuance from a mock Vault PKI server (`pki/issue/cloudberry-operator`). Verifies response contains `certificate`, `private_key`, `issuing_ca` as PEM-encoded data. Tests certificate rotation threshold — certificates past 2/3 of their lifetime trigger `NeedsRotation()`. Tests self-signed certificate generation with `IsCA=true` CA cert and server cert with correct DNS SANs (`{service}.{namespace}.svc`, `{service}.{namespace}.svc.cluster.local`). Tests `EnsureCertificates()` idempotency and `kubernetes.io/tls` Secret creation
- **Test case catalog**: `SSLConfigCase` type and `SSLConfigCases()` function in `test/cases/test_cases.go` (4 cases)
- **Builder method**: `WithSSLMinTLSVersion()` added to `test/testutil/fixtures.go`
- **Example CRs**: `test/examples/scenario47a-ssl-k8s-secret.yaml`, `test/examples/scenario47b-ssl-vault-pki.yaml`
- **Functional tests**: `test/functional/scenario47_ssl_tls_test.go`
- **E2E tests**: `test/e2e/scenario47_ssl_tls_e2e_test.go`

**Bug fix — TLS private key permissions**:

During real-cluster testing, PostgreSQL rejected the TLS private key because Kubernetes Secret volumes mount files as symlinks with `0777` permissions. PostgreSQL requires `0600` on the key file and fails with: `FATAL: private key file "/tls/tls.key" has group or world access`.

The fix (in `internal/builder/builder.go`) uses a two-volume approach with an `init-tls` init container:

1. `tls-secret` volume: K8s Secret mounted at `/tls-secret` (read-only, symlinked)
2. `tls` volume: EmptyDir mounted at `/tls`
3. `init-tls` init container: Copies certs from `/tls-secret` to `/tls` with ownership `gpadmin:gpadmin` (UID 1000), key permissions `0600`, cert permissions `0644`

Files modified: `internal/builder/builder.go`, `internal/builder/builder_test.go`, `test/functional/scenario47_ssl_tls_test.go`, `test/e2e/scenario47_ssl_tls_e2e_test.go`.

**Real-cluster verification results (47a — K8s Secret with init container fix)**:

| Check | Result |
|-------|--------|
| `SHOW ssl;` → `on` | ✅ |
| `SHOW ssl_cert_file;` → `/tls/tls.crt` | ✅ |
| `SHOW ssl_key_file;` → `/tls/tls.key` | ✅ |
| `SHOW ssl_ca_file;` → `/tls/ca.crt` | ✅ |
| `SHOW ssl_min_protocol_version;` → `TLSv1.2` | ✅ |
| `tls.key` owned by gpadmin with `0600` permissions | ✅ |
| `tls.crt` and `ca.crt` with `0644` permissions | ✅ |
| Database `mydb` created, 100 rows inserted, SELECT aggregates work | ✅ |
| `pg_hba.conf` contains `hostssl` rule | ✅ |

**Real-cluster verification results (47b — Vault PKI)**:

| Check | Result |
|-------|--------|
| Vault PKI issues certs (`certificate`, `private_key`, `issuing_ca`) | ✅ |
| Operator webhook cert Secret exists (`kubernetes.io/tls`) | ✅ |
| Cert rotation at 2/3 of certificate lifetime | ✅ |

**Functional tests (16 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestFunctional_Scenario47a_SSLEnabled_PostgresqlConf` | SSL enabled → all 5 SSL settings present in postgresql.conf |
| `TestFunctional_Scenario47a_SSLEnabled_TLSVolume` | TLS volume sourced from cert Secret, mounted at `/tls` read-only |
| `TestFunctional_Scenario47a_SSLEnabled_MinTLS12` | `minTLSVersion: "1.2"` → `ssl_min_protocol_version = 'TLSv1.2'` |
| `TestFunctional_Scenario47a_SSLEnabled_MinTLS13` | `minTLSVersion: "1.3"` → `ssl_min_protocol_version = 'TLSv1.3'`, no TLSv1.2 |
| `TestFunctional_Scenario47a_SSLDisabled_NoSSLInConf` | SSL disabled → no SSL settings in postgresql.conf |
| `TestFunctional_Scenario47a_SSLEnabled_NoCertSecret` | SSL enabled without certSecret → no TLS volume, mount still present |
| `TestFunctional_Scenario47a_HostSSLRule` | `hostssl` HBA rule rendered correctly in pg_hba.conf with SSL enabled |
| `TestFunctional_Scenario47a_SSLConfigCases` | 4 cases from `SSLConfigCases()` catalog executed |
| `TestFunctional_Scenario47b_VaultPKI_CertIssuance` | Mock Vault PKI issues cert with `certificate`, `private_key`, `issuing_ca` |
| `TestFunctional_Scenario47b_VaultPKI_CertRotation` | Near-expiry cert (past 2/3 threshold) triggers rotation |
| `TestFunctional_Scenario47b_SelfSigned_CertGeneration` | Self-signed CA with `IsCA=true`, server cert with correct DNS SANs |
| `TestFunctional_Scenario47b_CertManager_EnsureCertificates` | Secret created with `kubernetes.io/tls` type, idempotent on second call |

**E2E tests (12 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestE2E_Scenario47a_SSLConfig_PostgresqlConf` | All 5 SSL settings present in postgresql.conf |
| `TestE2E_Scenario47a_SSLConfig_TLSVolume` | TLS volume and mount verified on coordinator StatefulSet |
| `TestE2E_Scenario47a_SSLConfig_MinTLSVersions` | Both TLS 1.2 and 1.3 minimum versions verified (2 subtests) |
| `TestE2E_Scenario47a_SSLConfig_HostSSLRule` | hostssl HBA rule reconciled correctly via AuthReconciler |
| `TestE2E_Scenario47b_VaultPKI_SelfSignedFallback` | Self-signed cert generated with valid CA and server cert |
| `TestE2E_Scenario47b_VaultPKI_CertRotationCheck` | Rotation detected, regeneration succeeds, fresh cert does not need rotation |
| `TestE2E_Scenario47_SSLConfigCases` | 4 cases from `SSLConfigCases()` catalog executed in E2E context |
| `TestE2E_Scenario47_ClusterWithSSL` | Cluster CR with SSL config persists correctly in K8s |

```bash
# Run SSL/TLS functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario47

# Run SSL/TLS E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario47
```

#### Scenario 48 — Webhook Certificate Management Verification

Tests the operator's webhook certificate management across two certificate sources: Vault PKI (48a) and self-signed (48b). Verifies certificate issuance, Kubernetes Secret creation, webhook configuration patching with `caBundle`, certificate rotation detection, and Helm auto-generation of Secret and service names.

- **48a — Vault PKI cert source**: Operator authenticates to Vault with token auth, requests certificate from `pki/issue/cloudberry-operator` with correct CN (`{service}.{namespace}.svc`) and SANs (`.svc` and `.svc.cluster.local`). Certificate stored in `kubernetes.io/tls` Secret with `tls.crt`, `tls.key`, `ca.crt`. Both validating and mutating webhook configurations patched with `caBundle`. All `CLOUDBERRY_WEBHOOK_*` environment variables verified
- **48b — Self-signed cert source**: Operator generates ECDSA P-256 CA (10-year validity, CA:TRUE, pathlen:0) and server cert (1-year validity, CA:FALSE) with correct SANs. Secret created with all 3 keys. Webhook functional — CR accepted
- **Certificate rotation**: Background goroutine checks every 12 hours. Rotation threshold at 2/3 of certificate lifetime. `checkCertRotation()` correctly detects near-expiry certs
- **Helm auto-generation**: `certSecretName` auto-generated as `{release}-webhook-certs`, `serviceName` auto-generated as `{release}-webhook`, empty `caBundle` triggers runtime injection
- **Test case catalog**: `WebhookCertCase` type and `WebhookCertCases()` function in `test/cases/test_cases.go`
- **Example CRs**: `test/examples/scenario48a-webhook-vault-pki.yaml`, `test/examples/scenario48b-webhook-self-signed.yaml`
- **Functional tests**: `test/functional/scenario48_webhook_certs_test.go`
- **E2E tests**: `test/e2e/scenario48_webhook_certs_e2e_test.go`

**Bug fix 1 — Vault client wiring in `setupWebhookCerts()`**:

During real-cluster testing, `setupWebhookCerts()` in `cmd/operator/main.go` passed `nil` for the vault client to `certmanager.New()`. When `certSource=vault-pki`, the certmanager failed with "vault client is not enabled". The fix adds vault client creation when `cfg.WebhookCertSource == "vault-pki"`, mapping `config.VaultConfig` to `vault.Config` and creating a real vault client.

**Bug fix 2 — Missing viper config defaults**:

Viper config defaults were missing for `vault.address`, `vault.token`, `vault.role`, `vault.auth-path`, and OIDC fields. Without defaults, viper's `AutomaticEnv()` couldn't bind these env vars, so they were always empty even when set via Helm. The fix adds `viper.SetDefault()` calls in `internal/config/config.go` for all vault and OIDC fields.

**ECDSA vs RSA note**: The specification describes RSA 4096-bit CA and RSA 2048-bit server keys, but the implementation uses ECDSA P-256 for both. ECDSA P-256 provides equivalent security to RSA 3072-bit with smaller keys and faster operations.

**Real-cluster verification results**:

| Sub-Scenario | Checks | Result |
|-------------|--------|--------|
| 48a — Vault PKI (token auth) | Vault auth, cert issuance, CN/SANs, Secret, webhook patching (1524-byte caBundle), env vars, webhook functional | All ✅ |
| 48a-k8s — Vault PKI (k8s auth) | K8s SA token auth to Vault, cert issuance from Vault PKI, CN/SANs, Secret, webhook patching (1142-byte caBundle), webhook functional, data operations (3100 rows) | All ✅ |
| 48b — Self-signed | CA properties (ECDSA P-256, 10yr, CA:TRUE), server cert (1yr, CA:FALSE), SANs, Secret, webhook functional | All ✅ |
| Rotation | 12-hour check interval, 2/3 lifetime threshold, near-expiry detection | All ✅ |
| Helm | Auto-generated Secret/service names, runtime caBundle injection | All ✅ |

**48a-k8s — Kubernetes auth real-cluster verification**:

The Kubernetes auth method was verified on a real Docker Desktop cluster with the following configuration:

- **Vault k8s auth backend**: `kubernetes_host: https://kubernetes.docker.internal:6443` (Docker Desktop specific — the k8s API cert has `kubernetes.docker.internal` as a SAN but NOT `host.docker.internal`), `disable_iss_validation: true`, `disable_local_ca_jwt: true`
- **Dedicated service account**: `vault-auth` in `cloudberry-system` with `system:auth-delegator` ClusterRole for TokenReview API access
- **Vault role**: `cloudberry-operator` bound to SA `cloudberry-operator` in namespace `cloudberry-system` with policies `["default", "cloudberry-pki"]`
- **PKI role**: `cloudberry-operator` with `allow_any_name: true`
- **Operator deployed with**: `vault.authMethod=kubernetes`, `vault.authPath=auth/kubernetes`, `vault.role=cloudberry-operator`, `webhook.certSource=vault-pki`

| Check | Evidence | Result |
|-------|----------|--------|
| Operator authenticates via k8s SA token | Log: `"authenticated with vault using kubernetes method"` | ✅ |
| Vault client uses k8s auth | Log: `authMethod: "kubernetes"` | ✅ |
| Webhook cert issued from Vault PKI | CN=`cloudberry-operator-webhook.cloudberry-system.svc`, Issuer=`Test Root CA` | ✅ |
| SANs correct | `.svc` and `.svc.cluster.local` | ✅ |
| Cert stored in K8s Secret | `cloudberry-operator-webhook-certs` with `tls.crt`, `tls.key`, `ca.crt` | ✅ |
| Both webhook configs patched | caBundle present (1142 bytes) | ✅ |
| Webhook functional | CR `scenario48-k8s-auth-test` accepted | ✅ |
| Data operations | 3100 rows in mydb accessible | ✅ |
| Env vars | `CLOUDBERRY_VAULT_AUTH_METHOD=kubernetes`, all WEBHOOK vars set | ✅ |

**Docker Desktop hostname bug**: The Vault k8s auth backend must use `kubernetes.docker.internal` (not `host.docker.internal`) because the Kubernetes API server certificate only includes `kubernetes.docker.internal` as a SAN. Using `host.docker.internal` causes TLS verification failures during TokenReview API calls.

**Functional tests (9 test cases):**

| Test | What It Verifies |
|------|-----------------|
| Vault PKI cert issuance | Certificate requested from Vault PKI with correct CN and SANs |
| Vault PKI Secret creation | `kubernetes.io/tls` Secret with `tls.crt`, `tls.key`, `ca.crt` |
| Vault PKI webhook patching | Both webhooks patched with `caBundle` |
| Self-signed CA generation | ECDSA P-256 CA with 10-year validity, CA:TRUE, pathlen:0 |
| Self-signed server cert | ECDSA P-256 server cert with 1-year validity, CA:FALSE |
| Self-signed SANs | `.svc` and `.svc.cluster.local` SANs present |
| Self-signed Secret creation | `kubernetes.io/tls` Secret with all 3 keys |
| Cert rotation detection | Near-expiry cert triggers `NeedsRotation()` |
| Fresh cert no rotation | Fresh cert does not trigger rotation |

**E2E tests (7 test cases):**

| Test | What It Verifies |
|------|-----------------|
| Vault PKI end-to-end | Full Vault PKI cert lifecycle with real Vault |
| Self-signed end-to-end | Full self-signed cert lifecycle |
| Webhook functional with Vault PKI | CR accepted by webhook using Vault PKI certs |
| Webhook functional with self-signed | CR accepted by webhook using self-signed certs |
| Cert rotation check | Rotation detected for near-expiry certs |
| Secret contents | All required keys present in cert Secret |
| Helm auto-generation | Secret and service names auto-generated correctly |

```bash
# Run webhook cert functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario48

# Run webhook cert E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario48
```

#### Scenario 49 — cloudberry-ctl Authentication

Tests the `cloudberry-ctl` authentication commands (`auth login`, `auth status`, `auth logout`) against a mock operator API server, verifying credential validation, status reporting, and logout behavior.

- **Basic login (49b)**: `auth login --basic` validates credentials by calling `GET /api/v1alpha1/clusters` with HTTP Basic auth. Valid credentials print `Login successful (method=basic, user=<username>)`. Invalid credentials return an error and exit with code 3 (authentication failure)
- **OIDC login with credentials (49a)**: `auth login` (without `--basic`) with `--username` and `--password` simulates the OIDC resource owner password grant. Valid credentials print `Login successful (method=oidc, user=<username>)`. Without credentials, the browser-based flow returns `"not yet implemented"`
- **Auth status (49c)**: `auth status` checks connectivity and authentication by calling `GET /api/v1alpha1/clusters`. Returns a JSON/table response with `auth_method`, `username`, `operator_url`, and `authenticated` fields. Unauthenticated state is reported in the output (with an `error` field), not as a command error — the command always exits with code 0
- **Logout (49d)**: `auth logout` prints `"Logged out. Cached credentials have been cleared."` and reminds the user to unset `CLOUDBERRY_USERNAME` and `CLOUDBERRY_PASSWORD` environment variables
- **Test case catalog**: `CTLAuthCase` type and `CTLAuthCases()` function in `test/cases/test_cases.go` (6 cases)
- **Example CR**: `test/examples/scenario49-ctl-auth.yaml`
- **Functional tests**: `test/functional/scenario49_ctl_auth_test.go`
- **E2E tests**: `test/e2e/scenario49_ctl_auth_e2e_test.go`

**Real-cluster verification results (7/8 PASS)**:

Test environment: Vault, VictoriaMetrics, MinIO, Keycloak, Kafka, RabbitMQ — all running.

| # | Test | Result |
|---|------|--------|
| 49b | Basic login with correct password | ✅ `Login successful (method=basic, user=admin)` |
| 49b | Basic login with wrong password | ✅ Rejected (exit code 3) |
| 49c | Auth status (authenticated) | ✅ Shows `authenticated: true` |
| 49c | Auth status (unauthenticated) | ✅ Shows `authenticated: false` with error |
| 49d | Logout | ✅ `Logged out. Cached credentials have been cleared.` |
| 49a | OIDC login (with credentials) | ✅ `Login successful (method=oidc, user=admin)` |
| — | Cluster status after auth | ✅ Shows Running cluster |
| — | Data ops | ✅ 50 rows in mydb |

**Functional tests (7 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestFunctional_Scenario49a_LoginOIDC` | OIDC login without credentials returns not-implemented error |
| `TestFunctional_Scenario49b_LoginBasic` | Basic login with valid credentials succeeds, output contains username |
| `TestFunctional_Scenario49b_LoginBasic_InvalidPassword` | Basic login with wrong password fails with "login failed" error |
| `TestFunctional_Scenario49c_AuthStatus` | Auth status with valid credentials shows `authenticated` and `basic` |
| `TestFunctional_Scenario49c_AuthStatus_Unauthenticated` | Auth status with invalid credentials shows `authenticated` (no command error) |
| `TestFunctional_Scenario49d_Logout` | Logout prints "Logged out" and mentions `CLOUDBERRY_PASSWORD` |
| `TestFunctional_Scenario49_CTLAuthCases` | All 6 cases from `CTLAuthCases()` catalog validated |

**E2E tests (8 test cases):**

| Test | What It Verifies |
|------|-----------------|
| `TestE2E_Scenario49a_LoginOIDC` | OIDC login not-implemented in E2E context |
| `TestE2E_Scenario49b_LoginBasic` | Basic login succeeds with mock server |
| `TestE2E_Scenario49b_LoginBasic_InvalidPassword` | Basic login fails with wrong password |
| `TestE2E_Scenario49c_AuthStatus` | Auth status shows connectivity and auth method |
| `TestE2E_Scenario49d_Logout` | Logout clears state and prints reminder |
| `TestE2E_Scenario49_CTLAuthCasesCatalog` | All 6 cases from `CTLAuthCases()` catalog in E2E context |
| `TestE2E_Scenario49_ClusterCRWithAuthConfig` | Cluster CR with basic auth config persists; phase=Running |

```bash
# Run CTL auth functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario49

# Run CTL auth E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario49
```

#### Scenario 50 — Auditing (All Categories)

Tests auditing across three categories: connection auditing configuration, statement auditing configuration, and operator audit log format. Includes 31 tests (17 functional + 14 E2E).

- **50a — Connection auditing config**: Verifies that `log_connections = 'on'` and `log_disconnections = 'on'` appear in the generated `postgresql.conf` ConfigMap when configured. Verifies the ConfigMap has an `avsoft.io/config-hash` annotation. Verifies no audit params appear when not configured
- **50b — Statement auditing config**: Verifies that `log_statement = 'ddl'`, `log_min_duration_statement = '1000'`, and `log_duration = 'on'` appear in the ConfigMap. Verifies all statement audit params appear together. Verifies parameters are rendered in sorted alphabetical order (`log_duration` < `log_min_duration_statement` < `log_statement`). Verifies the full scenario config (all 5 audit params) with the `# User-defined parameters` section header
- **50c — Operator audit log format**: Verifies that successful basic auth produces a structured JSON log entry with `username`, `method`, `source_ip`, and `permission` fields. Verifies that failed auth produces a log entry with `method`, `error`, and `remote_addr` fields. Verifies that permission denied events are logged with `username`, `method`, `source_ip`, `required_permission`, `actual_permission`, `path`, and `http_method`. Verifies that config changes are audit-logged with `cluster`, `username`, `method`, and `source_ip`. Verifies that role assignments are audit-logged with `cluster`, `group`, `role`, `username`, `method`, and `source_ip`. Verifies all log entries are valid JSON with `level` and `msg` fields
- **Test case catalog**: `AuditCase` type and `AuditCases()` function in `test/cases/test_cases.go` (11 cases: 1 connection, 3 statement, 7 operator)
- **Example CR**: `test/examples/scenario50-auditing.yaml`
- **Functional tests**: `test/functional/scenario50_auditing_test.go`
- **E2E tests**: `test/e2e/scenario50_auditing_e2e_test.go`

**CR spec used:**

```yaml
config:
  parameters:
    log_connections: "on"
    log_disconnections: "on"
    log_statement: "ddl"
    log_min_duration_statement: "1000"
    log_duration: "on"
```

**Functional tests (17 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario50a_ConnectionAudit_ConfigMap` | `log_connections` and `log_disconnections` present in postgresql.conf |
| `TestFunctional_Scenario50a_ConnectionAudit_HashAnnotation` | ConfigMap has `avsoft.io/config-hash` annotation |
| `TestFunctional_Scenario50a_ConnectionAudit_NoParams` | No audit params when not configured |
| `TestFunctional_Scenario50b_StatementAudit_DDL` | `log_statement = 'ddl'` present |
| `TestFunctional_Scenario50b_StatementAudit_Duration` | `log_min_duration_statement` and `log_duration` present |
| `TestFunctional_Scenario50b_StatementAudit_AllParams` | All 3 statement audit params together |
| `TestFunctional_Scenario50b_StatementAudit_ParametersSorted` | Parameters in alphabetical order |
| `TestFunctional_Scenario50b_StatementAudit_FullScenarioConfig` | All 5 audit settings with section header |
| `TestFunctional_Scenario50c_OperatorAudit_BasicAuthSuccess` | Success log with `username`, `method`, `source_ip`, and `permission` |
| `TestFunctional_Scenario50c_OperatorAudit_BasicAuthFailure` | Failure log with `method` and `error` |
| `TestFunctional_Scenario50c_OperatorAudit_PermissionDenied` | Permission denied logged with user context AND 403 response |
| `TestFunctional_Scenario50c_OperatorAudit_JSONFormat` | All log entries valid JSON |
| `TestFunctional_Scenario50c_OperatorAudit_SuccessLogFields` | Success entry structured fields (including `method`, `source_ip`) verified |
| `TestFunctional_Scenario50c_OperatorAudit_FailureLogFields` | Failure entry structured fields verified |
| `TestFunctional_Scenario50c_OperatorAudit_ConfigChange` | Config change audit log with `cluster`, `username`, `method`, `source_ip` |
| `TestFunctional_Scenario50c_OperatorAudit_RoleAssignment` | Role assignment audit log with `cluster`, `group`, `role`, `username`, `method`, `source_ip` |
| `TestFunctional_Scenario50_AuditCases_Coverage` | All 11 audit cases from catalog verified |

**E2E tests (14 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario50a_ConnectionAudit_ConfigMap` | Connection audit settings end-to-end |
| `TestE2E_Scenario50a_ConnectionAudit_HashAnnotation` | Hash annotation end-to-end |
| `TestE2E_Scenario50b_StatementAudit_DDL` | DDL statement audit end-to-end |
| `TestE2E_Scenario50b_StatementAudit_Duration` | Duration audit settings end-to-end |
| `TestE2E_Scenario50b_StatementAudit_FullScenarioConfig` | Full config end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_BasicAuthSuccess` | Auth success log with `method`, `source_ip` end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_BasicAuthFailure` | Auth failure log end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_PermissionDenied` | Permission denied logged with user context end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_JSONFormat` | JSON format end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_SuccessLogFields` | Success fields (including `method`, `source_ip`) end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_FailureLogFields` | Failure fields end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_ConfigChange` | Config change audit log with user context end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_RoleAssignment` | Role assignment audit log with user context end-to-end |
| `TestE2E_Scenario50_AuditCases_Coverage` | All 11 audit cases end-to-end |

```bash
# Run auditing functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario50

# Run auditing E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario50
```

#### Scenario 51 — Security Headers

Tests that all 8 security headers are present with exact values on every API response, regardless of endpoint, HTTP method, or response status code. The `SecurityHeaders` middleware is applied as the outermost middleware wrapping the entire mux in `server.Handler()`. No production code changes were needed — the middleware was already fully implemented in `internal/auth/middleware.go`. Includes 21 tests (9 functional + 7 E2E mock + 5 E2E real cluster).

- **Headers verified**: `Cache-Control`, `Content-Security-Policy`, `Permissions-Policy`, `Referrer-Policy`, `Strict-Transport-Security`, `X-Content-Type-Options`, `X-Frame-Options`, `X-XSS-Protection`
- **Response types verified**: 200 OK (health, authenticated GET, authenticated POST), 401 Unauthorized, 403 Forbidden, 404 Not Found
- **Consistency check**: Same header values across all endpoints simultaneously
- **Real cluster verification**: Headers verified on an API server backed by a real Cloudberry database connection
- **Test case catalog**: `SecurityHeaderCase` type and `SecurityHeaderCases()` function in `test/cases/test_cases.go` (8 cases)
- **Example CR**: `test/examples/scenario51-security-headers.yaml`
- **Functional tests**: `test/functional/scenario51_security_headers_test.go`
- **E2E tests**: `test/e2e/scenario51_security_headers_e2e_test.go`

**Test case catalog (8 SecurityHeaderCase entries):**

| Case Name | Header | Expected Value |
|-----------|--------|----------------|
| `cache_control` | Cache-Control | `no-store` |
| `content_security_policy` | Content-Security-Policy | `default-src 'self'` |
| `permissions_policy` | Permissions-Policy | `camera=(), microphone=()` |
| `referrer_policy` | Referrer-Policy | `strict-origin-when-cross-origin` |
| `strict_transport_security` | Strict-Transport-Security | `max-age=31536000; includeSubDomains` |
| `x_content_type_options` | X-Content-Type-Options | `nosniff` |
| `x_frame_options` | X-Frame-Options | `DENY` |
| `x_xss_protection` | X-XSS-Protection | `1; mode=block` |

**Functional tests (9 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario51_AllHeaders_HealthEndpoint` | All 8 headers present on `GET /healthz` (200, no auth) |
| `TestFunctional_Scenario51_AllHeaders_AuthenticatedGET` | All 8 headers present on `GET /api/v1alpha1/clusters` (200, admin auth) |
| `TestFunctional_Scenario51_AllHeaders_AuthenticatedPOST` | All 8 headers present on `POST /api/v1alpha1/clusters` (admin auth) |
| `TestFunctional_Scenario51_AllHeaders_UnauthorizedResponse` | All 8 headers present on 401 Unauthorized (no auth header) |
| `TestFunctional_Scenario51_AllHeaders_ForbiddenResponse` | All 8 headers present on 403 Forbidden (viewer tries POST) |
| `TestFunctional_Scenario51_AllHeaders_NotFoundResponse` | All 8 headers present on 404 Not Found |
| `TestFunctional_Scenario51_AllHeaders_ReadyzEndpoint` | All 8 headers present on `GET /readyz` (200, no auth) |
| `TestFunctional_Scenario51_SecurityHeaderCases_Coverage` | `SecurityHeaderCases()` returns exactly 8 cases with non-empty fields |
| `TestFunctional_Scenario51_HeadersConsistentAcrossEndpoints` | Same header values on `/healthz`, `/readyz`, authenticated GET, and error POST |

**E2E tests — mock (7 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario51_AllHeaders_HealthEndpoint` | All 8 headers on `GET /healthz` end-to-end |
| `TestE2E_Scenario51_AllHeaders_AuthenticatedGET` | All 8 headers on authenticated GET end-to-end |
| `TestE2E_Scenario51_AllHeaders_UnauthorizedResponse` | All 8 headers on 401 response end-to-end |
| `TestE2E_Scenario51_AllHeaders_ForbiddenResponse` | All 8 headers on 403 response end-to-end |
| `TestE2E_Scenario51_AllHeaders_ErrorResponse` | All 8 headers on 404 response end-to-end |
| `TestE2E_Scenario51_HeadersConsistentAcrossEndpoints` | Consistent headers across multiple endpoints end-to-end |
| `TestE2E_Scenario51_SecurityHeaderCases_Coverage` | All 8 cases from catalog verified end-to-end |

**E2E tests — real cluster (5 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario51_RealCluster_HealthEndpoint` | All 8 headers on `GET /healthz` with real DB-backed server |
| `TestE2E_Scenario51_RealCluster_AuthenticatedGET` | All 8 headers on authenticated GET with real DB-backed server |
| `TestE2E_Scenario51_RealCluster_AuthFailure` | All 8 headers on 401 (wrong credentials) with real DB-backed server |
| `TestE2E_Scenario51_RealCluster_PermissionDenied` | All 8 headers on 403 (viewer tries POST) with real DB-backed server |
| `TestE2E_Scenario51_RealCluster_MultipleEndpoints` | Consistent headers across multiple endpoints with real DB-backed server |

```bash
# Run security headers functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario51

# Run security headers E2E tests (mock)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario51

# Run security headers E2E tests (real cluster)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario51_RealCluster
```

#### Scenario 52 — Negative Tests and Edge Cases

Tests negative and edge case behavior across authentication, JWT validation, Vault connection retry, OIDC configuration failure, and missing credentials. No production code changes were needed -- all tests exercise existing code paths with invalid or edge-case inputs. Includes 32 tests (16 functional + 11 E2E mock + 5 E2E real cluster).

- **52a -- JWT with wrong issuer**: JWT signed with the correct key but containing a wrong `iss` claim is rejected with 401. Verifies that the OIDC provider validates the issuer claim against the configured `issuerURL`
- **52b -- JWT with wrong audience**: JWT with the correct issuer and key but wrong `aud` claim is rejected with 401. Verifies that the OIDC provider validates the audience claim against the configured `clientID`
- **52c -- Expired JWT**: JWT with `exp` in the past is rejected with 401. Verifies that the OIDC provider checks token expiry
- **52d -- JWT with future iat**: JWT with `iat` 1 hour in the future is accepted by gooidc. This is a behavioral/documentation test confirming that the `gooidc` library does NOT validate the `iat` (issued-at) claim. The token is accepted as long as signature, issuer, audience, and expiry are valid
- **52e -- Token refresh failure**: Expired access token (simulating a failed refresh) returns 401 with "authentication failed" in the response body
- **52f -- Vault connection retry**: Tests `RetryWithBackoff` with four sub-tests: retry and recovery (3 failures then success), retry exhaustion (`ErrRetryExhausted` returned after `MaxRetries + 1` attempts), recovery after N failures, and context cancellation (250ms timeout stops retries before exhaustion)
- **52g -- Invalid OIDC configuration**: `NewOIDCProvider()` with an unreachable issuer URL returns an error and nil provider. When the OIDC provider is nil (simulating failed initialization), Basic auth continues to work (HTTP 200) and Bearer tokens are rejected with 401 mentioning OIDC
- **52h -- Missing K8s Secret for admin password**: Empty `InMemoryCredentialStore` causes `BasicAuthProvider.Authenticate()` to return "invalid credentials" error. Unknown user via API returns 401 with "authentication failed"
- **Test case catalog**: `NegativeEdgeCaseCase` type and `NegativeEdgeCaseCases()` function in `test/cases/test_cases.go` (8 cases: 5 jwt, 1 vault, 1 config, 1 auth)
- **Example CR**: `test/examples/scenario52-negative-edge-cases.yaml`
- **Functional tests**: `test/functional/scenario52_negative_edge_cases_test.go`
- **E2E tests**: `test/e2e/scenario52_negative_edge_cases_e2e_test.go`

**Test case catalog (8 NegativeEdgeCaseCase entries):**

| Case Name | Sub-Scenario | Category | Expected Status | Description |
|-----------|-------------|----------|-----------------|-------------|
| `52a_jwt_wrong_issuer` | 52a | jwt | 401 | JWT with wrong issuer should be rejected with 401 |
| `52b_jwt_wrong_audience` | 52b | jwt | 401 | JWT with wrong audience should be rejected with 401 |
| `52c_jwt_expired` | 52c | jwt | 401 | Expired JWT should be rejected with 401 |
| `52d_jwt_future_iat` | 52d | jwt | 401 | JWT with future iat should be rejected with 401 |
| `52e_token_refresh_failure` | 52e | jwt | 401 | Expired token without refresh should result in 401 |
| `52f_vault_connection_retry` | 52f | vault | 0 | Vault connection failure should trigger exponential backoff retries |
| `52g_invalid_oidc_config` | 52g | config | 0 | Invalid OIDC config should fail gracefully; Basic auth should still work |
| `52h_missing_admin_secret` | 52h | auth | 401 | Missing admin password secret should cause Basic auth to fail with 401 |

**Functional tests (16 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario52a_JWTWrongIssuer` | JWT signed with correct key but wrong `iss` claim rejected with 401 |
| `TestFunctional_Scenario52b_JWTWrongAudience` | JWT with correct issuer but wrong `aud` claim rejected with 401 |
| `TestFunctional_Scenario52c_JWTExpired` | JWT with `exp` in the past rejected with 401 |
| `TestFunctional_Scenario52d_JWTFutureIAT` | JWT with future `iat` accepted by gooidc (behavioral documentation test) |
| `TestFunctional_Scenario52e_TokenRefreshFailure` | Expired token returns 401 with "authentication failed" in response body |
| `TestFunctional_Scenario52f_VaultConnectionRetry` | `RetryWithBackoff` succeeds after 3 failures on attempt 4 |
| `TestFunctional_Scenario52f_VaultRetryExhausted` | `RetryWithBackoff` returns `ErrRetryExhausted` when all retries fail (4 total attempts) |
| `TestFunctional_Scenario52f_VaultRetryRecovery` | `RetryWithBackoff` succeeds when function recovers on attempt 4 |
| `TestFunctional_Scenario52f_VaultRetryContextCancellation` | `RetryWithBackoff` stops retrying when context is cancelled (250ms timeout) |
| `TestFunctional_Scenario52g_InvalidOIDCConfig` | `NewOIDCProvider` returns error with unreachable issuer URL |
| `TestFunctional_Scenario52g_BasicAuthFallback` | Basic auth works (200) and Bearer rejected (401) when OIDC provider is nil (2 sub-tests) |
| `TestFunctional_Scenario52h_MissingAdminSecret` | Empty credential store returns "invalid credentials" error |
| `TestFunctional_Scenario52h_UnknownUser` | Unknown user via API returns 401 with "authentication failed" |
| `TestFunctional_Scenario52_NegativeEdgeCaseCases_Coverage` | `NegativeEdgeCaseCases()` returns 8 cases with correct categories |

**E2E tests -- mock (11 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario52a_JWTWrongIssuer` | JWT with wrong issuer rejected with 401 end-to-end |
| `TestE2E_Scenario52b_JWTWrongAudience` | JWT with wrong audience rejected with 401 end-to-end |
| `TestE2E_Scenario52c_JWTExpired` | Expired JWT rejected with 401 end-to-end |
| `TestE2E_Scenario52d_JWTFutureIAT` | JWT with future iat accepted by gooidc (behavioral test) end-to-end |
| `TestE2E_Scenario52e_TokenRefreshFailure` | Expired token returns 401 with "authentication failed" end-to-end |
| `TestE2E_Scenario52f_VaultRetryExhausted` | `RetryWithBackoff` returns `ErrRetryExhausted` end-to-end |
| `TestE2E_Scenario52f_VaultRetryRecovery` | `RetryWithBackoff` succeeds after recovery end-to-end |
| `TestE2E_Scenario52g_InvalidOIDCConfig` | `NewOIDCProvider` fails with unreachable issuer end-to-end |
| `TestE2E_Scenario52g_BasicAuthFallback` | Basic auth works and Bearer rejected when OIDC nil end-to-end (2 sub-tests) |
| `TestE2E_Scenario52h_MissingAdminSecret` | Empty credential store causes 401 end-to-end |
| `TestE2E_Scenario52_NegativeEdgeCaseCases_Coverage` | All 8 cases from catalog verified end-to-end |

**E2E tests -- real cluster (5 test cases):**

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario52a_RealCluster_JWTWrongIssuer` | JWT with wrong issuer rejected with 401 on real DB-backed server |
| `TestE2E_Scenario52b_RealCluster_JWTWrongAudience` | JWT with wrong audience rejected with 401 on real DB-backed server |
| `TestE2E_Scenario52c_RealCluster_JWTExpired` | Expired JWT rejected with 401 on real DB-backed server |
| `TestE2E_Scenario52g_RealCluster_BasicAuthFallback` | Basic auth works and Bearer rejected when OIDC nil on real DB-backed server (2 sub-tests) |
| `TestE2E_Scenario52h_RealCluster_EmptyCredentialStore` | Empty credential store causes 401 on real DB-backed server |

```bash
# Run negative/edge case functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario52

# Run negative/edge case E2E tests (mock)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario52

# Run negative/edge case E2E tests (real cluster)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario52_RealCluster
```

#### Scenario 69 — Webhook Validation (All Rules)

Verifies that the validating admission webhook rejects every invalid `backup` configuration with a descriptive error and that the object is **not persisted**. Each sub-case is a rejected-CR negative test that constructs a CloudberryCluster with `backup.enabled: true`, a valid baseline backup spec, and exactly one offending field. The functional/E2E tests exercise the validator directly (infra-free); the E2E suite additionally includes a `KUBECONFIG`-gated live test that `Create`s each invalid CR against the API server, asserts the create is rejected, and confirms a follow-up `Get` returns `NotFound` (proving non-persistence). The rejection is also verified at deploy time via `kubectl apply`.

- **69a — missing destination type**: `enabled=true`, no `destination.type` → rejected (`backup.destination.type is required`)
- **69b — S3 missing bucket**: `type: s3`, no `s3.bucket` → rejected (`backup.destination.s3.bucket is required`)
- **69c — S3 missing credentials**: `type: s3`, no `credentialSecret.name` **and** no `vaultSecret.path` → rejected (`requires either credentialSecret.name or vaultSecret.path`). Providing **either** a `credentialSecret.name` or a `vaultSecret.path` is accepted (the `vaultSecret` path requires `spec.vault.enabled` at runtime)
- **69d — invalid compression level**: `gpbackup.compressionLevel=10` (and `=0`) → rejected (`compressionLevel must be between 1 and 9`). An omitted level is defaulted to `1` by the mutating webhook; an explicit `0` reaching the validator is rejected
- **69e — invalid compression type**: `gpbackup.compressionType="lz4"` → rejected (`compressionType must be gzip or zstd`)
- **69f — copyQueueSize without single data file**: `copyQueueSize=4` with `singleDataFile=false` → rejected (`copyQueueSize requires ... singleDataFile`)
- **69g — jobs with single data file**: `jobs=4` with `singleDataFile=true` → rejected (`jobs cannot be combined with ... singleDataFile`)
- **69h — incremental without leaf partition data**: `incremental=true` with `leafPartitionData=false` → rejected (`incremental requires ... leafPartitionData`)
- **69i — invalid cron schedule**: `schedule="not a cron"` → rejected (`schedule is not a valid cron expression`)
- **69j — empty backup image**: `enabled=true`, `image=""` → rejected (`backup.image is required`)
- **Test case catalog**: `Scenario69ValidationCase` type and `Scenario69ValidationCases()` in `test/cases/test_cases.go` (10 cases: 69a–69j)
- **Functional tests**: `test/functional/scenario69_webhook_validation_test.go`
- **E2E tests**: `test/e2e/scenario69_webhook_validation_e2e_test.go`

```bash
# Run Scenario 69 webhook validation functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario69

# Run Scenario 69 webhook validation E2E tests (live rejection gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario69
```

#### Scenario 70 — Webhook Defaults

Verifies that the mutating admission webhook applies all twelve backup defaults when a **minimal** backup spec (enabled + destination only) is submitted, and that the defaulted values appear on the **persisted** object. The webhook also defaults `backup.image` to the official backup toolchain image (`cloudberry-backup:2.1.0`) when unset — a backup-capable image must contain `kubectl` and the `gpbackup`/`gprestore` toolchain. The functional/E2E tests exercise the public defaulter (`webhook.NewCloudberryClusterDefaulter().Default`) — the same code path the admission server runs — and assert the resulting object's fields. The E2E suite additionally includes a `KUBECONFIG`-gated live test that `Create`s a minimal-backup CloudberryCluster, `Get`s it back, and asserts the defaults were persisted by the webhook (then deletes it). Defaulting is gated on `backup.enabled: true` and is non-destructive (explicit user values are preserved).

Defaulted fields verified (minimal spec → persisted object):

| Field | Default |
|-------|---------|
| `gpbackup.compressionLevel` | `1` |
| `gpbackup.compressionType` | `gzip` |
| `gpbackup.jobs` | `1` |
| `gpbackup.singleDataFile` | `false` |
| `gpbackup.withStats` | `true` |
| `gprestore.jobs` | `1` |
| `gprestore.withStats` | `false` — statistics restore is **opt-in** (`gprestore` exits 2 on the upstream `statistics.sql` bug; see the [User Guide](user-guide.md#backup-and-restore-to-s3)) |
| `retention.fullCount` | `3` |
| `retention.maxAge` | `30d` |
| `jobTemplate.backoffLimit` | `2` |
| `jobTemplate.activeDeadlineSeconds` | `7200` |
| `jobTemplate.ttlSecondsAfterFinished` | `86400` |

- **Negative control**: backup `enabled: false` → no defaults applied (`gpbackup`/`gprestore`/`jobTemplate` stay nil)
- **Preserve control**: explicit values (e.g. `compressionLevel: 9`, `retention.fullCount: 5`) are preserved while unset fields are still defaulted
- **Test case catalog**: `Scenario70DefaultsCase` type and `Scenario70DefaultsCases()` in `test/cases/test_cases.go` (12 entries: 70a–70l)
- **Functional tests**: `test/functional/scenario70_webhook_defaults_test.go`
- **E2E tests**: `test/e2e/scenario70_webhook_defaults_e2e_test.go`

```bash
# Run Scenario 70 webhook defaults functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario70

# Run Scenario 70 webhook defaults E2E tests (live persisted-defaults check gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario70
```

#### Scenario 89 — PXF Data-Loading Webhook Validation (All Rules)

Verifies that the validating admission webhook rejects every invalid `dataLoading` (PXF) configuration with a descriptive, field-path-anchored error and that the object is **not persisted**. This is the data-loading analogue of [Scenario 69](#scenario-69--webhook-validation-all-rules) (which covers `backup`). Each sub-case constructs a CloudberryCluster with `dataLoading.enabled: true`, a valid baseline PXF spec (an s3, a jdbc, and an hdfs server; one pxf job and one gpload job), and exactly **one** offending field. Validation is **gated on `dataLoading.enabled: true`** (a disabled spec with bad content is a no-op) and is **fail-fast** — the first offending field is reported and the create is rejected. The validator is exercised directly (infra-free) in `internal/webhook/validating_test.go` (`TestValidateDataLoading`); the W.10 profile allowlist has its own table-driven test (`TestIsValidPxfProfile`).

> **Numbering note:** the integration scenario [Scenario 89 — Backup Artifact Round-Trip Against Real MinIO](#scenario-89--backup-artifact-round-trip-against-real-minio-integration) shares the same scenario number. This entry is the **PXF data-loading webhook-validation** Scenario 89 (the `W.1`–`W.15` admission rules, catalogued as `Scenario89ValidationCases()`); the MinIO entry is an unrelated backup object-store integration test.

- **W.1 — pxf enabled without image**: `dataLoading.pxf.enabled: true`, `pxf.image: ""` → rejected (`dataLoading.pxf.image is required when pxf.enabled is true`). `pxf.enabled: false` with empty image is accepted.
- **W.2 — server name empty / duplicate**: empty `pxf.servers[].name`, or two servers with the same name → rejected (`dataLoading.pxf.servers[…].name` / `duplicate`)
- **W.3 — invalid server type**: `pxf.servers[].type: ftp` → rejected (`dataLoading.pxf.servers[…].type must be one of "s3","hdfs","jdbc","hbase","hive"`); `hbase`/`hive` are accepted
- **W.4 — s3 server missing endpoint or credentials**: `type: s3` without `config["fs.s3a.endpoint"]`, or with empty `credentialSecrets[]` → rejected (`fs.s3a.endpoint` / `credentialSecrets`)
- **W.5 — jdbc server missing driver or url**: `type: jdbc` without `config["jdbc.driver"]` or `config["jdbc.url"]` → rejected (`jdbc.driver` / `jdbc.url`)
- **W.6 — hdfs server missing defaultFS**: `type: hdfs` without `config["fs.defaultFS"]` → rejected (`fs.defaultFS`)
- **W.7 — job name empty / duplicate**: empty `jobs[].name`, or two jobs with the same name → rejected (`dataLoading.jobs[…].name` / `duplicate`)
- **W.8 — invalid job type**: `jobs[].type: kafka` (or any non-`pxf`/`gpload`) → rejected (`dataLoading.jobs[…].type must be "pxf" or "gpload"`)
- **W.9 — pxf job unknown server / nil body**: `pxfJob.server` referencing an undefined server, or a `type: pxf` job with `pxfJob: nil` → rejected (`pxfJob.server` / `pxfJob is required for type pxf`)
- **W.10 — invalid PXF profile**: `pxfJob.profile: s3:nonsense` (bad suffix), `foo:bar` (unknown scheme), bare `s3`/`hdfs`, or a **streaming profile** like `kafka` → rejected (`pxfJob.profile … is not a valid PXF profile`). The allowlist is case-insensitive: `s3`/`gs`/`abfss`/`wasbs` + `:{text,parquet,avro,json,orc}`; `hdfs` + `:{…,SequenceFile}`; `hive` bare or `:{text,orc,rc}`; bare `jdbc`; bare `hbase`
- **W.11 — pxf job missing target table**: empty `pxfJob.targetTable` → rejected (`pxfJob.targetTable is required`)
- **W.12 — gpload job missing target table / nil body**: empty `gploadJob.targetTable`, or a `type: gpload` job with `gploadJob: nil` → rejected (`gploadJob.targetTable is required`)
- **W.13 — invalid cron schedule**: `jobs[].schedule: "not-a-cron"` → rejected (`schedule is not a valid cron expression`); an empty schedule is accepted
- **W.14 — partitioning without range/interval**: `pxfJob.partitioning.column` set without `range` **and** `interval` → rejected (`partitioning requires column, range, and interval together`); the full triple (or an empty `partitioning`) is accepted
- **W.15 — invalid segmentRejectLimitType**: `pxfJob.errorHandling.segmentRejectLimitType: bytes` → rejected (`segmentRejectLimitType must be "rows" or "percent"`); `rows`/`percent` accepted
- **Test case catalog**: `Scenario89ValidationCase` type and `Scenario89ValidationCases()` in `test/cases/test_cases.go` (15 rules W.1–W.15; some rules have two variants exercised directly in the validator suite)
- **Validator unit tests**: `internal/webhook/validating_test.go` (`TestValidateDataLoading`, `TestIsValidPxfProfile`)
- **Rejection metric**: no per-rule metric; denials increment the shared `cloudberry_webhook_admission_total{webhook="validating",result="denied"}`
- **Breaking change**: the data-loading CRD model was **replaced** with the PXF model — the old `streamingServer` and `jobs[].type: s3|kafka|rabbitmq` (`s3Source`/`kafkaSource`/`rabbitmqSource`) fields were removed; only `type: pxf|gpload` remain
- **Scope**: Scenario 89 covers the **declarative contract only** (CRD + webhook validation/defaults). The **PXF/gpfdist runtime** — sidecars, server-config (`*-site.xml`) rendering, `pxf sync`, gpfdist Deployment, and CronJob/Job **execution** — is **Planned/not built**; the controller reconcile only validates config and counts jobs. *(The job-mutation + PXF servers CRUD + `jobs/{job}/logs` + `external-tables` **REST routes** are **FULL** — Scenario 107 — and the matching CLI subcommands (`pxf servers …` CRUD, `data-loading jobs logs`, `data-loading test-read`, `--from-yaml`, gpload flags) are now **Implemented** — Scenario 108 — plus a new `GET .../data-loading/test-read` endpoint.)* See [spec 12 §Implementation Status](../specifications/12-data-loading-spec.md#implementation-status).

```bash
# Run Scenario 89 PXF data-loading webhook validation (validator unit tests)
go test ./internal/webhook/... -v -run 'TestValidateDataLoading|TestIsValidPxfProfile'
```

#### Scenario 90 — PXF Data-Loading Webhook Defaults

Verifies that the **mutating** admission webhook applies all **14** `dataLoading` (PXF) defaults (`D.1`–`D.14`) to an enabled, minimal spec that sets none of them, that explicit user values are preserved (including explicit `false` on the `*bool` extension/job fields), and that a `dataLoading.enabled: false` spec receives **no** defaults. This is the defaulting counterpart to the validation [Scenario 89 — PXF Data-Loading Webhook Validation](#scenario-89--pxf-data-loading-webhook-validation-all-rules). Defaulting is **gated on `dataLoading.enabled: true`** and is **non-destructive** (each field is set only when unset/zero/`nil`).

- **Defaulted fields (D.1–D.14)**: `pxf.port=5888`, `pxf.jvmOpts="-Xmx1g -Xms256m"`, `pxf.logLevel=INFO`, `pxf.extensions.pxf=true` (`*bool`, only when `nil`), `pxf.extensions.pxfFdw=true` (`*bool`, only when `nil`), `gpfdist.replicas=1` (`*int32`), `gpfdist.port=8080`, `pxfJob.mode=insert`, `pxfJob.filterPushdown=true` (`*bool`, only when `nil`), `pxfJob.columnProjection=true` (`*bool`, only when `nil`), `gploadJob.mode=insert`, `jobTemplate.backoffLimit=3` (`*int32`), `jobTemplate.activeDeadlineSeconds=14400` (`*int64`), `jobTemplate.ttlSecondsAfterFinished=86400` (`*int32`).
- **Test case catalog**: `Scenario90DefaultsCase` type and `Scenario90DefaultsCases()` in `test/cases/test_cases.go` (14 defaults `D.1`–`D.14`, each with the expected value).
- **Functional suite**: `test/functional/scenario90_webhook_defaults_test.go` — defaults applied, explicit values preserved, disabled no-op, and a catalog-honesty check (`//go:build functional`).
- **E2E suite**: `test/e2e/scenario90_webhook_defaults_e2e_test.go` — defaulter-direct over all 14 + a validator-accepts check, plus a `KUBECONFIG`-gated live test that `Create`s a minimal (non-defaulted) valid CR into namespace `cloudberry-test` and `Get`s it back to assert the 14 defaults are present in the **persisted** object (proving the server-side mutating webhook ran); the live test **skips cleanly** when `KUBECONFIG` is unset (`//go:build e2e`).
- **Unit coverage**: the defaulting logic is unit-tested in `internal/webhook/mutating_test.go` (`TestSetDataLoadingDefaults`, all 14 fields both directions). See [spec 12 §Webhook Defaults](../specifications/12-data-loading-spec.md#webhook-defaults).

```bash
# Run Scenario 90 functional defaults suite (defaulter-direct, infra-free)
go test -tags functional ./test/functional/ -run Scenario90 -v

# Run Scenario 90 e2e defaults suite (defaulter-direct; live test skips without KUBECONFIG)
go test -tags e2e ./test/e2e/ -run Scenario90 -v

# Run the live persistence test against a real cluster with the mutating webhook deployed
KUBECONFIG=$HOME/.kube/config go test -tags e2e ./test/e2e/ -run 'Scenario90/.*LiveDefaultsPersisted' -v
```

#### Scenario 91 — PXF Full CRD Configuration

Exercises the **newly-implemented** PXF runtime: applying a **full** `dataLoading.pxf` spec (all **5** server types — `s3`/MinIO, `hdfs` with `hive`/`hbase` config, `jdbc` for MySQL and PostgreSQL, `hive`, `hbase` — plus `extensions`, `customConnectors`, `resources`, and a non-default `logLevel`) and proving the operator parses every field, injects the PXF **sidecar** into the **segment-primary** pod template, and renders the `<cluster>-pxf-servers` ConfigMap. This is the runtime counterpart to the declarative [Scenario 89](#scenario-89--pxf-data-loading-webhook-validation-all-rules) / [Scenario 90](#scenario-90--pxf-data-loading-webhook-defaults). The sidecar/ConfigMap are gated on `dataLoading.enabled && pxf.enabled && pxf.image != ""`; a default cluster (`dataLoading == nil`) yields a byte-identical pod template.

- **What's verified**:
  - **Sidecar injection (segment-primary only)**: `BuildSegmentPrimaryStatefulSet` injects a `pxf` container with env `PXF_HOME`/`PXF_BASE`/`PXF_JVM_OPTS`/`PXF_PORT`/`PXF_LOG_LEVEL`/`PXF_EXTENSION_PXF`/`PXF_EXTENSION_PXF_FDW`, `pxf.resources`, the `pxf` port, `/actuator/health` (PXF 2.1.0 Spring Boot actuator) liveness/readiness probes, and the `pxf-base`/`pxf-servers`/`pxf-lib` volume mounts. Coordinator, standby, and mirror pods are **byte-identical** to a default cluster.
  - **Servers ConfigMap**: `BuildPXFServersConfigMap` renders one `<name>__<file>.xml` per server with sorted (byte-stable) keys per the implemented file-mapping (`s3`→`s3-site.xml`; `hdfs`→`core-site.xml`+`hdfs-site.xml` always (+ optional `hive`/`hbase`/`mapred`/`yarn` site files); `jdbc`→`jdbc-site.xml`; `hive`→`core`+`hive`; `hbase`→`core`+`hbase`), `credentialSecrets[]` as `${PLACEHOLDER}` markers (live secret resolution now Implemented via `pxf-cred-init`; see [Scenario 93](#scenario-93--server-configmap-file-mapping-extensions-sync)), and `customConnectors` in `connectors.properties`.
  - **Reconcile/status/metric**: `reconcilePxf` applies the ConfigMap, sets `cloudberry_pxf_servers_configured = len(pxf.servers)` (config-derived), populates `status.dataLoading.pxf.{configured,servers}`, and enriches the `DataLoadingConfigured` condition with the PXF server count.
  - **C.6 — `logLevel` → `PXF_LOG_LEVEL` propagation**: the sidecar is rebuilt from spec each reconcile, so re-patching `pxf.logLevel` rolls the segment-primary pod template env; **`DEBUG`/`WARN`/`ERROR`** each flow verbatim into `PXF_LOG_LEVEL` (unset resolves to `INFO`).
- **Environment limitation**: live data loading is exercised against **s3/MinIO + HDFS + Hive**; the **jdbc (MySQL/PostgreSQL)** and **hbase** servers are **config-verified** (rendered + asserted) with **live ingestion Planned** — there is no Job execution for any server type yet.
- **Scope**: Scenario 91 covers the **sidecar + servers ConfigMap rendering** (full CRD parse). The per-type file-mapping + extensions/GRANTs + sync model are verified by [Scenario 93](#scenario-93--server-configmap-file-mapping-extensions-sync); the ingestion runtime by [Scenario 92](#scenario-92--data-loading-ingestion-runtime). (The gpfdist/gpload runtime is now Implemented in Scenario 101 and the **FDW-based loading path** in [Scenario 103](#scenario-103--fdw-based-loading-path).) See [spec 12 §Scenario 91](../specifications/12-data-loading-spec.md#scenario-91--enable-data-loading-with-full-pxf-crd-configuration) and [§Implementation Status](../specifications/12-data-loading-spec.md#implementation-status).
- **Functional/unit coverage**: `internal/builder/pxf_builder_test.go` (sidecar env/ports/probes/volumes, gating, `logLevel` rebuild loop, servers ConfigMap rendering incl. hive/hbase types) and `internal/controller/admin_controller_test.go` (`reconcilePxf`, `ensurePxfServersConfigMap` create/update, condition message, `cloudberry_pxf_servers_configured`).

```bash
# Run Scenario 91 functional/unit coverage (infra-free)
go test ./internal/builder/... -run 'PXF' -v
go test ./internal/controller/... -run 'Pxf|PXF|ReconcileDataLoading' -v

# Run the metric test for the config-derived PXF gauge
go test ./internal/metrics/... -run 'TestSetPXFServersConfigured' -v

# Run the e2e suite; the KUBECONFIG-gated live test applies the full PXF spec to a
# real API server and asserts the sidecar + servers ConfigMap materialize. It
# skips cleanly when KUBECONFIG is unset.
go test -tags e2e ./test/e2e/ -run Scenario91 -v
KUBECONFIG=$HOME/.kube/config go test -tags e2e ./test/e2e/ -run Scenario91 -v
```

#### Scenario 92 — Data-Loading Ingestion Runtime

Exercises the **newly-implemented** data-loading **ingestion runtime**: the operator genuinely **builds and launches** the per-job load `Job`/`CronJob`, runs the `psql` load script (`CREATE EXTERNAL TABLE (LIKE target)` → `INSERT…SELECT` → `DATALOAD_ROWS` marker → `DROP` → `ANALYZE`), harvests the marker, enriches the rich per-job status, and records the **5 honest** data-loading metrics. This is the runtime execution counterpart to [Scenario 91](#scenario-91--pxf-full-crd-configuration) (sidecar + ConfigMap rendering).

> **✅ PXF live-execution note.** The operator generates, launches, and **now runs** the `pxf://` load Job end-to-end. A **live `pxf://` read-back is Implemented and row-count verified**: an operator-driven `pxf://` load from MinIO S3 (via the PXF sidecar) loaded **183,961 rows** with credentials rendered automatically by the operator. **PXF Job generation/launch = Implemented; live `pxf://` execution = Implemented**, requiring the **`cloudberry-pxf` sidecar image** (`Dockerfile.cloudberry-pxf`, built from `apache/cloudberry-pxf` via `make docker-build-pxf`) **and** the **`pxf`/`pxf_fdw` extensions** in the DB image (the `cloudberry-official-pxf` image, `Dockerfile.cloudberry-official-pxf`, `make docker-build-official-pxf`). On a stock `cloudberry-official:2.1.0` (no PXF extension) the `pxf://` Job still generates/launches but cannot read back — an image prerequisite, not a code gap. The **engine-native** `gpfdist://`/`s3://` protocols (and bare paths served by the cluster gpfdist Service) also **load real data end-to-end** through the *same* operator Job machinery with **no PXF** — **row-count verified** (e.g. **183,961 rows** from a staged CSV). (The bare `file://` scheme is admission-rejected for multi-segment gpload jobs by webhook rule **W.16** — it requires a per-segment-host URI the operator cannot synthesize; use `gpfdist://`, `s3://`, or a bare path.)

- **What's verified**:
  - **Job/CronJob generation + launch**: an enabled job with **no** `schedule` → a one-off `Job` `<cluster>-dataload-<job>` (container `dataload`, image `cluster.Spec.Image`) whose `args[0]` carries the full load script (DDL + `INSERT…SELECT` + the `DATALOAD_ROWS=` marker) and an ownerRef; an enabled **scheduled** job → a `CronJob` (not a `Job`); a **disabled** job → **no** workload; a second reconcile is **idempotent** (no duplicate).
  - **External-table DDL** (`buildExternalTableDDL`): `pxf://<resource>?PROFILE=&SERVER=[&FILTER_PUSHDOWN&PROJECT&PARTITION_BY&RANGE&INTERVAL]` `FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')` for PXF jobs; `gpfdist://`/`s3://` (or a bare path served via the cluster gpfdist Service) `FORMAT 'CSV'`/`'TEXT'` for native/gpload jobs (`file://` is admission-rejected for multi-segment gpload jobs by W.16; the builder keeps a verbatim `file://` passthrough only for a future single-host caller); optional `LOG ERRORS SEGMENT REJECT LIMIT n ROWS|PERCENT`. The temp table uses `(LIKE <target>)` so columns inherit the target (no CRD column field). Output is byte-stable + injection-safe.
  - **Live credential resolution**: the `pxf-cred-init` init container `envsubst`-renders the resolved `*-site.xml` into the shared `pxf-servers` emptyDir (one `<server>/<file>.xml` directory per server) — **secrets never land in the ConfigMap**.
  - **Best-effort extension setup**: `SetupPXFExtensions` runs `CREATE EXTENSION IF NOT EXISTS pxf/pxf_fdw`, annotation-gated and **non-fatal** (the `pxf` extension is absent in `cloudberry-official`, so it is a tolerated no-op there).
  - **Rich status + 5 metrics on terminal Jobs**: a **Succeeded** Job carrying a `DATALOAD_ROWS=<n>` termination marker → `status.dataLoading.jobs[].{lastStatus=Succeeded, lastRun, rowsLoaded=<n>, duration}` + `cloudberry_data_loading_job_status=2`, `…_job_last_success_timestamp`, `…_job_duration_seconds`, `…_rows_total{source_type}` (source_type spec-derived, e.g. `s3` from `s3:parquet`, else `gpfdist`); a **Failed** Job → `lastStatus=Failed`, `…_job_status=3`, `…_errors_total++`. Values are **never synthesized** (the rowcount comes only from the harvested marker).
  - **Genuine native load**: the native `gpfdist://`/`s3://` path (and bare paths served via the cluster gpfdist Service) loads **real data** (row-count verified, e.g. 183,961 rows) at live-deployment time through the same Job machinery. (`file://` is admission-rejected for multi-segment gpload jobs by W.16.)
  - **Operator-driven `pxf://` live load**: with the `cloudberry-pxf` sidecar image + the `pxf` extension present, the `pxf://` path **executes for real** and is **row-count verified** (183,961 rows from MinIO S3 via the PXF sidecar). The KUBECONFIG-gated live e2e test (`test/e2e/scenario92_dataload_runtime_e2e_test.go`) asserts the real target row count (`SELECT count(*)` == `SCENARIO92_PXF_EXPECTED_ROWS`, default 183961) behind `SCENARIO92_PXF_LIVE=1` so it skips cleanly without the PXF image; the infra-free tests exercise the controller machinery (create → status → marker harvest → metric).
- **Scope**: Scenario 92 covers the **ingestion runtime** (Job/CronJob generation + launch + native execution + operator-driven `pxf://` live execution + status + 5 metrics). **Scenario 109** later added `cloudberry_pxf_service_up`, the actuator-passthrough `cloudberry_pxf_requests_total`/`cloudberry_pxf_request_duration_seconds`, and the conditional `cloudberry_data_loading_bytes_total` (all from real sources); the remaining `cloudberry_pxf_*` runtime/health metrics (`bytes_transferred_total`, `records_total`, the folded `errors_total`, `active_connections`) + the 2 `cloudberry_gpfdist_*` stay **honestly absent (Planned, never fabricated)**. *(The full data-loading **REST** surface P.1–P.15 — including the `pxf/*` servers CRUD, `jobs/{job}/logs`, and `external-tables` routes — is **Implemented** in [Scenario 107](#scenario-107--all-api-endpoints-p1p15), and the matching CLI subcommands (L.1–L.16) are now **Implemented** in [Scenario 108](#scenario-108--all-cli-commands-l1l16), plus a new `GET .../data-loading/test-read` endpoint.)* (The gpfdist `Deployment`/`Service` + the gpload control-file Job are now Implemented in Scenario 101, and the **FDW-based loading path** in [Scenario 103](#scenario-103--fdw-based-loading-path).) (An explicit `pxf sync` is **not needed** — config sync is structural via the shared `<cluster>-pxf-servers` ConfigMap; see [Scenario 93](#scenario-93--server-configmap-file-mapping-extensions-sync).) See [spec 12 §Data Loading Jobs](../specifications/12-data-loading-spec.md#data-loading-jobs), [§Scenario 92](../specifications/12-data-loading-spec.md#scenario-92--data-loading-ingestion-runtime), and [§Implemented vs Planned](../specifications/12-data-loading-spec.md#implemented-vs-planned-data-loading-runtime).
- **Functional/unit coverage**: `test/functional/scenario92_dataload_jobs_test.go` (controller reconcile: create one-off Job / scheduled CronJob / skip disabled / idempotent / succeeded-harvest / failed-errors), `internal/controller/dataload_controller_test.go` (`reconcileDataLoadingJobs`, `SetupPXFExtensions` non-fatal/idempotent, `DATALOAD_ROWS` marker parser, status enrichment), and `internal/builder/dataload_builder_job_test.go` (`BuildDataLoadJob`/`BuildDataLoadCronJob` shape, DDL, script, JobTemplate mapping).

```bash
# Run Scenario 92 functional suite (controller reconcile, infra-free)
go test -tags functional ./test/functional/ -run Scenario92 -v

# Run the builder + controller unit coverage for the ingestion runtime
go test ./internal/builder/... -run 'DataLoad' -v
go test ./internal/controller/... -run 'DataLoad|SetupPXFExtensions|ParseDataLoadRows' -v

# Run the 5 data-loading metric tests
go test ./internal/metrics/... -run 'DataLoading' -v

# Live native load (row-count verified) is exercised at deployment time. The
# operator-driven pxf:// live read-back is ALSO row-count verified (183,961 rows
# from MinIO S3 via the PXF sidecar); it requires the cloudberry-pxf sidecar
# image + the pxf extension and is gated behind SCENARIO92_PXF_LIVE=1:
#   KUBECONFIG=… SCENARIO92_PXF_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario92Runtime/.*LivePXFJobCreated -v
# Build the images: make docker-build-pxf docker-build-official-pxf
```

#### Scenario 93 — Server ConfigMap, File Mapping, Extensions, Sync

Verifies the PXF **server configuration contract** end-to-end: the per-type `*-site.xml` file-mapping (`renderPXFServer` / `splitHadoopSiteFiles`, **SL.1–SL.6**), the one-directory-per-server ConfigMap layout, the `${PLACEHOLDER}`-only (no-literal-secret) rendering, the `pxf-cred-init` resolution, and the best-effort `CREATE EXTENSION pxf`/`pxf_fdw` + `GRANT SELECT`/`INSERT ON PROTOCOL pxf TO "gpadmin"` (`SetupPXFExtensions` → `grantPXFProtocol`, **RP.8–RP.12**). This is the configuration/extensions counterpart to [Scenario 91](#scenario-91--pxf-full-crd-configuration) (full CRD parse) and [Scenario 94](#scenario-94--pxf-sidecar-deployment-verification) (sidecar container shape).

> **Scenario numbering note.** Scenario 91 = full PXF CRD config (parse + sidecar + servers ConfigMap). Scenario 92 = data-loading **ingestion runtime**. **Scenario 93** = Server ConfigMap / File Mapping / Extensions / Sync (SL.1–6 + RP.8–12, this entry). **Scenario 94** = **sidecar deployment verification** (the `pxf` container shape). A prior cycle's "Scenario 93 — PXF Sidecar Deployment Verification" was **renamed to Scenario 94** so this re-spec could take number 93.

- **What's verified:**
  - **Per-type file-mapping (SL.1–SL.6)** rendered into the `<cluster>-pxf-servers` ConfigMap as `<server>__<file>.xml` keys:
    - **SL.1** `s3` → `s3-site.xml` (Config + `fs.s3a.access.key`/`fs.s3a.secret.key` placeholders).
    - **SL.2** `hdfs` → `core-site.xml` **and** `hdfs-site.xml` ALWAYS (minimal `<configuration/>` when no `dfs.*`) + optional `hive-site.xml`/`hbase-site.xml` (from the `hive`/`hbase` map or `hive.*`/`hbase.*` keys) + optional `mapred-site.xml`/`yarn-site.xml` (from `mapred*`/`mapreduce.*` / `yarn.*` keys). The `config` map is **prefix-split**: `fs.*`→core, `dfs.*`→hdfs, `mapred*`/`mapreduce.*`→mapred, `yarn.*`→yarn, `hive.*`→hive, `hbase.*`→hbase, other→core.
    - **SL.3** `jdbc` → `jdbc-site.xml` (Config + `jdbc` map + `jdbc.user`/`jdbc.password` placeholders).
    - **SL.4** `hive` → `core-site.xml` **and** `hive-site.xml` (both always).
    - **SL.5** `hbase` → `core-site.xml` **and** `hbase-site.xml` (both always).
    - **SL.6** Every credentialed server's XML carries **`${PLACEHOLDER}` tokens, never literal secrets**.
  - **One directory per server**: the `pxf-cred-init` init container reorganizes the flat `<server>__<file>.xml` keys into nested `<server>/<file>.xml` under `$PXF_BASE/servers` in the shared emptyDir.
  - **Init-container rendering**: `envsubst` substitutes the `${<SANITIZED_NAME_KEY>}` tokens from `SecretKeyRef` env into the resolved `*-site.xml` — secrets never land in the ConfigMap.
  - **Extensions + GRANTs (RP.9–RP.11)**: `CREATE EXTENSION IF NOT EXISTS pxf` (RP.9), then `pxf_fdw` (RP.10), both best-effort/non-fatal; then — **only when `pxf` installed** — `GRANT SELECT ON PROTOCOL pxf TO "gpadmin"` and `GRANT INSERT ON PROTOCOL pxf TO "gpadmin"` (RP.11), also best-effort/non-fatal. The data-loader role is `gpadmin` (`util.DefaultAdminUser`, sanitized).
  - **Shared-ConfigMap sync (RP.12)**: the same `<cluster>-pxf-servers` ConfigMap mounted on every segment-primary sidecar IS the sync mechanism — all sidecars render byte-identical configs; **no explicit `pxf sync` is needed** (it was previously Planned).
- **Test JDBC sources**: the MySQL (`mysql-oltp`) and PostgreSQL (`postgres-source`) JDBC sources were added to the test env — docker-compose `mysql` (`jdbc:mysql://mysql:3306/oltp`) and `pgsource` (`jdbc:postgresql://pgsource:5432/sourcedb`) services seeded by `test/docker-compose/scripts/setup-jdbc-sources.sh`, with k8s Secrets `mysql-credentials` / `pg-source-credentials` — so the `jdbc` file-mapping and credential placeholders are exercised against real drivers.
- **Scope**: server-config rendering + extension/GRANT setup + sync model. See [spec 12 §Scenario 93](../specifications/12-data-loading-spec.md#scenario-93--server-configmap-file-mapping-extensions-sync), [§PXF Server Configuration Lifecycle](../specifications/12-data-loading-spec.md#creating-a-server-file-mapping-sl1sl6), and [§Implementation Status](../specifications/12-data-loading-spec.md#implementation-status).
- **Functional/unit coverage**: `internal/builder/pxf_builder_test.go` (`TestRenderPXFServer_FileMapping` — the SL.1–6 table; `TestRenderPXFServer_NoLiteralSecrets` — SL.6) and `internal/db/pgxclient_test.go` (`TestPgxClient_SetupPXFExtensions_*` incl. `BothSucceed`, `PxfFailsFdwSucceeds`, `BothFailBenign`, `GrantFailsNonFatal`, `ConnectivityError`).

```bash
# Run the file-mapping (SL.1–6) + no-literal-secrets builder tests
go test ./internal/builder/... -run 'TestRenderPXFServer' -v

# Run the CREATE EXTENSION + GRANT ON PROTOCOL pxf (RP.9–11) db-client tests
go test ./internal/db/... -run 'TestPgxClient_SetupPXFExtensions' -v

# Run the controller annotation-gated extension setup
go test ./internal/controller/... -run 'SetupPXFExtensions' -v

# e2e (file-mapping / extensions assertions live with the PXF e2e suites;
# JDBC sources require docker-compose mysql + pgsource up)
make compose-up   # brings up mysql + pgsource (+ minio/hdfs/hive) JDBC sources
go test -tags=e2e ./test/e2e/ -run 'Scenario9[134]' -v
```

#### Scenario 94 — PXF Sidecar Deployment Verification

Verifies the **exact deployment shape** of the `pxf` **sidecar container** the operator injects into the **segment-primary** pod template (`BuildPXFSidecarContainers` / `BuildSegmentPrimaryStatefulSet`). No production code change is involved — the sidecar builder is already correct and live-verified; this is a **test + docs** deliverable pinning the contract the data-loading runtime depends on.

> **Scenario numbering note.** Scenario 92 = data-loading **ingestion runtime** (external-table DDL + load `Job`/`CronJob` generation/launch). [Scenario 93](#scenario-93--server-configmap-file-mapping-extensions-sync) = **Server ConfigMap / File Mapping / Extensions / Sync** (SL.1–6 + RP.8–12). **Scenario 94** = **sidecar deployment verification** (the `pxf` container shape on the segment pod). [Scenario 91](#scenario-91--pxf-full-crd-configuration) *also* verifies sidecar **config** (env from the full 5-server spec + the rendered servers ConfigMap); Scenario 94 pins the full container **contract** (port, probes, command-absence, mounts, resources) deterministically and verifies it on a **live** segment pod.

- **What's verified** (the `pxf` container injected when `dataLoading.enabled && pxf.enabled && pxf.image != ""`):
  - **Container name** `pxf` on the segment-primary pod template (coordinator never carries it).
  - **Env**: `PXF_HOME=/usr/local/cloudberry-pxf`, `PXF_BASE=/pxf-base`, `PXF_JVM_OPTS == pxf.jvmOpts` (default `-Xmx1g -Xms256m`), `PXF_PORT="5888"` (string), `PXF_LOG_LEVEL == pxf.logLevel` (default `INFO`, propagates on rebuild), `PXF_EXTENSION_PXF` / `PXF_EXTENSION_PXF_FDW` (from `extensions.*`, default `true`).
  - **Port**: one container port `5888` named `pxf`, protocol `TCP`.
  - **Liveness probe**: `HTTPGet /actuator/health` on `5888`, `initialDelaySeconds: 60`, `periodSeconds: 20`. **Readiness probe**: `HTTPGet /actuator/health` on `5888`, `initialDelaySeconds: 30`, `periodSeconds: 10`.
  - **Volume mounts**: `pxf-base → /pxf-base`, `pxf-servers → /pxf-base/servers`, `pxf-lib → /pxf/lib/custom`.
  - **Resources**: `requests`/`limits` converted from `pxf.resources`.
  - **Command/Args ABSENCE**: `Command == nil && Args == nil`.
  - **Blast-radius negative**: a `pxf`-disabled or `dataLoading`-disabled cluster carries **no** `pxf` container in the segment pod.
- **Honesty notes** (the obvious-but-wrong values are explicitly rejected):
  - **Probe path is `/actuator/health`, NOT `/pxf/v15/Status`.** The real `apache/cloudberry-pxf` 2.1.0 image exposes health via the **Spring Boot actuator** at `/actuator/health` (verified live: `{"status":"UP"}`); the legacy `/pxf/v15/Status` path is a **DB-client** endpoint that returns **404** on that image, so it is **not** used for health checks.
  - **The `pxf prepare → pxf start → tail service log` lifecycle is owned by the image ENTRYPOINT (`hack/docker-entrypoint-pxf.sh`).** The operator sets **no** container `Command`/`Args`; overriding them would bypass the entrypoint's prepare/start sequence.
- **Scope**: container-shape verification only (mirrors Scenario 91's config verification + the live segment-pod check). See [spec 12 §Scenario 94](../specifications/12-data-loading-spec.md#scenario-94--pxf-sidecar-deployment-verification).
- **Functional/e2e coverage**: `test/functional/scenario94_pxf_sidecar_verification_test.go` (builder-direct: full contract, logLevel propagation, blast-radius negative, `cases.Scenario94Cases()` CatalogHonest) and `test/e2e/scenario94_pxf_sidecar_verification_e2e_test.go` (builder-direct mirror + the KUBECONFIG-gated live segment-pod check).

```bash
# Run Scenario 94 functional suite (builder-direct, infra-free)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario94 -v

# Run Scenario 94 e2e suite (builder-direct; live test skips without KUBECONFIG)
go test -tags=e2e -race ./test/e2e/ -run TestE2E_Scenario94 -v

# Live segment-pod verification against a deployed cluster. The pod-spec-shape
# assertions run whenever a segment pod with a pxf container exists; the
# Ready + `curl localhost:5888/actuator/health` => UP assertion is gated behind
# SCENARIO94_PXF_LIVE=1 (set once the real apache/cloudberry-pxf 2.1.0 image is
# deployed) so it skips cleanly otherwise:
#   KUBECONFIG=… SCENARIO94_PXF_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario94/.*LivePXFSidecarOnSegmentPod -v
```

#### Scenario 95 — PXF CLI Lifecycle

Exercises the **operator-driven PXF lifecycle** surfaced by `cloudberry-ctl pxf status|restart|sync` (`newPxfCmd` → `internal/api/server.go` `handlePXFStatus`/`handlePXFRestart`/`handlePXFSync`), plus the **sidecar-local** PXF-binary verbs (`pxf prepare/start/status/stop/restart/sync`) that run **inside** the `cloudberry-pxf:2.1.0` image and are exercised via `kubectl exec -c pxf`.

> **Scenario numbering note.** This work was requested as "Scenario 94", but [Scenario 94 — PXF Sidecar Deployment Verification](#scenario-94--pxf-sidecar-deployment-verification) is already implemented and embedded, so it is **RETAINED**; the PXF CLI lifecycle takes number **95**. Sequence: 91 = full PXF CRD config, 92 = ingestion runtime, 93 = Server ConfigMap / File Mapping / Extensions / Sync, 94 = PXF Sidecar Deployment Verification, **95 = PXF CLI Lifecycle** (this entry).

- **`status`/`restart`/`sync` and (as of Scenario 108) `servers …` are ctl commands.** `pxf prepare`/`start`/`stop` are **sidecar-local** PXF-binary verbs (run with `kubectl exec -c pxf`), **not** `cloudberry-ctl` subcommands. `pxf servers {list,create,update,delete}` CRUD is now **Implemented** (Scenario 108) — see [Scenario 108](#scenario-108--all-cli-commands-l1l16).
- **PID-1 sidecar semantics (verified live).** The PXF JVM is the sidecar container's **PID 1** (entrypoint `hack/docker-entrypoint-pxf.sh`), so the local `kubectl exec -c pxf -- pxf stop|restart` verbs kill PID 1 and trigger a **container restart** (k8s `restartPolicy` → entrypoint re-runs `pxf prepare`/`start` → recovers `UP`) — **not** an in-place JVM-only stop/start. Don't be surprised. (The operator `cloudberry-ctl pxf restart --cluster` pod-roll path below is unaffected.)
- **`cloudberry-ctl pxf restart --cluster` propagation (pod-roll caveat).** The command makes the operator patch the `<cluster>-segment-primary` StatefulSet **pod-template** restart-trigger annotation (`avsoft.io/restart-trigger`). The kubelet then **rolls all segment pods**; each re-runs the entrypoint (`pxf prepare` → `pxf start`), so every PXF sidecar restarts. This is a **pod ROLL — heavier** than an in-place `pxf restart` against a single sidecar. Records `cloudberry_pxf_restart_total{cluster,namespace,result}` (`result`=`started`/`failed`).
- **`cloudberry-ctl pxf sync --cluster`** uses the same roll primitive but first refreshes the `<cluster>-pxf-servers` ConfigMap, so the `pxf-cred-init` init container re-renders the resolved configs on the roll (the explicit, on-demand counterpart to the always-on structural shared-ConfigMap sync, RP.12).
- **`cloudberry-ctl pxf status --cluster`** aggregates **honest** sidecar readiness from the real `pxf` container `ContainerStatuses` of the segment-primary pods (`{servers,configured,sidecars:[{pod,ready}],readySidecars,totalSidecars}`) — **no** synthetic health, **no** exec, **no** cross-pod HTTP — and echoes `status.dataLoading.pxf.{configured,servers}`. A **not-enabled** cluster returns `400 PXF_NOT_ENABLED` for all three verbs with no StatefulSet/ConfigMap mutation.
- **Verbs verified** (`cases.Scenario95Cases()`, L.1–L.6): **L.1** `prepare` idempotent (exec); **L.2** `start` → status Running / sidecar Ready (exec + operator); **L.3** `stop` → readiness fails (exec + operator); **L.4** `restart` recovers + metric increments (operator); **L.5** `sync` redistributes config (operator); **L.6** `ctl pxf restart` restarts **all** segment-primary sidecars in one action (operator + exec).
- **Functional/e2e coverage**: `test/functional/scenario95_pxf_cli_lifecycle_test.go` (the real `api.Server` HTTP router + auth/RBAC over a fake k8s client: `202` restart/sync responses, restart-trigger annotation bump, ConfigMap (re)create/update, honest ready/total readiness, `cloudberry_pxf_restart_total{result="started"}` emission, and the `PXF_NOT_ENABLED` gate). The exec-driven verbs (L.1–L.3, L.6) run against a live deployed sidecar under `KUBECONFIG` + `SCENARIO95_PXF_LIVE=1`. See [spec 12 §Scenario 95](../specifications/12-data-loading-spec.md#scenario-95--pxf-cli-lifecycle).

```bash
# Run Scenario 95 functional suite (operator layer, infra-free)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario95 -v

# Live exec-driven lifecycle verbs (prepare/start/stop + the ctl pxf restart
# roll across all segment sidecars) against a deployed cluster. Gated behind
# SCENARIO95_PXF_LIVE=1 (set once the real cloudberry-pxf:2.1.0 image is
# deployed) so it skips cleanly otherwise:
#   KUBECONFIG=… SCENARIO95_PXF_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario95 -v
```

#### Scenario 96 — Object Store Profiles & Format Write-Capability

Exercises the object-store PXF profiles end-to-end: the `gs`/`abfss`/`wasbs` server **types** (+ Dell-ECS / MinIO variants), the per-format **write-capability matrix** enforced at admission (webhook **W.10b**) and re-checked by the builder, and the `pxfwritable_export` **writable external-table DDL**. The single source of truth for the matrix is the leaf package `internal/pxfpolicy` (`ModeWritable`, `WritableFormats={text,parquet,avro,sequencefile}`, `IsProfileWritable`), imported by **both** the webhook and the builder.

> **Scenario numbering note.** This work was requested as "Scenario 95", but [Scenario 95 — PXF CLI Lifecycle](#scenario-95--pxf-cli-lifecycle) is already implemented and documented, so it is **RETAINED**; object-store profiles & write-capability take number **96**. Sequence: 91 = full PXF CRD config, 92 = ingestion runtime, 93 = Server ConfigMap / File Mapping / Extensions / Sync, 94 = PXF Sidecar Deployment Verification, 95 = PXF CLI Lifecycle, **96 = Object Store Profiles & Format Write-Capability** (this entry).

- **Object-store server types.** CRD `PxfServerSpec.Type` enum widened to `s3;hdfs;jdbc;hbase;hive;gs;abfss;wasbs`. W.3 admits all eight; W.4 requires `fs.s3a.endpoint` for **all** object-store types but `credentialSecrets[]` **only for `s3`** (GCS/Azure use cloud-native auth). All four object-store types render into a single `<server>__s3-site.xml` (`renderPXFServer`) — the profile scheme selects the connector at query time. **Dell ECS** = an `s3` server with a custom `fs.s3a.endpoint`; **MinIO** = an `s3` server with `fs.s3a.path.style.access=true`.
- **Write-capability (W.10b + builder).** A `mode: writable` job whose profile format is read-only (`json`/`orc`/`rc`) is **rejected at admission** (error contains `write-unsupported` and `writable`); `text`/`parquet`/`avro` writable jobs admit and build `CREATE WRITABLE EXTERNAL TABLE … FORMATTER='pxfwritable_export'` with **no** `LOG ERRORS`/reject limit. The builder re-checks the same `pxfpolicy.IsProfileWritable` predicate (defense-in-depth) even if the webhook is bypassed.
- **Cases verified** (`cases.Scenario96Cases()`):
  - **OS.1–OS.10** — object-store **reads** on `s3-datalake` and `minio-warehouse` (path-style) across `text`/`parquet`/`avro`/`json`/`orc`. Live where the format is synthesizable (text/json natively; parquet/avro via the tooling container); **`s3:orc` is CONFIG-ONLY** (DDL/LOCATION only — ORC is not locally synthesizable), and avro is config-only if the avro tooling is absent.
  - **CFG.1–CFG.8** — `gs`/`abfss`/`wasbs`/Dell-ECS server-config + LOCATION correctness. **All CONFIG-ONLY** (no local GCS/Azure/Dell-ECS backing store).
  - **FF.1–FF.5** — the write matrix: **FF.1 `s3:text`**, **FF.2 `s3:parquet`**, **FF.3 `s3:avro`** writable **SUCCEED** (admitted + correct export DDL + live round-trip where MinIO-backed); **FF.4 `s3:json`**, **FF.5 `s3:orc`** writable **REJECTED** at admission (+ builder errors).
- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario96-objstore-test.yaml` (cluster `objstore-test`, namespace `cloudberry-test`; servers `s3-datalake`, `minio-warehouse`, `gcs-datalake`, `adls-gen2`, `azure-blob`, `dell-ecs`; jobs incl. writable FF.1–FF.3).
- **Sample data**: `scripts/gen-objstore-samples.sh` generates the object-store fixtures (text/json natively; parquet/avro via the python tooling container; ORC skipped). The compose stack adds an `hbase` service (`harisekhon/hbase:2.1`).
- **Config-only / honest notes**: cloud-only stores (`gs`/`abfss`/`wasbs`/Dell-ECS) and ORC are config-only; the **live export RUNTIME** (FF.1–FF.3) is proven only where the MinIO backing store is available. A first-class export-Job kind remains **Planned**. See [spec 12 §Scenario 96](../specifications/12-data-loading-spec.md#scenario-96--object-store-profiles--format-write-capability).

```bash
# Run Scenario 96 functional suite (webhook W.10b + builder DDL, infra-free)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario96 -v

# Integration (controller + fake k8s client)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario96 -v

# Performance (DDL / policy throughput, if present)
go test -tags=performance ./test/performance/ -run Benchmark_Scenario96 -bench=. -v

# Live object-store reads + writable export round-trip against a deployed cluster
# with a backing object store (MinIO). Gated behind SCENARIO96_OBJSTORE_LIVE=1 so
# it skips cleanly when no backing store is deployed:
#   KUBECONFIG=… SCENARIO96_OBJSTORE_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario96 -v
```

#### Scenario 97 — Hadoop Profiles (HDFS / Hive / HBase)

Exercises the Hadoop PXF profiles end-to-end over the combined `hadoop-cluster` (`hdfs`) server: HDFS reads, Hive reads, the HBase read, the **scheme-aware** write-capability matrix (webhook **W.10b** + builder defense-in-depth), and the `hive-site.xml`/`hbase-site.xml` rendering. Every behaviour is **already shipped** — Scenario 97 is a TEST + LIVE-VERIFICATION scenario plus one policy correction.

> **Scenario numbering note.** This work was requested as "Scenario 96", but [Scenario 96 — Object Store Profiles & Format Write-Capability](#scenario-96--object-store-profiles--format-write-capability) is already implemented and documented, so it is **RETAINED**; Hadoop profiles take number **97**. Sequence: 91 = full PXF CRD config, 92 = ingestion runtime, 93 = Server ConfigMap / File Mapping / Extensions / Sync, 94 = PXF Sidecar Deployment Verification, 95 = PXF CLI Lifecycle, 96 = Object Store Profiles & Format Write-Capability, **97 = Hadoop Profiles (HDFS / Hive / HBase)** (this entry).

> **Policy fix (scheme-aware `IsProfileWritable`).** `internal/pxfpolicy.IsProfileWritable` was a **pure FORMAT predicate** that WRONGLY admitted `hive:text` as writable (because `text` is a writable format). It is now **scheme-aware**: a new `readOnlySchemes={hive,hbase}` set makes the Hive/HBase connectors **write-unsupported regardless of format**, so writable `hive:text` (and all `hive*`/`HBase`) is now rejected with `write-unsupported`.

- **Read profiles.** HDFS: `hdfs:text`/`parquet`/`avro`/`json`/`orc`/`SequenceFile` (W.10 admits all via `hdfsFormats()`). Hive: `hive` (auto-detect), `hive:text`, `hive:orc`, `hive:rc` (RCFile read). HBase: bare `HBase` (case-insensitive at W.10).
- **Write matrix (W.10b).** `hdfs:text`/`parquet`/`avro`/`SequenceFile` writable **SUCCEED**; `hdfs:json`/`hdfs:orc` writable **REJECTED**; **all** `hive*` + `HBase` writable **REJECTED** (read-only **scheme**, regardless of format — `hive:text` is rejected even though `text` is a writable format).
- **Site-file rendering.** `renderPXFHDFSServer` for `hadoop-cluster` ALWAYS emits `core-site.xml` + `hdfs-site.xml`, plus `<server>__hive-site.xml` (`hive.metastore.uris=thrift://hive-metastore:9083`) and `<server>__hbase-site.xml` (`hbase.zookeeper.quorum=hbase:2181`) from `server.hive`/`server.hbase` (or `Config` `hive.*`/`hbase.*`).
- **Cases verified** (`cases.Scenario97Cases()`):
  - **HP.1–HP.6** — HDFS reads (`text`/`parquet`/`avro`/`json`/`orc`/`SequenceFile`).
  - **HV.1–HV.4** — Hive reads (`hive` auto-detect / `hive:text` / `hive:orc` / `hive:rc`).
  - **HB.1** — HBase read (case-insensitive `HBase`).
  - **SITE.1–SITE.4** — rendered `hive-site.xml` / `hbase-site.xml` / `core-site.xml` / `hdfs-site.xml` (metastore URI + ZK quorum + `fs.defaultFS` + always-emitted `hdfs-site.xml`).
  - **FF.6a/FF.6b** — `hive:rc` read OK + writable REJECT.
  - **FF.7 / FF.7t** — `hdfs:SequenceFile` / `hdfs:text` writable SUCCEED.
  - **WRej.1–WRej.7** — writable DENY matrix (`hdfs:json`/`hdfs:orc` + all `hive*` + `HBase`).
- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario97-hadoop-test.yaml` (cluster `hadoop-test`, namespace `cloudberry-test`; combined `hadoop-cluster` `hdfs` server + dedicated `hive-warehouse`/`hbase-store` servers; read jobs `hdfs:text`/`hdfs:json`/`hive`/`HBase` + writable FF.7 `hdfs:sequencefile`).
- **Sample data**: `test/docker-compose/scripts/gen-hadoop-samples.sh` generates the Hadoop fixtures (`text`/`json` natively via WebHDFS; `parquet`/`avro`/`orc`/`SequenceFile` + Hive `TEXTFILE`/`ORC`/`RCFILE` tables + the HBase table via `beeline`/`hbase shell` CTAS; logs PRODUCED vs CONFIG-ONLY).
- **Config-only / honest notes**: `text`/`json` reads are live; `parquet`/`avro`/`orc`/`SequenceFile`/`hive:rc` are config-only (DDL/LOCATION + admit) when the tooling/`beeline` is absent; HBase is config-only unless seeded; FF.7 export round-trips only where the HDFS stack is present. See [spec 12 §Scenario 97](../specifications/12-data-loading-spec.md#scenario-97--hadoop-profiles-hdfs--hive--hbase).

```bash
# Run Scenario 97 functional suite (webhook W.10b + builder DDL/site-files, infra-free)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario97 -v

# Integration (controller + fake k8s client)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario97 -v

# Performance (DDL / policy throughput, if present)
go test -tags=performance ./test/performance/ -run Benchmark_Scenario97 -bench=. -v

# Live Hadoop reads + writable export round-trip against a deployed cluster with a
# backing Hadoop stack (HDFS/Hive/HBase). Gated behind SCENARIO97_HADOOP_LIVE=1 so
# it skips cleanly when no Hadoop stack is deployed:
#   KUBECONFIG=… SCENARIO97_HADOOP_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario97 -v
```

#### Scenario 98 — Filter Pushdown, Column Projection, Per-Row Error Handling

Exercises the three PXF/load DDL knobs that **already ship** — `FILTER_PUSHDOWN=true`, `PROJECT=true`, and `[LOG ERRORS ]SEGMENT REJECT LIMIT <n> [ROWS|PERCENT]` — and proves their *runtime* behaviour via **honest, operator-observable signals** (NOT a fabricated byte counter). Scenario 98 is a TEST + LIVE-VERIFICATION + HONEST-OBSERVABILITY scenario.

> **Scenario numbering note.** No collision. Sequence: 95 = PXF CLI Lifecycle, 96 = Object Store Profiles & Format Write-Capability, 97 = Hadoop Profiles (HDFS / Hive / HBase), **98 = Filter Pushdown / Column Projection / Per-Row Error Handling** (this entry).

> **Honest-observability decision (`bytes_transferred` stays Planned).** PXF 2.1.0's Spring Boot Actuator exposes only `/actuator/health` by default; even with `/actuator/prometheus` it offers only `http_server_requests` + JVM metrics — **no honest external-source byte counter**. Emitting a fabricated `cloudberry_pxf_bytes_transferred_total` would violate the metrics-honesty rule, so it **stays Planned**. Filter pushdown is instead observed via **real** signals: (1) **row-count reduction** — `cloudberry_data_loading_rows_total` is lower for a filtered job than an unfiltered baseline; (2) **`EXPLAIN`** shows the pushed filter / projected columns; (3) **source-side query logs** (JDBC pgsource/MySQL, Hive/HS2) show the `WHERE` predicate. Per-row error handling is proven via the real `cloudberry_data_loading_job_status` (2=success / 3=failed) + `cloudberry_data_loading_errors_total` + `rows_total` (valid rows only).

- **Declarative knobs (shipped).** `buildPXFLocation` emits `FILTER_PUSHDOWN=true` when `pxfJob.filterPushdown=true` and `PROJECT=true` when `pxfJob.columnProjection=true` (both **default to `true`** in the mutating webhook when unset — an explicit `false` is preserved and emits nothing). `errorHandlingClause` emits `[LOG ERRORS ]SEGMENT REJECT LIMIT <n> [ROWS|PERCENT]` (gated on a positive `segmentRejectLimit`, W.15-validated type); the **writable export** path (`mode: writable`) correctly **OMITS** it.
- **Cases verified** (`cases.Scenario98Cases()`):
  - **FE.1** — filter pushdown on object-store `s3:parquet`: `FILTER_PUSHDOWN=true`; honest signal = `rows_total` filtered < baseline + `EXPLAIN` pushed filter (ORC leg `[CONFIG-ONLY]` if not synthesizable).
  - **FE.2** — filter pushdown on JDBC (`mysql-oltp`/`postgres-source`, table `jdbc_test_data`, filter column `category`): `FILTER_PUSHDOWN=true`; honest signal = **source-side query-log** `WHERE` predicate (strongest for JDBC) + `rows_total` < baseline.
  - **FE.3** — filter pushdown on Hive (`warehouse.fact_sales`): `FILTER_PUSHDOWN=true`; honest signal = Hive/HS2 query-log predicate + partition prune + `rows_total` < baseline (`[CONFIG-ONLY]` when no live Hive backing).
  - **FE.4** — column projection on WIDE `s3:parquet`: `PROJECT=true`; `[EXPLAIN-ONLY]` — `EXPLAIN` shows only the projected columns (no honest byte meter).
  - **FE.5** — column projection on WIDE `s3:orc`: `PROJECT=true`; `EXPLAIN` projected-columns where ORC synthesizable, else DDL+`PROJECT` correctness only (`[CONFIG-ONLY]`).
  - **FE.12a** — per-row error tolerance WITHIN limit: `LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS`; malformed source = 5 bad rows ≤ 10 → Job **Completed**, `job_status=2`, `rows_total` = VALID rows only.
  - **FE.12b** — per-row error tolerance OVER limit: same 5 bad rows with `SEGMENT REJECT LIMIT 2 ROWS` (< 5) → Job **Failed**, `job_status=3`, `errors_total` incremented.
- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario98-pushdown-test.yaml` (cluster `pushdown-test`; `s3-datalake`/`minio-warehouse`/`mysql-oltp`/`postgres-source`/`hadoop-cluster` servers).
- **Sample data**: `test/docker-compose/scripts/gen-pushdown-samples.sh` generates the pushdown fixtures (a filterable + WIDE parquet/ORC/text dataset; JDBC seed tables `jdbc_test_data` with a `category` filter column; a **malformed CSV** carrying 5 bad rows for FE.12; legs logged as PRODUCED vs CONFIG-ONLY).
- **Config-only / explain-only notes**: filter pushdown / column projection are **declarative + runtime-proven** (the operator emits the correct LOCATION option; the engine/PXF connector performs the actual prune). Column projection has **no honest byte meter** → `[EXPLAIN-ONLY]` (plan target-list narrowing). Hive/ORC legs degrade to `[CONFIG-ONLY]` when no live backing/synthesizable sample exists. Error handling (FE.12) is fully operator-observable via real `job_status`/`errors_total`/`rows_total`. **`bytes_transferred` is never asserted** — it stays Planned. The Scenario 98 dashboard adds a **"Filter Pushdown & Projection (Scenario 98)" doc text panel** (existing Job Status + Errors panels cover FE.12; no `bytes_transferred` panel). See [spec 12 §Scenario 98](../specifications/12-data-loading-spec.md#scenario-98--filter-pushdown-column-projection-per-row-error-handling).

```bash
# Run Scenario 98 functional suite (builder DDL knobs + mutating defaults, infra-free)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario98 -v

# Integration (controller + fake k8s client)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario98 -v

# Performance (filtered-vs-baseline + projected-vs-SELECT* read throughput; rows/sec
# only — never a bytes_transferred metric). Tagged e2e + gated, same as the e2e suite:
#   SCENARIO98_PUSHDOWN_LIVE=1 KUBECONFIG=… \
#     go test -tags=e2e -run=^$ -bench=BenchmarkScenario98 ./test/perf/...

# Live filter-pushdown / projection / error-handling against a deployed cluster with a
# backing pushdown stack (object store + JDBC + Hive). Gated behind
# SCENARIO98_PUSHDOWN_LIVE=1 so it skips cleanly when no stack is deployed:
#   KUBECONFIG=… SCENARIO98_PUSHDOWN_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario98 -v
```

#### Scenario 99 — Writable External Tables / Data Export

Exercises **writable external-table EXPORT** to **three** target types — **S3 / object store** (FE.9/WE.1), **HDFS** (FE.10) and **JDBC** (FE.11) — all via `CREATE WRITABLE EXTERNAL TABLE … FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')`, plus the **new `pxfJob.sourceFilter`** filtered export and the **new webhook rule W.17**. The writable DDL (`pxfwritable_export`) and write-capability enforcement shipped in Scenario 96; Scenario 99 **verifies the export targets live** and adds the filtered export. TEST + LIVE-VERIFICATION + HONEST-OBSERVABILITY scenario.

> **Scenario numbering note.** No collision. Sequence: 96 = Object Store Profiles & Format Write-Capability, 97 = Hadoop Profiles (HDFS / Hive / HBase), 98 = Filter Pushdown / Column Projection / Per-Row Error Handling, **99 = Writable External Tables / Data Export** (this entry).

> **Honest-observability decision (no new metric; `bytes_transferred` stays Planned).** A writable export is a data-loading job (`mode: writable`), so it **reuses** the existing metrics: `cloudberry_data_loading_rows_total` (the exported rowcount, from the SAME `DATALOAD_ROWS` marker — the filtered `sourceFilter` export reports fewer rows than the unfiltered baseline), `cloudberry_data_loading_job_status` (**2**=Succeeded / **3**=Failed) and `cloudberry_data_loading_errors_total`. **No export-specific metric is added**, and `cloudberry_pxf_bytes_transferred_total` is **never asserted** — it stays Planned.

- **Profile-agnostic builder (shipped).** `buildPXFExternalTableDDL` emits the SAME writable DDL for all three targets, differing only by the LOCATION `PROFILE`/`SERVER`. The load script **reverses the INSERT direction** — `INSERT INTO <writable_ext> SELECT * FROM <target> [WHERE <sourceFilter>]`. `pxfpolicy.IsProfileWritable` admits `s3:`/`hdfs:` text/parquet/avro (+`hdfs:sequencefile`) and bare `jdbc`.
- **`sourceFilter` filtered export (new).** `PxfJobSpec.SourceFilter` (`json: sourceFilter`) is an **optional** WHERE predicate, valid **only** on `mode: writable`. Set → `… SELECT * FROM <target> WHERE <sourceFilter>` (filtered subset); unset → full-table export, **byte-identical** to before. Emitted via a **quoted heredoc** piped to `psql -tA` so single quotes (e.g. `region='us-east'`) are safe; the `INSERT 0 <n>` rowcount is captured through the SAME `awk` extraction. The predicate is **admin-authored, trusted SQL** (same trust boundary as `targetTable`).
- **Webhook rule W.17 (`validateSourceFilter`).** **(a)** `sourceFilter` on a non-writable job → admission **DENY** (error contains `sourceFilter` + `writable`); **(b)** `sourceFilter` containing `;`, `--` or `/*` → **DENY** (statement terminators / SQL comments). A cheap substring scan (`sqlPredicateForbidden = {";","--","/*"}`), **not** a SQL parser.
- **Cases verified** (`cases.Scenario99Cases()`):
  - **FE.9 / WE.1** — S3 writable export on `s3:text`: WRITABLE DDL carries `FORMATTER='pxfwritable_export'`, no `LOG ERRORS`; reversed INSERT; honest signal = objects LAND in MinIO under `cloudberry-warehouse/exports/s3/` (S3 list/HEAD). `s3:parquet` `[CONFIG-ONLY]` when parquet write tooling absent.
  - **FE.10** — HDFS writable export on `hdfs:text`: same writable DDL; honest signal = part files LAND in HDFS under `/data-lake/exports/hdfs/` (WebHDFS `LISTSTATUS`). `hdfs:parquet`/`avro` export `[CONFIG-ONLY]` (needs `DATA_SCHEMA`) — prefer `hdfs:text`.
  - **FE.11** — JDBC writable export (bare `jdbc`): same writable DDL; honest signal (**strongest**) = rows LAND in `pgsource` `sourcedb.export_target` (`SELECT count(*) > 0`). Target table pre-created + granted by `gen-export-targets.sh`.
  - **WE.2** — data lands with the **correct format**: each export's WRITABLE DDL carries `FORMATTER='pxfwritable_export'` AND the correct format per profile (text/CSV-shaped or JDBC rows). parquet/avro format-landing `[CONFIG-ONLY]`.
  - **SF.1** — filtered export with `sourceFilter="region='us-east'"`: script emits `INSERT INTO <ext> SELECT * FROM <target> WHERE region='us-east'` (the WHERE is the ONLY script delta); honest signal = filtered export lands FEWER rows than the unfiltered baseline (JDBC `count(*)` deterministic).
  - **SF.2 / SF.2b** — webhook DENY: `sourceFilter` on a READ job (mode unset) → DENY (W.17(a)); a writable job whose `sourceFilter` contains `;` (`"1=1; DROP TABLE x"`) → DENY (W.17(b)). Decision: REJECT (not silently ignore).
- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario99-export-test.yaml` (cluster `export-test`; `minio-warehouse`/`hadoop-cluster`/`postgres-source` servers; jobs `s3-export`/`hdfs-export`/`jdbc-export` + `s3-export-filtered` with `sourceFilter`).
- **Export targets**: `test/docker-compose/scripts/gen-export-targets.sh` **pre-creates** the `pgsource` `export_target` JDBC table (FE.11, `GRANT ALL`) and the S3 (FE.9/WE.1) / HDFS (FE.10) export prefixes (logged as PRODUCED vs CONFIG-ONLY).
- **Config-only / honest notes**: `hdfs:parquet`/`hdfs:avro` (needs `DATA_SCHEMA`) and `s3:parquet` (write tooling) export are `[CONFIG-ONLY]`; the deterministic live-landing legs use `text`. The `sourceFilter` predicate is admin-authored trusted SQL (W.17 is a cheap sanity check, not a parser). A first-class **data-export Job kind** (beyond the writable-DDL path) and `bytes_transferred` remain **Planned**. The Scenario 99 dashboard adds **panel 263 — "Writable External Tables / Data Export (Scenario 99)"** doc text panel (existing Job Status + Rows + Errors panels cover the export signals; no new metric). See [spec 12 §Scenario 99](../specifications/12-data-loading-spec.md#scenario-99--writable-external-tables--data-export).

```bash
# Run Scenario 99 functional suite (webhook W.17 + builder writable DDL/script, infra-free)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario99 -v

# Integration (controller + fake k8s client)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario99 -v

# Performance (live export baseline; rows/sec only — never a bytes_transferred metric).
# Tagged e2e + gated by the SAME live flag as the e2e Part B:
#   SCENARIO99_EXPORT_LIVE=1 KUBECONFIG=… \
#     go test -tags=e2e -run=^$ -bench=BenchmarkScenario99 ./test/perf/...

# Live writable export (S3/HDFS/JDBC landing + filtered export) against a deployed
# cluster with a backing export stack. Gated behind SCENARIO99_EXPORT_LIVE=1 so it
# skips cleanly when no stack is deployed:
#   KUBECONFIG=… SCENARIO99_EXPORT_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario99 -v
```

#### Scenario 101 — gpfdist Deployment + gpload-csv

Flips **two previously-Planned** features to **Implemented**: the **gpfdist file-server runtime** (`Deployment`/`Service`/`PVC`, GP.2-GP.5) and the **gpload control-file load path** (control file GL.1-GL.7 → per-job ConfigMap → `Job`/`CronJob` running `gpload -f`, J.25-J.40). Adds the new `gploadJob` fields and webhook rules **W.18-W.22**. TEST + LIVE-VERIFICATION + HONEST-OBSERVABILITY scenario. (Scenario 99 entry above is intact.)

> **Scenario numbering note.** No collision (100 skipped by request). Sequence: 96 = Object Store Profiles, 97 = Hadoop Profiles, 98 = Pushdown / Projection / Error handling, 99 = Writable export, **101 = gpfdist Deployment + gpload-csv** (this entry).

> **Honest-observability decision (no new metric).** gpload is a data-loading job, so it **reuses** `cloudberry_data_loading_*` (`job_status` 2=success/3=failed, `rows_total` best-effort from gpload's summary via the SAME `DATALOAD_ROWS` marker — omitted when unparseable, `errors_total`, `job_duration_seconds`). **No new operator metric.** gpfdist Deployment readiness is observable via **kube-state-metrics** (`kube_deployment_status_replicas_ready`), but kube-state-metrics is **NOT deployed in the test env**, so gpfdist readiness is observed via **`kubectl`**, not VictoriaMetrics. The 2 `cloudberry_gpfdist_*` metrics (`connections_active`, `bytes_served_total`) stay **Planned** — gpfdist has **no scrapable endpoint**.

- **gpfdist runtime (GP.2-GP.5, `internal/builder/gpfdist_builder.go` + `reconcileGpfdist`).** Gated on `dataLoading.gpfdist.enabled`. `BuildGpfdistDeployment` → `<cluster>-gpfdist` (replicas honor `gpfdist.replicas`, default 1; image `gpfdist.image`, default `cloudberry-gpfdist:2.1.0`; container `gpfdist`, command `["gpfdist"]`, args `["-d","/data","-p","8080","-l","/var/log/gpfdist.log"]`; port 8080; mount `/data`). `BuildGpfdistPVC` → `<cluster>-gpfdist-data-pvc` (RWO, 1Gi, mounted `/data`). `BuildGpfdistService` → `<cluster>-gpfdist-svc` (selector `avsoft.io/component=gpfdist` == pod labels, port 8080 → targetPort 8080). When `enabled` flips off the three objects are best-effort GC'd.
  - **Documented divergences (honest).** The spec's literal `gpfdist-data-pvc` is implemented as the **per-cluster** `<cluster>-gpfdist-data-pvc` (`util.GpfdistDataPVCName`) — avoids same-namespace collisions, multi-cluster-safe, `ownerRef`-GC'd. The spec's illustrative selector `cloudberry.apache.org/component: gpfdist` is implemented with the repo's **actual** label domain `avsoft.io/component: gpfdist` (`util.LabelComponent`/`ComponentGpfdist`); pod labels and selector share `util.CommonLabels` so they can't drift.
- **gpload control-file path (GL.1-GL.7 + J.25-J.40, `internal/builder/gpload_builder.go`).** A `type: gpload` job is rerouted from the old native-external-table-DDL path to a gpload control file (PXF jobs unchanged). `BuildGploadControlFile` renders a byte-stable control file (`VERSION 1.0.0.1`/`DATABASE postgres`/`USER gpadmin`/`HOST <cluster>-coord-hl`/`PORT 5432`; `INPUT.SOURCE.FILE gpfdist://<cluster>-gpfdist-svc:8080<glob>` or local verbatim; `FORMAT`/`DELIMITER`/`HEADER`/`ENCODING`; `ERROR_LIMIT`/`LOG_ERRORS`; `OUTPUT.TABLE`/`MODE` insert|update|merge with `MATCH_COLUMNS`/`UPDATE_COLUMNS` for update/merge; `PRELOAD.TRUNCATE`; `SQL.AFTER`). Delivered via the per-job ConfigMap `<cluster>-gpload-<job>` (data key `<job>.yml`, `util.GploadControlFileConfigMapName`) mounted read-only at `/etc/gpload`. `BuildGploadCronJob` (when `schedule` set, J.25) / `BuildGploadJob` (one-off) runs `gpload -f /etc/gpload/<job>.yml` on the cluster image; the wrapper best-effort harvests gpload's summary rowcount into `DATALOAD_ROWS` (omitted when unparseable).
- **New `gploadJob` fields (`api/v1alpha1/types.go`).** `inputSource{type:gpfdist|local, host, port}`, `delimiter` (`MaxLength 1`), `header *bool`, `encoding`, `matchColumns[]`, `updateColumns[]`, `preload{truncate *bool}`, `postActions[]`; `mode` Enum `insert;update;merge`, `format` Enum `csv;text`. New sub-structs `GploadInputSourceSpec`, `GploadPreloadSpec`.
- **Webhook rules W.18-W.22 (`internal/webhook/validating.go`).** **W.18** `inputSource.type` ∈ {gpfdist, local}; **W.19** `delimiter` exactly one character; **W.20** `mode` update/merge requires non-empty `matchColumns`; **W.21** each `postActions[]` passes the W.17 SQL sanity check (no `;`/`--`/`/*`; reuses the W.17 helper); **W.22** `inputSource.host`/`.port` valid only for `type: gpfdist` (rejected on `local`).
- **Cases verified** (`cases.Scenario101Cases()`): GP.2-GP.5 (Deployment name/replicas/image/cmd-args/port, PVC name/mount, Service name/selector/port, ownerRef + live readiness); J.25 (schedule → CronJob, none → Job; control-file route not native DDL); GL.1-GL.7 (the control-file blocks); J.27 (local verbatim), J.28/J.29 (custom host/port), J.36/J.37 (update/merge MATCH_COLUMNS); W.18-W.22.
- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario101-gpfdist-test.yaml` (cluster `gpfdist-test`; `gpfdist.enabled: true` + the `gpload-csv` job).
- **Source staging**: `test/docker-compose/scripts/gen-gpload-csv.sh` + the seed CSVs — the **PVC-seed approach**: the CSVs are staged onto the gpfdist `/data` PVC so the live load reads them over `gpfdist://`.
- **Image**: the thin `cloudberry-gpfdist:2.1.0` image (`Dockerfile.cloudberry-gpfdist`, built via `make docker-build-gpfdist`) runs the gpfdist binary over `/data`. The gpfdist + gpload binaries are confirmed present in `cloudberry-official-pxf:2.1.0` (`/usr/local/cloudberry-db-2.1.0/bin/{gpfdist,gpload}`).
- **Dashboard**: a **gpfdist + gpload (Scenario 101)** doc-text panel (gpfdist Deployment observed via `kubectl` since kube-state-metrics is absent; gpload reuses the existing `cloudberry_data_loading_*` panels; no new metric).

```bash
# Run Scenario 101 functional suite (gpfdist + gpload builders, webhook W.18-22, infra-free)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario101 -v

# Integration (controller + fake k8s client: reconcileGpfdist + gpload ConfigMap/Job)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario101 -v

# Performance (live gpload baseline; rows/sec only — never a bytes metric).
# Tagged e2e + gated by the SAME live flag as the e2e live leg:
#   SCENARIO101_GPFDIST_LIVE=1 KUBECONFIG=… \
#     go test -tags=e2e -run=^$ -bench=BenchmarkScenario101 ./test/perf/...

# Live gpfdist Deployment + gpload-csv load against a deployed cluster with the
# gpfdist /data PVC seeded (gen-gpload-csv.sh). Gated behind SCENARIO101_GPFDIST_LIVE=1
# so it skips cleanly when no stack is deployed:
#   KUBECONFIG=… SCENARIO101_GPFDIST_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario101 -v

# Build the thin gpfdist image
make docker-build-gpfdist
```

#### Scenario 102 — kafka-cdc Continuous Streaming (Custom Connector)

**Reverses** the prior "kafka removed / no streaming profiles" policy **scoped to custom connectors**: the `kafka` profile is reinstated as a **custom-connector** profile (built-in streaming stays out of scope). Adds the `custom` PXF server type, a connector-JAR download init container, a continuous one-off streaming Job, three new `pxfJob` fields, and webhook rules **W.23/W.24/W.23c**. TEST + LIVE-VERIFICATION + HONEST-OBSERVABILITY scenario. (Scenario 101 entry above is intact.)

> **Scenario numbering note.** No collision (100 skipped by request). Sequence: 98 = Pushdown/Projection/Errors, 99 = Writable export, 101 = gpfdist Deployment + gpload-csv, **102 = kafka-cdc continuous streaming custom connector** (this entry).

> **Honest-observability decision (no new metric).** kafka-cdc is a data-loading job, so it **reuses** `cloudberry_data_loading_*`. The key honesty point: a continuous consumer's **steady state is `cloudberry_data_loading_job_status = Running`** (1), **NOT** Complete/2 — it never "succeeds" on its own. `rows_total` is best-effort per flush; `jobs_active` includes it while Running. **No new operator metric.** `cloudberry_pxf_*`/`cloudberry_gpfdist_*` stay **Planned**.

- **The custom-connector kafka model (C.18 / J.41 / J.42).** Modeled entirely within the existing PXF custom-connector machinery (no `streamingServer` block): a `pxf.servers[]` entry `{name: kafka-connector, type: custom}` (the new `custom` type, **W.3**, has no forced config keys) + a `pxf.customConnectors[]` entry `{name: kafka-connector, jarUrl: s3://…}` + a `pxfJob` `{server: kafka-connector, profile: kafka, continuous: true}`. The built-in allowlist (`pxfProfileSchemes`/`isValidPxfProfile`) is **UNCHANGED** (`isValidPxfProfile("kafka")` still false); recognition lives in a separate `pxfCustomConnectorSchemes = {kafka, rabbitmq}` (`isCustomConnectorProfile`), gated by W.23.
- **Connector-JAR download init container `pxf-connector-init` (C.18, `internal/builder/pxf_builder.go` `BuildPXFConnectorInitContainers`).** Wired into the segment-primary pod **after** `pxf-cred-init`. Guarded no-op unless the sidecar is enabled AND ≥1 `customConnectors[]` entry exists. For each `jarUrl` (sorted by name) downloads into `/pxf/lib/custom/<name>.jar` in the shared `pxf-lib` emptyDir: `s3://`→`aws --endpoint-url "$AWS_S3_ENDPOINT" s3 cp`, `http(s)://`→`curl -fsSL`, other→abort. S3 env reused from the cluster's **backup S3 destination** (`backup-s3-credentials` + `AWS_S3_ENDPOINT`/`AWS_DEFAULT_REGION`).
- **Continuous one-off Job — NOT a CronJob (J.43/J.46, `internal/builder/dataload_builder.go`).** `pxfJob.continuous: true` → a one-off long-running `Job` (never a CronJob, even with no schedule; a continuous job WITH a schedule is rejected by W.23c). Shaping: `ActiveDeadlineSeconds: nil` + `RestartPolicy: OnFailure` + `BackoffLimit: 6`. The loader (`buildContinuousDataLoadScript`) creates the `pxf://…?PROFILE=kafka&SERVER=kafka-connector` external table once, then loops `INSERT INTO <target> SELECT * FROM <ext>` per flush (best-effort `DATALOAD_ROWS` marker) until the Job is deleted.
- **New `pxfJob` fields (`api/v1alpha1/types.go`).** `continuous *bool` (J.43), `batchSize int32` (J.44, `Minimum=1`), `flushInterval string` (J.45, Go duration). Passed to the dataload Job container as `CBK_CONTINUOUS` / `CBK_BATCH_SIZE` / `CBK_FLUSH_INTERVAL` env (a non-continuous job emits `CBK_CONTINUOUS=false` and omits the batch/flush env when unset).
- **Webhook rules W.23/W.24/W.23c (`internal/webhook/validating.go`).** **W.23** a custom-connector/streaming profile (`kafka`/`rabbitmq`) is admitted **only** when the referenced server is `type: custom` with a matching `customConnectors[]` entry — bare `kafka` / `kafka` on a non-custom server **still rejected** (no built-in streaming). **W.24** a `type: custom` server requires a matching `customConnectors[].name` (link by name). **W.23c** `batchSize ≥ 1`, `flushInterval` parses as a Go duration, and `continuous: true` must NOT set a schedule.
- **Cases verified** (`cases.Scenario102Cases()`): C.18 (connector-init download/mount), J.41/J.42 (custom server + connector model), J.43-J.46 (continuous Job shaping + NOT-CronJob + CBK_* env + consume loop), W.23/W.24/W.23c.
- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario102-kafka-test.yaml` (cluster `kafka-test`, namespace `cloudberry-test`: a `custom` `kafka-connector` server + the matching `customConnectors[]` JAR + the continuous `kafka-cdc` job, `batchSize 10000` / `flushInterval 30s`).
- **Source staging**: `test/docker-compose/scripts/gen-kafka-cdc.sh` produces the sample CDC messages the kafka-cdc job consumes.
- **Dashboard**: **panel 272 — "kafka-cdc Continuous Streaming (Scenario 102)"** doc-text panel (observed via `cloudberry_data_loading_job_status = Running` steady state; no new metric).
- **HONEST live caveat (config-only).** The JAR download + mount, the Job creation + shaping, the streaming params (`CBK_*`), and the external-table DDL are **fully provable**. The end-to-end **kafka→table row landing** needs a **REAL Kafka→PXF connector JAR** — the staged one is a **placeholder** — so live row-landing is **CONFIG-ONLY / documented**; the Job still runs as a streaming consumer with the JAR mounted. The live e2e/perf legs are gated by **`SCENARIO102_KAFKA_LIVE`** and skip/CONFIG-ONLY cleanly absent a real connector JAR + reachable Kafka.
- **Why config-only — stock PXF has no Kafka connector (verified against `cloudberry-pxf:2.1.0`).** The image's `pxf-profiles.xml` registers **no `Kafka` profile**; the only `kafka` strings in the PXF app jar are Micrometer's Kafka *metrics binder*, not a PXF `Fragmenter`/`Accessor`/`Resolver`. Apache PXF is a batch, pull, request/response framework; Kafka is an unbounded push stream, so upstream PXF ships **no Kafka plugin**. A functional `kafka` profile can therefore only come from a **user-supplied custom-connector JAR** — which is precisely why the operator gates it behind W.23 and treats it as bring-your-own. To make row landing live, provide a real Kafka→PXF connector JAR at the `customConnectors[].jarUrl` (PXF SDK: Fragmenter→partitions, Accessor→offset consume, Resolver→deserialize, registered in `pxf-profiles.xml`) **or** consume Kafka in the loader with a non-PXF tool (`kcat`/Kafka Connect). Both are net-new **connector** work, not operator changes; the operator plumbing is complete.

```bash
# Run Scenario 102 functional suite (custom-connector model + continuous Job + CBK_* env
# + webhook W.23/W.24/W.23c; infra-free, ALWAYS runs)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario102 -v

# Integration (builder-level kafka-test artifacts against the real stack; gated on kafka/MinIO)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario102 -v

# E2E — Part A (contract, infra-free) always runs; Part B (live) needs the deployed
# kafka-test cluster + the staged connector JAR, gated by SCENARIO102_KAFKA_LIVE:
#   KUBECONFIG=… SCENARIO102_KAFKA_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario102 -v

# Performance (continuous-consume throughput baseline; rows/sec only — never a bytes
# metric). Tagged e2e + gated by the SAME live flag:
#   SCENARIO102_KAFKA_LIVE=1 KUBECONFIG=… \
#     go test -tags=e2e -run=^$ -bench=BenchmarkScenario102 ./test/perf/...

# Generate the sample CDC messages the kafka-cdc job consumes
bash test/docker-compose/scripts/gen-kafka-cdc.sh
```

#### Scenario 103 — FDW-Based Loading Path

Adds an **alternative FDW loading path** alongside the external-table path, selected by the new **`pxfJob.loadMethod: fdw`** field. A `loadMethod: fdw` PXF job builds a **PERSISTENT** foreign-data-wrapper chain (EX.5-EX.7) and loads via `INSERT INTO <target> SELECT * FROM <foreign>` (EX.8) instead of the transient external-table path — and is **EQUIVALENT** to it (the SAME rows land, proven by **equal row counts**, NOT a new metric). TEST + LIVE-VERIFICATION + HONEST-OBSERVABILITY scenario. (Scenario 102 entry above is intact.)

> **Scenario numbering note.** Sequence: 99 = Writable export, 101 = gpfdist Deployment + gpload-csv, 102 = kafka-cdc continuous streaming custom connector, **103 = FDW-based loading path** (this entry).

> **Honest-observability decision (no new metric).** An FDW load **is** a data-loading job, so it **reuses** `cloudberry_data_loading_*` (`job_status`/`rows_total`/`errors_total`). The FDW==external-table equivalence is proven by **EQUAL ROW COUNTS** (`count(public.events_ext) == count(public.events_fdw)` over the SAME dataset), **NOT** a new metric. `cloudberry_pxf_*`/`cloudberry_gpfdist_*` stay **Planned**.

- **`pxfJob.loadMethod` field (`api/v1alpha1/types.go`).** Enum `external-table` (default; also empty) | `fdw` (`+kubebuilder:validation:Enum=external-table;fdw`). `external-table` keeps the transient `CREATE EXTERNAL TABLE … DROP` path **byte-identical**; `fdw` routes (`isFDWPxfJob`) to the persistent FDW body.
- **EX.5-EX.7 — persistent FDW DDL (`buildFDWDDL`, `internal/builder/dataload_builder.go`).** Byte-stable, injection-safe (idents `quoteSQLIdentifier`-quoted; resource/format single-quoted literals), all `IF NOT EXISTS`, **NEVER dropped**:
  - **EX.5** `CREATE SERVER IF NOT EXISTS "foreign_<server>" FOREIGN DATA WRAPPER <wrapper> OPTIONS (resource '<res>'[, format '<fmt>'])` — deterministic server `"foreign_" + sanitize(server)`.
  - **EX.6** `CREATE USER MAPPING IF NOT EXISTS FOR "gpadmin" SERVER "foreign_<server>"` (`pxfDataLoaderRole = util.DefaultAdminUser = gpadmin`, the role RP.11 GRANTs `SELECT`/`INSERT ON PROTOCOL pxf`).
  - **EX.7** `CREATE FOREIGN TABLE IF NOT EXISTS "foreign_<job>" (LIKE "<target>") SERVER "foreign_<server>" OPTIONS (resource[, format])` — `(LIKE <target>)` inherits the column schema; the `format` OPTION is **OMITTED** for a **bare** profile (`jdbc`/`hive`).
- **Per-protocol `pxf_fdw` wrapper (LIVE-VERIFIED, `fdwWrapperByScheme`).** Confirmed via `SELECT fdwname FROM pg_foreign_data_wrapper` on `cloudberry-official-pxf:2.1.0` — each scheme has its **OWN** registered wrapper: `s3`→`s3_pxf_fdw`, `gs`→`gs_pxf_fdw`, `abfss`→`abfss_pxf_fdw`, `wasbs`→`wasbs_pxf_fdw`, `jdbc`→`jdbc_pxf_fdw`, `hdfs`→`hdfs_pxf_fdw`, `hive`→`hive_pxf_fdw`, `hbase`→`hbase_pxf_fdw` (generic fallback `pxf_fdw` for an unknown scheme).
- **EX.8 — load step (`buildFDWDataLoadScript`).** Ensures the persistent objects exist, **queries the foreign table directly** (`SELECT count(*) FROM "foreign_<job>"`), then `INSERT INTO "<target>" SELECT * FROM "foreign_<job>" [WHERE <sourceFilter>]` (shared `writeDataLoadInsert` — same `DATALOAD_ROWS` capture + quoted-heredoc single-quote safety) + `ANALYZE`. **NO DROP** — the foreign objects are persistent and stay directly queryable.
- **Webhook W.25 + W.17 tweak (`internal/webhook/validating.go`).** **W.25** (`validateLoadMethod`): `loadMethod` must be `external-table`|`fdw` (enum); `loadMethod: fdw` + `mode: writable` → **reject** (fdw is read-only); `loadMethod: fdw` + `continuous: true` → **reject** (fdw is a one-off persistent load). **W.17 tweak**: `sourceFilter` is now valid for `{mode: writable export}` **OR** `{loadMethod: fdw read}`, still rejected on a plain external-table read import. (W.25 runs **before** W.17 so it can consult `loadMethod`.)
- **Equivalence proof.** The SAME job built once with `loadMethod` unset (external-table) and once with `loadMethod: fdw` both produce a valid load Job whose `INSERT INTO <target> SELECT * FROM <src>` lands the same rows — the headline proof is `count(public.events_ext) == count(public.events_fdw)` over the SAME MinIO dataset; the filtered FDW leg lands FEWER rows (`sourceFilter: "value > 500"`).
- **Cases verified** (`cases.Scenario103Cases()`): EX.5-EX.7 (FDW DDL + per-scheme wrapper + bare-profile no-format), EX.8 (direct foreign-table query + INSERT…SELECT + persistence/no-drop), W.25 (enum / fdw+writable / fdw+continuous DENY; fdw read ADMIT), W.17 (fdw read filter ADMIT; plain external-table read filter DENY), RECON (a `loadMethod: fdw` job reconciles → the dataload Job Args[0] carries the FDW chain).
- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario103-fdw-test.yaml` (cluster `fdw-test`, namespace `cloudberry-test`: ONE `s3-datalake` server + three jobs over the SAME MinIO dataset — `s3-ext-load` (external-table) → `public.events_ext`, `s3-fdw-load` (fdw) → `public.events_fdw`, `s3-fdw-filtered` (fdw + `sourceFilter: "value > 500"`) → `public.events_fdw_filtered`).
- **Dashboard**: **panel 273 — "FDW-Based Loading Path (Scenario 103)"** doc-text panel (observed via `cloudberry_data_loading_*`; equivalence by row counts; **no new metric**).

```bash
# Run Scenario 103 functional suite (FDW DDL builder + EX.5-EX.8 + webhook W.25/W.17 +
# builder-level equivalence; infra-free, ALWAYS runs)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario103 -v

# Integration (builder-level fdw-test artifacts against the real stack; gated on MinIO)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario103 -v

# E2E — Part A (contract, infra-free) always runs; Part B (live equivalence proof on the
# deployed fdw-test cluster + MinIO dataset) is gated by SCENARIO103_FDW_LIVE:
#   KUBECONFIG=… SCENARIO103_FDW_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario103 -v

# Performance (FDW-vs-external-table load throughput baseline; rows/sec only — never a
# bytes metric). Tagged e2e + gated by the SAME live flag:
#   SCENARIO103_FDW_LIVE=1 KUBECONFIG=… \
#     go test -tags=e2e -run=^$ -bench=BenchmarkScenario103 ./test/perf/...
```

#### Scenario 104 — Pre-Load Health Checks

Flips the previously-Planned pre-load health-check init container to **Implemented**. A **`dataload-healthcheck`** init container is prepended **FIRST** on **BOTH** the pxf/native (`buildDataLoadPodSpec`) **AND** the gpload (`buildGploadPodSpec`) data-load Job pods (gated on `dataLoading.healthChecks.enabled`, default ON). It runs five gated checks (**HC.1-HC.5**) before the load; a non-zero exit **blocks the main load container** → the Job fails. TEST + LIVE-VERIFICATION + HONEST-OBSERVABILITY scenario. (Scenario 103 entry above is intact.)

> **Scenario numbering note.** Sequence: 101 = gpfdist + gpload, 102 = kafka-cdc, 103 = FDW-based loading path, **104 = pre-load health checks** (this entry).

> **Honest-observability decision (no new operator metric).** HC failures are observed via the EXISTING `cloudberry_data_loading_job_status=3` (Failed) + `cloudberry_data_loading_errors_total` + the **`DataLoadingHealthCheckFailed`** Event + the NEW **kube-state-metrics** (`kube_job_status_failed{job_name=~".*-dataload-.*"}` / `kube_pod_init_container_status_*` / `kube_deployment_status_replicas_available`). `cloudberry_pxf_*`/`cloudberry_gpfdist_*` stay **Planned**.

- **The init container (`buildDataLoadHealthCheckInitContainer`, `internal/builder/dataload_builder.go`).** Data-loader image, `bash -c` over `buildDataLoadHealthCheckScript` (`set -euo pipefail`); env = the PG\* data-loading env (HC.1/HC.2) PLUS the S3 creds env (HC.3, reused from the connector-init pattern — `AWS_*` via `SecretKeyRef` + `AWS_S3_ENDPOINT`, never plaintext); mounts `dataload-scratch` at `/dataload-scratch` (HC.5).
- **The 5 checks (HC.1-HC.5), gated:**
  - **HC.1** PXF readiness — pxf jobs when `pxf.enabled`: a `psql` **DB-PROXY** probe (`SELECT 1` → `SELECT 1 FROM pg_extension WHERE extname='pxf'` → `SELECT pxf_version()`). **HC.1 DB-proxy honesty:** the load pod **CANNOT** reach a segment's localhost-only PXF sidecar (segment↔sidecar is `localhost`-only), so HC.1 is a DB proxy, **NOT** a direct curl of the sidecar `/actuator/health`. The segment-pod sidecar **liveness probe** uses `/actuator/health`; `/pxf/v15/Status` is the legacy path that **404s** and must NOT be used. The LIVE proof of HC.1 is "stop PXF on a segment → the job fails".
  - **HC.2** target table exists — ALL jobs: `psql … to_regclass('<targetTable>')`. The DETERMINISTIC headline (drop the table → fail).
  - **HC.3** source connectivity — pxf **object-store** jobs: `curl -fsS --head "${AWS_S3_ENDPOINT}"`. **Skipped** for `jdbc`/`hive`/`hbase`/`hdfs`.
  - **HC.4** gpfdist reachability — gpload jobs when `gpfdist.enabled`: `curl -fsS "http://<cluster>-gpfdist-svc:8080/"`. Fails when the gpfdist Deployment is scaled to 0.
  - **HC.5** disk space — ALL jobs: `df -Pk /dataload-scratch` free `>= diskMinFreeMB`. **HC.5 fill mechanism:** the most deterministic break is raising `diskMinFreeMB` above the emptyDir free space (or filling `/dataload-scratch` beyond a small `scratchSizeLimit`).
- **The `healthChecks` knob (`api/v1alpha1.DataLoadHealthChecksSpec`).** `dataLoading.healthChecks { enabled *bool (default true; nil block ⇒ on), diskMinFreeMB int32 (default 64), scratchSizeLimit string }`. `enabled: false` → no init container, no scratch volume, no main-container scratch mount (byte-identical to a no-health-check pod).
- **The scratch volume.** A `dataload-scratch` `emptyDir` (`SizeLimit` from `scratchSizeLimit`) mounted at `/dataload-scratch` on **BOTH** the init AND the main data-load container — so error-log/temp files have a real home AND HC.5's `df` is testable.
- **The Event (`emitDataLoadHealthCheckFailureEvent`, `internal/controller/dataload_controller.go`).** A de-duplicated `EventTypeWarning` **`DataLoadingHealthCheckFailed`** (`cbv1alpha1.EventReasonDataLoadingHealthCheckFailed`) emitted when a data-load Job is observed Failed **AND** the `dataload-healthcheck` init container terminated non-zero — honest attribution via the Job pod's `initContainerStatuses` (`podInitContainerFailed`). A **main-container** failure does **NOT** get the HC event; an underivable init status → the controller stays silent (failure still surfaced via status + `errors_total` + the Job pod logs). Restore the condition → the Job re-runs → init passes → the load proceeds.
- **Cases verified** (`cases.Scenario104Cases()`): HC.1-HC.5 probes + gating (resolved against the real built artifact — the init container FIRST/named, the 5-check script, the `dataload-scratch` volume + mounts on both containers), the `healthChecks` knob (default-on / `enabled:false` / custom `diskMinFreeMB`), the gpload-path gating (HC.2/HC.4/HC.5, no HC.1/HC.3), and the `DataLoadingHealthCheckFailed` Event (failed init → ONE Warning; failed MAIN → NO HC event).
- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario104-healthcheck-test.yaml` (cluster `healthcheck-test`, namespace `cloudberry-test`: pxf + gpfdist + `healthChecks {enabled:true, diskMinFreeMB:64, scratchSizeLimit:"64Mi"}`; an `s3-load` pxf job → `public.events` (HC.1/HC.2/HC.3/HC.5) and a `gpload-csv` gpload job → `public.events` (HC.4)).
- **Monitoring dependency**: the **kube-state-metrics** chart (`test/monitoring/kube-state-metrics`) must be deployed so `kube_job_status_failed` / `kube_pod_init_container_status_*` / `kube_deployment_status_replicas_available` flow to VictoriaMetrics; dashboard panels **274** (doc text), **275** (data-load job failures), **276** (gpfdist ready replicas).

```bash
# Run Scenario 104 functional suite (the dataload-healthcheck init container + the
# HC.1-HC.5 script + gating + the scratch volume/mounts; infra-free, ALWAYS runs)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario104 -v

# Integration (builder-level healthcheck-test artifacts against the real stack)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario104 -v

# E2E — Part A (contract, infra-free) always runs; Part B (live fail+restore of each HC
# on the deployed healthcheck-test cluster + pxf + gpfdist + the MinIO dataset) is gated
# by SCENARIO104_HC_LIVE:
#   KUBECONFIG=… SCENARIO104_HC_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario104 -v

# Performance (init-container overhead / fail-fast latency baseline). Tagged e2e + gated
# by the SAME live flag:
#   SCENARIO104_HC_LIVE=1 KUBECONFIG=… \
#     go test -tags=e2e -run=^$ -bench=BenchmarkScenario104 ./test/perf/...
```

#### Scenario 105 — DataLoadingStatus PXF Fields (S.1–S.5)

Flips the previously-Planned live **PXF health** sub-status of `status.dataLoading.pxf` to **Implemented**, and re-pins the already-Implemented S.2/S.4/S.5 fields. Acceptance scenario: *with PXF running and several jobs configured, `pxf.status: Running`; stop PXF on a segment → `Error`/`Stopped`; `pxf.servers` == configured-server count (S.2); `pxf.extensionsInstalled` lists `pxf`/`pxf_fdw` (S.3); concurrent jobs → `activeJobs` matches (S.4); after runs each `jobs[]` entry has `name`/`lastRun`/`lastStatus`/`rowsLoaded`/`duration` (S.5)*. TEST + LIVE-VERIFICATION + HONEST-OBSERVABILITY scenario.

> **Scenario numbering note.** Sequence: 102 = kafka-cdc, 103 = FDW-based loading path, 104 = pre-load health checks, **105 = DataLoadingStatus PXF fields** (this entry).

> **HONESTY invariant.** Both new fields are **observed-only and ABSENT when unobservable** — never synthesized. `pxf.status` derives ONLY from real segment-primary `pxf` container readiness; `pxf.extensionsInstalled` ONLY from a real read-only `pg_extension` probe.

- **S.1 — `pxf.status` (NEW).** `reconcilePxf` (`internal/controller/admin_controller.go`) stamps `pxf.status` ∈ `Running`/`Stopped`/`Error` from the real segment-primary `pxf` `ContainerStatuses`, aggregated by the SHARED honesty helpers `util.PXFReadyCount` + `util.PXFStatusFromReadiness` + `util.SegmentPrimaryPXFSelector` (the SAME helpers the `pxf status` API handler `handlePXFStatus` consumes — integration-pinned to agree). **NO `kubectl exec`, NO live HTTP probe, NO synthesized health.** Mapping: all ready (`ready==total>0`) → `Running`; some down (`0<ready<total`) → `Error` (degraded — the **KEY segment-stop transition**); none ready (`ready==0, total>0`) → `Stopped`; no pods observed / pod-list error → field **ABSENT** (non-fatal).
- **S.2 — `pxf.servers` (already Implemented, re-pinned).** `pxf.servers == len(pxf.servers)` (spec-derived config count, not a live-reachable count); survives the `MergePatch`.
- **S.3 — `pxf.extensionsInstalled` (NEW).** `observePxfExtensions` → `db.Client.ListPXFExtensions` (`internal/db/client.go`) runs a real read-only `pg_extension` query; lists `pxf`/`pxf_fdw` when actually installed (deterministic order), honest subset when only one present, **ABSENT (nil)** when the DB is unreachable OR none installed (best-effort/non-fatal — `patchDataLoadingStatus` emits it ONLY when non-nil; an empty array is never synthesized).
- **S.4 — `activeJobs` (already Implemented, re-pinned).** `activeJobs` == the enabled-job count (`configuredJobs == len(jobs)`); concurrency does not change the enabled-count invariant; `status.dataLoadingJobs` mirrors `activeJobs`.
- **S.5 — `jobs[]` runtime fields (already Implemented, re-pinned).** A terminal Succeeded Job + a `DATALOAD_ROWS` marker → `name`/`lastRun`(=startTime)/`lastStatus`/`rowsLoaded`(marker)/`duration`; non-terminal → `lastStatus` only; Failed → no `rowsLoaded` (never synthesized); never-run → only `name`/`enabled`.
- **Honest metrics (MX, NEW).** Two gauges emitted **only when observable**: `cloudberry_pxf_status{cluster,namespace}` (0=Stopped/1=Running/2=Error — `SetPXFStatus`; NOT recorded when status is ABSENT) and `cloudberry_pxf_extensions_installed{cluster,namespace}` (== `len(extensionsInstalled)` — `SetPXFExtensionsInstalled`; NOT emitted when the DB is unreachable / none observed). **Scenario 109** later added `cloudberry_pxf_service_up` (the per-segment disaggregation of `cloudberry_pxf_status`), the actuator-passthrough request/latency series, and the conditional `cloudberry_data_loading_bytes_total`; the remaining `cloudberry_pxf_*` runtime/health metrics + `cloudberry_gpfdist_*` stay **honestly absent (Planned)** and are never asserted (a NOT-emitted metric is the passing evidence).
- **Cases verified** (`cases.Scenario105Cases()`, `test/cases/scenario105_dataloading_status_cases.go`): the S.1–S.5 + MX catalog — builder/reconcile `-B` rows (over fakes/envtest + the shared `util.PXF*` helpers and a fake `db.Client` whose `ListPXFExtensionsFunc` is set) and live `-L` rows (the happy path + the segment-stop → `Error` transition + the non-pxf-image honesty leg). Unit coverage: `internal/db/pgxclient_test.go` (pgxmock `ListPXFExtensions`), `internal/controller/admin_controller_pxf_test.go` (`reconcilePxf` status + extensions + the patch-emits-only-when-non-nil leak guard + the metric recording).

```bash
# Run Scenario 105 functional suite (reconcilePxf status from fake ContainerStatuses +
# extensionsInstalled from a fake db.Client + the honest gauges; infra-free, ALWAYS runs)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario105 -v

# Integration (the shared util.PXF* helpers + handlePXFStatus agree; envtest)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario105 -v

# E2E — contract always runs; the DB-real ListPXFExtensions honesty leg is gated by
# SCENARIO105_DB_LIVE, and the live segment-stop → Error leg by the deployed cluster:
#   KUBECONFIG=… SCENARIO105_DB_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario105 -v
```

#### Scenario 106 — Server Configuration Update / Delete (SL.7–SL.8)

Adds **honest observability** to the already-existing SL.7/SL.8 mechanics (the full-replacement reconcile of the `<cluster>-pxf-servers` ConfigMap by `ensurePxfServersConfigMap`). Acceptance scenario: *patch a server's endpoint (e.g. `minio-warehouse`'s `fs.s3a.endpoint`) → the ConfigMap regenerates that server's `<server>__s3-site.xml` (others byte-identical), sidecars pick up on the next volume sync or an explicit `pxf sync`, and reads use the NEW endpoint (SL.7); remove a server from `dataLoading.pxf.servers[]` → its `<server>__*.xml` keys are dropped and referencing external/foreign tables fail until recreated (SL.8); in BOTH cases a `PXFServersChanged` event is emitted and `cloudberry_pxf_servers_changed_total` increments by exactly 1 — but ONLY on a real diff*. HONEST-OBSERVABILITY scenario.

> **Scenario numbering note.** Sequence: 103 = FDW-based loading path, 104 = pre-load health checks, 105 = DataLoadingStatus PXF fields, **106 = server configuration update / delete** (this entry).

> **HONESTY invariant.** The `PXFServersChanged` event AND the `cloudberry_pxf_servers_changed_total` counter fire **only on a real `<cluster>-pxf-servers` ConfigMap `Data` diff** (a server added, removed, or its rendered `*-site.xml` keys changed) — **never** on a no-op sync or a first-time create. The diff is the shared, pure `internal/util.DiffPXFServerNames` over the previous vs. desired ConfigMap `Data`, consumed by BOTH the controller and the API path so they never disagree.

- **SL.7 — Update.** Patching `minio-warehouse`'s `fs.s3a.endpoint` re-renders only that server's `<server>__s3-site.xml` key (every OTHER server's keys stay byte-identical → `DiffPXFServerNames` reports `updated=[minio-warehouse]`). Sidecars re-render on the next volume sync (the `pxf-cred-init` init container) or immediately via the explicit `cloudberry-ctl pxf sync` trigger (**Implemented — Scenario 95**); subsequent `pxf://` reads use the **new** endpoint. Steady-state correctness is structural (shared ConfigMap, byte-identical renders); `pxf sync` is the on-demand ConfigMap-refresh + segment-primary roll, not a prerequisite.
- **SL.8 — Delete.** Removing a server from `dataLoading.pxf.servers[]` drops its `<server>__*.xml` keys on the next full-replacement reconcile (remaining servers intact → `removed=[<server>]`); external/foreign tables referencing the deleted `SERVER` fail until recreated.
- **MX — Honest event + metric.** On a real `Data` diff, BOTH the controller reconcile (`ensurePxfServersConfigMap` → `emitPXFServersChanged`, `internal/controller/cluster_controller.go`) and the explicit `pxf sync` API path (`handlePXFSync` → `recordPXFServersChanged`, `internal/api/server.go`) emit a Normal **`PXFServersChanged`** event (`cbv1alpha1.EventReasonPXFServersChanged`, message `PXF servers changed: added=[..] removed=[..] updated=[..]` via `util.FormatPXFServersChangedMessage`) and increment **`cloudberry_pxf_servers_changed_total{cluster,namespace}`** (`IncPXFServersChanged`, `internal/metrics/metrics.go`). A no-op sync / first create fires NEITHER.
- **Cases verified** (`cases.Scenario106Cases()`, `test/cases/scenario106_server_config_cases.go`): the SL.7/SL.8 + MX catalog (`Scenario106EventReason = "PXFServersChanged"`, `Scenario106ChangedMetric = "cloudberry_pxf_servers_changed_total"`). Unit coverage: `internal/util/pxf_test.go` (`DiffPXFServerNames` + `FormatPXFServersChangedMessage`), `internal/controller/cluster_controller_pxf_servers_test.go` (exactly-once event + counter on a real diff), `internal/api/pxf_handlers_test.go` (the `pxf sync` path increments exactly once), `internal/metrics/metrics_test.go` (counter increments by exactly 1 per call).

```bash
# Run Scenario 106 functional suite (router → handler → recorder; the honest event +
# counter fire only on a real diff via the shared util.DiffPXFServerNames; infra-free)
go test -tags=functional -race ./test/functional/ -run TestFunctional_Scenario106 -v

# Integration (the shared util.DiffPXFServerNames over the rendered ConfigMap Data; envtest)
go test -tags=integration -race ./test/integration/ -run TestIntegration_Scenario106 -v

# Perf benchmark (the pure diff over a patched render)
go test -tags=perf -bench BenchmarkScenario106 ./test/perf/ -run '^$'

# E2E — gated by SCENARIO106_LIVE (asserts the PXFServersChanged event was recorded
# for the real diff against a deployed cluster):
#   KUBECONFIG=… SCENARIO106_LIVE=1 go test -tags=e2e ./test/e2e/... \
#     -run TestE2E_Scenario106 -v
```

#### Scenario 109 — All Prometheus Metrics (M.1–M.16)

Closes out the Prometheus metric catalog (M.1–M.16) under one defining rule: **HONESTY** — every emitted metric traces to a **REAL** source; a metric with no honest source stays **intentionally ABSENT** and documented, **never synthesized**. Acceptance scenario: *verify each Implemented metric is emitted from a real source with honest labels, and each honestly-absent metric is NOT emitted (its absence is the PASS); kill a segment's PXF → its `pxf_service_up{segment_host}` → 0; drive a job lifecycle → `data_loading_job_status` cycles 0→1→2→3*. HONESTY scenario.

> **Scenario numbering note.** Sequence: 105 = DataLoadingStatus PXF fields, 106 = server configuration update / delete, **109 = all Prometheus metrics M.1–M.16** (this entry).

> **HONESTY invariant.** A NOT-emitted metric is a **PASS**. The honestly-absent families (`cloudberry_pxf_bytes_transferred_total` M.4, `cloudberry_pxf_records_total` M.5, `cloudberry_pxf_active_connections` M.7, `cloudberry_gpfdist_connections_active` M.15, `cloudberry_gpfdist_bytes_served_total` M.16) and the **folded** `cloudberry_pxf_errors_total` (M.6) are **never registered or synthesized**; the `109-HONESTY` guard test enumerates them and asserts none is registered (a regression lock against future fabrication).

- **M.1 — `cloudberry_pxf_service_up{cluster,namespace,segment_host}` (IMPL).** The **per-segment disaggregation** of `cloudberry_pxf_status`: `SetPXFServiceUp` records `1`/`0` per **observed** segment-primary pod from real `pxf` `ContainerStatuses[pxf].Ready` (via the new `util.PXFReadyByHost`, keyed by the pod's segment-host label), set in `reconcilePxf`. Killing one segment's `pxf` container flips that host's series to `0` while others stay `1`; an unobserved host gets **no** series (never synthesized). Cases `109-M1-U`/`-F`/`-L`/`-KILL`.
- **M.2/M.3 — `cloudberry_pxf_requests_total` / `cloudberry_pxf_request_duration_seconds` (IMPL, actuator passthrough).** The PXF Spring Boot Actuator `/actuator/prometheus` endpoint is enabled on the sidecar (`MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE=health,prometheus`) and a **dedicated vmagent scrape job** picks up the **real** `http_server_requests_seconds_count`/`_sum`/`_bucket` series at `:5888`. **A single pod scrape annotation cannot cover both exporters** — the segment-primary pod already carries `prometheus.io/scrape`/`port=9187`/`path=/metrics` for the pg query-exporter, so an **explicit additional `scrape_config`** (NOT annotation reuse) targets `:5888/actuator/prometheus` (re-using the annotation would silently scrape nothing — a honesty trap). **Label-honesty caveat:** the request count + latency histogram are REAL, but the catalog's `server`/`profile`/`operation` labels are **NOT** honestly derivable from the actuator URI → the series flow under their **actuator-native** `uri`/`method`/`status` labels (the URI is not relabeled into `{server,profile,operation}`). Cases `109-M2-U`/`-F`/`-L`/`-LABELHONEST`, `109-M3-F`/`-L`, `109-VM-SCRAPE` (both the `:9187` exporter AND the `:5888` actuator job scrape the SAME segment pod).
- **M.10 — `cloudberry_data_loading_bytes_total{cluster,namespace,job,source_type}` (IMPL, conditional/honest).** Emitted from the **real** `DATALOAD_BYTES=<n>` marker the gpload script computes via `wc -c` for a **local gpload input source** (harvested by `harvestDataLoadBytes`/`parseDataLoadBytesMessage`, mirroring the rows path, wired into `recordDataLoadJobMetrics`). For external-table/pxf/FDW/continuous loads — where psql returns only a rowcount tag — the marker is **not emitted** and the metric is honestly **absent**. Cases `109-M10-U`/`-F`/`-BUILDER`/`-L`/`-ABSENT`.
- **M.6 — `cloudberry_pxf_errors_total` (FOLDED, no synthetic metric).** PXF exposes no typed error counts. The honest error signals are the existing `cloudberry_data_loading_errors_total{job}` on a Failed load (+ `cloudberry_data_loading_job_status=3`) and actuator non-2xx (`http_server_requests{status=~"4..|5.."}`). **No typed `cloudberry_pxf_errors_total{error_type}` is registered.** Cases `109-M6-U` (no family registered), `109-M6-F` (forced pxf:// error → `data_loading_errors_total` +1, `job_status=3`), `109-M6-L` (actuator non-2xx visible).
- **M.4/M.5/M.7/M.15/M.16 — honestly absent (ABSENT).** No honest source in PXF 2.1.0 / gpfdist. M.5 record throughput is observed via `cloudberry_data_loading_rows_total` (`109-M5-SUBST`); M.7's `tomcat.threads.busy` is a JVM-thread proxy, not external connections; M.15/M.16 gpfdist has only `/var/log/gpfdist.log`, no scrapable endpoint. Cases `109-M4-ABSENT`, `109-M5-ABSENT`, `109-M7-ABSENT`, `109-M15-ABSENT`, `109-M16-ABSENT` (each asserts NOT-emitted).
- **M.8/M.9/M.11/M.12/M.13/M.14 — already Implemented (confirm).** `109-M14-CYCLE` asserts the job lifecycle drives `job_status` 0 (pending) → 1 (running) → 2 (success), and → 3 on a forced failure.
- **Cases verified** (`cases.Scenario109Cases()`): the `109-M{1..16}-{U,F,L}` catalog + the `109-HONESTY` (absent-family regression lock) and `109-VM-SCRAPE` (two-job scrape design) cross-cutting cases. New recorder surface: `SetPXFServiceUp` + `RecordDataLoadingBytes` (`internal/metrics/metrics.go`); `util.PXFReadyByHost` (`internal/util/pxf.go`); `harvestDataLoadBytes`/`parseDataLoadBytesMessage` (`internal/controller/dataload_controller.go`); the actuator env + `DATALOAD_BYTES` marker (`internal/builder/{pxf_builder,dataload_builder,gpload_builder}.go`); the explicit `:5888` scrape job (`test/monitoring/vmagent/templates/configmap.yaml`).

#### Scenario 110 — Webhook Validation (All Rules) (W.1–W.15)

The **complete, systematic** rejected-CR negative-test matrix proving that **each** of the 15 data-loading webhook rules **(a) rejects** an otherwise-valid `CloudberryCluster` carrying exactly **one** violation, **(b) with a descriptive (field-path + reason) error**, and **(c) the rejected CR does NOT persist** (a follow-up `GET` is `NotFound`), plus a **CONTROL** (a fully-valid CR admits — no false-positive). This is a **verification scenario with no production change** — all 15 rules are already implemented in `internal/webhook/validating.go` (and three are also CRD-enum-guarded); Scenario 110 contributes the multi-layer matrix and the per-rule **rejection-source** analysis. It is **complementary to** [Scenario 89](#scenario-89--pxf-data-loading-webhook-validation-all-rules) (the original validator-direct W.1–W.15 suite).

> **Rejection source per rule (verified against `internal/webhook/validating.go` + the CRD enums).** **11 WEBHOOK-enforced** — only the webhook rejects (the user sees our descriptive message on a live apply): **W.1** (empty `pxf.image`), **W.2** (empty/dup server name), **W.4** (s3 missing `fs.s3a.endpoint`/`credentialSecrets`), **W.5** (jdbc missing `jdbc.driver`/`jdbc.url`), **W.6** (hdfs missing `fs.defaultFS`), **W.7** (empty/dup job name), **W.9** (undefined `pxfJob.server`), **W.10** (`profile: s3:nonsense`), **W.13** (`schedule: "not a cron"`), **W.14** (partitioning column without range/interval). **3 CRD-SCHEMA-enum** — the CRD OpenAPI `Enum` rejects at the apiserver **before** the webhook runs (the webhook keeps the rule for defense-in-depth, and the apiserver error is itself descriptive — `Unsupported value: "…"`): **W.3** (`servers[].type: ftp`; enum `s3;hdfs;jdbc;hbase;hive;gs;abfss;wasbs;custom`), **W.8** (`jobs[].type: spark`; enum `pxf;gpload`), **W.15** (`segmentRejectLimitType: fraction`; enum `rows;percent`). **2 BOTH** — expression-dependent: **W.11** (`pxfJob.targetTable`) and **W.12** (`gploadJob.targetTable`) — an **omitted** key is rejected by the CRD schema `required`, an **empty-string** value by the webhook (the live `-L` rows use the omitted-key expression → schema `required`).

- **The 15 rules + triggers**: W.1 enabled+empty `pxf.image` · W.2 server empty/dup name · W.3 `type: ftp` · W.4 s3 missing endpoint/creds · W.5 jdbc missing driver/url · W.6 hdfs missing `fs.defaultFS` · W.7 job empty/dup name · W.8 `type: spark` · W.9 `pxfJob.server` undefined · W.10 profile `s3:nonsense` · W.11 `pxfJob` no `targetTable` · W.12 `gploadJob` no `targetTable` · W.13 schedule `"not a cron"` · W.14 partitioning column w/o range/interval · W.15 `segmentRejectLimitType: fraction`.
- **Descriptive error + NO-PERSIST**: every rejection names the field path + reason; each live (`-L`) row carries the `NoPersist` contract realized as a follow-up `GET` → `NotFound` (`110-NOPERSIST-L`).
- **CONTROL (no false-positive)**: the fully-valid base CR admits (`110-CONTROL-admit-F` functional; `110-CONTROL-admit-L` applies live, GETs back, then cleans up).
- **Three layers**: unit (validator-direct, `internal/webhook/scenario110_validation_test.go` — the schema-enum rules assert the webhook defense-in-depth message), functional (`CloudberryClusterValidator.ValidateCreate` over a base-valid CR with one violation), and live e2e (`kubectl apply` → reject → `GET NotFound`, `KUBECONFIG` + `SCENARIO110_LIVE` gated, SKIPs cleanly when unset).
- **Cases verified** (`cases.Scenario110WebhookCases()`): the `110-W{1..15}-{U,F,L}` catalog (with the W.2/W.4/W.5/W.7 OR sub-cases) + the `110-CONTROL-admit-{F,L}` and `110-NOPERSIST-L` cross-cutting rows. Artifacts: `test/cases/scenario110_webhook_validation_cases.go`, `internal/webhook/scenario110_validation_test.go`, and `test/{functional,integration,e2e,perf}/scenario110_webhook_validation*`.

```bash
# Run Scenario 110 webhook validation unit tests (validator-direct)
go test ./internal/webhook/... -v -run TestScenario110

# Run Scenario 110 functional + integration tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario110
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario110

# Run Scenario 110 e2e tests (live kubectl-apply reject + no-persist gated on KUBECONFIG + SCENARIO110_LIVE)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario110
```

See [spec 12 §Scenario 110](../specifications/12-data-loading-spec.md#scenario-110--webhook-validation-all-rules).

#### Scenario 111 — Data-Loading Security (SE.1–SE.6, SL.6)

Flips the previously-Planned data-loading **Security** controls (SE.1–SE.6, SL.6) to **Implemented**, with each control explicitly classified **REAL** (proven end-to-end) or **CONFIG-ONLY** (rendered config verified; a live handshake is **never faked** because the test env has no KDC and no TLS-speaking source). TEST + LIVE-VERIFICATION + HONESTY scenario.

> **Scenario numbering note.** Sequence: 108 = all CLI commands, 109 = all Prometheus metrics, 110 = webhook-validation rejected-CR matrix, **111 = data-loading security** (this entry).

- **SE.6 — dedicated minimal-privilege DB role (REAL).** `db.EnsureDataLoaderRole` runs (idempotent) `CREATE ROLE <role> NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN` and grants **only** `SELECT,INSERT ON PROTOCOL pxf`. Opt-in via the new CRD field **`dataLoading.pxf.dataLoaderRole`** (`PxfSpec.DataLoaderRole`): empty (default) ⇒ `gpadmin` (the existing behavior, byte-identical when unset); a non-empty value other than `gpadmin` targets the protocol GRANTs at the dedicated role. **Proof:** the role has the pxf protocol grants **and** cannot perform unrelated privileged ops (`CREATE ROLE`/`CREATE DATABASE` denied) — REAL least-privilege.
- **SE.4 — Kerberos keytab from Secret (config-correct).** New CRD field **`dataLoading.pxf.servers[].kerberos`** (`PxfServerSpec.Kerberos` → `PxfKerberosSpec{Principal, KeytabSecret{Name,Key}, Krb5ConfigMap, Realm}`). The operator mounts the keytab Secret on **both** the PXF sidecar and `pxf-cred-init` at `$PXF_BASE/keytabs/<server>/<key>`, renders `hadoop.security.authentication=kerberos` + `pxf.service.kerberos.principal=<principal>` + `pxf.service.kerberos.keytab=$PXF_BASE/keytabs/<server>/<key>` into `core-site.xml`, and optionally mounts a krb5.conf ConfigMap at `/etc/krb5.conf` (`KRB5_CONFIG`). The webhook **requires** `principal`+`keytabSecret{name,key}` when `kerberos` is set and **rejects** `kerberos` on non-`hdfs`/`hive`/`hbase` types. **HONESTY:** live *authenticated* Hadoop/Hive/HBase access is **CONFIG-ONLY** — the test env has **no KDC** and the operator **never runs a live `kinit`**; config-correctness is verified, a live Kerberos handshake is never simulated.
- **SE.5 — segment↔sidecar `localhost`-only NetworkPolicy (REAL).** `BuildPXFClusterNetworkPolicy` (`internal/builder/pxf_builder.go`) emits a NetworkPolicy selecting the segment-primary pods whose ingress rules **omit** cross-pod access to PXF `:5888`. Because same-pod `localhost` traffic is never subject to a NetworkPolicy, the in-pod segment→sidecar path keeps working: the policy is applied **and** a real `pxf://` load still succeeds (the proof is the combination, not the policy alone) — REAL.
- **SE.1 / SL.6 — init-container secret rendering, no plaintext in ConfigMap (REAL).** `pxf-cred-init` `envsubst`-resolves the `${PLACEHOLDER}` site-XML from `credentialSecrets[]` into the ephemeral `pxf-servers` emptyDir at pod start; the rendered `<server>/<file>.xml` carries the real value in the pod fs while the `<cluster>-pxf-servers` ConfigMap carries only the `${...}` token. A scan of the ConfigMap `Data` asserts **no** secret value appears (SL.6). Secrets live **only** in the ephemeral pod filesystem.
- **SE.2 / SE.3 — JDBC / S3 TLS passthrough (declarative).** TLS is wired declaratively: JDBC URL / `ssl` params render into `jdbc-site.xml` (SE.2); `fs.s3a.connection.ssl.enabled=true` renders into `s3-site.xml` (SE.3). **HONESTY:** a live encrypted handshake is asserted **only** against a real TLS-speaking source — otherwise **CONFIG-ONLY** (rendered config verified, never a faked handshake).
- **Artifacts**: `api/v1alpha1/types.go` (`PxfSpec.DataLoaderRole`, `PxfServerSpec.Kerberos`/`PxfKerberosSpec`) + `zz_generated.deepcopy.go` + `api/v1alpha1/deepcopy_security_scenario111_test.go`; `internal/db` (`EnsureDataLoaderRole`); `internal/builder/pxf_builder.go` (`BuildPXFClusterNetworkPolicy`, keytab/krb5 mounts, `core-site.xml` Kerberos rendering); `internal/webhook` (kerberos principal+keytab requirement + non-Hadoop-type rejection).

See [spec 12 §Scenario 111](../specifications/12-data-loading-spec.md#scenario-111--security-se1se6-sl6) and [§Security Considerations](../specifications/12-data-loading-spec.md#security-considerations).

#### Scenario 112 — Data-Loading Disabled States (DIS.1–DIS.3)

Makes the three data-loading *disabled* states honest and *active*. TEST + LIVE-VERIFICATION + HONESTY scenario; **DIS.1 is a behavior change** (teardown), DIS.2/DIS.3 document the real observable consequences.

- **DIS.1 — `dataLoading.enabled: false` TEARS DOWN (no longer a no-op).** `reconcileDataLoading` (`internal/controller/admin_controller.go`) now dispatches to **`cleanupDataLoading`** whenever `dataLoading == nil` **or** `dataLoading.enabled == false` (disabling a cluster does NOT fire ownerRef GC, so these explicit best-effort deletes reclaim the stale resources). It deletes: the `<cluster>-pxf-servers` ConfigMap (via the CLUSTER controller `ensurePxfServersConfigMap` **delete-when-disabled**), the gpfdist `Deployment`/`Service`/`PVC` (`deleteGpfdistResources`), all data-loading `Job`s **and** `CronJob`s (`deleteDataLoadingWorkloads`), the gpload control-file ConfigMaps (`deleteGploadControlFileConfigMaps`), and the PXF cluster `NetworkPolicy` (SE.5); the PXF **sidecar** is dropped from the segment-primary StatefulSet (re-rendered without it via the `pxfSidecarEnabled` gate). It then clears `Status.DataLoading`, sets `DataLoadingConfigured=False` reason **`DataLoadingDisabled`**, emits a **one-shot `DataLoadingDisabled` Normal event** (`EventReasonDataLoadingDisabled`, de-duplicated on the transition — a never-enabled or steady-disabled reconcile emits none), and zeroes `cloudberry_data_loading_jobs_active` + `cloudberry_pxf_servers_configured`. **Re-enable → the idempotent (get-or-create) reconcile redeploys everything** (no special-casing).
- **DIS.1 — API `DATA_LOADING_NOT_ENABLED`.** The data-loading REST surface (`internal/api/{server,dataloading}.go`) reports `DATA_LOADING_NOT_ENABLED`: **mutating** endpoints (`POST/PUT/DELETE` jobs, `start`/`stop`, `external-tables`, `jobs/{job}/logs`) → `400`; **read** endpoints (`GET` jobs list / one job) → `200` with a **disabled envelope** (`writeDataLoadingDisabled`). **PXF precedence:** a PXF endpoint on a DL-disabled cluster reports `DATA_LOADING_NOT_ENABLED` (the broader gate), NOT `PXF_NOT_ENABLED` (`getPXFCluster` checks `dataLoadingEnabled` before `pxfEnabled`).
- **DIS.2 — `pxf.enabled: false` independence.** No PXF sidecars (the `pxfSidecarEnabled` gate is false), no PXF extensions setup, and no `<cluster>-pxf-servers` ConfigMap (`ensurePxfServersConfigMap` **deletes** a stale CM when the sidecar is disabled — the only new code this cycle); gpload-type jobs are independent of PXF and **still function**.
- **DIS.3 — `gpfdist.enabled: false`.** The gpfdist `Deployment`/`Service`/`PVC` are **GC'd** (`reconcileGpfdist`); `inputSource.type: local` gpload jobs **still work**; a `gpfdist`-source gpload job reports the missing dependency via the **HONEST RUNTIME** signal — gpload can't reach the absent gpfdist host → **Job Failed** + `cloudberry_data_loading_errors_total` + `status=Failed`. **HONESTY:** the pre-load health-check **HC.4 (gpfdist reachability) is SKIPPED when gpfdist is disabled** (it is gated on `gpfdist.enabled`), so the dependency-missing signal is the **runtime gpload failure**, NOT a pre-flight check — the spec does **not** fabricate a pre-flight dependency check here.
- **Artifacts**: `internal/controller/admin_controller.go` (`cleanupDataLoading`, the `reconcileDataLoading` dispatch, `reconcileGpfdist` GC-when-disabled); `internal/controller/cluster_controller.go` (`ensurePxfServersConfigMap` delete-when-disabled); `internal/builder/{builder,pxf_builder,networkpolicy_builder}.go` (`pxfSidecarEnabled` gate); `internal/api/{server,dataloading}.go` (`DATA_LOADING_NOT_ENABLED`, `writeDataLoadingDisabled`, DL-before-PXF precedence); `api/v1alpha1/types.go` (`EventReasonDataLoadingDisabled`); tests `internal/controller/{dataload_disabled_scenario112,cluster_pxf_servers_delete_scenario112}_test.go`, `internal/api/scenario112_disabled_test.go`, `test/cases/scenario112_disabled_states_cases.go`, `test/{functional,e2e}/scenario112_disabled_states*`.

See [spec 12 §Scenario 112](../specifications/12-data-loading-spec.md#scenario-112--disabled-states-dis1dis3).

#### Scenario 71 — Enable Backup with Full S3 Configuration

Exercises the full S3 backup configuration (bucket, endpoint, region, folder, encryption, `forcePathStyle`, multipart tuning, retention, schedule) against MinIO, with **two credential-source variants**, and performs a backup → clean → restore data cycle on a live cluster.

**Precondition**: running CloudberryCluster, MinIO reachable, Secret `backup-s3-credentials` present (and, for the Vault variant, the same credentials stored at Vault path `secret/data/cloudberry/backup-s3`).

- **71a — Secret credentials**: `destination.s3.credentialSecret` references the Kubernetes Secret `backup-s3-credentials` (`accessKeyField: aws_access_key_id`, `secretKeyField: aws_secret_access_key`). The backup/restore Job injects `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` via `SecretKeyRef` to that Secret.
- **71b — Vault credentials**: `destination.s3.vaultSecret` references Vault path `secret/data/cloudberry/backup-s3` (requires `spec.vault.enabled`). The operator reads the path at reconcile time and materializes the Secret `<cluster>-backup-s3-vault-creds`, which the Job consumes via `SecretKeyRef`. Credentials are never written into the Job spec as plaintext. (The documented path form is now the **logical** KV path, `secret/cloudberry/backup-s3` — the operator injects `data/` for KV-v2 mounts automatically; the explicit `secret/data/...` form used by this scenario remains accepted with a webhook warning.)

Both variants verify the full S3 plugin config (`region`, `endpoint`, `bucket`, `folder`, `encryption`) and env (`S3_FORCE_PATH_STYLE=true`, multipart `BACKUP_*`/`RESTORE_*` = `4`/`10MB`). The functional/E2E Go tests assert the operator produces the correct ConfigMap, materialized creds Secret (Vault variant), and Job env/args; the actual backup→clean→restore data cycle (≈100 MB in `mydb`) is exercised at live deployment time.

> **Note**: `gpbackup_s3_plugin` 2.1.0 rejects the `aws_signature_version` option, so the operator's generated S3 plugin config no longer emits it (path-style is auto-derived for custom MinIO endpoints via `forcePathStyle`).

**Live data cycle (coordinator-exec)**: because `gpbackup` is an MPP tool — the coordinator dispatches to every segment over SSH (port 22) and a standalone Job pod is not a real segment host — the supported live backup/restore data cycle runs `gpbackup`/`gprestore` **inside the coordinator pod**. The orchestration script `test/e2e/scripts/scenario71-backup-restore.sh` drives this cycle for both variants and supports `EXEC_MODE=coordinator` (default) and `EXEC_MODE=rest`:

```bash
# Secret variant — 100MB live backup -> S3(MinIO) -> drop -> restore -> verify
DATA_TARGET_MB=100 bash test/e2e/scripts/scenario71-backup-restore.sh \
  --cluster scenario71-secret --variant secret

# Vault variant
DATA_TARGET_MB=100 bash test/e2e/scripts/scenario71-backup-restore.sh \
  --cluster scenario71-vault --variant vault
```

Verified live for both variants: 100MB `mydb` backed up to `cloudberry-backups/backups`, `mydb` dropped, restored, row counts match.

- **Sample CRs**: `deploy/helm/cloudberry-operator/config/samples/scenario71-backup-s3-secret.yaml`, `scenario71-backup-s3-vault.yaml`
- **Live orchestration script**: `test/e2e/scripts/scenario71-backup-restore.sh`
- **Test case catalog**: `Scenario71BackupConfigCase` type and `Scenario71BackupConfigCases()` in `test/cases/test_cases.go` (71a secret, 71b vault)
- **Functional tests**: `test/functional/scenario71_backup_s3_config_test.go`
- **E2E tests**: `test/e2e/scenario71_backup_s3_config_e2e_test.go`

```bash
# Run Scenario 71 backup S3 config functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario71

# Run Scenario 71 backup S3 config E2E tests (live resource-creation check gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario71
```

#### Scenario 72 — Backup Infrastructure Deployment

Verifies the backup **infrastructure** the operator deploys for a cluster with backups enabled — the toolchain image, the backup RBAC, the S3 plugin ConfigMap, the Job labels/namespace, the Job container env (incl. `envsubst`), and the `jobTemplate` pod-template overrides. The sample CR enables backups with the full S3 destination block (Secret credentials) **and** an explicit `backup.jobTemplate` exercising every override.

Six infrastructure verifications:

- **V1 — Image binaries**: `gpbackup`, `gprestore`, `gpbackup_s3_plugin` present in `cloudberry-backup:2.1.0` (verified live via `docker run`; the Job container uses the configured image).
- **V2 — RBAC**: `cloudberry-backup-sa` ServiceAccount + `cloudberry-backup-role` Role (`secrets` get, `configmaps` get, `events` create/patch) + RoleBinding (rendered from `deploy/helm/cloudberry-operator/templates/backup-rbac.yaml`; verified live and by `helm template`). The Job references `cloudberry-backup-sa`.
- **V3 — S3 ConfigMap**: `<cluster>-backup-s3-config` carries `executablepath: /usr/local/bin/gpbackup_s3_plugin`, the region/endpoint/credentials/bucket/folder/encryption placeholders and the four multipart placeholders, and **no** `aws_signature_version`.
- **V4 — Job labels/namespace**: Job in the cluster namespace labelled `app.kubernetes.io/managed-by: cloudberry-operator`, `avsoft.io/cluster: <cluster>`, `avsoft.io/component: backup`, `avsoft.io/backup-operation: backup`.
- **V5 — Job env + envsubst**: `CBDB_DATABASE`, `PGHOST`, `PGPORT`, `COMPRESSION_LEVEL`, `COMPRESSION_TYPE`, `BACKUP_JOBS` (defaults `1`/`gzip`/`1`; AWS creds via `SecretKeyRef` to `backup-s3-credentials`), rendering `/tmp/s3-config.yaml`. These env vars are informational; the CLI still passes `--dbname`/`--compression-level`/`--compression-type`/`--jobs`.
- **V6 — jobTemplate overrides**: `resources` (req `500m`/`512Mi`, lim `2`/`2Gi`), `nodeSelector` (`kubernetes.io/os=linux`), `tolerations` (`dedicated=backup:NoSchedule`), `serviceAccountName` (`cloudberry-backup-sa`), `backoffLimit=2`, `activeDeadlineSeconds=7200`, `ttlSecondsAfterFinished=86400` all propagate to the built Job.

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario72-backup-infrastructure.yaml`
- **Functional tests**: `test/functional/scenario72_backup_infrastructure_test.go`
- **E2E tests**: `test/e2e/scenario72_backup_infrastructure_e2e_test.go`

```bash
# Run Scenario 72 backup infrastructure functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario72

# Run Scenario 72 backup infrastructure E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario72
```

#### Scenario 73 — On-Demand Backup with gpbackup Options

Verifies that an on-demand backup (`POST /api/v1alpha1/clusters/{name}/backups`) creates a Kubernetes **Job DIRECTLY** (not via the scheduled CronJob) and renders the per-request `gpbackupOptions` into the `gpbackup` CLI invocation. The 73a/73b options are supplied **per-request at trigger time via REST** — they are **not** baked into the CR; the sample CR's cluster-level `backup.gpbackup` defaults are harmless and are overridden by the per-request options.

- **73a — Standard options**: `compressionLevel=6`, `compressionType=zstd`, `jobs=4`, `withStats=true`, `withoutGlobals=true`, `includeSchemas=[public, analytics]`.

  ```bash
  curl -X POST 'http://localhost:8080/api/v1alpha1/clusters/scenario73/backups?namespace=cloudberry-test' \
    -H 'Content-Type: application/json' \
    -d '{"type":"full","databases":["mydb"],
         "gpbackupOptions":{"compressionLevel":6,"compressionType":"zstd",
           "jobs":4,"withStats":true,"withoutGlobals":true,
           "includeSchemas":["public","analytics"]}}'
  ```

  Verified gpbackup args: `--compression-level 6 --compression-type zstd --jobs 4 --with-stats --without-globals --include-schema public --include-schema analytics` (one `--include-schema` per schema), and the operator returns a `Job` (never a `CronJob`).

- **73b — noCompression override**: `noCompression=true` together with `compressionLevel=6`.

  ```bash
  curl -X POST 'http://localhost:8080/api/v1alpha1/clusters/scenario73/backups?namespace=cloudberry-test' \
    -H 'Content-Type: application/json' \
    -d '{"type":"full","databases":["mydb"],
         "gpbackupOptions":{"noCompression":true,"compressionLevel":6}}'
  ```

  Verified gpbackup args: `--no-compression` is present and `--compression-level` is **absent** — the compression level is ignored (`--no-compression` precedence).

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario73-backup-options.yaml`
- **Functional tests**: `test/functional/scenario73_backup_options_test.go`
- **E2E tests**: `test/e2e/scenario73_backup_options_e2e_test.go`

```bash
# Run Scenario 73 on-demand backup options functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario73

# Run Scenario 73 on-demand backup options E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario73
```

#### Scenario 74 — Single Data File + Copy Queue + gpbackup_helper + Full-Option Restore

Verifies a **single-data-file** backup (with `--copy-queue-size`, which requires `gpbackup_helper` on every segment) followed by a **full-option restore** that exercises every `gprestore` option and the operator's three mutual-exclusivity precedence rules. Both option sets are supplied **per-request via REST**; the on-demand `POST` creates a Kubernetes **Job DIRECTLY** (not via the scheduled CronJob).

- **Single-data-file backup**: `gpbackupOptions{singleDataFile:true, copyQueueSize:4}`.

  ```bash
  curl -X POST 'http://localhost:8080/api/v1alpha1/clusters/scenario74/backups?namespace=cloudberry-test' \
    -H 'Content-Type: application/json' \
    -d '{"type":"full","databases":["mydb"],
         "gpbackupOptions":{"singleDataFile":true,"copyQueueSize":4}}'
  ```

  Verified gpbackup args: `--single-data-file --copy-queue-size 4`, with `--jobs` **omitted** (`gpbackup` rejects `--jobs` in single-data-file mode). Requires `gpbackup_helper` on every segment (present in `cloudberry-official:2.1.0`); produces exactly **one consolidated data file per segment** (`gpbackup_<contentid>_<TS>.gz`) plus a per-segment `_toc.yaml` and shared coordinator metadata. The operator returns a `Job` (never a `CronJob`).

- **Full-option restore**: `jobs=4`, `redirectDb=mydb_restored`, `redirectSchema=restored`, `createDb=true`, `includeSchemas=[public, analytics]`, `includeTables=[public.users, public.orders]`, `withGlobals=false`, `withStats=true`, `runAnalyze=true`, `onErrorContinue=true`, `truncateTable=false`.

  ```bash
  curl -X POST 'http://localhost:8080/api/v1alpha1/clusters/scenario74/backups/<timestamp>/restore?namespace=cloudberry-test' \
    -H 'Content-Type: application/json' \
    -d '{"databases":["mydb"],
         "gprestoreOptions":{"jobs":4,"redirectDb":"mydb_restored","redirectSchema":"restored",
           "createDb":true,"includeSchemas":["public","analytics"],
           "includeTables":["public.users","public.orders"],
           "withGlobals":false,"withStats":true,"runAnalyze":true,
           "onErrorContinue":true,"truncateTable":false}}'
  ```

  The operator resolves the conflicting options so the `gprestore` invocation stays valid:

  - **`--include-schema` / `--include-table` mutually exclusive** → emits `--include-table public.users --include-table public.orders` (table-level precedence), **omits** `--include-schema`.
  - **`--with-stats` / `--run-analyze` mutually exclusive** → emits `--run-analyze` (run-analyze precedence — ANALYZE recomputes statistics), **omits** `--with-stats`.
  - **`--jobs` invalid for a single-data-file restore** → the single-data-file restore parallelism flag is `--copy-queue-size`, so the live cycle maps `jobs=4` to `--copy-queue-size 4`.
  - **`--redirect-schema` requires a pre-existing schema** → `--create-db` creates the database but not the schema, so the flow pre-creates **both** `mydb_restored` and the `restored` schema before restoring.

  `withGlobals=false` / `truncateTable=false` are **omitted**. Verified outcome: `mydb_restored` is created and populated; objects are redirected into the `restored` schema; ANALYZE ran (`pg_stats` has rows for the restored tables).

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario74-single-data-file.yaml`
- **Functional tests**: `test/functional/scenario74_single_data_file_test.go`
- **E2E tests**: `test/e2e/scenario74_single_data_file_e2e_test.go`
- **API restore test**: `internal/api/scenario74_restore_test.go`
- **Live data cycle**: `test/e2e/scripts/scenario74-single-data-file.sh`

```bash
# Run Scenario 74 single-data-file functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario74

# Run Scenario 74 single-data-file E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario74

# Run Scenario 74 API restore round-trip test
go test ./internal/api/... -v -run TestHandleRestoreBackup_Scenario74Args
```

#### Scenario 75 — Compression Matrix (gzip vs zstd)

Triggers **two** on-demand full backups of the **same** data that differ **only** by compression algorithm — `gzip` and `zstd` — at the **same** compression level (`6`). Both backups complete cleanly and **both** restore into their own redirect databases; the on-disk sizes differ (zstd smaller). Both option sets are supplied **per-request via REST**; each on-demand `POST` creates a Kubernetes **Job DIRECTLY** (not via the scheduled CronJob).

- **gzip backup**: `gpbackupOptions{compressionType:"gzip", compressionLevel:6, includeSchemas:["public"]}`.

  ```bash
  curl -X POST 'http://localhost:8080/api/v1alpha1/clusters/scenario75/backups?namespace=cloudberry-test' \
    -H 'Content-Type: application/json' \
    -d '{"type":"full","databases":["mydb"],
         "gpbackupOptions":{"compressionType":"gzip","compressionLevel":6,"includeSchemas":["public"]}}'
  ```

- **zstd backup**: `gpbackupOptions{compressionType:"zstd", compressionLevel:6, includeSchemas:["public"]}`.

  ```bash
  curl -X POST 'http://localhost:8080/api/v1alpha1/clusters/scenario75/backups?namespace=cloudberry-test' \
    -H 'Content-Type: application/json' \
    -d '{"type":"full","databases":["mydb"],
         "gpbackupOptions":{"compressionType":"zstd","compressionLevel":6,"includeSchemas":["public"]}}'
  ```

  Verified gpbackup args differ in **exactly one** value: `--compression-type gzip` vs `--compression-type zstd` (both with `--compression-level 6`). `gpbackup` names per-segment data files by codec — gzip produces `gpbackup_<contentid>_<TS>_<oid>.gz`, zstd produces `gpbackup_<contentid>_<TS>_<oid>.zst` (the `.gz` vs `.zst` extension).

- **zstd CLI prerequisite**: zstd-compressed backups **require** the `zstd` CLI in the cluster image — `gpbackup` pipes each segment's `COPY` output through `zstd --compress` (`COPY … TO PROGRAM 'zstd --compress -N -c | gpbackup_s3_plugin …'`), so without it the pipe breaks (*"could not write to COPY program: Broken pipe"*). `Dockerfile.cloudberry-official` installs the `zstd` package (gzip is already in the base image), so `cloudberry-official:2.1.0` carries both codecs.

- **Operational notes**: both backups are scoped to `--include-schema public` (the substantial `public.users` + `public.orders` data) for a meaningful comparison; the `gpbackup_s3_plugin` runs with the CR's multipart settings (`chunksize 10MB`, `concurrency 4`) — not the unstable `500MB × 6` default under emulation; each backup is restored to its own redirect DB and row counts are verified.

- **Verified outcome**: both backups complete cleanly (2/2 tables); data-file totals differ — gzip = **4,204,206 bytes** (~4.01 MiB), zstd = **3,759,562 bytes** (~3.59 MiB), **zstd smaller by 444,644 bytes (~10.6%)**. Both restore into `mydb_gzip_restored` / `mydb_zstd_restored` with row counts matching the baseline (`users=9533`, `orders=476625`).

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario75-compression-matrix.yaml`
- **Functional tests**: `test/functional/scenario75_compression_matrix_test.go`
- **E2E tests**: `test/e2e/scenario75_compression_matrix_e2e_test.go`
- **API tests**: `internal/api/scenario75_compression_test.go`
- **Live data cycle**: `test/e2e/scripts/scenario75-compression-matrix.sh`

```bash
# Run Scenario 75 compression-matrix functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario75

# Run Scenario 75 compression-matrix E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario75

# Run Scenario 75 API gzip/zstd backup arg tests
go test ./internal/api/... -v -run TestHandleCreateBackup_Scenario75
```

#### Scenario 76 — Scheduled Backup via CronJob + Status Population

Exercises the **scheduled** backup path. Setting `spec.backup.schedule` causes the operator to reconcile a **CronJob** that fires on schedule and spawns a backup **Job**; after the Job succeeds the operator **populates the backup status** on the `CloudberryCluster`. Unlike the on-demand scenarios (73–75), which create a Kubernetes **Job DIRECTLY**, Scenario 76 verifies the `CronJob → Job` mechanism.

- **CronJob spec**: the operator creates `{cluster}-backup-schedule` (here `scenario76-backup-schedule`) with `ownerReferences` → the `CloudberryCluster`, `concurrencyPolicy: Forbid`, `successfulJobsHistoryLimit: 3`, `failedJobsHistoryLimit: 3`, and a `jobTemplate` whose pod `restartPolicy` is `Never`. When the CronJob fires, Kubernetes spawns a Job `{cluster}-backup-schedule-<hash>`.

- **Near-future schedule for testing**: the sample CR ships the production schedule `0 2 * * *`; the live test patches it to `*/2 * * * *` via `kubectl patch --type=merge` so the CronJob fires within ~2 minutes.

- **Status population**: after the backup Job succeeds, the operator populates `status.backup` — `lastBackupTime`, `lastBackupTimestamp` (14-digit `YYYYMMDDHHMMSS`), `lastBackupStatus: Success`, `lastBackupType: full`, `lastBackupJobName` (matches the Job), `cronJobName` (`{cluster}-backup-schedule`), and `backupHistory[]` entries each with `timestamp`, `type`, `status`, `size`, and `duration`.

- **14-digit timestamp guarantee**: `lastBackupTimestamp` is always a valid 14-digit `YYYYMMDDHHMMSS`. On-demand Jobs (`{cluster}-backup-<TS>`) keep the embedded timestamp; for CronJob-spawned Jobs (`{cluster}-backup-schedule-<hash>`), whose names don't embed a parseable timestamp, the operator derives it from the Job's `CompletionTime` (UTC).

- **Steady-state status refresh**: backup status (`lastBackup*`, `backupHistory`) is refreshed on the operator's periodic reconcile **even when the cluster spec generation is unchanged**. The CronJob's Job completes asynchronously (no spec change), and the next periodic reconcile discovers it and updates the status — this is the key behavior that makes scheduled-backup status population work.

- **`backupHistory` `size`**: each history entry now includes `size`, derived best-effort from the backup Job's `avsoft.io/backup-size-bytes` annotation (empty when unavailable).

- **Execution model**: the CronJob firing and spawning a Job verifies the schedule mechanism plus the CronJob spec (`Forbid`, `3/3` history, `ownerReferences`, pod `restartPolicy: Never`). A standalone CronJob Job pod is not a segment host in `gp_segment_configuration`, so the real `gpbackup` data cycle runs via the proven coordinator-exec path; status population is verified from the resulting successful backup.

- **Verified outcome**: the scheduled backup completes and the operator populates the status — `lastBackupTimestamp=20260607224409` (14-digit), `lastBackupStatus=Success`, `lastBackupType=full`, and `backupHistory[0]={timestamp, type:full, status:Success, size:4204206, duration:4s}`; `cronJobName=scenario76-backup-schedule` and `lastBackupJobName` matches the Job.

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario76-scheduled-backup.yaml` (schedule `0 2 * * *`, test override `*/2 * * * *`)
- **Functional tests**: `test/functional/scenario76_scheduled_backup_test.go`
- **E2E tests**: `test/e2e/scenario76_scheduled_backup_e2e_test.go`
- **Live data cycle**: `test/e2e/scripts/scenario76-scheduled-backup.sh`

```bash
# Run Scenario 76 scheduled-backup functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario76

# Run Scenario 76 scheduled-backup E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario76
```

#### Scenario 77 — Pre-Backup Health Checks

Every backup Job carries a `pre-backup-check` **init container** that validates cluster + destination health **before** the backup proceeds. Init-container semantics make it blocking: a non-zero exit means the `gpbackup` container never starts and the Job fails. On a backup-Job failure the operator records `status.backup.lastBackupStatus=Failed` and emits a de-duplicated **Warning** Event (reason `BackupFailed`); healing the fault lets a fresh backup reach `Success`.

- **Where the checks live** — `internal/builder/backup_builder.go`:
  - `addPreBackupCheckInitContainer` prepends the `pre-backup-check` init container (shares the backup image, env, and volume mounts with the `gpbackup` container) to the backup Job/CronJob pod spec.
  - `preBackupCheckScript` runs under `set -euo pipefail` and performs **77a** segments-up (`SELECT count(*) FROM gp_segment_configuration WHERE status='d'` → `exit 1` if `> 0`) and **77b** long-running txn (`pg_stat_activity` where `state <> 'idle'` and `now() - xact_start > interval`, threshold `longRunningTxnThresholdSeconds = 3600` → `exit 1` if `> 0`).
  - `preBackupDestinationCheck` appends the destination check: **77d** local (`df -Pk <path>` free KB `< minBackupDiskFreeKB = 1048576` KiB / 1 GiB → `exit 1`); **77c** S3 → `s3ReachabilityCheckScript`.
  - `s3ReachabilityCheckScript` builds a **fail-closed** SigV4-signed `curl -I` HEAD against `${S3_ENDPOINT}/${S3_BUCKET}` (path-style), region `${S3_REGION:-us-east-1}`, signing with the injected `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` and the `openssl`-HMAC chain mirroring `test/docker-compose/scripts/setup-minio.sh`; non-2xx/3xx (or a `--max-time 15` timeout → `000`) → `exit 1`. This replaces the prior best-effort `aws s3 ls` that never failed.

- **Where the event lives** — `internal/controller/admin_controller.go`:
  - `applyBackupJobToStatus` captures the previous `lastBackupStatus`/`lastBackupJobName` **before** overwriting, then calls `emitBackupFailureEvent`.
  - `emitBackupFailureEvent` emits `EventTypeWarning` / `EventReasonBackupFailed` (`api/v1alpha1.EventReasonBackupFailed = "BackupFailed"`) only on a real **transition into Failed** for a given Job name (de-dup), and only for **backup**-operation Jobs (restore failures are excluded).

- **Sample CRs**: `deploy/helm/cloudberry-operator/config/samples/scenario77-s3-prebackup.yaml` (S3 dest — 77a/77b/77c) and `scenario77-local-prebackup.yaml` (local dest + small `scenario77-backup-pvc` — 77d)
- **Functional tests**: `test/functional/scenario77_prebackup_checks_test.go` (`TestFunctional_Scenario77`)
- **Controller event/status tests**: `internal/controller/backup_event_scenario77_test.go` (`TestEmitBackupFailureEvent_Scenario77`, `TestApplyBackupJobToStatus_*_Scenario77`)
- **E2E tests**: `test/e2e/scenario77_prebackup_checks_e2e_test.go` (`TestE2E_Scenario77`)
- **Live script**: `test/e2e/scripts/scenario77-prebackup-checks.sh`

```bash
# Run Scenario 77 pre-backup-check functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario77

# Run Scenario 77 controller event/status tests
go test ./internal/controller/... -v -run Scenario77

# Run Scenario 77 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario77

# Run the live fault -> block -> heal -> success cycle against deployed clusters
bash test/e2e/scripts/scenario77-prebackup-checks.sh \
  --cluster scenario77-s3 --local-cluster scenario77-local
```

#### Scenario 78 — Incremental Backup Lifecycle

Exercises the incremental backup lifecycle: forced `--incremental --leaf-partition-data` flag wiring (78a), auto-base discovery vs. a pinned `--from-timestamp` base (78b/78c), deriving `status.lastBackupType`/`backupHistory[].type` from the Job's `avsoft.io/backup-type` label, and incremental-restore completeness with a `RestoreFailed` Warning on the refuse-on-missing-intermediate path (78d).

- **Where the builder logic lives** — `internal/builder/backup_builder.go`:
  - `appendIncrementalArgs(args, opts, jobOpts)` — when an incremental is effective (`opts.Incremental` **or** `jobOpts.Type=="incremental"`), it **always** appends `--incremental --leaf-partition-data` (exactly once each, regardless of `opts.LeafPartitionData` — gpbackup requires leaf-partition-data for incrementals), and appends `--from-timestamp <ts>` when `jobOpts.FromTimestamp` is set (the 78c pin). Both `BuildBackupJob` and `BuildBackupCronJob` route through `buildGpbackupArgs` → `appendIncrementalArgs`, so on-demand Jobs and CronJobs render identical incremental args.
  - `effectiveBackupType(cluster, opts)` — resolves `incremental` vs `full` from the spec (`Gpbackup.Incremental`) plus per-request overrides (`opts.Type=="incremental"` or `opts.Gpbackup.Incremental`); `opts` may be nil (the CronJob `jobTemplate` path consults the spec only).
  - **`avsoft.io/backup-type` labeling** — `BuildBackupJob` sets `labels[util.LabelBackupType] = effectiveBackupType(cluster, opts)`; `BuildBackupCronJob` sets it from `effectiveBackupType(cluster, nil)` and stamps it on the CronJob, its `jobTemplate`, and the pod template. The constant is `util.LabelBackupType = "avsoft.io/backup-type"` (`internal/util/constants.go`), valued `util.BackupTypeFull`/`util.BackupTypeIncremental`.

- **Where the controller logic lives** — `internal/controller/admin_controller.go`:
  - `backupTypeFromJob(job, cluster)` — reads the Job's `avsoft.io/backup-type` label; falls back to `backupTypeFromLabels(cluster)` (spec-derived) only when the label is absent. `applyBackupJobToStatus` uses it for **both** `LastBackupType` **and** the appended `BackupHistoryEntry.Type`, so per-Job incrementals (even against a `full` spec) and CronJob-spawned Jobs report the right type.
  - `emitRestoreFailureEvent(cluster, job, status, prevStatus, prevJobName)` — for a **restore**-operation Job that transitions into `Failed`, emits a de-duplicated `EventTypeWarning`/`EventReasonRestoreFailed` (`api/v1alpha1.EventReasonRestoreFailed = "RestoreFailed"`). It is called from `applyBackupJobToStatus` for restore Jobs (after `RecordRestore(…, failed)`), mirrors `emitBackupFailureEvent`'s transition-only de-dup, and is intentionally **separate** from `BackupFailed` so Scenario 77's backup-only semantics stay intact. gprestore's native incomplete-set refusal needs no operator change.

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario78-incremental-backup.yaml` (single S3 dest, `incremental: true`, same `folder: /backups` for the whole chain, `retention.incrementalCount: 10`, AO-table load)
- **Functional tests**: `test/functional/scenario78_incremental_backup_test.go` (`TestFunctional_Scenario78`)
- **Integration tests**: `test/integration/scenario78_incremental_backup_integration_test.go`
- **Builder/controller unit tests**: `internal/builder/backup_builder_scenario78_test.go`, `internal/controller/backup_event_scenario78_test.go`
- **E2E tests**: `test/e2e/scenario78_incremental_backup_e2e_test.go` (`TestE2E_Scenario78`)
- **Live script**: `test/e2e/scripts/scenario78-incremental-backup.sh`

```bash
# Run Scenario 78 incremental-backup functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario78

# Run Scenario 78 integration tests
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario78

# Run Scenario 78 builder + controller unit tests (args, labels, status, RestoreFailed)
go test ./internal/builder/... -v -run Scenario78
go test ./internal/controller/... -v -run Scenario78

# Run Scenario 78 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario78

# Run the live full -> incremental (auto-base) -> incremental (pinned) -> restore cycle
bash test/e2e/scripts/scenario78-incremental-backup.sh --cluster scenario78-s3
```

#### Scenario 79 — Retention Cleanup, All Policies

Exercises the gpbackman-driven retention cleanup lifecycle: after each successful backup the operator creates a single idempotent cleanup Job that enforces **all three** retention policies (`fullCount`, `incrementalCount`, `maxAge`) via the **real** `gpbackman` CLI, then feeds the cleanup Job's deletion count into `cloudberry_backup_retention_deleted_total`.

> **Real gpbackman CLI.** `gpbackman` (`woblerr/gpbackman`) has **no** `delete` subcommand and **no** count flags (`--keep-full`/`--older-than`). The cleanup script uses `backup-info` (list), `backup-delete --timestamp <ts> --cascade` (delete one), and `backup-clean --older-than-days <N> --cascade` (time-based). Count-based retention (`fullCount`/`incrementalCount`) is implemented by enumerating with `backup-info` and deleting the oldest excess with `backup-delete`.

- **Where the builder logic lives** — `internal/builder/backup_builder.go`:
  - `BuildRetentionCleanupJob(cluster, timestamp)` — builds the cleanup Job (label `avsoft.io/backup-operation=cleanup` via `util.BackupOperationCleanup`, owner-ref'd to the cluster, name `util.RetentionCleanupJobName(cluster, ts)` = `<cluster>-cleanup-<ts>`). Its container runs `buildGpbackmanRetentionScript(cluster)` as `args[0]` with `terminationMessagePolicy: FallbackToLogsOnError`.
  - `buildGpbackmanRetentionScript(cluster)` — renders the self-contained POSIX-sh / bash-3.2-safe script (no `declare -A`; every interpolated value `shellQuote`d — no injection surface). It renders the S3 plugin config, sets `HISTORY_DB` to the coordinator `gpbackup_history.db`, emits one `retentionCountBlock("full", FullCount)` / `retentionCountBlock("incremental", IncrementalCount)` per set count policy (each a re-enumerating delete loop using the `_gpbackman_timestamps` / `_gpbackman_delete` helpers), plus `retentionMaxAgeBlock(days)` for `maxAge`. It maintains a `DELETED` counter, prints `RETENTION_DELETED=<n>` to stdout, and writes `<n>` to `/dev/termination-log`. The marker prefix is the `retentionDeletedMarker` constant.
  - `parseMaxAgeDays(maxAge)` — converts the `maxAge` expression to whole days for `backup-clean --older-than-days` (`"30d"`→30, `"4w"`→28 via `parseDaysSuffix`, Go duration `"720h"`→30 / `"25h"`→1, bare `"30"`→30; positive sub-day → 1; empty/unparseable → `(0, false)` so the time-based step is skipped).

- **Where the controller logic lives** — `internal/controller/admin_controller.go`:
  - `ensureRetentionCleanup(ctx, cluster)` — called from `reconcileBackup` after the status refresh. No-op when backup is disabled or no retention policy is active (`retentionPolicyActive`). Finds the newest **Succeeded** backup-operation Job (`latestSucceededBackupJob`), derives its 14-digit timestamp, and calls `createRetentionCleanupJob` (Get-before-Create on `<cluster>-cleanup-<ts>` → idempotent, one cleanup per successful backup), then `reconcileRetentionCleanupAnnotations`.
  - `reconcileRetentionCleanupAnnotations(ctx, cluster, jobs)` — for each **Succeeded** cleanup Job missing the annotation, reads the deletion count from the terminated pod (`readRetentionDeletedCount` parses the `RETENTION_DELETED=<n>` marker) and patches `avsoft.io/backup-retention-deleted=<n>` once (`patchRetentionDeletedAnnotation`, `util.AnnotationBackupRetentionDeleted`). The existing `recordBackupJobMetrics` loop then calls `metrics.RecordBackupRetentionDeleted` → `cloudberry_backup_retention_deleted_total`. Non-fatal: parse/permission issues are logged and retried on a later reconcile.

- **gpbackman in the image** — `Dockerfile.cloudberry-backup` builds `gpbackman` from `woblerr/gpbackman` (pinned `GPBACKMAN_VERSION`, default `v0.8.1`; CGO build because it links `mattn/go-sqlite3`) into `/usr/local/bin/gpbackman`, `COPY`'d into the runtime stage and smoke-tested with `gpbackman --version`. The cleanup Job uses `cloudberry-backup:2.1.0`, so its `gpbackman` invocations resolve at runtime.

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario79-retention.yaml` (single S3 dest, `retention.{fullCount:3, incrementalCount:10, maxAge:"30d"}`, same `folder: /backups`, `gpbackup.incremental: true`, no schedule)
- **Functional tests**: `test/functional/scenario79_retention_test.go` (`TestFunctional_Scenario79`)
- **Integration tests**: `test/integration/scenario79_retention_integration_test.go` (`TestIntegration_Scenario79`)
- **E2E tests**: `test/e2e/scenario79_retention_e2e_test.go` (`TestE2E_Scenario79`)
- **Live script**: `test/e2e/scripts/scenario79-retention.sh`

```bash
# Run Scenario 79 retention functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario79

# Run Scenario 79 integration tests
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario79

# Run Scenario 79 builder + controller unit tests (script args, idempotent cleanup, annotation→metric)
go test ./internal/builder/... -v -run RetentionCleanup
go test ./internal/controller/... -v -run RetentionCleanup

# Run Scenario 79 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario79

# Run the live 79a-d retention cleanup cycle against the deployed cluster
bash test/e2e/scripts/scenario79-retention.sh --cluster scenario79-s3
```

#### Scenario 80 — Post-Restore Validation

Exercises the post-restore validation lifecycle: after each Succeeded restore the operator creates one idempotent validation Job whose script runs four checks (optional `ANALYZE` → row-count compare vs gpbackup history → invalid-index scan → health-check), and the operator records the validation Job's terminal status as a metric + de-duplicated Warning Event **without** failing the restore.

- **Where the builder logic lives** — `internal/builder/backup_builder.go`:
  - `ValidationJobOptions{Timestamp, Database, HealthCheckQuery, ExpectedRowCounts, RunAnalyze}` — per-request inputs. `ExpectedRowCounts` (`map[string]int64`, key `schema.table`) is the EXPECTED per-table counts from gpbackup history; `RunAnalyze` toggles the `ANALYZE` step; `HealthCheckQuery` defaults to `SELECT 1`.
  - `BuildPostRestoreValidationJob(cluster, opts)` — builds the validation Job (label `avsoft.io/backup-operation=validate` via `util.BackupOperationValidate`, container `post-restore-validate`, name `util.PostRestoreValidationJobName(cluster, ts)` = `<cluster>-validate-<ts>`, owner-ref'd to the cluster). It sets `PGDATABASE` from `opts.Database` and runs `postRestoreValidationScript(opts)` as `args[0]`.
  - `postRestoreValidationScript(opts)` — renders the bash script (`set -euo pipefail`): `writeAnalyzeStep` (database-wide `ANALYZE` + `ANALYZE_OK` only when `RunAnalyze`), `writeRowCountStep` (per-table compare loop emitting `ROW_COUNT_MATCH`/`ROW_COUNT_MISMATCH` and `exit 1` on any mismatch when `ExpectedRowCounts` is non-empty; else a best-effort `ROW_COUNT_PROBE_SKIPPED` probe that never fails), `writeInvalidIndexStep` (must-pass `relkind='i' AND NOT indisvalid` → `exit 1`), then the health-check query and the `validation passed` marker. Every interpolated table name / expected count is `shellQuote`d (no injection surface) and the expected map is iterated in sorted order so the rendered script is deterministic.

- **Where the controller logic lives** — `internal/controller/admin_controller.go`:
  - `ensurePostRestoreValidation(ctx, cluster)` — called from `reconcileBackup`; no-op when `validationEnabled(cluster)` is false (`backup.validation.enabled: false`). For each **Succeeded** restore-operation Job it calls `createValidationJob` (Get-before-Create on `<cluster>-validate-<ts>` → idempotent, one validation Job per Succeeded restore).
  - `createValidationJob(ctx, cluster, ts, restoreJob)` — populates `ValidationJobOptions` from the cluster + restore Job: `expectedRowCountsFromJob` reads the restore Job's `avsoft.io/expected-row-counts` annotation (`util.AnnotationExpectedRowCounts`, JSON `table → count`; nil/unparsable → best-effort probe), `validationHealthCheckQuery` reads `backup.validation.healthCheckQuery`, and `validationRunAnalyze` honors `backup.validation.runAnalyze` then falls back to `gprestore.runAnalyze`.
  - `observeValidationJobs(ctx, cluster)` — called from `reconcileBackup` after `ensurePostRestoreValidation`. For each terminal validation-operation Job not yet recorded (gated on the `avsoft.io/validation-recorded` annotation, `util.AnnotationValidationRecorded`), `recordValidationOutcome` patches the de-dup annotation **first** (the commit point — avoids double-counting), then calls `metrics.RecordRestoreValidation(cluster, namespace, result)` (`result ∈ {success, failed}` via `validationJobResult`) and, on `failed`, `emitValidationFailureEvent`. The restore status is never altered by the validation outcome.
  - `emitValidationFailureEvent(cluster, job)` — emits the `EventTypeWarning`/`EventReasonValidationFailed` (`api/v1alpha1.EventReasonValidationFailed = "ValidationFailed"`) Event once per Job (`recordValidationOutcome` invokes it only after the de-dup annotation is committed, mirroring `emitRestoreFailureEvent`'s transition de-dup).

- **Where the metric lives** — `internal/metrics/metrics.go`: `RecordRestoreValidation(cluster, namespace, result)` on the `Recorder` interface increments `cloudberry_restore_validation_total{cluster,namespace,result}` (`PrometheusRecorder` impl; `NoopRecorder` no-op), registered in `MustRegister` alongside `restore_total`.

- **CRD config** — `api/v1alpha1/types.go`: `BackupSpec.Validation *BackupValidation` with `BackupValidation{Enabled *bool, HealthCheckQuery string, RunAnalyze bool}` (deepcopy regenerated in `zz_generated.deepcopy.go`). Defaults: validation enabled, `SELECT 1`, run-analyze inherited from `gprestore.runAnalyze`.

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario80-post-restore-validation.yaml`
- **Functional tests**: `test/functional/scenario80_post_restore_validation_test.go` (`TestFunctional_Scenario80`)
- **Integration tests**: `test/integration/scenario80_post_restore_validation_integration_test.go` (`TestIntegration_Scenario80`)
- **E2E tests**: `test/e2e/scenario80_post_restore_validation_e2e_test.go` (`TestE2E_Scenario80`)
- **Builder + controller unit tests**: `internal/builder/backup_builder_scenario80_test.go`, `internal/controller/validation_scenario80_test.go`, `internal/metrics/metrics_test.go` (`TestRecordRestoreValidation_Scenario80`)
- **Live script**: `test/e2e/scripts/scenario80-post-restore-validation.sh`

```bash
# Run Scenario 80 post-restore-validation functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario80

# Run Scenario 80 integration tests
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario80

# Run Scenario 80 builder + controller + metrics unit tests (compare script, ROW_COUNT markers, ANALYZE_OK, metric+ValidationFailed event)
go test ./internal/builder/... -v -run Scenario80
go test ./internal/controller/... -v -run Scenario80
go test ./internal/metrics/... -v -run Scenario80

# Run Scenario 80 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario80

# Run the live 80a-e validation cycle (success + deliberate-mismatch paths) against the deployed cluster
bash test/e2e/scripts/scenario80-post-restore-validation.sh --cluster scenario80-s3 --namespace cloudberry-test
```

#### Scenario 81 — Local Destination Backup/Restore

Exercises the **local** (PVC-backed) backup/restore destination end-to-end: with `destination.type: local` the operator wires `gpbackup`/`gprestore` to a `--backup-dir` on a mounted PVC instead of the S3 plugin — no `--plugin-config`, no S3 ConfigMap, no Vault/Secret S3 credentials. The three destination-blind builder functions were made destination-aware via a small helper; nil/empty/unknown destinations default to the S3 leading args so every existing S3 caller and test stays byte-identical.

- **Where the builder logic lives** — `internal/builder/backup_builder.go`:
  - `isLocalDestination(cluster)` — `cluster.Spec.Backup != nil && Destination.Type == destinationTypeLocal`. The single predicate gating every local branch.
  - `localBackupDir(cluster)` — resolves the on-pod backup dir: `Destination.Local.Path` when set, else `localBackupMountPath` (`/backups`). The single source of truth shared by the args, the volume mount, and the retention script.
  - `backupDestinationArgs(cluster)` — returns the leading args: local → `["--backup-dir", localBackupDir(cluster)]`; s3 (and nil/unknown, to preserve current behavior) → `["--plugin-config", "/tmp/s3-config.yaml"]`.
  - `buildGpbackupArgs(cluster, opts, jobOpts)` / `buildGprestoreArgs(cluster, opts, jobOpts)` — both take the `cluster` and seed `args` from `backupDestinationArgs(cluster)` (instead of a hardcoded `--plugin-config` slice), so local renders `--backup-dir <path>` and **never** `--plugin-config` / `/tmp/s3-config.yaml`. The 3 call sites (`buildBackupPodSpec` / the restore + CronJob paths) all have `cluster` in scope.
  - `renderToolScript(cluster, tool, args)` — for a local destination **skips** the `envsubst` block that renders `/etc/gpbackup/s3-plugin-config.yaml.tpl` → `/tmp/s3-config.yaml` (gated on `!isLocalDestination(cluster)`). A local Job has no S3 ConfigMap at `/etc/gpbackup`, so reading the missing template under `set -euo pipefail` would otherwise abort the Job before `gpbackup` runs. The `gpEnvPreamble`/`sshSetupPreamble`/plugin-path preambles are kept for both destinations (harmless; SSH is still needed to reach segments).
  - `buildBackupVolumes(cluster)` / `buildBackupVolumeMounts(cluster)` — for `destinationTypeLocal` with a non-empty `Local.PersistentVolumeClaim`, append a `PersistentVolumeClaim` volume named `localBackupVolumeName` (`"backup-data"`, `ClaimName: <Local.PersistentVolumeClaim>`) and mount it at `localBackupDir(cluster)` (default `/backups`). No `s3-plugin-config` ConfigMap volume / no `/etc/gpbackup` mount for local.
  - `buildBackupEnv(cluster)` — emits only `PG*`/database env for non-S3 (the `S3_*`/`AWS_*` append is guarded on `Destination.Type == destinationTypeS3`), so local pods carry no S3 env.
  - `preBackupDestinationCheck(cluster)` — for local runs the `df -Pk <backup-dir>` free-space check (`< minBackupDiskFreeKB = 1048576` KiB / 1 GiB → `exit 1`, Scenario 77d); s3 → the SigV4 HEAD reachability check (77c).
  - `buildGpbackmanRetentionScript(cluster)` — for local sets `DEST_FLAGS=--backup-dir <path>` and **skips** the S3 plugin-config render; s3 renders the config and uses `--plugin-config <rendered>`. So local retention (`gpbackman backup-delete`/`backup-clean`) targets the local dir.

- **Where the controller logic lives** — `internal/controller/admin_controller.go`:
  - `ensureBackupS3ConfigMap(ctx, cluster)` — no-op for local: `BuildBackupS3ConfigMap` returns nil for a non-S3 destination, so **no** `<cluster>-backup-s3-config` ConfigMap is created.
  - `ensureBackupS3VaultCredentials(ctx, cluster)` — no-op for local: `backupS3VaultSpec(cluster)` returns nil when `Destination.S3 == nil`, so **no** `<cluster>-backup-s3-vault-creds` Secret is created and no Vault read is attempted. (Asserted by the Scenario 81 integration test as a regression guard.)

- **MPP / live model note** — `gpbackup --backup-dir <dir>` writes per-segment backup sets on the coordinator **and every segment host**. A single RWO PVC mounted on one Job pod holds **one** set, not the cluster-wide fan-out. The Job-spec assertions (81a/81b) prove the operator wiring (PVC at `local.path`, `--backup-dir`, no plugin); the real `gpbackup`/`gprestore` data cycle (81c/81d) runs via the coordinator-exec path into a segment-visible `--backup-dir` (e.g. `/tmp/scenario81-backups`).

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario81-local-destination.yaml` (declares the `backup-pvc` PVC + a cluster with `destination.type: local`, `local.path: /backups`, `local.persistentVolumeClaim: backup-pvc`, no schedule)
- **Functional tests**: `test/functional/scenario81_local_destination_test.go` (`TestFunctional_Scenario81`)
- **Integration tests**: `test/integration/scenario81_local_destination_integration_test.go` (`TestIntegration_Scenario81`)
- **E2E tests**: `test/e2e/scenario81_local_destination_e2e_test.go` (`TestE2E_Scenario81`)
- **Live script**: `test/e2e/scripts/scenario81-local-destination.sh`

```bash
# Run Scenario 81 local-destination functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario81

# Run Scenario 81 integration tests (no S3 ConfigMap/Vault creds for local; PVC volume + --backup-dir)
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario81

# Run Scenario 81 builder unit tests (--backup-dir args, no /etc/gpbackup render, PVC mount)
go test ./internal/builder/... -v -run Scenario81

# Run Scenario 81 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario81

# Run the live local backup -> restore cycle (Job-spec + coordinator-exec --backup-dir) against the deployed cluster
bash test/e2e/scripts/scenario81-local-destination.sh --cluster scenario81-local
```

#### Scenario 82 — Security and Encryption

Verifies the backup security posture: S3 credentials never on disk/ConfigMap (placeholders +
env-from-Secret), ephemeral `envsubst` rendering of `/tmp/s3-config.yaml`, the dedicated
`cloudberry-backup-sa` with scoped minimal RBAC, the `encryption` on/off plugin option, and
`jobTemplate.imagePullSecrets`. **82a/82b/82d** need no operator code change (already
satisfied — covered by tests + live); **82c** is a Helm RBAC change; **82e** adds a CRD field
wired through the existing helper.

- **Where the builder logic lives** — `internal/builder/backup_builder.go`:
  - `BuildBackupS3ConfigMap(cluster)` — emits the `<cluster>-backup-s3-config` ConfigMap whose
    `s3-plugin-config.yaml.tpl` `options:` values are **all** `${...}` placeholders
    (`${S3_REGION}`/`${S3_ENDPOINT}`/`${AWS_ACCESS_KEY_ID}`/`${AWS_SECRET_ACCESS_KEY}`/
    `${S3_BUCKET}`/`${S3_FOLDER}`/`${S3_ENCRYPTION}` + multipart). **No** literal credential
    material (82a). Returns nil for non-S3.
  - `buildBackupEnv(cluster)` / `buildS3CredentialEnv(...)` — inject `AWS_ACCESS_KEY_ID` /
    `AWS_SECRET_ACCESS_KEY` as `valueFrom.secretKeyRef` (never literal `value:`) (82a).
  - `renderToolScript(cluster, tool, args)` — for an S3 destination emits the runtime
    `envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml` line (with a
    POSIX `eval`+heredoc fallback), so the resolved config lands only in the ephemeral pod
    filesystem (82b); skipped for local.
  - `buildS3Env(cluster, s3)` — sets `S3_ENCRYPTION` from `s3.Encryption` (default `on`); the
    ConfigMap template line `encryption: ${S3_ENCRYPTION}` flips with it (82d). The enum
    `on|off` is enforced on `S3Destination.Encryption` (`api/v1alpha1/types.go`,
    `+kubebuilder:validation:Enum=on;off`) and in the generated CRD.
  - `applyJobTemplatePod(cluster, podSpec)` — after the nil-guard, calls
    `addImagePullSecrets(podSpec, tmpl.ImagePullSecrets)` (the reusable helper in
    `internal/builder/builder.go`, also used by the StatefulSets) so the backup Job pod —
    and the restore / post-restore-validation / cleanup / CronJob pods — carry
    `spec.backup.jobTemplate.imagePullSecrets` (82e). The field
    `ImagePullSecrets []ImagePullSecret` lives on `BackupJobTemplate`
    (`api/v1alpha1/types.go`); `zz_generated.deepcopy.go` and the CRD were regenerated
    (`make manifests`/`make generate`) to expose `spec.backup.jobTemplate.imagePullSecrets`.

- **Where the RBAC scoping lives** — `deploy/helm/cloudberry-operator/templates/backup-rbac.yaml`
  + `deploy/helm/cloudberry-operator/values.yaml`:
  - The `cloudberry-backup-role` `secrets: [get]` rule renders `resourceNames` from
    `.Values.backup.rbac.secretNames` **only when**
    `and .Values.backup.rbac.scopeSecrets .Values.backup.rbac.secretNames` is true; otherwise
    it stays namespace-wide (backward compatible). `configmaps: [get]` and
    `events: [create,patch]` are unchanged (82c).
  - `values.yaml` adds `backup.rbac.scopeSecrets` (default `false`) and
    `backup.rbac.secretNames` (default `[backup-s3-credentials]`), documenting the
    per-cluster Secrets the Jobs consume (`<cluster>-admin-password`, `<cluster>-ssh-keys`,
    `<cluster>-backup-s3-vault-creds`, + the user S3 credential Secret). The SA + Role are
    **namespace-fixed**, the consumed Secret names **per-cluster** — union them for multiple
    clusters in one namespace. Scoping the SA's API `get` does **not** break the Job's
    `secretKeyRef`/volume injection (kubelet-driven).
  - **CRD re-apply**: the regenerated CRD must be `kubectl apply`-ed after a Helm install —
    Helm does not upgrade `crds/`.

- **Builder unit tests**: `internal/builder/backup_builder_scenario82_test.go`
  (`TestBuildBackupS3ConfigMapPlaceholdersOnlyScenario82`,
  `TestBuildBackupS3ConfigMapEncryptionLineScenario82`, `applyJobTemplatePod` imagePullSecrets).
- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario82-security-encryption.yaml`
  (S3 cluster `scenario82-s3`, `encryption: on`, `jobTemplate.imagePullSecrets: [{name: regcred}]`,
  plus the `s3-credentials` and `unrelated-secret` Secrets and the scoped-RBAC values note).
- **Functional tests**: `test/functional/scenario82_security_encryption_test.go` (`TestFunctional_Scenario82`)
- **Integration tests**: `test/integration/scenario82_security_encryption_integration_test.go` (`TestIntegration_Scenario82`)
- **E2E tests**: `test/e2e/scenario82_security_encryption_e2e_test.go` (`TestE2E_Scenario82`)
- **Live script**: `test/e2e/scripts/scenario82-security-encryption.sh`

```bash
# Run Scenario 82 security/encryption functional tests (ConfigMap placeholders, secretKeyRef env, encryption flip, imagePullSecrets)
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario82

# Run Scenario 82 integration tests (envtest: ConfigMap placeholders, pod env secretKeyRef, imagePullSecrets, envsubst line)
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario82

# Run Scenario 82 builder unit tests (placeholder-only ConfigMap, encryption line, applyJobTemplatePod imagePullSecrets)
go test ./internal/builder/... -v -run Scenario82

# Run Scenario 82 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario82

# Run the live security/encryption checks (82a-e) against the deployed cluster
bash test/e2e/scripts/scenario82-security-encryption.sh --cluster scenario82-s3 \
  [--namespace cloudberry-test] [--checks 82a,82b,82c,82d,82e]
```

#### Scenario 83 — Backup Failure Handling

Exercises the backup **failure** path: a backup that cannot succeed retries up to
`backoffLimit` (default `2`) and ends `Failed`, a backup that outlives
`activeDeadlineSeconds` (default `7200`) is **killed** by Kubernetes at the deadline, and
both record `status.backup.lastBackupStatus=Failed`, `cloudberry_backup_last_status=1`, and
a de-duplicated `Warning`/`BackupFailed` Event. The only production change was making the
status mappings classify a Job with a terminal **Failed condition** (e.g.
`DeadlineExceeded`/`BackoffLimitExceeded`) as failed even when the failed-pod count is `0`;
the metric/status/event wiring downstream was already correct.

- **Where the builder logic lives** — `internal/builder/backup_builder.go`:
  - `buildJobSpec(cluster, labels, podSpec)` — seeds `backoff = defaultBackoffLimit`
    (`int32 2`) and `deadline = defaultActiveDeadlineSeconds` (`int64 7200`); when
    `jobTemplate(cluster) != nil` overrides each from `tmpl.BackoffLimit` /
    `tmpl.ActiveDeadlineSeconds` (a nil field keeps the default), and sets
    `JobSpec.BackoffLimit=&backoff` / `JobSpec.ActiveDeadlineSeconds=&deadline`. Every
    backup/restore/cleanup/validation Job **and** the backup CronJob's `jobTemplate` route
    through `buildJobSpec`, so the `spec.backup.jobTemplate.{backoffLimit,activeDeadlineSeconds}`
    override propagates to every Job spec uniformly. `defaultBackoffLimit` /
    `defaultActiveDeadlineSeconds` are unchanged (Scenario 83 must **not** change them).
  - The `*int32 BackoffLimit` / `*int64 ActiveDeadlineSeconds` fields live on
    `BackupJobTemplate` (`api/v1alpha1/types.go`).

- **Where the controller logic lives** — `internal/controller/admin_controller.go`:
  - `jobHasFailedCondition(job *batchv1.Job) bool` — iterates `job.Status.Conditions` and
    returns true when any condition has `Type == batchv1.JobFailed && Status ==
    corev1.ConditionTrue` (e.g. `reason=DeadlineExceeded` or `BackoffLimitExceeded`).
    Authoritative even when `Status.Failed == 0`.
  - `backupJobStatus(job)` (→ `"Failed"`) and `backupJobStatusCode(job)` (→ `3`) — after
    the `Succeeded > 0` precedence branch, the Failed arm is
    `case job.Status.Failed > 0 || jobHasFailedCondition(job):`. `validationJobResult(job)`
    uses the same combined check. Succeeded precedence is preserved; in-progress/pending
    are unchanged when no failure signal is present.
  - `recordLatestBackupMetrics(...)` — `Failed → SetBackupLastStatus(name, namespace,
    backupLastStatusFailed)` (`=1` → `cloudberry_backup_last_status=1`), `Success → 0`,
    otherwise `2`. Unchanged; the GAP-1 status arm now reaches it for deadline/backoff
    terminal Jobs. `applyBackupJobToStatus` still sets `lastBackupStatus` from
    `backupJobStatus` and calls `emitBackupFailureEvent` (de-duplicated `BackupFailed`
    Warning on a real transition into `Failed`).

- **Metric** — `internal/metrics/metrics.go`: `cloudberry_backup_last_status{cluster,namespace}`
  gauge (`0=success, 1=failed, 2=in-progress`), `SetBackupLastStatus`. No change; the
  existing Grafana panel renders `1` on failure / `0` on success.

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario83-backup-failure.yaml`
  (S3 cluster `scenario83-s3`, explicit `jobTemplate.backoffLimit: 2`, production-safe
  default `activeDeadlineSeconds`; the deadline sub-test uses a per-run Job so the
  cluster's deadline stays safe)
- **Functional tests**: `test/functional/scenario83_backup_failure_test.go` (`TestFunctional_Scenario83`)
- **Integration tests**: `test/integration/scenario83_backup_failure_integration_test.go` (`TestIntegration_Scenario83`)
- **E2E tests**: `test/e2e/scenario83_backup_failure_e2e_test.go` (`TestE2E_Scenario83`)
- **Live script**: `test/e2e/scripts/scenario83-backup-failure.sh`

```bash
# Run Scenario 83 backup-failure functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario83

# Run Scenario 83 integration tests (jobTemplate backoffLimit/activeDeadlineSeconds override reaches the jobspec; status-transition metric)
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario83

# Run Scenario 83 unit tests (jobHasFailedCondition truth-table; backupJobStatus/Code Failed for DeadlineExceeded/BackoffLimitExceeded; builder defaults/override)
go test ./internal/builder/... -v -run Scenario83
go test ./internal/controller/... -v -run Scenario83

# Run Scenario 83 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario83

# Run the live force-failure (backoffLimit) + deadline-kill checks against the deployed cluster
bash test/e2e/scripts/scenario83-backup-failure.sh --cluster scenario83-s3
```

#### Scenario 84 — Prometheus Metrics / `gpbackup_exporter`

Verifies the **nine** backup/restore metrics end-to-end. This is a **verification +
documentation + dashboard** scenario — all nine metrics were already defined and wired
incrementally across Scenarios 76–83, so **no operator code change** is required. The
`gpbackup_exporter` is implemented as the **operator metric endpoint**: the operator
derives the metrics from the Kubernetes Jobs it observes (backup-/restore-/cleanup-operation
Jobs) plus their `avsoft.io/*` annotations and history outcomes, and exposes them on its own
Prometheus `/metrics` endpoint — there is no separate exporter sidecar binary. vmagent
scrapes the operator `/metrics` (via the `prometheus.io/scrape` annotations on the operator
Deployment/Service) into VictoriaMetrics.

- **Where the metric definitions live** — `internal/metrics/metrics.go`:
  - `initBackupMetrics()` defines all nine under namespace `cloudberry` and registers them.
    The outcome label on `backup_total` / `restore_total` is **`labelResult`** (`result`,
    values `success`|`failed`) — **not** `status`. Recorder methods: `RecordBackup`,
    `ObserveBackupDuration`, `SetBackupSizeBytes`, `SetBackupLastSuccessTimestamp`,
    `SetBackupLastStatus`, `ObserveRestoreDuration`, `RecordBackupRetentionDeleted`,
    `SetBackupJobStatus`, `RecordRestore`. `RecordBackup`/`RecordRestore` lower-case the Job
    status (`Success → "success"`, `Failed → "failed"`).

- **Where the metrics are recorded** — `internal/controller/admin_controller.go`:
  - `refreshBackupStatus` calls `recordBackupJobMetrics(...)` for **every** observed
    backup/restore/cleanup Job, then `applyBackupJobToStatus(...)` on the latest Job.
  - `recordBackupJobMetrics(...)` — `SetBackupJobStatus` for each Job (code from
    `backupJobStatusCode`: `0=pending, 1=running, 2=succeeded, 3=failed`); on a Succeeded
    restore Job → `ObserveRestoreDuration`; on a Succeeded cleanup Job → 
    `RecordBackupRetentionDeleted` (from the `avsoft.io/backup-retention-deleted` annotation).
  - `applyBackupJobToStatus(...)` — restore Jobs → `RecordRestore`; backup Jobs →
    `recordLatestBackupMetrics`.
  - `recordLatestBackupMetrics(...)` — `RecordBackup` always; on Success →
    `SetBackupLastStatus(0)`, `SetBackupLastSuccessTimestamp`, `ObserveBackupDuration`
    (when both `startTime`+`completionTime` set), `SetBackupSizeBytes` (when the
    `avsoft.io/backup-size-bytes` annotation + a 14-digit timestamp are present); on Failed
    → `SetBackupLastStatus(1)`; otherwise (in-progress) → `SetBackupLastStatus(2)`.
    `type` (`full`|`incremental`) comes from `backupTypeFromJob` (the Job's
    `avsoft.io/backup-type` label, spec fallback).

- **`result` (not `status`) — doc alignment (GAP-A).** Scenario 84's wording said
  `cloudberry_backup_total{type,status}`; the implemented outcome label is **`result`**.
  The label is **not** renamed (dashboards/PromQL across 76–83 query `result`); the
  functional `…_ResultLabelContract` test guards that `backup_total`/`restore_total` expose
  `result` and emit **no** `status` label. Query with `{result="success"}` /
  `{result="failed"}`.

- **Dashboard** — `monitoring/grafana/cloudberry-operator.json` already has a panel for each
  of the nine metrics (queries use `result`); publish with `make grafana-publish`.

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario84-metrics.yaml`
  (single S3 cluster `scenario84-s3` exercising full + incremental + restore + cleanup +
  failure so every metric is populated)
- **Functional tests**: `test/functional/scenario84_metrics_test.go` (`TestFunctional_Scenario84`)
- **Integration tests**: `test/integration/scenario84_metrics_integration_test.go` (`TestIntegration_Scenario84`)
- **E2E tests**: `test/e2e/scenario84_metrics_e2e_test.go` (`TestE2E_Scenario84`)
- **Test-case catalog**: `Scenario84MetricCases` in `test/cases/test_cases.go`
- **Live script**: `test/e2e/scripts/scenario84-metrics.sh` (drives the full lifecycle and
  asserts all nine metrics in VictoriaMetrics)

```bash
# Run Scenario 84 metrics functional tests (full-lifecycle recorder sweep, forced-failure last_status, job-status lifecycle, restore metrics, result-label contract)
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario84

# Run Scenario 84 integration tests (envtest: full/incremental backup, restore, cleanup, failed backup record the metrics)
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario84

# Run Scenario 84 metrics unit tests (registry round-trip confirms the `result` label; recorder invocations)
go test ./internal/metrics/... -v -run Backup
go test ./internal/controller/... -v -run Scenario84

# Run Scenario 84 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario84

# Run the live full-lifecycle metric verification against the deployed cluster + VictoriaMetrics
bash test/e2e/scripts/scenario84-metrics.sh --cluster scenario84-s3
```

#### Scenario 85 — All Backup API Endpoints

Verifies the **seven** backup/restore REST API endpoints under
`/api/v1alpha1/clusters/{name}/backups` end-to-end: routing, per-endpoint OIDC/JWT auth +
RBAC, the request schemas, the option → `gpbackup`/`gprestore` flag mapping, and the
negative/mutual-exclusivity responses. It is mostly **verification + comprehensive tests**
plus **one** code fix (`leafPartitionData` on full backups).

- **Where the routes + RBAC live** — `internal/api/server.go` (registration `~297–312`):
  - `GET /backups` → `handleListBackups` (`PermissionBasic`)
  - `POST /backups` → `handleCreateBackup` (`PermissionOperator`)
  - `GET /backups/{timestamp}` → `handleGetBackup` (`PermissionBasic`)
  - `DELETE /backups/{timestamp}` → `handleDeleteBackup` (`PermissionAdmin`)
  - `POST /backups/{timestamp}/restore` → `handleRestoreBackup` (`PermissionAdmin`)
  - `GET /backups/jobs` → `handleListBackupJobs` (`PermissionBasic`)
  - `GET /backups/schedule` → `handleGetBackupSchedule` (`PermissionBasic`)
  - (`PATCH /backups/schedule` → `handleUpdateBackupSchedule`, `PermissionOperator` — outside
    the Scenario 85 seven). A missing cluster returns `404` via `writeClusterNotFound`.

- **Where the DTOs + option mapping live** — `internal/api/backup.go`:
  - `CreateBackupRequest` / `GpbackupOptionsRequest` and `RestoreRequest` /
    `GprestoreOptionsRequest` are the request bodies.
  - `buildBackupJobOptions` → `mergeGpbackupOptions` overlays the request `gpbackupOptions`
    over the cluster's `backup.gpbackup` defaults; `buildRestoreJobOptions` →
    `mergeGprestoreOptions` does the same for restore.
  - `restoreOptionsConflict` rejects `dataOnly`+`metadataOnly` with `400` **before** the Job
    is built. `backupScheduleResponse` / `nextScheduleTime` compute the 85g `nextScheduleTime`.
  - The merged options are rendered into CLI args by `buildGpbackupArgs` /
    `buildGprestoreArgs` in `internal/builder/backup_builder.go`.

- **The 85b fix — `leafPartitionData` on full backups.** `buildGpbackupArgs` calls
  `appendLeafPartitionDataArgs(args, opts, jobOpts)`, which emits `--leaf-partition-data`
  for a **full** backup when `opts.LeafPartitionData` is set, guarded on
  `!isEffectivelyIncremental(...)` so it never duplicates the `--leaf-partition-data` that
  `appendIncrementalArgs` force-pairs with `--incremental`. Previously the option was
  silently dropped on full backups. `isEffectivelyIncremental` is the single source of truth
  shared by both helpers so the flag is emitted **exactly once** in every case.

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario85-api-endpoints.yaml`
  (single S3 cluster `scenario85-s3` with a `backup.schedule` so all seven endpoints have data)
- **Functional tests**: `test/functional/scenario85_api_endpoints_test.go` (`TestFunctional_Scenario85`)
- **Integration tests**: `test/integration/scenario85_api_endpoints_integration_test.go` (`TestIntegration_Scenario85`)
- **E2E tests**: `test/e2e/scenario85_api_endpoints_e2e_test.go` (`TestE2E_Scenario85`)
- **Handler/builder unit tests**: `internal/api/backup_scenario85_test.go`,
  `internal/builder/backup_builder_scenario85_test.go`
- **Test-case catalog**: `test/cases/scenario85_api_endpoints_cases.go`
- **Live script**: `test/e2e/scripts/scenario85-api-endpoints.sh` (obtains an OIDC token,
  port-forwards the operator REST API, and exercises all seven endpoints — asserting the
  created Job args via `kubectl get job -o jsonpath`)

```bash
# Run Scenario 85 API-endpoints functional tests (all 7 endpoints: status + response shape + created Job args)
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario85

# Run Scenario 85 integration tests (envtest: RBAC 403, 404, 400 paths through the real router + auth; Job not CronJob)
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario85

# Run Scenario 85 handler + builder unit tests (option->flag matrices, leafPartitionData-on-full, RBAC, exclusivity)
go test ./internal/api/... -v -run Scenario85
go test ./internal/builder/... -v -run Scenario85

# Run Scenario 85 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario85

# Run the live OIDC-authed exercise of all 7 endpoints against the deployed cluster
bash test/e2e/scripts/scenario85-api-endpoints.sh --cluster scenario85-s3
```

#### Scenario 86 — All Backup CLI Commands

Verifies **all eleven** `cloudberry-ctl backup …` CLI commands (86a–k): each command builds the
right operator REST request, and `backup jobs logs` (86k) **streams** the Job's pod logs. Ten
commands reuse the Scenario 85 endpoints; the only code addition is the 86k streaming path — a
new operator endpoint, a new CLI streaming client method, and the rewired `backup jobs logs`
command.

- **Where the CLI backup commands live** — `cmd/cloudberry-ctl/main.go`:
  - `newBackupCmd` builds the tree: `newBackupCreateCmd` (→ `buildCreateBackupRequest`,
    `POST /backups`), `newBackupListCmd` (`GET /backups`), `newBackupStatusCmd`
    (`GET /backups/{ts}`), `newBackupDeleteCmd` (`DELETE /backups/{ts}`), `newBackupRestoreCmd`
    (→ `buildRestoreRequest`, `POST /backups/{ts}/restore` — incl. `--resize-cluster`),
    `newBackupScheduleCmd` (`GET /backups/schedule`) with `newBackupScheduleSetCmd`
    (`PATCH {schedule}`) and `newBackupScheduleSuspendCmd("suspend"|"resume", …)`
    (`PATCH {suspend}`), and `newBackupJobsCmd` (`GET /backups/jobs`).
  - **86k** — `newBackupJobsLogsCmd` (`--job`/`--follow`/`--tail`) → `runBackupJobsLogs` builds
    the path with `buildBackupJobLogsPath` (adds `?follow=true`/`?tailLines=N`) and calls
    `client.GetStream(ctx, path, cmd.OutOrStdout())`. On a stream error it calls
    `printBackupJobLogsFallback`, which prints the `kubectl logs -n <ns> job/<job>` instruction
    rather than failing silently.

- **The new operator logs endpoint** — `internal/api/server.go`:
  - Registered as `GET /clusters/{name}/backups/jobs/{job}/logs` with `PermissionBasic`
    (after `GET /backups/jobs`; the literal route wins over `/backups/{timestamp}`).
  - `handleBackupJobLogs` validates the job name, requires the typed clientset (`501` when nil),
    resolves the cluster (`404`), finds the Job's most-recent pod via `findJobPod` /
    `mostRecentPodName` (`404 JOB_NOT_FOUND` when none), then `streamPodLogs` opens
    `clientset.CoreV1().Pods(ns).GetLogs(pod, buildPodLogOptions(query))` and copies the stream
    as `text/plain` (flushing for `--follow` via `copyLogStream`).
  - **Clientset wiring** — the controller-runtime `client.Client` cannot stream pod logs, so the
    `Server` now holds a typed `kubernetes.Interface`, injected by `Server.WithClientset(...)`
    (wired from `mgr.GetConfig()` → `kubernetes.NewForConfig` in `cmd/operator/main.go`).

- **The CLI streaming client** — `internal/ctl/client.go`:
  - `OperatorClient.GetStream(ctx, path, out io.Writer)` applies auth, does **not** buffer or
    JSON-parse the body, and `io.Copy`s the `text/plain` stream to `out`; a non-2xx status is
    read (bounded) and returned as an `*APIError` via `streamError`. (The normal `do()` path
    buffers and JSON-parses, so it is unsuitable for streaming.)

- **Sample CR**: `deploy/helm/cloudberry-operator/config/samples/scenario86-cli-commands.yaml`
  (single S3 cluster `scenario86-s3`: backup enabled + schedule + incremental)
- **Functional tests**: `test/functional/scenario86_cli_commands_test.go` (`TestFunctional_Scenario86`)
- **Integration tests**: `test/integration/scenario86_cli_commands_integration_test.go` (`TestIntegration_Scenario86`)
- **E2E tests**: `test/e2e/scenario86_cli_commands_e2e_test.go` (`TestE2E_Scenario86`)
- **CLI unit tests**: `cmd/cloudberry-ctl/backup_scenario86_test.go` (build-request +
  cobra method/path/body assertions, 86k streaming + fallback)
- **Endpoint/handler unit tests**: `internal/api/backup_joblogs_test.go`,
  `internal/api/backup_joblogs_scenario86_test.go`; client unit tests in
  `internal/ctl/client_test.go` (`TestOperatorClient_GetStream_*`)
- **Test-case catalog**: `test/cases/scenario86_cli_commands_cases.go`
- **Live script**: `test/e2e/scripts/scenario86-cli-commands.sh` (builds the CLI, obtains an
  OIDC token, port-forwards the operator API, runs every backup command 86a–k, and asserts the
  created Job args, the CronJob schedule/suspend changes, and the streamed Job logs)

```bash
# Run Scenario 86 CLI-commands functional tests (all 11 commands: request mapping + 86k stream/fallback)
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario86

# Run Scenario 86 integration tests (envtest: real router + auth; logs endpoint stream/404)
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario86

# Run Scenario 86 CLI unit tests (buildCreate/Restore requests, method/path/body, GetStream stream+fallback)
go test ./cmd/cloudberry-ctl/... -v -run 'Backup|Scenario86'

# Run Scenario 86 logs-endpoint + GetStream unit tests
go test ./internal/api/... -v -run 'BackupJobLogs|PodLogOptions|CopyLogStream'
go test ./internal/ctl/... -v -run TestOperatorClient_GetStream

# Run Scenario 86 E2E tests (live portion gated on KUBECONFIG)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario86

# Run the live OIDC-authed exercise of all 11 backup CLI commands against the deployed cluster
bash test/e2e/scripts/scenario86-cli-commands.sh --cluster scenario86-s3
```

#### Scenario 89 — Backup Artifact Round-Trip Against Real MinIO (Integration)

Verifies the **object-store side** of the backup pipeline against the live MinIO from the
Docker Compose test environment — the real S3 dependency the backup Jobs talk to. The
gpbackup/gprestore binaries run inside the database pods (exercised by the live e2e scripts);
this scenario proves the store itself, using the same bucket (`cloudberry-backups`), the same
folder layout (`<folder>/<cluster>/<timestamp>/…`), and the same credentials the Jobs receive
from the `backup-s3-credentials` Secret (`aws_access_key_id`/`aws_secret_access_key`):

- **89-1** the provisioned backup bucket exists and is reachable with the Job credentials
- **89-2** backup upload path: a synthetic gpbackup artifact set (metadata + data segment) is
  uploaded under `<folder>/<cluster>/<timestamp>/` and a prefix listing returns exactly the
  uploaded keys
- **89-3** restore download path: every artifact is downloaded back and matches the original
  **byte-for-byte** (sha256)
- **89-4** retention delete path: deleting the timestamp prefix removes all artifacts and a
  subsequent listing is empty (the retention cleanup Job's delete semantics)

Configuration comes only from the environment (`MINIO_ADDR`, `MINIO_ACCESS_KEY`,
`MINIO_SECRET_KEY`) with docker-compose defaults — no hardcoded endpoints. Test-case catalog:
`test/cases/scenario89_backup_minio_roundtrip_cases.go`; S3 test helper: `test/testutil/s3.go`.

```bash
# Requires the Docker Compose environment (make test-env-up && make test-env-setup)
go test ./test/integration/... -v -tags integration -run TestIntegration_Scenario89
```

```bash
# Run all controller tests
go test ./internal/controller/... -v

# Run a specific scenario
go test ./internal/controller/... -v -run TestScenario1
go test ./internal/controller/... -v -run TestScenario2
go test ./internal/controller/... -v -run TestScenario3
go test ./internal/controller/... -v -run TestScenario4

# Run session management functional tests
go test ./test/functional/... -v -tags functional -run TestScenario5

# Run resource management functional tests
go test ./test/functional/... -v -tags functional -run TestScenario6

# Run data loading functional tests
go test ./test/functional/... -v -tags functional -run TestScenario7

# Run scale-out functional tests
go test ./test/functional/... -v -tags functional -run TestScenario8

# Run scale-in functional tests
go test ./test/functional/... -v -tags functional -run TestScenario9

# Run rebalance functional tests
go test ./test/functional/... -v -tags functional -run TestScenario10

# Run scale-out failure and rollback functional tests
go test ./test/functional/... -v -tags functional -run TestScenario11

# Run scale-in >50% confirmation functional tests
go test ./test/functional/... -v -tags functional -run TestScenario12

# Run PV expansion functional tests
go test ./test/functional/... -v -tags functional -run TestScenario13

# Run cluster upgrade functional tests
go test ./test/functional/... -v -tags functional -run TestScenario14

# Run error handling, retry, and observability functional tests
go test ./test/functional/... -v -tags functional -run TestScenario15

# Run cluster deletion functional tests
go test ./test/functional/... -v -tags functional -run TestScenario16

# Run mirroring enable/disable functional tests
go test ./test/functional/... -v -tags functional -run TestScenario19

# Run automatic failover functional tests
go test ./test/functional/... -v -tags functional -run TestScenario20

# Run workload bootstrap functional tests
go test ./test/functional/... -v -tags functional -run TestScenario25

# Run basic auth flow functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario39

# Run basic auth flow E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario39

# Run password rotation functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario40

# Run password rotation E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario40

# Run OIDC full flow functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario41

# Run OIDC full flow E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario41

# Run role claim modes functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario42

# Run role claim modes E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario42

# Run permission matrix functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario43

# Run permission matrix E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario43

# Run custom HBA rules functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario44

# Run custom HBA rules E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario44

# Run HBA default rules functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario45

# Run HBA default rules E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario45

# Run SSL/TLS configuration functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario47

# Run SSL/TLS configuration E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario47

# Run webhook cert management functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario48

# Run webhook cert management E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario48

# Run CTL auth functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario49

# Run CTL auth E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario49

# Run auditing functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario50

# Run auditing E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario50

# Run security headers functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario51

# Run security headers E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario51

# Run negative/edge case functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario52

# Run negative/edge case E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario52
```

### Scenario 1 Live Cluster Test

Scenario 1 validates the full cluster bootstrap with a real Apache Cloudberry 2.1.0 image on a live Kubernetes cluster. The test CR is at `test/examples/scenario1-cluster.yaml`.

**Prerequisites:**

1. Build the Cloudberry DB image:
   ```bash
   make docker-build-cloudberry
   kind load docker-image cloudberrydb/cloudberry:2.1.0 --name cloudberry-dev
   ```

2. Deploy the operator:
   ```bash
   make docker-build
   kind load docker-image cloudberry-operator:latest --name cloudberry-dev
   helm install cloudberry-operator deploy/helm/cloudberry-operator \
     --namespace cloudberry-system --create-namespace
   ```

3. Deploy the Scenario 1 cluster:
   ```bash
   kubectl create namespace cloudberry-test
   kubectl apply -f test/examples/scenario1-cluster.yaml
   ```

**What the test validates:**

| Check | Verification |
|-------|-------------|
| Cluster status | Phase = `Running` |
| Database creation | `CREATE DATABASE mydb` succeeds |
| Webhook validation | `segments.count=0` rejected |
| RBAC, ConfigMaps, Secrets | All created with correct labels |
| Headless Services | Created for coordinator and segments |
| Init container | Runs successfully (data dir preparation) |
| Segment registration | `gp_segment_configuration` populated (9 rows: 1 coordinator + 1 standby + 4 primaries + 4 mirrors - 1 coordinator entry = 9 segment entries) |
| Config layers | All 4 layers applied (cluster-wide, coordinator-only, database-specific, role-specific) |
| Status fields | `coordinatorReady=true`, `standbyReady=true`, `segmentsReady=4`, `mirroringStatus=InSync` |
| Prometheus metrics | `cloudberry_cluster_info`, `cloudberry_coordinator_up`, etc. |
| Structured logging | JSON logs with `cluster`, `namespace`, `controller` fields |
| Replication | Coordinator→standby and primary→mirror streaming replication working |
| Data distribution | Data distributed across 4 segments |

**Cluster topology** (10 pods total):

```
scenario1-cluster-coordinator-0       (coordinator, dispatch mode)
scenario1-cluster-standby-0           (standby, streaming replica of coordinator)
scenario1-cluster-segment-primary-0   (primary, content_id=0)
scenario1-cluster-segment-primary-1   (primary, content_id=1)
scenario1-cluster-segment-primary-2   (primary, content_id=2)
scenario1-cluster-segment-primary-3   (primary, content_id=3)
scenario1-cluster-segment-mirror-0    (mirror of primary-0)
scenario1-cluster-segment-mirror-1    (mirror of primary-1)
scenario1-cluster-segment-mirror-2    (mirror of primary-2)
scenario1-cluster-segment-mirror-3    (mirror of primary-3)
```

### Coverage

The project **enforces 90%+ unit test statement coverage per package** (not just overall). Total project coverage: **90.9%** (improved from 85.3%). Current coverage for key packages:

| Package | Coverage | Notes |
|---------|----------|-------|
| `internal/controller` | ~90% | Improved from 88.1% → 90.0% with mock DB client tests, action annotation retry, lifecycle phase error logging, and context-aware rebalance |
| `internal/certmanager` | ~93% | Improved from ~90% with additional rotation and edge case tests |
| `internal/vault` | 99.1% | Near-complete coverage |
| `internal/metrics` | 100% | Full coverage |
| `internal/db` | ~92% | Improved from 89.3% → 92.2% with mock DB client factory, SSL config tests, and connection string builder tests |
| `internal/api` | ~96% | Improved from ~74% with input validation, recovery type validation, and rate limiter shutdown tests |
| `internal/ctl` | ~85% | URL encoding and response size limit tests |
| `internal/auth` | ~97.6% | Improved from 89.4% → 97.6% with OIDC redirect protection, auth controller log level, and unused field removal tests |
| `internal/idle` | ~97% | Improved from 71.2% → 97.1% with reconnection mechanism, health check, and exponential backoff tests |
| `cmd/operator` | ~30.1% | New coverage — previously 0%. Covers main startup, WaitGroup-based goroutine tracking, and admin password persistence |
| `cmd/cloudberry-ctl` | ~83.4% | Improved from 28.5% → 83.4% with context propagation, bulk import, and signal handling tests |

All 14 internal packages now meet or exceed the 90% coverage target.

**Goroutine-leak detection**: goroutine-heavy packages (`internal/api`, `internal/controller`, `internal/idle`, `internal/vault`) run [goleak](https://github.com/uber-go/goleak) from their `TestMain` (`main_test.go`), failing the package's test run if any test leaks a goroutine. New tests in these packages must clean up servers, daemons, watchers, and clients they start.

```bash
# Generate coverage report
make test-cover

# View HTML coverage report
open coverage/coverage.html

# View coverage summary
go tool cover -func=coverage/coverage.out
```

### Test Patterns

#### Mock DB Client

Controller and API tests use a mock `DBClientFactory` to test database-dependent code paths without a real database. The mock implements the `db.DBClientFactory` interface and returns configurable responses:

```go
type mockDBClientFactory struct {
    client db.Client
    err    error
}

func (m *mockDBClientFactory) NewClient(ctx context.Context, cluster *v1alpha1.CloudberryCluster) (db.Client, error) {
    return m.client, m.err
}
```

The mock DB client supports configurable SSL modes (`disable`, `require`, `verify-full`) matching the cluster's `spec.auth.ssl` configuration. Tests verify that the factory respects the cluster's SSL settings when creating connections.

#### Action Annotation Retry Pattern

Action annotations (e.g., `avsoft.io/action=start`) are now removed **after** successful processing rather than before. This ensures that if the action handler fails, the annotation remains on the resource and the action is retried on the next reconciliation cycle. Tests verify this behavior by simulating handler failures and confirming the annotation persists.

#### Context-Aware Backoff

The `RetryWithBackoff` function and ConfigMap retry logic now respect context cancellation during backoff sleep. Tests verify that a canceled context interrupts the backoff wait rather than sleeping for the full duration.

### Writing Unit Tests

Unit tests use the standard Go testing package with [testify](https://github.com/stretchr/testify) for assertions:

```go
package util

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestCoordinatorName(t *testing.T) {
    tests := []struct {
        name     string
        cluster  string
        expected string
    }{
        {
            name:     "standard name",
            cluster:  "my-cluster",
            expected: "my-cluster-coordinator",
        },
        {
            name:     "long name truncated",
            cluster:  "very-long-cluster-name-that-exceeds-kubernetes-limits",
            expected: "very-long-cluster-name-that-exceeds-kuber-coordinator",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := CoordinatorName(tt.cluster)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

### Writing Controller Tests

Controller tests use controller-runtime's fake client:

```go
package controller

import (
    "context"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/apimachinery/pkg/types"
    "k8s.io/client-go/tools/record"
    ctrl "sigs.k8s.io/controller-runtime"
    "sigs.k8s.io/controller-runtime/pkg/client/fake"

    cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

func TestClusterReconciler_NewCluster(t *testing.T) {
    scheme := runtime.NewScheme()
    require.NoError(t, cbv1alpha1.AddToScheme(scheme))

    cluster := &cbv1alpha1.CloudberryCluster{
        ObjectMeta: metav1.ObjectMeta{
            Name:      "test-cluster",
            Namespace: "default",
        },
        Spec: cbv1alpha1.CloudberryClusterSpec{
            Coordinator: cbv1alpha1.CoordinatorSpec{
                Storage: cbv1alpha1.StorageSpec{Size: "10Gi"},
            },
            Segments: cbv1alpha1.SegmentsSpec{
                Count:   4,
                Storage: cbv1alpha1.StorageSpec{Size: "20Gi"},
            },
        },
    }

    client := fake.NewClientBuilder().
        WithScheme(scheme).
        WithObjects(cluster).
        Build()

    reconciler := NewClusterReconciler(
        client, scheme,
        record.NewFakeRecorder(10),
        // ... other dependencies
    )

    result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
        NamespacedName: types.NamespacedName{
            Name:      "test-cluster",
            Namespace: "default",
        },
    })

    require.NoError(t, err)
    assert.True(t, result.Requeue)
}
```

### Writing API Tests

API tests use `httptest`:

```go
package api

import (
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestHealthz(t *testing.T) {
    server := NewServer(nil, nil, nil, nil)
    req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
    w := httptest.NewRecorder()

    server.Handler().ServeHTTP(w, req)

    assert.Equal(t, http.StatusOK, w.Code)
    assert.Contains(t, w.Body.String(), "ok")
}
```

## Code Review Findings and Fixes

The following security and reliability fixes were applied during comprehensive code reviews.

### Refactoring Session (2026-05-24)

The following fixes were applied during a comprehensive refactoring session focused on security vulnerabilities, SQL injection prevention, performance, and code quality:

#### Critical Fixes

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| CRIT-01 | `golang.org/x/net` vulnerability (GO-2026-5026) | Upgraded dependency to patched version | `go.mod` |
| CRIT-02 | SQL injection in distribution key handling | Added `sanitizeDistKey()` helper that validates each column name against SQL identifier regex | `internal/db` |
| CRIT-03 | SQL injection in `updateNumsegments` | Parameterized query using `$1` placeholder | `internal/db` |

#### Major Fixes

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| MAJ-02 | Duplicated `OperatorAdminPasswordSecretName` and `PasswordSecretKey` constants | Extracted to `internal/util/constants.go` as shared constants | `internal/util` |
| MAJ-03 | Missing context cancellation checks in `propagateDatabasesToNewSegments` | Added `ctx.Err()` checks between database operations | `internal/db` |
| MAJ-05 | Unused `*runtime.Scheme` parameter in `NewAuthReconciler` | Removed unused parameter from constructor signature | `internal/controller` |
| MAJ-06 | No rate limiting for rebalance operations | Added `interTableDelay` (100ms) and `dispatchRebalanceTables` with bounded parallelism | `internal/controller` |

#### Minor Fixes

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| MIN-03 | Error handling in `reconcileSubComponents` drops errors | Used `errors.Join` to aggregate errors from all sub-reconcilers | `internal/controller` |
| MIN-04 | Magic number for auth reconcile interval | Extracted `authReconcileInterval` constant (5 minutes) | `internal/controller` |
| MIN-06 | Potential goroutine leak in `startOrUpdateIdleDaemon` | Properly stop existing daemon before starting new one | `internal/controller` |
| MIN-08 | Port range not validated in CRD types | Added port range validation (1–65535) | `api/v1alpha1` |

#### Improvement

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| IMP-05 | Webhook CA bundle injection fails on transient errors | Added retry with exponential backoff for CA bundle injection | `internal/certmanager` |

### Refactoring Session (2026-05-19)

The following code fixes were applied during a refactoring session focused on correctness, security, and clean shutdown:

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| R-01 | `Dockerfile.ctl` ldflags used wrong variable name | Fixed `-X main.appVersion` to `-X main.version` to match the Go variable declaration in `cmd/cloudberry-ctl/main.go` | `Dockerfile.ctl` |
| R-02 | OIDC HTTP client followed unlimited redirects | Added `CheckRedirect` function limiting to 5 redirects in the OIDC provider's HTTP client, preventing infinite redirect loops during OIDC discovery | `internal/auth/oidc.go` |
| R-03 | Cert rotation goroutine not tracked for clean shutdown | Added `sync.WaitGroup` in `cmd/operator/main.go` to track the certificate rotation background goroutine, ensuring it completes before the operator process exits | `cmd/operator/main.go` |
| R-04 | CLI `upsertRule` created a new HTTP client per rule during bulk imports | Refactored `upsertRule` in `cmd/cloudberry-ctl/main.go` to accept a shared context and client, reducing connection overhead during bulk rule imports | `cmd/cloudberry-ctl/main.go` |

### Previous Code Review Findings

The following security and reliability fixes were applied during a comprehensive code review:

### Critical Fixes

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| CRITICAL-01 | SQL injection risk in `AlterResourceGroup` | Parameterized queries with `pgx` | `internal/db` |
| CRITICAL-02 | Manual connection string escaping | Replaced with pgx native config builder | `internal/db` |

### High-Priority Fixes

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| HIGH-01 | RateLimiter goroutine leak | Added `sync.Once`-guarded `Stop()` method; called during server shutdown | `internal/api` |
| HIGH-02 | Unencoded URL path parameters in CLI | Added `url.PathEscape()` for namespace/path parameters | `internal/ctl` |
| HIGH-03 | DB connection pool leak on retry | Proper pool cleanup on connection retry failure | `internal/db` |
| HIGH-04 | Duplicated `DBClientFactory` interface | Extracted to shared `internal/db/factory.go` package | `internal/db` |
| HIGH-05 | Missing HTTP server timeouts | Added `ReadTimeout` (30s), `WriteTimeout` (60s), `IdleTimeout` (120s) | `internal/api` |
| HIGH-06 | Unbounded response body in CLI | Added 10 MiB response body size limit via `io.LimitReader` | `internal/ctl` |

### Medium-Priority Fixes

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| MEDIUM-01/07 | Inline condition type strings | Defined constants (`DataRedistribution`, `ScaleOutFailed`, etc.) | `internal/util` |
| MEDIUM-02 | Missing Godoc on `AuthMethod` constants | Added documentation comments | `internal/auth` |
| MEDIUM-04 | Builder methods silently ignore errors | Changed `Build*StatefulSet` methods to return `(*StatefulSet, error)` | `internal/builder` |
| MEDIUM-05 | Verbose flag not wired to CLI client | Connected `--verbose` flag to `OperatorClient` for debug logging | `internal/ctl`, `cmd/cloudberry-ctl` |
| MEDIUM-06 | Silent JSON encoding failures | Added error logging for `json.Encode` failures in API handlers | `internal/api` |
| MEDIUM-08 | Vault PKI using `ReadSecret` for cert issuance | Changed to `WriteSecretWithResponse` (PKI issue is a write operation) | `internal/certmanager` |
| MEDIUM-09 | ENV var priority incorrect in CLI | Fixed to: CLI flag > env var > config file > default | `cmd/cloudberry-ctl` |

### Refactoring Session Fixes (Group A — Critical/Security)

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| A-01 | Admin password lost on pod restart | Persisted to K8s Secret `cloudberry-operator-admin-password` (survives pod restarts) | `cmd/operator` |
| A-02 | RateLimiter goroutine leak | `Server.Close()` called on shutdown, which calls `rateLimiter.Stop()` | `internal/api` |
| A-03 | Action annotation removed before processing | Annotation now removed AFTER successful processing; failed actions retry on next reconcile | `internal/controller` |
| A-04 | ConfigMap retry ignores context cancellation | Context-aware backoff in ConfigMap retry (respects `ctx.Done()` during sleep) | `internal/controller` |
| A-05 | No input validation on API path parameters | SQL identifier regex validation on cluster name, namespace, and resource group names | `internal/api` |
| A-06 | No recovery type validation | Recovery type restricted to `incremental`, `full`, `differential` only | `internal/api` |
| A-07 | CLI password flag exposes credentials | Security warning recommending `CLOUDBERRY_PASSWORD` env var instead of `--password` flag | `cmd/cloudberry-ctl` |

### Refactoring Session Fixes (Group B — Quality)

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| B-01 | Rebalance silently ignores errors | Error collection returns aggregate error from rebalance operations | `internal/controller` |
| B-02 | Lifecycle phase errors silently ignored | Phase transition errors now logged at WARN level | `internal/controller` |
| B-03 | DB factory ignores cluster SSL config | `DBClientFactory` now respects `spec.auth.ssl` settings (`disable`, `require`, `verify-full`) | `internal/db` |
| B-04 | Duplicated "cluster not found" strings | Extracted to `ErrMsgClusterNotFound` constant in `internal/util/constants.go` | `internal/util` |
| B-05 | Auth controller logs at INFO for unchanged generation | Changed to DEBUG level for unchanged generation skip (reduces log noise) | `internal/controller` |
| B-06 | Unused `scheme` field in `AuthReconciler` | Removed unused field from struct | `internal/controller` |
| B-07 | CLI ignores SIGINT/SIGTERM | Signal-aware context in CLI main — `SIGINT`/`SIGTERM` triggers context cancellation | `cmd/cloudberry-ctl` |
| B-08 | Magic numbers in code | Extracted to named constants (timeouts, limits, retry counts, etc.) | Multiple |

### Low-Priority Fixes

| ID | Issue | Fix | Package |
|----|-------|-----|---------|
| LOW-01 | Missing package-level documentation | Added `// Package ...` doc comments to all packages | All |
| LOW-02 | Inline event type strings | Standardized with named constants | `internal/controller` |
| LOW-03 | Missing `NoopRecorder` method comments | Added Godoc comments | `internal/metrics` |
| LOW-04 | Magic numbers in code | Extracted to named constants (timeouts, limits, etc.) | Multiple |
| LOW-05 | Exported `Version()` in main package | Unexported to `version()` (not part of public API) | `cmd/cloudberry-ctl` |

### Test Coverage Requirements

All new code must meet the following coverage targets:

- **Per-package minimum**: 90% statement coverage — **enforced** for every package, including the critical ones (`internal/db`, `internal/auth`, `internal/controller`)
- **Run coverage check**: `make test-cover` generates an HTML report at `coverage/coverage.html`
- **CI enforcement**: The CI pipeline fails if coverage drops below the threshold
- **Goroutine hygiene**: packages with goleak in `TestMain` (`internal/api`, `internal/controller`, `internal/idle`, `internal/vault`) fail on leaked goroutines

**Current coverage** (as of 2026-05-24):

| Package | Coverage |
|---------|----------|
| `internal/controller` | 90.1% |
| `cmd/cloudberry-ctl` | 91.6% |
| `cmd/operator` | 30.0% |
| **Overall project** | **91.4%** |

**Test counts**: All **1,936 tests** pass:
- Functional: 1,063
- E2E: 833
- Integration: 38

### Performance Testing

Performance tests validate operator behavior under load and with large datasets. Run them after loading test data (Scenario 7):

```bash
# Load test data (~1.45M rows, ~218 MB)
bash test/scenarios/scenario7_load_data.sh

# Run scale-out performance test (measures time to scale from 4 to 6 segments)
go test ./test/functional/... -v -tags functional -run TestScenario8 -timeout 10m

# Run rebalance performance test
go test ./test/functional/... -v -tags functional -run TestScenario10 -timeout 10m

# Run all functional tests with extended timeout
make test-functional TIMEOUT=30m
```

**Key performance metrics to monitor:**

- Scale-out completion time (target: < 60s for 2 additional segments)
- Rebalance completion time (depends on data volume)
- Reconciliation duration (`cloudberry_reconcile_duration_seconds`)
- API response latency under rate limiting

### Running REST API Performance Tests

The project includes a comprehensive REST API performance test suite using [Yandex Tank](https://yandextank.readthedocs.io/) (or `hey` as a macOS alternative). Tests are located in `test/performance/`.

```bash
# Navigate to the performance test directory
cd test/performance

# Run a smoke test (quick validation)
./run-perftest.sh --scenario smoke

# Run baseline performance test
./run-perftest.sh --scenario baseline

# Run stress test (find breaking point)
./run-perftest.sh --scenario stress

# Run endurance test (detect memory leaks)
./run-perftest.sh --scenario endurance
```

**Current baseline** (latest perf-test cycle):

- Authenticated API throughput: **~7 RPS per client** — bcrypt password verification on every Basic-auth request is the dominant cost (intentional security/throughput trade-off)
- Health endpoints: **p99 < 10ms**
- The default API rate limit is **10 requests/minute per IP** — raise it or disable it (`CLOUDBERRY_API_RATE_LIMIT=0`) before load testing, otherwise the limiter (not the server) is what you measure (rejections show up on `cloudberry_api_rate_limit_rejections_total`)
- The authenticated ammo files (`test/performance/ammo/api-read.txt`) use the `operator_user` test credentials, which require the operator to run with `CLOUDBERRY_ENABLE_TEST_USERS=true` (test environments only — the operator logs a WARN when enabled)

**Earlier full load-test results** (2026-05-19):

| Endpoint Type | p50 | p95 | p99 | Peak RPS | Errors |
|---------------|-----|-----|-----|----------|--------|
| Health (`/healthz`, `/readyz`) | 2.7ms | 6.5ms | 10.6ms | 12,637 | 0% |
| API (authenticated) | 605ms | 794ms | 885ms | ~6 | 0% |

**Key findings:**
- Health endpoints handle 12,637 RPS with sub-3ms p50 latency
- API endpoint latency is dominated by bcrypt authentication (~100ms per request at cost factor 10)
- Zero errors across 287,122 total requests
- Memory stable at 82MB resident with no growth observed

See `test/performance/README.md` for full test documentation, scenario descriptions, and SLO targets.

## Monitoring Stack Makefile Targets

The Makefile provides three targets for managing the monitoring stack (vmagent + OpenTelemetry Collector) in a Kubernetes cluster:

```bash
# Deploy the monitoring stack (vmagent + otel-collector) to the test namespace
make monitoring-deploy

# Check the status of the monitoring stack
make monitoring-status

# Remove the monitoring stack
make monitoring-undeploy
```

**`monitoring-deploy`** installs:
- **vmagent** (via `test/monitoring/vmagent` Helm chart) — Prometheus-compatible metrics collection agent
- **vector** (via `test/monitoring/vector` Helm chart) — log shipper to VictoriaLogs
- **node-exporter** (via `test/monitoring/node-exporter` Helm chart) — node-level metrics
- **otel-collector** (via `test/monitoring/otel-collector` Helm chart) — OpenTelemetry Collector with OTLP gRPC (port 4317) and HTTP (port 4318) receivers; the repo chart renders the service as `otel-collector`, matching the operator's `telemetry.otlpEndpoint`
- **kube-state-metrics** (via `test/monitoring/kube-state-metrics` Helm chart) — Kubernetes object-state metrics (`kube_job_*`, `kube_pod_init_container_status_*`, `kube_deployment_status_replicas_available`); added in Scenario 104 for pre-load health-check / data-load-Job observability

All are deployed to the `monitoring` namespace by default (configurable via `NAMESPACE_MONITORING`).

**`monitoring-status`** shows the Helm release status and running pods for both components.

**`monitoring-undeploy`** removes both Helm releases from the namespace.

## Idle Daemon Reconnection Mechanism

The idle session enforcement daemon (`internal/idle/daemon.go`) includes a reconnection mechanism with exponential backoff and periodic health checks to handle database connection failures gracefully.

### Health Check Loop

The daemon runs a periodic health check (every 60 seconds) that pings the database connection:

```
┌─────────────────────────────────────────────────────────────────┐
│              Idle Daemon Health Check Loop                        │
│                                                                   │
│  healthCheck() — called every 60s                                 │
│    │                                                              │
│    ├── Ping the DB client                                         │
│    │   ├── Success → reset consecutiveFails to 0                  │
│    │   └── Failure → increment consecutiveFails                   │
│    │                  └── attempt reconnect()                     │
│    │                                                              │
│  scanAndEnforce() — called every ScanInterval (default 30s)       │
│    │                                                              │
│    ├── List sessions via DB client                                │
│    │   ├── Success → reset consecutiveFails, enforce rules        │
│    │   └── Failure → increment consecutiveFails                   │
│    │                  └── if consecutiveFails >= 3 → reconnect()  │
└─────────────────────────────────────────────────────────────────┘
```

### Reconnection with Exponential Backoff

When a reconnection is needed, the daemon uses the `DBClientFactory` interface to create a new database client:

| Parameter | Value | Description |
|-----------|-------|-------------|
| `reconnectInitialBackoff` | 1s | Wait time before the first retry |
| `reconnectMaxBackoff` | 60s | Maximum wait time between retries |
| `reconnectMultiplier` | 2 | Backoff multiplier (exponential growth) |
| `healthCheckInterval` | 60s | Interval between health check pings |

**Key behaviors:**
- If `DBClientFactory` is nil, reconnection is skipped (graceful degradation)
- On successful reconnection, the old client is closed and replaced with the new one
- `consecutiveFails` counter is reset to 0 after successful reconnection
- Context cancellation is respected during backoff sleep via `select` on `ctx.Done()`
- The daemon continues operating (scanning and enforcing rules) even during reconnection attempts

### DBClientFactory Interface

```go
// DBClientFactory defines the interface for creating database clients.
// This allows the daemon to reconnect when the connection drops.
type DBClientFactory interface {
    NewClient(ctx context.Context) (db.Client, error)
}
```

The factory is configured via `Config.DBClientFactory` when creating the daemon. When set, the daemon automatically attempts reconnection on connection failures.

## Shared DB Client Pattern in Admin Controller

The Admin Controller's `reconcileConfig()` method creates a **single shared DB client** for all parameter operations within a single reconciliation cycle. This avoids the previous pattern of creating multiple DB clients (one per parameter layer), which caused unnecessary connection overhead.

```
┌─────────────────────────────────────────────────────────────────┐
│              reconcileConfig() — Single DB Client                 │
│                                                                   │
│  1. Detect config changes via hash comparison                     │
│  2. Create ONE shared DB client via DBClientFactory               │
│  3. Pass sharedClient to all parameter handlers:                  │
│     ├── applyCoordinatorParameters(sharedClient)                  │
│     ├── applyDatabaseParameters(sharedClient)                     │
│     └── applyRoleParameters(sharedClient)                         │
│  4. Close the shared client (defer)                               │
│                                                                   │
│  Each handler checks: if sharedClient is nil → skip gracefully    │
│  (logs "no shared DB client available, skipping ...")              │
└─────────────────────────────────────────────────────────────────┘
```

**Benefits:**
- Reduces the number of database connections per reconciliation from 3 to 1
- Ensures consistent connection state across all parameter operations
- Graceful degradation when the DB client factory is unavailable

## Context-Aware Rebalance Goroutine Management

The `executeRebalanceViaDB()` method in the HA Controller uses context cancellation checks when acquiring a semaphore to prevent goroutine leaks:

```go
// Use a select with ctx.Done() when acquiring the semaphore to avoid
// goroutine leaks if the context is canceled while waiting.
select {
case <-ctx.Done():
    return ctx.Err()
case sem <- struct{}{}:
    // Proceed with rebalance operation
}
```

This ensures that if the reconciliation context is canceled (e.g., operator shutdown, timeout), goroutines waiting to acquire the semaphore are properly cleaned up instead of leaking.

## Code Style and Linting

### Linter Configuration

The project uses golangci-lint with configuration in `.golangci.yml`. Run the linter:

```bash
make lint
```

### Code Formatting

```bash
# Format all Go files
make fmt

# Check formatting (CI)
make fmt-check
```

### Go Vet

```bash
make vet
```

### Vulnerability Check

```bash
make vuln
```

### Style Guidelines

- Follow [Effective Go](https://go.dev/doc/effective_go) and the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- Use `slog` for structured logging (not `fmt.Println` or `log`)
- All exported types and functions must have doc comments
- Use table-driven tests with descriptive test names
- All external dependencies must be behind interfaces for testability
- Use `context.Context` for cancellation and timeout propagation
- Avoid global state; prefer dependency injection

## Code Generation

### Generate DeepCopy Methods

```bash
make generate
```

This runs `controller-gen object` on `api/v1alpha1/` to generate `zz_generated.deepcopy.go`.

### Generate CRD Manifests

```bash
make manifests
```

This generates:
- CRD YAML at `deploy/helm/cloudberry-operator/crds/`
- RBAC ClusterRole manifest

### When to Regenerate

Run `make generate && make manifests` after:
- Adding or modifying types in `api/v1alpha1/types.go`
- Adding or modifying kubebuilder markers
- Adding new RBAC markers to controllers

## Adding New Features

### Key Implementation Details

#### Mirroring Enable/Disable Implementation

The mirroring enable/disable feature (Scenario 19) is implemented in `internal/controller/cluster_controller.go` with the following key methods:

- **`isMirroringEnableNeeded()`**: Detection method that checks four conditions: `spec.segments.mirroring.enabled=true`, `status.mirroringStatus=NotConfigured`, cluster phase is `Running`, and no mirror StatefulSet exists. All four must be true.
- **`handleMirroringEnable()`**: Creates the mirror StatefulSet via `BuildMirrorStatefulSet()`, sets phase to `Updating`, sets `status.mirroringStatus` to `Initializing`, initiates WAL replication via the DB client, and emits `MirroringEnabled` event.
- **`checkMirroringProgress()`**: Called on each reconciliation when status is `Initializing` or `Syncing`. Monitors mirror StatefulSet readiness and replication lag. Transitions through `Initializing` → `Syncing` → `InSync`. Detects 30-minute timeout and sets `Degraded`.
- **`completeMirroringEnable()`**: Sets `status.mirroringStatus` to `InSync`, phase to `Running`, and emits `MirroringInSync` event.
- **`isMirroringDisableNeeded()`**: Checks `spec.segments.mirroring.enabled=false`, status is not `NotConfigured`, and cluster is `Running`.
- **`handleMirroringDisable()`**: Deletes the mirror StatefulSet, handles PVC cleanup based on `deletionPolicy`, sets `status.mirroringStatus` to `NotConfigured`, and emits `MirroringDisabled` event.

The webhook (`internal/webhook/validating.go`) validates mirroring changes on UPDATE:
- `validateMirroringChange()` checks that enabling mirroring is only allowed on `Running` clusters with sufficient nodes
- `isMirroringEnabled()` helper determines if mirroring is enabled in the spec
- Changing layout while mirroring is enabled is rejected

Metrics are recorded via `RecordMirroringOperation(cluster, namespace, operation)` where `operation` is `"enable"` or `"disable"`, and `SetReplicationLag(cluster, namespace, segment, lagBytes)` for replication monitoring.

#### Automatic Failover Implementation (Scenario 20)

The automatic segment failover feature is implemented in `internal/controller/ha_controller.go` with the following key methods:

- **`probeSegmentConfigWithRetries()`**: Retry wrapper around `dbClient.GetSegmentConfiguration()`. Creates a `context.WithTimeout` per attempt using `probeTimeout()`. Retries up to `probeRetries()` times. Records `fts_probe_total{result=failure}` per failed attempt. Returns segments on first success or the last error after exhaustion.
- **`analyzeSegments()`**: Iterates over segment configuration, skips coordinator entries (contentID < 0), records per-segment status metrics, and builds `failedSegments` and `failedPrimaries` lists. Returns a `segmentAnalysisResult` struct.
- **`handleFailover()`**: Called when `len(failedPrimaries) > 0` and mirroring is enabled. Calls `dbClient.TriggerFTSProbe()` to initiate Cloudberry's internal FTS scan. Re-reads segment configuration to verify promotion. Emits `SegmentFailover` events per failed primary. Increments `cloudberry_fts_failover_total`. Continues with status updates even if trigger or re-read fails.
- **`updateFTSProbeStatus()`**: Sets `status.failedSegments`, updates `mirroringStatus` to `InSync` or `MirroringDegraded`, and emits `MirroringDegraded` event when segments are down.
- **`reportMirrorReplicationLag()`**: Best-effort replication lag reporting via `dbClient.GetMirrorSyncStatus()`. Errors are logged but do not fail the probe.
- **`patchFTSStatus()`**: Manually constructs a MergePatch that always includes `failedSegments` (even when empty) to work around `omitempty` JSON tag behavior.

The failover verification uses a DBID comparison: after triggering the FTS scan, the operator checks whether a different DBID now holds the primary role (`role="p"`) for the same content ID. If the DBID changed, the mirror was successfully promoted.

#### Builder Interface Error Handling

Builder methods that construct StatefulSets (e.g., `BuildStandbyStatefulSet`) return `(*appsv1.StatefulSet, error)` instead of just `*appsv1.StatefulSet`. This change surfaces configuration errors early (e.g., invalid resource quantities, missing required fields) rather than silently producing invalid resources. Callers must check the error return value.

#### `BuildMaintenanceJob` (ResourceBuilder Interface)

The `BuildMaintenanceJob` method on the `ResourceBuilder` interface creates a Kubernetes `batchv1.Job` for maintenance operations:

```go
BuildMaintenanceJob(cluster *CloudberryCluster, operation, timestamp string) *batchv1.Job
```

- **Parameters**: `operation` is one of `vacuum`, `vacuum-analyze`, `vacuum-full`, `analyze`, `reindex`, `backup-on-delete`. `timestamp` is used for unique Job naming.
- **Job name**: `{cluster}-maintenance-{timestamp}`
- **Container**: Uses the cluster's image with a `psql` command connecting to the coordinator service
- **Environment**: `PGPASSWORD` sourced from `{cluster}-admin-password` Secret via `SecretKeyRef`
- **Properties**: `BackoffLimit=1`, `TTLSecondsAfterFinished=3600`, `RestartPolicy=Never`
- **Labels**: `avsoft.io/cluster={cluster}`, `avsoft.io/operation=maintenance`

#### `restartRequiredParams` Classification

The `restartRequiredParams` map in `internal/controller/admin_controller.go` classifies PostgreSQL parameters that require a server restart (context = `postmaster`). All parameters not in this map are treated as reload-safe (context = `sighup`).

When the Admin Controller detects a config change, it diffs the old and new parameter maps and checks each changed parameter against `restartRequiredParams`. If any changed parameter is in the map, a rolling restart is triggered; otherwise, a simple reload is performed.

```go
var restartRequiredParams = map[string]bool{
    "shared_buffers":                 true,
    "max_connections":                true,
    "max_prepared_transactions":      true,
    "max_worker_processes":           true,
    "max_wal_senders":                true,
    "wal_level":                      true,
    "wal_buffers":                    true,
    "huge_pages":                     true,
    "shared_preload_libraries":       true,
    "max_locks_per_transaction":      true,
    "max_files_per_process":          true,
    "port":                           true,
    "superuser_reserved_connections": true,
    "unix_socket_directories":        true,
    "listen_addresses":               true,
    "bonjour":                        true,
    "ssl":                            true,
}
```

To add a new restart-required parameter, add it to this map and update the documentation in `docs/user-guide.md`.

### Adding a New CRD Field

1. Add the field to the appropriate struct in `api/v1alpha1/types.go` with kubebuilder markers
2. Run `make generate` to regenerate deepcopy
3. Run `make manifests` to regenerate CRD YAML
4. Update the resource builder in `internal/builder/builder.go`
5. Update the relevant controller to handle the new field
6. Add unit tests for the new field
7. Update documentation

### Adding a New API Endpoint

1. Add the route in `internal/api/server.go` in `registerRoutes()`
2. Implement the handler function
3. Set the appropriate permission level
4. Add unit tests with `httptest`
5. Update `docs/api-reference.md`

### Adding a New CLI Command

1. Add the command in `cmd/cloudberry-ctl/main.go`
2. Register it with the parent command via `AddCommand()`
3. Add flags specific to the command
4. Use `internal/ctl.OperatorClient` to make API calls to the operator REST API
5. Use `internal/ctl.FormatOutput()` for table/JSON/YAML output formatting
6. Return appropriate exit codes (see `docs/cloudberry-ctl.md` for the exit code table)
7. Add unit tests
8. Update `docs/cloudberry-ctl.md`

The `internal/ctl` package provides:
- **`OperatorClient`**: HTTP client with basic/OIDC auth, timeout, and redirect protection
- **`FormatOutput`**: Renders API responses in table, JSON, or YAML format
- **Path helpers**: `ClusterPath()`, `ClusterStatusPath()`, `ClusterActionPath()`, etc.

### Status Update Pattern

All controllers use `Status().Patch()` with `MergePatchType` instead of `Status().Update()`. This prevents status clobbering when multiple controllers reconcile the same `CloudberryCluster` concurrently.

**Standard status patch:**

```go
func patchStatus(ctx context.Context, c client.Client, cluster *CloudberryCluster) error {
    statusPatch, _ := json.Marshal(map[string]interface{}{
        "status": cluster.Status,
    })
    return c.Status().Patch(ctx, cluster, client.RawPatch(types.MergePatchType, statusPatch))
}
```

**FTS status patch** (handles `omitempty` on `FailedSegments`):

```go
// Always include failedSegments explicitly to clear previous failures
statusMap["failedSegments"] = []interface{}{} // empty array, not omitted
```

When adding new status fields or updating status in a controller, always use `patchStatus()` or construct a MergePatch manually. Never use `Status().Update()`.

### Webhook Certificate Manager (`internal/certmanager`)

The `certmanager` package manages TLS certificates for the admission webhook server. It provides:

- **`CertManager` interface**: `EnsureCertificates()` and `NeedsRotation()`
- **Two strategies**: `vault-pki` (issues certs via Vault PKI engine) and `self-signed` (generates ECDSA P-256 CA + server certs)
- **Automatic rotation**: Certificates rotate when 2/3 of their lifetime has elapsed
- **Kubernetes Secret storage**: Certs stored as `kubernetes.io/tls` Secrets with `ca.crt`, `tls.crt`, `tls.key`

To extend the certmanager (e.g., adding cert-manager.io support), implement the certificate generation logic and add a new `CertSource` constant.

### Cross-Namespace Duplicate Detection

The validating webhook checks for duplicate `CloudberryCluster` names across all namespaces on CREATE operations. If a cluster with the same `.metadata.name` exists in any other namespace, the webhook rejects the request with an error:

```
CloudberryCluster with name "my-cluster" already exists in namespace "other-ns"
```

This is implemented in `internal/webhook/validating.go` via `checkDuplicateName()`, which lists all clusters and compares names. Updates to existing clusters in the same namespace are allowed.

### Adding a New Prometheus Metric

1. Define the metric in `internal/metrics/metrics.go`
2. Register it in the `NewPrometheusRecorder` constructor
3. Add a recording method to the `Recorder` interface
4. Call the recording method from the appropriate controller or handler
5. Add unit tests
6. Update the metrics table in `docs/user-guide.md`

### cloudberry-query-exporter Retention Durations

The `cloudberry-query-exporter` sidecar accepts a `--history-retention` flag controlling how long query-history entries are kept. The flag is parsed by `parseRetention()` in `cmd/cloudberry-query-exporter/main.go`, which accepts:

- Standard Go durations handled by `time.ParseDuration` (for example, `720h`, `1000ms`)
- CRD-friendly day and week suffixes: `d` (days) and `w` (weeks), for example `30d` → `720h`, `90d` → `2160h`, `2w` → `336h`

An empty value falls back to the default retention period, and negative or otherwise invalid values are rejected with a clear error.

> **Fixed**: Previously, passing a day- or week-based value such as `--history-retention=30d` crashed the exporter because `time.ParseDuration` does not understand the `d`/`w` units. `parseRetention()` now normalizes these suffixes to hours before parsing, so values like `30d`, `90d`, and `2w` work alongside standard Go durations.

## Debugging

### Running the Operator Locally

```bash
# Run against a local K8s cluster (uses current kubeconfig)
go run ./cmd/operator/ \
  --metrics-bind-address=:8080 \
  --health-probe-bind-address=:8081

# With debug logging
CLOUDBERRY_LOG_LEVEL=debug go run ./cmd/operator/
```

### Debugging with Delve

```bash
# Install delve
go install github.com/go-delve/delve/cmd/dlv@latest

# Debug the operator
dlv debug ./cmd/operator/ -- \
  --metrics-bind-address=:8080 \
  --health-probe-bind-address=:8081

# Debug a specific test
dlv test ./internal/controller/ -- -run TestClusterReconciler
```

### Viewing Operator Logs

```bash
# Follow operator logs
kubectl logs -n cloudberry-system deployment/cloudberry-operator -f

# Filter by log level
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.level == "ERROR")'

# Filter by cluster
kubectl logs -n cloudberry-system deployment/cloudberry-operator | \
  jq 'select(.cluster == "my-cluster")'
```

### Inspecting Kubernetes Resources

```bash
# Describe the cluster resource
kubectl describe cloudberrycluster my-cluster -n cloudberry-test

# View managed resources
kubectl get all -n cloudberry-test -l avsoft.io/cluster=my-cluster

# View events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp'

# View operator RBAC
kubectl get clusterrole cloudberry-operator -o yaml
```

### Common Issues

**"cannot find package" errors**: Run `go mod tidy` to sync dependencies.

**CRD out of date**: Run `make generate && make manifests` after changing types.

**Linter failures**: Run `make lint` locally before pushing. Fix issues or add `//nolint` with justification.

**Test flakiness**: Use `t.Parallel()` carefully. Ensure tests don't share mutable state. Use unique resource names in tests.
