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

# Run setup scripts (configures Vault PKI, Keycloak realm, MinIO buckets, Kafka topics, RabbitMQ queues)
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
4. `scripts/setup-keycloak.sh` — creates realm, clients for service-to-service auth
5. `scripts/setup-minio.sh` — creates test buckets
6. `scripts/setup-kafka.sh` — creates test topics
7. `scripts/setup-rabbitmq.sh` — creates test queues

The setup scripts (`test/docker-compose/scripts/`) configure:
- **Vault**: Enables the PKI secrets engine, creates policies and Kubernetes auth roles
- **Keycloak**: Creates the `cloudberry` realm, `cloudberry-operator` client, and test users with roles
- **MinIO**: Creates S3-compatible test buckets for backup testing
- **Kafka**: Creates test topics for event streaming
- **RabbitMQ**: Creates test queues for message processing

### Monitoring Stack Deployment

The project includes monitoring configurations in the `monitoring/` directory and the Docker Compose test environment:

- **Grafana dashboards**: Pre-built dashboards for operator metrics in `monitoring/grafana/`
- **vmagent**: VictoriaMetrics agent for Prometheus-compatible metrics collection
- **otel-collector**: OpenTelemetry Collector for distributed tracing

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

To deploy the monitoring stack alongside the operator in a local Kubernetes cluster:

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
│   │   └── auth_e2e_test.go
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
| `internal/metrics` | Prometheus metrics registration | prometheus/client_golang |
| `internal/telemetry` | OTLP tracing setup | opentelemetry-go |
| `internal/vault` | Vault client with retry | vault/api, internal/util |
| `internal/auth` | Auth providers and middleware | go-oidc, internal/vault |
| `internal/ctl` | Operator API HTTP client for CLI | net/http |
| `internal/db` | Database operations, client factory, and shared `DBClientFactory` interface | pgx/v5 |
| `internal/builder` | K8s resource construction | api/v1alpha1, internal/util |
| `internal/controller` | Reconciliation controllers | All internal packages |
| `internal/api` | REST API server | internal/auth, internal/metrics |
| `internal/certmanager` | Webhook TLS cert lifecycle (Vault PKI / self-signed) | internal/vault, k8s client |
| `internal/webhook` | Admission webhooks (with cross-namespace duplicate detection) | api/v1alpha1 |

### Key Internal Helpers

The codebase uses several internal helper functions that are important to understand when contributing:

| Helper | Package | Purpose |
|--------|---------|---------|
| `resolvePort()` | `internal/builder` | Returns the coordinator port from the cluster spec, falling back to the default (5432). Used by all resource builder functions to avoid duplicating port resolution logic |
| `notImplemented()` | `cmd/cloudberry-ctl` | Returns a standardized `"command %q is not yet implemented"` error for stub CLI commands. All unimplemented commands use this helper to provide consistent error messages |
| `removeAnnotationPatch()` | `internal/controller` | Removes an annotation from a cluster using a `MergePatch` instead of a full update. This avoids race conditions when multiple controllers modify the same resource concurrently |
| `patchStatus()` | `internal/controller` | Patches the status subresource using `Status().Patch()` with `MergePatchType`. Prevents status clobbering between concurrent controllers |
| `patchFTSStatus()` | `internal/controller` | Patches FTS-related status fields with a manually constructed MergePatch. Handles `omitempty` on `FailedSegments` by always including the field explicitly, even when empty |
| `checkDuplicateName()` | `internal/webhook` | Lists all `CloudberryCluster` resources across namespaces and rejects creation if the same name exists in a different namespace |
| `buildConnectionString()` | `internal/db` | Constructs a PostgreSQL connection string using the pgx native config builder (not manual string escaping). Returns an error (instead of falling back to a default) if the connection string cannot be parsed |
| `GenerateRandomPassword()` | `internal/util` | Generates a cryptographically secure random password including special characters (`!@#$%^&*()-_=+`). Used for auto-generated admin passwords |

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

1. **Builder stage**: Go 1.26 Alpine, compiles with `-trimpath` and `-ldflags="-s -w -X main.version=... -X main.commit=... -X main.buildDate=..."`
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

The project targets **90%+ unit test statement coverage** per package. Approximately 121 new test cases were added during the latest refactoring cycle, improving overall coverage from ~71% to ~85%+. Current coverage for key packages:

| Package | Coverage | Notes |
|---------|----------|-------|
| `internal/controller` | ~83% | Improved from 57% with comprehensive scenario tests |
| `internal/certmanager` | ~90% | Improved from 64% with Vault PKI and self-signed tests |
| `internal/vault` | 99.1% | Near-complete coverage |
| `internal/metrics` | 100% | Full coverage |
| `internal/db` | 92.9% | Includes SQL injection fix tests |
| `internal/api` | ~85% | Rate limiter, server timeout, and handler tests |
| `internal/ctl` | ~85% | URL encoding and response size limit tests |
| `internal/auth` | ~90% | Basic and OIDC provider tests |

```bash
# Generate coverage report
make test-cover

# View HTML coverage report
open coverage/coverage.html

# View coverage summary
go tool cover -func=coverage/coverage.out
```

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

- **Per-package minimum**: 90% statement coverage
- **Critical packages** (`internal/db`, `internal/auth`, `internal/controller`): 85%+ minimum
- **Run coverage check**: `make test-cover` generates an HTML report at `coverage/coverage.html`
- **CI enforcement**: The CI pipeline fails if overall coverage drops below the threshold

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
