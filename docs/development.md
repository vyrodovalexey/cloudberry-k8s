# Development Guide

This guide covers setting up a development environment, building the project, running tests, and contributing to the Cloudberry Kubernetes Operator.

## Table of Contents

- [Development Environment Setup](#development-environment-setup)
- [Project Structure](#project-structure)
- [Building](#building)
- [Testing](#testing)
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

### Test Environment (Docker Compose)

See [Docker Compose Test Environment](#docker-compose-test-environment) above for detailed setup instructions.

### Docker Compose Test Environment

The project includes a Docker Compose setup for integration testing with Vault and Keycloak:

```bash
# Start test services (Vault, Keycloak)
make test-env-up

# Run setup scripts (configures Vault policies, Keycloak realm/clients)
make test-env-setup

# Run integration tests
make test-integration

# Tear down
make test-env-down
```

The setup scripts (`test/docker-compose/scripts/`) configure:
- **Vault**: Enables the KV secrets engine, creates policies and Kubernetes auth roles
- **Keycloak**: Creates the `cloudberry` realm, `cloudberry-operator` client, and test users with roles

### Monitoring Stack Deployment

The project includes monitoring configurations in the `monitoring/` directory:

- **Grafana dashboards**: Pre-built dashboards for operator metrics in `monitoring/grafana/`
- **vmagent / otel-collector**: Deploy alongside the operator for metrics collection and distributed tracing

To deploy the monitoring stack to a local Kubernetes cluster:

```bash
# Deploy the operator with telemetry enabled
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
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
│   │   └── webhook_test.go
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
| `internal/db` | Database operations and client factory | pgx/v5 |
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
| `buildConnectionString()` | `internal/db` | Constructs a PostgreSQL connection string with properly escaped parameters. Returns an error (instead of falling back to a default) if the connection string cannot be parsed |
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

# Build Docker images
make docker-build

# Build operator Docker image only
make docker-build-operator

# Build CLI Docker image only
make docker-build-ctl

# Push Docker images
make docker-push
```

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
```

### Coverage

The project targets **90%+ unit test statement coverage**. Current coverage for key packages:

| Package | Coverage |
|---------|----------|
| `internal/db` | 92.9% |
| `internal/vault` | 99.1% |

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

#### `BuildMaintenanceJob` (ResourceBuilder Interface)

The `BuildMaintenanceJob` method on the `ResourceBuilder` interface creates a Kubernetes `batchv1.Job` for maintenance operations:

```go
BuildMaintenanceJob(cluster *CloudberryCluster, operation, timestamp string) *batchv1.Job
```

- **Parameters**: `operation` is one of `vacuum`, `vacuum-analyze`, `vacuum-full`, `analyze`, `reindex`. `timestamp` is used for unique Job naming.
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
