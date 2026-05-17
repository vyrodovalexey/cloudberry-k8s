# Cloudberry Operator - Task Breakdown

**Version**: 1.0.0
**Generated**: 2026-05-11
**Agent**: claude-opus-4-6

---

## Table of Contents

1. [Project Summary](#1-project-summary)
2. [Dependency Graph](#2-dependency-graph)
3. [Phase A: Project Scaffolding and Core Types](#3-phase-a-project-scaffolding-and-core-types)
4. [Phase B: Internal Packages](#4-phase-b-internal-packages)
5. [Phase C: Cloudberry DB Client and K8s Resource Builders](#5-phase-c-cloudberry-db-client-and-k8s-resource-builders)
6. [Phase D: Controllers](#6-phase-d-controllers)
7. [Phase E: Operator API and Webhooks](#7-phase-e-operator-api-and-webhooks)
8. [Phase F: cloudberry-ctl CLI](#8-phase-f-cloudberry-ctl-cli)
9. [Phase G: Unit Tests](#9-phase-g-unit-tests)
10. [Phase H: DevOps](#10-phase-h-devops)
11. [Phase I: Integration and E2E Tests](#11-phase-i-integration-and-e2e-tests)
12. [Phase J: Performance Tests](#12-phase-j-performance-tests)
13. [Phase K: Documentation](#13-phase-k-documentation)
14. [Prioritized Execution Order](#14-prioritized-execution-order)
15. [Risk Assessment](#15-risk-assessment)
16. [Effort Estimation](#16-effort-estimation)

---

## 1. Project Summary

| Attribute | Value |
|-----------|-------|
| Project | cloudberry-k8s |
| Type | Greenfield Go project |
| Operator | cloudberry-operator (controller-runtime / kubebuilder) |
| CLI | cloudberry-ctl (cobra / viper) |
| CRD | CloudberryCluster (v1alpha1, group: avsoft.io) |
| Test Coverage Target | 90%+ unit test statement coverage |
| Total Tasks | 61 |
| Estimated Effort | 93-130 developer-days |

---

## 2. Dependency Graph

### High-Level Phase Dependencies

```
Phase A (Scaffolding) ──> Phase B (Internal Packages)
                     ──> Phase C (DB Client + Resource Builders)
Phase B ──> Phase D (Controllers)
Phase C ──> Phase D
Phase D ──> Phase E (API + Webhooks)
Phase B ──> Phase F (CLI)
Phase E ──> Phase F
Phase A..F ──> Phase G (Unit Tests - continuous, final sweep)
Phase A..F ──> Phase H (DevOps)
Phase H ──> Phase I (Integration + E2E)
Phase I ──> Phase J (Performance Tests)
Phase A..J ──> Phase K (Documentation)
```

### Detailed Task Dependencies

```
A.1 ──> A.2 ──> A.3 ──> A.4
A.1 ──> A.5
A.1 ──> A.6
A.1 ──> B.1, B.2, B.3, B.4
A.5 ──> B.2, B.6
B.1 ──> B.4, B.5, B.6
B.2 ──> B.5, C.1, C.2
B.3 ──> C.1
B.5 ──> B.6, D.3
A.3 ──> C.2, E.2, E.3
C.1, C.2, B.3, B.4 ──> D.1
D.1 ──> D.2, D.3, D.4
B.6, D.1, C.1 ──> E.1
E.2, E.3 ──> E.3b
D.1..D.4, E.1..E.3b ──> E.4
B.1 ──> F.1 ──> F.2 ──> F.3..F.10
H.1 ──> H.2 ──> H.3 ──> H.4, H.5
D.1..D.4 ──> I.1 ──> I.2..I.5
H.5 ──> I.6 ──> I.7..I.9
I.7 ──> J.1..J.3
```

---

## 3. Phase A: Project Scaffolding and Core Types

### A.1 - Initialize Go Module and Project Structure

| Attribute | Value |
|-----------|-------|
| **Dependencies** | None |
| **Priority** | P0 |
| **Complexity** | M |

**Description**: Create `go.mod`, establish directory layout following standard Go project layout and kubebuilder conventions.

**Acceptance Criteria**:
- `go.mod` created with module path (e.g., `github.com/cloudberry-db/cloudberry-k8s`)
- Go version set to 1.23+
- Directory structure created:
  ```
  cmd/operator/main.go
  cmd/cloudberry-ctl/main.go
  api/v1alpha1/
  internal/controller/
  internal/config/
  internal/util/
  internal/metrics/
  internal/telemetry/
  internal/vault/
  internal/auth/
  internal/db/
  internal/builder/
  internal/api/
  deploy/helm/cloudberry-operator/
  deploy/docker/
  hack/
  ```
- `go build ./...` succeeds with no errors
- `golangci-lint run` passes (using existing `.golangci.yml`)

**Test Cases**:
- Verify `go mod tidy` produces no changes
- Verify directory structure matches conventions

---

### A.2 - Add Core Dependencies

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1 |
| **Priority** | P0 |
| **Complexity** | S |

**Description**: Add all required Go dependencies to `go.mod`: controller-runtime, cobra, viper, pgx, vault, prometheus, opentelemetry, go-oidc, testify, gomock, etc.

**Acceptance Criteria**:
- All dependencies listed in spec 01 (Technology Stack) are in `go.mod`
- `go mod tidy` succeeds
- No conflicting dependency versions
- `go.sum` is committed

**Test Cases**:
- Verify each dependency can be imported without error

---

### A.3 - Define CRD Go Types (api/v1alpha1)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.2 |
| **Priority** | P0 |
| **Complexity** | L |

**Description**: Implement CloudberryCluster Go types matching the CRD schema in specification 02. Include all spec and status fields, kubebuilder markers for validation, defaults, and printer columns.

**Acceptance Criteria**:
- `types.go`: CloudberryCluster, CloudberryClusterSpec, CloudberryClusterStatus
- All nested types: CoordinatorSpec, StandbySpec, SegmentsSpec, MirroringSpec, AuthSpec, BasicAuthSpec, OIDCSpec, HBARule, SSLSpec, ConfigSpec, HASpec, VaultSpec, MonitoringSpec, TelemetrySpec, SegmentStatus, Condition
- Kubebuilder markers: `+kubebuilder:object:root`, `+kubebuilder:subresource:status`, `+kubebuilder:printcolumn`, `+kubebuilder:validation:Enum`, `+kubebuilder:default`
- `groupversion_info.go` with SchemeBuilder
- `zz_generated.deepcopy.go` generated
- `doc.go` with package documentation
- All enums defined as string constants (Phase, MirroringStatus, DeletionPolicy, etc.)
- Status condition types defined as constants

**Test Cases**:
- Unit: Verify DeepCopy works correctly for all types
- Unit: Verify default values are set correctly
- Unit: Verify JSON serialization/deserialization roundtrip
- Unit: Verify required field validation markers present

---

### A.4 - Generate CRD Manifests and DeepCopy

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3 |
| **Priority** | P0 |
| **Complexity** | M |

**Description**: Set up code generation using controller-gen for CRD YAML, DeepCopy methods, and RBAC manifests.

**Acceptance Criteria**:
- Makefile target `make generate` runs controller-gen for deepcopy
- Makefile target `make manifests` generates CRD YAML to `deploy/crd/`
- Generated CRD YAML matches specification 02 schema
- RBAC ClusterRole manifest generated
- All generated files compile without errors

**Test Cases**:
- Verify CRD YAML is valid (`kubectl apply --dry-run=server`)
- Verify generated deepcopy methods exist for all types
- Verify CRD includes printer columns (Phase, Version, Segments, Mirroring, Age)

---

### A.5 - Define Shared Constants and Error Types

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1 |
| **Priority** | P0 |
| **Complexity** | S |

**Description**: Create shared constants package with API group, version, label keys, annotation keys, finalizer names, condition types, and custom error types used across the project.

**Acceptance Criteria**:
- `internal/util/constants.go`: API group, version, label keys, annotation keys
- `internal/util/errors.go`: Custom error types (ClusterNotFoundError, SegmentNotFoundError, ValidationError, AuthenticationError, etc.)
- Annotation keys for actions: start, stop, restart, recovery, rebalance, maintenance
- Finalizer name: `avsoft.io/finalizer`
- Label keys: `app.kubernetes.io/managed-by`, `avsoft.io/cluster`, etc.

**Test Cases**:
- Unit: Verify error types implement error interface
- Unit: Verify error wrapping/unwrapping works
- Unit: Verify `Is`/`As` error matching

---

### A.6 - Set Up Structured Logging

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.2 |
| **Priority** | P0 |
| **Complexity** | S |

**Description**: Configure slog-based structured logging with configurable log levels, JSON output format, and standard fields (cluster, namespace, controller, reconcileID).

**Acceptance Criteria**:
- `internal/util/logging.go`: Logger factory with configurable level and format
- Integration with controller-runtime's logging (logr adapter)
- Standard fields attached to logger context
- Log levels: DEBUG, INFO, WARN, ERROR
- JSON output format for production, text for development
- Logger can be extracted from `context.Context`

**Test Cases**:
- Unit: Verify logger creation with different levels
- Unit: Verify structured fields appear in output
- Unit: Verify context-based logger extraction

---

## 4. Phase B: Internal Packages

### B.1 - Config Package (internal/config)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.2 |
| **Priority** | P0 |
| **Complexity** | M |

**Description**: Implement configuration management package that reads from ENV variables, flags, and config files. ENV has highest priority. Uses viper for configuration binding.

**Acceptance Criteria**:
- `OperatorConfig` struct with all operator settings
- Load from: ENV (`CLOUDBERRY_` prefix), flags, config file
- Priority: ENV > flags > config file > defaults
- Validation of required fields
- Interface-based design for testability
- Support for: operator listen address/port, metrics port, log level, leader election, health probe bind address, webhook port, vault address, OIDC settings, telemetry endpoint

**Test Cases**:
- Unit: Verify ENV override of flag values
- Unit: Verify default values applied when nothing set
- Unit: Verify validation rejects missing required fields
- Unit: Verify config file loading
- Unit: Verify `CLOUDBERRY_` prefix ENV binding
- Edge: Empty string ENV vs unset ENV
- Edge: Invalid values (negative port, invalid URL)

---

### B.2 - Utility Package (internal/util)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.5 |
| **Priority** | P0 |
| **Complexity** | M |

**Description**: Common utility functions: retry with exponential backoff, string helpers, K8s resource name builders, hash functions, pointer helpers, condition helpers.

**Acceptance Criteria**:
- `retry.go`: `RetryWithBackoff(ctx, opts, fn)` with configurable MaxRetries, InitialBackoff, MaxBackoff, Multiplier, jitter
- `strings.go`: TruncateName, SanitizeK8sName, ContainsString, RemoveString
- `hash.go`: ComputeHash for ConfigMap/Secret data change detection
- `ptr.go`: `Ptr[T]`, `Deref[T]` generic pointer helpers
- `conditions.go`: SetCondition, FindCondition, IsConditionTrue helpers for `metav1.Condition` management
- `names.go`: Resource name builders (`{cluster}-coordinator`, `{cluster}-segment-primary`, etc.)

**Test Cases**:
- Unit: RetryWithBackoff succeeds after transient failures
- Unit: RetryWithBackoff respects context cancellation
- Unit: RetryWithBackoff respects max retries
- Unit: Exponential backoff timing is correct (with tolerance)
- Unit: K8s name sanitization handles edge cases (63 char limit, special chars)
- Unit: Hash computation is deterministic
- Unit: Condition helpers correctly set/find/update conditions
- Edge: Retry with 0 max retries
- Edge: Empty string inputs to name builders

---

### B.3 - Metrics Package (internal/metrics)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.2 |
| **Priority** | P0 |
| **Complexity** | M |

**Description**: Prometheus metrics registration and helpers. Define all metrics from specification 03 (section 4.2) and 04 (section 3.4).

**Acceptance Criteria**:
- `metrics.go`: Metrics struct with all Prometheus collectors
- Interface: `MetricsRecorder` for testability (mock in tests)
- All metrics from spec registered:
  - Cluster: `cloudberry_cluster_info`, `coordinator_up`, `standby_up`
  - Segments: `segments_ready`, `segments_total`, `segments_failed`
  - Mirroring: `mirroring_in_sync`
  - Reconciliation: `reconcile_total`, `reconcile_errors_total`, `reconcile_duration_seconds`
  - Config: `config_reload_total`
  - Connections: `connections_active`, `connections_max`
  - Disk: `disk_usage_bytes`
  - FTS: `fts_probe_total`, `fts_probe_failures_total`, `fts_probe_duration_seconds`, `fts_failover_total`, `segment_status`, `replication_lag_bytes`
  - Standby: `standby_replication_lag_bytes`
- Registration with `prometheus.DefaultRegisterer` or custom registry
- Helper methods: `RecordReconcile`, `RecordFTSProbe`, `UpdateSegmentStatus`, etc.

**Test Cases**:
- Unit: All metrics registered without panic
- Unit: Counter increment works
- Unit: Histogram observation works
- Unit: Gauge set/inc/dec works
- Unit: Metric labels are correct
- Unit: Mock recorder satisfies interface

---

### B.4 - Telemetry Package (internal/telemetry)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.2, B.1 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: OpenTelemetry (OTLP) tracing setup. Configure trace provider, exporters (gRPC/HTTP), sampling, and span helpers.

**Acceptance Criteria**:
- `telemetry.go`: `InitTracer(cfg TelemetryConfig) (shutdown func, error)`
- Support gRPC and HTTP OTLP exporters
- Configurable sampling rate
- Span helpers: `StartSpan`, `SpanFromContext`, `AddSpanEvent`, `SetSpanError`
- Resource attributes: `service.name`, `service.version`, `k8s.namespace`
- Graceful shutdown of trace provider
- No-op tracer when telemetry disabled
- Interface-based for testability

**Test Cases**:
- Unit: InitTracer with gRPC protocol
- Unit: InitTracer with HTTP protocol
- Unit: No-op tracer when disabled
- Unit: Span creation and context propagation
- Unit: Graceful shutdown
- Edge: Invalid OTLP endpoint
- Edge: Sampling rate 0 (no traces) and 1.0 (all traces)

---

### B.5 - Vault Client Package (internal/vault)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.2, B.1, B.2 |
| **Priority** | P1 |
| **Complexity** | L |

**Description**: HashiCorp Vault client with support for token, kubernetes, and approle auth methods. KV v2 secret read/write. Exponential backoff retry on connection failures.

**Acceptance Criteria**:
- Interface: `VaultClient` with Read, Write, IsEnabled methods
- Support auth methods: token, kubernetes, approle
- KV v2 operations: ReadSecret, WriteSecret
- Connection retry with exponential backoff (using B.2 retry)
- TLS configuration support
- Secret path convention: `secret/data/cloudberry/{key}`
- Graceful handling when Vault is unavailable (operator continues)
- Secret rotation detection (periodic poll)
- No-op client when `vault.enabled=false`

**Test Cases**:
- Unit: Token auth login
- Unit: Kubernetes auth login (mocked)
- Unit: AppRole auth login (mocked)
- Unit: ReadSecret returns correct data
- Unit: WriteSecret stores data
- Unit: Retry on transient connection failure
- Unit: No-op client returns empty/nil gracefully
- Unit: TLS configuration applied
- Edge: Vault returns 403 (permission denied)
- Edge: Vault returns 404 (secret not found)
- Edge: Connection timeout
- Negative: Invalid auth method

---

### B.6 - Auth Package (internal/auth)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.2, A.5, B.1, B.5 |
| **Priority** | P0 |
| **Complexity** | XL |

**Description**: Authentication providers (Basic + OIDC/JWT), permission levels, middleware chain, identity model. Matches spec 05.

**Acceptance Criteria**:
- Interface: `Provider` with `Authenticate(ctx, *http.Request) (*Identity, error)`
- `Identity` struct: Username, Email, Groups, Roles, Permission, AuthMethod, TokenExpiry
- `PermissionLevel` enum: SelfOnly, Basic, OperatorBasic, Operator, Admin
- `BasicAuthProvider`: validates against K8s Secret and DB roles
- `OIDCProvider`: JWT validation, JWKS caching, claim extraction
  - OIDC discovery from issuer URL
  - JWT signature verification against JWKS
  - Issuer, audience, expiry validation
  - Role claim extraction from configurable path (nested JSON)
  - Role matching modes: exact, suffix, prefix, contains
  - Role claim source: id_token or userinfo
  - PKCE support
  - Token refresh
- `AuthMiddleware`: extracts Authorization header, routes to provider
- `PermissionMiddleware`: `RequirePermission(level)` middleware
- Security headers middleware (spec 05, section 10)
- Permission matrix enforcement (spec 05, section 5.2)

**Test Cases**:
- Unit: Basic auth with valid credentials
- Unit: Basic auth with invalid credentials
- Unit: Basic auth with missing header
- Unit: JWT validation with valid token
- Unit: JWT validation with expired token
- Unit: JWT validation with wrong issuer
- Unit: JWT validation with wrong audience
- Unit: JWT validation with invalid signature
- Unit: Role claim extraction from nested path `realm_access.roles`
- Unit: Role matching - exact mode
- Unit: Role matching - suffix mode
- Unit: Role matching - prefix mode
- Unit: Role matching - contains mode
- Unit: Role mapping to permission levels
- Unit: Permission middleware allows sufficient permission
- Unit: Permission middleware blocks insufficient permission
- Unit: Security headers are set correctly
- Unit: OIDC discovery fetches well-known config
- Unit: JWKS refresh/caching
- Edge: Both Basic and Bearer headers present
- Edge: Malformed JWT (not 3 parts)
- Edge: Empty role claim
- Edge: Role claim source=userinfo (HTTP call)
- Negative: Unsupported auth type header

---

## 5. Phase C: Cloudberry DB Client and K8s Resource Builders

### C.1 - Database Client Package (internal/db)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.2, B.1, B.2, B.3 |
| **Priority** | P0 |
| **Complexity** | XL |

**Description**: Cloudberry/PostgreSQL database client using pgx/v5. Provides connection management, query execution, and Cloudberry-specific operations (segment config, parameters, session management, maintenance).

**Acceptance Criteria**:
- Interface: `DBClient` with all operations behind interface for mocking
- Connection pool management with `pgxpool`
- Retry with exponential backoff on connection failures
- Context-based timeout/cancellation on all operations
- Operations:
  - `Ping` / health check
  - `GetSegmentConfiguration()` - query `gp_segment_configuration`
  - `GetClusterState()` - overall cluster health
  - `SetParameter(name, value, scope)` - ALTER SYSTEM/DATABASE/ROLE SET
  - `ReloadConfig()` - `SELECT pg_reload_conf()`
  - `ShowParameter(name)` - SHOW parameter
  - `ListSessions()` - `pg_stat_activity` query
  - `CancelQuery(pid)` - `pg_cancel_backend`
  - `TerminateSession(pid)` - `pg_terminate_backend`
  - `CreateRole`, `AlterRole`, `DropRole`
  - `Vacuum`, `Analyze`, `Reindex` operations
  - `GetDiskUsage`, `GetDataSkew`, `GetBloat`, `GetMissingStats`
  - `GetReplicationLag`
  - `PromoteStandby`
- Structured logging for all operations
- Metrics recording for query durations

**Test Cases**:
- Unit: All interface methods have mock implementations
- Unit: Connection pool creation with valid config
- Unit: Ping succeeds on healthy DB
- Unit: GetSegmentConfiguration parses response correctly
- Unit: SetParameter generates correct SQL
- Unit: ReloadConfig executes correct SQL
- Unit: ListSessions parses `pg_stat_activity` correctly
- Unit: CancelQuery calls `pg_cancel_backend`
- Unit: TerminateSession calls `pg_terminate_backend`
- Unit: CreateRole generates correct SQL with options
- Unit: Retry on connection failure
- Edge: Connection pool exhaustion
- Edge: Context cancellation mid-query
- Edge: SQL injection prevention (parameterized queries)
- Negative: Invalid connection string

---

### C.2 - Kubernetes Resource Builder Package (internal/builder)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3, A.5, B.2 |
| **Priority** | P0 |
| **Complexity** | XL |

**Description**: Builder functions that construct K8s resources (StatefulSets, Services, ConfigMaps, Secrets, PVCs, Jobs, NetworkPolicies) from CloudberryCluster spec. Pure functions, no K8s API calls.

**Acceptance Criteria**:
- Interface: `ResourceBuilder` for testability
- Builders (all return `*resource`, no side effects):
  - `BuildCoordinatorStatefulSet(cluster) *appsv1.StatefulSet`
  - `BuildStandbyStatefulSet(cluster) *appsv1.StatefulSet`
  - `BuildSegmentPrimaryStatefulSet(cluster) *appsv1.StatefulSet`
  - `BuildSegmentMirrorStatefulSet(cluster) *appsv1.StatefulSet`
  - `BuildCoordinatorService(cluster) *corev1.Service` (headless)
  - `BuildStandbyService(cluster) *corev1.Service`
  - `BuildSegmentService(cluster) *corev1.Service`
  - `BuildClientService(cluster) *corev1.Service` (ClusterIP for external access)
  - `BuildPostgresqlConfConfigMap(cluster) *corev1.ConfigMap`
  - `BuildPgHbaConfConfigMap(cluster) *corev1.ConfigMap`
  - `BuildAdminPasswordSecret(cluster) *corev1.Secret`
  - `BuildRecoveryJob(cluster, recoveryType, segments) *batchv1.Job`
  - `BuildMaintenanceJob(cluster, maintenanceType) *batchv1.Job`
  - `BuildNetworkPolicies(cluster) []*networkingv1.NetworkPolicy`
  - `BuildServiceMonitor(cluster)` (if `monitoring.serviceMonitor=true`)
- All resources have correct:
  - Labels (managed-by, cluster name, component)
  - Owner references (for garbage collection)
  - Resource requests/limits from spec
  - Storage class and size from spec
  - Node selectors and tolerations from spec
  - Anti-affinity rules (segments)
  - Init containers for coordinator
  - Volume mounts for config, TLS, data
- Mirroring layout algorithms:
  - Group: mirrors on adjacent host
  - Spread: mirrors distributed across hosts

**Test Cases**:
- Unit: Coordinator StatefulSet has correct replicas, image, resources
- Unit: Coordinator StatefulSet has init container
- Unit: Coordinator StatefulSet has correct volume mounts
- Unit: Standby StatefulSet created only when enabled
- Unit: Segment StatefulSet has correct count
- Unit: Mirror StatefulSet has anti-affinity with primaries
- Unit: Group mirroring layout places mirrors correctly
- Unit: Spread mirroring layout distributes mirrors correctly
- Unit: Services are headless (ClusterIP: None)
- Unit: Client service has correct port (`spec.coordinator.port`)
- Unit: ConfigMap contains rendered `postgresql.conf`
- Unit: ConfigMap contains rendered `pg_hba.conf` with default rules
- Unit: ConfigMap contains custom `pg_hba.conf` rules
- Unit: Secret contains admin password reference
- Unit: Recovery Job has correct env vars and backoff
- Unit: Maintenance Job has correct command
- Unit: NetworkPolicies allow correct traffic patterns
- Unit: Owner references set correctly
- Unit: Labels match conventions
- Unit: TLS volume mounts when SSL enabled
- Edge: Minimal cluster (no standby, no mirroring, no OIDC)
- Edge: Full production cluster (all features enabled)
- Edge: Spread mirroring with insufficient hosts (should error)

---

## 6. Phase D: Controllers

### D.1 - Cluster Controller (internal/controller/cluster_controller.go)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3, A.4, B.2, B.3, B.4, C.1, C.2 |
| **Priority** | P0 |
| **Complexity** | XL |

**Description**: Main reconciliation controller for CloudberryCluster. Manages the full lifecycle: create, update, scale, delete. Handles StatefulSets, Services, ConfigMaps, Secrets, PVCs.

**Acceptance Criteria**:
- Implements `reconcile.Reconciler` interface
- Watches: CloudberryCluster, owned StatefulSets, Services, ConfigMaps
- Reconciliation flow:
  1. Fetch CloudberryCluster CR
  2. Handle deletion (finalizer pattern)
  3. Validate spec
  4. Build desired resources (using C.2 builders)
  5. Create/Update resources (create-or-update pattern)
  6. Wait for coordinator readiness
  7. Initialize cluster if new (gpinitsystem equivalent)
  8. Create standby if enabled
  9. Create segments (primaries + mirrors)
  10. Apply configuration
  11. Update status subresource
- Status transitions: `Pending` -> `Initializing` -> `Running`
- Finalizer: `avsoft.io/finalizer`
- Deletion handling: backup-on-delete, PVC retention policy
- Annotation-based actions: start, stop, restart
- Image/version upgrade with rolling update
- Requeue on transient errors with backoff
- Emit Kubernetes events for state changes
- Prometheus metrics for reconciliation
- OTLP tracing spans for reconciliation steps
- Structured logging with reconcileID

**Test Cases**:
- Unit: Reconcile creates all resources for new cluster
- Unit: Reconcile updates StatefulSet on image change
- Unit: Reconcile handles deletion with Retain policy
- Unit: Reconcile handles deletion with Delete policy
- Unit: Reconcile handles backup-on-delete
- Unit: Reconcile sets finalizer on new CR
- Unit: Reconcile removes finalizer on deletion
- Unit: Status updated to Running when all ready
- Unit: Status updated to Failed on error
- Unit: Start annotation triggers scale-up
- Unit: Stop annotation triggers scale-down
- Unit: Restart annotation triggers stop then start
- Unit: Requeue on transient error
- Unit: Events emitted for state changes
- Unit: Metrics recorded for reconciliation
- Edge: CR deleted while initializing
- Edge: StatefulSet already exists (idempotent)
- Edge: Partial failure (coordinator up, segments down)

---

### D.2 - HA Controller (internal/controller/ha_controller.go)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3, B.2, B.3, B.4, C.1, C.2, D.1 |
| **Priority** | P0 |
| **Complexity** | XL |

**Description**: High availability controller managing FTS probing, automatic failover, mirroring status, and standby lifecycle.

**Acceptance Criteria**:
- Implements `reconcile.Reconciler` or runs as background goroutine
- **FTS Probe Loop**:
  - Configurable interval (`spec.ha.ftsProbeInterval`)
  - TCP + SQL health check per primary segment
  - Configurable timeout and retries
  - On failure: mark segment down, promote mirror
- **Automatic Failover**:
  - Detect primary segment failure
  - Promote mirror to primary
  - Update segment configuration
  - Emit `SegmentFailover` event
  - Update Prometheus metrics
  - Update CR status (`failedSegments`)
- **Mirroring Status**:
  - Monitor replication lag
  - Report InSync/Syncing/Degraded/Down
  - Update CR status (`mirroringStatus`)
- **Standby Management**:
  - Monitor standby health
  - Monitor replication lag
  - Handle activate-standby annotation
  - Handle reinitialize-standby
- **Recovery Orchestration**:
  - Handle recovery annotations (incremental, full, differential)
  - Create recovery Jobs
  - Monitor recovery progress
  - Handle rebalance annotation

**Test Cases**:
- Unit: FTS probe detects healthy segment
- Unit: FTS probe detects failed segment after retries
- Unit: Failover promotes mirror to primary
- Unit: Failover updates CR status
- Unit: Failover emits event
- Unit: Mirroring status reported correctly (InSync)
- Unit: Mirroring status reported correctly (Degraded)
- Unit: Standby health monitoring
- Unit: Standby activation flow
- Unit: Recovery Job creation (incremental)
- Unit: Recovery Job creation (full)
- Unit: Recovery Job creation (differential)
- Unit: Rebalance flow
- Unit: FTS metrics recorded
- Edge: All segments fail simultaneously
- Edge: Mirror already acting as primary (no double failover)
- Edge: Recovery while another recovery in progress

---

### D.3 - Auth Controller (internal/controller/auth_controller.go)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3, B.2, B.5, B.6, C.1, C.2, D.1 |
| **Priority** | P1 |
| **Complexity** | L |

**Description**: Authentication controller managing pg_hba.conf ConfigMap, OIDC configuration, TLS secrets, and password rotation.

**Acceptance Criteria**:
- Watches auth section of CloudberryCluster spec
- **pg_hba.conf management**:
  - Render HBA rules to ConfigMap
  - Apply default rules when none specified
  - Reload config on change (no restart)
  - Version history via ConfigMap annotations
- **OIDC configuration**:
  - Validate OIDC settings (issuer URL reachable, client ID valid)
  - Store OIDC client secret from K8s Secret or Vault
- **TLS management**:
  - Mount TLS secret into pods
  - Configure `postgresql.conf` SSL parameters
  - Validate certificate chain
- **Password management**:
  - Sync admin password from Secret/Vault to DB
  - Handle password rotation
- Update `AuthConfigured` condition

**Test Cases**:
- Unit: HBA rules rendered correctly to ConfigMap
- Unit: Default HBA rules generated when none specified
- Unit: Config reload triggered on HBA change
- Unit: OIDC settings validated
- Unit: TLS parameters set in `postgresql.conf`
- Unit: Admin password synced to DB
- Unit: `AuthConfigured` condition set to True
- Unit: `AuthConfigured` condition set to False on error
- Edge: Empty HBA rules array
- Edge: OIDC issuer unreachable (graceful degradation)
- Edge: TLS secret missing

---

### D.4 - Admin Controller (internal/controller/admin_controller.go)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3, B.2, B.3, C.1, C.2, D.1 |
| **Priority** | P1 |
| **Complexity** | L |

**Description**: Administration controller managing configuration parameters, maintenance operations, and resource groups.

**Acceptance Criteria**:
- Watches config section of CloudberryCluster spec
- **Configuration management**:
  - Detect parameter changes (hash comparison)
  - Determine if restart required vs reload-safe
  - Apply cluster-wide parameters
  - Apply coordinator-only parameters
  - Apply per-database parameters (`ALTER DATABASE SET`)
  - Apply per-role parameters (`ALTER ROLE SET`)
  - Orchestrate rolling restart for restart-required params
  - Update `ConfigApplied` condition
- **Maintenance operations**:
  - Handle maintenance annotations (vacuum, analyze, reindex)
  - Create maintenance Jobs
  - Monitor Job completion
  - Clean up completed Jobs
- Log rotation (automatic daily)

**Test Cases**:
- Unit: Parameter change detected via hash
- Unit: Reload-safe parameter applied without restart
- Unit: Restart-required parameter triggers rolling restart
- Unit: Coordinator-only parameter applied only to coordinator
- Unit: Per-database parameter generates correct `ALTER DATABASE`
- Unit: Per-role parameter generates correct `ALTER ROLE`
- Unit: Vacuum annotation creates correct Job
- Unit: Analyze annotation creates correct Job
- Unit: Reindex annotation creates correct Job
- Unit: `ConfigApplied` condition updated
- Edge: Multiple parameters changed simultaneously
- Edge: Mix of reload-safe and restart-required params
- Edge: Maintenance Job fails (backoff retry)

---

## 7. Phase E: Operator API and Webhooks

### E.1 - Operator HTTP API Server (internal/api)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | B.1, B.3, B.4, B.6, C.1, D.1 |
| **Priority** | P1 |
| **Complexity** | XL |

**Description**: REST API server exposing cluster management endpoints as defined in specification 06. Includes routing, request validation, response formatting, error handling, pagination.

**Acceptance Criteria**:
- HTTP server with TLS support
- Router with all endpoints from spec 06:
  - Cluster management: `GET/POST/PUT/DELETE /clusters`
  - Cluster operations: `POST /clusters/{name}/start|stop|restart|reload`
  - Configuration: `GET/PUT /clusters/{name}/config`
  - HA: `GET/POST /clusters/{name}/segments, mirroring, recovery, standby`
  - Sessions: `GET/POST/DELETE /clusters/{name}/sessions`
  - Maintenance: `POST /clusters/{name}/maintenance/vacuum|analyze|reindex`
  - Auth management: `GET/POST/PUT/DELETE /clusters/{name}/auth/roles`
  - Health: `GET /healthz, /readyz, /metrics`
- Auth middleware chain (Basic + OIDC)
- Permission enforcement per endpoint (spec 06 permission column)
- Request validation
- Error response format (spec 06, section 6)
- Pagination support (limit/offset)
- Rate limiting (100 req/min per user, configurable)
- OpenAPI v3 spec served at `/openapi/v3`
- Structured logging per request
- OTLP tracing per request

**Test Cases**:
- Unit: Each endpoint returns correct status code
- Unit: Auth middleware rejects unauthenticated requests
- Unit: Permission middleware enforces correct levels
- Unit: Error responses match spec format
- Unit: Pagination works correctly
- Unit: Rate limiting triggers 429
- Unit: Health endpoints return correct status
- Unit: Metrics endpoint returns Prometheus format
- Edge: Request body too large
- Edge: Invalid JSON body
- Edge: Concurrent requests to same cluster

---

### E.2 - Validating Webhook

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3, A.4 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: Admission webhook that validates CloudberryCluster CR before creation/update. Enforces validation rules from spec 02.

**Acceptance Criteria**:
- Implements `admission.CustomValidator` interface
- Validation rules:
  - `segments.count >= 1`
  - Spread mirroring requires hosts > primariesPerHost
  - OIDC enabled requires issuerURL and clientID
  - Vault enabled requires address
  - Valid parameter names in `config.parameters`
  - `deletionPolicy` is Retain or Delete
- Returns detailed validation errors
- Webhook configuration manifest generated

**Test Cases**:
- Unit: Valid minimal cluster passes
- Unit: Valid production cluster passes
- Unit: `segments.count=0` rejected
- Unit: OIDC without issuerURL rejected
- Unit: Vault without address rejected
- Unit: Invalid parameter name rejected
- Unit: Spread mirroring with insufficient hosts rejected
- Edge: Empty spec
- Edge: Update that changes immutable fields

---

### E.3 - Mutating Webhook

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3, A.4 |
| **Priority** | P1 |
| **Complexity** | S |

**Description**: Admission webhook that sets defaults on CloudberryCluster CR.

**Acceptance Criteria**:
- Implements `admission.CustomDefaulter` interface
- Sets defaults:
  - `version`: "7.7"
  - `image`: "cloudberrydb/cloudberry:7.7"
  - `imagePullPolicy`: IfNotPresent
  - `coordinator.replicas`: 1
  - `coordinator.port`: 5432
  - `segments.primariesPerHost`: 2
  - `segments.mirroring.enabled`: true
  - `segments.mirroring.layout`: group
  - `segments.antiAffinity`: preferred
  - `auth.basic.enabled`: true
  - `auth.basic.adminUser`: gpadmin
  - `ha.ftsProbeInterval`: 60
  - `ha.ftsProbeTimeout`: 20
  - `ha.ftsProbeRetries`: 5
  - `ha.checksums`: true
  - `monitoring.enabled`: true
  - `monitoring.metricsPort`: 9187
  - `deletionPolicy`: Retain

**Test Cases**:
- Unit: Minimal spec gets all defaults applied
- Unit: Explicitly set values are not overwritten
- Unit: Partial spec gets missing defaults filled

---

### E.3b - Webhook Certificate Management

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3, B.1, B.5, E.2, E.3 |
| **Priority** | P1 |
| **Complexity** | L |

**Description**: Implement webhook TLS certificate lifecycle management supporting two certificate sources: Vault PKI (preferred for production) and self-signed (fallback for development). Includes certificate issuance, storage in Kubernetes Secrets, caBundle injection into webhook configurations, and automatic rotation.

**Acceptance Criteria**:
- Interface: `CertManager` with `EnsureCertificates(ctx) error` and `NeedsRotation() bool`
- **Self-signed certificate source**:
  - Generate RSA 4096-bit CA key pair with 10-year validity
  - Generate RSA 2048-bit server certificate signed by CA with 1-year validity
  - SANs: `{serviceName}.{namespace}.svc`, `{serviceName}.{namespace}.svc.cluster.local`
  - Store CA cert, server cert, and server key in Kubernetes Secret
- **Vault PKI certificate source**:
  - Request certificate from `{mountPath}/issue/{role}` endpoint
  - Common name: `{serviceName}.{namespace}.svc`
  - Parse PEM response and store in Kubernetes Secret
- **Certificate rotation**:
  - Background goroutine checking every 12 hours
  - Rotate when 2/3 of certificate lifetime has elapsed
  - Re-issue certificate from the configured source
  - Update Kubernetes Secret with new cert/key
  - Patch webhook configurations with new `caBundle`
  - Emit `CertificateRotated` Kubernetes event
- **caBundle injection**:
  - Patch `ValidatingWebhookConfiguration` and `MutatingWebhookConfiguration`
  - Set `webhooks[].clientConfig.caBundle` to base64-encoded CA certificate
- Configuration via environment variables:
  - `CLOUDBERRY_WEBHOOK_CERT_SOURCE` (`self-signed` or `vault-pki`)
  - `CLOUDBERRY_WEBHOOK_CERT_SECRET_NAME`
  - `CLOUDBERRY_WEBHOOK_SERVICE_NAME`
  - `CLOUDBERRY_WEBHOOK_VAULT_PKI_MOUNT` (vault-pki only)
  - `CLOUDBERRY_WEBHOOK_VAULT_PKI_ROLE` (vault-pki only)

**Test Cases**:
- Unit: Self-signed CA generation produces valid CA certificate
- Unit: Self-signed server cert is signed by generated CA
- Unit: Server cert SANs match expected service DNS names
- Unit: Certificate stored in correct Kubernetes Secret
- Unit: caBundle injected into ValidatingWebhookConfiguration
- Unit: caBundle injected into MutatingWebhookConfiguration
- Unit: Rotation triggered when 2/3 lifetime elapsed
- Unit: Rotation not triggered when certificate is fresh
- Unit: Vault PKI request uses correct mount path and role
- Unit: Vault PKI response parsed correctly
- Unit: CertificateRotated event emitted after rotation
- Edge: Secret does not exist yet (create vs update)
- Edge: Vault unavailable falls back gracefully
- Edge: Certificate already expired triggers immediate rotation
- Negative: Invalid Vault PKI role returns error

---

### E.4 - Operator Main Entry Point (cmd/operator/main.go)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | B.1, B.3, B.4, B.5, B.6, D.1, D.2, D.3, D.4, E.1, E.2, E.3 |
| **Priority** | P0 |
| **Complexity** | L |

**Description**: Main function that wires everything together: manager setup, controller registration, webhook registration, metrics server, health probes, leader election.

**Acceptance Criteria**:
- Creates controller-runtime Manager
- Registers all controllers (Cluster, HA, Auth, Admin)
- Registers webhooks (validating, mutating)
- Starts API server
- Configures metrics endpoint
- Configures health/readiness probes
- Leader election support
- Graceful shutdown on SIGTERM/SIGINT
- Initializes telemetry
- Initializes Vault client (if enabled)
- Initializes OIDC provider (if enabled)
- Structured logging setup

**Test Cases**:
- Unit: Manager creation with valid config
- Unit: Controller registration
- Unit: Graceful shutdown
- Edge: Missing required config (should fail fast)

---

## 8. Phase F: cloudberry-ctl CLI

### F.1 - CLI Scaffolding and Root Command

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, A.2, B.1 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: Set up cobra/viper CLI framework with root command, global flags, config file loading, output formatting.

**Acceptance Criteria**:
- `cmd/cloudberry-ctl/main.go` entry point
- Root command with global flags from spec 07 (section 3): `--cluster`, `--namespace`, `--kubeconfig`, `--context`, `--operator-url`, `--auth-method`, `--username`, `--password`, `--output`, `--verbose`, `--timeout`
- ENV variable binding (`CLOUDBERRY_` prefix, ENV > flags > config > defaults)
- Config file loading (`~/.cloudberry-ctl.yaml`)
- Output formatters: table, json, yaml
- Version command
- Shell completion (bash, zsh, fish)
- Exit codes from spec 07 (section 9)

**Test Cases**:
- Unit: Global flags parsed correctly
- Unit: ENV overrides flag values
- Unit: Config file loaded
- Unit: Table output formatter
- Unit: JSON output formatter
- Unit: YAML output formatter
- Unit: Version command output
- Unit: Exit codes set correctly
- Edge: Missing config file (should use defaults)
- Edge: Invalid output format

---

### F.2 - CLI API Client

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1, B.6 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: HTTP client for communicating with the operator API. Handles authentication (Basic + OIDC), token caching, request/response serialization, error handling.

**Acceptance Criteria**:
- Interface: `APIClient` for testability
- HTTP client with TLS support
- Basic auth: username/password from flags/env/config
- OIDC auth: token caching in `~/.cloudberry-ctl/tokens/`
- Auto-discovery of operator URL via K8s service
- Request timeout from `--timeout` flag
- Error response parsing (spec 06 error format)
- Retry on transient errors (5xx, connection refused)

**Test Cases**:
- Unit: Basic auth header set correctly
- Unit: Bearer token header set correctly
- Unit: Token cache read/write
- Unit: Error response parsed correctly
- Unit: Retry on 503
- Unit: Timeout respected
- Edge: Expired cached token triggers re-auth
- Edge: Operator URL auto-discovery

---

### F.3 - Cluster Commands

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1, F.2 |
| **Priority** | P1 |
| **Complexity** | L |

**Description**: Implement all cluster lifecycle commands: `cluster status/start/stop/restart/create/delete/upgrade`.

**Acceptance Criteria**:
- `cluster status` - displays cluster status in table/json/yaml
- `cluster start` - with `--mode` (normal/restricted/maintenance)
- `cluster stop` - with `--mode` (smart/fast/immediate)
- `cluster restart`
- `cluster create` - from YAML file (`-f` flag)
- `cluster delete` - with `--confirm` and `--retain-data` flags
- `cluster upgrade` - with `--version` and `--image` flags
- All commands respect `--output` format
- All commands handle errors with correct exit codes

**Test Cases**:
- Unit: Status command formats output correctly (table)
- Unit: Status command formats output correctly (json)
- Unit: Start command sends correct mode
- Unit: Stop command sends correct mode
- Unit: Create command reads YAML file
- Unit: Delete command requires `--confirm`
- Unit: Error handling with correct exit codes
- Edge: Cluster not found
- Edge: Permission denied

---

### F.4 - Config Commands

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1, F.2 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: Implement configuration management commands: `config get/set/reset/reload/hba`.

**Acceptance Criteria**:
- `config get` - all params or specific `--param`
- `config set` - with `--param`, `--value`, `--coordinator-only`, `--database`, `--role`
- `config reset` - reset param to default
- `config reload` - trigger config reload
- `config hba list` - list HBA rules
- `config hba update` - from YAML file
- `config hba history` - view change history

**Test Cases**:
- Unit: Get all parameters
- Unit: Get specific parameter
- Unit: Set cluster-wide parameter
- Unit: Set coordinator-only parameter
- Unit: Set per-database parameter
- Unit: Set per-role parameter
- Unit: Reset parameter
- Unit: Reload triggers API call
- Unit: HBA list formats correctly
- Unit: HBA update reads YAML file

---

### F.5 - HA Commands

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1, F.2 |
| **Priority** | P1 |
| **Complexity** | L |

**Description**: Implement high availability commands: `ha mirroring/recovery/rebalance/standby/fts`.

**Acceptance Criteria**:
- `ha mirroring status` - show mirroring status
- `ha mirroring enable` - with `--layout`
- `ha mirroring disable`
- `ha recovery start` - with `--type`, `--target-node`, `--parallel`
- `ha recovery status`
- `ha recovery cancel`
- `ha rebalance` - with optional `--content-ids`
- `ha standby status`
- `ha standby activate` - with `--confirm`
- `ha standby reinitialize`
- `ha standby restore-roles`
- `ha fts status`
- `ha fts configure` - with `--probe-interval`, `--probe-timeout`, `--probe-retries`

**Test Cases**:
- Unit: Mirroring status displays correctly
- Unit: Recovery start sends correct type
- Unit: Rebalance with specific content IDs
- Unit: Standby activate requires `--confirm`
- Unit: FTS configure sends correct parameters
- Edge: Recovery when no failed segments

---

### F.6 - Session Commands

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1, F.2 |
| **Priority** | P1 |
| **Complexity** | S |

**Description**: Implement session management commands: `sessions list/cancel-query/terminate`.

**Acceptance Criteria**:
- `sessions list` - with `--state` and `--user` filters
- `sessions cancel-query` - with `--pid`
- `sessions terminate` - with `--pid`

**Test Cases**:
- Unit: List sessions with filters
- Unit: Cancel query by PID
- Unit: Terminate session by PID
- Edge: Non-existent PID

---

### F.7 - Maintenance Commands

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1, F.2 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: Implement maintenance commands: `maintenance vacuum/analyze/reindex/check-catalog/jobs`.

**Acceptance Criteria**:
- `maintenance vacuum` - with `--table`, `--full`, `--analyze`
- `maintenance analyze` - with `--table`
- `maintenance reindex` - with `--database`, `--table`
- `maintenance check-catalog` - with `--database`
- `maintenance jobs` - list maintenance jobs

**Test Cases**:
- Unit: Vacuum with `--full` flag
- Unit: Vacuum with `--analyze` flag
- Unit: Vacuum specific table
- Unit: Analyze specific table
- Unit: Reindex specific database
- Unit: Jobs list formats correctly

---

### F.8 - Auth Commands

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1, F.2, B.6 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: Implement authentication commands: `auth login/logout/status/rotate-password/roles`.

**Acceptance Criteria**:
- `auth login` - OIDC flow (opens browser) or `--basic`
- `auth logout` - clear cached tokens
- `auth status` - show current auth status
- `auth rotate-password` - rotate admin password
- `auth roles list`
- `auth roles create` - with `--name`, `--login`, `--password`
- `auth roles update` - with `--name`, `--valid-until`
- `auth roles delete` - with `--name`

**Test Cases**:
- Unit: Login with basic auth
- Unit: Logout clears token cache
- Unit: Status shows current user
- Unit: Roles list formats correctly
- Unit: Roles create sends correct payload
- Unit: Roles delete sends correct request

---

### F.9 - Inspect Commands

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1, F.2 |
| **Priority** | P2 |
| **Complexity** | M |

**Description**: Implement inspection commands: `inspect disk-usage/skew/bloat/missing-stats/connections/locks/logs`.

**Acceptance Criteria**:
- `inspect disk-usage` - with `--database`
- `inspect skew` - with `--table`
- `inspect bloat`
- `inspect missing-stats`
- `inspect connections`
- `inspect locks`
- `inspect logs` - with `--severity`, `--last`

**Test Cases**:
- Unit: Disk usage formats correctly
- Unit: Skew analysis for specific table
- Unit: Logs with severity filter
- Unit: Logs with time range filter

---

### F.10 - Resource Group Commands

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1, F.2 |
| **Priority** | P2 |
| **Complexity** | M |

**Description**: Implement resource group management commands: `resource-group list/create/update/delete/assign`.

**Acceptance Criteria**:
- `resource-group list`
- `resource-group create` - with `--name`, `--concurrency`, `--cpu-max-percent`, `--memory-limit`
- `resource-group update`
- `resource-group delete`
- `resource-group assign` - with `--group`, `--role`

**Test Cases**:
- Unit: Create resource group with all options
- Unit: Assign role to group
- Unit: List formats correctly

---

## 9. Phase G: Unit Tests (Comprehensive Sweep)

### G.1 - Unit Tests for API Types (api/v1alpha1)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3 |
| **Priority** | P0 |
| **Complexity** | M |

**Description**: Comprehensive unit tests for CRD types, deepcopy, serialization, and validation.

**Acceptance Criteria**:
- 90%+ statement coverage for `api/v1alpha1` package
- Tests for DeepCopy on all types
- Tests for JSON marshal/unmarshal roundtrip
- Tests for default values
- Tests for enum validation

---

### G.2 - Unit Tests for Internal Packages

| Attribute | Value |
|-----------|-------|
| **Dependencies** | B.1, B.2, B.3, B.4 |
| **Priority** | P0 |
| **Complexity** | L |

**Description**: Comprehensive unit tests for all internal utility packages (config, util, metrics, telemetry).

**Acceptance Criteria**:
- 90%+ statement coverage for each package
- All happy paths covered
- All error paths covered
- Edge cases covered
- Mocks used for external dependencies

---

### G.3 - Unit Tests for Vault and Auth Packages

| Attribute | Value |
|-----------|-------|
| **Dependencies** | B.5, B.6 |
| **Priority** | P0 |
| **Complexity** | L |

**Description**: Comprehensive unit tests for vault client and auth providers.

**Acceptance Criteria**:
- 90%+ statement coverage
- All auth flows tested with mocked HTTP servers
- JWT validation tested with test keys
- Vault operations tested with mocked Vault API
- Permission matrix fully tested

---

### G.4 - Unit Tests for DB Client and Resource Builders

| Attribute | Value |
|-----------|-------|
| **Dependencies** | C.1, C.2 |
| **Priority** | P0 |
| **Complexity** | L |

**Description**: Comprehensive unit tests for database client and K8s resource builder packages.

**Acceptance Criteria**:
- 90%+ statement coverage
- All DB operations tested with mocked pgx
- All resource builders tested with snapshot comparison
- SQL injection prevention verified

---

### G.5 - Unit Tests for Controllers

| Attribute | Value |
|-----------|-------|
| **Dependencies** | D.1, D.2, D.3, D.4 |
| **Priority** | P0 |
| **Complexity** | XL |

**Description**: Comprehensive unit tests for all controllers using envtest or fake client.

**Acceptance Criteria**:
- 90%+ statement coverage for each controller
- All reconciliation paths tested
- Status updates verified
- Event emissions verified
- Error handling verified
- Requeue behavior verified

---

### G.6 - Unit Tests for API Server and Webhooks

| Attribute | Value |
|-----------|-------|
| **Dependencies** | E.1, E.2, E.3 |
| **Priority** | P0 |
| **Complexity** | L |

**Description**: Comprehensive unit tests for REST API endpoints and admission webhooks.

**Acceptance Criteria**:
- 90%+ statement coverage
- All endpoints tested with `httptest`
- Auth middleware tested
- Webhook validation rules tested
- Webhook defaulting tested
- Error responses tested

---

### G.7 - Unit Tests for CLI Commands

| Attribute | Value |
|-----------|-------|
| **Dependencies** | F.1-F.10 |
| **Priority** | P0 |
| **Complexity** | L |

**Description**: Comprehensive unit tests for all cloudberry-ctl commands.

**Acceptance Criteria**:
- 90%+ statement coverage for CLI package
- All commands tested with mocked API client
- Output formatting tested (table, json, yaml)
- Flag parsing tested
- Error handling and exit codes tested

---

## 10. Phase H: DevOps

### H.1 - Makefile

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1 |
| **Priority** | P0 |
| **Complexity** | M |

**Description**: Comprehensive Makefile with all build, test, generate, lint, and deploy targets.

**Acceptance Criteria**:
- Targets:
  - `make build` - build operator binary
  - `make build-ctl` - build cloudberry-ctl binary
  - `make test` - run unit tests with coverage
  - `make test-coverage` - generate coverage report (fail if <90%)
  - `make lint` - run golangci-lint
  - `make generate` - run controller-gen deepcopy
  - `make manifests` - generate CRD/RBAC manifests
  - `make docker-build` - build Docker image
  - `make docker-push` - push Docker image
  - `make helm-install` - install Helm chart
  - `make helm-uninstall` - uninstall Helm chart
  - `make deploy` - deploy to local K8s (cloudberry-test namespace)
  - `make undeploy` - remove from local K8s
  - `make fmt` - format code
  - `make vet` - run go vet
  - `make clean` - clean build artifacts
  - `make integration-test` - run integration tests
  - `make e2e-test` - run e2e tests
  - `make all` - lint + test + build
- Variables: `IMG`, `VERSION`, `NAMESPACE` configurable
- Help target with descriptions

---

### H.2 - Dockerfile (Multi-stage)

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.1, H.1 |
| **Priority** | P0 |
| **Complexity** | S |

**Description**: Multi-stage Dockerfile for operator and CLI binaries. Minimal final image, non-root user, security best practices.

**Acceptance Criteria**:
- Multi-stage build (Go builder + minimal runtime)
- Non-root user
- Binary at `/usr/local/bin/cloudberry-operator`
- Health check endpoint configured
- Labels: version, maintainer, description
- `.dockerignore` file
- Image size < 100MB

---

### H.3 - Helm Chart

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.4, H.2 |
| **Priority** | P0 |
| **Complexity** | L |

**Description**: Helm chart for deploying the operator to Kubernetes.

**Acceptance Criteria**:
- Chart structure: `deploy/helm/cloudberry-operator/`
- Templates: deployment, service, serviceaccount, clusterrole, clusterrolebinding, CRDs, webhook-configuration, configmap, secret, `_helpers.tpl`, NOTES.txt
- Configurable values: image, replicas, resources, nodeSelector, tolerations, affinity, serviceAccount, metrics, telemetry, vault, OIDC, log level
- RBAC: minimum required permissions
- Leader election support
- Namespace: `cloudberry-system` (configurable)

---

### H.4 - GitHub Actions CI/CD Pipeline

| Attribute | Value |
|-----------|-------|
| **Dependencies** | H.1, H.2, H.3 |
| **Priority** | P1 |
| **Complexity** | L |

**Description**: CI/CD pipeline for linting, testing, building, and publishing.

**Acceptance Criteria**:
- `.github/workflows/ci.yml`: lint, test, coverage check (90%), build, Docker build, Helm lint
- `.github/workflows/release.yml`: Docker push, Helm push, GitHub release
- `.github/workflows/integration.yml`: kind cluster, deploy, integration tests, e2e tests
- Caching: Go modules, Docker layers
- Matrix: Go versions (1.23, 1.24)

---

### H.5 - Local Development Environment Setup

| Attribute | Value |
|-----------|-------|
| **Dependencies** | H.1, H.3 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: Scripts and configuration for local development with kind/minikube.

**Acceptance Criteria**:
- `hack/setup-local.sh`: Create kind cluster, install operator
- `hack/teardown-local.sh`: Clean up
- `hack/setup-test-infra.sh`: Deploy Vault + Keycloak to K8s
- `hack/create-test-cluster.sh`: Create sample CloudberryCluster
- Integration with existing `test/docker-compose/`

---

## 11. Phase I: Integration and E2E Tests

### I.1 - Integration Test Framework Setup

| Attribute | Value |
|-----------|-------|
| **Dependencies** | H.1, D.1-D.4 |
| **Priority** | P0 |
| **Complexity** | M |

**Description**: Set up integration test framework using envtest (controller-runtime test environment).

**Acceptance Criteria**:
- `test/integration/` directory structure
- envtest suite setup (`TestMain` with `envtest.Environment`)
- CRD installation in test environment
- Helper functions for creating test clusters
- Cleanup between tests
- Timeout handling
- Parallel test support

---

### I.2 - Integration Tests: Cluster Lifecycle

| Attribute | Value |
|-----------|-------|
| **Dependencies** | I.1, D.1 |
| **Priority** | P0 |
| **Complexity** | L |

**Description**: Integration tests for full cluster lifecycle using envtest.

**Test Cases**:
- Create minimal cluster and verify all resources created
- Create cluster with standby and verify standby StatefulSet
- Create cluster with mirroring and verify mirror StatefulSet
- Update image and verify rolling update
- Delete cluster and verify PVC retention
- Stop cluster and verify StatefulSets scaled to 0
- Start cluster and verify StatefulSets scaled up

---

### I.3 - Integration Tests: Configuration Management

| Attribute | Value |
|-----------|-------|
| **Dependencies** | I.1, D.4 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: Integration tests for configuration parameter management.

**Test Cases**:
- Change `max_connections` and verify ConfigMap updated
- Change `work_mem` (reload-safe) and verify no restart
- Change `shared_buffers` (restart-required) and verify restart
- Update HBA rules and verify ConfigMap

---

### I.4 - Integration Tests: Authentication

| Attribute | Value |
|-----------|-------|
| **Dependencies** | I.1, B.6, E.1 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: Integration tests for auth middleware with mocked Keycloak/OIDC provider.

**Test Cases**:
- Basic auth with valid credentials -> 200
- Basic auth with invalid credentials -> 401
- JWT with valid token -> 200
- JWT with expired token -> 401
- Insufficient permission -> 403
- No auth header -> 401

---

### I.5 - Integration Tests: Vault

| Attribute | Value |
|-----------|-------|
| **Dependencies** | I.1, B.5 |
| **Priority** | P1 |
| **Complexity** | M |

**Description**: Integration tests for Vault client using test Vault server.

**Test Cases**:
- Connect to test Vault and read secret
- Write secret and read back
- Vault unavailable -> operator continues
- Vault returns after outage -> reconnect

---

### I.6 - E2E Test Framework Setup

| Attribute | Value |
|-----------|-------|
| **Dependencies** | H.1, H.3, H.5 |
| **Priority** | P1 |
| **Complexity** | L |

**Description**: Set up end-to-end test framework using kind cluster with full operator deployment.

**Acceptance Criteria**:
- `test/e2e/` directory structure
- kind cluster creation/teardown
- Operator deployment via Helm
- Test infrastructure deployment (Vault, Keycloak)
- CloudberryCluster CR creation helpers
- Wait-for-condition helpers
- Cleanup between tests

---

### I.7 - E2E Tests: Full Cluster Lifecycle

| Attribute | Value |
|-----------|-------|
| **Dependencies** | I.6 |
| **Priority** | P1 |
| **Complexity** | XL |

**Description**: End-to-end tests for complete cluster lifecycle in real K8s.

**Test Cases**:
- Create minimal cluster -> all pods running
- Connect to coordinator via client service
- Apply config change -> verify parameter updated
- Delete cluster -> all resources cleaned up
- `cloudberry-ctl cluster status` returns correct info
- `cloudberry-ctl config get` returns parameters

---

### I.8 - E2E Tests: HA and Recovery

| Attribute | Value |
|-----------|-------|
| **Dependencies** | I.6, I.7 |
| **Priority** | P2 |
| **Complexity** | XL |

**Description**: End-to-end tests for high availability features.

**Test Cases**:
- Create cluster with mirroring -> mirrors in sync
- Kill primary segment pod -> mirror promoted
- Run incremental recovery -> segment recovered
- Rebalance -> original roles restored
- Create cluster with standby -> standby synced
- Activate standby -> new coordinator serving

---

### I.8b - Scenario 19: Enable/Disable Mirroring Tests

| Attribute | Value |
|-----------|-------|
| **Dependencies** | I.6, I.7, D.1, D.2 |
| **Priority** | P1 |
| **Complexity** | XL |

**Description**: Functional and E2E tests for enabling and disabling mirroring on existing clusters (Scenario 19). Covers the full mirroring lifecycle including state machine transitions, webhook validation, timeout handling, and metrics recording.

**Test Cases — Enable Mirroring**:
- Enable mirroring on Running cluster with group layout → phase transitions through `Updating` → `Running`, mirroring status through `Initializing` → `Syncing` → `InSync`
- Enable mirroring on Running cluster with spread layout → mirrors distributed correctly
- Enable mirroring creates mirror StatefulSet `{cluster}-segment-mirror` with correct replica count
- Enable mirroring sets `avsoft.io/mirroring-state` annotation with `creating-sts` phase
- Mirror STS readiness advances state to `initializing` phase
- DB client `InitializeMirrors()` called with correct layout and segment count
- DB client `ConfigureReplication()` called with sync mode
- `GetMirrorSyncStatus()` polled during syncing phase; `SetReplicationLag` metric updated per segment
- On completion: `MirroringInSync` event emitted, `cloudberry_mirroring_operations_total{operation="enable"}` incremented
- On completion: `avsoft.io/mirroring-state` annotation removed
- `MirroringHealthy` condition transitions: `False/MirroringInitializing` → `True/MirroringInSync`

**Test Cases — Enable Mirroring Validation**:
- Enable mirroring on non-Running cluster → blocked with `MirroringFailed` event
- Enable mirroring with insufficient segments for group layout → blocked with warning event
- Enable mirroring with insufficient segments for spread layout → blocked with warning event
- Webhook rejects enable on non-Running cluster with descriptive error
- Webhook rejects enable with insufficient segment count
- Webhook warns on spread layout with marginal segment count

**Test Cases — Enable Mirroring Timeout**:
- Mirroring enable exceeding 30-minute timeout → `mirroringStatus: Degraded`
- Timeout sets `MirroringHealthy` condition to `False/MirroringTimeout`
- Timeout emits `MirroringFailed` event with timeout message
- Timeout removes `avsoft.io/mirroring-state` annotation

**Test Cases — Disable Mirroring**:
- Disable mirroring on Running cluster → mirror StatefulSet deleted, `mirroringStatus: NotConfigured`
- Disable mirroring with `deletionPolicy: Delete` → mirror PVCs cleaned up
- Disable mirroring with `deletionPolicy: Retain` → mirror PVCs preserved
- Disable mirroring emits `MirroringDisabled` events (warning on initiation, normal on completion)
- Disable mirroring records `cloudberry_mirroring_operations_total{operation="disable"}`
- Disable mirroring on non-Running cluster → blocked with `MirroringFailed` event
- `MirroringHealthy` condition set to `False/MirroringDisabled`

**Test Cases — Webhook Validation**:
- Layout change while mirroring enabled → rejected ("disable mirroring first")
- Layout change while mirroring disabled → allowed
- Enable with group layout and `count >= 2 * primariesPerHost` → allowed
- Enable with spread layout and `count > primariesPerHost` → allowed
- Enable with spread layout and `count <= primariesPerHost` → rejected

**Test Cases — Edge Cases**:
- Enable mirroring without dbFactory → DB-level init skipped, STS readiness used as sync indicator
- Disable mirroring when mirror StatefulSet does not exist → no error (idempotent)
- `checkMirroringProgress` called without state annotation → requeue with default interval
- Unknown mirroring phase in state annotation → auto-completes
- Re-enable mirroring after previous disable → full cycle works correctly

---

### I.8c - Scenario 20: Automatic Segment Failover via FTS Tests

| Attribute | Value |
|-----------|-------|
| **Dependencies** | I.6, I.7, D.2 |
| **Priority** | P1 |
| **Complexity** | XL |

**Description**: Functional and E2E tests for automatic segment failover via the FTS probe mechanism (Scenario 20). Covers the full detection → failover → verification flow, retry logic, metrics recording, event emission, and edge cases.

**Test Cases — Detection Phase (probeSegmentConfigWithRetries)**:
- FTS probe succeeds on first attempt → segments returned, no failure metric incremented
- FTS probe fails once then succeeds on retry → `fts_probe_failures_total` incremented once, success logged with attempt number
- FTS probe fails all `ftsProbeRetries` attempts → error returned with "after N retries" message, `fts_probe_failures_total` incremented N times
- Each retry attempt uses a fresh context with `ftsProbeTimeout` deadline (default 20s)
- Custom `ftsProbeTimeout` from HASpec is respected (e.g., 10s)
- Custom `ftsProbeRetries` from HASpec is respected (e.g., 3)
- Default timeout (20s) used when HASpec.FTSProbeTimeout is 0 or unset
- Default retries (5) used when HASpec.FTSProbeRetries is 0 or unset

**Test Cases — Segment Analysis (analyzeSegments)**:
- All segments healthy (status="u") → `allHealthy=true`, no failed segments
- One primary down (role="p", status!="u") → `failedPrimaries` contains that segment, `allHealthy=false`
- Multiple primaries down → all appear in `failedPrimaries`
- Mirror down but primary healthy → `failedSegments` populated but `failedPrimaries` empty (no failover triggered)
- Coordinator entries (contentID < 0) are skipped
- Per-segment `cloudberry_segment_status` metric set for each non-coordinator segment

**Test Cases — Failover Flow (handleFailover)**:
- Single failed primary with successful mirror promotion → `SegmentFailover` event emitted with "failover completed" message, `cloudberry_fts_failover_total` incremented
- Multiple failed primaries → `SegmentFailover` event emitted per failed primary, `cloudberry_fts_failover_total` incremented once (not per segment)
- Mirror promoted successfully → event includes original and new primary hostnames
- Mirror not promoted (pending) → event includes "mirror promotion pending" message
- `TriggerFTSProbe()` calls `gp_request_fts_probe_scan()` on coordinator
- `TriggerFTSProbe()` failure → failover continues, events still emitted for detected failures, `RecordFTSFailover` still called
- Post-failover `GetSegmentConfiguration()` failure → events emitted for originally detected failures, `RecordFTSFailover` called, error returned
- `cloudberry_segment_status` set to 0 for each failed primary during failover

**Test Cases — Status Update (updateFTSProbeStatus)**:
- All healthy → `mirroringStatus: InSync`, `cloudberry_mirroring_in_sync` set to true
- Segments failed → `mirroringStatus: Degraded`, `cloudberry_mirroring_in_sync` set to false, `cloudberry_segments_failed` set to count, `MirroringDegraded` event emitted

**Test Cases — Integration (runFTSProbe → handleFailover)**:
- Full flow: probe detects failure → `analyzeSegments` identifies failed primary → `handleFailover` triggers FTS scan → re-read verifies promotion → events and metrics recorded → status patched
- Mirroring disabled → `handleFailover` not called even if primaries are down
- All segments healthy → no failover triggered, status set to `InSync`
- `dbFactory` is nil → probe fails immediately with failure metric

**Test Cases — Edge Cases**:
- Empty `failedPrimaries` list passed to `handleFailover` → `TriggerFTSProbe` still called, re-read still performed (no short-circuit)
- Failover when cluster is not in Running phase → FTS probe skipped entirely (HA controller guard)
- Concurrent FTS probe cycles → handled by controller-runtime's single-threaded reconciliation per resource
- `AnnotationFailoverState` (`avsoft.io/failover-state`) set during failover lifecycle

---

### I.9 - E2E Tests: Authentication and Authorization

| Attribute | Value |
|-----------|-------|
| **Dependencies** | I.6, I.7 |
| **Priority** | P2 |
| **Complexity** | L |

**Description**: End-to-end tests for auth with real Keycloak.

**Test Cases**:
- Get Keycloak token -> access operator API
- Admin token can delete cluster
- Basic token cannot delete cluster
- `cloudberry-ctl auth login` with basic
- `cloudberry-ctl auth status` shows user info

---

## 12. Phase J: Performance Tests

### J.1 - Reconciliation Performance Benchmarks

| Attribute | Value |
|-----------|-------|
| **Dependencies** | D.1, G.5 |
| **Priority** | P2 |
| **Complexity** | M |

**Description**: Benchmark tests for reconciliation loop performance.

**Test Cases**:
- `BenchmarkReconcileMinimalCluster`
- `BenchmarkReconcileLargeCluster` (100 segments)
- `BenchmarkBuildCoordinatorStatefulSet`
- `BenchmarkBuildSegmentStatefulSets`
- `BenchmarkStatusUpdate`

---

### J.2 - API Server Load Tests

| Attribute | Value |
|-----------|-------|
| **Dependencies** | E.1, I.6 |
| **Priority** | P2 |
| **Complexity** | M |

**Description**: Load tests for the operator REST API.

**Test Cases**:
- 100 concurrent `GET /clusters/{name}/status`
- 50 concurrent `POST /clusters/{name}/config`
- Rate limiting kicks in at configured threshold
- Memory usage stable over 1000 requests

---

### J.3 - FTS Probe Performance Tests

| Attribute | Value |
|-----------|-------|
| **Dependencies** | D.2 |
| **Priority** | P2 |
| **Complexity** | S |

**Description**: Performance tests for FTS probing at scale.

**Test Cases**:
- `BenchmarkFTSProbe1Segment`
- `BenchmarkFTSProbe10Segments`
- `BenchmarkFTSProbe100Segments`

---

## 13. Phase K: Documentation

### K.1 - API Reference Documentation

| Attribute | Value |
|-----------|-------|
| **Dependencies** | A.3, E.1 |
| **Priority** | P2 |
| **Complexity** | M |

**Description**: Auto-generated API reference from Go types and OpenAPI spec.

---

### K.2 - User Guide

| Attribute | Value |
|-----------|-------|
| **Dependencies** | All phases |
| **Priority** | P2 |
| **Complexity** | L |

**Description**: User-facing documentation for deploying and operating Cloudberry clusters.

---

### K.3 - Developer Guide

| Attribute | Value |
|-----------|-------|
| **Dependencies** | All phases |
| **Priority** | P2 |
| **Complexity** | M |

**Description**: Developer documentation for contributing to the project.

---

## 14. Prioritized Execution Order

### Sprint 1: Foundation (P0)
| # | Task | Description |
|---|------|-------------|
| 1 | A.1 | Initialize Go Module and Project Structure |
| 2 | A.2 | Add Core Dependencies |
| 3 | A.5 | Define Shared Constants and Error Types |
| 4 | A.6 | Set Up Structured Logging |
| 5 | A.3 | Define CRD Go Types |
| 6 | A.4 | Generate CRD Manifests and DeepCopy |
| 7 | H.1 | Makefile |

### Sprint 2: Internal Packages (P0)
| # | Task | Description |
|---|------|-------------|
| 8 | B.1 | Config Package |
| 9 | B.2 | Utility Package |
| 10 | B.3 | Metrics Package |
| 11 | B.4 | Telemetry Package |

### Sprint 3: Auth + Vault + DB (P0/P1)
| # | Task | Description |
|---|------|-------------|
| 12 | B.5 | Vault Client Package |
| 13 | B.6 | Auth Package |
| 14 | C.1 | Database Client Package |

### Sprint 4: Builders + Controllers (P0)
| # | Task | Description |
|---|------|-------------|
| 15 | C.2 | Kubernetes Resource Builder Package |
| 16 | D.1 | Cluster Controller |
| 17 | D.2 | HA Controller |

### Sprint 5: Controllers + Webhooks (P0/P1)
| # | Task | Description |
|---|------|-------------|
| 18 | D.3 | Auth Controller |
| 19 | D.4 | Admin Controller |
| 20 | E.2 | Validating Webhook |
| 21 | E.3 | Mutating Webhook |
| 21b | E.3b | Webhook Certificate Management |

### Sprint 6: API + Operator Main (P0/P1)
| # | Task | Description |
|---|------|-------------|
| 22 | E.1 | Operator HTTP API Server |
| 23 | E.4 | Operator Main Entry Point |
| 24 | H.2 | Dockerfile |

### Sprint 7: CLI (P1)
| # | Task | Description |
|---|------|-------------|
| 25 | F.1 | CLI Scaffolding and Root Command |
| 26 | F.2 | CLI API Client |
| 27 | F.3 | Cluster Commands |
| 28 | F.4 | Config Commands |
| 29 | F.5 | HA Commands |

### Sprint 8: CLI + DevOps (P1)
| # | Task | Description |
|---|------|-------------|
| 30 | F.6 | Session Commands |
| 31 | F.7 | Maintenance Commands |
| 32 | F.8 | Auth Commands |
| 33 | H.3 | Helm Chart |
| 34 | H.5 | Local Development Environment Setup |

### Sprint 9: Unit Test Sweep (P0)
| # | Task | Description |
|---|------|-------------|
| 35 | G.1 | Unit Tests for API Types |
| 36 | G.2 | Unit Tests for Internal Packages |
| 37 | G.3 | Unit Tests for Vault and Auth |
| 38 | G.4 | Unit Tests for DB Client and Resource Builders |
| 39 | G.5 | Unit Tests for Controllers |
| 40 | G.6 | Unit Tests for API Server and Webhooks |
| 41 | G.7 | Unit Tests for CLI Commands |

### Sprint 10: Integration Tests (P0/P1)
| # | Task | Description |
|---|------|-------------|
| 42 | I.1 | Integration Test Framework Setup |
| 43 | I.2 | Integration Tests: Cluster Lifecycle |
| 44 | I.3 | Integration Tests: Configuration Management |
| 45 | I.4 | Integration Tests: Authentication |
| 46 | I.5 | Integration Tests: Vault |

### Sprint 11: E2E + CI/CD (P1)
| # | Task | Description |
|---|------|-------------|
| 47 | H.4 | GitHub Actions CI/CD Pipeline |
| 48 | I.6 | E2E Test Framework Setup |
| 49 | I.7 | E2E Tests: Full Cluster Lifecycle |

### Sprint 12: Polish (P2)
| # | Task | Description |
|---|------|-------------|
| 50 | F.9 | Inspect Commands |
| 51 | F.10 | Resource Group Commands |
| 52 | I.8 | E2E Tests: HA and Recovery |
| 52b | I.8b | Scenario 19: Enable/Disable Mirroring Tests |
| 52c | I.8c | Scenario 20: Automatic Segment Failover via FTS Tests |
| 53 | I.9 | E2E Tests: Authentication and Authorization |
| 54 | J.1 | Reconciliation Performance Benchmarks |
| 55 | J.2 | API Server Load Tests |
| 56 | J.3 | FTS Probe Performance Tests |
| 57 | K.1 | API Reference Documentation |
| 58 | K.2 | User Guide |
| 59 | K.3 | Developer Guide |

---

## 15. Risk Assessment

### High Risk
| Risk | Impact | Mitigation |
|------|--------|------------|
| Cloudberry DB container image availability | Blocks integration/e2e tests | Mock DB for unit tests, use official image for integration |
| Controller complexity (many states/transitions) | Bugs, hard to test | State machine pattern, extensive unit tests |
| OIDC integration complexity | Auth failures | Use well-tested go-oidc library, mock Keycloak for tests |

### Medium Risk
| Risk | Impact | Mitigation |
|------|--------|------------|
| Mirroring layout algorithms | Incorrect placement | Pure functions, extensive unit tests with known inputs |
| FTS probe reliability | False positives/negatives | Configurable thresholds, conservative defaults |
| Vault integration | Connection issues | Interface-based design, no-op fallback |

### Low Risk
| Risk | Impact | Mitigation |
|------|--------|------------|
| CLI implementation | Minor | Well-understood cobra/viper patterns |
| Helm chart | Minor | Standard K8s deployment patterns |
| CI/CD pipeline | Minor | Standard GitHub Actions patterns |

---

## 16. Effort Estimation

| Phase | Tasks | Complexity | Estimated Days |
|-------|-------|------------|----------------|
| A - Scaffolding | 6 | M-L | 5-7 |
| B - Internal Packages | 6 | XL | 10-14 |
| C - DB Client + Builders | 2 | XXL | 8-12 |
| D - Controllers | 4 | XXL | 12-16 |
| E - API + Webhooks | 5 | XL | 10-13 |
| F - CLI | 10 | XL | 10-14 |
| G - Unit Tests | 7 | XXL | 10-14 |
| H - DevOps | 5 | L | 6-8 |
| I - Integration + E2E | 10 | XXL | 16-22 |
| J - Performance | 3 | M | 3-5 |
| K - Documentation | 3 | L | 5-7 |
| **TOTAL** | **61** | | **95-132 days** |

> **Note**: Unit tests (Phase G) are listed as a separate phase for the final coverage sweep, but tests should be written alongside implementation in each phase. The G phase represents the gap analysis and additional tests needed to reach 90%+ coverage.
