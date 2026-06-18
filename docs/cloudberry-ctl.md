# cloudberry-ctl CLI Reference

`cloudberry-ctl` is a command-line utility that provides imperative access to Cloudberry cluster management operations through the Cloudberry Operator REST API.

All commands communicate with the operator via HTTP calls to the REST API server (default port `:8090`). The CLI uses the `internal/ctl.OperatorClient` package, which supports basic and OIDC authentication, configurable timeouts, and automatic redirect protection.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Global Flags](#global-flags)
- [Environment Variables](#environment-variables)
- [Verbose Mode](#verbose-mode)
- [Command Reference](#command-reference)
  - [version](#version)
  - [cluster](#cluster) (including [scale-status](#cluster-scale-status))
  - [config](#config)
  - [segments](#segments)
  - [ha](#ha)
  - [sessions](#sessions)
  - [queries](#queries) (including [list](#queries-list), [detail](#queries-detail), [cancel](#queries-cancel), [move](#queries-move), [export](#queries-export), [history](#queries-history), [plan-check](#queries-plan-check))
  - [metrics](#metrics) (including [exporters](#metrics-exporters))
  - [maintenance](#maintenance)
  - [auth](#auth)
  - [inspect](#inspect)
  - [resource-group](#resource-group)
  - [pxf](#pxf) (including [status](#pxf-status), [restart](#pxf-restart), [sync](#pxf-sync))
  - [migrate](#migrate)
- [Output Formats](#output-formats)
- [Exit Codes](#exit-codes)
- [Shell Completion](#shell-completion)

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/cloudberry-contrib/cloudberry-k8s.git
cd cloudberry-k8s

# Build the binary
make build-ctl

# The binary is at ./bin/cloudberry-ctl
./bin/cloudberry-ctl version
```

### Docker

```bash
# Build the Docker image
make docker-build-ctl

# Run via Docker
docker run --rm cloudberry-ctl:latest version
```

### Move to PATH

```bash
sudo cp bin/cloudberry-ctl /usr/local/bin/
cloudberry-ctl version
```

## Configuration

### Configuration File

`cloudberry-ctl` reads configuration from `~/.cloudberry-ctl.yaml`:

```yaml
# Default cluster and namespace
defaultCluster: my-cluster
defaultNamespace: cloudberry-test
defaultOutput: table

# Per-cluster configuration
clusters:
  my-cluster:
    namespace: cloudberry-test
    auth:
      method: oidc
      issuer: https://keycloak.auth-system/realms/cloudberry
      clientID: cloudberry-ctl
  dev-cluster:
    namespace: cloudberry-dev
    auth:
      method: basic
      username: admin
```

### Configuration Priority

Configuration values are resolved in this order (highest priority first):

1. **Command-line flags** (`--cluster my-cluster`)
2. **Environment variables** (`CLOUDBERRY_CLUSTER=my-cluster`)
3. **Configuration file** (`~/.cloudberry-ctl.yaml`)
4. **Default values**

This priority order is enforced consistently across all settings. For example, if `CLOUDBERRY_NAMESPACE=production` is set as an environment variable but you pass `--namespace staging` on the command line, the CLI uses `staging`.

## Global Flags

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--cluster` | | Target cluster name | (required for most commands) |
| `--namespace` | | Kubernetes namespace | `cloudberry-test` |
| `--kubeconfig` | | Path to kubeconfig | `~/.kube/config` |
| `--context` | | Kubernetes context | (current context) |
| `--operator-url` | | Operator API URL | `http://localhost:8090` (auto-discover) |
| `--auth-method` | | Auth method (`basic` or `oidc`) | `basic` |
| `--username` | | Basic auth username | |
| `--password` | | Basic auth password (see security note below) | (prompted if not set) |
| `--output` | `-o` | Output format (`table`, `json`, `yaml`) | `table` |
| `--verbose` | `-v` | Enable verbose output (logs HTTP requests and responses) | `false` |
| `--timeout` | | Operation timeout | `5m` |

> **Security warning**: Avoid using the `--password` flag on the command line, as the password may be visible in shell history and process listings. Use the `CLOUDBERRY_PASSWORD` environment variable instead:
>
> ```bash
> export CLOUDBERRY_PASSWORD='your-secure-password'
> cloudberry-ctl cluster status --cluster my-cluster
> ```

## Signal Handling

`cloudberry-ctl` handles `SIGINT` (Ctrl+C) and `SIGTERM` signals gracefully. When a signal is received, the CLI cancels the current operation's context, allowing in-flight HTTP requests to be terminated cleanly. This prevents the CLI from hanging when interrupted during long-running operations.

```bash
# Ctrl+C cancels the current operation
cloudberry-ctl ha rebalance --cluster my-cluster
# Press Ctrl+C to cancel
# Output: "operation canceled"
```

## Environment Variables

All flags can be set via environment variables with the `CLOUDBERRY_` prefix:

| Variable | Corresponding Flag |
|----------|-------------------|
| `CLOUDBERRY_CLUSTER` | `--cluster` |
| `CLOUDBERRY_NAMESPACE` | `--namespace` |
| `CLOUDBERRY_OPERATOR_URL` | `--operator-url` |
| `CLOUDBERRY_AUTH_METHOD` | `--auth-method` |
| `CLOUDBERRY_USERNAME` | `--username` |
| `CLOUDBERRY_PASSWORD` | `--password` |
| `CLOUDBERRY_TIMEOUT` | `--timeout` |
| `CLOUDBERRY_OUTPUT` | `--output` |

## Verbose Mode

The `--verbose` (`-v`) flag enables debug logging of HTTP requests and responses. When enabled, the CLI logs the HTTP method, URL, status code, and response body for each API call. This is useful for debugging connectivity issues, authentication failures, and unexpected API responses.

```bash
# Enable verbose output for a single command
cloudberry-ctl cluster status --cluster my-cluster --verbose

# Short form
cloudberry-ctl cluster status --cluster my-cluster -v
```

**Example verbose output:**

```
GET http://localhost:8090/api/v1alpha1/clusters/my-cluster/status
HTTP 200 OK
{"name":"my-cluster","namespace":"cloudberry-test","status":{"phase":"Running",...}}
```

The verbose flag is wired through to the `OperatorClient`, which performs the actual HTTP calls. All request/response details are logged via `slog` at debug level.

## Implementation Status

The following commands are fully implemented and communicate with the operator REST API:

- `version`
- `cluster status`, `cluster start`, `cluster stop`, `cluster restart`, `cluster create`, `cluster delete`, `cluster scale-status`
- `config get`, `config set`
- `segments list`, `segments status`, `segments inspect`
- `ha mirroring status`, `ha recovery start`, `ha recovery status`, `ha rebalance` (with `--status` and `--tables` flags), `ha standby status`, `ha standby activate`
- `sessions list`, `sessions cancel-query`, `sessions terminate`
- `queries list`, `queries detail`, `queries cancel`, `queries move`, `queries export`
- `queries history list`, `queries history detail`, `queries history export`, `queries history --export csv`
- `metrics exporters`
- `maintenance vacuum`, `maintenance analyze`, `maintenance reindex`
- `inspect disk-usage`, `inspect skew`, `inspect bloat`, `inspect missing-stats`, `inspect connections`, `inspect locks`
- `resource-group list`, `resource-group create`, `resource-group delete`, `resource-group assign`
- `pxf status`, `pxf restart`, `pxf sync` (operator-driven PXF data-loading lifecycle — see [pxf](#pxf))

- `auth login` (basic, OIDC with credentials, and the browser-based OIDC Authorization Code flow with PKCE), `auth status`, `auth logout`

All other commands are **stub commands** that return a `"command %q is not yet implemented"` error with a non-zero exit code. This includes commands such as `cluster upgrade`, `config reset`, `config hba *`, `ha mirroring enable/disable`, `ha recovery cancel`, `ha standby reinitialize/restore-roles`, `ha fts *`, `auth rotate-password`, `auth roles *`, `resource-group update`, `inspect logs`, and `maintenance check-catalog/jobs`. Note also that `ha recovery start` succeeds at the API level but the operator currently acknowledges segment recovery as a no-op — see [ha recovery start](#ha-recovery-start).

> **Note**: Stub commands use the `notImplemented()` helper to return a consistent error message. They exit with code `1` (general error). This behavior is intentional — it prevents silent no-ops in automation scripts.

## Command Reference

### version

Show version information.

```bash
cloudberry-ctl version
```

**Output:**

```
cloudberry-ctl version v0.1.0 (commit: abc1234, built: 2026-05-13T10:00:00Z)
```

Version information is injected at build time via ldflags. When built with `make build-ctl`, the version, commit hash, and build date are embedded in the binary.

---

### cluster

Cluster lifecycle management commands.

#### cluster status

Show cluster status.

```bash
cloudberry-ctl cluster status --cluster my-cluster
```

**Output (table):**

```
CLUSTER      PHASE    VERSION  COORDINATOR  STANDBY  SEGMENTS  MIRRORING
my-cluster   Running  2.1.0    Ready        Ready    4/4       InSync
```

#### cluster start

Start a cluster. The start mode determines which components are brought up and the resulting cluster phase.

```bash
# Normal start — all components (coordinator, standby, primaries, mirrors)
# Resulting phase: Running
cloudberry-ctl cluster start --cluster my-cluster

# Restricted mode — coordinator only, superuser connections only
# Resulting phase: Restricted
cloudberry-ctl cluster start --cluster my-cluster --mode restricted

# Maintenance mode — coordinator only, utility mode
# Resulting phase: Maintenance
cloudberry-ctl cluster start --cluster my-cluster --mode maintenance
```

| Mode | Annotation Value | Components Started | Resulting Phase |
|------|-----------------|-------------------|-----------------|
| normal (default) | `start` | All | `Running` |
| restricted | `start-restricted` | Coordinator only | `Restricted` |
| maintenance | `start-maintenance` | Coordinator only | `Maintenance` |

#### cluster stop

Stop a cluster. The stop mode determines how active connections are handled.

```bash
# Smart stop — wait for clients to disconnect (default)
# Annotation: avsoft.io/action=stop
cloudberry-ctl cluster stop --cluster my-cluster

# Fast stop — rollback active transactions, disconnect clients
# Annotation: avsoft.io/action=stop-fast
cloudberry-ctl cluster stop --cluster my-cluster --mode fast

# Immediate stop — abort all connections immediately
# Annotation: avsoft.io/action=stop-immediate
cloudberry-ctl cluster stop --cluster my-cluster --mode immediate
```

| Mode | Annotation Value | Behavior |
|------|-----------------|----------|
| smart (default) | `stop` | Wait for clients to disconnect |
| fast | `stop-fast` | Rollback active transactions, disconnect clients |
| immediate | `stop-immediate` | Abort all connections immediately |

Scale-down order: mirrors → primaries → standby → coordinator. The cluster transitions through `Stopping` → `Stopped` (0 pods).

#### cluster restart

Restart a cluster. Performs a stop followed by a full start.

```bash
# Annotation: avsoft.io/action=restart
cloudberry-ctl cluster restart --cluster my-cluster
```

Phase transitions: `Running` → `Stopping` → `Initializing` → `Running`.

#### cluster create

Create a cluster from a YAML specification.

```bash
cloudberry-ctl cluster create --cluster my-cluster -f cluster.yaml
```

#### cluster delete

Delete a cluster.

```bash
# Requires confirmation
cloudberry-ctl cluster delete --cluster my-cluster --confirm

# Retain data (PVCs)
cloudberry-ctl cluster delete --cluster my-cluster --retain-data --confirm
```

#### cluster scale-status

Show the current scale operation status for a cluster. Reports whether a scale-out is in progress, segment readiness, and data redistribution state.

```bash
cloudberry-ctl cluster scale-status --cluster my-cluster
```

**Output (JSON — scaling in progress):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "scaling": true,
  "phase": "Scaling",
  "segmentsReady": 4,
  "segmentsTotal": 6,
  "redistribution": {
    "status": "True",
    "reason": "InProgress",
    "message": "Data redistribution in progress"
  }
}
```

**Output (JSON — scaling completed):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "scaling": false,
  "phase": "Running",
  "segmentsReady": 6,
  "segmentsTotal": 6,
  "redistribution": {
    "status": "True",
    "reason": "Completed",
    "message": "Data redistribution completed"
  }
}
```

**Output (table):**

```
CLUSTER      PHASE    SCALING  SEGMENTS  REDISTRIBUTION
my-cluster   Running  false    6/6       Completed
```

This command calls `GET /clusters/{name}/scale/status` on the operator REST API.

#### cluster upgrade

Upgrade cluster version.

```bash
cloudberry-ctl cluster upgrade --cluster my-cluster \
  --version 7.8 --image cloudberrydb/cloudberry:7.8
```

---

### config

Configuration management commands.

#### config get

Get parameter values.

```bash
# All parameters
cloudberry-ctl config get --cluster my-cluster

# Specific parameter
cloudberry-ctl config get --cluster my-cluster --param max_connections
```

#### config set

Set a parameter value.

```bash
# Cluster-wide parameter
cloudberry-ctl config set --cluster my-cluster \
  --param work_mem --value 256MB

# Coordinator-only parameter
cloudberry-ctl config set --cluster my-cluster \
  --param optimizer --value on --coordinator-only

# Per-database parameter
cloudberry-ctl config set --cluster my-cluster \
  --param work_mem --value 512MB --database mydb

# Per-role parameter
cloudberry-ctl config set --cluster my-cluster \
  --param statement_mem --value 1GB --role analyst
```

#### config reset

Reset a parameter to its default value.

```bash
cloudberry-ctl config reset --cluster my-cluster --param work_mem
```

#### config reload

Reload configuration without restart.

```bash
cloudberry-ctl config reload --cluster my-cluster
```

#### config hba list

List current HBA rules.

```bash
cloudberry-ctl config hba list --cluster my-cluster
```

#### config hba update

Update HBA rules from a YAML file.

```bash
cloudberry-ctl config hba update --cluster my-cluster -f hba-rules.yaml
```

#### config hba history

View HBA change history.

```bash
cloudberry-ctl config hba history --cluster my-cluster
```

---

### segments

Segment management commands.

#### segments list

List all segments.

```bash
cloudberry-ctl segments list --cluster my-cluster
```

#### segments status

Show segment status.

```bash
cloudberry-ctl segments status --cluster my-cluster
```

#### segments inspect

Show detailed segment information.

```bash
cloudberry-ctl segments inspect --cluster my-cluster
```

---

### ha

High availability management commands.

#### ha mirroring status

Show mirroring status.

```bash
cloudberry-ctl ha mirroring status --cluster my-cluster
```

#### ha mirroring enable

Enable segment mirroring.

```bash
cloudberry-ctl ha mirroring enable --cluster my-cluster --layout spread
```

#### ha mirroring disable

Disable segment mirroring.

```bash
cloudberry-ctl ha mirroring disable --cluster my-cluster
```

#### ha recovery start

Start segment recovery.

> **Implementation status**: segment recovery is **not implemented yet** in the operator. The command succeeds (the API validates and accepts the request), but the operator acknowledges the recovery annotation without performing recovery work — it emits a `RecoveryNotImplemented` Warning event and records `cloudberry_recovery_operations_total{result="noop"}`. See the [User Guide — Segment Recovery](user-guide.md#segment-recovery).

```bash
# Incremental recovery
cloudberry-ctl ha recovery start --cluster my-cluster --type incremental

# Full recovery
cloudberry-ctl ha recovery start --cluster my-cluster --type full

# Differential recovery with parallel streams
cloudberry-ctl ha recovery start --cluster my-cluster \
  --type differential --parallel 4

# Recovery to a specific node
cloudberry-ctl ha recovery start --cluster my-cluster --target-node node-3
```

#### ha recovery status

Show recovery status.

```bash
cloudberry-ctl ha recovery status --cluster my-cluster
```

#### ha recovery cancel

Cancel an in-progress recovery.

```bash
cloudberry-ctl ha recovery cancel --cluster my-cluster
```

#### ha rebalance

Rebalance segment data distribution. Redistributes data across segments based on the configured skew threshold, parallelism, and table exclusion patterns.

```bash
# Rebalance all segments (uses configured or default settings)
cloudberry-ctl ha rebalance --cluster my-cluster

# Rebalance specific tables only
cloudberry-ctl ha rebalance --cluster my-cluster --tables orders,customers,logs

# Check rebalance status (config + DataRedistribution condition)
cloudberry-ctl ha rebalance --cluster my-cluster --status

# Rebalance specific segments (legacy)
cloudberry-ctl ha rebalance --cluster my-cluster --content-ids 0,1,2
```

**Flags:**

| Flag | Type | Description |
|------|------|-------------|
| `--status` | bool | Show rebalance status instead of triggering a rebalance. Returns the rebalance configuration and `DataRedistribution` condition |
| `--tables` | string | Comma-separated list of tables to rebalance. When specified, only the listed tables are redistributed |
| `--content-ids` | string | Comma-separated list of segment content IDs to rebalance |

**Output (JSON — `--status`):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "config": {
    "skewThreshold": 10,
    "parallelism": 2,
    "excludeTables": ["audit_log", "temp_*"]
  },
  "redistribution": {
    "status": "True",
    "reason": "RebalanceCompleted",
    "message": "Rebalance completed successfully",
    "lastTransition": "2026-05-14T10:05:00Z"
  }
}
```

This command calls `GET /clusters/{name}/rebalance/status` (with `--status`) or `POST /clusters/{name}/rebalance` (without `--status`) on the operator REST API.

#### ha standby status

Show standby coordinator status.

```bash
cloudberry-ctl ha standby status --cluster my-cluster
```

#### ha standby activate

Activate the standby coordinator (manual failover). This performs a **real promotion** via `pg_promote()` (`PromoteStandby`) with at-most-once semantics — a failed promotion is surfaced via events/metrics and is never silently retried with a duplicate promote.

```bash
cloudberry-ctl ha standby activate --cluster my-cluster --confirm
```

#### ha standby reinitialize

Reinitialize the standby after failover.

```bash
cloudberry-ctl ha standby reinitialize --cluster my-cluster
```

#### ha standby restore-roles

Restore original coordinator and standby roles.

```bash
cloudberry-ctl ha standby restore-roles --cluster my-cluster
```

#### ha fts status

Show FTS (Fault Tolerance Service) status.

```bash
cloudberry-ctl ha fts status --cluster my-cluster
```

#### ha fts configure

Configure FTS probe parameters.

```bash
cloudberry-ctl ha fts configure --cluster my-cluster \
  --probe-interval 30 \
  --probe-timeout 10 \
  --probe-retries 3
```

---

### sessions

Session management commands. These commands query `pg_stat_activity` on the cluster's coordinator database via the `DBClientFactory` to provide real-time session information.

#### sessions list

List active sessions from the cluster's coordinator.

```bash
# All sessions
cloudberry-ctl sessions list --cluster my-cluster

# Filter by state
cloudberry-ctl sessions list --cluster my-cluster --state active

# Filter by user
cloudberry-ctl sessions list --cluster my-cluster --user analyst
```

**Output (table):**

```
PID    USERNAME  APPLICATION  CLIENT_ADDRESS  STATE   QUERY                          DURATION
1234   gpadmin   psql         10.0.0.1        active  SELECT * FROM orders           00:05:30
5678   appuser   pgbench      10.0.0.2        idle    INSERT INTO logs VALUES ($1)   00:15:30
```

**Output (JSON):**

```json
{
  "sessions": [
    {
      "pid": 1234,
      "username": "gpadmin",
      "application": "psql",
      "clientAddress": "10.0.0.1",
      "state": "active",
      "query": "SELECT * FROM orders",
      "queryStart": "2026-05-14T10:00:00Z",
      "duration": "00:05:30"
    },
    {
      "pid": 5678,
      "username": "appuser",
      "application": "pgbench",
      "clientAddress": "10.0.0.2",
      "state": "idle",
      "query": "INSERT INTO logs VALUES ($1)",
      "queryStart": "2026-05-14T09:50:00Z",
      "duration": "00:15:30"
    }
  ],
  "total": 2
}
```

When the database connection is not available, the command returns an empty list with a message:

```json
{
  "sessions": [],
  "total": 0,
  "message": "database connection not available"
}
```

#### sessions cancel-query

Cancel a running query by PID. The PID is passed as a positional argument. This calls `pg_cancel_backend()` on the coordinator — the session remains connected but the current query is interrupted.

```bash
cloudberry-ctl sessions cancel-query --cluster my-cluster 12345
```

**Output (JSON):**

```json
{
  "pid": 12345,
  "canceled": true
}
```

A `canceled: false` result means the PID was not found or the query had already completed.

> **PID validation**: The PID must be a positive integer. The API rejects PIDs that are zero, negative, or non-numeric with a `400 Bad Request` error.

#### sessions terminate

Terminate a session by PID. The PID is passed as a positional argument. This calls `pg_terminate_backend()` on the coordinator — the client is disconnected.

```bash
cloudberry-ctl sessions terminate --cluster my-cluster 5678
```

**Output (JSON):**

```json
{
  "pid": 5678,
  "terminated": true
}
```

A `terminated: false` result means the PID was not found or the session had already ended.

> **PID validation**: The PID must be a positive integer. The API rejects PIDs that are zero, negative, or non-numeric with a `400 Bad Request` error.

---

### queries

Query monitoring and history commands. The `queries` command group provides access to active query monitoring and historical query analysis.

#### queries list

List active queries by querying the sessions endpoint with optional status filtering.

```bash
# List all active queries
cloudberry-ctl queries list --cluster my-cluster

# Filter by status
cloudberry-ctl queries list --cluster my-cluster --status running
cloudberry-ctl queries list --cluster my-cluster --status idle
cloudberry-ctl queries list --cluster my-cluster --status blocked
```

**Flags:**

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--status` | string | | Filter by status (`running`, `queued`, `blocked`, `idle`) |

**Output (JSON):**

```json
{
  "sessions": [
    {
      "pid": 1234,
      "username": "gpadmin",
      "database": "testdb",
      "state": "active",
      "query": "SELECT * FROM orders",
      "queryStart": "2026-05-30T10:00:00Z",
      "resourceGroup": "default_group"
    }
  ],
  "total": 1
}
```

This command calls `GET /clusters/{name}/sessions` on the operator REST API.

#### queries detail

Show detailed execution information for a specific running query, including execution metrics, lock information, and tables accessed.

```bash
cloudberry-ctl queries detail --cluster my-cluster --query-id 12345
```

**Flags:**

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query-id` | int | Yes | Backend process ID (PID) of the query |

**Output (JSON):**

```json
{
  "pid": 12345,
  "username": "analyst",
  "database": "mydb",
  "state": "active",
  "query": "SELECT * FROM large_table JOIN dim_table ON ...",
  "queryStart": "2026-05-27T12:00:00Z",
  "duration": "00:00:30",
  "waitEventType": "",
  "waitEvent": "",
  "backendType": "client backend",
  "locks": [
    {"lockType": "relation", "mode": "AccessShareLock", "granted": true, "relation": "large_table"},
    {"lockType": "relation", "mode": "AccessShareLock", "granted": true, "relation": "dim_table"}
  ],
  "tablesAccessed": ["public.large_table", "public.dim_table"]
}
```

This command calls `GET /clusters/{name}/queries/{pid}` on the operator REST API.

#### queries cancel

Cancel a running query by PID. The session remains connected but the current query is interrupted. An optional reason can be provided for audit logging.

```bash
# Cancel a query
cloudberry-ctl queries cancel --cluster my-cluster --query-id 12345

# Cancel with a reason
cloudberry-ctl queries cancel --cluster my-cluster --query-id 12345 --reason "Too long"
```

**Flags:**

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query-id` | int | Yes | Backend process ID (PID) of the query to cancel |
| `--reason` | string | No | Human-readable reason for cancellation (logged for audit) |

**Output (JSON):**

```json
{
  "pid": 12345,
  "canceled": true,
  "reason": "Too long"
}
```

A `canceled: false` result means the PID was not found or the query had already completed.

This command calls `POST /clusters/{name}/queries/{pid}/cancel` on the operator REST API.

#### queries move

Move a running query to a different resource group. This reassigns the user's resource group, affecting the running query's resource allocation.

```bash
cloudberry-ctl queries move --cluster my-cluster --query-id 12345 --target-group etl
```

**Flags:**

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query-id` | int | Yes | Backend process ID (PID) of the query to move |
| `--target-group` | string | Yes | Name of the target resource group |

**Output (JSON):**

```json
{
  "pid": 12345,
  "moved": true,
  "targetGroup": "etl",
  "previousGroup": "default_group"
}
```

A `moved: false` result means the PID was not found or the resource group reassignment failed.

This command calls `POST /clusters/{name}/queries/{pid}/move` on the operator REST API.

#### queries export

Export active queries as CSV. Queries the coordinator's `pg_stat_activity` and writes the results as CSV.

```bash
# Export to stdout
cloudberry-ctl queries export --cluster my-cluster --format csv

# Export to a file
cloudberry-ctl queries export --cluster my-cluster --format csv -O active-queries.csv
```

**Flags:**

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--format` | string | `csv` | Export format (currently only `csv` is supported) |
| `-O`, `--output-file` | string | | Output file path (stdout if omitted) |

**Output (CSV):**

```csv
pid,username,database,state,query,duration,wait_event_type,resource_group
1234,gpadmin,testdb,active,SELECT * FROM orders,,default_group
5678,analyst,mydb,idle,,,analytics
```

**Response headers:**

```
Content-Type: text/csv
Content-Disposition: attachment; filename="active-queries.csv"
```

This command calls `POST /clusters/{name}/queries/export` on the operator REST API.

#### queries history list

List query history with optional filters and pagination.

```bash
# List recent query history
cloudberry-ctl queries history list --cluster my-cluster

# Filter by time range
cloudberry-ctl queries history list --cluster my-cluster --last 24h

# Filter by user and database
cloudberry-ctl queries history list --cluster my-cluster --user analyst --database warehouse

# Search with regex pattern
cloudberry-ctl queries history list --cluster my-cluster \
  --pattern "SELECT.*FROM orders" --pattern-type regex

# Search with wildcard pattern
cloudberry-ctl queries history list --cluster my-cluster \
  --pattern "INSERT*" --pattern-type wildcard

# Filter by resource group
cloudberry-ctl queries history list --cluster my-cluster --resource-group analytics_group

# Paginate results
cloudberry-ctl queries history list --cluster my-cluster --limit 20 --offset 40
```

**Flags:**

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--last` | string | | Show history from the last duration (e.g., `24h`, `7d`) |
| `--user` | string | | Filter by username |
| `--database` | string | | Filter by database name |
| `--pattern` | string | | Search pattern (regex or wildcard) |
| `--pattern-type` | string | | Pattern type: `regex` (default) or `wildcard` |
| `--resource-group` | string | | Filter by resource group |
| `--export` | string | | Export format (`csv`) — calls the export endpoint instead of listing |
| `--limit` | int | 50 | Maximum number of results (max: 100) |
| `--offset` | int | 0 | Pagination offset |

When `--export csv` is specified, the command calls `POST /clusters/{name}/queries/history/export` instead of the list endpoint, streaming CSV output directly.

**Output (JSON):**

```json
{
  "items": [
    {
      "id": 42,
      "queryId": "q-1234-1716984000000000000",
      "pid": 1234,
      "username": "analyst",
      "databaseName": "warehouse",
      "queryText": "SELECT * FROM orders WHERE created_at > '2026-01-01'",
      "queryStart": "2026-05-29T10:00:00Z",
      "queryEnd": "2026-05-29T10:00:02.5Z",
      "durationMs": 2500.00,
      "state": "completed",
      "rowsAffected": 15000,
      "cpuTimeMs": 1800.50,
      "memoryBytes": 67108864,
      "spillBytes": 0,
      "resourceGroup": "default_group"
    }
  ],
  "total": 156,
  "limit": 50,
  "offset": 0
}
```

This command calls `GET /clusters/{name}/queries/history` on the operator REST API.

#### queries history detail

Show detailed information for a specific historical query, including the EXPLAIN execution plan.

```bash
cloudberry-ctl queries history detail --cluster my-cluster \
  --query-id q-1234-1716984000000000000
```

**Flags:**

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query-id` | string | Yes | Query ID to show details for |

**Output (JSON):**

```json
{
  "id": 42,
  "queryId": "q-1234-1716984000000000000",
  "pid": 1234,
  "username": "analyst",
  "databaseName": "warehouse",
  "queryText": "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
  "queryStart": "2026-05-29T10:00:00Z",
  "queryEnd": "2026-05-29T10:00:05.2Z",
  "durationMs": 5200.00,
  "state": "completed",
  "rowsAffected": 3200,
  "cpuTimeMs": 4100.25,
  "memoryBytes": 134217728,
  "spillBytes": 268435456,
  "explainPlan": "Gather Motion 4:1  (slice1; segments: 4)\n  ->  Hash Join\n        ...",
  "resourceGroup": "analytics_group"
}
```

This command calls `GET /clusters/{name}/queries/history/{qid}` on the operator REST API.

#### queries history export

Export query history to CSV. Supports the same filters as `queries history list`.

```bash
# Export all history to a file
cloudberry-ctl queries history export --cluster my-cluster \
  --output-file queries.csv

# Export with filters
cloudberry-ctl queries history export --cluster my-cluster \
  --last 24h --user analyst --output-file filtered.csv

# Export with pattern filter
cloudberry-ctl queries history export --cluster my-cluster \
  --pattern "SELECT.*FROM orders" --pattern-type regex \
  --output-file orders-queries.csv

# Export to stdout (for piping)
cloudberry-ctl queries history export --cluster my-cluster | head -20
```

**Flags:**

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-O`, `--output-file` | string | | Output file path (stdout if omitted) |
| `--last` | string | | Export history from the last duration (e.g., `24h`) |
| `--user` | string | | Filter by username |
| `--database` | string | | Filter by database name |
| `--pattern` | string | | Search pattern |
| `--pattern-type` | string | | Pattern type: `regex` or `wildcard` |

**Output (CSV):**

```csv
query_id,username,database,query_text,start_time,end_time,duration_ms,rows_affected,cpu_time_ms,memory_bytes,spill_bytes,state
q-1234-1716984000000000000,analyst,warehouse,"SELECT * FROM orders",2026-05-29T10:00:00Z,2026-05-29T10:00:02.5Z,2500.00,15000,1800.50,67108864,0,completed
```

This command calls `POST /clusters/{name}/queries/history/export` on the operator REST API.

#### queries monitor pause

Pause the query monitor for a cluster. Takes a snapshot of the current query data and freezes it. While paused, all query monitoring endpoints return the cached snapshot with a `stale` indicator.

```bash
# Pause the query monitor
cloudberry-ctl queries monitor pause --cluster my-cluster

# Pause with namespace
cloudberry-ctl queries monitor pause --cluster my-cluster --namespace production
```

**Output:**

```
Query monitor paused for cluster my-cluster
Paused at: 2026-05-30T10:00:00Z
```

This command calls `POST /clusters/{name}/queries/monitor/pause` on the operator REST API. Requires **Operator** permission.

#### queries monitor resume

Resume the query monitor for a cluster. Removes the cached snapshot so subsequent requests return fresh data.

```bash
# Resume the query monitor
cloudberry-ctl queries monitor resume --cluster my-cluster
```

**Output:**

```
Query monitor resumed for cluster my-cluster
```

This command calls `POST /clusters/{name}/queries/monitor/resume` on the operator REST API. Requires **Operator** permission.

#### queries monitor state

Get the current pause/resume state of the query monitor.

```bash
# Check monitor state
cloudberry-ctl queries monitor state --cluster my-cluster

# JSON output
cloudberry-ctl queries monitor state --cluster my-cluster -o json
```

**Output (table):**

```
CLUSTER      PAUSED  STALE  PAUSED AT
my-cluster   false   false  -
```

**Output (paused):**

```
CLUSTER      PAUSED  STALE  PAUSED AT
my-cluster   true    true   2026-05-30T10:00:00Z
```

**Output (JSON):**

```json
{
  "paused": true,
  "stale": true,
  "pausedAt": "2026-05-30T10:00:00Z"
}
```

This command calls `GET /clusters/{name}/queries/monitor/state` on the operator REST API. Requires **Basic** permission.

#### queries plan-check

Run the static plan checker on EXPLAIN ANALYZE output. Parses the plan text, identifies performance issues, and returns actionable recommendations. No database connection is required — the analysis is purely text-based.

```bash
# Analyze a plan from a file
cloudberry-ctl queries plan-check --cluster my-cluster -f explain.txt

# Analyze a plan from stdin
cat explain.txt | cloudberry-ctl queries plan-check --cluster my-cluster -f -

# JSON output
cloudberry-ctl queries plan-check --cluster my-cluster -f explain.txt -o json

# YAML output
cloudberry-ctl queries plan-check --cluster my-cluster -f explain.txt -o yaml
```

**Flags:**

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `-f`, `--file` | string | Yes | Path to EXPLAIN ANALYZE output file (use `-` for stdin) |

**Output (table):**

```
SEVERITY  CATEGORY               NODE TYPE     RELATION  DESCRIPTION
warning   sequential_scan        Seq Scan      orders    Sequential scan on orders returned 50000 rows
warning   row_estimate_mismatch  Seq Scan      orders    Row estimate mismatch on orders: estimated 100 rows, actual 50000 rows (499x off)
warning   sort_spill             Sort                    Sort spilled to disk using 8192kB
info      high_cost_node         Nested Loop             High-cost node Nested Loop (cost=12000.00)

Summary: Found 4 performance issues: 3 warning(s), 1 info
Total nodes: 5 | Execution time: 150.000 ms
```

**Output (JSON):**

```json
{
  "issues": [
    {
      "severity": "warning",
      "category": "sequential_scan",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Sequential scan on orders returned 50000 rows",
      "recommendation": "Consider creating an index on orders for filter condition (status = 'pending')",
      "details": {
        "actualRows": 50000,
        "filter": "(status = 'pending')",
        "totalCost": 5000.00
      }
    }
  ],
  "summary": "Found 1 performance issue: 1 warning(s)",
  "totalNodes": 1,
  "executionTime": 10.5
}
```

**Detection rules applied:**

| Category | Trigger | Severity |
|----------|---------|----------|
| `sequential_scan` | Seq Scan with `actualRows > 10,000` | warning |
| `row_estimate_mismatch` | Estimated vs actual rows differ by `> 10x` | warning |
| `sort_spill` | Sort using disk instead of memory | warning |
| `nested_loop_high_rows` | Nested loop with `rows * loops > 100,000` | warning |
| `excessive_filter_rows` | Filter removes `> 10x` more rows than returned (min 1,000) | warning |
| `high_cost_node` | Node with `TotalCost > 10,000` | info |

This command calls `POST /clusters/{name}/queries/plan-check` on the operator REST API.

---

### maintenance

Maintenance operation commands. Each maintenance command triggers the creation of a Kubernetes `batchv1.Job` that runs the requested operation against the coordinator. Jobs are automatically cleaned up after 1 hour (`TTLSecondsAfterFinished=3600`).

#### maintenance vacuum

Run vacuum. Creates a Job with annotation `avsoft.io/maintenance=vacuum`.

```bash
# Regular vacuum
cloudberry-ctl maintenance vacuum --cluster my-cluster

# Vacuum specific table
cloudberry-ctl maintenance vacuum --cluster my-cluster --table public.large_table

# Vacuum with analyze (annotation: vacuum-analyze)
cloudberry-ctl maintenance vacuum --cluster my-cluster --analyze

# Full vacuum (exclusive lock, annotation: vacuum-full)
cloudberry-ctl maintenance vacuum --cluster my-cluster --full
```

| Flag | Annotation Value | SQL Command |
|------|-----------------|-------------|
| (none) | `vacuum` | `VACUUM` |
| `--analyze` | `vacuum-analyze` | `VACUUM ANALYZE` |
| `--full` | `vacuum-full` | `VACUUM FULL` |

#### maintenance analyze

Run analyze to refresh planner statistics. Creates a Job with annotation `avsoft.io/maintenance=analyze`.

```bash
# All tables
cloudberry-ctl maintenance analyze --cluster my-cluster

# Specific table
cloudberry-ctl maintenance analyze --cluster my-cluster --table public.large_table
```

#### maintenance reindex

Rebuild indexes. Creates a Job with annotation `avsoft.io/maintenance=reindex`.

```bash
# All indexes in a database
cloudberry-ctl maintenance reindex --cluster my-cluster --database mydb

# Specific table
cloudberry-ctl maintenance reindex --cluster my-cluster --table public.large_table
```

#### maintenance check-catalog

Run catalog consistency check.

```bash
cloudberry-ctl maintenance check-catalog --cluster my-cluster --database mydb
```

#### maintenance jobs

List maintenance jobs. Shows all Jobs created by the operator for the specified cluster.

```bash
cloudberry-ctl maintenance jobs --cluster my-cluster
```

You can also view maintenance Jobs directly with kubectl:

```bash
kubectl get jobs -n cloudberry-test \
  -l avsoft.io/cluster=my-cluster,avsoft.io/operation=maintenance
```

---

### auth

Authentication management commands. The `login`, `status`, and `logout` subcommands are fully implemented (Scenario 49). They validate credentials against the operator API, display authentication state, and clear cached credentials.

#### auth login

Authenticate with the operator. Supports two modes: basic auth (username/password) and OIDC.

**Basic auth** (`--basic` flag): Validates credentials by calling `GET /api/v1alpha1/clusters` with HTTP Basic authentication. Requires `--username` and `--password` (or the corresponding environment variables).

```bash
# Basic auth login
cloudberry-ctl auth login --basic --username admin
# Password is read from CLOUDBERRY_PASSWORD env var (recommended)
# or --password flag (visible in process listings — not recommended)

# With explicit password (for testing only)
cloudberry-ctl auth login --basic --username admin --password secret
```

**OIDC with credentials**: When `--username` and `--password` are provided without `--basic`, the CLI simulates the OIDC resource owner password grant flow.

```bash
# OIDC login with credentials (password grant simulation)
cloudberry-ctl auth login --username admin --password secret \
  --auth-method oidc --operator-url http://localhost:8090
```

**OIDC browser flow**: When no credentials are provided, the CLI runs the full browser-based Authorization Code flow with PKCE (S256): it generates the PKCE verifier/challenge and a CSRF state, starts a local callback server on `localhost:8085/callback`, opens the browser to the IdP authorization endpoint (falling back to printing the URL), waits for the callback, and exchanges the authorization code for tokens. PKCE is a **client-side** concern — the operator itself only verifies Bearer tokens.

```bash
# OIDC login (browser flow with PKCE)
cloudberry-ctl auth login --auth-method oidc \
  --issuer-url http://localhost:8090/realms/test \
  --client-id cloudberry-ctl
# --issuer-url (or CLOUDBERRY_OIDC_ISSUER_URL) is required for the browser flow
```

**Flags:**

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--basic` | bool | `false` | Use basic (username/password) authentication |

**Output (success):**

```
Login successful (method=basic, user=admin)
```

**Exit codes:**

| Scenario | Exit Code |
|----------|-----------|
| Login successful | 0 |
| Invalid credentials (HTTP 401) | 3 |
| Missing username or password | 1 |
| OIDC browser flow (not implemented) | 1 |

#### auth status

Show current authentication status. Checks connectivity and authentication against the operator API by calling `GET /api/v1alpha1/clusters`. The command always succeeds (exit code 0) — unauthenticated state is reported in the output, not as an error.

```bash
# Check auth status
cloudberry-ctl auth status --operator-url http://localhost:8090 \
  --username admin --auth-method basic
```

**Output (JSON — authenticated):**

```json
{
  "auth_method": "basic",
  "authenticated": true,
  "operator_url": "http://localhost:8090",
  "username": "admin"
}
```

**Output (JSON — unauthenticated):**

```json
{
  "auth_method": "basic",
  "authenticated": false,
  "error": "API error (HTTP 401): invalid credentials",
  "operator_url": "http://localhost:8090",
  "username": "admin"
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `auth_method` | string | Current auth method (`basic` or `oidc`) |
| `username` | string | Current username |
| `operator_url` | string | Operator API URL |
| `authenticated` | bool | `true` if credentials are valid |
| `error` | string | Error message (only present when `authenticated=false`) |

#### auth logout

Clear cached credentials. Since `cloudberry-ctl` uses flags and environment variables for authentication (not a persistent token cache), this command is effectively a reminder to clean up your environment.

```bash
cloudberry-ctl auth logout
```

**Output:**

```
Logged out. Cached credentials have been cleared.
Note: If you set CLOUDBERRY_USERNAME or CLOUDBERRY_PASSWORD environment variables, unset them to fully log out.
```

To fully log out, unset the environment variables:

```bash
unset CLOUDBERRY_USERNAME CLOUDBERRY_PASSWORD
```

#### auth rotate-password

Rotate the admin password.

```bash
cloudberry-ctl auth rotate-password --cluster my-cluster
```

#### auth roles list

List database roles.

```bash
cloudberry-ctl auth roles list --cluster my-cluster
```

#### auth roles create

Create a database role.

```bash
cloudberry-ctl auth roles create --cluster my-cluster \
  --name analyst --login --password mypass
```

#### auth roles update

Update a database role.

```bash
cloudberry-ctl auth roles update --cluster my-cluster \
  --name analyst --valid-until "2026-12-31"
```

#### auth roles delete

Delete a database role.

```bash
cloudberry-ctl auth roles delete --cluster my-cluster --name analyst
```

---

### inspect

Inspection and diagnostic commands.

#### inspect disk-usage

Show disk usage.

```bash
cloudberry-ctl inspect disk-usage --cluster my-cluster
cloudberry-ctl inspect disk-usage --cluster my-cluster --database mydb
```

#### inspect skew

Show data distribution skew.

```bash
cloudberry-ctl inspect skew --cluster my-cluster --table public.large_table
```

#### inspect bloat

Show table bloat.

```bash
cloudberry-ctl inspect bloat --cluster my-cluster
```

#### inspect missing-stats

Show tables missing planner statistics.

```bash
cloudberry-ctl inspect missing-stats --cluster my-cluster
```

#### inspect connections

Show connection information.

```bash
cloudberry-ctl inspect connections --cluster my-cluster
```

#### inspect locks

Show lock information.

```bash
cloudberry-ctl inspect locks --cluster my-cluster
```

#### inspect logs

View server logs.

```bash
cloudberry-ctl inspect logs --cluster my-cluster --severity ERROR --last 1h
```

---

### resource-group

Resource group management commands. These commands manage Cloudberry resource groups for workload isolation by executing SQL commands on the cluster's coordinator database via the `DBClientFactory`.

#### resource-group list

List all resource groups in the cluster. When a database connection is available, groups are queried from `gp_toolkit.gp_resgroup_status`. Otherwise, the CRD spec is used as a fallback.

```bash
cloudberry-ctl resource-group list --cluster my-cluster
```

**Output (JSON):**

```json
{
  "resourceGroups": [
    {
      "name": "analytics",
      "concurrency": 10,
      "cpuMaxPercent": 50,
      "memoryLimit": 30
    }
  ],
  "total": 1
}
```

#### resource-group create

Create a resource group with concurrency, CPU, and memory limits.

```bash
cloudberry-ctl resource-group create --cluster my-cluster \
  --name analytics --concurrency 10 --cpu-max-percent 50 --memory-limit 30
```

**Flags:**

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--name` | string | Yes | Resource group name |
| `--concurrency` | int | No | Maximum number of concurrent transactions (default: 0) |
| `--cpu-max-percent` | int | No | Maximum CPU usage percentage, 0–100 (default: 0) |
| `--memory-limit` | int | No | Memory limit percentage, 0–100 (default: 0) |

**Output (JSON):**

```json
{
  "name": "analytics",
  "concurrency": 10,
  "cpuMaxPercent": 50,
  "memoryLimit": 30,
  "status": "created"
}
```

When the database connection is not available, the response includes a pending message:

```json
{
  "name": "analytics",
  "concurrency": 10,
  "cpuMaxPercent": 50,
  "memoryLimit": 30,
  "message": "resource group creation pending; database connection not available"
}
```

#### resource-group delete

Delete a resource group from the cluster.

```bash
cloudberry-ctl resource-group delete --cluster my-cluster --name analytics
```

**Flags:**

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--name` | string | Yes | Resource group name to delete |

**Output (JSON):**

```json
{
  "group": "analytics",
  "status": "deleted"
}
```

#### resource-group assign

Assign a database role to a resource group. This executes `ALTER ROLE <role> RESOURCE GROUP <group>` on the coordinator.

```bash
cloudberry-ctl resource-group assign --cluster my-cluster \
  --group analytics --role analyst
```

**Flags:**

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--group` | string | Yes | Resource group name |
| `--role` | string | Yes | Database role to assign |

**Output (JSON):**

```json
{
  "group": "analytics",
  "role": "analyst",
  "status": "assigned"
}
```

#### resource-group update

Update a resource group.

```bash
cloudberry-ctl resource-group update --cluster my-cluster \
  --name analytics --concurrency 20
```

> **Note**: This command is a stub and returns a `"command \"resource-group update\" is not yet implemented"` error.

### pxf

Operator-driven PXF data-loading sidecar lifecycle commands. These call the operator REST API at `/clusters/{name}/data-loading/pxf/*` and require `--cluster`; `--namespace` defaults to `cloudberry-test`. The cluster must have `dataLoading.pxf` enabled — otherwise the operator returns `400 PXF_NOT_ENABLED`.

> `status`, `restart`, `sync`, and (as of **Scenario 108**) `servers …` are `cloudberry-ctl` commands. The sidecar-local PXF-binary verbs `pxf prepare`, `pxf start`, and `pxf stop` are **not** ctl commands — they run inside the `cloudberry-pxf` sidecar and are issued with `kubectl exec -c pxf -- pxf <verb>`. `pxf servers {list,create,update,delete}` CRUD is now **Implemented** (Scenario 108) — see [pxf servers](#pxf-servers) below. See [spec 12 §Scenario 95](../specifications/12-data-loading-spec.md#scenario-95--pxf-cli-lifecycle) and [§Scenario 108](../specifications/12-data-loading-spec.md#scenario-108--all-cli-commands-l1l16).

> **Writable export jobs & write-capability (Scenario 96).** Data-loading jobs are defined in the CRD (`dataLoading.jobs[]`); the ctl job-mutation commands (`data-loading jobs {create,start,stop,delete}`) now hit their **FULL** REST routes (Scenario 107), though a writable job is still typically configured via the CR. A PXF job with `pxfJob.mode: writable` builds a writable external table (`FORMATTER='pxfwritable_export'`) for **write-capable** object-store formats (`text`/`parquet`/`avro`). A writable job using a **read-only** format (`json`/`orc`/`rc`) is **rejected at admission** (webhook rule W.10b — the error contains `write-unsupported`/`writable`), so `kubectl apply` of such a CR fails. See [spec 12 §Scenario 96](../specifications/12-data-loading-spec.md#scenario-96--object-store-profiles--format-write-capability).

> **Hadoop writable jobs (Scenario 97).** For the **Hadoop** profiles, writable jobs are limited to **`hdfs:text`/`hdfs:parquet`/`hdfs:avro`/`hdfs:SequenceFile`**; `hdfs:json`/`hdfs:orc` and **all `hive*` profiles + `HBase`** are **read-only** and a `mode: writable` CR using them is **rejected at admission** (W.10b is scheme-aware — `hive:text` writable is rejected even though `text` is a writable format). See [spec 12 §Scenario 97](../specifications/12-data-loading-spec.md#scenario-97--hadoop-profiles-hdfs--hive--hbase).

> **Writable export targets & filtered export (Scenario 99).** A `mode: writable` PXF job (configured in the CR; the ctl job-mutation commands now hit their FULL REST routes — Scenario 107) **exports** rows OUT to **S3 / object store**, **HDFS** or **JDBC** via the SAME `FORMATTER='pxfwritable_export'` writable table; the load script runs `INSERT INTO <writable_ext> SELECT * FROM <targetTable>` (reversed direction). The optional `pxfJob.sourceFilter` WHERE predicate (valid only on `mode: writable`) exports a **filtered subset** (`… WHERE region='us-east'`); admission rule **W.17** rejects a `sourceFilter` on a non-writable job and any predicate containing `;`/`--`/`/*`. An export reuses the data-loading metrics (no new metric) — observe it via `cloudberry_data_loading_rows_total` (the exported rowcount) + `cloudberry_data_loading_job_status`; `bytes_transferred` stays Planned. See [spec 12 §Scenario 99](../specifications/12-data-loading-spec.md#scenario-99--writable-external-tables--data-export).

> **gpfdist + gpload control-file load (Scenario 101).** A `type: gpload` job (configured in the CR; the ctl job-mutation commands now hit their FULL REST routes — Scenario 107) now uses the **gpload control-file path** rather than a native external-table DDL: the operator renders a gpload YAML control file delivered via the `<cluster>-gpload-<job>` ConfigMap (mounted at `/etc/gpload`) and a `Job`/`CronJob` runs `gpload -f /etc/gpload/<job>.yml`. Enabling `dataLoading.gpfdist.enabled: true` deploys the gpfdist `Deployment`/`Service`/`PVC` (`<cluster>-gpfdist*`) that the control file reads over `gpfdist://`. The new `gploadJob` fields (`inputSource`, `delimiter`, `header`, `encoding`, `matchColumns`, `updateColumns`, `preload.truncate`, `postActions`) drive the control file and are guarded by admission rules **W.18-W.22**. gpload reuses the data-loading metrics (no new metric); gpfdist Deployment readiness is observed via `kubectl` (kube-state-metrics absent in the test env), and the 2 `cloudberry_gpfdist_*` metrics stay **Planned** (no scrapable endpoint). See [spec 12 §Scenario 101](../specifications/12-data-loading-spec.md#scenario-101--gpfdist-deployment--gpload-csv).

> **kafka-cdc continuous streaming via a custom connector (Scenario 102).** A kafka streaming job (configured in the CR; the ctl job-mutation commands now hit their FULL REST routes — Scenario 107) reinstates the `kafka` profile as a **custom-connector** profile (policy reversal scoped to custom connectors; built-in streaming stays out of scope): a `dataLoading.pxf.servers[]` entry of `type: custom` + a matching `dataLoading.pxf.customConnectors[]` JAR + a `pxfJob` with `profile: kafka`, `continuous: true` (admission rules **W.23/W.24/W.23c**). The operator's `pxf-connector-init` init container downloads the connector JAR into `/pxf/lib/custom/<name>.jar` on the sidecar. A `continuous: true` job runs as a **one-off long-running Job** (NOT a CronJob, and it must NOT set a `schedule`); `batchSize`/`flushInterval` drive the streaming consume loop (`CBK_BATCH_SIZE`/`CBK_FLUSH_INTERVAL`). kafka-cdc reuses the data-loading metrics (**no new metric**): a continuous consumer's **steady state is `cloudberry_data_loading_job_status = Running`** (NOT Complete) — observe it with `cloudberry-ctl data-loading status` / `kubectl get job`. End-to-end kafka→table row landing needs a REAL connector JAR (the staged one is a placeholder), so live row-landing is **config-only/documented**. See [spec 12 §Scenario 102](../specifications/12-data-loading-spec.md#scenario-102--kafka-cdc-continuous-streaming-custom-connector).

> **FDW-based loading path (Scenario 103).** A PXF job (configured in the CR; the ctl job-mutation commands now hit their FULL REST routes — Scenario 107) with **`pxfJob.loadMethod: fdw`** loads via a **PERSISTENT** foreign-data-wrapper chain instead of the transient external table: the operator builds `CREATE SERVER`/`CREATE USER MAPPING FOR gpadmin`/`CREATE FOREIGN TABLE (LIKE <target>)` (all `IF NOT EXISTS`, never dropped, per-protocol `pxf_fdw` wrapper) then `INSERT INTO <target> SELECT * FROM "foreign_<job>" [WHERE <sourceFilter>]`. The default `loadMethod: external-table` keeps the historical path. The FDW path is **read-only** (admission rule **W.25** rejects `loadMethod: fdw` with `mode: writable`/`continuous: true`, and an unknown `loadMethod`); it is **EQUIVALENT** to the external-table path — observe via the data-loading metrics (**no new metric**) and confirm by **equal row counts** between the two targets (`cloudberry-ctl data-loading status` / `kubectl get job`). See [spec 12 §Scenario 103](../specifications/12-data-loading-spec.md#scenario-103--fdw-based-loading-path).

> **Pre-load health checks (Scenario 104).** Before each data-loading Job runs (PXF/native and gpload alike), the operator prepends a **`dataload-healthcheck` init container** that runs five gated checks (HC.1 PXF readiness via a coordinator DB-proxy — not a direct sidecar probe; HC.2 target table exists; HC.3 object-store source connectivity; HC.4 gpfdist reachability; HC.5 scratch disk space). A failed check **blocks the load** and the Job fails. Toggle/tune with `dataLoading.healthChecks { enabled (default true), diskMinFreeMB (default 64), scratchSizeLimit }`. A failure surfaces via `cloudberry-ctl data-loading status` (the job shows Failed), a de-duplicated **`DataLoadingHealthCheckFailed`** Warning Event on the cluster, `cloudberry_data_loading_job_status=3` + `errors_total`, and the kube-state-metrics families (`kube_job_status_failed` / `kube_pod_init_container_status_*` / `kube_deployment_status_replicas_available`); **no new operator metric**. See [spec 12 §Scenario 104](../specifications/12-data-loading-spec.md#scenario-104--pre-load-health-checks).

#### pxf status

Show **honest** PXF sidecar readiness across the segment-primary pods. Readiness is read straight from each pod's real `pxf` container `ContainerStatuses` (no synthetic health, no exec, no cross-pod HTTP); the spec-derived `configured`/`servers` counts are echoed for context.

```bash
cloudberry-ctl pxf status --cluster my-cluster
```

**Output (JSON):**

```json
{
  "servers": 2,
  "configured": true,
  "sidecars": [
    { "pod": "my-cluster-segment-primary-0", "ready": true },
    { "pod": "my-cluster-segment-primary-1", "ready": false }
  ],
  "readySidecars": 1,
  "totalSidecars": 2
}
```

#### pxf restart

Trigger an **operator-driven** PXF restart. The operator patches the `<cluster>-segment-primary` StatefulSet pod-template restart-trigger annotation (`avsoft.io/restart-trigger`); the kubelet then **rolls all segment pods**, each re-running the entrypoint (`pxf prepare` → `pxf start`), so every PXF sidecar restarts. Returns `202`. Records the `cloudberry_pxf_restart_total{cluster,namespace,result}` metric (`result`=`started`/`failed`).

```bash
cloudberry-ctl pxf restart --cluster my-cluster
```

> **Pod-roll caveat.** This is a **pod roll**, strictly heavier than a single-sidecar `pxf restart`. For a single-sidecar restart, use `kubectl exec <segpod> -c pxf -- pxf restart` instead.
>
> **Sidecar PID-1 semantics (verified live).** The per-sidecar local `pxf stop`/`pxf restart` exec verbs are **not** an in-place JVM stop/start: the PXF JVM is the sidecar container's **PID 1**, so killing it makes Kubernetes restart the whole `pxf` container (entrypoint re-runs `pxf prepare`/`start`, then recovers). The ctl-driven `pxf restart` above (operator pod roll across all segment sidecars) is the cluster-wide path and is unaffected.

#### pxf sync

Refresh the `<cluster>-pxf-servers` ConfigMap from the current spec **and** bump the segment-primary restart-trigger so the `pxf-cred-init` init container re-renders the resolved configs on the resulting roll. Returns `202`. This is the **on-demand** counterpart to the always-on structural shared-ConfigMap sync — use it to force a config refresh + sidecar roll immediately (e.g. after a server-config or referenced-secret change).

```bash
cloudberry-ctl pxf sync --cluster my-cluster
```

#### pxf servers

Manage configured PXF servers via the operator REST API (`/clusters/{name}/data-loading/pxf/servers`). **Implemented in Scenario 108.** Listing returns server **references** only — never literal secret values.

```bash
# List configured PXF servers -> GET .../pxf/servers
cloudberry-ctl pxf servers list --cluster my-cluster

# Create a server -> POST .../pxf/servers (201 returns the rendered <server>__*.xml; 409 SERVER_EXISTS)
cloudberry-ctl pxf servers create --cluster my-cluster \
  --name s3-lake --type s3 \
  --endpoint http://minio:9000 --bucket data-lake \
  --credential-secret s3-creds:access_key \
  --credential-secret s3-creds:secret_key

# Update a server's endpoint -> PUT .../pxf/servers/{name} (via runAPIPut; 404 SERVER_NOT_FOUND)
cloudberry-ctl pxf servers update s3-lake --cluster my-cluster --endpoint http://minio:9001

# Delete a server -> DELETE .../pxf/servers/{name} (409 SERVER_IN_USE if a job references it)
cloudberry-ctl pxf servers delete s3-lake --cluster my-cluster
```

| Subcommand | REST | Flags |
|------------|------|-------|
| `pxf servers list` | `GET .../pxf/servers` | — |
| `pxf servers create` | `POST .../pxf/servers` | `--name` (required), `--type`, `--endpoint`, `--bucket`, `--credential-secret` (repeatable, `name[:key]`) |
| `pxf servers update [name]` | `PUT .../pxf/servers/{name}` | positional `name`, `--endpoint` |
| `pxf servers delete [name]` | `DELETE .../pxf/servers/{name}` | positional `name` |

---

### data-loading

Data-loading job + PXF source commands via the operator REST API (`/clusters/{name}/data-loading/*`). The `jobs {list,create,start,stop,delete}` verbs and `status` have been Live (their mutation REST routes are FULL — Scenario 107); **Scenario 108** added the enriched `jobs create` flag set, `jobs logs` streaming, and `test-read`.

#### data-loading status / jobs list

```bash
cloudberry-ctl data-loading status    --cluster my-cluster   # lists jobs
cloudberry-ctl data-loading jobs list --cluster my-cluster   # -> GET .../jobs
```

#### data-loading jobs create

Build a job from flags (`--type pxf` or `--type gpload`), or read a complete job from `--from-yaml` (which **takes precedence** over the flags). **Enriched in Scenario 108** — before, `jobs create` posted a minimal body.

```bash
# PXF job -> POST .../jobs
cloudberry-ctl data-loading jobs create --cluster my-cluster \
  --name s3-ingest --type pxf \
  --server s3-lake --profile s3:parquet \
  --resource "s3a://data-lake/events/" \
  --target public.events --schedule "0 */6 * * *"

# gpload job -> POST .../jobs
cloudberry-ctl data-loading jobs create --cluster my-cluster \
  --name csv-load --type gpload \
  --gpfdist-host gpfdist-svc --gpfdist-port 8080 \
  --file-path "/data/incoming/*.csv" \
  --target public.raw_data --format csv

# Full job from YAML -> POST .../jobs
cloudberry-ctl data-loading jobs create --cluster my-cluster --from-yaml job-config.yaml
```

| Mode | Flags |
|------|-------|
| `--type pxf` | `--name`, `--server`, `--profile`, `--resource`, `--target`, `--schedule` |
| `--type gpload` | `--name`, `--gpfdist-host`, `--gpfdist-port`, `--file-path`, `--format`, `--target`, `--schedule` |
| `--from-yaml <file>` | reads + unmarshals a complete job; overrides flag-built body |

#### data-loading jobs start / stop / delete

```bash
cloudberry-ctl data-loading jobs start  --cluster my-cluster s3-ingest   # -> POST .../jobs/{job}/start
cloudberry-ctl data-loading jobs stop   --cluster my-cluster s3-ingest   # -> POST .../jobs/{job}/stop
cloudberry-ctl data-loading jobs delete --cluster my-cluster s3-ingest   # -> DELETE .../jobs/{job}
```

#### data-loading jobs logs

**Stream** a data-loading Job's pod logs (NEW — Scenario 108), exactly mirroring `backup jobs logs`: it uses `OperatorClient.GetStream` (copies the `text/plain` body straight to stdout) and falls back to a `kubectl logs` instruction when the endpoint is unavailable (older operator, `404`/`405`, connection error, or `501 LOGS_NOT_AVAILABLE` when no clientset is wired).

```bash
cloudberry-ctl data-loading jobs logs --cluster my-cluster --job s3-ingest
cloudberry-ctl data-loading jobs logs --cluster my-cluster --job s3-ingest --follow --tail 200
```

| Flag | Description |
|------|-------------|
| `--job` | Data-loading Job name (**required**) |
| `--follow` | Stream logs as produced → `?follow=true` |
| `--tail` | Recent log lines (`-1` = all) → `?tailLines=N` |

#### data-loading test-read

Perform an **honest** sample read of a PXF source (NEW — Scenario 108) via the new `GET .../data-loading/test-read` endpoint (`handleTestReadPXFSource`, Permission Basic, **no metric**). The operator builds a **transient** external table, runs `SELECT … LIMIT N`, and **always DROPs** it.

```bash
# By job
cloudberry-ctl data-loading test-read --cluster my-cluster --job s3-ingest --limit 5

# Inline source
cloudberry-ctl data-loading test-read --cluster my-cluster \
  --server s3-lake --profile s3:parquet \
  --resource "s3a://data-lake/events/sample.parquet" --limit 10
```

| Flag | Description |
|------|-------------|
| `--job <job>` | Read the source defined by an existing job |
| `--server` / `--profile` / `--resource` | Specify the source inline |
| `--limit N` | Max rows to sample — **default 10**, **capped at 1000** |

The response shape is `TestReadResponse` `{cluster, source{server,profile,resource}, limit, available, rowCount, columns, rows}`. **Honest contract:** prints **real rows** when reachable, or `available:false`/empty when the DB or source is unreachable — values are **never fabricated** and the endpoint **never returns `500`**.

---

### metrics

Metrics and exporter management commands.

#### metrics exporters

List the health status of all Prometheus exporters deployed for a cluster. Shows each exporter's availability, last scrape time, and metric count.

```bash
cloudberry-ctl metrics exporters --cluster my-cluster
```

**Output (table):**

```
NAME                         TYPE                        PORT  HEALTHY  LAST SCRAPE           METRICS
postgres-exporter            postgres_exporter           9187  true     2026-05-30T10:00:15Z  142
cloudberry-query-exporter    cloudberry_query_exporter   9188  true     2026-05-30T10:00:15Z  87
node-exporter                node_exporter               9100  true     2026-05-30T10:00:15Z  256
```

**Output (JSON):**

```json
{
  "exporters": [
    {
      "name": "postgres-exporter",
      "type": "postgres_exporter",
      "port": 9187,
      "healthy": true,
      "lastScrape": "2026-05-30T10:00:15Z",
      "scrapeInterval": "15s",
      "metricsCount": 142,
      "errorMessage": ""
    },
    {
      "name": "cloudberry-query-exporter",
      "type": "cloudberry_query_exporter",
      "port": 9188,
      "healthy": true,
      "lastScrape": "2026-05-30T10:00:15Z",
      "scrapeInterval": "15s",
      "metricsCount": 87,
      "errorMessage": ""
    },
    {
      "name": "node-exporter",
      "type": "node_exporter",
      "port": 9100,
      "healthy": true,
      "lastScrape": "2026-05-30T10:00:15Z",
      "scrapeInterval": "15s",
      "metricsCount": 256,
      "errorMessage": ""
    }
  ],
  "total": 3,
  "healthyCount": 3
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `exporters[].name` | string | Exporter container name |
| `exporters[].type` | string | Exporter type (`postgres_exporter`, `cloudberry_query_exporter`, `node_exporter`) |
| `exporters[].port` | int | Metrics port number |
| `exporters[].healthy` | bool | `true` if the exporter's `/metrics` endpoint is reachable |
| `exporters[].lastScrape` | string | ISO 8601 timestamp of the last successful scrape |
| `exporters[].metricsCount` | int | Number of metric families exposed |
| `exporters[].errorMessage` | string | Error message if unhealthy (empty when healthy) |
| `total` | int | Total number of exporters |
| `healthyCount` | int | Number of healthy exporters |

This command calls `GET /clusters/{name}/metrics/exporters` on the operator REST API.

---

### migrate

Migrate a database between two clusters. The operator creates a **single coordinated migration Job** that runs the whole backup → restore → validate sequence and validates row counts on the target on success.

```bash
cloudberry-ctl migrate --source-cluster src --target-cluster dst \
  --database mydb \
  --tables "public.users,public.orders" \
  --truncate
```

Internally the operator creates **one** Job `<source>-migration-<ts>` (label `avsoft.io/backup-operation=migrate`) that, under the coordinator-exec model:

1. Execs `gpbackup` inside the **source** coordinator and **captures the real gpbackup timestamp** from its output.
2. Prepares the target DB on the **target** coordinator: with `--truncate` it DROPs+recreates the target DB (clean target); without `--truncate` it CREATEs the DB if absent.
3. Execs `gprestore --timestamp <captured>` inside the **target** coordinator (it does **not** pass `--truncate-table`).
4. Runs post-migration validation (row-count probe + invalid-index scan + health check) on the target, emitting `post-restore-validate: passed`.

**Flags:**

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--source-cluster` | string | Yes | Source cluster name |
| `--target-cluster` | string | Yes | Target cluster name |
| `--database` | string | Yes (server-enforced) | Database to migrate (`gpbackup --dbname`). The migration backup phase runs `gpbackup`, which hard-requires `--dbname`, so the API rejects a database-less request with `400 INVALID_REQUEST` |
| `--tables` | string | No | Comma-separated tables → repeated `--include-table` on both tools |
| `--truncate` | bool | No | Clean target: DROP+recreate the target DB empty before restore |
| `--redirect-db` | string | No | `gprestore --redirect-db` on the target |
| `--redirect-schema` | string | No | `gprestore --redirect-schema` on the target |
| `--jobs` | int | No | `gprestore --jobs` (restore parallelism) on the target |

Both clusters must be backup-enabled with an **S3** destination sharing the **same bucket** (else `400`); the migration reads the **source** cluster's S3 folder for both phases. This command calls `POST /clusters/{source}/migrate` (**Admin** permission) on the operator REST API.

---

## Output Formats

All table output uses **deterministic column ordering** — columns are sorted alphabetically by header name. This ensures consistent output across runs, making it safe to use in scripts and automated pipelines that parse table output.

### Table (default)

```bash
cloudberry-ctl cluster status --cluster my-cluster
```

```
CLUSTER      PHASE    VERSION  COORDINATOR  STANDBY  SEGMENTS  MIRRORING
my-cluster   Running  2.1.0    Ready        Ready    4/4       InSync
```

### JSON

```bash
cloudberry-ctl cluster status --cluster my-cluster --output json
```

```json
{
  "name": "my-cluster",
  "phase": "Running",
  "version": "2.1.0",
  "coordinator": {"ready": true},
  "standby": {"ready": true},
  "segments": {"ready": 4, "total": 4},
  "mirroring": "InSync"
}
```

### YAML

```bash
cloudberry-ctl cluster status --cluster my-cluster --output yaml
```

```yaml
name: my-cluster
phase: Running
version: "2.1.0"
coordinator:
  ready: true
standby:
  ready: true
segments:
  ready: 4
  total: 4
mirroring: InSync
```

## Exit Codes

Exit codes are properly wired to the CLI process, enabling reliable scripting and automation:

| Code | Description | Typical Cause |
|------|-------------|---------------|
| `0` | Success | Command completed successfully |
| `1` | General error | Unexpected error, internal failure |
| `2` | Invalid arguments | Missing required flags, invalid flag values |
| `3` | Authentication failure | Invalid credentials, expired token (HTTP 401) |
| `4` | Permission denied | Insufficient permission level (HTTP 403) |
| `5` | Cluster not found | Cluster does not exist in the specified namespace (HTTP 404) |
| `6` | Operation timeout | API request exceeded the configured timeout |
| `7` | Connection error | Cannot reach the operator API server |

**Usage in scripts:**

```bash
cloudberry-ctl cluster status --cluster my-cluster
case $? in
  0) echo "Cluster is healthy" ;;
  3) echo "Authentication failed — check credentials" ;;
  5) echo "Cluster not found" ;;
  7) echo "Cannot connect to operator API" ;;
  *) echo "Unexpected error" ;;
esac
```

### Rate Limiting

When the operator API returns `429 Too Many Requests`, `cloudberry-ctl` reports the error and exits with code `1`. The `Retry-After` header value is displayed in the error message. Implement retry logic in automation scripts:

```bash
# Retry with backoff on rate limiting
for i in 1 2 3; do
  cloudberry-ctl cluster status --cluster my-cluster && break
  echo "Retrying in ${i}0 seconds..."
  sleep "${i}0"
done
```

## Shell Completion

Generate shell completion scripts for your shell:

### Bash

```bash
# Generate completion script
cloudberry-ctl completion bash > /etc/bash_completion.d/cloudberry-ctl

# Or for the current session
source <(cloudberry-ctl completion bash)
```

### Zsh

```bash
# Generate completion script
cloudberry-ctl completion zsh > "${fpath[1]}/_cloudberry-ctl"

# Or for the current session
source <(cloudberry-ctl completion zsh)
```

### Fish

```bash
cloudberry-ctl completion fish > ~/.config/fish/completions/cloudberry-ctl.fish
```
