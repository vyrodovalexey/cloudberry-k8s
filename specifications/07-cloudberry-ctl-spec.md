# Cloudberry CTL - CLI Utility Specification

**Version**: 1.0.0

---

## 1. Overview

`cloudberry-ctl` is a command-line utility that provides imperative access to Cloudberry cluster management operations through the Cloudberry Operator API. It supports all functions described in the Administration, High Availability, and Authentication specifications.

## 2. Installation

```bash
# From source
make build-ctl

# Binary location
./bin/cloudberry-ctl
```

## 3. Global Flags

| Flag | Env Variable | Description | Default |
|------|-------------|-------------|---------|
| `--cluster` | `CLOUDBERRY_CLUSTER` | Target cluster name | (required) |
| `--namespace` | `CLOUDBERRY_NAMESPACE` | Kubernetes namespace | `cloudberry-test` |
| `--kubeconfig` | `KUBECONFIG` | Path to kubeconfig | `~/.kube/config` |
| `--context` | `CLOUDBERRY_CONTEXT` | Kubernetes context | (current) |
| `--operator-url` | `CLOUDBERRY_OPERATOR_URL` | Operator API URL | (auto-discover) |
| `--auth-method` | `CLOUDBERRY_AUTH_METHOD` | Auth method (basic/oidc) | `basic` |
| `--username` | `CLOUDBERRY_USERNAME` | Basic auth username | |
| `--password` | `CLOUDBERRY_PASSWORD` | Basic auth password | (prompted) |
| `--output` | | Output format (table/json/yaml) | `table` |
| `--verbose` | | Enable verbose output | `false` |
| `--timeout` | `CLOUDBERRY_TIMEOUT` | Operation timeout | `5m` |

## 4. Command Structure

```
cloudberry-ctl
├── auth                          # Authentication management
│   ├── login                     # Authenticate with operator
│   ├── logout                    # Clear cached credentials
│   ├── status                    # Show auth status
│   ├── rotate-password           # Rotate admin password
│   └── roles                     # Manage roles
│       ├── list                  # List roles
│       ├── create                # Create role
│       ├── update                # Update role
│       └── delete                # Delete role
├── cluster                       # Cluster lifecycle
│   ├── status                    # Show cluster status
│   ├── start                     # Start cluster
│   ├── stop                      # Stop cluster
│   ├── restart                   # Restart cluster
│   ├── create                    # Create cluster from spec
│   ├── delete                    # Delete cluster
│   └── upgrade                   # Upgrade cluster version
├── config                        # Configuration management
│   ├── get                       # Get parameter value(s)
│   ├── set                       # Set parameter value
│   ├── reset                     # Reset parameter to default
│   ├── reload                    # Reload configuration
│   └── hba                       # HBA rules management
│       ├── list                  # List HBA rules
│       ├── update                # Update HBA rules
│       └── history               # View HBA change history
├── segments                      # Segment management
│   ├── list                      # List all segments
│   ├── status                    # Show segment status
│   └── inspect                   # Detailed segment info
├── ha                            # High availability
│   ├── mirroring                 # Mirroring management
│   │   ├── status                # Show mirroring status
│   │   ├── enable                # Enable mirroring
│   │   └── disable               # Disable mirroring
│   ├── recovery                  # Segment recovery
│   │   ├── start                 # Start recovery
│   │   ├── status                # Show recovery status
│   │   └── cancel                # Cancel recovery
│   ├── rebalance                 # Rebalance segments
│   ├── standby                   # Coordinator standby
│   │   ├── status                # Show standby status
│   │   ├── activate              # Activate standby
│   │   ├── reinitialize          # Reinitialize standby
│   │   └── restore-roles         # Restore original roles
│   └── fts                       # Fault tolerance
│       ├── status                # Show FTS status
│       └── configure             # Configure FTS parameters
├── sessions                      # Session management
│   ├── list                      # List active sessions
│   ├── cancel-query              # Cancel running query
│   └── terminate                 # Terminate session
├── maintenance                   # Maintenance operations
│   ├── vacuum                    # Run vacuum
│   ├── analyze                   # Run analyze
│   ├── reindex                   # Run reindex
│   ├── check-catalog             # Run catalog check
│   └── jobs                      # List maintenance jobs
├── backup                        # Backup and restore (Scenario 86)
│   ├── create                    # Create a backup
│   ├── list                      # List backups
│   ├── status                    # Show one backup's detail
│   ├── delete                    # Delete a backup
│   ├── restore                   # Restore from a backup
│   ├── schedule                  # Show the backup schedule
│   │   ├── set                   # Set the cron schedule
│   │   ├── suspend               # Suspend the schedule
│   │   └── resume                # Resume the schedule
│   └── jobs                      # List backup/restore Jobs
│       └── logs                  # Stream a backup Job's logs
├── migrate                       # Cross-cluster database migration (Scenario 87)
├── inspect                       # Inspection commands
│   ├── disk-usage                # Show disk usage
│   ├── skew                      # Show data distribution skew
│   ├── bloat                     # Show table bloat
│   ├── missing-stats             # Show tables missing stats
│   ├── connections               # Show connection info
│   ├── locks                     # Show lock info
│   └── logs                      # View server logs
├── resource-group                # Resource group management
│   ├── list                      # List resource groups
│   ├── create                    # Create resource group
│   ├── update                    # Update resource group
│   ├── delete                    # Delete resource group
│   └── assign                    # Assign role to group
├── workload                      # Workload management
│   ├── status                    # Show workload status
│   ├── resource-groups           # Resource group management
│   │   ├── list                  # List resource groups
│   │   └── create                # Create resource group
│   ├── rules                     # Workload rule management
│   │   ├── list                  # List workload rules
│   │   ├── create                # Create rule from file
│   │   ├── import                # Import rules from YAML
│   │   └── export                # Export rules to YAML
│   └── idle-rules                # List idle session rules
└── version                       # Show version info
```

## 5. Command Examples

### 5.1 Cluster Lifecycle

```bash
# Check cluster status
cloudberry-ctl cluster status --cluster my-cluster

# Start cluster
cloudberry-ctl cluster start --cluster my-cluster
cloudberry-ctl cluster start --cluster my-cluster --mode restricted
cloudberry-ctl cluster start --cluster my-cluster --mode maintenance

# Stop cluster
cloudberry-ctl cluster stop --cluster my-cluster
cloudberry-ctl cluster stop --cluster my-cluster --mode fast
cloudberry-ctl cluster stop --cluster my-cluster --mode immediate

# Restart cluster
cloudberry-ctl cluster restart --cluster my-cluster

# Create cluster from YAML
cloudberry-ctl cluster create --cluster my-cluster -f cluster.yaml

# Delete cluster
cloudberry-ctl cluster delete --cluster my-cluster --confirm
cloudberry-ctl cluster delete --cluster my-cluster --retain-data
```

### 5.2 Configuration

```bash
# Get all parameters
cloudberry-ctl config get --cluster my-cluster

# Get specific parameter
cloudberry-ctl config get --cluster my-cluster --param max_connections

# Set parameter
cloudberry-ctl config set --cluster my-cluster --param work_mem --value 256MB

# Set coordinator-only parameter
cloudberry-ctl config set --cluster my-cluster --param optimizer --value on --coordinator-only

# Set per-database parameter
cloudberry-ctl config set --cluster my-cluster --param work_mem --value 512MB --database mydb

# Set per-role parameter
cloudberry-ctl config set --cluster my-cluster --param statement_mem --value 1GB --role analyst

# Reset parameter
cloudberry-ctl config reset --cluster my-cluster --param work_mem

# Reload configuration
cloudberry-ctl config reload --cluster my-cluster

# Manage HBA rules
cloudberry-ctl config hba list --cluster my-cluster
cloudberry-ctl config hba update --cluster my-cluster -f hba-rules.yaml
cloudberry-ctl config hba history --cluster my-cluster
```

### 5.3 High Availability

```bash
# Mirroring
cloudberry-ctl ha mirroring status --cluster my-cluster
cloudberry-ctl ha mirroring enable --cluster my-cluster --layout spread

# Recovery
cloudberry-ctl ha recovery start --cluster my-cluster --type incremental
cloudberry-ctl ha recovery start --cluster my-cluster --type full
cloudberry-ctl ha recovery start --cluster my-cluster --type differential --parallel 4
cloudberry-ctl ha recovery start --cluster my-cluster --target-node node-3
cloudberry-ctl ha recovery status --cluster my-cluster

# Rebalance
cloudberry-ctl ha rebalance --cluster my-cluster
cloudberry-ctl ha rebalance --cluster my-cluster --content-ids 0,1,2

# Standby
cloudberry-ctl ha standby status --cluster my-cluster
cloudberry-ctl ha standby activate --cluster my-cluster --confirm
cloudberry-ctl ha standby reinitialize --cluster my-cluster
cloudberry-ctl ha standby restore-roles --cluster my-cluster

# FTS
cloudberry-ctl ha fts status --cluster my-cluster
cloudberry-ctl ha fts configure --cluster my-cluster \
  --probe-interval 30 --probe-timeout 10 --probe-retries 3
```

### 5.4 Sessions

```bash
# List sessions
cloudberry-ctl sessions list --cluster my-cluster
cloudberry-ctl sessions list --cluster my-cluster --state active
cloudberry-ctl sessions list --cluster my-cluster --user analyst

# Cancel query
cloudberry-ctl sessions cancel-query --cluster my-cluster --pid 12345

# Terminate session
cloudberry-ctl sessions terminate --cluster my-cluster --pid 12345
```

### 5.5 Maintenance

```bash
# Vacuum
cloudberry-ctl maintenance vacuum --cluster my-cluster
cloudberry-ctl maintenance vacuum --cluster my-cluster --table public.large_table
cloudberry-ctl maintenance vacuum --cluster my-cluster --full
cloudberry-ctl maintenance vacuum --cluster my-cluster --analyze

# Analyze
cloudberry-ctl maintenance analyze --cluster my-cluster
cloudberry-ctl maintenance analyze --cluster my-cluster --table public.large_table

# Reindex
cloudberry-ctl maintenance reindex --cluster my-cluster --database mydb
cloudberry-ctl maintenance reindex --cluster my-cluster --table public.large_table

# Catalog check
cloudberry-ctl maintenance check-catalog --cluster my-cluster --database mydb
```

### 5.6 Backup and Restore

All `backup` subcommands talk to the operator's backup/restore REST API (see
[API Specification §4.9](06-api-specification.md#49-backup-and-restore)) over an
OIDC bearer token. Point the CLI at the operator API and authenticate first
(`--operator-url` / `CLOUDBERRY_OPERATOR_URL`, `--auth-method oidc` + a token via
`--password` / `CLOUDBERRY_PASSWORD`; see [§5.6.1](#561-pointing-the-cli-at-the-operator-api)).

```bash
# Create a backup (all gpbackup flags) -> POST /backups
cloudberry-ctl backup create --cluster my-cluster --database mydb \
  --type full --compression-level 6 --compression-type zstd --jobs 4 \
  --include-schema public --exclude-table public.temp \
  --with-stats --without-globals

# Single-data-file variant (queue size needs single-data-file; --jobs is dropped)
cloudberry-ctl backup create --cluster my-cluster --database mydb \
  --type full --single-data-file --copy-queue-size 4

# Incremental variant against an explicit base timestamp
cloudberry-ctl backup create --cluster my-cluster --database mydb \
  --type incremental --incremental --from-timestamp 20260601020000 \
  --leaf-partition-data

# List backups -> GET /backups
cloudberry-ctl backup list --cluster my-cluster

# Show one backup's detail -> GET /backups/{ts}
cloudberry-ctl backup status --cluster my-cluster --timestamp 20260601020000

# Delete a backup -> DELETE /backups/{ts} (creates a gpbackman cleanup Job)
cloudberry-ctl backup delete --cluster my-cluster --timestamp 20260601020000

# Restore (all gprestore flags incl. --resize-cluster) -> POST /backups/{ts}/restore
cloudberry-ctl backup restore --cluster my-cluster --timestamp 20260601020000 \
  --redirect-db mydb_restored --redirect-schema restored --create-db \
  --include-schema public --include-table public.users --jobs 4 \
  --with-stats --run-analyze --on-error-continue --truncate-table --resize-cluster

# Schedule -> GET /backups/schedule ; set/suspend/resume -> PATCH /backups/schedule
cloudberry-ctl backup schedule --cluster my-cluster
cloudberry-ctl backup schedule set --cluster my-cluster --cron "0 3 * * *"
cloudberry-ctl backup schedule suspend --cluster my-cluster
cloudberry-ctl backup schedule resume --cluster my-cluster

# List backup/restore/cleanup Jobs -> GET /backups/jobs
cloudberry-ctl backup jobs --cluster my-cluster

# Stream a backup Job's logs -> GET /backups/jobs/{job}/logs (streams text/plain)
cloudberry-ctl backup jobs logs --cluster my-cluster --job my-cluster-backup-1
cloudberry-ctl backup jobs logs --cluster my-cluster --job my-cluster-backup-1 \
  --follow --tail 200
```

**`backup create` flags** (`buildCreateBackupRequest` → `gpbackupOptions`): `--type`
(`full`|`incremental`), `--database` (repeatable / comma-separated), `--compression-level`,
`--compression-type`, `--jobs`, `--single-data-file`, `--copy-queue-size`, `--include-schema`
(repeatable), `--exclude-table` (repeatable), `--incremental`, `--from-timestamp`,
`--leaf-partition-data`, `--with-stats`, `--without-globals`.

**`backup restore` flags** (`buildRestoreRequest` → `gprestoreOptions`): `--timestamp`
(required), `--redirect-db`, `--redirect-schema`, `--create-db`, `--include-schema`
(repeatable), `--include-table` (repeatable), `--jobs`, `--with-stats`, `--run-analyze`,
`--on-error-continue`, `--truncate-table`, **`--resize-cluster`**. `--resize-cluster` maps to
`gprestoreOptions.resizeCluster` → the restore Job's `--resize-cluster` flag — it is what
enables restoring a backup into a cluster with a **different segment count**.

#### 5.6.1 Pointing the CLI at the operator API

The `backup` commands call the operator REST API (not `kubectl`). Configure two things:

1. **API URL** — `--operator-url` (or `CLOUDBERRY_OPERATOR_URL`). When the operator API
   Service is not directly reachable, port-forward it:
   ```bash
   kubectl -n cloudberry-test port-forward svc/<operator-api-service> 8090:8090
   export CLOUDBERRY_OPERATOR_URL=http://127.0.0.1:8090
   ```
2. **OIDC token** — `--auth-method oidc` (or `CLOUDBERRY_AUTH_METHOD=oidc`) with the bearer
   token passed via `--password` (or `CLOUDBERRY_PASSWORD`). With `--auth-method oidc` the CLI
   sends `Authorization: Bearer <token>` on every request:
   ```bash
   TOKEN=$(curl -s -X POST \
     'http://keycloak:8090/realms/cloudberry/protocol/openid-connect/token' \
     -d grant_type=password -d client_id=cloudberry-ctl \
     -d username=adminuser -d password=adminpass | jq -r .access_token)
   export CLOUDBERRY_AUTH_METHOD=oidc
   export CLOUDBERRY_PASSWORD="$TOKEN"
   export CLOUDBERRY_CLUSTER=my-cluster
   export CLOUDBERRY_NAMESPACE=cloudberry-test
   ```

The endpoint permissions still apply: `create` needs **Operator**, `delete`/`restore` need
**Admin**, and the read-only commands (`list`/`status`/`schedule`/`jobs`/`jobs logs`) need
**Basic** (see [API Specification §4.9](06-api-specification.md#49-backup-and-restore)).

#### 5.6.2 Streaming backup Job logs (`backup jobs logs`)

`backup jobs logs --job <name>` **streams** the selected backup Job's pod logs to stdout by
calling `GET /clusters/{cluster}/backups/jobs/{job}/logs` (the new Scenario 86k endpoint, see
[API Specification §11.1](06-api-specification.md#111-get-clustersnamebackupsjobsjoblogs)).
The CLI uses a dedicated streaming client method (`OperatorClient.GetStream`) that copies the
`text/plain` body straight to stdout without buffering or JSON-parsing.

| Flag | Description |
|------|-------------|
| `--job` | Backup Job name (**required**) |
| `--follow` | Stream logs as they are produced → `?follow=true` |
| `--tail` | Number of recent log lines to show (`-1` = all) → `?tailLines=N` |

**kubectl fallback.** If the streaming endpoint is unavailable (e.g. an older operator
without the endpoint, a `404`/`405`, or a connection error), the CLI does **not** fail
silently — it prints the equivalent instruction:

```
unable to stream logs from the operator API (<cause>); run:
  kubectl logs -n <namespace> job/<job>
```

> **Note.** A finished Job's pod can be garbage-collected by `ttlSecondsAfterFinished`; in
> that case the endpoint returns `404 JOB_NOT_FOUND` and the CLI prints the kubectl fallback.
> Stream from a recently created Job, or use `--tail` while the pod still exists.

#### 5.6.3 Cross-cluster migration (`migrate`, Scenario 87)

`migrate` performs a cross-cluster database migration by POSTing to
`/clusters/{source}/migrate` (**Admin**). The operator creates **one coordinated
Job** `<source>-migration-<ts>` (label `avsoft.io/backup-operation=migrate`) that,
under the coordinator-exec model, execs `gpbackup` inside the **source**
coordinator and **captures the real gpbackup timestamp**, prepares the target DB
on the **target** coordinator, execs `gprestore --timestamp <captured>` inside the
target, and runs post-migration validation (row-count probe + invalid-index scan +
health check) — emitting `post-restore-validate: passed` on success.

```bash
cloudberry-ctl migrate --source-cluster src --target-cluster dst \
  --database mydb \
  --tables "public.users,public.orders" \
  --truncate
```

| Flag | Description |
|------|-------------|
| `--source-cluster` | Source cluster name (**required**) |
| `--target-cluster` | Target cluster name (**required**) |
| `--database` | Database to migrate (`gpbackup --dbname`) |
| `--tables` | Comma-separated tables → repeated `--include-table` on both tools |
| `--truncate` | Clean target: DROP+recreate the target DB empty before restore |
| `--redirect-db` | `gprestore --redirect-db` on the target |
| `--redirect-schema` | `gprestore --redirect-schema` on the target |
| `--jobs` | `gprestore --jobs` (restore parallelism) on the target |

> **Requirements.** Both clusters must be backup-enabled with an **S3** destination
> sharing the **same bucket** (else `400`); the migration reads the **source**
> cluster's S3 folder for both the backup and the (target) restore. `--truncate`
> requests a clean target DB — it does **not** pass `gprestore --truncate-table`
> (which would abort a fresh-DB metadata restore). The backup ServiceAccount needs
> `pods` + `pods/exec` RBAC (coordinator-exec model).

### 5.7 Authentication

```bash
# Login
cloudberry-ctl auth login --cluster my-cluster
cloudberry-ctl auth login --cluster my-cluster --basic --username admin

# Status
cloudberry-ctl auth status --cluster my-cluster

# Rotate password
cloudberry-ctl auth rotate-password --cluster my-cluster

# Role management
cloudberry-ctl auth roles list --cluster my-cluster
cloudberry-ctl auth roles create --cluster my-cluster \
  --name analyst --login --password mypass
cloudberry-ctl auth roles update --cluster my-cluster \
  --name analyst --valid-until "2026-12-31"
cloudberry-ctl auth roles delete --cluster my-cluster --name analyst
```

### 5.8 Inspection

```bash
# Disk usage
cloudberry-ctl inspect disk-usage --cluster my-cluster
cloudberry-ctl inspect disk-usage --cluster my-cluster --database mydb

# Data skew
cloudberry-ctl inspect skew --cluster my-cluster --table public.large_table

# Table bloat
cloudberry-ctl inspect bloat --cluster my-cluster

# Missing statistics
cloudberry-ctl inspect missing-stats --cluster my-cluster

# Server logs
cloudberry-ctl inspect logs --cluster my-cluster --severity ERROR --last 1h
```

### 5.9 Workload Management

```bash
# Show workload status
cloudberry-ctl workload status --cluster my-cluster

# List resource groups
cloudberry-ctl workload resource-groups list --cluster my-cluster

# Create resource group
cloudberry-ctl workload resource-groups create --cluster my-cluster \
  --name analytics --concurrency 10

# List workload rules
cloudberry-ctl workload rules list --cluster my-cluster

# Create rule from YAML file
cloudberry-ctl workload rules create --cluster my-cluster \
  --name cancel-long -f rule.yaml

# Import rules from YAML (upsert semantics)
cloudberry-ctl workload rules import --cluster my-cluster -f rules.yaml

# Export rules to YAML file
cloudberry-ctl workload rules export --cluster my-cluster -O rules.yaml
```

## 6. Output Formats

### 6.1 Table (default)

```
$ cloudberry-ctl cluster status --cluster my-cluster

CLUSTER      PHASE    VERSION  COORDINATOR  STANDBY  SEGMENTS  MIRRORING
my-cluster   Running  7.7      Ready        Ready    4/4       InSync
```

### 6.2 JSON

```bash
$ cloudberry-ctl cluster status --cluster my-cluster --output json
```

```json
{
  "name": "my-cluster",
  "phase": "Running",
  "version": "7.7",
  "coordinator": {"ready": true},
  "standby": {"ready": true},
  "segments": {"ready": 4, "total": 4},
  "mirroring": "InSync"
}
```

### 6.3 YAML

```bash
$ cloudberry-ctl cluster status --cluster my-cluster --output yaml
```

## 7. Configuration File

`~/.cloudberry-ctl.yaml`:

```yaml
defaultCluster: my-cluster
defaultNamespace: cloudberry-test
defaultOutput: table

clusters:
  my-cluster:
    namespace: cloudberry-test
    auth:
      method: oidc
      issuer: http://keycloak:8090/realms/cloudberry
      clientID: cloudberry-ctl
  dev-cluster:
    namespace: cloudberry-dev
    auth:
      method: basic
      username: admin
```

## 8. Environment Variables

All flags can be set via environment variables with `CLOUDBERRY_` prefix:

| Variable | Flag |
|----------|------|
| `CLOUDBERRY_CLUSTER` | `--cluster` |
| `CLOUDBERRY_NAMESPACE` | `--namespace` |
| `CLOUDBERRY_OPERATOR_URL` | `--operator-url` |
| `CLOUDBERRY_AUTH_METHOD` | `--auth-method` |
| `CLOUDBERRY_USERNAME` | `--username` |
| `CLOUDBERRY_PASSWORD` | `--password` |
| `CLOUDBERRY_TIMEOUT` | `--timeout` |
| `CLOUDBERRY_OUTPUT` | `--output` |

ENV variables take priority over config file values, which take priority over flag defaults.

## 9. Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments |
| 3 | Authentication failure |
| 4 | Permission denied |
| 5 | Cluster not found |
| 6 | Operation timeout |
| 7 | Connection error |

## 10. Scenario 49 — cloudberry-ctl Authentication

Scenario 49 implements and verifies the `auth login`, `auth status`, and `auth logout` commands in `cloudberry-ctl`.

### 10.1 Implemented Commands

#### auth login --basic

Validates basic auth credentials against the operator API by calling `GET /api/v1alpha1/clusters` with the configured username and password. On success, prints `Login successful (method=basic, user=<username>)`. On failure (HTTP 401), exits with code 3 (authentication failure).

**Flags:**

| Flag | Type | Description |
|------|------|-------------|
| `--basic` | bool | Use basic (username/password) authentication |

**Requirements:**
- `--username` (or `CLOUDBERRY_USERNAME`) is required
- `--password` (or `CLOUDBERRY_PASSWORD`) is required

#### auth login (OIDC)

When `--username` and `--password` are provided (without `--basic`), simulates the OIDC resource owner password grant by calling `GET /api/v1alpha1/clusters` with the configured credentials. On success, prints `Login successful (method=oidc, user=<username>)`.

When no credentials are provided, the browser-based authorization code flow with PKCE returns a `"not yet implemented"` error.

#### auth status

Checks connectivity and authentication against the operator API and displays the current auth status as a JSON/table/YAML response containing:

| Field | Description |
|-------|-------------|
| `auth_method` | Current auth method (`basic` or `oidc`) |
| `username` | Current username |
| `operator_url` | Operator API URL |
| `authenticated` | `true` if credentials are valid, `false` otherwise |
| `error` | Error message (only when `authenticated=false`) |

The command always succeeds (exit code 0) — unauthenticated state is reported in the output, not as an error.

#### auth logout

Clears cached credentials and prints:
1. `Logged out. Cached credentials have been cleared.`
2. A reminder to unset `CLOUDBERRY_USERNAME` and `CLOUDBERRY_PASSWORD` environment variables.

Since `cloudberry-ctl` uses flags and environment variables for authentication (not a persistent token cache), logout is effectively a no-op that reminds the user to clean up their environment.

### 10.2 Real-Cluster Verification Results

Test environment: Vault, VictoriaMetrics, MinIO, Keycloak, Kafka, RabbitMQ — all running.

| # | Test | Result |
|---|------|--------|
| 49b | Basic login with correct password | `Login successful (method=basic, user=admin)` |
| 49b | Basic login with wrong password | Rejected (exit code 3) |
| 49c | Auth status (authenticated) | Shows `authenticated: true` |
| 49c | Auth status (unauthenticated) | Shows `authenticated: false` with error |
| 49d | Logout | `Logged out. Cached credentials have been cleared.` |
| 49a | OIDC login (with credentials) | `Login successful (method=oidc, user=admin)` |
| — | Cluster status after auth | Shows Running cluster |
| — | Data ops | 50 rows in mydb |

### 10.3 Test Files

| File | Description |
|------|-------------|
| `cmd/cloudberry-ctl/main.go` | `newAuthLoginCmd()`, `runAuthLoginBasic()`, `runAuthLoginOIDC()`, `runAuthStatus()`, `runAuthLogout()` |
| `test/examples/scenario49-ctl-auth.yaml` | Example cluster CR with basic auth config |
| `test/cases/test_cases.go` | `CTLAuthCase` type and `CTLAuthCases()` (6 test cases) |
| `test/functional/scenario49_ctl_auth_test.go` | 7 functional tests (mock HTTP server) |
| `test/e2e/scenario49_ctl_auth_e2e_test.go` | 8 E2E tests (mock HTTP server + cluster CR) |

### 10.4 Exit Codes

| Scenario | Exit Code |
|----------|-----------|
| Login successful | 0 |
| Login failed (wrong credentials) | 3 (authentication failure) |
| Auth status (always) | 0 |
| Logout (always) | 0 |
| OIDC browser flow (not implemented) | 1 (general error) |

## 11. Shell Completion

```bash
# Bash
cloudberry-ctl completion bash > /etc/bash_completion.d/cloudberry-ctl

# Zsh
cloudberry-ctl completion zsh > "${fpath[1]}/_cloudberry-ctl"

# Fish
cloudberry-ctl completion fish > ~/.config/fish/completions/cloudberry-ctl.fish
```

## 12. Scenario 86 — All Backup CLI Commands

**Scenario 86** verifies **all eleven** `cloudberry-ctl backup …` CLI commands end-to-end:
each command builds the right operator REST request (method/path/body), the responses are
rendered correctly, and the one new behavior — `backup jobs logs` log **streaming** — works
with a kubectl fallback. Ten of the eleven commands reuse the Scenario 85 backup/restore
endpoints; the single code addition is the `backup jobs logs` streaming path (sub-case 86k) and
its operator endpoint (see [API Specification §11.1](06-api-specification.md#111-get-clustersnamebackupsjobsjoblogs)).

All commands inherit the global flags (`--cluster`, `--namespace` default `cloudberry-test`,
`--operator-url`/`CLOUDBERRY_OPERATOR_URL`, `--auth-method oidc` + token via
`--password`/`CLOUDBERRY_PASSWORD`); the CLI prefixes every path with the API prefix
(`/api/v1alpha1`). Acceptance per sub-case (86a–86k):

| Sub-case | Command (cobra path) | REST request | Builder / notes |
|----------|----------------------|--------------|-----------------|
| **86a** | `backup create …` (3 variants) | `POST /clusters/{cluster}/backups` | `buildCreateBackupRequest` → `gpbackupOptions` (full/single-data-file/incremental variants) |
| **86b** | `backup list` | `GET /clusters/{cluster}/backups` | lists recorded history |
| **86c** | `backup status --timestamp <ts>` | `GET /clusters/{cluster}/backups/{ts}` | empty `--timestamp` → list fallback |
| **86d** | `backup delete --timestamp <ts>` | `DELETE /clusters/{cluster}/backups/{ts}` | `--timestamp` required; creates a cleanup Job |
| **86e** | `backup restore --timestamp <ts> …` | `POST /clusters/{cluster}/backups/{ts}/restore` | `buildRestoreRequest` → `gprestoreOptions`, incl. `--resize-cluster` |
| **86f** | `backup schedule` | `GET /clusters/{cluster}/backups/schedule` | shows CronJob status + `nextScheduleTime` |
| **86g** | `backup schedule set --cron …` | `PATCH /clusters/{cluster}/backups/schedule` `{"schedule":"…"}` | `--cron` required |
| **86h** | `backup schedule suspend` | `PATCH /clusters/{cluster}/backups/schedule` `{"suspend":true}` | sets CronJob `.spec.suspend=true` |
| **86i** | `backup schedule resume` | `PATCH /clusters/{cluster}/backups/schedule` `{"suspend":false}` | sets CronJob `.spec.suspend=false` |
| **86j** | `backup jobs` | `GET /clusters/{cluster}/backups/jobs` | lists backup/restore/cleanup Job statuses |
| **86k** | `backup jobs logs --job <name>` | `GET /clusters/{cluster}/backups/jobs/{job}/logs` | **streams** pod logs (`--follow`/`--tail`); kubectl fallback |

- **86a — `backup create`.** `buildCreateBackupRequest` maps the flags listed in
  [§5.6](#56-backup-and-restore) to `gpbackupOptions`. Three variants are exercised: the full
  flag set; a `--single-data-file --copy-queue-size 4` variant (mutually exclusive with
  `--jobs`); and an `--incremental --from-timestamp <ts> --leaf-partition-data` variant.

- **86e — `backup restore` (incl. `--resize-cluster`).** `buildRestoreRequest` maps the
  restore flags to `gprestoreOptions`; `--resize-cluster` → `resizeCluster: true` → the restore
  Job's `--resize-cluster` flag, which is required to restore into a cluster with a different
  segment count. Mutual-exclusivity resolution (include-table over include-schema, run-analyze
  over with-stats) is performed operator-side.

- **86k — `backup jobs logs` (streaming + fallback).** The CLI streams the response of the new
  `GET …/backups/jobs/{job}/logs` endpoint to stdout via `OperatorClient.GetStream` (no
  buffering, no JSON parse). `--follow` → `?follow=true`; `--tail N` → `?tailLines=N`. Missing
  `--job` errors before any request. When the endpoint is unavailable (older operator, `404`,
  connection error) the CLI prints the kubectl fallback (see
  [§5.6.2](#562-streaming-backup-job-logs-backup-jobs-logs)).

**Implementation.** The command tree lives in `cmd/cloudberry-ctl/main.go`
(`newBackupCmd` → `newBackupCreateCmd`/`newBackupListCmd`/`newBackupStatusCmd`/
`newBackupDeleteCmd`/`newBackupRestoreCmd`/`newBackupScheduleCmd`(+`set`/`suspend`/`resume`)/
`newBackupJobsCmd` → `newBackupJobsLogsCmd`), with `buildCreateBackupRequest`,
`buildRestoreRequest`, `buildBackupJobLogsPath`, `runBackupJobsLogs`, and
`printBackupJobLogsFallback`. The streaming client method is `OperatorClient.GetStream`
(`internal/ctl/client.go`); the operator endpoint is `handleBackupJobLogs`
(`internal/api/server.go`).

**Verification.** Sample CR
`deploy/helm/cloudberry-operator/config/samples/scenario86-cli-commands.yaml`
(single S3 cluster `scenario86-s3`: backup enabled + schedule + incremental). Tests:
`test/functional/scenario86_cli_commands_test.go`,
`test/integration/scenario86_cli_commands_integration_test.go`,
`test/e2e/scenario86_cli_commands_e2e_test.go`, the test-case catalog
`test/cases/scenario86_cli_commands_cases.go`, and the live OIDC-authed exercise script
`test/e2e/scripts/scenario86-cli-commands.sh` (builds the CLI, obtains an OIDC token,
port-forwards the operator API, runs every backup command 86a–k, and asserts the created
Jobs/args, the CronJob schedule/suspend changes, and the streamed Job logs).
