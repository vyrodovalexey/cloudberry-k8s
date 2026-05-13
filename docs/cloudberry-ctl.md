# cloudberry-ctl CLI Reference

`cloudberry-ctl` is a command-line utility that provides imperative access to Cloudberry cluster management operations through the Cloudberry Operator REST API.

All commands communicate with the operator via HTTP calls to the REST API server (default port `:8090`). The CLI uses the `internal/ctl.OperatorClient` package, which supports basic and OIDC authentication, configurable timeouts, and automatic redirect protection.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Global Flags](#global-flags)
- [Environment Variables](#environment-variables)
- [Command Reference](#command-reference)
  - [version](#version)
  - [cluster](#cluster)
  - [config](#config)
  - [segments](#segments)
  - [ha](#ha)
  - [sessions](#sessions)
  - [maintenance](#maintenance)
  - [auth](#auth)
  - [inspect](#inspect)
  - [resource-group](#resource-group)
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
| `--password` | | Basic auth password | (prompted if not set) |
| `--output` | `-o` | Output format (`table`, `json`, `yaml`) | `table` |
| `--verbose` | `-v` | Enable verbose output | `false` |
| `--timeout` | | Operation timeout | `5m` |

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

## Implementation Status

The following commands are fully implemented and communicate with the operator REST API:

- `version`
- `cluster status`, `cluster start`, `cluster stop`, `cluster restart`, `cluster create`, `cluster delete`
- `config get`, `config set`
- `segments list`, `segments status`, `segments inspect`
- `ha mirroring status`, `ha recovery start`, `ha recovery status`, `ha rebalance`, `ha standby status`, `ha standby activate`
- `sessions list`, `sessions cancel-query`, `sessions terminate`
- `maintenance vacuum`, `maintenance analyze`, `maintenance reindex`
- `inspect disk-usage`, `inspect skew`, `inspect bloat`, `inspect missing-stats`, `inspect connections`, `inspect locks`

All other commands are **stub commands** that return a `"command %q is not yet implemented"` error with a non-zero exit code. This includes commands such as `cluster upgrade`, `config reset`, `config hba *`, `ha mirroring enable/disable`, `ha recovery cancel`, `ha standby reinitialize/restore-roles`, `ha fts *`, `auth *`, `resource-group *`, `inspect logs`, and `maintenance check-catalog/jobs`.

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
my-cluster   Running  7.7      Ready        Ready    4/4       InSync
```

#### cluster start

Start a cluster.

```bash
# Normal start
cloudberry-ctl cluster start --cluster my-cluster

# Restricted mode (superuser connections only)
cloudberry-ctl cluster start --cluster my-cluster --mode restricted

# Maintenance mode (coordinator only)
cloudberry-ctl cluster start --cluster my-cluster --mode maintenance
```

#### cluster stop

Stop a cluster.

```bash
# Smart stop (wait for clients)
cloudberry-ctl cluster stop --cluster my-cluster

# Fast stop (rollback transactions)
cloudberry-ctl cluster stop --cluster my-cluster --mode fast

# Immediate stop (abort all)
cloudberry-ctl cluster stop --cluster my-cluster --mode immediate
```

#### cluster restart

Restart a cluster.

```bash
cloudberry-ctl cluster restart --cluster my-cluster
```

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

Rebalance segment roles after recovery.

```bash
# Rebalance all segments
cloudberry-ctl ha rebalance --cluster my-cluster

# Rebalance specific segments
cloudberry-ctl ha rebalance --cluster my-cluster --content-ids 0,1,2
```

#### ha standby status

Show standby coordinator status.

```bash
cloudberry-ctl ha standby status --cluster my-cluster
```

#### ha standby activate

Activate the standby coordinator (manual failover).

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

Session management commands.

#### sessions list

List active sessions.

```bash
# All sessions
cloudberry-ctl sessions list --cluster my-cluster

# Filter by state
cloudberry-ctl sessions list --cluster my-cluster --state active

# Filter by user
cloudberry-ctl sessions list --cluster my-cluster --user analyst
```

#### sessions cancel-query

Cancel a running query by PID.

```bash
cloudberry-ctl sessions cancel-query --cluster my-cluster --pid 12345
```

> **PID validation**: The `--pid` value must be a positive integer. The API rejects PIDs that are zero, negative, or non-numeric with a `400 Bad Request` error.

#### sessions terminate

Terminate a session by PID.

```bash
cloudberry-ctl sessions terminate --cluster my-cluster --pid 12345
```

> **PID validation**: The `--pid` value must be a positive integer. The API rejects PIDs that are zero, negative, or non-numeric with a `400 Bad Request` error.

---

### maintenance

Maintenance operation commands.

#### maintenance vacuum

Run vacuum.

```bash
# Regular vacuum
cloudberry-ctl maintenance vacuum --cluster my-cluster

# Vacuum specific table
cloudberry-ctl maintenance vacuum --cluster my-cluster --table public.large_table

# Vacuum with analyze
cloudberry-ctl maintenance vacuum --cluster my-cluster --analyze

# Full vacuum (exclusive lock)
cloudberry-ctl maintenance vacuum --cluster my-cluster --full
```

#### maintenance analyze

Run analyze to refresh planner statistics.

```bash
# All tables
cloudberry-ctl maintenance analyze --cluster my-cluster

# Specific table
cloudberry-ctl maintenance analyze --cluster my-cluster --table public.large_table
```

#### maintenance reindex

Rebuild indexes.

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

List maintenance jobs.

```bash
cloudberry-ctl maintenance jobs --cluster my-cluster
```

---

### auth

Authentication management commands.

#### auth login

Authenticate with the operator.

```bash
# OIDC login (opens browser for auth code flow)
cloudberry-ctl auth login --cluster my-cluster

# Basic auth login
cloudberry-ctl auth login --cluster my-cluster --basic --username admin
```

#### auth logout

Clear cached credentials.

```bash
cloudberry-ctl auth logout --cluster my-cluster
```

#### auth status

Show current authentication status.

```bash
cloudberry-ctl auth status --cluster my-cluster
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

Resource group management commands.

#### resource-group list

List resource groups.

```bash
cloudberry-ctl resource-group list --cluster my-cluster
```

#### resource-group create

Create a resource group.

```bash
cloudberry-ctl resource-group create --cluster my-cluster \
  --name analytics --concurrency 10 --cpu-max-percent 50 --memory-limit 30
```

#### resource-group update

Update a resource group.

```bash
cloudberry-ctl resource-group update --cluster my-cluster \
  --name analytics --concurrency 20
```

#### resource-group delete

Delete a resource group.

```bash
cloudberry-ctl resource-group delete --cluster my-cluster --name analytics
```

#### resource-group assign

Assign a role to a resource group.

```bash
cloudberry-ctl resource-group assign --cluster my-cluster \
  --group analytics --role analyst
```

## Output Formats

All table output uses **deterministic column ordering** — columns are sorted alphabetically by header name. This ensures consistent output across runs, making it safe to use in scripts and automated pipelines that parse table output.

### Table (default)

```bash
cloudberry-ctl cluster status --cluster my-cluster
```

```
CLUSTER      PHASE    VERSION  COORDINATOR  STANDBY  SEGMENTS  MIRRORING
my-cluster   Running  7.7      Ready        Ready    4/4       InSync
```

### JSON

```bash
cloudberry-ctl cluster status --cluster my-cluster --output json
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

### YAML

```bash
cloudberry-ctl cluster status --cluster my-cluster --output yaml
```

```yaml
name: my-cluster
phase: Running
version: "7.7"
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
