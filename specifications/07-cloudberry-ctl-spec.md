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

### 5.6 Authentication

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

### 5.7 Inspection

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

## 10. Shell Completion

```bash
# Bash
cloudberry-ctl completion bash > /etc/bash_completion.d/cloudberry-ctl

# Zsh
cloudberry-ctl completion zsh > "${fpath[1]}/_cloudberry-ctl"

# Fish
cloudberry-ctl completion fish > ~/.config/fish/completions/cloudberry-ctl.fish
```
