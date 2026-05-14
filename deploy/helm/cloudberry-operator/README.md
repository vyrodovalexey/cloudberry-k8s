# Cloudberry Operator Helm Chart

A Helm chart for deploying the Cloudberry Database Kubernetes Operator.

## Prerequisites

- Kubernetes 1.26+
- Helm 3.x

## Installation

```bash
# Add the chart repository (if published)
# helm repo add cloudberry https://charts.avsoft.io

# Install with default values
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace

# Install with custom values
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  -f my-values.yaml
```

### Installation Examples

**Minimal (development):**

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set installCRDs=true
```

**With OTLP telemetry (insecure, for local collectors):**

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set telemetry.enabled=true \
  --set telemetry.otlpEndpoint=otel-collector:4317 \
  --set telemetry.otlpInsecure=true
```

**With webhooks and self-signed certificates (default):**

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set webhook.enabled=true
```

**With webhooks and Vault PKI certificates:**

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set webhook.enabled=true \
  --set webhook.certSource=vault-pki \
  --set webhook.vaultPKI.mountPath=pki \
  --set webhook.vaultPKI.role=cloudberry-operator \
  --set vault.enabled=true \
  --set vault.address=http://vault:8200
```

**With webhooks and externally managed certificates:**

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set webhook.enabled=true \
  --set webhook.certSecretName=my-webhook-tls \
  --set webhook.caBundle="$(base64 < ca.crt)"
```

**Production with Vault and OIDC:**

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set vault.enabled=true \
  --set vault.address=http://vault:8200 \
  --set oidc.enabled=true \
  --set oidc.issuerURL=https://keycloak/realms/cloudberry \
  --set oidc.clientID=cloudberry-operator \
  --set oidc.existingSecret=oidc-client-secret
```

## Configuration

See [values.yaml](values.yaml) for the full list of configurable parameters.

### Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of operator replicas | `1` |
| `image.repository` | Operator image repository | `cloudberry-operator` |
| `image.tag` | Operator image tag | Chart appVersion |
| `installCRDs` | Install CRDs with the chart | `true` |
| `operator.logLevel` | Log level | `info` |
| `operator.leaderElection` | Enable leader election | `true` |
| `operator.apiAddress` | REST API server bind address | `:8090` |
| `operator.webhookEnabled` | Enable admission webhooks (requires TLS certs) | `false` |
| `env.CLOUDBERRY_API_ADMIN_PASSWORD` | Admin password for the operator REST API (auto-generated if not set) | (generated) |
| `vault.enabled` | Enable Vault integration | `false` |
| `oidc.enabled` | Enable OIDC authentication | `false` |
| `telemetry.enabled` | Enable OTLP telemetry | `false` |
| `telemetry.otlpInsecure` | Disable TLS for OTLP exporter connections | `false` |
| `serviceMonitor.enabled` | Create Prometheus ServiceMonitor | `false` |
| `webhook.enabled` | Enable admission webhooks | `true` |
| `webhook.certSource` | Certificate source: `self-signed` or `vault-pki` | `self-signed` |
| `webhook.certSecretName` | TLS certificate secret name (auto-generated if empty) | `""` |
| `webhook.serviceName` | Webhook service name (defaults to `{release}-webhook`) | `""` |
| `webhook.caBundle` | Static CA bundle (base64-encoded); leave empty for runtime injection | `""` |
| `webhook.vaultPKI.mountPath` | Vault PKI engine mount path | `pki` |
| `webhook.vaultPKI.role` | Vault PKI role name | `cloudberry-operator` |
| `networkPolicy.enabled` | Enable network policies | `false` |

### New Configuration Options

The following configuration options were added in the latest release:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `operator.apiAddress` | Bind address for the REST API server used by `cloudberry-ctl` | `:8090` |
| `operator.webhookEnabled` | Controls whether admission webhooks are registered at startup. Disable in development environments where webhook certificates are not available | `false` |
| `telemetry.otlpInsecure` | When `true`, the OTLP exporter uses plaintext (non-TLS) connections. Use for local development with collectors that do not have TLS configured | `false` |

### API Admin Password

The operator REST API uses an admin password for authentication. Configure it via the `CLOUDBERRY_API_ADMIN_PASSWORD` environment variable:

```yaml
# In your custom values.yaml or via --set
extraEnv:
  - name: CLOUDBERRY_API_ADMIN_PASSWORD
    valueFrom:
      secretKeyRef:
        name: operator-api-credentials
        key: admin-password
```

Or pass it directly (not recommended for production):

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set-string env.CLOUDBERRY_API_ADMIN_PASSWORD="your-secure-password"
```

**Behavior:**
- If `CLOUDBERRY_API_ADMIN_PASSWORD` is set, the operator uses it as the admin password for the REST API
- If not set, the operator auto-generates a cryptographically secure random password (including special characters) and logs a warning
- The auto-generated password changes on every operator restart — always set this variable in production

### Webhook Certificate Management

The operator manages TLS certificates for its admission webhooks. Two certificate sources are supported:

**Self-signed (default):** The operator generates an ECDSA P-256 CA and server certificate on startup. Certificates are stored in a Kubernetes Secret and automatically rotated when 2/3 of their lifetime has elapsed. No external dependencies are required.

**Vault PKI:** The operator issues certificates from Vault's PKI secrets engine. Configure the mount path and role:

```yaml
webhook:
  enabled: true
  certSource: vault-pki
  vaultPKI:
    mountPath: pki
    role: cloudberry-operator
```

The operator injects the CA bundle into the `ValidatingWebhookConfiguration` and `MutatingWebhookConfiguration` at runtime. To use a static CA bundle instead (e.g., for externally managed certificates), set `webhook.caBundle` to the base64-encoded CA certificate.

The deployment template automatically:
- Sets `CLOUDBERRY_WEBHOOK_CERT_SOURCE`, `CLOUDBERRY_WEBHOOK_CERT_SECRET_NAME`, and `CLOUDBERRY_WEBHOOK_SERVICE_NAME` environment variables
- Mounts the certificate Secret at `/tmp/k8s-webhook-server/serving-certs`
- Exposes the webhook port (default `9443`)
- Creates a dedicated webhook Service

### Cluster Admin Password Secret

The operator automatically creates an admin password Secret (`{cluster}-admin-password`) for each `CloudberryCluster` if one does not already exist. The `POSTGRES_PASSWORD` environment variable in coordinator pods uses a `SecretKeyRef` to reference this Secret, ensuring passwords are never hardcoded in pod specs.

## Uninstallation

```bash
helm uninstall cloudberry-operator --namespace cloudberry-system
```

## Cluster Phases

The `CloudberryCluster` resource reports its current lifecycle state via `status.phase`:

| Phase | Description |
|-------|-------------|
| `Pending` | Cluster resource created, waiting for initialization |
| `Initializing` | StatefulSets and Services are being created |
| `Running` | All components are running and healthy |
| `Updating` | Cluster spec changed, resources are being updated |
| `Scaling` | Segment count is being changed |
| `Stopping` | Cluster is shutting down (scale-down in progress) |
| `Stopped` | All pods are scaled to zero |
| `Restricted` | Coordinator only, superuser connections only |
| `Maintenance` | Coordinator only, utility mode |
| `Failed` | An error occurred during reconciliation |
| `Deleting` | Cluster is being deleted |

### Lifecycle Actions

Trigger lifecycle actions via annotations on the `CloudberryCluster` resource:

```bash
# Stop the cluster (fast mode)
kubectl annotate cloudberrycluster my-cluster avsoft.io/action=stop-fast

# Start the cluster
kubectl annotate cloudberrycluster my-cluster avsoft.io/action=start

# Start in restricted mode (coordinator only)
kubectl annotate cloudberrycluster my-cluster avsoft.io/action=start-restricted

# Start in maintenance mode
kubectl annotate cloudberrycluster my-cluster avsoft.io/action=start-maintenance

# Restart the cluster
kubectl annotate cloudberrycluster my-cluster avsoft.io/action=restart
```

## Maintenance Operations

The operator creates Kubernetes Jobs for database maintenance operations. Trigger them via annotations:

```bash
# Vacuum
kubectl annotate cloudberrycluster my-cluster avsoft.io/maintenance=vacuum

# Vacuum with analyze
kubectl annotate cloudberrycluster my-cluster avsoft.io/maintenance=vacuum-analyze

# Full vacuum (exclusive lock)
kubectl annotate cloudberrycluster my-cluster avsoft.io/maintenance=vacuum-full

# Analyze (refresh planner statistics)
kubectl annotate cloudberrycluster my-cluster avsoft.io/maintenance=analyze

# Reindex
kubectl annotate cloudberrycluster my-cluster avsoft.io/maintenance=reindex
```

**Job properties:**
- `BackoffLimit`: 1
- `TTLSecondsAfterFinished`: 3600 (auto-cleanup after 1 hour)
- `RestartPolicy`: Never
- `PGPASSWORD`: Sourced from `{cluster}-admin-password` Secret

Monitor maintenance Jobs:

```bash
kubectl get jobs -l avsoft.io/cluster=my-cluster,avsoft.io/operation=maintenance
```

## Configuration Hot-Reload

The operator automatically detects whether a parameter change requires a restart or can be applied via reload:

- **Reload-safe parameters** (e.g., `log_min_messages`, `work_mem`): ConfigMap updated, no pod restarts
- **Restart-required parameters** (e.g., `shared_buffers`, `max_connections`, `wal_level`): ConfigMap updated, rolling restart triggered

Rolling restart order: mirrors → primaries → standby → coordinator.

## CRDs

The chart includes the CloudberryCluster CRD. Set `installCRDs: false` to skip CRD installation if they are managed separately.
