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

**With webhooks enabled (requires cert-manager or manual TLS certs):**

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set operator.webhookEnabled=true \
  --set webhook.certSecretName=cloudberry-webhook-tls
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
| `vault.enabled` | Enable Vault integration | `false` |
| `oidc.enabled` | Enable OIDC authentication | `false` |
| `telemetry.enabled` | Enable OTLP telemetry | `false` |
| `telemetry.otlpInsecure` | Disable TLS for OTLP exporter connections | `false` |
| `serviceMonitor.enabled` | Create Prometheus ServiceMonitor | `false` |
| `webhook.enabled` | Enable admission webhooks | `true` |
| `networkPolicy.enabled` | Enable network policies | `false` |

### New Configuration Options

The following configuration options were added in the latest release:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `operator.apiAddress` | Bind address for the REST API server used by `cloudberry-ctl` | `:8090` |
| `operator.webhookEnabled` | Controls whether admission webhooks are registered at startup. Disable in development environments where webhook certificates are not available | `false` |
| `telemetry.otlpInsecure` | When `true`, the OTLP exporter uses plaintext (non-TLS) connections. Use for local development with collectors that do not have TLS configured | `false` |

### Admin Password Secret

The operator automatically creates an admin password Secret (`{cluster}-admin-password`) for each `CloudberryCluster` if one does not already exist. The `POSTGRES_PASSWORD` environment variable in coordinator pods uses a `SecretKeyRef` to reference this Secret, ensuring passwords are never hardcoded in pod specs.

## Uninstallation

```bash
helm uninstall cloudberry-operator --namespace cloudberry-system
```

## CRDs

The chart includes the CloudberryCluster CRD. Set `installCRDs: false` to skip CRD installation if they are managed separately.
