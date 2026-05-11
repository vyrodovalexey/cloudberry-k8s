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
| `vault.enabled` | Enable Vault integration | `false` |
| `oidc.enabled` | Enable OIDC authentication | `false` |
| `telemetry.enabled` | Enable OTLP telemetry | `false` |
| `serviceMonitor.enabled` | Create Prometheus ServiceMonitor | `false` |
| `webhook.enabled` | Enable admission webhooks | `true` |
| `networkPolicy.enabled` | Enable network policies | `false` |

## Uninstallation

```bash
helm uninstall cloudberry-operator --namespace cloudberry-system
```

## CRDs

The chart includes the CloudberryCluster CRD. Set `installCRDs: false` to skip CRD installation if they are managed separately.
