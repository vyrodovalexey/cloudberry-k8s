# Installation Guide

This guide covers installing the Cloudberry Operator on a Kubernetes cluster, configuring it, and managing upgrades and uninstallation.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Helm Installation](#helm-installation)
- [Configuration Options](#configuration-options)
- [Upgrading](#upgrading)
- [Uninstalling](#uninstalling)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Required

| Component | Minimum Version | Notes |
|-----------|----------------|-------|
| Kubernetes | 1.26+ | Any conformant distribution (EKS, GKE, AKS, kind, minikube) |
| Helm | 3.x | For chart-based installation |
| kubectl | 1.26+ | Matching your cluster version |

### Optional

| Component | Purpose | Notes |
|-----------|---------|-------|
| HashiCorp Vault | Secrets management | Token, Kubernetes, or AppRole auth |
| Keycloak | OIDC authentication | Or any OpenID Connect provider |
| Prometheus | Metrics collection | Operator exposes `/metrics` endpoint |
| OpenTelemetry Collector | Distributed tracing | gRPC or HTTP OTLP protocol |
| cert-manager | TLS certificate management | For automatic certificate rotation |

### Cluster Requirements

- **RBAC** must be enabled (default on most clusters)
- **PersistentVolume provisioner** must be available for database storage
- Sufficient **node resources** for the operator and database pods
- **Network connectivity** between operator and managed pods

## Helm Installation

### Quick Install

```bash
# Create the operator namespace
kubectl create namespace cloudberry-system

# Install with default values
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set installCRDs=true
```

### Install with Custom Values

Create a `custom-values.yaml` file:

```yaml
# Operator configuration
operator:
  logLevel: info
  logFormat: json
  leaderElection: true

# Resource limits
resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: "1"
    memory: 1Gi

# Enable Vault integration
vault:
  enabled: true
  address: http://vault.vault-system:8200
  authMethod: kubernetes
  role: cloudberry-operator
  secretPath: secret/data/cloudberry

# Enable OIDC authentication
oidc:
  enabled: true
  issuerURL: https://keycloak.auth-system/realms/cloudberry
  clientID: cloudberry-operator
  existingSecret: oidc-client-secret
  existingSecretKey: client-secret

# Enable telemetry
telemetry:
  enabled: true
  otlpEndpoint: otel-collector.observability:4317
  otlpProtocol: grpc
  samplingRate: 0.5

# Enable ServiceMonitor for Prometheus Operator
serviceMonitor:
  enabled: true
  interval: 30s
```

Install with the custom values:

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --values custom-values.yaml
```

### Verify Installation

```bash
# Check operator pod is running
kubectl get pods -n cloudberry-system

# Expected output:
# NAME                                    READY   STATUS    RESTARTS   AGE
# cloudberry-operator-7d8f9b6c4d-x2k9p   1/1     Running   0          30s

# Check CRD is installed
kubectl get crd cloudberryclusters.avsoft.io

# Check operator logs
kubectl logs -n cloudberry-system deployment/cloudberry-operator
```

## Configuration Options

### Full values.yaml Reference

#### Image Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of operator replicas | `1` |
| `image.repository` | Container image repository | `cloudberry-operator` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `image.tag` | Image tag (defaults to chart appVersion) | `""` |
| `imagePullSecrets` | Image pull secrets for private registries | `[]` |

#### Operator Settings

| Parameter | Description | Default |
|-----------|-------------|---------|
| `operator.logLevel` | Log level (debug, info, warn, error) | `info` |
| `operator.logFormat` | Log format (json, text) | `json` |
| `operator.leaderElection` | Enable leader election for HA | `true` |
| `operator.reconcileInterval` | Reconciliation interval | `30s` |
| `operator.operationTimeout` | Operation timeout | `5m` |
| `operator.watchNamespace` | Namespace to watch (empty = all) | `""` |
| `operator.metricsAddress` | Metrics bind address | `:8080` |
| `operator.healthProbeAddress` | Health probe bind address | `:8081` |
| `operator.apiAddress` | REST API server bind address | `:8090` |
| `operator.webhookPort` | Webhook server port | `9443` |
| `operator.webhookEnabled` | Enable admission webhooks (disable in dev environments without webhook certs) | `false` |

#### Service Account & RBAC

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create a service account | `true` |
| `serviceAccount.annotations` | Service account annotations | `{}` |
| `serviceAccount.name` | Service account name (auto-generated if empty) | `""` |
| `rbac.create` | Create RBAC resources | `true` |

#### Vault Integration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `vault.enabled` | Enable Vault integration | `false` |
| `vault.address` | Vault server address | `""` |
| `vault.authMethod` | Auth method (token, kubernetes, approle) | `kubernetes` |
| `vault.authPath` | Vault auth mount path | `auth/kubernetes` |
| `vault.role` | Vault role name | `cloudberry-operator` |
| `vault.secretPath` | Vault secret path | `secret/data/cloudberry` |
| `vault.tlsSecretName` | Vault TLS secret name | `""` |

#### OIDC Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `oidc.enabled` | Enable OIDC authentication | `false` |
| `oidc.issuerURL` | OIDC issuer URL | `""` |
| `oidc.clientID` | OIDC client ID | `""` |
| `oidc.clientSecret` | OIDC client secret (use existingSecret for production) | `""` |
| `oidc.existingSecret` | Existing secret name for OIDC client secret | `""` |
| `oidc.existingSecretKey` | Key in the existing secret | `client-secret` |

#### Telemetry

| Parameter | Description | Default |
|-----------|-------------|---------|
| `telemetry.enabled` | Enable OTLP telemetry | `false` |
| `telemetry.otlpEndpoint` | OTLP collector endpoint | `""` |
| `telemetry.otlpProtocol` | OTLP protocol (grpc, http) | `grpc` |
| `telemetry.samplingRate` | Trace sampling rate (0.0 to 1.0) | `1.0` |
| `telemetry.otlpInsecure` | Disable TLS for OTLP exporter (use for local/dev collectors) | `false` |
| `telemetry.serviceName` | Service name for traces | `cloudberry-operator` |

#### Metrics & Monitoring

| Parameter | Description | Default |
|-----------|-------------|---------|
| `metrics.enabled` | Enable metrics endpoint | `true` |
| `metrics.port` | Metrics service port | `8080` |
| `serviceMonitor.enabled` | Create ServiceMonitor resource | `false` |
| `serviceMonitor.namespace` | ServiceMonitor namespace | `""` |
| `serviceMonitor.labels` | Additional ServiceMonitor labels | `{}` |
| `serviceMonitor.interval` | Scrape interval | `30s` |
| `serviceMonitor.scrapeTimeout` | Scrape timeout | `10s` |

#### Webhook Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `webhook.enabled` | Enable admission webhooks | `true` |
| `webhook.port` | Webhook service port | `443` |
| `webhook.targetPort` | Target port on the operator | `9443` |
| `webhook.failurePolicy` | Failure policy (Fail or Ignore) | `Fail` |
| `webhook.certSecretName` | TLS certificate secret name | `""` |

#### Pod Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.requests.cpu` | CPU request | `100m` |
| `resources.requests.memory` | Memory request | `128Mi` |
| `resources.limits.cpu` | CPU limit | `500m` |
| `resources.limits.memory` | Memory limit | `512Mi` |
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `affinity` | Affinity rules | `{}` |
| `podAnnotations` | Pod annotations | `{}` |
| `podDisruptionBudget.enabled` | Enable PDB | `false` |
| `podDisruptionBudget.minAvailable` | Minimum available pods | `1` |

#### Security

| Parameter | Description | Default |
|-----------|-------------|---------|
| `podSecurityContext.runAsNonRoot` | Run as non-root | `true` |
| `podSecurityContext.runAsUser` | Run as user ID | `65532` |
| `podSecurityContext.fsGroup` | Filesystem group | `65532` |
| `securityContext.allowPrivilegeEscalation` | Allow privilege escalation | `false` |
| `securityContext.readOnlyRootFilesystem` | Read-only root filesystem | `true` |
| `networkPolicy.enabled` | Enable network policies | `false` |

### Admin Password Secret

The operator automatically creates an admin password Secret (`{cluster}-admin-password`) for each `CloudberryCluster` if one does not already exist. The Secret contains a bcrypt-hashed password used for basic authentication and database admin access.

- **Auto-creation**: If no `adminPasswordSecret` is specified in the CRD, the operator generates a random password and creates the Secret automatically
- **Custom password**: To use a specific password, create the Secret before deploying the cluster:

```bash
kubectl create secret generic my-cluster-admin-password \
  -n cloudberry-test \
  --from-literal=password='your-secure-password'
```

- **POSTGRES_PASSWORD**: The operator injects the admin password into the coordinator pod via a `SecretKeyRef` environment variable (not a hardcoded value), ensuring the password is never stored in plain text in pod specs

### Environment Variable Configuration

All operator settings can be configured via environment variables with the `CLOUDBERRY_` prefix. Nested keys use underscores as separators:

| Environment Variable | Config Key | Description |
|---------------------|------------|-------------|
| `CLOUDBERRY_API_ADDRESS` | `api-address` | REST API bind address |
| `CLOUDBERRY_WEBHOOK_ENABLED` | `webhook-enabled` | Enable admission webhooks |
| `CLOUDBERRY_TELEMETRY_OTLP_INSECURE` | `telemetry.otlp-insecure` | Disable TLS for OTLP |
| `CLOUDBERRY_LOG_LEVEL` | `log-level` | Log level |
| `CLOUDBERRY_NAMESPACE` | `namespace` | Namespace to watch |

## Upgrading

### Helm Upgrade

```bash
# Upgrade with existing values
helm upgrade cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system

# Upgrade with new values
helm upgrade cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --values custom-values.yaml

# Upgrade with specific image tag
helm upgrade cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set image.tag=v0.2.0
```

### CRD Upgrades

CRDs are installed as part of the Helm chart when `installCRDs=true`. On upgrade, Helm updates the CRDs automatically.

To manually update CRDs:

```bash
kubectl apply -f deploy/helm/cloudberry-operator/crds/
```

> **Note**: CRD changes are backward-compatible. New fields have defaults and existing clusters continue to work without modification.

### Upgrade Checklist

1. Review the changelog for breaking changes
2. Back up existing `CloudberryCluster` resources: `kubectl get cloudberryclusters -A -o yaml > backup.yaml`
3. Test the upgrade in a non-production environment
4. Run `helm upgrade` with `--dry-run` first to preview changes
5. Apply the upgrade and monitor operator logs

## Uninstalling

### Remove the Operator (Keep Clusters)

```bash
# Uninstall the Helm release
helm uninstall cloudberry-operator --namespace cloudberry-system

# The operator is removed but CloudberryCluster resources and
# their managed pods remain running.
```

### Full Cleanup (Destructive)

```bash
# 1. Delete all CloudberryCluster resources first
kubectl delete cloudberryclusters --all -A

# 2. Wait for clusters to be fully deleted
kubectl get cloudberryclusters -A

# 3. Uninstall the operator
helm uninstall cloudberry-operator --namespace cloudberry-system

# 4. Delete the CRD (removes all remaining CR data)
kubectl delete crd cloudberryclusters.avsoft.io

# 5. Delete the operator namespace
kubectl delete namespace cloudberry-system
```

> **Warning**: Deleting the CRD removes all `CloudberryCluster` resources and their associated metadata. Database PVCs may be retained depending on the `deletionPolicy` setting.

## Troubleshooting

### Operator Pod Not Starting

**Symptom**: Operator pod is in `CrashLoopBackOff` or `Error` state.

```bash
# Check pod status
kubectl describe pod -n cloudberry-system -l app.kubernetes.io/name=cloudberry-operator

# Check operator logs
kubectl logs -n cloudberry-system deployment/cloudberry-operator --previous
```

**Common causes**:
- Missing RBAC permissions — ensure `rbac.create=true`
- Invalid configuration — check environment variables and config
- Leader election conflict — ensure only one release is installed per namespace

### CRD Not Found

**Symptom**: `error: the server doesn't have a resource type "cloudberryclusters"`

```bash
# Verify CRD is installed
kubectl get crd cloudberryclusters.avsoft.io

# Reinstall CRDs
kubectl apply -f deploy/helm/cloudberry-operator/crds/
```

### Webhook Errors

**Symptom**: `Error from server (InternalError): Internal error occurred: failed calling webhook`

```bash
# Check webhook configuration
kubectl get validatingwebhookconfigurations
kubectl get mutatingwebhookconfigurations

# Check webhook service
kubectl get svc -n cloudberry-system -l app.kubernetes.io/name=cloudberry-operator

# Temporarily disable webhooks
helm upgrade cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set webhook.enabled=false
```

### Cluster Stuck in Initializing

**Symptom**: `CloudberryCluster` remains in `Initializing` phase.

```bash
# Check cluster status and conditions
kubectl describe cloudberrycluster my-cluster -n cloudberry-test

# Check managed pods
kubectl get pods -n cloudberry-test -l avsoft.io/cluster=my-cluster

# Check operator logs for the specific cluster
kubectl logs -n cloudberry-system deployment/cloudberry-operator | grep my-cluster
```

**Common causes**:
- Insufficient node resources for coordinator or segment pods
- PVC provisioning failure — check storage class availability. If no `storageClass` is specified in the CR, the cluster default is used. Ensure a default StorageClass exists (`kubectl get storageclass`)
- Image pull errors — verify image name and pull secrets. The sample CR uses `postgres:16`, which is publicly available from Docker Hub
- Init container failure — the operator uses a `busybox:1.36` init container to prepare the data directory. Ensure this image is accessible

### Vault Connection Failures

**Symptom**: `VaultConnected` condition is `False`.

```bash
# Check Vault connectivity from operator pod
kubectl exec -n cloudberry-system deployment/cloudberry-operator -- \
  wget -qO- http://vault.vault-system:8200/v1/sys/health

# Verify Vault role and policy
vault read auth/kubernetes/role/cloudberry-operator
```

**Common causes**:
- Vault address is incorrect or unreachable
- Kubernetes auth not configured in Vault
- Vault role does not have required policies

### Collecting Debug Information

```bash
# Operator logs
kubectl logs -n cloudberry-system deployment/cloudberry-operator > operator.log

# Cluster resource details
kubectl get cloudberrycluster my-cluster -n cloudberry-test -o yaml > cluster.yaml

# All managed resources
kubectl get all -n cloudberry-test -l avsoft.io/cluster=my-cluster -o yaml > resources.yaml

# Events
kubectl get events -n cloudberry-test --sort-by='.lastTimestamp' > events.log
```
