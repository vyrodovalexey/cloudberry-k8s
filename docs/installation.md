# Installation Guide

This guide covers installing the Cloudberry Operator on a Kubernetes cluster, configuring it, and managing upgrades and uninstallation.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Helm Installation](#helm-installation)
- [Configuration Options](#configuration-options)
  - [Webhook Certificate Configuration](#webhook-certificate-configuration)
  - [API Admin Password](#api-admin-password)
  - [Environment Variable Configuration](#environment-variable-configuration)
  - [Monitoring Stack Setup](#monitoring-stack-setup)
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
| `webhook.certSource` | Certificate source: `self-signed` or `vault-pki` | `self-signed` |
| `webhook.certSecretName` | TLS certificate secret name (auto-generated if empty) | `""` |
| `webhook.serviceName` | Webhook service name (defaults to `{release}-webhook`) | `""` |
| `webhook.caBundle` | Static CA bundle (base64-encoded); leave empty for runtime injection | `""` |
| `webhook.vaultPKI.mountPath` | Vault PKI engine mount path (when `certSource` is `vault-pki`) | `pki` |
| `webhook.vaultPKI.role` | Vault PKI role name (when `certSource` is `vault-pki`) | `cloudberry-operator` |

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

### Webhook Certificate Configuration

The operator manages TLS certificates for its admission webhooks automatically. Two certificate sources are supported:

#### Self-Signed Certificates (Default)

The operator generates a self-signed CA and server certificate on startup. No external dependencies are required.

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set webhook.enabled=true \
  --set webhook.certSource=self-signed
```

The operator:
1. Generates an ECDSA P-256 CA key pair and a server certificate
2. Stores both in a Kubernetes Secret (`{release}-webhook-certs`)
3. Injects the CA bundle into the `ValidatingWebhookConfiguration` and `MutatingWebhookConfiguration`
4. Checks for rotation every 12 hours and rotates when 2/3 of the certificate lifetime has elapsed

#### Vault PKI Certificates (Recommended for Production)

Use Vault's PKI secrets engine to issue trusted certificates:

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set webhook.enabled=true \
  --set webhook.certSource=vault-pki \
  --set webhook.vaultPKI.mountPath=pki \
  --set webhook.vaultPKI.role=cloudberry-operator \
  --set vault.enabled=true \
  --set vault.address=http://vault:8200
```

**Vault PKI prerequisites:**
1. Enable the PKI secrets engine at the configured mount path
2. Configure a root or intermediate CA
3. Create a role that allows issuing certificates for the webhook service DNS names:
   - `{serviceName}.{namespace}.svc`
   - `{serviceName}.{namespace}.svc.cluster.local`

```bash
# Example Vault PKI setup
vault secrets enable -path=pki pki
vault write pki/root/generate/internal \
  common_name="cloudberry-operator-ca" ttl=87600h
vault write pki/roles/cloudberry-operator \
  allowed_domains="cloudberry-system.svc,cloudberry-system.svc.cluster.local" \
  allow_subdomains=true max_ttl=8760h
```

#### Custom Certificate Secret

To use externally managed certificates, create a TLS Secret and reference it:

```bash
kubectl create secret tls my-webhook-certs \
  -n cloudberry-system \
  --cert=server.crt --key=server.key

helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set webhook.enabled=true \
  --set webhook.certSecretName=my-webhook-certs \
  --set webhook.caBundle="$(base64 < ca.crt)"
```

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

### API Admin Password

The operator REST API requires authentication. The admin password for the API server is configured via the `CLOUDBERRY_API_ADMIN_PASSWORD` environment variable:

```bash
# Set the admin password for the operator REST API
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set-string env.CLOUDBERRY_API_ADMIN_PASSWORD="your-secure-api-password"
```

Or set it directly in the deployment:

```yaml
env:
  - name: CLOUDBERRY_API_ADMIN_PASSWORD
    valueFrom:
      secretKeyRef:
        name: operator-api-credentials
        key: admin-password
```

**Behavior:**
- If `CLOUDBERRY_API_ADMIN_PASSWORD` is set, the operator uses it as the admin password for the REST API
- If **not** set, the operator auto-generates a cryptographically secure random password (including special characters) and logs a warning with a hint to set the variable for production use
- The generated password is logged only as a warning hint — it is not persisted. Restarting the operator without `CLOUDBERRY_API_ADMIN_PASSWORD` generates a new password each time

> **Production recommendation**: Always set `CLOUDBERRY_API_ADMIN_PASSWORD` via a Kubernetes Secret reference to ensure a stable, known password for API access.

### Environment Variable Configuration

All operator settings can be configured via environment variables with the `CLOUDBERRY_` prefix. Nested keys use underscores as separators:

| Environment Variable | Config Key | Description |
|---------------------|------------|-------------|
| `CLOUDBERRY_API_ADMIN_PASSWORD` | — | Admin password for the operator REST API (auto-generated if not set) |
| `CLOUDBERRY_API_ADDRESS` | `api-address` | REST API bind address |
| `CLOUDBERRY_WEBHOOK_ENABLED` | `webhook-enabled` | Enable admission webhooks |
| `CLOUDBERRY_TELEMETRY_OTLP_INSECURE` | `telemetry.otlp-insecure` | Disable TLS for OTLP |
| `CLOUDBERRY_LOG_LEVEL` | `log-level` | Log level |
| `CLOUDBERRY_NAMESPACE` | `namespace` | Namespace to watch |

### Monitoring Stack Setup

The project includes monitoring configurations for metrics collection and distributed tracing. Deploy these alongside the operator for full observability.

#### vmagent (VictoriaMetrics Agent)

vmagent collects Prometheus metrics from the operator and forwards them to a VictoriaMetrics or Prometheus-compatible backend:

```bash
# Deploy the operator with metrics enabled
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set metrics.enabled=true \
  --set serviceMonitor.enabled=true
```

Pre-built Grafana dashboards for operator metrics are available in the `monitoring/grafana/` directory.

#### OpenTelemetry Collector

The otel-collector receives OTLP traces from the operator and exports them to a tracing backend (e.g., Tempo, Jaeger):

```bash
# Deploy the operator with telemetry enabled
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --set telemetry.enabled=true \
  --set telemetry.otlpEndpoint=otel-collector:4317 \
  --set telemetry.otlpProtocol=grpc \
  --set telemetry.otlpInsecure=true
```

For local development, the Docker Compose test environment includes VictoriaMetrics (port 8428), Grafana (port 3000), and Tempo (ports 3200/4317/4318) pre-configured for metrics and tracing.

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
