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

**With Vault AppRole authentication:**

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set vault.enabled=true \
  --set vault.address=http://vault:8200 \
  --set vault.authMethod=approle \
  --set vault.roleID="$VAULT_ROLE_ID" \
  --set vault.secretID="$VAULT_SECRET_ID"
```

The AppRole credentials are passed to the operator as `CLOUDBERRY_VAULT_ROLE_ID` / `CLOUDBERRY_VAULT_SECRET_ID`. Token renewal and re-authentication are automatic (background `LifetimeWatcher`).

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
| `operator.enableTestUsers` | Seed well-known TEST users (`basic_user`/`opbasic_user`/`operator_user`) into the API credential store (sets `CLOUDBERRY_ENABLE_TEST_USERS=true`; the operator logs a WARN). **Test suites only — never enable in production, the credentials are publicly known** | `false` |
| `env.CLOUDBERRY_API_ADMIN_PASSWORD` | Admin password for the operator REST API (auto-generated if not set) | (generated) |
| `vault.enabled` | Enable Vault integration | `false` |
| `vault.authMethod` | Vault auth method: `token`, `kubernetes`, or `approle` | `kubernetes` |
| `vault.roleID` | Vault AppRole `role_id` (approle auth; sets `CLOUDBERRY_VAULT_ROLE_ID`) | `""` |
| `vault.secretID` | Vault AppRole `secret_id` (approle auth; sets `CLOUDBERRY_VAULT_SECRET_ID` — prefer a Secret in production) | `""` |
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

The following configuration options were added or updated in the latest release:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `operator.apiAddress` | Bind address for the REST API server used by `cloudberry-ctl` | `:8090` |
| `operator.webhookEnabled` | Controls whether admission webhooks are registered at startup. Disable in development environments where webhook certificates are not available. This value is now included in the ConfigMap template for runtime access | `false` |
| `operator.enableTestUsers` | Seed the publicly known TEST users into the API credential store (`CLOUDBERRY_ENABLE_TEST_USERS`). Test suites only | `false` |
| `vault.roleID` / `vault.secretID` | Vault AppRole credentials for `vault.authMethod=approle` (rendered as `CLOUDBERRY_VAULT_ROLE_ID`/`CLOUDBERRY_VAULT_SECRET_ID` env vars) | `""` |
| `telemetry.otlpInsecure` | When `true`, the OTLP exporter uses plaintext (non-TLS) connections. Use for local development with collectors that do not have TLS configured | `false` |

**Configuration precedence** (highest wins): environment variable > command-line flag > config file > default. The environment always wins, even over an explicitly set flag.

**Vault token lifecycle**: Vault token renewal and re-authentication are automatic — the operator runs a background Vault `LifetimeWatcher` that renews the login token before expiry and re-authenticates when the token can no longer be renewed (observable via `cloudberry_vault_operations_total{operation="renew"|"reauth"}`). No manual token rotation is required for `kubernetes` or `approle` auth.

### Webhook Configuration Notes

- The `webhook-enabled` field is included in the operator ConfigMap template, allowing the operator to read the webhook configuration at runtime
- CA bundle injection is handled automatically by the operator for both `ValidatingWebhookConfiguration` and `MutatingWebhookConfiguration`
- Webhook certificate namespace handling ensures certificates are created in the correct namespace regardless of the release namespace

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
- If `CLOUDBERRY_API_ADMIN_PASSWORD` is set, the operator uses it and persists it to a Kubernetes Secret (`cloudberry-operator-admin-password`)
- If not set but the Secret exists (from a previous run), the operator reads the password from the Secret — this ensures the password survives pod restarts
- If neither the env var nor the Secret exists, the operator auto-generates a cryptographically secure random password (including special characters), persists it to the Secret, and logs a warning
- The auto-generated password is stable across restarts (persisted to Secret). For explicit control, set `CLOUDBERRY_API_ADMIN_PASSWORD` in production

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

## Monitoring Stack

The operator integrates with monitoring tools for metrics collection and distributed tracing:

### vmagent / VictoriaMetrics

Deploy with a ServiceMonitor for Prometheus-compatible metrics scraping:

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set metrics.enabled=true \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.interval=30s
```

Pre-built Grafana dashboards are available in the `monitoring/grafana/` directory of the source repository.

### OpenTelemetry Collector

Deploy with OTLP tracing for distributed trace collection:

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set telemetry.enabled=true \
  --set telemetry.otlpEndpoint=otel-collector:4317 \
  --set telemetry.otlpProtocol=grpc \
  --set telemetry.otlpInsecure=true
```

The operator emits spans for reconciliation loops, API request handling, database operations, and Vault interactions.

## Mirroring Configuration

The operator supports enabling and disabling segment mirroring on existing clusters. Mirroring is configured in the `CloudberryCluster` CRD:

```yaml
spec:
  segments:
    mirroring:
      enabled: true       # Enable or disable mirroring
      layout: spread      # "group" (default) or "spread"
```

You can toggle mirroring on a running cluster by patching the CR:

```bash
# Enable mirroring
kubectl patch cloudberrycluster my-cluster --type merge \
  -p '{"spec": {"segments": {"mirroring": {"enabled": true}}}}'

# Disable mirroring
kubectl patch cloudberrycluster my-cluster --type merge \
  -p '{"spec": {"segments": {"mirroring": {"enabled": false}}}}'
```

**Requirements:**
- The cluster must be in `Running` phase to enable or disable mirroring
- For `spread` layout, the number of hosts must exceed `primariesPerHost`
- Mirroring enable has a 30-minute timeout; status transitions to `Degraded` on timeout

**Mirroring status values:** `NotConfigured` → `Initializing` → `Syncing` → `InSync`

See the [User Guide](../../docs/user-guide.md#enable-mirroring-on-existing-cluster) for detailed instructions.

## Webhook Configuration

### Certificate Sources

The operator supports three certificate management strategies for admission webhooks:

| Strategy | Configuration | Description |
|----------|--------------|-------------|
| **Self-signed** (default) | `webhook.certSource=self-signed` | Operator generates ECDSA P-256 CA + server certificate. No external dependencies. Auto-rotates when 2/3 of lifetime elapsed |
| **Vault PKI** | `webhook.certSource=vault-pki` | Issues certificates from Vault's PKI secrets engine. Recommended for production |
| **External** | `webhook.certSecretName=<name>` | Use a pre-existing TLS Secret. Set `webhook.caBundle` for static CA injection |

### Vault PKI Integration

To use Vault PKI for webhook certificates:

1. **Prerequisites**: Enable the PKI secrets engine and create a role in Vault:

   ```bash
   vault secrets enable -path=pki pki
   vault write pki/root/generate/internal \
     common_name="cloudberry-operator-ca" ttl=87600h
   vault write pki/roles/cloudberry-operator \
     allowed_domains="cloudberry-system.svc,cloudberry-system.svc.cluster.local,svc.cluster.local" \
     allow_subdomains=true allow_glob_domains=true max_ttl=8760h
   ```

   > The same PKI mount/role also backs **cluster TLS auto-issuance** (see below), whose certificates carry wildcard SANs for per-pod FQDNs (`*.<svc>.<ns>.svc.cluster.local`) — the role therefore needs both `allow_subdomains=true` and `allow_glob_domains=true`, and `allowed_domains` must cover the cluster Service domains.

2. **Deploy** with Vault PKI:

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

3. **Verify** certificates are issued:

   ```bash
   kubectl get secret -n cloudberry-system -l app.kubernetes.io/component=webhook-certs
   kubectl get validatingwebhookconfigurations -o jsonpath='{.items[*].webhooks[*].clientConfig.caBundle}' | head -c 50
   ```

### Cluster TLS Auto-Issuance from Vault PKI

When a `CloudberryCluster` enables both `spec.vault.enabled: true` and `spec.auth.ssl.enabled: true` with a named `certSecret` that does **not** exist, the operator auto-issues the cluster server certificate from the same Vault PKI mount/role configured for webhook certificates (`webhook.vaultPKI.mountPath`/`webhook.vaultPKI.role`) and creates the Secret itself:

- The Secret is **generic (Opaque)** with `tls.crt`, `tls.key`, **and** `ca.crt` (the cluster's `init-tls` container requires the CA)
- Operator-issued certificates are renewed in place once 2/3 of their lifetime has elapsed
- A pre-existing (user-provided) Secret is **never modified**
- Events `ClusterTLSIssued`/`ClusterTLSRenewed`/`ClusterTLSFailed` and the metric `cloudberry_cluster_cert_issuance_total{cluster,namespace,result}` report the outcomes

See the [User Guide](../../docs/user-guide.md#automatic-cluster-tls-issuance-from-vault-pki) for details.

> **Vault Kubernetes Auth (docker-desktop) — `kubernetes.docker.internal` gotcha**: When the operator authenticates to Vault with `vault.authMethod=kubernetes` on Docker Desktop, the Vault Kubernetes auth backend must be configured with `kubernetes_host=https://kubernetes.docker.internal:6443` — **not** `host.docker.internal`. The Docker Desktop API-server serving certificate includes only `kubernetes.docker.internal` in its SANs; using `host.docker.internal` makes Vault's `TokenReview` TLS hostname verification fail, and operator login returns `403 permission denied`. In the bundled test environment, this is handled by `test/docker-compose/scripts/setup-vault-k8s-auth.sh` (run via `make test-env-setup`); deploy the operator into `cloudberry-test` with `make helm-install-test`. See the [Installation Guide](../../docs/installation.md#vault-pki-with-kubernetes-auth-on-docker-desktop-make-targets) for the full flow.

### Webhook Configuration Values

| Parameter | Description | Default |
|-----------|-------------|---------|
| `webhook.enabled` | Enable admission webhooks | `true` |
| `webhook.port` | Webhook service port | `443` |
| `webhook.targetPort` | Target port on the operator | `9443` |
| `webhook.failurePolicy` | Failure policy (`Fail` or `Ignore`) | `Fail` |
| `webhook.certSource` | Certificate source (`self-signed` or `vault-pki`) | `self-signed` |
| `webhook.certSecretName` | TLS certificate secret name (auto-generated if empty) | `""` |
| `webhook.serviceName` | Webhook service name (defaults to `{release}-webhook`) | `""` |
| `webhook.caBundle` | Static CA bundle (base64-encoded) | `""` |
| `webhook.vaultPKI.mountPath` | Vault PKI engine mount path | `pki` |
| `webhook.vaultPKI.role` | Vault PKI role name | `cloudberry-operator` |

### Webhook Runtime Behavior

The deployment template automatically:
- Sets `CLOUDBERRY_WEBHOOK_CERT_SOURCE`, `CLOUDBERRY_WEBHOOK_CERT_SECRET_NAME`, and `CLOUDBERRY_WEBHOOK_SERVICE_NAME` environment variables
- Mounts the certificate Secret at `/tmp/k8s-webhook-server/serving-certs`
- Exposes the webhook port (default `9443`)
- Creates a dedicated webhook Service
- Injects the CA bundle into `ValidatingWebhookConfiguration` and `MutatingWebhookConfiguration` at runtime

## Monitoring Integration

The operator integrates with Prometheus-compatible metrics collectors and OpenTelemetry for distributed tracing.

### Deploying the Monitoring Stack

Use the Makefile targets to deploy the monitoring stack alongside the operator:

```bash
# Deploy vmagent + otel-collector
make monitoring-deploy

# Check status
make monitoring-status

# Remove
make monitoring-undeploy
```

### Metrics Collection (vmagent / Prometheus)

Deploy with a ServiceMonitor for automatic metrics scraping:

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set metrics.enabled=true \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.interval=30s
```

The operator exposes metrics at `/metrics` on port 8080, including:
- Cluster health (`cloudberry_cluster_info`, `cloudberry_coordinator_up`)
- Reconciliation performance (`cloudberry_reconcile_duration_seconds`)
- FTS probing (`cloudberry_fts_probe_total`, `cloudberry_fts_failover_total`)
- Scale operations (`cloudberry_scale_operations_total`, including `rebalance`/`rebalance-failed`)
- PVC sizes (`cloudberry_pvc_size_bytes`, published in steady state on every reconcile)
- REST API server (`cloudberry_api_requests_total`/`_duration_seconds`/`_in_flight`, `cloudberry_api_rate_limit_rejections_total` — all labelled by route template)
- Database client (`cloudberry_db_connect_*`, `cloudberry_db_query_duration_seconds`, `cloudberry_db_pool_acquired/idle/max_conns`)
- Idle daemon and sessions (`cloudberry_idle_daemon_up`, `cloudberry_idle_scan_failures_total`, `cloudberry_session_terminations_total`)
- Security and lifecycle (`cloudberry_cert_rotation_total`, `cloudberry_cluster_cert_issuance_total`, `cloudberry_vault_operations_total` incl. `renew`/`reauth`, `cloudberry_backup_on_delete_total`, `cloudberry_scale_phase_duration_seconds`)

Pre-built Grafana dashboards are available in the `monitoring/grafana/` directory (operator, exporters, node-metrics, and OTel dashboards). The test monitoring stack charts (vmagent, vector, otel-collector, node-exporter) live under `test/monitoring/`.

### Distributed Tracing (OpenTelemetry)

Deploy with OTLP tracing:

```bash
helm install cloudberry-operator deploy/helm/cloudberry-operator \
  --namespace cloudberry-system \
  --create-namespace \
  --set telemetry.enabled=true \
  --set telemetry.otlpEndpoint=otel-collector:4317 \
  --set telemetry.otlpProtocol=grpc \
  --set telemetry.otlpInsecure=true
```

The operator emits spans for reconciliation loops (`Reconcile`, `controller.*`), API request handling (server spans named by route template, e.g. `GET /api/v1alpha1/clusters/{name}`), database operations (`db.*`), authentication (`auth.*`), admission webhooks (`webhook.*`), the idle daemon (`idle.*`), Vault interactions (`vault.*`), and operator startup (`operator.*`). Span names are low-cardinality by design (bounded sets — never raw URL paths or cluster names).

### Monitoring Configuration Values

| Parameter | Description | Default |
|-----------|-------------|---------|
| `metrics.enabled` | Enable metrics endpoint | `true` |
| `metrics.port` | Metrics service port | `8080` |
| `serviceMonitor.enabled` | Create Prometheus ServiceMonitor | `false` |
| `serviceMonitor.interval` | Scrape interval | `30s` |
| `serviceMonitor.scrapeTimeout` | Scrape timeout | `10s` |
| `telemetry.enabled` | Enable OTLP telemetry | `false` |
| `telemetry.otlpEndpoint` | OTLP collector endpoint | `""` |
| `telemetry.otlpProtocol` | OTLP protocol (`grpc` or `http`) | `grpc` |
| `telemetry.samplingRate` | Trace sampling rate (0.0–1.0) | `1.0` |
| `telemetry.otlpInsecure` | Disable TLS for OTLP exporter | `false` |

## Security

The operator includes the following security hardening measures:

- **SQL injection prevention**: All database queries use parameterized queries via pgx. Distribution key handling uses the `sanitizeDistKey()` helper for additional validation
- **Dependency security**: `golang.org/x/net` upgraded to fix GO-2026-5026 vulnerability
- **Port validation**: CRD types validate port values are in the range 1–65535
- **Rate limiting**: Per-IP token bucket rate limiting on API endpoints, plus inter-table delay for rebalance operations
- **Context cancellation**: Database propagation operations check for context cancellation between operations
- **Webhook CA bundle retry**: CA bundle injection uses exponential backoff for transient API server errors
- **Error aggregation**: Sub-component reconciliation uses `errors.Join` to report all errors

## CRDs

The chart includes the CloudberryCluster CRD. Set `installCRDs: false` to skip CRD installation if they are managed separately.
