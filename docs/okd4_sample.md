# Cloudberry Operator on OKD4 / OpenShift — End-to-End Walkthrough

This article walks you through deploying the Cloudberry Operator and a full-featured
`CloudberryCluster` on **OKD4 / OpenShift**, then running a complete nine-step
operational scenario: data generation, scale-out, rebalance, backup, restore, and
PXF external parquet load. Everything you need is embedded here — the Helm values,
the cluster CR, the infrastructure prerequisites, the exact operator commands, and
the verified evidence from the live run.

Unlike a vanilla Kubernetes deployment, OKD enforces
[SecurityContextConstraints](#3-openshift-specific-helm-support) (SCC). This guide
uses a **scoped custom SCC** for the database pods — deliberately **not** the broad
`anyuid` — plus Vault-PKI TLS, Vault Kubernetes auth, Keycloak OIDC, and MinIO
S3 backups.


## Table of Contents

1. [Overview & Architecture](#1-overview--architecture)
2. [Prerequisites](#2-prerequisites)
3. [OpenShift-Specific Helm Support](#3-openshift-specific-helm-support)
4. [Deploy the Operator](#4-deploy-the-operator)
5. [Deploy the Cluster](#5-deploy-the-cluster)
6. [The Nine-Step Scenario](#6-the-nine-step-scenario)
7. [Performance Smoke Summary](#7-performance-smoke-summary)
8. [Troubleshooting & OKD-Specific Notes](#8-troubleshooting--okd-specific-notes)
9. [Definitive Image Tag Table](#9-definitive-image-tag-table)

---

## 1. Overview & Architecture

The Cloudberry Operator runs on OKD4 the same way it runs on vanilla Kubernetes,
with one crucial addition: **OpenShift enablement** in the Helm chart
(`openshift.enabled=true`). This flag switches on three things:

- **OKD-aware operator securityContext** — the operator pod drops its fixed UID
  (`65532`) so OKD assigns a UID from the namespace's allocated range. The operator
  pod is admitted under the built-in **`restricted-v2`** SCC (careful, non-root,
  read-only root filesystem) — never `anyuid`.
- **A scoped custom SCC** for the database workload pods, bound to a single
  ServiceAccount. The Cloudberry image needs a fixed UID 1000 (gpadmin) plus a
  root init container (to `chown` data/TLS volumes), which `restricted-v2` cannot
  satisfy. Rather than reach for `anyuid`, the chart creates a **minimal custom
  SCC** that is strictly narrower than `anyuid` (limited caps, no host namespaces,
  single-SA binding). See [§3](#3-openshift-specific-helm-support).
- **An optional Route** for externally exposing the operator API/metrics with edge
  TLS (off by default; the database is never publicly exposed).

The deployed cluster is fully HA and observable:

- **HA coordinator** with a synchronous standby.
- **Segment mirroring** — every segment has a mirror (`group` layout).
- **TLS from Vault PKI** — the operator issues `okd4-cluster-tls` from the Vault
  PKI engine; the cert chains to *Some Vault Root CA*.
- **PXF sidecars on primary AND mirror pods** — so PXF external loads survive a
  segment failover (the acting primary always has a local PXF on `localhost:5888`).
- **postgres-exporter** on every segment and mirror, plus a
  **cloudberry-query-exporter** on the coordinator.
- **Backups to MinIO** bucket `cloudberry-test`, with S3 credentials read from a
  Vault KV secret.

### Architecture diagram

```
                         OKD4 `svc` cluster  (namespace: cloudberry)
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │                                                                              │
  │   ┌──────────────────────────┐        Vault (PKI `pki`, k8s auth)            │
  │   │  cloudberry-operator     │──k8s-auth──▶  policy: cloudberry              │
  │   │  SCC: restricted-v2      │            role: cloudberry-operator          │
  │   │  webhook cert: Vault PKI │◀─issue─────  PKI role: cloudberry             │
  │   └───────────┬──────────────┘            KV: secret/cloudberry/backup-s3    │
  │      manages  │                            KV: secret/cloudberry (OIDC)      │
  │               ▼                                                              │
  │   ┌─────────────────────────────────────────────────────────────────────┐    │
  │   │  CloudberryCluster: okd4-cluster                                    │    │
  │   │  SCC: cloudberry-operator-cluster (scoped custom, NOT anyuid)       │    │
  │   │                                                                     │    │
  │   │   coordinator-0 ══sync══ coordinator-standby-0                      │    │
  │   │      (query-exporter + postgres-exporter)                           │    │
  │   │                                                                     │    │
  │   │   segment-primary-0 ─mirror─ segment-mirror-0   ┐                   │    │
  │   │   segment-primary-1 ─mirror─ segment-mirror-1   │ each seg + mirror │    │
  │   │   segment-primary-2 ─mirror─ segment-mirror-2   ┘ has PXF + exporter│    │
  │   │        │  udpifc interconnect (UDP+TCP)                             │    │
  │   │        │  TLS: okd4-cluster-tls  (issuer CN=Some Vault Root CA)     │    │
  │   └────────┼────────────────────────────────────────────────────────────┘    │
  │            │ PXF s3a / gpbackup S3                                           │
  │            ▼                                                                 │
  │      MinIO  https://minio.dos.svc.cluster.local                              │
  │      bucket cloudberry-test  (backups/  +  parquet/)                         │
  │                                                                              │
  │   Keycloak DL realm ── OIDC ──▶ operator + cluster auth                      │          
  └──────────────────────────────────────────────────────────────────────────────┘

```

---

## 2. Prerequisites

This is the single comprehensive setup reference for the walkthrough. You reach the
internal OKD4 API only through the **Jump Host** (JH); establish that access first,
then ensure every backing service below is configured to match. Each requirement is
stated **declaratively** — *what must exist and how to verify it* — so you can wire
it with whatever tooling (GitOps, Terraform, Vault/Keycloak/MinIO admin CLIs) your
platform uses.

> **Credentials**: this is a user guide, not a secret store. Wherever a password or
> key would appear, use a placeholder and reference the **secret name / Vault path /
> KV key** instead. Never paste raw secrets into value files or CRs.

### 2.1 Prerequisite summary

| Requirement | Detail                                                                                                                                                    |
|---|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Namespace** | `cloudberry` (always, on OKD4).                                                                                                                           |
| **Storage class** | `managed-csi` (Azure CSI). 20Gi PVC per segment.                                                                                                          |
| **Vault** | Internal `http://vault.vault.svc:8200`; PKI mount `pki`; Kubernetes auth enabled; policy `cloudberry`; role `cloudberry-operator`; PKI role `cloudberry`. |
| **Keycloak** | DL realm issuer `https://keycloak.apps.svc.demo.ai/realms/cl`; confidential client `cloudberry`.                                                          |
| **MinIO** | Internal S3 `https://minio.minio.svc.cluster.local` (path-style); bucket `cloudberry-test`; S3 creds stored in Vault KV `secret/cloudberry/backup-s3`.    |

Each backing service is expanded into its own subsection below. The definitive image
tags used by this scenario are listed in [§9](#9-definitive-image-tag-table).

### 2.2 OKD4 / platform

- **Access model** — the internal OKD4 API is reachable only through the **Jump
  Host**. Run all `oc`/`kubectl` and admin CLI commands from a context that can
  reach the JH; the database is never publicly exposed.
- **Namespace** — everything lands in `cloudberry`.
- **Storage class** — `managed-csi` (Azure CSI). Each segment (and the coordinator
  and standby) provisions a **20Gi PVC**.
- **Worker sizing** — workers are **4 vCPU / 16 GB**. The embedded CR requests are
  sized to fit this envelope; keep coordinator/standby/segment requests modest.
- **SCC model (summary)** — the **operator** pod runs under the built-in
  **`restricted-v2`** SCC (non-root, read-only root filesystem). The **database**
  pods are admitted under a **scoped custom SCC** — deliberately **not** the broad
  `anyuid` — that permits UID 1000 (gpadmin) plus a root init container while staying
  strictly narrower than `anyuid`. Full details in
  [§3 OpenShift-Specific Helm Support](#3-openshift-specific-helm-support).


- **OIDC client secret** — the Keycloak client secret is provided out-of-band as a
  Kubernetes Secret named **`cloudberry-oidc`** with key **`client-secret`**
  (see [§2.4](#24-keycloak-configuration)):

  ```bash
  oc -n cloudberry create secret generic cloudberry-oidc \
    --from-literal=client-secret='<keycloak-client-secret>'   # placeholder
  ```

#### Images

All images must exist in  `ghcr.io` repositories.
[Definitive Image Tag Table](#9-definitive-image-tag-table):
`cloudberry-k8s-operator`, `cloudberry-official-pxf`, `cloudberry-pxf`,
`cloudberry-backup`, `cloudberry-query-exporter`, and `postgres-exporter`. 


### 2.3 Vault configuration

The operator authenticates to Vault with the Kubernetes auth method, issues webhook
and cluster TLS from Vault PKI, and reads S3 backup credentials from Vault KV.

- **Address** — internal `http://vault.vault.svc:8200`.
- **PKI** — engine mounted at **`pki`**. Certificates chain to *Some Vault Root CA*.
- **Kubernetes auth** — `auth/kubernetes` enabled, configured with the cluster's
  reviewer JWT, CA, and API host.
- **Policy `cloudberry`** — grants `pki/issue/cloudberry` (create/update) and read on
  `secret/data/cloudberry` and `secret/data/cloudberry/*`.
- **Role `cloudberry-operator`** — bound to
  `bound_service_account_names=<operator-sa>` and
  `bound_service_account_namespaces=cloudberry`, mapped to the `cloudberry` policy.
- **PKI role `cloudberry`** — `allowed_domains` covering the operator webhook DNS
  (`*.cloudberry.svc`, `*.cloudberry.svc.cluster.local`) and the cluster service DNS
  (cluster service names under `svc.cluster.local`), with `allow_subdomains=true`.
  Issued certificates chain to *Some Vault Root CA*.

Smoke/verify (the Vault CLI is a legitimate operator tool):

```bash
# The operator SA can log in and receives the cloudberry policy
vault write auth/kubernetes/login role=cloudberry-operator jwt=<sa-jwt>

# The PKI role can issue a cert for a cloudberry SAN
vault write pki/issue/cloudberry \
  common_name=okd4-cluster.cloudberry.svc.cluster.local

# The backup S3 credentials are present (values redacted)
vault kv get secret/cloudberry/backup-s3
```

### 2.4 Keycloak configuration

- **Issuer** — DL realm issuer `https://keycloak.apps.svc.demo.ai/realms/cl`.
- **Client** — a **confidential** client named **`cloudberry`**, with redirect URIs
  for the operator and a **group-membership mapper** configured with
  `full.path=false`.
- **Client secret** — stored in the Kubernetes Secret **`cloudberry-oidc`** under key
  **`client-secret`** (created in [§2.2](#22-okd4--platform)); the operator values
  reference it via `existingSecret`/`existingSecretKey`.

Verify the realm's OIDC discovery document is reachable:

```bash
curl -s https://keycloak.apps.svc.demo.ai/realms/cl/.well-known/openid-configuration | head
```

### 2.5 MinIO configuration

MinIO is the S3 target for both gpbackup backups and PXF `s3:parquet` external data.

- **Endpoint** — internal S3 `https://minio.minio.svc.cluster.local`, **path-style**
  addressing, served with an internal cert-manager certificate (*"Some CA"*).
- **Bucket** — **`cloudberry-test`**, with `backups/` and `parquet/` prefixes.
- **Credentials in Vault** — the S3 access/secret keys are seeded into Vault KV
  **`secret/cloudberry/backup-s3`** under keys **`aws_access_key_id`** and
  **`aws_secret_access_key`**. The operator **materializes** these into the
  `okd4-cluster-backup-s3-vault-creds` Secret when it reconciles the cluster; that
  Secret backs both the backup destination and the PXF S3 server.
- **Internal-CA trust** — because MinIO's certificate is issued by the internal
  *Some CA*, the **gpbackup S3 plugin** (postgres process env) and the **PXF JVM
  truststore** must trust that CA, or S3 TLS fails with
  `x509: certificate signed by unknown authority`. See
  [§8 Troubleshooting](#8-troubleshooting--okd-specific-notes) for how the CA is
  injected.

Verify the KV entry exists (values redacted):

```bash
vault kv get secret/cloudberry/backup-s3      # returns both fields
```

---

## 3. OpenShift-Specific Helm Support

The chart is **backward-compatible**: with `openshift.enabled=false` (the default)
it renders exactly as it does for vanilla Kubernetes. Setting `openshift.enabled=true`
turns on the OKD-specific behavior.

### 3.1 `openshift.enabled` and OKD-aware securityContext

When `openshift.enabled=true`, the operator Deployment **drops the fixed
`runAsUser`/`fsGroup`** so OKD's admission injects a UID from the namespace's
allocated range. The pod keeps `runAsNonRoot: true`, `allowPrivilegeEscalation: false`,
`readOnlyRootFilesystem: true`, `drop: [ALL]`, and the `RuntimeDefault` seccomp
profile. The result: the operator pod is admitted under **`restricted-v2`**.

### 3.2 The scoped SCC — why not `anyuid`

The Cloudberry database pods have hard requirements the built-in `restricted-v2`
cannot meet:

- The main DB container runs as **UID 1000** (gpadmin), a fixed UID outside the
  namespace's allocated range.
- The init container runs as **root (UID 0)** to `chown` the data and TLS volumes.

`anyuid` would satisfy this, but it is far too broad. Instead the chart templates a
**scoped custom SCC** — `<release>-cloudberry-operator-cluster` — that is strictly
narrower than `anyuid`:

| Property | Value | Why it is narrower than `anyuid` |
|---|---|---|
| `runAsUser.type` | `RunAsAny` | Needed for the non-contiguous {0, 1000} UID set, but bound to a single SA only. |
| `allowedCapabilities` | `CHOWN, DAC_OVERRIDE, FOWNER, SETGID, SETUID` | Only the file-ownership caps the chown init needs — nothing else. |
| `requiredDropCapabilities` | `KILL, MKNOD, NET_RAW` | Explicitly drops dangerous caps. |
| `allowPrivilegedContainer` | `false` | No privileged containers. |
| `allowPrivilegeEscalation` | `false` | No escalation. |
| host namespaces | all `false` | No `allowHostNetwork/PID/IPC/Ports/DirVolumePlugin`. |
| binding | single ServiceAccount (`cloudberry:default`) | Not `system:authenticated`, not cluster-wide. |

The SCC (and its `use` ClusterRole + RoleBinding) is created via a Helm
`pre-install,pre-upgrade` hook so it exists **before** the cluster pods are admitted.
The operator does not set an explicit `ServiceAccountName` on the DB pod template,
so cluster pods run under the namespace `default` SA — that is the exact SA the SCC
is bound to.

```yaml
# Rendered SCC (excerpt) — chart template templates/openshift/scc.yaml
kind: SecurityContextConstraints
metadata:
  name: cloudberry-operator-cluster
allowPrivilegedContainer: false
allowPrivilegeEscalation: false
allowHostNetwork: false
allowHostPID: false
allowHostIPC: false
allowedCapabilities: [CHOWN, DAC_OVERRIDE, FOWNER, SETGID, SETUID]
requiredDropCapabilities: [KILL, MKNOD, NET_RAW]
runAsUser: { type: RunAsAny }
seLinuxContext: { type: MustRunAs }
users:
  - system:serviceaccount:cloudberry:default   # single-SA binding
```

> **Verify at runtime**: the DB pods carry annotation
> `openshift.io/scc: cloudberry-operator-cluster` — **not** `anyuid`, **not**
> `restricted-v2`. A `grep -ri anyuid deploy/helm/cloudberry-operator` finds only
> explanatory comments; no `anyuid` SCC or binding is rendered.

### 3.3 Optional Route

`route.enabled` (default `false`) renders a `route.openshift.io/v1` Route with edge
TLS termination pointing at the operator service — only for the operator API/metrics
if you need external access. The database is never exposed this way. This scenario
leaves it disabled.

---

## 4. Deploy the Operator

Install the operator in `cloudberry` with the OKD4 values file. The full file is
embedded below.

```bash
helm upgrade --install cloudberry-operator \
  deploy/helm/cloudberry-operator \
  --namespace cloudberry --create-namespace \
  -f deploy/helm/cloudberry-operator/okd4-example.yaml \
  --wait --timeout 10m
```

### 4.1 `okd4-example.yaml` (embedded, verbatim)

```yaml
# =============================================================================
# Cloudberry Operator — OKD4 / OpenShift Helm VALUES (namespace: cloudberry)
# =============================================================================
# Saved Helm values for installing the cloudberry-operator on the OKD4 `svc`
# cluster in the `cloudberry` namespace:
#
#   helm upgrade --install cloudberry-operator \
#     deploy/helm/cloudberry-operator \
#     -n cloudberry --create-namespace \
#     -f deploy/helm/cloudberry-operator/okd4-example.yaml
#
# Highlights:
#   - openshift.enabled=true → OKD-aware operator securityContext (no fixed UID),
#     operator SA bound to restricted-v2, and a SCOPED custom SCC bound only to
#     the CloudberryCluster workload ServiceAccount (NOT anyuid).
#   - Webhook certs issued from Vault PKI; Vault Kubernetes auth; Keycloak OIDC.
# =============================================================================

replicaCount: 1

# All operator images come from ghcr.io/vyrodovalexey.
image:
  repository: ghcr.io/vyrodovalexey/cloudberry-k8s-operator
  pullPolicy: IfNotPresent
  tag: "0.9.5-cldb-2.1.0"


installCRDs: true

# -----------------------------------------------------------------------------
# OpenShift / OKD4 enablement
# -----------------------------------------------------------------------------
openshift:
  enabled: true
  # Operator pod runs non-root / read-only → the built-in restricted-v2 SCC.
  operatorSCC: restricted-v2
  # Create the scoped minimal custom SCC for the cluster workload pods.
  createClusterSCC: true
  # The operator does NOT set an explicit ServiceAccountName on the DB pod
  # template, so cluster pods run under the namespace `default` ServiceAccount.
  # The custom SCC is bound ONLY to this SA in cloudberry.
  clusterServiceAccount: default
  scc:
    # Name auto-generates to "<release>-cloudberry-operator-cluster".
    name: ""
    priority: 10
    # RunAsAny is required for the non-contiguous {0 (root init), 1000 (gpadmin)}
    # UID set. Still strictly narrower than anyuid: limited caps, no host access,
    # single-SA binding.
    runAsUser: RunAsAny
    allowedCapabilities:
      - CHOWN
      - DAC_OVERRIDE
      - FOWNER
      - SETGID
      - SETUID
    requiredDropCapabilities:
      - KILL
      - MKNOD
      - NET_RAW

# Route disabled — the operator API/metrics are not externally exposed by default.
route:
  enabled: false

# -----------------------------------------------------------------------------
# Webhook — certificates issued from Vault PKI
# -----------------------------------------------------------------------------
webhook:
  enabled: true
  certSource: vault-pki
  vaultPKI:
    mountPath: pki
    # ASSUMPTION / DevOps action required: this uses the Vault PKI role
    # `cloudberry`. That role MUST exist and its allowed_domains MUST cover the
    # operator webhook DNS (*.cloudberry.svc, *.cloudberry.svc.cluster.local) and
    # svc.cluster.local + cloudberry SANs.
    role: cloudberry

# -----------------------------------------------------------------------------
# Vault — Kubernetes auth
# -----------------------------------------------------------------------------
vault:
  enabled: true
  address: "http://vault.vault.svc:8200"
  authMethod: kubernetes
  authPath: auth/kubernetes
  role: cloudberry-operator
  secretPath: secret/data/cloudberry
  # PKI defaults for the vault-pki webhook cert source (aligns with webhook.vaultPKI).
  pkiMountPath: pki
  pkiRole: cloudberry

# -----------------------------------------------------------------------------
# OIDC — Keycloak DL realm
# -----------------------------------------------------------------------------
oidc:
  enabled: true
  issuerURL: "https://keycloak.apps.svc.demo.ai/realms/cl"
  clientID: cloudberry
  # Client secret provided out-of-band as a Secret in cloudberry.
  existingSecret: cloudberry-oidc
  existingSecretKey: client-secret

# -----------------------------------------------------------------------------
# Telemetry / Metrics
# -----------------------------------------------------------------------------
telemetry:
  enabled: true
  otlpEndpoint: "otel-collector.observability.svc:4317"
  otlpProtocol: grpc
  otlpInsecure: true
  samplingRate: 1.0
  serviceName: cloudberry-operator

metrics:
  enabled: true
  port: 8080

# ServiceMonitor optional — enable if the Prometheus Operator is installed.
serviceMonitor:
  enabled: false
  interval: 30s
  scrapeTimeout: 10s

api:
  enabled: true
  port: 8090
```

### 4.2 Verify the operator

```bash
# Operator deployment Available; pod Running
oc -n cloudberry get deploy cloudberry-operator
oc -n cloudberry get pods -l app.kubernetes.io/name=cloudberry-operator

# Operator SCC is restricted-v2 (careful, NOT anyuid)
OP=$(oc -n cloudberry get pod -l app.kubernetes.io/name=cloudberry-operator \
      -o jsonpath='{.items[0].metadata.name}')
oc -n cloudberry get pod "$OP" -o jsonpath='{.metadata.annotations.openshift\.io/scc}'
# → restricted-v2

# Webhook admitting: non-empty CA bundle injected from Vault PKI
oc get validatingwebhookconfiguration -l app.kubernetes.io/name=cloudberry-operator \
  -o jsonpath='{.items[*].webhooks[*].clientConfig.caBundle}' | head -c 40
# → caBundle length 1544B (admitting)
```

**Verified evidence (ITERATION 4):** operator SCC `restricted-v2`; webhook
`caBundle=1544B` admitting; operator image `okd4fix29`.

---

## 5. Deploy the Cluster

Apply the `CloudberryCluster` CR **after** the operator is Running. The full CR is
embedded below.

```bash
oc apply -f deploy/helm/cloudberry-operator/config/samples/okd4-cluster.yaml -n cloudberry
```

### 5.1 `okd4-cluster.yaml` (embedded, verbatim)

```yaml
# =============================================================================
# OKD4 CloudberryCluster — namespace: cloudberry
# =============================================================================
# End-to-end CloudberryCluster for the OKD4 `svc` cluster. Deployed AFTER the
# operator (okd4-example.yaml) is installed in cloudberry. . Admitted by the scoped custom SCC
# (<release>-cloudberry-operator-cluster) bound to the cloudberry `default`
# ServiceAccount.
#
# Layout:
#   - DB image ghcr.io/vyrodovalexey/cloudberry-k8s-official-pxf:0.9.5-cldb-2.1.0 (pxf + pxf_fdw available)
#   - HA coordinator with standby
#   - 2 segments, primariesPerHost=2, 20Gi PVC/segment, group mirroring
#   - TLS from Vault PKI (auth.ssl, certSecret okd4-cluster-tls, not pre-created)
#   - Keycloak OIDC (cl realm) + Vault KV secrets + Vault k8s auth
#   - postgres-exporter (segments + mirrors) + cloudberry-query-exporter
#   - PXF s3:parquet external load from MinIO cloudberry-test
#   - Backup → MinIO cloudberry-test with S3 creds from Vault KV
#   - deletionPolicy Retain
#
# Resources are modest (workers are 4 vCPU / 16 GB).
# =============================================================================
apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: okd4-cluster
  namespace: cloudberry
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    platform: okd4
spec:
  version: "2.1.0"
  image: "ghcr.io/vyrodovalexey/cloudberry-k8s-official-pxf:0.9.5-cldb-2.1.0"
  imagePullPolicy: IfNotPresent
  imagePullSecrets:
    - name: acr-pull-secret

  coordinator:
    replicas: 1
    resources:
      requests:
        cpu: "500m"
        memory: "2Gi"
      limits:
        cpu: "2"
        memory: "4Gi"
    storage:
      size: "20Gi"

  standby:
    enabled: true
    resources:
      requests:
        cpu: "500m"
        memory: "2Gi"
      limits:
        cpu: "2"
        memory: "4Gi"
    storage:
      size: "20Gi"

  segments:
    count: 2
    primariesPerHost: 2
    resources:
      requests:
        cpu: "500m"
        memory: "2Gi"
      limits:
        cpu: "2"
        memory: "4Gi"
    storage:
      size: "20Gi"
    mirroring:
      enabled: true
      layout: group
    antiAffinity: preferred

  auth:
    basic:
      enabled: true
      adminUser: gpadmin
    ssl:
      # TLS certificates issued from Vault PKI (stored in okd4-cluster-tls).
      enabled: true
      certSecret:
        name: okd4-cluster-tls
      minTLSVersion: "1.2"
    oidc:
      enabled: true
      issuerURL: "https://keycloak.apps.svc.demo.ai/realms/cl"
      clientID: "cloudberry"
      clientSecret:
        secretRef:
          name: cloudberry-oidc
          key: client-secret
      scopes:
        - openid
        - profile
        - email
      roleClaimPath: "realm_access.roles"
      roleClaimSource: id_token
      roleMatchMode: exact
      roleMapping:
        admin: Admin
        user: Basic
        reader: "Self Only"
      pkce: true
      allowLocalSignIn: true
    hbaRules:
      - type: local
        database: all
        user: gpadmin
        method: trust
      - type: host
        database: all
        user: all
        address: "0.0.0.0/0"
        method: scram-sha-256

  ha:
    ftsProbeInterval: 60
    ftsProbeTimeout: 20
    ftsProbeRetries: 5

  vault:
    enabled: true
    address: "http://vault.vault.svc:8200"
    authMethod: kubernetes
    authPath: "auth/kubernetes"
    role: "cloudberry-operator"
    secretPath: "secret/data/cloudberry"

  queryMonitoring:
    enabled: true
    guestAccess: false
    historyRetention: "30d"
    samplingInterval: 6
    planCollection: true
    slowQueryThreshold: "1000ms"
    exporters:
      postgresExporter:
        enabled: true
        image: "prometheuscommunity/postgres-exporter:v0.16.0"
        port: 9187
        segments: true
        mirrors: true
      cloudberryQueryExporter:
        enabled: true
        image: "ghcr.io/vyrodovalexey/cloudberry-k8s-query-exporter:0.9.5-cldb-2.1.0"
        port: 9188

  monitoring:
    enabled: true
    metricsPort: 9187
    serviceMonitor: false

  # ---------------------------------------------------------------------------
  # Data loading — PXF s3:parquet from MinIO cloudberry-test
  # ---------------------------------------------------------------------------
  dataLoading:
    enabled: true
    pxf:
      enabled: true
      image: "ghcr.io/vyrodovalexey/cloudberry-k8s-official-pxf:0.9.5-cldb-2.1.0"
      jvmOpts: "-Xmx1g -Xms256m"
      port: 5888
      logLevel: INFO
      extensions:
        pxf: true
        pxfFdw: true
      servers:
        - name: minio-cloudberry
          type: s3
          config:
            fs.s3a.endpoint: "https://minio.minio.svc.cluster.local"
            fs.s3a.path.style.access: "true"
          credentialSecrets:
            # Materialized from Vault KV by the operator's backup credential flow
            # (see backup.destination.s3.vaultSecret below).
            - name: okd4-cluster-backup-s3-vault-creds
              key: aws_access_key_id
            - name: okd4-cluster-backup-s3-vault-creds
              key: aws_secret_access_key
      resources:
        requests:
          cpu: "250m"
          memory: "512Mi"
        limits:
          cpu: "1"
          memory: "1Gi"
    jobs:
      # s3:parquet read from cloudberry-test into a target table (scenario step 9).
      - name: s3-parquet-read
        type: pxf
        enabled: true
        pxfJob:
          server: minio-cloudberry
          profile: "s3:parquet"
          resource: "cloudberry-test/parquet/data.parquet"
          targetTable: "public.okd4_parquet_read"

  # ---------------------------------------------------------------------------
  # Backup — MinIO cloudberry-test, S3 credentials from Vault KV
  # ---------------------------------------------------------------------------
  backup:
    enabled: true
    schedule: "0 2 * * *"
    image: "ghcr.io/vyrodovalexey/cloudberry-k8s-backup:0.9.5-cldb-2.1.0"
    retention:
      fullCount: 3
      incrementalCount: 10
      maxAge: "30d"
    destination:
      type: s3
      s3:
        bucket: cloudberry-test
        endpoint: "https://minio.minio.svc.cluster.local"
        region: us-east-1
        folder: /backups
        encryption: "on"
        forcePathStyle: true
        vaultSecret:
          path: "secret/cloudberry/backup-s3"
          accessKeyField: aws_access_key_id
          secretKeyField: aws_secret_access_key
        multipart:
          backupMaxConcurrentRequests: 4
          backupMultipartChunksize: "10MB"
          restoreMaxConcurrentRequests: 4
          restoreMultipartChunksize: "10MB"

  deletionPolicy: Retain
```

> **Optional: spread roles across the worker nodes with `spec.affinity`.** The CR
> above uses only the default `segments.antiAffinity: preferred` (same-role segment
> spread). To also spread the coordinator, standby, segment primaries/mirrors, and
> the backup Job across the OKD4 worker nodes, add a cluster-level `affinity` block
> with `mode: full`:
>
> ```yaml
> spec:
>   affinity:
>     mode: full
>     type: required          # hard for coordinator<->standby and coordinator<->backup
>     topologyKey: kubernetes.io/hostname
> ```
>
> The coordinator↔standby and coordinator↔backup terms honor `type: required` (each
> targets the single coordinator pod, so they are safe with ≥ 2 workers). The
> segment primary↔mirror term stays **best-effort (preferred)** regardless — the
> webhook emits a non-fatal Warning — so segment/mirror separation across the
> workers is preferred, not guaranteed. This is fully compatible with the scoped
> custom SCC deployment (anti-affinity affects only pod scheduling, not the pods'
> SecurityContext or SCC admission). If you taint a worker for the standby, also add
> `spec.standby.tolerations` so the standby can still be scheduled there.

### 5.2 Verify the cluster

```bash
# All pods Running: coordinator + standby + 2 primary + 2 mirror
oc -n cloudberry get pods -l app.kubernetes.io/managed-by=cloudberry-operator

# DB pods admitted under the scoped custom SCC (NOT anyuid, NOT restricted-v2)
DB=$(oc -n cloudberry get pod -l app.kubernetes.io/component=coordinator \
      -o jsonpath='{.items[0].metadata.name}')
oc -n cloudberry get pod "$DB" -o jsonpath='{.metadata.annotations.openshift\.io/scc}'
# → cloudberry-operator-cluster

# Every segment mirrored, all up; standby streaming
psql -c "SELECT content, role, preferred_role, mode, status
         FROM gp_segment_configuration ORDER BY content;"

# TLS secret issued by Vault PKI, chains to Vault Root CA
oc -n cloudberry get secret okd4-cluster-tls -o jsonpath='{.data.tls\.crt}' \
  | base64 -d | openssl x509 -noout -issuer
# → issuer=CN=Some Vault Root CA
```

**Verified evidence (ITERATION 4):** coordinator + standby + 2 primary + 2 mirror
Running; every segment mirrored (all `s/u`); standby `streaming/sync`;
`okd4-cluster-tls` issuer `CN=Some Vault Root CA`; cluster SCC
`cloudberry-operator-cluster`; **PXF sidecar present + Ready on BOTH primary AND
mirror pods**.

```
gp_segment_configuration (content|role|preferred_role|mode|status):
  -1|p|p|s|u   0|m|m|s|u   0|p|p|s|u   1|m|m|s|u   1|p|p|s|u
custom SCC caps: allowed=[CHOWN,DAC_OVERRIDE,FOWNER,SETGID,SETUID]
                 drop=[KILL,MKNOD,NET_RAW]  allowPrivEsc=false  privileged=false
```

---

## 6. The Nine-Step Scenario

This section walks the complete operational scenario against the running cluster:
verify → generate → scale-out → rebalance → backup → drop → restore → generate
parquet → external PXF load. Each step is described as an **operation** with the
underlying `oc`/`psql`/SQL you run (on the Jump Host over SSH), followed by the
**verified evidence** captured from the live ITERATION 4 run. All parameters
(cluster name `okd4-cluster`, namespace `cloudberry`, S3 bucket `cloudberry-test`)
are the values used throughout this guide.

> **OKD-specific configuration notes**: a few OKD/SCC product gaps require
> additional, idempotent configuration alongside this scenario — an
> allow-intra-namespace **interconnect NetworkPolicy**, injecting the MinIO internal
> **CA into the DB StatefulSet env and PXF JVM truststore**, and confirming the
> **rootless sshd on port 2022**. These are documented, one-time setup — not product
> regressions — and are detailed in
> [§8 Troubleshooting](#8-troubleshooting--okd-specific-notes).

### Step 1 — Verify the deployment

**What this step does:** re-confirms the deployment is healthy before running the
data operations. Assert the operator SCC is `restricted-v2`; the webhook is
admitting; coordinator + standby + 2 primary + 2 mirror are Running; every segment
is mirrored (all up); the standby is `streaming/sync`; `okd4-cluster-tls` is issued
by Vault PKI; the cluster SCC is `cloudberry-operator-cluster` (NOT anyuid); and —
crucially for D9 — the **PXF sidecar is present + Ready on both primary AND mirror
pods**. These are the same checks shown in [§5.2](#52-verify-the-cluster).

**Verified evidence:** PASS (150s) — all assertions above hold.

### Step 2 — Generate ~1000 MB in `mydb`

**What this step does:** creates `mydb` and three MPP tables — `customers`,
`orders`, `lineitem` — with indexes, sized to ~1 GB. Generation is idempotent (a
second run detects existing data and skips regeneration). The DDL and data
generation are plain SQL:

- **`customers`** `DISTRIBUTED BY (c_custkey)` — 200,000 rows; indexes on
  `c_mktsegment`.
- **`orders`** `DISTRIBUTED BY (o_orderkey)` — 800,000 rows; indexes on `o_custkey`
  and `o_orderdate`.
- **`lineitem`** `DISTRIBUTED BY (l_orderkey)` (the bulk table) — 4,500,000 rows;
  indexes on `l_orderkey` and `l_shipdate`.
- `ANALYZE` on all three.

Data is generated with `generate_series` bulk inserts (row-size targets baked into
the SQL); pass row counts as `-v cust_rows=... -v order_rows=... -v line_rows=...`.

**Verify:**

```sql
SELECT pg_size_pretty(pg_database_size('mydb'));   -- 1010 MB
SELECT count(*) FROM customers;                    -- 200000
SELECT count(*) FROM orders;                        -- 800000
SELECT count(*) FROM lineitem;                       -- 4500000
EXPLAIN SELECT ... FROM orders WHERE o_orderdate BETWEEN ...;  -- Index Scan using orders_orderdate_idx
```

**Verified evidence:** PASS (105s). `mydb` = **1010 MB**; counts 200k/800k/4.5M;
index scan confirmed in `EXPLAIN`; indexed lookup returns 548; idempotent (2nd run
skips regen).

### Step 3 — Scale-out 2 → 3 segments

**What this step does:** patches `spec.segments.count` from 2 to 3 and lets the
operator drive the expansion:

```bash
oc -n cloudberry patch cloudberrycluster okd4-cluster --type=merge \
  -p '{"spec":{"segments":{"count":3}}}'
```

Scale-out on OKD uses a **gpexpand-driven coordinator-exec Job**, not a pure-SQL
seed. Because a database's segment membership is fixed at `CREATE DATABASE` time, a
late-added segment must be **physically seeded** (basebackup of the coordinator
template) — only `gpexpand` does that. The mechanism, briefly:

- The operator creates a **Kubernetes Job** that `kubectl exec`s into
  `okd4-cluster-coordinator-0` and runs real `gpexpand` (`-i` add+init, then `-a`
  redistribute, then `--clean`), reusing the same coordinator-exec pattern as the
  backup subsystem.
- gpexpand dispatches to the new segment over **SSH on port 2022** (a rootless sshd
  is baked into the image; a per-host `~/.ssh/config` `Port 2022` override
  redirects gpexpand's ssh client) — because the scoped SCC forbids binding the
  privileged port 22.
- A `dnsConfig` and a baked `pg_ctl` shim / `gp_contentid` handling let the new
  segment pod be initialized **by gpexpand** rather than self-`initdb`-ing an empty
  cluster.
- On completion the operator **auto-finalizes** the gpexpand status schema
  (`DROP SCHEMA gpexpand CASCADE`) so a later gpbackup is never blocked by
  "expansion currently in process".

**Verify:**

```sql
-- numsegments advanced to 3 on every table
SELECT numsegments FROM gp_distribution_policy
  WHERE localoid = 'lineitem'::regclass;            -- 3
-- data present on all 3 segments
SELECT gp_segment_id, count(*) FROM lineitem GROUP BY 1 ORDER BY 1;
```

**Verified evidence:** PASS (417s). gpexpand green; **numsegments=3**
(customers/orders/lineitem); **3 primaries + 3 mirrors up**; segments-with-data = 3;
per-segment lineitem **1,499,245 / 1,496,991 / 1,503,764**; lineitem preserved
(4.5M); gpexpand status schema **auto-finalized** (gpbackup not blocked).

### Step 4 — Storage rebalance

**What this step does:** confirms the gpexpand redistribution produced an even 3-way
split with no data loss. Re-check the per-segment row distribution:

```sql
SELECT gp_segment_id, count(*) FROM lineitem GROUP BY 1 ORDER BY 1;
```

**Verified evidence:** PASS (76s). numsegments=3 all tables; per-segment lineitem
**1,499,245 / 1,496,991 / 1,503,764**; **skew 0%**; no data loss.

### Step 5 — Backup `mydb` → MinIO `cloudberry-test`

**What this step does:** runs `gpbackup` of `mydb` **from inside the coordinator
pod** (the coordinator dispatches to every segment over SSH — the correct MPP
model). S3 credentials come from the Vault-materialized Secret
`okd4-cluster-backup-s3-vault-creds`, and the MinIO internal CA is
present in the trust bundle (`AWS_CA_BUNDLE`/`SSL_CERT_FILE`, see
[§8](#8-troubleshooting--okd-specific-notes)) so the S3 plugin's TLS to MinIO
verifies. The gpbackup plugin config (rendered in-pod) targets
`endpoint=https://minio.dos.svc.cluster.local`, `bucket=cloudberry-test`,
`folder=/backups`. Capture the 14-digit gpbackup timestamp for the restore. You can
drive the backup by exec'ing into the coordinator:

```bash
oc -n cloudberry exec -it okd4-cluster-coordinator-0 -c coordinator -- \
  gpbackup --dbname mydb --plugin-config /path/to/s3-plugin-config.yaml
# capture the reported timestamp (YYYYMMDDHHMMSS) for the restore step
```

**Verified evidence:** PASS (1018s). gpbackup success across 3 segments;
**not blocked** by any leftover gpexpand schema (auto-finalized); timestamp
**`20260707053455`**; objects present in `cloudberry-test/backups/`.

### Step 6 — Drop `mydb`

**What this step does:** drops the database to prove the restore reconstructs it from
scratch:

```sql
DROP DATABASE mydb;
```

**Verified evidence:** PASS (32s). `pg_database` count 1 → 0.

### Step 7 — Restore `mydb`

**What this step does:** runs `gprestore` from the timestamp captured in Step 5 and
asserts row counts match the pre-backup baseline:

```bash
oc -n cloudberry exec -it okd4-cluster-coordinator-0 -c coordinator -- \
  gprestore --timestamp 20260707053455 \
  --plugin-config /path/to/s3-plugin-config.yaml --create-db
```

**Verified evidence:** PASS (180s). gprestore success; customers=200,000,
orders=800,000, lineitem=4,500,000 — all match baseline.

### Step 8 — Generate ~100 MB `s3:parquet`

**What this step does:** builds a staging table and writes it to
`cloudberry-test/parquet/` through a **writable PXF `s3:parquet`** external table.
The writable/readable external tables use
`LOCATION ('pxf://cloudberry-test/parquet?PROFILE=s3:parquet&SERVER=minio-cloudberry')`.
The DDL and `INSERT ... SELECT` are plain SQL run on the coordinator, for example:

```sql
CREATE WRITABLE EXTERNAL TABLE ext_parquet_write (...)
  LOCATION ('pxf://cloudberry-test/parquet?PROFILE=s3:parquet&SERVER=minio-cloudberry')
  FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');
INSERT INTO ext_parquet_write SELECT ... FROM staging;
```

**Verified evidence:** PASS (258s). Writable PXF s3:parquet; **3 objects, 107 MiB**
in `cloudberry-test/parquet/`; 1,000,000 rows written.

### Step 9 — External PXF `s3:parquet` load

**What this step does:** reads the parquet back through a **readable PXF
`s3:parquet`** external table and loads it into a target table:

```sql
CREATE EXTERNAL TABLE ext_parquet_read (...)
  LOCATION ('pxf://cloudberry-test/parquet?PROFILE=s3:parquet&SERVER=minio-cloudberry')
  FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
INSERT INTO public.okd4_parquet_read SELECT * FROM ext_parquet_read;
SELECT count(*) FROM public.okd4_parquet_read;   -- 1000000
```

**Verified evidence:** PASS (142s). Readable s3:parquet **`SELECT count(*) =
1,000,000`** (matches the dataset); loaded to target = 1,000,000; category aggregate
5×200,000.

> **D9 — PXF survives failover (verified live):** during steps 8/9 content-2's
> acting primary was on a **mirror pod** (`okd4-cluster-segment-mirror-2`). Because
> the PXF sidecar now runs on mirror pods too, PXF followed the acting primary and
> both the parquet write (3 objects / 107 MiB / 1M rows) and the external read
> (1,000,000 rows) **succeeded** — proving PXF survives a segment failover.

### Scenario summary

| Step | Name | Result | Key evidence |
|---|---|---|---|
| 1 | verify deploy | PASS | operator SCC `restricted-v2`; webhook 1544B; coord+standby+2p+2m; every segment mirrored; PXF on primary AND mirror |
| 2 | generate ~1000 MB | PASS | mydb 1010 MB; 200k/800k/4.5M; index scan; idempotent |
| 3 | scale-out 2→3 | PASS | numsegments=3; 3p+3m; per-seg 1499245/1496991/1503764; gpexpand auto-finalized |
| 4 | rebalance | PASS | skew 0%; no data loss |
| 5 | backup → MinIO | PASS | ts `20260707053455`; objects in `cloudberry-test/backups/` |
| 6 | drop mydb | PASS | pg_database 1→0 |
| 7 | restore | PASS | counts match baseline (200k/800k/4.5M) |
| 8 | generate 100 MB parquet | PASS | 3 objects, 107 MiB; 1,000,000 rows |
| 9 | external PXF load | PASS | count = 1,000,000; PXF survives failover |

All nine steps PASS in a single end-to-end sequence (total 2378s).

---

## 7. Performance Smoke Summary

A read-only smoke test (10 iterations, 4 parallel sessions, 2 warmup) ran against
the 3-segment cluster (`mydb` 1347 MB; numsegments=3). Verdict: **PASS**, zero
errors across 173 query executions.

### Serial latency (p50, 1 session)

| Query | Description | p50 | p95 | Errors |
|---|---|---|---|---|
| Q1 | Bulk aggregate, full scan 4.5M rows | **977 ms** | 1016 ms | 0 |
| Q2 | Point lookup by `c_custkey` | **28 ms** | 29 ms | 0 |
| Q3 | 3-table join + date filter + agg | **1001 ms** | 1269 ms | 0 |
| Q4 | GROUP BY `gp_segment_id` | **994 ms** | 1062 ms | 0 |
| Q5 | Date-range scan (~93k rows) | **149 ms** | 170 ms | 0 |
| Q6 | PXF s3:parquet read (1M rows) | **1559 ms** | 1672 ms | 0 |

### Parallel throughput (4 sessions)

| Query | Parallel p50 | ~QPS | Degradation vs serial |
|---|---|---|---|
| Q1 | 988 ms | ~37 q/s | +1.1% |
| Q3 | 1547 ms | ~21 q/s | +54.5% |
| Q5 | 246 ms | ~145 q/s | +65.1% |

### Segment distribution

```
 seg |  rows   | pct
-----+---------+------
   0 | 1499245 | 33.3
   1 | 1496991 | 33.3
   2 | 1503764 | 33.4
```

**Skew ~0.2%** — an effectively even 3-way split from the gpexpand redistribution.
GPORCA generates efficient MPP plans (4 slices, index pushdown, redistribute
motion, distributed aggregation). Buffer-cache hit ratio 99.85%; 0 deadlocks/conflicts;
workers at 2–10% CPU. The cluster is production-ready for this data volume.

---

## 8. Troubleshooting & OKD-Specific Notes

The following are OKD/SCC-specific considerations. Several are handled by the
image/operator; a few require additional, idempotent configuration, and are noted
here so you understand them as documented setup — not product regressions.

### 8.1 SCC — cluster pods rejected

**Symptom:** DB pods fail to schedule under `restricted-v2`.

**Cause & fix:** the DB pods need UID 1000 + root init. Ensure the scoped custom
SCC exists and is bound to the `cloudberry:default` SA. It is created by a Helm
`pre-install,pre-upgrade` hook. As a belt-and-suspenders check you can also bind it
explicitly:

```bash
oc adm policy add-scc-to-user cloudberry-operator-cluster \
  -z default -n cloudberry
```

Confirm the admitted SCC:

```bash
oc -n cloudberry get pod "$DB" -o jsonpath='{.metadata.annotations.openshift\.io/scc}'
# → cloudberry-operator-cluster (NOT anyuid, NOT restricted-v2)
```

### 8.2 sshd on port 2022 (backup + scale-out dispatch)

gpbackup and gpexpand dispatch to segments over SSH. The scoped SCC sets
`allowPrivilegeEscalation=false` and drops `NET_BIND_SERVICE`, so the image's
privileged sshd on port 22 cannot bind. The image (`okd4fix28`+) **bakes a
rootless sshd on port 2022** and adds a `Port 2022` override to the gpadmin ssh
client config. Confirm the rootless sshd is listening on 2022 inside the DB pods —
no operator action is required beyond using the correct image tag.

### 8.3 MinIO internal CA trust

The MinIO endpoint uses an internal cert-manager certificate. The
gpbackup S3 plugin (postgres process env) and PXF (JVM truststore) do not trust it
out of the box, producing `x509: certificate signed by unknown authority`. As an
OKD-specific configuration step, inject the *Some CA* into: the DB StatefulSet env
(`SSL_CERT_FILE`/`AWS_CA_BUNDLE`), the gpadmin `~/.bashrc`, and the PXF JVM
truststore, so both gpbackup S3 TLS and PXF S3 TLS verify. Note: patching the STS
env restarts DB pods, which can transiently fail content-2 over to its mirror —
harmless now that PXF runs on mirror pods (D9).

### 8.4 Interconnect NetworkPolicy

The operator's `okd4-cluster-pxf` NetworkPolicy makes selected pods default-deny on
OVN-Kubernetes, which drops the dynamic `udpifc` UDP interconnect used by
cross-segment motion (joins/redistribute). As an OKD-specific configuration step,
apply an additive **allow-intra-namespace** NetworkPolicy so cross-segment queries
work, for example:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-interconnect
  namespace: cloudberry
spec:
  podSelector: {}
  ingress:
    - from:
        - podSelector: {}
  policyTypes:
    - Ingress
```

### 8.5 gprecoverseg after failover (minor)

Recovering a *down* mirror to its preferred role via `gprecoverseg` can hit an SSH
race on OKD (the "stop failed segment" step trips the pod readiness probe, dropping
the rootless sshd mid-recovery). This affects only restoring the down mirror to
preferred roles — it does **not** affect correctness of steps 1–9, because PXF stays
available on the acting primary (D9) and gpbackup dispatches to acting primaries.



## 9. Definitive Image Tag Table

All images are mirrored to ACR `alphyndemo.azurecr.io/cloudberry/*` and pulled via
`acr-pull-secret`. These are the authoritative tags from the final green run
(DevOps iter29 / e2e ITERATION 4).

| Component | ACR repository | Tag |
|---|---|----|
| Operator | `ghcr.io/vyrodovalexey/cloudberry-k8s-operator` | `0.9.5-cldb-2.1.0` |
| Cluster (DB + PXF) | `ghcr.io/vyrodovalexey/cloudberry-k8s-official-pxf` | `0.9.5-cldb-2.1.0` |
| Backup | `ghcr.io/vyrodovalexey/cloudberry-k8s-backup` | `0.9.5-cldb-2.1.0` |
| PXF sidecar | `ghcr.io/vyrodovalexey/cloudberry-k8s-pxf` | `0.9.5-cldb-2.1.0` |
| Query exporter | `ghcr.io/vyrodovalexey/cloudberry-k8s-query-exporter` | `0.9.5-cldb-2.1.0` |
| Postgres exporter | `prometheuscommunity/postgres-exporter` | `v0.16.0` |

---

## See Also

- [Installation Guide](installation.md) — general operator installation, including
  the **OKD4 / OpenShift** section.
- [User Guide](user-guide.md) — day-to-day cluster operations.
- `deploy/helm/cloudberry-operator/okd4-example.yaml` — the operator Helm values
  embedded in [§4.1](#41-okd4-exampleyaml-embedded-verbatim).
- `deploy/helm/cloudberry-operator/config/samples/okd4-cluster.yaml` — the
  `CloudberryCluster` manifest embedded in
  [§5.1](#51-okd4-clusteryaml-embedded-verbatim).
