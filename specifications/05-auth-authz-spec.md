# Cloudberry Operator - Authentication & Authorization Specification

**Version**: 1.0.0

---

## 1. Overview

This specification covers the operator's authentication and authorization capabilities, including Basic authentication, OIDC/Keycloak integration, JWT validation, permission levels, pg_hba.conf management, SSL/TLS, and Vault integration for secrets management.

## 2. Authentication Architecture

### 2.1 Dual-Mode Authentication

The operator API supports two authentication modes simultaneously:

```
┌──────────────────────────────────────────────────────────┐
│                    Incoming Request                        │
│                                                           │
│  Authorization: Basic base64(user:pass)                   │
│  -- OR --                                                 │
│  Authorization: Bearer <JWT token>                        │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│                  Auth Middleware Chain                      │
│                                                           │
│  1. Extract Authorization header                          │
│  2. Detect auth type (Basic vs Bearer)                    │
│  3. Route to appropriate provider                         │
│                                                           │
│  ┌─────────────────┐    ┌──────────────────────────────┐ │
│  │  Basic Auth      │    │  OIDC/JWT Auth               │ │
│  │  Provider        │    │  Provider                    │ │
│  │                  │    │                              │ │
│  │  - Validate      │    │  - Validate JWT signature    │ │
│  │    credentials   │    │  - Check issuer              │ │
│  │  - Check against │    │  - Check audience            │ │
│  │    admin secret  │    │  - Check expiry              │ │
│  │  - Check against │    │  - Extract claims            │ │
│  │    DB roles      │    │  - Map roles to permissions  │ │
│  └────────┬────────┘    └──────────────┬───────────────┘ │
│           │                             │                  │
│           └──────────┬──────────────────┘                  │
│                      ▼                                     │
│  ┌──────────────────────────────────────────────────────┐ │
│  │           Permission Resolver                         │ │
│  │                                                       │ │
│  │  Determine effective permission level:                 │ │
│  │  - Self Only                                          │ │
│  │  - Basic                                              │ │
│  │  - Operator Basic                                     │ │
│  │  - Operator                                           │ │
│  │  - Admin                                              │ │
│  └──────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### 2.2 Authentication Interface

```go
// Provider defines the authentication provider interface.
type Provider interface {
    // Authenticate validates the request and returns the authenticated identity.
    Authenticate(ctx context.Context, r *http.Request) (*Identity, error)
    // Type returns the provider type name.
    Type() string
}

// Identity represents an authenticated user.
type Identity struct {
    Username    string
    Email       string
    Groups      []string
    Roles       []string
    Permission  PermissionLevel
    AuthMethod  string // "basic" or "oidc"
    TokenExpiry time.Time
}

// PermissionLevel represents the user's access tier.
type PermissionLevel int

const (
    PermissionSelfOnly      PermissionLevel = iota
    PermissionBasic
    PermissionOperatorBasic
    PermissionOperator
    PermissionAdmin
)
```

## 3. Basic Authentication

### 3.1 Configuration

**Source**: `spec.auth.basic`

```yaml
auth:
  basic:
    enabled: true
    adminUser: gpadmin
    adminPasswordSecret:
      name: cloudberry-admin-password
      key: password
```

### 3.2 Flow

1. Client sends `Authorization: Basic base64(username:password)`
2. Middleware decodes Base64 credentials
3. Validate against:
   a. Admin user/password from Kubernetes Secret
   b. Database roles (via SQL query to coordinator)
4. Determine permission level based on role membership
5. Return Identity with resolved permissions

### 3.3 Password Storage

- Admin password stored in Kubernetes Secret
- Optional: Store in Vault (`spec.vault.secretPath`)
- Passwords are never logged or exposed in status

### 3.4 Password Rotation

```bash
# Rotate admin password
cloudberry-ctl auth rotate-password --cluster my-cluster

# This updates:
# 1. Kubernetes Secret
# 2. Database role password
# 3. pg_hba.conf if needed
# 4. Vault secret if enabled
```

## 4. OIDC Authentication (Keycloak Integration)

### 4.1 Configuration

**Source**: `spec.auth.oidc`

```yaml
auth:
  oidc:
    enabled: true
    issuerURL: http://keycloak:8090/realms/cloudberry
    clientID: cloudberry-operator
    clientSecret:
      secretRef:
        name: oidc-client-secret
        key: client-secret
    scopes:
      - openid
      - profile
      - email
    roleClaimPath: "realm_access.roles"
    roleClaimSource: id_token  # or userinfo
    roleMatchMode: exact       # exact, suffix, prefix, contains
    roleMapping:
      admin: Admin
      operator: Operator
      operator-basic: "Operator Basic"
      user: Basic
      reader: "Self Only"
    pkce: true
    allowLocalSignIn: true
```

### 4.2 OIDC Discovery

On startup, the operator:
1. Fetches `{issuerURL}/.well-known/openid-configuration`
2. Caches the JWKS (JSON Web Key Set) endpoint
3. Periodically refreshes JWKS (every 5 minutes)
4. Validates issuer matches configured `issuerURL`

### 4.3 JWT Validation Flow

1. Client sends `Authorization: Bearer <JWT>`
2. Middleware extracts JWT from header
3. Validate JWT:
   a. Verify signature against JWKS
   b. Check `iss` (issuer) matches `issuerURL`
   c. Check `aud` (audience) contains `clientID`
   d. Check `exp` (expiry) is in the future
   e. Check `iat` (issued at) is not in the future
4. Extract claims:
   a. `sub` -> Username
   b. `email` -> Email
   c. Role claim (from `roleClaimPath`) -> Roles
5. Map roles to permission level using `roleMapping`
6. Return Identity

### 4.4 Role Claim Extraction

The `roleClaimPath` supports nested JSON paths:

```json
// Example JWT payload
{
  "sub": "user123",
  "email": "user@example.com",
  "realm_access": {
    "roles": ["admin", "user"]
  }
}
```

With `roleClaimPath: "realm_access.roles"`, the operator extracts `["admin", "user"]`.

### 4.5 Role Matching Modes

| Mode | Description | Example |
|------|-------------|---------|
| `exact` | Role must match exactly | `admin` matches `admin` only |
| `suffix` | Role must end with value | `org-admin` matches `*admin` |
| `prefix` | Role must start with value | `admin-team` matches `admin*` |
| `contains` | Role must contain value | `super-admin-user` matches `*admin*` |

### 4.6 Role Claim Source

| Source | Description |
|--------|-------------|
| `id_token` | Extract roles from the ID token claims (default, faster) |
| `userinfo` | Fetch roles from the UserInfo endpoint (more up-to-date) |

### 4.7 PKCE Support

When `pkce: true`:
- Use PKCE (Proof Key for Code Exchange) for authorization code flow
- Generate code_verifier and code_challenge
- Suitable for public clients (no client secret needed in browser)

### 4.8 Token Refresh

The operator supports token refresh for long-lived sessions:
1. Monitor token expiry
2. Before expiry, use refresh_token to obtain new access_token
3. If refresh fails, require re-authentication

### 4.9 Keycloak-Specific Integration

#### Realm Configuration
```bash
# Setup script creates:
# 1. Realm: cloudberry
# 2. Client: cloudberry-operator (confidential)
# 3. Client: cloudberry-ctl (public, PKCE)
# 4. Roles: admin, operator, operator-basic, user, reader
# 5. Test users with role assignments
```

#### Service Account (Client Credentials)
For operator-to-Keycloak communication:
```yaml
# The operator uses client_credentials grant for:
# - Token introspection
# - User info lookup
# - Admin API calls (optional)
```

## 5. Permission Levels

### 5.1 Level Definitions

| Level | Description | Capabilities |
|-------|-------------|-------------|
| **Self Only** | View own queries and sessions | Read own session info, cancel own queries |
| **Basic** | View cluster state | Read cluster status, view dashboards, list databases |
| **Operator Basic** | Basic operations | Basic + view all sessions, view configurations |
| **Operator** | Cluster operations | Operator Basic + start/stop, config changes, maintenance |
| **Admin** | Full access | Operator + user management, security config, cluster lifecycle |

### 5.2 Permission Matrix

| Operation | Self Only | Basic | Op Basic | Operator | Admin |
|-----------|-----------|-------|----------|----------|-------|
| View own sessions | Y | Y | Y | Y | Y |
| View all sessions | - | - | Y | Y | Y |
| View cluster status | - | Y | Y | Y | Y |
| View configuration | - | - | Y | Y | Y |
| Cancel own query | Y | Y | Y | Y | Y |
| Cancel any query | - | - | - | Y | Y |
| Terminate session | - | - | - | Y | Y |
| Start/Stop cluster | - | - | - | Y | Y |
| Change configuration | - | - | - | Y | Y |
| Run maintenance | - | - | - | Y | Y |
| Manage users/roles | - | - | - | - | Y |
| Manage auth config | - | - | - | - | Y |
| Manage HBA rules | - | - | - | - | Y |
| Delete cluster | - | - | - | - | Y |

### 5.3 Permission Enforcement

```go
// Middleware enforces permissions on each endpoint
func RequirePermission(level PermissionLevel) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            identity := IdentityFromContext(r.Context())
            if identity.Permission < level {
                http.Error(w, "Forbidden", http.StatusForbidden)
                return
            }
            next.ServeHTTP(w, r)
        })
    }
}
```

## 6. Host-Based Authentication (pg_hba.conf)

### 6.1 CRD Configuration

**Source**: `spec.auth.hbaRules`

```yaml
auth:
  hbaRules:
    - type: local
      database: all
      user: gpadmin
      method: trust
    - type: host
      database: all
      user: all
      address: "10.0.0.0/8"
      method: scram-sha-256
    - type: hostssl
      database: all
      user: all
      address: "0.0.0.0/0"
      method: scram-sha-256
    - type: host
      database: all
      user: all
      address: "0.0.0.0/0"
      method: reject
```

### 6.2 Reconciliation

1. Render HBA rules into ConfigMap `{cluster}-pg-hba-conf`
2. Mount ConfigMap into coordinator pod
3. Reload configuration (no restart needed)
4. Sync to standby coordinator
5. Version history maintained via ConfigMap annotations

### 6.3 Default Rules

If no `hbaRules` specified, the operator generates defaults:

```
local   all   gpadmin                 trust
local   all   all                     scram-sha-256
host    all   gpadmin   127.0.0.1/32  trust
host    all   all       0.0.0.0/0     scram-sha-256
host    replication  all  0.0.0.0/0   scram-sha-256
```

## 7. SSL/TLS Configuration

### 7.1 CRD Configuration

**Source**: `spec.auth.ssl`

```yaml
auth:
  ssl:
    enabled: true
    certSecret:
      name: cloudberry-tls  # K8s TLS Secret
    minTLSVersion: "1.2"
```

### 7.2 Certificate Sources

1. **Kubernetes Secret** (type: kubernetes.io/tls)
   - `tls.crt` - Server certificate
   - `tls.key` - Server private key
   - `ca.crt` - CA certificate (optional)

2. **cert-manager** (future)
   - Certificate CR references Issuer/ClusterIssuer
   - Auto-renewal

3. **Vault PKI** (when vault.enabled)
   - Issue certificates from Vault PKI engine
   - Auto-renewal before expiry

### 7.3 TLS Parameters

Applied to postgresql.conf:
```
ssl = on
ssl_cert_file = '/tls/tls.crt'
ssl_key_file = '/tls/tls.key'
ssl_ca_file = '/tls/ca.crt'
ssl_min_protocol_version = 'TLSv1.2'
```

## 8. Vault Integration

### 8.1 Configuration

**Source**: `spec.vault`

```yaml
vault:
  enabled: true
  address: http://vault:8200
  authMethod: kubernetes  # token, kubernetes, approle
  authPath: auth/kubernetes
  role: cloudberry-operator
  secretPath: secret/data/cloudberry
  tlsSecret:
    name: vault-tls  # optional
```

### 8.2 Authentication Methods

| Method | Description | Use Case |
|--------|-------------|----------|
| `token` | Static token (dev/test only) | Development, CI |
| `kubernetes` | K8s service account auth | Production |
| `approle` | AppRole auth | Automation |

### 8.3 Secret Storage

Vault KV v2 paths:
```
secret/data/cloudberry/admin-password    # Admin password
secret/data/cloudberry/oidc-secret       # OIDC client secret
secret/data/cloudberry/monitoring-password # Monitoring role password
secret/data/cloudberry/tls               # TLS certificates (if not using K8s secrets)
```

### 8.4 Connection Retry

```go
// Vault client with exponential backoff
type VaultClient struct {
    client     *vault.Client
    retryOpts  RetryOptions
}

type RetryOptions struct {
    MaxRetries     int           // default: 5
    InitialBackoff time.Duration // default: 1s
    MaxBackoff     time.Duration // default: 30s
    Multiplier     float64       // default: 2.0
}
```

### 8.5 Secret Rotation

The operator watches Vault secrets for changes:
1. Periodic poll (configurable interval)
2. On change detected:
   a. Update Kubernetes Secret
   b. Reload affected components
   c. Emit event

## 9. Webhook Certificate Management

### 9.1 Overview

The operator manages TLS certificates for Kubernetes admission webhooks (validating and mutating). Two certificate sources are supported:

| Source | Description | Use Case |
|--------|-------------|----------|
| **Vault PKI** | Certificates issued by HashiCorp Vault PKI engine | Production (preferred) |
| **Self-signed** | Operator generates its own CA and certificates | Development, environments without Vault |

### 9.2 Certificate Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│                Certificate Lifecycle                         │
│                                                              │
│  1. Issuance                                                 │
│     ├── Vault PKI: Request cert from Vault PKI engine        │
│     └── Self-signed: Generate CA + server cert               │
│                                                              │
│  2. Storage                                                  │
│     └── Store in Kubernetes Secret:                          │
│         {release}-webhook-certs                              │
│         ├── tls.crt  (server certificate)                    │
│         ├── tls.key  (server private key)                    │
│         └── ca.crt   (CA certificate)                        │
│                                                              │
│  3. Injection                                                │
│     └── Patch ValidatingWebhookConfiguration and             │
│         MutatingWebhookConfiguration with caBundle           │
│                                                              │
│  4. Rotation                                                 │
│     ├── Check every 12 hours                                 │
│     ├── Rotate at 2/3 of certificate lifetime                │
│     └── Re-inject caBundle after rotation                    │
└─────────────────────────────────────────────────────────────┘
```

### 9.3 Vault PKI Integration

When `webhook.certSource` is `vault-pki`:

1. **Authentication**: Use the operator's Vault token (from `vault.authMethod`)
2. **Certificate Request**: Issue certificate from `{vaultPKI.mountPath}/issue/{vaultPKI.role}`
3. **Common Name**: `{webhookServiceName}.{namespace}.svc`
4. **SANs**: `{webhookServiceName}.{namespace}.svc`, `{webhookServiceName}.{namespace}.svc.cluster.local`
5. **TTL**: Requested from Vault role configuration (typically 720h)

```go
// Vault PKI certificate request
type VaultPKICertRequest struct {
    CommonName string   `json:"common_name"`
    AltNames   string   `json:"alt_names"`
    TTL        string   `json:"ttl"`
    Format     string   `json:"format"` // "pem"
}
```

### 9.4 Self-Signed CA Generation

When `webhook.certSource` is `self-signed`:

1. **CA Generation**: Generate RSA 4096-bit CA key pair
2. **CA Certificate**: Self-signed, 10-year validity, CA:TRUE basic constraint
3. **Server Certificate**: Signed by the generated CA
   - RSA 2048-bit key
   - 1-year validity
   - SANs: `{webhookServiceName}.{namespace}.svc`, `{webhookServiceName}.{namespace}.svc.cluster.local`
4. **Storage**: CA cert, server cert, and server key stored in the webhook cert Secret

### 9.5 Certificate Rotation Strategy

The operator runs a background goroutine for certificate rotation:

- **Check interval**: Every 12 hours
- **Rotation threshold**: Rotate when 2/3 of the certificate lifetime has elapsed
- **Rotation steps**:
  1. Issue or generate new certificate (depending on `certSource`)
  2. Update the Kubernetes Secret with new cert/key
  3. Patch webhook configurations with new `caBundle`
  4. The webhook server picks up the new certificate from the mounted Secret volume
  5. Emit `CertificateRotated` Kubernetes event

```go
// Certificate rotation check
func shouldRotate(cert *x509.Certificate) bool {
    lifetime := cert.NotAfter.Sub(cert.NotBefore)
    threshold := cert.NotBefore.Add(lifetime * 2 / 3)
    return time.Now().After(threshold)
}
```

### 9.6 Helm Chart Configuration

```yaml
webhook:
  enabled: true
  certSource: "self-signed"  # "self-signed" or "vault-pki"
  certSecretName: ""          # auto-generated: {release}-webhook-certs
  serviceName: ""             # auto-generated: {release}-webhook
  caBundle: ""                # static CA bundle (leave empty for runtime injection)
  vaultPKI:
    mountPath: "pki"
    role: "cloudberry-operator"
```

Environment variables injected into the operator pod:

| Variable | Description |
|----------|-------------|
| `CLOUDBERRY_WEBHOOK_CERT_SOURCE` | Certificate source (`self-signed` or `vault-pki`) |
| `CLOUDBERRY_WEBHOOK_CERT_SECRET_NAME` | Secret name for storing certificates |
| `CLOUDBERRY_WEBHOOK_SERVICE_NAME` | Webhook service name for SAN generation |
| `CLOUDBERRY_WEBHOOK_VAULT_PKI_MOUNT` | Vault PKI mount path (vault-pki only) |
| `CLOUDBERRY_WEBHOOK_VAULT_PKI_ROLE` | Vault PKI role name (vault-pki only) |

## 10. Auditing

### 10.1 Connection Auditing

Enabled via configuration parameters:
```yaml
config:
  parameters:
    log_connections: "on"
    log_disconnections: "on"
```

### 10.2 Statement Auditing

```yaml
config:
  parameters:
    log_statement: "ddl"           # none, ddl, mod, all
    log_min_duration_statement: "1000"  # ms
    log_duration: "on"
```

### 10.3 Operator Audit Log

The operator logs all authentication and authorization events:
- Successful/failed login attempts
- Permission denied events
- Configuration changes
- Role/user management operations

Format:
```json
{
  "level": "info",
  "msg": "authentication_success",
  "user": "admin",
  "method": "oidc",
  "source_ip": "10.0.0.1",
  "permission": "Admin",
  "timestamp": "2026-05-11T18:00:00Z"
}
```

## 11. Security Headers (Operator API)

When the operator exposes an HTTP API:

```
Cache-Control: no-store
Content-Security-Policy: default-src 'self'
Permissions-Policy: camera=(), microphone=()
Referrer-Policy: strict-origin-when-cross-origin
Strict-Transport-Security: max-age=31536000; includeSubDomains
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
X-XSS-Protection: 1; mode=block
```

## 12. cloudberry-ctl Authentication

### 12.1 Configuration File

```yaml
# ~/.cloudberry-ctl.yaml
clusters:
  my-cluster:
    endpoint: https://cloudberry-operator.cloudberry-system:8443
    auth:
      method: oidc  # or basic
      # For basic:
      username: admin
      # password: (prompted or from env CLOUDBERRY_PASSWORD)
      # For OIDC:
      issuer: http://keycloak:8090/realms/cloudberry
      client-id: cloudberry-ctl
      # Token cached in ~/.cloudberry-ctl/tokens/
```

### 12.2 Authentication Commands

```bash
# Login with OIDC (opens browser for auth code flow)
cloudberry-ctl auth login --cluster my-cluster

# Login with basic auth
cloudberry-ctl auth login --cluster my-cluster --basic

# Check current auth status
cloudberry-ctl auth status --cluster my-cluster

# Logout (clear cached tokens)
cloudberry-ctl auth logout --cluster my-cluster
```
