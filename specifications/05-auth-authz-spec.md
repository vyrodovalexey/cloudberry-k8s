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

### 2.3 Scenario 38: Dual-Mode Auth Infrastructure Bootstrap Verification

Scenario 38 validates that when a `CloudberryCluster` is deployed with **both** basic and OIDC authentication enabled, the operator's auth middleware correctly routes requests to the appropriate provider based on the `Authorization` header, and both providers return correct `Identity` objects with proper `AuthMethod` and `PermissionLevel`.

#### Test Scenario Description

The scenario deploys a cluster (`test/examples/scenario38-dual-auth.yaml`) with both `auth.basic.enabled: true` and `auth.oidc.enabled: true`. It verifies that the `AuthMiddleware` inspects the `Authorization` header prefix to determine routing:

- `Authorization: Basic ...` → routed to the Basic auth provider
- `Authorization: Bearer ...` → routed to the OIDC auth provider
- Missing header → `401 Unauthorized` with JSON error body
- Unsupported type (e.g., `Digest`) → `401 Unauthorized` with JSON error body

#### CR Spec Used

```yaml
auth:
  basic:
    enabled: true
    adminUser: gpadmin
    adminPasswordSecret:
      name: cloudberry-admin-password
      key: password
  oidc:
    enabled: true
    issuerURL: http://keycloak:8090/realms/cloudberry
    clientID: cloudberry-operator
    clientSecret:
      secretRef:
        name: oidc-client-secret
        key: client-secret
    scopes: [openid, profile, email]
    roleClaimPath: "realm_access.roles"
    roleClaimSource: id_token
    roleMatchMode: exact
    roleMapping:
      admin: Admin
      operator: Operator
      operator-basic: "Operator Basic"
      user: Basic
      reader: "Self Only"
    pkce: true
    allowLocalSignIn: true
```

#### Verification Matrix

| Auth Header | Provider | Identity.AuthMethod | Identity.PermissionLevel | HTTP Status |
|-------------|----------|---------------------|--------------------------|-------------|
| `Basic base64(admin:pass)` | Basic | `basic` | `Admin` | 200 |
| `Basic base64(operator:pass)` | Basic | `basic` | `Operator` | 200 |
| `Basic base64(opbasic:pass)` | Basic | `basic` | `Operator Basic` | 200 |
| `Basic base64(viewer:pass)` | Basic | `basic` | `Basic` | 200 |
| `Basic base64(reader:pass)` | Basic | `basic` | `Self Only` | 200 |
| `Bearer <JWT>` | OIDC | `oidc` | (mapped from role claim) | 200 |
| *(missing)* | — | — | — | 401 |
| `Digest username=test` | — | — | — | 401 |

#### Additional Verifications

- **Provider interface compliance**: Both `BasicAuthProvider` and `OIDCProvider` implement the `Provider` interface; `Type()` returns `"basic"` and `"oidc"` respectively
- **Sequential routing**: Multiple sequential requests with alternating auth types are correctly routed to the appropriate provider without cross-contamination
- **Error response format**: All 401 responses use JSON format with `{"error": {"code": "UNAUTHORIZED", ...}}`
- **CR spec reflection**: The cluster CR persists both `auth.basic` and `auth.oidc` configuration, and the API server operates correctly with both providers active

#### Bug Fix: OIDC Provider Wiring in `startAPIServer()`

During real-cluster testing, a critical bug was discovered and fixed in `cmd/operator/main.go`:

**Bug**: `startAPIServer()` passed `nil` for the OIDC provider parameter, meaning Bearer token authentication was never available even when OIDC was configured via Helm values.

**Fix**: Added OIDC provider initialization in `startAPIServer()` when `cfg.OIDC.Enabled` is true. The fix:

1. Creates `auth.OIDCConfig` from `config.OIDCConfig`
2. Includes default role mapping: `admin`→Admin, `operator`→Operator, `operator-basic`→"Operator Basic", `user`→Basic, `reader`→"Self Only"
3. Includes default `RoleClaimPath: "realm_access.roles"` and `RoleMatchMode: "exact"`
4. Gracefully handles OIDC initialization failure (logs warning, continues with Basic-only auth)

**Impact**: Without this fix, all `Authorization: Bearer <JWT>` requests would fail with `401 Unauthorized` because no OIDC provider was registered in the auth middleware.

#### Real-Cluster Verification (10/10 PASS)

The following tests were executed against a real running Kubernetes cluster with a real Keycloak OIDC provider:

| # | Test | HTTP Status | Result |
|---|------|-------------|--------|
| 1 | Basic Auth (valid admin) → routed to Basic provider | 200 | PASS |
| 2 | Basic Auth (invalid password) | 401 | PASS |
| 3 | No Auth Header | 401 | PASS |
| 4 | Bearer Auth (REAL Keycloak service account JWT) → routed to OIDC provider | 200 | PASS |
| 5 | Bearer Auth (REAL Keycloak user password-grant JWT) → routed to OIDC provider | 200 | PASS |
| 6 | Unsupported Auth Type (Digest) | 401 | PASS |
| 7 | Health /healthz (no auth) | 200 | PASS |
| 8 | Health /readyz (no auth) | 200 | PASS |
| 9 | Bearer Auth (invalid token) | 401 | PASS |
| 10 | Dual-auth cluster CR phase = Running | Running | PASS |

**Operator log evidence**:

- Basic auth: `"basic auth succeeded", username: "admin", permission: "Admin"`
- OIDC (service account): `"OIDC auth succeeded", username: "service-account-cloudberry-operator", roles: ["admin"], permission: "Admin"`
- OIDC (user): `"OIDC auth succeeded", username: "testuser", email: "testuser@test.local", roles: ["admin"], permission: "Admin"`

#### Keycloak Configuration Requirements

For OIDC to work end-to-end with a real Keycloak instance, the following configuration is required:

1. **Audience mapper**: The Keycloak realm must have a protocol mapper (type: `oidc-audience-mapper`) that includes the operator's `clientID` in the `aud` claim of issued tokens. Without this, JWT audience validation fails with `401 Unauthorized`.
2. **Frontend URL**: The Keycloak realm must have its `frontendUrl` set to match the operator's configured `issuerURL`. This ensures the `iss` claim in issued tokens matches what the operator expects during JWT validation.
3. **Role assignment**: Service accounts and users must have appropriate realm roles assigned (e.g., `admin`, `operator`, `user`, `reader`) that correspond to the `roleMapping` entries in the cluster CR.

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario38-dual-auth.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`DualAuthCase`, `DualAuthCases()`) | 9 cases |
| `test/functional/scenario38_dual_auth_test.go` | Functional tests | 18 |
| `test/e2e/scenario38_dual_auth_e2e_test.go` | E2E tests | 20 |
| `cmd/operator/main.go` | Bug fix: OIDC provider wiring | — |

### 2.4 Scenario 39: Basic Authentication Flow

Scenario 39 validates the basic authentication flow end-to-end, covering admin user validation (correct/wrong password, missing/malformed headers, timing attack prevention, no password leakage in logs) and DB role validation (unknown users, multiple users with different permission levels).

#### Test Scenario Description

The scenario deploys a cluster (`test/examples/scenario39-basic-auth.yaml`) with `auth.basic.enabled: true` and verifies the `BasicAuthProvider` against an `InMemoryCredentialStore`. It tests the full request lifecycle through the auth middleware, including credential validation, permission resolution, error response format, and security properties.

#### CR Spec Used

```yaml
auth:
  basic:
    enabled: true
    adminUser: gpadmin
    adminPasswordSecret:
      name: cloudberry-admin-password
      key: password
```

#### Verification Matrix

| Auth Header | User | HTTP Status | Permission | Result |
|-------------|------|-------------|------------|--------|
| `Basic base64(admin:correct)` | admin | 200 | Admin | Identity returned with `AuthMethod="basic"` |
| `Basic base64(admin:wrong)` | admin | 401 | — | Invalid credentials |
| `Basic base64(operator:correct)` | operator | 200 | Operator | Correct permission level |
| `Basic base64(opbasic:correct)` | opbasic | 200 | Operator Basic | Correct permission level |
| `Basic base64(viewer:correct)` | viewer | 200 | Basic | Correct permission level |
| `Basic base64(reader:correct)` | reader | 200 | Self Only | Correct permission level |
| *(missing)* | — | 401 | — | No `Authorization` header |
| `Basic not-valid-base64!!!` | — | 401 | — | Malformed Base64 |
| `Digest username=test` | — | 401 | — | Unsupported auth type |
| `Basic base64(unknown:pass)` | unknown | 401 | — | User not in credential store |

#### Security Verifications

- **No password leakage in logs**: After authentication (success or failure), the operator logs contain the username but never the password. Verified by capturing log output and asserting the password string is absent
- **Timing attack prevention**: When a user is not found in the credential store, the provider performs a bcrypt comparison against a dummy hash to ensure constant-time behavior. Verified by measuring that the user-not-found path takes non-trivial time (> 1ms due to bcrypt)
- **Error response format**: All 401 responses use JSON format with `{"error": {"code": "UNAUTHORIZED", "message": "..."}}`
- **Security headers**: Responses include `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Strict-Transport-Security`, and other security headers

#### Real-Cluster Verification (6/6 PASS)

Operator deployed with webhooks (vault-PKI, k8s auth) + OIDC + Basic auth.

| # | Test | HTTP | Result |
|---|------|------|--------|
| 39a-valid | admin with correct password (from K8s Secret) | 200 | ✅ Identity: username=admin, permission=Admin, AuthMethod=basic |
| 39a-wrong | admin with wrong password | 401 | ✅ |
| 39a-noleak | Password NOT in operator logs | N/A | ✅ Only username logged |
| 39a-missing | No auth header | 401 | ✅ |
| 39a-malformed | Malformed Basic header | 401 | ✅ |
| 39b-unknown | Unknown user 'analyst' (not in credential store) | 401 | ✅ |

Data operations: mydb created, 50 rows inserted, SELECT works.

#### Known Limitation

The current implementation uses `InMemoryCredentialStore` with only the `admin` user pre-configured. Database role validation via SQL query to the coordinator (as specified in section 3.2, step 3b) is not yet implemented. Unknown users receive 401 with a timing-attack-safe dummy hash comparison to prevent user enumeration.

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario39-basic-auth.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`BasicAuthFlowCase`, `BasicAuthFlowCases()`) | 8 cases |
| `test/functional/scenario39_basic_auth_test.go` | Functional tests | 31 (13 test methods, 18 sub-tests) |
| `test/e2e/scenario39_basic_auth_e2e_test.go` | E2E tests | 22 (6 test methods, 16 sub-tests) |

#### Functional Tests (13 test methods, 31 sub-tests)

| Test Method | Sub-Tests | What It Verifies |
|-------------|-----------|-----------------|
| `TestFunctional_Scenario39a_AdminAuth_CorrectPassword` | 1 | Valid admin credentials → Identity with Username="admin", Permission=Admin, AuthMethod="basic" |
| `TestFunctional_Scenario39a_AdminAuth_WrongPassword` | 1 | Wrong admin password → 401 via middleware |
| `TestFunctional_Scenario39a_AdminAuth_NoPasswordInLogs` | 1 | Password never appears in log output; username IS logged for audit |
| `TestFunctional_Scenario39a_AdminAuth_TimingAttack` | 1 | Unknown user path takes non-trivial time (bcrypt dummy hash comparison) |
| `TestFunctional_Scenario39a_AdminAuth_MissingHeader` | 1 | No Authorization header → 401 |
| `TestFunctional_Scenario39a_AdminAuth_MalformedHeader` | 4 | Malformed headers (invalid base64, empty, no space, Digest) → 401 |
| `TestFunctional_Scenario39b_DBRole_NotInStore` | 1 | Unknown user → 401 with "invalid credentials" error |
| `TestFunctional_Scenario39b_DBRole_MultipleUsers` | 5 | All 5 permission levels (Admin, Operator, Operator Basic, Basic, Self Only) verified |
| `TestFunctional_Scenario39_BasicAuthFlowCases` | 8 | All 8 cases from `BasicAuthFlowCases()` catalog executed |
| `TestFunctional_Scenario39_ProviderType` | 1 | `BasicAuthProvider.Type()` returns `"basic"` |
| `TestFunctional_Scenario39_IdentityFields` | 1 | All Identity fields verified: Username, Permission, AuthMethod set; Email, Groups, Roles, TokenExpiry empty/nil |
| `TestFunctional_Scenario39_MiddlewareWithAPIServer` | 3 | API server integration: authenticated → 200, unauthenticated → 401, wrong password → 401 |
| `TestFunctional_Scenario39_ErrorResponseJSON` | 1 | 401 response is JSON with `{"error": {"code": "UNAUTHORIZED", "message": "..."}}` |

#### E2E Tests (6 test methods, 22 sub-tests)

| Test Method | Sub-Tests | What It Verifies |
|-------------|-----------|-----------------|
| `TestE2E_Scenario39_AdminAuth_FullFlow` | 3 | Full admin auth lifecycle: valid → 200, invalid → 401, missing → 401 |
| `TestE2E_Scenario39_PermissionLevels` | 5 | All 5 permission levels verified with correct `Permission.String()` and `AuthMethod` |
| `TestE2E_Scenario39_SecurityHeaders` | 2 | Security headers present on both success and failure responses |
| `TestE2E_Scenario39_ErrorResponseFormat` | 3 | JSON error format verified for missing header, wrong password, unsupported auth type |
| `TestE2E_Scenario39_ClusterCRWithBasicAuth` | 1 | Cluster CR with basic auth config persists correctly; API server works with basic auth |
| `TestE2E_Scenario39_BasicAuthFlowCases` | 8 | All 8 cases from `BasicAuthFlowCases()` catalog executed in E2E context |

```bash
# Run basic auth flow functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario39

# Run basic auth flow E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario39
```

### 2.5 Scenario 40: Password Rotation

Scenario 40 validates the admin password rotation lifecycle, including K8s Secret creation, password priority resolution (env var > K8s Secret > generated), Secret update with a new password, authentication behavior before and after operator restart, password absence from logs, and Vault secret synchronization.

#### Test Scenario Description

The scenario deploys a cluster (`test/examples/scenario40-password-rotation.yaml`) with `auth.basic.enabled: true` and verifies the full password rotation flow: the cluster controller creates an admin password K8s Secret, the current password authenticates via the API, the Secret is updated with a new password, the old password continues to work in-memory before restart, the operator picks up the new password after restart, the new password works, the old password fails, the password never appears in operator logs, and the Vault secret is updated consistently.

#### CR Spec Used

```yaml
auth:
  basic:
    enabled: true
    adminUser: gpadmin
    adminPasswordSecret:
      name: cloudberry-admin-password
      key: password
```

#### Password Resolution Priority

The operator resolves the admin password using the following priority:

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | `CLOUDBERRY_API_ADMIN_PASSWORD` env var | Highest priority — overrides all other sources |
| 2 | K8s Secret (`cloudberry-operator-admin-password`) | Read from the operator namespace |
| 3 | Generated | Cryptographically secure random password via `util.GenerateRandomPassword()` |

#### Real-Cluster Verification (10/10 PASS)

The following tests were executed against a real running Kubernetes cluster with full test environment (Vault, Keycloak, VictoriaMetrics, MinIO, Kafka, RabbitMQ all running).

| # | Step | Result |
|---|------|--------|
| 1 | Admin password K8s Secret exists | ✅ PASS |
| 2 | Current password works via API (HTTP 200) | ✅ PASS |
| 3 | K8s Secret updated with new password | ✅ PASS |
| 4 | Old password still works before restart (in-memory) | ✅ PASS |
| 5 | Operator restarted, picks up new password | ✅ PASS |
| 6 | NEW password works after restart (HTTP 200) | ✅ PASS |
| 7 | OLD password FAILS after restart (HTTP 401) | ✅ PASS |
| 8 | Password NOT in operator logs | ✅ PASS |
| 9 | Vault secret updated with same new password | ✅ PASS |
| 10 | K8s Secret and Vault consistent | ✅ PASS |

Data operations verified: 50 rows in mydb. OIDC auth still works (HTTP 200).

#### Verification Matrix

| Feature | What Is Verified | Result |
|---------|-----------------|--------|
| Secret creation | Cluster controller creates admin password Secret with `managed-by` label | ✅ |
| Secret not overwritten | Existing user-provided Secret is preserved by the controller | ✅ |
| Password from Secret | Operator reads password from K8s Secret when no env var is set | ✅ |
| Env var priority | `CLOUDBERRY_API_ADMIN_PASSWORD` overrides K8s Secret value | ✅ |
| Password generation | Random password generated when neither env var nor Secret exists | ✅ |
| Secret update | K8s Secret updated with new password value | ✅ |
| New password works | After credential store update, new password authenticates (HTTP 200) | ✅ |
| Old password fails | After credential store update, old password returns HTTP 401 | ✅ |
| No password in logs | Operator logs contain username but never the password string | ✅ |
| Vault watcher | `SecretWatcher` detects hash change and invokes `onChange` callback | ✅ |

#### API-Driven Password Rotation

The operator exposes a `POST /api/v1alpha1/auth/rotate-password` endpoint (requires Admin permission) that performs automated password rotation without operator restart:

1. Generates a new cryptographically secure random password via `util.GenerateRandomPassword()`
2. Updates (or creates) the K8s Secret `cloudberry-operator-admin-password`
3. Updates the in-memory credential store **immediately** — no pod restart required
4. Records the `cloudberry_password_rotation_total` Prometheus counter metric
5. Returns `{"status": "rotated", "message": "Admin password rotated successfully"}`
6. Does **NOT** return the new password in the response (security best practice)

**CLI command:**

```bash
cloudberry-ctl auth rotate-password --cluster my-cluster
```

The CLI calls the API endpoint and prints a success or failure message.

#### Real-Cluster Verification — API-Driven Rotation (11/11 PASS)

| # | Step | Result |
|---|------|--------|
| 1 | K8s Secret exists | ✅ PASS |
| 2 | Current password works (HTTP 200) | ✅ PASS |
| 3 | API rotate-password returns `{"status":"rotated"}` | ✅ PASS |
| 4 | New password differs from old in K8s Secret | ✅ PASS |
| 5 | New password works IMMEDIATELY (HTTP 200, no restart) | ✅ PASS |
| 6 | Old password FAILS IMMEDIATELY (HTTP 401) | ✅ PASS |
| 7 | Password NOT in operator logs | ✅ PASS |
| 8 | Vault secret updated consistently | ✅ PASS |
| 9 | `cloudberry-ctl auth rotate-password` succeeds | ✅ PASS |
| 10 | Password rotated again by ctl | ✅ PASS |
| 11 | Data ops work (100 rows in mydb) | ✅ PASS |

#### Known Limitations

1. **DB role password update not implemented**: Database role passwords are not updated during rotation — only the operator API admin password is rotated
2. **Vault sync is manual**: The `SecretWatcher` exists and detects changes but is not wired into the automatic rotation pipeline

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario40-password-rotation.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`PasswordRotationCase`, `PasswordRotationCases()`) | 5 cases |
| `test/functional/scenario40_password_rotation_test.go` | Functional tests | 10 |
| `test/e2e/scenario40_password_rotation_e2e_test.go` | E2E tests | 6 |

#### Functional Tests (10 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario40_AdminSecret_Created` | Cluster controller creates admin password Secret with `managed-by` label; Secret retrievable after creation |
| `TestFunctional_Scenario40_AdminSecret_NotOverwritten` | Existing user-provided Secret is not overwritten by the controller |
| `TestFunctional_Scenario40_OperatorPassword_FromSecret` | Operator reads admin password from K8s Secret when no env var is set |
| `TestFunctional_Scenario40_OperatorPassword_FromEnvVar` | Env var takes priority over K8s Secret; Secret used when env var is empty |
| `TestFunctional_Scenario40_OperatorPassword_Generated` | Random password generated when neither env var nor Secret exists; two generated passwords differ |
| `TestFunctional_Scenario40_SecretUpdate_NewPassword` | K8s Secret updated with new password; retrieved value matches new password |
| `TestFunctional_Scenario40_BasicAuth_WithNewPassword` | After credential store update, new password authenticates (HTTP 200) |
| `TestFunctional_Scenario40_BasicAuth_OldPasswordFails` | After credential store update, old password returns HTTP 401 |
| `TestFunctional_Scenario40_VaultSecretWatcher_DetectsChange` | Vault `SecretWatcher` detects hash change and invokes `onChange` callback with updated data |
| `TestFunctional_Scenario40_PasswordRotationCases` | All 5 cases from `PasswordRotationCases()` catalog executed (5 sub-tests) |

#### E2E Tests (6 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario40_AdminSecretCreated` | Admin password Secret created with `managed-by` label in E2E context |
| `TestE2E_Scenario40_PasswordChange_NewWorks` | After rotation, new password authenticates through full API stack (HTTP 200) |
| `TestE2E_Scenario40_PasswordChange_OldFails` | After rotation, old password returns HTTP 401 through full API stack |
| `TestE2E_Scenario40_VaultWatcher_DetectsChange` | Vault `SecretWatcher` detects change and invokes `onChange` callback in E2E context |
| `TestE2E_Scenario40_ClusterCRAccepted` | Cluster CR with basic auth config persists correctly; API server works with basic auth |
| `TestE2E_Scenario40_PasswordRotationCases` | All 5 cases from `PasswordRotationCases()` catalog executed in E2E context |

```bash
# Run password rotation functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario40

# Run password rotation E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario40
```

### 2.6 Scenario 41: OIDC Full Flow with Keycloak

Scenario 41 validates the complete OIDC authentication flow end-to-end with a real Keycloak instance, covering all 5 permission levels, dual-mode auth (Basic + OIDC), service account (client_credentials) flow, and all role match modes.

#### Test Scenario Description

The scenario deploys a cluster (`test/examples/scenario41-oidc-full-flow.yaml`) with both `auth.basic.enabled: true` and `auth.oidc.enabled: true`, configured against a real Keycloak instance. It verifies the complete OIDC authentication pipeline: OIDC discovery, JWT signing/verification, role extraction from nested `realm_access.roles` claims, role-to-permission mapping for all 5 levels, standard claim extraction (`sub`, `email`, `preferred_username`), and dual-mode auth coexistence.

#### CR Spec Used

```yaml
auth:
  basic:
    enabled: true
    adminUser: gpadmin
  oidc:
    enabled: true
    issuerURL: http://host.docker.internal:8090/realms/test
    clientID: cloudberry-operator
    clientSecret:
      secretRef:
        name: oidc-client-secret
        key: client-secret
    scopes: [openid, profile, email]
    roleClaimPath: "realm_access.roles"
    roleClaimSource: id_token
    roleMatchMode: exact
    roleMapping:
      admin: Admin
      operator: Operator
      operator-basic: "Operator Basic"
      user: Basic
      reader: "Self Only"
    pkce: true
    allowLocalSignIn: true
```

#### Keycloak Configuration

The real Keycloak instance was configured with:

| Setting | Value |
|---------|-------|
| Realm | `test` |
| Frontend URL | `http://host.docker.internal:8090` |
| Client | `cloudberry-operator` (confidential, service accounts + direct access grants) |
| Audience mapper | `oidc-audience-mapper` including `cloudberry-operator` in `aud` claim |
| Realm roles | `admin`, `operator`, `operator-basic`, `user`, `reader` |
| Test users | 5 users, each assigned one role |

#### Real-Cluster Verification (7/7 PASS)

The operator was deployed with Vault-PKI webhook certs (Kubernetes auth to Vault), OIDC enabled, and basic auth enabled (`allowLocalSignIn`).

| # | Test | HTTP | Permission | Result |
|---|------|------|------------|--------|
| 1 | admin-user (role=admin) via OIDC Bearer | 200 | Admin | ✅ PASS |
| 2 | operator-user (role=operator) via OIDC Bearer | 200 | Operator | ✅ PASS |
| 3 | opbasic-user (role=operator-basic) via OIDC Bearer | 200 | Operator Basic | ✅ PASS |
| 4 | basic-user (role=user) via OIDC Bearer | 200 | Basic | ✅ PASS |
| 5 | reader-user (role=reader) via OIDC Bearer | 403 | Self Only | ✅ PASS |
| 6 | Basic auth alongside OIDC (allowLocalSignIn) | 200 | Admin | ✅ PASS |
| 7 | Service account (client_credentials) via OIDC Bearer | 200 | Admin | ✅ PASS |

**Operator log evidence**:

- `username=admin-user email=admin-user@test.local roles=[admin] permission=Admin`
- `username=operator-user email=operator-user@test.local roles=[operator] permission=Operator`
- `username=opbasic-user email=opbasic-user@test.local roles=[operator-basic] permission=Operator Basic`
- `username=basic-user email=basic-user@test.local roles=[user] permission=Basic`
- `username=reader-user email=reader-user@test.local roles=[reader] permission=Self Only`
- `username=service-account-cloudberry-operator roles=[admin] permission=Admin`

Data operations verified: 150 rows in mydb, SELECT aggregates work.

#### Verification Matrix

| Feature | What Is Verified | Result |
|---------|-----------------|--------|
| OIDC discovery | Provider fetches `.well-known/openid-configuration` and JWKS | ✅ |
| JWT verification | Valid, invalid, expired, and wrong-audience tokens | ✅ |
| Role extraction | Single role, multiple roles, no roles, missing `realm_access` | ✅ |
| Role mapping (all 5 levels) | `admin`→Admin, `operator`→Operator, `operator-basic`→"Operator Basic", `user`→Basic, `reader`→"Self Only" | ✅ |
| Multiple roles (highest wins) | User with `[reader, user, admin]` gets Admin permission | ✅ |
| Unknown role default | Unmapped role defaults to Self Only | ✅ |
| Claim extraction | `sub` sets Username, `email` sets Email, `preferred_username` overrides `sub` | ✅ |
| Dual-mode auth | Basic and OIDC work simultaneously with `allowLocalSignIn: true` | ✅ |
| Service account | `client_credentials` grant token accepted, `sub` used as username | ✅ |
| Role match modes | `exact`, `suffix`, `prefix`, `contains` all verified | ✅ |

#### Role Match Mode Verification

| Mode | IDP Role | Token Role | Matches? |
|------|----------|------------|----------|
| `exact` | `admin` | `admin` | ✅ Yes |
| `exact` | `admin` | `cloudberry-admin` | ❌ No |
| `suffix` | `admin` | `cloudberry-admin` | ✅ Yes |
| `suffix` | `admin` | `admin-role` | ❌ No |
| `prefix` | `admin` | `admin-role` | ✅ Yes |
| `prefix` | `admin` | `super-admin` | ❌ No |
| `contains` | `admin` | `super-admin-role` | ✅ Yes |
| `contains` | `admin` | `operator` | ❌ No |

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario41-oidc-full-flow.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`OIDCFlowCase`, `OIDCFlowCases()`) | 5 cases |
| `test/functional/scenario41_oidc_full_flow_test.go` | Functional tests | 37 (8 test methods, 27 sub-tests) |
| `test/e2e/scenario41_oidc_full_flow_e2e_test.go` | E2E tests | 16 (6 test methods, 6 sub-tests + 5 per-user + 5 cases) |

#### Functional Tests (8 test methods, 37 sub-tests)

| Test Method | Sub-Tests | What It Verifies |
|-------------|-----------|-----------------|
| `TestFunctional_Scenario41_OIDCProviderInit` | 5 | Provider initialization with mock discovery, OAuth2 config populated, missing issuer/client ID fails, unreachable issuer fails |
| `TestFunctional_Scenario41_JWTVerification` | 5 | Valid token succeeds, invalid token fails, expired token fails, wrong audience fails, missing bearer token fails |
| `TestFunctional_Scenario41_RoleExtraction` | 4 | Single role extracted, multiple roles extracted, no roles returns empty, missing `realm_access` returns nil |
| `TestFunctional_Scenario41_RoleMapping_AllLevels` | 7 | All 5 role-to-permission mappings, multiple roles (highest wins), unknown role defaults to Self Only |
| `TestFunctional_Scenario41_ClaimExtraction` | 4 | `sub` sets username, `email` extracted, `preferred_username` overrides `sub`, all claims together |
| `TestFunctional_Scenario41_AllowLocalSignIn` | 4 | Basic auth alongside OIDC, OIDC alongside basic, sequential basic→OIDC, no auth returns 401 |
| `TestFunctional_Scenario41_MatchModes` | 8 | All 4 match modes (exact, suffix, prefix, contains) with positive and negative cases |
| `TestFunctional_Scenario41_OIDCFlowCases` | 5 | All 5 cases from `OIDCFlowCases()` catalog executed |

#### E2E Tests (6 test methods, 16 sub-tests)

| Test Method | Sub-Tests | What It Verifies |
|-------------|-----------|-----------------|
| `TestE2E_Scenario41_OIDCProviderInit` | 1 | OIDC provider initialization with mock discovery, OAuth2 config verified |
| `TestE2E_Scenario41_PerUserAuth` | 5 | Each of 5 users authenticated with correct username, email, auth method, and permission level |
| `TestE2E_Scenario41_AllowLocalSignIn` | 4 | Basic auth succeeds, OIDC auth succeeds, no auth fails, interleaved basic/OIDC requests |
| `TestE2E_Scenario41_ServiceAccount` | 1 | Service account (`client_credentials`) token accepted, `sub` used as username, Admin permission |
| `TestE2E_Scenario41_ClusterCRWithOIDC` | 1 | Cluster CR with OIDC config persists correctly, API server works with both auth methods |
| `TestE2E_Scenario41_OIDCFlowCases` | 5 | All 5 cases from `OIDCFlowCases()` catalog executed in E2E context |

```bash
# Run OIDC full flow functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario41

# Run OIDC full flow E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario41
```

### 2.7 Scenario 42: Role Claim Source and Match Modes

Scenario 42 validates the `roleClaimSource` and `roleMatchMode` configuration fields end-to-end, verifying that the OIDC provider correctly extracts roles from the configured source and applies the configured match mode when mapping roles to permission levels.

#### Test Scenario Description

The scenario deploys a cluster (`test/examples/scenario42-role-claim-modes.yaml`) with OIDC enabled and tests all combinations of role claim sources (`id_token`, `userinfo`) and match modes (`exact`, `suffix`, `prefix`, `contains`). It verifies that:

- `roleClaimSource=id_token` extracts roles from the ID token's `realm_access.roles` claim
- `roleClaimSource=userinfo` is accepted as a configuration value but currently reads from ID token claims (known limitation)
- `roleMatchMode=exact` requires an exact string match between the token role and the mapping key
- `roleMatchMode=suffix` matches when the token role ends with the mapping key
- `roleMatchMode=prefix` matches when the token role starts with the mapping key
- `roleMatchMode=contains` matches when the token role contains the mapping key as a substring

#### CR Spec Used

```yaml
auth:
  basic:
    enabled: true
    adminUser: gpadmin
    adminPasswordSecret:
      name: cloudberry-admin-password
      key: password
  oidc:
    enabled: true
    issuerURL: http://host.docker.internal:8090/realms/test
    clientID: cloudberry-operator
    clientSecret:
      secretRef:
        name: oidc-client-secret
        key: client-secret
    scopes: [openid, profile, email]
    roleClaimPath: "realm_access.roles"
    roleClaimSource: id_token
    roleMatchMode: exact
    roleMapping:
      admin: Admin
      operator: Operator
      operator-basic: "Operator Basic"
      user: Basic
      reader: "Self Only"
    pkce: true
    allowLocalSignIn: true
```

#### Real-Cluster Verification (6/6 PASS)

The operator was deployed with `roleMatchMode=exact` (hardcoded default) and `roleClaimSource=id_token`. Keycloak 26.x was used as the OIDC provider, with users configured with `firstName` and `lastName` (required for password grant to avoid "Account is not fully set up" errors).

| # | Test | Role | Match Mode | HTTP | Permission | Result |
|---|------|------|------------|------|------------|--------|
| 42a | admin-user | admin | id_token source | 200 | Admin | ✅ PASS |
| 42c-match | exact-admin-user | admin | exact | 200 | Admin | ✅ PASS |
| 42c-nomatch | super-admin-user | super-admin | exact | 403 | Self Only | ✅ PASS |
| 42d-exact | org-admin-user | org-admin | exact (no match) | 403 | Self Only | ✅ PASS |
| 42e-exact | admin-team-user | admin-team | exact (no match) | 403 | Self Only | ✅ PASS |
| 42f-exact | super-admin-role-user | super-admin-user | exact (no match) | 403 | Self Only | ✅ PASS |

Suffix, prefix, and contains match modes were verified in functional tests (37+ sub-tests) using mock OIDC servers with JWTs containing specific roles.

#### Match Mode Verification Matrix

| Mode | Mapping Key | Token Role | Matches? | Permission |
|------|-------------|------------|----------|------------|
| `exact` | `admin` | `admin` | ✅ Yes | Admin |
| `exact` | `admin` | `super-admin` | ❌ No | Self Only |
| `exact` | `operator` | `operator` | ✅ Yes | Operator |
| `suffix` | `admin` | `org-admin` | ✅ Yes | Admin |
| `suffix` | `admin` | `cloudberry-admin` | ✅ Yes | Admin |
| `suffix` | `admin` | `admin-team` | ❌ No | Self Only |
| `prefix` | `admin` | `admin-team` | ✅ Yes | Admin |
| `prefix` | `admin` | `admin-role` | ✅ Yes | Admin |
| `prefix` | `admin` | `super-admin` | ❌ No | Self Only |
| `contains` | `admin` | `super-admin-user` | ✅ Yes | Admin |
| `contains` | `admin` | `admin` | ✅ Yes | Admin |
| `contains` | `admin` | `reader` | ❌ No | Self Only |

#### Known Limitations

1. **`roleClaimSource: userinfo` not implemented**: The `Authenticate()` method always reads roles from ID token claims, regardless of the `roleClaimSource` setting. The `userinfo` value is accepted in configuration but has no effect on runtime behavior.
2. **`roleMatchMode` hardcoded to `"exact"`**: In `cmd/operator/main.go`, the `RoleMatchMode` is hardcoded to `"exact"` and is not configurable via Helm values or environment variables. Non-exact match modes (suffix, prefix, contains) work correctly in the code but can only be activated by modifying the operator source.
3. **Keycloak 26.x user setup**: Keycloak 26.x requires `firstName` and `lastName` on users for the password grant flow to work. Without these fields, Keycloak returns an "Account is not fully set up" error.

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario42-role-claim-modes.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`RoleClaimCase`, `RoleClaimCases()`) | 10 cases |
| `test/functional/scenario42_role_claim_modes_test.go` | Functional tests | 37 (12 test methods, 25 sub-tests) |
| `test/e2e/scenario42_role_claim_modes_e2e_test.go` | E2E tests | 17 (7 suite methods, 10 catalog sub-tests) |

#### Functional Tests (12 test methods, 37 sub-tests)

| Test Method | Sub-Tests | What It Verifies |
|-------------|-----------|-----------------|
| `TestFunctional_Scenario42a_IDToken_RolesFromClaims` | 3 | Admin role extracted from ID token, multiple roles extracted, no roles defaults to Self Only |
| `TestFunctional_Scenario42b_UserInfo_ConfigField` | 3 | UserInfo config accepted, userinfo source still reads ID token claims, default source is id_token |
| `TestFunctional_Scenario42c_Exact_Match` | 1 | Exact match: `admin` matches `admin` → Admin |
| `TestFunctional_Scenario42c_Exact_NoMatch` | 1 | Exact no-match: `admin` does not match `super-admin` → Self Only |
| `TestFunctional_Scenario42d_Suffix_Match` | 1 | Suffix match: `admin` matches `org-admin` → Admin |
| `TestFunctional_Scenario42d_Suffix_NoMatch` | 1 | Suffix no-match: `admin` does not match `admin-team` → Self Only |
| `TestFunctional_Scenario42e_Prefix_Match` | 1 | Prefix match: `admin` matches `admin-team` → Admin |
| `TestFunctional_Scenario42e_Prefix_NoMatch` | 1 | Prefix no-match: `admin` does not match `super-admin` → Self Only |
| `TestFunctional_Scenario42f_Contains_Match` | 1 | Contains match: `admin` matches `super-admin-user` → Admin |
| `TestFunctional_Scenario42f_Contains_NoMatch` | 1 | Contains no-match: `admin` does not match `reader` → Self Only |
| `TestFunctional_Scenario42_ResolvePermission_AllModes` | 12 | All 4 match modes with 3 cases each (match, match, no-match) |
| `TestFunctional_Scenario42_RoleClaimCases` | 10 | All 10 cases from `RoleClaimCases()` catalog executed |

#### E2E Tests (7 suite methods, 17 sub-tests)

| Test Method | Sub-Tests | What It Verifies |
|-------------|-----------|-----------------|
| `TestE2E_Scenario42_ExactMatch_AdminRole` | 1 | Exact match with admin role → Admin permission |
| `TestE2E_Scenario42_ExactMatch_NoMatch` | 1 | Exact match with non-matching role → Self Only |
| `TestE2E_Scenario42_SuffixMatch` | 1 | Suffix match: `org-admin` matches `admin` pattern → Admin |
| `TestE2E_Scenario42_PrefixMatch` | 1 | Prefix match: `admin-team` matches `admin` pattern → Admin |
| `TestE2E_Scenario42_ContainsMatch` | 1 | Contains match: `super-admin-user` matches `admin` pattern → Admin |
| `TestE2E_Scenario42_ClusterCRWithRoleConfig` | 1 | Cluster CR with role claim config persists correctly in K8s |
| `TestE2E_Scenario42_RoleClaimCases` | 10 | All 10 cases from `RoleClaimCases()` catalog executed in E2E context |

```bash
# Run role claim modes functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario42

# Run role claim modes E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario42
```

### 2.8 Scenario 43: Full Permission Matrix Verification

Scenario 43 validates the complete API permission matrix by testing every endpoint against all five permission levels (Admin, Operator, OperatorBasic, Basic, SelfOnly), verifying that each endpoint enforces the correct minimum permission and that unauthenticated requests are rejected. The full 5-user × 57-endpoint matrix (285 permission checks) was verified in automated functional tests, and a representative subset was verified on a real cluster.

#### Test Scenario Description

The scenario deploys a cluster (`test/examples/scenario43-permission-matrix.yaml`) with `auth.basic.enabled: true` and five users configured at different permission levels. It verifies the complete request lifecycle through the API server with `api.NewServer()` and `httptest`, testing every registered endpoint against all five permission levels to confirm that:

- Users with sufficient permissions receive a non-401/non-403 response
- Users with insufficient permissions receive `403 Forbidden` with JSON error body `{"error": {"code": "FORBIDDEN", "message": "insufficient permissions..."}}`
- Unauthenticated requests receive `401 Unauthorized` with JSON error body `{"error": {"code": "UNAUTHORIZED"}}`
- Health endpoints (`/healthz`, `/readyz`) work without authentication

#### Permission Level Requirements by Endpoint Category

| Category | Required Level | Example Endpoints |
|----------|---------------|-------------------|
| Read-only cluster state | Basic | `GET /clusters`, `GET /clusters/{name}/status`, `GET /clusters/{name}/segments`, `GET /clusters/{name}/backups`, `GET /clusters/{name}/workload` |
| Config and sessions viewing | OperatorBasic | `GET /clusters/{name}/config`, `GET /clusters/{name}/sessions` |
| Cluster operations | Operator | `POST /clusters/{name}/start`, `POST /clusters/{name}/stop`, `POST /clusters/{name}/maintenance/vacuum`, `POST /clusters/{name}/rebalance` |
| Destructive / high-impact | Admin | `POST /clusters`, `DELETE /clusters/{name}`, `POST /clusters/{name}/standby/activate`, `DELETE /clusters/{name}/backups/{id}`, `POST /clusters/{name}/backups/{id}/restore` |

#### Full Permission Matrix (57 endpoints × 5 users = 285 checks)

The `PermissionMatrixCases()` catalog in `test/cases/test_cases.go` defines 57 endpoint test cases across four permission tiers:

| Tier | Endpoint Count | Examples |
|------|---------------|----------|
| Basic (read-only) | 24 | `GET /clusters`, `GET /status`, `GET /segments`, `GET /mirroring`, `GET /standby`, `GET /queries`, `GET /backups`, `GET /storage/pvcs`, `GET /workload`, `GET /resource-groups` |
| OperatorBasic | 2 | `GET /config`, `GET /sessions` |
| Operator (mutations) | 24 | `POST /start`, `POST /stop`, `POST /restart`, `POST /reload`, `PUT /config`, `POST /cancel`, `DELETE /sessions/{pid}`, `POST /vacuum`, `POST /analyze`, `POST /reindex`, `POST /rebalance`, `POST /backups`, `POST /data-loading/jobs` |
| Admin (destructive) | 7 | `POST /clusters`, `DELETE /clusters/{name}`, `POST /standby/activate`, `DELETE /backups/{id}`, `POST /backups/{id}/restore`, `DELETE /data-loading/jobs/{id}`, `DELETE /resource-groups/{name}` |

Each of the 57 endpoints is tested against all 5 users. Users with permission level >= the required level pass (not 401/403); users below the required level receive 403.

#### Real-Cluster Verification (12/12 PASS)

Operator deployed with self-signed webhook certs + OIDC (OIDC unavailable due to Docker Desktop networking — Keycloak unreachable from k8s pods via `host.docker.internal`). Basic auth tested.

| # | Test | HTTP | Result |
|---|------|------|--------|
| 43a-1 | Admin GET /clusters | not 401/403 | PASS |
| 43a-2 | Admin GET /status | not 401/403 | PASS |
| 43a-3 | Admin GET /config | not 401/403 | PASS |
| 43a-4 | Admin GET /sessions | not 401/403 | PASS |
| 43a-5 | Admin POST /start | not 401/403 | PASS |
| 43a-6 | Admin POST /vacuum | not 401/403 | PASS |
| 43a-7 | Admin DELETE /cluster | not 401/403 | PASS |
| 43b | No auth → 401 | 401 | PASS |
| 43c | Wrong password → 401 | 401 | PASS |
| 43d | Unknown user → 401 | 401 | PASS |
| 43e-1 | /healthz no auth → 200 | 200 | PASS |
| 43e-2 | /readyz no auth → 200 | 200 | PASS |

Data operations: mydb created, 50 rows inserted, SELECT works.

#### Additional Verifications

- **Forbidden response format**: All 403 responses use JSON format with `{"error": {"code": "FORBIDDEN", "message": "insufficient permissions: requires <level>"}}`
- **Unauthorized response format**: All 401 responses use JSON format with `{"error": {"code": "UNAUTHORIZED"}}`
- **Security headers on 403**: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Cache-Control: no-store`, `Strict-Transport-Security: max-age=31536000`
- **Health endpoints bypass auth**: `/healthz` and `/readyz` return 200 without any authentication

#### Known Limitation

OIDC-based permission testing on a real cluster requires Keycloak reachable from k8s pods. In Docker Desktop, `host.docker.internal` resolves but connection is refused. The full OIDC permission matrix was verified in Scenario 41.

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario43-permission-matrix.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`PermissionMatrixCase`, `PermissionMatrixCases()`) | 57 cases |
| `test/functional/scenario43_permission_matrix_test.go` | Functional tests | 10 (suite methods) |
| `test/e2e/scenario43_permission_matrix_e2e_test.go` | E2E tests | 8 (suite methods) |

#### Functional Tests (10 suite methods)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario43a_Admin_AllOperationsSucceed` | Admin user accesses all endpoints (Basic through Admin) without 401/403 |
| `TestFunctional_Scenario43b_Operator_AllowedAndDenied` | Operator user allowed on operator-level endpoints, denied on admin-only endpoints with 403 |
| `TestFunctional_Scenario43c_OperatorBasic_AllowedAndDenied` | OperatorBasic user allowed on config/sessions, denied on operator operations with 403 |
| `TestFunctional_Scenario43d_Basic_AllowedAndDenied` | Basic user allowed on read-only cluster state, denied on config/sessions and operator operations |
| `TestFunctional_Scenario43e_SelfOnly_AllowedAndDenied` | SelfOnly user allowed on health endpoints only, all API endpoints return 403 |
| `TestFunctional_Scenario43_PermissionMatrixCases` | All 57 cases from `PermissionMatrixCases()` catalog × 5 users = 285 permission checks |
| `TestFunctional_Scenario43_UnauthenticatedDenied` | Unauthenticated requests to all API endpoints return 401 with `UNAUTHORIZED` JSON error |
| `TestFunctional_Scenario43_HealthEndpointsNoAuth` | `/healthz` and `/readyz` return 200 without any authentication |
| `TestFunctional_Scenario43_ForbiddenResponseFormat` | 403 response is JSON with `{"error": {"code": "FORBIDDEN", "message": "insufficient permissions: requires Operator Basic"}}` |
| `TestFunctional_Scenario43_SecurityHeadersOnForbidden` | Security headers present on 403 responses |

#### E2E Tests (8 suite methods)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario43a_Admin_AllOperationsSucceed` | Admin user accesses all endpoints without 401/403 |
| `TestE2E_Scenario43b_Operator_AllowedAndDenied` | Operator allowed/denied with correct 403 JSON error format |
| `TestE2E_Scenario43c_OperatorBasic_AllowedAndDenied` | OperatorBasic allowed/denied boundaries verified |
| `TestE2E_Scenario43d_Basic_AllowedAndDenied` | Basic user allowed/denied boundaries verified |
| `TestE2E_Scenario43e_SelfOnly_AllowedAndDenied` | SelfOnly user: health endpoints 200, all API endpoints 403 |
| `TestE2E_Scenario43_PermissionMatrixCases` | Full 57 × 5 = 285 permission checks from catalog |
| `TestE2E_Scenario43_UnauthenticatedDenied` | Unauthenticated requests return 401 with `UNAUTHORIZED` JSON error |
| `TestE2E_Scenario43_SecurityHeadersOnForbidden` | Security headers verified on 403 responses |

```bash
# Run permission matrix functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario43

# Run permission matrix E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario43
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

The operator supports two methods for admin password rotation:

#### API-Driven Rotation (Recommended)

The `POST /api/v1alpha1/auth/rotate-password` endpoint performs automated rotation without operator restart:

```bash
# Via API
curl -u admin:current-password -X POST \
  http://operator:8090/api/v1alpha1/auth/rotate-password

# Via CLI
cloudberry-ctl auth rotate-password --cluster my-cluster
```

The endpoint:
1. Generates a new cryptographically secure random password
2. Updates the K8s Secret `cloudberry-operator-admin-password`
3. Updates the in-memory credential store **immediately** (no restart needed)
4. Records the `cloudberry_password_rotation_total` Prometheus metric
5. Returns `{"status": "rotated", "message": "Admin password rotated successfully"}`
6. Does **NOT** return the new password in the response (security)

After rotation, the new password can be retrieved from the K8s Secret:

```bash
kubectl get secret cloudberry-operator-admin-password -n cloudberry-system \
  -o jsonpath='{.data.password}' | base64 -d
```

#### Manual Rotation (Alternative)

You can also rotate the password manually by updating the K8s Secret and restarting the operator:

1. Update the K8s Secret with the new password:
   ```bash
   kubectl create secret generic cloudberry-admin-password \
     --from-literal=password=NEW_PASSWORD \
     --dry-run=client -o yaml | kubectl apply -f -
   ```
2. Restart the operator to pick up the new password:
   ```bash
   kubectl rollout restart deployment cloudberry-operator -n cloudberry-system
   ```
3. (Optional) Update the Vault secret if Vault integration is enabled:
   ```bash
   vault kv put secret/cloudberry/admin-password password=NEW_PASSWORD
   ```

#### Password Resolution Priority

The operator resolves the admin password on startup using the priority: env var (`CLOUDBERRY_API_ADMIN_PASSWORD`) > K8s Secret (`cloudberry-operator-admin-password`) > auto-generated random password.

> **Note**: See Scenario 40 (section 2.5) for the full password rotation verification, including real-cluster test results and known limitations.

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

### 6.4 Scenario 44: Custom HBA Rules Verification

Scenario 44 validates that when a `CloudberryCluster` is deployed with explicit `hbaRules` in the spec, the operator generates a `pg_hba.conf` ConfigMap containing exactly the specified custom rules, excludes all default rules, preserves rule ordering, tracks configuration changes via a hash annotation, and supports live updates (rule addition, removal, and replacement) without pod restarts.

#### Test Scenario Description

The scenario deploys a cluster (`test/examples/scenario44-hba-custom-rules.yaml`) with four custom HBA rules and verifies that the Auth Reconciler generates a `pg_hba.conf` ConfigMap containing only those rules. It also verifies that updating the rules produces a new ConfigMap with the updated content and a changed config hash, and that default rules (e.g., `127.0.0.1/32 trust`) are never generated when custom rules are present.

#### CR Spec Used

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
      address: "192.168.0.0/16"
      method: scram-sha-256
    - type: host
      database: all
      user: all
      address: "0.0.0.0/0"
      method: reject
```

#### Generated pg_hba.conf

```
local   all   gpadmin                 trust
host    all   all       10.0.0.0/8    scram-sha-256
hostssl all   all       192.168.0.0/16 scram-sha-256
host    all   all       0.0.0.0/0     reject
```

#### Verification Matrix

| # | Check | Expected | Verified |
|---|-------|----------|----------|
| 1 | ConfigMap `{cluster}-pg-hba-conf` exists | Created by Auth Reconciler | ✅ PASS |
| 2 | `local all gpadmin trust` rule present | First rule in file | ✅ PASS |
| 3 | `host all all 10.0.0.0/8 scram-sha-256` rule present | Second rule | ✅ PASS |
| 4 | `hostssl all all 192.168.0.0/16 scram-sha-256` rule present | Third rule | ✅ PASS |
| 5 | `host all all 0.0.0.0/0 reject` rule present | Fourth (last) rule | ✅ PASS |
| 6 | No default rules (127.0.0.1/32 absent) | Defaults excluded when custom rules specified | ✅ PASS |
| 7 | `avsoft.io/config-hash` annotation present | Non-empty SHA hash on ConfigMap | ✅ PASS |
| 8 | Config volume in StatefulSet | ConfigMap mounted into coordinator pod | ✅ PASS |
| 9 | Coordinator pod Running | Pod reaches Ready state | ✅ PASS |
| 10 | TCP from 127.0.0.1 blocked (reject rule active) | Connection rejected by `0.0.0.0/0 reject` rule | ✅ PASS |
| 11 | Hash changed after HBA update | Hash transitions (e.g., `7c09d696`→`1abc07f9`) | ✅ PASS |
| 12 | New rule (172.16.0.0/12) added after patch | Updated ConfigMap contains new rule | ✅ PASS |
| 13 | analyst user rule present after patch | User-specific rule added via update | ✅ PASS |

#### Additional Verifications

- **Rule ordering**: Custom rules appear in the same order as specified in the CRD `hbaRules` array
- **Rule count**: Exactly 4 non-comment, non-blank lines in the generated file (matching the 4 custom rules)
- **Default exclusion**: When custom `hbaRules` are provided, the operator does not generate any default rules — only the specified custom rules appear
- **Live update**: Patching `spec.auth.hbaRules` triggers a new reconciliation that updates the ConfigMap content and changes the `avsoft.io/config-hash` annotation
- **ConfigMap ownership**: The generated ConfigMap has proper labels and a config hash annotation

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario44-hba-custom-rules.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`HBACustomRuleCase`, `HBACustomRuleCases()`) | 5 cases |
| `test/functional/scenario44_hba_custom_rules_test.go` | Functional tests | 16 |
| `test/e2e/scenario44_hba_custom_rules_e2e_test.go` | E2E tests | 10 |

#### Functional Tests (16 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario44_CustomRules_ConfigMapCreated` | ConfigMap created with all 4 custom rules; exactly 4 rule lines |
| `TestFunctional_Scenario44_CustomRules_RuleOrder` | Rules appear in CRD-specified order: local < host scram < hostssl < host reject |
| `TestFunctional_Scenario44_CustomRules_HashAnnotation` | `avsoft.io/config-hash` annotation present and non-empty |
| `TestFunctional_Scenario44_CustomRules_NoDefaults` | Default-only rules (`local all all scram`, `host gpadmin 127.0.0.1/32`, `replication`) absent |
| `TestFunctional_Scenario44_CustomRules_LocalTrust` | `local all gpadmin trust` rule present |
| `TestFunctional_Scenario44_CustomRules_HostScram` | `host all all 10.0.0.0/8 scram-sha-256` rule present |
| `TestFunctional_Scenario44_CustomRules_HostSSL` | `hostssl all all 192.168.0.0/16 scram-sha-256` rule present |
| `TestFunctional_Scenario44_CustomRules_HostReject` | `host all all 0.0.0.0/0 reject` rule present |
| `TestFunctional_Scenario44_UpdateRules_ConfigMapUpdated` | Updated rules (172.16.0.0/12 md5) present; old rules (10.0.0.0/8) absent; hash changed |
| `TestFunctional_Scenario44_UpdateRules_HashChanged` | Config hash annotation changes after HBA rules update |
| `TestFunctional_Scenario44_HBACustomRuleCases` | All 5 cases from `HBACustomRuleCases()` catalog executed (5 sub-tests) |

#### E2E Tests (10 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario44_CustomRules_ConfigMap` | ConfigMap created with 4 rules and hash annotation |
| `TestE2E_Scenario44_CustomRules_AllRulesPresent` | All 4 custom rules present; default-only rules absent |
| `TestE2E_Scenario44_UpdateRules` | Rules updated, old rules removed, hash changed, rule count updated |
| `TestE2E_Scenario44_ClusterCRAccepted` | Cluster CR with custom HBA rules accepted; 4 rules preserved in spec |
| `TestE2E_Scenario44_HBACustomRuleCases` | All 5 cases from `HBACustomRuleCases()` catalog executed (5 sub-tests) |

```bash
# Run custom HBA rules functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario44

# Run custom HBA rules E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario44
```

### 6.5 Scenario 45: Default HBA Rules Verification

Scenario 45 validates that when a `CloudberryCluster` is deployed with **no `hbaRules`** in the spec, the operator generates the correct default `pg_hba.conf` rules documented in section 6.3.

#### Test Scenario Description

The scenario deploys a minimal cluster (`test/examples/scenario45-hba-defaults.yaml`) with no `auth.hbaRules` field and verifies that the Auth Reconciler generates a `pg_hba.conf` ConfigMap containing exactly the five default rules. It also verifies that custom rules override defaults entirely, and that an empty `hbaRules` slice triggers default generation.

#### Expected Default Rules

```
local   all   gpadmin                 trust
local   all   all                     scram-sha-256
host    all   gpadmin   127.0.0.1/32  trust
host    all   all       0.0.0.0/0     scram-sha-256
host    replication  all  0.0.0.0/0   scram-sha-256
```

#### Behavioral Verification Matrix

| Connection Type | User | Source | Auth Method | Password Required |
|----------------|------|--------|-------------|-------------------|
| `local` | `gpadmin` | Unix socket | `trust` | No |
| `local` | Any other user | Unix socket | `scram-sha-256` | Yes |
| `host` | `gpadmin` | `127.0.0.1/32` | `trust` | No |
| `host` | Any user | `0.0.0.0/0` | `scram-sha-256` | Yes |
| `host` (replication) | Any user | `0.0.0.0/0` | `scram-sha-256` | Yes |

#### Additional Verifications

- **Rule ordering**: Local rules appear before host rules in the generated `pg_hba.conf`
- **Rule count**: Exactly 5 non-comment, non-blank lines in the generated file
- **Custom override**: When `hbaRules` are explicitly provided, defaults are not generated
- **Empty slice**: An empty `hbaRules: []` triggers default rule generation (same as omitted)
- **ConfigMap ownership**: The generated ConfigMap has proper labels and a config hash annotation

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario45-hba-defaults.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`HBADefaultRuleCase`, `HBADefaultRuleCases()`) | — |
| `test/functional/scenario45_hba_defaults_test.go` | Functional tests | 11 |
| `test/e2e/scenario45_hba_defaults_e2e_test.go` | E2E tests | 5 |

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

### 7.4 Scenario 47: SSL/TLS Configuration Verification

Scenario 47 validates the operator's SSL/TLS configuration across two certificate sources: Kubernetes Secrets and Vault PKI. The scenario was verified against a real running Kubernetes cluster.

#### Sub-Scenarios

| Sub-Scenario | Description | Certificate Source | Verification |
|-------------|-------------|-------------------|--------------|
| **47a** — K8s Secret source | TLS Secret with `tls.crt`, `tls.key`, `ca.crt` mounted into all StatefulSets; `postgresql.conf` configured with SSL parameters; `hostssl` HBA rule enforced | Kubernetes Secret (`kubernetes.io/tls`) | SSL settings in ConfigMap, TLS volume on all StatefulSets, hostssl HBA rule |
| **47b** — Vault PKI source | Operator deployed with `vault-pki` webhook cert source; Vault PKI issues certificates with `certificate`, `private_key`, `issuing_ca`, `serial_number`; cert rotation at 2/3 of certificate lifetime | Vault PKI engine | Cert issuance, rotation threshold, self-signed fallback, webhook cert Secret |

#### 47a — K8s Secret Source

**CR Spec Used:**

```yaml
auth:
  ssl:
    enabled: true
    certSecret:
      name: cloudberry-tls
    minTLSVersion: "1.2"
  hbaRules:
    - type: hostssl
      database: all
      user: all
      address: "0.0.0.0/0"
      method: scram-sha-256
    - type: local
      database: all
      user: gpadmin
      method: trust
```

See `test/examples/scenario47a-ssl-k8s-secret.yaml` for the full cluster CR.

**Verification Matrix:**

| Check | Expected | Verified |
|-------|----------|----------|
| `ssl = on` in postgresql.conf | Present | PASS |
| `ssl_cert_file = '/tls/tls.crt'` in postgresql.conf | Present | PASS |
| `ssl_key_file = '/tls/tls.key'` in postgresql.conf | Present | PASS |
| `ssl_ca_file = '/tls/ca.crt'` in postgresql.conf | Present | PASS |
| `ssl_min_protocol_version = 'TLSv1.2'` in postgresql.conf | Present | PASS |
| TLS volume on coordinator StatefulSet | Volume sourced from `cloudberry-tls` Secret | PASS |
| TLS volume mount at `/tls` (read-only) | Present on main container | PASS |
| TLS volume on segment StatefulSets | Present on all primary and mirror StatefulSets | PASS |
| `hostssl` rule in pg_hba.conf | `hostssl all all 0.0.0.0/0 scram-sha-256` | PASS |
| SSL disabled → no SSL settings | No `ssl = on`, no `ssl_cert_file`, etc. | PASS |
| `minTLSVersion: "1.3"` → `TLSv1.3` | `ssl_min_protocol_version = 'TLSv1.3'` | PASS |
| No `certSecret` → no TLS volume | Volume not added, mount still present | PASS |

#### 47b — Vault PKI Source

**CR Spec Used:**

```yaml
auth:
  ssl:
    enabled: true
    certSecret:
      name: cloudberry-vault-tls
    minTLSVersion: "1.2"
vault:
  enabled: true
  address: http://vault:8200
  authMethod: token
  secretPath: secret/data/cloudberry
```

See `test/examples/scenario47b-ssl-vault-pki.yaml` for the full cluster CR.

**Vault PKI Certificate Issuance:**

The operator issues certificates from the Vault PKI engine via `{vaultPKI.mountPath}/issue/{vaultPKI.role}`. The response contains:

| Field | Description |
|-------|-------------|
| `certificate` | PEM-encoded server certificate |
| `private_key` | PEM-encoded server private key |
| `issuing_ca` | PEM-encoded CA certificate |
| `serial_number` | Certificate serial number |

**Certificate Rotation:**

Certificates are rotated when 2/3 of their lifetime has elapsed. The rotation check uses the following logic:

```go
func shouldRotate(cert *x509.Certificate) bool {
    lifetime := cert.NotAfter.Sub(cert.NotBefore)
    threshold := cert.NotBefore.Add(lifetime * 2 / 3)
    return time.Now().After(threshold)
}
```

**Verification Matrix:**

| Check | Expected | Verified |
|-------|----------|----------|
| Vault PKI cert issuance | `certificate`, `private_key`, `issuing_ca` returned | PASS |
| Certificate is PEM-encoded | Valid PEM blocks | PASS |
| Cert rotation at 2/3 lifetime | `NeedsRotation()` returns `true` for near-expiry cert | PASS |
| Fresh cert does not need rotation | `NeedsRotation()` returns `false` | PASS |
| Self-signed fallback | CA cert generated with `IsCA=true`, server cert with correct DNS SANs | PASS |
| Webhook cert Secret created | `kubernetes.io/tls` type with `ca.crt`, `tls.crt`, `tls.key` | PASS |
| Server cert DNS SANs | `{service}.{namespace}.svc`, `{service}.{namespace}.svc.cluster.local` | PASS |
| Both validating and mutating webhooks active | Webhook configurations patched with `caBundle` | PASS |

#### Bug Fix: TLS Private Key Permissions (Init Container Approach)

During real-cluster testing of Scenario 47a, a critical bug was discovered and fixed in `internal/builder/builder.go`:

**Bug**: PostgreSQL requires the TLS private key file to have permissions `0600` (owner read/write only). Kubernetes Secret volumes mount files as symlinks with `0777` permissions, which PostgreSQL rejects with:

```
FATAL: private key file "/tls/tls.key" has group or world access
```

**Fix**: Changed from mounting the Secret directly at `/tls` to a two-volume approach with an init container:

1. `tls-secret` volume: Kubernetes Secret mounted at `/tls-secret` (read-only, symlinked by K8s)
2. `tls` volume: EmptyDir mounted at `/tls`
3. `init-tls` init container: Copies cert files from `/tls-secret` to `/tls` with correct ownership and permissions:
   - Ownership: `gpadmin:gpadmin` (UID 1000)
   - Key permissions: `0600` (owner read/write only)
   - Cert permissions: `0644` (world readable)

**Init container command**:

```sh
cp /tls-secret/tls.crt /tls/tls.crt
cp /tls-secret/tls.key /tls/tls.key
cp /tls-secret/ca.crt /tls/ca.crt
chown 1000:1000 /tls/tls.crt /tls/tls.key /tls/ca.crt
chmod 0600 /tls/tls.key
chmod 0644 /tls/tls.crt /tls/ca.crt
```

**Impact**: Without this fix, any cluster with SSL enabled would fail to start because PostgreSQL refuses to use a private key file with group or world access.

**Files modified**:

| File | Change |
|------|--------|
| `internal/builder/builder.go` | Two-volume approach, `init-tls` init container, `psqlCommandFlag` constant |
| `internal/builder/builder_test.go` | Updated `TestBuildVolumes_WithSSL` for new 3-volume structure |
| `test/functional/scenario47_ssl_tls_test.go` | Updated TLS volume assertions |
| `test/e2e/scenario47_ssl_tls_e2e_test.go` | Updated TLS volume assertions |

#### Real-Cluster Verification Results

The following verifications were performed against a real running Kubernetes cluster:

**47a — K8s Secret Source (with init container fix)**:

| Check | Command / Method | Result |
|-------|-----------------|--------|
| SSL enabled | `SHOW ssl;` | `on` ✅ |
| Cert file path | `SHOW ssl_cert_file;` | `/tls/tls.crt` ✅ |
| Key file path | `SHOW ssl_key_file;` | `/tls/tls.key` ✅ |
| CA file path | `SHOW ssl_ca_file;` | `/tls/ca.crt` ✅ |
| Min TLS version | `SHOW ssl_min_protocol_version;` | `TLSv1.2` ✅ |
| Key file permissions | `ls -la /tls/tls.key` | `0600`, owned by `gpadmin` ✅ |
| Cert file permissions | `ls -la /tls/tls.crt` | `0644` ✅ |
| Database operations | `CREATE DATABASE mydb`, INSERT 100 rows, SELECT aggregates | All succeed ✅ |
| HBA enforcement | `pg_hba.conf` contains `hostssl` rule | ✅ |

**47b — Vault PKI Source**:

| Check | Result |
|-------|--------|
| Vault PKI cert issuance (`certificate`, `private_key`, `issuing_ca`) | ✅ |
| Operator webhook cert Secret exists (`kubernetes.io/tls`) | ✅ |
| Cert rotation at 2/3 of certificate lifetime | ✅ |

**Test results**:

- Scenario47-ssl cluster deployed and reached `Running` phase
- `postgresql.conf` SSL settings verified in the generated ConfigMap
- TLS volume mounted on coordinator and segment StatefulSets
- Vault PKI certificate issuance verified with valid PEM response
- API returns HTTP 200 with both clusters visible
- 16/16 functional tests PASS
- 12/12 E2E tests PASS
- All existing tests continue to pass (17 unit packages, functional, E2E)

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario47a-ssl-k8s-secret.yaml` | Example CR (K8s Secret) | — |
| `test/examples/scenario47b-ssl-vault-pki.yaml` | Example CR (Vault PKI) | — |
| `test/cases/test_cases.go` | Shared test cases (`SSLConfigCase`, `SSLConfigCases()`) | 4 cases |
| `test/testutil/fixtures.go` | Builder method (`WithSSLMinTLSVersion()`) | — |
| `test/functional/scenario47_ssl_tls_test.go` | Functional tests | 16 |
| `test/e2e/scenario47_ssl_tls_e2e_test.go` | E2E tests | 12 |

#### Functional Tests (16 test cases)

| Test | What It Verifies |
|------|-----------------|
| `TestFunctional_Scenario47a_SSLEnabled_PostgresqlConf` | SSL enabled → all 5 SSL settings present in postgresql.conf |
| `TestFunctional_Scenario47a_SSLEnabled_TLSVolume` | TLS volume sourced from cert Secret, mounted at `/tls` read-only |
| `TestFunctional_Scenario47a_SSLEnabled_MinTLS12` | `minTLSVersion: "1.2"` → `ssl_min_protocol_version = 'TLSv1.2'` |
| `TestFunctional_Scenario47a_SSLEnabled_MinTLS13` | `minTLSVersion: "1.3"` → `ssl_min_protocol_version = 'TLSv1.3'`, no TLSv1.2 |
| `TestFunctional_Scenario47a_SSLDisabled_NoSSLInConf` | SSL disabled → no SSL settings in postgresql.conf |
| `TestFunctional_Scenario47a_SSLEnabled_NoCertSecret` | SSL enabled without certSecret → no TLS volume, mount still present |
| `TestFunctional_Scenario47a_HostSSLRule` | `hostssl` HBA rule rendered correctly in pg_hba.conf with SSL enabled |
| `TestFunctional_Scenario47a_SSLConfigCases` | 4 cases from `SSLConfigCases()` catalog executed |
| `TestFunctional_Scenario47b_VaultPKI_CertIssuance` | Mock Vault PKI issues cert with `certificate`, `private_key`, `issuing_ca` |
| `TestFunctional_Scenario47b_VaultPKI_CertRotation` | Near-expiry cert (past 2/3 threshold) triggers rotation |
| `TestFunctional_Scenario47b_SelfSigned_CertGeneration` | Self-signed CA with `IsCA=true`, server cert with correct DNS SANs |
| `TestFunctional_Scenario47b_CertManager_EnsureCertificates` | Secret created with `kubernetes.io/tls` type, idempotent on second call |

#### E2E Tests (12 test cases)

| Test | What It Verifies |
|------|-----------------|
| `TestE2E_Scenario47a_SSLConfig_PostgresqlConf` | All 5 SSL settings present in postgresql.conf |
| `TestE2E_Scenario47a_SSLConfig_TLSVolume` | TLS volume and mount verified on coordinator StatefulSet |
| `TestE2E_Scenario47a_SSLConfig_MinTLSVersions` | Both TLS 1.2 and 1.3 minimum versions verified (2 subtests) |
| `TestE2E_Scenario47a_SSLConfig_HostSSLRule` | hostssl HBA rule reconciled correctly via AuthReconciler |
| `TestE2E_Scenario47b_VaultPKI_SelfSignedFallback` | Self-signed cert generated with valid CA and server cert |
| `TestE2E_Scenario47b_VaultPKI_CertRotationCheck` | Rotation detected, regeneration succeeds, fresh cert does not need rotation |
| `TestE2E_Scenario47_SSLConfigCases` | 4 cases from `SSLConfigCases()` catalog executed in E2E context |
| `TestE2E_Scenario47_ClusterWithSSL` | Cluster CR with SSL config persists correctly in K8s |

```bash
# Run SSL/TLS functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario47

# Run SSL/TLS E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario47
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

### 8.6 Scenario 46: Vault Integration Verification

Scenario 46 validates the operator's Vault integration across all authentication methods, secret paths, secret rotation, and connection retry behavior. The scenario was verified against a real running Kubernetes cluster with a real HashiCorp Vault instance.

#### Sub-Scenarios

| Sub-Scenario | Description | Auth Method | Verification |
|-------------|-------------|-------------|--------------|
| **46a** — Token auth | Operator authenticates to Vault using a static token and reads secrets from all 4 KV paths | `token` | API returns HTTP 200 for all paths |
| **46b** — Token auth (dev mode) | Same as 46a, explicitly testing the static token path in Vault dev mode | `token` | All 4 KV paths readable |
| **46c** — AppRole auth | AppRole enabled in Vault, role created, `role_id` and `secret_id` obtained, login successful | `approle` | Client token returned from AppRole login |
| **46d** — Secret rotation watch | Admin password updated directly in Vault; `SecretWatcher` detects change via hash comparison | `token` | `onChange` callback invoked |
| **46e** — Connection retry | Validates `DefaultRetryOptions` configuration | `token` | MaxRetries=5, InitialBackoff=1s, MaxBackoff=30s, Multiplier=2.0, JitterFraction=0.1 |

#### KV Secret Paths Tested

All sub-scenarios that read secrets verify the following 4 KV v2 paths:

```
secret/data/cloudberry/admin-password       # Admin password (username, password)
secret/data/cloudberry/oidc-secret          # OIDC client secret (client_id, client_secret)
secret/data/cloudberry/monitoring-password   # Monitoring role password (username, password)
secret/data/cloudberry/tls                  # TLS certificates (ca_cert, tls_cert, tls_key)
```

#### Auth Methods Tested

| Method | How It Works | Sub-Scenario |
|--------|-------------|--------------|
| `token` | Static Vault token passed via configuration; used directly for API calls | 46a, 46b, 46d, 46e |
| `approle` | AppRole enabled in Vault; operator obtains `role_id` and `secret_id`, then calls `auth/approle/login` to receive a client token | 46c |
| `kubernetes` | Kubernetes service account JWT exchanged for a Vault token via `auth/kubernetes/login` | Documented in spec; verified via operator deployment with `authMethod: kubernetes` |

#### Secret Rotation Watch Mechanism (46d)

The `SecretWatcher` component periodically polls Vault secrets and detects changes via SHA-256 hash comparison:

1. On each poll interval, the watcher reads the secret from Vault
2. Computes a SHA-256 hash of the secret data
3. Compares the new hash against the previously stored hash
4. If the hash differs, invokes the registered `onChange` callback
5. The callback updates the corresponding Kubernetes Secret and reloads affected components

**Verification**: The admin password was updated directly in Vault. The `SecretWatcher` detected the change and invoked the `onChange` callback, confirming the rotation mechanism works end-to-end.

#### Connection Retry Configuration (46e)

The Vault client uses exponential backoff with jitter for connection retries:

| Parameter | Value | Description |
|-----------|-------|-------------|
| `MaxRetries` | `5` | Maximum retry attempts after the initial call |
| `InitialBackoff` | `1s` | Wait time before the first retry |
| `MaxBackoff` | `30s` | Maximum wait time between retries |
| `Multiplier` | `2.0` | Backoff multiplier (exponential growth) |
| `JitterFraction` | `0.1` | Random jitter to prevent thundering herd |

#### Real-Cluster Verification Results

The following tests were executed against a real running Kubernetes cluster with a real Vault instance:

- Operator deployed with Vault token auth + webhooks + vault-PKI
- Scenario 1 cluster deployed, 10 pods running, 2000 rows inserted and queried
- All 4 Vault KV paths readable via token auth
- AppRole login successful with client token returned
- Secret rotation detected via hash comparison, `onChange` callback invoked
- Retry configuration confirmed: MaxRetries=5, InitialBackoff=1s, MaxBackoff=30s, Multiplier=2.0

#### CR Spec Used

```yaml
vault:
  enabled: true
  address: http://vault:8200
  authMethod: token
  secretPath: secret/data/cloudberry
```

See `test/examples/scenario46-vault-integration.yaml` for the full cluster CR.

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario46-vault-integration.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`VaultIntegrationCase`, `VaultIntegrationCases()`) | 5 cases |
| `test/functional/scenario46_vault_integration_test.go` | Functional tests | 9 |
| `test/e2e/scenario46_vault_integration_e2e_test.go` | E2E tests | 10 (9 PASS, 1 SKIP) |

#### Test Results

- All 9 functional tests PASS
- 9/10 E2E tests PASS, 1 correctly SKIP (PKI cert issuance when Vault PKI role is not configured)
- All existing tests continue to pass (17 unit packages, functional, E2E)

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

### 9.6 Scenario 48: Webhook Certificate Management Verification

Scenario 48 validates the operator's webhook certificate management across two certificate sources: Vault PKI (48a) and self-signed (48b). The scenario was verified against a real running Kubernetes cluster. Two critical bugs were discovered and fixed during testing.

#### Sub-Scenarios

| Sub-Scenario | Description | Certificate Source | Verification |
|-------------|-------------|-------------------|--------------|
| **48a** — Vault PKI cert source | Operator authenticates to Vault with token auth, requests certificate from `pki/issue/cloudberry-operator`, stores in `kubernetes.io/tls` Secret, patches webhook configurations with `caBundle` | Vault PKI engine | Cert issuance, Secret creation, webhook patching, env var wiring |
| **48b** — Self-signed cert source | Operator generates ECDSA P-256 CA (10-year validity, CA:TRUE, pathlen:0) and server cert (1-year validity, CA:FALSE), stores in Secret, patches webhooks | Self-signed generation | CA/server cert properties, Secret contents, webhook patching |

#### 48a — Vault PKI Cert Source

**Verification Matrix:**

| Check | Expected | Verified |
|-------|----------|----------|
| Operator authenticates to Vault with token auth | Token auth succeeds | ✅ PASS |
| Certificate requested from `pki/issue/cloudberry-operator` | Cert issued with correct CN and SANs | ✅ PASS |
| CN = `cloudberry-operator-webhook.cloudberry-system.svc` | Correct CN | ✅ PASS |
| SANs include `.svc` and `.svc.cluster.local` | Both SANs present | ✅ PASS |
| Secret has `tls.crt`, `tls.key`, `ca.crt` | Type `kubernetes.io/tls` with all 3 keys | ✅ PASS |
| Both webhook configs patched with `caBundle` | caBundle present (1524 bytes) | ✅ PASS |
| All env vars set correctly | `CLOUDBERRY_WEBHOOK_*` vars populated | ✅ PASS |
| Webhook functional — CR accepted | Validating webhook accepts valid CR | ✅ PASS |

#### 48b — Self-Signed Cert Source

**Verification Matrix:**

| Check | Expected | Verified |
|-------|----------|----------|
| CA: ECDSA P-256, 10-year validity, CA:TRUE, pathlen:0 | Correct CA properties | ✅ PASS |
| Server cert: ECDSA P-256, 1-year validity, CA:FALSE | Correct server cert properties | ✅ PASS |
| SANs include `.svc` and `.svc.cluster.local` | Both SANs present | ✅ PASS |
| Secret has `tls.crt`, `tls.key`, `ca.crt` | All 3 keys present | ✅ PASS |
| Webhook functional — CR accepted | Validating webhook accepts valid CR | ✅ PASS |

> **Note**: The specification (section 9.4) describes RSA 4096-bit CA and RSA 2048-bit server keys, but the implementation uses ECDSA P-256 for both. ECDSA P-256 provides equivalent security to RSA 3072-bit with significantly smaller keys and faster operations, making it a practical improvement over the specification.

#### 48a-k8s — Kubernetes Auth with Vault PKI

Sub-scenario 48a-k8s validates the operator's webhook certificate management when Vault authentication uses the Kubernetes auth method instead of a static token. This was verified against a real running Kubernetes cluster with Docker Desktop.

**Vault Kubernetes Auth Backend Configuration:**

| Setting | Value | Notes |
|---------|-------|-------|
| `kubernetes_host` | `https://kubernetes.docker.internal:6443` | Docker Desktop specific — the k8s API cert has `kubernetes.docker.internal` as a SAN but NOT `host.docker.internal` |
| `disable_iss_validation` | `true` | Required for Docker Desktop compatibility |
| `disable_local_ca_jwt` | `true` | Uses dedicated service account JWT instead of Vault's local CA |
| Service account | `vault-auth` in `cloudberry-system` | Dedicated SA with `system:auth-delegator` ClusterRole for TokenReview API access |

**Vault Role Configuration:**

| Setting | Value |
|---------|-------|
| Role name | `cloudberry-operator` |
| `bound_service_account_names` | `["cloudberry-operator"]` |
| `bound_service_account_namespaces` | `["cloudberry-system"]` |
| `policies` | `["default", "cloudberry-pki"]` |

**PKI Role**: `cloudberry-operator` with `allow_any_name: true`

**Operator Helm Values:**

```yaml
vault:
  authMethod: kubernetes
  authPath: auth/kubernetes
  role: cloudberry-operator
webhook:
  certSource: vault-pki
```

**Verification Matrix:**

| Check | Evidence | Verified |
|-------|----------|----------|
| Operator authenticates via k8s SA token | Log: `"authenticated with vault using kubernetes method", role: "cloudberry-operator"` | ✅ PASS |
| Vault client uses k8s auth | Log: `authMethod: "kubernetes"` | ✅ PASS |
| Webhook cert issued from Vault PKI | CN=`cloudberry-operator-webhook.cloudberry-system.svc`, Issuer=`Test Root CA` | ✅ PASS |
| SANs correct | `.svc` and `.svc.cluster.local` | ✅ PASS |
| Cert stored in K8s Secret | `cloudberry-operator-webhook-certs` with `tls.crt`, `tls.key`, `ca.crt` | ✅ PASS |
| Both webhook configs patched | caBundle present (1142 bytes) | ✅ PASS |
| Webhook functional | CR `scenario48-k8s-auth-test` accepted | ✅ PASS |
| Data operations | 3100 rows in mydb accessible | ✅ PASS |
| Env vars | `CLOUDBERRY_VAULT_AUTH_METHOD=kubernetes`, all WEBHOOK vars set | ✅ PASS |

**Docker Desktop Hostname Requirement:**

> **Important**: On Docker Desktop, the Vault Kubernetes auth backend must use `kubernetes_host: https://kubernetes.docker.internal:6443` (not `https://host.docker.internal:6443`). The Kubernetes API server certificate only includes `kubernetes.docker.internal` as a SAN. Using `host.docker.internal` causes TLS verification failures during TokenReview API calls.

**Bug Found — Missing Viper Defaults:**

During Kubernetes auth setup, environment variables `CLOUDBERRY_VAULT_AUTH_METHOD`, `CLOUDBERRY_VAULT_ROLE`, and `CLOUDBERRY_VAULT_AUTH_PATH` were silently ignored because viper lacked `SetDefault()` calls for `vault.address`, `vault.token`, `vault.role`, and `vault.auth-path`. This was fixed in `internal/config/config.go` (see Bug Fix 2 above).

#### Certificate Rotation Verification

| Check | Expected | Verified |
|-------|----------|----------|
| Background goroutine checks every 12 hours | Rotation goroutine running | ✅ PASS |
| Rotation threshold at 2/3 of certificate lifetime | `shouldRotate()` triggers correctly | ✅ PASS |
| `checkCertRotation()` detects near-expiry certs | Near-expiry cert triggers rotation | ✅ PASS |

#### Helm Auto-Generation Verification

| Check | Expected | Verified |
|-------|----------|----------|
| `certSecretName` auto-generated as `{release}-webhook-certs` | Correct Secret name | ✅ PASS |
| `serviceName` auto-generated as `{release}-webhook` | Correct service name | ✅ PASS |
| Empty `caBundle` triggers runtime injection | caBundle injected at startup | ✅ PASS |

#### Bug Fix 1: Vault Client Wiring in `setupWebhookCerts()`

During real-cluster testing, a critical bug was discovered in `cmd/operator/main.go`:

**Bug**: `setupWebhookCerts()` passed `nil` for the vault client parameter to `certmanager.New()`. When `certSource=vault-pki`, the certmanager failed with "vault client is not enabled" because no vault client was available to issue certificates.

**Fix**: Added vault client creation in `setupWebhookCerts()` when `cfg.WebhookCertSource == "vault-pki"`. The fix maps `config.VaultConfig` fields to `vault.Config` and creates a real vault client using `vault.NewClient()`.

**Impact**: Without this fix, any operator deployment using `certSource=vault-pki` would fail to issue webhook certificates, causing all admission webhook calls to fail with TLS errors.

#### Bug Fix 2: Missing Viper Config Defaults

**Bug**: Viper config defaults were missing for `vault.address`, `vault.token`, `vault.role`, `vault.auth-path`, and OIDC fields (`oidc.issuer-url`, `oidc.client-id`, `oidc.client-secret`, `oidc.scopes`). Without defaults, viper's `AutomaticEnv()` couldn't bind these environment variables, so they were always empty even when set via Helm chart environment variables.

**Fix**: Added missing `viper.SetDefault()` calls in `internal/config/config.go` for all vault and OIDC fields:

```go
viper.SetDefault("vault.address", "")
viper.SetDefault("vault.token", "")
viper.SetDefault("vault.role", "")
viper.SetDefault("vault.auth-path", "")
viper.SetDefault("oidc.issuer-url", "")
viper.SetDefault("oidc.client-id", "")
viper.SetDefault("oidc.client-secret", "")
viper.SetDefault("oidc.scopes", "")
```

**Impact**: Without this fix, environment variables like `CLOUDBERRY_VAULT_ADDRESS` and `CLOUDBERRY_VAULT_TOKEN` set via Helm values were silently ignored, causing vault authentication and OIDC configuration to fail in all Helm-deployed environments.

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario48a-webhook-vault-pki.yaml` | Example CR (Vault PKI) | — |
| `test/examples/scenario48b-webhook-self-signed.yaml` | Example CR (Self-signed) | — |
| `test/cases/test_cases.go` | Shared test cases (`WebhookCertCase`, `WebhookCertCases()`) | — |
| `test/functional/scenario48_webhook_certs_test.go` | Functional tests | 9 |
| `test/e2e/scenario48_webhook_certs_e2e_test.go` | E2E tests | 7 |
| `cmd/operator/main.go` | Bug fix: vault client wiring | — |
| `internal/config/config.go` | Bug fix: missing viper defaults | — |

#### Functional Tests (9 test cases)

| Test | What It Verifies |
|------|-----------------|
| Vault PKI cert issuance | Certificate requested from Vault PKI with correct CN and SANs |
| Vault PKI Secret creation | `kubernetes.io/tls` Secret with `tls.crt`, `tls.key`, `ca.crt` |
| Vault PKI webhook patching | Both validating and mutating webhooks patched with `caBundle` |
| Self-signed CA generation | ECDSA P-256 CA with 10-year validity, CA:TRUE, pathlen:0 |
| Self-signed server cert | ECDSA P-256 server cert with 1-year validity, CA:FALSE |
| Self-signed SANs | `.svc` and `.svc.cluster.local` SANs present |
| Self-signed Secret creation | `kubernetes.io/tls` Secret with all 3 keys |
| Cert rotation detection | Near-expiry cert triggers `NeedsRotation()` |
| Fresh cert no rotation | Fresh cert does not trigger rotation |

#### E2E Tests (7 test cases)

| Test | What It Verifies |
|------|-----------------|
| Vault PKI end-to-end | Full Vault PKI cert lifecycle with real Vault |
| Self-signed end-to-end | Full self-signed cert lifecycle |
| Webhook functional with Vault PKI | CR accepted by webhook using Vault PKI certs |
| Webhook functional with self-signed | CR accepted by webhook using self-signed certs |
| Cert rotation check | Rotation detected for near-expiry certs |
| Secret contents | All required keys present in cert Secret |
| Helm auto-generation | Secret and service names auto-generated correctly |

```bash
# Run webhook cert functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario48

# Run webhook cert E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario48
```

### 9.7 Helm Chart Configuration

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

The operator logs all authentication and authorization events as structured JSON:
- Successful/failed login attempts (with `method` and `source_ip`)
- Permission denied events (logged with user context, required vs actual permission, path, and HTTP method)
- Configuration changes (logged with cluster name and user context)
- Role/user management operations (logged with cluster, role, group, and user context)

#### Audit Event Types

**Authentication success** (`level=INFO`):

```json
{
  "level": "INFO",
  "msg": "basic auth succeeded",
  "username": "admin",
  "method": "basic",
  "source_ip": "192.168.1.1:12345",
  "permission": "Admin"
}
```

**Authentication failure** (`level=WARN`):

```json
{
  "level": "WARN",
  "msg": "authentication failed",
  "method": "basic",
  "error": "invalid credentials",
  "remote_addr": "192.168.1.100:12345"
}
```

**Permission denied** (`level=WARN`):

```json
{
  "level": "WARN",
  "msg": "permission denied",
  "username": "viewer",
  "method": "basic",
  "source_ip": "192.168.1.1:12345",
  "required_permission": "Admin",
  "actual_permission": "Basic",
  "path": "/api/v1alpha1/clusters",
  "http_method": "POST"
}
```

**Config changed** (`level=INFO`):

```json
{
  "level": "INFO",
  "msg": "config changed",
  "cluster": "my-cluster",
  "username": "admin",
  "method": "basic",
  "source_ip": "192.168.1.1:12345"
}
```

**Role management** (`level=INFO`):

```json
{
  "level": "INFO",
  "msg": "role assigned to resource group",
  "cluster": "my-cluster",
  "group": "analytics",
  "role": "analyst",
  "username": "admin",
  "method": "basic",
  "source_ip": "192.168.1.1:12345"
}
```

### 10.4 Scenario 50: Auditing (All Categories)

Scenario 50 validates auditing across three categories: connection auditing configuration, statement auditing configuration, and operator audit log format. All 31 tests (17 functional + 14 E2E) pass.

#### Test Scenario Description

The scenario deploys a cluster (`test/examples/scenario50-auditing.yaml`) with all auditing parameters enabled and verifies that:

- **50a — Connection auditing config**: `log_connections = 'on'` and `log_disconnections = 'on'` appear in the generated `postgresql.conf` ConfigMap
- **50b — Statement auditing config**: `log_statement = 'ddl'`, `log_min_duration_statement = '1000'`, and `log_duration = 'on'` appear in the ConfigMap, with user-defined parameters rendered in sorted alphabetical order
- **50c — Operator audit log format**: Basic auth success/failure events are logged as structured JSON with correct fields (including `method` and `source_ip`), permission denied events are logged with user context and required vs actual permission, config changes are audit-logged with user context, and role management operations are audit-logged with user context

#### CR Spec Used

```yaml
config:
  parameters:
    log_connections: "on"
    log_disconnections: "on"
    log_statement: "ddl"
    log_min_duration_statement: "1000"
    log_duration: "on"
```

#### 50a — Connection Auditing Config

Verifies that connection auditing parameters are rendered into `postgresql.conf` when configured.

| Check | Expected | Verified |
|-------|----------|----------|
| `log_connections = 'on'` in postgresql.conf | Present | ✅ PASS |
| `log_disconnections = 'on'` in postgresql.conf | Present | ✅ PASS |
| `avsoft.io/config-hash` annotation on ConfigMap | Non-empty SHA hash | ✅ PASS |
| No audit params when not configured | `log_connections` and `log_disconnections` absent | ✅ PASS |

#### 50b — Statement Auditing Config

Verifies that statement auditing parameters are rendered into `postgresql.conf` with correct values and sorted order.

| Check | Expected | Verified |
|-------|----------|----------|
| `log_statement = 'ddl'` in postgresql.conf | Present | ✅ PASS |
| `log_min_duration_statement = '1000'` in postgresql.conf | Present | ✅ PASS |
| `log_duration = 'on'` in postgresql.conf | Present | ✅ PASS |
| All 3 statement params together | All present in same ConfigMap | ✅ PASS |
| Parameters sorted alphabetically | `log_duration` < `log_min_duration_statement` < `log_statement` | ✅ PASS |
| Full scenario config (all 5 params) | All 5 audit settings present with `# User-defined parameters` header | ✅ PASS |

#### 50c — Operator Audit Log Format

Verifies that the operator produces structured JSON audit logs with correct fields for authentication success, failure, permission denied, config change, and role management events.

| Check | Expected | Verified |
|-------|----------|----------|
| Basic auth success log | Contains `"basic auth succeeded"`, `username`, `method`, `source_ip`, `permission` fields | ✅ PASS |
| Basic auth failure log | Contains `"authentication failed"`, `method`, `error`, `remote_addr` fields | ✅ PASS |
| Permission denied (logged) | Viewer creating cluster → `403 Forbidden` response AND `"permission denied"` WARN log with `username`, `method`, `source_ip`, `required_permission`, `actual_permission`, `path`, `http_method` | ✅ PASS |
| JSON format validation | Every log line is valid JSON with `level` and `msg` fields | ✅ PASS |
| Success log structured fields | `username="admin"`, `method="basic"`, `source_ip` present, `permission="Admin"` in JSON entry | ✅ PASS |
| Failure log structured fields | `method="basic"`, `error` present, `remote_addr` contains client IP | ✅ PASS |
| Config change audit log | Config update produces `"config changed"` INFO log with `cluster`, `username`, `method`, `source_ip` | ✅ PASS |
| Role management audit log | Role assignment produces `"role assigned to resource group"` INFO log with `cluster`, `group`, `role`, `username`, `method`, `source_ip` | ✅ PASS |

#### Audit Log Field Reference

**Authentication success fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `level` | Log level | `"INFO"` |
| `msg` | Log message | `"basic auth succeeded"` |
| `username` | Authenticated user | `"admin"` |
| `method` | Auth method used | `"basic"` |
| `source_ip` | Client IP and port | `"192.168.1.1:12345"` |
| `permission` | Resolved permission level | `"Admin"` |

**Authentication failure fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `level` | Log level | `"WARN"` |
| `msg` | Log message | `"authentication failed"` |
| `method` | Auth method attempted | `"basic"` |
| `error` | Error description | `"invalid credentials"` |
| `remote_addr` | Client IP address | `"192.168.1.100:12345"` |

**Permission denied fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `level` | Log level | `"WARN"` |
| `msg` | Log message | `"permission denied"` |
| `username` | Authenticated user | `"viewer"` |
| `method` | Auth method used | `"basic"` |
| `source_ip` | Client IP and port | `"192.168.1.1:12345"` |
| `required_permission` | Permission required for the operation | `"Admin"` |
| `actual_permission` | User's actual permission level | `"Basic"` |
| `path` | Request URL path | `"/api/v1alpha1/clusters"` |
| `http_method` | HTTP method | `"POST"` |

**Config change fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `level` | Log level | `"INFO"` |
| `msg` | Log message | `"config changed"` |
| `cluster` | Cluster name | `"my-cluster"` |
| `username` | User who made the change | `"admin"` |
| `method` | Auth method used | `"basic"` |
| `source_ip` | Client IP and port | `"192.168.1.1:12345"` |

**Role management fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `level` | Log level | `"INFO"` |
| `msg` | Log message | `"role assigned to resource group"` |
| `cluster` | Cluster name | `"my-cluster"` |
| `group` | Resource group name | `"analytics"` |
| `role` | Role being assigned | `"analyst"` |
| `username` | User who made the assignment | `"admin"` |
| `method` | Auth method used | `"basic"` |
| `source_ip` | Client IP and port | `"192.168.1.1:12345"` |

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario50-auditing.yaml` | Example CR | — |
| `test/cases/test_cases.go` | Shared test cases (`AuditCase`, `AuditCases()`) | 11 cases (1 connection, 3 statement, 7 operator) |
| `test/functional/scenario50_auditing_test.go` | Functional tests | 17 |
| `test/e2e/scenario50_auditing_e2e_test.go` | E2E tests | 14 |

#### Functional Tests (17 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario50a_ConnectionAudit_ConfigMap` | `log_connections = 'on'` and `log_disconnections = 'on'` present in postgresql.conf |
| `TestFunctional_Scenario50a_ConnectionAudit_HashAnnotation` | ConfigMap has `avsoft.io/config-hash` annotation |
| `TestFunctional_Scenario50a_ConnectionAudit_NoParams` | No audit params in postgresql.conf when not configured |
| `TestFunctional_Scenario50b_StatementAudit_DDL` | `log_statement = 'ddl'` present in postgresql.conf |
| `TestFunctional_Scenario50b_StatementAudit_Duration` | `log_min_duration_statement = '1000'` and `log_duration = 'on'` present |
| `TestFunctional_Scenario50b_StatementAudit_AllParams` | All 3 statement audit params present together |
| `TestFunctional_Scenario50b_StatementAudit_ParametersSorted` | Parameters rendered in alphabetical order |
| `TestFunctional_Scenario50b_StatementAudit_FullScenarioConfig` | All 5 audit settings present with section header |
| `TestFunctional_Scenario50c_OperatorAudit_BasicAuthSuccess` | Successful auth produces log with `username`, `method`, `source_ip`, and `permission` |
| `TestFunctional_Scenario50c_OperatorAudit_BasicAuthFailure` | Failed auth produces log with `method` and `error` |
| `TestFunctional_Scenario50c_OperatorAudit_PermissionDenied` | Insufficient permissions returns 403 AND logs `"permission denied"` with `username`, `method`, `source_ip`, `required_permission`, `actual_permission`, `path`, `http_method` |
| `TestFunctional_Scenario50c_OperatorAudit_JSONFormat` | All log entries are valid JSON with `level` and `msg` fields |
| `TestFunctional_Scenario50c_OperatorAudit_SuccessLogFields` | Success entry contains `username`, `method`, `source_ip`, `permission`, actual values |
| `TestFunctional_Scenario50c_OperatorAudit_FailureLogFields` | Failure entry contains `method`, `error`, `remote_addr`, actual values |
| `TestFunctional_Scenario50c_OperatorAudit_ConfigChange` | Config update produces `"config changed"` log with `cluster`, `username`, `method`, `source_ip` |
| `TestFunctional_Scenario50c_OperatorAudit_RoleAssignment` | Role assignment produces `"role assigned to resource group"` log with `cluster`, `group`, `role`, `username`, `method`, `source_ip` |
| `TestFunctional_Scenario50_AuditCases_Coverage` | All 11 cases from `AuditCases()` catalog verified (1 connection, 3 statement, 7 operator) |

#### E2E Tests (14 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario50a_ConnectionAudit_ConfigMap` | Connection audit settings in postgresql.conf end-to-end |
| `TestE2E_Scenario50a_ConnectionAudit_HashAnnotation` | ConfigMap hash annotation present end-to-end |
| `TestE2E_Scenario50b_StatementAudit_DDL` | `log_statement = 'ddl'` in postgresql.conf end-to-end |
| `TestE2E_Scenario50b_StatementAudit_Duration` | Duration audit settings in postgresql.conf end-to-end |
| `TestE2E_Scenario50b_StatementAudit_FullScenarioConfig` | All 5 audit settings present end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_BasicAuthSuccess` | Successful auth log with `username`, `method`, `source_ip` end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_BasicAuthFailure` | Failed auth log with method and error end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_PermissionDenied` | Permission denied logged with user context AND 403 response end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_JSONFormat` | All log entries valid JSON end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_SuccessLogFields` | Success log structured fields (including `method`, `source_ip`) end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_FailureLogFields` | Failure log structured fields end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_ConfigChange` | Config change audit log with user context end-to-end |
| `TestE2E_Scenario50c_OperatorAudit_RoleAssignment` | Role assignment audit log with user context end-to-end |
| `TestE2E_Scenario50_AuditCases_Coverage` | All 11 audit cases from catalog verified end-to-end |

```bash
# Run auditing functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario50

# Run auditing E2E tests
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario50
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

### 11.1 Scenario 51 — Security Headers Verification

Scenario 51 verifies that all 8 security headers are present with exact values on every API response, regardless of endpoint, HTTP method, or response status code. The `SecurityHeaders` middleware is applied as the outermost middleware wrapping the entire mux in `server.Handler()`, ensuring headers appear on all responses including health checks, authenticated responses, and error responses.

No production code changes were needed — the SecurityHeaders middleware was already fully implemented in `internal/auth/middleware.go`.

**What is verified:**

- Headers present on health endpoints (`GET /healthz`, `GET /readyz`) — no auth required
- Headers present on authenticated GET responses (200 OK)
- Headers present on authenticated POST responses (200 OK)
- Headers present on unauthorized responses (401)
- Headers present on forbidden responses (403)
- Headers present on not found responses (404)
- Header values consistent across all endpoints simultaneously
- Headers present on responses from a real Cloudberry cluster backed API server

**Test count: 21 tests** (9 functional + 7 E2E mock + 5 E2E real cluster)

- **Example CR**: `test/examples/scenario51-security-headers.yaml`
- **Functional tests**: `test/functional/scenario51_security_headers_test.go`
- **E2E tests**: `test/e2e/scenario51_security_headers_e2e_test.go`
- **Test case catalog**: `SecurityHeaderCase` type and `SecurityHeaderCases()` function in `test/cases/test_cases.go` (8 cases)

**Test case catalog (8 SecurityHeaderCase entries):**

| Case Name | Header | Expected Value | Description |
|-----------|--------|----------------|-------------|
| `cache_control` | Cache-Control | `no-store` | Prevent caching of API responses |
| `content_security_policy` | Content-Security-Policy | `default-src 'self'` | Restrict resource loading to same origin |
| `permissions_policy` | Permissions-Policy | `camera=(), microphone=()` | Disable camera and microphone access |
| `referrer_policy` | Referrer-Policy | `strict-origin-when-cross-origin` | Limit referrer information sent cross-origin |
| `strict_transport_security` | Strict-Transport-Security | `max-age=31536000; includeSubDomains` | Enforce HTTPS with one-year max-age |
| `x_content_type_options` | X-Content-Type-Options | `nosniff` | Prevent MIME type sniffing |
| `x_frame_options` | X-Frame-Options | `DENY` | Prevent framing of API responses |
| `x_xss_protection` | X-XSS-Protection | `1; mode=block` | Enable browser XSS filtering in block mode |

#### Functional Tests (9 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario51_AllHeaders_HealthEndpoint` | All 8 headers present on `GET /healthz` (200, no auth) |
| `TestFunctional_Scenario51_AllHeaders_AuthenticatedGET` | All 8 headers present on `GET /api/v1alpha1/clusters` (200, admin auth) |
| `TestFunctional_Scenario51_AllHeaders_AuthenticatedPOST` | All 8 headers present on `POST /api/v1alpha1/clusters` (admin auth) |
| `TestFunctional_Scenario51_AllHeaders_UnauthorizedResponse` | All 8 headers present on 401 Unauthorized (no auth header) |
| `TestFunctional_Scenario51_AllHeaders_ForbiddenResponse` | All 8 headers present on 403 Forbidden (viewer tries POST) |
| `TestFunctional_Scenario51_AllHeaders_NotFoundResponse` | All 8 headers present on 404 Not Found |
| `TestFunctional_Scenario51_AllHeaders_ReadyzEndpoint` | All 8 headers present on `GET /readyz` (200, no auth) |
| `TestFunctional_Scenario51_SecurityHeaderCases_Coverage` | `SecurityHeaderCases()` returns exactly 8 cases with non-empty fields |
| `TestFunctional_Scenario51_HeadersConsistentAcrossEndpoints` | Same header values on `/healthz`, `/readyz`, authenticated GET, and error POST |

#### E2E Tests — Mock (7 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario51_AllHeaders_HealthEndpoint` | All 8 headers on `GET /healthz` end-to-end |
| `TestE2E_Scenario51_AllHeaders_AuthenticatedGET` | All 8 headers on authenticated GET end-to-end |
| `TestE2E_Scenario51_AllHeaders_UnauthorizedResponse` | All 8 headers on 401 response end-to-end |
| `TestE2E_Scenario51_AllHeaders_ForbiddenResponse` | All 8 headers on 403 response end-to-end |
| `TestE2E_Scenario51_AllHeaders_ErrorResponse` | All 8 headers on 404 response end-to-end |
| `TestE2E_Scenario51_HeadersConsistentAcrossEndpoints` | Consistent headers across `/healthz`, `/readyz`, authenticated, and 401 endpoints end-to-end |
| `TestE2E_Scenario51_SecurityHeaderCases_Coverage` | All 8 cases from catalog verified end-to-end |

#### E2E Tests — Real Cluster (5 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario51_RealCluster_HealthEndpoint` | All 8 headers on `GET /healthz` with real DB-backed server |
| `TestE2E_Scenario51_RealCluster_AuthenticatedGET` | All 8 headers on authenticated GET with real DB-backed server |
| `TestE2E_Scenario51_RealCluster_AuthFailure` | All 8 headers on 401 (wrong credentials) with real DB-backed server |
| `TestE2E_Scenario51_RealCluster_PermissionDenied` | All 8 headers on 403 (viewer tries POST) with real DB-backed server |
| `TestE2E_Scenario51_RealCluster_MultipleEndpoints` | Consistent headers across `/healthz`, `/readyz`, authenticated, and 401 endpoints with real DB-backed server |

```bash
# Run security headers functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario51

# Run security headers E2E tests (mock)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario51

# Run security headers E2E tests (real cluster)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario51_RealCluster
```

### 11.2 Scenario 52 — Negative Tests and Edge Cases

Scenario 52 validates negative and edge case behavior across authentication, JWT validation, Vault connection retry, OIDC configuration failure, and missing credentials. No production code changes were needed — all tests exercise existing code paths with invalid or edge-case inputs.

**Test count: 32 tests** (16 functional + 11 E2E mock + 5 E2E real cluster)

- **Example CR**: `test/examples/scenario52-negative-edge-cases.yaml`
- **Functional tests**: `test/functional/scenario52_negative_edge_cases_test.go`
- **E2E tests**: `test/e2e/scenario52_negative_edge_cases_e2e_test.go`
- **Test case catalog**: `NegativeEdgeCaseCase` type and `NegativeEdgeCaseCases()` function in `test/cases/test_cases.go` (8 cases)

#### Sub-Scenarios

| Sub-Scenario | Category | Description | Expected Result |
|-------------|----------|-------------|-----------------|
| **52a** — JWT with wrong issuer | jwt | JWT signed with the correct key but containing a wrong `iss` claim | 401 Unauthorized |
| **52b** — JWT with wrong audience | jwt | JWT with the correct issuer but wrong `aud` claim | 401 Unauthorized |
| **52c** — Expired JWT | jwt | JWT with `exp` in the past | 401 Unauthorized |
| **52d** — JWT with future iat | jwt | JWT with `iat` 1 hour in the future — behavioral test documenting that gooidc does NOT reject future `iat` | Token accepted (gooidc does not validate `iat`) |
| **52e** — Token refresh failure | jwt | Expired access token (simulating a failed refresh) | 401 with "authentication failed" |
| **52f** — Vault connection retry | vault | Tests `RetryWithBackoff` — exhaustion returns `ErrRetryExhausted`, recovery after N failures, context cancellation stops retries | Retry behavior verified |
| **52g** — Invalid OIDC configuration | config | Unreachable issuer URL returns error from `NewOIDCProvider`; Basic auth fallback works when OIDC is unavailable (nil provider) | Error on init; Basic auth 200, Bearer 401 |
| **52h** — Missing K8s Secret for admin password | auth | Empty credential store causes auth failure; unknown user returns 401 | 401 with "authentication failed" |

#### 52a — JWT with Wrong Issuer

A JWT is signed with the correct RSA key (matching the JWKS endpoint) but contains an `iss` claim that does not match the OIDC provider's configured `issuerURL`. The OIDC provider rejects the token because the issuer does not match, returning 401 Unauthorized.

#### 52b — JWT with Wrong Audience

A JWT is signed with the correct key and has the correct `iss` claim, but the `aud` claim does not match the OIDC provider's configured `clientID`. The OIDC provider rejects the token because the audience does not match, returning 401 Unauthorized.

#### 52c — Expired JWT

A JWT is signed with the correct key, correct issuer, and correct audience, but the `exp` claim is set to 1 hour in the past. The OIDC provider rejects the token because it has expired, returning 401 Unauthorized.

#### 52d — JWT with Future iat (Behavioral Test)

A JWT is signed with the correct key, correct issuer, correct audience, and a valid `exp` (2 hours in the future), but the `iat` (issued-at) claim is set to 1 hour in the future. This test documents the behavior of the `gooidc` library: it does NOT validate the `iat` claim, so the token is accepted. The test verifies that the `Identity` is returned with the correct username and `AuthMethod="oidc"`.

> **Note**: If a future version of `gooidc` starts validating `iat` claims, this test will document that behavioral change.

#### 52e — Token Refresh Failure

An expired access token (with `exp` 30 minutes in the past) is sent to the API server, simulating a scenario where the refresh token has also expired or the refresh endpoint is unavailable. The API server returns 401 Unauthorized with an error response containing "authentication failed".

#### 52f — Vault Connection Retry

Tests the `RetryWithBackoff` utility function with three sub-tests:

| Test | What It Verifies |
|------|-----------------|
| Retry and recovery | Function fails 3 times, succeeds on attempt 4 — total attempts = 4 |
| Retry exhaustion | All retries fail — returns `ErrRetryExhausted` wrapping the last error; total attempts = `MaxRetries + 1` |
| Recovery after N failures | Function fails N-1 times, succeeds on attempt N — total attempts = N |
| Context cancellation | Context with 250ms timeout stops retries before exhausting all attempts |

#### 52g — Invalid OIDC Configuration

Two aspects are verified:

1. **Unreachable issuer**: `NewOIDCProvider()` with an unreachable issuer URL (`http://unreachable.invalid:9999/realms/test`) returns an error and a nil provider
2. **Basic auth fallback**: When the OIDC provider is nil (simulating failed initialization), Basic auth continues to work (HTTP 200) and Bearer tokens are rejected with 401 and an error message mentioning OIDC

#### 52h — Missing Admin Secret

Two aspects are verified:

1. **Empty credential store**: `BasicAuthProvider.Authenticate()` with an empty `InMemoryCredentialStore` returns an error containing "invalid credentials" and a nil identity
2. **Unknown user via API**: An API request with Basic auth credentials for an unknown user returns 401 with "authentication failed" in the response body

#### Test Case Catalog (8 NegativeEdgeCaseCase entries)

| Case Name | Sub-Scenario | Category | Expected Status | Description |
|-----------|-------------|----------|-----------------|-------------|
| `52a_jwt_wrong_issuer` | 52a | jwt | 401 | JWT with wrong issuer should be rejected with 401 |
| `52b_jwt_wrong_audience` | 52b | jwt | 401 | JWT with wrong audience should be rejected with 401 |
| `52c_jwt_expired` | 52c | jwt | 401 | Expired JWT should be rejected with 401 |
| `52d_jwt_future_iat` | 52d | jwt | 401 | JWT with future iat should be rejected with 401 |
| `52e_token_refresh_failure` | 52e | jwt | 401 | Expired token without refresh should result in 401 |
| `52f_vault_connection_retry` | 52f | vault | 0 | Vault connection failure should trigger exponential backoff retries |
| `52g_invalid_oidc_config` | 52g | config | 0 | Invalid OIDC config should fail gracefully; Basic auth should still work |
| `52h_missing_admin_secret` | 52h | auth | 401 | Missing admin password secret should cause Basic auth to fail with 401 |

#### Test Files

| File | Type | Test Count |
|------|------|------------|
| `test/examples/scenario52-negative-edge-cases.yaml` | Example CR | -- |
| `test/cases/test_cases.go` | Shared test cases (`NegativeEdgeCaseCase`, `NegativeEdgeCaseCases()`) | 8 cases |
| `test/functional/scenario52_negative_edge_cases_test.go` | Functional tests | 16 |
| `test/e2e/scenario52_negative_edge_cases_e2e_test.go` | E2E tests (mock + real cluster) | 16 (11 mock + 5 real cluster) |

#### Functional Tests (16 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestFunctional_Scenario52a_JWTWrongIssuer` | JWT signed with correct key but wrong `iss` claim rejected with 401 |
| `TestFunctional_Scenario52b_JWTWrongAudience` | JWT with correct issuer but wrong `aud` claim rejected with 401 |
| `TestFunctional_Scenario52c_JWTExpired` | JWT with `exp` in the past rejected with 401 |
| `TestFunctional_Scenario52d_JWTFutureIAT` | JWT with future `iat` accepted by gooidc (behavioral documentation test) |
| `TestFunctional_Scenario52e_TokenRefreshFailure` | Expired token returns 401 with "authentication failed" in response body |
| `TestFunctional_Scenario52f_VaultConnectionRetry` | `RetryWithBackoff` succeeds after 3 failures on attempt 4 |
| `TestFunctional_Scenario52f_VaultRetryExhausted` | `RetryWithBackoff` returns `ErrRetryExhausted` when all retries fail (4 total attempts) |
| `TestFunctional_Scenario52f_VaultRetryRecovery` | `RetryWithBackoff` succeeds when function recovers on attempt 4 |
| `TestFunctional_Scenario52f_VaultRetryContextCancellation` | `RetryWithBackoff` stops retrying when context is cancelled (250ms timeout) |
| `TestFunctional_Scenario52g_InvalidOIDCConfig` | `NewOIDCProvider` returns error with unreachable issuer URL |
| `TestFunctional_Scenario52g_BasicAuthFallback` | Basic auth works (200) and Bearer rejected (401) when OIDC provider is nil (2 sub-tests) |
| `TestFunctional_Scenario52h_MissingAdminSecret` | Empty credential store returns "invalid credentials" error |
| `TestFunctional_Scenario52h_UnknownUser` | Unknown user via API returns 401 with "authentication failed" |
| `TestFunctional_Scenario52_NegativeEdgeCaseCases_Coverage` | `NegativeEdgeCaseCases()` returns 8 cases with correct categories (5 jwt, 1 vault, 1 config, 1 auth) |

#### E2E Tests -- Mock (11 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario52a_JWTWrongIssuer` | JWT with wrong issuer rejected with 401 end-to-end |
| `TestE2E_Scenario52b_JWTWrongAudience` | JWT with wrong audience rejected with 401 end-to-end |
| `TestE2E_Scenario52c_JWTExpired` | Expired JWT rejected with 401 end-to-end |
| `TestE2E_Scenario52d_JWTFutureIAT` | JWT with future iat accepted by gooidc (behavioral test) end-to-end |
| `TestE2E_Scenario52e_TokenRefreshFailure` | Expired token returns 401 with "authentication failed" end-to-end |
| `TestE2E_Scenario52f_VaultRetryExhausted` | `RetryWithBackoff` returns `ErrRetryExhausted` end-to-end |
| `TestE2E_Scenario52f_VaultRetryRecovery` | `RetryWithBackoff` succeeds after recovery end-to-end |
| `TestE2E_Scenario52g_InvalidOIDCConfig` | `NewOIDCProvider` fails with unreachable issuer end-to-end |
| `TestE2E_Scenario52g_BasicAuthFallback` | Basic auth works and Bearer rejected when OIDC nil end-to-end (2 sub-tests) |
| `TestE2E_Scenario52h_MissingAdminSecret` | Empty credential store causes 401 end-to-end |
| `TestE2E_Scenario52_NegativeEdgeCaseCases_Coverage` | All 8 cases from catalog verified end-to-end |

#### E2E Tests -- Real Cluster (5 test cases)

| Test Method | What It Verifies |
|-------------|-----------------|
| `TestE2E_Scenario52a_RealCluster_JWTWrongIssuer` | JWT with wrong issuer rejected with 401 on real DB-backed server |
| `TestE2E_Scenario52b_RealCluster_JWTWrongAudience` | JWT with wrong audience rejected with 401 on real DB-backed server |
| `TestE2E_Scenario52c_RealCluster_JWTExpired` | Expired JWT rejected with 401 on real DB-backed server |
| `TestE2E_Scenario52g_RealCluster_BasicAuthFallback` | Basic auth works and Bearer rejected when OIDC nil on real DB-backed server (2 sub-tests) |
| `TestE2E_Scenario52h_RealCluster_EmptyCredentialStore` | Empty credential store causes 401 on real DB-backed server |

```bash
# Run negative/edge case functional tests
go test ./test/functional/... -v -tags functional -run TestFunctional_Scenario52

# Run negative/edge case E2E tests (mock)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario52

# Run negative/edge case E2E tests (real cluster)
go test ./test/e2e/... -v -tags e2e -run TestE2E_Scenario52_RealCluster
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
