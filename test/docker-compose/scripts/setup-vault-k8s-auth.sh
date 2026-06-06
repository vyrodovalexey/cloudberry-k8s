#!/usr/bin/env bash
# =============================================================================
# setup-vault-k8s-auth.sh - Configure Vault Kubernetes Auth + PKI for operator
# =============================================================================
# This script configures Vault for the cloudberry-operator running in k8s:
#   1. Enables auth/kubernetes backend
#   2. Configures kubernetes auth to reach the k8s API (docker-desktop)
#   3. Creates a Vault policy for the operator
#   4. Creates a kubernetes auth role bound to the operator ServiceAccount
#   5. Creates a PKI role for webhook + cluster TLS
#   6. Stores a placeholder KV secret at secret/data/cloudberry
#
# All operations are idempotent (safe to re-run).
#
# Usage:
#   ./scripts/setup-vault-k8s-auth.sh            # full setup
#   ./scripts/setup-vault-k8s-auth.sh --verify    # verify only
#   ./scripts/setup-vault-k8s-auth.sh --ci        # CI mode (no docker-desktop)
#
# Prerequisites:
#   - Vault dev server running at VAULT_ADDR
#   - PKI engine already enabled (by setup-vault.sh)
#   - For k8s auth to work at runtime, the operator ServiceAccount
#     'cloudberry-operator' must exist in namespace 'cloudberry-test'
#
# Docker-Desktop Note:
#   The Vault container reaches the k8s API at https://kubernetes.docker.internal:6443.
#   IMPORTANT: we MUST use the hostname "kubernetes.docker.internal" (NOT
#   "host.docker.internal"). The docker-desktop API server serving certificate
#   includes "kubernetes.docker.internal" in its SANs but NOT
#   "host.docker.internal"; using the latter causes Vault's TokenReview HTTPS
#   call to fail TLS hostname verification, surfacing as "permission denied"
#   (HTTP 403) on login.
#
#   Vault validates the operator SA JWT via the k8s TokenReview API using a
#   dedicated token-reviewer ServiceAccount (system:auth-delegator). This script
#   creates that reviewer SA + a long-lived token Secret, then wires its JWT and
#   the cluster CA into the kubernetes auth config.
# =============================================================================

set -euo pipefail

VAULT_ADDR="${VAULT_ADDR:-http://127.0.0.1:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-myroot}"
K8S_HOST="${K8S_HOST:-https://kubernetes.docker.internal:6443}"
OPERATOR_SA="${OPERATOR_SA:-cloudberry-operator}"
OPERATOR_NS="${OPERATOR_NS:-cloudberry-test}"
REVIEWER_SA="${REVIEWER_SA:-vault-auth-reviewer}"
REVIEWER_SECRET="${REVIEWER_SECRET:-vault-auth-reviewer-token}"
KUBECTL="${KUBECTL:-kubectl}"
VAULT_POLICY_NAME="${VAULT_POLICY_NAME:-cloudberry-operator}"
VAULT_ROLE_NAME="${VAULT_ROLE_NAME:-cloudberry-operator}"
PKI_MOUNT="${PKI_MOUNT:-pki}"
PKI_ROLE_NAME="${PKI_ROLE_NAME:-cloudberry-operator}"
KV_MOUNT="${KV_MOUNT:-secret}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "${BLUE}[STEP]${NC}  $*"; }

# ---------------------------------------------------------------------------
# Wait for Vault to be ready
# ---------------------------------------------------------------------------
wait_for_vault() {
    log_info "Waiting for Vault at ${VAULT_ADDR}..."
    local retries=30
    for i in $(seq 1 $retries); do
        if curl -sf "${VAULT_ADDR}/v1/sys/health" > /dev/null 2>&1; then
            log_info "Vault is ready"
            return 0
        fi
        sleep 2
    done
    log_error "Vault not ready after ${retries} attempts"
    return 1
}

# ---------------------------------------------------------------------------
# Helper: vault API call
# ---------------------------------------------------------------------------
vault_api() {
    local method="$1"
    local path="$2"
    shift 2
    curl -sf \
        -X "$method" \
        -H "X-Vault-Token: ${VAULT_TOKEN}" \
        -H "Content-Type: application/json" \
        "${VAULT_ADDR}/v1/${path}" \
        "$@"
}

# ---------------------------------------------------------------------------
# Step a: Enable auth/kubernetes
# ---------------------------------------------------------------------------
enable_k8s_auth() {
    log_step "Enabling auth/kubernetes..."
    vault_api POST "sys/auth/kubernetes" \
        -d '{"type":"kubernetes"}' 2>/dev/null || true
    log_info "  auth/kubernetes enabled (or already enabled)"
}

# ---------------------------------------------------------------------------
# Step a2: Create the token-reviewer ServiceAccount (system:auth-delegator)
# ---------------------------------------------------------------------------
create_token_reviewer() {
    log_step "Creating token-reviewer ServiceAccount '${REVIEWER_SA}' in '${OPERATOR_NS}'..."
    if ! command -v "${KUBECTL}" > /dev/null 2>&1; then
        log_warn "  kubectl not found; skipping reviewer SA creation (CI mode?)"
        return 0
    fi

    "${KUBECTL}" create namespace "${OPERATOR_NS}" --dry-run=client -o yaml | "${KUBECTL}" apply -f - > /dev/null 2>&1 || true

    cat <<EOF | "${KUBECTL}" apply -f - > /dev/null 2>&1 || true
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${REVIEWER_SA}
  namespace: ${OPERATOR_NS}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ${REVIEWER_SA}-delegator
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:auth-delegator
subjects:
  - kind: ServiceAccount
    name: ${REVIEWER_SA}
    namespace: ${OPERATOR_NS}
---
apiVersion: v1
kind: Secret
metadata:
  name: ${REVIEWER_SECRET}
  namespace: ${OPERATOR_NS}
  annotations:
    kubernetes.io/service-account.name: ${REVIEWER_SA}
type: kubernetes.io/service-account-token
EOF
    # Give the token controller a moment to populate the Secret
    sleep 3
    log_info "  Token-reviewer SA '${REVIEWER_SA}' ready (system:auth-delegator)"
}

# ---------------------------------------------------------------------------
# Step b: Configure kubernetes auth
# ---------------------------------------------------------------------------
configure_k8s_auth() {
    log_step "Configuring auth/kubernetes with kubernetes_host=${K8S_HOST}..."

    local reviewer_jwt="" k8s_ca=""
    if command -v "${KUBECTL}" > /dev/null 2>&1; then
        reviewer_jwt=$("${KUBECTL}" get secret "${REVIEWER_SECRET}" -n "${OPERATOR_NS}" -o jsonpath='{.data.token}' 2>/dev/null | base64 -d 2>/dev/null || echo "")
        k8s_ca=$("${KUBECTL}" get secret "${REVIEWER_SECRET}" -n "${OPERATOR_NS}" -o jsonpath='{.data.ca\.crt}' 2>/dev/null | base64 -d 2>/dev/null || echo "")
    fi

    if [ -n "${reviewer_jwt}" ] && [ -n "${k8s_ca}" ]; then
        # Production-style: dedicated token reviewer JWT + cluster CA.
        # Use --data-urlencode to safely transmit multi-line PEM + JWT.
        curl -sf -X POST \
            -H "X-Vault-Token: ${VAULT_TOKEN}" \
            "${VAULT_ADDR}/v1/auth/kubernetes/config" \
            --data-urlencode "kubernetes_host=${K8S_HOST}" \
            --data-urlencode "token_reviewer_jwt=${reviewer_jwt}" \
            --data-urlencode "kubernetes_ca_cert=${k8s_ca}" \
            --data-urlencode "disable_iss_validation=true" > /dev/null
        log_info "  auth/kubernetes configured (token_reviewer_jwt + CA)"
    else
        # Fallback (CI / no kubectl): rely on the login JWT for token review.
        vault_api POST "auth/kubernetes/config" \
            -d "{
                \"kubernetes_host\": \"${K8S_HOST}\",
                \"disable_iss_validation\": true,
                \"disable_local_ca_jwt\": true
            }" > /dev/null
        log_warn "  auth/kubernetes configured WITHOUT reviewer JWT (fallback)"
    fi
    log_info "  kubernetes_host=${K8S_HOST}"
}

# ---------------------------------------------------------------------------
# Step c: Create Vault policy for the operator
# ---------------------------------------------------------------------------
create_operator_policy() {
    log_step "Creating Vault policy '${VAULT_POLICY_NAME}'..."

    # Vault policy in HCL format (passed as JSON-escaped string)
    local policy
    policy=$(cat <<'POLICY'
# cloudberry-operator policy
# PKI - issue certificates
path "pki/issue/cloudberry-operator" {
  capabilities = ["create", "update"]
}

# PKI - sign certificates
path "pki/sign/cloudberry-operator" {
  capabilities = ["create", "update"]
}

# PKI - read CA cert
path "pki/cert/ca" {
  capabilities = ["read"]
}

# KV - read cloudberry secrets
path "secret/data/cloudberry*" {
  capabilities = ["read"]
}
POLICY
)

    # Use the sys/policies/acl endpoint (Vault 1.10+)
    local escaped_policy
    escaped_policy=$(echo "$policy" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read()))")

    vault_api PUT "sys/policies/acl/${VAULT_POLICY_NAME}" \
        -d "{\"policy\": ${escaped_policy}}" > /dev/null
    log_info "  Policy '${VAULT_POLICY_NAME}' created/updated"
}

# ---------------------------------------------------------------------------
# Step d: Create kubernetes auth role for the operator
# ---------------------------------------------------------------------------
create_k8s_auth_role() {
    log_step "Creating auth/kubernetes/role/${VAULT_ROLE_NAME}..."
    vault_api POST "auth/kubernetes/role/${VAULT_ROLE_NAME}" \
        -d "{
            \"bound_service_account_names\": [\"${OPERATOR_SA}\"],
            \"bound_service_account_namespaces\": [\"${OPERATOR_NS}\"],
            \"policies\": [\"${VAULT_POLICY_NAME}\"],
            \"ttl\": \"1h\"
        }" > /dev/null
    log_info "  Role '${VAULT_ROLE_NAME}' created/updated"
    log_info "  bound_service_account_names=${OPERATOR_SA}"
    log_info "  bound_service_account_namespaces=${OPERATOR_NS}"
    log_info "  policies=${VAULT_POLICY_NAME}"
    log_info "  ttl=1h"
}

# ---------------------------------------------------------------------------
# Step e: Create PKI role for operator webhook + cluster TLS
# ---------------------------------------------------------------------------
create_pki_role() {
    log_step "Creating PKI role '${PKI_MOUNT}/roles/${PKI_ROLE_NAME}'..."
    vault_api POST "${PKI_MOUNT}/roles/${PKI_ROLE_NAME}" \
        -d "{
            \"allowed_domains\": [
                \"cloudberry-test.svc\",
                \"svc.cluster.local\",
                \"cloudberry-operator-webhook.cloudberry-test.svc\",
                \"cloudberry-operator-webhook.cloudberry-test.svc.cluster.local\"
            ],
            \"allow_any_name\": true,
            \"allow_localhost\": true,
            \"allow_subdomains\": true,
            \"allow_ip_sans\": true,
            \"enforce_hostnames\": false,
            \"server_flag\": true,
            \"client_flag\": true,
            \"max_ttl\": \"8760h\",
            \"key_type\": \"rsa\",
            \"key_bits\": 2048
        }" > /dev/null
    log_info "  PKI role '${PKI_ROLE_NAME}' created/updated"
    log_info "  allow_any_name=true, server_flag=true, client_flag=true"
}

# ---------------------------------------------------------------------------
# Step f: Store placeholder KV secret at secret/data/cloudberry
# ---------------------------------------------------------------------------
store_kv_secret() {
    log_step "Storing placeholder KV secret at '${KV_MOUNT}/data/cloudberry'..."

    # Check if secret already exists
    if vault_api GET "${KV_MOUNT}/data/cloudberry" > /dev/null 2>&1; then
        log_info "  Secret already exists at '${KV_MOUNT}/data/cloudberry', skipping"
        return 0
    fi

    vault_api POST "${KV_MOUNT}/data/cloudberry" \
        -d '{
            "data": {
                "admin_password": "changeme-placeholder",
                "replication_password": "changeme-placeholder"
            }
        }' > /dev/null
    log_info "  Placeholder secret stored at '${KV_MOUNT}/data/cloudberry'"
}

# ---------------------------------------------------------------------------
# Step g: Verify setup
# ---------------------------------------------------------------------------
verify_setup() {
    log_info "=== Verifying Vault Kubernetes Auth Setup ==="
    echo ""

    local errors=0

    # Check auth/kubernetes is enabled
    log_step "Checking auth/kubernetes..."
    if vault_api GET "sys/auth" 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
if 'kubernetes/' in data.get('data', data):
    print('  enabled')
else:
    sys.exit(1)
" 2>/dev/null; then
        log_info "  ✓ auth/kubernetes is enabled"
    else
        log_error "  ✗ auth/kubernetes is NOT enabled"
        errors=$((errors + 1))
    fi

    # Check kubernetes auth config
    log_step "Checking auth/kubernetes config..."
    local k8s_config
    k8s_config=$(vault_api GET "auth/kubernetes/config" 2>/dev/null || echo "{}")
    local configured_host
    configured_host=$(echo "$k8s_config" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data.get('data', {}).get('kubernetes_host', 'NOT SET'))
" 2>/dev/null || echo "PARSE ERROR")
    log_info "  kubernetes_host=${configured_host}"

    # Check policy
    log_step "Checking policy '${VAULT_POLICY_NAME}'..."
    if vault_api GET "sys/policies/acl/${VAULT_POLICY_NAME}" > /dev/null 2>&1; then
        log_info "  ✓ Policy '${VAULT_POLICY_NAME}' exists"
    else
        log_error "  ✗ Policy '${VAULT_POLICY_NAME}' NOT found"
        errors=$((errors + 1))
    fi

    # Check kubernetes auth role
    log_step "Checking auth/kubernetes/role/${VAULT_ROLE_NAME}..."
    local role_data
    role_data=$(vault_api GET "auth/kubernetes/role/${VAULT_ROLE_NAME}" 2>/dev/null || echo "{}")
    if echo "$role_data" | python3 -c "
import sys, json
data = json.load(sys.stdin)
role = data.get('data', {})
sa_names = role.get('bound_service_account_names', [])
sa_ns = role.get('bound_service_account_namespaces', [])
policies = role.get('token_policies', [])
print(f'  bound_service_account_names={sa_names}')
print(f'  bound_service_account_namespaces={sa_ns}')
print(f'  token_policies={policies}')
if not sa_names:
    sys.exit(1)
" 2>/dev/null; then
        log_info "  ✓ Role '${VAULT_ROLE_NAME}' exists and configured"
    else
        log_error "  ✗ Role '${VAULT_ROLE_NAME}' NOT found or misconfigured"
        errors=$((errors + 1))
    fi

    # Check PKI role
    log_step "Checking PKI role '${PKI_MOUNT}/roles/${PKI_ROLE_NAME}'..."
    if vault_api GET "${PKI_MOUNT}/roles/${PKI_ROLE_NAME}" > /dev/null 2>&1; then
        log_info "  ✓ PKI role '${PKI_ROLE_NAME}' exists"
    else
        log_error "  ✗ PKI role '${PKI_ROLE_NAME}' NOT found"
        errors=$((errors + 1))
    fi

    # Check KV secret
    log_step "Checking KV secret at '${KV_MOUNT}/data/cloudberry'..."
    if vault_api GET "${KV_MOUNT}/data/cloudberry" > /dev/null 2>&1; then
        log_info "  ✓ KV secret exists at '${KV_MOUNT}/data/cloudberry'"
    else
        log_warn "  ⚠ KV secret not found at '${KV_MOUNT}/data/cloudberry'"
    fi

    # Test-issue a certificate via the operator PKI role
    log_step "Test-issuing certificate via '${PKI_MOUNT}/issue/${PKI_ROLE_NAME}'..."
    local cert_resp
    cert_resp=$(vault_api POST "${PKI_MOUNT}/issue/${PKI_ROLE_NAME}" \
        -d '{
            "common_name": "cloudberry-operator-webhook.cloudberry-test.svc",
            "ttl": "1h"
        }' 2>/dev/null || echo "FAILED")

    if [ "$cert_resp" = "FAILED" ]; then
        log_error "  ✗ Failed to issue test certificate"
        errors=$((errors + 1))
    else
        local serial cn expiration
        serial=$(echo "$cert_resp" | python3 -c "
import sys, json
data = json.load(sys.stdin).get('data', {})
print(data.get('serial_number', 'N/A'))
" 2>/dev/null || echo "N/A")
        cn=$(echo "$cert_resp" | python3 -c "
import sys, json
data = json.load(sys.stdin).get('data', {})
print(data.get('certificate', '')[:80] + '...')
" 2>/dev/null || echo "N/A")
        expiration=$(echo "$cert_resp" | python3 -c "
import sys, json
data = json.load(sys.stdin).get('data', {})
print(data.get('expiration', 'N/A'))
" 2>/dev/null || echo "N/A")

        log_info "  ✓ Test certificate issued successfully"
        log_info "    serial_number=${serial}"
        log_info "    expiration=${expiration}"
        log_info "    certificate (first 80 chars): ${cn}"
    fi

    echo ""
    if [ $errors -gt 0 ]; then
        log_error "Verification FAILED with ${errors} error(s)"
        return 1
    fi

    log_info "=== All verifications PASSED ==="
    return 0
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    local verify_only=false

    # Parse arguments
    for arg in "$@"; do
        case "$arg" in
            --verify)
                verify_only=true
                ;;
            --ci)
                # In CI, k8s API is not available; skip k8s-specific config
                # but still create the policy and PKI role for testing
                K8S_HOST="${K8S_HOST:-https://kubernetes.default.svc}"
                ;;
        esac
    done

    wait_for_vault

    if [ "$verify_only" = true ]; then
        verify_setup
        exit $?
    fi

    echo ""
    log_info "=== Setting up Vault Kubernetes Auth for Cloudberry Operator ==="
    echo ""

    # Step a: Enable auth/kubernetes
    enable_k8s_auth

    # Step a2: Create token-reviewer ServiceAccount (system:auth-delegator)
    create_token_reviewer

    # Step b: Configure kubernetes auth (with reviewer JWT + CA)
    configure_k8s_auth

    # Step c: Create operator policy
    create_operator_policy

    # Step d: Create kubernetes auth role
    create_k8s_auth_role

    # Step e: Create PKI role for operator
    create_pki_role

    # Step f: Store placeholder KV secret
    store_kv_secret

    echo ""
    log_info "=== Vault Kubernetes Auth setup complete ==="
    echo ""

    # Run verification
    verify_setup
}

main "$@"
