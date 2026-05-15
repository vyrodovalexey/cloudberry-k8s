#!/usr/bin/env bash
# =============================================================================
# setup-keycloak.sh - Configure Keycloak for backend OIDC testing
# =============================================================================
# This script creates:
#   1. Realm "test" for  authentication
#   2. Client "cloudberry-operator" (service account for operator obtain tokens)
#   3. Test users for user auth testing
#
# Usage:
#   ./scripts/setup-keycloak.sh [--verify] [--ci]
# =============================================================================

set -euo pipefail

KEYCLOAK_ADDR="${KEYCLOAK_ADDR:-http://127.0.0.1:8090}"
KEYCLOAK_ADMIN="${KEYCLOAK_ADMIN:-admin}"
KEYCLOAK_ADMIN_PASSWORD="${KEYCLOAK_ADMIN_PASSWORD:-admin}"

#OIDC realm (service-to-service)
REALM="test"
CLOUDBERRY_OPERATOR_CLIENT_ID="cloudberry-operator"
CLOUDBERRY_OPERATOR_CLIENT_SECRET="some-secret"
CI_MODE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ---------------------------------------------------------------------------
# Wait for Keycloak to be ready
# ---------------------------------------------------------------------------
wait_for_keycloak() {
    log_info "Waiting for Keycloak at ${KEYCLOAK_ADDR}..."
    local retries=60
    for i in $(seq 1 $retries); do
        if curl -sf "${KEYCLOAK_ADDR}/health/ready" > /dev/null 2>&1; then
            log_info "Keycloak is ready"
            return 0
        fi
        # Fallback: try realm endpoint
        if curl -sf "${KEYCLOAK_ADDR}/realms/master" > /dev/null 2>&1; then
            log_info "Keycloak is ready (via realm endpoint)"
            return 0
        fi
        sleep 3
    done
    log_error "Keycloak not ready after ${retries} attempts"
    return 1
}

# ---------------------------------------------------------------------------
# Disable SSL requirements on Keycloak realms
# ---------------------------------------------------------------------------
disable_ssl_requirements() {
    log_info "Disabling SSL requirements on Keycloak realms..."

    # Authenticate kcadm.sh with the Keycloak admin
    docker exec keycloak_web /opt/keycloak/bin/kcadm.sh config credentials \
        --server http://127.0.0.1:8090 \
        --realm master \
        --user "${KEYCLOAK_ADMIN}" \
        --password "${KEYCLOAK_ADMIN_PASSWORD}"

    # Disable SSL on master realm
    docker exec keycloak_web /opt/keycloak/bin/kcadm.sh update realms/master \
        -s sslRequired=NONE

    log_info "  SSL requirement disabled on 'master' realm"

    # Disable SSL on test realm (will be created later, but set it if it already exists)
    docker exec keycloak_web /opt/keycloak/bin/kcadm.sh update "realms/${REALM}" \
        -s sslRequired=NONE 2>/dev/null || true

    log_info "  SSL requirements configured"
}

# ---------------------------------------------------------------------------
# Disable SSL on test realm (after it has been created)
# ---------------------------------------------------------------------------
disable_ssl_on_test_realm() {
    log_info "Disabling SSL requirement on '${REALM}' realm..."
    docker exec keycloak_web /opt/keycloak/bin/kcadm.sh update "realms/${REALM}" \
        -s sslRequired=NONE
    log_info "  SSL requirement disabled on '${REALM}' realm"
}

# ---------------------------------------------------------------------------
# Get admin token
# ---------------------------------------------------------------------------
ADMIN_TOKEN=""

get_admin_token() {
    log_info "Obtaining admin token..."
    local resp
    resp=$(curl -sf -X POST \
        "${KEYCLOAK_ADDR}/realms/master/protocol/openid-connect/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -H "X-Forwarded-Proto: https" \
        -d "username=${KEYCLOAK_ADMIN}" \
        -d "password=${KEYCLOAK_ADMIN_PASSWORD}" \
        -d "grant_type=password" \
        -d "client_id=admin-cli")

    ADMIN_TOKEN=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

    if [ -z "$ADMIN_TOKEN" ]; then
        log_error "Failed to obtain admin token"
        return 1
    fi
    log_info "Admin token obtained"
}

# ---------------------------------------------------------------------------
# Helper: Keycloak admin API call
# ---------------------------------------------------------------------------
kc_api() {
    local method="$1"
    local path="$2"
    shift 2
    curl -sf \
        -X "$method" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -H "X-Forwarded-Proto: https" \
        "${KEYCLOAK_ADDR}/admin/${path}" \
        "$@"
}

# ---------------------------------------------------------------------------
# Create realm
# ---------------------------------------------------------------------------
create_realm() {
    local realm_name="$1"
    log_info "Creating realm '${realm_name}'..."

    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -H "X-Forwarded-Proto: https" \
        "${KEYCLOAK_ADDR}/admin/realms" \
        -d "{
            \"realm\": \"${realm_name}\",
            \"enabled\": true,
            \"sslRequired\": \"none\",
            \"registrationAllowed\": false,
            \"loginWithEmailAllowed\": false,
            \"duplicateEmailsAllowed\": true,
            \"resetPasswordAllowed\": false,
            \"editUsernameAllowed\": false,
            \"bruteForceProtected\": false
        }")

    if [ "$status" = "201" ]; then
        log_info "  Realm '${realm_name}' created"
    elif [ "$status" = "409" ]; then
        log_info "  Realm '${realm_name}' already exists"
    else
        log_warn "  Realm creation returned status ${status}"
    fi
}

# ---------------------------------------------------------------------------
# Create client
# ---------------------------------------------------------------------------
create_client() {
    local realm="$1"
    local client_id="$2"
    local client_secret="$3"
    local service_accounts="${4:-true}"
    local direct_access="${5:-false}"
    local standard_flow="${6:-false}"

    log_info "Creating client '${client_id}' in realm '${realm}'..."

    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -H "X-Forwarded-Proto: https" \
        "${KEYCLOAK_ADDR}/admin/realms/${realm}/clients" \
        -d "{
            \"clientId\": \"${client_id}\",
            \"enabled\": true,
            \"publicClient\": false,
            \"secret\": \"${client_secret}\",
            \"serviceAccountsEnabled\": ${service_accounts},
            \"directAccessGrantsEnabled\": ${direct_access},
            \"standardFlowEnabled\": ${standard_flow},
            \"protocol\": \"openid-connect\",
            \"clientAuthenticatorType\": \"client-secret\"
        }")

    if [ "$status" = "201" ]; then
        log_info "  Client '${client_id}' created"
    elif [ "$status" = "409" ]; then
        log_info "  Client '${client_id}' already exists"
    else
        log_warn "  Client creation returned status ${status}"
    fi
}

# ---------------------------------------------------------------------------
# Create realm role
# ---------------------------------------------------------------------------
create_realm_role() {
    local realm="$1"
    local role_name="$2"

    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -H "X-Forwarded-Proto: https" \
        "${KEYCLOAK_ADDR}/admin/realms/${realm}/roles" \
        -d "{\"name\": \"${role_name}\"}")

    if [ "$status" = "201" ] || [ "$status" = "409" ]; then
        return 0
    fi
    log_warn "  Role '${role_name}' creation returned status ${status}"
}

# ---------------------------------------------------------------------------
# Create user
# ---------------------------------------------------------------------------
create_user() {
    local realm="$1"
    local username="$2"
    local password="$3"
    local email="${4:-${username}@test.local}"

    log_info "Creating user '${username}' in realm '${realm}'..."

    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -H "X-Forwarded-Proto: https" \
        "${KEYCLOAK_ADDR}/admin/realms/${realm}/users" \
        -d "{
            \"username\": \"${username}\",
            \"enabled\": true,
            \"emailVerified\": true,
            \"email\": \"${email}\",
            \"firstName\": \"${username}\",
            \"lastName\": \"TestUser\",
            \"requiredActions\": [],
            \"credentials\": [{
                \"type\": \"password\",
                \"value\": \"${password}\",
                \"temporary\": false
            }]
        }")

    if [ "$status" = "201" ]; then
        log_info "  User '${username}' created"
    elif [ "$status" = "409" ]; then
        log_info "  User '${username}' already exists"
    else
        log_warn "  User creation returned status ${status}"
    fi
}

# ---------------------------------------------------------------------------
# Setup backend-test realm (service-to-service OIDC)
# ---------------------------------------------------------------------------
setup_realm() {
    log_info "=== Setting up backend-test realm (service-to-service OIDC) ==="

    create_realm "${REALM}"

    # Gateway service account client (uses this to obtain tokens via client_credentials)
    create_client "${REALM}" "${CLOUDBERRY_OPERATOR_CLIENT_ID}" "${CLOUDBERRY_OPERATOR_CLIENT_SECRET}" "true" "true" "false"

    log_info "Backend realm setup complete"
    log_info "  Gateway can obtain tokens via:"
    log_info "    POST ${KEYCLOAK_ADDR}/realms/${REALM}/protocol/openid-connect/token"
    log_info "    grant_type=client_credentials"
    log_info "    client_id=${CLOUDBERRY_OPERATOR_CLIENT_ID}"
    log_info "    client_secret=${CLOUDBERRY_OPERATOR_CLIENT_SECRET}"

    # Create realm roles
    for role in user admin reader writer; do
        create_realm_role "${REALM}" "$role"
    done

    # Create test users
    create_user "${REALM}" "testuser" "testpass"
    create_user "${REALM}" "adminuser" "adminpass"
    create_user "${REALM}" "reader" "readerpass"

}

# ---------------------------------------------------------------------------
# Verify setup
# ---------------------------------------------------------------------------
verify_setup() {
    log_info "Verifying Keycloak setup..."
    local errors=0

    # Check backend-test realm
    if curl -sf "${KEYCLOAK_ADDR}/realms/${REALM}" > /dev/null 2>&1; then
        log_info "  ✓ Realm '${REALM}' exists"
    else
        log_error "  ✗ Realm '${REALM}' not found"
        errors=$((errors + 1))
    fi

    # Test client_credentials grant on test realm
    log_info "  Testing client_credentials grant on '${REALM}'..."
    local token_resp
    token_resp=$(curl -sf -X POST \
        "${KEYCLOAK_ADDR}/realms/${REALM}/protocol/openid-connect/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "grant_type=client_credentials" \
        -d "client_id=${CLOUDBERRY_OPERATOR_CLIENT_ID}" \
        -d "client_secret=${CLOUDBERRY_OPERATOR_CLIENT_SECRET}" 2>/dev/null || echo "")

    if echo "$token_resp" | python3 -c "import sys,json; t=json.load(sys.stdin).get('access_token',''); sys.exit(0 if t else 1)" 2>/dev/null; then
        log_info "  ✓ Client credentials grant works for '${CLOUDBERRY_OPERATOR_CLIENT_ID}'"
    else
        log_error "  ✗ Client credentials grant failed for '${CLOUDBERRY_OPERATOR_CLIENT_ID}'"
        errors=$((errors + 1))
    fi

    # Test password grant on test realm
    log_info "  Testing password grant on '${REALM}'..."
    local user_token_resp
    user_token_resp=$(curl -sf -X POST \
        "${KEYCLOAK_ADDR}/realms/${REALM}/protocol/openid-connect/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "grant_type=password" \
        -d "client_id=${CLOUDBERRY_OPERATOR_CLIENT_ID}" \
        -d "client_secret=${CLOUDBERRY_OPERATOR_CLIENT_SECRET}" \
        -d "username=testuser" \
        -d "password=testpass" 2>/dev/null || echo "")

    if echo "$user_token_resp" | python3 -c "import sys,json; t=json.load(sys.stdin).get('access_token',''); sys.exit(0 if t else 1)" 2>/dev/null; then
        log_info "  ✓ Password grant works for 'testuser'"
    else
        log_error "  ✗ Password grant failed for 'testuser'"
        errors=$((errors + 1))
    fi

    # Check OIDC discovery endpoints
    for realm in "${REALM}"; do
        if curl -sf "${KEYCLOAK_ADDR}/realms/${realm}/.well-known/openid-configuration" > /dev/null 2>&1; then
            log_info "  ✓ OIDC discovery endpoint for '${realm}' works"
        else
            log_error "  ✗ OIDC discovery endpoint for '${realm}' not found"
            errors=$((errors + 1))
        fi
    done

    if [ $errors -gt 0 ]; then
        log_error "Verification failed with ${errors} error(s)"
        return 1
    fi

    log_info "Keycloak setup verified successfully"
    return 0
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    if [ "${1:-}" = "--verify" ]; then
        wait_for_keycloak
        get_admin_token
        verify_setup
        exit $?
    fi

    for arg in "$@"; do
      case "$arg" in
        --ci)     CI_MODE=true ;;
      esac
    done

    wait_for_keycloak
    get_admin_token

    setup_realm
    if [ "$CI_MODE" = false ]; then
      disable_ssl_requirements
      # After realm is created, also disable SSL on the test realm
      disable_ssl_on_test_realm
    fi


    verify_setup

    log_info ""
    log_info "=== Keycloak setup complete ==="
    log_info ""
    log_info "OIDC:"
    log_info "  Realm:         ${REALM}"
    log_info "  Issuer URL:    ${KEYCLOAK_ADDR}/realms/${REALM}"
    log_info "  Operator client: ${CLOUDBERRY_OPERATOR_CLIENT_ID} / ${CLOUDBERRY_OPERATOR_CLIENT_SECRET}"
    log_info ""
    log_info "User authentication:"
    log_info "  Realm:         ${REALM}"
    log_info "  Issuer URL:    ${KEYCLOAK_ADDR}/realms/${REALM}"
    log_info "  Client:        ${CLOUDBERRY_OPERATOR_CLIENT_ID} / ${CLOUDBERRY_OPERATOR_CLIENT_SECRET}"
    log_info "  Test users:    testuser/testpass, adminuser/adminpass, reader/readerpass"
    log_info ""
    log_info "Test token acquisition:"
    log_info "  # Service-to-service token:"
    log_info "  curl -X POST ${KEYCLOAK_ADDR}/realms/${REALM}/protocol/openid-connect/token \\"
    log_info "    -d grant_type=client_credentials \\"
    log_info "    -d client_id=${CLOUDBERRY_OPERATOR_CLIENT_ID} \\"
    log_info "    -d client_secret=${CLOUDBERRY_OPERATOR_CLIENT_SECRET}"
}

main "$@"
