#!/usr/bin/env bash
# =============================================================================
# okd4_render_test.sh — repeatable unit-level render tests for the OKD4/OpenShift
# Helm chart changes in deploy/helm/cloudberry-operator.
# =============================================================================
# Purpose
#   Validate — WITHOUT a live cluster — that:
#     1. (regression) With openshift.enabled=false (the chart default) NO
#        SecurityContextConstraints / Route objects are rendered and the operator
#        Deployment keeps its fixed UID 65532 (pod + container).
#     2. (OKD render) With -f okd4-example.yaml the chart renders a SCOPED custom
#        SCC (NOT anyuid), a RoleBinding of the operator SA to the built-in SCC,
#        an OKD-aware operator securityContext (no fixed UID, but runAsNonRoot +
#        seccomp RuntimeDefault + drop:[ALL] retained), and helm hook annotations
#        on the SCC.
#     3. `helm lint` passes for both the default and OKD value sets.
#     4. The CR sample (config/samples/okd4-cluster.yaml) and okd4-example.yaml are
#        valid YAML, and the CR structurally validates against the bundled CRD
#        openAPIV3Schema (best-effort, offline).
#
# Requirements: bash, helm (>=3.x), yq (mikefarah v4). grep is used for text checks.
# kubectl is used opportunistically for a client dry-run if a matching CRD is
# reachable; its absence/failure is NON-fatal (offline schema check still runs).
#
# Usage:
#   test/helm/okd4_render_test.sh            # run from repo root (or anywhere)
# Exit code: 0 = all checks passed, non-zero = at least one check failed.
# =============================================================================
set -uo pipefail

# --- Locate repo root & chart (script is at <root>/test/helm/) ----------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHART="${REPO_ROOT}/deploy/helm/cloudberry-operator"
OKD_VALUES="${CHART}/okd4-example.yaml"
CR_SAMPLE="${CHART}/config/samples/okd4-cluster.yaml"
CRD_FILE="${CHART}/crds/avsoft.io_cloudberryclusters.yaml"
RELEASE="testrel"
FULLNAME="${RELEASE}-cloudberry-operator"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

# --- Tiny assertion framework -------------------------------------------------
PASS=0
FAIL=0
RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'
[ -t 1 ] || { RED=""; GREEN=""; YELLOW=""; RESET=""; }

ok()   { PASS=$((PASS+1)); printf '  %sPASS%s %s\n' "$GREEN" "$RESET" "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  %sFAIL%s %s\n' "$RED"   "$RESET" "$1"; }
info() { printf '%s==>%s %s\n' "$YELLOW" "$RESET" "$1"; }

# assert_eq <label> <expected> <actual>
assert_eq() {
  if [ "$2" = "$3" ]; then ok "$1 (= '$2')"; else bad "$1 (expected '$2', got '$3')"; fi
}
# assert_empty <label> <value>  (value must be empty/null/whitespace)
assert_empty() {
  local v; v="$(printf '%s' "$2" | tr -d '[:space:]')"
  if [ -z "$v" ] || [ "$v" = "null" ]; then ok "$1 (empty)"; else bad "$1 (expected empty, got '$2')"; fi
}
# assert_nonempty <label> <value>
assert_nonempty() {
  local v; v="$(printf '%s' "$2" | tr -d '[:space:]')"
  if [ -n "$v" ] && [ "$v" != "null" ]; then ok "$1 (= '$2')"; else bad "$1 (unexpectedly empty)"; fi
}

# --- Preflight: required tooling ---------------------------------------------
for bin in helm yq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "ERROR: required tool '$bin' not found in PATH" >&2; exit 2; }
done
[ -d "$CHART" ] || { echo "ERROR: chart not found at $CHART" >&2; exit 2; }

echo "======================================================================"
echo " OKD4 Helm render tests — chart: $CHART"
echo "======================================================================"

# =============================================================================
# CHECK 1 — Regression: default render (openshift.enabled=false)
# =============================================================================
info "CHECK 1: default render (regression, openshift.enabled=false)"
DEFAULT="${WORKDIR}/default_render.yaml"
if ! helm template "$RELEASE" "$CHART" >"$DEFAULT" 2>"${WORKDIR}/default_err.txt"; then
  bad "helm template (default) rendered without error"
  sed 's/^/      /' "${WORKDIR}/default_err.txt"
else
  ok "helm template (default) rendered without error"

  # TC-R2/R3: ZERO SecurityContextConstraints and ZERO Route objects.
  SCC_COUNT="$(grep -cE '^kind: SecurityContextConstraints' "$DEFAULT" || true)"
  ROUTE_COUNT="$(grep -cE '^kind: Route' "$DEFAULT" || true)"
  assert_eq "no SecurityContextConstraints in default render" "0" "$SCC_COUNT"
  assert_eq "no Route in default render"                     "0" "$ROUTE_COUNT"

  # TC-R1: operator Deployment keeps fixed UID 65532 (pod + container).
  POD_UID="$(yq eval-all "select(.kind==\"Deployment\" and .metadata.name==\"${FULLNAME}\") | .spec.template.spec.securityContext.runAsUser" "$DEFAULT")"
  C_UID="$(yq eval-all "select(.kind==\"Deployment\" and .metadata.name==\"${FULLNAME}\") | .spec.template.spec.containers[0].securityContext.runAsUser" "$DEFAULT")"
  assert_eq "default pod securityContext.runAsUser"       "65532" "$POD_UID"
  assert_eq "default container securityContext.runAsUser" "65532" "$C_UID"
fi

# =============================================================================
# CHECK 2 — OKD render (-f okd4-example.yaml)
# =============================================================================
info "CHECK 2: OKD render (-f okd4-example.yaml)"
OKD="${WORKDIR}/okd_render.yaml"
if [ ! -f "$OKD_VALUES" ]; then
  bad "okd4-example.yaml exists at $OKD_VALUES"
elif ! helm template "$RELEASE" "$CHART" -f "$OKD_VALUES" >"$OKD" 2>"${WORKDIR}/okd_err.txt"; then
  bad "helm template (OKD) rendered without error"
  sed 's/^/      /' "${WORKDIR}/okd_err.txt"
else
  ok "helm template (OKD) rendered without error"

  SCC_Q='select(.kind=="SecurityContextConstraints")'

  # (3a) A SCC named like *-cluster exists.
  SCC_NAME="$(yq eval-all "${SCC_Q} | .metadata.name" "$OKD")"
  assert_eq "custom SCC name" "${FULLNAME}-cluster" "$SCC_NAME"

  # (3a) SCC is NOT the anyuid SCC (name != anyuid, no roleRef to scc:anyuid).
  case "$SCC_NAME" in
    anyuid) bad "custom SCC is NOT named anyuid" ;;
    *)      ok  "custom SCC is NOT named anyuid" ;;
  esac
  ANYUID_REF="$(yq eval-all 'select(.roleRef.name=="system:openshift:scc:anyuid") | .metadata.name' "$OKD" | grep -v '^null$' || true)"
  assert_empty "no RoleBinding/roleRef to anyuid SCC" "$ANYUID_REF"

  # (3a) allowedCapabilities EXACTLY [CHOWN,DAC_OVERRIDE,FOWNER,SETGID,SETUID].
  ALLOWED="$(yq eval-all "${SCC_Q} | .allowedCapabilities | join(\",\")" "$OKD")"
  assert_eq "SCC allowedCapabilities exact set" "CHOWN,DAC_OVERRIDE,FOWNER,SETGID,SETUID" "$ALLOWED"

  # (3a) requiredDropCapabilities includes KILL, MKNOD, NET_RAW.
  for cap in KILL MKNOD NET_RAW; do
    HAS="$(yq eval-all "${SCC_Q} | .requiredDropCapabilities | contains([\"${cap}\"])" "$OKD")"
    assert_eq "SCC requiredDropCapabilities contains ${cap}" "true" "$HAS"
  done

  # (3a) allowPrivilegedContainer false; no host namespaces.
  assert_eq "SCC allowPrivilegedContainer"  "false" "$(yq eval-all "${SCC_Q} | .allowPrivilegedContainer" "$OKD")"
  assert_eq "SCC allowHostNetwork"          "false" "$(yq eval-all "${SCC_Q} | .allowHostNetwork" "$OKD")"
  assert_eq "SCC allowHostPID"              "false" "$(yq eval-all "${SCC_Q} | .allowHostPID" "$OKD")"
  assert_eq "SCC allowHostIPC"              "false" "$(yq eval-all "${SCC_Q} | .allowHostIPC" "$OKD")"
  assert_eq "SCC allowHostDirVolumePlugin"  "false" "$(yq eval-all "${SCC_Q} | .allowHostDirVolumePlugin" "$OKD")"

  # (3b) RoleBinding binds operator SA to system:openshift:scc:restricted-v2
  #      (name is configurable via openshift.operatorSCC; default restricted-v2).
  OP_SCC="$(yq eval '.openshift.operatorSCC // "restricted-v2"' "$OKD_VALUES")"
  RB_NAME="$(yq eval-all "select(.kind==\"RoleBinding\" and .roleRef.name==\"system:openshift:scc:${OP_SCC}\") | .metadata.name" "$OKD")"
  assert_nonempty "operator SCC RoleBinding to system:openshift:scc:${OP_SCC}" "$RB_NAME"
  RB_SUBJ="$(yq eval-all "select(.kind==\"RoleBinding\" and .roleRef.name==\"system:openshift:scc:${OP_SCC}\") | .subjects[0].name" "$OKD")"
  assert_eq "operator SCC RoleBinding subject is operator SA" "$FULLNAME" "$RB_SUBJ"

  # (3c) Operator Deployment: NO fixed runAsUser, but runAsNonRoot + seccomp
  #      RuntimeDefault + drop:[ALL] retained.
  DEP="select(.kind==\"Deployment\" and .metadata.name==\"${FULLNAME}\")"
  assert_empty    "OKD pod runAsUser dropped"        "$(yq eval-all "${DEP} | .spec.template.spec.securityContext.runAsUser" "$OKD")"
  assert_empty    "OKD container runAsUser dropped"  "$(yq eval-all "${DEP} | .spec.template.spec.containers[0].securityContext.runAsUser" "$OKD")"
  assert_eq "OKD pod runAsNonRoot retained"          "true"           "$(yq eval-all "${DEP} | .spec.template.spec.securityContext.runAsNonRoot" "$OKD")"
  assert_eq "OKD pod seccomp RuntimeDefault retained" "RuntimeDefault" "$(yq eval-all "${DEP} | .spec.template.spec.securityContext.seccompProfile.type" "$OKD")"
  assert_eq "OKD container runAsNonRoot retained"    "true"           "$(yq eval-all "${DEP} | .spec.template.spec.containers[0].securityContext.runAsNonRoot" "$OKD")"
  assert_eq "OKD container drop:[ALL] retained"      "ALL"            "$(yq eval-all "${DEP} | .spec.template.spec.containers[0].securityContext.capabilities.drop | join(\",\")" "$OKD")"
  assert_eq "OKD container allowPrivilegeEscalation false" "false"    "$(yq eval-all "${DEP} | .spec.template.spec.containers[0].securityContext.allowPrivilegeEscalation" "$OKD")"

  # (3d) SCC carries helm pre-install/pre-upgrade hook annotations.
  HOOK="$(yq eval-all "${SCC_Q} | .metadata.annotations[\"helm.sh/hook\"]" "$OKD")"
  assert_eq "SCC helm.sh/hook annotation" "pre-install,pre-upgrade" "$HOOK"
fi

# =============================================================================
# CHECK 3 — helm lint (default + OKD)
# =============================================================================
info "CHECK 3: helm lint"
if helm lint "$CHART" >"${WORKDIR}/lint_default.txt" 2>&1; then
  ok "helm lint (default) passed"
else
  bad "helm lint (default) passed"; sed 's/^/      /' "${WORKDIR}/lint_default.txt"
fi
if helm lint "$CHART" -f "$OKD_VALUES" >"${WORKDIR}/lint_okd.txt" 2>&1; then
  ok "helm lint (-f okd4-example.yaml) passed"
else
  bad "helm lint (-f okd4-example.yaml) passed"; sed 's/^/      /' "${WORKDIR}/lint_okd.txt"
fi

# =============================================================================
# CHECK 4 — CR / values YAML validity + best-effort CRD schema validation
# =============================================================================
info "CHECK 4: CR & values YAML validity + CRD schema (offline, best-effort)"

# YAML parse validity.
if yq eval '.' "$OKD_VALUES" >/dev/null 2>&1; then ok "okd4-example.yaml is valid YAML"; else bad "okd4-example.yaml is valid YAML"; fi
if [ -f "$CR_SAMPLE" ] && yq eval '.' "$CR_SAMPLE" >/dev/null 2>&1; then ok "okd4-cluster.yaml (CR) is valid YAML"; else bad "okd4-cluster.yaml (CR) is valid YAML"; fi

if [ -f "$CRD_FILE" ] && [ -f "$CR_SAMPLE" ]; then
  # apiVersion / kind match the CRD.
  CRD_GROUP="$(yq eval '.spec.group' "$CRD_FILE")"
  CRD_KIND="$(yq eval '.spec.names.kind' "$CRD_FILE")"
  CRD_VER="$(yq eval '.spec.versions[0].name' "$CRD_FILE")"
  CR_API="$(yq eval '.apiVersion' "$CR_SAMPLE")"
  CR_KIND="$(yq eval '.kind' "$CR_SAMPLE")"
  assert_eq "CR apiVersion matches CRD" "${CRD_GROUP}/${CRD_VER}" "$CR_API"
  assert_eq "CR kind matches CRD"       "${CRD_KIND}"             "$CR_KIND"

  # Structural check: every top-level spec key of the CR must exist in the CRD
  # openAPIV3Schema spec.properties (offline analogue of --dry-run validation).
  SCHEMA_KEYS="$(yq eval ".spec.versions[] | select(.name==\"${CRD_VER}\") | .schema.openAPIV3Schema.properties.spec.properties | keys | .[]" "$CRD_FILE")"
  UNKNOWN=""
  while IFS= read -r k; do
    [ -z "$k" ] && continue
    echo "$SCHEMA_KEYS" | grep -qx "$k" || UNKNOWN="${UNKNOWN} $k"
  done < <(yq eval '.spec | keys | .[]' "$CR_SAMPLE")
  if [ -z "$UNKNOWN" ]; then
    ok "all CR spec keys are defined in the CRD schema"
  else
    bad "unknown CR spec keys (not in CRD schema):${UNKNOWN}"
  fi
else
  info "CRD or CR sample not found — skipping CRD schema validation"
fi

# Opportunistic (non-fatal): kubectl client-side dry-run when the CRD kind is
# registered in the reachable cluster. Skipped/ignored otherwise.
if command -v kubectl >/dev/null 2>&1; then
  if kubectl apply --dry-run=client -f "$CR_SAMPLE" >/dev/null 2>&1; then
    ok "kubectl --dry-run=client accepted the CR (CRD registered in cluster)"
  else
    info "kubectl --dry-run=client unavailable/CRD not registered — non-fatal (offline schema check already ran)"
  fi
fi

# =============================================================================
# Summary
# =============================================================================
echo "======================================================================"
printf ' RESULT: %s%d passed%s, %s%d failed%s\n' "$GREEN" "$PASS" "$RESET" "$([ "$FAIL" -gt 0 ] && echo "$RED" || echo "$GREEN")" "$FAIL" "$RESET"
echo "======================================================================"
[ "$FAIL" -eq 0 ]
