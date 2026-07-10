#!/usr/bin/env bash
# =============================================================================
# load-image-kind-desktop.sh — load local docker images into the Docker Desktop
# kind-mode Kubernetes node (node "desktop-control-plane" by default).
# =============================================================================
# WHY THIS EXISTS (verified 2026-07-10 against Docker Desktop / k8s v1.36.1):
#   - The cluster is Docker Desktop's kind-based Kubernetes: the kind node is
#     NOT visible as a docker container, so `kind load docker-image` does not
#     work, and the docker engine (overlay2 store) does NOT auto-share local
#     images with the node's containerd.
#   - Loading therefore streams `docker save` through a privileged loader pod
#     into the node's containerd: `ctr -n k8s.io images import -`.
#   - k8s >= 1.36 kubelet ships the image-pull-manager (KEP-2535,
#     KubeletEnsureSecretPulledImages, on by default): images imported at
#     RUNTIME are not treated as verified-pulled, so pods fail with
#     ErrImageNeverPull / "image can't be pulled" even though the image is in
#     the CRI store. The one-time node fix (applied by --ensure-kubelet-gate):
#       featureGates: { KubeletEnsureSecretPulledImages: false }
#     appended to /var/lib/kubelet/config.yaml + kubelet restart.
#     (A backup is kept at /var/lib/kubelet/config.yaml.bak.)
#
# Usage:
#   load-image-kind-desktop.sh [--ensure-kubelet-gate] IMAGE [IMAGE...]
#
# Environment (overridable, no hardcode):
#   NODE_NAME    target node                    (default: desktop-control-plane)
#   LOADER_NS    namespace for the loader pod   (default: default)
#   LOADER_POD   loader pod name                (default: cloudberry-image-loader)
#   KEEP_LOADER  "true" keeps the loader pod    (default: true — reuse across calls)
# =============================================================================

set -euo pipefail

NODE_NAME="${NODE_NAME:-desktop-control-plane}"
LOADER_NS="${LOADER_NS:-default}"
LOADER_POD="${LOADER_POD:-cloudberry-image-loader}"
KEEP_LOADER="${KEEP_LOADER:-true}"

ENSURE_GATE="false"
if [ "${1:-}" = "--ensure-kubelet-gate" ]; then
  ENSURE_GATE="true"
  shift
fi

[ $# -ge 1 ] || { echo "usage: $0 [--ensure-kubelet-gate] IMAGE [IMAGE...]" >&2; exit 1; }

log() { echo "[load-image] $*"; }

# --- 1. Ensure the privileged loader pod exists and is Ready -----------------
ensure_loader() {
  if kubectl get pod "$LOADER_POD" -n "$LOADER_NS" >/dev/null 2>&1; then
    log "loader pod $LOADER_NS/$LOADER_POD already exists"
  else
    log "creating loader pod $LOADER_NS/$LOADER_POD on node $NODE_NAME"
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${LOADER_POD}
  namespace: ${LOADER_NS}
  labels:
    app.kubernetes.io/name: cloudberry-image-loader
spec:
  nodeName: ${NODE_NAME}
  restartPolicy: Never
  hostPID: true
  containers:
    - name: loader
      image: docker.io/library/alpine:3.20
      command: ["sleep", "86400"]
      securityContext:
        privileged: true
      volumeMounts:
        - name: host-root
          mountPath: /host
  volumes:
    - name: host-root
      hostPath:
        path: /
EOF
  fi
  kubectl wait --for=condition=Ready "pod/${LOADER_POD}" -n "$LOADER_NS" --timeout=120s
}

node_exec() {
  kubectl exec -n "$LOADER_NS" "$LOADER_POD" -- chroot /host sh -c "$*"
}

# --- 2. One-time kubelet gate for runtime-imported images --------------------
ensure_kubelet_gate() {
  if node_exec 'grep -q "KubeletEnsureSecretPulledImages: false" /var/lib/kubelet/config.yaml' 2>/dev/null; then
    log "kubelet gate already set (KubeletEnsureSecretPulledImages: false)"
    return 0
  fi
  log "setting kubelet featureGate KubeletEnsureSecretPulledImages: false (+ kubelet restart)"
  node_exec 'cp /var/lib/kubelet/config.yaml /var/lib/kubelet/config.yaml.bak &&
    printf "featureGates:\n  KubeletEnsureSecretPulledImages: false\n" >> /var/lib/kubelet/config.yaml &&
    systemctl restart kubelet' || true
  # kubectl exec may drop the connection when kubelet restarts — wait for Ready.
  for _ in $(seq 1 30); do
    if kubectl get node "$NODE_NAME" 2>/dev/null | grep -q " Ready"; then break; fi
    sleep 2
  done
  kubectl wait --for=condition=Ready "node/${NODE_NAME}" --timeout=120s
  log "kubelet gate applied"
}

check_kubelet_gate() {
  if ! node_exec 'grep -q "KubeletEnsureSecretPulledImages: false" /var/lib/kubelet/config.yaml' 2>/dev/null; then
    log "WARNING: kubelet gate NOT set — runtime-imported images will fail with"
    log "         ErrImageNeverPull/ErrImagePull on k8s >= 1.36."
    log "         Re-run with --ensure-kubelet-gate to apply the one-time fix."
  fi
}

# --- 3. Import each image -----------------------------------------------------
load_image() {
  local img="$1"
  docker image inspect "$img" >/dev/null 2>&1 || { log "ERROR: local image $img not found"; return 1; }
  log "importing $img into node containerd (k8s.io namespace)..."
  docker save "$img" | kubectl exec -i -n "$LOADER_NS" "$LOADER_POD" -- \
    chroot /host ctr -n k8s.io images import -
  # containerd normalizes docker-saved tags to docker.io/library/<name>:<tag>;
  # verify the CRI actually resolves the image reference pods will use.
  node_exec "crictl --runtime-endpoint unix:///run/containerd/containerd.sock inspecti '$img' >/dev/null" \
    && log "OK: CRI resolves $img" \
    || { log "ERROR: CRI cannot resolve $img after import"; return 1; }
}

ensure_loader
if [ "$ENSURE_GATE" = "true" ]; then
  ensure_kubelet_gate
  ensure_loader
else
  check_kubelet_gate
fi

for img in "$@"; do
  load_image "$img"
done

if [ "$KEEP_LOADER" != "true" ]; then
  kubectl delete pod "$LOADER_POD" -n "$LOADER_NS" --ignore-not-found
fi
log "done"
