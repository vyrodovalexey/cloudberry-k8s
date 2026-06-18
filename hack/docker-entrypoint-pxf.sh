#!/usr/bin/env bash
# =============================================================================
# docker-entrypoint-pxf.sh — PXF sidecar entrypoint for Kubernetes
# =============================================================================
# This script is the ENTRYPOINT for the cloudberry-pxf sidecar image. The
# operator does NOT set command/args on the sidecar container — it relies on
# this script to bootstrap and run the PXF Java server.
#
# Flow:
#   1. Ensure PXF_BASE directory exists and is writable.
#   2. Stash init-container-rendered server configs from the pxf-servers
#      emptyDir, run `pxf prepare`, then restore them.
#   3. Start PXF server (Java process listening on :5888).
#   4. Tail the PXF service log to keep the container alive and provide logs.
#
# Environment (set by the operator sidecar env):
#   PXF_HOME      — /usr/local/cloudberry-pxf (installation dir)
#   PXF_BASE      — /pxf-base (runtime config dir, emptyDir in K8s)
#   PXF_JVM_OPTS  — JVM options (e.g. -Xmx1g -Xms256m)
#   PXF_PORT      — PXF service port (default 5888)
#   PXF_LOG_LEVEL — log level (default INFO)
#   JAVA_HOME     — JRE location
# =============================================================================
set -euo pipefail

echo "=== PXF Sidecar Entrypoint ==="
echo "PXF_HOME=${PXF_HOME:-not set}"
echo "PXF_BASE=${PXF_BASE:-not set}"
echo "JAVA_HOME=${JAVA_HOME:-not set}"
echo "PXF_PORT=${PXF_PORT:-5888}"
echo "PXF_LOG_LEVEL=${PXF_LOG_LEVEL:-INFO}"

# ---------------------------------------------------------------------------
# 1. Ensure PXF_BASE exists and is writable
# ---------------------------------------------------------------------------
PXF_BASE="${PXF_BASE:-/pxf-base}"
export PXF_BASE

mkdir -p "${PXF_BASE}"

# ---------------------------------------------------------------------------
# 2. Stash init-container-rendered server configs before pxf prepare
# ---------------------------------------------------------------------------
# The operator's pxf-cred-init init container writes the resolved *-site.xml
# files into the pxf-servers emptyDir (mounted at $PXF_BASE/servers). Because
# pxf prepare requires an EMPTY $PXF_BASE we stash the server configs in a
# temp dir, run prepare, then restore them.
#
# VOLUME NOTE: the sidecar mounts TWO emptyDirs:
#   - pxf-base    at $PXF_BASE           (the PXF runtime root)
#   - pxf-servers at $PXF_BASE/servers    (shared with the init container)
# The sub-mount at $PXF_BASE/servers is a separate filesystem. We stash its
# contents into /tmp (on the container's own overlay), clear the sub-mount so
# pxf prepare sees an empty $PXF_BASE, then restore after prepare.
STASH_DIR="/tmp/pxf-servers-stash"
SERVERS_DIR="${PXF_BASE}/servers"

# List the servers dir contents (for diagnostics).
echo "Checking ${SERVERS_DIR} for init-container configs..."
ls -la "${SERVERS_DIR}/" 2>/dev/null || echo "  (servers dir not yet present)"

# Count real (non-hidden, non-. / ..) entries in the servers dir.
server_file_count=0
if [ -d "${SERVERS_DIR}" ]; then
    server_file_count=$(find "${SERVERS_DIR}" -mindepth 1 -maxdepth 3 -not -name '.*' -print 2>/dev/null | wc -l)
fi

if [ "${server_file_count}" -gt 0 ]; then
    echo "Stashing ${server_file_count} operator-mounted server config(s) before pxf prepare..."
    mkdir -p "${STASH_DIR}"
    # Use find + cp to avoid glob issues with mount-point symlinks.
    cp -a "${SERVERS_DIR}/"* "${STASH_DIR}/" 2>/dev/null || true
    # Clear the sub-mount contents so pxf prepare sees an empty PXF_BASE.
    find "${SERVERS_DIR}" -mindepth 1 -delete 2>/dev/null || rm -rf "${SERVERS_DIR:?}"/* 2>/dev/null || true
    echo "Stash complete."
else
    echo "No init-container configs found in ${SERVERS_DIR} (will proceed without)."
fi

# ---------------------------------------------------------------------------
# 2b. Run pxf prepare (creates PXF_BASE layout: conf, logs, run, servers)
# ---------------------------------------------------------------------------
echo "Running: pxf prepare"
if ! pxf prepare 2>&1; then
    echo "WARNING: pxf prepare failed, manually creating PXF_BASE layout..."
    # Manually replicate what pxf prepare does: copy conf from PXF_HOME,
    # create required directories.
    mkdir -p "${PXF_BASE}/conf" "${PXF_BASE}/logs" "${PXF_BASE}/run" \
             "${PXF_BASE}/servers/default" "${PXF_BASE}/lib"
    # Copy default configuration files from PXF_HOME
    for f in pxf-application.properties pxf-env.sh pxf-log4j2.xml pxf-profiles.xml; do
        if [ -f "${PXF_HOME}/conf/${f}" ] && [ ! -f "${PXF_BASE}/conf/${f}" ]; then
            cp -v "${PXF_HOME}/conf/${f}" "${PXF_BASE}/conf/${f}"
        fi
    done
    echo "PXF_BASE layout created manually."
fi

# ---------------------------------------------------------------------------
# 2c. Configure PXF to listen on all interfaces (required for K8s probes)
# ---------------------------------------------------------------------------
PXF_APP_PROPS="${PXF_BASE}/conf/pxf-application.properties"
if [ -f "${PXF_APP_PROPS}" ]; then
    if ! grep -q "^server.address=" "${PXF_APP_PROPS}"; then
        echo "server.address=0.0.0.0" >> "${PXF_APP_PROPS}"
        echo "Configured PXF to listen on all interfaces (0.0.0.0)"
    fi
fi

# ---------------------------------------------------------------------------
# 3. Restore server configs into PXF_BASE/servers
# ---------------------------------------------------------------------------
# The operator's pxf-cred-init container writes resolved site files in the
# NATIVE nested layout (servers/<server>/<file>.xml), so the common path is a
# straight directory-tree restore. For backward compatibility we ALSO handle
# the legacy FLAT layout ("<server>__<file>.xml") by splitting on "__".
if [ -d "${STASH_DIR}" ] && [ "$(ls -A "${STASH_DIR}" 2>/dev/null)" ]; then
    echo "Restoring server configs from stash..."
    mkdir -p "${SERVERS_DIR}"
    for entry in "${STASH_DIR}"/*; do
        [ -e "${entry}" ] || continue
        base=$(basename "${entry}")
        if [ -d "${entry}" ]; then
            # Already nested: servers/<server>/ — restore the subtree verbatim.
            cp -a "${entry}" "${SERVERS_DIR}/${base}"
            echo "  ${base}/ -> servers/${base}/ (nested, no reorg)"
        elif echo "${base}" | grep -q '__'; then
            # Legacy flat key "<server>__<file>.xml" — reorganize.
            server_name="${base%%__*}"
            file_name="${base#*__}"
            mkdir -p "${SERVERS_DIR}/${server_name}"
            cp "${entry}" "${SERVERS_DIR}/${server_name}/${file_name}"
            echo "  ${base} -> servers/${server_name}/${file_name}"
        else
            # Non-server file (e.g. connectors.properties) — copy to servers/.
            cp "${entry}" "${SERVERS_DIR}/${base}"
            echo "  ${base} -> servers/${base}"
        fi
    done
    rm -rf "${STASH_DIR}"
    echo "Server configs restored."
fi

# Show final server layout
echo "PXF_BASE/servers final layout:"
find "${PXF_BASE}/servers" -type f 2>/dev/null | sort || true

# ---------------------------------------------------------------------------
# 4. Start PXF server
# ---------------------------------------------------------------------------
echo "Starting PXF server..."

# pxf start launches the JVM in the background and returns. We need to keep
# the container alive after that. The PXF log file location depends on the
# PXF version but is typically under PXF_BASE/logs or PXF_HOME/logs.
pxf start

echo "PXF server started. Waiting for it to become ready..."

# Give the JVM a moment to initialize
sleep 5

# Check if PXF is responding
PXF_PORT="${PXF_PORT:-5888}"
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${PXF_PORT}/actuator/health" >/dev/null 2>&1; then
        echo "PXF is ready and serving on port ${PXF_PORT}"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "WARNING: PXF did not become ready within 150s, continuing anyway..."
    fi
    sleep 5
done

# ---------------------------------------------------------------------------
# 5. Keep the container alive by tailing the PXF log
# ---------------------------------------------------------------------------
# Find the log file — PXF writes to PXF_BASE/logs/pxf-service.log or
# PXF_HOME/logs/pxf-service.log depending on configuration.
PXF_LOG=""
for candidate in \
    "${PXF_BASE}/logs/pxf-service.log" \
    "${PXF_HOME}/logs/pxf-service.log" \
    "${PXF_BASE}/logs/pxf-app.log" \
    "${PXF_HOME}/logs/pxf-app.log"; do
    if [ -f "${candidate}" ]; then
        PXF_LOG="${candidate}"
        break
    fi
done

if [ -n "${PXF_LOG}" ]; then
    echo "Tailing PXF log: ${PXF_LOG}"
    exec tail -F "${PXF_LOG}"
else
    echo "No PXF log file found, keeping container alive with wait loop..."
    # Keep the container alive. If the PXF JVM dies, the health check will
    # fail and Kubernetes will restart the container.
    while true; do
        # Check if PXF is still running
        if ! pxf status >/dev/null 2>&1; then
            echo "ERROR: PXF server is no longer running, exiting..."
            exit 1
        fi
        sleep 30
    done
fi
