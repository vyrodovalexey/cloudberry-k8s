#!/usr/bin/env bash
# =============================================================================
# docker-entrypoint-cloudberry.sh
# =============================================================================
# Entrypoint for the Apache Cloudberry Database Kubernetes image.
# Handles initialization and startup for coordinator and segment roles.
#
# Environment variables:
#   PGDATA                      - Data directory path (default: /data/pgdata)
#   POSTGRES_PASSWORD           - Admin password for gpadmin user
#   CLOUDBERRY_ROLE             - Role: coordinator, standby, primary, mirror
#   CLOUDBERRY_CONTENT_ID       - Segment content ID (-1 for coordinator)
#   CLOUDBERRY_COORDINATOR_HOST - Hostname of the coordinator node
#   CLOUDBERRY_SEGMENT_PORT     - Port for this segment (default: 5432)
#   CLOUDBERRY_DB_ID            - Database ID for this segment
#   CLOUDBERRY_MAX_CONNECTIONS  - Max connections (default: 100)
#   CLOUDBERRY_GPEXPAND_MANAGED - When "true", a scale-out segment added by
#                                 gpexpand: init_segment SKIPS initdb so gpexpand
#                                 initializes the datadir from the coordinator
#                                 template (default: false / normal first-boot).
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
# GPHOME is intentionally not readonly — cloudberry-env.sh reassigns it.
GPHOME="${GPHOME:-/usr/local/cloudberry-db}"
export GPHOME

# CLUSTER_SSH_PORT is the single source of truth for the intra-cluster SSH port.
# Under the scoped OKD restricted-v2 SCC the DB container runs as gpadmin
# (UID 1000) WITHOUT NET_BIND_SERVICE and WITHOUT dependable sudo-to-root, so it
# cannot bind the privileged port 22. A rootless sshd on a high port (>=1024)
# needs no capability and no root; 2022 is the proven default (matches the
# backup subsystem precedent). gpexpand/gpssh/gpsync are redirected to this port
# via the gpadmin ~/.ssh/config `Port` directive (see write_ssh_client_config).
readonly CLUSTER_SSH_PORT="${CLUSTER_SSH_PORT:-2022}"

# gpadmin-writable runtime paths for the rootless sshd (no root needed): host
# key + pid file must live where gpadmin can write since /etc/ssh and /run are
# root-owned under the SCC.
readonly SSHD_HOSTKEY_DIR="${SSHD_HOSTKEY_DIR:-/home/gpadmin/.ssh/hostkeys}"
readonly SSHD_HOSTKEY="${SSHD_HOSTKEY_DIR}/ssh_host_ed25519_key"
readonly SSHD_PID_FILE="${SSHD_PID_FILE:-/home/gpadmin/run/sshd.pid}"

readonly PGDATA="${PGDATA:-/data/pgdata}"
readonly CLOUDBERRY_ROLE="${CLOUDBERRY_ROLE:-coordinator}"
readonly CLOUDBERRY_COORDINATOR_HOST="${CLOUDBERRY_COORDINATOR_HOST:-localhost}"
readonly CLOUDBERRY_SEGMENT_PORT="${CLOUDBERRY_SEGMENT_PORT:-5432}"
readonly CLOUDBERRY_MAX_CONNECTIONS="${CLOUDBERRY_MAX_CONNECTIONS:-100}"
readonly COORDINATOR_DATA_DIR="${PGDATA}/gpseg-1"

# ---------------------------------------------------------------------------
# Derive content ID and DB ID from POD_NAME ordinal for segments.
# For coordinator/standby, content ID is always -1.
# For segments, the ordinal from the StatefulSet pod name is used.
# ---------------------------------------------------------------------------
derive_ids_from_pod_name() {
    local content_id="${CLOUDBERRY_CONTENT_ID:--1}"
    local db_id="${CLOUDBERRY_DB_ID:-1}"

    case "${CLOUDBERRY_ROLE}" in
        coordinator)
            content_id="-1"
            db_id="1"
            ;;
        standby)
            content_id="-1"
            db_id="2"
            ;;
        primary|mirror)
            if [ -n "${POD_NAME:-}" ]; then
                # Extract ordinal from pod name (e.g., "cluster-segment-primary-2" -> "2")
                local ordinal
                ordinal="${POD_NAME##*-}"
                if [[ "${ordinal}" =~ ^[0-9]+$ ]]; then
                    content_id="${ordinal}"
                    # DB ID: coordinator=1, standby=2, segments start at 3
                    # Primary segments: 3 + ordinal
                    # Mirror segments: 3 + segment_count + ordinal
                    local seg_count="${CLOUDBERRY_SEGMENT_COUNT:-4}"
                    if [ "${CLOUDBERRY_ROLE}" = "primary" ]; then
                        db_id=$(( 3 + ordinal ))
                    else
                        db_id=$(( 3 + seg_count + ordinal ))
                    fi
                fi
            fi
            ;;
    esac

    echo "${content_id}" "${db_id}"
}

# Derive content ID and DB ID
read -r CLOUDBERRY_CONTENT_ID CLOUDBERRY_DB_ID <<< "$(derive_ids_from_pod_name)"
readonly CLOUDBERRY_CONTENT_ID
readonly CLOUDBERRY_DB_ID
readonly SEGMENT_DATA_DIR="${PGDATA}/gpseg${CLOUDBERRY_CONTENT_ID}"

# ---------------------------------------------------------------------------
# Source Cloudberry environment
# ---------------------------------------------------------------------------
load_cloudberry_env() {
    if [ -f "${GPHOME}/cloudberry-env.sh" ]; then
        # shellcheck disable=SC1091
        source "${GPHOME}/cloudberry-env.sh"
    elif [ -f "${GPHOME}/greenplum_path.sh" ]; then
        # shellcheck disable=SC1091
        source "${GPHOME}/greenplum_path.sh"
    fi

    # Ensure Cloudberry binaries are on PATH
    case ":${PATH}:" in
        *":${GPHOME}/bin:"*) ;;
        *) export PATH="${GPHOME}/bin:${PATH}" ;;
    esac

    export LD_LIBRARY_PATH="${GPHOME}/lib:${LD_LIBRARY_PATH:-}"
}

# ---------------------------------------------------------------------------
# Ensure gpbackup toolchain binaries are symlinked into /usr/local/bin.
# The operator's gpbackup_s3_plugin config uses
#   executablepath: /usr/local/bin/gpbackup_s3_plugin
# and gpbackup dispatches the plugin to segments using that path. On the
# cloudberry-official image the binaries live at $GPHOME/bin; the Dockerfile
# creates the symlinks at build time, but this runtime safety net re-creates
# them idempotently in case the image was rebuilt without the symlink step.
# ---------------------------------------------------------------------------
ensure_gpbackup_symlinks() {
    local bins="gpbackup gprestore gpbackup_helper gpbackup_s3_plugin"
    for bin in ${bins}; do
        if [ ! -x "/usr/local/bin/${bin}" ] && [ -x "${GPHOME}/bin/${bin}" ]; then
            ln -sf "${GPHOME}/bin/${bin}" "/usr/local/bin/${bin}" 2>/dev/null || \
            sudo ln -sf "${GPHOME}/bin/${bin}" "/usr/local/bin/${bin}" 2>/dev/null || true
        fi
    done
}

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
log_info()  { echo "[entrypoint] INFO:  $*"; }
log_warn()  { echo "[entrypoint] WARN:  $*" >&2; }
log_error() { echo "[entrypoint] ERROR: $*" >&2; }

# ---------------------------------------------------------------------------
# Directory where the operator mounts the cluster-wide SHARED gpadmin SSH
# keypair Secret (read-only). When present, the entrypoint installs it into
# /home/gpadmin/.ssh with the strict permissions sshd requires INSTEAD of
# generating a per-pod key, so the whole cluster shares one SSH identity.
# ---------------------------------------------------------------------------
readonly SHARED_SSH_DIR="${SHARED_SSH_DIR:-/etc/cloudberry/ssh}"

# ---------------------------------------------------------------------------
# Write a silent SSH client config so coordinator->segment SSH (used by
# gpbackup/gprestore MPP dispatch) produces NO extra stdout/stderr. gpbackup's
# command-runner treats any noise on the SSH session as a failure (exit 254):
#   - host-key "Warning: Permanently added ..." (StrictHostKeyChecking), and
#   - the host-key check itself.
# Disabling StrictHostKeyChecking + UserKnownHostsFile + lowering LogLevel keeps
# the session clean. PAM lastlog/MOTD noise is suppressed separately (see
# silence_login_noise + the image-build sshd/pam changes).
#
# The `Port ${CLUSTER_SSH_PORT}` (2022) directive is REQUIRED under the scoped
# restricted-v2 SCC: the cluster pods run a ROOTLESS sshd on 2022 (no privileged
# 22 bind is possible), and gpexpand/gpssh/gpsync/gpbackup all shell out to the
# OpenSSH client, which honors this per-host `Port`. The only ssh targets in
# these pods are other cluster pods, so a wildcard `Host *` block is safe.
# ---------------------------------------------------------------------------
write_ssh_client_config() {
    cat > /home/gpadmin/.ssh/config <<EOF
Host *
  Port ${CLUSTER_SSH_PORT}
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR
  BatchMode yes
EOF
    chmod 600 /home/gpadmin/.ssh/config 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Install the SHARED gpadmin SSH keypair (mounted by the operator) into
# /home/gpadmin/.ssh with the strict ownership/permissions sshd requires. A
# Secret volume is symlinked and not 0600, which sshd rejects, so we copy.
# Returns 0 when the shared keys were installed, 1 when they are absent (caller
# then falls back to per-pod key generation for non-operator/local runs).
# ---------------------------------------------------------------------------
install_shared_ssh_keys() {
    if [ ! -f "${SHARED_SSH_DIR}/id_ed25519" ] || [ ! -f "${SHARED_SSH_DIR}/id_ed25519.pub" ]; then
        return 1
    fi
    log_info "Installing shared gpadmin SSH keypair from ${SHARED_SSH_DIR}..."
    install -m 600 "${SHARED_SSH_DIR}/id_ed25519"     /home/gpadmin/.ssh/id_ed25519
    install -m 644 "${SHARED_SSH_DIR}/id_ed25519.pub" /home/gpadmin/.ssh/id_ed25519.pub
    if [ -f "${SHARED_SSH_DIR}/authorized_keys" ]; then
        install -m 600 "${SHARED_SSH_DIR}/authorized_keys" /home/gpadmin/.ssh/authorized_keys
    else
        install -m 600 "${SHARED_SSH_DIR}/id_ed25519.pub" /home/gpadmin/.ssh/authorized_keys
    fi
    chmod 700 /home/gpadmin/.ssh 2>/dev/null || true
    chown -R gpadmin:gpadmin /home/gpadmin/.ssh 2>/dev/null || true
    return 0
}

# ---------------------------------------------------------------------------
# Suppress PAM session failures and login noise for gpadmin SSH sessions.
#
# ROOT CAUSE: the stock /etc/pam.d/sshd session stack includes container-hostile
# modules (pam_namespace.so, pam_selinux.so, pam_loginuid.so, pam_lastlog.so)
# via "session include password-auth" and "session include postlogin". These
# cause pam_open_session() to fail => sshd logs "PAM session not opened,
# exiting" => every remote command exits 254.
#
# The preferred fix is at image-build time (Dockerfile.cloudberry-official
# replaces /etc/pam.d/sshd with a minimal container-friendly version). This
# runtime fallback re-applies the same minimal PAM config idempotently, so the
# fix is robust even if the image was not rebuilt.
# ---------------------------------------------------------------------------
silence_login_noise() {
    touch /home/gpadmin/.hushlogin 2>/dev/null || true

    local sshd_cfg=/etc/ssh/sshd_config
    if [ -w "${sshd_cfg}" ] || sudo test -w "${sshd_cfg}" 2>/dev/null; then
        sudo sed -i \
            -e 's/^[#[:space:]]*PrintMotd.*/PrintMotd no/' \
            -e 's/^[#[:space:]]*PrintLastLog.*/PrintLastLog no/' \
            "${sshd_cfg}" 2>/dev/null || true
        grep -q '^PrintMotd no'     "${sshd_cfg}" 2>/dev/null || echo 'PrintMotd no'     | sudo tee -a "${sshd_cfg}" >/dev/null 2>&1 || true
        grep -q '^PrintLastLog no'  "${sshd_cfg}" 2>/dev/null || echo 'PrintLastLog no'  | sudo tee -a "${sshd_cfg}" >/dev/null 2>&1 || true
    fi

    # Replace /etc/pam.d/sshd with a minimal container-friendly version that
    # keeps only pam_unix.so for session management. This is idempotent — if the
    # image already has the correct content, the write is a no-op.
    if [ -f /etc/pam.d/sshd ]; then
        sudo tee /etc/pam.d/sshd >/dev/null 2>&1 <<'PAMEOF' || true
#%PAM-1.0
auth       substack     password-auth
account    required     pam_nologin.so
account    include      password-auth
password   include      password-auth
session    required     pam_unix.so
session    optional     pam_keyinit.so force revoke
PAMEOF
    fi
}

# ---------------------------------------------------------------------------
# Start SSH daemon (needed for inter-segment communication and gpbackup MPP
# dispatch). The cluster-wide SHARED keypair is preferred (mounted by the
# operator); a per-pod key is generated only as a fallback for local runs.
# ---------------------------------------------------------------------------
start_sshd() {
    log_info "Starting SSH daemon..."
    sudo mkdir -p /run/sshd 2>/dev/null || true
    mkdir -p /home/gpadmin/.ssh 2>/dev/null || true
    chmod 700 /home/gpadmin/.ssh 2>/dev/null || true

    # Generate SSH host keys if they don't exist (first start)
    if [ ! -f /etc/ssh/ssh_host_ed25519_key ]; then
        log_info "Generating SSH host keys..."
        sudo ssh-keygen -A 2>/dev/null || log_warn "Failed to generate SSH host keys"
    fi

    # Prefer the operator-mounted SHARED keypair; fall back to a per-pod key so
    # non-operator/local runs still work.
    if install_shared_ssh_keys; then
        log_info "Using cluster-wide shared gpadmin SSH identity"
    elif [ ! -f /home/gpadmin/.ssh/id_ed25519 ]; then
        log_info "Shared SSH keys absent; generating per-pod gpadmin SSH keypair (fallback)..."
        ssh-keygen -t ed25519 -N '' -C 'gpadmin@cloudberry-k8s' \
                   -f /home/gpadmin/.ssh/id_ed25519 2>/dev/null || true
        cat /home/gpadmin/.ssh/id_ed25519.pub >> /home/gpadmin/.ssh/authorized_keys 2>/dev/null || true
        chmod 600 /home/gpadmin/.ssh/id_ed25519 /home/gpadmin/.ssh/authorized_keys 2>/dev/null || true
        chmod 644 /home/gpadmin/.ssh/id_ed25519.pub 2>/dev/null || true
    fi

    # Silence host-key warnings on the client side and PAM lastlog/MOTD noise so
    # gpbackup/gprestore SSH sessions stay clean (exit-254 root cause).
    write_ssh_client_config
    silence_login_noise

    start_rootless_sshd
}

# ---------------------------------------------------------------------------
# Start a ROOTLESS sshd on CLUSTER_SSH_PORT (default 2022) as gpadmin.
#
# Under the scoped OKD restricted-v2 SCC the DB container runs as gpadmin
# (UID 1000) WITHOUT NET_BIND_SERVICE and WITHOUT dependable sudo-to-root, so it
# cannot bind privileged port 22. Binding a high port (>=1024) needs no
# capability and no root. gpexpand's segment-init phase SSHes to the new segment
# hosts to seed their data dirs (basebackup of the coordinator template); those
# ssh/gpssh/gpsync calls are redirected to 2022 via the gpadmin ~/.ssh/config
# `Port` directive, so a rootless sshd on 2022 is exactly what gpexpand reaches.
#
# `UsePAM=no` sidesteps the container-hostile PAM stack entirely (no sudo needed
# to edit /etc/pam.d/sshd), which also fixes the exit-254 class of problems. Host
# key + pid file live in gpadmin-writable paths. The legacy sudo:22 daemon is
# still started as a best-effort LOCAL/non-OKD fallback so current non-SCC
# behavior is preserved.
# ---------------------------------------------------------------------------
start_rootless_sshd() {
    # gpadmin-owned runtime dirs (no root/privileged bind needed for :2022).
    mkdir -p "${SSHD_HOSTKEY_DIR}" "$(dirname "${SSHD_PID_FILE}")" 2>/dev/null || true

    # Generate a gpadmin-writable host key if missing (does NOT need root, unlike
    # ssh-keygen -A which writes into root-owned /etc/ssh).
    if [ ! -f "${SSHD_HOSTKEY}" ]; then
        log_info "Generating rootless sshd host key at ${SSHD_HOSTKEY}..."
        ssh-keygen -t ed25519 -N '' -f "${SSHD_HOSTKEY}" 2>/dev/null || \
            log_warn "Failed to generate rootless sshd host key"
    fi

    log_info "Starting rootless SSH daemon on port ${CLUSTER_SSH_PORT}..."
    # -f /dev/null: skip /etc/ssh/sshd_config which is root-owned and unreadable
    # under the restricted-v2 SCC (Permission denied). All config is passed via -o.
    /usr/sbin/sshd -f /dev/null \
        -o "Port=${CLUSTER_SSH_PORT}" \
        -o "HostKey=${SSHD_HOSTKEY}" \
        -o "PidFile=${SSHD_PID_FILE}" \
        -o "PubkeyAuthentication=yes" \
        -o "PasswordAuthentication=no" \
        -o "UsePAM=no" \
        -o "StrictModes=no" \
        -o "PrintMotd=no" \
        -o "PrintLastLog=no" \
        -o "AuthorizedKeysFile=/home/gpadmin/.ssh/authorized_keys" \
        2>/dev/null || \
        log_warn "Rootless SSH daemon failed to start on ${CLUSTER_SSH_PORT} (non-fatal)"

    # Legacy sudo:22 daemon — best-effort LOCAL/non-OKD fallback only. Under the
    # restricted-v2 SCC this is expected to fail (no root/NET_BIND_SERVICE); the
    # rootless :2022 daemon above is the load-bearing listener.
    sudo /usr/sbin/sshd 2>/dev/null || \
        log_info "Privileged sshd on :22 not started (expected under restricted-v2 SCC; rootless :${CLUSTER_SSH_PORT} is used)"
}

# ---------------------------------------------------------------------------
# Apply configuration from /etc/cloudberry/ if present
# ---------------------------------------------------------------------------
apply_config_overrides() {
    local data_dir="$1"

    if [ -d /etc/cloudberry ] && [ -n "$(ls -A /etc/cloudberry/ 2>/dev/null)" ]; then
        log_info "Applying configuration overrides from /etc/cloudberry/..."

        # Reference the mounted ConfigMap path directly so runtime updates are
        # picked up by PostgreSQL on pg_reload_conf() without requiring a restart.
        # Kubernetes propagates ConfigMap changes to the mounted volume automatically.
        if [ -f /etc/cloudberry/postgresql.conf ]; then
            log_info "Applying postgresql.conf overrides (direct mount reference)"
            if ! grep -q "include_if_exists = '/etc/cloudberry/postgresql.conf'" "${data_dir}/postgresql.conf" 2>/dev/null; then
                echo "include_if_exists = '/etc/cloudberry/postgresql.conf'" >> "${data_dir}/postgresql.conf"
            fi
        fi

        # Copy pg_hba.conf overrides
        if [ -f /etc/cloudberry/pg_hba.conf ]; then
            log_info "Applying pg_hba.conf overrides"
            cp /etc/cloudberry/pg_hba.conf "${data_dir}/pg_hba.conf"
        fi
    fi
}

# ---------------------------------------------------------------------------
# Configure pg_hba.conf for network access
# ---------------------------------------------------------------------------
configure_hba() {
    local data_dir="$1"
    local hba_file="${data_dir}/pg_hba.conf"

    log_info "Configuring pg_hba.conf..."

    # Append network access rules if not already present
    if ! grep -q "0.0.0.0/0" "${hba_file}" 2>/dev/null; then
        {
            echo ""
            echo "# Added by cloudberry-k8s entrypoint"
            echo "# Inter-segment communication: gpadmin must use trust for coordinator<->segment"
            echo "local   all   gpadmin                 trust"
            echo "host    all   gpadmin   0.0.0.0/0     trust"
            echo "host    all   gpadmin   ::/0          trust"
            echo "# Other users use password auth"
            echo "local   all   all                     scram-sha-256"
            echo "host    all   all   127.0.0.1/32      trust"
            echo "host    all   all   ::1/128            trust"
            echo "host    all   all   0.0.0.0/0          scram-sha-256"
            echo "host    all   all   ::/0               scram-sha-256"
            echo "host    replication  all  0.0.0.0/0    trust"
            echo "host    replication  all  ::/0         trust"
        } >> "${hba_file}"
    fi
}

# ---------------------------------------------------------------------------
# Set gpadmin password
# ---------------------------------------------------------------------------
set_admin_password() {
    local data_dir="$1"

    if [ -n "${POSTGRES_PASSWORD:-}" ]; then
        log_info "Setting gpadmin password..."
        # Wait for PostgreSQL to accept local connections (uses trust auth)
        local retries=30
        while ! pg_isready -U gpadmin -q 2>/dev/null; do
            retries=$((retries - 1))
            if [ "${retries}" -le 0 ]; then
                log_warn "Timed out waiting for PostgreSQL to accept connections for password setup"
                return 1
            fi
            sleep 1
        done
        # Use local socket connection (trust auth) to set the password
        psql -U gpadmin -d postgres \
            -c "ALTER USER gpadmin PASSWORD '${POSTGRES_PASSWORD}';" 2>/dev/null || \
            log_warn "Failed to set gpadmin password (may already be set)"
    fi
}

# ---------------------------------------------------------------------------
# Initialize coordinator data directory
# ---------------------------------------------------------------------------
init_coordinator() {
    local data_dir="${COORDINATOR_DATA_DIR}"

    if [ -f "${data_dir}/PG_VERSION" ]; then
        log_info "Coordinator data directory already initialized at ${data_dir}"
        return 0
    fi

    log_info "Initializing coordinator data directory at ${data_dir}..."
    mkdir -p "${data_dir}"

    # Use initdb directly for the coordinator
    "${GPHOME}/bin/initdb" \
        --pgdata="${data_dir}" \
        --encoding=UTF-8 \
        --locale=en_US.UTF-8 \
        --username=gpadmin \
        --data-checksums \
        --auth-local=trust \
        --auth-host=scram-sha-256

    # Set Cloudberry internal parameters (gp_contentid, gp_dbid)
    # These are stored in internal.auto.conf and read at startup.
    {
        echo "gp_contentid = -1"
        echo "gp_dbid = ${CLOUDBERRY_DB_ID}"
    } > "${data_dir}/internal.auto.conf"

    # Configure coordinator-specific settings in postgresql.conf
    {
        echo ""
        echo "# Cloudberry coordinator settings (added by entrypoint)"
        echo "listen_addresses = '*'"
        echo "port = ${CLOUDBERRY_SEGMENT_PORT}"
        echo "max_connections = ${CLOUDBERRY_MAX_CONNECTIONS}"
        echo "shared_buffers = '128MB'"
        echo "wal_level = replica"
        echo "max_wal_senders = 10"
        echo "wal_keep_size = '512MB'"
        echo "hot_standby = on"
    } >> "${data_dir}/postgresql.conf"

    configure_hba "${data_dir}"
    log_info "Coordinator initialization complete"
}

# ---------------------------------------------------------------------------
# Decide whether THIS segment pod is a gpexpand-managed scale-out addition.
#
# Returns success (0) when the pod must SKIP self-initdb and let gpexpand
# initialize its datadir. Two signals, in order of precedence:
#   1. Explicit override CLOUDBERRY_GPEXPAND_MANAGED=true (local/manual runs).
#   2. Operator-provided CLOUDBERRY_EXPANSION_BASE_COUNT (the pre-scale segment
#      count) present AND this pod's content id (ordinal) >= that base — i.e.
#      this is one of the newly-added segments in an in-progress scale-out.
# Any other case (flag unset/false, no base count, or ordinal < base) => normal
# first-boot initdb, unchanged.
# ---------------------------------------------------------------------------
is_gpexpand_managed_segment() {
    if [ "${CLOUDBERRY_GPEXPAND_MANAGED:-false}" = "true" ]; then
        return 0
    fi

    local base="${CLOUDBERRY_EXPANSION_BASE_COUNT:-}"
    # Only act on a valid, non-empty integer base count.
    if [[ "${base}" =~ ^[0-9]+$ ]]; then
        local content="${CLOUDBERRY_CONTENT_ID:--1}"
        if [[ "${content}" =~ ^[0-9]+$ ]] && [ "${content}" -ge "${base}" ]; then
            return 0
        fi
    fi
    return 1
}

# ---------------------------------------------------------------------------
# Upsert the Cloudberry segment content id GUC into a datadir's
# internal.auto.conf.
#
# In Cloudberry 2.1 the segment content id is carried by the GUC `gp_contentid`
# in the datadir's internal.auto.conf (read at postmaster start), NOT by a
# `postgres -C <id>` flag (that is standard PostgreSQL "print a config parameter
# and exit"). init_segment writes `gp_contentid = <id>` for NORMAL segments; a
# gpexpand-managed segment's datadir is basebackup'd from the COORDINATOR
# template and therefore inherits the WRONG value (`gp_contentid = -1`), so it
# must be corrected before postgres starts.
#
# This helper REPLACES any existing `gp_contentid` line (the inherited -1) and
# appends one if none is present, using the EXACT `gp_contentid = <id>` spelling
# init_segment uses so normal and gpexpand-managed segments are identical on
# disk. It is idempotent and safe to call repeatedly.
# ---------------------------------------------------------------------------
upsert_gp_contentid() {
    local data_dir="$1"
    local content_id="$2"
    local conf="${data_dir}/internal.auto.conf"

    # Guard: only a valid integer content id may be written.
    if ! [[ "${content_id}" =~ ^-?[0-9]+$ ]]; then
        log_warn "refusing to write gp_contentid: invalid content id '${content_id}'"
        return 1
    fi

    # Remove any prior gp_contentid line (e.g. the coordinator template's -1),
    # then append the correct value. Matches init_segment's `gp_contentid = <id>`.
    # grep -v to a temp file + mv is used (instead of `sed -i`) so the behavior is
    # identical on GNU sed (container) and BSD sed (dev macOS) — `sed -i` has an
    # incompatible in-place backup-suffix syntax between the two.
    if [ -f "${conf}" ]; then
        local tmp="${conf}.cbk.$$"
        grep -v -E '^[[:space:]]*gp_contentid[[:space:]]*=' "${conf}" > "${tmp}" 2>/dev/null || true
        mv "${tmp}" "${conf}" 2>/dev/null || rm -f "${tmp}" 2>/dev/null || true
    fi
    echo "gp_contentid = ${content_id}" >> "${conf}"
    log_info "set gp_contentid = ${content_id} in ${conf}"
}

# ---------------------------------------------------------------------------
# Location of the REAL pg_ctl binary AFTER the shim is installed in its place.
# The shim is installed AT ${GPHOME}/bin/pg_ctl (replacing the real binary,
# which is renamed to pg_ctl.real) so it ALWAYS wins regardless of PATH order.
# ---------------------------------------------------------------------------
readonly GPEXPAND_REAL_PGCTL_SUFFIX=".real"
# Marker line the generated shim carries so the install is idempotent and
# distinguishable from the real binary (which is ELF, not a shell script).
readonly GPEXPAND_SHIM_MARKER="# cloudberry-k8s gpexpand pg_ctl shim"

# ---------------------------------------------------------------------------
# NO-OP: the gpexpand pg_ctl shim is now BAKED AT BUILD TIME (as root) into the
# image at ${GPHOME}/bin/pg_ctl (the real binary renamed to pg_ctl.real). See
# hack/gpexpand-pgctl-shim.sh + Dockerfile.cloudberry-official.
#
# WHY BAKED, NOT RUNTIME-INSTALLED: at RUNTIME the pod runs as gpadmin (UID
# 1000) under the restricted-v2 SCC and ${GPHOME}/bin is root-owned, so the
# previous runtime rename+write here failed with "cannot write
# /usr/local/cloudberry-db/bin; gpexpand pg_ctl shim NOT installed" and the shim
# never installed — gpexpand's pg_ctl start then never upserted gp_contentid and
# postgres FATAL'd "contentid not specified". Baking at build time (root) side-
# steps the SCC entirely, and the baked shim DERIVES the content id from the
# `-D <datadir>` gpseg<N> suffix so the SAME shim is correct in every per-pod
# image (no runtime env / no entrypoint cooperation needed).
#
# This function is kept (invoked by main()) as a TOLERANT no-op: it merely
# VERIFIES the baked shim is present and logs, and NEVER fails on the read-only
# ${GPHOME}/bin (so a normal segment start path is unaffected). The two readonly
# constants above are retained for documentation/compat.
# ---------------------------------------------------------------------------
install_gpexpand_pgctl_shim() {
    local pgctl="${GPHOME}/bin/pg_ctl"

    if grep -q "${GPEXPAND_SHIM_MARKER}" "${pgctl}" 2>/dev/null; then
        log_info "gpexpand pg_ctl shim is baked into the image at ${pgctl} (content id derived from the -D gpseg<N> datadir); no runtime install needed"
    else
        # The image predates the baked shim (or was rebuilt without it). Under
        # the restricted-v2 SCC the runtime rename is NOT possible (root-owned
        # ${GPHOME}/bin), so we do NOT attempt it — tolerate and warn. The
        # gpexpand_handoff_to_steady_state upsert_gp_contentid path still
        # corrects gp_contentid on this pod's own datadir after gpexpand init.
        log_warn "gpexpand pg_ctl shim NOT baked into ${pgctl}; relying on the post-gpexpand handoff to correct gp_contentid (rebuild the cluster image to bake the shim)"
    fi
    return 0
}

# ---------------------------------------------------------------------------
# Pre-init idle loop for a gpexpand-managed scale-out segment.
#
# A newly-added, gpexpand-managed segment has an EMPTY datadir: gpexpand (driven
# by the operator's coordinator-exec Job) initializes it from the coordinator
# template over SSH AND starts postgres on it via `pg_ctl -D <datadir> ... start`
# (using the segment port 5432). Until that happens there is NO local database
# to start, so this pod must NOT exec `postgres` on the empty datadir (that would
# exit immediately -> CrashLoopBackOff -> the pod is NOT Running, and the
# operator's scale-out gate waits for the pod to be Running before it runs
# gpexpand -> deadlock).
#
# CRITICAL: during this idle phase the entrypoint must NOT bind or hold the
# segment postgres port (5432) and must NOT run any postgres process, otherwise
# gpexpand's `pg_ctl start` cannot bind 5432 ("could not start server"). This
# loop only polls the filesystem for PG_VERSION and re-asserts sshd:2022 — it
# never touches 5432.
#
# We KEEP THE CONTAINER RUNNING (its foreground process is this wait loop; sshd
# was already started in the background by start_sshd, so gpexpand can reach the
# pod on ssh:2022) and poll for PG_VERSION to appear. Once gpexpand has
# initialized the datadir, we return so the caller performs the post-gpexpand
# HANDOFF (see gpexpand_handoff_to_steady_state) rather than racing gpexpand by
# also starting postgres on the same datadir/port.
#
# Bounded by CLOUDBERRY_GPEXPAND_WAIT_SECONDS (default 3600s) so a wedged
# expansion eventually fails the container instead of idling forever.
# ---------------------------------------------------------------------------
wait_for_gpexpand_init() {
    local data_dir="$1"
    local waited=0
    local max_wait="${CLOUDBERRY_GPEXPAND_WAIT_SECONDS:-3600}"
    local interval="${CLOUDBERRY_GPEXPAND_WAIT_INTERVAL:-5}"

    log_info "gpexpand-managed segment (content=${CLOUDBERRY_CONTENT_ID}): datadir ${data_dir} is empty; keeping container Running (sshd up on ${CLUSTER_SSH_PORT}, NOT holding segment port ${CLOUDBERRY_SEGMENT_PORT}) until gpexpand initializes it"

    while [ ! -f "${data_dir}/PG_VERSION" ]; do
        if [ "${waited}" -ge "${max_wait}" ]; then
            log_error "gpexpand did not initialize ${data_dir} within ${max_wait}s; giving up"
            return 1
        fi
        # Re-assert sshd is alive so gpexpand can always reach this pod on 2022.
        # This loop deliberately does NO postgres/port work: port 5432 stays free
        # for gpexpand's pg_ctl start.
        if [ -n "${SSHD_PID_FILE:-}" ] && [ -f "${SSHD_PID_FILE}" ]; then
            if ! kill -0 "$(cat "${SSHD_PID_FILE}" 2>/dev/null)" 2>/dev/null; then
                log_warn "sshd not running during gpexpand wait; restarting"
                start_sshd
            fi
        fi
        sleep "${interval}"
        waited=$((waited + interval))
    done

    log_info "gpexpand initialized ${data_dir} (PG_VERSION present after ~${waited}s); handing off to steady state"
    return 0
}

# ---------------------------------------------------------------------------
# Post-gpexpand handoff: turn a gpexpand-managed segment into a normal, durable
# segment whose postgres is the container's MAIN process.
#
# ROOT CAUSE this addresses: gpexpand owns BOTH the datadir init AND the postgres
# start on the new segment. It runs, over SSH, roughly:
#   pg_ctl -D <datadir> -o "-p 5432 -c gp_role=utility -M" start
# The instant PG_VERSION appears, wait_for_gpexpand_init returns. If the
# entrypoint then ALSO starts its own postgres (the background password-setup
# start AND the final `exec postgres`), TWO postmasters race for the same datadir
# and port 5432 -> "could not start server" (postmaster.pid lock / address in
# use). That is the observed pg_ctl failure.
#
# The correct handoff is:
#   1. Wait for gpexpand's pg_ctl-started postmaster to come fully up (its
#      postmaster.pid + a live PID). We do NOT start postgres ourselves here.
#   2. Fix ownership/permissions so gpadmin (UID 1000) owns the whole datadir
#      (gpexpand rsync may leave template-copied files with other ownership).
#   3. Stop that gpexpand/pg_ctl-started instance cleanly (fast shutdown) so the
#      datadir/port are released with NO data loss.
#   4. UPSERT the correct `gp_contentid` into the datadir's internal.auto.conf.
#      gpexpand basebackups the datadir from the COORDINATOR template, which
#      carries `gp_contentid = -1`; a segment MUST carry its own content id, so
#      we replace the inherited -1 with this pod's ordinal-derived content id
#      (the same `gp_contentid = <id>` line init_segment writes for normal
#      segments). This is what lets the entrypoint start the segment WITHOUT the
#      (wrong) `-C` flag — Cloudberry 2.1 reads the content id from this GUC.
#   5. Return so the caller `exec postgres` re-launches it as the container's
#      PID-appropriate main process (probes pass; survives pod restart, on which
#      PG_VERSION now exists so this is a normal existing segment).
#
# If no gpexpand-started postmaster is detected (e.g. gpexpand started it in
# utility mode and already stopped it, which is the normal gpexpand flow), we
# simply ensure nothing is holding the datadir and return so the caller starts
# postgres in the normal execute-mode way.
#
# NOTE on gp_dbid: unlike gp_contentid, the coordinator template's gp_dbid is
# NOT reused here. gpexpand assigns and persists the new segment's dbid itself
# when it registers the segment (it writes the segment's own internal.auto.conf
# during segment-init), so the entrypoint only needs to correct gp_contentid.
# ---------------------------------------------------------------------------
gpexpand_handoff_to_steady_state() {
    local data_dir="$1"
    local waited=0
    local max_wait="${CLOUDBERRY_GPEXPAND_HANDOFF_SECONDS:-300}"
    local interval="${CLOUDBERRY_GPEXPAND_WAIT_INTERVAL:-5}"
    local pidfile="${data_dir}/postmaster.pid"

    log_info "post-gpexpand handoff for ${data_dir}: ensuring gpexpand's pg_ctl-started instance is stopped before the entrypoint takes over as main process"

    # (1) Fix ownership/permissions FIRST so a gpadmin-owned pg_ctl stop and the
    # subsequent exec postgres both succeed. gpexpand rsync of the coordinator
    # template can leave files owned by another uid; postgres refuses to start a
    # datadir it does not own. Best-effort chown (may already be correct).
    if [ -n "$(find "${data_dir}" ! -uid "$(id -u)" -print -quit 2>/dev/null)" ]; then
        log_info "normalizing datadir ownership to gpadmin ($(id -un):$(id -gn)) after gpexpand rsync"
        chown -R "$(id -u):$(id -g)" "${data_dir}" 2>/dev/null || \
            sudo chown -R "$(id -u):$(id -g)" "${data_dir}" 2>/dev/null || \
            log_warn "could not fully chown ${data_dir}; postgres start may fail"
    fi
    chmod 700 "${data_dir}" 2>/dev/null || true

    # (2) If gpexpand left a running postmaster, wait briefly for it to be fully
    # up, then stop it cleanly so the entrypoint can re-exec it as PID 1's child
    # (the container main process). We poll the pidfile rather than the port so we
    # never bind 5432 ourselves.
    if [ -f "${pidfile}" ]; then
        local pg_pid
        pg_pid="$(head -n1 "${pidfile}" 2>/dev/null || true)"
        if [[ "${pg_pid}" =~ ^[0-9]+$ ]] && kill -0 "${pg_pid}" 2>/dev/null; then
            log_info "gpexpand-started postmaster is running (pid=${pg_pid}); stopping it (fast) for clean handoff"
            "${GPHOME}/bin/pg_ctl" -D "${data_dir}" -m fast -w -t "${max_wait}" stop 2>/dev/null || \
                log_warn "pg_ctl stop returned non-zero; attempting to wait for shutdown"
        fi
    fi

    # (3) Confirm the datadir/port are released: no live postmaster + no stale
    # pidfile that would block our exec postgres. pg_ctl -w already waited, but be
    # defensive against a stale pidfile.
    while [ -f "${pidfile}" ]; do
        local stale_pid
        stale_pid="$(head -n1 "${pidfile}" 2>/dev/null || true)"
        if ! [[ "${stale_pid}" =~ ^[0-9]+$ ]] || ! kill -0 "${stale_pid}" 2>/dev/null; then
            log_info "removing stale postmaster.pid (owner pid ${stale_pid:-none} not running)"
            rm -f "${pidfile}" 2>/dev/null || true
            break
        fi
        if [ "${waited}" -ge "${max_wait}" ]; then
            log_warn "gpexpand postmaster still holding ${pidfile} after ${max_wait}s; proceeding (exec postgres may report an existing lock)"
            break
        fi
        sleep "${interval}"
        waited=$((waited + interval))
    done

    # (4) Correct gp_contentid in internal.auto.conf. The datadir was
    # basebackup'd from the coordinator template (gp_contentid = -1); replace it
    # with this pod's ordinal-derived content id so the segment starts with the
    # right content id WITHOUT any `-C` flag (Cloudberry 2.1 reads gp_contentid
    # from this GUC). Uses the exact `gp_contentid = <id>` spelling init_segment
    # writes for normal segments.
    upsert_gp_contentid "${data_dir}" "${CLOUDBERRY_CONTENT_ID}" || \
        log_warn "could not correct gp_contentid in ${data_dir}/internal.auto.conf; segment start may use wrong content id"

    log_info "post-gpexpand handoff complete; entrypoint will now exec postgres as the container main process on ${data_dir}"
    return 0
}

# ---------------------------------------------------------------------------
# Initialize segment data directory
# ---------------------------------------------------------------------------
init_segment() {
    local data_dir="${SEGMENT_DATA_DIR}"
    local role="${CLOUDBERRY_ROLE}"

    if [ -f "${data_dir}/PG_VERSION" ]; then
        log_info "Segment data directory already initialized at ${data_dir}"
        return 0
    fi

    # Scale-out (gpexpand) segments: SKIP initdb. A segment added during an
    # active expansion is physically initialized by gpexpand's segment-init phase
    # (basebackup from the coordinator template + catalog fix-ups), which creates
    # every existing user database on the new segment with matching OIDs. If this
    # pod self-initdb's an empty stock cluster first, gpexpand either rejects the
    # pre-existing non-empty datadir or the segment diverges (empty catalog ->
    # "database ... does not exist" on EXPAND TABLE). When this pod is a
    # gpexpand-managed scale-out addition, leave the datadir untouched and let
    # gpexpand initialize it. Normal first-boot bring-up is unchanged.
    #
    # The operator cannot flag individual pods via the SHARED StatefulSet env, so
    # it exports CLOUDBERRY_EXPANSION_BASE_COUNT (the pre-scale segment count)
    # while a scale-out is in progress; each pod derives the flag PER-POD by
    # comparing its own content id (ordinal) against that base: ordinal >= base
    # => new segment => gpexpand-managed. An explicit CLOUDBERRY_GPEXPAND_MANAGED
    # override is still honored for local/manual runs.
    if is_gpexpand_managed_segment; then
        log_info "Segment ${role} (content=${CLOUDBERRY_CONTENT_ID}) is gpexpand-managed; skipping initdb (gpexpand will initialize ${data_dir} from the coordinator template)"
        return 0
    fi

    log_info "Initializing ${role} segment (content=${CLOUDBERRY_CONTENT_ID}) at ${data_dir}..."
    mkdir -p "${data_dir}"

    "${GPHOME}/bin/initdb" \
        --pgdata="${data_dir}" \
        --encoding=UTF-8 \
        --locale=en_US.UTF-8 \
        --username=gpadmin \
        --data-checksums \
        --auth-local=trust \
        --auth-host=scram-sha-256

    # Set Cloudberry internal parameters (gp_contentid, gp_dbid)
    {
        echo "gp_contentid = ${CLOUDBERRY_CONTENT_ID}"
        echo "gp_dbid = ${CLOUDBERRY_DB_ID}"
    } > "${data_dir}/internal.auto.conf"

    # Configure segment-specific settings in postgresql.conf
    {
        echo ""
        echo "# Cloudberry segment settings (added by entrypoint)"
        echo "listen_addresses = '*'"
        echo "port = ${CLOUDBERRY_SEGMENT_PORT}"
        echo "max_connections = ${CLOUDBERRY_MAX_CONNECTIONS}"
        echo "shared_buffers = '128MB'"
        echo "wal_level = replica"
        echo "max_wal_senders = 10"
        echo "wal_keep_size = '512MB'"
        echo "hot_standby = on"
    } >> "${data_dir}/postgresql.conf"

    configure_hba "${data_dir}"
    log_info "Segment initialization complete"
}

# ---------------------------------------------------------------------------
# Initialize standby coordinator (via pg_basebackup from primary coordinator)
# ---------------------------------------------------------------------------
init_standby() {
    local data_dir="${COORDINATOR_DATA_DIR}"

    if [ -f "${data_dir}/PG_VERSION" ]; then
        log_info "Standby data directory already initialized at ${data_dir}"
        return 0
    fi

    log_info "Initializing standby coordinator from ${CLOUDBERRY_COORDINATOR_HOST}..."
    mkdir -p "${data_dir}"

    # Wait for the primary coordinator to be available
    local retries=60
    while ! pg_isready -h "${CLOUDBERRY_COORDINATOR_HOST}" -p "${CLOUDBERRY_SEGMENT_PORT}" -U gpadmin -q 2>/dev/null; do
        retries=$((retries - 1))
        if [ "${retries}" -le 0 ]; then
            log_error "Timed out waiting for primary coordinator at ${CLOUDBERRY_COORDINATOR_HOST}:${CLOUDBERRY_SEGMENT_PORT}"
            exit 1
        fi
        log_info "Waiting for primary coordinator... (${retries} retries left)"
        sleep 5
    done

    # Use pg_basebackup to create the standby
    # Cloudberry requires --target-gp-dbid for pg_basebackup
    pg_basebackup \
        -h "${CLOUDBERRY_COORDINATOR_HOST}" \
        -p "${CLOUDBERRY_SEGMENT_PORT}" \
        -U gpadmin \
        -D "${data_dir}" \
        -X stream \
        -R \
        --checkpoint=fast \
        --target-gp-dbid "${CLOUDBERRY_DB_ID}"

    # Update internal.auto.conf for standby
    {
        echo "gp_contentid = -1"
        echo "gp_dbid = ${CLOUDBERRY_DB_ID}"
    } > "${data_dir}/internal.auto.conf"

    log_info "Standby initialization complete"
}

# ---------------------------------------------------------------------------
# Initialize mirror segment (via pg_basebackup from primary segment)
# ---------------------------------------------------------------------------
init_mirror() {
    local data_dir="${SEGMENT_DATA_DIR}"

    if [ -f "${data_dir}/PG_VERSION" ]; then
        log_info "Mirror data directory already initialized at ${data_dir}"
        return 0
    fi

    log_info "Initializing mirror segment (content=${CLOUDBERRY_CONTENT_ID}) from primary..."
    mkdir -p "${data_dir}"

    # Derive the primary segment host from the pod name pattern.
    # Mirror pod: <cluster>-segment-mirror-<N>
    # Primary pod: <cluster>-segment-primary-<N>
    # The primary is addressable via the headless segment service.
    local primary_host="${CLOUDBERRY_PRIMARY_HOST:-}"
    local primary_port="${CLOUDBERRY_PRIMARY_PORT:-${CLOUDBERRY_SEGMENT_PORT}}"

    if [ -z "${primary_host}" ] && [ -n "${POD_NAME:-}" ]; then
        # Derive primary pod name from mirror pod name
        # e.g., "scenario1-cluster-segment-mirror-0" -> "scenario1-cluster-segment-primary-0"
        local primary_pod_name
        primary_pod_name="$(echo "${POD_NAME}" | sed 's/-segment-mirror-/-segment-primary-/')"
        # Use the segment service for DNS resolution
        local segment_svc="${CLOUDBERRY_SEGMENT_SERVICE:-}"
        if [ -z "${segment_svc}" ]; then
            # Derive service name from pod name: remove the last component (ordinal) and "mirror"
            # e.g., "scenario1-cluster-segment-mirror-0" -> "scenario1-cluster-seg-hl"
            segment_svc="$(echo "${POD_NAME}" | sed 's/-segment-mirror-[0-9]*$/-seg-hl/')"
        fi
        primary_host="${primary_pod_name}.${segment_svc}"
        log_info "Derived primary host: ${primary_host}"
    fi

    # Fallback to coordinator host if primary host is still empty
    primary_host="${primary_host:-${CLOUDBERRY_COORDINATOR_HOST}}"

    local retries=60
    while ! pg_isready -h "${primary_host}" -p "${primary_port}" -U gpadmin -q 2>/dev/null; do
        retries=$((retries - 1))
        if [ "${retries}" -le 0 ]; then
            log_error "Timed out waiting for primary segment at ${primary_host}:${primary_port}"
            exit 1
        fi
        log_info "Waiting for primary segment... (${retries} retries left)"
        sleep 5
    done

    # Cloudberry requires --target-gp-dbid for pg_basebackup
    pg_basebackup \
        -h "${primary_host}" \
        -p "${primary_port}" \
        -U gpadmin \
        -D "${data_dir}" \
        -X stream \
        -R \
        --checkpoint=fast \
        --target-gp-dbid "${CLOUDBERRY_DB_ID}"

    # Update internal.auto.conf for mirror
    {
        echo "gp_contentid = ${CLOUDBERRY_CONTENT_ID}"
        echo "gp_dbid = ${CLOUDBERRY_DB_ID}"
    } > "${data_dir}/internal.auto.conf"

    log_info "Mirror initialization complete"
}

# ---------------------------------------------------------------------------
# Register segments with the coordinator (gpinitsystem equivalent)
# ---------------------------------------------------------------------------
# In a traditional Cloudberry/Greenplum deployment, gpinitsystem populates
# gp_segment_configuration via SSH. In Kubernetes, we simulate this by
# directly inserting rows into the catalog after the coordinator starts.
register_segments() {
    local data_dir="$1"
    local seg_count="${CLOUDBERRY_SEGMENT_COUNT:-4}"
    local port="${CLOUDBERRY_SEGMENT_PORT}"
    local coordinator_host="${CLOUDBERRY_COORDINATOR_HOST}"
    local segment_svc="${CLOUDBERRY_SEGMENT_SERVICE:-}"

    # Derive segment service from coordinator host if not set.
    # Coordinator host format: <cluster>-coord-hl
    # Segment service format: <cluster>-seg-hl
    if [ -z "${segment_svc}" ]; then
        segment_svc="$(echo "${coordinator_host}" | sed 's/-coord-hl$/-seg-hl/')"
    fi

    log_info "Registering segments with coordinator (count=${seg_count})..."

    # Wait for PostgreSQL to accept connections.
    local retries=30
    while ! pg_isready -U gpadmin -q 2>/dev/null; do
        retries=$((retries - 1))
        if [ "${retries}" -le 0 ]; then
            log_warn "Timed out waiting for coordinator to accept connections for segment registration"
            return 1
        fi
        sleep 1
    done

    # Check if segments are already registered.
    local seg_registered
    seg_registered="$(psql -U gpadmin -d postgres -tAc "SELECT count(*) FROM gp_segment_configuration;" 2>/dev/null || echo "0")"
    if [ "${seg_registered}" -gt "0" ]; then
        log_info "Segments already registered (${seg_registered} entries), skipping registration"
        return 0
    fi

    log_info "Populating gp_segment_configuration..."

    # Derive the cluster name prefix from the coordinator host.
    # Coordinator host: <cluster>-coord-hl -> cluster prefix: <cluster>
    local cluster_prefix
    cluster_prefix="$(echo "${coordinator_host}" | sed 's/-coord-hl$//')"

    # Build the coordinator hostname (pod-0 of the coordinator StatefulSet).
    # Cloudberry has a 64-char hostname limit in gp_segment_configuration.
    # Service names are kept short (<cluster>-coord-hl) to stay within limit.
    local coord_hostname="${cluster_prefix}-coordinator-0.${coordinator_host}"
    if [ ${#coord_hostname} -gt 64 ]; then
        log_warn "Coordinator hostname exceeds 64 chars (${#coord_hostname}): ${coord_hostname}"
        log_warn "Consider using a shorter cluster name"
    fi

    # Register coordinator (content=-1, dbid=1, role=p).
    # Note: allow_system_table_mods is required to INSERT into system catalogs.
    psql -U gpadmin -d postgres -c "
        SET allow_system_table_mods = true;
        INSERT INTO gp_segment_configuration
            (dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
        VALUES
            (1, -1, 'p', 'p', 's', 'u', ${port}, '${coord_hostname}', '${coord_hostname}', '${data_dir}');
    " 2>/dev/null || log_warn "Failed to register coordinator in gp_segment_configuration"

    # Register primary segments (content=0..N-1, dbid=3..3+N-1).
    local i=0
    while [ "${i}" -lt "${seg_count}" ]; do
        local dbid=$(( 3 + i ))
        local primary_hostname="${cluster_prefix}-segment-primary-${i}.${segment_svc}"
        local seg_datadir="${PGDATA}/gpseg${i}"

        psql -U gpadmin -d postgres -c "
            SET allow_system_table_mods = true;
            INSERT INTO gp_segment_configuration
                (dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
            VALUES
                (${dbid}, ${i}, 'p', 'p', 's', 'u', ${port}, '${primary_hostname}', '${primary_hostname}', '${seg_datadir}');
        " 2>/dev/null || log_warn "Failed to register primary segment ${i}"

        i=$(( i + 1 ))
    done

    # Register mirror segments if mirroring is enabled.
    # Detect mirroring by checking if CLOUDBERRY_MIRRORING_ENABLED is set,
    # or if mirror pods exist (the builder sets this env var when mirroring is enabled).
    if [ "${CLOUDBERRY_MIRRORING_ENABLED:-false}" = "true" ]; then
        local j=0
        while [ "${j}" -lt "${seg_count}" ]; do
            local mirror_dbid=$(( 3 + seg_count + j ))
            local mirror_hostname="${cluster_prefix}-segment-mirror-${j}.${segment_svc}"
            local mirror_datadir="${PGDATA}/gpseg${j}"

            psql -U gpadmin -d postgres -c "
                SET allow_system_table_mods = true;
                INSERT INTO gp_segment_configuration
                    (dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
                VALUES
                    (${mirror_dbid}, ${j}, 'm', 'm', 's', 'u', ${port}, '${mirror_hostname}', '${mirror_hostname}', '${mirror_datadir}');
            " 2>/dev/null || log_warn "Failed to register mirror segment ${j}"

            j=$(( j + 1 ))
        done
    fi

    log_info "Segment registration complete"
}

# ---------------------------------------------------------------------------
# Set coordinator to dispatch mode
# ---------------------------------------------------------------------------
set_dispatch_mode() {
    local data_dir="$1"

    # In Cloudberry 2.1.0, the coordinator dispatch mode is determined by
    # gp_contentid = -1 in internal.auto.conf. The gp_role GUC cannot be set
    # in postgresql.conf directly. Remove any stale gp_role entries.
    if grep -q "^gp_role" "${data_dir}/postgresql.conf" 2>/dev/null; then
        log_info "Removing invalid gp_role from postgresql.conf (handled by internal.auto.conf)..."
        sed -i '/^gp_role/d' "${data_dir}/postgresql.conf"
    fi
    log_info "Coordinator dispatch mode set via internal.auto.conf (gp_contentid=-1)"
}

# ---------------------------------------------------------------------------
# Start PostgreSQL/Cloudberry
# ---------------------------------------------------------------------------
start_postgres() {
    local data_dir="$1"

    # Ensure data directory has correct permissions (required by PostgreSQL)
    chmod 700 "${data_dir}" 2>/dev/null || true

    # Apply any operator-provided configuration overrides
    apply_config_overrides "${data_dir}"

    log_info "Starting PostgreSQL (role=${CLOUDBERRY_ROLE}, content=${CLOUDBERRY_CONTENT_ID})..."
    log_info "Data directory: ${data_dir}"

    # gp_role is a backend-context GUC, so it must be passed via command line (-c).
    # Coordinator starts in dispatch mode; segments start in execute mode.
    case "${CLOUDBERRY_ROLE}" in
        coordinator)
            # Wait for own hostname to be resolvable via DNS before starting.
            # The FTS probe resolves all hostnames in gp_segment_configuration at
            # startup; if the coordinator's own hostname is not yet in DNS (race
            # condition with headless service endpoint propagation), FTS crashes
            # and blocks distributed transaction recovery indefinitely.
            if [ -n "${HOSTNAME:-}" ] && [ -n "${CLOUDBERRY_COORDINATOR_HOST:-}" ]; then
                local my_fqdn="${HOSTNAME}.${CLOUDBERRY_COORDINATOR_HOST}"
                local dns_retries=30
                log_info "Waiting for DNS resolution of ${my_fqdn}..."
                while ! getent hosts "${my_fqdn}" >/dev/null 2>&1; do
                    dns_retries=$((dns_retries - 1))
                    if [ "${dns_retries}" -le 0 ]; then
                        log_warn "DNS resolution timeout for ${my_fqdn}, proceeding anyway"
                        break
                    fi
                    sleep 1
                done
                if [ "${dns_retries}" -gt 0 ]; then
                    log_info "DNS resolved: ${my_fqdn}"
                fi
            fi
            log_info "Starting coordinator in dispatch mode"
            exec postgres -D "${data_dir}" -c gp_role=dispatch
            ;;
        primary|mirror)
            # Cloudberry 2.1 reads the segment content id from the GUC
            # `gp_contentid` in the datadir's internal.auto.conf — NOT from a
            # command-line flag. In standard PostgreSQL 14 (which Cloudberry 2.1
            # is based on) `postgres -C <name>` means "print the value of config
            # parameter <name> and exit", so passing the content id via that flag
            # (e.g. `-C 0`) FATALs with `unrecognized configuration parameter "0"`.
            # Normal first-boot segments get `gp_contentid = <id>` written by
            # init_segment; gpexpand-managed segments get it written/upserted into
            # internal.auto.conf during the handoff (gpexpand_handoff_to_steady_state),
            # correcting the -1 inherited from the coordinator template basebackup.
            # The start command therefore mirrors the normal path with no -C flag.
            log_info "Starting segment in execute mode (content=${CLOUDBERRY_CONTENT_ID})"
            exec postgres -D "${data_dir}" -c gp_role=execute
            ;;
        gpexpand-wait)
            # No-op placeholder role (never dispatched here): the gpexpand pre-init
            # idle path is handled in main() via wait_for_gpexpand_init, which keeps
            # sshd up until gpexpand initializes the datadir. Kept for clarity only.
            log_info "gpexpand-managed segment waiting for datadir init"
            ;;
        *)
            exec postgres -D "${data_dir}"
            ;;
    esac
}

# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------
main() {
    load_cloudberry_env
    ensure_gpbackup_symlinks

    log_info "Apache Cloudberry Database — Kubernetes Entrypoint"
    log_info "Role: ${CLOUDBERRY_ROLE}, Content ID: ${CLOUDBERRY_CONTENT_ID}"
    log_info "Data directory: ${PGDATA}"

    # Start SSH for inter-segment communication
    start_sshd

    # Ensure data directory exists and has correct ownership
    mkdir -p "${PGDATA}"

    case "${CLOUDBERRY_ROLE}" in
        coordinator)
            init_coordinator
            set_dispatch_mode "${COORDINATOR_DATA_DIR}"
            # Start in background to set password and register segments, then restart in foreground
            if [ -n "${POSTGRES_PASSWORD:-}" ] && [ ! -f "${COORDINATOR_DATA_DIR}/.password_set" ]; then
                postgres -D "${COORDINATOR_DATA_DIR}" &
                local bg_pid=$!
                set_admin_password "${COORDINATOR_DATA_DIR}"
                touch "${COORDINATOR_DATA_DIR}/.password_set"
                register_segments "${COORDINATOR_DATA_DIR}"
                kill "${bg_pid}" 2>/dev/null || true
                wait "${bg_pid}" 2>/dev/null || true
                sleep 2
            elif [ ! -f "${COORDINATOR_DATA_DIR}/.segments_registered" ]; then
                # Password already set but segments not yet registered
                postgres -D "${COORDINATOR_DATA_DIR}" &
                local bg_pid=$!
                register_segments "${COORDINATOR_DATA_DIR}"
                touch "${COORDINATOR_DATA_DIR}/.segments_registered"
                kill "${bg_pid}" 2>/dev/null || true
                wait "${bg_pid}" 2>/dev/null || true
                sleep 2
            fi
            start_postgres "${COORDINATOR_DATA_DIR}"
            ;;
        standby)
            init_standby
            start_postgres "${COORDINATOR_DATA_DIR}"
            ;;
        primary)
            init_segment
            # gpexpand-managed scale-out segment: gpexpand owns BOTH the datadir
            # init AND the postgres start (pg_ctl) on this pod. The entrypoint must
            # NOT race gpexpand by also starting postgres on the same datadir/port
            # (that collision is the "could not start server" pg_ctl failure).
            #
            # Flow for a gpexpand-managed segment:
            #   a. EMPTY datadir  -> idle (sshd up, port 5432 free) until gpexpand
            #      initializes it (wait_for_gpexpand_init).
            #   b. datadir ready  -> hand off: stop gpexpand's pg_ctl-started
            #      instance, fix ownership, then fall through to exec postgres as
            #      the container main process (gpexpand_handoff_to_steady_state).
            #   c. SKIP the background password-setup start below: it would race
            #      gpexpand, and the password/config already come from the
            #      coordinator template gpexpand rsync'd in.
            if is_gpexpand_managed_segment; then
                # Install the pg_ctl shim BEFORE gpexpand runs its segment-init:
                # gpexpand's ssh'd `pg_ctl start` must find the CORRECT
                # gp_contentid in the datadir's internal.auto.conf (Cloudberry
                # 2.1 reads the content id from that GUC; the basebackup'd datadir
                # inherits the coordinator template's -1). The shim replaces
                # ${GPHOME}/bin/pg_ctl (real -> pg_ctl.real) so it wins regardless
                # of PATH order, and upserts gp_contentid right before each start.
                install_gpexpand_pgctl_shim || \
                    log_warn "pg_ctl shim not installed; gpexpand -i start may use wrong gp_contentid"
                if [ ! -f "${SEGMENT_DATA_DIR}/PG_VERSION" ]; then
                    if ! wait_for_gpexpand_init "${SEGMENT_DATA_DIR}"; then
                        exit 1
                    fi
                fi
                gpexpand_handoff_to_steady_state "${SEGMENT_DATA_DIR}"
                start_postgres "${SEGMENT_DATA_DIR}"
                # start_postgres exec's postgres; not reached.
                exit 0
            fi
            if [ -n "${POSTGRES_PASSWORD:-}" ] && [ ! -f "${SEGMENT_DATA_DIR}/.password_set" ]; then
                postgres -D "${SEGMENT_DATA_DIR}" &
                local bg_pid=$!
                set_admin_password "${SEGMENT_DATA_DIR}"
                touch "${SEGMENT_DATA_DIR}/.password_set"
                kill "${bg_pid}" 2>/dev/null || true
                wait "${bg_pid}" 2>/dev/null || true
                sleep 2
            fi
            start_postgres "${SEGMENT_DATA_DIR}"
            ;;
        mirror)
            # gpexpand initializes the new mirror's datadir (basebackup from the
            # new primary) AND starts postgres on it via pg_ctl. Until then, keep
            # the pod Running (sshd up, port 5432 free) rather than exec'ing
            # postgres on an empty dir. After gpexpand initializes it, hand off
            # (stop gpexpand's pg_ctl instance + fix ownership) before the
            # entrypoint exec's postgres as the container main process.
            if is_gpexpand_managed_segment; then
                # Same rationale as the primary path: gpexpand's ssh'd pg_ctl
                # start of the new mirror needs the correct gp_contentid in the
                # datadir's internal.auto.conf (Cloudberry 2.1 reads it from that
                # GUC; the basebackup'd datadir inherits the coordinator's -1).
                # The shim replaces ${GPHOME}/bin/pg_ctl so it wins over PATH.
                install_gpexpand_pgctl_shim || \
                    log_warn "pg_ctl shim not installed; gpexpand -i start may use wrong gp_contentid"
                if [ ! -f "${SEGMENT_DATA_DIR}/PG_VERSION" ]; then
                    if ! wait_for_gpexpand_init "${SEGMENT_DATA_DIR}"; then
                        exit 1
                    fi
                fi
                gpexpand_handoff_to_steady_state "${SEGMENT_DATA_DIR}"
            else
                init_mirror
            fi
            start_postgres "${SEGMENT_DATA_DIR}"
            ;;
        *)
            log_error "Unknown role: ${CLOUDBERRY_ROLE}"
            log_error "Valid roles: coordinator, standby, primary, mirror"
            exit 1
            ;;
    esac
}

# If the first argument is "postgres" or starts with "-", run the entrypoint
# Otherwise, execute the command directly (e.g., for debugging)
if [ "${1:-}" = "postgres" ] || [ "${1:0:1}" = "-" ] || [ -z "${1:-}" ]; then
    main "$@"
else
    exec "$@"
fi
