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
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
# GPHOME is intentionally not readonly — cloudberry-env.sh reassigns it.
GPHOME="${GPHOME:-/usr/local/cloudberry-db}"
export GPHOME

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
# ---------------------------------------------------------------------------
write_ssh_client_config() {
    cat > /home/gpadmin/.ssh/config <<'EOF'
Host *
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

    sudo /usr/sbin/sshd 2>/dev/null || log_warn "SSH daemon failed to start (non-fatal)"
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
# Initialize segment data directory
# ---------------------------------------------------------------------------
init_segment() {
    local data_dir="${SEGMENT_DATA_DIR}"
    local role="${CLOUDBERRY_ROLE}"

    if [ -f "${data_dir}/PG_VERSION" ]; then
        log_info "Segment data directory already initialized at ${data_dir}"
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
            log_info "Starting segment in execute mode"
            exec postgres -D "${data_dir}" -c gp_role=execute
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
            init_mirror
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
