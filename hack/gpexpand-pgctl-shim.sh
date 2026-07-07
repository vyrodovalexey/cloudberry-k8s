#!/usr/bin/env bash
# cloudberry-k8s gpexpand pg_ctl shim
# =============================================================================
# BAKED (build-time) pg_ctl shim for the Apache Cloudberry Kubernetes image.
#
# WHY THIS EXISTS
# ---------------
# In Cloudberry 2.1 the segment content id is carried by the GUC `gp_contentid`
# in the datadir's internal.auto.conf (read at postmaster start) — it is NOT a
# `postgres` print-parameter flag (that is standard PostgreSQL "print a config
# parameter and exit"). gpexpand's OWN segment-init phase basebackups the new
# datadir from the COORDINATOR template (which carries `gp_contentid = -1`) and
# then starts it over SSH with a hardcoded
#     pg_ctl -D <datadir> -o " -p 5432 -c gp_role=utility -M " start
# Because the basebackup'd datadir still says `gp_contentid = -1`, that start
# comes up with the WRONG content id and postgres FATALs "contentid not
# specified". We cannot change gpexpand's baked command, so this pg_ctl wrapper
# UPSERTS the correct `gp_contentid = <id>` into the target `-D <datadir>`'s
# internal.auto.conf right before each `start`/`restart`. It NEVER injects `-C`.
#
# WHY BAKED AT BUILD TIME (not installed at runtime)
# --------------------------------------------------
# At RUNTIME the pod runs as gpadmin (UID 1000) under the restricted-v2 SCC and
# ${GPHOME}/bin is root-owned, so the entrypoint cannot rename pg_ctl there
# ("permission denied"). The shim is therefore installed at BUILD time as root:
# the real binary is renamed to pg_ctl.real and this script is dropped at
# ${GPHOME}/bin/pg_ctl (chmod 0755). Being root-owned is fine — it is only
# executed. It wins over PATH regardless of ordering (gpexpand's ssh sessions
# source cloudberry-env.sh which prepends ${GPHOME}/bin FIRST).
#
# WHY CONTENT ID IS DERIVED FROM THE DATADIR (self-contained, no runtime env)
# ---------------------------------------------------------------------------
# The SAME baked shim binary is in every segment image, but each pod is a
# DIFFERENT content id. Rather than depend on a per-pod env var surviving into
# gpexpand's non-interactive ssh session, the shim DERIVES the content id from
# the `-D <datadir>` path itself: the datadir suffix is always `gpseg<N>`
# (e.g. /data/pgdata/gpseg2 -> content 2; gpseg-1 -> coordinator -1). This is
# fully self-contained — no CBK_CONTENT_ID env and no entrypoint cooperation
# required. An env override (CBK_CONTENT_ID) is honored if present as a
# belt-and-braces fallback, but the datadir derivation is authoritative.
#
# For a normal/coordinator segment whose datadir already carries the correct
# gp_contentid, the upsert is a harmless idempotent no-op.
# =============================================================================
set -euo pipefail

# The real pg_ctl binary lives next to this shim with a .real suffix (renamed at
# build time). Resolve it relative to THIS script so the shim is location-safe.
CBK_SHIM_SELF="${BASH_SOURCE[0]}"
CBK_REAL_PGCTL="${CBK_SHIM_SELF}.real"

# ---------------------------------------------------------------------------
# cbk_derive_content_id_from_datadir <datadir>
# Extract the trailing integer N from a `gpseg<N>` datadir suffix and echo it as
# the content id. gpseg-1 (coordinator) => -1; gpseg2 => 2. Echoes nothing when
# the path does not match the gpseg<N> pattern (caller then skips the upsert).
# ---------------------------------------------------------------------------
cbk_derive_content_id_from_datadir() {
    local dir="$1"
    local base
    base="$(basename "${dir}")"
    # Match gpseg<optional-minus><digits> and capture the signed integer.
    if [[ "${base}" =~ ^gpseg(-?[0-9]+)$ ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
    fi
}

# ---------------------------------------------------------------------------
# cbk_upsert_gp_contentid <datadir> <content_id>
# Replace any existing gp_contentid line (e.g. the coordinator template's -1)
# in <datadir>/internal.auto.conf, then append the correct value. grep -v + mv
# is used (not sed -i) for GNU/BSD portability. Upserting an already-correct
# value is a harmless idempotent no-op.
# ---------------------------------------------------------------------------
cbk_upsert_gp_contentid() {
    local data_dir="$1"
    local content_id="$2"
    local conf="${data_dir}/internal.auto.conf"

    # Guard: only a valid (>= -1) integer content id may be written.
    if ! [[ "${content_id}" =~ ^-?[0-9]+$ ]]; then
        return 0
    fi
    if [ -f "${conf}" ]; then
        local tmp="${conf}.cbk.$$"
        grep -v -E '^[[:space:]]*gp_contentid[[:space:]]*=' "${conf}" > "${tmp}" 2>/dev/null || true
        mv "${tmp}" "${conf}" 2>/dev/null || rm -f "${tmp}" 2>/dev/null || true
    fi
    echo "gp_contentid = ${content_id}" >> "${conf}"
}

# Detect a start/restart subcommand (only those need the gp_contentid upsert).
cbk_cmd=""
for cbk_a in "$@"; do
    case "${cbk_a}" in
        start|restart) cbk_cmd="${cbk_a}"; break;;
    esac
done

if [ "${cbk_cmd}" = "start" ] || [ "${cbk_cmd}" = "restart" ]; then
    # Extract the -D <datadir> argument (supports both `-D dir` and `-Ddir`).
    cbk_data_dir=""
    cbk_argv=("$@")
    cbk_n="${#cbk_argv[@]}"
    cbk_i=0
    while [ "${cbk_i}" -lt "${cbk_n}" ]; do
        if [ "${cbk_argv[${cbk_i}]}" = "-D" ] && [ $((cbk_i + 1)) -lt "${cbk_n}" ]; then
            cbk_data_dir="${cbk_argv[$((cbk_i + 1))]}"
            break
        fi
        case "${cbk_argv[${cbk_i}]}" in
            -D*) cbk_data_dir="${cbk_argv[${cbk_i}]#-D}";;
        esac
        cbk_i=$((cbk_i + 1))
    done

    if [ -n "${cbk_data_dir}" ] && [ -d "${cbk_data_dir}" ]; then
        # Content id is DERIVED from the datadir's gpseg<N> suffix (self-
        # contained). An explicit CBK_CONTENT_ID env is honored as a fallback
        # only when the datadir does not match the gpseg<N> pattern.
        cbk_content_id="$(cbk_derive_content_id_from_datadir "${cbk_data_dir}")"
        if [ -z "${cbk_content_id}" ]; then
            cbk_content_id="${CBK_CONTENT_ID:-}"
        fi
        if [ -n "${cbk_content_id}" ]; then
            cbk_upsert_gp_contentid "${cbk_data_dir}" "${cbk_content_id}"
        fi
    fi
fi

# Hand off to the real pg_ctl UNCHANGED — the shim NEVER injects -C.
exec "${CBK_REAL_PGCTL}" "$@"
