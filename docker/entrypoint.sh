#!/bin/bash
# WebZFS container entrypoint: prepares persistent state, the login
# account, and then execs the server (gunicorn by default).
set -euo pipefail

APP_DIR=/opt/webzfs
DATA_DIR="${APP_DIR}/.config/webzfs"
ENV_FILE="${DATA_DIR}/webzfs.env"

mkdir -p "${DATA_DIR}/progress" "${DATA_DIR}/logs" "${DATA_DIR}/generated/systemd"

# Persistent .env kept in the data volume, linked to where the app reads it.
if [ ! -f "${ENV_FILE}" ]; then
    echo "[webzfs] Creating ${ENV_FILE} from .env.example"
    cp "${APP_DIR}/.env.example" "${ENV_FILE}"
    NEW_SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    sed -i "s/CHANGE_ME_GENERATE_NEW_KEY/${NEW_SECRET_KEY}/" "${ENV_FILE}"
fi
ln -sfn "${ENV_FILE}" "${APP_DIR}/.env"

# Pre-create JSON data files (same defaults as install_linux.sh).
init_json() { [ -f "${DATA_DIR}/$1" ] || echo "$2" > "${DATA_DIR}/$1"; }
init_json replication_history.json   '{"executions": [], "next_id": 1}'
init_json notification_log.json      '{"notifications": []}'
init_json syncoid_jobs.json          '{"jobs": [], "next_id": 1}'
init_json scrub_schedules.json       '{"schedules": [], "next_id": 1}'
init_json smart_test_history.json    '{"history": []}'
init_json smart_scheduled_tests.json '{}'
init_json health_reports.json        '{"reports": []}'
init_json health_schedules.json      '{"schedules": [], "next_id": 1}'

# WebZFS authenticates through PAM against this container's /etc/shadow.
# Create (or refresh) a login account from the environment on every start.
ADMIN_USER="${WEBZFS_ADMIN_USER:-}"
ADMIN_PASSWORD="${WEBZFS_ADMIN_PASSWORD:-}"
if [ -n "${WEBZFS_ADMIN_PASSWORD_FILE:-}" ] && [ -f "${WEBZFS_ADMIN_PASSWORD_FILE}" ]; then
    ADMIN_PASSWORD="$(head -n1 "${WEBZFS_ADMIN_PASSWORD_FILE}")"
fi
if [ -n "${ADMIN_USER}" ]; then
    if [ -z "${ADMIN_PASSWORD}" ]; then
        echo "[webzfs] ERROR: WEBZFS_ADMIN_USER is set but no password was given" >&2
        echo "[webzfs]        (set WEBZFS_ADMIN_PASSWORD or WEBZFS_ADMIN_PASSWORD_FILE)" >&2
        exit 1
    fi
    if ! id "${ADMIN_USER}" >/dev/null 2>&1; then
        useradd -m -s /bin/bash "${ADMIN_USER}"
        echo "[webzfs] Created login user '${ADMIN_USER}'"
    fi
    echo "${ADMIN_USER}:${ADMIN_PASSWORD}" | chpasswd
else
    echo "[webzfs] WARNING: WEBZFS_ADMIN_USER not set; no account exists to log in with." >&2
fi
unset ADMIN_PASSWORD WEBZFS_ADMIN_PASSWORD

if [ ! -e /dev/zfs ]; then
    echo "[webzfs] WARNING: /dev/zfs not found. Load the zfs kernel module on the host" >&2
    echo "[webzfs]          and run the container privileged with /dev mounted." >&2
fi

cd "${APP_DIR}"
exec "$@"
