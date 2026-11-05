#!/bin/sh
# PROVIDE: webzfs
# REQUIRE: LOGIN
# KEYWORD: shutdown

. /etc/rc.subr

name="webzfs"
rcvar="webzfs_enable"

load_rc_config "$name"

: ${webzfs_enable:="NO"}
: ${webzfs_user:="zfsadmin"}
: ${webzfs_chdir:="/usr/local/www/webzfs"}
: ${webzfs_venv:="${webzfs_chdir}/.venv"}
: ${webzfs_settings_module:="config.settings.dev"}
: ${webzfs_gunicorn_conf:="${webzfs_chdir}/config/gunicorn.conf.py"}
: ${webzfs_pidfile:="/var/run/${name}.pid"}

command="/usr/sbin/daemon"

# IMPORTANT:
# - REMOVED -u, since rc.subr performs su automatically via webzfs_user
# - Added -S for logging to syslog (instead of stdout)
# - Added -- to separate daemon options from the command

command_args="-S -p ${webzfs_pidfile} -- ${webzfs_venv}/bin/gunicorn -c ${webzfs_gunicorn_conf}"

pidfile_child="${webzfs_pidfile}"
procname="${webzfs_venv}/bin/python"
chdir="${webzfs_chdir}"

# Environment Variables

webzfs_env="HOME=${webzfs_chdir} PYTHONPATH=${webzfs_chdir} SETTINGS_MODULE=${webzfs_settings_module}"

# Cleaning the stale PID file

start_precmd="webzfs_precmd"
webzfs_precmd() {
    if [ -f "${pidfile}" ]; then
        if ! kill -0 $(cat "${pidfile}") 2>/dev/null; then
            rm -f "${pidfile}"
        fi
    fi
}

run_rc_command "$1"
