{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.webzfs;
  webzfsDir = "${cfg.package}/opt/webzfs";
in
{
  options.services.webzfs = {
    enable = lib.mkEnableOption "WebZFS - Web-based ZFS management interface";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage ./package.nix { };
      defaultText = lib.literalExpression "pkgs.webzfs";
      description = ''
        The WebZFS package to use.
      '';
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 26619;
      description = "Port to listen on.";
    };

    host = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1";
      description = "Host address to bind to.";
    };

    settings = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      example = {
        SECRET_KEY = "your-secret-key-here";
        AUTH_SESSION_EXPIRES_SECONDS = "3600";
      };
      description = "Additional environment variables for WebZFS.";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Whether to open the firewall for the WebZFS port.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "webzfs";
      description = "User account to run WebZFS as.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "webzfs";
      description = "Group for the WebZFS user.";
    };
  };

  config = lib.mkIf cfg.enable {

    # Enable ZFS filesystem support
    boot.supportedFilesystems = [ "zfs" ];

    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.group;
      description = "WebZFS service user";
      extraGroups = [
        # Add to shadow group for PAM authentication
        "shadow"
      ];
    };

    users.groups.${cfg.group} = { };

    systemd.services.webzfs = {
      wantedBy = [ "multi-user.target" ];
      after = [
        "network.target"
        "zfs-mount.service"
      ];

      environment = {
        HOME = "/var/lib/webzfs";
        PYTHONPATH = webzfsDir;
        HOST = cfg.host;
        PORT = toString cfg.port;
        BIND_IP = cfg.host;
        CAPTION = "webzfs ${cfg.package.version or "git"}";
        SETTINGS_MODULE = "config.settings.base";
        SECRET_KEY = cfg.settings.SECRET_KEY or "changeme-in-production";
        WEBZFS_STATE_DIR = "/var/lib/webzfs";
      }
      // cfg.settings;

      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        StateDirectory = "webzfs";
        StateDirectoryMode = "0750";
        WorkingDirectory = webzfsDir;
        Restart = "always";
        RestartSec = "5";
      };

      script = ''
        exec ${cfg.package}/bin/webzfs
      '';
    };

    # NOPASSWD config as per sudoers file config in install_linux.sh
    security.sudo = {
      enable = true;
      extraRules = [
        {
          users = [ cfg.user ];
          commands =
            map
              (cmd: {
                command = cmd;
                options = [ "NOPASSWD" ];
              })
              [
                # ZFS commands
                (lib.getExe' pkgs.zfs "zpool")
                (lib.getExe pkgs.zfs)
                "${lib.getExe' pkgs.zfs "zdb"} -l *"

                # SMART monitoring
                (lib.getExe pkgs.smartmontools)

                # Disk utilities
                (lib.getExe' pkgs.util-linux "lsblk")
                (lib.getExe' pkgs.util-linux "blkid")

                # Open file / lock inspection (pool export busy investigation)
                (lib.getExe pkgs.lsof)
                (lib.getExe' pkgs.util-linux "lslocks")

                # Sanoid/Syncoid
                (lib.getExe' pkgs.sanoid "sanoid")
                (lib.getExe' pkgs.sanoid "syncoid")

                # Service management (systemctl for system services page)
                (lib.getExe' pkgs.systemd "systemctl")

                # Crontab editing
                (lib.getExe' pkgs.cron "crontab")

                # Scheduled syncoid job timers.
                # Unit files are created and edited with "sudo tee" (covered by the
                # general tee entry below) and enabled/disabled/reloaded with
                # "sudo systemctl" (covered by the systemctl entry above). The explicit
                # tee entries here document that intent and keep timer management
                # working even if the general tee entry is ever narrowed. rm is
                # restricted to WebZFS-owned unit files only.
                "${lib.getExe' pkgs.coreutils "tee"} /etc/systemd/system/webzfs-syncoid-job-*"
                "${lib.getExe' pkgs.coreutils "rm"} -f /etc/systemd/system/webzfs-syncoid-job-*"

                # Unified Scheduling Hub timers. All scheduled task types (scrub, SMART
                # self-test, health check, and replication) use the webzfs-task-* unit
                # naming scheme managed by services/job_scheduler.py.
                "${lib.getExe' pkgs.coreutils "tee"} /etc/systemd/system/webzfs-task-*"
                "${lib.getExe' pkgs.coreutils "rm"} -f /etc/systemd/system/webzfs-task-*"

                # File editing (for config files like smartd.conf, sanoid.conf)
                (lib.getExe' pkgs.coreutils "cat")
                (lib.getExe' pkgs.coreutils "tee")
                (lib.getExe' pkgs.coreutils "mkdir")

                # Read system journal and plain-text syslog files for the
                # Observability -> System Log page. journalctl needs sudo (or
                # systemd-journal group) on most distros. tail covers Debian/Ubuntu
                # (/var/log/syslog) and old RHEL (/var/log/messages).
                (lib.getExe' pkgs.systemd "journalctl")
                (lib.getExe' pkgs.coreutils "tail")

                # Support bundle log collection. Reading /var/log/messages and
                # /var/log/syslog (typically mode 640 root:adm) and the kernel ring
                # buffer requires elevated privileges for the unprivileged webzfs user.
                (lib.getExe pkgs.gnugrep)
                (lib.getExe' pkgs.util-linux "dmesg")
              ];
        }
      ];
    };

    networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ cfg.port ];
  };
}
