# Running WebZFS in Docker

The image bundles WebZFS and every userland tool it uses: Python venv,
built Tailwind/xterm assets, OpenZFS utilities (`zfs`, `zpool`, `zdb`),
smartmontools, sanoid/syncoid (plus mbuffer, pv, lzop, zstd, ssh),
util-linux, lsof and PAM.

The **ZFS kernel module is not in the image.** It must be loaded on the
host (`modprobe zfs`), and the container runs privileged with `/dev`
mounted so it can reach `/dev/zfs` and the disks.

## Quick start

```bash
echo 'WEBZFS_ADMIN_PASSWORD=change-me' > .env.compose
docker compose --env-file .env.compose up -d --build
# open http://localhost:26619 and log in as "admin"
```

Without compose:

```bash
docker build -t webzfs .
docker run -d --name webzfs --privileged --pid host \
  -e WEBZFS_ADMIN_USER=admin -e WEBZFS_ADMIN_PASSWORD=change-me \
  -p 127.0.0.1:26619:26619 \
  -v webzfs-data:/opt/webzfs/.config/webzfs \
  -v /dev:/dev -v /run/udev:/run/udev:ro \
  -v /etc/zfs:/etc/zfs -v /etc/sanoid:/etc/sanoid \
  webzfs
```

## Configuration

| Variable | Purpose |
|---|---|
| `WEBZFS_ADMIN_USER` | Login account created in the container at startup (PAM auth). |
| `WEBZFS_ADMIN_PASSWORD` / `WEBZFS_ADMIN_PASSWORD_FILE` | Its password; the `_FILE` form works with Docker secrets. |
| `BIND_IP`, `PORT` | Listener inside the container (default `0.0.0.0:26619`). |
| anything in `.env.example` | Can be passed as an environment variable and overrides the file. |

On first start, a `.env` with a freshly generated `SECRET_KEY` is written
to the data volume (`/opt/webzfs/.config/webzfs/webzfs.env`). That volume
also holds the app's JSON state and audit logs.

The port is published on `127.0.0.1` only by default. Change the
`ports:` entry to `"26619:26619"` to reach it from your LAN, preferably
behind a TLS reverse proxy (set `FORWARDED_ALLOW_IPS` to the proxy's
address).

## Limitations in a container

- **Version match:** the image ships OpenZFS 2.3.x (Debian trixie). Keep
  the host kernel module on a compatible release. For a different
  version, build with `--build-arg BASE_IMAGE=...` (Debian-based).
- **Scheduled tasks** (scheduled scrubs, SMART tests, syncoid jobs,
  health reports) are registered as host systemd timers on Linux. A
  container has no systemd, so scheduling from the UI does not work.
  Schedule those with host cron/systemd, or run sanoid on the host.
- **Systemd service pages** and `journalctl` views show the container,
  not the host. Mount `/var/log:/var/log:ro` for file-based host logs.
- **Mounted datasets:** `zpool import` / `zfs mount` run inside the
  container's mount namespace. The compose files bind `/mnt:/mnt:rshared`
  so those mounts appear on the host (and host mounts appear in the
  container). Add an `rshared` line for every mountpoint outside `/mnt`
  (for example `- /tank:/tank:rshared`; a pool named `tank` mounts at
  `/tank` by default). If Docker reports that a path "is not a shared
  mount", run `sudo mount --make-rshared /` on the host.
  Debug: `zfs get mountpoint,mounted <dataset>` inside the container,
  `findmnt -t zfs` on the host.
- **Users:** login accounts belong to the container, not the host. Only
  the account from `WEBZFS_ADMIN_USER` exists unless you add more.
- **Security:** a privileged container is effectively root on the host,
  just like WebZFS's sudo policy on a normal install. Don't expose it
  to untrusted networks.
