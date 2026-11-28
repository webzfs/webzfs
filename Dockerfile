# syntax=docker/dockerfile:1
#
# WebZFS container image.
#
# The container ships the web application plus every userland tool it
# drives (zfs/zpool/zdb, smartctl, sanoid/syncoid, lsblk/blkid, lsof...).
# The ZFS *kernel module* must still come from the host: run the
# container privileged with /dev mounted (see docker-compose.yml and
# DOCKER.md). Keep the image's OpenZFS userland close to the host
# module version; override BASE_IMAGE if they drift apart.

ARG BASE_IMAGE=debian:trixie-slim
ARG NODE_IMAGE=node:22-trixie-slim

############################
# Stage 1: CSS / JS assets #
############################
FROM ${NODE_IMAGE} AS assets
WORKDIR /build
COPY package.json package-lock.json postcss.config.js tailwind.config.js ./
RUN npm ci --no-audit --no-fund
COPY src ./src
COPY templates ./templates
COPY static ./static
RUN npm run build:css

################################
# Stage 2: Python virtualenv   #
################################
FROM ${BASE_IMAGE} AS python-build
ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 python3-venv python3-dev build-essential pkg-config \
      libsodium-dev libffi-dev libssl-dev libpam0g-dev \
 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
# The venv lives at its final path so its shebangs stay valid when copied.
RUN python3 -m venv /opt/webzfs/.venv \
 && SODIUM_INSTALL=system /opt/webzfs/.venv/bin/pip install -r /tmp/requirements.txt

################################
# Stage 3: Runtime             #
################################
FROM ${BASE_IMAGE} AS runtime
ENV DEBIAN_FRONTEND=noninteractive

# zfsutils-linux lives in Debian's "contrib" component.
RUN if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
      sed -i 's/^Components: main$/Components: main contrib/' /etc/apt/sources.list.d/debian.sources; \
    fi \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 \
      zfsutils-linux \
      smartmontools \
      sanoid mbuffer pv lzop zstd openssh-client \
      util-linux lsof procps kmod udev \
      libpam-modules libpam-runtime libsodium23 \
      sudo rsync ca-certificates tini bash \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/webzfs

COPY --from=python-build /opt/webzfs/.venv /opt/webzfs/.venv
COPY . /opt/webzfs
COPY --from=assets /build/static/css/styles.css /opt/webzfs/static/css/styles.css
COPY --from=assets /build/static/vendor /opt/webzfs/static/vendor
COPY docker/entrypoint.sh /usr/local/bin/webzfs-entrypoint

RUN chmod 0755 /usr/local/bin/webzfs-entrypoint \
 && rm -f /opt/webzfs/install*.sh /opt/webzfs/update*.sh \
 && mkdir -p /opt/webzfs/.config/webzfs

# The app runs as root inside the container, so it calls zfs/zpool/etc.
# directly instead of through sudo (see services/utils.py).
# Real environment variables override values from .env.
ENV HOME=/opt/webzfs \
    PATH=/opt/webzfs/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    PYTHONPATH=/opt/webzfs \
    PYTHONUNBUFFERED=1 \
    BIND_IP=0.0.0.0 \
    PORT=26619

VOLUME ["/opt/webzfs/.config/webzfs"]
EXPOSE 26619

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python3 -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/' % os.environ.get('PORT','26619'), timeout=4)" || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/webzfs-entrypoint"]
CMD ["gunicorn", "-c", "config/gunicorn.conf.py"]
