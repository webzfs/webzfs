# Python image based on Debian
FROM python:3.14-slim-trixie

# Define working directory
WORKDIR /webzfs

# Enable contrib repositories for ZFS and install packages
RUN sed -i 's/main/main contrib non-free-firmware/' /etc/apt/sources.list.d/debian.sources && \
  apt-get update && apt-get install -y --no-install-recommends \
  bash \
  zfsutils-linux \
  smartmontools \
  git \
  nodejs \
  npm \
  libsodium-dev \
  build-essential && \
  rm -rf /var/lib/apt/lists/*

# Clone repository
RUN git clone https://github.com/webzfs/webzfs .

# Install and build NPM
RUN npm install && mkdir -p static/css && npm run build:css

# Install Python requirements
RUN pip install --no-cache-dir -r requirements.txt

# Create directories, write config, copy .env, write host interface and create secret
RUN mkdir -p .config/webzfs/progress .config/webzfs/logs .config/webzfs/generated/systemd && \
    echo '{"executions": [], "next_id": 1}' > .config/webzfs/replication_history.json && \
    echo '{"notifications": []}' > .config/webzfs/notification_log.json && \
    echo '{"jobs": [], "next_id": 1}' > .config/webzfs/syncoid_jobs.json && \
    echo '{"schedules": [], "next_id": 1}' > .config/webzfs/scrub_schedules.json && \
    echo '{"history": []}' > .config/webzfs/smart_test_history.json && \
    echo '{}' > .config/webzfs/smart_scheduled_tests.json && \
    echo '{"reports": []}' > .config/webzfs/health_reports.json && \
    echo '{"schedules": [], "next_id": 1}' > .config/webzfs/health_schedules.json && \
    cp .env.example .env && \
    sed -i "s/127.0.0.1/0.0.0.0/" .env && \
    sed -i "s/CHANGE_ME_GENERATE_NEW_KEY/$(python3 -c 'import secrets; print(secrets.token_hex(32))')/" .env

# Expose port
EXPOSE 26619

# Set root password from compose variable and run gunicorn
CMD ["sh", "-c", "echo \"root:${ADMIN_PASSWORD:-webzfsadmin}\" | chpasswd && exec gunicorn -c config/gunicorn.conf.py"]
