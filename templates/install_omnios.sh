#!/usr/bin/bash

# ==============================================================================
# OmniOS/Illumos Project Environment Setup Script
# Includes: Python 3.11, FastAPI, Sanoid, and SMF Service Generation
# ==============================================================================

PROJECT_NAME="my_python_app"
PROJECT_ROOT=$(pwd)
VENV_PATH="$PROJECT_ROOT/.venv"
APP_PORT=8000

# Ensure we are running as root
if [ "$EUID" -ne 0 ]; then 
  echo "Please run as root or with pfexec"
  exit 1
fi

echo "--- 1. Installing System Dependencies ---"
pkg refresh
pkg install \
    runtime/python-311 \
    library/python/pip-311 \
    runtime/nodejs \
    system/storage/smartmontools \
    developer/lang/rustc \
    developer/gcc-11 \
    developer/build/make \
    library/security/openssl \
    library/security/libsodium \
    runtime/perl-534 \
    developer/versioning/git

echo "--- 2. Automating Sanoid Installation ---"
cd /opt
if [ ! -d "sanoid" ]; then
    git clone https://github.com/jimsalterjrs/sanoid.git
    cd sanoid
    ln -s $(pwd)/sanoid /usr/local/bin/sanoid
    ln -s $(pwd)/syncoid /usr/local/bin/syncoid
    mkdir -p /etc/sanoid
    cp sanoid.conf /etc/sanoid/sanoid.conf
    echo "Sanoid installed to /usr/local/bin"
else
    echo "Sanoid already exists in /opt/sanoid, skipping clone."
fi
cd $PROJECT_ROOT

echo "--- 3. Setting up Python Virtual Environment ---"
python3.11 -m venv $VENV_PATH
source $VENV_PATH/bin/activate
export OPENSSL_DIR=/usr/openssl/3
pip install --upgrade pip setuptools wheel

echo "--- 4. Installing Python Requirements ---"
# This will compile Rust/C extensions for Illumos
pip install fastapi uvicorn gunicorn pydantic-core cryptography psutil python-pam paramiko

echo "--- 5. Generating SMF Manifest (Illumos Service File) ---"
# This replaces the need for a systemd .service file
cat <<EOF > ${PROJECT_NAME}_manifest.xml
<?xml version="1.0"?>
<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="manifest" name="${PROJECT_NAME}">
    <service name="site/${PROJECT_NAME}" type="service" version="1">
        <create_default_instance enabled="false" />
        <single_instance />
        
        <dependency name="network" grouping="require_all" restart_on="error" type="service">
            <service_fmri value="svc:/milestone/network:default" />
        </dependency>

        <exec_method type="method" name="start" 
            exec="${VENV_PATH}/bin/gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:${APP_PORT}" 
            timeout_seconds="60">
            <method_context working_directory="${PROJECT_ROOT}">
                <method_credential user="root" group="root" />
            </method_context>
        </exec_method>

        <exec_method type="method" name="stop" exec=":kill" timeout_seconds="60" />
        
        <stability value="Unstable" />
        <template>
            <common_name>
                <loctext xml:lang="C">FastAPI Application: ${PROJECT_NAME}</loctext>
            </common_name>
        </template>
    </service>
</service_bundle>
EOF

echo "--- 6. Registering Service with SMF ---"
svccfg import ${PROJECT_NAME}_manifest.xml
echo "Service registered as: svc:/site/${PROJECT_NAME}:default"

echo "--------------------------------------------------------"
echo "Setup Complete!"
echo "To start your app:  svcadm enable site/${PROJECT_NAME}"
echo "To check status:    svcs -xv site/${PROJECT_NAME}"
echo "To view logs:       tail -f /var/svc/log/site-${PROJECT_NAME}:default.log"
echo "--------------------------------------------------------"