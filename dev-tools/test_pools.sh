#!/bin/bash
set -euo pipefail

# =============================================================================
# WebZFS Test Pool Creation Script
#
# Creates 9 test zpools using RAM-backed SCSI devices via scsi_debug.
# All devices are fully read/write (no fake_rw).
#
# Disk allocation (219 total, all 1GB):
#   DATA[0-99]   (100 disks) : Pool data disks
#   AUX[0-23]     (24 disks) : Auxiliary vdev disks (special, log, cache, dedup)
#   DRAID[0-94]   (95 disks) : dRAID pool disks
#
# RAM usage: ~219GB
# =============================================================================

TOTAL_DISKS=219
DISK_SIZE_MB=1024

# -----------------------------------------------------------------------------
# Step 1: Record existing block devices before creating any test disks
# -----------------------------------------------------------------------------
echo "=== WebZFS Test Pool Setup ==="
echo ""
echo "Recording existing block devices..."
EXISTING_DISKS=$(lsblk -dno NAME,TYPE 2>/dev/null | awk '$2=="disk"{print $1}' | sort)
EXISTING_COUNT=$(echo "${EXISTING_DISKS}" | grep -c . || true)
echo "Found ${EXISTING_COUNT} existing disk(s):"
if [ -n "${EXISTING_DISKS}" ]; then
  echo "${EXISTING_DISKS}" | sed 's/^/  /'
fi
echo ""

# -----------------------------------------------------------------------------
# Step 2: Verify scsi_debug is not already loaded
# -----------------------------------------------------------------------------
if lsmod | grep -q scsi_debug; then
  echo "ERROR: scsi_debug module is already loaded."
  echo "To clean up, run:"
  echo "  sudo zpool destroy test-z1 test-z2 test-z3 test-m2 test-m3 test-aux test-d1 test-d2 test-d3 2>/dev/null"
  echo "  sudo rmmod scsi_debug"
  exit 1
fi

# -----------------------------------------------------------------------------
# Step 3: Load scsi_debug module with RAM-backed devices
# -----------------------------------------------------------------------------
RAM_NEEDED_GB=$(( TOTAL_DISKS * DISK_SIZE_MB / 1024 ))
echo "Loading scsi_debug: ${TOTAL_DISKS} devices x ${DISK_SIZE_MB}MB = ${RAM_NEEDED_GB}GB RAM"
sudo modprobe scsi_debug dev_size_mb=${DISK_SIZE_MB} add_host=${TOTAL_DISKS} num_tgts=1 per_host_store=1

echo "Waiting for device nodes to settle..."
sudo udevadm settle
sleep 3

# -----------------------------------------------------------------------------
# Step 4: Identify new devices (exclude all pre-existing disks)
# -----------------------------------------------------------------------------
ALL_DISKS=$(lsblk -dno NAME,TYPE 2>/dev/null | awk '$2=="disk"{print $1}' | sort)
mapfile -t NEW_DISKS < <(comm -23 <(echo "${ALL_DISKS}") <(echo "${EXISTING_DISKS}"))

echo "Detected ${#NEW_DISKS[@]} new scsi_debug devices."
echo ""

if [ "${#NEW_DISKS[@]}" -ne "${TOTAL_DISKS}" ]; then
  echo "ERROR: Expected ${TOTAL_DISKS} new disks, got ${#NEW_DISKS[@]}"
  echo "Removing scsi_debug module..."
  sudo rmmod scsi_debug
  exit 1
fi

# -----------------------------------------------------------------------------
# Step 5: Map new devices into named arrays
# -----------------------------------------------------------------------------
DATA=()
for i in $(seq 0 99); do
  DATA+=("/dev/${NEW_DISKS[$i]}")
done

AUX=()
for i in $(seq 100 123); do
  AUX+=("/dev/${NEW_DISKS[$i]}")
done

DRAID=()
for i in $(seq 124 218); do
  DRAID+=("/dev/${NEW_DISKS[$i]}")
done

echo "Disk assignments:"
echo "  Data disks  [100]: ${DATA[0]} .. ${DATA[99]}"
echo "  Aux disks    [24]: ${AUX[0]} .. ${AUX[23]}"
echo "  dRAID disks  [95]: ${DRAID[0]} .. ${DRAID[94]}"
echo ""

# -----------------------------------------------------------------------------
# Step 6: Safety verification - ensure no existing disk is in our device lists
# -----------------------------------------------------------------------------
echo "Running safety verification..."

# Build a lookup set of all existing disk names
declare -A EXISTING_SET
while IFS= read -r disk_name; do
  [ -n "${disk_name}" ] && EXISTING_SET["/dev/${disk_name}"]=1
done <<< "${EXISTING_DISKS}"

# Check every device we plan to use against the existing disk set
ALL_POOL_DEVICES=("${DATA[@]}" "${AUX[@]}" "${DRAID[@]}")
for dev in "${ALL_POOL_DEVICES[@]}"; do
  if [ -n "${EXISTING_SET[${dev}]+exists}" ]; then
    echo "FATAL: ${dev} is a pre-existing system disk! Aborting."
    echo "Removing scsi_debug module..."
    sudo rmmod scsi_debug
    exit 1
  fi
done
echo "  [PASS] No pre-existing disks found in device lists."

# Verify each new device is actually backed by scsi_debug driver
FAILED_DRIVER_CHECK=0
for dev_name in "${NEW_DISKS[@]}"; do
  driver_path="/sys/block/${dev_name}/device/driver"
  if [ -L "${driver_path}" ]; then
    driver=$(basename "$(readlink "${driver_path}")")
    if [ "${driver}" != "sd" ]; then
      echo "  WARNING: /dev/${dev_name} driver is '${driver}', expected 'sd'"
    fi
  fi
  model_path="/sys/block/${dev_name}/device/model"
  if [ -f "${model_path}" ]; then
    model=$(cat "${model_path}" | tr -d ' ')
    if [ "${model}" != "scsi_debug" ]; then
      echo "  FATAL: /dev/${dev_name} model is '${model}', not 'scsi_debug'"
      FAILED_DRIVER_CHECK=1
    fi
  else
    echo "  FATAL: /dev/${dev_name} has no model file at ${model_path}"
    FAILED_DRIVER_CHECK=1
  fi
done

if [ "${FAILED_DRIVER_CHECK}" -ne 0 ]; then
  echo "FATAL: One or more devices are not scsi_debug devices! Aborting."
  echo "Removing scsi_debug module..."
  sudo rmmod scsi_debug
  exit 1
fi
echo "  [PASS] All ${#NEW_DISKS[@]} devices confirmed as scsi_debug devices."
echo ""

# =============================================================================
# Pool Creation (each requires user confirmation)
# =============================================================================

# Confirmation helper: shows full command, requires 'y' to proceed
confirm_and_create() {
  local pool_name="$1"
  shift
  local cmd_args=("$@")

  echo "----------------------------------------------------------------------"
  echo "Pool: ${pool_name}"
  echo ""
  echo "  sudo zpool create ${cmd_args[*]}"
  echo ""
  read -rp "Create this pool? [y/N]: " answer
  if [ "${answer}" != "y" ] && [ "${answer}" != "Y" ]; then
    echo "Skipped ${pool_name}."
    echo ""
    return
  fi
  sudo zpool create "${cmd_args[@]}"
  echo "Created ${pool_name}."
  echo ""
}

# --- test-z1: raidz1, two vdevs of 8 disks + 1 spare (17 disks) ---
confirm_and_create "test-z1" test-z1 \
  raidz1 "${DATA[@]:0:8}" \
  raidz1 "${DATA[@]:8:8}" \
  spare "${DATA[16]}"

# --- test-z2: raidz2, four vdevs of 6 disks + 1 spare (25 disks) ---
confirm_and_create "test-z2" test-z2 \
  raidz2 "${DATA[@]:17:6}" \
  raidz2 "${DATA[@]:23:6}" \
  raidz2 "${DATA[@]:29:6}" \
  raidz2 "${DATA[@]:35:6}" \
  spare "${DATA[41]}"

# --- test-z3: raidz3, three vdevs of 9 disks + 1 spare (28 disks) ---
confirm_and_create "test-z3" test-z3 \
  raidz3 "${DATA[@]:42:9}" \
  raidz3 "${DATA[@]:51:9}" \
  raidz3 "${DATA[@]:60:9}" \
  spare "${DATA[69]}"

# --- test-m2: mirror, four vdevs of 2 disks + 2 spares (10 disks) ---
confirm_and_create "test-m2" test-m2 \
  mirror "${DATA[70]}" "${DATA[71]}" \
  mirror "${DATA[72]}" "${DATA[73]}" \
  mirror "${DATA[74]}" "${DATA[75]}" \
  mirror "${DATA[76]}" "${DATA[77]}" \
  spare "${DATA[78]}" "${DATA[79]}"

# --- test-m3: mirror, three vdevs of 3 disks + 1 spare (10 disks) ---
confirm_and_create "test-m3" test-m3 \
  mirror "${DATA[80]}" "${DATA[81]}" "${DATA[82]}" \
  mirror "${DATA[83]}" "${DATA[84]}" "${DATA[85]}" \
  mirror "${DATA[86]}" "${DATA[87]}" "${DATA[88]}" \
  spare "${DATA[89]}"

# --- test-aux: raidz2, 8 disks + 2 spares + special/log/cache/dedup (10 data + 7 aux) ---
# -f required: mixed redundancy levels across data (raidz2) and auxiliary vdevs (mirror, single)
confirm_and_create "test-aux" test-aux -f \
  raidz2 "${DATA[@]:90:8}" \
  spare "${DATA[98]}" "${DATA[99]}" \
  special mirror "${AUX[0]}" "${AUX[1]}" \
  log mirror "${AUX[2]}" "${AUX[3]}" \
  cache "${AUX[4]}" "${AUX[5]}" \
  dedup "${AUX[6]}"

# --- test-d1: draid1, single parity, 5 data per group, 31 children, 1 spare ---
confirm_and_create "test-d1" test-d1 \
  draid1:5d:31c:1s "${DRAID[@]:0:31}"

# --- test-d2: draid2, double parity, 8 data per group, 32 children, 2 spares ---
confirm_and_create "test-d2" test-d2 \
  draid2:8d:32c:2s "${DRAID[@]:31:32}"

# --- test-d3: draid3, triple parity, 9 data per group, 30 children, 3 spares ---
confirm_and_create "test-d3" test-d3 \
  draid3:9d:30c:3s "${DRAID[@]:63:30}"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=== All 9 test pools created successfully ==="
echo ""
sudo zpool list
echo ""
echo "To tear down all test pools and free RAM:"
echo "  for p in test-z1 test-z2 test-z3 test-m2 test-m3 test-aux test-d1 test-d2 test-d3; do sudo zpool destroy \$p; done"
echo "  sudo rmmod scsi_debug"
