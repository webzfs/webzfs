#!/bin/bash
set -euo pipefail

# =============================================================================
# WebZFS Test Pool Fill Script
#
# Uses dd to write 100MB files into each test pool until target fill
# percentage is reached. Creates a new subdirectory every 10 files.
# Sets recordsize=1M on each pool before writing for maximum throughput.
#
# Source: /dev/urandom
# File size: 100MB each
# =============================================================================

FILE_SIZE_MB=25
FILES_PER_DIR=4

# Pool fill targets (percentage of total pool capacity)
POOL_ORDER=(test-z1 test-z2 test-z3 test-m2 test-m3 test-aux test-d1 test-d2 test-d3)

declare -A TARGETS
TARGETS[test-z1]=40
TARGETS[test-z2]=72
TARGETS[test-z3]=22
TARGETS[test-m2]=84
TARGETS[test-m3]=92
TARGETS[test-aux]=90
TARGETS[test-d1]=22
TARGETS[test-d2]=44
TARGETS[test-d3]=66

# -----------------------------------------------------------------------------
# Fill function: writes 100MB files via dd until target % is reached
# -----------------------------------------------------------------------------
fill_pool() {
  local pool=$1
  local target_pct=$2

  # Check if pool exists
  if ! zpool list "${pool}" &>/dev/null; then
    echo "  SKIP: Pool ${pool} does not exist."
    return
  fi

  # Get usable pool capacity (used + available, accounts for RAID parity overhead)
  local current_used
  current_used=$(zfs get -Hp -o value used "${pool}")
  local available
  available=$(zfs get -Hp -o value available "${pool}")
  local usable_capacity=$(( current_used + available ))
  local mountpoint
  mountpoint=$(zfs get -Hp -o value mountpoint "${pool}")

  local target_bytes=$(( usable_capacity * target_pct / 100 ))
  local target_mb=$(( target_bytes / 1048576 ))
  local usable_mb=$(( usable_capacity / 1048576 ))

  # Check current usage
  if [ "${current_used}" -ge "${target_bytes}" ]; then
    local current_pct=$(( current_used * 100 / usable_capacity ))
    echo "  ${pool}: Already at ${current_pct}% (target: ${target_pct}%). Skipping."
    return
  fi

  # Set recordsize to 1M for maximum write throughput
  echo "  Setting recordsize=1M on ${pool}..."
  sudo zfs set recordsize=1M "${pool}"

  echo "  Filling ${pool} to ~${target_pct}% (~${target_mb}MB)..."

  local dest="${mountpoint}/fill_data"
  local file_num=0
  local dir_num=0
  local current_dir="${dest}/dir_${dir_num}"
  sudo mkdir -p "${current_dir}"

  while true; do
    # Check if target reached
    local used
    used=$(zfs get -Hp -o value used "${pool}")
    if [ "${used}" -ge "${target_bytes}" ]; then
      break
    fi

    # New subdirectory every FILES_PER_DIR files
    if [ $((file_num % FILES_PER_DIR)) -eq 0 ] && [ "${file_num}" -gt 0 ]; then
      dir_num=$((dir_num + 1))
      current_dir="${dest}/dir_${dir_num}"
      sudo mkdir -p "${current_dir}"
    fi

    # Write 100MB file
    sudo dd if=/dev/urandom of="${current_dir}/file_${file_num}.dat" \
      bs=4M count=${FILE_SIZE_MB} status=none 2>/dev/null || true

    file_num=$((file_num + 1))

    # Progress every 10 files (1GB)
    if [ $((file_num % 10)) -eq 0 ]; then
      used=$(zfs get -Hp -o value used "${pool}")
      local used_mb=$(( used / 1048576 ))
      echo "    ${file_num} files written (${used_mb}MB / ${target_mb}MB)"
    fi
  done

  # Report final state
  local final_used
  final_used=$(zfs get -Hp -o value used "${pool}")
  local final_mb=$(( final_used / 1048576 ))
  local final_pct=$(( final_used * 100 / usable_capacity ))
  echo "  ${pool}: ${final_pct}% filled (${final_mb}MB, ${file_num} files in $((dir_num + 1)) dirs)"
}

# =============================================================================
# Main
# =============================================================================
echo "=== WebZFS Test Pool Fill ==="
echo ""
echo "Method: dd from /dev/urandom, ${FILE_SIZE_MB}MB per file"
echo "Subdirectory every ${FILES_PER_DIR} files"
echo "Recordsize: 1M (set before writing)"
echo ""
echo "Fill targets:"
for pool in "${POOL_ORDER[@]}"; do
  if zpool list "${pool}" &>/dev/null; then
    local_used=$(zfs get -Hp -o value used "${pool}")
    local_avail=$(zfs get -Hp -o value available "${pool}")
    local_usable=$(( local_used + local_avail ))
    local_target_mb=$(( local_usable * ${TARGETS[$pool]} / 100 / 1048576 ))
    local_usable_mb=$(( local_usable / 1048576 ))
    echo "  ${pool}: ${TARGETS[$pool]}% (~${local_target_mb}MB of ${local_usable_mb}MB usable)"
  else
    echo "  ${pool}: ${TARGETS[$pool]}% (pool not found)"
  fi
done
echo ""

read -rp "Begin filling pools? [y/N]: " answer
if [ "${answer}" != "y" ] && [ "${answer}" != "Y" ]; then
  echo "Aborted."
  exit 0
fi
echo ""

for pool in "${POOL_ORDER[@]}"; do
  fill_pool "${pool}" "${TARGETS[$pool]}"
  echo ""
done

echo "=== Fill complete ==="
echo ""
zpool list
