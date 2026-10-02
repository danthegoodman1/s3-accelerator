#!/usr/bin/env bash
# Prepares a host for the load test, as root: `prepare.sh node|client
# [monitor]`. Safe to run again. Its last line of output is the host's
# facts as JSON, which `loadtest render` sizes each node's cache from.
#
# Storage nodes stripe their instance-store NVMe drives into one ext4
# filesystem at /mnt/s3accel. Every host gets the kernel's TLS module,
# network and file limits for many connections, node_exporter, and the
# systemd unit `s3accel@NAME`, which runs /opt/s3accel/etc/NAME.toml.
set -euo pipefail

role=${1:?usage: prepare.sh node|client [monitor]}
monitor=${2:-}
mount=/mnt/s3accel

export DEBIAN_FRONTEND=noninteractive
packages=(curl jq mdadm nvme-cli iproute2 ethtool sysstat prometheus-node-exporter
          linux-tools-common "linux-tools-$(uname -r)")
if [ "$monitor" = monitor ]; then
  packages+=(prometheus)
fi
if ! dpkg -s "${packages[@]}" >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y -qq "${packages[@]}" >/dev/null
fi

# Kernel TLS, which moves clients' and members' TLS sessions into the
# kernel so blocks still leave through sendfile and splice.
echo tls >/etc/modules-load.d/s3accel-tls.conf
modprobe tls || echo "no kernel TLS module; TLS runs in userspace" >&2

cat >/etc/sysctl.d/90-s3accel.conf <<'SYSCTL'
# Queues for bursts of new connections; listeners ask for 4,096.
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 8192
net.core.netdev_max_backlog = 16384
# Buffers that fill a 25-100 Gbps link.
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.ipv4.tcp_rmem = 4096 131072 134217728
net.ipv4.tcp_wmem = 4096 131072 134217728
# Clients open many connections; keep ephemeral ports clear of the
# cluster's own 9000-9402.
net.ipv4.ip_local_port_range = 10240 65535
net.ipv4.tcp_tw_reuse = 1
fs.nr_open = 4194304
fs.file-max = 8388608
# Relays splice through pipes; on kernels before 7.1 a user past this
# limit gets two-page pipes, which skip bytes of kernel TLS records.
fs.pipe-user-pages-soft = 0
SYSCTL
sysctl -q --system

mkdir -p /opt/s3accel/bin /opt/s3accel/etc/plans /var/lib/s3accel-load

# The soft limit on open files starts low, as many service managers leave
# it; the server raises it to the hard limit when it starts.
cat >/etc/systemd/system/s3accel@.service <<'UNIT'
[Unit]
Description=s3-accelerator %i
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/opt/s3accel/bin/s3-accelerator /opt/s3accel/etc/%i.toml
LimitNOFILE=1024:1048576
LimitMEMLOCK=infinity
Restart=no
TimeoutStopSec=120
SyslogIdentifier=s3accel-%i

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now prometheus-node-exporter >/dev/null 2>&1 || true

cache_bytes=0
devices=()
if [ "$role" = node ]; then
  # Instance-store drives, less any that hold a mounted filesystem or
  # belong to the root volume.
  mapfile -t found < <(
    for link in /dev/disk/by-id/nvme-Amazon_EC2_NVMe_Instance_Storage_*; do
      [ -e "$link" ] && readlink -f "$link"
    done | grep -v 'p[0-9]*$' | sort -u
  )
  if [ "${#found[@]}" -eq 0 ]; then
    echo "no instance-store NVMe drives on this host" >&2
    exit 1
  fi
  if ! mountpoint -q "$mount"; then
    if [ "${#found[@]}" -gt 1 ]; then
      device=/dev/md/s3accel
      if [ ! -e "$device" ]; then
        mdadm --create "$device" --run --level=0 --raid-devices="${#found[@]}" "${found[@]}"
      fi
    else
      device=${found[0]}
    fi
    if ! blkid "$device" >/dev/null 2>&1; then
      mkfs.ext4 -q -F -E lazy_itable_init=1,lazy_journal_init=1,nodiscard -L s3accel "$device"
    fi
    mkdir -p "$mount"
    mount -o noatime "$device" "$mount"
  fi
  mkdir -p "$mount/data"
  source=$(findmnt -n -o SOURCE "$mount")
  # Count reads and writes on the md device when striped, or the drive.
  devices=("$(basename "$(readlink -f "$source")")")
  printf '%s\n' "${devices[@]}" >/opt/s3accel/etc/cache-devices
  cache_bytes=$(df -B1 --output=avail "$mount" | tail -1 | tr -d ' ')
fi

jq -nc \
  --arg host "$(hostname)" \
  --arg kernel "$(uname -r)" \
  --argjson cpus "$(nproc)" \
  --argjson memory_bytes "$(awk '/^MemTotal/ { printf "%.0f", $2 * 1024 }' /proc/meminfo)" \
  --argjson cache_bytes "$cache_bytes" \
  --arg devices "${devices[*]}" \
  --argjson tls "$([ -e /proc/net/tls_stat ] && echo true || echo false)" \
  '{host: $host, kernel: $kernel, cpus: $cpus, memory_bytes: $memory_bytes,
    cache_bytes: $cache_bytes, devices: ($devices | split(" ") | map(select(. != ""))), kernel_tls: $tls}'
