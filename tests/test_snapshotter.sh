#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o xtrace

CONTD_VER=$(curl -fsSLI -o /dev/null -w %{url_effective} https://github.com/containerd/containerd/releases/latest | awk -F '/' '{sub("^v", "", $8); print $8}')
drive="`pwd`/dummydevice.img"
drivesize="10G"
lodevice=""
vgname="vgthin"
lvpoolname="lvthinpool"
SPID=""

function teardown_device()
{
  cat /tmp/lvmsnapshotter.log
  sudo vgs
  sudo lvs
  sudo pvs
  stop_snapshotter || true
  sudo vgremove -y ${vgname} || true
  sudo losetup -D ${lodevice}
  sudo rm ${drive}
}

trap teardown_device EXIT

function setup_device()
{
  sudo truncate -s ${drivesize} ${drive}
  sudo losetup -f ${drive}
  lodevice=$(sudo losetup -a | grep ${drive} | cut -d ":" -f1)
}

function create_lvm_stuff()
{
  sudo vgcreate ${vgname} ${lodevice}
  sudo lvcreate --thinpool ${lvpoolname} -l 90%FREE ${vgname}
}

function start_snapshotter()
{
  cmd="$(pwd)/lvm-snapshotter /var/run/lvmsnapshotter.sock vgthin lvthinpool"
  nohup sudo $cmd 2>&1 > /tmp/lvmsnapshotter.log &
}

function stop_snapshotter()
{
  SPID=$(sudo pgrep lvm-snapshotter)
  i=0
  sudo kill -SIGINT $SPID
  while [[ -n $(sudo ps -p $SPID -o pid=) ]] && [[ i -le 10 ]]
  do
    sleep 2
    i=$[$i+1]
  done || sudo kill -SIGKILL $SPID
}

function start_containerd()
{
  echo "Downloading containerd release for ${CONTD_VER}..."
  curl https://storage.googleapis.com/cri-containerd-release/cri-containerd-${CONTD_VER}.linux-amd64.tar.gz | sudo tar --no-overwrite-dir -C / -xzf -
  sudo mkdir -p /etc/containerd
  sudo bash -c 'cat << EOF > /etc/containerd/config.toml
[proxy_plugins]
  [proxy_plugins.lvmsnapshotter]
    type = "snapshot"
    address = "/var/run/lvmsnapshotter.sock"
EOF'
  nohup sudo /usr/local/bin/containerd 2>&1 > /tmp/containerd.log &
  i=0
  while [[ ! $(sudo ls /run/containerd/containerd.sock) && i -le 10 ]]
  do
    sleep 2
    i=$[$i+1]
  done || (echo "Unable to start containerd" && exit 1)
}

function test_snapshotter()
{
  sudo ctr i pull --snapshotter=lvmsnapshotter docker.io/library/busybox:latest
  sudo ctr i pull --snapshotter=lvmsnapshotter docker.io/library/nginx:latest
  if [ $(sudo lvs ${vgname} --no-heading | wc -l) != 6 ]; then
    echo "Right no. of volumes not created"
    exit 1
  fi

  sudo ctr i remove --sync docker.io/library/busybox:latest
  sudo ctr i remove --sync docker.io/library/nginx:latest
  if [ $(sudo lvs ${vgname} --no-heading | wc -l) != 2 ]; then
    echo "Right no. of volumes not created"
    exit 1
  fi
}

setup_device
create_lvm_stuff
start_snapshotter
start_containerd
test_snapshotter
