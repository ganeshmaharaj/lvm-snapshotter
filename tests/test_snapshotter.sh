#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o xtrace

contd_ver=$(curl -q https://api.github.com/repos/containerd/containerd/releases | grep "tag_name" | awk -F '"' '{print $4}'  | sort -rV | head -1)
: ${CONTD_VER:=${contd_ver#v}}
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
  cmd="$(dirname ${0})/../lvm-snapshotter --addr /var/run/lvmsnapshotter.sock --vgname vgthin --lvpoolname lvthinpool"
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
  curl --location https://github.com/containerd/containerd/releases/download/v${CONTD_VER}/cri-containerd-cni-${CONTD_VER}-linux-amd64.tar.gz --output - | sudo -E tar -C / -zxvf -
  sudo mkdir -p /etc/containerd
  sudo bash -c 'cat << EOF > /etc/containerd/config.toml
[proxy_plugins]
  [proxy_plugins.lvmsnapshotter]
    type = "snapshot"
    address = "/var/run/lvmsnapshotter.sock"
EOF'
  sudo systemctl daemon-reload
  sudo systemctl enable --now containerd
  i=0
  while [[ ! $(sudo ls /run/containerd/containerd.sock) && i -le 10 ]]
  do
    sleep 2
    i=$[$i+1]
  done || (echo "Unable to start containerd" && exit 1)
}

function test_snapshotter()
{
  sudo ctr i pull --snapshotter=lvmsnapshotter mirror.gcr.io/library/busybox:latest
  sudo ctr i pull --snapshotter=lvmsnapshotter mirror.gcr.io/library/nginx:latest
  if [ $(sudo lvs ${vgname} --no-heading | wc -l) != 9 ]; then
    echo "Right no. of volumes not created"
    exit 1
  fi

  sudo ctr i remove --sync mirror.gcr.io/library/busybox:latest
  sudo ctr i remove --sync mirror.gcr.io/library/nginx:latest
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
