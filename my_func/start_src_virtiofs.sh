#!/bin/bash

# 引数が指定されていればその値を使用し、なければ 4G をデフォルトとする
MEM_SIZE=${1:-4G}

echo "[INFO] Starting Source VM (virtio-fs) with Memory: ${MEM_SIZE}"

# 古いソケットのクリーンアップ
rm -f /tmp/vhostqemu

# virtiofsd をバックグラウンドで起動
echo "[INFO] Starting virtiofsd..."
/usr/libexec/virtiofsd \
    --socket-path=/tmp/vhostqemu \
    --shared-dir=/mnt/nfs_share/mongo_data \
    --cache=always &

VIRTIOFSD_PID=$!

# QEMU終了時に自動で virtiofsd を停止させるトラップ
trap 'echo "[INFO] Stopping virtiofsd..."; kill $VIRTIOFSD_PID; exit' SIGINT SIGTERM EXIT

# ソケットが作られるまで少し待機
sleep 1

# QEMU本体の起動
echo "[INFO] Starting QEMU..."
QEMU_ALLOW_IVSHMEM_MIGRATION=1 \
/home/mitsuki/my_qemu/build/qemu-system-x86_64 \
     -name migration \
     -enable-kvm \
     -machine memory-backend=mem \
     -object memory-backend-memfd,id=mem,size=${MEM_SIZE},share=on \
     -m ${MEM_SIZE} \
     -cpu host \
     -smp 2 \
     -L /usr/share/qemu \
     -drive file=/var/lib/libvirt/images/migration.qcow2,format=qcow2,if=virtio,cache=none \
     -netdev bridge,id=net0,br=br0 \
     -device virtio-net-pci,netdev=net0,mac=52:54:00:12:34:56 \
     -device virtio-serial-pci \
     -chardev socket,id=meta_char,path=/tmp/metadata_port.sock,server=on,wait=off \
     -device virtserialport,chardev=meta_char,name=metadata_port \
     -object memory-backend-file,size=8M,share=on,mem-path=/dev/shm/mongo_bitmap,id=shm0 \
     -device ivshmem-plain,memdev=shm0 \
     -chardev socket,id=char0,path=/tmp/vhostqemu \
     -device vhost-user-fs-pci,queue-size=1024,chardev=char0,tag=myfs \
     -vnc 0.0.0.0:0 \
     -monitor stdio \
     -monitor telnet:127.0.0.1:4444,server,nowait \
     -serial file:/tmp/host_mongo_debug_src.log \
     2>&1 | tee /tmp/qemu_monitor_src.log