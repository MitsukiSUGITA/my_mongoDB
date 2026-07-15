#!/bin/bash
set -e # エラーが発生したら即停止

# ==========================================
# 1. 設定
# ==========================================
MONGOD_BINARY="/home/mitsuki/mongod"
# ★変更: virtio-fsのマウントポイントに変更
DB_PATH="/mnt/virtio_mongo"
LOG_PATH="$DB_PATH/mongod.log"
PORT=27017

# --- Root権限チェック ---
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: This script must be run as root (sudo)."
  exit 1
fi

echo "=================================================="
echo "      Starting Custom MongoDB on virtio-fs        "
echo "=================================================="

# ==========================================
# 2. 安全なプロセス終了とクリーンアップ
# ==========================================
echo "--- [Step 1] プロセスのクリーンアップ ---"
systemctl stop mongod 2>/dev/null || true
killall -9 mongod 2>/dev/null || true
sleep 2

if ! mountpoint -q /mnt/virtio_mongo; then
    echo "❌ Error: /mnt/virtio_mongo がマウントされていません。"
    echo "先に 'sudo mount -t virtiofs myfs /mnt/virtio_mongo' を実行してください。"
    exit 1
fi

# ★重要: データ削除(rm -rf)は行いません。既存のデータをそのまま使います。
# 念のためロックファイルだけは削除して起動失敗を防ぐ
rm -f "$DB_PATH/mongod.lock"

# ==========================================
# 3. OSレベルの最適化
# ==========================================
echo "--- [Step 2] OSのキャッシュクリアとTHP無効化 ---"
sync; echo 3 > /proc/sys/vm/drop_caches

if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
fi

# ==========================================
# 4. ivshmemの設定 (マイグレーション研究に必須)
# ==========================================
echo "--- [Step 3] 共有ビットマップ(ivshmem)の設定 ---"
IVSHMEM_DIR=$(lspci -D -d 1af4:1110 | awk '{print $1}' | head -n 1)
if [ -n "$IVSHMEM_DIR" ]; then
    export WT_IVSHMEM_PATH="/sys/bus/pci/devices/${IVSHMEM_DIR}/resource2"
    echo "✅ Found ivshmem device at: $WT_IVSHMEM_PATH"
else
    # デフォルトのフォールバックパス
    export WT_IVSHMEM_PATH="/sys/bus/pci/devices/0000:00:05.0/resource2"
    echo "⚠️ Warning: Using default ivshmem path: $WT_IVSHMEM_PATH"
fi

# ==========================================
# 5. MongoDB起動
# ==========================================
echo "--- [Step 4] 自作 mongod の起動 ---"

"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" \
  --port "$PORT" --bind_ip 0.0.0.0

sleep 5

if ! pgrep -f "mongod.*$PORT" > /dev/null; then
    echo "❌ ERROR: mongod の起動に失敗しました。"
    echo "--- mongod.log の末尾を確認 ---"
    tail -n 20 "$LOG_PATH"
    exit 1
fi

echo "✅ mongod が正常に起動しました (PID: $(pgrep -f "mongod.*$PORT"))"