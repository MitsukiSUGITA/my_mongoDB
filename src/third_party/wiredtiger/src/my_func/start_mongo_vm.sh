#!/bin/bash
set -e # エラーが発生したら即停止

# ==========================================
# 1. 設定と引数チェック
# ==========================================
MONGOD_BINARY="/home/mitsuki/mongod"
DB_PATH="/mnt/tabata/mongo_migration_test"
LOG_PATH="$DB_PATH/mongod.log"
PORT=27017

# ★ 追加: 引数からキャッシュ比率を取得（指定がなければデフォルト60）
CACHE_RATIO=${1:-60}

# --- Root権限チェック ---
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: This script must be run as root (sudo)."
  exit 1
fi

echo "=================================================="
echo "      MongoDB Environment Reset & Startup         "
echo "        (Target Data Size: ${CACHE_RATIO}% of Cache)"
echo "=================================================="

# ==========================================
# 2. 環境の徹底クリーニング (Step 1)
# ==========================================
echo "--- [Step 1] 環境リセットとクリーンアップ ---"

systemctl stop mongod 2>/dev/null || true
echo "既存の mongod プロセスを終了しています..."
killall -9 mongod 2>/dev/null || true
sleep 2

if ! mountpoint -q /mnt/tabata; then
    echo "❌ Error: /mnt/tabata がマウントされていません。"
    exit 1
fi

echo "古い実験データを削除しています: $DB_PATH"
rm -rf "$DB_PATH"
mkdir -p "$DB_PATH"
chmod 777 "$DB_PATH"

# ==========================================
# 3. ディスク空き容量チェック (Phase 0)
# ==========================================
echo "--- [Phase 0] ディスク空き容量の動的計算とチェック ---"

VM_MEMORY_GB=$(awk '/MemTotal/ {printf "%.0f\n", $2/1024/1024}' /proc/meminfo)
if [ "$VM_MEMORY_GB" -le 0 ]; then VM_MEMORY_GB=1; fi

WT_CACHE_MB=$(( (VM_MEMORY_GB * 1000 - 1000) / 2 ))
if [ "$WT_CACHE_MB" -lt 0 ]; then WT_CACHE_MB=0; fi

# ★ 修正: 6/10 の固定値ではなく、引数の CACHE_RATIO を使用する
TARGET_DATA_MB=$(( WT_CACHE_MB * CACHE_RATIO / 100 ))

# 必要な空き容量 = (TARGET_DATA_MB * 1.5) + 2048MB(安全マージン)
DISK_THRESHOLD_MB=$(( TARGET_DATA_MB + (TARGET_DATA_MB * 1 / 10) + 2048 ))

echo "ℹ️  検出されたVMメモリ: ${VM_MEMORY_GB} GB"
echo "ℹ️  要求される空き容量: ${DISK_THRESHOLD_MB} MB"

FREE_SPACE_MB=$(df -m /mnt/tabata | awk 'NR==2 {print $4}')

if [ -n "$FREE_SPACE_MB" ] && [ "$FREE_SPACE_MB" -lt "$DISK_THRESHOLD_MB" ]; then
    echo "❌ エラー: ホスト側の空き容量が不足しています。"
    echo "   現在: $((FREE_SPACE_MB)) MB"
    echo "   データを消去しても $DISK_THRESHOLD_MB MB 未満のため、実験を中止します。"
    exit 1
fi
echo "✅ 空き容量チェック完了（現在空き: $((FREE_SPACE_MB)) MB）"

# ==========================================
# 4. OSレベルの最適化
# ==========================================
echo "--- [Step 2] OSのクリーンアップと設定 ---"

echo "OSのページキャッシュをクリアしています..."
sync; echo 3 > /proc/sys/vm/drop_caches

echo "Transparent Huge Pages (THP) を無効化しています..."
if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
fi

# ==========================================
# 5. ivshmemの設定
# ==========================================
IVSHMEM_DIR=$(lspci -D -d 1af4:1110 | awk '{print $1}' | head -n 1)
if [ -n "$IVSHMEM_DIR" ]; then
    export WT_IVSHMEM_PATH="/sys/bus/pci/devices/${IVSHMEM_DIR}/resource2"
    echo "✅ Found ivshmem device at: $WT_IVSHMEM_PATH"
else
    export WT_IVSHMEM_PATH="/sys/bus/pci/devices/0000:00:05.0/resource2"
    echo "⚠️ Warning: Using default ivshmem path: $WT_IVSHMEM_PATH"
fi

# ==========================================
# 6. MongoDB起動 (Step 3)
# ==========================================
echo "--- [Step 3] 自作 mongod の起動 ---"

# ★追加：このスクリプト（Rootサブシェル）内でコアダンプを無制限に許可する
#ulimit -c unlimited
# ★追加：念のためコアダンプの出力先を /tmp に強制指定する
#sysctl -w kernel.core_pattern=/tmp/core.%e.%p > /dev/null

#本番用
"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" \
  --port "$PORT" --bind_ip 0.0.0.0
#デバッグ用
#"$MONGOD_BINARY" --dbpath "$DB_PATH" --logpath "$LOG_PATH" \
#  --port "$PORT" --bind_ip 0.0.0.0 &

sleep 5

if ! pgrep -f "mongod.*$PORT" > /dev/null; then
    echo "❌ ERROR: mongod の起動に失敗しました。"
    echo "--- mongod.log の末尾を確認 ---"
    tail -n 20 "$LOG_PATH"
    exit 1
fi

echo "✅ mongod が正常に起動しました (PID: $(pgrep -f "mongod.*$PORT"))"
echo "YCSBからの接続準備が整いました"