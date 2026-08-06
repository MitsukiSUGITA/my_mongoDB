#!/bin/bash
set -e # エラーが発生したら即停止

# --- 引数でターゲットのメモリサイズを受け取る (デフォルト 1G) ---
TARGET_MEM=${1:-1G}

# --- Root権限チェック ---
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: This script must be run as root (sudo)."
  exit 1
fi

# ==========================================
# 0. サイズ計算ロジック (4G などを バイト数とドキュメント数に変換)
# ==========================================
PADDING_SIZE=10240 

MEM_UNIT=$(echo "$TARGET_MEM" | grep -o -E '[A-Za-z]+' | tr '[:lower:]' '[:upper:]')
MEM_NUM=$(echo "$TARGET_MEM" | grep -o -E '[0-9]+')

case "$MEM_UNIT" in
    G|GB) TARGET_BYTES=$((MEM_NUM * 1024 * 1024 * 1024)) ;;
    M|MB) TARGET_BYTES=$((MEM_NUM * 1024 * 1024)) ;;
    K|KB) TARGET_BYTES=$((MEM_NUM * 1024)) ;;
    *)    TARGET_BYTES=$MEM_NUM ;; # 単位なしの場合はバイト扱い
esac

# 必要なドキュメント数を逆算
INPUT_DOC_COUNT=$(( TARGET_BYTES / PADDING_SIZE ))

# ==========================================
# 1. 設定
# ==========================================
MONGOD_BINARY="./mongod"
# ★注意: qcow2上のローカルパスに変更済みであることを確認
DB_PATH="/home/mitsuki/mongo_data"
LOG_PATH="$DB_PATH/mongod.log"
PORT=27017

# ivshmemのパスを動的に取得・設定
IVSHMEM_DIR=$(lspci -D -d 1af4:1110 | awk '{print $1}' | head -n 1)
if [ -n "$IVSHMEM_DIR" ]; then
    export WT_IVSHMEM_PATH="/sys/bus/pci/devices/${IVSHMEM_DIR}/resource2"
    echo "✅ Found ivshmem device at: $WT_IVSHMEM_PATH"
else
    export WT_IVSHMEM_PATH="/sys/bus/pci/devices/0000:00:05.0/resource2"
    echo "⚠️ Warning: ivshmem device auto-detect failed. Using default: $WT_IVSHMEM_PATH"
fi

echo "=================================================="
echo "      MongoDB Data Insertion (Clean Cache Warmer) "
echo "=================================================="
echo "Target Size :  ${TARGET_MEM} ($TARGET_BYTES bytes)"
echo "Doc Count   :  ${INPUT_DOC_COUNT}"
echo "--------------------------------------------------"

# ==========================================
# [関数] キャッシュ統計を表示するヘルパー
# ==========================================
check_cache_stats() {
    local STEP_NAME="$1"
    echo ""
    echo "📊 --- [Stats] $STEP_NAME ---"
    mongosh --quiet --port "$PORT" --eval "
      try {
          const status = db.serverStatus().wiredTiger.cache;
          const bytes = status['bytes currently in the cache'];
          const max_bytes = status['maximum bytes configured'];
          const dirty_bytes = status['tracked dirty bytes in the cache'];
          const pages = status['pages currently held in the cache'];
          const dirty_pages = status['tracked dirty pages in the cache'];
          
          const gb = (bytes / (1024 * 1024 * 1024)).toFixed(2);
          const max_gb = (max_bytes / (1024 * 1024 * 1024)).toFixed(2);
          const dirty_gb = (dirty_bytes / (1024 * 1024 * 1024)).toFixed(2);
          const dirty_percent = (dirty_bytes / bytes * 100).toFixed(2);
          const usage_percent = (bytes / max_bytes * 100).toFixed(2);

          print('  - Max Configured: ' + max_gb + ' GB');
          print('  - Current Cache : ' + bytes + ' bytes (' + gb + ' GB)');
          print('  - Usage Rate    : ' + usage_percent + ' %');
          print('  - Dirty Data    : ' + dirty_bytes + ' bytes (' + dirty_gb + ' GB)');
          print('  - Dirty Rate    : ' + dirty_percent + ' %');
          print('  --------------------------------');
          print('  - Total Pages   : ' + pages);
          print('  - Dirty Pages   : ' + dirty_pages);
      } catch(e) {
          print('Error getting stats: ' + e);
      }
    "
    echo "------------------------------------------------"
}

# ==========================================
# 2. 環境リセット & 起動
# ==========================================
echo "--- [Step 1] 環境リセットと起動 ---"
killall -9 mongod 2>/dev/null || true
sleep 2

# ★前回分のデータを消去 (純粋な状態から作成)
rm -rf "$DB_PATH"/*
mkdir -p "$DB_PATH"

rm -f "$DB_PATH/my_debug.log"
touch "$DB_PATH/my_debug.log"
chmod 777 "$DB_PATH/my_debug.log"

echo "OSのページキャッシュをクリアしています..."
sync; echo 3 > /proc/sys/vm/drop_caches

echo "Transparent Huge Pages (THP) を無効化しています..."
if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
fi
if [ -f /sys/kernel/mm/transparent_hugepage/defrag ]; then
    echo never > /sys/kernel/mm/transparent_hugepage/defrag
fi

echo "Applying Core Dump and TCMALLOC settings..."
ulimit -c unlimited
export TCMALLOC_RELEASE_RATE=0

echo "Starting mongod..."
"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" \
  --port "$PORT" --bind_ip 0.0.0.0 \
  --syncdelay 3600 \
  --wiredTigerCollectionBlockCompressor none \
  --wiredTigerIndexPrefixCompression false \
  --wiredTigerEngineConfigString "mmap=false,checkpoint=(wait=3600),eviction_dirty_target=90,eviction_dirty_trigger=95,eviction_target=95,eviction_trigger=99"

sleep 5

if ! pgrep -f "mongod.*$PORT" > /dev/null; then
    echo "❌ ERROR: mongod failed to start."
    tail -n 20 "$LOG_PATH"
    exit 1
fi
echo "✅ mongod started (PID: $(pgrep -f "mongod.*$PORT"))"

# ==========================================
# 3. データ挿入とクリーン化 (キャッシュ温め)
# ==========================================
echo "--- [Step 2] データ挿入 ---"
mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  db.my_table.drop();
  let bulk = db.my_table.initializeUnorderedBulkOp();
  
  const padding = 'A'.repeat($PADDING_SIZE); 
  const totalDocs = $INPUT_DOC_COUNT;

  print('Executing bulk insert in batches...');
  for (let i = 0; i < totalDocs; i++) {
      bulk.insert({ _id: i, val: padding });
      
      if ((i + 1) % 10000 === 0) {
          bulk.execute();
          print('  Inserted ' + (i + 1) + ' / ' + totalDocs + ' documents...');
          bulk = db.my_table.initializeUnorderedBulkOp();
      }
  }
  if (totalDocs % 10000 !== 0) {
      bulk.execute();
  }
  print('✅ Insert complete: ' + totalDocs + ' documents.');
"

check_cache_stats "データ挿入直後 (Dirty Rateが高い状態)"

# ★追加・変更: クリーンページを生成するための同期処理
echo "--- [Step 3] ディスク同期 (クリーンページの生成) ---"
echo "🔄 発行: fsync (メモリ上のダーティデータをqcow2へ強制書き込み)..."
mongosh --quiet --port "$PORT" --eval "
  db.adminCommand({ fsync: 1 });
  print('✅ fsync complete. Data is now synced to disk.');
"

check_cache_stats "fsync直後 (Dirty Rateがほぼ0% = クリーンページ状態)"

echo ""
echo "=================================================="
echo "✅ CLEAN CACHE WARM-UP COMPLETE"
echo "=================================================="
echo "MongoDBはポート $PORT で待機中です。"
echo "メモリ内はqcow2と完全に同期した「クリーンページ」で満たされています。"
echo "この状態でFIEMAPのLBA取得・検証を行ってください。"