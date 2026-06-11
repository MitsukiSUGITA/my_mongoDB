#!/bin/bash
set -e # エラーが発生したら即停止

# --- 引数で設定を受け取る ---
INPUT_DOC_COUNT=${1:-200000}

# --- Root権限チェック ---
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: This script must be run as root (sudo)."
  exit 1
fi

# ==========================================
# 1. 設定
# ==========================================
MONGOD_BINARY="./mongod"
DB_PATH="/tmp/mongo_migration_test"
LOG_PATH="$DB_PATH/mongod.log"
PORT=27017
PADDING_SIZE=10240 

# 🌟 改善点1: ivshmemのパスを動的に取得・設定（スキップ判定ロジック用）
IVSHMEM_DIR=$(lspci -D -d 1af4:1110 | awk '{print $1}' | head -n 1)
if [ -n "$IVSHMEM_DIR" ]; then
    export WT_IVSHMEM_PATH="/sys/bus/pci/devices/${IVSHMEM_DIR}/resource2"
    echo "✅ Found ivshmem device at: $WT_IVSHMEM_PATH"
else
    export WT_IVSHMEM_PATH="/sys/bus/pci/devices/0000:00:05.0/resource2"
    echo "⚠️ Warning: ivshmem device auto-detect failed. Using default: $WT_IVSHMEM_PATH"
fi

echo "=================================================="
echo "      MongoDB Data Insertion (Cache Warmer)       "
echo "=================================================="
echo "Doc Count:   ${INPUT_DOC_COUNT}"
echo "Approx Data: $(( INPUT_DOC_COUNT * PADDING_SIZE / 1024 / 1024 )) MB"
echo "--------------------------------------------------"

# ==========================================
# [関数] キャッシュ統計を表示するヘルパー
# (変更なし: 非常に良く書かれています)
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
          print('  - Dirty Rate    : ' + dirty_percent + ' % (Target: >90%)');
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

rm -rf "$DB_PATH"
mkdir -p "$DB_PATH"

rm -f "$DB_PATH/my_debug.log"
touch "$DB_PATH/my_debug.log"
chmod 777 "$DB_PATH/my_debug.log"

# 🌟 改善点2: OSのページキャッシュとTHPの無効化（ノイズ排除）
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
  --port "$PORT" --bind_ip 127.0.0.1 \
  --syncdelay 3600 \
  --wiredTigerEngineConfigString "mmap=false,checkpoint=(wait=3600),eviction_dirty_target=90,eviction_dirty_trigger=95,eviction_target=95,eviction_trigger=99"

sleep 5

if ! pgrep -f "mongod.*$PORT" > /dev/null; then
    echo "❌ ERROR: mongod failed to start."
    tail -n 20 "$LOG_PATH"
    exit 1
fi
echo "✅ mongod started (PID: $(pgrep -f "mongod.*$PORT"))"

# ==========================================
# 3. データ挿入 (キャッシュ温め)
# ==========================================
echo "--- [Step 2] データ挿入とダーティ化 ---"
mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  db.my_table.drop();
  let bulk = db.my_table.initializeUnorderedBulkOp();
  
  const padding = 'A'.repeat($PADDING_SIZE); 
  const totalDocs = $INPUT_DOC_COUNT;

  print('Executing bulk insert in batches...');
  for (let i = 0; i < totalDocs; i++) {
      bulk.insert({ _id: i, val: padding });
      
      // 🌟 改善点3: 1万件ごとにバッチ実行し、mongoshのOOMクラッシュを防ぐ
      if ((i + 1) % 10000 === 0) {
          bulk.execute();
          print('  Inserted ' + (i + 1) + ' / ' + totalDocs + ' documents...');
          bulk = db.my_table.initializeUnorderedBulkOp(); // バルク再初期化
      }
  }
  // 端数の処理
  if (totalDocs % 10000 !== 0) {
      bulk.execute();
  }
  print('✅ Insert complete: ' + totalDocs + ' documents.');

  print('🔄 Force-updating all documents to maximize Dirty Rate...');
  db.my_table.updateMany({}, { \$set: { dirty_flag: 1 } });
  print('✅ Update complete.');
"

# 統計確認: 挿入後
check_cache_stats "データ挿入直後 (High Cache Usage)"

echo ""
echo "=================================================="
echo "✅ DATA INSERTION COMPLETE"
echo "=================================================="
echo "MongoDBはポート $PORT で起動中です。"
echo "キャッシュにデータが充填されました。"