#!/bin/bash
set -e # エラーが発生したら即停止

# --- Root権限チェック ---
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: This script must be run as root (sudo)."
  exit 1
fi

# ==========================================
# 1. 設定 (環境に合わせて調整してください)
# ==========================================
MONGOD_BINARY="./mongod"
DB_PATH="/tmp/mongo_migration_test"
LOG_PATH="$DB_PATH/mongod.log"
PORT=27017

# データ量 (前回と同じ設定)
DOC_COUNT=200000
PADDING_SIZE=10240 

echo "=================================================="
echo "   MongoDB Read-Only Workload Test (Restart & Scan)   "
echo "=================================================="

# ==========================================
# [関数] キャッシュ統計を表示
# ==========================================
check_cache_stats() {
    local STEP_NAME="$1"
    echo ""
    echo "📊 --- [Stats] $STEP_NAME ---"
    mongosh --quiet --port "$PORT" --eval "
      try {
          const status = db.serverStatus().wiredTiger.cache;
          const bytes = status['bytes currently in the cache'];
          const mb = (bytes / (1024 * 1024)).toFixed(2);
          const pages = status['pages currently held in the cache'];
          const dirty = status['tracked dirty pages in the cache'];
          
          print('  - Cache Size : ' + bytes + ' bytes (' + mb + ' MB)');
          print('  - Total Pages: ' + pages);
          print('  - Dirty Pages: ' + dirty);
      } catch(e) { print('Error getting stats: ' + e); }
    "
    echo "------------------------------------------------"
}

# ==========================================
# 2. 初期化 & データ挿入 (Write Workload)
# ==========================================
echo "--- [Step 1] 環境リセットとデータ挿入 ---"
killall -9 mongod 2>/dev/null || true
rm -rf "$DB_PATH"
mkdir -p "$DB_PATH"
rm -f "$DB_PATH/my_debug.log"

# 起動 (キャッシュ1GB)
"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" \
  --port "$PORT" --bind_ip 127.0.0.1 --wiredTigerCacheSizeGB 1
sleep 5

# データ挿入
mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  db.my_table.drop();
  const bulk = db.my_table.initializeUnorderedBulkOp();
  const padding = 'A'.repeat($PADDING_SIZE); 
  print('Preparing ' + $DOC_COUNT + ' documents...');
  for (let i = 0; i < $DOC_COUNT; i++) {
      bulk.insert({ _id: i, val: padding });
  }
  print('Executing bulk insert...');
  bulk.execute();
  print('Insert complete.');
"

check_cache_stats "データ挿入直後 (Dirty Pagesが多い状態)"

# ==========================================
# 3. 再起動 (Restart)
# ==========================================
echo ""
echo "--- [Step 2] MongoDB 再起動 (メモリ構造のリセット) ---"
echo "👉 ここで一度停止し、メモリ上の断片化されたデータを破棄します。"

# 停止
mongosh --quiet --port "$PORT" --eval "db.shutdownServer({force: true})" || true
sleep 5

# 再起動 (同じ設定で)
echo "Starting mongod again..."
"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" \
  --port "$PORT" --bind_ip 127.0.0.1 --wiredTigerCacheSizeGB 1

sleep 5
echo "✅ mongod restarted."

check_cache_stats "再起動直後 (キャッシュは空に近い)"

# ==========================================
# 4. 全件スキャン (Read-Only Workload)
# ==========================================
echo ""
echo "--- [Step 3] 全件スキャン (ディスク -> メモリ読み込み) ---"
echo "👉 データをディスクから読み込みます。これにより連続領域(dsk)としてキャッシュされます。"

mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  print('Scanning all documents to warm up cache...');
  // itcount() でカーソルを最後まで回し、全データをメモリに乗せる
  const count = db.my_table.find({}).itcount();
  print('Scan complete. Loaded ' + count + ' documents.');
"

check_cache_stats "スキャン完了後 (Clean Pagesが多い状態)"

# ==========================================
# 5. カスタムキャッシュクリア実行
# ==========================================
echo ""
echo "--- [Step 4] カスタムキャッシュクリアの実行 (customClear) ---"
echo "👉 Read-Only ページに対するキャッシュ退避を実行します。"

mongosh --quiet --port "$PORT" --eval "
  const res = db.adminCommand({ customClear: 1 }); 
  printjson(res);
"

sleep 2
check_cache_stats "キャッシュクリア後"

echo ""
echo "=================================================="
echo "✅ TEST READY: Run 'migrate' in QEMU monitor now."
echo "=================================================="
