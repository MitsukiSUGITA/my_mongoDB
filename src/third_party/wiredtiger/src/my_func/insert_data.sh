#!/bin/bash
set -e # エラーが発生したら即停止

# --- Root権限チェック ---
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: This script must be run as root (sudo)."
  exit 1
fi

# ==========================================
# 1. 設定
# ==========================================
# VM内のmongodへのパス
MONGOD_BINARY="./mongod"

# テスト用DBとログの場所
DB_PATH="/tmp/mongo_migration_test"
LOG_PATH="$DB_PATH/mongod.log"

PORT=27017

# データ量設定 (約1GBのデータを生成してキャッシュを埋める)
DOC_COUNT=200000
PADDING_SIZE=10240 

echo "=================================================="
echo "      MongoDB Data Insertion (Cache Warmer)       "
echo "=================================================="
echo "Target Port: $PORT"
echo "DB Path:     $DB_PATH"
echo "Binary:      $MONGOD_BINARY"
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
          const mb = (bytes / (1024 * 1024)).toFixed(2);
          const pages = status['pages currently held in the cache'];
          const dirty = status['tracked dirty pages in the cache'];
          
          print('  - Cache Size : ' + bytes + ' bytes (' + mb + ' MB)');
          print('  - Total Pages: ' + pages);
          print('  - Dirty Pages: ' + dirty);
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
# 古いプロセスを停止
killall -9 mongod 2>/dev/null || true

# 古いディレクトリを削除して再作成
rm -rf "$DB_PATH"
mkdir -p "$DB_PATH"

# デバッグログのリセット（必要であれば）
rm -f "$DB_PATH/my_debug.log"
touch "$DB_PATH/my_debug.log"
chmod 777 "$DB_PATH/my_debug.log"

echo "Starting mongod..."
# 起動 (キャッシュサイズを1GBに固定して、データがメモリに載るようにする)
"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" \
  --port "$PORT" --bind_ip 127.0.0.1 \
  --wiredTigerCacheSizeGB 1

sleep 5

# 起動確認
if ! pgrep -f "mongod.*$PORT" > /dev/null; then
    echo "❌ ERROR: mongod failed to start."
    echo "--- Tail of mongod.log ---"
    tail -n 20 "$LOG_PATH"
    exit 1
fi
echo "✅ mongod started (PID: $(pgrep -f "mongod.*$PORT"))"

# ==========================================
# 3. データ挿入 (キャッシュ温め)
# ==========================================
echo "--- [Step 2] データ挿入 (約 1GB) ---"
mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  db.my_table.drop();
  const bulk = db.my_table.initializeUnorderedBulkOp();
  
  // QEMUでの検証用に 'A' (0x41) で埋める
  const padding = 'A'.repeat($PADDING_SIZE); 

  print('Preparing bulk insert...');
  for (let i = 0; i < $DOC_COUNT; i++) {
      bulk.insert({ 
          _id: i, 
          val: padding 
      });
      // 進捗表示
      // if (i % 20000 == 0 && i > 0) print('  Prepared ' + i + ' documents...');
  }
  print('Executing bulk insert (this may take a while)...');
  bulk.execute();
  print('✅ Insert complete: $DOC_COUNT documents.');
"

# 統計確認: 挿入後
check_cache_stats "データ挿入直後 (High Cache Usage)"

echo ""
echo "=================================================="
echo "✅ DATA INSERTION COMPLETE"
echo "=================================================="
echo "MongoDBはポート $PORT で起動中です。"
echo "キャッシュにデータが充填されました。"
echo "次にキャッシュクリア用スクリプトを実行するか、手動でコマンドを試してください。"