#!/bin/bash
set -e

# --- Root権限チェック ---
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: Please run as root (sudo)."
  exit 1
fi

# ==========================================
# 1. 設定
# ==========================================
# VM内のmongodのパス (scpで送った場所に合わせてください)
MONGOD_BINARY="./mongod"

DB_PATH="/tmp/mongo_migration_test"
LOG_PATH="$DB_PATH/mongod.log"
PORT=27017
DOC_COUNT=100000

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
          print('(Is mongod running?)');
          quit(1);
      }
    "
    echo "------------------------------------------------"
}

# ==========================================
# 2. 環境リセット & 起動
# ==========================================
echo "--- [Step 1] Cleaning up and Starting mongod ---"
killall -9 mongod 2>/dev/null || true
rm -rf "$DB_PATH"
mkdir -p "$DB_PATH"

# デバッグログのリセット
rm -f /tmp/my_debug.log
touch /tmp/my_debug.log
chmod 777 /tmp/my_debug.log

# 起動 (監視スレッドもここで走り始めます)
"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" --port "$PORT" --bind_ip 127.0.0.1 --wiredTigerCacheSizeGB 1
sleep 5

if ! pgrep -f "mongod.*$PORT" > /dev/null; then
    echo "❌ ERROR: mongod failed to start."
    cat "$LOG_PATH"
    exit 1
fi
echo "✅ mongod started. Monitor thread should be polling port 0x5004."

# ==========================================
# 3. データ挿入 (キャッシュ温め)
# ==========================================
echo "--- [Step 2] Inserting 500MB Data... ---"
mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  const bulk = db.my_table.initializeUnorderedBulkOp();
  
  // 1件あたり約 10KB (10240 bytes) のパディング
  const padding = 'A'.repeat(10240); 

  for (let i = 0; i < $DOC_COUNT; i++) {
      bulk.insert({ 
          _id: i, 
          val: padding
      });
      
      // 1000件ごとに進捗表示 (フリーズ防止)
      if (i % 10000 === 0) {
          print('Inserted ' + i + ' documents...');
      }
  }
  print('Executing bulk insert...');
  bulk.execute();
  print('Insert complete: ' + $DOC_COUNT + ' docs (~500MB).');
"
# 統計確認: 挿入後
check_cache_stats "データ挿入後 (High Cache Usage)"

# ==========================================
# 4. 待機モード
# ==========================================
echo ""
echo "🚀 READY FOR MIGRATION!"
echo "-----------------------------------------------------"
echo "MongoDB is now running and polling QEMU port 0x5004."
echo "Please start migration from QEMU monitor (Host)."
echo "Tail of debug log (/tmp/my_debug.log):"
echo "-----------------------------------------------------"

# ログを流し続けて、QEMUからのトリガーが来た瞬間を見えるようにする
tail -f /tmp/my_debug.log