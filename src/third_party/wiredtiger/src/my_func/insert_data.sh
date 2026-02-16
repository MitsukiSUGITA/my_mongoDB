#!/bin/bash
set -e # エラーが発生したら即停止

# --- 引数で設定を受け取る (デフォルト値は以前の実験設定) ---
# 第1引数: MongoDBキャッシュサイズ (GB)
#CACHE_SIZE_GB=${1:-1} 
# 第2引数: ドキュメント数 (1件10KB計算)
# 例: 200,000件 ≒ 2GB, 2,500,000件 ≒ 25GB
#INPUT_DOC_COUNT=${2:-200000}
INPUT_DOC_COUNT=${1:-200000}

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
PADDING_SIZE=10240 

echo "=================================================="
echo "      MongoDB Data Insertion (Cache Warmer)       "
echo "=================================================="
#echo "Cache Size:  ${CACHE_SIZE_GB} GB"
echo "Doc Count:   ${INPUT_DOC_COUNT}"
echo "Approx Data: $(( INPUT_DOC_COUNT * PADDING_SIZE / 1024 / 1024 )) MB"
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
          
          // 基本的なサイズ情報
          const bytes = status['bytes currently in the cache'];
          const max_bytes = status['maximum bytes configured'];
          const dirty_bytes = status['tracked dirty bytes in the cache'];
          
          // ページ数情報
          const pages = status['pages currently held in the cache'];
          const dirty_pages = status['tracked dirty pages in the cache'];
          
          // 計算（MB/GB変換とパーセンテージ）
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
  --syncdelay 3600 \
  --wiredTigerEngineConfigString "checkpoint=(wait=3600),eviction_dirty_target=90,eviction_dirty_trigger=95,eviction_target=95,eviction_trigger=99"
  #  --wiredTigerEngineConfigString "checkpoint=(wait=3600),eviction_dirty_target=90,eviction_dirty_trigger=95,eviction_target=95,eviction_trigger=99"
#  --wiredTigerCacheSizeGB "$CACHE_SIZE_GB"

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
#echo "--- [Step 2] データ挿入 (約 $CACHE_SIZE_GB GB) ---"
mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  db.my_table.drop();
  const bulk = db.my_table.initializeUnorderedBulkOp();
  
  // QEMUでの検証用に 'A' (0x41) で埋める
  const padding = 'A'.repeat($PADDING_SIZE); 

  print('Preparing bulk insert...');
  for (let i = 0; i < $INPUT_DOC_COUNT; i++) {
      bulk.insert({ 
          _id: i, 
          val: padding 
      });
      // 進捗表示
      // if (i % 20000 == 0 && i > 0) print('  Prepared ' + i + ' documents...');
  }
  print('Executing bulk insert (this may take a while)...');
  bulk.execute();
  print('✅ Insert complete: $INPUT_DOC_COUNT documents.');

  print('🔄 Force-updating all documents to maximize Dirty Rate...');
  // 全ドキュメントのフラグを書き換える
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
echo "次にキャッシュクリア用スクリプトを実行するか、手動でコマンドを試してください。"