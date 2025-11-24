#!/bin/bash

# ========================================================
# MongoDB Custom Cache Verification Script
# ========================================================

set -e # エラーが発生したら即停止

# --- Root権限チェック ---
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: This script must be run as root (sudo) for iopl/pagemap access."
  exit 1
fi

# ==========================================
# 1. 設定 (環境に合わせて変更してください)
# ==========================================
BASE_DIR=$(dirname "$0")

# ★ ビルドしたバイナリのパス
MONGOD_BINARY="/home/mitsuki/mongo/bazel-bin/install-dist-test/bin/mongod"
# VM上で実行する場合
# MONGOD_BINARY="/home/mitsuki/mongod"

# テスト用DBとログの場所
DB_PATH="/tmp/mongo_single_session_test_$(date +%s)"
LOG_PATH="$DB_PATH/mongod.log"
PORT=27017

# テストデータの件数
DOC_COUNT=10000

echo "=================================================="
echo "   MongoDB Custom Cache Verification (Single Session)   "
echo "=================================================="
echo "DB Path: $DB_PATH"
echo "Log Path: $LOG_PATH"

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
# 2. 初期化と起動
# ==========================================
echo "--- [Step 0] 環境初期化 ---"
rm -rf "$DB_PATH"
mkdir -p "$DB_PATH"

# C言語側(my_printf)用のログファイルを初期化
# sudo で実行しているため、権限を777にして誰でも書き込めるようにする
sudo rm -f /tmp/my_debug.log
sudo touch /tmp/my_debug.log
sudo chmod 777 /tmp/my_debug.log

echo "--- [Step 1] Custom mongod を起動 ---"
"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" --port "$PORT" --bind_ip 127.0.0.1
sleep 5

# 起動確認
if ! pgrep -f "mongod.*$PORT" > /dev/null; then
    echo "❌ ERROR: mongod の起動に失敗しました。ログ: $LOG_PATH"
    exit 1
fi
echo "✅ mongod started (PID: $(pgrep -f "mongod.*$PORT"))"

# ==========================================
# 3. データ挿入 (キャッシュ温め)
# ==========================================
echo "--- [Step 2] データ挿入 (キャッシュを温める) ---"
mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  db.my_table.drop();
  const bulk = db.my_table.initializeUnorderedBulkOp();
  // データを詰め込む
  for (let i = 0; i < $DOC_COUNT; i++) {
      bulk.insert({ 
          _id: i, 
          key: 'key_' + i, 
          val: 'value_' + i,
          // キャッシュを消費させるためのパディング
          pad: 'A'.repeat(2048) 
      });
  }
  bulk.execute();
  print('Insert complete: $DOC_COUNT documents.');
"

# 統計確認: 挿入後
check_cache_stats "データ挿入直後 (High Cache Usage)"

# ==========================================
# 4. カスタムキャッシュクリア (wt_clear_cache)
# ==========================================
echo "--- [Step 3] カスタムキャッシュクリアの実行 (customClear) ---"

mongosh --quiet --port "$PORT" --eval "
  print('Executing customClear command...');
  const res = db.adminCommand({ customClear: 1 }); 
  printjson(res);
  
  if (res.ok !== 1) {
      print('❌ Command Failed');
      quit(1);
  }
"

sleep 2
check_cache_stats "キャッシュクリア後 (Low Cache Usage)"

# ==========================================
# 5. カスタムキャッシュ復元 (wt_reconstruct_cache)
# ==========================================
echo "--- [Step 4] カスタムキャッシュ復元の実行 (customReconstruct) ---"

mongosh --quiet --port "$PORT" --eval "
  print('Executing customReconstruct command...');
  const res = db.adminCommand({ customReconstruct: 1 });
  printjson(res);

  if (res.ok !== 1) {
      print('❌ Command Failed');
      quit(1);
  }
"

sleep 5
check_cache_stats "キャッシュ復元後 (Restored Usage)"

# ==========================================
# 6. データ整合性の最終検証
# ==========================================
echo "--- [Step 5] データ整合性検証 ---"

mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  const count = db.my_table.countDocuments();
  
  print('Documents in DB: ' + count);
  
  if (count === $DOC_COUNT) {
      const sample = db.my_table.findOne({_id: 0});
      if(sample && sample.key === 'key_0') {
          print('✅ VALIDATION PASSED');
          quit(0);
      } else {
          print('❌ VALIDATION FAILED: Data corruption detected');
          quit(1);
      }
  } else {
      print('❌ VALIDATION FAILED: Count mismatch (Expected $DOC_COUNT)');
      quit(1);
  }
"

if [ $? -eq 0 ]; then
    RESULT="PASSED"
else
    RESULT="FAILED"
fi

# ==========================================
# 7. ログ抽出 & クリーンアップ
# ==========================================
echo "--- [Step 6] ログ確認 & クリーンアップ ---"

# ★★★ ここでカスタムログを表示 ★★★
echo ""
echo "🔍 Checking for [MYDEBUG] logs in /tmp/my_debug.log ..."
if [ -f "/tmp/my_debug.log" ]; then
    cat /tmp/my_debug.log
else
    echo "No custom log file found."
fi
echo ""

# サーバー停止
mongosh --quiet --port "$PORT" --eval "db.getSiblingDB('admin').shutdownServer()" 2>/dev/null || true
sleep 3

# ディレクトリ削除
rm -rf "$DB_PATH"

echo "======================================"
echo "TEST RESULT: $RESULT"
echo "======================================"

if [ "$RESULT" = "FAILED" ]; then
    exit 1
fi