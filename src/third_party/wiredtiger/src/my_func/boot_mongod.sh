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
DOC_COUNT=1
PADDING_SIZE=10240 

echo "=================================================="
echo "      MongoDB Data Insertion (Cache Warmer)       "
echo "=================================================="
echo "Target Port: $PORT"
echo "DB Path:     $DB_PATH"
echo "Binary:      $MONGOD_BINARY"
echo "--------------------------------------------------"

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

echo ""
echo "=================================================="
echo "✅ DATA INSERTION COMPLETE"
echo "=================================================="
echo "MongoDBはポート $PORT で起動中です。"
echo "キャッシュにデータが充填されました。"
echo "次にキャッシュクリア用スクリプトを実行するか、手動でコマンドを試してください。"