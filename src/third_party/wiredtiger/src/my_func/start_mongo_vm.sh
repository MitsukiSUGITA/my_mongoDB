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
# VM内のmongodへのパス (適宜変更してください)
MONGOD_BINARY="./mongod"
# テスト用DBとログの場所
DB_PATH="/tmp/mongo_migration_test"
LOG_PATH="$DB_PATH/mongod.log"
PORT=27017

echo "=================================================="
echo "      MongoDB Environment Reset & Startup         "
echo "=================================================="

# ==========================================
# 2. 環境リセット
# ==========================================
echo "--- [Step 1] 環境リセット ---"
# 古いプロセスを停止
killall -9 mongod 2>/dev/null || true

# 古いディレクトリを削除して再作成
rm -rf "$DB_PATH"
mkdir -p "$DB_PATH"

echo "古いデータを削除し、環境をリセットしました。"

# ==========================================
# 3. MongoDB起動
# ==========================================
echo "--- [Step 2] mongodの起動 ---"
# 注意: 外部(YCSB)からアクセスさせるため bind_ip を 0.0.0.0 に変更
"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" \
  --port "$PORT" --bind_ip 0.0.0.0 \
  --syncdelay 3600 \
  --wiredTigerEngineConfigString "checkpoint=(wait=3600),eviction_dirty_target=90,eviction_dirty_trigger=95,eviction_target=95,eviction_trigger=99"

sleep 5

# 起動確認
if ! pgrep -f "mongod.*$PORT" > /dev/null; then
    echo "❌ ERROR: mongod failed to start."
    echo "--- Tail of mongod.log ---"
    tail -n 20 "$LOG_PATH"
    exit 1
fi

echo "✅ mongod started successfully (PID: $(pgrep -f "mongod.*$PORT"))"
echo "YCSBからの接続を待機しています..."