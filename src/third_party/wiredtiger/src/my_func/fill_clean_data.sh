#!/bin/bash
set -e

if [ -z "$1" ]; then
  echo "❌ Error: VMのメモリサイズ(GB)を指定してください。"
  exit 1
fi

VM_MEMORY_GB=$1
NUM_THREADS=$(nproc) # CPUのコア数に合わせて並列実行

if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: sudoで実行してください。"
  exit 1
fi

# ==========================================
# 1. パラメータ計算
# ==========================================
MONGOD_BINARY="./mongod"
DB_PATH="/tmp/mongo_migration_test" # 高速化したい場合は /dev/shm/mongo_test 等に変更
LOG_PATH="$DB_PATH/mongod.log"
PORT=27017
PADDING_SIZE=10240 

WT_CACHE_MB=$(( VM_MEMORY_GB * 1024 * 10 / 7))
[ "$WT_CACHE_MB" -lt 256 ] && WT_CACHE_MB=256

TARGET_BYTES=$(( WT_CACHE_MB * 1024 * 1024 ))
# 余裕を持って110%充填
BUFFERED_BYTES=$(( TARGET_BYTES * 110 / 100 ))
TOTAL_DOC_COUNT=$(( BUFFERED_BYTES / PADDING_SIZE ))
DOCS_PER_THREAD=$(( TOTAL_DOC_COUNT / NUM_THREADS ))

echo "🚀 並列度: $NUM_THREADS で実行します ($TOTAL_DOC_COUNT ドキュメント)"

# ==========================================
# 2. 起動設定
# ==========================================
killall -9 mongod 2>/dev/null || true
rm -rf "$DB_PATH" && mkdir -p "$DB_PATH"
sync; echo 3 > /proc/sys/vm/drop_caches

# 起動オプションに eviction設定を維持
"$MONGOD_BINARY" --fork --dbpath "$DB_PATH" --logpath "$LOG_PATH" \
  --port "$PORT" --bind_ip 127.0.0.1 \
  --syncdelay 3600 \
  --wiredTigerEngineConfigString "mmap=false,checkpoint=(wait=3600),eviction_target=98,eviction_trigger=99"

sleep 3

# ==========================================
# 3. 並列データ挿入 (進捗表示をクリーンに)
# ==========================================
echo "--- [Step 2] 並列データ挿入開始 ---"
echo "スレッド数: $NUM_THREADS / 総ドキュメント数: $TOTAL_DOC_COUNT"

for i in $(seq 1 "$NUM_THREADS"); do
    START_ID=$(( (i - 1) * DOCS_PER_THREAD ))
    [ "$i" -eq "$NUM_THREADS" ] && DOCS_PER_THREAD=$(( TOTAL_DOC_COUNT - START_ID ))

    # mongosh 内の出力をすべて /dev/null に捨て、終了時に echo を出す
    (
      mongosh --quiet --port "$PORT" --eval "
        const db = db.getSiblingDB('test_db');
        const padding = 'A'.repeat($PADDING_SIZE);
        let bulk = db.my_table.initializeUnorderedBulkOp();
        for (let j = 0; j < $DOCS_PER_THREAD; j++) {
            bulk.insert({ _id: ($START_ID + j), val: padding });
            if ((j + 1) % 10000 === 0) {
                bulk.execute();
                bulk = db.my_table.initializeUnorderedBulkOp();
            }
        }
        bulk.execute();
      " > /dev/null 2>&1
      echo "  [Thread $i] 挿入完了 (ID: $START_ID ～)"
    ) & 
done

wait
echo "✅ 全スレッドのデータ挿入が完了しました。"

# ==========================================
# 4. クリーン化 (fsync)
# ==========================================
echo "💾 ダーティページの書き出し中 (fsync)..."
mongosh --quiet --port "$PORT" --eval "db.adminCommand({ fsync: 1 })" > /dev/null 2>&1
echo "✅ fsync 完了"

# ==========================================
# 5. 並列ウォームアップ (進捗表示をクリーンに)
# ==========================================
echo "🔍 並列フルスキャンでキャッシュ充填中..."
for i in $(seq 1 "$NUM_THREADS"); do
    START_ID=$(( (i - 1) * DOCS_PER_THREAD ))
    END_ID=$(( i * DOCS_PER_THREAD ))
    
    (
      mongosh --quiet --port "$PORT" --eval "
        db.getSiblingDB('test_db').my_table.find({_id: {\$gte: $START_ID, \$lt: $END_ID}}).itcount();
      " > /dev/null 2>&1
      echo "  [Thread $i] スキャン完了"
    ) &
done

wait
echo "✅ 全スレッドのウォームアップが完了しました。"

# 最終統計表示
mongosh --quiet --port "$PORT" --eval "
  const status = db.serverStatus().wiredTiger.cache;
  print('\n📊 Final Stats:');
  print('  Usage: ' + (status['bytes currently in the cache'] / (1024**3)).toFixed(2) + ' GB');
  print('  Dirty: ' + (status['tracked dirty bytes in the cache'] / (1024**2)).toFixed(2) + ' MB');
"