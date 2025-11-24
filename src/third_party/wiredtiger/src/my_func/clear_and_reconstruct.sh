#!/bin/bash
set -e # エラーが発生したら即停止

# ==========================================
# 1. 設定
# ==========================================
# VS Codeの launch.json で設定しているポート番号と一致させること
PORT=27017

# テストデータの件数
DOC_COUNT=10

echo "=================================================="
echo "   MongoDB Debug Trigger (Client Operations Only)   "
echo "=================================================="
echo "Target Port: $PORT"
echo "Make sure mongod is running in VS Code debugger (F5)!"
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
          print('(Is mongod running?)');
          quit(1);
      }
    "
    echo "------------------------------------------------"
}

# ==========================================
# 2. サーバー接続確認
# ==========================================
echo "--- [Step 1] サーバー接続確認 ---"
# mongodが起動しているかチェックする
if ! mongosh --quiet --port "$PORT" --eval "db.runCommand({ping:1})" > /dev/null 2>&1; then
    echo "❌ ERROR: Could not connect to mongod on port $PORT."
    echo "   Please start mongod in VS Code debugger first!"
    exit 1
fi
echo "✅ Connected to mongod."


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
          pad: 'x'.repeat(1024) 
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
echo "👉 VS Code でブレークポイントを設定している場合、ここで止まります！"

# ★ここであなたの wt_clear_cache を呼び出すコマンドを実行
mongosh --quiet --port "$PORT" --eval "
  print('Executing customClear command...');
  
  const res = db.adminCommand({ customClear: 1 }); 
  printjson(res);
  
  if (res.ok !== 1) {
      print('❌ Command Failed');
      quit(1);
  }
"

# 実行後の安定待ち
sleep 2

# 統計確認: クリア後 (サイズが減っていることを期待)
check_cache_stats "キャッシュクリア後 (Low Cache Usage)"

# ==========================================
# 5. カスタムキャッシュ復元 (wt_reconstruct_cache)
# ==========================================
echo "--- [Step 4] カスタムキャッシュ復元の実行 (customReconstruct) ---"
echo "👉 復元処理のデバッグをするなら、ここで止まります！"

# ★ここであなたの wt_reconstruct_cache を呼び出すコマンドを実行
mongosh --quiet --port "$PORT" --eval "
  print('Executing customReconstruct command...');
  
  const res = db.adminCommand({ customReconstruct: 1 });
  printjson(res);

  if (res.ok !== 1) {
      print('❌ Command Failed');
      quit(1);
  }
"

# 復元処理待ち
sleep 5

# 統計確認: 復元後 (サイズが増えていることを期待)
check_cache_stats "キャッシュ復元後 (Restored Usage)"

# ==========================================
# 6. データ整合性の最終検証
# ==========================================
echo "--- [Step 5] データ整合性検証 ---"

mongosh --quiet --port "$PORT" --eval "
  const db = db.getSiblingDB('test_db');
  const count = db.my_table.countDocuments();
  
  print('Documents in DB: ' + count);
  
  // 単純な件数チェック (必要に応じて中身のチェックを追加)
  if (count === $DOC_COUNT) {
      // 特定のデータが存在するかチェック
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
# 7. 終了（クリーンアップはしない）
# ==========================================
echo "--- [Step 6] テスト完了 ---"
echo "デバッグ中のため、mongod はシャットダウンしません。"
echo "VS Code の停止ボタン■ を押して終了してください。"

echo "======================================"
echo "TEST RESULT: $RESULT"
echo "======================================"

if [ "$RESULT" = "FAILED" ]; then
    exit 1
fi