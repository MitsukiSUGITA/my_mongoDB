#!/bin/bash
# verify_restore.sh
# 移送先で実行し、キャッシュが復元されたか確認する

PORT=27017

echo "========================================="
echo "   MongoDB Cache Restoration Check       "
echo "========================================="

# ログの確認
echo "--- [1] Log Check ---"
grep "MONITOR" /tmp/mongo_migration_test/my_debug.log | tail -n 5

# キャッシュ統計の確認
echo ""
echo "--- [2] Cache Stats Check ---"
mongosh --quiet --port "$PORT" --eval "
  try {
      const status = db.serverStatus().wiredTiger.cache;
      const bytes = status['bytes currently in the cache'];
      const mb = (bytes / (1024 * 1024)).toFixed(2);
      const pages = status['pages currently held in the cache'];
      
      print('Current Cache Size: ' + mb + ' MB');
      print('Total Pages:        ' + pages);
      
      // insert_data.shの設定だと約1GB (1000MB) 程度になるはず
      if (bytes > 500 * 1024 * 1024) {
          print('✅ SUCCESS: Cache is warm (Reconstruction worked!)');
      } else {
          print('❌ FAILURE: Cache is cold (Reconstruction failed or not finished)');
      }
  } catch(e) {
      print('Error: ' + e);
  }
"