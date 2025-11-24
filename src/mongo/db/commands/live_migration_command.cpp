// live_migrate_prepare_command.cpp

#include "mongo/db/commands/live_migration_command.h"
#include "mongo/db/commands/command.h"
#include "mongo/db/commands/collection_command.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h" // B.で追加するヘッダ

namespace mongo {
namespace commands {

LiveMigrationCommand::LiveMigrationCommand()
    : Command("liveMigration", /* Authorization::kGlobal */ true, {
        Authorization::kWrite
    }) {}

Status LiveMigrationCommand::doInternalImplementation(
    OperationContext* opCtx,
    const BSONObj& cmdObj,
    BSONObj* result) {

    // 1. StorageEngineのインスタンスを取得
    StorageEngine* storageEngine = StorageEngine::getGlobal();
    
    // 2. WiredTigerエンジンであることを確認
    // ここで、WiredTiger固有のメソッドを呼び出すためにキャストを行います。
    WiredTigerKVEngine* wtEngine = dynamic_cast<WiredTigerKVEngine*>(storageEngine);

    if (!wtEngine) {
        return Status(ErrorCodes::InternalError, "WiredTiger engine not found.");
    }

    // 3. ✨ B.で実装する Eviction 関数を呼び出す！ ✨
    // (これは、次のステップ B. で `wiredtiger_kv_engine.cpp` に追加する関数名です)
    Status evictStatus = wtEngine->forceEvictCachePages(opCtx);

    if (!evictStatus.isOK()) {
        return evictStatus;
    }
    
    // 成功メッセージを結果に追加
    *result = BSON("ok" << 1);

    return Status::OK();
}

} // namespace commands
} // namespace mongo