// live_migration_command.h

#pragma once

#include "mongo/db/commands/command.h"

namespace mongo {
namespace commands {

/**
 * @brief ライブマイグレーションの準備として、WiredTigerキャッシュを強制Evictするコマンド。
 */
class LiveMigrationCommand : public Command {
public:
    LiveMigratePrepareCommand();

    // コマンドの実行ロジック
    // 'BSONObj' はコマンドのパラメータ（例： Evictのオプションなど）
    // 'OperationContext' は現在の操作コンテキスト
    virtual Status doInternalImplementation(
        OperationContext* opCtx,
        const BSONObj& cmdObj,
        BSONObj* result) override;

    // コマンドの権限設定
    virtual bool requiresAuth() const override { return true; } // 管理者権限が必要
};

} // namespace commands
} // namespace mongo