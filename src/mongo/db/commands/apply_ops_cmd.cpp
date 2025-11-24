/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/util/bson_check.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/auth/authorization_session.h"  // IWYU pragma: keep
#include "mongo/db/catalog/document_validation.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/oplog_application_checks.h"
#include "mongo/db/database_name.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/apply_ops.h"
#include "mongo/db/repl/apply_ops_command_info.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/db/service_context.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"
//TCMalloc拡張機能
#include "third_party/tcmalloc/dist/tcmalloc/malloc_extension.h"
//#include "cache_clear_and_reconstruct.cpp"
#include <fstream>
#include <iostream>
#include <wiredtiger.h>
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h" 
#include "mongo/db/storage/wiredtiger/wiredtiger_connection.h"
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand
#include <cstddef>
#include <stack>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>

namespace mongo {
namespace {

bool checkCOperationType(const BSONObj& opObj, const StringData opName) {
    BSONElement opTypeElem = opObj["op"];
    checkBSONType(BSONType::string, opTypeElem);
    const StringData opType = opTypeElem.checkAndGetStringData();

    if (opType == "c"_sd) {
        BSONElement oElem = opObj["o"];
        checkBSONType(BSONType::object, oElem);
        BSONObj o = oElem.Obj();

        if (o.firstElement().fieldNameStringData() == opName) {
            return true;
        }
    }
    return false;
};

/**
 * Returns kNeedsSuperuser, if the provided applyOps command contains an empty applyOps command or
 * createCollection/renameCollection commands are mixed in applyOps batch.
 *
 * Returns kNeedForceAndUseUUID if an operation contains a UUID, and will create a collection with
 * the user-specified UUID.
 *
 * Returns kNeedsUseUUID if the operation contains a UUID.
 *
 * Returns kOk if no conditions which must be specially handled are detected.
 *
 * May throw exceptions if the input is malformed.
 */
OplogApplicationValidity validateApplyOpsCommand(const BSONObj& cmdObj) {
    const size_t maxApplyOpsDepth = 10;
    std::stack<std::pair<size_t, BSONObj>> toCheck;

    auto operationContainsUUID = [](const BSONObj& opObj) {
        auto anyTopLevelElementIsUUID = [](const BSONObj& opObj) {
            for (const BSONElement& opElement : opObj) {
                if (opElement.type() == BSONType::binData &&
                    opElement.binDataType() == BinDataType::newUUID) {
                    return true;
                }
            }
            return false;
        };
        if (anyTopLevelElementIsUUID(opObj)) {
            return true;
        }

        BSONElement opTypeElem = opObj["op"];
        checkBSONType(BSONType::string, opTypeElem);
        const StringData opType = opTypeElem.checkAndGetStringData();

        if (opType == "c"_sd) {
            BSONElement oElem = opObj["o"];
            checkBSONType(BSONType::object, oElem);
            BSONObj o = oElem.Obj();

            if (anyTopLevelElementIsUUID(o)) {
                return true;
            }
        }

        return false;
    };

    OplogApplicationValidity ret = OplogApplicationValidity::kOk;
    auto demandAuthorization = [&ret](OplogApplicationValidity oplogApplicationValidity) {
        // Uses the fact that OplogApplicationValidity is ordered by increasing requirements
        ret = oplogApplicationValidity > ret ? oplogApplicationValidity : ret;
    };

    // Insert the top level applyOps command into the stack.
    toCheck.emplace(std::make_pair(0, cmdObj));

    while (!toCheck.empty()) {
        size_t depth;
        BSONObj applyOpsObj;
        std::tie(depth, applyOpsObj) = toCheck.top();
        toCheck.pop();

        checkBSONType(BSONType::array, applyOpsObj.firstElement());
        // Check if the applyOps command is empty. This is probably not something that should
        // happen, so require a superuser to do this.
        if (applyOpsObj.firstElement().Array().empty()) {
            // TODO(SERVER-96657): Do not return here, as it skips the remaining validations
            return OplogApplicationValidity::kNeedsSuperuser;
        }

        // createCollection and renameCollection are only allowed to be applied
        // individually. Ensure there is no create/renameCollection in a batch
        // of size greater than 1.
        if (applyOpsObj.firstElement().Array().size() > 1) {
            for (const BSONElement& e : applyOpsObj.firstElement().Array()) {
                checkBSONType(BSONType::object, e);
                auto oplogEntry = e.Obj();
                if (checkCOperationType(oplogEntry, "create"_sd) ||
                    checkCOperationType(oplogEntry, "renameCollection"_sd)) {
                    // TODO(SERVER-96657): Do not return here, as it skips the remaining validations
                    return OplogApplicationValidity::kNeedsSuperuser;
                }
            }
        }

        // For each applyOps command, iterate the ops.
        for (BSONElement element : applyOpsObj.firstElement().Array()) {
            checkBSONType(BSONType::object, element);
            BSONObj opObj = element.Obj();

            // Applying an entry with using a given FCV requires superuser privileges,
            // as it may create inconsistencies if it doesn't match the current FCV.
            if (opObj.hasField(repl::OplogEntryBase::kVersionContextFieldName)) {
                uassert(
                    10296501, "versionContext is not allowed inside nested applyOps", depth == 0);
                demandAuthorization(OplogApplicationValidity::kNeedsSuperuser);
            }

            bool opHasUUIDs = operationContainsUUID(opObj);

            // If the op uses any UUIDs at all then the user must possess extra privileges.
            if (opHasUUIDs) {
                demandAuthorization(OplogApplicationValidity::kNeedsUseUUID);
            }
            if (opHasUUIDs && checkCOperationType(opObj, "create"_sd)) {
                // If the op is 'c' and forces the server to ingest a collection
                // with a specific, user defined UUID.
                demandAuthorization(OplogApplicationValidity::kNeedsForceAndUseUUID);
            }

            if (checkCOperationType(opObj, "dropDatabase"_sd)) {
                // dropDatabase is not allowed to run inside a nested applyOps command.
                // Typically applyOps takes the global write lock, but dropDatabase requires the
                // lock not to be taken. We allow it on a top-level applyOps as a special case,
                // but running it inside a nested applyOps is non-trivial and does not fulfill any
                // use case, so we disallow it and return an error instead.
                uassert(9585500, "dropDatabase is not allowed inside nested applyOps", depth == 0);
            }

            // If the op contains a nested applyOps...
            if (checkCOperationType(opObj, "applyOps"_sd)) {
                // And we've recursed too far, then bail out.
                uassert(ErrorCodes::FailedToParse,
                        "Too many nested applyOps",
                        depth < maxApplyOpsDepth);

                // Otherwise, if the op contains an applyOps, but we haven't recursed too far:
                // extract the applyOps command, and insert it into the stack.
                checkBSONType(BSONType::object, opObj["o"]);
                BSONObj oObj = opObj["o"].Obj();
                toCheck.emplace(std::make_pair(depth + 1, std::move(oObj)));
            }
        }
    }

    return ret;
}

class ApplyOpsCmd : public BasicCommand {
public:
    ApplyOpsCmd() : BasicCommand("applyOps") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    std::string help() const override {
        return "internal command to apply oplog entries\n{ applyOps : [ ] }";
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const DatabaseName& dbName,
                                 const BSONObj& cmdObj) const override {
        OplogApplicationValidity validity = validateApplyOpsCommand(cmdObj);
        return OplogApplicationChecks::checkAuthForOperation(opCtx, dbName, cmdObj, validity);
    }

    bool run(OperationContext* opCtx,
             const DatabaseName& dbName,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        validateApplyOpsCommand(cmdObj);

        boost::optional<DisableDocumentValidation> maybeDisableValidation;
        if (shouldBypassDocumentValidationForCommand(cmdObj))
            maybeDisableValidation.emplace(opCtx);

        auto status = OplogApplicationChecks::checkOperationArray(cmdObj.firstElement());
        uassertStatusOK(status);

        // TODO (SERVER-30217): When a write concern is provided to the applyOps command, we
        // normally wait on the OpTime of whichever operation successfully completed last. This is
        // erroneous, however, if the last operation in the array happens to be a write no-op and
        // thus isn’t assigned an OpTime. Let the second to last operation in the applyOps be write
        // A, the last operation in applyOps be write B. Let B do a no-op write and let the
        // operation that caused B to be a no-op be C. If C has an OpTime after A but before B,
        // then we won’t wait for C to be replicated and it could be rolled back, even though B
        // was acknowledged. To fix this, we should wait for replication of the node’s last applied
        // OpTime if the last write operation was a no-op write.

        // We set the OplogApplication::Mode argument based on the mode argument given in the
        // command object. If no mode is given, default to the 'kApplyOpsCmd' mode.
        repl::OplogApplication::Mode oplogApplicationMode =
            repl::OplogApplication::Mode::kApplyOpsCmd;  // the default mode.
        std::string oplogApplicationModeString;
        status = bsonExtractStringField(
            cmdObj, repl::ApplyOps::kOplogApplicationModeFieldName, &oplogApplicationModeString);

        if (status.isOK()) {
            auto modeSW = repl::OplogApplication::parseMode(oplogApplicationModeString);
            if (!modeSW.isOK()) {
                // Unable to parse the mode argument.
                uassertStatusOK(modeSW.getStatus().withContext(
                    str::stream() << "Could not parse " +
                        repl::ApplyOps::kOplogApplicationModeFieldName));
            }
            oplogApplicationMode = modeSW.getValue();
        } else if (status != ErrorCodes::NoSuchKey) {
            // NoSuchKey means the user did not supply a mode.
            uassertStatusOK(status.withContext(str::stream()
                                               << "Could not parse out "
                                               << repl::ApplyOps::kOplogApplicationModeFieldName));
        }

        OperationShardingState::ScopedAllowImplicitCollectionCreate_UNSAFE unsafeCreateCollection(
            opCtx);

        auto applyOpsStatus = CommandHelpers::appendCommandStatusNoThrow(
            result, repl::applyOps(opCtx, dbName, cmdObj, oplogApplicationMode, &result));

        return applyOpsStatus;
    }
};
MONGO_REGISTER_COMMAND(ApplyOpsCmd).forShard();

}  // namespace
// 必要なヘッダを明示的にインクルード
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/storage/kv/kv_engine.h" 
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h" 
#include "mongo/db/storage/wiredtiger/wiredtiger_connection.h"
#include <wiredtiger.h>

// ヘルパー関数 (無名名前空間で隠蔽)
namespace {
    WT_CONNECTION* getWiredTigerConnection_Custom(OperationContext* opCtx) {
        // 1. StorageEngine (ラッパー) を取得
        StorageEngine* storageEngine = opCtx->getServiceContext()->getStorageEngine();
        if (!storageEngine) return nullptr;
        
        // 2. KVEngine (実体) を取得
        // StorageEngine は KVEngine を持っている構造なので、getEngine() で取り出します
        KVEngine* kvEngine = storageEngine->getEngine();
        if (!kvEngine) return nullptr;

        // 3. WiredTigerKVEngine にダウンキャスト
        // KVEngine は WiredTigerKVEngine の親クラスなので static_cast が通ります
        auto wtEngine = static_cast<WiredTigerKVEngine*>(kvEngine);
        if (!wtEngine) return nullptr;
        
        // 4. 接続を取得 (参照からポインタを取り出す)
        auto& wrapper = wtEngine->getConnection();
        return wrapper.conn();
    }
}

// 起動確認用イニシャライザ
MONGO_INITIALIZER(RegisterCustomCacheCommands)(InitializerContext* context) {
    std::ofstream outfile("/tmp/mongo_custom_check.txt");
    if (outfile.is_open()) {
        outfile << "Custom Cache Commands are LINKED and RUNNING!" << std::endl;
        outfile.close();
    }
    std::cout << "\n[CUSTOM DEBUG] Custom Cache Commands Initialized (Correct Cast).\n" << std::endl;
}

// コマンド1: customClear
class CustomClearCacheCmd : public BasicCommand {
public:
    CustomClearCacheCmd() : BasicCommand("customClear") {}
    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override { return AllowedOnSecondary::kNever; }
    Status checkAuthForOperation(OperationContext*, const DatabaseName&, const BSONObj&) const override { return Status::OK(); }
    bool supportsWriteConcern(const BSONObj&) const override { return false; }
    bool adminOnly() const override { return true; }
    bool run(OperationContext* opCtx, const DatabaseName&, const BSONObj&, BSONObjBuilder& result) override {
        
        // ★修正した関数を使用
        WT_CONNECTION* conn = getWiredTigerConnection_Custom(opCtx);
        
        if (!conn) {
            result.append("ok", 0.0);
            result.append("errmsg", "Failed to get WiredTiger connection (getEngine failed)");
            return false;
        }

        // C関数呼び出し
        int ret = wt_clear_cache(conn);
        
        // キャッシュクリア成功後にメモリを強制解放
        // ▼▼▼ 修正: TCMalloc 統計をファイルに出力してメモリ解放 ▼▼▼
        /*
        if (ret == 0) {
            // 出力先ファイルを開く (追記モード)
            std::ofstream logFile("/tmp/mongo_migration_test/tcmalloc_stats.log", std::ios::app);
            
            if (logFile.is_open()) {
                logFile << "=== [CustomClear] Start Memory Release ===" << std::endl;

                // 1. 解放前の統計を取得・出力
                // (GetStatsの戻り値が std::string である前提)
                std::string statsBefore = tcmalloc::MallocExtension::GetStats();
                logFile << "--- Stats BEFORE Release ---" << std::endl;
                logFile << statsBefore << std::endl;

                // 2. メモリ解放を実行
                logFile << ">>> Calling ReleaseMemoryToSystem(-1)..." << std::endl;
                size_t max_bytes = static_cast<size_t>(-1);
                tcmalloc::MallocExtension::ReleaseMemoryToSystem(max_bytes);

                // 3. 解放後の統計を取得・出力
                std::string statsAfter = tcmalloc::MallocExtension::GetStats();
                logFile << "--- Stats AFTER Release ---" << std::endl;
                logFile << statsAfter << std::endl;
                
                logFile << "==========================================" << std::endl << std::endl;
                logFile.close();
            } else {
                // ファイルが開けなかった場合は標準ログに出す（バックアップ）
                LOGV2(0, "Failed to open log file /tmp/mongo_migration_test/tcmalloc_stats.log");
            }
        }
        */
        // ▲▲▲ 修正終了 ▲▲▲

        if (ret == 0) {
            result.append("msg", "Successfully executed wt_clear_cache");
            return true;
        } else {
            result.append("ok", 0.0);
            result.append("errmsg", wiredtiger_strerror(ret));
            return false;
        }
    }
};
MONGO_REGISTER_COMMAND(CustomClearCacheCmd).forShard();

// コマンド2: customReconstruct
class CustomReconstructCacheCmd : public BasicCommand {
public:
    CustomReconstructCacheCmd() : BasicCommand("customReconstruct") {}
    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override { return AllowedOnSecondary::kNever; }
    Status checkAuthForOperation(OperationContext*, const DatabaseName&, const BSONObj&) const override { return Status::OK(); }
    bool supportsWriteConcern(const BSONObj&) const override { return false; }
    bool adminOnly() const override { return true; }
    bool run(OperationContext* opCtx, const DatabaseName&, const BSONObj&, BSONObjBuilder& result) override {
        
        WT_CONNECTION* conn = getWiredTigerConnection_Custom(opCtx);
        
        if (!conn) {
            result.append("ok", 0.0);
            result.append("errmsg", "Failed to get WiredTiger connection (getEngine failed)");
            return false;
        }

        int ret = wt_reconstruct_cache(conn);

        if (ret == 0) {
            result.append("msg", "Successfully executed wt_reconstruct_cache");
            return true;
        } else {
            result.append("ok", 0.0);
            result.append("errmsg", wiredtiger_strerror(ret));
            return false;
        }
    }
};
MONGO_REGISTER_COMMAND(CustomReconstructCacheCmd).forShard();

}  // namespace mongo
