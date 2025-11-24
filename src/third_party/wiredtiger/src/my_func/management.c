/**
 * @file clear_and_reconstruct.c
 * @brief キャッシュのクリアと再構成（プリウォーミング）を実演する統合プログラム
 *
 * このプログラムは、あなたの最終目標である処理を一つの流れで実行します。
 * 1. データベースを作成し、データを書き込み、キャッシュが温まった状態を作ります。
 * 2. conn->reconfigure() を使い、キャッシュを強制的にクリアします。
 * 3. metadata:カーソルから全テーブルリストを取得し、それらを全件スキャンすることで
 * キャッシュを能動的に再構成（プリウォーミング）します。
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wiredtiger.h>
#include <sys/stat.h> // stat関数を使うために追加
#include <management.h>
//#include "management.h"

#define error_check(call)                                       \
    do {                                                        \
        int res = call;                                         \
        if (res != 0) {                                         \
            fprintf(stderr, "%s:%d: %s: %s\n", __FILE__,         \
                __LINE__, #call, wiredtiger_strerror(res));     \
            return (res);                                       \
        }                                                       \
    } while (0)

// データベースを新規作成する関数。第１引数にDB名、第２関数にキャッシュサイズ（例：512M）
int create_db(const char *dbname, const char *param, WT_CONNECTION **conn)
{
    char command[256];
    char config_str[256];
    snprintf(command, sizeof(command), "rm -rf %s && mkdir %s", dbname, dbname);
    if (system(command) != 0) {
        fprintf(stderr, "ホームディレクトリのクリーンアップに失敗しました。\n");
        return EXIT_FAILURE;
    }
    snprintf(config_str, sizeof(config_str), "create,%s", param);
    error_check(wiredtiger_open(dbname, NULL, config_str, conn));
    return EXIT_SUCCESS;
}

// 既存のデータベースを開く関数。第１引数にDB名、第２関数にキャッシュサイズ（例：512M）
int open_db(const char *dbname, const char *cache_size, WT_CONNECTION **conn)
{
    char config_str[256];
    snprintf(config_str, sizeof(config_str), "cache_size=%s", cache_size);
    error_check(wiredtiger_open(dbname, NULL, config_str, conn));
    return EXIT_SUCCESS;
}

// 既存のデータベースを削除する関数。
int remove_db(const char *dbname)
{
    char command[256];
    snprintf(command, sizeof(command), "rm -rf %s", dbname);
    if (system(command) != 0) {
        fprintf(stderr, "ホームディレクトリのクリーンアップに失敗しました。\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

// キャッシュ統計情報を表示する関数
/*
uint64_t get_cache(WT_SESSION *session, int param)
{
    WT_CURSOR *cursor;
    const char *desc, *pvalue;
    uint64_t value = 0;
    int ret;

    error_check(session->open_cursor(session, "statistics:", NULL, NULL, &cursor));

    cursor->set_key(cursor, param);
    ret = cursor->search(cursor);
    if (ret == 0) {
        error_check(cursor->get_value(cursor, &desc, &pvalue, &value));
    }
    cursor->reset(cursor);
    
    error_check(cursor->close(cursor));

    return value;
}
*/
uint64_t get_cache(WT_SESSION *session, int param)
{
    WT_CURSOR *cursor;
    const char *desc, *pvalue;
    uint64_t value = 0;
    int ret;

    ret = session->open_cursor(session, "statistics:", NULL, NULL, &cursor);
    if (ret != 0) {
        fprintf(stderr, "Error opening statistics cursor: %s\n", wiredtiger_strerror(ret));
        return (0); // エラー時は0を返す
    }

    cursor->set_key(cursor, param);
    ret = cursor->search(cursor);
    if (ret == 0) {
        ret = cursor->get_value(cursor, &desc, &pvalue, &value);
        if (ret != 0) {
            fprintf(stderr, "Error opening cursor: %s\n", wiredtiger_strerror(ret));
            return (0); // エラー時は0を返す
        }
    }
    cursor->reset(cursor);
    ret = cursor->close(cursor);
    if (ret != 0) {
        fprintf(stderr, "Error closing cursor: %s\n", wiredtiger_strerror(ret));
        return (0); // エラー時は0を返す
    }
    return value;
}

int insert_data(WT_SESSION *session, const char *table_name, int start_key, size_t target_size_mb)
{
    WT_CURSOR *cursor;
    char key_buf[64];
    // 一度に書き込むデータ量を増やすため、バッファを大きくする
    char value_buf[1024];
    int ret;

    // 目標サイズをMBからバイトに変換
    size_t target_size_bytes = target_size_mb * 1024 * 1024;
    size_t total_inserted_bytes = 0;
    long long records_inserted = 0;

    const size_t one_gb = 1024 * 1024 * 1024;
    size_t next_report_threshold_bytes = one_gb;
    //printf("Inserting data into '%s' until size exceeds %zu MB...\n", table_name, target_size_mb);

    // カーソルを開く
    ret = session->open_cursor(session, table_name, NULL, "overwrite=true", &cursor);
    if (ret != 0) {
        fprintf(stderr, "Error opening cursor: %s\n", wiredtiger_strerror(ret));
        return ret;
    }

    // サイズを早く満たすため、ある程度の大きさのダミーデータを用意する
    memset(value_buf, 'X', sizeof(value_buf) - 1);
    value_buf[sizeof(value_buf) - 1] = '\0';
    size_t value_len = strlen(value_buf);

    // 合計サイズが目標を超えるまでループ
    for (int i = 0; ; ++i) {
        int current_key = start_key + i;
        snprintf(key_buf, sizeof(key_buf), "key%010d", current_key);

        cursor->set_key(cursor, key_buf);
        cursor->set_value(cursor, value_buf);
        
        ret = cursor->insert(cursor);
        if (ret != 0) {
            fprintf(stderr, "Error inserting data: %s\n", wiredtiger_strerror(ret));
            cursor->close(cursor);
            return ret;
        }

        // 挿入したキーと値のサイズを合計に加算
        total_inserted_bytes += strlen(key_buf) + value_len;
        records_inserted++;

        // --- 追加: 1GBごとの経過出力 ---
        if (total_inserted_bytes >= next_report_threshold_bytes) {
            printf("    -> Progress: %.2f GB inserted...\n", (double)total_inserted_bytes / one_gb);
            next_report_threshold_bytes += one_gb; // 次の目標を1GB増やす
        }
        // 目標サイズに達したらループを抜ける
        if (total_inserted_bytes >= target_size_bytes) {
            break;
        }
    }

    ret = cursor->close(cursor);
    if (ret != 0) {
        fprintf(stderr, "Error closing cursor: %s\n", wiredtiger_strerror(ret));
        return ret;
    }

    printf("Insertion complete. Inserted %lld records, total size: %.2f GB\n",
           records_inserted, (double)total_inserted_bytes / (1024 * 1024 * 1024));
           
    return 0;
}

int read_all_data(WT_SESSION *session, const char *table_name)
{
    WT_CURSOR *cursor;
    const char *key, *value;
    int ret, num = 0;
    long long records_read = 0;

    printf("\nReading all data from '%s'...\n", table_name);

    // 読み込み用にカーソルを開く
    ret = session->open_cursor(session, table_name, NULL, NULL, &cursor);
    if (ret != 0) {
        fprintf(stderr, "Error opening cursor for reading: %s\n", wiredtiger_strerror(ret));
        return ret;
    }

    // cursor->next() が 0 を返す間、次のレコードが存在する
    while ((ret = cursor->next(cursor)) == 0) {
        // 現在のカーソル位置にあるキーと値を取得
        if ((ret = cursor->get_key(cursor, &key)) != 0) {
            fprintf(stderr, "Error getting key: %s\n", wiredtiger_strerror(ret));
            break;
        }
        if ((ret = cursor->get_value(cursor, &value)) != 0) {
            fprintf(stderr, "Error getting value: %s\n", wiredtiger_strerror(ret));
            break;
        }

        // 読み込んだキーと値（の先頭15文字）を表示
        num++;
        if(num % 1000000 == 0) printf("  Key: %s, Value: %.15s...\n", key, value);
        records_read++;
    }

    // ループ終了の理由を確認
    // WT_NOTFOUND は正常な終了（全てのレコードを読み終えた）
    if (ret != WT_NOTFOUND) {
        fprintf(stderr, "Error during cursor traversal: %s\n", wiredtiger_strerror(ret));
    }

    printf("\nRead complete. Total records read: %lld\n", records_read);

    // カーソルを閉じる
    return cursor->close(cursor);
}

void print_cache(WT_SESSION *session){
    uint64_t val;

    printf("\n--- Cache Statistics ---\n");
    //current_usage = get_cache(session, WT_STAT_CONN_CACHE_BYTES_MAX);
    //fprintf(fp, "・設定されている最大キャッシュサイズ: %" PRIu64 " bytes (%.2f MB)\n", current_usage, (double)current_usage / (1024 * 1024));
    // --- 使用状況 ---
    val = get_cache(session, WT_STAT_CONN_CACHE_BYTES_INUSE);
    printf("・使用量: %" PRIu64 " bytes (%.2f GB)\n", val, (double)val / (1024 * 1024* 1024));
    val = get_cache(session, WT_STAT_CONN_CACHE_BYTES_DIRTY);
    printf("・ダーティデータ量: %" PRIu64 " bytes (%.2f GB)\n", val, (double)val / (1024 * 1024* 1024));
    // --- 累計I/O ---
    val = get_cache(session, WT_STAT_CONN_CACHE_BYTES_READ);
    printf("・読み込まれた累計データ量: %" PRIu64 " bytes (%.2f GB)\n", val, (double)val / (1024 * 1024* 1024));
    val = get_cache(session, WT_STAT_CONN_CACHE_BYTES_WRITE);
    printf("・ディスクに書き出された累計データ量: %" PRIu64 " bytes (%.2f GB)\n", val, (double)val / (1024 * 1024* 1024));
    // --- Eviction（ページ追い出し）状況 ---
    //cache: modified pages evicted=0
    uint64_t dirty_evicted = get_cache(session, WT_STAT_CONN_CACHE_EVICTION_DIRTY);
    uint64_t clean_evicted = get_cache(session, WT_STAT_CONN_CACHE_EVICTION_CLEAN);
    printf("・追い出されたページ数 (Dirty/Clean): %" PRIu64 " / %" PRIu64 "\n", dirty_evicted, clean_evicted);
    /*
    val = get_cache(session, WT_STAT_CONN_CACHE_EVICTION_TRIGGER_REACHED);
    printf("・ページ追い出し回数: %" PRIu64 " 回\n", val);
    // --- キャッシュヒット率の計算 ---
    uint64_t hits = get_cache(session, WT_STAT_CONN_BLOCK_CACHE_HITS);
    uint64_t misses = get_cache(session, WT_STAT_CONN_BLOCK_CACHE_MISSES);
    uint64_t total_access = hits + misses;
    double hit_rate = 0.0;
    if (total_access > 0) {
        hit_rate = ((double)hits / total_access) * 100.0;
    }
    printf("・キャッシュヒット/ミス (回数): %" PRIu64 " / %" PRIu64 "\n", hits, misses);
    printf("・キャッシュヒット率: %.2f %%\n", hit_rate);
    */

    printf("------------------------\n");
    return;
}

void print_database_disk_size(const char *dbname)
{
    char command[512];
    printf("\n--- データベースのディスク使用量 ---\n");
    snprintf(command, sizeof(command), "du -sh %s", dbname);
    fflush(stdout);
    if (system(command) != 0) {
        fprintf(stderr, "警告: ディスク使用量の取得に失敗しました。\n");
    }
}

int get_metadata(WT_SESSION *session){
    WT_CURSOR *cursor;
    const char *key, *value;
    int ret;

    // URIに "metadata:" を指定することで、WiredTiger.wt の中身にアクセスするstatistics
    error_check(session->open_cursor(session, "metadata:", NULL, NULL, &cursor));
    printf("--- WiredTiger.wt ファイルのメタデータ内容 ---\n");

    // --- カーソルを使って、メタデータを一行ずつ読み出す ---
    while ((ret = cursor->next(cursor)) == 0) {
        if ((ret = cursor->get_key(cursor, &key)) != 0) {
            fprintf(stderr, "Failed to get key: %s\n", wiredtiger_strerror(ret));
            break;
        }
        if ((ret = cursor->get_value(cursor, &value)) != 0) {
            fprintf(stderr, "Failed to get value: %s\n", wiredtiger_strerror(ret));
            break;
        }
        // キーと値を表示する
        printf("キー: %s\n値: %s\n\n", key, value);
    }

    if (ret != WT_NOTFOUND) {
        fprintf(stderr, "Cursor iteration failed: %s\n", wiredtiger_strerror(ret));
    }
    return EXIT_SUCCESS;
}

int get_btree_page(WT_SESSION *session, const char *table_name){
    WT_CURSOR *cursor;
    const char *key, *value;
    int ret;
    char command[256];

    // URIに "metadata:" を指定することで、WiredTiger.wt の中身にアクセスするstatistics
    snprintf(command, sizeof(command), "debug:b+tree:%s", table_name);
    error_check(session->open_cursor(session, command, NULL, NULL, &cursor));
    printf("--- btreeのキャッシュ内容 ---\n");

    // --- カーソルを使って、メタデータを一行ずつ読み出す ---
    while ((ret = cursor->next(cursor)) == 0) {
        if ((ret = cursor->get_key(cursor, &key)) != 0) {
            fprintf(stderr, "Failed to get key: %s\n", wiredtiger_strerror(ret));
            break;
        }
        if ((ret = cursor->get_value(cursor, &value)) != 0) {
            fprintf(stderr, "Failed to get value: %s\n", wiredtiger_strerror(ret));
            break;
        }
        // キーと値を表示する
        printf("キー: %s\n値: %s\n\n", key, value);
    }

    if (ret != WT_NOTFOUND) {
        fprintf(stderr, "Cursor iteration failed: %s\n", wiredtiger_strerror(ret));
    }
    return EXIT_SUCCESS;
}

/* バイナリデータを16進数とASCIIでダンプするヘルパー関数 */
/*
void __my_dump_hex(const void *data, size_t size)
{
    const unsigned char *p = (const unsigned char *)data;
    size_t i, j;
    for (i = 0; i < size; i += 16) {
        // Address
        printf("    %08zx: ", i);
        // Hex
        for (j = 0; j < 16; j++)
            if (i + j < size) printf("%02x ", p[i + j]); else printf("   ");
        printf(" ");
        // ASCII
        for (j = 0; j < 16; j++)
            if (i + j < size) printf("%c", (p[i + j] >= 32 && p[i + j] <= 126) ? p[i + j] : '.');
        printf("\n");
    }
}
//呼び出し方
    if (ref->page != NULL){
        printf("  -> Raw Page Data Dump:\n");
        __my_dump_hex(ref->page, __wt_atomic_loadsize(&ref->page->memory_footprint));
    }
*/
