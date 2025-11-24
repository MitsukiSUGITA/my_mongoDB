/**
 * @file clear_and_reconstruct.c
 * @brief キャッシュのクリアと再構成（プリウォーミング）を実演する統合プログラム
 *
 * 
vim clear_and_reconstruct.c
:78
:80
:82
^i

gcc -o clear_and_reconstruct clear_and_reconstruct.c management.c -I.. -I../build/include -L../build -lwiredtiger
export LD_LIBRARY_PATH=../build
./clear_and_reconstruct

cat 5GB.txt
* このプログラムは、あなたの最終目標である処理を一つの流れで実行します。
 * 1. データベースを作成し、データを書き込み、キャッシュが温まった状態を作ります。
 * 2. conn->reconfigure() を使い、キャッシュを強制的にクリアします。
 * 3. metadata:カーソルから全テーブルリストを取得し、それらを全件スキャンすることで
 * キャッシュを能動的に再構成（プリウォーミング）します。
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h> // PRIu64 マクロのためにインクルード
#include <unistd.h>     // !!! usleep のためにヘッダーを追加 !!!
#include <wiredtiger.h>
#include "management.h" // 自作のヘッダーファイルをインクルード

// エラーをチェックし、メッセージを出力して終了する
#define error_check(call)                                       \
    do {                                                        \
        int ret = call;                                         \
        if (ret != 0) {                                         \
            fprintf(stderr, "%s:%d: %s: %s\n", __FILE__,         \
                __LINE__, #call, wiredtiger_strerror(ret));     \
            return (ret);                                       \
        }                                                       \
    } while (0)
    
// エラーをチェックし、メッセージを出力して終了するヘルパー関数
static void
die_on_error(int ret, WT_CONNECTION *conn, const char *msg)
{
    if (ret != 0) {
        fprintf(stderr, "ERROR: %s: %s\n", msg,
            (conn != NULL) ? wiredtiger_strerror(ret) : wiredtiger_strerror(ret));
        exit(EXIT_FAILURE);
    }
}

void print_cache(WT_SESSION *session, FILE *fp){
    uint64_t current_usage;
    int ret, i;

    //current_usage = get_cache(session, WT_STAT_CONN_CACHE_BYTES_MAX);
    //fprintf(fp, "・設定されている最大キャッシュサイズ: %" PRIu64 " bytes (%.2f MB)\n", current_usage, (double)current_usage / (1024 * 1024));
    current_usage = get_cache(session, WT_STAT_CONN_CACHE_BYTES_INUSE);
    fprintf(fp, "・使用量: %" PRIu64 " bytes (%.2f GB)\n", current_usage, (double)current_usage / (1024 * 1024* 1024));
    current_usage = get_cache(session, WT_STAT_CONN_CACHE_BYTES_DIRTY);
    fprintf(fp, "・ダーティデータ量: %" PRIu64 " bytes (%.2f GB)\n", current_usage, (double)current_usage / (1024 * 1024* 1024));
    current_usage = get_cache(session, WT_STAT_CONN_CACHE_BYTES_READ);
    fprintf(fp, "・読み込まれた累計データ量: %" PRIu64 " bytes (%.2f GB)\n", current_usage, (double)current_usage / (1024 * 1024* 1024));
    current_usage = get_cache(session, WT_STAT_CONN_CACHE_BYTES_WRITE);
    fprintf(fp, "・ディスクに書き出された累計データ量: %" PRIu64 " bytes (%.2f GB)\n", current_usage, (double)current_usage / (1024 * 1024* 1024));
    current_usage = get_cache(session, WT_STAT_CONN_BLOCK_CACHE_HITS);
    current_usage = get_cache(session, WT_STAT_CONN_CACHE_EVICTION_TRIGGER_REACHED);
    fprintf(fp, "・ページ追い出し回数: %" PRIu64 " 回\n", current_usage);
/*
    fprintf(fp, "・キャッシュヒット率: %" PRIu64 " bytes (%.2f MB)\n", current_usage, (double)current_usage / (1024 * 1024));
    current_usage = get_cache(session, WT_STAT_CONN_BLOCK_CACHE_MISSES);
    fprintf(fp, "・キャッシュミス率: %" PRIu64 " bytes (%.2f MB)\n", current_usage, (double)current_usage / (1024 * 1024));
*/
    return;
}

int main(void)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor, *meta_cursor, *table_cursor;
    const char *home = "testdb";
    const char *table = "table:my_table";
    const char *initial_cache_size = "1G";  //キャッシュサイズはここから変更
    const char *small_cache_size = "1M";
    int datasize = 1 * 1024;  //データセットのサイズはここから変更
    uint64_t current_usage;
    const char *output_filename = "5GB.txt";
    FILE *fp;
    int ret;

    fp = fopen(output_filename, "w");
    // --- 1. データベースの準備とデータの作成 ---
    create_db(home, initial_cache_size, &conn);
    print_database_disk_size(home);
    error_check(conn->open_session(conn, NULL, NULL, &session));
    error_check(session->create(session, table, "key_format=S,value_format=S"));
    // 初期状態のキャッシュサイズを確認
    fprintf(fp, "--- Initial State ---\n");
    print_cache(session, fp);
    //print_cache(session, fp);
    // 1万件のデータを挿入
    fprintf(fp, "\n--- Inserting %d GB data ---\n", datasize/1024);
    insert_data(session, table, 0, datasize);
    print_cache(session, fp);

    // --- 2. キャッシュのクリア ---
    fprintf(fp, "--- clear cache ---\n");
    //printf("reconfigureを呼び出してキャッシュサイズを %s に縮小します...\n", small_cache_size);
    char reconfig_clear_str[256];
    snprintf(reconfig_clear_str, sizeof(reconfig_clear_str), "cache_size=%s", small_cache_size);
    error_check(session->checkpoint(session, NULL));
    error_check(conn->reconfigure(conn, reconfig_clear_str));
    
    // キャッシュが目標値以下になるまでループで監視
    uint64_t target_bytes = 1 * 1024 * 1024; // 1MBを目標
    for (int i = 0; i < 100; ++i) { // 最大10秒待つ
        current_usage = get_cache(session, WT_STAT_CONN_CACHE_BYTES_INUSE);
        if (current_usage < target_bytes) break;
        usleep(100000); // 100ミリ秒待機
    }
    print_cache(session, fp);

    fprintf(fp, "--- reconstruct cache ---\n");
    //キャッシュサイズを元に戻す
    char reconfig_back_str[256];
    snprintf(reconfig_back_str, sizeof(reconfig_back_str), "cache_size=%s", initial_cache_size);
    error_check(conn->reconfigure(conn, reconfig_back_str));

    //printf("メタデータから全テーブルリストを取得し、スキャンを開始します...\n");
    error_check(session->open_cursor(session, "metadata:", NULL, NULL, &meta_cursor));
/*
    const char *uri;
    while ((ret = meta_cursor->next(meta_cursor)) == 0) {
        error_check(meta_cursor->get_key(meta_cursor, &uri));

        if (strncmp(uri, "table:", 6) == 0 || strncmp(uri, "index:", 6) == 0) {
            printf("  プリウォーミング中: %s\n", uri);
            ret = session->open_cursor(session, uri, NULL, NULL, &table_cursor);
            if (ret != 0) {
                fprintf(stderr, "警告: %s のカーソルを開けませんでした。スキップします。\n", uri);
                continue;
            }
            while (table_cursor->next(table_cursor) == 0) {} // 全件スキャン
            error_check(table_cursor->close(table_cursor));
        }
    }
    if (ret != WT_NOTFOUND)
        die_on_error(ret, conn, "メタデータカーソルの走査中にエラーが発生しました");
*/
    const char *uri;
    while ((ret = meta_cursor->next(meta_cursor)) == 0) {
        ret = meta_cursor->get_key(meta_cursor, &uri);
        if (strncmp(uri, "table:", 6) == 0 || strncmp(uri, "index:", 6) == 0) {
            printf("  軽量プリウォーミング中: %s\n", uri);
            // !!! 修正点: カーソルを開いてすぐに閉じるだけ !!!
            ret = session->open_cursor(session, uri, NULL, NULL, &table_cursor);
            if (ret == 0) {
                table_cursor->close(table_cursor);
            }
        }
    }
    error_check(meta_cursor->close(meta_cursor));
    print_cache(session, fp);

    // --- 4. クリーンアップ ---
    error_check(conn->close(conn, NULL));
    print_database_disk_size(home);
    remove_db(home);
    return EXIT_SUCCESS;
}
