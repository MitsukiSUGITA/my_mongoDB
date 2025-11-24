/*
・ビルド
cd /mnt/c/Users/Mitsuki25/lab/wiredtiger/build && cmake .. && make
・実行
cd /mnt/c/Users/Mitsuki25/lab/wiredtiger/my_cache && gcc -o cache_all_clear cache_all_clear.c management.c -I.. -I../build/include -L../build -lwiredtiger && export LD_LIBRARY_PATH=../build && ./cache_all_clear
・gdb ver.
cd /mnt/c/Users/Mitsuki25/lab/wiredtiger/my_cache && gcc -o cache_all_clear cache_all_clear.c management.c -I.. -I../build/include -L../build -lwiredtiger && export LD_LIBRARY_PATH=../build && gdb ./cache_all_clear
set environment LD_LIBRARY_PATH ../build
run
bt

cd /mnt/c/Users/Mitsuki25/lab/wiredtiger/my_cache && ../build/wt -h ./clear_cache_db dump my_table > table_content.txt

cd /mnt/c/Users/Mitsuki25/lab/wiredtiger/my_cache && ../build/wt -h ./clear_cache_db verify table:my_table

cd /mnt/c/Users/Mitsuki25/lab/wiredtiger/my_cache && ../build/wt -h ./clear_cache_db read table:my_table key0000000009
./wt -h ../my_cache/WT_HOME_search_test read table:my_table key0000000001

cd /mnt/c/Users/Mitsuki25/lab/wiredtiger/my_cache && ../build/wt -h ./clear_cache_db -v list table:my_table
cd /mnt/c/Users/Mitsuki25/lab/wiredtiger/my_cache && ../build/wt -h ./clear_cache_db dump table:my_table

../build/wt -h metadata_db dump table:my_table

*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>       // 変更点: system() 関数のために追加
#include <wiredtiger.h>
#include "management.h" // 自作のヘッダーファイル
#include <inttypes.h> // PRIu64 を使うために必要

//evict_lru.cで定義されたグローバル変数にアクセスするために、extern宣言を行う
extern CACHE_PAGE_INFO *metadata_list;
extern size_t metadata_count;

extern REF_WITH_CONTEXT *ref_list;
extern size_t ref_count;

// エラーハンドリング用のマクロ
#define error_check(call)                                       \
    do {                                                        \
        int ret = call;                                         \
        if (ret != 0) {                                         \
            fprintf(stderr, "%s:%d: %s: %s\n", __FILE__,         \
                __LINE__, #call, wiredtiger_strerror(ret));     \
            return (ret);                                       \
        }                                                       \
    } while (0)

void print_key_hex(const uint8_t *data, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        printf("%02x ", data[i]);
    }
}

/* グローバル変数に保存されたメタデータリストの内容を全て出力する関数 */
void print_saved_page_info(void)
{

    printf("\n========= Verifying Saved Metadata =========\n");

    if (metadata_list != NULL && metadata_count > 0) {
        printf("Total metadata entries saved: %zu\n", metadata_count);
        for (size_t i = 0; i < metadata_count; ++i) {
            printf("-----------------------------------------\n");
            printf("Entry #%zu:\n", i);
            printf("  -> URI          : %s\n", metadata_list[i].uri);
            printf("  -> Parent Addr  : %p\n", (void *)metadata_list[i].parent_page_addr);
            printf("  -> Page Size  : %d\n", (int)metadata_list[i].page_size);
            printf("  -> Child Key    : ");
            print_key_hex(metadata_list[i].child_key, metadata_list[i].child_key_size);
            printf("\n");
            printf("  -> Child Key (Str) : %.*s\n", (int)metadata_list[i].child_key_size, metadata_list[i].child_key);
            printf("\n");
        }
        printf("=========================================\n");

    } else {
        printf("No metadata was saved.\n");
    }
}

/*
int get_data(WT_CONNECTION *conn, WT_SESSION *session) {
    // ... データベースを開き、データを挿入してキャッシュをウォームアップ ...

    CACHE_PAGE_INFO *metadata_list = NULL;
    size_t page_info_count = 0;
    int ret;

    printf("Step 1: Saving cache structure metadata...\n");
    ret = wt_save_cache_structure(conn, &metadata_list, &page_info_count);

    if (ret == 0) {
        printf(" -> Successfully collected metadata for %zu parent pages.\n", page_info_count);

        // 収集した全メタデータを表示
        for (size_t i = 0; i < page_info_count; ++i) {
            printf("  - Link %zu: [Parent Addr: %p] -> Child Key: ",
                i, (void *)metadata_list[i].parent_page_addr);
            print_key_hex(metadata_list[i].child_key, metadata_list[i].child_key_size);
            printf(" (URI: %s)\n", metadata_list[i].uri);
        }

        // 重要：使い終わったら必ずメモリを解放する
        free(metadata_list);
    } else {
        fprintf(stderr, "Failed to collect metadata: %s\n", wiredtiger_strerror(ret));
    }

    // ... 接続を閉じる ...
    return 0;
}
*/

void clear_cache(WT_CONNECTION *conn, WT_SESSION *session){
    const char *default_threads = "eviction=(threads_max=4)";
    const char *increased_threads = "eviction=(threads_max=8)";
    uint64_t val;
    // --- ページ削除 ---
    //conn->open_session(conn, NULL, NULL, &session);
    //conn->reconfigure(conn, increased_threads);

    wt_clear_cache(conn);
    //conn->reconfigure(conn, default_threads);

    conn->open_session(conn, NULL, NULL, &session);
    val = get_cache(session, WT_STAT_CONN_CACHE_BYTES_INUSE);
    printf("・使用量: %" PRIu64 " bytes (%.2f GB)\n", val, (double)val / (1024 * 1024* 1024));
}

//1つのデータベース内に対し、データベースの作成、データの挿入、キャッシュのクリア、再構成を一度開いたままの状態で行う。
int onetable_clear_and_reconstruct(void){
    WT_CONNECTION *conn;
    WT_SESSION *session;
    const char *db_name = "clear_cache_db";
    //const char *param = "cache_size=1GB,eviction=(threads_min=1,threads_max=1),statistics=(all)";
    const char *param = "cache_size=1GB,statistics=(all)";
    const char *table = "table:my_table";
    WT_CURSOR *cursor;
    uint64_t val;
    int datasize = 0.5 * 1024;
    int ret;

    // --- 初期化 ---
    create_db(db_name, param, &conn);
    error_check(conn->open_session(conn, NULL, NULL, &session));

    // --- テーブルを作成 ---
    //error_check(session->create(session, table, "key_format=S,value_format=S"));
    error_check(session->create(session, table, "key_format=S,value_format=S,leaf_page_max=5MB")); 
    // --- データ挿入 ---
    //insert_data(session, "table:my_table", 0, datasize);
    //1ページの再構成用のコード
    insert_data(session, "table:my_table", 0, 15);
    // --- 2つのテーブル ---
    /*
    error_check(session->create(session, "table:table_1", "key_format=S,value_format=S,leaf_page_max=5MB"));
    error_check(session->create(session, "table:table_2", "key_format=S,value_format=S,leaf_page_max=5MB"));
    insert_data(session, "table:table_1", 0, datasize);
    insert_data(session, "table:table_2", 0, datasize);
    */

    error_check(session->checkpoint(session, NULL));
    //stdout (printf) と stderr (fprintf(stderr,...)) の出力先を "cache_all_clear.log" ファイルに変更する
    freopen("cache_all_clear.log", "w", stdout);
    freopen("cache_all_clear.log", "a", stderr);
    setvbuf(stdout, NULL, _IONBF, 0);
    
    clear_cache(conn, session);
    print_saved_page_info();
    wt_reconstruct_cache(conn);
    //conn->debug_info(conn, "cache");

    //ここにwt_reconstruct_cacheを呼び出す関数reconstruct_cacheの関数を呼び出したい
    error_check(session->close(session, NULL));
    error_check(conn->close(conn, NULL));
    printf("\n==================== Reconstruction complete ====================\n");
    return 0;
}

//1つのデータベース内にデータを挿入後、再オープンして再構成を行う。
int onedb_reopen_clear_and_reconstruct(void){
}
int main(void){
    WT_CONNECTION *conn;
    WT_SESSION *session;
    const char *db_name = "clear_cache_db";
    const char *cache_size = "1GB,statistics=(all)";
    const char *param = "cache_size=1GB,statistics=(all)";
    WT_CURSOR *cursor;
    uint64_t val;
    int datasize = 0.1 * 1024;
    int ret;

    // --- データベースの初期化 ---
    create_db(db_name, param, &conn);
    error_check(conn->open_session(conn, NULL, NULL, &session));
    // --- 2つのテーブルを作成 ---
    error_check(session->create(session, "table:table_1", "key_format=S,value_format=S"));
    //error_check(session->create(session, "table:table_1", "key_format=S,value_format=S,leaf_page_max=5MB"));
    
    // --- データ挿入 ---
    insert_data(session, "table:table_1", 0, datasize);

    freopen("cache_all_clear.log", "w", stdout);
    freopen("cache_all_clear.log", "a", stderr);
    setvbuf(stdout, NULL, _IONBF, 0);

    clear_cache(conn, session);

    error_check(session->close(session, NULL));
    error_check(conn->close(conn, NULL));

    open_db(db_name, cache_size, &conn);
    
    /*
    printf("\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= Dumping after reopen ");
    conn->debug_info(conn, "cache");
    printf("\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n");
    */
    error_check(conn->open_session(conn, NULL, NULL, &session));
    
    print_saved_page_info();
    wt_reconstruct_cache(conn);

    //ここにwt_reconstruct_cacheを呼び出す関数reconstruct_cacheの関数を呼び出したい
    error_check(session->close(session, NULL));
    error_check(conn->close(conn, NULL));
    printf("\n==================== Reconstruction complete ====================\n");
    return 0;
}

