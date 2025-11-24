/**
 * @file management.h
 * @brief データベース管理関数の宣言を記述するヘッダーファイル
 */
#ifndef MANAGEMENT_H
#define MANAGEMENT_H

#include <wiredtiger.h>

// データベースを新規作成する関数の宣言
int create_db(const char *dbname, const char *param, WT_CONNECTION **conn_p);

// 既存のデータベースを開く関数の宣言
int open_db(const char *dbname, const char *cache_size, WT_CONNECTION **conn_p);

// 既存のデータベースを削除する関数。
int remove_db(const char *dbname);
// キャッシュ統計情報を表示する関数
uint64_t get_cache(WT_SESSION *session, int param);

void print_cache(WT_SESSION *session);

// 指定された件数のデータを挿入する関数の宣言
int insert_data(WT_SESSION *session, const char *table_name, int start_key, size_t target_size_mb);

int read_all_data(WT_SESSION *session, const char *table_name);

void print_database_disk_size(const char *dbname);

int get_metadata(WT_SESSION *session);

int get_btree_page(WT_SESSION *session, const char *table_name);

#endif // MANAGEMENT_H
