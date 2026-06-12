/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "wt_internal.h"

/*
 * =========================================================================
 * QEMU Migration Extensions (Live Migration Optimization)
 * =========================================================================
 */
#ifndef __WT_MIGRATION_H
#define __WT_MIGRATION_H

/* マイグレーションの現在の進行フェーズを管理するグローバル状態変数 */
extern volatile int wt_migration_state;

/* シリアルポート(/dev/ttyS0)経由でQEMUホストへ直接デバッグログを出力するロガー */
void my_log(const char *format, ...);

/* PCIバスを走査し、IVSHMEMデバイスを特定してQEMUとの共有メモリをマッピングする初期化関数 */
void __wt_migration_init_shared_bitmap(WT_SESSION_IMPL *session, WT_CONNECTION_IMPL *conn);

/* 仮想アドレス(GVA)から物理ページフレーム番号(PFN)をロックフリーで高速変換する関数 */
uint64_t vaddr_to_pfn(void *vaddr);

/* ページ範囲からPFNを計算し、WT_PAGE構造体内の配列にセットする関数 */
void populate_page_pfn_array(WT_PAGE *page);

/* PFN配列に基づき、QEMU側の転送スキップ用共有ビットマップをアトミックに操作する関数 */
void update_migration_bitmap(WT_CONNECTION *connection, WT_PAGE *page, int is_clean);

/* MongoDB本体(C++側)で定義される、マイグレーション全体の排他ロックを獲得する関数 (Weakリンク) */
extern void mongo_acquire_global_migration_lock(void) __attribute__((weak));

/* MongoDB本体(C++側)で定義される、マイグレーション全体の排他ロックを解放する関数 (Weakリンク) */
extern void mongo_release_global_migration_lock(void) __attribute__((weak));

/* プレコピーフェーズにてB-Treeを走査し、クリーンなページの転送スキップフラグを立てる関数 */
int __wt_migration_set_skippages_bitmap(WT_CONNECTION *connection);

/* ストップ＆コピーフェーズにて、ダーティ状態を再評価し最終的なスキップ処理を行う関数 */
int __wt_migration_mark_clean_pages_dsk(WT_CONNECTION *connection);

#endif /* !__WT_MIGRATION_H */