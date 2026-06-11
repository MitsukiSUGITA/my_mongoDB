/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#include <sys/io.h> // QEMUとのコマンドポート通信 (iopl, inl, outl) に必要なLinux固有ヘッダ
#include <time.h>
#include <dirent.h>

// =========================================================================
// 1. システム・環境設定 (System & Environment)
// =========================================================================
// clock_gettime などの高度な時間計測APIを有効にするためのPOSIX拡張指定
#define _POSIX_C_SOURCE 200809L
// シリアルポート経由のカスタムロガー出力先 
// [WARNING] ゲストOS起動後に必ず `sudo chmod 666 /dev/ttyS0` を実行すること
#define MY_LOG_FILE "/dev/ttyS0"

// =========================================================================
// 2. QEMU 連携用 I/Oポート定義 (QEMU Communication Ports)
// =========================================================================
#define QEMU_PORT_MONGO_CMD  0x1241  // QEMUからのコマンドを受信するポート
#define QEMU_PORT_MONGO_DONE 0x1243  // QEMUへ処理完了を通知するポート

// =========================================================================
// 3. マイグレーション状態管理・スレッド制御 (Migration State & Control)
// =========================================================================
// 現在のマイグレーションフェーズ 
// (0: 通常稼働, 1: プレコピー, 2: 状態変更処理, 3: ストップ＆コピー)
// ※ 複数スレッドから参照・更新されるため volatile 指定
volatile int wt_migration_state = 0;
// モニタースレッドの多重起動を防止するフラグ (0: 未起動, 1: 起動済み)
static int monitor_thread_started = 0;
// フェーズ1(プレコピー)とフェーズ3(ストップ＆コピー)の純粋なCPU実行時間を記録する配列
double guest_cpu_time[2] = {0, 0};

// =========================================================================
// 4. Pagemap キャッシュ機構 (Pagemap Caching for GVA to PFN)
// =========================================================================
#define PAGEMAP_ENTRY_SIZE  8     // pagemap 1エントリあたりのバイト数 (64bit = 8bytes)
#define PAGEMAP_CACHE_COUNT 8192  // 一度に先読みするエントリ数 (8192 * 8 = 64KB, L1キャッシュ最適化サイズ)

// マルチスレッド環境での競合を完全に防ぐためのスレッドローカル変数 (__thread)
// 各スレッドが独立してキャッシュ空間を持つことで、排他ロック不要の高速変換を実現する
static __thread uint64_t pagemap_buffer[PAGEMAP_CACHE_COUNT]; // スレッド専用のキャッシュバッファ
static __thread uintptr_t buffer_base_vpn = (uintptr_t)-1;    // 現在バッファに保持しているVPNの開始位置 (無効状態は -1)
static __thread int local_pagemap_fd = -1;                    // スレッド専用の /proc/self/pagemap ファイルディスクリプタ

/*
 * my_log --
 * シリアルポート経由でハイパーバイザ(QEMU)へ直接ログを出力するカスタムロガー関数
 * パフォーマンス低下を防ぐため、デバイスは一度開いたら使い回す
 */
void my_log(const char *format, ...) {
    static FILE *fp = NULL;
    static int init_failed = 0;

    // 1. 権限エラー等で過去に開けなかった場合は、無駄な再試行(システムコール)を避ける
    if (init_failed) return;

    // 2. 初回呼び出し時のみデバイスをオープンする（Singletonパターン）
    if (fp == NULL) {
        fp = fopen(MY_LOG_FILE, "a");
        if (fp == NULL) {
            init_failed = 1; // 失敗フラグを立てて以降は即リターン
            return;
        }
        // シリアル通信のバッファリングを完全に無効化し、fflushなしで即時出力させる
        setvbuf(fp, NULL, _IONBF, 0);
    }

    // 3. 引数のフォーマット出力
    va_list args;
    va_start(args, format);
    vfprintf(fp, format, args);
    va_end(args);
    
    // ※ setvbuf で _IONBF を指定しているため、fflush(fp) や fclose(fp) は不要
}

/*
 * get_mongo_cpu_time --
 * 呼び出し元スレッドが実際に消費したCPU時間を秒単位（double）で取得する関数。
 * I/O待ち、ロック待ち、スリープ等の非実行時間を除外した「純粋な処理時間」の計測に使用する。
 */
static double get_mongo_cpu_time(void) {
    struct timespec ts;
    // CLOCK_THREAD_CPUTIME_ID: プロセス全体ではなく、このスレッド専用のCPU実行時間を取得
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0) {
        // ナノ秒(nsec)を秒(sec)に変換して合算
        return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
    }
    return 0.0;
}

/*
 * qemu_monitor_thread --
 * QEMUからのコマンドポートをポーリングし，各フェーズの処理を起動・同期する制御スレッド
 * 引数 : arg - WiredTiger のデータベース全体を管理する WT_CONNECTION 構造体
 */
static void* qemu_monitor_thread(void *arg) {
    WT_CONNECTION *conn = (WT_CONNECTION *)arg;
    double cpu_start, cpu_end;

    // x86のI/Oポート特権レベル（I/O Privilege Level）を最高（3）に引き上げ，inl/outlを許可する
    if (iopl(3) < 0) {
        my_log("[MONITOR ERROR] iopl failed. Cannot monitor QEMU.\n");
        return NULL;
    }
    my_log("[MONITOR] QEMU Monitor Thread Started.\n");

    // QEMUからのコマンドをポーリングして処理するループ
    while (true) {
        // 1. QEMUからの指令トリガーをポーリング監視
        uint32_t flag = inl(QEMU_PORT_MONGO_CMD);
        
        if (flag == 1 || flag == 2) {
            my_log("[MONITOR] Phase %u Triggered!\n", flag);
            
            // QEMU側へ指令を受け取ったことを通知 (コマンドポートを一度リセット)
            outl(0, QEMU_PORT_MONGO_CMD);
            
            cpu_start = get_mongo_cpu_time(); // 計測開始

            // 2. 集約された各フェーズの実行
            if (flag == 1) __wt_migration_set_skippages_bitmap(conn);
            else __wt_migration_mark_clean_pages_dsk(conn);
            
            // 3. QEMUへの完了通知
            outl(2, QEMU_PORT_MONGO_CMD);

            cpu_end = get_mongo_cpu_time(); // 計測終了
            
            // 4. 実行時間の記録とサマリーログの出力
            guest_cpu_time[flag - 1] = cpu_end - cpu_start;
            my_log("[MONITOR] Phase %u Finished. cpu time: %lf sec\n", flag, guest_cpu_time[flag - 1]);
            
            if (flag == 2) {
                my_log("[MONITOR] Total cpu time: %lf sec\n", guest_cpu_time[0] + guest_cpu_time[1]);
            }

            // 5. QEMU側のセマフォ待機（qemu_sem_wait）を解除し，移送処理を再開
            outl(1, QEMU_PORT_MONGO_DONE); 
        } else if (flag == 3) { // 移送先での再開処理
            my_log("[MONITOR] Resumed on Destination VM! Releasing barriers.\n");
            
            // コマンドポートをリセット (Ack)
            outl(0, QEMU_PORT_MONGO_CMD);
            
            // 6. 移送先で凍結していたMongoDBのグローバルロックおよびバリアを完全解除
            wt_migration_state = 0; 
            if (mongo_release_global_migration_lock != NULL) {
                mongo_release_global_migration_lock();
            }
        }
        usleep(1000); // CPUのスパイクを防ぐためのウェイト（1ms）
    }
    return NULL;
}

/*
 * start_qemu_monitor --
 * 移送制御用の QEMU モニタースレッド（qemu_monitor_thread）を起動する関数
 * 引数 : conn - WiredTiger のデータベース全体を管理する WT_CONNECTION 構造体
 */
void start_qemu_monitor(WT_CONNECTION *conn) {
    pthread_t thread_id;
    int ret;

    // アトミックにフラグをチェック＆セット
    if (!__sync_bool_compare_and_swap(&monitor_thread_started, 0, 1)) {
        my_log("[MONITOR] Thread already running. Skipping.\n");
        return;
    }
    
    // バックグラウンドでQEMUを監視する専用スレッドを生成
    ret = pthread_create(&thread_id, NULL, qemu_monitor_thread, (void*)conn);
    if (ret != 0) {
        my_log("[MONITOR] Failed to create thread: %d", ret);
        monitor_thread_started = 0; // スレッド生成に失敗した場合はフラグを解放して戻す
    } else {
        // スレッドを切り離し(Detach)状態にすることで，
        // スレッド終了時に pthread_join を待つことなく自動的に全リソースが解放される
        pthread_detach(thread_id);
        my_log("[MONITOR] Monitor thread launched successfully.\n");
    }
}

#define WT_MIGRATION_PAGE_SIZE 4096ULL
#define WT_MIGRATION_PAGE_MASK (~(WT_MIGRATION_PAGE_SIZE - 1))

/*
 * __wt_migration_init_shared_bitmap --
 * ゲストOSのPCIバスをスキャンして QEMU IVSHMEM デバイスを特定し、
 * マイグレーションの転送状態を同期するための共有メモリをマッピングする関数。
 */
void __wt_migration_init_shared_bitmap(WT_SESSION_IMPL *session ,WT_CONNECTION_IMPL *conn) {
    DIR *dir;
    struct dirent *entry;
    char path[256];
    char vendor_str[16], device_str[16];
    int found = 0;
    
    int fd_v, fd_d, fd;
    ssize_t n;
    struct stat st;

    // 1. PCIデバイスディレクトリを開き、デバイスを列挙する
    dir = opendir("/sys/bus/pci/devices");
    if (dir != NULL) {
        my_log("[SHARED BITMAP] Scanning PCI devices for IVSHMEM...\n");
        
        while ((entry = readdir(dir)) != NULL) {
            if (entry->d_name[0] == '.') continue;

            memset(vendor_str, 0, sizeof(vendor_str));
            memset(device_str, 0, sizeof(device_str));

            // ベンダーIDの読み取り
            snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/vendor", entry->d_name);
            fd_v = open(path, O_RDONLY);
            if (fd_v >= 0) {
                n = read(fd_v, vendor_str, sizeof(vendor_str) - 1);
                if (n > 0) vendor_str[n] = '\0';
                close(fd_v);
            }

            // デバイスIDの読み取り
            snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/device", entry->d_name);
            fd_d = open(path, O_RDONLY);
            if (fd_d >= 0) {
                n = read(fd_d, device_str, sizeof(device_str) - 1);
                if (n > 0) device_str[n] = '\0';
                close(fd_d);
            }

            // 2. QEMU IVSHMEM デバイス (Vendor: 0x1af4, Device: 0x1110) か判定
            if (strncmp(vendor_str, "0x1af4", 6) == 0 && strncmp(device_str, "0x1110", 6) == 0) {
                // IVSHMEMの共有メモリ領域は通常 BAR2 (resource2) に割り当てられる
                snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/resource2", entry->d_name);
                found = 1;
                break;
            }
        }
        closedir(dir);
    }

    // 3. 対象デバイスが見つかった場合、メモリマッピング(mmap)を実行
    if (found) {
        fd = open(path, O_RDWR);
        if (fd >= 0) {
            // ファイル（PCIリソース）の情報を取得
            if (fstat(fd, &st) == 0) {
                // OSが認識しているサイズをそのままビットマップのサイズとして採用
                conn->shared_bitmap_size = st.st_size; 
                
                // 共有メモリとしてプロセスのアドレス空間にマッピング
                conn->shared_bitmap = mmap(NULL, conn->shared_bitmap_size, 
                                           PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                close(fd);
                
                if (conn->shared_bitmap != NULL && conn->shared_bitmap != MAP_FAILED) {
                    __wt_verbose_info(session, WT_VERB_RECOVERY, 
                        "IVSHMEM mapped at %s (Size: %zu bytes)", path, conn->shared_bitmap_size);
                    my_log("[SHARED BITMAP] Successfully mapped IVSHMEM. Size: %zu bytes\n", conn->shared_bitmap_size);
                } else {
                    conn->shared_bitmap = NULL;
                    __wt_err(session, errno, "IVSHMEM mmap failed");
                    my_log("[SHARED BITMAP ERROR] mmap failed (errno: %d)\n", errno);
                }
            } else {
                close(fd);
                __wt_err(session, errno, "IVSHMEM fstat failed");
                my_log("[SHARED BITMAP ERROR] fstat failed (errno: %d)\n", errno);
            }
        } else {
            my_log("[SHARED BITMAP ERROR] Failed to open %s\n", path);
        }
    } else {
        my_log("[SHARED BITMAP ERROR] IVSHMEM device (1af4:1110) not found on PCI bus.\n");
    }
}

/*
 * vaddr_to_pfn --
 * 指定されたGVAに対応するPFNを /proc/self/pagemap をパースして直接返す関数
 * キャッシュ機構によりシステムコールのオーバーヘッドを極限まで削減している
 * 引数 : vaddr - 変換元のGVA
 */
uint64_t vaddr_to_pfn(void *vaddr) {
    uintptr_t vpn = (uintptr_t)vaddr / WT_MIGRATION_PAGE_SIZE;
    uint64_t pfn_item = 0, offset;
    ssize_t bytes_read;

    // 1. キャッシュヒット判定 : 目的のVPNが現在のキャッシュバッファの範囲内にあるか確認
    if (buffer_base_vpn != (uintptr_t)-1 && vpn >= buffer_base_vpn && vpn < buffer_base_vpn + PAGEMAP_CACHE_COUNT) {
        pfn_item = pagemap_buffer[vpn - buffer_base_vpn];
    } else { // 2. キャッシュミス処理 (システムコールによるバッチ読み込み)
        // 初回呼び出し時のみファイルディスクリプタを開く
        if (local_pagemap_fd < 0) {
            local_pagemap_fd = open("/proc/self/pagemap", O_RDONLY);
            if (local_pagemap_fd < 0) return 0; // 開けなければ終了
        }

        // キャッシュの境界に合わせてベースVPNを計算し、オフセット(バイト位置)を決定
        buffer_base_vpn = (vpn / PAGEMAP_CACHE_COUNT) * PAGEMAP_CACHE_COUNT;
        offset = buffer_base_vpn * PAGEMAP_ENTRY_SIZE;
        // 指定したオフセットからキャッシュサイズ分をまとめて読み込む
        bytes_read = pread(local_pagemap_fd, pagemap_buffer, sizeof(pagemap_buffer), offset);
        
        if (bytes_read < PAGEMAP_ENTRY_SIZE) {
            buffer_base_vpn = (uintptr_t)-1; // エラー時やEOF時はキャッシュを無効化
            return 0;
        }

        // 実際に読み込めた範囲内に目的のVPNが含まれているか確認
        if (vpn < buffer_base_vpn + (bytes_read / 8)) {
            pfn_item = pagemap_buffer[vpn - buffer_base_vpn];
        } else {
            return 0;
        }
    }

    // 3. pagemap エントリの解析
    // Bit 63: Page Present フラグ (1なら物理メモリに実体が存在する)
    if ((pfn_item & (1ULL << 63)) == 0) return 0;
    // Bit 61のチェック(File/Shared page)は要件に応じてコメントアウトのまま維持
    // if ((pfn_item & (1ULL << 61)) != 0) return 0;

    // Bits 0-54: PFN (物理ページフレーム番号) を抽出して返す
    return pfn_item & ((1ULL << 55) - 1);
}

/*
 * populate_page_pfn_array --
 * ページのPFN配列を設定する関数
 * 引数 : page - 操作対象の PFN 配列を保持する WT_PAGE 構造体へのポインタ
 */
void populate_page_pfn_array(WT_PAGE *page) {
    uintptr_t start, end;
    uintptr_t aligned_start, aligned_end;
    uint16_t required_pages;

    // 1. すでにフェーズ1等で計算済みであれば、重い再計算（pagemap参照）を避けるため即リターン
    if (page->mig_pfn_cnt > 0) return;

    // 2. ディスクバッファ（dsk）が存在しない、またはサイズが0の場合は処理不可のためリターン
    if (page->dsk == NULL || page->dsk->mem_size == 0) return;

    start = (uintptr_t)page->dsk;
    end = start + page->dsk->mem_size;

    // 3. データの範囲内に完全に含まれる 4KB ページのアライメント境界を計算
    aligned_start = (start + WT_MIGRATION_PAGE_SIZE) & WT_MIGRATION_PAGE_MASK;
    aligned_end = end & WT_MIGRATION_PAGE_MASK;

    if (aligned_start >= aligned_end) return; // 有効なページ範囲が1ページ分も満たない場合はリターン
    
    // 4. 必要ページ数の算出
    required_pages = (aligned_end - aligned_start) / WT_MIGRATION_PAGE_SIZE;
    
    // 5. フェイルセーフ：ページ数が配列の最大サイズを超過する場合，転送漏れを防ぐためページ全体を通常通り転送
    if (required_pages > WT_MIG_MAX_PFNS) {
        page->mig_pfn_cnt = 0; 
        return;
    }

    // 6. 各4KBページのGVAからPFNへの変換と配列への格納
    for (uint16_t i = 0; i < required_pages; i++) {
        page->mig_pfns[i] = vaddr_to_pfn((void *)(aligned_start + (i * WT_MIGRATION_PAGE_SIZE)));
    }
    page->mig_pfn_cnt = required_pages; // 確定したページ数をセット
}

/*
 * update_migration_bitmap --
 * ページのPFNに基づいてスキップビットマップの該当ビットをアトミックに操作する関数
 * 引数 :
 * connection - データベース全体を管理する構造体へのポインタ
 * page       - 操作対象の PFN 配列を保持する WT_PAGE 構造体へのポインタ
 * is_clean   - ビットの変更値（1: スキップ対象としてセット, 0: 通常転送としてクリア）
 */
void update_migration_bitmap(WT_CONNECTION *connection, WT_PAGE *page, int is_clean) {
    WT_CONNECTION_IMPL *conn = (WT_CONNECTION_IMPL *)connection;
    uint8_t *bitmap;
    uint64_t pfn, byte_idx;
    uint8_t bit_mask;
    
    if (page == NULL || connection == NULL) return;
    if (conn->shared_bitmap == NULL) return;
    bitmap = (uint8_t *)conn->shared_bitmap;
    
    // 1. ページを構成する全ての物理ページをループ
    for(uint16_t i = 0; i < page->mig_pfn_cnt; i++){
        pfn = page->mig_pfns[i];

        // 2. 有効な PFN かつ ビットマップのバッファサイズ内であるかを厳密にチェック
        if (pfn != 0 && (pfn / 8) < conn->shared_bitmap_size) {
            byte_idx = pfn / 8; // 8ビットごとにインデックス
            bit_mask = 1 << (pfn % 8); // 8ビットにキャスト

            if (is_clean) {
                // 他スレッドの Eviction や並行クエリと衝突しないよう、アトミックにビットを1にセット
                __sync_fetch_and_or(&bitmap[byte_idx], bit_mask);
            } else {
                // 他スレッドの操作と衝突しないよう、アトミックにビットを0にクリア
                __sync_fetch_and_and(&bitmap[byte_idx], (uint8_t)(~bit_mask));
            }
        }
    }
}

/*
 * __wt_migration_set_skippages_bitmap --
 * 移送開始時に実行する，スキップするページを収集する関数
 * 引数 : connection - データベース全体を管理する構造体へのポインタ
 */
int __wt_migration_set_skippages_bitmap(WT_CONNECTION *connection) {
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)connection;
    WT_SESSION_IMPL *session_impl;
    WT_DATA_HANDLE *dhandle;
    WT_REF *ref;
    uint32_t walk_flags;
    int ret = 0, tret;
    int walked_pages_total = 0, clean_pages_marked = 0;
    
    wt_migration_state = 1; // フェーズ1開始

    // 1. ツリー探索用のセッションを開く
    if ((ret = __wt_open_session(conn_impl, NULL, NULL, false, &session_impl)) != 0) return (ret);

    // 2. 転送スキップ用共有ビットマップの初期化
    if (conn_impl->shared_bitmap != NULL) {
        memset(conn_impl->shared_bitmap, 0, conn_impl->shared_bitmap_size);
    }

    // フェーズ1はMongoDBが稼働中のため，待機せずにキャッシュ上の可視データを読み取る
    walk_flags = WT_READ_CACHE | WT_READ_RESTART_OK | WT_READ_VISIBLE_ALL | WT_READ_NO_WAIT;

    __wt_readlock(session_impl, &conn_impl->dhandle_lock); // dhandleリスト（オープン中のDB一覧）を保護するためのReadロック

    // 3. 使用しているデータベース内のツリーをループ
    TAILQ_FOREACH(dhandle, &conn_impl->dhqh, q){

        if (F_ISSET(dhandle, WT_DHANDLE_IS_METADATA | WT_DHANDLE_DEAD | WT_DHANDLE_DISCARD) || 
            WT_PREFIX_MATCH(dhandle->name, "WiredTiger")) continue; // メタデータ，削除済みのハンドル，システム固有（WiredTiger名前空間）はスキップ
        if (!WT_DHANDLE_BTREE(dhandle) || dhandle->handle == NULL) continue; // B-Tree構造でないもの，実体（ハンドル）がないものはスキップ
        if (!WT_PREFIX_MATCH(dhandle->name, "file:collection")) continue; // ユーザーデータ（コレクション）のみを対象
    
        __wt_atomic_add32((uint32_t *)&dhandle->session_inuse, 1); // ハンドルが途中でクローズ・削除されないように使用中カウントを増やす
        __wt_readunlock(session_impl, &conn_impl->dhandle_lock);  // リスト保護ロックを早期解放し、探索中の並行性を確保

        WT_WITH_DHANDLE(session_impl, dhandle, {
            ref = NULL;
            int restart_count = 0;
            
            // 4. ツリーの探索とスキップ候補の収集
            while(1) {
                // ツリーが途中でクローズされた場合のフェイルセーフ
                if (dhandle->handle == NULL) {
                    if (ref != NULL) {
                        WT_TRET(__wt_page_release(session_impl, ref, walk_flags));
                        ref = NULL;
                    }
                    break;
                }

                // 4-1. 探索対象のページを取得（内部でsession_implのハザードポインタが自動取得)
                int walk_ret = __wt_tree_walk(session_impl, &ref, walk_flags);

                // 4-2. 関数の戻り値チェック
                if (walk_ret != 0) { 
                    if (walk_ret == WT_NOTFOUND) break; // ツリーの終端まで到達した場合は正常終了としてループを抜ける
                    if (ref != NULL) { // その他の内部エラーの場合はスキップして次へ
                        WT_TRET(__wt_page_release(session_impl, ref, walk_flags));
                        ref = NULL;
                    }
                    // B-Treeの構造変更による再探索要求（RESTART）の場合の無限ループ防止機構
                    if (++restart_count > 100) {
                        my_log("[P1-WARN] Too many restarts in tree. Skipping the rest for safety.\n");
                        break; 
                    }
                    continue;
                }

                // 4-3. ポインタ自体のチェック
                if (ref == NULL) break; 
                
                // 4-4. ポインタの中身のチェック (ディスク上にのみ存在し，メモリに実体がないものはスキップ)
                if (ref->page == NULL) {
                    WT_TRET(__wt_page_release(session_impl, ref, walk_flags));
                    ref = NULL;
                    continue; 
                }

                walked_pages_total++;

                // スキップ条件1: ユーザーデータを持つ葉ページであること
                bool is_leaf = (ref->page->type == WT_PAGE_ROW_LEAF || 
                                ref->page->type == WT_PAGE_COL_VAR || 
                                ref->page->type == WT_PAGE_COL_FIX);

                // スキップ条件2: メモリに読み込まれてから変更されていないクリーンページであること
                bool is_clean = !__wt_page_is_modified(ref->page) && (ref->page->modify == NULL);
                                               
                // 4-5. 条件を満たす場合、ビットマップへ登録
                if (is_leaf && is_clean && ref->addr != NULL ) {
                    // 4-5-1. ページのPFNを計算してWT_PAGE構造体にセット
                    populate_page_pfn_array(ref->page);
                    // 4-5-2. スキップビットマップにアトミックに1をセット
                    update_migration_bitmap(connection, ref->page, 1);
                    __sync_synchronize(); // メモリバリア：QEMU 側の移送スレッドに対してビットマップへの登録を即座に可視化

                    // 4-5-3. ダブルチェック：フラグを立てた直後にバックグラウンドで書き換えられていないか
                    if (__wt_page_is_modified(ref->page)) {
                        update_migration_bitmap(connection, ref->page, 0);
                    } else {
                        clean_pages_marked++; 
                    }
                }
            } // while(1) 終了
        });
        
        __wt_readlock(session_impl, &conn_impl->dhandle_lock); // リスト保護ロックを再取得
        __wt_atomic_sub32((uint32_t *)&dhandle->session_inuse, 1); // ハンドルの使用中カウントを減らす
    } // TAILQ_FOREACH 終了
    
    // 全てのツリーの走査が終了したらロックを解放
    __wt_readunlock(session_impl, &conn_impl->dhandle_lock);
    
    // 5. WT_SESSIONを閉じる
    if ((tret = __wt_session_close_internal(session_impl)) != 0 && ret == 0) ret = tret;

    my_log("\n=== Phase 1 Bitmap Walk Summary ===\n");
    my_log("Total Walked       : %d pages\n", walked_pages_total);
    my_log("Clean Pages Marked : %d pages registered to bitmap\n", clean_pages_marked);
    my_log("===================================\n\n");

    return (ret);
}

/*
 * force_kvm_dirty_dsk --
 * ハザードチェックなどで転送スキップをあきらめたページ全体を KVM に転送させるためのダーティ化関数
 * 引数 : page - ダーティ化する WT_PAGE 構造体へのポインタ
 */
static void force_kvm_dirty_dsk(WT_PAGE *page) {
    if (page == NULL) return;

    // 1. WT_PAGE構造体そのものをダーティ化
    volatile uint8_t *page_ptr = (volatile uint8_t *)page;
    *page_ptr = *page_ptr;

    // 2. ディスクイメージ(dsk)のダーティ化
    if (page->dsk != NULL) {
        uint8_t *dsk_ptr = (uint8_t *)page->dsk;
        size_t size = page->dsk->mem_size;

        // ページ全体を4096バイトごとにアトミックにダーティ化する
        for (size_t offset = 0; offset < size; offset += 4096) {
            __sync_fetch_and_or((volatile uint32_t *)(dsk_ptr + offset), 0);
        }
        if (size > 0) {
            __sync_fetch_and_or((volatile uint8_t *)(dsk_ptr + size - 1), 0);
        }
    }
}

/*
 * __wt_migration_mark_clean_pages_dsk --
 * ストップ＆コピーフェーズ直前に実行する，転送をスキップしたページの状態を DISK に変更する関数
 * 引数 : connection - データベース全体を管理する構造体へのポインタ
 */
int __wt_migration_mark_clean_pages_dsk(WT_CONNECTION *connection) {
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)connection;
    WT_DATA_HANDLE *dhandle;
    WT_SESSION_IMPL *walk_session, *mark_session;
    WT_REF *ref;
    struct timeval phase3_start, phase3_end;
    uint32_t walk_flags;
    int ret = 0, tret;
    int walked_pages_total = 0, marked_pages_success = 0;

    // 1. トラフィックの完全遮断 (YCSBのクエリが静まるまでブロックされる)
    if (mongo_acquire_global_migration_lock != NULL) {
        mongo_acquire_global_migration_lock();
    }

    wt_migration_state = 2; // フェーズ3(ストップ＆コピーフェーズ直前の状態変更処理)開始

    gettimeofday(&phase3_start, NULL); // 計測開始

    // 2. 転送スキップ用共有ビットマップの初期化
    if (conn_impl->shared_bitmap != NULL) {
        memset(conn_impl->shared_bitmap, 0, conn_impl->shared_bitmap_size);
    }
    __sync_synchronize(); // メモリバリア：QEMU 側の移送スレッドに対してビットマップのクリアを即座に可視化

    // 3. 2つの独立したセッションを開く (探索と状態変更のコンテキストを分離)
    if ((ret = __wt_open_session(conn_impl, NULL, NULL, false, &walk_session)) != 0) // ツリーを探索し、ハザードポインタを取得・解放するためのセッション
        return (ret);
    if ((ret = __wt_open_session(conn_impl, NULL, NULL, false, &mark_session)) != 0) { // 探索完了後，ハザードチェックと状態変更(CAS)を行うためのセッション
        WT_TRET(__wt_session_close_internal(walk_session));
        return (ret);
    }

    walk_flags = WT_READ_CACHE | WT_READ_NOTFOUND_OK | WT_READ_RESTART_OK | WT_READ_VISIBLE_ALL; // 操作フラグを設定 : キャッシュ内の可視な全データを読み取り、Not Foundでもエラーにしない

    __wt_readlock(walk_session, &conn_impl->dhandle_lock); // dhandleリスト（オープン中のDB一覧）を保護するためのReadロック

    // 4. 使用しているデータベース内のツリーをループ
    TAILQ_FOREACH(dhandle, &conn_impl->dhqh, q) {
        
        if (F_ISSET(dhandle, WT_DHANDLE_IS_METADATA | WT_DHANDLE_DEAD | WT_DHANDLE_DISCARD) || 
            WT_PREFIX_MATCH(dhandle->name, "WiredTiger")) continue; // メタデータ，削除済みのハンドル，システム固有（WiredTiger名前空間）はスキップ
        if (!WT_DHANDLE_BTREE(dhandle) || dhandle->handle == NULL) continue; // B-Tree構造でないもの，実体（ハンドル）がないものはスキップ
        if (!WT_PREFIX_MATCH(dhandle->name, "file:collection")) continue; // ユーザーデータ（コレクション）のみを対象
        
        __wt_atomic_add32((uint32_t *)&dhandle->session_inuse, 1); // ハンドルが途中でクローズ・削除されないように使用中カウントを増やす
        __wt_readunlock(walk_session, &conn_impl->dhandle_lock);  // リスト保護ロックを早期解放し、探索中の並行性を確保

        // 5. 状態変更対象を管理する動的配列の初期化
        int skippages_capacity = 100000;
        WT_REF **skippages_array = malloc(skippages_capacity * sizeof(WT_REF*));
        int skippages_cnt = 0;

        // 探索と状態変更のDHANDLEコンテキストをセット
        WT_WITH_DHANDLE(walk_session, dhandle, {
        WT_WITH_DHANDLE(mark_session, dhandle, {
            ref = NULL;

            // 6. ツリーの探索とスキップ候補の収集
            while(1) {
                // ツリーが途中でクローズされた場合のフェイルセーフ
                if (dhandle->handle == NULL) {
                    if (ref != NULL) {
                        WT_TRET(__wt_page_release(walk_session, ref, walk_flags));
                        ref = NULL;
                    }
                    break;
                }

                walked_pages_total++; // 全走査ページ数のカウント
                
                // 6-1. 探索対象のページを取得（内部でwalk_sessionのハザードポインタが自動取得)
                int walk_ret = __wt_tree_walk(walk_session, &ref, walk_flags);
                
                // 6-2. 関数の戻り値チェック
                if (walk_ret != 0) { 
                    if (walk_ret == WT_NOTFOUND) break; // ツリーの終端まで到達した場合は正常終了としてループを抜ける
                    if (ref != NULL) { // その他の内部エラーの場合はスキップして次へ
                        WT_TRET(__wt_page_release(walk_session, ref, walk_flags));
                        ref = NULL;
                    }
                    continue;
                }

                // 6-3. ポインタ自体のチェック
                if (ref == NULL) break; 
                
                // 6-4. ポインタの中身のチェック (ディスク上にのみ存在し，メモリに実体がないものはスキップ)
                if (ref->page == NULL) {
                    WT_TRET(__wt_page_release(walk_session, ref, walk_flags));
                    ref = NULL;
                    continue; 
                }

                // スキップ条件1: ユーザーデータを持つ葉ページであること
                bool is_leaf = (ref->page->type == WT_PAGE_ROW_LEAF || 
                                ref->page->type == WT_PAGE_COL_VAR || 
                                ref->page->type == WT_PAGE_COL_FIX);
                // スキップ条件2: メモリに読み込まれてから変更されていないクリーンページであること
                bool is_clean = !__wt_page_is_modified(ref->page) && (ref->page->modify == NULL);
                // スキップ条件3: ディスク上のアドレスが存在し、かつ現在メモリ上(MEM)にあること
                bool is_valid_and_in_mem = (ref->addr != NULL) && 
                                               (ref->page->dsk != NULL) && 
                                               (WT_REF_GET_STATE(ref) == WT_REF_MEM);
                
                // 6-5. 全ての条件を満たす場合のみ、候補として配列に登録
                if (is_leaf && is_clean && is_valid_and_in_mem) {
                    if (skippages_cnt >= skippages_capacity) { // 動的配列の容量チェックと拡張
                        skippages_capacity *= 2;
                        skippages_array = realloc(skippages_array, skippages_capacity * sizeof(WT_REF*));
                        
                        if (skippages_array == NULL) { // メモリ枯渇時のフェイルセーフ
                            WT_TRET(__wt_page_release(walk_session, ref, walk_flags));
                            break; 
                        }
                    }
                    skippages_array[skippages_cnt++] = ref;
                }
            } // while(1) 終了

        // 7. スキップ候補に対して状態変更とハザードチェックを行う
        for (int i = 0; i < skippages_cnt; i++) {
            WT_REF *target = skippages_array[i];
            
            // 7-1. 状態変更前の再チェック：状態が変わっていたらスキップ
            if (WT_REF_GET_STATE(target) != WT_REF_MEM) continue;

            // 7-2. アトミックなCAS命令により，ページの独占権（WT_REF_LOCKED）を安全に取得
            bool cas_success = WT_REF_CAS_STATE(mark_session, target, WT_REF_MEM, WT_REF_LOCKED);
            
            if (cas_success) {
                // 7-3. ハザードチェック：他のスレッドがこのページを使用していないかチェック
                void *hazard_res = __wt_hazard_check(mark_session, target, NULL);

                if (hazard_res != NULL) { // 他スレッドが使用中の場合は，状態を元の MEM に戻して KVM 側に強制転送を要求
                    WT_REF_CAS_STATE(mark_session, target, WT_REF_LOCKED, WT_REF_MEM);
                    force_kvm_dirty_dsk(target->page); 
                } else { // 7-4. ハザードなし：ページを安全に DISK 状態に変更してその変更部分だけ転送を要求
                    // 1. 移送先でゴミポインタを参照させないため，ページ実体へのリンクを絶つ
                    target->page = NULL;
                    // 2. 状態を（ディスク上のみに存在）に変更
                    WT_REF_CAS_STATE(mark_session, target, WT_REF_LOCKED, WT_REF_DISK);
                    // 3. 親ノード（target自身）の書き換えをKVMのダーティトラッキングに確実に検知させるためのダミーライト
                    volatile uint8_t *dirty_ptr = (volatile uint8_t *)target;
                    *dirty_ptr = *dirty_ptr; 

                    marked_pages_success++; // スキップ成功カウンタをインクリメント
                }
            }
        } // for 終了
        }); 
        });

        // 8. 探索フェーズで使用した動的配列の解放とハンドルの後処理
        free(skippages_array);
        __wt_readlock(walk_session, &conn_impl->dhandle_lock); // リスト保護ロックを再取得
        __wt_atomic_sub32((uint32_t *)&dhandle->session_inuse, 1); // ハンドルの使用中カウントを減らす
    } // TAILQ_FOREACH 終了

    __wt_readunlock(walk_session, &conn_impl->dhandle_lock); // ハンドルリストの走査が完全に終わったためロックを解除
    
    // 9. 開いていた2つのセッションをクローズ
    if ((tret = __wt_session_close_internal(mark_session)) != 0 && ret == 0) ret = tret;
    if ((tret = __wt_session_close_internal(walk_session)) != 0 && ret == 0) ret = tret;
    
    wt_migration_state = 3; // ストップ＆コピーフェーズ開始

    gettimeofday(&phase3_end, NULL); // 計測終了
    double time_ms = (phase3_end.tv_sec - phase3_start.tv_sec) * 1000.0 + 
              (phase3_end.tv_usec - phase3_start.tv_usec) / 1000.0;

    my_log("\n=== Phase 3 State Change Summary ===\n");
    my_log("Execution Time   : %.2f ms\n", time_ms);
    my_log("Total Walked     : %d pages\n", walked_pages_total);
    my_log("Marked Success   : %d pages changed to WT_REF_DISK\n", marked_pages_success);
    my_log("====================================\n\n");
    
    return (ret);
}
