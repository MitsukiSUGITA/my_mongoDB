/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define _GNU_SOURCE 1
#include "wt_internal.h"

#include <sys/io.h>
#include <sys/mman.h> // mmap用
#include <stdarg.h> // va_list用

#define MY_LOG_FILE "/tmp/mongo_migration_test/my_debug.log"

#define QEMU_PORT_DATA_LOW      0x1230
#define QEMU_PORT_DATA_HIGH     0x1234
#define QEMU_PORT_MONGO_EVICT   0x1240
#define QEMU_PORT_MONGO_CMD     0x1241
bool clearing_cache = false;
pthread_t clearing_thread_id;
/* メタデータを格納するためのグローバルなリスト */
CACHE_PAGE_INFO *metadata_list = NULL;
size_t metadata_count = 0;
size_t metadata_capacity = 0;
int stable_count = 0;
// 1ページ(4KB)に収まるサイズに調整 (4096 - 8バイト(count)) / 8 = 511
#define BATCH_SIZE 510
typedef struct {
    uint64_t count;
    uint64_t total_pages;
    //uint64_t vaddr_list[BATCH_SIZE];
    uint64_t gpa_list[BATCH_SIZE]; 
} mongoDB_evict_List;


// グローバル変数としてリストを確保（スタックオーバーフロー防止）
// posix_memalignなどでページ境界に合わせるとより安全です
static mongoDB_evict_List evict_list;
static pthread_mutex_t qemu_lock = PTHREAD_MUTEX_INITIALIZER;

static int __evict_clear_all_walks_and_saved_tree(WT_SESSION_IMPL *);
static void __evict_list_clear_page_locked(WT_SESSION_IMPL *, WT_REF *, bool);
static int WT_CDECL __evict_lru_cmp(const void *, const void *);
static int __evict_lru_pages(WT_SESSION_IMPL *, bool);
static int __evict_lru_walk(WT_SESSION_IMPL *);
static int __evict_page(WT_SESSION_IMPL *, bool);
static int __evict_pass(WT_SESSION_IMPL *);
static int __evict_server(WT_SESSION_IMPL *, bool *);
static void __evict_tune_workers(WT_SESSION_IMPL *session);
static int __evict_walk(WT_SESSION_IMPL *, WTI_EVICT_QUEUE *);
static int __evict_walk_tree(WT_SESSION_IMPL *, WTI_EVICT_QUEUE *, u_int, u_int *);

//static void __my_dump_eviction_queue(WTI_EVICT_QUEUE *);
void print_key_hex(const uint8_t *data, size_t size);
int __my_evict_walk_tree(WT_SESSION_IMPL *, WTI_EVICT_QUEUE *, u_int, u_int *);
static void save_page_info_to_buffer(WT_BTREE *btree, WT_REF *ref, CACHE_PAGE_INFO *info);
void print_clear_page_info(WT_SESSION_IMPL *session, const char *title, WT_BTREE *btree, WT_REF *ref, int is_before);
//void print_clear_page_info(const char *title, WT_BTREE *btree, WT_REF *ref, int is_before);
uint32_t my_count_all_keys_in_page(WT_REF *ref);
void __my_dump_all_keys_before_evict(WT_SESSION_IMPL *session, WT_REF *ref);
static const char *ref_state_to_string(WT_REF_STATE state);
void write_metadata(const char *dbname);
static void my_log(const char *format, ...);
static uintptr_t GVA_to_GPA(void *vaddr);
static void add_mongoDB_evict_List(uintptr_t GVA, uintptr_t GPA);

#include <stdlib.h> // malloc, realloc, free を使うために必要

REF_WITH_CONTEXT *ref_list = NULL;
size_t ref_count = 0;
static int __collect_context_callback(WT_REF *ref, char *uri, void *arg);

#define WT_EVICT_HAS_WORKERS(s) (__wt_atomic_load32(&S2C(s)->evict_threads.current_threads) > 1)

/*
 * __evict_lock_handle_list --
 *     Try to get the handle list lock, with yield and sleep back off. Keep timing statistics
 *     overall.
 */
static int
__evict_lock_handle_list(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_RWLOCK *dh_lock;
    u_int spins;

    conn = S2C(session);
    evict = conn->evict;
    dh_lock = &conn->dhandle_lock;

    /*
     * Use a custom lock acquisition back off loop so the eviction server notices any interrupt
     * quickly.
     */
    for (spins = 0; (ret = __wt_try_readlock(session, dh_lock)) == EBUSY &&
         __wt_atomic_loadv32(&evict->pass_intr) == 0;
         spins++) {
        if (spins < WT_THOUSAND)
            __wt_yield();
        else
            __wt_sleep(0, WT_THOUSAND);
    }
    return (ret);
}

/*
 * __evict_entry_priority --
 *     Get the adjusted read generation for an eviction entry.
 */
static WT_INLINE uint64_t
__evict_entry_priority(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_BTREE *btree;
    WT_PAGE *page;
    uint64_t read_gen;

    btree = S2BT(session);
    page = ref->page;

    /* Any page set to the evict_soon or wont_need generation should be discarded. */
    if (__wti_evict_readgen_is_soon_or_wont_need(&page->read_gen))
        return (WT_READGEN_EVICT_SOON);

    /* Any page from a dead tree is a great choice. */
    if (F_ISSET(btree->dhandle, WT_DHANDLE_DEAD))
        return (WT_READGEN_EVICT_SOON);

    /* Any empty page (leaf or internal), is a good choice. */
    if (__wt_page_is_empty(page))
        return (WT_READGEN_EVICT_SOON);

    /* Any large page in memory is likewise a good choice. */
    if (__wt_atomic_loadsize(&page->memory_footprint) > btree->splitmempage)
        return (WT_READGEN_EVICT_SOON);

    /*
     * The base read-generation is skewed by the eviction priority. Internal pages are also
     * adjusted, we prefer to evict leaf pages.
     */
    if (page->modify != NULL && F_ISSET(S2C(session)->evict, WT_EVICT_CACHE_DIRTY) &&
      !F_ISSET(S2C(session)->evict, WT_EVICT_CACHE_CLEAN))
        read_gen = __wt_atomic_load64(&page->modify->update_txn);
    else
        read_gen = __wt_atomic_load64(&page->read_gen);

    read_gen += btree->evict_priority;

#define WT_EVICT_INTL_SKEW WT_THOUSAND
    if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
        read_gen += WT_EVICT_INTL_SKEW;

    return (read_gen);
}

/*
 * __evict_lru_cmp_debug --
 *     Qsort function: sort the eviction array. Version for eviction debug mode.
 */
static int WT_CDECL
__evict_lru_cmp_debug(const void *a_arg, const void *b_arg)
{
    const WTI_EVICT_ENTRY *a, *b;
    uint64_t a_score, b_score;

    a = a_arg;
    b = b_arg;
    a_score = (a->ref == NULL ? UINT64_MAX : 0);
    b_score = (b->ref == NULL ? UINT64_MAX : 0);

    return ((a_score < b_score) ? -1 : (a_score == b_score) ? 0 : 1);
}

/*
 * __evict_lru_cmp --
 *     Qsort function: sort the eviction array.
 */
static int WT_CDECL
__evict_lru_cmp(const void *a_arg, const void *b_arg)
{
    const WTI_EVICT_ENTRY *a, *b;
    uint64_t a_score, b_score;

    a = a_arg;
    b = b_arg;
    a_score = (a->ref == NULL ? UINT64_MAX : a->score);
    b_score = (b->ref == NULL ? UINT64_MAX : b->score);

    return ((a_score < b_score) ? -1 : (a_score == b_score) ? 0 : 1);
}

/*
 * __evict_list_clear --
 *     Clear an entry in the LRU eviction list.
 */
static WT_INLINE void
__evict_list_clear(WT_SESSION_IMPL *session, WTI_EVICT_ENTRY *e)
{
    if (e->ref != NULL) {
        WT_ASSERT(session, F_ISSET_ATOMIC_16(e->ref->page, WT_PAGE_EVICT_LRU));
        F_CLR_ATOMIC_16(e->ref->page, WT_PAGE_EVICT_LRU | WT_PAGE_EVICT_LRU_URGENT);
    }
    e->ref = NULL;
    e->btree = WT_DEBUG_POINT;
}

/*
 * __evict_list_clear_page_locked --
 *     This function searches for the page in all the eviction queues (skipping the urgent queue if
 *     requested) and clears it if found. It does not take the eviction queue lock, so the caller
 *     should hold the appropriate locks before calling this function.
 */
static void
__evict_list_clear_page_locked(WT_SESSION_IMPL *session, WT_REF *ref, bool exclude_urgent)
{
    WT_EVICT *evict;
    WTI_EVICT_ENTRY *evict_entry;
    uint32_t elem, i, q, last_queue_idx;
    bool found;

    last_queue_idx = exclude_urgent ? WTI_EVICT_URGENT_QUEUE : WTI_EVICT_QUEUE_MAX;
    evict = S2C(session)->evict;
    found = false;

    WT_ASSERT_SPINLOCK_OWNED(session, &evict->evict_queue_lock);

    for (q = 0; q < last_queue_idx && !found; q++) {
        __wt_spin_lock(session, &evict->evict_queues[q].evict_lock);
        elem = evict->evict_queues[q].evict_max;
        for (i = 0, evict_entry = evict->evict_queues[q].evict_queue; i < elem; i++, evict_entry++)
            if (evict_entry->ref == ref) {
                found = true;
                __evict_list_clear(session, evict_entry);
                break;
            }
        __wt_spin_unlock(session, &evict->evict_queues[q].evict_lock);
    }
    WT_ASSERT(session, !F_ISSET_ATOMIC_16(ref->page, WT_PAGE_EVICT_LRU));
}

/*
 * __wti_evict_list_clear_page --
 *     Check whether a page is present in the LRU eviction list. If the page is found in the list,
 *     remove it. This is called from the page eviction code to make sure there is no attempt to
 *     evict a child page multiple times.
 */
void
__wti_evict_list_clear_page(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_EVICT *evict;

    WT_ASSERT(session, __wt_ref_is_root(ref) || WT_REF_GET_STATE(ref) == WT_REF_LOCKED);

    /* Fast path: if the page isn't in the queue, don't bother searching. */
    if (!F_ISSET_ATOMIC_16(ref->page, WT_PAGE_EVICT_LRU))
        return;
    evict = S2C(session)->evict;

    __wt_spin_lock(session, &evict->evict_queue_lock);

    /* Remove the reference from the eviction queues. */
    __evict_list_clear_page_locked(session, ref, false);

    __wt_spin_unlock(session, &evict->evict_queue_lock);
}

/*
 * __evict_queue_empty --
 *     Is the queue empty? Note that the eviction server is pessimistic and treats a half full queue
 *     as empty.
 */
static WT_INLINE bool
__evict_queue_empty(WTI_EVICT_QUEUE *queue, bool server_check)
{
    uint32_t candidates, used;

    if (queue->evict_current == NULL)
        return (true);
    if(clearing_cache == true) return (queue->evict_candidates == 0);

    /* The eviction server only considers half of the candidates. */
    candidates = queue->evict_candidates;
    if (server_check && candidates > 1)
        candidates /= 2;
    used = (uint32_t)(queue->evict_current - queue->evict_queue);
    return (used >= candidates);
}

/*
 * __evict_queue_full --
 *     Is the queue full (i.e., it has been populated with candidates and none of them have been
 *     evicted yet)?
 */
static WT_INLINE bool
__evict_queue_full(WTI_EVICT_QUEUE *queue)
{
    return (queue->evict_current == queue->evict_queue && queue->evict_candidates != 0);
}

/* !!!
 * __wt_evict_server_wake --
 *     Wake up the eviction server thread. The eviction server typically sleeps for some time when
 *     cache usage is below the target thresholds. When the cache is expected to exceed these
 *     thresholds, callers can nudge the eviction server to wake up and resume its work.
 *
 *     This function is called in situations where pages are queued for urgent eviction or when
 *     application threads request eviction assistance.
 */
void
__wt_evict_server_wake(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    cache = conn->cache;

    if (WT_VERBOSE_LEVEL_ISSET(session, WT_VERB_EVICTION, WT_VERBOSE_DEBUG_2)) {
        uint64_t bytes_dirty, bytes_inuse, bytes_max, bytes_updates;

        bytes_inuse = __wt_cache_bytes_inuse(cache);
        bytes_max = conn->cache_size;
        bytes_dirty = __wt_cache_dirty_inuse(cache);
        bytes_updates = __wt_cache_bytes_updates(cache);
        __wt_verbose_debug2(session, WT_VERB_EVICTION,
          "waking, bytes inuse %s max (%" PRIu64 "MB %s %" PRIu64 "MB), bytes dirty %" PRIu64
          "(bytes), bytes updates %" PRIu64 "(bytes)",
          bytes_inuse <= bytes_max ? "<=" : ">", bytes_inuse / WT_MEGABYTE,
          bytes_inuse <= bytes_max ? "<=" : ">", bytes_max / WT_MEGABYTE, bytes_dirty,
          bytes_updates);
    }

    __wt_cond_signal(session, conn->evict->evict_cond);
}

/*
 * __evict_thread_chk --
 *     Check to decide if the eviction thread should continue running.
 */
static bool
__evict_thread_chk(WT_SESSION_IMPL *session)
{
    return (FLD_ISSET(S2C(session)->server_flags, WT_CONN_SERVER_EVICTION));
}

/*
 * __evict_thread_run --
 *     Entry function for an eviction thread. This is called repeatedly from the thread group code
 *     so it does not need to loop itself.
 */
static int
__evict_thread_run(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    bool did_work, was_intr;

    conn = S2C(session);
    evict = conn->evict;

    //グローバルな一時停止リクエストがあるかチェック
    if (__wt_atomic_load32((uint32_t *)&eviction_server_pause_request) != 0) {
        //自分が「停止中」であることをメインスレッドに通知
        __wt_atomic_store32((uint32_t *)&eviction_server_is_paused, 1);

        while (__wt_atomic_load32((uint32_t *)&eviction_server_pause_request) != 0) {
            usleep(10 * 1000); // 10ミリ秒待機
            if (!__evict_thread_chk(session))
                return (0);
        }
    }
    
    /* 自分が「動作中」であることをメインスレッドに通知 */
    __wt_atomic_store32((uint32_t *)&eviction_server_is_paused, 0);

    /* Mark the session as an eviction thread session. */
    F_SET(session, WT_SESSION_EVICTION);

    /*
     * Cache a history store cursor to avoid deadlock: if an eviction thread marks a file busy and
     * then opens a different file (in this case, the HS file), it can deadlock with a thread
     * waiting for the first file to drain from the eviction queue. See WT-5946 for details.
     */
    WT_ERR(__wt_curhs_cache(session));
    if (__wt_atomic_loadbool(&conn->evict_server_running) &&
      __wt_spin_trylock(session, &evict->evict_pass_lock) == 0) {
        /*
         * Cannot use WTI_WITH_PASS_LOCK because this is a try lock. Fix when that is supported. We
         * set the flag on both sessions because we may call clear_walk when we are walking with the
         * walk session, locked.
         */
        FLD_SET(session->lock_flags, WT_SESSION_LOCKED_PASS);
        FLD_SET(evict->walk_session->lock_flags, WT_SESSION_LOCKED_PASS);
        ret = __evict_server(session, &did_work);
        FLD_CLR(evict->walk_session->lock_flags, WT_SESSION_LOCKED_PASS);
        FLD_CLR(session->lock_flags, WT_SESSION_LOCKED_PASS);
        was_intr = __wt_atomic_loadv32(&evict->pass_intr) != 0;
        __wt_spin_unlock(session, &evict->evict_pass_lock);
        WT_ERR(ret);

        /*
         * If the eviction server was interrupted, wait until requests have been processed: the
         * system may otherwise be busy so don't go to sleep.
         */
        if (was_intr)
            while (__wt_atomic_loadv32(&evict->pass_intr) != 0 &&
              FLD_ISSET(conn->server_flags, WT_CONN_SERVER_EVICTION) &&
              F_ISSET(thread, WT_THREAD_RUN))
                __wt_yield();
        else {
            __wt_verbose_debug2(session, WT_VERB_EVICTION, "%s", "sleeping");

            /* Don't rely on signals: check periodically. */
            __wt_cond_auto_wait(session, evict->evict_cond, did_work, NULL);
            __wt_verbose_debug2(session, WT_VERB_EVICTION, "%s", "waking");
        }
    } else
        WT_ERR(__evict_lru_pages(session, false));

    if (0) {
err:
        WT_RET_PANIC(session, ret, "eviction thread error");
    }
    return (ret);
}

/*
 * __evict_set_saved_walk_tree --
 *     Set saved walk tree maintaining use count. Call it with NULL to clear the saved walk tree.
 */
static void
__evict_set_saved_walk_tree(WT_SESSION_IMPL *session, WT_DATA_HANDLE *new_dhandle)
{
    WT_DATA_HANDLE *old_dhandle;
    WT_EVICT *evict;

    evict = S2C(session)->evict;
    old_dhandle = evict->walk_tree;

    if (old_dhandle == new_dhandle)
        return;

    if (new_dhandle != NULL)
        (void)__wt_atomic_addi32(&new_dhandle->session_inuse, 1);

    evict->walk_tree = new_dhandle;

    if (old_dhandle != NULL) {
        WT_ASSERT(session, __wt_atomic_loadi32(&old_dhandle->session_inuse) > 0);
        (void)__wt_atomic_subi32(&old_dhandle->session_inuse, 1);
    }
}

/*
 * __evict_thread_stop --
 *     Shutdown function for an eviction thread.
 */
static int
__evict_thread_stop(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;

    if (thread->id != 0)
        return (0);

    conn = S2C(session);
    evict = conn->evict;
    /*
     * The only time the first eviction thread is stopped is on shutdown: in case any trees are
     * still open, clear all walks now so that they can be closed.
     */
    WTI_WITH_PASS_LOCK(session, ret = __evict_clear_all_walks_and_saved_tree(session));
    WT_ERR(ret);
    /*
     * The only cases when the eviction server is expected to stop are when recovery is finished,
     * when the connection is closing or when an error has occurred and connection panic flag is
     * set.
     */
    WT_ASSERT(session, F_ISSET(conn, WT_CONN_CLOSING | WT_CONN_PANIC | WT_CONN_RECOVERING));

    /* Clear the eviction thread session flag. */
    F_CLR(session, WT_SESSION_EVICTION);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "eviction thread exiting");

    if (0) {
err:
        WT_RET_PANIC(session, ret, "eviction thread error");
    }
    return (ret);
}

/*
 * __evict_server --
 *     Thread to evict pages from the cache.
 */
static int
__evict_server(WT_SESSION_IMPL *session, bool *did_work)
{
    struct timespec now;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    uint64_t time_diff_ms;

    /* Assume there has been no progress. */
    *did_work = false;

    conn = S2C(session);
    evict = conn->evict;

    WT_ASSERT_SPINLOCK_OWNED(session, &evict->evict_pass_lock);

    /*
     * Copy the connection setting for use in the current run of Eviction Server. This ensures that
     * no hazard pointers are leaked in case the setting is reconfigured while eviction pass is
     * running.
     */
    evict->use_npos_in_pass = __wt_atomic_loadbool(&conn->evict_use_npos);

    /* Evict pages from the cache as needed. */
    WT_RET(__evict_pass(session));

    if (!FLD_ISSET(conn->server_flags, WT_CONN_SERVER_EVICTION) ||
      __wt_atomic_loadv32(&evict->pass_intr) != 0)
        return (0);

    if (!__wt_evict_cache_stuck(session)) {
        if (evict->use_npos_in_pass)
            __evict_set_saved_walk_tree(session, NULL);
        else {
            /*
             * Try to get the handle list lock: if we give up, that indicates a session is waiting
             * for us to clear walks. Do that as part of a normal pass (without the handle list
             * lock) to avoid deadlock.
             */
            if ((ret = __evict_lock_handle_list(session)) == EBUSY)
                return (0);
            WT_RET(ret);

            /*
             * Clear the walks so we don't pin pages while asleep, otherwise we can block
             * applications evicting large pages.
             */
            ret = __evict_clear_all_walks_and_saved_tree(session);

            __wt_readunlock(session, &conn->dhandle_lock);
            WT_RET(ret);
        }
        /* Make sure we'll notice next time we're stuck. */
        evict->last_eviction_progress = 0;
        return (0);
    }

    /* Track if work was done. */
    *did_work = __wt_atomic_loadv64(&evict->eviction_progress) != evict->last_eviction_progress;
    evict->last_eviction_progress = __wt_atomic_loadv64(&evict->eviction_progress);

    /* Eviction is stuck, check if we have made progress. */
    if (*did_work) {
#if !defined(HAVE_DIAGNOSTIC)
        /* Need verbose check only if not in diagnostic build */
        if (WT_VERBOSE_ISSET(session, WT_VERB_EVICTION))
#endif
            __wt_epoch(session, &evict->stuck_time);
        return (0);
    }

#if !defined(HAVE_DIAGNOSTIC)
    /* Need verbose check only if not in diagnostic build */
    if (!WT_VERBOSE_ISSET(session, WT_VERB_EVICTION))
        return (0);
#endif
    /*
     * If we're stuck for 5 minutes in diagnostic mode, or the verbose eviction flag is configured,
     * log the cache and transaction state.
     *
     * If we're stuck for 5 minutes in diagnostic mode, give up.
     *
     * We don't do this check for in-memory workloads because application threads are not blocked by
     * the cache being full. If the cache becomes full of clean pages, we can be servicing reads
     * while the cache appears stuck to eviction.
     */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY))
        return (0);

    __wt_epoch(session, &now);

    /* The checks below should only be executed when a cache timeout has been set. */
    if (evict->cache_stuck_timeout_ms > 0) {
        time_diff_ms = WT_TIMEDIFF_MS(now, evict->stuck_time);
#ifdef HAVE_DIAGNOSTIC
        /* Enable extra logs 20ms before timing out. */
        if (evict->cache_stuck_timeout_ms < 20 ||
          (time_diff_ms > evict->cache_stuck_timeout_ms - 20))
            WT_SET_VERBOSE_LEVEL(session, WT_VERB_EVICTION, WT_VERBOSE_DEBUG_1);
#endif

        if (time_diff_ms >= evict->cache_stuck_timeout_ms) {
#ifdef HAVE_DIAGNOSTIC
            __wt_err(session, ETIMEDOUT, "Cache stuck for too long, giving up");
            WT_RET(__wt_verbose_dump_txn(session));
            WT_RET(__wt_verbose_dump_cache(session));
            return (__wt_set_return(session, ETIMEDOUT));
#else
            if (WT_VERBOSE_ISSET(session, WT_VERB_EVICTION)) {
                WT_RET(__wt_verbose_dump_txn(session));
                WT_RET(__wt_verbose_dump_cache(session));

                /* Reset the timer. */
                __wt_epoch(session, &evict->stuck_time);
            }
#endif
        }
    }
    return (0);
}

/* !!!
 * __wt_evict_threads_create --
 *     Initiate the eviction process by creating and launching the eviction threads.
 *
 *     The `threads_max` and `threads_min` configurations in `api_data.py` control the maximum and
 *     minimum number of eviction worker threads in WiredTiger. One of the threads acts as the
 *     eviction server, responsible for identifying evictable pages and placing them in eviction
 *     queues. The remaining threads are eviction workers, responsible for evicting pages from these
 *     eviction queues.
 *
 *     This function is called once during `wiredtiger_open` or recovery.
 *
 *     Return an error code if the thread group creation fails.
 */
int
__wt_evict_threads_create(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    uint32_t session_flags;

    conn = S2C(session);
    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "starting eviction threads");

    /*
     * In case recovery has allocated some transaction IDs, bump to the current state. This will
     * prevent eviction threads from pinning anything as they start up and read metadata in order to
     * open cursors.
     */
    WT_RET(__wt_txn_update_oldest(session, WT_TXN_OLDEST_STRICT | WT_TXN_OLDEST_WAIT));

    WT_ASSERT(session, conn->evict_threads_min > 0);
    /* Set first, the thread might run before we finish up. */
    FLD_SET(conn->server_flags, WT_CONN_SERVER_EVICTION);

    /*
     * Create the eviction thread group. Set the group size to the maximum allowed sessions.
     */
    session_flags = WT_THREAD_CAN_WAIT | WT_THREAD_PANIC_FAIL;
    WT_RET(__wt_thread_group_create(session, &conn->evict_threads, "eviction-server",
      conn->evict_threads_min, conn->evict_threads_max, session_flags, __evict_thread_chk,
      __evict_thread_run, __evict_thread_stop));

/*
 * Ensure the cache stuck timer is initialized when starting eviction.
 */
#if !defined(HAVE_DIAGNOSTIC)
    /* Need verbose check only if not in diagnostic build */
    if (WT_VERBOSE_ISSET(session, WT_VERB_EVICTION))
#endif
        __wt_epoch(session, &conn->evict->stuck_time);

    /*
     * Allow queues to be populated now that the eviction threads are running.
     */
    __wt_atomic_storebool(&conn->evict_server_running, true);

    return (0);
}

/* !!!
 * __wt_evict_threads_destroy --
 *     Stop and destroy the eviction threads. It must be called exactly once during
 *     `WT_CONNECTION::close` or recovery to ensure all eviction threads are properly terminated.
 *
 *     Return an error code if the thread group destruction fails.
 */
int
__wt_evict_threads_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    /* We are done if the eviction server didn't start successfully. */
    if (!__wt_atomic_loadbool(&conn->evict_server_running))
        return (0);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "stopping eviction threads");

    /* Wait for any eviction thread group changes to stabilize. */
    __wt_writelock(session, &conn->evict_threads.lock);

    /*
     * Signal the threads to finish and stop populating the queue.
     */
    FLD_CLR(conn->server_flags, WT_CONN_SERVER_EVICTION);
    __wt_atomic_storebool(&conn->evict_server_running, false);
    __wt_evict_server_wake(session);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "waiting for eviction threads to stop");

    /*
     * We call the destroy function still holding the write lock. It assumes it is called locked.
     */
    WT_RET(__wt_thread_group_destroy(session, &conn->evict_threads));

    return (0);
}

/*
 * __evict_update_work --
 *     Configure eviction work state.
 */
static bool
__evict_update_work(WT_SESSION_IMPL *session)
{
    WT_BTREE *hs_tree;
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    double dirty_target, dirty_trigger, target, trigger, updates_target, updates_trigger;
    uint64_t bytes_dirty, bytes_inuse, bytes_max, bytes_updates;
    uint32_t flags;

    conn = S2C(session);
    cache = conn->cache;
    evict = conn->evict;

    dirty_target = __wti_evict_dirty_target(evict);
    dirty_trigger = evict->eviction_dirty_trigger;
    target = evict->eviction_target;
    trigger = evict->eviction_trigger;
    updates_target = evict->eviction_updates_target;
    updates_trigger = evict->eviction_updates_trigger;

    /* Build up the new state. */
    flags = 0;

    if (!FLD_ISSET(conn->server_flags, WT_CONN_SERVER_EVICTION)) {
        __wt_atomic_store32(&evict->flags, 0);
        return (false);
    }

    if (!__evict_queue_empty(evict->evict_urgent_queue, false))
        LF_SET(WT_EVICT_CACHE_URGENT);

    /*
     * TODO: We are caching the cache usage values associated with the history store because the
     * history store dhandle isn't always available to eviction. Keeping potentially out-of-date
     * values could lead to surprising bugs in the future.
     */
    if (F_ISSET(conn, WT_CONN_HS_OPEN) && __wt_hs_get_btree(session, &hs_tree) == 0) {
        uint64_t bytes_hs_dirty;
        __wt_atomic_store64(&cache->bytes_hs, __wt_atomic_load64(&hs_tree->bytes_inmem));
        bytes_hs_dirty = __wt_atomic_load64(&hs_tree->bytes_dirty_intl) +
          __wt_atomic_load64(&hs_tree->bytes_dirty_leaf);
        __wt_atomic_store64(&cache->bytes_hs_dirty, bytes_hs_dirty);
        __wt_atomic_store64(&cache->bytes_hs_updates, __wt_atomic_load64(&hs_tree->bytes_updates));
    }

    /*
     * If we need space in the cache, try to find clean pages to evict.
     *
     * Avoid division by zero if the cache size has not yet been set in a shared cache.
     */
    bytes_max = conn->cache_size + 1;
    bytes_inuse = __wt_cache_bytes_inuse(cache);
    if (__wt_evict_clean_needed(session, NULL)) {
        LF_SET(WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_CLEAN_HARD);
        WT_STAT_CONN_INCR(session, cache_eviction_trigger_reached);
    } else if (bytes_inuse > (target * bytes_max) / 100) {
        LF_SET(WT_EVICT_CACHE_CLEAN);
    }

    bytes_dirty = __wt_cache_dirty_leaf_inuse(cache);
    if (__wt_evict_dirty_needed(session, NULL)) {
        LF_SET(WT_EVICT_CACHE_DIRTY | WT_EVICT_CACHE_DIRTY_HARD);
        WT_STAT_CONN_INCR(session, cache_eviction_trigger_dirty_reached);
    } else if (bytes_dirty > (uint64_t)(dirty_target * bytes_max) / 100) {
        LF_SET(WT_EVICT_CACHE_DIRTY);
    }

    bytes_updates = __wt_cache_bytes_updates(cache);
    if (__wti_evict_updates_needed(session, NULL)) {
        LF_SET(WT_EVICT_CACHE_UPDATES | WT_EVICT_CACHE_UPDATES_HARD);
        WT_STAT_CONN_INCR(session, cache_eviction_trigger_updates_reached);
    } else if (bytes_updates > (uint64_t)(updates_target * bytes_max) / 100) {
        LF_SET(WT_EVICT_CACHE_UPDATES);
    }

    /*
     * If application threads are blocked by data in cache, track the fill ratio.
     *
     */
    uint64_t cache_fill_ratio = bytes_inuse / bytes_max;
    bool evict_is_hard = LF_ISSET(WT_EVICT_CACHE_HARD);
    if (evict_is_hard) {
        if (cache_fill_ratio < 0.25)
            WT_STAT_CONN_INCR(session, cache_eviction_app_threads_fill_ratio_lt_25);
        else if (cache_fill_ratio < 0.50)
            WT_STAT_CONN_INCR(session, cache_eviction_app_threads_fill_ratio_25_50);
        else if (cache_fill_ratio < 0.75)
            WT_STAT_CONN_INCR(session, cache_eviction_app_threads_fill_ratio_50_75);
        else
            WT_STAT_CONN_INCR(session, cache_eviction_app_threads_fill_ratio_gt_75);
    }

    /*
     * If application threads are blocked by the total volume of data in cache, try dirty pages as
     * well.
     */
    if (__wt_evict_aggressive(session) && LF_ISSET(WT_EVICT_CACHE_CLEAN_HARD))
        LF_SET(WT_EVICT_CACHE_DIRTY);

    /*
     * Scrub dirty pages and keep them in cache if we are less than half way to the clean, dirty or
     * updates triggers.
     *
     * WT_CACHE_PREFER_SCRUB_EVICTION that can be turned on to enable scrub eviction
     *    as long as overall cache usage is under half way to the trigger limit.
     */
    if (bytes_inuse < (uint64_t)((target + trigger) * bytes_max) / 200) {
        if (F_ISSET_ATOMIC_16(
              &(conn->cache->cache_eviction_controls), WT_CACHE_PREFER_SCRUB_EVICTION)) {
            LF_SET(WT_EVICT_CACHE_SCRUB);
        } else if (bytes_dirty < (uint64_t)((dirty_target + dirty_trigger) * bytes_max) / 200 &&
          bytes_updates < (uint64_t)((updates_target + updates_trigger) * bytes_max) / 200) {
            LF_SET(WT_EVICT_CACHE_SCRUB);
        }
    } else
        LF_SET(WT_EVICT_CACHE_NOKEEP);

    if (FLD_ISSET(conn->debug_flags, WT_CONN_DEBUG_UPDATE_RESTORE_EVICT)) {
        LF_SET(WT_EVICT_CACHE_SCRUB);
        LF_CLR(WT_EVICT_CACHE_NOKEEP);
    }

    /*
     * With an in-memory cache, we only do dirty eviction in order to scrub pages.
     */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY)) {
        if (LF_ISSET(WT_EVICT_CACHE_CLEAN))
            LF_SET(WT_EVICT_CACHE_DIRTY);
        if (LF_ISSET(WT_EVICT_CACHE_CLEAN_HARD))
            LF_SET(WT_EVICT_CACHE_DIRTY_HARD);
        LF_CLR(WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_CLEAN_HARD);
    }

    /* Update the global eviction state. */
    __wt_atomic_store32(&evict->flags, flags);

    return (F_ISSET(evict, WT_EVICT_CACHE_ALL | WT_EVICT_CACHE_URGENT));
}

/*
 * __evict_pass --
 *     Evict pages from memory.
 */
static int
__evict_pass(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    WT_TXN_GLOBAL *txn_global;
    uint64_t eviction_progress, oldest_id, prev_oldest_id;
    uint64_t time_now, time_prev;
    u_int loop;

    conn = S2C(session);
    cache = conn->cache;
    evict = conn->evict;
    txn_global = &conn->txn_global;
    time_prev = 0; /* [-Wconditional-uninitialized] */

    /* Track whether pages are being evicted and progress is made. */
    eviction_progress = __wt_atomic_loadv64(&evict->eviction_progress);
    prev_oldest_id = __wt_atomic_loadv64(&txn_global->oldest_id);

    /* Evict pages from the cache. */
    for (loop = 0; __wt_atomic_loadv32(&evict->pass_intr) == 0; loop++) {
        time_now = __wt_clock(session);
        if (loop == 0)
            time_prev = time_now;

        __evict_tune_workers(session);
        /*
         * Increment the shared read generation. Do this occasionally even if eviction is not
         * currently required, so that pages have some relative read generation when the eviction
         * server does need to do some work.
         */
        __wt_atomic_add64(&evict->read_gen, 1);
        __wt_atomic_add64(&evict->evict_pass_gen, 1);

        /*
         * Update the oldest ID: we use it to decide whether pages are candidates for eviction.
         * Without this, if all threads are blocked after a long-running transaction (such as a
         * checkpoint) completes, we may never start evicting again.
         *
         * Do this every time the eviction server wakes up, regardless of whether the cache is full,
         * to prevent the oldest ID falling too far behind. Don't wait to lock the table: with
         * highly threaded workloads, that creates a bottleneck.
         */
        WT_RET(__wt_txn_update_oldest(session, WT_TXN_OLDEST_STRICT));

        if (!__evict_update_work(session))
            break;

        __wt_verbose_debug2(session, WT_VERB_EVICTION,
          "Eviction pass with: Max: %" PRIu64 " In use: %" PRIu64 " Dirty: %" PRIu64
          " Updates: %" PRIu64,
          conn->cache_size, __wt_atomic_load64(&cache->bytes_inmem),
          __wt_atomic_load64(&cache->bytes_dirty_intl) +
            __wt_atomic_load64(&cache->bytes_dirty_leaf),
          __wt_atomic_load64(&cache->bytes_updates));

        if (F_ISSET(evict, WT_EVICT_CACHE_ALL))
            WT_RET(__evict_lru_walk(session));

        /*
         * If the queue has been empty recently, keep queuing more pages to evict. If the rate of
         * queuing pages is high enough, this score will go to zero, in which case the eviction
         * server might as well help out with eviction.
         *
         * Also, if there is a single eviction server thread with no workers, it must service the
         * urgent queue in case all application threads are busy.
         */
        if (!WT_EVICT_HAS_WORKERS(session) &&
          (evict->evict_empty_score < WT_EVICT_SCORE_CUTOFF ||
            !__evict_queue_empty(evict->evict_urgent_queue, false)))
            WT_RET(__evict_lru_pages(session, true));

        if (__wt_atomic_loadv32(&evict->pass_intr) != 0)
            break;

        /*
         * If we're making progress, keep going; if we're not making any progress at all, mark the
         * cache "stuck" and go back to sleep, it's not something we can fix.
         *
         * We check for progress every 20ms, the idea being that the aggressive score will reach 10
         * after 200ms if we aren't making progress and eviction will start considering more pages.
         * If there is still no progress after 2s, we will treat the cache as stuck and start
         * rolling back transactions and writing updates to the history store table.
         */
        if (eviction_progress == __wt_atomic_loadv64(&evict->eviction_progress)) {
            if (WT_CLOCKDIFF_MS(time_now, time_prev) >= 20 && F_ISSET(evict, WT_EVICT_CACHE_HARD)) {
                if (__wt_atomic_load32(&evict->evict_aggressive_score) < WT_EVICT_SCORE_MAX)
                    (void)__wt_atomic_addv32(&evict->evict_aggressive_score, 1);
                oldest_id = __wt_atomic_loadv64(&txn_global->oldest_id);
                if (prev_oldest_id == oldest_id &&
                  __wt_atomic_loadv64(&txn_global->current) != oldest_id &&
                  __wt_atomic_load32(&evict->evict_aggressive_score) < WT_EVICT_SCORE_MAX)
                    (void)__wt_atomic_addv32(&evict->evict_aggressive_score, 1);
                time_prev = time_now;
                prev_oldest_id = oldest_id;
            }

            /*
             * Keep trying for long enough that we should be able to evict a page if the server
             * isn't interfering.
             */
            if (loop < 100 ||
              __wt_atomic_load32(&evict->evict_aggressive_score) < WT_EVICT_SCORE_MAX) {
                /*
                 * Back off if we aren't making progress: walks hold the handle list lock, blocking
                 * other operations that can free space in cache.
                 *
                 * Allow this wait to be interrupted (e.g. if a checkpoint completes): make sure we
                 * wait for a non-zero number of microseconds).
                 */
                WT_STAT_CONN_INCR(session, eviction_server_slept);
                __wt_cond_wait(session, evict->evict_cond, WT_THOUSAND, NULL);
                continue;
            }

            WT_STAT_CONN_INCR(session, eviction_slow);
            __wt_verbose_debug1(session, WT_VERB_EVICTION, "%s", "unable to reach eviction goal");
            break;
        }
        __wt_atomic_decrement_if_positive(&evict->evict_aggressive_score);
        loop = 0;
        eviction_progress = __wt_atomic_loadv64(&evict->eviction_progress);
    }
    return (0);
}

/*
 * __evict_clear_walk --
 *     Clear a single walk point and remember its position as a soft pointer if clear_pos is unset.
 */
static int
__evict_clear_walk(WT_SESSION_IMPL *session, bool clear_pos)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_REF *ref;
#define PATH_STR_MAX 1024
    char path_str[PATH_STR_MAX];
    const char *where;
    size_t path_str_offset;
    double pos;

    btree = S2BT(session);
    evict = S2C(session)->evict;

    WT_ASSERT(session, FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_PASS));

    if ((ref = btree->evict_ref) == NULL)
        return (0);

    if (!evict->use_npos_in_pass || clear_pos)
        WT_STAT_CONN_INCR(session, eviction_walks_abandoned);

    /*
     * Clear evict_ref before releasing it in case that forces eviction (we assert that we never try
     * to evict the current eviction walk point).
     */
    btree->evict_ref = NULL;

    if (evict->use_npos_in_pass) {
        /* If soft pointers are in use, remember the page's position unless clear_pos is set. */
        if (clear_pos)
            __wt_evict_clear_npos(btree);
        else {
            /*
             * Remember the last position before clearing it so that we can restart from about the
             * same point later. evict_saved_ref_check is used as an opaque page id to compare with
             * it upon restoration for the purpose of stats.
             */
            btree->evict_saved_ref_check = (uint64_t)ref;

            if (F_ISSET(ref, WT_REF_FLAG_LEAF)) {
                /* If we're at a leaf page, use the middle of the page. */
                pos = WT_NPOS_MID;
                where = "MIDDLE";
            } else {
                /*
                 * If we're at an internal page, then we've just finished all its leafs, so get the
                 * position of the very beginning or the very end of it depending on the direction
                 * of walk.
                 */
                if (btree->evict_start_type == WT_EVICT_WALK_NEXT ||
                  btree->evict_start_type == WT_EVICT_WALK_RAND_NEXT) {
                    pos = WT_NPOS_RIGHT;
                    where = "RIGHT";
                } else {
                    pos = WT_NPOS_LEFT;
                    where = "LEFT";
                }
            }
            if (!WT_VERBOSE_LEVEL_ISSET(session, WT_VERB_EVICTION, WT_VERBOSE_DEBUG_1))
                btree->evict_pos = __wt_page_npos(session, ref, pos, NULL, NULL, 0);
            else {
                btree->evict_pos =
                  __wt_page_npos(session, ref, pos, path_str, &path_str_offset, PATH_STR_MAX);
                __wt_verbose_debug1(session, WT_VERB_EVICTION,
                  "Evict walk point memorized at position %lf %s of %s page %s ref %p",
                  btree->evict_pos, where, F_ISSET(ref, WT_REF_FLAG_INTERNAL) ? "INTERNAL" : "LEAF",
                  path_str, (void *)ref);
            }
        }
    }

    WT_WITH_DHANDLE(evict->walk_session, session->dhandle,
      (ret = __wt_page_release(evict->walk_session, ref, WT_READ_NO_EVICT)));
    return (ret);
#undef PATH_STR_MAX
}

/*
 * __evict_clear_all_walks_and_saved_tree --
 *     Clear the eviction walk points for all files a session is waiting on.
 */
static int
__evict_clear_all_walks_and_saved_tree(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;

    conn = S2C(session);

    TAILQ_FOREACH (dhandle, &conn->dhqh, q)
        if (WT_DHANDLE_BTREE(dhandle))
            WT_WITH_DHANDLE(session, dhandle, WT_TRET(__evict_clear_walk(session, true)));
    __evict_set_saved_walk_tree(session, NULL);
    return (ret);
}

/*
 * __evict_clear_walk_and_saved_tree_if_current_locked --
 *     Clear single walk points and clear the walk tree if it's the current session's dhandle.
 */
static int
__evict_clear_walk_and_saved_tree_if_current_locked(WT_SESSION_IMPL *session)
{
    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->evict->evict_pass_lock);
    if (session->dhandle == S2C(session)->evict->walk_tree)
        __evict_set_saved_walk_tree(session, NULL);
    return (__evict_clear_walk(session, false));
}

/* !!!
 * __wt_evict_file_exclusive_on --
 *     Acquire exclusive access to a file/tree making it possible to evict the entire file using
 *     `__wt_evict_file`. It does this by incrementing the `evict_disabled` counter for a
 *     tree, which disables all other means of eviction (except file eviction).
 *
 *     For the incremented `evict_disabled` value, the eviction server skips walking this tree for
 *     eviction candidates, and force-evicting or queuing pages from this tree is not allowed.
 *
 *     It is called from multiple places in the code base, such as when initiating file eviction
 *     `__wt_evict_file` or when opening or closing trees.
 *
 *     Return an error code if unable to acquire necessary locks or clear the eviction queues.
 */
int
__wt_evict_file_exclusive_on(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_EVICT *evict;
    WTI_EVICT_ENTRY *evict_entry;
    u_int elem, i, q;

    btree = S2BT(session);
    evict = S2C(session)->evict;

    /* Hold the walk lock to turn off eviction. */
    __wt_spin_lock(session, &evict->evict_walk_lock);
    if (++btree->evict_disabled > 1) {
        __wt_spin_unlock(session, &evict->evict_walk_lock);
        return (0);
    }

    __wt_verbose_debug1(session, WT_VERB_EVICTION, "obtained exclusive eviction lock on btree %s",
      btree->dhandle->name);

    /*
     * Special operations don't enable eviction, however the underlying command (e.g. verify) may
     * choose to turn on eviction. This falls outside of the typical eviction flow, and here
     * eviction may forcibly remove pages from the cache. Consequently, we may end up evicting
     * internal pages which still have child pages present on the pre-fetch queue. Remove any refs
     * still present on the pre-fetch queue so that they are not accidentally accessed in an invalid
     * way later on.
     */
    WT_ERR(__wt_conn_prefetch_clear_tree(session, false));

    /*
     * Ensure no new pages from the file will be queued for eviction after this point, then clear
     * any existing LRU eviction walk for the file.
     */
    (void)__wt_atomic_addv32(&evict->pass_intr, 1);
    WTI_WITH_PASS_LOCK(session, ret = __evict_clear_walk_and_saved_tree_if_current_locked(session));
    (void)__wt_atomic_subv32(&evict->pass_intr, 1);
    WT_ERR(ret);

    /*
     * The eviction candidate list might reference pages from the file, clear it. Hold the evict
     * lock to remove queued pages from a file.
     */
    __wt_spin_lock(session, &evict->evict_queue_lock);

    for (q = 0; q < WTI_EVICT_QUEUE_MAX; q++) {
        __wt_spin_lock(session, &evict->evict_queues[q].evict_lock);
        elem = evict->evict_queues[q].evict_max;
        for (i = 0, evict_entry = evict->evict_queues[q].evict_queue; i < elem; i++, evict_entry++)
            if (evict_entry->btree == btree)
                __evict_list_clear(session, evict_entry);
        __wt_spin_unlock(session, &evict->evict_queues[q].evict_lock);
    }

    __wt_spin_unlock(session, &evict->evict_queue_lock);

    /*
     * We have disabled further eviction: wait for concurrent LRU eviction activity to drain.
     */
    while (btree->evict_busy > 0)
        __wt_yield();

    if (0) {
err:
        --btree->evict_disabled;
    }
    __wt_spin_unlock(session, &evict->evict_walk_lock);
    return (ret);
}

/* !!!
 * __wt_evict_file_exclusive_off --
 *     Release exclusive access to a file/tree by decrementing the `evict_disabled` count
 *     back to zero, allowing eviction to proceed for the tree.
 *
 *     It is called from multiple places in the code where exclusive eviction access is no longer
 *     needed.
 */
void
__wt_evict_file_exclusive_off(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;

    btree = S2BT(session);

    /*
     * We have seen subtle bugs with multiple threads racing to turn eviction on/off. Make races
     * more likely in diagnostic builds.
     */
    WT_DIAGNOSTIC_YIELD;

/*
 * Atomically decrement the evict-disabled count, without acquiring the eviction walk-lock. We can't
 * acquire that lock here because there's a potential deadlock. When acquiring exclusive eviction
 * access, we acquire the eviction walk-lock and then the eviction's pass-intr lock. The current
 * eviction implementation can hold the pass-intr lock and call into this function (see WT-3303 for
 * the details), which might deadlock with another thread trying to get exclusive eviction access.
 */
#if defined(HAVE_DIAGNOSTIC)
    {
        int32_t v;

        WT_ASSERT(session, btree->evict_ref == NULL);
        v = __wt_atomic_subi32(&btree->evict_disabled, 1);
        WT_ASSERT(session, v >= 0);
    }
#else
    (void)__wt_atomic_subi32(&btree->evict_disabled, 1);
#endif
    __wt_verbose_debug1(session, WT_VERB_EVICTION, "released exclusive eviction lock on btree %s",
      btree->dhandle->name);
}

#define EVICT_TUNE_BATCH 1 /* Max workers to add each period */
                           /*
                            * Data points needed before deciding if we should keep adding workers or
                            * settle on an earlier value.
                            */
#define EVICT_TUNE_DATAPT_MIN 8
#define EVICT_TUNE_PERIOD 60 /* Tune period in milliseconds */

/*
 * We will do a fresh re-tune every that many milliseconds to adjust to significant phase changes.
 */
#define EVICT_FORCE_RETUNE (25 * WT_THOUSAND)

/*
 * __evict_tune_workers --
 *     Find the right number of eviction workers. Gradually ramp up the number of workers increasing
 *     the number in batches indicated by the setting above. Store the number of workers that gave
 *     us the best throughput so far and the number of data points we have tried. Every once in a
 *     while when we have the minimum number of data points we check whether the eviction throughput
 *     achieved with the current number of workers is the best we have seen so far. If so, we will
 *     keep increasing the number of workers. If not, we are past the infliction point on the
 *     eviction throughput curve. In that case, we will set the number of workers to the best
 *     observed so far and settle into a stable state.
 */
static void
__evict_tune_workers(WT_SESSION_IMPL *session)
{
    struct timespec current_time;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    uint64_t delta_msec, delta_pages;
    uint64_t eviction_progress, eviction_progress_rate, time_diff;
    uint32_t current_threads;
    int32_t cur_threads, i, target_threads, thread_surplus;

    conn = S2C(session);
    evict = conn->evict;

    /*
     * If we have a fixed number of eviction threads, there is no value in calculating if we should
     * do any tuning.
     */
    if (conn->evict_threads_max == conn->evict_threads_min)
        return;

    __wt_epoch(session, &current_time);
    time_diff = WT_TIMEDIFF_MS(current_time, evict->evict_tune_last_time);

    /*
     * If we have reached the stable state and have not run long enough to surpass the forced
     * re-tuning threshold, return.
     */
    if (evict->evict_tune_stable) {
        if (time_diff < EVICT_FORCE_RETUNE)
            return;

        /*
         * Stable state was reached a long time ago. Let's re-tune. Reset all the state.
         */
        evict->evict_tune_stable = false;
        evict->evict_tune_last_action_time.tv_sec = 0;
        evict->evict_tune_progress_last = 0;
        evict->evict_tune_num_points = 0;
        evict->evict_tune_progress_rate_max = 0;

        /* Reduce the number of eviction workers by one */
        thread_surplus = (int32_t)__wt_atomic_load32(&conn->evict_threads.current_threads) -
          (int32_t)conn->evict_threads_min;

        if (thread_surplus > 0)
            __wt_thread_group_stop_one(session, &conn->evict_threads);

    } else if (time_diff < EVICT_TUNE_PERIOD)
        /*
         * If we have not reached stable state, don't do anything unless enough time has passed
         * since the last time we have taken any action in this function.
         */
        return;

    /*
     * Measure the evicted progress so far. Eviction rate correlates to performance, so this is our
     * metric of success.
     */
    eviction_progress = __wt_atomic_loadv64(&evict->eviction_progress);

    /*
     * If we have recorded the number of pages evicted at the end of the previous measurement
     * interval, we can compute the eviction rate in evicted pages per second achieved during the
     * current measurement interval. Otherwise, we just record the number of evicted pages and
     * return.
     */
    if (evict->evict_tune_progress_last == 0)
        goto done;

    delta_msec = WT_TIMEDIFF_MS(current_time, evict->evict_tune_last_time);
    delta_pages = eviction_progress - evict->evict_tune_progress_last;
    eviction_progress_rate = (delta_pages * WT_THOUSAND) / delta_msec;
    evict->evict_tune_num_points++;

    /*
     * Keep track of the maximum eviction throughput seen and the number of workers corresponding to
     * that throughput.
     */
    if (eviction_progress_rate > evict->evict_tune_progress_rate_max) {
        evict->evict_tune_progress_rate_max = eviction_progress_rate;
        evict->evict_tune_workers_best = __wt_atomic_load32(&conn->evict_threads.current_threads);
    }

    /*
     * Compare the current number of data points with the number needed variable. If they are equal,
     * we will check whether we are still going up on the performance curve, in which case we will
     * increase the number of needed data points, to provide opportunity for further increasing the
     * number of workers. Or we are past the inflection point on the curve, in which case we will go
     * back to the best observed number of workers and settle into a stable state.
     */
    if (evict->evict_tune_num_points >= evict->evict_tune_datapts_needed) {
        current_threads = __wt_atomic_load32(&conn->evict_threads.current_threads);
        if (evict->evict_tune_workers_best == current_threads &&
          current_threads < conn->evict_threads_max) {
            /*
             * Keep adding workers. We will check again at the next check point.
             */
            evict->evict_tune_datapts_needed += WT_MIN(EVICT_TUNE_DATAPT_MIN,
              (conn->evict_threads_max - current_threads) / EVICT_TUNE_BATCH);
        } else {
            /*
             * We are past the inflection point. Choose the best number of eviction workers observed
             * and settle into a stable state.
             */
            thread_surplus = (int32_t)__wt_atomic_load32(&conn->evict_threads.current_threads) -
              (int32_t)evict->evict_tune_workers_best;

            for (i = 0; i < thread_surplus; i++)
                __wt_thread_group_stop_one(session, &conn->evict_threads);

            evict->evict_tune_stable = true;
            goto done;
        }
    }

    /*
     * If we have not added any worker threads in the past, we set the number of data points needed
     * equal to the number of data points that we must accumulate before deciding if we should keep
     * adding workers or settle on a previously tried stable number of workers.
     */
    if (evict->evict_tune_last_action_time.tv_sec == 0)
        evict->evict_tune_datapts_needed = EVICT_TUNE_DATAPT_MIN;

    if (F_ISSET(evict, WT_EVICT_CACHE_ALL)) {
        cur_threads = (int32_t)__wt_atomic_load32(&conn->evict_threads.current_threads);
        target_threads = WT_MIN(cur_threads + EVICT_TUNE_BATCH, (int32_t)conn->evict_threads_max);
        /*
         * Start the new threads.
         */
        for (i = cur_threads; i < target_threads; ++i) {
            __wt_thread_group_start_one(session, &conn->evict_threads, false);
            __wt_verbose_debug1(session, WT_VERB_EVICTION, "%s", "added worker thread");
        }
        evict->evict_tune_last_action_time = current_time;
    }

done:
    evict->evict_tune_last_time = current_time;
    evict->evict_tune_progress_last = eviction_progress;
}

/*
 * __evict_lru_pages --
 *     Get pages from the LRU queue to evict.
 */
static int
__evict_lru_pages(WT_SESSION_IMPL *session, bool is_server)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_TRACK_OP_DECL;

    WT_TRACK_OP_INIT(session);
    conn = S2C(session);

    /*
     * Reconcile and discard some pages: EBUSY is returned if a page fails eviction because it's
     * unavailable, continue in that case.
     */
    while (FLD_ISSET(conn->server_flags, WT_CONN_SERVER_EVICTION) && ret == 0)
        if ((ret = __evict_page(session, is_server)) == EBUSY)
            ret = 0;

    /* If any resources are pinned, release them now. */
    WT_TRET(__wt_session_release_resources(session));

    /* If a worker thread found the queue empty, pause. */
    if (ret == WT_NOTFOUND && !is_server && FLD_ISSET(conn->server_flags, WT_CONN_SERVER_EVICTION))
        __wt_cond_wait(session, conn->evict_threads.wait_cond, 10 * WT_THOUSAND, NULL);

    WT_TRACK_OP_END(session);
    return (ret == WT_NOTFOUND ? 0 : ret);
}

/*
 * __evict_lru_walk --
 *     Add pages to the LRU queue to be evicted from cache.
 */
static int
__evict_lru_walk(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WTI_EVICT_QUEUE *other_queue, *queue;
    WT_TRACK_OP_DECL;
    uint64_t read_gen_oldest;
    uint32_t candidates, entries;

    WT_TRACK_OP_INIT(session);
    conn = S2C(session);
    evict = conn->evict;

    /* Age out the score of how much the queue has been empty recently. */
    if (evict->evict_empty_score > 0)
        --evict->evict_empty_score;

    /* Fill the next queue (that isn't the urgent queue). */
    queue = evict->evict_fill_queue;
    other_queue = evict->evict_queues + (1 - (queue - evict->evict_queues));
    evict->evict_fill_queue = other_queue;

    /* If this queue is full, try the other one. */
    if (__evict_queue_full(queue) && !__evict_queue_full(other_queue))
        queue = other_queue;

    /*
     * If both queues are full and haven't been empty on recent refills, we're done.
     */
    if (__evict_queue_full(queue) && evict->evict_empty_score < WT_EVICT_SCORE_CUTOFF) {
        WT_STAT_CONN_INCR(session, eviction_queue_not_empty);
        goto err;
    }
    /*
     * If the queue we are filling is empty, pages are being requested faster than they are being
     * queued.
     */
    if (__evict_queue_empty(queue, false)) {
        if (F_ISSET(evict, WT_EVICT_CACHE_HARD))
            evict->evict_empty_score =
              WT_MIN(evict->evict_empty_score + WT_EVICT_SCORE_BUMP, WT_EVICT_SCORE_MAX);
        WT_STAT_CONN_INCR(session, eviction_queue_empty);
    } else
        WT_STAT_CONN_INCR(session, eviction_queue_not_empty);

    /*
     * Get some more pages to consider for eviction.
     *
     * If the walk is interrupted, we still need to sort the queue: the next walk assumes there are
     * no entries beyond WTI_EVICT_WALK_BASE.
     */
    if ((ret = __evict_walk(evict->walk_session, queue)) == EBUSY)
        ret = 0;
    WT_ERR_NOTFOUND_OK(ret, false);

    /* Sort the list into LRU order and restart. */
    __wt_spin_lock(session, &queue->evict_lock);

    /*
     * We have locked the queue: in the (unusual) case where we are filling the current queue, mark
     * it empty so that subsequent requests switch to the other queue.
     */
    if (queue == evict->evict_current_queue)
        queue->evict_current = NULL;

    entries = queue->evict_entries;
    /*
     * Style note: __wt_qsort is a macro that can leave a dangling else. Full curly braces are
     * needed here for the compiler.
     */
    if (FLD_ISSET(conn->debug_flags, WT_CONN_DEBUG_EVICT_AGGRESSIVE_MODE)) {
        __wt_qsort(queue->evict_queue, entries, sizeof(WTI_EVICT_ENTRY), __evict_lru_cmp_debug);
    } else {
        __wt_qsort(queue->evict_queue, entries, sizeof(WTI_EVICT_ENTRY), __evict_lru_cmp);
    }

    /* Trim empty entries from the end. */
    while (entries > 0 && queue->evict_queue[entries - 1].ref == NULL)
        --entries;

    /*
     * If we have more entries than the maximum tracked between walks, clear them. Do this before
     * figuring out how many of the entries are candidates so we never end up with more candidates
     * than entries.
     */
    while (entries > WTI_EVICT_WALK_BASE)
        __evict_list_clear(session, &queue->evict_queue[--entries]);

    queue->evict_entries = entries;

    if (entries == 0) {
        /*
         * If there are no entries, there cannot be any candidates. Make sure application threads
         * don't read past the end of the candidate list, or they may race with the next walk.
         */
        queue->evict_candidates = 0;
        queue->evict_current = NULL;
        __wt_spin_unlock(session, &queue->evict_lock);
        goto err;
    }

    /* Decide how many of the candidates we're going to try and evict. */
    if (__wt_evict_aggressive(session))
        queue->evict_candidates = entries;
    else {
        /*
         * Find the oldest read generation apart that we have in the queue, used to set the initial
         * value for pages read into the system. The queue is sorted, find the first "normal"
         * generation.
         */
        read_gen_oldest = WT_READGEN_START_VALUE;
        for (candidates = 0; candidates < entries; ++candidates) {
            WT_READ_ONCE(read_gen_oldest, queue->evict_queue[candidates].score);
            if (!__wti_evict_readgen_is_soon_or_wont_need(&read_gen_oldest))
                break;
        }

        /*
         * Take all candidates if we only gathered pages with an oldest
         * read generation set.
         *
         * We normally never take more than 50% of the entries but if
         * 50% of the entries were at the oldest read generation, take
         * all of them.
         */
        if (__wti_evict_readgen_is_soon_or_wont_need(&read_gen_oldest))
            queue->evict_candidates = entries;
        else if (candidates > entries / 2)
            queue->evict_candidates = candidates;
        else {
            /*
             * Take all of the urgent pages plus a third of ordinary candidates (which could be
             * expressed as WTI_EVICT_WALK_INCR / WTI_EVICT_WALK_BASE). In the steady state, we want
             * to get as many candidates as the eviction walk adds to the queue.
             *
             * That said, if there is only one entry, which is normal when populating an empty file,
             * don't exclude it.
             */
            queue->evict_candidates = 1 + candidates + ((entries - candidates) - 1) / 3;
            if (queue->evict_candidates > entries / 2)
                queue->evict_candidates = entries / 2;

            evict->read_gen_oldest = read_gen_oldest;
        }
    }

    WT_STAT_CONN_INCRV(session, eviction_pages_queued_post_lru, queue->evict_candidates);
    /*
     * Add stats about pages that have been queued.
     */
    for (candidates = 0; candidates < queue->evict_candidates; ++candidates) {
        WT_PAGE *page = queue->evict_queue[candidates].ref->page;
        if (__wt_page_is_modified(page))
            WT_STAT_CONN_DSRC_INCR(session, cache_eviction_pages_queued_dirty);
        else if (page->modify != NULL)
            WT_STAT_CONN_DSRC_INCR(session, cache_eviction_pages_queued_updates);
        else
            WT_STAT_CONN_DSRC_INCR(session, cache_eviction_pages_queued_clean);
    }
    queue->evict_current = queue->evict_queue;
    __wt_spin_unlock(session, &queue->evict_lock);

    /*
     * Signal any application or helper threads that may be waiting to help with eviction.
     */
    __wt_cond_signal(session, conn->evict_threads.wait_cond);

err:
    WT_TRACK_OP_END(session);
    return (ret);
}

/*
 * __evict_walk_choose_dhandle --
 *     Randomly select a dhandle for the next eviction walk
 */
static void
__evict_walk_choose_dhandle(WT_SESSION_IMPL *session, WT_DATA_HANDLE **dhandle_p)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    u_int dh_bucket_count, rnd_bucket, rnd_dh;

    conn = S2C(session);

    WT_ASSERT(session, __wt_rwlock_islocked(session, &conn->dhandle_lock));

#undef RANDOM_DH_SELECTION_ENABLED

#ifdef RANDOM_DH_SELECTION_ENABLED
    *dhandle_p = NULL;

    /*
     * If we don't have many dhandles, most hash buckets will be empty. Just pick a random dhandle
     * from the list in that case.
     */
    if (conn->dhandle_count < conn->dh_hash_size / 4) {
        rnd_dh = __wt_random(&session->rnd_random) % conn->dhandle_count;
        dhandle = TAILQ_FIRST(&conn->dhqh);
        for (; rnd_dh > 0; rnd_dh--)
            dhandle = TAILQ_NEXT(dhandle, q);
        *dhandle_p = dhandle;
        return;
    }

    /*
     * Keep picking up a random bucket until we find one that is not empty.
     */
    do {
        rnd_bucket = __wt_random(&session->rnd_random) & (conn->dh_hash_size - 1);
    } while ((dh_bucket_count = conn->dh_bucket_count[rnd_bucket]) == 0);

    /* We can't pick up an empty bucket with a non zero bucket count. */
    WT_ASSERT(session, !TAILQ_EMPTY(&conn->dhhash[rnd_bucket]));

    /* Pick a random dhandle in the chosen bucket. */
    rnd_dh = __wt_random(&session->rnd_random) % dh_bucket_count;
    dhandle = TAILQ_FIRST(&conn->dhhash[rnd_bucket]);
    for (; rnd_dh > 0; rnd_dh--)
        dhandle = TAILQ_NEXT(dhandle, hashq);
#else
    /* Just step through dhandles. */
    dhandle = *dhandle_p;
    if (dhandle != NULL)
        dhandle = TAILQ_NEXT(dhandle, q);
    if (dhandle == NULL)
        dhandle = TAILQ_FIRST(&conn->dhqh);

    WT_UNUSED(dh_bucket_count);
    WT_UNUSED(rnd_bucket);
    WT_UNUSED(rnd_dh);
#endif

    *dhandle_p = dhandle;
}

/*
 * __evict_btree_dominating_cache --
 *     Return if a single btree is occupying at least half of any of our target's cache usage.
 */
static WT_INLINE bool
__evict_btree_dominating_cache(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_CACHE *cache;
    WT_EVICT *evict;
    uint64_t bytes_dirty;
    uint64_t bytes_max;

    cache = S2C(session)->cache;
    evict = S2C(session)->evict;
    bytes_max = S2C(session)->cache_size + 1;

    if (__wt_cache_bytes_plus_overhead(cache, __wt_atomic_load64(&btree->bytes_inmem)) >
      (uint64_t)(0.5 * evict->eviction_target * bytes_max) / 100)
        return (true);

    bytes_dirty =
      __wt_atomic_load64(&btree->bytes_dirty_intl) + __wt_atomic_load64(&btree->bytes_dirty_leaf);
    if (__wt_cache_bytes_plus_overhead(cache, bytes_dirty) >
      (uint64_t)(0.5 * evict->eviction_dirty_target * bytes_max) / 100)
        return (true);
    if (__wt_cache_bytes_plus_overhead(cache, __wt_atomic_load64(&btree->bytes_updates)) >
      (uint64_t)(0.5 * evict->eviction_updates_target * bytes_max) / 100)
        return (true);

    return (false);
}

/*
 * __evict_walk --
 *     Fill in the array by walking the next set of pages.
 */
static int
__evict_walk(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue)
{
    WT_BTREE *btree;
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_TRACK_OP_DECL;
    uint32_t evict_walk_period;
    u_int loop_count, max_entries, retries, slot, start_slot;
    u_int total_candidates;
    bool dhandle_list_locked;

    WT_TRACK_OP_INIT(session);

    conn = S2C(session);
    cache = conn->cache;
    evict = conn->evict;
    btree = NULL;
    dhandle = NULL;
    dhandle_list_locked = false;
    retries = 0;

    /*
     * Set the starting slot in the queue and the maximum pages added per walk.
     */
    start_slot = slot = queue->evict_entries;
    max_entries = WT_MIN(slot + WTI_EVICT_WALK_INCR, evict->evict_slots);

    /*
     * Another pathological case: if there are only a tiny number of candidate pages in cache, don't
     * put all of them on one queue.
     */
    total_candidates = (u_int)(F_ISSET(evict, WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_UPDATES) ?
        __wt_cache_pages_inuse(cache) :
        __wt_atomic_load64(&cache->pages_dirty_leaf));
    max_entries = WT_MIN(max_entries, 1 + total_candidates / 2);

    if(clearing_cache == true){
        max_entries = (u_int)__wt_cache_pages_inuse(cache);
        // 安全装置：ただし、キューが物理的に保持できる上限は超えない
        if (max_entries > S2C(session)->evict->evict_slots)
            max_entries = S2C(session)->evict->evict_slots;
    }

retry:
    loop_count = 0;
    while (slot < max_entries && loop_count++ < conn->dhandle_count) {
        /* We're done if shutting down or reconfiguring. */
        if (F_ISSET(conn, WT_CONN_CLOSING) || F_ISSET(conn, WT_CONN_RECONFIGURING))
            break;

        /*
         * If another thread is waiting on the eviction server to clear the walk point in a tree,
         * give up.
         */
        if (__wt_atomic_loadv32(&evict->pass_intr) != 0)
            WT_ERR(EBUSY);

        /*
         * Lock the dhandle list to find the next handle and bump its reference count to keep it
         * alive while we sweep.
         */
        if (!dhandle_list_locked) {
            WT_ERR(__evict_lock_handle_list(session));
            dhandle_list_locked = true;
        }

        if (dhandle == NULL) {
            /*
             * On entry, continue from wherever we got to in the scan last time through. If we don't
             * have a saved handle, pick one randomly from the list.
             */
            if ((dhandle = evict->walk_tree) != NULL)
                __evict_set_saved_walk_tree(session, NULL);
            else
                __evict_walk_choose_dhandle(session, &dhandle);
        } else {
            __evict_set_saved_walk_tree(session, NULL);
            __evict_walk_choose_dhandle(session, &dhandle);
        }

        /* If we couldn't find any dhandle, we're done. */
        if (dhandle == NULL)
            break;

        /* Ignore non-btree handles, or handles that aren't open. */
        if (!WT_DHANDLE_BTREE(dhandle) || !F_ISSET(dhandle, WT_DHANDLE_OPEN))
            continue;

        /* Skip files that don't allow eviction. */
        btree = dhandle->handle;
        if (btree->evict_disabled > 0) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_trees_eviction_disabled);
            continue;
        }

        if(clearing_cache == false){
        /*
         * Skip files that are checkpointing if we are only looking for dirty pages.
         */
        if (WT_BTREE_SYNCING(btree) &&
          !F_ISSET(evict, WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_UPDATES)) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_checkpointing_trees);
            continue;
        }

        /*
         * Skip files that are configured to stick in cache until we become aggressive.
         *
         * If the file is contributing heavily to our cache usage then ignore the "stickiness" of
         * its pages.
         */
        if (btree->evict_priority != 0 && !__wt_evict_aggressive(session) &&
          !__evict_btree_dominating_cache(session, btree)) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_trees_stick_in_cache);
            continue;
        }

        if (!evict->use_npos_in_pass) {
            /*
             * Skip files if we have too many active walks.
             *
             * This used to be limited by the configured maximum number of hazard pointers per
             * session. Even though that ceiling has been removed, we need to test eviction with
             * huge numbers of active trees before allowing larger numbers of hazard pointers in the
             * walk session.
             */
            if (btree->evict_ref == NULL && session->hazards.num_active > WTI_EVICT_MAX_TREES) {
                WT_STAT_CONN_INCR(session, eviction_server_skip_trees_too_many_active_walks);
                continue;
            }
        }

        /*
         * If we are filling the queue, skip files that haven't been useful in the past.
         */
        evict_walk_period = __wt_atomic_load32(&btree->evict_walk_period);
        if (evict_walk_period != 0 && btree->evict_walk_skips++ < evict_walk_period) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_trees_not_useful_before);
            continue;
        }
        }
        btree->evict_walk_skips = 0;

        __evict_set_saved_walk_tree(session, dhandle);
        __wt_readunlock(session, &conn->dhandle_lock);
        dhandle_list_locked = false;

        /*
         * Re-check the "no eviction" flag, used to enforce exclusive access when a handle is being
         * closed.
         *
         * Only try to acquire the lock and simply continue if we fail; the lock is held while the
         * thread turning off eviction clears the tree's current eviction point, and part of the
         * process is waiting on this thread to acknowledge that action.
         *
         * If a handle is being discarded, it will still be marked open, but won't have a root page.
         */
        if (btree->evict_disabled == 0 && !__wt_spin_trylock(session, &evict->evict_walk_lock)) {
            if (btree->evict_disabled == 0 && btree->root.page != NULL) {
                if(clearing_cache == false)
                WT_WITH_DHANDLE(
                  session, dhandle, ret = __evict_walk_tree(session, queue, max_entries, &slot));

                else if(clearing_cache == true)
                WT_WITH_DHANDLE(
                  session, dhandle, ret = __my_evict_walk_tree(session, queue, max_entries, &slot));

                WT_ASSERT(session, __wt_session_gen(session, WT_GEN_SPLIT) == 0);
            }
            __wt_spin_unlock(session, &evict->evict_walk_lock);
            WT_ERR(ret);
            /*
             * If there is a checkpoint thread gathering handles, which means it is holding the
             * schema lock, then there is often contention on the evict walk lock with that thread.
             * If eviction is not in aggressive mode, sleep a bit to give the checkpoint thread a
             * chance to gather its handles.
             */
            if (F_ISSET(conn, WT_CONN_CKPT_GATHER) && !__wt_evict_aggressive(session)) {
                __wt_sleep(0, 10);
                WT_STAT_CONN_INCR(session, eviction_walk_sleeps);
            }
        }
    }

    /*
     * Repeat the walks a few times if we don't find enough pages. Give up when we have some
     * candidates and we aren't finding more.
     */
    if (slot < max_entries &&
      (retries < 2 ||
        (retries < WT_RETRY_MAX && (slot == queue->evict_entries || slot > start_slot)))) {
        start_slot = slot;
        ++retries;
        goto retry;
    }

err:
    if (dhandle_list_locked)
        __wt_readunlock(session, &conn->dhandle_lock);

    /*
     * If we didn't find any entries on a walk when we weren't interrupted, let our caller know.
     */
    if (queue->evict_entries == slot && __wt_atomic_loadv32(&evict->pass_intr) == 0)
        ret = WT_NOTFOUND;

    queue->evict_entries = slot;
    WT_TRACK_OP_END(session);
    return (ret);
}

/*
 * __evict_push_candidate --
 *     Initialize a WTI_EVICT_ENTRY structure with a given page.
 */
static bool
__evict_push_candidate(
  WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue, WTI_EVICT_ENTRY *evict_entry, WT_REF *ref)
{
    uint16_t new_flags, orig_flags;
    u_int slot;

    /*
     * Threads can race to queue a page (e.g., an ordinary LRU walk can race with a page being
     * queued for urgent eviction).
     */
    orig_flags = new_flags = ref->page->flags_atomic;
    FLD_SET(new_flags, WT_PAGE_EVICT_LRU);
    if (orig_flags == new_flags ||
      !__wt_atomic_cas16(&ref->page->flags_atomic, orig_flags, new_flags))
        return (false);

    /* Keep track of the maximum slot we are using. */
    slot = (u_int)(evict_entry - queue->evict_queue);
    if (slot >= queue->evict_max)
        queue->evict_max = slot + 1;

    if (evict_entry->ref != NULL)
        __evict_list_clear(session, evict_entry);

    evict_entry->btree = S2BT(session);
    evict_entry->ref = ref;
    evict_entry->score = __evict_entry_priority(session, ref);

    /* Adjust for size when doing dirty eviction. */
    if (F_ISSET(S2C(session)->evict, WT_EVICT_CACHE_DIRTY) &&
      evict_entry->score != WT_READGEN_EVICT_SOON && evict_entry->score != UINT64_MAX &&
      !__wt_page_is_modified(ref->page))
        evict_entry->score +=
          WT_MEGABYTE - WT_MIN(WT_MEGABYTE, __wt_atomic_loadsize(&ref->page->memory_footprint));

    return (true);
}

/*
 * __evict_walk_target --
 *     Calculate how many pages to queue for a given tree.
 */
static uint32_t
__evict_walk_target(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_EVICT *evict;
    uint64_t btree_clean_inuse, btree_dirty_inuse, btree_updates_inuse, bytes_per_slot, cache_inuse;
    uint32_t target_pages, target_pages_clean, target_pages_dirty, target_pages_updates;
    bool want_tree;

    cache = S2C(session)->cache;
    evict = S2C(session)->evict;
    btree_clean_inuse = btree_dirty_inuse = btree_updates_inuse = 0;
    target_pages_clean = target_pages_dirty = target_pages_updates = 0;

/*
 * The minimum number of pages we should consider per tree.
 */
#define MIN_PAGES_PER_TREE 10

    /*
     * The target number of pages for this tree is proportional to the space it is taking up in
     * cache. Round to the nearest number of slots so we assign all of the slots to a tree filling
     * 99+% of the cache (and only have to walk it once).
     */
    if (F_ISSET(evict, WT_EVICT_CACHE_CLEAN)) {
        btree_clean_inuse = __wt_btree_bytes_evictable(session);
        cache_inuse = __wt_cache_bytes_inuse(cache);
        bytes_per_slot = 1 + cache_inuse / evict->evict_slots;
        target_pages_clean = (uint32_t)((btree_clean_inuse + bytes_per_slot / 2) / bytes_per_slot);
    }

    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY)) {
        btree_dirty_inuse = __wt_btree_dirty_leaf_inuse(session);
        cache_inuse = __wt_cache_dirty_leaf_inuse(cache);
        bytes_per_slot = 1 + cache_inuse / evict->evict_slots;
        target_pages_dirty = (uint32_t)((btree_dirty_inuse + bytes_per_slot / 2) / bytes_per_slot);
    }

    if (F_ISSET(evict, WT_EVICT_CACHE_UPDATES)) {
        btree_updates_inuse = __wt_btree_bytes_updates(session);
        cache_inuse = __wt_cache_bytes_updates(cache);
        bytes_per_slot = 1 + cache_inuse / evict->evict_slots;
        target_pages_updates =
          (uint32_t)((btree_updates_inuse + bytes_per_slot / 2) / bytes_per_slot);
    }

    target_pages = WT_MAX(target_pages_clean, target_pages_dirty);
    target_pages = WT_MAX(target_pages, target_pages_updates);

    /*
     * Walk trees with a small fraction of the cache in case there are so many trees that none of
     * them use enough of the cache to be allocated slots. Only skip a tree if it has no bytes of
     * interest.
     */
    if (target_pages == 0) {
        want_tree = (F_ISSET(evict, WT_EVICT_CACHE_CLEAN) && (btree_clean_inuse > 0)) ||
          (F_ISSET(evict, WT_EVICT_CACHE_DIRTY) && (btree_dirty_inuse > 0)) ||
          (F_ISSET(evict, WT_EVICT_CACHE_UPDATES) && (btree_updates_inuse > 0));

        if (!want_tree) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_unwanted_tree);
            return (0);
        }
    }

    /*
     * There is some cost associated with walking a tree. If we're going to visit this tree, always
     * look for a minimum number of pages.
     */
    if (target_pages < MIN_PAGES_PER_TREE)
        target_pages = MIN_PAGES_PER_TREE;

    /* If the tree is dead, take a lot of pages. */
    if (F_ISSET(session->dhandle, WT_DHANDLE_DEAD))
        target_pages *= 10;

    return (target_pages);
}

/*
 * __evict_skip_dirty_candidate --
 *     Check if eviction should skip the dirty page.
 */
static WT_INLINE bool
__evict_skip_dirty_candidate(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_CONNECTION_IMPL *conn;
    WT_TXN *txn;

    conn = S2C(session);
    txn = session->txn;

    /*
     * If the global transaction state hasn't changed since the last time we tried eviction, it's
     * unlikely we can make progress. This heuristic avoids repeated attempts to evict the same
     * page.
     */
    if (!__wt_page_evict_retry(session, page)) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_pages_retry);
        return (true);
    }

    /*
     * If we are under cache pressure, allow evicting pages with newly committed updates to free
     * space. Otherwise, avoid doing that as it may thrash the cache.
     */
    if (F_ISSET(conn->evict, WT_EVICT_CACHE_DIRTY_HARD | WT_EVICT_CACHE_UPDATES_HARD) &&
      F_ISSET(txn, WT_TXN_HAS_SNAPSHOT)) {
        if (!__txn_visible_id(session, __wt_atomic_load64(&page->modify->update_txn)))
            return (true);
    } else if (__wt_atomic_load64(&page->modify->update_txn) >=
      __wt_atomic_loadv64(&conn->txn_global.last_running)) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_pages_last_running);
        return (true);
    }

    return (false);
}

/*
 * __evict_get_target_pages --
 *     Calculate the target pages to add to the queue.
 */
static WT_INLINE uint32_t
__evict_get_target_pages(WT_SESSION_IMPL *session, u_int max_entries, uint32_t slot)
{
    WT_BTREE *btree;
    uint32_t remaining_slots, target_pages;

    btree = S2BT(session);

    /*
     * Figure out how many slots to fill from this tree. Note that some care is taken in the
     * calculation to avoid overflow.
     */
    remaining_slots = max_entries - slot;

    /*
     * For this handle, calculate the number of target pages to evict. If the number of target pages
     * is zero, then simply return early from this function.
     *
     * If the progress has not met the previous target, continue using the previous target.
     */
    target_pages = __evict_walk_target(session);

    if ((target_pages == 0) || btree->evict_walk_progress >= btree->evict_walk_target) {
        btree->evict_walk_target = target_pages;
        btree->evict_walk_progress = 0;
    }
    target_pages = btree->evict_walk_target - btree->evict_walk_progress;

    if (target_pages > remaining_slots)
        target_pages = remaining_slots;

    /*
     * Reduce the number of pages to be selected from btrees other than the history store (HS) if
     * the cache pressure is high and HS content dominates the cache. Evicting unclean non-HS pages
     * can generate even more HS content and will not help with the cache pressure, and will
     * probably just amplify it further.
     */
    if (!WT_IS_HS(btree->dhandle) && __wti_evict_hs_dirty(session)) {
        /* If target pages are less than 10, keep it like that. */
        if (target_pages >= 10) {
            target_pages = target_pages / 10;
            WT_STAT_CONN_DSRC_INCR(session, cache_eviction_target_page_reduced);
        }
    }

    if (target_pages != 0) {
        /*
         * These statistics generate a histogram of the number of pages targeted for eviction each
         * round. The range of values here start at MIN_PAGES_PER_TREE as this is the smallest
         * number of pages we can target, unless there are fewer slots available. The aim is to
         * cover the likely ranges of target pages in as few statistics as possible to reduce the
         * overall overhead.
         */
        if (target_pages < MIN_PAGES_PER_TREE) {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_lt10);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_lt10);
        } else if (target_pages < 32) {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_lt32);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_lt32);
        } else if (target_pages < 64) {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_lt64);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_lt64);
        } else if (target_pages < 128) {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_lt128);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_lt128);
        } else {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_ge128);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_ge128);
        }
    }

    return (target_pages);
}

/*
 * __evict_get_min_pages --
 *     Calculate the minimum pages to visit.
 */
static WT_INLINE uint64_t
__evict_get_min_pages(WT_SESSION_IMPL *session, uint32_t target_pages)
{
    WT_EVICT *evict;
    uint64_t min_pages;

    evict = S2C(session)->evict;

    /*
     * Examine at least a reasonable number of pages before deciding whether to give up. When we are
     * not looking for clean pages, search the tree for longer.
     */
    min_pages = 10 * (uint64_t)target_pages;
    if (F_ISSET(evict, WT_EVICT_CACHE_CLEAN))
        WT_STAT_CONN_INCR(session, eviction_target_strategy_clean);
    else
        min_pages *= 10;
    if (F_ISSET(evict, WT_EVICT_CACHE_UPDATES))
        WT_STAT_CONN_INCR(session, eviction_target_strategy_updates);
    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY))
        WT_STAT_CONN_INCR(session, eviction_target_strategy_dirty);

    return (min_pages);
}

/*
 * __evict_try_restore_walk_position --
 *     Try to restore the walk position from saved soft pos. Returns true if the walk position is
 *     restored.
 */
static WT_INLINE int
__evict_try_restore_walk_position(WT_SESSION_IMPL *session, WT_BTREE *btree, uint32_t walk_flags)
{
#define PATH_STR_MAX 1024
    char path_str[PATH_STR_MAX];
    size_t path_str_offset;
    double unused; /* GCC fails to WT_UNUSED() :( */

    if (btree->evict_ref != NULL)
        return (0); /* We've got a pointer already */
    if (WT_NPOS_IS_INVALID(btree->evict_pos))
        return (0); /* No restore point */
    WT_RET_ONLY(
      __wt_page_from_npos_for_eviction(session, &btree->evict_ref, btree->evict_pos, 0, walk_flags),
      WT_PANIC);

    if (btree->evict_ref != NULL &&
      WT_VERBOSE_LEVEL_ISSET(session, WT_VERB_EVICTION, WT_VERBOSE_DEBUG_1)) {
        WT_UNUSED(unused = __wt_page_npos(session, btree->evict_ref, WT_NPOS_MID, path_str,
                    &path_str_offset, PATH_STR_MAX));
        __wt_verbose_debug1(session, WT_VERB_EVICTION,
          "Evict walk point recalled from position %lf %s page %s ref %p", btree->evict_pos,
          F_ISSET(btree->evict_ref, WT_REF_FLAG_INTERNAL) ? "INTERNAL" : "LEAF", path_str,
          (void *)btree->evict_ref);
    }

    WT_STAT_CONN_INCR(session, eviction_restored_pos);
    if (btree->evict_saved_ref_check != 0 &&
      btree->evict_saved_ref_check != (uint64_t)btree->evict_ref)
        WT_STAT_CONN_INCR(session, eviction_restored_pos_differ);

    return (0);
#undef PATH_STR_MAX
}

/*
 * __evict_walk_prepare --
 *     Choose the walk direction and descend to the initial walk point.
 */
static WT_INLINE int
__evict_walk_prepare(WT_SESSION_IMPL *session, uint32_t *walk_flagsp)
{
    WT_BTREE *btree;
    WT_DECL_RET;

    btree = S2BT(session);

    *walk_flagsp = WT_READ_EVICT_WALK_FLAGS;
    if (!F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT))
        FLD_SET(*walk_flagsp, WT_READ_VISIBLE_ALL);

    WT_RET(__evict_try_restore_walk_position(session, btree, *walk_flagsp));

    if (btree->evict_ref != NULL)
        WT_STAT_CONN_INCR(session, eviction_walk_saved_pos);
    else
        WT_STAT_CONN_INCR(session, eviction_walk_from_root);

    /*
     * Choose a random point in the tree if looking for candidates in a tree with no starting point
     * set. This is mostly aimed at ensuring eviction fairly visits all pages in trees with a lot of
     * in-cache content.
     */
    switch (btree->evict_start_type) {
    case WT_EVICT_WALK_NEXT:
        /* Each time when evict_ref is null, alternate between linear and random walk */
        if (!S2C(session)->evict_legacy_page_visit_strategy && btree->evict_ref == NULL &&
          (++btree->linear_walk_restarts) & 1) {
            if (S2C(session)->evict->use_npos_in_pass)
                /* Alternate with rand_prev so that the start of the tree is visited more often */
                goto rand_prev;
            else
                goto rand_next;
        }
        break;
    case WT_EVICT_WALK_PREV:
        /* Each time when evict_ref is null, alternate between linear and random walk */
        if (!S2C(session)->evict_legacy_page_visit_strategy && btree->evict_ref == NULL &&
          (++btree->linear_walk_restarts) & 1) {
            if (S2C(session)->evict->use_npos_in_pass)
                /* Alternate with rand_next so that the end of the tree is visited more often */
                goto rand_next;
            else
                goto rand_prev;
        }
        FLD_SET(*walk_flagsp, WT_READ_PREV);
        break;
    case WT_EVICT_WALK_RAND_PREV:
rand_prev:
        FLD_SET(*walk_flagsp, WT_READ_PREV);
    /* FALLTHROUGH */
    case WT_EVICT_WALK_RAND_NEXT:
rand_next:
        if (btree->evict_ref == NULL) {
            for (;;) {
                /* Ensure internal pages indexes remain valid */
                WT_WITH_PAGE_INDEX(session,
                  ret = __wt_random_descent(
                    session, &btree->evict_ref, WT_READ_EVICT_READ_FLAGS, &session->rnd_random));
                if (ret != WT_RESTART)
                    break;
                WT_STAT_CONN_INCR(session, eviction_walk_restart);
            }
            WT_RET_NOTFOUND_OK(ret);

            if (btree->evict_ref == NULL)
                WT_STAT_CONN_INCR(session, eviction_walk_random_returns_null_position);
        }
        break;
    }

    return (ret);
}

/*
 * __evict_should_give_up_walk --
 *     Check if we should give up on the current walk.
 */
static WT_INLINE bool
__evict_should_give_up_walk(WT_SESSION_IMPL *session, uint64_t pages_seen, uint64_t pages_queued,
  uint64_t min_pages, uint32_t target_pages)
{
    WT_BTREE *btree;
    bool give_up;

    btree = S2BT(session);

    /*
     * Check whether we're finding a good ratio of candidates vs pages seen. Some workloads create
     * "deserts" in trees where no good eviction candidates can be found. Abandon the walk if we get
     * into that situation.
     */
    give_up = !__wt_evict_aggressive(session) && !WT_IS_HS(btree->dhandle) &&
      pages_seen > min_pages &&
      (pages_queued == 0 || (pages_seen / pages_queued) > (min_pages / target_pages));
    if (give_up) {
        /*
         * Try a different walk start point next time if a walk gave up.
         */
        switch (btree->evict_start_type) {
        case WT_EVICT_WALK_NEXT:
            btree->evict_start_type = WT_EVICT_WALK_PREV;
            break;
        case WT_EVICT_WALK_PREV:
            btree->evict_start_type = WT_EVICT_WALK_RAND_PREV;
            break;
        case WT_EVICT_WALK_RAND_PREV:
            btree->evict_start_type = WT_EVICT_WALK_RAND_NEXT;
            break;
        case WT_EVICT_WALK_RAND_NEXT:
            btree->evict_start_type = WT_EVICT_WALK_NEXT;
            break;
        }

        /*
         * We differentiate the reasons we gave up on this walk and increment the stats accordingly.
         */
        if (pages_queued == 0)
            WT_STAT_CONN_INCR(session, eviction_walks_gave_up_no_targets);
        else
            WT_STAT_CONN_INCR(session, eviction_walks_gave_up_ratio);
    }

    return (give_up);
}

/*
 * __evict_try_queue_page --
 *     Check if we should queue the page for eviction. Queue it to the urgent queue or the regular
 *     queue.
 */
static WT_INLINE void
__evict_try_queue_page(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue, WT_REF *ref,
  WT_PAGE *last_parent, WTI_EVICT_ENTRY *evict_entry, bool *urgent_queuedp, bool *queuedp)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    WT_PAGE *page;
    bool modified, want_page;

    btree = S2BT(session);
    conn = S2C(session);
    evict = conn->evict;
    page = ref->page;
    modified = __wt_page_is_modified(page);
    *queuedp = false;

    /* Don't queue dirty pages in trees during checkpoints. */
    if (modified && WT_BTREE_SYNCING(btree)) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_dirty_pages_during_checkpoint);
        return;
    }

    /*
     * It's possible (but unlikely) to visit a page without a read generation, if we race with the
     * read instantiating the page. Set the page's read generation here to ensure a bug doesn't
     * somehow leave a page without a read generation.
     */
    if (__wt_atomic_load64(&page->read_gen) == WT_READGEN_NOTSET)
        __wti_evict_read_gen_new(session, page);

    /* Pages being forcibly evicted go on the urgent queue. */
    if (modified &&
      (__wt_atomic_load64(&page->read_gen) == WT_READGEN_EVICT_SOON ||
        __wt_atomic_loadsize(&page->memory_footprint) >= btree->splitmempage)) {
        WT_STAT_CONN_INCR(session, eviction_pages_queued_oldest);
        if (__wt_evict_page_urgent(session, ref))
            *urgent_queuedp = true;
        return;
    }

    /*
     * If history store dirty content is dominating the cache, we want to prioritize evicting
     * history store pages over other btree pages. This helps in keeping cache contents below the
     * configured cache size during checkpoints where reconciling non-HS pages can generate a
     * significant amount of HS dirty content very quickly.
     */
    if (WT_IS_HS(btree->dhandle) && __wti_evict_hs_dirty(session)) {
        WT_STAT_CONN_INCR(session, eviction_pages_queued_urgent_hs_dirty);
        if (__wt_evict_page_urgent(session, ref))
            *urgent_queuedp = true;
        return;
    }

    /* Pages that are empty or from dead trees are fast-tracked. */
    if (__wt_page_is_empty(page) || F_ISSET(session->dhandle, WT_DHANDLE_DEAD))
        goto fast;

    /* Skip pages we don't want. */
    want_page = (F_ISSET(evict, WT_EVICT_CACHE_CLEAN) && !modified) ||
      (F_ISSET(evict, WT_EVICT_CACHE_DIRTY) && modified) ||
      (F_ISSET(evict, WT_EVICT_CACHE_UPDATES) && page->modify != NULL);
    if (!want_page) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_unwanted_pages);
        return;
    }

    /*
     * Do not evict a clean metadata page that contains historical data needed to satisfy a reader.
     * Since there is no history store for metadata, we won't be able to serve an older reader if we
     * evict this page.
     */
    if (WT_IS_METADATA(session->dhandle) && F_ISSET(evict, WT_EVICT_CACHE_CLEAN_HARD) &&
      F_ISSET(ref, WT_REF_FLAG_LEAF) && !modified && page->modify != NULL &&
      !__wt_txn_visible_all(session, page->modify->rec_max_txn, page->modify->rec_max_timestamp)) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_metatdata_with_history);
        return;
    }

    /*
     * Don't attempt eviction of internal pages with children in cache (indicated by seeing an
     * internal page that is the parent of the last page we saw).
     *
     * Also skip internal page unless we get aggressive, the tree is idle (indicated by the tree
     * being skipped for walks), or we are in eviction debug mode. The goal here is that if trees
     * become completely idle, we eventually push them out of cache completely.
     */
    if (!FLD_ISSET(conn->debug_flags, WT_CONN_DEBUG_EVICT_AGGRESSIVE_MODE) &&
      F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
        if (page == last_parent) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_intl_page_with_active_child);
            return;
        }
        if (__wt_atomic_load32(&btree->evict_walk_period) == 0 && !__wt_evict_aggressive(session))
            return;
    }

    /* Evaluate dirty page candidacy, when eviction is not aggressive. */
    if (!__wt_evict_aggressive(session) && modified && __evict_skip_dirty_candidate(session, page))
        return;

fast:
    /* If the page can't be evicted, give up. */
    if (!__wt_page_can_evict(session, ref, NULL))
        return;

    WT_ASSERT(session, evict_entry->ref == NULL);
    if (!__evict_push_candidate(session, queue, evict_entry, ref))
        return;

    *queuedp = true;
    __wt_verbose_debug2(session, WT_VERB_EVICTION, "walk select: %p, size %" WT_SIZET_FMT,
      (void *)page, __wt_atomic_loadsize(&page->memory_footprint));

    return;
}

/*
 * __evict_walk_tree --
 *     Get a few page eviction candidates from a single underlying file.
 */
static int
__evict_walk_tree(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue, u_int max_entries, u_int *slotp)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WTI_EVICT_ENTRY *end, *evict_entry, *start;
    WT_PAGE *last_parent, *page;
    WT_REF *ref;
    WT_TXN *txn;
    uint64_t internal_pages_already_queued, internal_pages_queued, internal_pages_seen;
    uint64_t min_pages, pages_already_queued, pages_queued, pages_seen, refs_walked;
    uint64_t pages_seen_clean, pages_seen_dirty, pages_seen_updates;
    uint32_t evict_walk_period, target_pages, walk_flags;
    int restarts;
    bool give_up, queued, urgent_queued;

    conn = S2C(session);
    btree = S2BT(session);
    evict = conn->evict;
    last_parent = NULL;
    restarts = 0;
    give_up = urgent_queued = false;
    txn = session->txn;

    WT_ASSERT_SPINLOCK_OWNED(session, &evict->evict_walk_lock);

    start = queue->evict_queue + *slotp;
    target_pages = __evict_get_target_pages(session, max_entries, *slotp);

    /* If we don't want any pages from this tree, move on. */
    if (target_pages == 0)
        return (0);

    end = start + target_pages;

    min_pages = __evict_get_min_pages(session, target_pages);

    WT_RET_NOTFOUND_OK(__evict_walk_prepare(session, &walk_flags));

    /*
     * Get some more eviction candidate pages, starting at the last saved point. Clear the saved
     * point immediately, we assert when discarding pages we're not discarding an eviction point, so
     * this clear must be complete before the page is released.
     */
    ref = btree->evict_ref;
    btree->evict_ref = NULL;
    /* Clear the saved position just in case we never put it back. */
    __wt_evict_clear_npos(btree);

    /*
     * Get the snapshot for the eviction server when we want to evict dirty content under cache
     * pressure. This snapshot is used to check for the visibility of the last modified transaction
     * id on the page.
     */
    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY_HARD | WT_EVICT_CACHE_UPDATES_HARD))
        __wt_txn_bump_snapshot(session);

    /*
     * !!! Take care terminating this loop.
     *
     * Don't make an extra call to __wt_tree_walk after we hit the end of a
     * tree: that will leave a page pinned, which may prevent any work from
     * being done.
     *
     * Once we hit the page limit, do one more step through the walk in
     * case we are appending and only the last page in the file is live.
     */
    internal_pages_already_queued = internal_pages_queued = internal_pages_seen = 0;
    pages_seen_clean = pages_seen_dirty = pages_seen_updates = 0;
    for (evict_entry = start, pages_already_queued = pages_queued = pages_seen = refs_walked = 0;
         evict_entry < end && (ret == 0 || ret == WT_NOTFOUND);
         last_parent = ref == NULL ? NULL : ref->home,
        ret = __wt_tree_walk_count(session, &ref, &refs_walked, walk_flags)) {

        if ((give_up = __evict_should_give_up_walk(
               session, pages_seen, pages_queued, min_pages, target_pages)))
            break;

        if (ref == NULL) {
            WT_STAT_CONN_INCR(session, eviction_walks_ended);

            if (++restarts == 2) {
                WT_STAT_CONN_INCR(session, eviction_walks_stopped);
                break;
            }
            WT_STAT_CONN_INCR(session, eviction_walks_started);
            continue;
        }

        ++pages_seen;

        /* Ignore root pages entirely. */
        if (__wt_ref_is_root(ref))
            continue;

        page = ref->page;

        /*
         * Update the maximum evict pass generation gap seen at time of eviction. This helps track
         * how long it's been since a page was last queued for eviction. We need to update the
         * statistic here during the walk and not at __evict_page because the evict_pass_gen is
         * reset here.
         */
        if (page->evict_pass_gen == 0) {
            const uint64_t gen_gap =
              __wt_atomic_load64(&evict->evict_pass_gen) - page->cache_create_gen;
            if (gen_gap > __wt_atomic_load64(&evict->evict_max_unvisited_gen_gap))
                __wt_atomic_store64(&evict->evict_max_unvisited_gen_gap, gen_gap);
            if (gen_gap > __wt_atomic_load64(&evict->evict_max_unvisited_gen_gap_per_checkpoint))
                __wt_atomic_store64(&evict->evict_max_unvisited_gen_gap_per_checkpoint, gen_gap);
        } else {
            const uint64_t gen_gap =
              __wt_atomic_load64(&evict->evict_pass_gen) - page->evict_pass_gen;
            if (gen_gap > __wt_atomic_load64(&evict->evict_max_visited_gen_gap))
                __wt_atomic_store64(&evict->evict_max_visited_gen_gap, gen_gap);
            if (gen_gap > __wt_atomic_load64(&evict->evict_max_visited_gen_gap_per_checkpoint))
                __wt_atomic_store64(&evict->evict_max_visited_gen_gap_per_checkpoint, gen_gap);
        }

        page->evict_pass_gen = __wt_atomic_load64(&evict->evict_pass_gen);

        if (__wt_page_is_modified(page))
            ++pages_seen_dirty;
        else if (page->modify != NULL)
            ++pages_seen_updates;
        else
            ++pages_seen_clean;

        /* Count internal pages seen. */
        if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
            internal_pages_seen++;

        /* Use the EVICT_LRU flag to avoid putting pages onto the list multiple times. */
        if (F_ISSET_ATOMIC_16(page, WT_PAGE_EVICT_LRU)) {
            pages_already_queued++;
            if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
                internal_pages_already_queued++;
            continue;
        }

        __evict_try_queue_page(
          session, queue, ref, last_parent, evict_entry, &urgent_queued, &queued);

        if (queued) {
            ++evict_entry;
            ++pages_queued;
            ++btree->evict_walk_progress;

            /* Count internal pages queued. */
            if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
                internal_pages_queued++;
        }
    }
    if (F_ISSET(txn, WT_TXN_HAS_SNAPSHOT))
        __wt_txn_release_snapshot(session);
    WT_RET_NOTFOUND_OK(ret);

    *slotp += (u_int)(evict_entry - start);
    WT_STAT_CONN_INCRV(session, eviction_pages_ordinary_queued, (u_int)(evict_entry - start));

    __wt_verbose_debug2(session, WT_VERB_EVICTION,
      "%s walk: target %" PRIu32 ", seen %" PRIu64 ", queued %" PRIu64, session->dhandle->name,
      target_pages, pages_seen, pages_queued);

    /* If we couldn't find the number of pages we were looking for, skip the tree next time. */
    evict_walk_period = __wt_atomic_load32(&btree->evict_walk_period);
    if (pages_queued < target_pages / 2 && !urgent_queued)
        __wt_atomic_store32(
          &btree->evict_walk_period, WT_MIN(WT_MAX(1, 2 * evict_walk_period), 100));
    else if (pages_queued == target_pages) {
        __wt_atomic_store32(&btree->evict_walk_period, 0);
        /*
         * If there's a chance the Btree was fully evicted, update the evicted flag in the handle.
         */
        if (__wt_btree_bytes_evictable(session) == 0)
            FLD_SET(session->dhandle->advisory_flags, WT_DHANDLE_ADVISORY_EVICTED);
    } else if (evict_walk_period > 0)
        __wt_atomic_store32(&btree->evict_walk_period, evict_walk_period / 2);

    /*
     * Give up the walk occasionally.
     *
     * If we happen to end up on the root page or a page requiring urgent eviction, clear it. We
     * have to track hazard pointers, and the root page complicates that calculation.
     *
     * Likewise if we found no new candidates during the walk: there is no point keeping a page
     * pinned, since it may be the only candidate in an idle tree.
     *
     * If we land on a page requiring forced eviction, or that isn't an ordinary in-memory page,
     * move until we find an ordinary page: we should not prevent exclusive access to the page until
     * the next walk.
     */
    if (ref != NULL) {
        if (__wt_ref_is_root(ref) || evict_entry == start || give_up ||
          __wt_atomic_loadsize(&ref->page->memory_footprint) >= btree->splitmempage) {
            if (restarts == 0)
                WT_STAT_CONN_INCR(session, eviction_walks_abandoned);
            WT_RET(__wt_page_release(evict->walk_session, ref, walk_flags));
            ref = NULL;
        } else {
            while (ref != NULL &&
              (WT_REF_GET_STATE(ref) != WT_REF_MEM ||
                __wti_evict_readgen_is_soon_or_wont_need(&ref->page->read_gen)))
                WT_RET_NOTFOUND_OK(__wt_tree_walk_count(session, &ref, &refs_walked, walk_flags));
        }
        btree->evict_ref = ref;
        if (evict->use_npos_in_pass)
            __evict_clear_walk(session, false);
    }

    WT_STAT_CONN_INCRV(session, eviction_walk, refs_walked);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen, pages_seen);
    WT_STAT_CONN_INCRV(session, eviction_pages_already_queued, pages_already_queued);
    WT_STAT_CONN_INCRV(session, eviction_internal_pages_seen, internal_pages_seen);
    WT_STAT_CONN_INCRV(
      session, eviction_internal_pages_already_queued, internal_pages_already_queued);
    WT_STAT_CONN_INCRV(session, eviction_internal_pages_queued, internal_pages_queued);
    WT_STAT_CONN_DSRC_INCR(session, eviction_walk_passes);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_clean, pages_seen_clean);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_dirty, pages_seen_dirty);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_updates, pages_seen_updates);
    return (0);
}

/*
 * __evict_get_ref --
 *     Get a page for eviction.
 */
static int
__evict_get_ref(WT_SESSION_IMPL *session, bool is_server, WT_BTREE **btreep, WT_REF **refp,
  WT_REF_STATE *previous_statep)
{
    WT_EVICT *evict;
    WTI_EVICT_ENTRY *evict_entry;
    WTI_EVICT_QUEUE *queue, *other_queue, *urgent_queue;
    WT_REF_STATE previous_state;
    uint32_t candidates;
    bool is_app, server_only, urgent_ok;
    WT_CONNECTION_IMPL *conn;

    *btreep = NULL;
    conn = S2C(session);
    /*
     * It is polite to initialize output variables, but it isn't safe for callers to use the
     * previous state if we don't return a locked ref.
     */
    *previous_statep = WT_REF_MEM;
    *refp = NULL;

    evict = S2C(session)->evict;
    is_app = !F_ISSET(session, WT_SESSION_INTERNAL);
    server_only = is_server && !WT_EVICT_HAS_WORKERS(session);
    /* Application threads do eviction when cache is full of dirty data */
    urgent_ok = (!is_app && !is_server) || !WT_EVICT_HAS_WORKERS(session) ||
      (is_app && F_ISSET(evict, WT_EVICT_CACHE_DIRTY_HARD));
    urgent_queue = evict->evict_urgent_queue;

    if (clearing_cache == true) {
        // もし current と other が同じキューを指していたらバグなので直す
        if (evict->evict_current_queue == evict->evict_other_queue) {
            my_log("WARNING: Queue pointers aliased! Fixing...\n");
            // 強制的に [0] と [1] に割り振り直す
            evict->evict_current_queue = &evict->evict_queues[0];
            evict->evict_other_queue   = &evict->evict_queues[1];
        }
    }
    /* Avoid the LRU lock if no pages are available. */
    if (__evict_queue_empty(evict->evict_current_queue, is_server) &&
      __evict_queue_empty(evict->evict_other_queue, is_server) &&
      (!urgent_ok || __evict_queue_empty(urgent_queue, false))) {
        WT_STAT_CONN_INCR(session, eviction_get_ref_empty);
        if(clearing_cache == false)
        return (WT_NOTFOUND);
    }

    /*
     * The server repopulates whenever the other queue is not full, as long as at least one page has
     * been evicted out of the current queue.
     *
     * Note that there are pathological cases where there are only enough eviction candidates in the
     * cache to fill one queue. In that case, we will continually evict one page and attempt to
     * refill the queues. Such cases are extremely rare in real applications.
     */
    if (clearing_cache == false) {
    if (is_server && (!urgent_ok || __evict_queue_empty(urgent_queue, false)) &&
      !__evict_queue_full(evict->evict_current_queue) &&
      !__evict_queue_full(evict->evict_fill_queue) &&
      (evict->evict_empty_score > WT_EVICT_SCORE_CUTOFF ||
        __evict_queue_empty(evict->evict_fill_queue, false)))
        return (WT_NOTFOUND);
      }

    __wt_spin_lock(session, &evict->evict_queue_lock);

    /* Check the urgent queue first. */
    if (urgent_ok && !__evict_queue_empty(urgent_queue, false))
        queue = urgent_queue;
    else {
        /*
         * Check if the current queue needs to change.
         *
         * The server will only evict half of the pages before looking for more, but should only
         * switch queues if there are no other eviction workers.
         */
        queue = evict->evict_current_queue;
        other_queue = evict->evict_other_queue;
        if (__evict_queue_empty(queue, server_only) &&
          !__evict_queue_empty(other_queue, server_only)) {
            evict->evict_current_queue = other_queue;
            evict->evict_other_queue = queue;
        }
    }

    WTI_EVICT_ENTRY *entry;
    uint32_t i;
    if(clearing_cache == true){
        for (i = 0; i < queue->evict_entries; i++) {
            entry = &queue->evict_queue[i];
            if (entry->ref != NULL && F_ISSET(entry->ref, WT_REF_FLAG_LEAF)) break;
        }
        if((i == queue->evict_entries) || (queue->evict_entries == 0)){
            other_queue = evict->evict_other_queue;
            evict->evict_current_queue = other_queue;
            evict->evict_other_queue = queue;
            queue = other_queue;            

        }
        /*
        if (__evict_queue_empty(evict->evict_current_queue, is_server) &&
        __evict_queue_empty(evict->evict_other_queue, is_server)){            
            queue = evict->evict_current_queue;
            other_queue = evict->evict_current_queue + (1 - (queue - evict->evict_current_queue));
            evict->evict_current_queue = other_queue;
            evict->evict_other_queue = queue;
            queue = evict->evict_other_queue;
        }
*/
        if(stable_count > 2 || __wt_cache_pages_inuse(conn->cache) < 30){
        //printf("(__evict_get_ref):-> Page size: %" PRIu64 " pages\n", __wt_cache_pages_inuse(conn->cache));
        //printf("///// queue (__evict_get_ref)/////\n");
        //__my_dump_eviction_queue(evict->evict_current_queue);
        //printf("///// other_queue (__evict_get_ref)/////\n");
        //__my_dump_eviction_queue(evict->evict_other_queue);

        }
    }

    __wt_spin_unlock(session, &evict->evict_queue_lock);

    /*
     * We got the queue lock, which should be fast, and chose a queue. Now we want to get the lock
     * on the individual queue.
     */
    for (;;) {
        /* Verify there are still pages available. */
        if (__evict_queue_empty(queue, is_server && queue != urgent_queue)) {
            if (clearing_cache == true) {
                // どのキューが空判定されたのかもわかるようにしておくと便利です
                const char *target_q_name = "Unknown";
                if (queue == evict->evict_current_queue) target_q_name = "Current";
                else if (queue == evict->evict_other_queue) target_q_name = "Other";
                else if (queue == evict->evict_urgent_queue) target_q_name = "Urgent";

                my_log("[__evict_get_ref:EMPTY] Target: %s | Current: %u, Other: %u, Urgent: %u\n",
                       target_q_name,
                       evict->evict_current_queue ? evict->evict_current_queue->evict_entries : 0,
                       evict->evict_other_queue ? evict->evict_other_queue->evict_entries : 0,
                       evict->evict_urgent_queue ? evict->evict_urgent_queue->evict_entries : 0);
            }
            WT_STAT_CONN_INCR(session, eviction_get_ref_empty2);
            return (WT_NOTFOUND);
        }
        if (!is_server)
            __wt_spin_lock(session, &queue->evict_lock);
        else if (__wt_spin_trylock(session, &queue->evict_lock) != 0)
            continue;
        break;
    }

    /*
     * Only evict half of the pages before looking for more. The remainder are left to eviction
     * workers (if configured), or application thread if necessary.
     */
    candidates = queue->evict_candidates;
    if (is_server && queue != urgent_queue && candidates > 1)
        candidates /= 2;

    if(clearing_cache == true) candidates = __wt_cache_pages_inuse(conn->cache);

    if (clearing_cache == true) {
        // 現在のキューが「メイン」か「その他」か「緊急」かを判定（アドレス比較）
        const char *q_name = "UNKNOWN";
        if (queue == evict->evict_fill_queue) q_name = "FILL_QUEUE";
        else if (queue == evict->evict_current_queue) q_name = "CURRENT_QUEUE";
        else if (queue == evict->evict_other_queue) q_name = "OTHER_QUEUE";
        else if (queue == evict->evict_urgent_queue) q_name = "URGENT_QUEUE";

        // 現在の処理位置（インデックス）を計算
        long current_idx = -1;
        if (queue->evict_current >= queue->evict_queue) {
            current_idx = queue->evict_current - queue->evict_queue;
        }

        my_log("[__evict_get_ref] Queue: %s (%p), Entries: %u, Candidates: %u (Local limit: %u), Start Index: %ld\n",
               q_name, (void *)queue, 
               queue->evict_entries, 
               queue->evict_candidates, 
               candidates,
               current_idx);
    }

    /* Get the next page queued for eviction. */
    for (evict_entry = queue->evict_current;
         evict_entry >= queue->evict_queue && evict_entry < queue->evict_queue + candidates;
         ++evict_entry) {
        if (evict_entry->ref == NULL)
            continue;
        WT_ASSERT(session, evict_entry->btree != NULL);

        /*
         * Evicting a dirty page in the server thread could stall during a write and prevent
         * eviction from finding new work.
         *
         * However, we can't skip entries in the urgent queue or they may never be found again.
         *
         * Don't force application threads to evict dirty pages if they aren't stalled by the amount
         * of dirty data in cache.
         */
        if(clearing_cache == false){
        if (!urgent_ok &&
          (is_server || !F_ISSET(evict, WT_EVICT_CACHE_DIRTY_HARD | WT_EVICT_CACHE_UPDATES_HARD)) &&
          __wt_page_is_modified(evict_entry->ref->page)) {
            --evict_entry;
            break;
        }
        }

        /*
         * Lock the page while holding the eviction mutex to prevent multiple attempts to evict it.
         * For pages that are already being evicted, this operation will fail and we will move on.
         */
        if ((previous_state = WT_REF_GET_STATE(evict_entry->ref)) != WT_REF_MEM ||
          !WT_REF_CAS_STATE(session, evict_entry->ref, previous_state, WT_REF_LOCKED)) {
            __evict_list_clear(session, evict_entry);
            continue;
        }

        /*
         * Increment the busy count in the btree handle to prevent it from being closed under us.
         */
        (void)__wt_atomic_addv32(&evict_entry->btree->evict_busy, 1);

        *btreep = evict_entry->btree;
        *refp = evict_entry->ref;
        *previous_statep = previous_state;

        /*
         * Remove the entry so we never try to reconcile the same page on reconciliation error.
         */
        __evict_list_clear(session, evict_entry);
        break;
    }

    /* Move to the next item. */
    if (evict_entry != NULL && evict_entry + 1 < queue->evict_queue + queue->evict_candidates)
        queue->evict_current = evict_entry + 1;
    else /* Clear the current pointer if there are no more candidates. */
        queue->evict_current = NULL;

    __wt_spin_unlock(session, &queue->evict_lock);

    return (*refp == NULL ? WT_NOTFOUND : 0);
}

/*
 * __evict_page --
 *     Called by both eviction and application threads to evict a page.
 */
static int
__evict_page(WT_SESSION_IMPL *session, bool is_server)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_REF *ref;
    WT_REF_STATE previous_state;
    WT_TRACK_OP_DECL;
    uint64_t time_start, time_stop;
    uint32_t flags;
    bool page_is_modified;
    CACHE_PAGE_INFO page_info_buffer; // ★ メタデータを一時的に保持するバッファ

    WT_TRACK_OP_INIT(session);

    //1ページだけのクリア＆再構成のためのコード追加
    //if (clearing_cache == true && metadata_count != 0) return 0;

    //WT_RET_TRACK(__evict_get_ref(session, is_server, &btree, &ref, &previous_state));
// 呼び出し実行
    int get_ref_ret = __evict_get_ref(session, is_server, &btree, &ref, &previous_state);

    // ★ログ追加: 結果確認
    if (get_ref_ret != 0) {
        my_log("__evict_get_ref failed with code: %d (Skipping eviction)\n", get_ref_ret);
        return (get_ref_ret); // エラーならここで帰る（WT_RET_TRACK相当）
    }

    WT_RET_TRACK(get_ref_ret);
    WT_ASSERT(session, WT_REF_GET_STATE(ref) == WT_REF_LOCKED);

    time_start = 0;

    flags = 0;
    page_is_modified = false;

    /*
     * An internal session flags either the server itself or an eviction worker thread.
     */
    if (is_server)
        WT_STAT_CONN_INCR(session, eviction_server_evict_attempt);
    else if (F_ISSET(session, WT_SESSION_INTERNAL))
        WT_STAT_CONN_INCR(session, eviction_worker_evict_attempt);
    else {
        if (__wt_page_is_modified(ref->page)) {
            page_is_modified = true;
            WT_STAT_CONN_INCR(session, eviction_app_dirty_attempt);
        }
        WT_STAT_CONN_INCR(session, eviction_app_attempt);
        S2C(session)->evict->app_evicts++;
        time_start = WT_STAT_ENABLED(session) ? __wt_clock(session) : 0;
    }

    /*
     * In case something goes wrong, don't pick the same set of pages every time.
     *
     * We used to bump the page's read generation only if eviction failed, but that isn't safe: at
     * that point, eviction has already unlocked the page and some other thread may have evicted it
     * by the time we look at it.
     */
    __wti_evict_read_gen_bump(session, ref->page);

    // クリアしたページをmetadatalistに保存する
    // ▼▼▼ ステップ1: 退避「前」にメタデータを一時バッファに保存 ▼▼▼
    if (clearing_cache == true && pthread_equal(pthread_self(), clearing_thread_id)) {
        if (__wt_ref_is_root(ref) || F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
            my_log("[SKIP] Ignoring Internal/Root page: %p", ref);
            ret = 0; // 正常終了として扱う
        } else 
        {
        // 既存の出力・保存関数を呼び出して、バッファにメタデータを保存
        save_page_info_to_buffer(btree, ref, &page_info_buffer);
        // QEMUへの通知
        uintptr_t GVA = (uintptr_t)ref->page; // 仮想アドレス
        uintptr_t GPA = GVA_to_GPA(ref->page); // 物理アドレス
        // 2. ページの退避を試行
        /*
        if (ref == NULL) {
            my_log("[PRE-EVICT CRITICAL] ref is NULL!");
        } else if (ref->page == NULL) {
            my_log("[PRE-EVICT CRITICAL] ref exists (%p) but page is NULL!", (void *)ref);
        } else {
        // 正常ケース: アドレスを出力
            my_log("[PRE-EVICT] Ready to evict. ref=%p, page=%p", (void *)ref, (void *)ref->page);
        }
        my_log("\n");
        */

        print_clear_page_info(session, "BEFORE", btree, ref, 0);
        WT_WITH_BTREE(session, btree, ret = __wt_evict(session, ref, previous_state, 0));
        print_clear_page_info(session, "AFTER", btree, ref, 1);
        my_log("\n");

        // ▼▼▼ ステップ2: 結果を確認し、成功した場合のみグローバルリストに保存 ▼▼▼
        // 退避が成功し、かつ状態がDISKになった場合のみ保存
        //if (ret == 0 && WT_REF_GET_STATE(ref) == WT_REF_DISK) {
        if (ret == 0) {
            //printf(" -> Eviction SUCCESS. Saving metadata for page.\n");
            
            // リストの容量が足りなければ拡張する
            if (metadata_count >= metadata_capacity) {
                metadata_capacity = (metadata_capacity == 0) ? 1024 : metadata_capacity * 2;
                metadata_list = realloc(metadata_list, sizeof(CACHE_PAGE_INFO) * metadata_capacity);
            }
            // バッファからグローバルリストにコピー
            if (metadata_list != NULL) {
                metadata_list[metadata_count] = page_info_buffer;
                metadata_count++;
            }
            add_mongoDB_evict_List(GVA, GPA);
        } else {
            // 失敗した場合は、理由を出力して何もしない
            if (WT_REF_GET_STATE(ref) == WT_REF_SPLIT)
                printf(" -> Page was SPLIT. Metadata NOT saved.\n");
            else
                printf(" -> Eviction FAILED with code %d. Metadata NOT saved.\n", ret);
        }
        }
    }
    else
    WT_WITH_BTREE(session, btree, ret = __wt_evict(session, ref, previous_state, flags));

    (void)__wt_atomic_subv32(&btree->evict_busy, 1);

    if (time_start != 0) {
        time_stop = __wt_clock(session);
        WT_STAT_CONN_INCRV(session, eviction_app_time, WT_CLOCKDIFF_US(time_stop, time_start));
    }

    if (WT_UNLIKELY(ret != 0)) {
        if (is_server)
            WT_STAT_CONN_INCR(session, eviction_server_evict_fail);
        else if (F_ISSET(session, WT_SESSION_INTERNAL))
            WT_STAT_CONN_INCR(session, eviction_worker_evict_fail);
        else {
            if (page_is_modified)
                WT_STAT_CONN_INCR(session, eviction_app_dirty_fail);
            WT_STAT_CONN_INCR(session, eviction_app_fail);
        }
    }

    WT_TRACK_OP_END(session);
    return (ret);
}

/*
 * __wti_evict_app_assist_worker --
 *     Worker function for __wt_evict_app_assist_worker_check: evict pages if the cache crosses
 *     eviction trigger thresholds.
 *
 * The function returns an error code from either __evict_page or __wt_txn_is_blocking.
 */
int
__wti_evict_app_assist_worker(
  WT_SESSION_IMPL *session, bool busy, bool readonly, bool interruptible)
{
    WT_DECL_RET;
    WT_TRACK_OP_DECL;

    WT_TRACK_OP_INIT(session);

    WT_CONNECTION_IMPL *conn = S2C(session);
    WT_EVICT *evict = conn->evict;
    uint64_t time_start = 0;
    WT_TXN_GLOBAL *txn_global = &conn->txn_global;
    WT_TXN_SHARED *txn_shared = WT_SESSION_TXN_SHARED(session);

    uint64_t cache_max_wait_us =
      session->cache_max_wait_us != 0 ? session->cache_max_wait_us : evict->cache_max_wait_us;

    /*
     * Before we enter the eviction generation, make sure this session has a cached history store
     * cursor, otherwise we can deadlock with a session wanting exclusive access to a handle: that
     * session will have a handle list write lock and will be waiting on eviction to drain, we'll be
     * inside eviction waiting on a handle list read lock to open a history store cursor. The
     * eviction server should be started at this point so it is safe to open the history store.
     */
    WT_ERR(__wt_curhs_cache(session));

    /* Wake the eviction server if we need to do work. */
    __wt_evict_server_wake(session);

    /* Track how long application threads spend doing eviction. */
    if (!F_ISSET(session, WT_SESSION_INTERNAL))
        time_start = __wt_clock(session);

    /*
     * Note that this for loop is designed to reset expected eviction error codes before exiting,
     * namely, the busy return and empty eviction queue. We do not need the calling functions to
     * have to deal with internal eviction return codes.
     */
    for (uint64_t initial_progress = __wt_atomic_loadv64(&evict->eviction_progress);; ret = 0) {
        /*
         * If eviction is stuck, check if this thread is likely causing problems and should be
         * rolled back. Ignore if in recovery, those transactions can't be rolled back.
         */
        if (!F_ISSET(conn, WT_CONN_RECOVERING) && __wt_evict_cache_stuck(session)) {
            ret = __wt_txn_is_blocking(session);
            if (ret == WT_ROLLBACK) {
                __wt_atomic_decrement_if_positive(&evict->evict_aggressive_score);
                if (F_ISSET(session, WT_SESSION_SAVE_ERRORS))
                    __wt_verbose_debug1(session, WT_VERB_TRANSACTION, "rollback reason: %s",
                      session->err_info.err_msg);
            }
            WT_ERR(ret);
        }

        /*
         * Check if we've exceeded our operation timeout, this would also get called from the
         * previous txn is blocking call, however it won't pickup transactions that have been
         * committed or rolled back as their mod count is 0, and that txn needs to be the oldest.
         *
         * Additionally we don't return rollback which could confuse the caller.
         */
        if (__wt_op_timer_fired(session))
            break;

        /* Check if we have exceeded the global or the session timeout for waiting on the cache. */
        if (time_start != 0 && cache_max_wait_us != 0) {
            uint64_t time_stop = __wt_clock(session);
            if (session->cache_wait_us + WT_CLOCKDIFF_US(time_stop, time_start) > cache_max_wait_us)
                break;
        }

        /*
         * Check if we have become busy.
         *
         * If we're busy (because of the transaction check we just did or because our caller is
         * waiting on a longer-than-usual event such as a page read), and the cache level drops
         * below 100%, limit the work to 5 evictions and return. If that's not the case, we can do
         * more.
         */
        if (!busy && __wt_atomic_loadv64(&txn_shared->pinned_id) != WT_TXN_NONE &&
          __wt_atomic_loadv64(&txn_global->current) != __wt_atomic_loadv64(&txn_global->oldest_id))
            busy = true;
        uint64_t max_progress = busy ? 5 : 20;

        /* See if eviction is still needed. */
        double pct_full;
        if (!__wt_evict_needed(session, busy, readonly, &pct_full) ||
          (pct_full < 100.0 &&
            (__wt_atomic_loadv64(&evict->eviction_progress) > initial_progress + max_progress)))
            break;

        if (!__evict_check_user_ok_with_eviction(session, interruptible))
            break;

        /* Evict a page. */
        ret = __evict_page(session, false);
        if (ret == 0) {
            /* If the caller holds resources, we can stop after a successful eviction. */
            if (busy)
                break;
        } else if (ret == WT_NOTFOUND) {
            /* Allow the queue to re-populate before retrying. */
            __wt_cond_wait(session, conn->evict_threads.wait_cond, 10 * WT_THOUSAND, NULL);
            evict->app_waits++;
        } else if (ret != EBUSY)
            WT_ERR(ret);

        /* Update elapsed cache metrics. */
        if (time_start != 0) {
            uint64_t time_stop = __wt_clock(session);
            uint64_t elapsed = WT_CLOCKDIFF_US(time_stop, time_start);
            WT_STAT_CONN_INCRV(session, application_cache_time, elapsed);
            if (!interruptible) {
                WT_STAT_CONN_INCRV(session, application_cache_uninterruptible_time, elapsed);
                WT_STAT_SESSION_INCRV(session, cache_time_mandatory, elapsed);
            } else {
                WT_STAT_CONN_INCRV(session, application_cache_interruptible_time, elapsed);
                WT_STAT_SESSION_INCRV(session, cache_time_interruptible, elapsed);
            }
            session->cache_wait_us += elapsed;
            time_start = time_stop;
        }
    }

err:
    if (time_start != 0) {
        uint64_t time_stop = __wt_clock(session);
        uint64_t elapsed = WT_CLOCKDIFF_US(time_stop, time_start);
        WT_STAT_CONN_INCR(session, application_cache_ops);
        WT_STAT_CONN_INCRV(session, application_cache_time, elapsed);
        WT_STAT_SESSION_INCRV(session, cache_time, elapsed);
        if (!interruptible) {
            WT_STAT_CONN_INCR(session, application_cache_uninterruptible_ops);
            WT_STAT_CONN_INCRV(session, application_cache_uninterruptible_time, elapsed);
            WT_STAT_SESSION_INCRV(session, cache_time_mandatory, elapsed);
        } else {
            WT_STAT_CONN_INCR(session, application_cache_interruptible_ops);
            WT_STAT_CONN_INCRV(session, application_cache_interruptible_time, elapsed);
            WT_STAT_SESSION_INCRV(session, cache_time_interruptible, elapsed);
        }
        session->cache_wait_us += elapsed;
        /*
         * Check if a rollback is required only if there has not been an error. Returning an error
         * takes precedence over asking for a rollback. We can not do both.
         */
        if (ret == 0 && cache_max_wait_us != 0 && session->cache_wait_us > cache_max_wait_us) {
            ret = WT_ROLLBACK;
            __wt_session_set_last_error(
              session, ret, WT_CACHE_OVERFLOW, WT_TXN_ROLLBACK_REASON_CACHE_OVERFLOW);
            __wt_atomic_decrement_if_positive(&evict->evict_aggressive_score);

            WT_STAT_CONN_INCR(session, eviction_timed_out_ops);
            if (F_ISSET(session, WT_SESSION_SAVE_ERRORS))
                __wt_verbose_notice(
                  session, WT_VERB_TRANSACTION, "rollback reason: %s", session->err_info.err_msg);
        }
    }

    WT_TRACK_OP_END(session);

    return (ret);
}

/* !!!
 * __wt_evict_page_urgent --
 *     Push a page into the urgent eviction queue.
 *
 *     It is called by the eviction server if pages require immediate eviction or by the application
 *     threads as part of forced eviction when directly evicting pages is not feasible.
 *
 *     Input parameters:
 *       `ref`: A reference to the page that is being added to the urgent eviction queue.
 *
 *     Return `true` if the page has been successfully added to the urgent queue, or `false` is
 *     already marked for eviction.
 */
bool
__wt_evict_page_urgent(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_EVICT *evict;
    WTI_EVICT_ENTRY *evict_entry;
    WTI_EVICT_QUEUE *urgent_queue;
    WT_PAGE *page;
    bool queued;

    /* Root pages should never be evicted via LRU. */
    WT_ASSERT(session, !__wt_ref_is_root(ref));

    page = ref->page;
    if (S2BT(session)->evict_disabled > 0 || F_ISSET_ATOMIC_16(page, WT_PAGE_EVICT_LRU_URGENT))
        return (false);

    evict = S2C(session)->evict;
    if (F_ISSET_ATOMIC_16(page, WT_PAGE_EVICT_LRU) && F_ISSET(evict, WT_EVICT_CACHE_ALL))
        return (false);

    /* Append to the urgent queue if we can. */
    urgent_queue = &evict->evict_queues[WTI_EVICT_URGENT_QUEUE];
    queued = false;

    __wt_spin_lock(session, &evict->evict_queue_lock);

    /* Check again, in case we raced with another thread. */
    if (S2BT(session)->evict_disabled > 0 || F_ISSET_ATOMIC_16(page, WT_PAGE_EVICT_LRU_URGENT))
        goto done;

    /*
     * If the page is already in the LRU eviction list, clear it from the list if eviction server is
     * not running.
     */
    if (F_ISSET_ATOMIC_16(page, WT_PAGE_EVICT_LRU)) {
        if (!F_ISSET(evict, WT_EVICT_CACHE_ALL)) {
            __evict_list_clear_page_locked(session, ref, true);
            WT_STAT_CONN_INCR(session, eviction_clear_ordinary);
        } else
            goto done;
    }

    __wt_spin_lock(session, &urgent_queue->evict_lock);
    if (__evict_queue_empty(urgent_queue, false)) {
        urgent_queue->evict_current = urgent_queue->evict_queue;
        urgent_queue->evict_candidates = 0;
    }
    evict_entry = urgent_queue->evict_queue + urgent_queue->evict_candidates;
    if (evict_entry < urgent_queue->evict_queue + evict->evict_slots &&
      __evict_push_candidate(session, urgent_queue, evict_entry, ref)) {
        ++urgent_queue->evict_candidates;
        queued = true;
        FLD_SET(page->flags_atomic, WT_PAGE_EVICT_LRU_URGENT);
    }
    __wt_spin_unlock(session, &urgent_queue->evict_lock);

done:
    __wt_spin_unlock(session, &evict->evict_queue_lock);
    if (queued) {
        WT_STAT_CONN_INCR(session, eviction_pages_queued_urgent);
        if (WT_EVICT_HAS_WORKERS(session))
            __wt_cond_signal(session, S2C(session)->evict_threads.wait_cond);
        else
            __wt_evict_server_wake(session);
    }

    return (queued);
}

/* !!!
 * __wt_evict_priority_set --
 *     Set a tree's eviction priority. A higher priority indicates less likelihood for the tree to
 *     be considered for eviction. The eviction server skips the eviction of trees with a non-zero
 *     priority unless eviction is in an aggressive state and the Btree is significantly utilizing
 *     the cache.
 *
 *     At present, it is exclusively called for metadata and bloom filter files, as these are meant
 *     to be retained in the cache.
 *
 *     Input parameter:
 *       `v`: An integer that denotes the priority level.
 */
void
__wt_evict_priority_set(WT_SESSION_IMPL *session, uint64_t v)
{
    S2BT(session)->evict_priority = v;
}

/*
 * __wt_evict_priority_clear --
 *     Clear a tree's eviction priority to zero. It is called during the closure of the
 *     dhandle/btree.
 */
void
__wt_evict_priority_clear(WT_SESSION_IMPL *session)
{
    S2BT(session)->evict_priority = 0;
}

/*
 * __verbose_dump_cache_single --
 *     Output diagnostic information about a single file in the cache.
 */
static int
__verbose_dump_cache_single(WT_SESSION_IMPL *session, uint64_t *total_bytesp,
  uint64_t *total_dirty_bytesp, uint64_t *total_updates_bytesp)
{
    WT_BTREE *btree;
    WT_DATA_HANDLE *dhandle;
    WT_PAGE *page;
    WT_REF *next_walk;
    size_t size;
    uint64_t intl_bytes, intl_bytes_max, intl_dirty_bytes;
    uint64_t intl_dirty_bytes_max, intl_dirty_pages, intl_pages;
    uint64_t leaf_bytes, leaf_bytes_max, leaf_dirty_bytes;
    uint64_t leaf_dirty_bytes_max, leaf_dirty_pages, leaf_pages, updates_bytes;

    intl_bytes = intl_bytes_max = intl_dirty_bytes = 0;
    intl_dirty_bytes_max = intl_dirty_pages = intl_pages = 0;
    leaf_bytes = leaf_bytes_max = leaf_dirty_bytes = 0;
    leaf_dirty_bytes_max = leaf_dirty_pages = leaf_pages = 0;
    updates_bytes = 0;

    dhandle = session->dhandle;
    btree = dhandle->handle;
    WT_RET(__wt_msg(session, "%s(%s%s)%s%s:", dhandle->name,
      WT_DHANDLE_IS_CHECKPOINT(dhandle) ? "checkpoint=" : "",
      WT_DHANDLE_IS_CHECKPOINT(dhandle) ? dhandle->checkpoint : "<live>",
      btree->evict_disabled != 0 ? " eviction disabled" : "",
      btree->evict_disabled_open ? " at open" : ""));

    /*
     * We cannot walk the tree of a dhandle held exclusively because the owning thread could be
     * manipulating it in a way that causes us to dump core. So print out that we visited and
     * skipped it.
     */
    if (F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE))
        return (__wt_msg(session, " handle opened exclusively, cannot walk tree, skipping"));

    next_walk = NULL;
    while (__wt_tree_walk(session, &next_walk,
             WT_READ_CACHE | WT_READ_NO_EVICT | WT_READ_NO_WAIT | WT_READ_VISIBLE_ALL) == 0 &&
      next_walk != NULL) {
        page = next_walk->page;
        size = __wt_atomic_loadsize(&page->memory_footprint);

        if (F_ISSET(next_walk, WT_REF_FLAG_INTERNAL)) {
            ++intl_pages;
            intl_bytes += size;
            intl_bytes_max = WT_MAX(intl_bytes_max, size);
            if (__wt_page_is_modified(page)) {
                ++intl_dirty_pages;
                intl_dirty_bytes += size;
                intl_dirty_bytes_max = WT_MAX(intl_dirty_bytes_max, size);
            }
        } else {
            ++leaf_pages;
            leaf_bytes += size;
            leaf_bytes_max = WT_MAX(leaf_bytes_max, size);
            if (__wt_page_is_modified(page)) {
                ++leaf_dirty_pages;
                leaf_dirty_bytes += size;
                leaf_dirty_bytes_max = WT_MAX(leaf_dirty_bytes_max, size);
            }
            if (page->modify != NULL)
                updates_bytes += page->modify->bytes_updates;
        }
    }

    if (intl_pages == 0)
        WT_RET(__wt_msg(session, "internal: 0 pages"));
    else
        WT_RET(
          __wt_msg(session,
            "internal: "
            "%" PRIu64 " pages, %.2f KB, "
            "%" PRIu64 "/%" PRIu64 " clean/dirty pages, "
            "%.2f/%.2f clean / dirty KB, "
            "%.2f KB max page, "
            "%.2f KB max dirty page ",
            intl_pages, (double)intl_bytes / WT_KILOBYTE, intl_pages - intl_dirty_pages,
            intl_dirty_pages, (double)(intl_bytes - intl_dirty_bytes) / WT_KILOBYTE,
            (double)intl_dirty_bytes / WT_KILOBYTE, (double)intl_bytes_max / WT_KILOBYTE,
            (double)intl_dirty_bytes_max / WT_KILOBYTE));
    if (leaf_pages == 0)
        WT_RET(__wt_msg(session, "leaf: 0 pages"));
    else
        WT_RET(
          __wt_msg(session,
            "leaf: "
            "%" PRIu64 " pages, %.2f KB, "
            "%" PRIu64 "/%" PRIu64 " clean/dirty pages, "
            "%.2f /%.2f /%.2f clean/dirty/updates KB, "
            "%.2f KB max page, "
            "%.2f KB max dirty page",
            leaf_pages, (double)leaf_bytes / WT_KILOBYTE, leaf_pages - leaf_dirty_pages,
            leaf_dirty_pages, (double)(leaf_bytes - leaf_dirty_bytes) / WT_KILOBYTE,
            (double)leaf_dirty_bytes / WT_KILOBYTE, (double)updates_bytes / WT_KILOBYTE,
            (double)leaf_bytes_max / WT_KILOBYTE, (double)leaf_dirty_bytes_max / WT_KILOBYTE));

    *total_bytesp += intl_bytes + leaf_bytes;
    *total_dirty_bytesp += intl_dirty_bytes + leaf_dirty_bytes;
    *total_updates_bytesp += updates_bytes;

    return (0);
}

/*
 * __verbose_dump_cache_apply --
 *     Apply dumping cache for all the dhandles.
 */
static int
__verbose_dump_cache_apply(WT_SESSION_IMPL *session, uint64_t *total_bytesp,
  uint64_t *total_dirty_bytesp, uint64_t *total_updates_bytesp)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;

    conn = S2C(session);
    for (dhandle = NULL;;) {
        WT_DHANDLE_NEXT(session, dhandle, &conn->dhqh, q);
        if (dhandle == NULL)
            break;

        /* Skip if the tree is marked discarded by another thread. */
        if (!WT_DHANDLE_BTREE(dhandle) || !F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
          F_ISSET(dhandle, WT_DHANDLE_DISCARD))
            continue;

        WT_WITH_DHANDLE(session, dhandle,
          ret = __verbose_dump_cache_single(
            session, total_bytesp, total_dirty_bytesp, total_updates_bytesp));
        if (ret != 0)
            WT_RET(ret);
    }
    return (0);
}

/*
 * __wt_verbose_dump_cache --
 *     Output diagnostic information about the cache.
 */
int
__wt_verbose_dump_cache(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    double pct;
    uint64_t bytes_dirty_intl, bytes_dirty_leaf, bytes_inmem;
    uint64_t cache_bytes_updates, total_bytes, total_dirty_bytes, total_updates_bytes;
    bool needed;

    conn = S2C(session);
    cache = conn->cache;
    total_bytes = total_dirty_bytes = total_updates_bytes = 0;
    pct = 0.0; /* [-Werror=uninitialized] */
    WT_NOT_READ(cache_bytes_updates, 0);

    WT_RET(__wt_msg(session, "%s", WT_DIVIDER));
    WT_RET(__wt_msg(session, "cache dump"));

    WT_RET(__wt_msg(session, "cache full: %s", __wt_cache_full(session) ? "yes" : "no"));
    needed = __wt_evict_clean_needed(session, &pct);
    WT_RET(__wt_msg(session, "cache clean check: %s (%2.3f%%)", needed ? "yes" : "no", pct));
    needed = __wt_evict_dirty_needed(session, &pct);
    WT_RET(__wt_msg(session, "cache dirty check: %s (%2.3f%%)", needed ? "yes" : "no", pct));
    needed = __wti_evict_updates_needed(session, &pct);
    WT_RET(__wt_msg(session, "cache updates check: %s (%2.3f%%)", needed ? "yes" : "no", pct));

    WT_WITH_HANDLE_LIST_READ_LOCK(session,
      ret = __verbose_dump_cache_apply(
        session, &total_bytes, &total_dirty_bytes, &total_updates_bytes));
    WT_RET(ret);

    /*
     * Apply the overhead percentage so our total bytes are comparable with the tracked value.
     */
    total_bytes = __wt_cache_bytes_plus_overhead(conn->cache, total_bytes);
    cache_bytes_updates = __wt_cache_bytes_updates(cache);

    bytes_inmem = __wt_atomic_load64(&cache->bytes_inmem);
    bytes_dirty_intl = __wt_atomic_load64(&cache->bytes_dirty_intl);
    bytes_dirty_leaf = __wt_atomic_load64(&cache->bytes_dirty_leaf);

    WT_RET(__wt_msg(session, "cache dump: total found: %.2f MB vs tracked inuse %.2f MB",
      (double)total_bytes / WT_MEGABYTE, (double)bytes_inmem / WT_MEGABYTE));
    WT_RET(__wt_msg(session, "total dirty bytes: %.2f MB vs tracked dirty %.2f MB",
      (double)total_dirty_bytes / WT_MEGABYTE,
      (double)(bytes_dirty_intl + bytes_dirty_leaf) / WT_MEGABYTE));
    WT_RET(__wt_msg(session, "total updates bytes: %.2f MB vs tracked updates %.2f MB",
      (double)total_updates_bytes / WT_MEGABYTE, (double)cache_bytes_updates / WT_MEGABYTE));

    return (0);
}


static const char *
ref_state_to_string(WT_REF_STATE state)
{
    switch (state) {
    case WT_REF_MEM:
        return "MEM (In Cache)";
    case WT_REF_DISK:
        return "DISK (On Disk)";
    case WT_REF_DELETED:
        return "DELETED";
    case WT_REF_LOCKED:
        return "LOCKED";
    case WT_REF_SPLIT:
        return "SPLIT";
    default:
        break;
    }
    return "UNKNOWN";
}

/*
 * 退避キューの中身を全て表示するための、自作のデバッグ関数
 */
/*
static void
__my_dump_eviction_queue(WTI_EVICT_QUEUE *queue)
{
    WTI_EVICT_ENTRY *entry;
    uint32_t i;

    printf("\n========== Eviction Queue Dump ==========\n");
    printf("Total pages queued: %u\n", queue->evict_entries);
    printf("-----------------------------------------\n");

    // evict_queue配列を、有効なエントリ数だけループする
    for (i = 0; i < queue->evict_entries; i++) {
        entry = &queue->evict_queue[i];

        if (entry->ref == NULL)
            continue;

        printf("[%u] Page Ref Addr: %p\n", i, (void *)entry->ref);
        printf("    -> Page Ptr  : %p\n", (void *)entry->ref->page);
        printf("    -> Page URI  : %s\n", entry->btree->dhandle->name);
        printf("    -> Page Type : %s\n",
            __wt_ref_is_root(entry->ref) ? "ROOT" :
            (F_ISSET(entry->ref, WT_REF_FLAG_INTERNAL) ? "INTERNAL" : "LEAF"));
        printf("    -> Ref State : %s\n",
            ref_state_to_string(WT_REF_GET_STATE(entry->ref)));
    }
    printf("Pages to be evicted (evict_candidates): %u\n", queue->evict_candidates);
    printf("=========================================\n\n");
}
 */

 /*
 * メタデータリストを管理するための構造体
 */
typedef struct {
    CACHE_PAGE_INFO *list;
    size_t count;
    size_t capacity;
} METADATA_COLLECTOR;

int __my_evict_walk_tree(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue, u_int max_entries, u_int *slotp)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WTI_EVICT_ENTRY *end, *evict_entry, *start;
    //WT_PAGE *last_parent, *page;
    WT_PAGE *page;
    WT_REF *ref;
    WT_TXN *txn;
    uint64_t internal_pages_already_queued, internal_pages_queued, internal_pages_seen;
    //uint64_t min_pages, pages_already_queued, pages_queued, pages_seen, refs_walked;
    uint64_t pages_already_queued, pages_queued, pages_seen, refs_walked;
    uint64_t pages_seen_clean, pages_seen_dirty, pages_seen_updates;
    //uint32_t evict_walk_period, target_pages, walk_flags;
    uint32_t target_pages, walk_flags;
    int restarts;
    //bool give_up, queued, urgent_queued;
    bool queued;

    conn = S2C(session);
    btree = S2BT(session);
    evict = conn->evict;
    //last_parent = NULL;
    restarts = 0;
    //give_up = urgent_queued = false;
    txn = session->txn;

    WT_ASSERT_SPINLOCK_OWNED(session, &evict->evict_walk_lock);

    start = queue->evict_queue + *slotp;
    //変更箇所：キューの空きスペース全てを目標に設定
    target_pages = max_entries - *slotp;

    /* If we don't want any pages from this tree, move on. */
    if (target_pages == 0)
        return (0);

    end = start + target_pages;

    //min_pages = __evict_get_min_pages(session, target_pages);

    WT_RET_NOTFOUND_OK(__evict_walk_prepare(session, &walk_flags));

    /*
     * 全てのページを確実に見つけるため、内部ページや
     * 削除済みページをスキップするフラグを強制的に解除する。
    if (__evict_queue_empty(evict->evict_current_queue, true) &&
      __evict_queue_empty(evict->evict_other_queue, true))
      FLD_CLR(walk_flags, WT_READ_SKIP_INTL | WT_READ_SKIP_DELETED);
     */

    /*
     * Get some more eviction candidate pages, starting at the last saved point. Clear the saved
     * point immediately, we assert when discarding pages we're not discarding an eviction point, so
     * this clear must be complete before the page is released.
     */
    ref = btree->evict_ref;
    btree->evict_ref = NULL;

    /* Clear the saved position just in case we never put it back. */
    __wt_evict_clear_npos(btree);

    /*
     * Get the snapshot for the eviction server when we want to evict dirty content under cache
     * pressure. This snapshot is used to check for the visibility of the last modified transaction
     * id on the page.
     */
    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY_HARD | WT_EVICT_CACHE_UPDATES_HARD))
        __wt_txn_bump_snapshot(session);

    /*
     * !!! Take care terminating this loop.
     *
     * Don't make an extra call to __wt_tree_walk after we hit the end of a
     * tree: that will leave a page pinned, which may prevent any work from
     * being done.
     *
     * Once we hit the page limit, do one more step through the walk in
     * case we are appending and only the last page in the file is live.
     */
    internal_pages_already_queued = internal_pages_queued = internal_pages_seen = 0;
    for (evict_entry = start, pages_already_queued = pages_queued = pages_seen = refs_walked = 0;
         evict_entry < end && (ret == 0 || ret == WT_NOTFOUND);
         //last_parent = ref == NULL ? NULL : ref->home,
        ret = __wt_tree_walk_count(session, &ref, &refs_walked, walk_flags)) {

        //変更箇所：（見ているページ数に対して候補が少なすぎる）から、このテーブルの探索は諦めようとする処理を削除
        /*
            if ((give_up = __evict_should_give_up_walk(
               session, pages_seen, pages_queued, min_pages, target_pages)))
            break;
        */
        if (ref == NULL) {
            WT_STAT_CONN_INCR(session, eviction_walks_ended);

            if (++restarts == 2) {
                WT_STAT_CONN_INCR(session, eviction_walks_stopped);
                break;
            }
            WT_STAT_CONN_INCR(session, eviction_walks_started);
            continue;
        }

        ++pages_seen;

        //変更箇所：ルートページを無視しないように変更
        /* Ignore root pages entirely. */
        /*
        */
        if (__wt_ref_is_root(ref))
        if(!(__evict_queue_empty(evict->evict_current_queue, true) && __evict_queue_empty(evict->evict_other_queue, true)))
            continue;

        page = ref->page;

        /*
         * Update the maximum evict pass generation gap seen at time of eviction. This helps track
         * how long it's been since a page was last queued for eviction. We need to update the
         * statistic here during the walk and not at __evict_page because the evict_pass_gen is
         * reset here.
         */
        if (page->evict_pass_gen == 0) {
            const uint64_t gen_gap =
              __wt_atomic_load64(&evict->evict_pass_gen) - page->cache_create_gen;
            if (gen_gap > __wt_atomic_load64(&evict->evict_max_unvisited_gen_gap))
                __wt_atomic_store64(&evict->evict_max_unvisited_gen_gap, gen_gap);
            if (gen_gap > __wt_atomic_load64(&evict->evict_max_unvisited_gen_gap_per_checkpoint))
                __wt_atomic_store64(&evict->evict_max_unvisited_gen_gap_per_checkpoint, gen_gap);
        } else {
            const uint64_t gen_gap =
              __wt_atomic_load64(&evict->evict_pass_gen) - page->evict_pass_gen;
            if (gen_gap > __wt_atomic_load64(&evict->evict_max_visited_gen_gap))
                __wt_atomic_store64(&evict->evict_max_visited_gen_gap, gen_gap);
            if (gen_gap > __wt_atomic_load64(&evict->evict_max_visited_gen_gap_per_checkpoint))
                __wt_atomic_store64(&evict->evict_max_visited_gen_gap_per_checkpoint, gen_gap);
        }

        page->evict_pass_gen = __wt_atomic_load64(&evict->evict_pass_gen);

        if (__wt_page_is_modified(page))
            ++pages_seen_dirty;
        else if (page->modify != NULL)
            ++pages_seen_updates;
        else
            ++pages_seen_clean;
        
        /* Count internal pages seen. */
        if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
            internal_pages_seen++;

        /* Use the EVICT_LRU flag to avoid putting pages onto the list multiple times. */
        if (F_ISSET_ATOMIC_16(page, WT_PAGE_EVICT_LRU)) {
            pages_already_queued++;
            if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
                internal_pages_already_queued++;
            continue;
        }

        //変更箇所：__evict_try_queue_page(...) の呼び出しを置き換える
        queued = __evict_push_candidate(session, queue, evict_entry, ref);
        /*
        __evict_try_queue_page(
          session, queue, ref, last_parent, evict_entry, &urgent_queued, &queued);
          */

        
        /* ▼▼▼ このブロックを追加して、リストアップされたページ情報を出力 ▼▼▼ */
        /*
        if (queued) {
            printf("-> Queued page %p from URI: %s (Type: %s)\n",
                (void *)ref->page,
                session->dhandle->name,
                __wt_ref_is_root(ref) ? "ROOT" :
                (F_ISSET(ref, WT_REF_FLAG_INTERNAL) ? "INTERNAL" : "LEAF"));
        }
        */
        /*********************************************************/
        if (queued) {
            ++evict_entry;
            ++pages_queued;
            ++btree->evict_walk_progress;

            /* Count internal pages queued. */
            if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
                internal_pages_queued++;
        }
    }
    if (F_ISSET(txn, WT_TXN_HAS_SNAPSHOT))
        __wt_txn_release_snapshot(session);
    WT_RET_NOTFOUND_OK(ret);

    *slotp += (u_int)(evict_entry - start);
    WT_STAT_CONN_INCRV(session, eviction_pages_ordinary_queued, (u_int)(evict_entry - start));

    __wt_verbose_debug2(session, WT_VERB_EVICTION,
      "%s walk: target %" PRIu32 ", seen %" PRIu64 ", queued %" PRIu64, session->dhandle->name,
      target_pages, pages_seen, pages_queued);

    /* If we couldn't find the number of pages we were looking for, skip the tree next time. */
    //変更箇所：「今回あまり成果がなかったから、次はこのテーブルのスキャンを後回しにしよう」**と判断するための、元のコードの効率化ヒューリスティックの一部を削除
    /*
    evict_walk_period = __wt_atomic_load32(&btree->evict_walk_period);
    if (pages_queued < target_pages / 2 && !urgent_queued)
        __wt_atomic_store32(
          &btree->evict_walk_period, WT_MIN(WT_MAX(1, 2 * evict_walk_period), 100));
    else if (pages_queued == target_pages) {
        __wt_atomic_store32(&btree->evict_walk_period, 0);
        
         // If there's a chance the Btree was fully evicted, update the evicted flag in the handle.
        
        if (__wt_btree_bytes_evictable(session) == 0)
            FLD_SET(session->dhandle->advisory_flags, WT_DHANDLE_ADVISORY_EVICTED);
    } else if (evict_walk_period > 0)
        __wt_atomic_store32(&btree->evict_walk_period, evict_walk_period / 2);
    */

    /*
     * Give up the walk occasionally.
     *
     * If we happen to end up on the root page or a page requiring urgent eviction, clear it. We
     * have to track hazard pointers, and the root page complicates that calculation.
     *
     * Likewise if we found no new candidates during the walk: there is no point keeping a page
     * pinned, since it may be the only candidate in an idle tree.
     *
     * If we land on a page requiring forced eviction, or that isn't an ordinary in-memory page,
     * move until we find an ordinary page: we should not prevent exclusive access to the page until
     * the next walk.
     */

    //変更箇所：探索が完了した後、次にスキャンを再開する場所（btree->evict_ref）を保存する処理を削除
    /*//////////ここが正常に復帰できない原因か
    if (ref != NULL) {
        if (__wt_ref_is_root(ref) || evict_entry == start || give_up ||
          __wt_atomic_loadsize(&ref->page->memory_footprint) >= btree->splitmempage) {
            if (restarts == 0)
                WT_STAT_CONN_INCR(session, eviction_walks_abandoned);
            WT_RET(__wt_page_release(evict->walk_session, ref, walk_flags));
            ref = NULL;
        } else {
            while (ref != NULL &&
              (WT_REF_GET_STATE(ref) != WT_REF_MEM ||
                __wti_evict_readgen_is_soon_or_wont_need(&ref->page->read_gen)))
                WT_RET_NOTFOUND_OK(__wt_tree_walk_count(session, &ref, &refs_walked, walk_flags));
        }
        btree->evict_ref = ref;
        if (evict->use_npos_in_pass)
            __evict_clear_walk(session, false);
    }
    */
    /*
     * ref が NULL でない場合、ウォークが途中で終わってページを掴んだままの状態です。
     * これを解放する必要がありますが、必ず「自分のセッション」を使います。
     */
    if (ref != NULL) {
        // ★修正: evict->walk_session ではなく session を使う！
        WT_TRET(__wt_page_release(session, ref, walk_flags));
        ref = NULL;
    }

    WT_STAT_CONN_INCRV(session, eviction_walk, refs_walked);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen, pages_seen);
    WT_STAT_CONN_INCRV(session, eviction_pages_already_queued, pages_already_queued);
    WT_STAT_CONN_INCRV(session, eviction_internal_pages_seen, internal_pages_seen);
    WT_STAT_CONN_INCRV(
      session, eviction_internal_pages_already_queued, internal_pages_already_queued);
    WT_STAT_CONN_INCRV(session, eviction_internal_pages_queued, internal_pages_queued);
    WT_STAT_CONN_DSRC_INCR(session, eviction_walk_passes);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_clean, pages_seen_clean);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_dirty, pages_seen_dirty);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_updates, pages_seen_updates);
    return (0);
}

static int WT_CDECL
my_evict_cmp(const void *a_arg, const void *b_arg)
{
    const WTI_EVICT_ENTRY *a = a_arg;
    const WTI_EVICT_ENTRY *b = b_arg;
    int score_a, score_b;

    /* 空のエントリは常に最低優先度(3)とする */
    if (a->ref == NULL)
        score_a = 3;
    else {
        /* ページの種類に応じて優先度を割り当てる */
        if (F_ISSET(a->ref, WT_REF_FLAG_LEAF))
            score_a = 0; // LEAFは最高優先度(0)
        else if (F_ISSET(a->ref, WT_REF_FLAG_INTERNAL))
            score_a = 1; // INTERNALは中間(1)
        else // __wt_ref_is_root(a->ref)
            score_a = 2; // ROOTは最低優先度(2)
    }

    if (b->ref == NULL)
        score_b = 3;
    else {
        if (F_ISSET(b->ref, WT_REF_FLAG_LEAF))
            score_b = 0;
        else if (F_ISSET(b->ref, WT_REF_FLAG_INTERNAL))
            score_b = 1;
        else
            score_b = 2;
    }
    
    /* スコアを比較して、昇順（小さい方が先）に並べる */
    if (score_a < score_b)
        return (-1);
    if (score_a > score_b)
        return (1);
    
    return (0);
}

// 引数に int stable_count を追加
static void
__filter_single_queue(WTI_EVICT_QUEUE *q)
{
    if (q->evict_entries == 0) return;

    uint32_t write_idx = 0;
    uint32_t original_entries = q->evict_entries;
    uint32_t dropped_count = 0; // 削除数カウンタ

    for (uint32_t read_idx = 0; read_idx < original_entries; read_idx++) {
        WT_REF *ref = q->evict_queue[read_idx].ref;
        bool keep = true;
        const char *drop_reason = ""; // 削除理由

        if (ref == NULL) {
            keep = false;
            drop_reason = "NULL_REF";
        } else if (ref->page == NULL) {
            keep = false;
            drop_reason = "NO_PAGE_IN_MEMORY";
        } else {
            // 1. 内部ノードは除外
            if (__wt_ref_is_root(ref)) {
                 keep = false;
                 drop_reason = "ROOT_PAGE";
            } else if (F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
                 keep = false;
                 drop_reason = "INTERNAL_PAGE";
            }
            
            // 2. 停滞時の強制削除（先頭のみ）
            if (keep && stable_count >= 5 && read_idx == 0) {
                keep = false;
                drop_reason = "STAGNATION_FORCE_DROP (Head)";
            }
        }

        if (!keep) {
            // ▼▼▼ 追加: 削除ログ ▼▼▼
            // ※ RefがNULLでない場合のみアドレス等を表示
            if (ref != NULL) {
                // ファイル名が取れるなら表示したいところですが、安全のためアドレスと理由のみ
                my_log("[Filter] Dropping [%u]: Ref=%p, Reason=%s\n", 
                       read_idx, (void *)ref, drop_reason);
            } else {
                my_log("[Filter] Dropping [%u]: Ref=NULL, Reason=%s\n", 
                       read_idx, drop_reason);
            }
            dropped_count++;
            // ▲▲▲ 追加終了 ▲▲▲
        }

        if (keep) {
            if (read_idx != write_idx) {
                q->evict_queue[write_idx] = q->evict_queue[read_idx];
            }
            write_idx++;
        }
    }

    // ▼▼▼ 追加: まとめのログ（削除があった場合のみ） ▼▼▼
    if (dropped_count > 0) {
        my_log("[Filter] Summary: %u pages dropped, %u pages kept.\n", 
               dropped_count, write_idx);
    }

    // --- 以下、既存の処理 ---

    // 残りの領域をNULLクリア
    for (uint32_t i = write_idx; i < original_entries; i++) {
        q->evict_queue[i].ref = NULL;
    }

    q->evict_entries = write_idx;

    if (q->evict_entries > 0) {
        q->evict_current = q->evict_queue;
    } else {
        q->evict_current = NULL;
    }

    // 安全対策: NULLチェックと切り詰め
    for (uint32_t i = 0; i < q->evict_entries; i++) {
        if (q->evict_queue[i].ref == NULL) {
            my_log("[Filter] WARNING: NULL found after compaction at index %u. Truncating.\n", i);
            q->evict_entries = i; 
            if (i == 0) q->evict_current = NULL;
            break;
        }
    }
}

/*
 * __my_evict_lru_walk --
 * A modified version of __evict_lru_walk that calls __my_evict_walk
 * and considers all found pages as candidates for eviction.
 */
static int
__my_evict_lru_walk(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WTI_EVICT_QUEUE *other_queue, *queue;
    WT_TRACK_OP_DECL;
    uint32_t entries;

    WT_TRACK_OP_INIT(session);
    conn = S2C(session);
    evict = conn->evict;

    if (evict->evict_empty_score > 0)
        --evict->evict_empty_score;

    queue = evict->evict_fill_queue;
    other_queue = evict->evict_queues + (1 - (queue - evict->evict_queues));
    evict->evict_fill_queue = other_queue;

    if (__evict_queue_full(queue) && !__evict_queue_full(other_queue))
        queue = other_queue;

    if (__evict_queue_empty(queue, false)) {
        if (F_ISSET(evict, WT_EVICT_CACHE_HARD))
            evict->evict_empty_score =
              WT_MIN(evict->evict_empty_score + WT_EVICT_SCORE_BUMP, WT_EVICT_SCORE_MAX);
        WT_STAT_CONN_INCR(session, eviction_queue_empty);
    } else
        WT_STAT_CONN_INCR(session, eviction_queue_not_empty);
    
    if ((ret = __evict_walk(evict->walk_session, queue)) == EBUSY)
        ret = 0;

    WT_ERR_NOTFOUND_OK(ret, false);

    __wt_spin_lock(session, &queue->evict_lock);

    if (queue == evict->evict_current_queue)
        queue->evict_current = NULL;
    
    entries = queue->evict_entries;

    __wt_qsort(queue->evict_queue, entries, sizeof(WTI_EVICT_ENTRY), my_evict_cmp);

    while (entries > 0 && queue->evict_queue[entries - 1].ref == NULL)
        --entries;


    // 1. 今回充填したメインキューを掃除
    __filter_single_queue(queue);

    queue->evict_entries = entries;

    // ★安全対策: 最終チェック (万が一NULLがあればそこで打ち切る)
    for (uint32_t i = 0; i < entries; i++) {
        if (queue->evict_queue[i].ref == NULL) {
            // ログ関数が使えるなら警告を出す
            // my_log("WARNING: NULL ref found after compaction at index %u\n", i);
            entries = i;
            queue->evict_entries = entries;
            break;
        }
    }

    my_log("Eviction LRU walk found %u candidates(stable_count=%d)\n", queue->evict_candidates, stable_count);
    my_log("Other queue found %u candidates(stable_count=%d)\n", other_queue->evict_candidates, stable_count);

    // 候補がない場合はエラーではなく WT_NOTFOUND を返して呼び出し元に知らせる
    if (entries == 0) {
        queue->evict_candidates = 0;
        queue->evict_current = NULL;
        __wt_spin_unlock(session, &queue->evict_lock);
        ret = WT_NOTFOUND; 
        goto err;
    }

    queue->evict_candidates = entries;

    WT_STAT_CONN_INCRV(session, eviction_pages_queued_post_lru, queue->evict_candidates);
    queue->evict_current = queue->evict_queue;
    __wt_spin_unlock(session, &queue->evict_lock);

    __wt_cond_signal(session, conn->evict_threads.wait_cond);

err:
    WT_TRACK_OP_END(session);
    return (ret);
}

/*
 * ページ情報を、渡されたバッファ構造体に保存するヘルパー関数
 */
static void
save_page_info_to_buffer(WT_BTREE *btree, WT_REF *ref, CACHE_PAGE_INFO *info)
{
    /* URIをコピー */
    strncpy(info->uri, btree->dhandle->name, sizeof(info->uri) - 1);

    /* 親ページのアドレスをコピー */
    info->parent_page_addr = (uint64_t)(void *)ref->home;

    /* 子ページのキーをコピー (row-storeの場合) */
    if (ref->key.ikey != NULL) {
        WT_IKEY *ikey_ptr = (WT_IKEY *)ref->key.ikey;
        info->child_key_size = ikey_ptr->size;
        if(ikey_ptr->size != 1 )
            memcpy(info->child_key, (uint8_t *)ikey_ptr + sizeof(size_t), info->child_key_size);
        else {
            sprintf((char *)info->child_key, "key%010d", 0);
            info->child_key_size = strlen((const char *)info->child_key);
            //char *zero_key = "key0000000000";
            //memcpy(info->child_key, (uint8_t *)zero_key + sizeof(size_t), strlen(zero_key) + 1);
        }
        info->page_size = __wt_atomic_loadsize(&ref->page->memory_footprint);
    } else {
         info->child_key_size = 0;
    }
    info->key_entries = my_count_all_keys_in_page(ref);
    /*
     * ページの物理アドレス（オフセットとサイズ）を保存する。
     * ref->addr が NULL (ディスクに未書き込み) の場合は 0 を設定する。
     */
    if (ref->addr != NULL) {
        WT_ADDR *physical_addr = (WT_ADDR *)ref->addr;
        info->page_disk_offset = *((uint64_t *)physical_addr->addr);
        info->page_disk_size   = physical_addr->size;
    } else {
        info->page_disk_offset = 0;
        info->page_disk_size = 0;
    }
}

/* ページ情報を出力し、かつメタデータリストに保存する関数 */
void print_clear_page_info(WT_SESSION_IMPL *session, const char *title, WT_BTREE *btree, WT_REF *ref, int is_before)
//void print_clear_page_info(const char *title, WT_BTREE *btree, WT_REF *ref, int is_before)
{
    // 1. まず、これまで通りデバッグ情報を画面に出力する
    my_log("・%s --\t", title);
    if(is_before != 1){
        my_log("・Page Ref Addr: %p\t", (void *)ref);
        //printf("  -> Page Ptr      : %p\n", (void *)ref->page);
        my_log("・Page URI: %s\t", btree->dhandle->name);
    }
    my_log("・Pagce Type: %s\t", __wt_ref_is_root(ref) ? "ROOT" : (F_ISSET(ref, WT_REF_FLAG_INTERNAL) ? "INTERNAL" : "LEAF"));
    //printf("  -> Page State   : %s\n", __wt_page_is_modified(ref->page) ? "DIRTY" : "CLEAN");
    //printf("・Ref State: %s (%d)\t", ref_state_to_string(WT_REF_GET_STATE(ref)), WT_REF_GET_STATE(ref));
    if (is_before != 1) {
        WT_PAGE *page = ref->page;
        my_log("・Page Size: %zu bytes\t", __wt_atomic_loadsize(&page->memory_footprint));

        if (is_before == 0 && ref->key.ikey != NULL) {
            WT_IKEY *ikey_ptr = (WT_IKEY *)ref->key.ikey;
            printf("・First Key: %.*s\t", (int)ikey_ptr->size, (char *)((uint8_t *)ikey_ptr + sizeof(size_t)));
        }
        else if (is_before == 2 && F_ISSET(ref, WT_REF_FLAG_LEAF) && page->entries > 0) {
            WT_ITEM key_item;
            WT_ROW *first_row = page->pg_row; // 最初の行を取得

            if (__wt_row_leaf_key(session, page, first_row, &key_item, false) == 0) {
                printf("・First Key: %.*s\t", (int)key_item.size, (char *)key_item.data);
            }
        } else {
            printf("・First Key: (Not a leaf page or page is empty)\t");
        }
    }
    if(is_before != 1) __my_dump_all_keys_before_evict(session, ref);
     
    /*
    //この物理アドレスを取得するコードはDBを再起動した場合に実行すると、セグフォになる
    if (is_before != 0 && ref->addr != NULL) {
        WT_ADDR *physical_addr = (WT_ADDR *)ref->addr;
        printf("  -> Disk Offset : %lu\t", *((uint64_t *)physical_addr->addr));
    } else {
        printf("  -> Disk Offset : (N/A - Page not on disk)\t");
    }
    */
}

/* REF_WITH_CONTEXTをリストに集めるためのコールバック */
static int
__collect_context_callback(WT_REF *ref, char *uri, void *arg)
{
    CONTEXT_COLLECTOR *collector = (CONTEXT_COLLECTOR *)arg;

    char key1[256], key2[256];
    WT_IKEY *ikey_ptr;
    ikey_ptr = (WT_IKEY *)ref->key.ikey;
    memcpy(key1, (char *)((uint8_t *)ikey_ptr + sizeof(size_t)), ikey_ptr->size);
    key1[ikey_ptr->size] = '\0';
    //int c = 1;

    for(size_t i = 0; i < collector->count; i++){
        ikey_ptr = (WT_IKEY *)(collector->list[i].ref->key.ikey);
        memcpy(key2, (char *)((uint8_t *)ikey_ptr + sizeof(size_t)), ikey_ptr->size);
        key2[ikey_ptr->size] = '\0';
        if(strcmp(key1, key2) == 0) return 0;
    }
    //if(ref == collector->list[collector->count].ref) return 0;
    
    // リストの容量が足りなければ、reallocで拡張する
    if (collector->count >= collector->capacity) {
        collector->capacity = (collector->capacity == 0) ? 128 : collector->capacity * 2;
        REF_WITH_CONTEXT *new_list =
          realloc(collector->list, sizeof(REF_WITH_CONTEXT) * collector->capacity);
        if (new_list == NULL) return (ENOMEM);
        collector->list = new_list;
    }

    /* ページ参照と、その時のdhandleをペアで保存 */
    collector->list[collector->count].ref = ref;
    collector->list[collector->count].uri = uri;
    collector->count++;
    ikey_ptr = (WT_IKEY *)ref->key.ikey;
    /*
    if(ikey_ptr->size != 1 )
        printf("-> Child Key(Str): %.*s\t", (int)(ikey_ptr->size), (char *)((uint8_t *)ikey_ptr + sizeof(size_t)));
    else printf("  -> Child Key(Str): key0000000000\n");
    if((c++ % 3) == 0) printf("\n");
    printf("  -> Page URI      : %s\n", uri);
    */
    return (0);
}

/* キャッシュ上の全ページの参照とコンテキストをリストとして収集するAPI */
/*
*/
int wt_collect_page_refs(WT_CONNECTION *connection)
{
    WT_CONNECTION_IMPL *conn_impl;
    WT_SESSION *session = NULL;
    WT_CURSOR *dhandle_cursor = NULL;
    WT_CURSOR *table_cursor = NULL;
    CONTEXT_COLLECTOR collector;
    int ret = 0;
    char *uri;

    conn_impl = (WT_CONNECTION_IMPL *)connection;
    memset(&collector, 0, sizeof(collector));

    if ((ret = conn_impl->iface.open_session(&conn_impl->iface, NULL, NULL, &session)) != 0)
        return (ret);
    
    // 1. まず、データベース内の全テーブル(URI)のリストを取得するカーソルを開く
    if ((ret = session->open_cursor(session, "metadata:", NULL, NULL, &dhandle_cursor)) != 0)
        goto done;

    // 2. 全てのテーブルをループで処理
    while ((ret = dhandle_cursor->next(dhandle_cursor)) == 0) {
        if ((ret = dhandle_cursor->get_key(dhandle_cursor, &uri)) != 0)
            goto done;
        
        // ユーザーが作成したテーブルのみを対象とする
        if (strncmp(uri, "table:", 6) != 0)
            continue;

        // 3. 各テーブルに対して、中身をスキャンするためのカーソルを開く
        if ((ret = session->open_cursor(session, uri, NULL, NULL, &table_cursor)) != 0)
            continue; // エラーでも次のテーブルへ

        // 4. カーソルをループさせて、キャッシュ上にあるページを収集する
        while ((ret = table_cursor->next(table_cursor)) == 0) {
            WT_CURSOR_BTREE *cbt = (WT_CURSOR_BTREE *)table_cursor;
            // ページがキャッシュに乗っている場合のみリストに追加
            //if (cbt->ref != NULL && cbt->ref->page != NULL && F_ISSET(cbt->ref, WT_REF_FLAG_LEAF)) {
            if (cbt->ref != NULL && F_ISSET(cbt->ref, WT_REF_FLAG_LEAF) && (WT_REF_GET_STATE(cbt->ref) == WT_REF_MEM || WT_REF_GET_STATE(cbt->ref) == WT_REF_LOCKED)) {
                if ((ret = __collect_context_callback(cbt->ref, uri, &collector)) != 0) {
                    (void)table_cursor->close(table_cursor);
                    goto done;
                }
            }
        }
        (void)table_cursor->close(table_cursor);
        table_cursor = NULL;
    }

done:
    if (dhandle_cursor != NULL) (void)dhandle_cursor->close(dhandle_cursor);
    if (table_cursor != NULL) (void)table_cursor->close(table_cursor);
    if (session != NULL) (void)session->close(session, NULL);
    
    if (ret == WT_NOTFOUND) // ループの正常終了
        ret = 0;

    if (ret == 0) {
        ref_list = collector.list;
        ref_count = collector.count;
    } else {
        free(collector.list);
    }
    
    return (ret);
}

/* 収集したページ参照のリストを元に、各ページの詳細情報をダンプするAPI */
int
wt_dump_pages_from_refs(WT_CONNECTION *connection)
{
    WT_SESSION_IMPL *session;
    WT_REF *ref;
    int ret = 0, tret;
    uint32_t i;
    WT_CURSOR *cursor = NULL;
    WT_ITEM key, value;
    char last_uri[256] = "", key_string_buffer[256], uri[256];

    if ((ret = __wt_open_session((WT_CONNECTION_IMPL *)connection, NULL, NULL, false, &session)) != 0)
        return (ret);
    
    printf("\n========= Dumping all pages in cache =========\n");
    printf("Total pages found: %zu\n", ref_count);

    for (i = 0; i < ref_count; ++i) {
        ref = ref_list[i].ref;

        strncpy(uri, ref_list[i].uri, sizeof(last_uri) - 1);
        /* リストからrefとdhandleの両方を取り出す */

        if (F_ISSET(ref, WT_REF_FLAG_LEAF) && ref->page != NULL && WT_REF_GET_STATE(ref) == WT_REF_MEM){
            if (strcmp(last_uri, uri) != 0) {
                if (cursor != NULL) (void)cursor->close(cursor);
                if ((ret = ((WT_SESSION *)session)->open_cursor(((WT_SESSION *)session), uri, NULL, NULL, &cursor)) != 0) {
                    fprintf(stderr, " -> FAILED to open cursor.\n");
                    continue;
                }
                strncpy(last_uri, uri, sizeof(last_uri) - 1);
            }
            WT_IKEY *ikey_ptr = (WT_IKEY *)ref->key.ikey;
            memcpy(key_string_buffer, (char *)((uint8_t *)ikey_ptr + sizeof(size_t)), ikey_ptr->size);
            //key_string_buffer[ikey_ptr->size] = '\0'; // NULL終端文字を追加
            printf(" Page Size     : %zu bytes\t", __wt_atomic_loadsize(&ref->page->memory_footprint));
            if(strncmp(key_string_buffer, "key", 3) != 0) strcpy(key_string_buffer, "key0000000000");
            printf(" First Key : %s\t", key_string_buffer);
            cursor->set_key(cursor, key_string_buffer);
            if ((ret = cursor->search(cursor)) == 0) {
                do {
                    cursor->get_key(cursor, &key);
                    cursor->get_value(cursor, &value);
                    //printf("     - Key: %.*s", (int)key.size, (const char *)key.data);
                    //printf("     - Last Key: %.*s", (int)(strlen((const char *)key.data)), (const char *)key.data);
                    //printf(" | Value: (size %zu)\n", value.size);
                } while (cursor->next(cursor) == 0 && ((WT_CURSOR_BTREE *)cursor)->ref->page == ref->page);
            } else {
                printf("\n  -> FAILED to search for the first key of the page.\n");
            }
            printf("Last Key : %.*s\n", (int)(strlen((const char *)key.data)), (const char *)key.data);
            //printf(" | Value: (size %zu)\n", value.size);
        } else {
            /* 内部ページやルートページの場合は、基本的な情報のみ表示 */
            printf("  -> Page Type: %s (Content dump skipped)\t",
            __wt_ref_is_root(ref) ? "ROOT" : "INTERNAL");
            printf("  -> Ref State    : %s (%d)\n",
        ref_state_to_string(WT_REF_GET_STATE(ref)), WT_REF_GET_STATE(ref));
        }
    }
    if (cursor != NULL) (void)cursor->close(cursor);
    if ((tret = ((WT_SESSION *)session)->close(((WT_SESSION *)session), NULL)) != 0 && ret == 0) ret = tret;
    
    printf("=========================================================\n");
    return (ret);
}

/*
 * __my_count_all_keys_in_page --
 * ページ内の正規データ領域とインサートリストの両方をスキャンし、
 * 論理的なキーの総数を数え上げて返す自作関数。
 */
uint32_t my_count_all_keys_in_page(WT_REF *ref)
{
    WT_PAGE *page;
    uint32_t i, key_count = 0;
    WT_INSERT *ins;

    /* --- 安全のためのチェック --- */
    if (ref == NULL || (page = ref->page) == NULL) {
        printf("  -> Keys: (Page reference or page pointer is NULL)\n");
        return 0;
    }
    if (!F_ISSET(ref, WT_REF_FLAG_LEAF)) {
        printf("  -> Keys: (Not a leaf page)\n");
        return 0;
    }
    /* ===== 1. 正規のデータ領域 (page->entries) のスキャン ===== */
    key_count += page->entries;

    /* ===== 2. インサートリストのスキャン ===== */
    if (page->modify != NULL && page->modify->mod_row_insert != NULL) {
        
        /* 2a. 「最小キー」リストのスキャン */
        if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT_SMALLEST(page))) != NULL) 
            for (; ins != NULL; ins = *ins->next) key_count++;
        
        /* 2b. 各スロットのインサートリストをスキャン */
        for (i = 0; i < page->entries; ++i) {
            if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT_SLOT(page, i))) != NULL)
                for (; ins != NULL; ins = *ins->next) key_count++;
        }
    }
    return (key_count);
}


//ページ内の正規データ領域とインサートリストの両方から全キーを出力する自作関数。
void
__my_dump_all_keys_before_evict(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_PAGE *page;
    uint32_t i, key_count = 0;
    WT_INSERT *ins;
    void *key_data;
    size_t key_size;
    int print;

    /* --- 安全のためのチェック --- */
    if (ref == NULL || (page = ref->page) == NULL) {
        printf("  -> Keys: (Page reference or page pointer is NULL)\n");
        return;
    }
    if (!F_ISSET(ref, WT_REF_FLAG_LEAF)) {
        printf("  -> Keys: (Not a leaf page)\n");
        return;
    }
    
    printf("\n--- Dumping All Keys in Page %p ---\n", (void *)page);

    /* ===== 1. 正規のデータ領域 (page->entries) のスキャン ===== */
    if (page->entries > 0) {
        WT_ITEM key_item;
        WT_ROW *row_ptr;
        printf("  -> Keys from Main Area (%u entries):\n", page->entries);
        for (i = 0; i < page->entries; ++i) {
            row_ptr = page->pg_row + i;
            if (__wt_row_leaf_key(session, page, row_ptr, &key_item, false) == 0) {
                key_count++;
            }
        }
        printf("       - Main Last Key %-5u: %.*s\n",
                key_count, (int)key_item.size, (char *)key_item.data);
    } else {
        printf("  -> Keys from Main Area: 0 entries\n");
    }

    /* ===== 2. インサートリストのスキャン ===== */
    if (page->modify != NULL && page->modify->mod_row_insert != NULL) {
        printf("  -> Keys from Insert Lists:\n");
        
        /* 2a. 「最小キー」リストのスキャン */
        if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT_SMALLEST(page))) != NULL) {
            print = 0;
            key_size = 0;
            for (; ins != NULL; ins = *ins->next) {
                key_data = WT_INSERT_KEY(ins);
                key_size = WT_INSERT_KEY_SIZE(ins);
                if(print == 0){
                    printf("       - Insert Key %-5u (from 'smallest' list): %.*s\n",
                       key_count, (int)key_size, (char *)key_data);
                    print = 1;
                    }
                key_count++;
            }
            printf("       - Insert Key %-5u (from 'smallest' list): %.*s\n",
                       key_count, (int)key_size, (char *)key_data);
        }
        
        /* 2b. 各スロットのインサートリストをスキャン */
        for (i = 0; i < page->entries; ++i) {
            if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT_SLOT(page, i))) != NULL) {
                print = 0;
                key_size = 0;
                for (; ins != NULL; ins = *ins->next) {
                    key_data = WT_INSERT_KEY(ins);
                    key_size = WT_INSERT_KEY_SIZE(ins);
                    if(print == 0){
                        printf("       - Insert Key %-5u (from list for slot %u): %.*s\n",
                            key_count, i, (int)key_size, (char *)key_data);
                    }
                    key_count++;
                }
                printf("       - Insert Key %-5u (from 'smallest' list): %.*s\n",
                       key_count, (int)key_size, (char *)key_data);
            }
        }
    } else {
        printf("  -> Keys from Insert Lists: Not present\n");
    }

    printf("--- Total Keys Found: %u ---\n\n", key_count);
}

static void my_log(const char *format, ...) {
    // 追記モード(a)で開く
    FILE *fp = fopen(MY_LOG_FILE, "a");
    if (fp == NULL) return; // 開けなければ諦める（クラッシュさせない）
    // タグを付ける
    //fprintf(fp, "[MYDEBUG] ");

    // 引数のフォーマット出力
    va_list args;
    va_start(args, format);
    vfprintf(fp, format, args);
    va_end(args);

    fflush(fp);
    fclose(fp);
}

// GPA正当性検証用関数
static void log_page_content(void *vaddr, uintptr_t gpa) {
    unsigned char *p = (unsigned char *)vaddr;    
    my_log("VAddr: %p maps to GPA: 0x%lx\n", vaddr, gpa);
    
    // 先頭 48バイト を16進数でダンプ
    my_log("%02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X\n",
           p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
    my_log("%02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X\n",
           p[16], p[17], p[18], p[19], p[20], p[21], p[22], p[23], p[24], p[25], p[26], p[27], p[28], p[29], p[30], p[31]);
    my_log("%02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X %02X\n",
           p[32], p[33], p[34], p[35], p[36], p[37], p[38], p[39], p[40], p[41], p[42], p[43], p[44], p[45], p[46], p[47]);

    my_log("--------------------------\n");
}

// 仮想アドレス(User VA) -> 物理アドレス(GPA) 変換
// /proc/self/pagemap を使用 (要root権限)
static uintptr_t GVA_to_GPA(void *vaddr) {
    int fd = open("/proc/self/pagemap", O_RDONLY);
    if (fd < 0) {
        // エラーログを毎回出すと重いので省略、または頻度制限
        return 0;
    }

    uint64_t vaddr_val = (uintptr_t)vaddr;
    uint64_t pfn_item_offset = (vaddr_val / 4096) * 8;
    uint64_t pfn_item = 0;

    if (pread(fd, &pfn_item, 8, pfn_item_offset) != 8) {
        close(fd);
        return 0;
    }
    close(fd);

    if ((pfn_item & (1ULL << 63)) == 0) {
        return 0; 
    }
    // --- ここで生の値をチェック ---
    uint64_t pfn = pfn_item & ((1ULL << 55) - 1);

    //my_log("VAddr: %p -> RawItem: 0x%016lx | Present: %d | PFN: 0x%lx\n", vaddr, pfn_item, present, pfn);
    //my_log("VAddr: %p -> GPA: 0x%lx\n", vaddr, (pfn * 4096) + (vaddr_val % 4096));

    return (pfn * 4096) + (vaddr_val % 4096);
}

// QEMUにリストを送信する関数
static void notify_qemu_eviction() {
    // カウント0ならロックする前に帰る
    if (evict_list.count == 0) return;

    // 1. 物理アドレス変換 (ロックの外で行う方が効率が良い)
    uintptr_t list_phys_addr = GVA_to_GPA(&evict_list);
    
    // 変換失敗なら終了
    if (list_phys_addr == 0) {
        fprintf(stderr, "WiredTiger Error: Failed to get physical address of eviction list.\n");
        return;
    }

    // 2. 権限チェック
    if (iopl(3) < 0) {
        static int iopl_error_printed = 0;
        if (!iopl_error_printed) {
            fprintf(stderr, "WiredTiger Error: iopl(3) failed: %s. (Need sudo?)\n", strerror(errno));
            iopl_error_printed = 1;
        }
        return;
    }

    // 3. ロック取得 (ここからクリティカルセクション)
    pthread_mutex_lock(&qemu_lock);

    // ロック取得後、念のため再度カウントチェック (他のスレッドが既に送ったかもしれない)
    if (evict_list.count == 0) {
        pthread_mutex_unlock(&qemu_lock);
        return;
    }

    // メモリバリア
    __asm__ __volatile__("mfence" ::: "memory");

    
    // 送信
    outl((uint32_t)(list_phys_addr & 0xFFFFFFFF), QEMU_PORT_DATA_LOW);
    outl((uint32_t)(list_phys_addr >> 32), QEMU_PORT_DATA_HIGH);
    
    my_log("WiredTiger: Sent eviction list at GPA 0x%lx with %zu entries to QEMU.(thread %lu)\n", 
        list_phys_addr, evict_list.count, pthread_self());

    // トリガー
    outl(1, QEMU_PORT_MONGO_EVICT);

    // リセット
    evict_list.count = 0;

    // 4. ロック解除 (必須！！)
    pthread_mutex_unlock(&qemu_lock);
}

/* QEMUスキップリストへの追加を行う専用関数 */
static void 
add_mongoDB_evict_List(uintptr_t GVA, uintptr_t GPA) 
{

    if (!pthread_equal(pthread_self(), clearing_thread_id)) {
        return;
    }
    if (GPA != 0) {
        // グローバルリストに追加
        //evict_list.vaddr_list[evict_list.count] = (uint64_t)GVA;
        evict_list.gpa_list[evict_list.count++] = (uint64_t)GPA;
        evict_list.total_pages++;
        
        // バッファがいっぱいなら送信
        if (evict_list.count >= BATCH_SIZE) {
            notify_qemu_eviction();
        }

        // 1ページクリア用のコード：リスト１つずつ送る
        /*
        if (evict_list.count > -1) {
            // デバッグログ出力
            unsigned char *p = (unsigned char *)ref->page; 
            my_log("[MONGO DATA] VAddr: %p, GPA: 0x%lx, Data: %02X %02X %02X %02X ...", 
                p, phys_addr, p[0], p[1], p[2], p[3]);
            notify_qemu_eviction();
        }
        */
    }
}
/* QEMUスキップリストへの追加を行う専用関数 */
/*
static void 
add_mongoDB_evict_List(WT_REF *ref) 
{
    if (ref == NULL || ref->page == NULL) return;

    // ページ構造体の物理アドレスを取得
    uintptr_t phys_addr = GVA_to_GPA(ref->page);


    if (phys_addr != 0) {
        // 検証関数の呼び出し
        if (evict_list.count < 5) { 
            log_page_content(ref->page, phys_addr);
        }

        // グローバルリストに追加
        evict_list.vaddr_list[evict_list.count] = (uint64_t)ref->page;
        evict_list.gpa_list[evict_list.count++] = (uint64_t)phys_addr;
        evict_list.total_pages++;
        
        // バッファがいっぱいなら送信
        if (evict_list.count >= BATCH_SIZE) {
            notify_qemu_eviction();
        }
    }
}
*/

// QEMUからの指令を待つスレッド
static void* qemu_monitor_thread(void *arg) {
    WT_CONNECTION *conn = (WT_CONNECTION *)arg;
    
    my_log("[MONITOR] QEMU Monitor Thread Started.\n");

    // ルート権限チェック (iopl用)
    if (iopl(3) < 0) {
        my_log("[MONITOR ERROR] iopl failed. Cannot monitor QEMU.\n");
        return NULL;
    }

    while (true) {
        // 1. ポートを読んでフラグチェック
        // inl: 32bit read (QEMU側の size=4 に合わせる)
        uint32_t flag = inl(QEMU_PORT_MONGO_CMD);
        //my_log("[MONITOR] Checked QEMU port, flag=%u\n", flag);
        if (flag == 1) {
            my_log("[MONITOR] QEMU migration signal detected! Executing wt_clear_cache...");
            // 2. 「了解した」とQEMUに伝える（フラグを0に戻させる）
            outl(0, QEMU_PORT_MONGO_CMD);
            // これをやらないと、このループ内で何度も実行されてしまう
            // 3. キャッシュクリア実行
            wt_clear_cache(conn);
            outl(2, QEMU_PORT_MONGO_CMD);
            my_log("[MONITOR] Cache Clear Finished. Resuming monitoring.");
        }
        // CPUを使いすぎないように少し寝る (1000ms)
        usleep(10000000); 
    }
    return NULL;
}

// static変数で多重起動を防止
static int monitor_thread_started = 0;

void start_qemu_monitor(WT_CONNECTION *conn) {
    // 既に起動済みなら何もしない
    if (monitor_thread_started) {
        my_log("[MONITOR] Thread already running. Skipping.");
        return;
    }
    
    monitor_thread_started = 1; // フラグを立てる

    pthread_t thread_id;
    int ret = pthread_create(&thread_id, NULL, qemu_monitor_thread, (void*)conn);
    if (ret != 0) {
        my_log("[MONITOR ERROR] Failed to create thread: %d", ret);
        monitor_thread_started = 0; // 失敗したらフラグを戻す
    } else {
        pthread_detach(thread_id);
        my_log("[MONITOR] Monitor thread launched successfully.");
    }
}

/*
void start_qemu_monitor(WT_CONNECTION *conn) {
    pthread_t thread_id;
    // スレッド作成
    int ret = pthread_create(&thread_id, NULL, qemu_monitor_thread, (void*)conn);
    if (ret == 0) {
        // デタッチ（メインスレッド終了時に道連れで終了させるため）
        pthread_detach(thread_id);
    } else {
        my_log("[MONITOR ERROR] Failed to create thread: %d", ret);
    }
}
*/
void
dump_evict_queue_list(WT_CONNECTION_IMPL *conn){
    WT_EVICT *evict = conn->evict;
my_log("=== Final Eviction Queue Dump ===\n");

    // 現在のfill_queueと、もう片方のキューを取得
    WTI_EVICT_QUEUE *q_list[2];
    q_list[0] = evict->evict_fill_queue;
    q_list[1] = evict->evict_queues + (1 - (q_list[0] - evict->evict_queues));

    const char *q_names[] = {"Fill Queue", "Other Queue"};

    for (int q_idx = 0; q_idx < 2; q_idx++) {
        WTI_EVICT_QUEUE *q = q_list[q_idx];
        my_log("--- %s (entries: %u) ---\n", q_names[q_idx], q->evict_entries);

        for (uint32_t i = 0; i < q->evict_entries; i++) {
            WT_REF *r = q->evict_queue[i].ref;
            if (r == NULL) {
                my_log("  [%u] NULL\n", i);
                continue;
            }

            // ページタイプの判定
            const char *type = "UNKNOWN";
            if (__wt_ref_is_root(r)) {
                type = "ROOT";
            } else if (F_ISSET(r, WT_REF_FLAG_INTERNAL)) {
                type = "INTERNAL";
            } else {
                type = "LEAF"; // 通常はこれが退避対象
            }

            // ファイル名の取得について:
            // WT_PAGE構造体にdhandleがない場合、安全にファイル名を取るのは難しいため
            // 無理にアクセスせず、アドレスとタイプだけを表示するのが最もクラッシュしにくい方法です。
            my_log("  [%u] Ref: %p, Type: %s\n", i, (void *)r, type);
        }
    }
    my_log("=====================================\n");
}

/*
 * wt_clear_cache --
 * Force evict all pages from the cache. This is a new custom API function.
 */
int
wt_clear_cache(WT_CONNECTION *connection)
{
    WT_CONNECTION_IMPL *conn_impl;
    WT_SESSION_IMPL *session_impl;
    WT_SESSION *session; // API呼び出し用の通常セッションポインタ
    WT_EVICT *evict; // evict構造体へのポインタを追加
    int ret = 0, tret;
    uint64_t current_size, prev_size = UINT64_MAX;

    my_log("start wt_clear_cache\n");

    conn_impl = (WT_CONNECTION_IMPL *)connection;
    if (conn_impl == NULL) {
        my_log("Error: connection is NULL\n");
    }
    evict = conn_impl->evict; // evict構造体を取得
    if (evict == NULL) {
        my_log("Error: eviction server is not running or not initialized (evict is NULL)\n");
        // Evictionサーバーが動いていないならキャッシュクリア処理もできないため、エラーで戻る
    }

    if ((ret = __wt_open_session(conn_impl, NULL, NULL, false, &session_impl)) != 0)
        return (ret);
    
    session = (WT_SESSION *)session_impl;
    F_SET(session_impl, WT_SESSION_EVICTION);
    /* ▼▼▼ ステップ1: チェックポイントでダーティページを全てクリーンにする ▼▼▼ */
    if ((ret = session->checkpoint(session, NULL)) != 0) {
        fprintf(stderr, "Checkpoint failed: %s\n", wiredtiger_strerror(ret));
        tret = __wt_session_close_internal(session_impl); // エラーでもセッションは閉じる
        (void)tret;
        return (ret);
    }
    // バックグラウンドのチェックポイント処理と競合してクラッシュするのを防ぎます
    __wt_spin_lock(session_impl, &conn_impl->checkpoint_lock);
    // 2. ハンドル操作(Sweep/Open/Close)を止める
    //__wt_writelock(session_impl, &conn_impl->dhandle_lock);
    // Evictionロックの取得
    __wt_spin_lock(session_impl, &evict->evict_pass_lock);
    
    clearing_cache = true;
    current_size = __wt_cache_pages_inuse(conn_impl->cache);
    
    wt_pause_eviction_server(connection);
    // 現在のスレッドIDを記録
    clearing_thread_id = pthread_self();
    // リストの初期化
    evict_list.count = 0;

    while (stable_count < 10) { // 10回連続でサイズが変わらなければ完了とみなす
        /*
        if((stable_count != 0) && (stable_count % 10) == 0){
            printf("...Recheckpoint...\n");
            __wt_spin_unlock(session_impl, &evict->evict_pass_lock);
            if((ret = session->checkpoint(session, NULL)) != 0) {
                fprintf(stderr, "Checkpoint failed: %s\n", wiredtiger_strerror(ret));
                tret = __wt_session_close_internal(session_impl); // エラーでもセッションは閉じる
                (void)tret;
                return (ret);
            }
            __wt_spin_lock(session_impl, &evict->evict_pass_lock);
        }
        */
        prev_size = __wt_cache_pages_inuse(conn_impl->cache);

        // 1. 補充: リスト作成とCompaction
        int walk_ret = __my_evict_lru_walk(session_impl);

        // 2. 在庫確認: 2つのキューのどちらかにデータがあるかチェック
        // evict->evict_queues は配列なので、0番と1番を確認
        uint32_t total_entries = 0;
        WTI_EVICT_QUEUE *q_list[2];
        q_list[0] = conn_impl->evict->evict_fill_queue;
        q_list[1] = conn_impl->evict->evict_queues + (1 - (q_list[0] - conn_impl->evict->evict_queues));
        total_entries += q_list[0]->evict_entries;
        total_entries += q_list[1]->evict_entries;

        // 3. 終了判定
        // 「補充失敗(NOTFOUND)」かつ「在庫なし(0)」なら終了
        if (walk_ret == WT_NOTFOUND && total_entries == 0) {
            my_log("Eviction queues are completely empty. Cache clear complete.\n");
            ret = 0;
            break;
        }
        
        // 4. 退避実行
        // 在庫があるなら、walkの結果に関わらず退避を試みる
        if (total_entries > 0) {
            // my_log("Evicting... (Pending entries: %u)\n", total_entries);
            if ((ret = __evict_lru_pages(session_impl, false)) != 0) break;
        }

        current_size = __wt_cache_pages_inuse(conn_impl->cache);
        //printf("%d:  -> Page size: %" PRIu64 " pages\n", i++, current_size);

        if (current_size >= prev_size) stable_count++;
        else stable_count = 0;
        
        //1ページだけのクリア＆再構成のためのコード追加
        //if (metadata_count >= 1) break;
        }    
    
    //wt_dump_pages_from_refs(connection);

    //wt_resume_eviction_server(connection);

    clearing_cache = false;
    clearing_thread_id = 0;

    /*
    printf("total pagecount: %zu\n", evict_list.total_pages);
    printf("metadata_count: %zu\n", metadata_count);
    my_log("total pagecount: %zu\n", evict_list.total_pages);
    my_log("metadata_count: %zu\n", metadata_count);
    for(size_t i = 0; i < evict_list.count; i++) {
        my_log("Evicted Page %zu: GPA: 0x%lx\n", i, (unsigned long)evict_list.gpa_list[i]);
    }
    */
    if (evict_list.count > 0) {
        notify_qemu_eviction();
    }

    __wt_spin_unlock(session_impl, &evict->evict_pass_lock);
    //__wt_writeunlock(session_impl, &conn_impl->dhandle_lock);
    __wt_spin_unlock(session_impl, &conn_impl->checkpoint_lock);

    if ((tret = __wt_session_close_internal(session_impl)) != 0 && ret == 0) ret = tret;

    write_metadata(conn_impl->home);
    //重要：再構成が完了したら、グローバルリストが確保したメモリを解放し、カウンタをリセットして、次回の実行に備える。
    /*
    if (metadata_list != NULL) {
        free(metadata_list);
        metadata_list = NULL;
    }
    metadata_count = 0;
    */
    outl(2, QEMU_PORT_MONGO_CMD);

    dump_evict_queue_list(conn_impl);
    return (ret);
}
