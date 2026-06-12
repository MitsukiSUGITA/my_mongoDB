#!/bin/bash
set -e

# ==========================================
# 引数のチェックと設定
# ==========================================
if [ "$#" -lt 3 ]; then
    echo "❌ エラー: 引数が不足しています。"
    echo "使い方: $0 <VMのIPアドレス> <モード: ro | rw | wo> <VMのメモリ(GB)> [Ops/sec(省略可:1800)] [負荷率%(省略可:25)]"
    echo "【実行例】"
    echo "  高負荷検証 (25%) : $0 192.168.200.69 rw 8 1800 25"
    exit 1
fi

VM_IP="$1"
MODE="$2"
VM_MEMORY_GB="$3"
# 前回のご相談を踏まえ、Target Opsのデフォルトを高負荷だが安定する1800付近に変更しています
TARGET_OPS="${4:-1800}" 
LOAD_FACTOR_PERCENT="${5:-25}"

MONGO_URL="mongodb://${VM_IP}:27017/ycsb?w=1"
YCSB_BIN="./bin/ycsb"

# --- モード設定とログファイル名の動的生成 ---
if [ "$MODE" = "ro" ]; then
    WORKLOAD_RUN="workloads/workloadc"
    RESULT_LOG="ycsb_ro_${VM_MEMORY_GB}GB_${LOAD_FACTOR_PERCENT}pct.log"
    MODE_NAME="Read-Only (Read 100%)"
elif [ "$MODE" = "rw" ]; then
    WORKLOAD_RUN="workloads/workloada"
    RESULT_LOG="ycsb_rw_${VM_MEMORY_GB}GB_${LOAD_FACTOR_PERCENT}pct.log"
    MODE_NAME="Read-Write (Read 50% / Update 50%)"
elif [ "$MODE" = "wo" ]; then
    WORKLOAD_RUN="workloads/workloada"
    RESULT_LOG="ycsb_wo_${VM_MEMORY_GB}GB_${LOAD_FACTOR_PERCENT}pct.log"
    MODE_NAME="Write-Only (Update 100%)"
else
    echo "❌ エラー: 'ro', 'rw', 'wo' のいずれかを指定してください。"
    exit 1
fi

# --- データサイズの自動計算 ---
RECORD_COUNT=$(( VM_MEMORY_GB * 100000 * LOAD_FACTOR_PERCENT / 100 ))
RUN_COUNT=$(( RECORD_COUNT * 5 ))

echo "=================================================="
echo "    YCSB Auto-Scaling & Auto-Load Benchmark     "
echo "=================================================="
echo "Target VM   : $MONGO_URL"
echo "Mode        : $MODE_NAME"
echo "VM Memory   : ${VM_MEMORY_GB} GB"
echo "Load Factor : ${LOAD_FACTOR_PERCENT} %"
echo "Records     : $(printf "%'d\n" $RECORD_COUNT)"
echo "Target Ops  : $TARGET_OPS ops/sec"
echo "Log File    : $RESULT_LOG"
echo "--------------------------------------------------"

# ==========================================
# Phase 1: データの状態確認と自動ロード
# ==========================================
echo "▶ [Phase 1] データの存在確認を行っています..."

CURRENT_COUNT=$(mongosh "mongodb://${VM_IP}:27017/ycsb" --quiet --eval "db.usertable.countDocuments({})" 2>/dev/null || echo "0")

# 改善点1: -eq ではなく -ge (以上) で判定。既存データが十分ならスキップ
if [ "$CURRENT_COUNT" -ge "$RECORD_COUNT" ]; then
    echo "✅ [SKIP] データベースには十分なデータ（現在: ${CURRENT_COUNT}件 / 目標: ${RECORD_COUNT}件）が存在します。ロードをスキップします。"
else
    echo "⚠️ データが不足している、または存在しません。(現在: ${CURRENT_COUNT}件 / 目標: ${RECORD_COUNT}件)"
    
    if [ "$CURRENT_COUNT" -gt 0 ]; then
        echo "🧹 古いデータを削除（Drop）しています..."
        mongosh "mongodb://${VM_IP}:27017/ycsb" --quiet --eval "db.usertable.drop()" 2>/dev/null || true
    fi

    echo "▶ データのロードを開始します (少々お待ちください)..."
    $YCSB_BIN load mongodb -s -P "$WORKLOAD_RUN" \
      -p mongodb.url="$MONGO_URL" \
      -p recordcount="$RECORD_COUNT" \
      -p fieldlength=1000 \
      -threads 16
    echo "✅ Load 完了"
fi
echo "--------------------------------------------------"

# ==========================================
# Phase 2: キャッシュのウォームアップ（超高速化）
# ==========================================
echo "▶ [Phase 2] キャッシュのウォームアップを開始します..."
# 改善点2: YCSBを使わず、MongoDB側で全件を強制的にメモリ(WiredTigerキャッシュ)に引き上げる
mongosh "mongodb://${VM_IP}:27017/ycsb" --quiet --eval "db.usertable.find().itcount()" > /dev/null
echo "✅ Warmup 完了 (データがメモリにキャッシュされました)"
echo "--------------------------------------------------"

# ==========================================
# Phase 3: 本番計測（バックグラウンド実行）
# ==========================================
echo "▶ [Phase 3] 本番計測を開始します (実行時間: 180秒)..."

if [ "$MODE" = "wo" ]; then
    $YCSB_BIN run mongodb -s -P "$WORKLOAD_RUN" \
      -p mongodb.url="$MONGO_URL" \
      -p target="$TARGET_OPS" \
      -threads 4 \
      -p recordcount="$RECORD_COUNT" \
      -p operationcount="$RUN_COUNT" \
      -p maxexecutiontime=180 \
      -p readproportion=0 -p updateproportion=1 > "$RESULT_LOG" 2>&1 &
else
    $YCSB_BIN run mongodb -s -P "$WORKLOAD_RUN" \
      -p mongodb.url="$MONGO_URL" \
      -p target="$TARGET_OPS" \
      -threads 4 \
      -p recordcount="$RECORD_COUNT" \
      -p operationcount="$RUN_COUNT" \
      -p maxexecutiontime=180 > "$RESULT_LOG" 2>&1 &
fi

YCSB_PID=$!

echo "=================================================="
echo "✅ 計測($MODE_NAME)がバックグラウンド(PID: $YCSB_PID)で開始されました！"
echo "  tail -f $RESULT_LOG"
echo ""
echo "🔥 【重要】この状態でVMのマイグレーションを開始してください！ 🔥"
echo "=================================================="