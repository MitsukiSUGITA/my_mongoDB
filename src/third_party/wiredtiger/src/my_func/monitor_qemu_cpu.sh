# 1. バックグラウンド(&)でpidstatによる計測を開始する (一旦.logとして保存)
pidstat -p $(pgrep -n -f "qemu-system") 1 > cpu_usage_xbzrle.log &
PIDSTAT_PID=$! # 今起動したpidstat自身のPIDを記憶しておく

echo "✅ CPU計測を開始しました。QEMUモニターからマイグレーションを開始してください。"
echo "終了したら Enter キーを押してください..."

# 2. ユーザーの入力（Enterキー）を待つ
read wait_for_enter

# 3. Enterが押されたら、バックグラウンドのpidstatを終了(kill)する
kill $PIDSTAT_PID
echo "🛑 CPU計測を終了しました。"

# 4. [追加] 計測結果からヘッダーを除去し、カンマ区切りの本物のCSVに変換する
grep -E '^[0-9]' cpu_usage_xbzrle.log | tr -s ' ' ',' > cpu_usage_xbzrle.csv
echo "✅ エクセル用ファイル (cpu_usage_xbzrle.csv) を生成しました！"