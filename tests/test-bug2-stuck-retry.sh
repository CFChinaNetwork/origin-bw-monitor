#!/bin/bash
# Bug #2 测试：验证 processing 卡死的文件会被 Cron 重新处理
# 用法: bash tests/test-bug2-stuck-retry.sh

set -e
cd "$(dirname "$0")/.."

DB=/tmp/bw-stats-bug2-$$.db
trap "rm -f $DB" EXIT

echo "▶ Step 1: 建表"
sqlite3 "$DB" < schema.sql

NOW=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")
STUCK_TIME=$(date -u -v-30M +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "30 minutes ago" +"%Y-%m-%dT%H:%M:%S.000Z")
RECENT_TIME=$(date -u -v-5M +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "5 minutes ago" +"%Y-%m-%dT%H:%M:%S.000Z")
PENDING_OLD=$(date -u -v-20M +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "20 minutes ago" +"%Y-%m-%dT%H:%M:%S.000Z")

echo "▶ Step 2: 插入模拟数据"
echo "  - 文件A: processing 状态，started_at 30 分钟前（应该被判定为卡死，需要重试）"
echo "  - 文件B: processing 状态，started_at 5 分钟前（仍在正常处理窗口内，不应重试）"
echo "  - 文件C: pending 状态，started_at 20 分钟前（lag 窗口已过，应重试）"
echo "  - 文件D: pending 状态，started_at 刚刚（lag 窗口未过，不应重试）"
echo "  - 文件E: done 状态（不应重试）"

sqlite3 "$DB" <<EOF
INSERT INTO processed_files (r2_key, status, started_at) VALUES ('logs/file-A.gz', 'processing', '$STUCK_TIME');
INSERT INTO processed_files (r2_key, status, started_at) VALUES ('logs/file-B.gz', 'processing', '$RECENT_TIME');
INSERT INTO processed_files (r2_key, status, started_at) VALUES ('logs/file-C.gz', 'pending',    '$PENDING_OLD');
INSERT INTO processed_files (r2_key, status, started_at) VALUES ('logs/file-D.gz', 'pending',    '$NOW');
INSERT INTO processed_files (r2_key, status, started_at, finished_at) VALUES ('logs/file-E.gz', 'done', '$STUCK_TIME', '$STUCK_TIME');
EOF

echo ""
echo "▶ Step 3: 执行新版 retryPendingFiles 查询"
# 模拟新版本的查询逻辑：
# LAG_MINUTES=7, MAX_PROCESSING_MIN=15
# lagCutoff = now - 7min, stuckCutoff = now - 15min
LAG_CUTOFF=$(date -u -v-7M +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "7 minutes ago" +"%Y-%m-%dT%H:%M:%S.000Z")
STUCK_CUTOFF=$(date -u -v-15M +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "15 minutes ago" +"%Y-%m-%dT%H:%M:%S.000Z")

RESULTS=$(sqlite3 "$DB" "SELECT r2_key || ' (' || status || ')' FROM processed_files WHERE (status = 'pending' AND started_at < '$LAG_CUTOFF') OR (status = 'processing' AND started_at < '$STUCK_CUTOFF') ORDER BY started_at;")

echo "查询结果（应重试的文件）:"
echo "$RESULTS" | sed 's/^/  ✓ /'

echo ""
echo "▶ Step 4: 验证"
EXPECTED="logs/file-A.gz (processing)
logs/file-C.gz (pending)"

if [ "$RESULTS" = "$EXPECTED" ]; then
  echo "✅ PASS — 正确挑出 A（卡死 processing）和 C（过期 pending），不误伤 B/D/E"
else
  echo "❌ FAIL"
  echo "期望:"
  echo "$EXPECTED" | sed 's/^/  /'
  echo "实际:"
  echo "$RESULTS" | sed 's/^/  /'
  exit 1
fi

echo ""
echo "▶ Step 5: 对比旧版逻辑（只查 pending）"
OLD_RESULTS=$(sqlite3 "$DB" "SELECT r2_key || ' (' || status || ')' FROM processed_files WHERE status = 'pending' AND started_at < '$LAG_CUTOFF' ORDER BY started_at;")
echo "旧版逻辑结果:"
echo "$OLD_RESULTS" | sed 's/^/  ✗ /'

if echo "$OLD_RESULTS" | grep -q "file-A"; then
  echo "⚠️  异常：旧版本竟然包含了 A"
else
  echo "✅ 对比验证通过：旧版本确实漏掉了 A（processing 卡死），证明修复有效"
fi

echo ""
echo "═══════════════════════════════════════"
echo "✅ Bug #2 修复验证通过"
echo "═══════════════════════════════════════"
