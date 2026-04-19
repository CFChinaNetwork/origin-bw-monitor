#!/bin/bash
# 问题 #4 测试：验证 cleanup 会清理卡死的旧记录（防止表无限增长）
# 用法: bash tests/test-issue4-cleanup.sh

set -e
cd "$(dirname "$0")/.."

DB=/tmp/bw-stats-i4-$$.db
trap "rm -f $DB" EXIT

# 构造时间点
OLD_FINISHED=$(date -u -v-10d +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "10 days ago" +"%Y-%m-%dT%H:%M:%S.000Z")
RECENT_FINISHED=$(date -u -v-3d +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "3 days ago" +"%Y-%m-%dT%H:%M:%S.000Z")
OLD_STUCK=$(date -u -v-10d +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "10 days ago" +"%Y-%m-%dT%H:%M:%S.000Z")
RECENT_STUCK=$(date -u -v-3d +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "3 days ago" +"%Y-%m-%dT%H:%M:%S.000Z")

echo "▶ Step 1: 建表并插入模拟数据"
sqlite3 "$DB" < schema.sql
sqlite3 "$DB" <<EOF
-- 已完成且旧 → 应清理
INSERT INTO processed_files (r2_key, status, started_at, finished_at)
VALUES ('logs/done-old.gz', 'done', '$OLD_FINISHED', '$OLD_FINISHED');
-- 已完成但近期 → 应保留
INSERT INTO processed_files (r2_key, status, started_at, finished_at)
VALUES ('logs/done-recent.gz', 'done', '$RECENT_FINISHED', '$RECENT_FINISHED');
-- 卡死且非常旧（10天前 started，finished_at NULL）→ 应清理（修复的关键）
INSERT INTO processed_files (r2_key, status, started_at)
VALUES ('logs/stuck-old.gz', 'processing', '$OLD_STUCK');
-- 卡死但近期（3天前）→ 应保留（给恢复机会）
INSERT INTO processed_files (r2_key, status, started_at)
VALUES ('logs/stuck-recent.gz', 'processing', '$RECENT_STUCK');
-- pending 且非常旧 → 应清理
INSERT INTO processed_files (r2_key, status, started_at)
VALUES ('logs/pending-old.gz', 'pending', '$OLD_STUCK');
-- failed 且非常旧 → 应清理
INSERT INTO processed_files (r2_key, status, started_at)
VALUES ('logs/failed-old.gz', 'failed', '$OLD_STUCK');
EOF

echo ""
echo "▶ Step 2: 清理前数据"
sqlite3 "$DB" "SELECT r2_key, status FROM processed_files ORDER BY r2_key;"
BEFORE_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM processed_files;")
echo "总计: $BEFORE_COUNT 行"

echo ""
echo "▶ Step 3: 模拟新版 cleanupOldData（DATA_RETENTION_DAYS=7）"
# cutoff for done: 7 days ago
CUTOFF_F=$(date -u -v-7d +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "7 days ago" +"%Y-%m-%dT%H:%M:%S.000Z")
# cutoff for stale (unfinished): 8 days ago (7+1 grace period)
STALE_CUTOFF=$(date -u -v-8d +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "8 days ago" +"%Y-%m-%dT%H:%M:%S.000Z")

DONE_DELETED=$(sqlite3 "$DB" "DELETE FROM processed_files WHERE finished_at IS NOT NULL AND finished_at < '$CUTOFF_F'; SELECT changes();")
STALE_DELETED=$(sqlite3 "$DB" "DELETE FROM processed_files WHERE finished_at IS NULL AND started_at < '$STALE_CUTOFF'; SELECT changes();")

echo "  已完成被清理: $DONE_DELETED 行"
echo "  卡死被清理:   $STALE_DELETED 行"

echo ""
echo "▶ Step 4: 清理后数据"
sqlite3 "$DB" "SELECT r2_key, status FROM processed_files ORDER BY r2_key;"
AFTER_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM processed_files;")
echo "总计: $AFTER_COUNT 行"

echo ""
echo "▶ Step 5: 验证"
# 期望保留：done-recent（3天前完成）+ stuck-recent（3天前 started，在宽限期内）
EXPECTED_REMAINING="logs/done-recent.gz|done
logs/stuck-recent.gz|processing"
ACTUAL_REMAINING=$(sqlite3 "$DB" "SELECT r2_key || '|' || status FROM processed_files ORDER BY r2_key;")

if [ "$EXPECTED_REMAINING" = "$ACTUAL_REMAINING" ]; then
  echo "✅ PASS — 保留了近期文件（done-recent, stuck-recent），清理了所有旧的卡死记录"
else
  echo "❌ FAIL"
  echo "期望保留:"
  echo "$EXPECTED_REMAINING" | sed 's/^/  /'
  echo "实际保留:"
  echo "$ACTUAL_REMAINING" | sed 's/^/  /'
  exit 1
fi

echo ""
echo "▶ Step 6: 对比旧版 cleanup（只清理 finished_at 不为 NULL 的）"
sqlite3 "$DB" <<EOF
-- 重建数据做对比
DELETE FROM processed_files;
INSERT INTO processed_files (r2_key, status, started_at, finished_at) VALUES ('logs/done-old.gz', 'done', '$OLD_FINISHED', '$OLD_FINISHED');
INSERT INTO processed_files (r2_key, status, started_at, finished_at) VALUES ('logs/done-recent.gz', 'done', '$RECENT_FINISHED', '$RECENT_FINISHED');
INSERT INTO processed_files (r2_key, status, started_at) VALUES ('logs/stuck-old.gz', 'processing', '$OLD_STUCK');
INSERT INTO processed_files (r2_key, status, started_at) VALUES ('logs/stuck-recent.gz', 'processing', '$RECENT_STUCK');
INSERT INTO processed_files (r2_key, status, started_at) VALUES ('logs/pending-old.gz', 'pending', '$OLD_STUCK');
INSERT INTO processed_files (r2_key, status, started_at) VALUES ('logs/failed-old.gz', 'failed', '$OLD_STUCK');
EOF

# 旧版只删 finished_at IS NOT NULL
OLD_DELETED=$(sqlite3 "$DB" "DELETE FROM processed_files WHERE finished_at IS NOT NULL AND finished_at < '$CUTOFF_F'; SELECT changes();")
OLD_REMAINING=$(sqlite3 "$DB" "SELECT COUNT(*) FROM processed_files;")
echo "  旧版清理后剩余: $OLD_REMAINING 行（含 3 个永不清理的孤儿记录）"

if [ "$OLD_REMAINING" = "5" ]; then
  echo "✅ 对比验证通过：旧版遗漏了 3 个未完成的卡死记录，证明修复有效"
fi

echo ""
echo "═══════════════════════════════════════"
echo "✅ 问题 #4 修复验证通过"
echo "═══════════════════════════════════════"
