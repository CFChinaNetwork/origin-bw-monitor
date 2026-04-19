#!/bin/bash
# Bug #1 幂等性测试：验证同一文件重复处理不会导致数据翻倍
# 用法: bash tests/test-bug1-idempotency.sh

set -e
cd "$(dirname "$0")/.."

DB=/tmp/bw-stats-test-$$.db
trap "rm -f $DB" EXIT

echo "▶ Step 1: 建表（新 schema）"
sqlite3 "$DB" < schema.sql

echo "▶ Step 2: 模拟文件A第一次处理（贡献 100 bytes）"
sqlite3 "$DB" <<'EOF'
INSERT OR REPLACE INTO bw_stats (minute_utc, zone, r2_key, sum_bytes)
VALUES ('2026-04-19T10:00', 'example.com', 'logs/file-A.gz', 100);
EOF

echo "▶ Step 3: 模拟 Worker 崩溃后文件A被重试（再次贡献 100 bytes）"
sqlite3 "$DB" <<'EOF'
INSERT OR REPLACE INTO bw_stats (minute_utc, zone, r2_key, sum_bytes)
VALUES ('2026-04-19T10:00', 'example.com', 'logs/file-A.gz', 100);
EOF

echo "▶ Step 4: 模拟文件B独立贡献（同一分钟同一zone，200 bytes）"
sqlite3 "$DB" <<'EOF'
INSERT OR REPLACE INTO bw_stats (minute_utc, zone, r2_key, sum_bytes)
VALUES ('2026-04-19T10:00', 'example.com', 'logs/file-B.gz', 200);
EOF

echo "▶ Step 5: 验证 — 查询同一分钟同一zone的 SUM"
RESULT=$(sqlite3 "$DB" "SELECT SUM(sum_bytes) FROM bw_stats WHERE minute_utc='2026-04-19T10:00' AND zone='example.com';")

EXPECTED=300   # 文件A(100) + 文件B(200) = 300，重试不应翻倍

echo ""
echo "期望值: $EXPECTED bytes"
echo "实际值: $RESULT bytes"

if [ "$RESULT" = "$EXPECTED" ]; then
  echo "✅ PASS — 幂等性正确：重试不翻倍"
else
  echo "❌ FAIL — 幂等性破坏：实际 $RESULT，期望 $EXPECTED"
  echo ""
  echo "详细数据:"
  sqlite3 "$DB" "SELECT * FROM bw_stats;"
  exit 1
fi

echo ""
echo "▶ Step 6: 对比测试 — 模拟旧的累加 UPSERT 行为（应该翻倍）"
DB2=/tmp/bw-stats-old-$$.db
trap "rm -f $DB $DB2" EXIT

sqlite3 "$DB2" <<'EOF'
CREATE TABLE bw_stats_old (
    minute_utc TEXT, zone TEXT, sum_bytes INTEGER,
    PRIMARY KEY (minute_utc, zone)
);
EOF

sqlite3 "$DB2" <<'EOF'
-- 旧的累加 UPSERT 逻辑
INSERT INTO bw_stats_old VALUES ('2026-04-19T10:00', 'example.com', 100)
  ON CONFLICT(minute_utc, zone) DO UPDATE SET sum_bytes = sum_bytes + excluded.sum_bytes;
-- 重试
INSERT INTO bw_stats_old VALUES ('2026-04-19T10:00', 'example.com', 100)
  ON CONFLICT(minute_utc, zone) DO UPDATE SET sum_bytes = sum_bytes + excluded.sum_bytes;
-- 文件B
INSERT INTO bw_stats_old VALUES ('2026-04-19T10:00', 'example.com', 200)
  ON CONFLICT(minute_utc, zone) DO UPDATE SET sum_bytes = sum_bytes + excluded.sum_bytes;
EOF

OLD_RESULT=$(sqlite3 "$DB2" "SELECT sum_bytes FROM bw_stats_old WHERE minute_utc='2026-04-19T10:00' AND zone='example.com';")

echo "旧逻辑下的结果: $OLD_RESULT bytes（应该是 400，证明有翻倍bug）"

if [ "$OLD_RESULT" = "400" ]; then
  echo "✅ 对比验证通过 — 旧逻辑确实有翻倍问题，新修复有效"
else
  echo "⚠️  对比异常"
fi

echo ""
echo "═══════════════════════════════════════"
echo "✅ Bug #1 幂等性修复验证通过"
echo "═══════════════════════════════════════"
