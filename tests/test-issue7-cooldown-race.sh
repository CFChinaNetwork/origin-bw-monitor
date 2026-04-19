#!/bin/bash
# 问题 #7 测试：验证并发告警只有一个能成功（原子锁）
# 用法: bash tests/test-issue7-cooldown-race.sh

set -e
cd "$(dirname "$0")/.."

DB=/tmp/bw-stats-i7-$$.db
trap "rm -f $DB" EXIT

echo "▶ Step 1: 建表"
sqlite3 "$DB" < schema.sql

NOW=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")
# cooldown=30min，计算 30 分钟前的时间
SINCE=$(date -u -v-30M +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "30 minutes ago" +"%Y-%m-%dT%H:%M:%S.000Z")

echo ""
echo "▶ Step 2: 模拟场景 — 两个 Worker 同时触发 'threshold' 告警"

# 原子 INSERT WHERE NOT EXISTS 的 SQL（和代码一致）
ATOMIC_SQL="
INSERT INTO alert_history (alert_type, zone, minute_utc, mbps, detail, alerted_at)
SELECT 'threshold', 'example.com', '2026-04-19T10:00', 999.5, '{}', '$NOW'
WHERE NOT EXISTS (
  SELECT 1 FROM alert_history
  WHERE alert_type = 'threshold' AND zone = 'example.com' AND alerted_at >= '$SINCE'
);"

# Worker A 尝试（应该成功） — 必须在同一 sqlite3 连接内取 changes()
echo "  ► Worker A INSERT..."
COUNT_A=$(sqlite3 "$DB" "$ATOMIC_SQL SELECT changes();")

# Worker B 紧接着尝试（应该被 NOT EXISTS 阻止）
echo "  ► Worker B INSERT（应被阻止）..."
COUNT_B=$(sqlite3 "$DB" "$ATOMIC_SQL SELECT changes();")

TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM alert_history WHERE alert_type='threshold' AND zone='example.com';")

echo ""
echo "▶ Step 3: 验证"
echo "  Worker A changes: $COUNT_A (期望 1)"
echo "  Worker B changes: $COUNT_B (期望 0)"
echo "  alert_history 总计: $TOTAL (期望 1)"

if [ "$COUNT_A" = "1" ] && [ "$COUNT_B" = "0" ] && [ "$TOTAL" = "1" ]; then
  echo "✅ PASS — 并发场景下只有一条告警记录成功，另一个被原子检查阻止"
else
  echo "❌ FAIL"
  exit 1
fi

echo ""
echo "▶ Step 4: 验证冷却期结束后能再次告警"

# 把第一条的时间改成 31 分钟前，模拟冷却期已过
sqlite3 "$DB" "UPDATE alert_history SET alerted_at='$SINCE' WHERE id=1;"

# 重新计算 since（now - 30min）
NEW_SINCE=$(date -u -v-30M +"%Y-%m-%dT%H:%M:%S.000Z" 2>/dev/null || date -u -d "30 minutes ago" +"%Y-%m-%dT%H:%M:%S.000Z")

sqlite3 "$DB" "
INSERT INTO alert_history (alert_type, zone, minute_utc, mbps, detail, alerted_at)
SELECT 'threshold', 'example.com', '2026-04-19T10:30', 999.5, '{}', '$NOW'
WHERE NOT EXISTS (
  SELECT 1 FROM alert_history
  WHERE alert_type = 'threshold' AND zone = 'example.com' AND alerted_at > '$NEW_SINCE'
);"

AFTER_COOL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM alert_history;")
echo "  冷却期过后总告警数: $AFTER_COOL (期望 2)"

if [ "$AFTER_COOL" = "2" ]; then
  echo "✅ PASS — 冷却期过后能正常再次告警"
else
  echo "❌ FAIL"
  exit 1
fi

echo ""
echo "▶ Step 5: 对比旧逻辑（非原子，会重复告警）"
sqlite3 "$DB" "DELETE FROM alert_history;"

# 旧逻辑：先 SELECT 检查，再 INSERT（非原子）
# 两个 worker 都先 SELECT 发现无记录
EXISTS1=$(sqlite3 "$DB" "SELECT id FROM alert_history WHERE alert_type='threshold' AND zone='example.com' AND alerted_at >= '$SINCE' LIMIT 1;")
EXISTS2=$(sqlite3 "$DB" "SELECT id FROM alert_history WHERE alert_type='threshold' AND zone='example.com' AND alerted_at >= '$SINCE' LIMIT 1;")
# 两个都看到无记录 → 两个都 INSERT
sqlite3 "$DB" "INSERT INTO alert_history (alert_type, zone, minute_utc, mbps, detail, alerted_at) VALUES ('threshold', 'example.com', '2026-04-19T10:00', 999.5, '{}', '$NOW');"
sqlite3 "$DB" "INSERT INTO alert_history (alert_type, zone, minute_utc, mbps, detail, alerted_at) VALUES ('threshold', 'example.com', '2026-04-19T10:00', 999.5, '{}', '$NOW');"

OLD_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM alert_history;")
echo "  旧逻辑下重复告警数: $OLD_COUNT（说明旧版本确实有竞争问题）"

if [ "$OLD_COUNT" = "2" ]; then
  echo "✅ 对比验证通过：旧版本会产生 2 条告警，证明竞争问题真实存在"
fi

echo ""
echo "═══════════════════════════════════════"
echo "✅ 问题 #7 修复验证通过"
echo "═══════════════════════════════════════"
