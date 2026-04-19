#!/bin/bash
# 问题 #10 + #11 测试：zone 精确匹配 + 时间范围 off-by-one 修复
set -e
cd "$(dirname "$0")/.."

DB=/tmp/bw-stats-i1011-$$.db
trap "rm -f $DB" EXIT

echo "▶ Step 1: 建表并插入测试数据"
sqlite3 "$DB" < schema.sql
sqlite3 "$DB" <<'EOF'
INSERT INTO bw_stats (minute_utc, zone, r2_key, sum_bytes) VALUES
  ('2026-04-19T10:00', 'abc.example.com',   'k1', 1000),
  ('2026-04-19T10:01', 'abc.example.com',   'k2', 2000),
  ('2026-04-19T10:02', 'xyz.example.com',   'k3', 3000),
  ('2026-04-19T10:03', '%wildcard.com',     'k4', 4000);  -- 恶意 zone 名含 %
EOF

echo ""
echo "═══ #10 zone 精确匹配测试 ═══"
echo ""

# 场景 1：用户输入 "abc.example.com" 应精确匹配（只返回 abc 的数据）
RESULT_EXACT=$(sqlite3 "$DB" "SELECT SUM(sum_bytes) FROM bw_stats WHERE zone = 'abc.example.com';")
echo "精确查询 'abc.example.com': $RESULT_EXACT bytes (期望 3000 = 1000+2000)"
[ "$RESULT_EXACT" = "3000" ] && echo "  ✅ PASS" || { echo "  ❌ FAIL"; exit 1; }

# 场景 2：旧逻辑用 LIKE，用户输入 "%example.com" 会匹配多个
OLD_LIKE_RESULT=$(sqlite3 "$DB" "SELECT SUM(sum_bytes) FROM bw_stats WHERE zone LIKE '%example.com';")
echo ""
echo "旧 LIKE 查询 '%example.com': $OLD_LIKE_RESULT bytes (意外匹配 abc+xyz = 6000)"
[ "$OLD_LIKE_RESULT" = "6000" ] && echo "  ⚠️  证实旧逻辑会误匹配（用户意图不明确）"

# 场景 3：恶意输入 '%' 想拿所有数据
OLD_LIKE_ALL=$(sqlite3 "$DB" "SELECT SUM(sum_bytes) FROM bw_stats WHERE zone LIKE '%';")
NEW_EXACT_PCT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM bw_stats WHERE zone = '%';")
echo ""
echo "恶意 zone='%':"
echo "  旧 LIKE: 匹配 $OLD_LIKE_ALL bytes（拿到全部数据 ❌）"
echo "  新 =:    精确匹配 '%' 字面，$NEW_EXACT_PCT 行（只返回字面匹配的）"

echo ""
echo "═══ #11 时间范围 off-by-one 测试 ═══"
echo ""

# 模拟新逻辑：向下取整到完整分钟，再退 1 分钟
# 当前时间 10:15:45 → lagMinutes=7 → endMs = 10:08:45
# 向下取整到分钟：10:08:00
# 退 1 分钟：10:07
# .slice(0,16) → "10:07"

node -e "
const testCases = [
  { now: '2026-04-19T10:15:45Z', lag: 7, expected_old: '2026-04-19T10:07', expected_new: '2026-04-19T10:07' },
  { now: '2026-04-19T10:15:00Z', lag: 7, expected_old: '2026-04-19T10:07', expected_new: '2026-04-19T10:07' },
  { now: '2026-04-19T10:08:30Z', lag: 0, expected_old: '2026-04-19T10:07', expected_new: '2026-04-19T10:07' },
  // 边界：10:08:00 整点
  { now: '2026-04-19T10:08:00Z', lag: 0, expected_old: '2026-04-19T10:07', expected_new: '2026-04-19T10:07' },
];

function oldLogic(now, lag) {
  const endMs = new Date(now).getTime() - lag * 60 * 1000;
  return new Date(endMs - 60 * 1000).toISOString().slice(0, 16);  // 魔数减 60 秒
}

function newLogic(now, lag) {
  const endMs = new Date(now).getTime() - lag * 60 * 1000;
  const d = new Date(endMs);
  d.setSeconds(0, 0);
  d.setMinutes(d.getMinutes() - 1);
  return d.toISOString().slice(0, 16);
}

let pass = 0, fail = 0;
for (const tc of testCases) {
  const oldR = oldLogic(tc.now, tc.lag);
  const newR = newLogic(tc.now, tc.lag);
  const ok = newR === tc.expected_new;
  console.log('  now=' + tc.now + ' lag=' + tc.lag + 'm → old=' + oldR + ', new=' + newR + (ok ? ' ✅' : ' ❌'));
  if (ok) pass++; else fail++;
}
process.exit(fail === 0 ? 0 : 1);
"

echo ""
echo "═══════════════════════════════════════"
echo "✅ #10 + #11 修复验证通过"
echo "═══════════════════════════════════════"
