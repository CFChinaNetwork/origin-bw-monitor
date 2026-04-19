#!/bin/bash
# 问题 #17 测试：分批删除大量过期数据
set -e
cd "$(dirname "$0")/.."

DB=/tmp/bw-stats-i17-$$.db
trap "rm -f $DB" EXIT

echo "▶ Step 1: 建表并插入 12000 条旧数据（模拟长期运行积累）"
sqlite3 "$DB" < schema.sql

# 批量插入 12000 条 10 天前的数据
python3 <<PYEOF > /tmp/bulk-insert-$$.sql
print("BEGIN;")
for i in range(12000):
    print(f"INSERT INTO bw_stats (minute_utc, zone, r2_key, sum_bytes) VALUES ('2026-04-01T{i//60:02d}:{i%60:02d}', 'zone{i%10}.example.com', 'old-{i}.gz', {i*100});")
print("COMMIT;")
PYEOF
sqlite3 "$DB" < /tmp/bulk-insert-$$.sql
rm /tmp/bulk-insert-$$.sql

BEFORE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM bw_stats;")
echo "  插入后总计: $BEFORE 行"

echo ""
echo "▶ Step 2: 模拟分批删除（LIMIT=5000）"
CUTOFF='2026-04-15T00:00'  # 删除所有 4月15日前的数据

ITER=0
TOTAL_DELETED=0
while true; do
  ITER=$((ITER+1))
  DELETED=$(sqlite3 "$DB" "DELETE FROM bw_stats WHERE rowid IN (SELECT rowid FROM bw_stats WHERE minute_utc < '$CUTOFF' LIMIT 5000); SELECT changes();")
  TOTAL_DELETED=$((TOTAL_DELETED+DELETED))
  echo "  第 $ITER 批: 删除 $DELETED 行（累计 $TOTAL_DELETED）"
  if [ "$DELETED" = "0" ]; then break; fi
  if [ "$ITER" -gt 10 ]; then echo "❌ 迭代过多，异常"; exit 1; fi
done

AFTER=$(sqlite3 "$DB" "SELECT COUNT(*) FROM bw_stats;")
echo ""
echo "▶ Step 3: 验证"
echo "  删除前: $BEFORE 行"
echo "  删除后: $AFTER 行"
echo "  总删除: $TOTAL_DELETED 行"
echo "  迭代次数: $ITER"

if [ "$TOTAL_DELETED" = "$BEFORE" ] && [ "$AFTER" = "0" ] && [ "$ITER" -ge 3 ]; then
  echo "✅ PASS — 分批正确删除所有旧数据（3 批以上，证明批量逻辑生效）"
else
  echo "❌ FAIL"
  exit 1
fi

echo ""
echo "▶ Step 4: 验证 rowid IN (SELECT ... LIMIT) 语法（SQLite 通用）"
# 单次测试
sqlite3 "$DB" <<'EOF'
INSERT INTO bw_stats VALUES ('2026-04-01T00:00', 'test', 'k1', 100), ('2026-04-01T00:01', 'test', 'k2', 200), ('2026-04-01T00:02', 'test', 'k3', 300);
EOF

# 单批删除 2 行（LIMIT 2）
DEL=$(sqlite3 "$DB" "DELETE FROM bw_stats WHERE rowid IN (SELECT rowid FROM bw_stats WHERE minute_utc < '2026-04-15T00:00' LIMIT 2); SELECT changes();")
REMAIN=$(sqlite3 "$DB" "SELECT COUNT(*) FROM bw_stats;")
echo "  单批 LIMIT=2: 删除 $DEL 行，剩余 $REMAIN 行（期望 删 2 剩 1）"

if [ "$DEL" = "2" ] && [ "$REMAIN" = "1" ]; then
  echo "✅ PASS — rowid IN (SELECT LIMIT) 语法在 SQLite 正常工作"
else
  echo "❌ FAIL"
  exit 1
fi

echo ""
echo "═══════════════════════════════════════"
echo "✅ 问题 #17 修复验证通过"
echo "═══════════════════════════════════════"
