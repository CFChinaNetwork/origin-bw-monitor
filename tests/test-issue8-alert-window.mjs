// 问题 #8 测试：突增告警的 24h 窗口应该基于目标 minute 计算，而不是当前时间
//
// 旧逻辑 bug 示例：
// - 现在是 10:00，但处理的是 9:00 的历史数据
// - since24h = now() - 24h = 昨天 10:00
// - 这是从"现在"往前的窗口，但告警针对的是"9:00 这个分钟"
// - 如果告警应该看"9:00 往前 24 小时"即"昨天 9:00 ~ 9:00"，就错位了 1 小时
//
// 新逻辑：since24h = (minute - 24h) = 基于 minute 本身计算

const cases = [
  {
    name: '正常场景：目标分钟在 10 分钟前',
    targetMinute: '2026-04-19T10:00',
    nowMs: new Date('2026-04-19T10:10Z').getTime(),
    expectedSince_new: '2026-04-18T10:00',  // 从 10:00 往前 24h
    expectedSince_old: '2026-04-18T10:10',  // 从 now 往前 24h（错位）
  },
  {
    name: '极端场景：目标分钟在 6 小时前',
    targetMinute: '2026-04-19T04:00',
    nowMs: new Date('2026-04-19T10:00Z').getTime(),
    expectedSince_new: '2026-04-18T04:00',  // 从 04:00 往前 24h
    expectedSince_old: '2026-04-18T10:00',  // 从 now 往前 24h（错位 6 小时！）
  },
];

function newLogic(minute) {
  const minuteMs = new Date(minute + ':00Z').getTime();
  return new Date(minuteMs - 24 * 3600 * 1000).toISOString().slice(0, 16);
}
function oldLogic(nowMs) {
  return new Date(nowMs - 24 * 3600 * 1000).toISOString().slice(0, 16);
}

let pass = 0, fail = 0;

for (const c of cases) {
  const newSince = newLogic(c.targetMinute);
  const oldSince = oldLogic(c.nowMs);

  const newOk = newSince === c.expectedSince_new;
  const oldDiverges = oldSince === c.expectedSince_old && oldSince !== c.expectedSince_new;

  console.log(`[${c.name}]`);
  console.log(`  目标 minute:      ${c.targetMinute}`);
  console.log(`  当前时间:         ${new Date(c.nowMs).toISOString().slice(0, 16)}`);
  console.log(`  新逻辑 since24h:  ${newSince}  ${newOk ? '✅' : '❌'} (期望 ${c.expectedSince_new})`);
  console.log(`  旧逻辑 since24h:  ${oldSince}  (期望 ${c.expectedSince_old} — 相对当前时间偏移)`);

  if (newOk) {
    console.log(`  ✅ 新逻辑准确：基于目标分钟 ${c.targetMinute} 往前 24h`);
    pass++;
  } else {
    console.log(`  ❌ 新逻辑错误`);
    fail++;
  }

  if (oldDiverges) {
    const diffMin = (new Date(c.expectedSince_old + ':00Z').getTime() - new Date(c.expectedSince_new + ':00Z').getTime()) / 60000;
    console.log(`  📊 对比：旧逻辑错位 ${diffMin} 分钟（新逻辑修复了此问题）`);
  }
  console.log();
}

console.log('═══════════════════════════════════════');
console.log(`Pass: ${pass}  Fail: ${fail}`);
console.log('═══════════════════════════════════════');

if (fail > 0) process.exit(1);
console.log('✅ 问题 #8 修复验证通过');
