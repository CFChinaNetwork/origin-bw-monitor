// 问题 #5 测试：buildDashboardUrl 能正确处理各种 dashUrl 格式
import { readAllSrc } from './_read-all-src.mjs';
const source = readAllSrc();

const match = source.match(/function buildDashboardUrl\(dashUrl, zone\) \{[\s\S]+?\n\}/);
if (!match) {
  console.error('❌ 找不到 buildDashboardUrl 函数');
  process.exit(1);
}
const buildDashboardUrl = new Function('return ' + match[0])();

const cases = [
  {
    name: '基础 URL（无参数）',
    input: ['https://monitor.example.com/', 'example.com'],
    expectedZone: 'example.com',
    expectedHours: '24',
    notContainDouble: true,
  },
  {
    name: '基础 URL（无尾部 /）',
    input: ['https://monitor.example.com', 'example.com'],
    expectedZone: 'example.com',
    expectedHours: '24',
    notContainDouble: true,
  },
  {
    name: 'URL 已含 ?token=xxx（关键 bug 场景）',
    input: ['https://monitor.example.com/?token=abc123', 'example.com'],
    expectedZone: 'example.com',
    expectedHours: '24',
    mustContain: ['token=abc123'],  // 原参数必须保留
    notContainDouble: true,
  },
  {
    name: 'zone 含特殊字符（编码测试）',
    input: ['https://monitor.example.com/', 'foo&bar.example.com'],
    expectedZone: 'foo&bar.example.com',  // URL.searchParams 会自动 encode
    expectedHours: '24',
    notContainDouble: true,
  },
  {
    name: 'URL 已含 hours 参数（覆盖测试）',
    input: ['https://monitor.example.com/?hours=1', 'example.com'],
    expectedZone: 'example.com',
    expectedHours: '24',  // 应该被覆盖为 24
    notContainDouble: true,
  },
  {
    name: '非法 URL（容错）',
    input: ['not-a-valid-url', 'example.com'],
    expectedRaw: 'not-a-valid-url',  // 回退到原值
  },
];

let pass = 0, fail = 0;

for (const c of cases) {
  const result = buildDashboardUrl(...c.input);
  const errors = [];

  if (c.expectedRaw !== undefined) {
    if (result !== c.expectedRaw) errors.push(`应回退到原值 ${c.expectedRaw}, 实际 ${result}`);
  } else {
    let u;
    try { u = new URL(result); }
    catch { errors.push(`结果不是合法 URL: ${result}`); }

    if (u) {
      const zone = u.searchParams.get('zone');
      const hours = u.searchParams.get('hours');
      if (c.expectedZone && zone !== c.expectedZone) errors.push(`zone 期望 ${c.expectedZone}, 实际 ${zone}`);
      if (c.expectedHours && hours !== c.expectedHours) errors.push(`hours 期望 ${c.expectedHours}, 实际 ${hours}`);
      for (const m of (c.mustContain || [])) {
        if (!result.includes(m)) errors.push(`应包含 "${m}"，实际不包含`);
      }
      if (c.notContainDouble && (result.match(/\?/g) || []).length > 1) {
        errors.push(`出现多个 ? (URL 拼接 bug)`);
      }
    }
  }

  if (errors.length === 0) {
    console.log(`✅ [${c.name}]`);
    console.log(`   输入: buildDashboardUrl(${JSON.stringify(c.input[0])}, ${JSON.stringify(c.input[1])})`);
    console.log(`   输出: ${result}\n`);
    pass++;
  } else {
    console.log(`❌ [${c.name}]`);
    console.log(`   输出: ${result}`);
    errors.forEach(e => console.log(`   - ${e}`));
    console.log();
    fail++;
  }
}

console.log('═══════════════════════════════════════');
console.log(`Pass: ${pass}  Fail: ${fail}`);
console.log('═══════════════════════════════════════');
if (fail > 0) process.exit(1);
console.log('✅ 问题 #5 修复验证通过');
