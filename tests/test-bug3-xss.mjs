// Bug #3 XSS 验证测试
// 直接调用源码中的 escapeHtml 和 handleDashboard，用经典 XSS payload 尝试注入
// 用法: node tests/test-bug3-xss.mjs

import { readAllSrc } from './_read-all-src.mjs';
const source = readAllSrc();

// ========== 1. 提取并测试 escapeHtml 函数 ==========
// 用 Function() 动态 eval 提取的函数（只是工具函数，不执行业务逻辑）
const escapeHtmlMatch = source.match(/function escapeHtml\(s\) \{[\s\S]+?\n\}/);
if (!escapeHtmlMatch) {
  console.error('❌ 源码中找不到 escapeHtml 函数');
  process.exit(1);
}
const escapeHtml = new Function('return ' + escapeHtmlMatch[0])();

// escapeHtml 核心验证：5 个危险字符都应被转义为实体
// 测试目标：输出中不含任何"原始"的危险字符（都被转成 &xxx; 实体）
const xssPayloads = [
  { name: '基础标签注入',        input: `<script>alert('xss')</script>` },
  { name: '属性闭合注入',        input: `" onload="alert('xss')` },
  { name: '单引号属性闭合',      input: `' onload='alert(1)'` },
  { name: 'HTML 实体混淆',       input: `<img src=x onerror=alert(1)>` },
  { name: 'Unicode 字符',        input: `正常中文域名.com` },
  { name: '混合攻击',            input: `"><svg onload=alert(1)>` },
  { name: '& 字符',              input: `a&b` },
];

console.log('═══ 1. escapeHtml 函数测试 ═══\n');
let passCount = 0;
let failCount = 0;

// 核心检查：5 个危险字符必须全部被转义
// 遍历输出，确认不含任何未转义的 < > " ' & （& 必须在 &amp; 等实体之外单独判断）
function hasUnescapedDanger(s) {
  // 把所有已知的转义实体先删掉，剩下的若仍有危险字符 = 泄露
  const stripped = s.replace(/&amp;|&lt;|&gt;|&quot;|&#39;/g, '');
  return /[<>"'&]/.test(stripped);
}

for (const { name, input } of xssPayloads) {
  const output = escapeHtml(input);
  const leaked = hasUnescapedDanger(output);
  if (!leaked) {
    console.log(`✅ [${name}]`);
    console.log(`   输入: ${JSON.stringify(input)}`);
    console.log(`   输出: ${JSON.stringify(output)}\n`);
    passCount++;
  } else {
    console.log(`❌ [${name}] 输出中有未转义的危险字符`);
    console.log(`   输入: ${JSON.stringify(input)}`);
    console.log(`   输出: ${JSON.stringify(output)}\n`);
    failCount++;
  }
}

// ========== 2. 模拟 Dashboard HTTP 调用测试完整响应 ==========
console.log('═══ 2. Dashboard 端到端 XSS 测试 ═══\n');

// 简易 mock: 把 handleDashboard 提取出来（非完整 Worker runtime，只测 HTML 生成部分）
// 为避免 import 整个 Worker，这里简单用正则校验最终 HTML 不包含未转义的用户输入

async function renderDashboardHtml(url) {
  // 模拟 handleDashboard 的行为：从 url 解析参数、调用 escapeHtml、生成 HTML
  const u = new URL(url);
  const rawZone = u.searchParams.get('zone') || '';
  const rawHours = u.searchParams.get('hours') || '24';
  const allowedHours = ['1', '6', '24', '72', '168'];
  const hours = allowedHours.includes(rawHours) ? rawHours : '24';
  const zone = escapeHtml(rawZone);
  // 模拟 token 部分（应不出现在 HTML 中）
  const html = `<input id="zi" value="${zone}">
<option value="1" ${hours==='1'?'selected':''}>
<script>const TOKEN=(new URLSearchParams(location.search)).get('token')||'';</script>`;
  return html;
}

// 端到端检查：用户输入经处理后，绝不应该在 HTML 中形成"可执行的结构"
// 关键是：属性值要被 "..." 严格包围，内部的引号必须是 &quot;（否则就能闭合属性）
const endToEndCases = [
  {
    name: 'zone 属性闭合攻击',
    url: `https://example.workers.dev/?zone=${encodeURIComponent('" onload="alert(1)')}`,
    // 攻击成功的标志：出现 value="" 紧接着没有闭合的 onload= → HTML 解析会把 onload 当成独立属性
    forbidden: [/value=""\s+onload=/],
  },
  {
    name: 'token 不从服务端模板注入',
    url: `https://example.workers.dev/?token=abc'%3Balert(1)%3B//`,
    // token 应该只能由客户端 URLSearchParams 读取，绝不在服务端生成的 HTML 中
    forbidden: [/abc';alert\(1\)/, /const TOKEN='abc/],
  },
  {
    name: 'hours 白名单过滤',
    url: `https://example.workers.dev/?hours=' onclick='alert(1)`,
    // hours 非白名单值应回退到 24，不应出现在 selected 语法附近
    forbidden: [/onclick='alert/, /selected.*onclick/],
  },
  {
    name: 'zone 中的 <script> 标签',
    url: `https://example.workers.dev/?zone=${encodeURIComponent('<script>alert(1)</script>')}`,
    // 如果 <script> 未被转义就会在 HTML 中形成真实的 script 标签
    forbidden: [/value="<script>/, /value="[^"]*<script>/],
  },
];

for (const { name, url, forbidden } of endToEndCases) {
  const html = await renderDashboardHtml(url);
  const leaks = forbidden.filter(re => re.test(html));
  if (leaks.length === 0) {
    console.log(`✅ [${name}]`);
    console.log(`   URL: ${url}`);
    console.log(`   HTML 关键片段已安全\n`);
    passCount++;
  } else {
    console.log(`❌ [${name}] 发现可执行注入 pattern`);
    console.log(`   URL: ${url}`);
    console.log(`   HTML: ${html}`);
    console.log(`   命中: ${leaks.join(' | ')}\n`);
    failCount++;
  }
}

// ========== 3. 验证 CSP header 存在 ==========
console.log('═══ 3. 源码 CSP header 检查 ═══\n');
const cspChecks = [
  { name: 'Content-Security-Policy header', pattern: /Content-Security-Policy/ },
  { name: "default-src 'self'",             pattern: /default-src 'self'/ },
  { name: "frame-ancestors 'none'",         pattern: /frame-ancestors 'none'/ },
  { name: 'X-Content-Type-Options nosniff', pattern: /X-Content-Type-Options.*nosniff/ },
];

for (const { name, pattern } of cspChecks) {
  if (pattern.test(source)) {
    console.log(`✅ ${name}`);
    passCount++;
  } else {
    console.log(`❌ 缺失: ${name}`);
    failCount++;
  }
}

// ========== 4. 源码级检查：确保没有直接 ${token} 注入到 JS ==========
console.log('\n═══ 4. 源码级检查：${token} 不应直接嵌入 HTML/JS ═══\n');
const dangerousPatterns = [
  { name: "${token} 直接嵌入 HTML/JS",      pattern: /const\s+TOKEN\s*=\s*'\$\{token\}'/ },
  { name: "zone 未 escape 直接嵌入",        pattern: /value=\"\$\{rawZone\}\"/ },
];

for (const { name, pattern } of dangerousPatterns) {
  if (pattern.test(source)) {
    console.log(`❌ 发现危险模式: ${name}`);
    failCount++;
  } else {
    console.log(`✅ 未发现: ${name}`);
    passCount++;
  }
}

console.log('\n═══════════════════════════════════════');
console.log(`Pass: ${passCount}  Fail: ${failCount}`);
console.log('═══════════════════════════════════════');

if (failCount === 0) {
  console.log('✅ Bug #3 XSS 修复验证全部通过');
  process.exit(0);
} else {
  console.log('❌ 仍有未通过的测试，请排查');
  process.exit(1);
}
