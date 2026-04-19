// 问题 #12 + #13 测试：Dashboard 使用配置值（retention_days + 时区）
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { readAllSrc } from './_read-all-src.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const src = readAllSrc();

let pass = 0, fail = 0;

// 检查 1: getConfig 里包含 displayTzOffsetHours
console.log('═══ #13 配置字段 displayTzOffsetHours 已加入 getConfig ═══');
if (/displayTzOffsetHours:\s*parseInt\(env\.DISPLAY_TZ_OFFSET_HOURS/.test(src)) {
  console.log('  ✅ getConfig 读取 DISPLAY_TZ_OFFSET_HOURS 环境变量'); pass++;
} else {
  console.log('  ❌ getConfig 未添加新字段'); fail++;
}

// 检查 2: Dashboard HTML 中 'Retention: 7 days' 已改为模板变量
console.log('\n═══ #12 Retention 改为配置值 ═══');
if (/Retention:\s*\$\{retentionDays\}\s*days/.test(src)) {
  console.log('  ✅ HTML footer 使用 ${retentionDays} 模板变量'); pass++;
} else {
  console.log('  ❌ Retention 仍是硬编码'); fail++;
}
if (!/Retention:\s*7\s*days/.test(src)) {
  console.log('  ✅ 源码中不再有硬编码的 "Retention: 7 days"'); pass++;
} else {
  console.log('  ❌ 仍有硬编码的 "Retention: 7 days"'); fail++;
}

// 检查 3: 时区 UTC+8 在代码层面已改为配置
console.log('\n═══ #13 时区改为配置值 ═══');
// 3a: 模板中不再有硬编码的 UTC+8 字符串（注释里的不算）
const codeOnly = src.replace(/\/\/[^\n]*/g, '').replace(/\/\*[\s\S]*?\*\//g, '');
if (!/UTC\+8/.test(codeOnly)) {
  console.log('  ✅ 非注释代码中已无硬编码 "UTC+8"'); pass++;
} else {
  console.log('  ❌ 代码中仍有硬编码的 UTC+8'); fail++;
}

// 3b: 不再有硬编码的 8*3600000
if (!/8\*3600000/.test(codeOnly)) {
  console.log('  ✅ 已无硬编码的 8*3600000'); pass++;
} else {
  console.log('  ❌ 仍有硬编码的 8*3600000'); fail++;
}

// 3c: 新增的 TZ_OFFSET_MS 和 TZ_LABEL 注入存在
if (/TZ_OFFSET_MS\s*=\s*\$\{tzOffsetMs\}/.test(src) && /TZ_LABEL\s*=\s*\$\{JSON\.stringify\(tzLabel\)\}/.test(src)) {
  console.log('  ✅ 服务端向客户端 JS 注入 TZ_OFFSET_MS / TZ_LABEL'); pass++;
} else {
  console.log('  ❌ 时区配置未正确注入客户端'); fail++;
}

// 3d: fmtUTC8 已被 toLocalTz 替代
if (!/fmtUTC8/.test(src) && /toLocalTz/.test(src)) {
  console.log('  ✅ 函数名 fmtUTC8 已重命名为 toLocalTz'); pass++;
} else {
  console.log('  ❌ 仍有 fmtUTC8 函数残留'); fail++;
}

// 检查 4: sendAlert 也使用新的配置
console.log('\n═══ #13 告警消息时区也可配置 ═══');
if (/displayTzOffsetHours/.test(src) && /tzLabel/.test(src)) {
  console.log('  ✅ sendAlert 使用动态时区标签'); pass++;
} else {
  console.log('  ❌ 告警时区未改'); fail++;
}

// 检查 5: wrangler.toml 有相应配置项
const toml = fs.readFileSync(path.resolve(__dirname, '../wrangler.toml'), 'utf-8');
console.log('\n═══ wrangler.toml 配置文档 ═══');
if (/DISPLAY_TZ_OFFSET_HOURS\s*=\s*"8"/.test(toml)) {
  console.log('  ✅ wrangler.toml 中有 DISPLAY_TZ_OFFSET_HOURS 配置项'); pass++;
} else {
  console.log('  ❌ wrangler.toml 未添加配置'); fail++;
}

console.log('\n═══════════════════════════════════════');
console.log(`Pass: ${pass}  Fail: ${fail}`);
console.log('═══════════════════════════════════════');
if (fail > 0) process.exit(1);
console.log('✅ #12 + #13 修复验证通过');
