// 多模块重构后的静态验证
// 1. 每个模块的 import/export 对应
// 2. 循环依赖检测
// 3. 所有引用的函数都有对应的 export

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const srcDir = path.resolve(__dirname, '../src');

const MODULES = ['index', 'config', 'utils', 'processor', 'alerting', 'cleanup', 'dashboard'];

let pass = 0, fail = 0;
const issues = [];

// 解析每个模块的 imports 和 exports
const moduleInfo = {};
for (const name of MODULES) {
  const src = fs.readFileSync(path.join(srcDir, `${name}.js`), 'utf-8');
  const imports = [];
  const exports = [];

  // 匹配 import { a, b, c } from './xxx.js'
  for (const m of src.matchAll(/import\s*\{\s*([^}]+)\s*\}\s*from\s*['"]\.\/([\w-]+)\.js['"]/g)) {
    const names = m[1].split(',').map(s => s.trim()).filter(Boolean);
    imports.push({ from: m[2], names });
  }
  // 匹配 export function / export async function / export const / export let
  for (const m of src.matchAll(/export\s+(?:async\s+)?function\s+(\w+)/g)) exports.push(m[1]);
  for (const m of src.matchAll(/export\s+(?:const|let|var)\s+(\w+)/g)) exports.push(m[1]);

  moduleInfo[name] = { imports, exports, src };
}

console.log('═══ 1. 模块间 import/export 一致性检查 ═══\n');
for (const [name, info] of Object.entries(moduleInfo)) {
  for (const imp of info.imports) {
    const target = moduleInfo[imp.from];
    if (!target) {
      console.log(`❌ ${name}.js imports from './${imp.from}.js' 但该模块不存在`);
      fail++;
      continue;
    }
    for (const sym of imp.names) {
      if (!target.exports.includes(sym)) {
        console.log(`❌ ${name}.js 导入了 '${sym}'，但 ${imp.from}.js 没有导出它`);
        issues.push(`${name}.js imports ${sym} from ${imp.from}.js (not exported)`);
        fail++;
      }
    }
  }
}
if (fail === 0) {
  console.log('✅ 所有 import 都能在对应模块找到 export\n');
  pass++;
}

console.log('═══ 2. 循环依赖检测 ═══\n');
const graph = {};
for (const [name, info] of Object.entries(moduleInfo)) {
  graph[name] = info.imports.map(i => i.from);
}
function hasCycle(start) {
  const visited = new Set();
  const stack = [];
  function dfs(node) {
    if (stack.includes(node)) return [...stack, node];
    if (visited.has(node)) return null;
    visited.add(node);
    stack.push(node);
    for (const next of graph[node] || []) {
      const cycle = dfs(next);
      if (cycle) return cycle;
    }
    stack.pop();
    return null;
  }
  return dfs(start);
}
let cycleFound = false;
for (const m of MODULES) {
  const cycle = hasCycle(m);
  if (cycle) {
    console.log(`❌ 循环依赖: ${cycle.join(' → ')}`);
    cycleFound = true;
  }
}
if (!cycleFound) {
  console.log('✅ 无循环依赖\n');
  pass++;
}

console.log('═══ 3. 每个模块导出汇总 ═══\n');
for (const [name, info] of Object.entries(moduleInfo)) {
  if (info.exports.length > 0) {
    console.log(`  ${name}.js → [${info.exports.join(', ')}]`);
  } else {
    console.log(`  ${name}.js → (no exports, 可能是入口)`);
  }
}

console.log('\n═══ 4. 依赖图 ═══\n');
for (const [name, info] of Object.entries(moduleInfo)) {
  const deps = info.imports.map(i => i.from);
  console.log(`  ${name}.js → [${deps.join(', ') || '(独立)'}]`);
}

console.log('\n═══ 5. 入口文件 export default 检查 ═══\n');
if (/export\s+default\s*\{/.test(moduleInfo.index.src)) {
  console.log('✅ index.js 有 export default（Worker 入口）');
  pass++;
} else {
  console.log('❌ index.js 缺少 export default');
  fail++;
}

// 检查 export default 对象里有 fetch/queue/scheduled 三个必要 handler
const reqHandlers = ['fetch', 'queue', 'scheduled'];
const found = reqHandlers.filter(h => new RegExp(`async\\s+${h}\\s*\\(`).test(moduleInfo.index.src));
if (found.length === 3) {
  console.log('✅ Worker handlers 全部存在：fetch / queue / scheduled');
  pass++;
} else {
  console.log(`❌ 缺少 handler: ${reqHandlers.filter(h => !found.includes(h))}`);
  fail++;
}

console.log('\n═══════════════════════════════════════');
console.log(`Pass: ${pass}  Fail: ${fail}`);
console.log('═══════════════════════════════════════');
if (fail > 0) process.exit(1);
console.log('✅ 多模块重构结构验证全部通过');
