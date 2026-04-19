// 测试帮助：读取 src/ 所有 .js 文件合并
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const srcDir = path.resolve(__dirname, '../src');
export function readAllSrc() {
  return fs.readdirSync(srcDir)
    .filter(f => f.endsWith('.js'))
    .map(f => fs.readFileSync(path.join(srcDir, f), 'utf-8'))
    .join('\n\n// ===== module boundary =====\n\n');
}
export function readSrc(filename) {
  return fs.readFileSync(path.resolve(srcDir, filename), 'utf-8');
}
