'use strict';

/**
 * 处理模块：R2 日志文件 ingestion 主流程
 *
 * 核心函数：
 *   - processFile:       处理单个 R2 文件（幂等，流式只读）
 *   - applyFormula:      按官方公式处理单行日志
 *   - flushToD1:         批量写入 D1（真正幂等，#1 修复）
 *   - recordFailure:     失败状态追踪（#6 修复）
 *   - retryPendingFiles: Cron 重试（#2 修复：同时恢复 processing 卡死文件）
 */

import { getConfig } from './config.js';
import { log } from './utils.js';
import { checkAndAlertFromD1 } from './alerting.js';

// ═══════════════════════════════════════════════════════════════
// 处理单个 R2 文件（幂等，流式只读）
// ═══════════════════════════════════════════════════════════════
export async function processFile(key, env) {
  const c = getConfig(env);

  // 幂等保护 + 超时检测（同时查 status 和 started_at）
  const existing = await env.DB.prepare(
    `SELECT status, started_at FROM processed_files WHERE r2_key = ?`
  ).bind(key).first();
  if (existing?.status === 'done') {
    log(env, 'info', `Already done, skip: ${key}`); return;
  }

  // processing 状态：检查是否超时（超时则允许重新处理）
  if (existing?.status === 'processing') {
    const stuckAt   = existing.started_at || '';
    const threshold = new Date(Date.now() - c.maxProcessingMin * 60 * 1000).toISOString();
    if (stuckAt > threshold) {
      log(env, 'debug', `Still processing (not timed out): ${key}`); return;
    }
    log(env, 'warn', `Processing timed out, retrying: ${key}`);
  }

  await env.DB.prepare(`
    INSERT INTO processed_files (r2_key, status, started_at)
    VALUES (?, 'processing', ?)
    ON CONFLICT(r2_key) DO UPDATE SET
      status='processing', started_at=excluded.started_at, error_msg=NULL
  `).bind(key, new Date().toISOString()).run();

  log(env, 'info', `Processing: ${key}`);

  const safeCutoffMs = Date.now() - c.lagMinutes * 60 * 1000;

  // R2 Binding 只读取，gzip 流式解压
  const obj = await env.LOGPUSH_BUCKET.get(key);
  if (!obj) throw new Error(`R2 object not found: ${key}`);

  const minuteMap = new Map();
  let lineCount = 0, skippedLag = 0, skippedFilter = 0, errCount = 0;

  const stream  = obj.body.pipeThrough(new DecompressionStream('gzip'));
  const reader  = stream.getReader();
  const decoder = new TextDecoder('utf-8');
  let   buffer  = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        const last = buffer.trim();
        if (last) {
          const res = applyFormula(last, minuteMap, safeCutoffMs);
          if (res === 'ok') lineCount++;
          else if (res === 'lag') skippedLag++;
          else if (res === 'filter') skippedFilter++;
          else errCount++;
        }
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        const t = line.trim();
        if (!t) continue;
        const res = applyFormula(t, minuteMap, safeCutoffMs);
        if (res === 'ok') lineCount++;
        else if (res === 'lag') skippedLag++;
        else if (res === 'filter') skippedFilter++;
        else errCount++;
      }
    }
  } finally {
    reader.releaseLock();
  }

  // 有行被 lag 过滤 = 文件包含当前 lag 窗口内的数据，需要稍后重试补全
  // 重置为 pending 等 Cron 重试；此时不 flush，等下次完整重新处理
  // 幂等保证：下次重试时 INSERT OR REPLACE 会按 r2_key 覆盖，不会累加（#1 修复）
  if (skippedLag > 0) {
    await env.DB.prepare(
      `UPDATE processed_files SET status='pending', error_msg='has_lag_retry' WHERE r2_key=?`
    ).bind(key).run();
    log(env, 'info', `File has lag rows, reset pending for retry: ${key} lag=${skippedLag} ok=${lineCount}`);
    return;
  }

  await flushToD1(minuteMap, key, env);

  // 写入后：对本次写入的每个分钟查 D1 累计值做告警判断
  if (c.alertThresholdMbps !== null || c.alertSpikeMultiplier !== null) {
    const minutesWritten = new Set(
      [...minuteMap.keys()].map(k => k.slice(0, k.indexOf('\x00')))
    );
    checkAndAlertFromD1(minutesWritten, env).catch(err =>
      log(env, 'warn', `Alert check failed: ${err.message}`)
    );
  }

  // 记录文件处理范围（用于 status 展示）
  let fileMinuteMin = null, fileMinuteMax = null;
  for (const k of minuteMap.keys()) {
    const minute = k.slice(0, k.indexOf('\x00'));
    if (!fileMinuteMin || minute < fileMinuteMin) fileMinuteMin = minute;
    if (!fileMinuteMax || minute > fileMinuteMax) fileMinuteMax = minute;
  }

  await env.DB.prepare(`
    UPDATE processed_files SET
      status='done', finished_at=?, line_count=?,
      file_minute_min=?, file_minute_max=?
    WHERE r2_key=?
  `).bind(new Date().toISOString(), lineCount, fileMinuteMin, fileMinuteMax, key).run();

  log(env, 'info',
    `Done: ${key} | lines=${lineCount} lag=${skippedLag} filter=${skippedFilter} err=${errCount} minutes=${minuteMap.size}`
  );
}

// ═══════════════════════════════════════════════════════════════
// 官方公式：处理单行日志
// SUM(CacheResponseBytes) WHERE OriginResponseStatus NOT IN (0, 304)
// 时间分桶：EdgeEndTimestamp（与 CF 控制台口径一致）
// ═══════════════════════════════════════════════════════════════
function applyFormula(line, minuteMap, safeCutoffMs) {
  let r;
  try { r = JSON.parse(line); } catch { return 'error'; }

  const te = r.EdgeEndTimestamp;
  if (!te || te * 1000 > safeCutoffMs) return 'lag';

  const os = r.OriginResponseStatus;
  if (os === 0 || os === 304) return 'filter';

  const cb = r.CacheResponseBytes || 0;
  if (cb === 0) return 'filter';

  const d = new Date(te * 1000);
  d.setSeconds(0, 0);
  const minute = d.toISOString().slice(0, 16);
  const zone   = (r.ClientRequestHost || 'unknown').replace(/\x00/g, '');
  const mapKey = `${minute}\x00${zone}`;
  const entry  = minuteMap.get(mapKey) || { sum: 0 };
  entry.sum   += cb;
  minuteMap.set(mapKey, entry);
  return 'ok';
}

// ═══════════════════════════════════════════════════════════════
// 批量写入 D1（修复 #1：真正幂等的 INSERT OR REPLACE）
// 每个文件的贡献以 (minute_utc, zone, r2_key) 为主键独立存储
// 同一文件被重复处理（Queue 重试、Worker 崩溃重跑）时，写入结果始终一致
// 查询时用 SUM() 按 (minute_utc, zone) 聚合得到真实带宽
// ═══════════════════════════════════════════════════════════════
async function flushToD1(minuteMap, r2Key, env) {
  if (minuteMap.size === 0) return;
  const entries = [...minuteMap.entries()];
  const CHUNK   = 80;
  for (let i = 0; i < entries.length; i += CHUNK) {
    const stmts = entries.slice(i, i + CHUNK).map(([mapKey, v]) => {
      const idx    = mapKey.indexOf('\x00');
      const minute = mapKey.slice(0, idx);
      const zone   = mapKey.slice(idx + 1);
      return env.DB.prepare(`
        INSERT OR REPLACE INTO bw_stats (minute_utc, zone, r2_key, sum_bytes)
        VALUES (?, ?, ?, ?)
      `).bind(minute, zone, r2Key, v.sum);
    });
    await env.DB.batch(stmts);
  }
}

// ═══════════════════════════════════════════════════════════════
// 记录文件处理失败（修复 #6）
// 累加 retry_count，达到阈值后置 status='failed'
// ═══════════════════════════════════════════════════════════════
export async function recordFailure(r2Key, errorMsg, maxRetries, env) {
  // 截断超长错误信息，避免 D1 存储过大
  const truncated    = String(errorMsg || 'unknown').slice(0, 500);
  const initialState = (1 >= maxRetries) ? 'failed' : 'processing';
  await env.DB.prepare(`
    INSERT INTO processed_files (r2_key, status, started_at, retry_count, error_msg)
    VALUES (?, ?, ?, 1, ?)
    ON CONFLICT(r2_key) DO UPDATE SET
      retry_count = retry_count + 1,
      error_msg   = excluded.error_msg,
      status      = CASE WHEN retry_count + 1 >= ? THEN 'failed' ELSE status END
  `).bind(r2Key, initialState, new Date().toISOString(), truncated, maxRetries).run();
}

// ═══════════════════════════════════════════════════════════════
// Cron 每 8 分钟：重试需要恢复的文件
// 两类候选：
//   1. pending 状态 - 之前因 lag 窗口被推迟，现在 lag 窗口已过（原设计）
//   2. processing 状态且 started_at 超过 MAX_PROCESSING_MIN
//      = Worker 崩溃/超时后卡死的文件，不重试就永远丢失（#2 修复）
// ═══════════════════════════════════════════════════════════════
export async function retryPendingFiles(env) {
  const c = getConfig(env);

  const nowMs       = Date.now();
  const lagCutoff   = new Date(nowMs - c.lagMinutes * 60 * 1000).toISOString();
  const stuckCutoff = new Date(nowMs - c.maxProcessingMin * 60 * 1000).toISOString();

  const rows = (await env.DB.prepare(
    `SELECT r2_key, status FROM processed_files
     WHERE (status = 'pending'    AND started_at < ?)
        OR (status = 'processing' AND started_at < ?)
     ORDER BY started_at ASC LIMIT 20`
  ).bind(lagCutoff, stuckCutoff).all()).results;

  if (rows.length === 0) return;

  const pendingCount = rows.filter(r => r.status === 'pending').length;
  const stuckCount   = rows.filter(r => r.status === 'processing').length;
  log(env, 'info', `Cron retry: ${rows.length} files (pending=${pendingCount}, stuck-processing=${stuckCount})`);

  for (const { r2_key, status } of rows) {
    try {
      if (status === 'processing') {
        log(env, 'warn', `Recovering stuck processing file: ${r2_key}`);
      }
      await processFile(r2_key, env);
    } catch (err) {
      log(env, 'warn', `Retry failed for ${r2_key}: ${err.message}`);
    }
  }
}
