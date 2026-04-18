'use strict';

/**
 * CF Origin Pull Bandwidth Monitor
 *
 * 从 Logpush R2 日志中计算回源带宽，生成分钟级时序图表。
 * 部署在客户自己的 Cloudflare 账号下，直接通过 R2 Binding 访问。
 *
 * 架构：
 *   Logpush → R2（已有）
 *     ↓ R2 Event Notification（自动触发）
 *   bw-ingest-queue（仅传 R2 key，不传内容）
 *     ↓
 *   Queue Consumer（流式解析 → 官方公式聚合 → 写入 D1）
 *     ↓
 *   D1（分钟级带宽聚合，7天保留）
 *     ↓
 *   Dashboard（HTTP 查询 → Chart.js 图表）
 *
 * 回源带宽公式（官方）：
 *   SUM(CacheResponseBytes) WHERE OriginResponseStatus NOT IN (0, 304)
 *   Mbps = sum_bytes / 60s * 8 / 1024²
 *   参考：https://developers.cloudflare.com/logs/faq/common-calculations/
 *
 * 字段说明：
 *   - OS=0：HIT，CF 从缓存返回，未联系源站，排除
 *   - OS=304：revalidated，源站确认未变更，无 body 传输，排除
 *   - 其余 OS：源站有实际响应（含 body），计入带宽
 *   - 时间分桶：EdgeEndTimestamp（请求结束时间），与 CF 控制台口径一致
 *
 * 注意：所有 R2 操作均为只读（LIST / GET），绝不写入客户数据。
 *       配置项全部来自 wrangler.toml [vars] 或 Secrets，
 *       index.js 本身不含任何账号或密钥信息。
 */

// ─── 常量 ─────────────────────────────────────────────────────
const LOG_LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };

// ─── 读取配置（全部来自 wrangler.toml [vars]，不硬编码）───────
function getConfig(env) {
  return {
    prefix:           env.LOG_PREFIX            || 'logs/',
    startTime:        env.START_TIME            || '1970-01-01T00:00:00Z',
    lagMinutes:       parseInt(env.LAG_MINUTES  || '10', 10),
    scanBatchSize:    parseInt(env.SCAN_BATCH_SIZE      || '20', 10),
    maxProcessingMin: parseInt(env.MAX_PROCESSING_MIN   || '15', 10),
    retentionDays:    parseInt(env.DATA_RETENTION_DAYS  || '7',  10),
    dashToken:        env.DASHBOARD_TOKEN || '',
    logLevel:         env.LOG_LEVEL       || 'info',
    // 告警配置（留空 = 不启用）
    alertThresholdMbps:   env.ALERT_THRESHOLD_MBPS   ? parseFloat(env.ALERT_THRESHOLD_MBPS)   : null,
    alertSpikeMultiplier: env.ALERT_SPIKE_MULTIPLIER  ? parseFloat(env.ALERT_SPIKE_MULTIPLIER) : null,
    alertCooldownMin:     parseInt(env.ALERT_COOLDOWN_MIN || '30', 10),
    alertDashboardUrl:    env.ALERT_DASHBOARD_URL || '',
  };
}

// ─── 主入口 ───────────────────────────────────────────────────
export default {

  // ── Cron 调度 ────────────────────────────────────────────────
  // cron 1：*/10 * * * *  → 扫描 R2 新文件并入队（作为 Cron 触发的备用方式）
  //         正常情况下由 R2 Event Notification 实时触发，此 Cron 仅作兜底
  // cron 2：0 2 * * *     → 每天 UTC 02:00 清理 D1 过期数据
  async scheduled(event, env, ctx) {
    if (event.cron === '0 2 * * *') {
      ctx.waitUntil(cleanupOldData(env));
    } else {
      ctx.waitUntil(scanAndEnqueue(env));
    }
  },

  // ── Queue Consumer：串行解析文件，写入 D1 ──────────────────
  // 每批最多3个文件，串行处理，避免内存压力
  async queue(batch, env, ctx) {
    for (const msg of batch.messages) {
      const key = msg.body?.key;
      if (!key) { msg.ack(); continue; }
      try {
        await processFile(key, env);
        msg.ack();
      } catch (err) {
        log(env, 'error', `processFile failed key=${key}: ${err.message}`);
        await markFileFailed(key, err.message, env).catch(() => {});
        msg.retry();
      }
    }
  },

  // ── HTTP：Dashboard 图表 + 管理接口 ────────────────────────
  async fetch(request, env) {
    if (!isAuthorized(request, env)) {
      return new Response('Unauthorized', { status: 401 });
    }
    const url  = new URL(request.url);
    const path = url.pathname;
    if (path === '/api/stats')  return handleApiStats(request, env);
    if (path === '/api/import') return handleApiImport(request, env);
    if (path === '/api/status') return handleApiStatus(request, env);
    if (path === '/api/retry')  return handleApiRetry(request, env);
    return handleDashboard(request, env);
  },
};

// ═══════════════════════════════════════════════════════════════
// Cron 兜底扫描：列出新文件并入队
// 正常流程由 R2 Event Notification 触发，此 Cron 仅作兜底保障
// 支持分页，确保不遗漏文件
// ═══════════════════════════════════════════════════════════════
async function scanAndEnqueue(env) {
  const c = getConfig(env);
  const safeCutoff     = new Date(Date.now() - c.lagMinutes * 60 * 1000).toISOString();
  const stuckThreshold = new Date(Date.now() - c.maxProcessingMin * 60 * 1000).toISOString();

  log(env, 'info', `Cron scan: prefix=${c.prefix} startTime=${c.startTime} safeCutoff=${safeCutoff}`);

  const toEnqueue = [];
  let cursor = undefined;
  let pageCount = 0;

  // 分页扫描，直到没有更多文件或达到批次上限
  while (true) {
    let listed;
    try {
      listed = await env.LOGPUSH_BUCKET.list({
        prefix: c.prefix,
        limit:  1000,             // R2 list 单页上限
        cursor,
      });
    } catch (err) {
      log(env, 'error', `R2 list failed: ${err.message}`);
      break;
    }
    pageCount++;

    // 批量查询 D1（减少串行 SELECT，一次查完这一页所有 key 的状态）
    const keys = listed.objects.map(o => o.key);
    const placeholders = keys.map(() => '?').join(',');
    const existingRows = keys.length > 0
      ? (await env.DB.prepare(
          `SELECT r2_key, status, started_at FROM processed_files WHERE r2_key IN (${placeholders})`
        ).bind(...keys).all()).results
      : [];
    const existingMap = new Map(existingRows.map(r => [r.r2_key, r]));

    for (const obj of listed.objects) {
      const lm = obj.uploaded?.toISOString() || '';
      if (lm < c.startTime)  continue;
      if (lm > safeCutoff) { log(env, 'debug', `Skip (too recent): ${obj.key}`); continue; }

      const existing = existingMap.get(obj.key);
      if (existing?.status === 'done') continue;

      if (existing?.status === 'processing') {
        if ((existing.started_at || '') > stuckThreshold) {
          log(env, 'debug', `Skip (still processing): ${obj.key}`); continue;
        }
        log(env, 'warn', `Resetting stuck file: ${obj.key}`);
        await env.DB.prepare(
          `UPDATE processed_files SET status='pending', error_msg='reset_stuck' WHERE r2_key=?`
        ).bind(obj.key).run();
      }
      toEnqueue.push(obj.key);

      // 达到批次上限就先入队，避免 Cron CPU 超时
      if (toEnqueue.length >= c.scanBatchSize) break;
    }

    if (toEnqueue.length >= c.scanBatchSize || !listed.truncated) break;
    cursor = listed.cursor;
  }

  if (toEnqueue.length === 0) { log(env, 'info', `Cron: no new files (scanned ${pageCount} pages)`); return; }

  await env.INGEST_QUEUE.sendBatch(toEnqueue.map(key => ({ body: { key } })));
  log(env, 'info', `Cron: enqueued ${toEnqueue.length} files (scanned ${pageCount} pages)`);
}

// ═══════════════════════════════════════════════════════════════
// 处理单个文件（幂等，流式，只读）
// ═══════════════════════════════════════════════════════════════
async function processFile(key, env) {
  const c = getConfig(env);

  // 幂等保护：已成功处理的文件绝不重复处理
  const existing = await env.DB.prepare(
    `SELECT status FROM processed_files WHERE r2_key = ?`
  ).bind(key).first();
  if (existing?.status === 'done') {
    log(env, 'info', `Already done, skip: ${key}`); return;
  }

  // 标记为 processing
  await env.DB.prepare(`
    INSERT INTO processed_files (r2_key, status, started_at)
    VALUES (?, 'processing', ?)
    ON CONFLICT(r2_key) DO UPDATE SET
      status='processing', started_at=excluded.started_at, error_msg=NULL
  `).bind(key, new Date().toISOString()).run();

  log(env, 'info', `Processing: ${key}`);

  const safeCutoffMs = Date.now() - c.lagMinutes * 60 * 1000;

  // ★ 通过 R2 Binding 读取文件（只读，绝不写入）
  // R2 Binding 返回的是原始 gzip 流，需要解压
  const obj = await env.LOGPUSH_BUCKET.get(key);
  if (!obj) throw new Error(`R2 object not found: ${key}`);

  // ★ 流式解压 + 逐行处理（不把整个文件装入内存）
  // Logpush 文件是 gzip 压缩的 NDJSON，通过 DecompressionStream 流式解压
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

  // 统计文件涉及的分钟范围（用于 status 展示）
  let fileMinuteMin = null, fileMinuteMax = null;
  for (const k of minuteMap.keys()) {
    const minute = k.split('|')[0];
    if (!fileMinuteMin || minute < fileMinuteMin) fileMinuteMin = minute;
    if (!fileMinuteMax || minute > fileMinuteMax) fileMinuteMax = minute;
  }

  // 写入 D1（累加 upsert，支持多文件写同一分钟）
  await flushToD1(minuteMap, env);

  // 写入后立即检测告警（异步，不阻塞主流程）
  if (c.alertThresholdMbps !== null || c.alertSpikeMultiplier !== null) {
    checkAndAlert(minuteMap, env).catch(err =>
      log(env, 'warn', `Alert check failed: ${err.message}`)
    );
  }

  // 标记为 done
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
// 参考：https://developers.cloudflare.com/logs/faq/common-calculations/
//
// SUM(CacheResponseBytes) WHERE OriginResponseStatus NOT IN (0, 304)
//   OS=0：HIT，未回源，排除
//   OS=304：revalidated，无 body 传输，排除
//   其余：源站有实际响应，计入带宽（与官方口径一致）
//
// 时间分桶：EdgeEndTimestamp（请求结束时间），与 CF 控制台口径一致
// ═══════════════════════════════════════════════════════════════
function applyFormula(line, minuteMap, safeCutoffMs) {
  let r;
  try { r = JSON.parse(line); } catch { return 'error'; }

  // Safe Lag Window：用 EdgeEndTimestamp 判断（与分桶字段一致）
  const te = r.EdgeEndTimestamp;
  if (!te || te * 1000 > safeCutoffMs) return 'lag';

  // 官方公式过滤
  const os = r.OriginResponseStatus;
  if (os === 0 || os === 304) return 'filter';

  const cb = r.CacheResponseBytes || 0;
  if (cb === 0) return 'filter';

  // 按 EdgeEndTimestamp 分桶（UTC 自然分钟）
  const d = new Date(te * 1000);
  d.setSeconds(0, 0);
  const minute = d.toISOString().slice(0, 16);       // "2026-04-14T03:01"
  // 用 \x00 作为分隔符（域名不含此字符，防止含特殊字符的域名导致 split 错误）
  const zone   = (r.ClientRequestHost || 'unknown').replace(/\x00/g, '');
  const mapKey = `${minute}\x00${zone}`;
  const entry  = minuteMap.get(mapKey) || { sum: 0 };
  entry.sum   += cb;
  minuteMap.set(mapKey, entry);
  return 'ok';
}

// ═══════════════════════════════════════════════════════════════
// 批量写入 D1（累加 upsert，幂等，支持多文件写同一分钟）
// ═══════════════════════════════════════════════════════════════
async function flushToD1(minuteMap, env) {
  if (minuteMap.size === 0) return;
  const entries = [...minuteMap.entries()];
  const CHUNK   = 80;
  for (let i = 0; i < entries.length; i += CHUNK) {
    const stmts = entries.slice(i, i + CHUNK).map(([mapKey, v]) => {
      const idx    = mapKey.indexOf('\x00');
      const minute = mapKey.slice(0, idx);
      const zone   = mapKey.slice(idx + 1);
      return env.DB.prepare(`
        INSERT INTO bw_stats (minute_utc, zone, sum_bytes)
        VALUES (?, ?, ?)
        ON CONFLICT(minute_utc, zone) DO UPDATE SET
          sum_bytes = sum_bytes + excluded.sum_bytes
      `).bind(minute, zone, v.sum);
    });
    await env.DB.batch(stmts);
  }
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：手动批量导入历史文件
// POST /api/import  Body: { from_time?, prefix?, cursor?, page_size? }
// ═══════════════════════════════════════════════════════════════
async function handleApiImport(request, env) {
  if (request.method !== 'POST') return jsonResponse({ error: 'POST only' }, 405);

  const c    = getConfig(env);
  const body = await request.json().catch(() => ({}));
  const fromTime  = body.from_time || c.startTime;
  const prefix    = body.prefix    || c.prefix;
  const pageSize  = Math.min(parseInt(body.page_size || '50', 10), 1000);
  const safeCutoff = new Date(Date.now() - c.lagMinutes * 60 * 1000).toISOString();

  // 通过 R2 Binding 列出文件（只读）
  let listed;
  try {
    listed = await env.LOGPUSH_BUCKET.list({
      prefix,
      limit:  pageSize,
      cursor: body.cursor || undefined,
    });
  } catch (err) {
    return jsonResponse({ error: `R2 list failed: ${err.message}` }, 500);
  }

  const toEnqueue = [];
  let filteredByTime = 0, filteredByLag = 0, filteredDone = 0;

  // 批量查询 D1，避免串行 N 次 SELECT
  const candidateKeys = listed.objects
    .filter(o => {
      const lm = o.uploaded?.toISOString() || '';
      if (lm < fromTime)   { filteredByTime++; return false; }
      if (lm > safeCutoff) { filteredByLag++;  return false; }
      return true;
    })
    .map(o => o.key);

  const existingMap = new Map();
  if (candidateKeys.length > 0) {
    const ph   = candidateKeys.map(() => '?').join(',');
    const rows = (await env.DB.prepare(
      `SELECT r2_key, status FROM processed_files WHERE r2_key IN (${ph})`
    ).bind(...candidateKeys).all()).results;
    rows.forEach(r => existingMap.set(r.r2_key, r.status));
  }

  for (const key of candidateKeys) {
    if (existingMap.get(key) === 'done') { filteredDone++; continue; }
    toEnqueue.push(key);
  }

  let queued = 0;
  for (let i = 0; i < toEnqueue.length; i += 100) {
    await env.INGEST_QUEUE.sendBatch(
      toEnqueue.slice(i, i + 100).map(key => ({ body: { key } }))
    );
    queued += Math.min(100, toEnqueue.length - i);
  }

  return jsonResponse({
    from_time:        fromTime,
    safe_cutoff:      safeCutoff,
    prefix,
    listed:           listed.objects.length,
    filtered_too_old: filteredByTime,
    filtered_too_new: filteredByLag,
    filtered_done:    filteredDone,
    queued,
    truncated:        listed.truncated,
    next_cursor:      listed.cursor || null,
    note: listed.truncated
      ? 'More files available. Use next_cursor to continue (recommend 30s interval).'
      : 'All files in this page enqueued.',
  });
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：系统状态
// ═══════════════════════════════════════════════════════════════
async function handleApiStatus(request, env) {
  const c = getConfig(env);
  const safeCutoff = new Date(Date.now() - c.lagMinutes * 60 * 1000).toISOString();

  const [total, done, processing, failed, dataRange, lastDone] = await Promise.all([
    env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files`).first(),
    env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='done'`).first(),
    env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='processing'`).first(),
    env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='failed'`).first(),
    env.DB.prepare(`SELECT MIN(minute_utc) AS min_t, MAX(minute_utc) AS max_t FROM bw_stats`).first(),
    env.DB.prepare(`SELECT r2_key, finished_at, line_count
                    FROM processed_files WHERE status='done'
                    ORDER BY finished_at DESC LIMIT 1`).first(),
  ]);

  return jsonResponse({
    config: {
      log_prefix:        c.prefix,
      start_time:        c.startTime,
      lag_minutes:       c.lagMinutes,
      safe_cutoff_now:   safeCutoff,
      scan_batch_size:   c.scanBatchSize,
      retention_days:    c.retentionDays,
      cleanup_schedule:  'daily at UTC 02:00',
    },
    files: {
      total:      total?.c      || 0,
      done:       done?.c       || 0,
      processing: processing?.c || 0,
      failed:     failed?.c     || 0,
    },
    data_range: dataRange,
    last_done:  lastDone,
    estimated_chart_lag: `~${c.lagMinutes + 10}–${c.lagMinutes + 20} min`,
  });
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：重试失败文件
// ═══════════════════════════════════════════════════════════════
async function handleApiRetry(request, env) {
  if (request.method !== 'POST') return jsonResponse({ error: 'POST only' }, 405);
  const failed = await env.DB.prepare(
    `SELECT r2_key FROM processed_files WHERE status='failed' LIMIT 50`
  ).all();
  if (!failed.results.length) return jsonResponse({ retried: 0, msg: 'No failed files' });
  await env.DB.batch(failed.results.map(r =>
    env.DB.prepare(
      `UPDATE processed_files SET status='pending', error_msg=NULL, retry_count=retry_count+1 WHERE r2_key=?`
    ).bind(r.r2_key)
  ));
  await env.INGEST_QUEUE.sendBatch(failed.results.map(r => ({ body: { key: r.r2_key } })));
  return jsonResponse({ retried: failed.results.length });
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：查询带宽统计数据
// GET /api/stats?zone=xxx&hours=24
// ═══════════════════════════════════════════════════════════════
async function handleApiStats(request, env) {
  const url   = new URL(request.url);
  const zone  = url.searchParams.get('zone')  || '%';
  const hours = parseInt(url.searchParams.get('hours') || '24', 10);
  const since = new Date(Date.now() - hours * 3600 * 1000).toISOString().slice(0, 16);

  const rows = await env.DB.prepare(`
    SELECT
      minute_utc,
      zone,
      sum_bytes,
      ROUND(CAST(sum_bytes AS REAL) / 60.0 * 8.0 / 1048576.0, 2) AS mbps
    FROM bw_stats
    WHERE minute_utc >= ? AND zone LIKE ?
    ORDER BY minute_utc ASC
  `).bind(since, zone).all();

  return jsonResponse({
    formula: 'SUM(CacheResponseBytes) WHERE OriginStatus NOT IN (0,304) ÷ 60s × 8bits ÷ 1024² = Mbps',
    ref:     'https://developers.cloudflare.com/logs/faq/common-calculations/',
    since, zone,
    count: rows.results.length,
    data:  rows.results,
  });
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：Dashboard 图表页面
// ═══════════════════════════════════════════════════════════════
async function handleDashboard(request, env) {
  const url   = new URL(request.url);
  const zone  = url.searchParams.get('zone')  || '';
  const hours = url.searchParams.get('hours') || '24';
  const token = url.searchParams.get('token') || '';

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CF Origin Pull Bandwidth Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#0a0a0a;color:#e0e0e0;padding:24px}
h1{color:#f6821f;font-size:18px;margin-bottom:4px}
.sub{color:#555;font-size:11px;margin-bottom:18px}.sub a{color:#666}
.ctrl{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;align-items:center}
input,select,button{background:#1a1a1a;color:#e0e0e0;border:1px solid #2a2a2a;
  padding:6px 10px;border-radius:4px;font-size:13px;outline:none}
input{width:320px}
.btn{background:#f6821f;color:#000;border:none;cursor:pointer;font-weight:600;padding:6px 14px}
.btn:hover{background:#ff9a3c}
.btn-sec{background:#1a1a1a;color:#aaa}
.cards{display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.card{background:#111;border:1px solid #1e1e1e;border-radius:6px;padding:10px 16px;min-width:130px}
.card-l{font-size:11px;color:#666;margin-bottom:4px}
.card-v{font-size:20px;font-weight:700;color:#f6821f}
.wrap{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:18px}
.foot{font-size:11px;color:#333;margin-top:12px;line-height:1.6}
#st{font-size:12px;color:#666;margin-left:8px}
code{background:#1a1a1a;padding:2px 6px;border-radius:3px;font-size:11px;color:#aaa}
</style>
</head>
<body>
<h1>🟠 CF Origin Pull Bandwidth Monitor</h1>
<p class="sub">
  <code>SUM(CacheResponseBytes) WHERE OriginStatus NOT IN (0,304) ÷ 60s × 8 = Mbps</code>
  &nbsp;—&nbsp;<a href="https://developers.cloudflare.com/logs/faq/common-calculations/" target="_blank">Official Ref</a>
  &nbsp;|&nbsp;Times in CST (UTC+8)
</p>

<div class="ctrl">
  <input  id="zi" placeholder="Zone hostname, e.g. example.com (blank = all)" value="${zone}">
  <select id="hs">
    <option value="1"   ${hours==='1'  ?'selected':''}>Last 1 hour</option>
    <option value="6"   ${hours==='6'  ?'selected':''}>Last 6 hours</option>
    <option value="24"  ${hours==='24' ?'selected':''}>Last 24 hours</option>
    <option value="72"  ${hours==='72' ?'selected':''}>Last 3 days</option>
    <option value="168" ${hours==='168'?'selected':''}>Last 7 days</option>
  </select>
  <button class="btn"     onclick="loadChart()">Load</button>
  <button class="btn btn-sec" onclick="loadStatus()">Status</button>
  <span id="st"></span>
</div>

<div class="cards">
  <div class="card"><div class="card-l">Peak Bandwidth</div><div class="card-v" id="cPk">—</div></div>
  <div class="card"><div class="card-l">Avg Bandwidth</div><div class="card-v" id="cAv">—</div></div>
  <div class="card"><div class="card-l">Total Origin Pull</div><div class="card-v" id="cTo">—</div></div>
  <div class="card"><div class="card-l">Data Points</div><div class="card-v" id="cPt">—</div></div>
</div>

<div class="wrap">
  <div style="font-size:12px;color:#888;margin-bottom:8px;font-weight:600">
    📡 Origin Pull Bandwidth (Mbps)
    <span style="color:#555;font-weight:normal">&nbsp;SUM(CacheResponseBytes) ÷ 60s × 8</span>
  </div>
  <canvas id="chartBw" height="110"></canvas>
  <p class="foot">
    Source: Logpush → R2 → Workers → D1 → Chart.js
    &nbsp;|&nbsp;Data lag: ~20 min&nbsp;|&nbsp;Retention: 7 days
    &nbsp;|&nbsp;Last updated: <span id="lu">—</span>
  </p>
</div>

<script>
const TOKEN = '${token}';
const HDRS  = TOKEN ? { Authorization: 'Bearer ' + TOKEN } : {};
let chartBw = null;

function toCST(minute_utc) {
  const t   = new Date(minute_utc + ':00Z');
  const cst = new Date(t.getTime() + 8 * 3600000);
  const iso = cst.toISOString();
  // 格式：2026-04-18 10:38 GMT+8
  return iso.slice(0, 10) + ' ' + iso.slice(11, 16) + ' GMT+8';
}

async function loadChart() {
  const zone  = document.getElementById('zi').value.trim();
  const hours = document.getElementById('hs').value;
  document.getElementById('st').textContent = 'Loading...';
  let json;
  try {
    const p = new URLSearchParams({ hours });
    if (zone) p.set('zone', zone);
    const r = await fetch('/api/stats?' + p, { headers: HDRS });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    json = await r.json();
  } catch (e) {
    document.getElementById('st').textContent = '❌ ' + e.message; return;
  }
  const data  = json.data || [];
  const labels = data.map(d => toCST(d.minute_utc));
  const vals   = data.map(d => d.mbps);
  const nonZ   = vals.filter(v => v > 0);
  document.getElementById('cPk').textContent = (nonZ.length ? Math.max(...vals).toFixed(1) : '0') + ' Mbps';
  document.getElementById('cAv').textContent = (nonZ.length ? (nonZ.reduce((a,b)=>a+b,0)/nonZ.length).toFixed(1) : '0') + ' Mbps';
  document.getElementById('cTo').textContent = (data.reduce((a,d)=>a+(d.sum_bytes||0),0)/1073741824).toFixed(2) + ' GB';
  document.getElementById('cPt').textContent = data.length;
  document.getElementById('lu').textContent  = new Date().toLocaleString('zh-CN');
  document.getElementById('st').textContent  = data.length + ' points | zone: ' + (zone || 'all');
  const ctx = document.getElementById('chartBw').getContext('2d');
  if (chartBw) chartBw.destroy();
  chartBw = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets: [{
      label: (zone || 'All Zones') + ' — Origin Pull Bandwidth',
      data: vals,
      borderColor: 'rgb(246,130,31)',
      backgroundColor: 'rgba(246,130,31,0.08)',
      borderWidth: 1.5, tension: 0.2, fill: true,
      pointRadius: vals.length > 300 ? 0 : 2, pointHoverRadius: 5,
    }]},
    options: {
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: { ticks: { color: '#777', maxTicksLimit: 12, maxRotation: 0,
               callback: function(value, index, ticks) {
                 const label = this.getLabelForValue(value);
                 // 第一个和最后一个显示完整日期+时间，中间的只显示时间
                 if (index === 0 || index === ticks.length - 1) return label;
                 // 去掉日期部分，只保留 "HH:MM GMT+8"
                 const parts = label.split(' ');
                 return parts[1] + ' ' + parts[2]; // "HH:MM GMT+8"
               }
             }, grid: { color: '#1a1a1a' } },
        y: { ticks: { color: '#777', callback: v => v + ' Mbps' }, grid: { color: '#1a1a1a' }, beginAtZero: true }
      },
      plugins: {
        legend: { labels: { color: '#bbb', font: { size: 12 } } },
        tooltip: { callbacks: { label: c => [
          'Bandwidth: ' + c.parsed.y + ' Mbps',
          'Sum: ' + ((data[c.dataIndex]?.sum_bytes||0)/1048576).toFixed(0) + ' MB',
        ]}}
      }
    }
  });
}

async function loadStatus() {
  try {
    const r = await fetch('/api/status', { headers: HDRS });
    const j = await r.json();
    const cst = iso => iso ? new Date(new Date(iso+':00Z').getTime()+8*3600000)
      .toISOString().slice(0,16).replace('T',' ') + ' CST' : 'N/A';
    document.getElementById('st').textContent =
      'done=' + j.files.done + ' proc=' + j.files.processing + ' fail=' + j.files.failed +
      ' | data: ' + cst(j.data_range?.min_t) + ' ~ ' + cst(j.data_range?.max_t);
  } catch (e) { document.getElementById('st').textContent = '❌ ' + e.message; }
}

if ('${zone}') loadChart();
</script>
</body>
</html>`;

  return new Response(html, { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });
}

// ═══════════════════════════════════════════════════════════════
// D1 数据生命周期清理（每日 UTC 02:00）
// ═══════════════════════════════════════════════════════════════
async function cleanupOldData(env) {
  const c = getConfig(env);
  const cutoff     = new Date(Date.now() - c.retentionDays * 24 * 3600 * 1000).toISOString().slice(0, 16);
  const cutoffFull = new Date(Date.now() - c.retentionDays * 24 * 3600 * 1000).toISOString();
  log(env, 'info', `Cleanup: deleting data older than ${cutoff} (retention=${c.retentionDays}d)`);
  try {
    const r1 = await env.DB.prepare(`DELETE FROM bw_stats WHERE minute_utc < ?`).bind(cutoff).run();
    const r2 = await env.DB.prepare(
      `DELETE FROM processed_files WHERE finished_at IS NOT NULL AND finished_at < ?`
    ).bind(cutoffFull).run();
    const r3 = await env.DB.prepare(
      `DELETE FROM alert_history WHERE alerted_at < ?`
    ).bind(cutoffFull).run();
    log(env, 'info',
      `Cleanup done: bw_stats=${r1.meta?.changes??0} rows, processed_files=${r2.meta?.changes??0} rows, alert_history=${r3.meta?.changes??0} rows`
    );
  } catch (err) { log(env, 'error', `Cleanup failed: ${err.message}`); }
}

// ─── 标记失败 ─────────────────────────────────────────────────
async function markFileFailed(key, errMsg, env) {
  await env.DB.prepare(
    `UPDATE processed_files SET status='failed', error_msg=?, finished_at=? WHERE r2_key=?`
  ).bind(errMsg.slice(0, 500), new Date().toISOString(), key).run();
}

// ─── 工具函数 ─────────────────────────────────────────────────
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  });
}

function isAuthorized(request, env) {
  const c = getConfig(env);
  if (!c.dashToken) return true;
  const url   = new URL(request.url);
  const auth  = request.headers.get('Authorization') || '';
  const token = auth.replace('Bearer ', '').trim() || url.searchParams.get('token') || '';
  return token === c.dashToken;
}

function log(env, level, msg) {
  if ((LOG_LEVELS[level] ?? 1) >= (LOG_LEVELS[env?.LOG_LEVEL ?? 'info'] ?? 1)) {
    const fn = level === 'error' ? console.error : level === 'warn' ? console.warn : console.log;
    fn(`[${level.toUpperCase()}] ${new Date().toISOString()} ${msg}`);
  }
}

// ═══════════════════════════════════════════════════════════════
// 告警模块：写入 D1 后立即检测，支持固定阈值 + 突增检测
// 支持企业微信 / 钉钉 / 飞书 三平台卡片消息，任意组合
// ═══════════════════════════════════════════════════════════════

async function checkAndAlert(minuteMap, env) {
  const c   = getConfig(env);
  const now = new Date().toISOString();

  // 24小时前（用于突增检测的历史窗口）
  const since24h = new Date(Date.now() - 24 * 3600 * 1000).toISOString().slice(0, 16);

  for (const [mapKey, v] of minuteMap.entries()) {
    const idx    = mapKey.indexOf('\x00');
    const minute = mapKey.slice(0, idx);
    const zone   = mapKey.slice(idx + 1);
    if (zone === 'unknown') continue;

    const mbps = v.sum / 60 * 8 / 1048576;
    if (mbps <= 0) continue;

    // ── 告警1：固定阈值 ──────────────────────────────────────
    if (c.alertThresholdMbps !== null && mbps >= c.alertThresholdMbps) {
      const cooldownOk = await checkCooldown('threshold', zone, c.alertCooldownMin, env);
      if (cooldownOk) {
        const detail = { threshold: c.alertThresholdMbps, current: mbps };
        await sendAlert({
          type:    'threshold',
          zone,
          minute,
          mbps,
          detail,
          title:   '🚨 CF 回源带宽超阈值告警',
          summary: `当前带宽 **${mbps.toFixed(1)} Mbps** 超过设定阈值 **${c.alertThresholdMbps} Mbps**`,
          dashUrl: c.alertDashboardUrl,
        }, env);
        await recordAlert('threshold', zone, minute, mbps, detail, env);
        log(env, 'warn', `Alert [threshold] zone=${zone} mbps=${mbps.toFixed(1)} threshold=${c.alertThresholdMbps}`);
      }
    }

    // ── 告警2：突增检测 ──────────────────────────────────────
    if (c.alertSpikeMultiplier !== null) {
      // 查过去24小时该 zone 的历史最高 mbps（排除当前分钟）
      const row = await env.DB.prepare(`
        SELECT MAX(ROUND(CAST(sum_bytes AS REAL) / 60.0 * 8.0 / 1048576.0, 2)) AS peak_mbps
        FROM bw_stats
        WHERE zone = ? AND minute_utc >= ? AND minute_utc < ?
      `).bind(zone, since24h, minute).first();

      const historicPeak = row?.peak_mbps || 0;
      if (historicPeak > 0 && mbps >= historicPeak * c.alertSpikeMultiplier) {
        const cooldownOk = await checkCooldown('spike', zone, c.alertCooldownMin, env);
        if (cooldownOk) {
          const ratio  = (mbps / historicPeak).toFixed(1);
          const detail = { multiplier: c.alertSpikeMultiplier, current: mbps, historicPeak, ratio };
          await sendAlert({
            type:    'spike',
            zone,
            minute,
            mbps,
            detail,
            title:   '⚡ CF 回源带宽突增告警',
            summary: `当前带宽 **${mbps.toFixed(1)} Mbps**，是过去24小时最高值 **${historicPeak.toFixed(1)} Mbps** 的 **${ratio} 倍**（阈值：${c.alertSpikeMultiplier} 倍）`,
            dashUrl: c.alertDashboardUrl,
          }, env);
          await recordAlert('spike', zone, minute, mbps, detail, env);
          log(env, 'warn', `Alert [spike] zone=${zone} mbps=${mbps.toFixed(1)} peak=${historicPeak.toFixed(1)} ratio=${ratio}`);
        }
      }
    }
  }
}

// ── 冷却检查：同 zone 同类型告警是否已在冷却期内 ──────────────
async function checkCooldown(alertType, zone, cooldownMin, env) {
  const since = new Date(Date.now() - cooldownMin * 60 * 1000).toISOString();
  const row = await env.DB.prepare(`
    SELECT id FROM alert_history
    WHERE alert_type = ? AND zone = ? AND alerted_at >= ?
    LIMIT 1
  `).bind(alertType, zone, since).first();
  return !row;  // true = 不在冷却期，可以发告警
}

// ── 记录告警历史 ──────────────────────────────────────────────
async function recordAlert(alertType, zone, minute, mbps, detail, env) {
  await env.DB.prepare(`
    INSERT INTO alert_history (alert_type, zone, minute_utc, mbps, detail, alerted_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).bind(alertType, zone, minute, mbps, JSON.stringify(detail), new Date().toISOString()).run();
}

// ── 发送告警到各平台 ─────────────────────────────────────────
async function sendAlert({ type, zone, minute, mbps, detail, title, summary, dashUrl }, env) {
  // 时间转 GMT+8 显示
  const t   = new Date(minute + ':00Z');
  const cst = new Date(t.getTime() + 8 * 3600000);
  const timeStr = cst.toISOString().slice(0, 10) + ' ' + cst.toISOString().slice(11, 16) + ' GMT+8';

  const tasks = [];

  // 企业微信
  if (env.ALERT_WEBHOOK_WECOM) {
    tasks.push(sendWeCom(env.ALERT_WEBHOOK_WECOM, { title, summary, zone, timeStr, mbps, detail, dashUrl }));
  }
  // 钉钉
  if (env.ALERT_WEBHOOK_DINGTALK) {
    tasks.push(sendDingTalk(env.ALERT_WEBHOOK_DINGTALK, { title, summary, zone, timeStr, mbps, detail, dashUrl }));
  }
  // 飞书
  if (env.ALERT_WEBHOOK_FEISHU) {
    tasks.push(sendFeishu(env.ALERT_WEBHOOK_FEISHU, { title, summary, zone, timeStr, mbps, detail, dashUrl }));
  }

  if (tasks.length === 0) return;
  const results = await Promise.allSettled(tasks);
  results.forEach((r, i) => {
    if (r.status === 'rejected') {
      console.warn(`[WARN] Alert channel ${i} failed: ${r.reason?.message}`);
    }
  });
}

// ── 企业微信 Markdown 卡片 ────────────────────────────────────
async function sendWeCom(webhookUrl, { title, summary, zone, timeStr, mbps, detail, dashUrl }) {
  let content = `## ${title}\n\n`;
  content += `> **域名：** ${zone}\n`;
  content += `> **时间：** ${timeStr}\n`;
  content += `> **当前带宽：** <font color="warning">${mbps.toFixed(1)} Mbps</font>\n`;
  if (detail.threshold) {
    content += `> **设定阈值：** ${detail.threshold} Mbps\n`;
  }
  if (detail.historicPeak) {
    content += `> **24h历史峰值：** ${detail.historicPeak.toFixed(1)} Mbps\n`;
    content += `> **当前倍数：** <font color="warning">${detail.ratio} 倍</font>\n`;
  }
  if (dashUrl) {
    content += `\n[查看监控面板](${dashUrl}?zone=${encodeURIComponent(zone)}&hours=24)`;
  }

  await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ msgtype: 'markdown', markdown: { content } }),
  });
}

// ── 钉钉 ActionCard ───────────────────────────────────────────
async function sendDingTalk(webhookUrl, { title, summary, zone, timeStr, mbps, detail, dashUrl }) {
  let text = `## ${title}\n\n`;
  text += `**域名：** ${zone}  \n`;
  text += `**时间：** ${timeStr}  \n`;
  text += `**当前带宽：** ${mbps.toFixed(1)} Mbps  \n`;
  if (detail.threshold) {
    text += `**设定阈值：** ${detail.threshold} Mbps  \n`;
  }
  if (detail.historicPeak) {
    text += `**24h历史峰值：** ${detail.historicPeak.toFixed(1)} Mbps  \n`;
    text += `**当前倍数：** ${detail.ratio} 倍  \n`;
  }

  const body = {
    msgtype: 'actionCard',
    actionCard: {
      title,
      text,
      btnOrientation: '0',
      btns: dashUrl
        ? [{ title: '查看监控面板', actionURL: dashUrl + '?zone=' + encodeURIComponent(zone) + '&hours=24' }]
        : [],
    },
  };

  await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

// ── 飞书 Interactive Card ─────────────────────────────────────
async function sendFeishu(webhookUrl, { title, summary, zone, timeStr, mbps, detail, dashUrl }) {
  const fields = [
    { is_short: true, text: { tag: 'lark_md', content: `**域名**\n${zone}` } },
    { is_short: true, text: { tag: 'lark_md', content: `**时间**\n${timeStr}` } },
    { is_short: true, text: { tag: 'lark_md', content: `**当前带宽**\n${mbps.toFixed(1)} Mbps` } },
  ];

  if (detail.threshold) {
    fields.push({ is_short: true, text: { tag: 'lark_md', content: `**设定阈值**\n${detail.threshold} Mbps` } });
  }
  if (detail.historicPeak) {
    fields.push({ is_short: true, text: { tag: 'lark_md', content: `**24h历史峰值**\n${detail.historicPeak.toFixed(1)} Mbps` } });
    fields.push({ is_short: true, text: { tag: 'lark_md', content: `**当前倍数**\n${detail.ratio} 倍` } });
  }

  const elements = [{ tag: 'div', fields }];
  if (dashUrl) {
    elements.push({
      tag: 'action',
      actions: [{
        tag: 'button',
        text: { tag: 'plain_text', content: '查看监控面板' },
        url: dashUrl + '?zone=' + encodeURIComponent(zone) + '&hours=24',
        type: 'default',
      }],
    });
  }

  const body = {
    msg_type: 'interactive',
    card: {
      header: { title: { tag: 'plain_text', content: title }, template: 'red' },
      elements,
    },
  };

  await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}
