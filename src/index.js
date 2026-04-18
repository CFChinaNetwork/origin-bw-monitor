'use strict';

/**
 * CF Origin Pull Bandwidth Monitor
 *
 * 从客户 Logpush R2 日志中计算回源带宽，生成分钟级时序图表。
 *
 * 架构：
 *   客户 R2（只读）→ Cron/HTTP 触发 → Queue（仅传 R2 key）
 *   → Queue Consumer（流式解析）→ D1（分钟级聚合）→ Dashboard
 *
 * 回源带宽公式（官方）：
 *   SUM(CacheResponseBytes) WHERE OriginResponseStatus NOT IN (0, 304)
 *   Mbps = sum_bytes / 60s * 8 / 1024²
 *   参考：https://developers.cloudflare.com/logs/faq/common-calculations/
 *
 * 字段说明：
 *   - OS=0：HIT，未回源，排除
 *   - OS=304：revalidated，无 body 传输，排除
 *   - 其余 OS：计入带宽（与官方口径一致）
 *   - 时间分桶：EdgeEndTimestamp（请求结束时间），与 CF 控制台口径一致
 *
 * 对目标 R2 的承诺：所有操作均为只读（LIST / GET），绝不执行任何写入。
 *
 * 配置说明：所有参数均来自 wrangler.toml [vars] 或 Secrets，
 *            index.js 本身不含任何账号、密钥或客户信息。
 */

// ─── 常量 ─────────────────────────────────────────────────────
const CF_API_BASE = 'https://api.cloudflare.com/client/v4';
const LOG_LEVELS  = { debug: 0, info: 1, warn: 2, error: 3 };

// ─── 读取并解析所有配置（统一从 env 取，不写死）──────────────
function getConfig(env) {
  return {
    // 客户 R2
    accountId:         env.CUSTOMER_ACCOUNT_ID  || '',
    bucket:            env.CUSTOMER_BUCKET       || '',
    prefix:            env.LOG_PREFIX            || 'logs/',
    // 时间控制
    startTime:         env.START_TIME            || '1970-01-01T00:00:00Z',
    lagMinutes:        parseInt(env.LAG_MINUTES  || '10', 10),
    // 流控
    scanBatchSize:     parseInt(env.SCAN_BATCH_SIZE      || '20', 10),
    rateLimitDelayMs:  parseInt(env.RATE_LIMIT_DELAY_MS  || '300', 10),
    maxProcessingMin:  parseInt(env.MAX_PROCESSING_MIN   || '15', 10),
    // 数据生命周期
    retentionDays:     parseInt(env.DATA_RETENTION_DAYS  || '5', 10),
    // 安全
    dashToken:         env.DASHBOARD_TOKEN || '',
    // 日志
    logLevel:          env.LOG_LEVEL || 'info',
  };
}

// ─── 主入口 ───────────────────────────────────────────────────
export default {

  // ── Cron 调度 ────────────────────────────────────────────────
  // cron 1：*/10 * * * *  → 每10分钟扫描 R2 新文件并入队
  // cron 2：0 2 * * *     → 每天 UTC 02:00 清理 D1 过期数据
  async scheduled(event, env, ctx) {
    const cron = event.cron;
    if (cron === '0 2 * * *') {
      // 每日清理：删除超过 DATA_RETENTION_DAYS 天的历史数据
      ctx.waitUntil(cleanupOldData(env));
    } else {
      // 默认：扫描 R2 新文件并入队
      ctx.waitUntil(scanAndEnqueue(env));
    }
  },

  // ── Queue Consumer：串行解析文件，写入 D1 ──────────────────
  async queue(batch, env, ctx) {
    // 串行处理（不并发），避免内存压力和 D1 写冲突
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
    // 所有 HTTP 请求都先鉴权
    if (!isAuthorized(request, env)) {
      return new Response('Unauthorized', { status: 401 });
    }

    const url  = new URL(request.url);
    const path = url.pathname;

    if (path === '/api/stats')   return handleApiStats(request, env);
    if (path === '/api/import')  return handleApiImport(request, env);
    if (path === '/api/status')  return handleApiStatus(request, env);
    if (path === '/api/retry')   return handleApiRetry(request, env);

    // 默认返回 Dashboard 图表页面
    return handleDashboard(request, env);
  },
};

// ═══════════════════════════════════════════════════════════════
// Cron：扫描新文件，入队
// ═══════════════════════════════════════════════════════════════
async function scanAndEnqueue(env) {
  const c = getConfig(env);

  // Safe Lag Window：计算安全截止时间
  // 只处理 last_modified <= safeCutoff 的文件，
  // 保证同一时间窗口内的 Logpush 文件已全部写入 R2
  const safeCutoff = new Date(Date.now() - c.lagMinutes * 60 * 1000).toISOString();

  log(env, 'info', `Cron scan: prefix=${c.prefix} startTime=${c.startTime} safeCutoff=${safeCutoff} batchSize=${c.scanBatchSize}`);

  // 列出 R2 文件（只读）
  let listResult;
  try {
    listResult = await r2List(env, c.prefix, c.scanBatchSize);
  } catch (err) {
    log(env, 'error', `R2 list failed: ${err.message}`);
    return;
  }

  const toEnqueue = [];
  const stuckThreshold = new Date(Date.now() - c.maxProcessingMin * 60 * 1000).toISOString();

  for (const obj of listResult.objects) {
    // 过滤1：只处理 START_TIME 之后写入的文件
    if (obj.last_modified < c.startTime) continue;

    // 过滤2：Safe Lag Window，太新的文件跳过（数据可能还不完整）
    if (obj.last_modified > safeCutoff) {
      log(env, 'debug', `Skip (too recent): ${obj.key}`);
      continue;
    }

    // 查询该文件的处理状态
    const existing = await env.DB.prepare(
      `SELECT status, started_at FROM processed_files WHERE r2_key = ?`
    ).bind(obj.key).first();

    // 过滤3：已成功处理的文件永远跳过
    if (existing?.status === 'done') continue;

    // 过滤4：正在处理中的文件，检查是否超时（Worker 可能已崩溃）
    if (existing?.status === 'processing') {
      if ((existing.started_at || '') > stuckThreshold) {
        // 仍在合理处理时间内，跳过
        log(env, 'debug', `Skip (still processing): ${obj.key}`);
        continue;
      }
      // 超时，认为 Worker 已崩溃，重置状态，重新入队
      log(env, 'warn', `Resetting stuck file: ${obj.key}`);
      await env.DB.prepare(
        `UPDATE processed_files SET status = 'pending', error_msg = 'reset_stuck' WHERE r2_key = ?`
      ).bind(obj.key).run();
    }

    toEnqueue.push(obj.key);
  }

  if (toEnqueue.length === 0) {
    log(env, 'info', 'Cron: no new files to enqueue');
    return;
  }

  // 入队（每条消息只含 R2 key，消息体极小，远低于 128KB 限制）
  const msgs = toEnqueue.map(key => ({ body: { key } }));
  await env.INGEST_QUEUE.sendBatch(msgs);
  log(env, 'info', `Cron: enqueued ${msgs.length} files`);
}

// ═══════════════════════════════════════════════════════════════
// Queue Consumer：完整安全地处理单个文件
// ═══════════════════════════════════════════════════════════════
async function processFile(key, env) {
  const c = getConfig(env);

  // ★ 幂等保护：已成功处理的文件绝不重复处理
  const existing = await env.DB.prepare(
    `SELECT status FROM processed_files WHERE r2_key = ?`
  ).bind(key).first();
  if (existing?.status === 'done') {
    log(env, 'info', `Already done, skip: ${key}`);
    return;
  }

  // 标记为 processing（记录开始时间，用于崩溃检测）
  await env.DB.prepare(`
    INSERT INTO processed_files (r2_key, status, started_at)
    VALUES (?, 'processing', ?)
    ON CONFLICT(r2_key) DO UPDATE SET
      status     = 'processing',
      started_at = excluded.started_at,
      error_msg  = NULL
  `).bind(key, new Date().toISOString()).run();

  log(env, 'info', `Processing: ${key}`);

  // 计算本次处理的 Safe Lag 截止时间
  // 行级过滤：文件内每行日志也要过滤，确保只统计完整的分钟数据
  const safeCutoffMs = Date.now() - c.lagMinutes * 60 * 1000;

  // ★ 读取客户 R2（只读，绝不写入）
  await sleep(c.rateLimitDelayMs);  // 速率限制保护
  const resp = await r2Get(env, key);
  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(`R2 GET ${resp.status}: ${errText.slice(0, 300)}`);
  }

  // ★ 流式解析：
  //   - CF REST API 返回 chunked 传输，response.body 是真正的 ReadableStream
  //   - CF REST API 自动解压 gzip，返回明文 NDJSON，不需要 DecompressionStream
  //   - 逐行处理，内存中只保留 minuteMap（聚合数字），不缓存原始文本
  const minuteMap = new Map(); // key: "2026-04-14T03:01|zone", value: {sum, count}
  let lineCount     = 0;
  let skippedLag    = 0;  // 因 Safe Lag Window 被跳过的行数
  let skippedFilter = 0;  // 因官方公式过滤被跳过的行数
  let errCount      = 0;

  const reader  = resp.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let   buffer  = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        // 处理最后一行（文件末尾可能没有换行符）
        const last = buffer.trim();
        if (last) {
          const result = applyFormula(last, minuteMap, safeCutoffMs);
          if (result === 'ok')      lineCount++;
          else if (result === 'lag')     skippedLag++;
          else if (result === 'filter')  skippedFilter++;
          else errCount++;
        }
        break;
      }

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';  // 最后一段可能不完整，留给下次

      for (const line of lines) {
        const t = line.trim();
        if (!t) continue;
        const result = applyFormula(t, minuteMap, safeCutoffMs);
        if (result === 'ok')      lineCount++;
        else if (result === 'lag')     skippedLag++;
        else if (result === 'filter')  skippedFilter++;
        else errCount++;
      }
    }
  } finally {
    reader.releaseLock();
  }

  // 统计文件涉及的分钟范围（用于 status 展示和调试）
  let fileMinuteMin = null;
  let fileMinuteMax = null;
  for (const k of minuteMap.keys()) {
    const minute = k.split('|')[0];
    if (!fileMinuteMin || minute < fileMinuteMin) fileMinuteMin = minute;
    if (!fileMinuteMax || minute > fileMinuteMax) fileMinuteMax = minute;
  }

  // ★ 分钟完整性保证：累加 upsert
  //   同一分钟的数据来自多个文件时，多次累加最终得到完整数据
  //   幂等保护（上方的 done 检查）确保同一文件不被重复计算
  await flushToD1(minuteMap, env);

  // 全部成功后才标记为 done
  await env.DB.prepare(`
    UPDATE processed_files SET
      status          = 'done',
      finished_at     = ?,
      line_count      = ?,
      file_minute_min = ?,
      file_minute_max = ?
    WHERE r2_key = ?
  `).bind(
    new Date().toISOString(),
    lineCount,
    fileMinuteMin,
    fileMinuteMax,
    key
  ).run();

  log(env, 'info',
    `Done: ${key} | lines=${lineCount} skippedLag=${skippedLag} skippedFilter=${skippedFilter} errors=${errCount} minutes=${minuteMap.size}`
  );
}

// ═══════════════════════════════════════════════════════════════
// 官方公式：处理单行日志，统计回源带宽
// 返回值：'ok' | 'lag' | 'filter' | 'error'
//
// 公式来源：https://developers.cloudflare.com/logs/faq/common-calculations/
//   SUM(CacheResponseBytes) WHERE OriginResponseStatus NOT IN (0, 304)
//   - OS=0：未回源（HIT），排除
//   - OS=304：revalidated，无 body 传输，排除
//   - 其余 OS：计入（含 4xx/5xx，与官方口径一致）
//
// 时间分桶：EdgeEndTimestamp（请求结束时间），与 CF 控制台口径一致
// ═══════════════════════════════════════════════════════════════
function applyFormula(line, minuteMap, safeCutoffMs) {
  let r;
  try {
    r = JSON.parse(line);
  } catch {
    return 'error';
  }

  // Safe Lag Window：用 EdgeEndTimestamp 判断（与分桶时间字段一致）
  const te = r.EdgeEndTimestamp;
  if (!te || te * 1000 > safeCutoffMs) return 'lag';

  // 官方公式过滤：排除 OS=0（未回源）和 OS=304（revalidated，无 body）
  const os = r.OriginResponseStatus;
  if (os === 0 || os === 304) return 'filter';

  const cb = r.CacheResponseBytes || 0;
  if (cb === 0) return 'filter';

  // 按 EdgeEndTimestamp 分桶（UTC 自然分钟）
  const d = new Date(te * 1000);
  d.setSeconds(0, 0);
  const minute = d.toISOString().slice(0, 16);
  const zone   = r.ClientRequestHost || 'unknown';
  const mapKey = `${minute}|${zone}`;
  const entry  = minuteMap.get(mapKey) || { sum: 0 };
  entry.sum   += cb;
  minuteMap.set(mapKey, entry);

  return 'ok';
}

// ═══════════════════════════════════════════════════════════════
// 批量写入 D1（累加 upsert，支持多文件写同一分钟）
// ═══════════════════════════════════════════════════════════════
async function flushToD1(minuteMap, env) {
  if (minuteMap.size === 0) return;

  const entries = [...minuteMap.entries()];
  const CHUNK   = 80;  // D1 batch 单次上限100，保守取80

  for (let i = 0; i < entries.length; i += CHUNK) {
    const stmts = entries.slice(i, i + CHUNK).map(([mapKey, v]) => {
      const [minute, zone] = mapKey.split('|');
      // ON CONFLICT DO UPDATE：累加而非覆盖，天然支持多文件写同一分钟
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
// CF REST API：R2 LIST（只读）
// ═══════════════════════════════════════════════════════════════
async function r2List(env, prefix, limit, cursor) {
  const c = getConfig(env);
  const params = new URLSearchParams({
    prefix,
    limit: String(Math.min(limit || 100, 1000)),
  });
  if (cursor) params.set('cursor', cursor);

  const url  = `${CF_API_BASE}/accounts/${c.accountId}/r2/buckets/${c.bucket}/objects?${params}`;
  const resp = await fetch(url, { headers: cfApiHeaders(env) });

  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(`R2 list ${resp.status}: ${t.slice(0, 200)}`);
  }

  const json = await resp.json();
  return {
    objects:   json.result || [],
    truncated: !!json.result_info?.cursor,
    cursor:    json.result_info?.cursor || null,
  };
}

// ═══════════════════════════════════════════════════════════════
// CF REST API：R2 GET 文件内容（只读，流式）
// ★ 此函数只做 GET，绝不做任何写操作
// ═══════════════════════════════════════════════════════════════
async function r2Get(env, key) {
  const c = getConfig(env);
  // 对 key 中的每一段路径分别 encode，避免 / 被 encode 掉
  const encodedKey = key.split('/').map(encodeURIComponent).join('/');
  const url = `${CF_API_BASE}/accounts/${c.accountId}/r2/buckets/${c.bucket}/objects/${encodedKey}`;
  return fetch(url, { headers: cfApiHeaders(env) });
  // 注意：
  //   response.body 是 ReadableStream（CF API 使用 chunked 传输，无 content-length）
  //   CF API 自动解压 gzip，返回明文 NDJSON，不需要 DecompressionStream
}

// ─── CF API 鉴权 Headers ──────────────────────────────────────
// 使用 CF API Token（Bearer 方式），通过 wrangler secret put CF_API_TOKEN 设置
// 比 X-Auth-Email + X-Auth-Key 方式更安全，支持最小权限原则
function cfApiHeaders(env) {
  return {
    'Authorization': `Bearer ${env.CF_API_TOKEN}`,
  };
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：标记失败
// ═══════════════════════════════════════════════════════════════
async function markFileFailed(key, errMsg, env) {
  await env.DB.prepare(`
    UPDATE processed_files SET
      status     = 'failed',
      error_msg  = ?,
      finished_at = ?
    WHERE r2_key = ?
  `).bind(errMsg.slice(0, 500), new Date().toISOString(), key).run();
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：手动触发分页批量导入历史文件
// POST /api/import
// Body: { from_time?, prefix?, cursor?, page_size? }
// ═══════════════════════════════════════════════════════════════
async function handleApiImport(request, env) {
  if (request.method !== 'POST') {
    return jsonResponse({ error: 'POST only' }, 405);
  }

  const c    = getConfig(env);
  const body = await request.json().catch(() => ({}));

  // 导入起始时间：优先用请求参数，其次用配置的 START_TIME
  const fromTime = body.from_time || c.startTime;
  const prefix   = body.prefix    || c.prefix;
  const cursor   = body.cursor    || undefined;
  // 每页文件数：最多100（Queue sendBatch 上限），默认50
  const pageSize = Math.min(parseInt(body.page_size || '50', 10), 100);

  // Safe Lag Window：导入时同样过滤太新的文件
  const safeCutoff = new Date(Date.now() - c.lagMinutes * 60 * 1000).toISOString();

  let listResult;
  try {
    listResult = await r2List(env, prefix, pageSize, cursor);
  } catch (err) {
    return jsonResponse({ error: `R2 list failed: ${err.message}` }, 500);
  }

  const toEnqueue        = [];
  let filteredByTime     = 0;  // 早于 from_time 的文件数
  let filteredByLag      = 0;  // 晚于 safeCutoff 的文件数（太新）
  let filteredAlreadyDone = 0; // 已处理的文件数

  for (const obj of listResult.objects) {
    if (obj.last_modified < fromTime) { filteredByTime++;     continue; }
    if (obj.last_modified > safeCutoff) { filteredByLag++;    continue; }

    const existing = await env.DB.prepare(
      `SELECT status FROM processed_files WHERE r2_key = ?`
    ).bind(obj.key).first();

    if (existing?.status === 'done') { filteredAlreadyDone++; continue; }

    toEnqueue.push(obj.key);
  }

  // 分批入队（每100条一批，符合 Queue sendBatch 限制）
  let queued = 0;
  for (let i = 0; i < toEnqueue.length; i += 100) {
    const batch = toEnqueue.slice(i, i + 100).map(key => ({ body: { key } }));
    await env.INGEST_QUEUE.sendBatch(batch);
    queued += batch.length;
  }

  return jsonResponse({
    from_time:          fromTime,
    safe_cutoff:        safeCutoff,
    prefix,
    listed:             listResult.objects.length,
    filtered_too_old:   filteredByTime,
    filtered_too_new:   filteredByLag,
    filtered_done:      filteredAlreadyDone,
    queued,
    truncated:          listResult.truncated,
    next_cursor:        listResult.cursor,
    note: listResult.truncated
      ? '还有更多文件，用 next_cursor 参数继续导入下一页（建议间隔30秒以上）'
      : '本批次已全部入队，无更多文件',
  });
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：系统状态
// GET /api/status
// ═══════════════════════════════════════════════════════════════
async function handleApiStatus(request, env) {
  const c = getConfig(env);

  const [total, done, processing, failed, pending, dataRange, lastDone] =
    await Promise.all([
      env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files`).first(),
      env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='done'`).first(),
      env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='processing'`).first(),
      env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='failed'`).first(),
      env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='pending'`).first(),
      env.DB.prepare(`SELECT MIN(minute_utc) AS min_t, MAX(minute_utc) AS max_t FROM bw_stats`).first(),
      env.DB.prepare(`SELECT r2_key, finished_at, line_count, file_minute_min, file_minute_max
                      FROM processed_files WHERE status='done'
                      ORDER BY finished_at DESC LIMIT 1`).first(),
    ]);

  // 计算当前 safe cutoff，供参考
  const safeCutoff = new Date(Date.now() - c.lagMinutes * 60 * 1000).toISOString();

  return jsonResponse({
    config: {
      start_time:         c.startTime,
      lag_minutes:        c.lagMinutes,
      safe_cutoff_now:    safeCutoff,
      scan_batch_size:    c.scanBatchSize,
      rate_limit_delay:   c.rateLimitDelayMs + 'ms',
      max_processing_min: c.maxProcessingMin,
      retention_days:     c.retentionDays,
      cleanup_schedule:   'daily at UTC 02:00',
    },
    files: {
      total:      total?.c      || 0,
      done:       done?.c       || 0,
      processing: processing?.c || 0,
      failed:     failed?.c     || 0,
      pending:    pending?.c    || 0,
    },
    data_range: dataRange,
    last_done:  lastDone,
    estimated_delay: `约 ${c.lagMinutes + 10}~${c.lagMinutes + 20} 分钟（LAG + Cron间隔 + 处理时间）`,
  });
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：重试失败的文件
// POST /api/retry
// ═══════════════════════════════════════════════════════════════
async function handleApiRetry(request, env) {
  if (request.method !== 'POST') {
    return jsonResponse({ error: 'POST only' }, 405);
  }

  const failed = await env.DB.prepare(
    `SELECT r2_key FROM processed_files WHERE status = 'failed' LIMIT 50`
  ).all();

  if (!failed.results.length) {
    return jsonResponse({ retried: 0, msg: 'No failed files found' });
  }

  // 重置状态为 pending，允许重新处理
  const resetStmts = failed.results.map(r =>
    env.DB.prepare(
      `UPDATE processed_files SET status='pending', error_msg=NULL, retry_count=retry_count+1 WHERE r2_key=?`
    ).bind(r.r2_key)
  );
  await env.DB.batch(resetStmts);

  // 重新入队
  const msgs = failed.results.map(r => ({ body: { key: r.r2_key } }));
  await env.INGEST_QUEUE.sendBatch(msgs);

  return jsonResponse({ retried: msgs.length, keys: failed.results.map(r => r.r2_key) });
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：查询带宽统计数据（JSON API）
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
// GET / （带 ?zone= &hours= &token= 参数）
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
<h1>🟠 CF Origin Pull Monitor</h1>
<p class="sub">
  <code>SUM(CacheResponseBytes) WHERE OriginStatus NOT IN (0,304) ÷ 60s × 8 = Mbps</code>
  &nbsp;—&nbsp;<a href="https://developers.cloudflare.com/logs/faq/common-calculations/" target="_blank">Official Ref</a>
  &nbsp;|&nbsp;Times in CST (UTC+8)
</p>

<div class="ctrl">
  <input  id="zi" placeholder="Zone hostname (blank = all zones)" value="${zone}">
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
    Source: CF Logpush → R2 (read-only) → Workers → D1 → Chart.js
    &nbsp;|&nbsp; Data lag: ~20 min &nbsp;|&nbsp; Retention: 7 days
    &nbsp;|&nbsp; Last updated: <span id="lu">—</span>
  </p>
</div>

<script>
const TOKEN = '${token}';
const HDRS  = TOKEN ? { Authorization: 'Bearer ' + TOKEN } : {};
let chartBw = null;

function toCST(minute_utc) {
  const t   = new Date(minute_utc + ':00Z');
  const cst = new Date(t.getTime() + 8 * 3600000);
  return cst.toISOString().slice(11, 16) + ' CST';
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
    document.getElementById('st').textContent = '❌ ' + e.message;
    return;
  }

  const data   = json.data || [];
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
    data: {
      labels,
      datasets: [{
        label: (zone || 'All Zones') + ' — Origin Pull Bandwidth',
        data: vals,
        borderColor: 'rgb(246,130,31)',
        backgroundColor: 'rgba(246,130,31,0.08)',
        borderWidth: 1.5,
        tension: 0.2,
        fill: true,
        pointRadius: vals.length > 300 ? 0 : 2,
        pointHoverRadius: 5,
      }]
    },
    options: {
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: { ticks: { color: '#777', maxTicksLimit: 12, maxRotation: 0 }, grid: { color: '#1a1a1a' } },
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
  } catch (e) {
    document.getElementById('st').textContent = '❌ ' + e.message;
  }
}

if ('${zone}') loadChart();
</script>
</body>
</html>`;

  return new Response(html, {
    headers: { 'Content-Type': 'text/html;charset=UTF-8' }
  });
}

// ═══════════════════════════════════════════════════════════════
// 工具函数
// ═══════════════════════════════════════════════════════════════

// 统一 JSON 响应
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type':                'application/json',
      'Access-Control-Allow-Origin': '*',
    },
  });
}

// 鉴权：支持 Authorization: Bearer <token> 或 URL ?token=
function isAuthorized(request, env) {
  const c     = getConfig(env);
  if (!c.dashToken) return true;  // 未设置 Token 则不鉴权（仅开发调试用）
  const url   = new URL(request.url);
  const auth  = request.headers.get('Authorization') || '';
  const token = auth.replace('Bearer ', '').trim() || url.searchParams.get('token') || '';
  return token === c.dashToken;
}

// ─── D1 数据生命周期清理（每日 UTC 02:00 触发）─────────────────
// 删除超过 DATA_RETENTION_DAYS 天的历史数据
// 同时清理 bw_stats（带宽统计）和 processed_files（文件处理记录）
async function cleanupOldData(env) {
  const c = getConfig(env);
  const cutoff = new Date(Date.now() - c.retentionDays * 24 * 3600 * 1000)
    .toISOString()
    .slice(0, 16);  // "2026-04-12T02:00" 格式，与 minute_utc 字段对齐

  log(env, 'info', `Cleanup: deleting data older than ${cutoff} (retention=${c.retentionDays}d)`);

  try {
    // 删除 bw_stats 中超期的分钟级带宽数据
    const r1 = await env.DB.prepare(
      `DELETE FROM bw_stats WHERE minute_utc < ?`
    ).bind(cutoff).run();

    // 删除 processed_files 中超期的文件记录
    // 用 finished_at 判断（处理完成时间），NULL 的（processing/pending 中）保留
    const cutoffFull = new Date(Date.now() - c.retentionDays * 24 * 3600 * 1000).toISOString();
    const r2 = await env.DB.prepare(
      `DELETE FROM processed_files WHERE finished_at IS NOT NULL AND finished_at < ?`
    ).bind(cutoffFull).run();

    log(env, 'info',
      `Cleanup done: bw_stats deleted ${r1.meta?.changes ?? 0} rows, ` +
      `processed_files deleted ${r2.meta?.changes ?? 0} rows`
    );
  } catch (err) {
    log(env, 'error', `Cleanup failed: ${err.message}`);
  }
}

// 休眠（用于 API 速率限制控制）
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// 日志输出
function log(env, level, msg) {
  const levels = LOG_LEVELS;
  const cfgLevel = env?.LOG_LEVEL || 'info';
  if ((levels[level] ?? 1) >= (levels[cfgLevel] ?? 1)) {
    const fn = level === 'error' ? console.error
             : level === 'warn'  ? console.warn
             : console.log;
    fn(`[${level.toUpperCase()}] ${new Date().toISOString()} ${msg}`);
  }
}
