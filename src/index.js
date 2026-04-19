'use strict';

/**
 * CF Origin Pull Bandwidth Monitor
 *
 * 工作原理：
 *   R2 Event Notification（新文件落盘时触发）
 *     → bw-ingest-queue（仅传 R2 key）
 *     → Queue Consumer（流式解压解析 → 官方公式 → 写入 D1）
 *     → 告警检测（查 D1 上一分钟累计带宽）
 *     → Dashboard（HTTP 查询 D1 → Chart.js 图表）
 *
 * 设计原则：
 *   - 只处理 R2 Event Notification 触发的新文件，不处理历史文件
 *   - R2 Binding 只读，绝不写入
 *   - 所有配置在 wrangler.toml，index.js 不含任何硬编码
 *
 * 回源带宽公式（官方）：
 *   SUM(CacheResponseBytes) WHERE OriginResponseStatus NOT IN (0, 304)
 *   Mbps = sum_bytes / 60s * 8bits / 1024²
 *   参考：https://developers.cloudflare.com/logs/faq/common-calculations/
 */

const LOG_LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };

// ─── 配置（全部来自 wrangler.toml，不硬编码）─────────────────
function getConfig(env) {
  return {
    prefix:           env.LOG_PREFIX        || 'logs/',
    lagMinutes:       parseInt(env.LAG_MINUTES || '7', 10),
    retentionDays:    parseInt(env.DATA_RETENTION_DAYS || '7', 10),
    maxProcessingMin: parseInt(env.MAX_PROCESSING_MIN  || '15', 10),
    dashToken:        env.DASHBOARD_TOKEN   || '',
    logLevel:         env.LOG_LEVEL         || 'info',
    // 告警配置（留空 = 不启用）
    alertThresholdMbps:   env.ALERT_THRESHOLD_MBPS
      ? parseFloat(env.ALERT_THRESHOLD_MBPS) : null,
    alertSpikeMultiplier: env.ALERT_SPIKE_MULTIPLIER
      ? parseFloat(env.ALERT_SPIKE_MULTIPLIER) : null,
    alertCooldownMin: parseInt(env.ALERT_COOLDOWN_MIN || '30', 10),
    alertDashboardUrl: env.ALERT_DASHBOARD_URL || '',
  };
}

// ─── 主入口 ───────────────────────────────────────────────────
export default {

  // Cron 调度：
  //   */8 * * * *   → 每8分钟重试 pending 状态文件（配合 LAG=7min，总延迟约 10-15min）
  //   0 2 * * *     → 每天 UTC 02:00 清理 D1 过期数据
  async scheduled(event, env, ctx) {
    if (event.cron === '0 2 * * *') {
      ctx.waitUntil(cleanupOldData(env));
    } else {
      ctx.waitUntil(retryPendingFiles(env));
    }
  },

  // Queue Consumer：处理 R2 Event Notification 触发的新文件
  async queue(batch, env, ctx) {
    for (const msg of batch.messages) {
      // R2 Event Notification 格式：{ object: { key: "..." } }
      // 直接入队格式：{ key: "..." }
      const key = msg.body?.object?.key || msg.body?.key;
      if (!key || !key.endsWith('.gz')) { msg.ack(); continue; }
      try {
        await processFile(key, env);
        msg.ack();
      } catch (err) {
        // 修复问题 #6：记录失败状态到 processed_files，累加 retry_count
        // 超过阈值后置为 failed 并 ack（避免无限重试占用 Queue 配额）
        const maxRetries = 3;
        await recordFailure(key, err.message, maxRetries, env);
        const { retry_count } = (await env.DB.prepare(
          `SELECT retry_count FROM processed_files WHERE r2_key=?`
        ).bind(key).first()) || { retry_count: 0 };

        if (retry_count >= maxRetries) {
          log(env, 'error', `processFile failed key=${key} (final, retry=${retry_count}): ${err.message}`);
          msg.ack();  // 达到重试上限，终止消费，由 DLQ 或人工介入
        } else {
          log(env, 'error', `processFile failed key=${key} (retry=${retry_count}): ${err.message}`);
          msg.retry();
        }
      }
    }
  },

  // HTTP：Dashboard + 状态查询
  async fetch(request, env) {
    if (!isAuthorized(request, env)) {
      return new Response('Unauthorized', { status: 401 });
    }
    const path = new URL(request.url).pathname;
    if (path === '/api/stats')       return handleApiStats(request, env);
    if (path === '/api/status')      return handleApiStatus(request, env);

    return handleDashboard(request, env);
  },
};

// ═══════════════════════════════════════════════════════════════
// 处理单个 R2 文件（幂等，流式只读）
// ═══════════════════════════════════════════════════════════════
async function processFile(key, env) {
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
      // 仍在正常处理时间内，跳过避免重复处理
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
  // 幂等保证：即使此处已有部分数据写入（不会发生，此处 return 前未调用 flush），
  //          下次重试时 INSERT OR REPLACE 会按 r2_key 覆盖，不会累加
  if (skippedLag > 0) {
    await env.DB.prepare(
      `UPDATE processed_files SET status='pending', error_msg='has_lag_retry' WHERE r2_key=?`
    ).bind(key).run();
    log(env, 'info', `File has lag rows, reset pending for retry: ${key} lag=${skippedLag} ok=${lineCount}`);
    return;
  }

  await flushToD1(minuteMap, key, env);

  // 写入后：对本次写入的每个分钟查 D1 累计值做告警判断
  // 传入本次涉及的分钟集合，确保不错过任何峰值分钟
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
// 记录文件处理失败（修复问题 #6）
// 累加 retry_count，达到阈值后置 status='failed'
// 若记录不存在（消息格式异常时 processFile 未进入 INSERT 就抛错），也插入一条
// ═══════════════════════════════════════════════════════════════
async function recordFailure(r2Key, errorMsg, maxRetries, env) {
  // 截断超长错误信息，避免 D1 存储过大
  const truncated   = String(errorMsg || 'unknown').slice(0, 500);
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
// 批量写入 D1（真正幂等：INSERT OR REPLACE 按 r2_key 覆盖）
// 每个文件的贡献以 (minute_utc, zone, r2_key) 为主键独立存储
// 即使同一文件被重复处理（Queue 重试、Worker 崩溃重跑），写入结果始终一致
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
// 告警：查 D1 本次写入涉及的每个分钟的真实累计带宽
// 传入 minutesWritten（本次 processFile 写入的分钟集合），
// 对每个分钟都检查一遍，确保不错过任何峰值
// 修复问题 #8：since24h 改为按"目标分钟往前 24 小时"计算，
// 而非"当前时间往前 24 小时"，更准确地反映该分钟的历史峰值
// ═══════════════════════════════════════════════════════════════
async function checkAndAlertFromD1(minutesWritten, env) {
  const c = getConfig(env);

  for (const minute of minutesWritten) {
    // 查该分钟各 zone 的 D1 累计带宽（SUM 聚合所有 r2_key 贡献，得到真实总值）
    const rows = (await env.DB.prepare(`
      SELECT zone,
             ROUND(CAST(SUM(sum_bytes) AS REAL) / 60.0 * 8.0 / 1048576.0, 4) AS mbps
      FROM bw_stats
      WHERE minute_utc = ?
      GROUP BY zone
      ORDER BY mbps DESC
    `).bind(minute).all()).results;

    // 针对当前 minute 独立计算 24h 窗口（修复问题 #8）
    const minuteMs   = new Date(minute + ':00Z').getTime();
    const since24h   = new Date(minuteMs - 24 * 3600 * 1000).toISOString().slice(0, 16);

    // 并发处理每个 zone 的告警检测
    await Promise.allSettled(rows
      .filter(({ zone, mbps }) => zone !== 'unknown' && mbps > 0)
      .map(({ zone, mbps }) => checkZoneAlert(zone, mbps, minute, since24h, c, env))
    );
  }
}

// ── 单个 zone 的告警检测（并发安全）────────────────────────────
async function checkZoneAlert(zone, mbps, prevMinute, since24h, c, env) {
  // 告警1：固定阈值
  if (c.alertThresholdMbps !== null && mbps >= c.alertThresholdMbps) {
    const detail = { threshold: c.alertThresholdMbps, current: mbps };
    // 原子锁：INSERT WHERE NOT EXISTS，只有最先到达的 Worker 能成功写入并发告警
    const locked = await tryAcquireAlertLock('threshold', zone, prevMinute, mbps, detail, c.alertCooldownMin, env);
    if (locked) {
      await sendAlert({
        type: 'threshold', zone, minute: prevMinute, mbps, detail,
        title:   '🚨 CF 回源带宽超阈值告警',
        summary: `当前带宽 **${mbps.toFixed(3)} Mbps** 超过设定阈值 **${c.alertThresholdMbps} Mbps**`,
        dashUrl: c.alertDashboardUrl,
      }, env);
    }
  }

  // 告警2：突增检测
  if (c.alertSpikeMultiplier !== null) {
    // 先按 minute 聚合 SUM（所有 r2_key 贡献），再取 MAX，得到真实历史峰值
    const row = await env.DB.prepare(`
      SELECT MAX(mbps) AS peak FROM (
        SELECT ROUND(CAST(SUM(sum_bytes) AS REAL)/60.0*8.0/1048576.0,4) AS mbps
        FROM bw_stats
        WHERE zone = ? AND minute_utc >= ? AND minute_utc < ?
        GROUP BY minute_utc
      )
    `).bind(zone, since24h, prevMinute).first();

    const peak = row?.peak || 0;
    if (peak > 0 && mbps >= peak * c.alertSpikeMultiplier) {
      const ratio  = (mbps / peak).toFixed(1);
      const detail = { multiplier: c.alertSpikeMultiplier, current: mbps, historicPeak: peak, ratio };
      const locked = await tryAcquireAlertLock('spike', zone, prevMinute, mbps, detail, c.alertCooldownMin, env);
      if (locked) {
        await sendAlert({
          type: 'spike', zone, minute: prevMinute, mbps, detail,
          title:   '⚡ CF 回源带宽突增告警',
          summary: `当前带宽 **${mbps.toFixed(3)} Mbps**，是过去24小时最高值 **${peak.toFixed(3)} Mbps** 的 **${ratio} 倍**`,
          dashUrl: c.alertDashboardUrl,
        }, env);
      }
    }
  }
}

// ── 告警原子锁（修复问题 #7：防止并发重复告警）────────────────
// 旧逻辑：checkCooldown → sendAlert → recordAlert 三步非原子，并发 Worker 会各自通过检查后都发送
// 新逻辑：INSERT ... WHERE NOT EXISTS 一次性检查+写入，SQLite/D1 保证语句级原子性
// 只有最先到达的 Worker 能 INSERT 成功（changes=1），其他 Worker 被 WHERE NOT EXISTS 阻止（changes=0）
// 权衡：alert_history 先于告警发送写入，若发送失败告警会丢失一个冷却周期（at-most-once，可接受）
async function tryAcquireAlertLock(alertType, zone, minute, mbps, detail, cooldownMin, env) {
  const sinceIso = new Date(Date.now() - cooldownMin * 60 * 1000).toISOString();
  const nowIso   = new Date().toISOString();
  const result = await env.DB.prepare(`
    INSERT INTO alert_history (alert_type, zone, minute_utc, mbps, detail, alerted_at)
    SELECT ?, ?, ?, ?, ?, ?
    WHERE NOT EXISTS (
      SELECT 1 FROM alert_history
      WHERE alert_type = ? AND zone = ? AND alerted_at >= ?
    )
  `).bind(
    alertType, zone, minute, mbps, JSON.stringify(detail), nowIso,
    alertType, zone, sinceIso
  ).run();
  return (result.meta?.changes ?? 0) === 1;
}

// ── 发送告警（企业微信 / 钉钉 / 飞书，任意组合）──────────────
async function sendAlert({ type, zone, minute, mbps, detail, title, summary, dashUrl }, env) {
  const t      = new Date(minute + ':00Z');
  const cst    = new Date(t.getTime() + 8 * 3600000);
  const iso    = cst.toISOString();
  const timeStr = iso.slice(0, 10) + ' ' + iso.slice(11, 16) + ' UTC+8';

  const tasks = [];
  if (env.ALERT_WEBHOOK_WECOM)    tasks.push(sendWeCom(env.ALERT_WEBHOOK_WECOM, { title, summary, zone, timeStr, mbps, detail, dashUrl }));
  if (env.ALERT_WEBHOOK_DINGTALK) tasks.push(sendDingTalk(env.ALERT_WEBHOOK_DINGTALK, { title, summary, zone, timeStr, mbps, detail, dashUrl }));
  if (env.ALERT_WEBHOOK_FEISHU)   tasks.push(sendFeishu(env.ALERT_WEBHOOK_FEISHU, { title, summary, zone, timeStr, mbps, detail, dashUrl }));
  if (tasks.length === 0) return;

  const results = await Promise.allSettled(tasks);
  results.forEach((r, i) => {
    if (r.status === 'rejected') log(env, 'warn', `Alert channel ${i} failed: ${r.reason?.message}`);
  });
}

// ── 企业微信 ──────────────────────────────────────────────────
async function sendWeCom(url, { title, zone, timeStr, mbps, detail, dashUrl }) {
  let content = `## ${title}\n\n`;
  content += `> **域名：** ${zone}\n`;
  content += `> **时间：** ${timeStr}\n`;
  content += `> **当前带宽：** <font color="warning">${mbps.toFixed(3)} Mbps</font>\n`;
  if (detail.threshold) content += `> **设定阈值：** ${detail.threshold} Mbps\n`;
  if (detail.historicPeak) {
    content += `> **24h历史峰值：** ${detail.historicPeak.toFixed(3)} Mbps\n`;
    content += `> **当前倍数：** <font color="warning">${detail.ratio} 倍</font>\n`;
  }
  if (dashUrl) content += `\n[查看监控面板](${buildDashboardUrl(dashUrl, zone)})`;
  await fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ msgtype: 'markdown', markdown: { content } }),
  });
}

// ── 钉钉 ──────────────────────────────────────────────────────
async function sendDingTalk(url, { title, zone, timeStr, mbps, detail, dashUrl }) {
  let text = `## ${title}\n\n`;
  text += `**域名：** ${zone}  \n**时间：** ${timeStr}  \n**当前带宽：** ${mbps.toFixed(3)} Mbps  \n`;
  if (detail.threshold) text += `**设定阈值：** ${detail.threshold} Mbps  \n`;
  if (detail.historicPeak) {
    text += `**24h历史峰值：** ${detail.historicPeak.toFixed(3)} Mbps  \n`;
    text += `**当前倍数：** ${detail.ratio} 倍  \n`;
  }
  const body = {
    msgtype: 'actionCard',
    actionCard: {
      title, text, btnOrientation: '0',
      btns: dashUrl ? [{ title: '查看监控面板', actionURL: buildDashboardUrl(dashUrl, zone) }] : [],
    },
  };
  await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
}

// ── 飞书 ──────────────────────────────────────────────────────
async function sendFeishu(url, { title, zone, timeStr, mbps, detail, dashUrl }) {
  const fields = [
    { is_short: true, text: { tag: 'lark_md', content: `**域名**\n${zone}` } },
    { is_short: true, text: { tag: 'lark_md', content: `**时间**\n${timeStr}` } },
    { is_short: true, text: { tag: 'lark_md', content: `**当前带宽**\n${mbps.toFixed(3)} Mbps` } },
  ];
  if (detail.threshold) fields.push({ is_short: true, text: { tag: 'lark_md', content: `**设定阈值**\n${detail.threshold} Mbps` } });
  if (detail.historicPeak) {
    fields.push({ is_short: true, text: { tag: 'lark_md', content: `**24h历史峰值**\n${detail.historicPeak.toFixed(3)} Mbps` } });
    fields.push({ is_short: true, text: { tag: 'lark_md', content: `**当前倍数**\n${detail.ratio} 倍` } });
  }
  const elements = [{ tag: 'div', fields }];
  if (dashUrl) elements.push({
    tag: 'action',
    actions: [{ tag: 'button', text: { tag: 'plain_text', content: '查看监控面板' },
      url: buildDashboardUrl(dashUrl, zone), type: 'default' }],
  });
  await fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      msg_type: 'interactive',
      card: { header: { title: { tag: 'plain_text', content: title }, template: 'red' }, elements },
    }),
  });
}

// ═══════════════════════════════════════════════════════════════
// Cron 每 8 分钟：重试需要恢复的文件
// 两类候选：
//   1. pending 状态 - 之前因 lag 窗口被推迟，现在 lag 窗口已过（原设计）
//   2. processing 状态且 started_at 超过 MAX_PROCESSING_MIN
//      = Worker 崩溃/超时后卡死的文件，不重试就永远丢失（Bug #2）
// ═══════════════════════════════════════════════════════════════
async function retryPendingFiles(env) {
  const c = getConfig(env);

  const nowMs = Date.now();
  // pending 重试条件：被置为 pending 的时间早于 lag 窗口（lag 已过，可安全重试）
  const lagCutoff   = new Date(nowMs - c.lagMinutes * 60 * 1000).toISOString();
  // processing 卡死判定：started_at 已超过 maxProcessingMin，Worker 肯定已经不在处理
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

// ═══════════════════════════════════════════════════════════════
// D1 数据生命周期清理（每日 UTC 02:00）
// 修复问题 #4：processed_files 表无限增长
//   旧逻辑仅清理 'finished_at IS NOT NULL AND finished_at < cutoff'，
//   任何卡在 pending/processing/failed 永不完成的记录永远不会被清理。
//   新逻辑：
//     - 已完成（done）: 按 finished_at 清理
//     - 未完成（pending/processing/failed）: 按 started_at 清理，但给更长的宽限期
//       避免误删"刚卡住但还可能恢复"的文件
// ═══════════════════════════════════════════════════════════════
async function cleanupOldData(env) {
  const c       = getConfig(env);
  // 主 cutoff：按保留天数
  const cutoff  = new Date(Date.now() - c.retentionDays * 24 * 3600 * 1000).toISOString().slice(0, 16);
  const cutoffF = new Date(Date.now() - c.retentionDays * 24 * 3600 * 1000).toISOString();
  // 针对未完成的文件，额外多给 1 天宽限期，避免误删刚卡住的文件
  const staleCutoffF = new Date(Date.now() - (c.retentionDays + 1) * 24 * 3600 * 1000).toISOString();

  log(env, 'info', `Cleanup: retention=${c.retentionDays}d cutoff=${cutoff}`);
  try {
    const r1 = await env.DB.prepare(`DELETE FROM bw_stats WHERE minute_utc < ?`).bind(cutoff).run();

    // 清理 processed_files：
    //   1) 已完成的按 finished_at（原逻辑，保留）
    //   2) 未完成但 started_at 过于久远的（修复：原逻辑从不清理，导致表无限增长）
    const r2Done = await env.DB.prepare(
      `DELETE FROM processed_files WHERE finished_at IS NOT NULL AND finished_at < ?`
    ).bind(cutoffF).run();
    const r2Stale = await env.DB.prepare(
      `DELETE FROM processed_files WHERE finished_at IS NULL AND started_at < ?`
    ).bind(staleCutoffF).run();

    const r3 = await env.DB.prepare(`DELETE FROM alert_history WHERE alerted_at < ?`).bind(cutoffF).run();
    log(env, 'info',
      `Cleanup done: bw_stats=${r1.meta?.changes??0} ` +
      `processed_files=${(r2Done.meta?.changes??0) + (r2Stale.meta?.changes??0)} ` +
      `(done=${r2Done.meta?.changes??0}, stale=${r2Stale.meta?.changes??0}) ` +
      `alert_history=${r3.meta?.changes??0}`
    );
  } catch (err) { log(env, 'error', `Cleanup failed: ${err.message}`); }
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：带宽统计数据 API
// ═══════════════════════════════════════════════════════════════
async function handleApiStats(request, env) {
  const url   = new URL(request.url);
  const zone  = url.searchParams.get('zone')  || '%';
  const hours = parseInt(url.searchParams.get('hours') || '24', 10);

  // 时间范围：从 hours 小时前 到 现在-lag（已处理的最新分钟）
  const c       = getConfig(env);
  const nowMs   = Date.now();
  const endMs   = nowMs - c.lagMinutes * 60 * 1000;  // 最新可显示时间点
  const startMs = nowMs - hours * 3600 * 1000;        // 选定时间范围起点

  const since  = new Date(startMs).toISOString().slice(0, 16);
  const endMin = new Date(endMs - 60 * 1000).toISOString().slice(0, 16); // 含最新完整分钟

  // 按分钟聚合所有 zone（跨 zone 求和），确保每个时间点只有一条数据
  // 这样无论查单个 zone 还是全部 zone，图表始终只有一条线，标签清晰
  const rows = await env.DB.prepare(`
    SELECT minute_utc,
           SUM(sum_bytes) AS sum_bytes,
           ROUND(CAST(SUM(sum_bytes) AS REAL)/60.0*8.0/1048576.0, 4) AS mbps
    FROM bw_stats
    WHERE minute_utc >= ? AND minute_utc <= ? AND zone LIKE ?
    GROUP BY minute_utc
    ORDER BY minute_utc ASC
  `).bind(since, endMin, zone).all();

  // 补首尾端点，确保横轴覆盖完整所选时间范围
  const dataMap = new Map(rows.results.map(r => [r.minute_utc, r]));
  const finalData = [...rows.results];

  if (!dataMap.has(since)) {
    finalData.unshift({ minute_utc: since, sum_bytes: 0, mbps: 0 });
  }
  if (!dataMap.has(endMin) && endMin > since) {
    finalData.push({ minute_utc: endMin, sum_bytes: 0, mbps: 0 });
  }

  return jsonResponse({
    formula: 'SUM(CacheResponseBytes) WHERE OriginStatus NOT IN (0,304) / 60s * 8bits / 1024^2 = Mbps',
    ref:     'https://developers.cloudflare.com/logs/faq/common-calculations/',
    since, end: endMin, zone,
    count: rows.results.length,
    data:  finalData,
  });
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：系统状态
// ═══════════════════════════════════════════════════════════════
async function handleApiStatus(request, env) {
  const c = getConfig(env);
  const [total, done, processing, failed, dataRange, lastDone, alertCount] = await Promise.all([
    env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files`).first(),
    env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='done'`).first(),
    env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='processing'`).first(),
    env.DB.prepare(`SELECT COUNT(*) AS c FROM processed_files WHERE status='failed'`).first(),
    env.DB.prepare(`SELECT MIN(minute_utc) AS min_t, MAX(minute_utc) AS max_t FROM bw_stats`).first(),
    env.DB.prepare(`SELECT r2_key, finished_at FROM processed_files WHERE status='done' ORDER BY finished_at DESC LIMIT 1`).first(),
    env.DB.prepare(`SELECT COUNT(*) AS c FROM alert_history`).first(),
  ]);

  return jsonResponse({
    config: {
      lag_minutes:        c.lagMinutes,
      retention_days:     c.retentionDays,
      alert_threshold:    c.alertThresholdMbps   ?? 'disabled',
      alert_spike:        c.alertSpikeMultiplier  ?? 'disabled',
      alert_cooldown_min: c.alertCooldownMin,
    },
    files: {
      total:      total?.c      || 0,
      done:       done?.c       || 0,
      processing: processing?.c || 0,
      failed:     failed?.c     || 0,
    },
    data_range:   dataRange,
    last_done:    lastDone,
    alert_count:  alertCount?.c || 0,
  });
}



// ═══════════════════════════════════════════════════════════════
// HTTP Handler：Dashboard 图表页面
// ═══════════════════════════════════════════════════════════════
async function handleDashboard(request, env) {
  const url   = new URL(request.url);
  // XSS 防御：所有用户输入都经 escape 或白名单验证后才嵌入 HTML
  const rawZone  = url.searchParams.get('zone')  || '';
  const rawHours = url.searchParams.get('hours') || '24';
  // hours 白名单（只允许预定义的 5 个选项）
  const allowedHours = ['1', '6', '24', '72', '168'];
  const hours = allowedHours.includes(rawHours) ? rawHours : '24';
  // zone：HTML escape 后才能安全嵌入 HTML 属性
  const zone  = escapeHtml(rawZone);
  // token：完全不嵌入 HTML，改由客户端 JS 从 URL 参数动态读取（见 <script> 内 readToken()）

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
.btn:hover{background:#ff9a3c}.btn-sec{background:#1a1a1a;color:#aaa}
.cards{display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.card{background:#111;border:1px solid #1e1e1e;border-radius:6px;padding:10px 16px;min-width:130px}
.card-l{font-size:11px;color:#666;margin-bottom:4px}.card-v{font-size:20px;font-weight:700;color:#f6821f}
.wrap{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:18px}
.foot{font-size:11px;color:#333;margin-top:12px;line-height:1.6}
#st{font-size:12px;color:#666;margin-left:8px}
</style>
</head>
<body>
<h1>🟠 CF Origin Pull Bandwidth Monitor</h1>
<p class="sub">
  <code>SUM(CacheResponseBytes) WHERE OriginStatus NOT IN (0,304) ÷ 60s × 8 = Mbps</code>
  &nbsp;—&nbsp;<a href="https://developers.cloudflare.com/logs/faq/common-calculations/" target="_blank">Official Ref</a>
  &nbsp;|&nbsp;Times in UTC+8
</p>
<div class="ctrl">
  <input id="zi" placeholder="Zone hostname (blank = all)" value="${zone}">
  <select id="hs">
    <option value="1"   ${hours==='1'  ?'selected':''}>Last 1 hour</option>
    <option value="6"   ${hours==='6'  ?'selected':''}>Last 6 hours</option>
    <option value="24"  ${hours==='24' ?'selected':''}>Last 24 hours</option>
    <option value="72"  ${hours==='72' ?'selected':''}>Last 3 days</option>
    <option value="168" ${hours==='168'?'selected':''}>Last 7 days</option>
  </select>
  <button class="btn" onclick="loadChart()">Load</button>
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
    &nbsp;|&nbsp;Data lag: ~10 min
    &nbsp;|&nbsp;Retention: 7 days
    &nbsp;|&nbsp;Auto-refresh: every 60s
    &nbsp;|&nbsp;Last updated: <span id="lu">—</span>
  </p>
</div>
<script>
// Token 不从服务端模板注入（防 XSS），改为客户端从当前 URL 读取
// 服务端模板里绝不拼接用户可控字符串到 JS context
const TOKEN=(new URLSearchParams(location.search)).get('token')||'';
const HDRS=TOKEN?{Authorization:'Bearer '+TOKEN}:{};
let chart=null;
function toUTC8(m){
  const t=new Date(m+':00Z'),c=new Date(t.getTime()+8*3600000),i=c.toISOString();
  return i.slice(0,10)+' '+i.slice(11,16)+' UTC+8';
}
async function loadChart(){
  const z=document.getElementById('zi').value.trim();
  const h=document.getElementById('hs').value;
  document.getElementById('st').textContent='Loading...';
  let j;
  try{
    const p=new URLSearchParams({hours:h});if(z)p.set('zone',z);
    const r=await fetch('/api/stats?'+p,{headers:HDRS});
    if(!r.ok)throw new Error('HTTP '+r.status);
    j=await r.json();
  }catch(e){document.getElementById('st').textContent='❌ '+e.message;return;}
  const d=j.data||[];
  // 根据实际数据跨度计算标注间隔
  // 用 API 返回的 since/end 计算，不依赖 DOM
  const spanHours=j.since&&j.end
    ?(new Date(j.end+':00Z')-new Date(j.since+':00Z'))/3600000
    :24;
  const intervalMin=spanHours<=1?10:spanHours<=6?30:spanHours<=24?120:spanHours<=72?360:720;
  // 生成标签：首尾强制显示，中间按intervalMin整点显示，其余留空
  function fmtUTC8(minute_utc){
    const t=new Date(minute_utc+':00Z');
    const c=new Date(t.getTime()+8*3600000),i=c.toISOString();
    return i.slice(0,10)+' '+i.slice(11,16)+' UTC+8';
  }
  const labels=d.map((x,idx)=>{
    if(idx===0||idx===d.length-1)return fmtUTC8(x.minute_utc); // 首尾必显示
    const t=new Date(x.minute_utc+':00Z');
    const c=new Date(t.getTime()+8*3600000);
    const totalMin=c.getUTCHours()*60+c.getUTCMinutes();
    return totalMin%intervalMin===0?fmtUTC8(x.minute_utc):'';
  });
  const vals=d.map(x=>x.mbps);
  const nz=vals.filter(v=>v>0);
  document.getElementById('cPk').textContent=(nz.length?Math.max(...vals).toFixed(3):0)+' Mbps';
  document.getElementById('cAv').textContent=(nz.length?(nz.reduce((a,b)=>a+b,0)/nz.length).toFixed(3):0)+' Mbps';
  document.getElementById('cTo').textContent=(d.reduce((a,x)=>a+(x.sum_bytes||0),0)/1073741824).toFixed(4)+' GB';
  document.getElementById('cPt').textContent=d.length;
  document.getElementById('lu').textContent=new Date().toLocaleString('zh-CN');
  document.getElementById('st').textContent=d.length+' points | zone: '+(z||'all');
  const ctx=document.getElementById('chartBw').getContext('2d');
  if(chart)chart.destroy();
  chart=new Chart(ctx,{
    type:'line',
    data:{labels,datasets:[{
      label:(z||'All Zones')+' — Origin Pull Bandwidth',
      data:vals,borderColor:'rgb(246,130,31)',backgroundColor:'rgba(246,130,31,0.08)',
      borderWidth:1.5,tension:0.2,fill:true,
      pointRadius:d.length>300?0:2,pointHoverRadius:5,
    }]},
    options:{
      responsive:true,interaction:{mode:'index',intersect:false},
      scales:{
        x:{ticks:{color:'#777',maxRotation:0,autoSkip:false,
           callback:function(value){
             const label=this.getLabelForValue(value);
             return label||null;
           }},grid:{color:'#1a1a1a'}},
        y:{ticks:{color:'#777',callback:v=>v+' Mbps'},grid:{color:'#1a1a1a'},beginAtZero:true}
      },
      plugins:{
        legend:{labels:{color:'#bbb',font:{size:12}}},
        tooltip:{callbacks:{
          title:function(items){
            // tooltip 标题显示当前数据点的 UTC+8 时间
            const idx=items[0]?.dataIndex;
            if(idx==null||!d[idx])return '';
            return fmtUTC8(d[idx].minute_utc);
          },
          label:c=>[
            'Bandwidth: '+c.parsed.y+' Mbps',
            'Sum: '+((d[c.dataIndex]?.sum_bytes||0)/1048576).toFixed(2)+' MB',
          ]
        }}
      }
    }
  });
}
async function loadStatus(){
  try{
    const r=await fetch('/api/status',{headers:HDRS});const j=await r.json();
    const cst=s=>s?new Date(new Date(s+':00Z').getTime()+8*3600000).toISOString().slice(0,16).replace('T',' ')+' UTC+8':'N/A';
    document.getElementById('st').textContent=
      'done='+j.files.done+' proc='+j.files.processing+' fail='+j.files.failed+
      ' | alerts='+j.alert_count+
      ' | data: '+cst(j.data_range?.min_t)+' ~ '+cst(j.data_range?.max_t);
  }catch(e){document.getElementById('st').textContent='❌ '+e.message;}
}
// 自动刷新：每60秒重新加载图表数据
let autoRefreshTimer = null;
function startAutoRefresh() {
  if (autoRefreshTimer) clearInterval(autoRefreshTimer);
  autoRefreshTimer = setInterval(() => {
    loadChart();
  }, 60000);
}

// 页面加载时自动加载图表并启动自动刷新
loadChart().then(() => startAutoRefresh());
</script>
</body></html>`;

  // CSP：浏览器层兜底防御，即使有注入也无法执行外部/内联任意脚本
  // - default-src 'self': 默认只允许本站资源
  // - script-src: 允许 Chart.js CDN + 本模板自身的内联脚本（通过 'unsafe-inline' 有妥协，后续迁移到 nonce 或独立 .js 文件）
  // - style-src: 内联 style 标签需 'unsafe-inline'
  // - img-src 'self' data: : 允许本站图片和 data URI
  // - connect-src: API 请求限制在本站
  const csp = [
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",
    "style-src 'self' 'unsafe-inline'",
    "img-src 'self' data:",
    "connect-src 'self'",
    "frame-ancestors 'none'",
    "base-uri 'self'",
  ].join('; ');
  return new Response(html, {
    headers: {
      'Content-Type': 'text/html;charset=UTF-8',
      'Content-Security-Policy': csp,
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'no-referrer',
    },
  });
}

// ─── 工具函数 ─────────────────────────────────────────────────
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  });
}

// HTML escape：防御 XSS（用于 HTML 文本 + 属性值 context）
// 覆盖 OWASP 推荐的 5 个字符：& < > " '
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

// 构造 Dashboard 跳转 URL（修复问题 #5）
// 使用 URL 对象安全处理，支持 dashUrl 本身已包含 query/fragment 的场景
// 例如用户配置了 https://xxx.workers.dev/?token=abc 也能正确追加 zone/hours 参数
function buildDashboardUrl(dashUrl, zone) {
  try {
    const u = new URL(dashUrl);
    u.searchParams.set('zone', zone);
    u.searchParams.set('hours', '24');
    return u.toString();
  } catch {
    // dashUrl 格式异常时回退到原始字符串（至少能显示）
    return dashUrl;
  }
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
