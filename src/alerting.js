'use strict';

/**
 * 告警模块：检测 + 原子锁 + 多渠道发送（企微/钉钉/飞书）
 *
 * 关键设计决策：
 *   - #7 并发安全：tryAcquireAlertLock 用 INSERT ... WHERE NOT EXISTS 原子写入
 *   - #8 时间精度：24h 窗口按目标 minute 往前推，而非当前时间
 *   - #13 可配置时区：DISPLAY_TZ_OFFSET_HOURS 控制告警消息里的时区
 */

import { getConfig } from './config.js';
import { log, buildDashboardUrl } from './utils.js';

/**
 * 对本次写入涉及的每个分钟进行告警检查
 * 传入 minutesWritten（本次 processFile 写入的分钟集合）
 */
export async function checkAndAlertFromD1(minutesWritten, env) {
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

    // 针对当前 minute 独立计算 24h 窗口（修复 #8）
    const minuteMs = new Date(minute + ':00Z').getTime();
    const since24h = new Date(minuteMs - 24 * 3600 * 1000).toISOString().slice(0, 16);

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

// ── 告警原子锁（修复 #7：防止并发重复告警）──────────────────
// INSERT ... WHERE NOT EXISTS 一次性检查+写入，SQLite/D1 保证语句级原子性
// 只有最先到达的 Worker 能 INSERT 成功（changes=1），其他被阻止（changes=0）
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

// ── 发送告警（企业微信 / 钉钉 / 飞书，任意组合）─────────────────
async function sendAlert({ type, zone, minute, mbps, detail, title, summary, dashUrl }, env) {
  // 修复 #13：时区可配置（通过 DISPLAY_TZ_OFFSET_HOURS 环境变量）
  const c       = getConfig(env);
  const tzHours = c.displayTzOffsetHours;
  const tzLabel = `UTC${tzHours >= 0 ? '+' : ''}${tzHours}`;
  const t       = new Date(minute + ':00Z');
  const local   = new Date(t.getTime() + tzHours * 3600000);
  const iso     = local.toISOString();
  const timeStr = iso.slice(0, 10) + ' ' + iso.slice(11, 16) + ' ' + tzLabel;

  const tasks = [];
  if (env.ALERT_WEBHOOK_WECOM)    tasks.push(sendWeCom(env.ALERT_WEBHOOK_WECOM,       { title, summary, zone, timeStr, mbps, detail, dashUrl }));
  if (env.ALERT_WEBHOOK_DINGTALK) tasks.push(sendDingTalk(env.ALERT_WEBHOOK_DINGTALK, { title, summary, zone, timeStr, mbps, detail, dashUrl }));
  if (env.ALERT_WEBHOOK_FEISHU)   tasks.push(sendFeishu(env.ALERT_WEBHOOK_FEISHU,     { title, summary, zone, timeStr, mbps, detail, dashUrl }));
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
