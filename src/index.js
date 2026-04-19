'use strict';

/**
 * CF Origin Pull Bandwidth Monitor — Worker 入口
 *
 * 工作原理：
 *   R2 Event Notification（新文件落盘时触发）
 *     → bw-ingest-queue（仅传 R2 key）
 *     → Queue Consumer（流式解压解析 → 官方公式 → 写入 D1）
 *     → 告警检测（查 D1 累计带宽）
 *     → Dashboard（HTTP 查询 D1 → Chart.js 图表）
 *
 * 设计原则：
 *   - 只处理 R2 Event Notification 触发的新文件，不处理历史文件
 *   - R2 Binding 只读，绝不写入
 *   - 所有配置在 wrangler.toml，代码不含硬编码
 *
 * 回源带宽公式（官方）：
 *   SUM(CacheResponseBytes) WHERE OriginResponseStatus NOT IN (0, 304)
 *   Mbps = sum_bytes / 60s * 8bits / 1024²
 *   参考：https://developers.cloudflare.com/logs/faq/common-calculations/
 *
 * 模块组织（ESM 多文件结构）：
 *   - index.js      ← 当前文件：Worker 入口（scheduled / queue / fetch）
 *   - config.js     : 统一配置与缓存
 *   - utils.js      : 日志、XSS 防御、URL 工具、鉴权
 *   - processor.js  : R2 文件处理主流程 + 重试
 *   - alerting.js   : 告警检测 + 多渠道发送
 *   - cleanup.js    : D1 数据生命周期清理
 *   - dashboard.js  : HTTP handlers + Dashboard HTML
 */

import { processFile, recordFailure, retryPendingFiles } from './processor.js';
import { cleanupOldData } from './cleanup.js';
import { handleApiStats, handleApiStatus, handleDashboard } from './dashboard.js';
import { isAuthorized, log } from './utils.js';

export default {
  // Cron 调度：
  //   */8 * * * *   → 每 8 分钟重试 pending / 卡死的 processing 文件
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
        // 修复 #6：记录失败状态到 processed_files，累加 retry_count
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
    if (path === '/api/stats')  return handleApiStats(request, env);
    if (path === '/api/status') return handleApiStatus(request, env);
    return handleDashboard(request, env);
  },
};
