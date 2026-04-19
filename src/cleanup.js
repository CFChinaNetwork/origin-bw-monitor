'use strict';

/**
 * D1 数据生命周期清理（每日 UTC 02:00）
 *
 * 修复 #4：processed_files 表无限增长
 *   旧逻辑仅清理 'finished_at IS NOT NULL AND finished_at < cutoff'，
 *   任何卡在 pending/processing/failed 永不完成的记录永远不会被清理。
 *   新逻辑：
 *     - 已完成（done）: 按 finished_at 清理
 *     - 未完成（pending/processing/failed）: 按 started_at 清理 + 1 天宽限期
 *
 * 修复 #17：分批删除，避免大量过期数据时 D1 单次 DELETE 超时
 *   SQLite 默认不支持 DELETE ... LIMIT，改用 WHERE rowid IN (SELECT ... LIMIT)
 */

import { getConfig } from './config.js';
import { log } from './utils.js';

export async function cleanupOldData(env) {
  const c       = getConfig(env);
  const cutoff  = new Date(Date.now() - c.retentionDays * 24 * 3600 * 1000).toISOString().slice(0, 16);
  const cutoffF = new Date(Date.now() - c.retentionDays * 24 * 3600 * 1000).toISOString();
  // 针对未完成的文件，额外多给 1 天宽限期，避免误删刚卡住的文件
  const staleCutoffF = new Date(Date.now() - (c.retentionDays + 1) * 24 * 3600 * 1000).toISOString();

  log(env, 'info', `Cleanup: retention=${c.retentionDays}d cutoff=${cutoff}`);
  try {
    const r1Count   = await deleteBatched(env,
      `DELETE FROM bw_stats WHERE rowid IN (SELECT rowid FROM bw_stats WHERE minute_utc < ? LIMIT 5000)`, [cutoff]);
    const r2DoneC   = await deleteBatched(env,
      `DELETE FROM processed_files WHERE rowid IN (SELECT rowid FROM processed_files WHERE finished_at IS NOT NULL AND finished_at < ? LIMIT 5000)`, [cutoffF]);
    const r2StaleC  = await deleteBatched(env,
      `DELETE FROM processed_files WHERE rowid IN (SELECT rowid FROM processed_files WHERE finished_at IS NULL AND started_at < ? LIMIT 5000)`, [staleCutoffF]);
    const r3Count   = await deleteBatched(env,
      `DELETE FROM alert_history WHERE rowid IN (SELECT rowid FROM alert_history WHERE alerted_at < ? LIMIT 5000)`, [cutoffF]);

    log(env, 'info',
      `Cleanup done: bw_stats=${r1Count} ` +
      `processed_files=${r2DoneC + r2StaleC} (done=${r2DoneC}, stale=${r2StaleC}) ` +
      `alert_history=${r3Count}`
    );
  } catch (err) { log(env, 'error', `Cleanup failed: ${err.message}`); }
}

// ─── 分批删除工具（#17 修复）────────────────────────────────
// 每批删 LIMIT 行，循环直到无更多可删行，防止单条 DELETE 扫描过多行超时
async function deleteBatched(env, sql, bindings, maxIterations = 100) {
  let totalDeleted = 0;
  for (let i = 0; i < maxIterations; i++) {
    const result = await env.DB.prepare(sql).bind(...bindings).run();
    const changed = result.meta?.changes ?? 0;
    totalDeleted += changed;
    if (changed === 0) break;
  }
  return totalDeleted;
}
