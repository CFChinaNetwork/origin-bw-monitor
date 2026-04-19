'use strict';

/**
 * 配置模块：所有环境变量 / wrangler.toml 配置的统一入口
 *
 * 修复 #15：使用 WeakMap 缓存，避免每次 HTTP/Queue 调用都 parseInt/parseFloat
 * Worker 冷启动后 env 对象引用不变，可安全做 WeakMap 缓存
 */

const _configCache = new WeakMap();

export function getConfig(env) {
  const cached = _configCache.get(env);
  if (cached) return cached;
  const config = _buildConfig(env);
  _configCache.set(env, config);
  return config;
}

function _buildConfig(env) {
  return {
    prefix:           env.LOG_PREFIX        || 'logs/',
    lagMinutes:       parseInt(env.LAG_MINUTES || '7', 10),
    retentionDays:    parseInt(env.DATA_RETENTION_DAYS || '7', 10),
    maxProcessingMin: parseInt(env.MAX_PROCESSING_MIN  || '15', 10),
    dashToken:        env.DASHBOARD_TOKEN   || '',
    logLevel:         env.LOG_LEVEL         || 'info',
    // Dashboard 显示时区偏移（小时）：默认 UTC+8（中国），可配置为其他时区
    // 例如 UTC-8（西八区）= -8，UTC+0 = 0，UTC+9（东京/首尔）= 9
    displayTzOffsetHours: parseInt(env.DISPLAY_TZ_OFFSET_HOURS || '8', 10),
    // 告警配置（留空 = 不启用）
    alertThresholdMbps:   env.ALERT_THRESHOLD_MBPS
      ? parseFloat(env.ALERT_THRESHOLD_MBPS) : null,
    alertSpikeMultiplier: env.ALERT_SPIKE_MULTIPLIER
      ? parseFloat(env.ALERT_SPIKE_MULTIPLIER) : null,
    alertCooldownMin: parseInt(env.ALERT_COOLDOWN_MIN || '30', 10),
    alertDashboardUrl: env.ALERT_DASHBOARD_URL || '',
  };
}
