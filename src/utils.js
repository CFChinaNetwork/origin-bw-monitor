'use strict';

/**
 * 通用工具：日志、HTTP 响应、XSS 防御、URL 构造、鉴权
 */

import { getConfig } from './config.js';

const LOG_LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };

// 条件日志：根据 LOG_LEVEL 环境变量决定是否输出
export function log(env, level, msg) {
  if ((LOG_LEVELS[level] ?? 1) >= (LOG_LEVELS[env?.LOG_LEVEL ?? 'info'] ?? 1)) {
    const fn = level === 'error' ? console.error : level === 'warn' ? console.warn : console.log;
    fn(`[${level.toUpperCase()}] ${new Date().toISOString()} ${msg}`);
  }
}

// 统一 JSON 响应
export function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  });
}

// HTML escape：防御 XSS（用于 HTML 文本 + 属性值 context）
// 覆盖 OWASP 推荐的 5 个字符：& < > " '
export function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

// 构造 Dashboard 跳转 URL（修复 #5：URL 双 ? 拼接 bug）
// 使用 URL 对象安全处理，支持 dashUrl 本身已包含 query/fragment 的场景
export function buildDashboardUrl(dashUrl, zone) {
  try {
    const u = new URL(dashUrl);
    u.searchParams.set('zone', zone);
    u.searchParams.set('hours', '24');
    return u.toString();
  } catch {
    // dashUrl 格式异常时回退到原始字符串
    return dashUrl;
  }
}

// Dashboard 鉴权：支持 Authorization: Bearer 头或 URL ?token=
// DASHBOARD_TOKEN 未配置时不做鉴权（仅供开发调试）
export function isAuthorized(request, env) {
  const c = getConfig(env);
  if (!c.dashToken) return true;
  const url   = new URL(request.url);
  const auth  = request.headers.get('Authorization') || '';
  const token = auth.replace('Bearer ', '').trim() || url.searchParams.get('token') || '';
  return token === c.dashToken;
}
