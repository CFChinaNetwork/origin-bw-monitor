'use strict';

/**
 * Dashboard 模块：HTTP handlers + 前端 HTML
 *
 * - handleApiStats:  带宽统计 API（支持 zone 过滤，精确匹配，#10/#11 修复）
 * - handleApiStatus: 系统状态 API
 * - handleDashboard: 渲染 Dashboard HTML（#12/#13/#14/#16 修复）
 */

import { getConfig } from './config.js';
import { jsonResponse, escapeHtml } from './utils.js';

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：带宽统计数据 API
// ═══════════════════════════════════════════════════════════════
export async function handleApiStats(request, env) {
  const url    = new URL(request.url);
  const zoneIn = url.searchParams.get('zone');
  // hours 白名单验证（防止超大值导致性能问题）
  const rawHours   = url.searchParams.get('hours');
  const allowedHrs = [1, 6, 24, 72, 168];
  const hours      = allowedHrs.includes(parseInt(rawHours, 10)) ? parseInt(rawHours, 10) : 24;

  // 修复 #11：时间范围 off-by-one
  // 显式向下取整到完整分钟边界，语义比 "减60秒" 更清晰
  const c       = getConfig(env);
  const nowMs   = Date.now();
  const endMs   = nowMs - c.lagMinutes * 60 * 1000;
  const startMs = nowMs - hours * 3600 * 1000;

  const since   = new Date(startMs).toISOString().slice(0, 16);
  const endDate = new Date(endMs);
  endDate.setSeconds(0, 0);
  endDate.setMinutes(endDate.getMinutes() - 1);
  const endMin  = endDate.toISOString().slice(0, 16);

  // 修复 #10：zone 参数用精确匹配（=），防止 %/_ 被当作 LIKE 通配符
  // 未传 zone 或空串 = 查询全部 zone
  let query, bindings;
  if (zoneIn && zoneIn.trim() !== '') {
    query = `
      SELECT minute_utc,
             SUM(sum_bytes) AS sum_bytes,
             ROUND(CAST(SUM(sum_bytes) AS REAL)/60.0*8.0/1048576.0, 4) AS mbps
      FROM bw_stats
      WHERE minute_utc >= ? AND minute_utc <= ? AND zone = ?
      GROUP BY minute_utc
      ORDER BY minute_utc ASC
    `;
    bindings = [since, endMin, zoneIn];
  } else {
    query = `
      SELECT minute_utc,
             SUM(sum_bytes) AS sum_bytes,
             ROUND(CAST(SUM(sum_bytes) AS REAL)/60.0*8.0/1048576.0, 4) AS mbps
      FROM bw_stats
      WHERE minute_utc >= ? AND minute_utc <= ?
      GROUP BY minute_utc
      ORDER BY minute_utc ASC
    `;
    bindings = [since, endMin];
  }
  const rows = await env.DB.prepare(query).bind(...bindings).all();

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
    since, end: endMin, zone: zoneIn || '(all)',
    count: rows.results.length,
    data:  finalData,
  });
}

// ═══════════════════════════════════════════════════════════════
// HTTP Handler：系统状态
// ═══════════════════════════════════════════════════════════════
export async function handleApiStatus(request, env) {
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
export async function handleDashboard(request, env) {
  const url = new URL(request.url);
  // XSS 防御：所有用户输入都经 escape 或白名单验证后才嵌入 HTML
  const rawZone  = url.searchParams.get('zone')  || '';
  const rawHours = url.searchParams.get('hours') || '24';
  const allowedHours = ['1', '6', '24', '72', '168'];
  const hours = allowedHours.includes(rawHours) ? rawHours : '24';
  const zone  = escapeHtml(rawZone);

  // 修复 #12 / #13：从配置读取保留天数和显示时区
  const c             = getConfig(env);
  const retentionDays = c.retentionDays;
  const tzHours       = c.displayTzOffsetHours;
  const tzLabel       = `UTC${tzHours >= 0 ? '+' : ''}${tzHours}`;
  const tzOffsetMs    = tzHours * 3600 * 1000;

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CF Origin Pull Bandwidth Monitor</title>
<!-- #14 修复：Chart.js CDN fallback 链。
     主 CDN: jsDelivr (国际)
     备用 1: unpkg (国际)
     备用 2: cdnjs (Cloudflare, 国内可达)
     加载失败时依次降级，任一成功即可渲染图表 -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"
  onerror="this.onerror=null;var s=document.createElement('script');s.src='https://unpkg.com/chart.js@4.4.0/dist/chart.umd.min.js';s.onerror=function(){var t=document.createElement('script');t.src='https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js';document.head.appendChild(t);};document.head.appendChild(s);"></script>
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
  &nbsp;|&nbsp;Times in ${tzLabel}
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
    &nbsp;|&nbsp;Retention: ${retentionDays} days
    &nbsp;|&nbsp;Auto-refresh: every 60s
    &nbsp;|&nbsp;Last updated: <span id="lu">—</span>
  </p>
</div>
<script>
// Token 不从服务端模板注入（防 XSS），改为客户端从当前 URL 读取
const TOKEN=(new URLSearchParams(location.search)).get('token')||'';
const HDRS=TOKEN?{Authorization:'Bearer '+TOKEN}:{};
// 时区配置由服务端注入（#13 修复：不再硬编码 UTC+8）
const TZ_OFFSET_MS=${tzOffsetMs};
const TZ_LABEL=${JSON.stringify(tzLabel)};
let chart=null;
function toLocalTz(m){
  const t=new Date(m+':00Z'),c=new Date(t.getTime()+TZ_OFFSET_MS),i=c.toISOString();
  return i.slice(0,10)+' '+i.slice(11,16)+' '+TZ_LABEL;
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
  const spanHours=j.since&&j.end
    ?(new Date(j.end+':00Z')-new Date(j.since+':00Z'))/3600000
    :24;
  const intervalMin=spanHours<=1?10:spanHours<=6?30:spanHours<=24?120:spanHours<=72?360:720;
  const labels=d.map((x,idx)=>{
    if(idx===0||idx===d.length-1)return toLocalTz(x.minute_utc);
    const t=new Date(x.minute_utc+':00Z');
    const c=new Date(t.getTime()+TZ_OFFSET_MS);
    const totalMin=c.getUTCHours()*60+c.getUTCMinutes();
    return totalMin%intervalMin===0?toLocalTz(x.minute_utc):'';
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
            const idx=items[0]?.dataIndex;
            if(idx==null||!d[idx])return '';
            return toLocalTz(d[idx].minute_utc);
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
    const cst=s=>s?new Date(new Date(s+':00Z').getTime()+TZ_OFFSET_MS).toISOString().slice(0,16).replace('T',' ')+' '+TZ_LABEL:'N/A';
    document.getElementById('st').textContent=
      'done='+j.files.done+' proc='+j.files.processing+' fail='+j.files.failed+
      ' | alerts='+j.alert_count+
      ' | data: '+cst(j.data_range?.min_t)+' ~ '+cst(j.data_range?.max_t);
  }catch(e){document.getElementById('st').textContent='❌ '+e.message;}
}
// 自动刷新：每60秒重新加载图表数据（修复 #16：页面不可见时暂停刷新）
let autoRefreshTimer = null;
function startAutoRefresh() {
  if (autoRefreshTimer) clearInterval(autoRefreshTimer);
  autoRefreshTimer = setInterval(() => {
    if (document.visibilityState === 'visible') {
      loadChart();
    }
  }, 60000);
}
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    loadChart();
  }
});
loadChart().then(() => startAutoRefresh());
</script>
</body></html>`;

  // CSP：浏览器层兜底防御
  const csp = [
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com https://cdnjs.cloudflare.com",
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
