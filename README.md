# origin-bw-monitor

**CF Origin Pull Bandwidth Monitor** — A Cloudflare Worker deployed in your own account that automatically computes per-minute origin pull bandwidth from Logpush R2 logs and renders a real-time chart dashboard with alerting.

## How It Works

```
Logpush writes new .gz file to R2
    ↓ R2 Event Notification (2–5 sec)
bw-ingest-queue
    ↓ Queue Consumer
Stream decompress → parse → official formula → write D1
    ↓
Alert check (previous minute cumulative bandwidth from D1)
    ↓
Dashboard chart (query D1 → Chart.js)
```

**Key design:**
- **Event-driven only** — processes new files via R2 Event Notification; no historical backfill
- **Read-only on R2** — never writes, modifies, or deletes R2 objects
- **Safe Lag Window** — waits 7 min before processing, ensuring all Logpush files for a given minute have landed
- **Pending retry** — files too new for the lag window are retried every 20 min via Cron

## Formula

```
SUM(CacheResponseBytes) WHERE OriginResponseStatus NOT IN (0, 304)
÷ 60 seconds × 8 bits = Mbps
```

Reference: https://developers.cloudflare.com/logs/faq/common-calculations/

| OriginResponseStatus | Meaning | Included |
|---|---|---|
| `0` | Cache HIT — no origin request | ❌ |
| `304` | Revalidated — no body transferred | ❌ |
| Others | Real origin response with body | ✅ |

## Quick Start

```bash
# 1. Clone
git clone https://github.com/CFChinaNetwork/origin-bw-monitor.git
cd origin-bw-monitor

# 2. Create D1 → copy database_id into wrangler.toml
wrangler d1 create bw-stats

# 3. Init tables
wrangler d1 execute bw-stats --file=./schema.sql --remote

# 4. Create Queues
wrangler queues create bw-ingest-queue
wrangler queues create bw-ingest-dlq

# 5. Edit wrangler.toml — fill in bucket_name and database_id

# 6. Configure R2 Event Notification (CF Dashboard)
#    R2 Bucket → Settings → Event Notifications → Add
#    Event: object-create → Queue: bw-ingest-queue

# 7. Deploy
wrangler deploy

# 8. Open Dashboard
# https://origin-bw-monitor.<subdomain>.workers.dev/?hours=24
```

## Alerting

Supports **WeCom / DingTalk / Feishu** webhook alerts (any combination):

```bash
wrangler secret put ALERT_WEBHOOK_WECOM      # Enterprise WeChat
wrangler secret put ALERT_WEBHOOK_DINGTALK   # DingTalk
wrangler secret put ALERT_WEBHOOK_FEISHU     # Feishu / Lark
```

Configure thresholds in `wrangler.toml`:
```toml
ALERT_THRESHOLD_MBPS   = "500"   # Alert when bandwidth > 500 Mbps
ALERT_SPIKE_MULTIPLIER = "5"     # Alert when bandwidth > 5× 24h peak
ALERT_COOLDOWN_MIN     = "30"    # Min interval between same alerts
ALERT_DASHBOARD_URL    = "https://origin-bw-monitor.xxx.workers.dev"
```

Leave empty to disable that alert type.

## End-to-End Latency

| Stage | Latency |
|---|---|
| Logpush batch interval | ~60 sec |
| R2 Event Notification | 2–5 sec |
| Queue + Worker processing | 5–30 sec |
| Safe Lag Window | +7 min |
| **Total chart data lag** | **~10 min** |

## Documentation

| Language | File |
|---|---|
| English | [CF Origin Pull Bandwidth Monitor — Deployment Guide](https://cfchinanetwork.github.io/origin-bw-monitor/Origin-Pull-Bandwidth-Monitor-Guide.html) |
| 中文 | [CF 回源带宽监控工具——部署与使用指南](https://cfchinanetwork.github.io/origin-bw-monitor/%E5%9B%9E%E6%BA%90%E5%B8%A6%E5%AE%BD%E7%9B%91%E6%8E%A7%E5%B7%A5%E5%85%B7%E9%83%A8%E7%BD%B2%E6%8C%87%E5%8D%97.html) |

## License

Proprietary - Internal Use Only
