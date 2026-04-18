# origin-bw-monitor

**CF Origin Pull Bandwidth Monitor** — A Cloudflare Worker deployed in your own account that reads your Logpush R2 logs and renders a per-minute origin pull bandwidth chart dashboard.

## Formula

```
SUM(CacheResponseBytes) WHERE OriginResponseStatus NOT IN (0, 304)
÷ 60 seconds × 8 bits = Mbps
```

Reference: https://developers.cloudflare.com/logs/faq/common-calculations/

## Quick Start

```bash
# 1. Clone
git clone https://github.com/CFChinaNetwork/origin-bw-monitor.git
cd origin-bw-monitor

# 2. Create D1 database → copy database_id into wrangler.toml
wrangler d1 create bw-stats

# 3. Init tables
wrangler d1 execute bw-stats --file=./schema.sql --remote

# 4. Create Queues
wrangler queues create bw-ingest-queue
wrangler queues create bw-ingest-dlq

# 5. Edit wrangler.toml — fill in your bucket name, D1 id, START_TIME

# 6. Configure R2 Event Notification
#    R2 Bucket → Settings → Event Notifications → Add
#    Event: object-create → Queue: bw-ingest-queue

# 7. (Optional) Set Dashboard token
wrangler secret put DASHBOARD_TOKEN

# 8. Deploy
wrangler deploy

# 9. Open Dashboard
# https://origin-bw-monitor.<your-subdomain>.workers.dev/?hours=24
```

## Documentation

| Language | File |
|---|---|
| English | [CF Origin Pull Bandwidth Monitor — Deployment Guide](https://cfchinanetwork.github.io/origin-bw-monitor/Origin-Pull-Bandwidth-Monitor-Guide.html) |
| 中文 | [CF 回源带宽监控工具——部署与使用指南](https://cfchinanetwork.github.io/origin-bw-monitor/%E5%9B%9E%E6%BA%90%E5%B8%A6%E5%AE%BD%E7%9B%91%E6%8E%A7%E5%B7%A5%E5%85%B7%E9%83%A8%E7%BD%B2%E6%8C%87%E5%8D%97.html) |

## License

[MIT](https://opensource.org/licenses/MIT) — Free to use, modify and distribute with attribution.
