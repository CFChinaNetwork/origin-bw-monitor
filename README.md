# origin-bw-monitor

**CF Origin Pull Bandwidth Monitor** — A Cloudflare Worker deployed in your own account that automatically computes per-minute origin pull bandwidth from Logpush R2 logs and renders a real-time chart dashboard with multi-channel alerting.

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
- **Idempotent writes** — each file's contribution stored under `(minute_utc, zone, r2_key)`; Queue retries never cause data doubling
- **Stuck-file recovery** — cron also retries `processing` files that have been stuck longer than `MAX_PROCESSING_MIN`

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

## Project Structure

```
src/                    ← ES module source (deployed via wrangler bundle)
├── index.js            Worker entry (scheduled / queue / fetch)
├── config.js           Config + WeakMap cache
├── utils.js            Log / XSS escape / URL builder / auth
├── processor.js        R2 file ingestion + retry
├── alerting.js         Alert detection + WeCom/DingTalk/Feishu webhooks
├── cleanup.js          D1 data lifecycle cleanup
└── dashboard.js        HTTP handlers + Dashboard HTML

migrations/             ← D1 schema migrations (run in order)
tests/                  ← Regression test scripts (bash + Node)
docs/                   ← Full deployment guides (EN/CN)
.github/workflows/      ← Auto-deploy on push to main
```

## Quick Start

```bash
# 1. Clone
git clone https://github.com/CFChinaNetwork/origin-bw-monitor.git
cd origin-bw-monitor

# 2. Create D1 database (record the UUID)
wrangler d1 create bw-stats

# 3. Fill in wrangler.toml:
#    - [[r2_buckets]].bucket_name  → your Logpush R2 bucket
#    - [[d1_databases]].database_id → UUID from step 2

# 4. Init tables
wrangler d1 execute bw-stats --remote --file=./schema.sql

# 5. Create queues
wrangler queues create bw-ingest-queue
wrangler queues create bw-ingest-dlq

# 6. Configure R2 Event Notification (CF Dashboard)
#    R2 Bucket → Settings → Event Notifications → Add
#    Event: object-create → Queue: bw-ingest-queue

# 7. Deploy
wrangler deploy

# 8. Open Dashboard
# https://origin-bw-monitor.<subdomain>.workers.dev/?hours=24
```

> **Auto-deploy:** pushing to `main` branch triggers `.github/workflows/deploy.yml`, which runs `wrangler deploy` with secrets `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID`.

## Configuration

All configs live in `wrangler.toml`:

| Variable | Default | Description |
|---|---|---|
| `LAG_MINUTES` | `7` | Wait window ensuring all Logpush files for a minute have landed |
| `DATA_RETENTION_DAYS` | `7` | Auto-delete data older than N days (daily 02:00 UTC) |
| `MAX_PROCESSING_MIN` | `15` | Mark processing file as stuck after N min; cron will retry it |
| `DISPLAY_TZ_OFFSET_HOURS` | `8` | Dashboard/alert timezone offset (UTC+8 for China; 0 for UTC; -8 for US West) |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `DASHBOARD_TOKEN` | _(empty)_ | Set via `wrangler secret put` to require `?token=` auth |

## Alerting

Supports **WeCom / DingTalk / Feishu** webhook alerts (any combination):

```bash
wrangler secret put ALERT_WEBHOOK_WECOM      # Enterprise WeChat
wrangler secret put ALERT_WEBHOOK_DINGTALK   # DingTalk
wrangler secret put ALERT_WEBHOOK_FEISHU     # Feishu / Lark
```

Configure thresholds in `wrangler.toml`:
```toml
ALERT_THRESHOLD_MBPS   = "500"    # Alert when bandwidth > 500 Mbps
ALERT_SPIKE_MULTIPLIER = "5"      # Alert when bandwidth > 5× past-24h peak
ALERT_COOLDOWN_MIN     = "30"     # Min interval between same-type alerts per zone
ALERT_DASHBOARD_URL    = "https://origin-bw-monitor.xxx.workers.dev"
```

Leave empty to disable that alert type. Concurrent Workers cannot produce duplicate alerts — atomic SQL lock (`INSERT ... WHERE NOT EXISTS`) guarantees at-most-once within cooldown window.

## End-to-End Latency

| Stage | Latency |
|---|---|
| Logpush batch interval | ~60 sec |
| R2 Event Notification | 2–5 sec |
| Queue + Worker processing | 5–30 sec |
| Safe Lag Window | +7 min |
| **Total chart data lag** | **~10 min** |

## Testing

Regression suite under `tests/` (14 scripts, ~75 scenarios):

```bash
# Run all (requires sqlite3 for *.sh, node for *.mjs)
for t in tests/*.sh;  do bash "$t"; done
for t in tests/*.mjs; do
  [ "$(basename "$t")" = "_read-all-src.mjs" ] || node "$t"
done
```

Covers: idempotency (#1), XSS defense (#3), stuck-file recovery (#2),
table growth (#4), URL building (#5), failure tracking (#6), cooldown
race (#7), alert window (#8), zone SQL safety (#10), time boundaries
(#11), dashboard config (#12/#13), batch cleanup (#17), module structure.

## Migrations

Migrations apply in order, idempotent (`IF NOT EXISTS`):

```bash
wrangler d1 execute bw-stats --remote --file=./migrations/001-add-r2-key-to-bw-stats.sql
wrangler d1 execute bw-stats --remote --file=./migrations/002-add-cleanup-indexes.sql
```

Fresh installs using `schema.sql` already have everything; migrations
are for upgrading existing deployments.

## Documentation

| Language | File |
|---|---|
| English | [CF Origin Pull Bandwidth Monitor — Deployment Guide](https://cfchinanetwork.github.io/origin-bw-monitor/Origin-Pull-Bandwidth-Monitor-Guide.html) |
| 中文 | [CF 回源带宽监控工具——部署与使用指南](https://cfchinanetwork.github.io/origin-bw-monitor/%E5%9B%9E%E6%BA%90%E5%B8%A6%E5%AE%BD%E7%9B%91%E6%8E%A7%E5%B7%A5%E5%85%B7%E9%83%A8%E7%BD%B2%E6%8C%87%E5%8D%97.html) |

## License

Proprietary - Internal Use Only
