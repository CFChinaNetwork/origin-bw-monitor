# origin-bw-monitor

**CF Origin Pull Bandwidth Monitor** — A Cloudflare Worker that reads Logpush logs from R2 storage (read-only), computes per-minute origin pull bandwidth using the official Cloudflare formula, and renders a real-time chart dashboard.

---

## Architecture

```
Customer R2 (read-only, Logpush logs)
    ↓  CF REST API (LIST + GET, no writes ever)
[Cron Worker]  every 10 min → scan new files → enqueue keys
    ↓
[bw-ingest-queue]
    ↓
[Queue Consumer]  streaming parse → official formula → write D1
    ↓
[D1 Database]  per-minute bandwidth aggregation
    ↓
[Dashboard Worker]  HTTP → query D1 → Chart.js chart
```

**Key design principles:**
- **Read-only on customer R2** — only `LIST` and `GET` operations, never `PUT` / `DELETE` / `POST`
- **No data migration needed** — reads directly from existing Logpush R2 bucket
- **Cross-account access** — the Worker can be deployed in your own account while reading any R2 bucket you have API Token access to
- **Streaming parse** — 10–150 MB gzip files are processed line-by-line, no OOM risk
- **Safe Lag Window** — only processes files older than `LAG_MINUTES` to ensure all logs for a given minute have landed in R2

---

## Formula

**Origin Pull Bandwidth (official):**

```
SUM(CacheResponseBytes)
WHERE OriginResponseStatus NOT IN (0, 304)
÷ 60 seconds × 8 bits = Mbps
```

Reference: https://developers.cloudflare.com/logs/faq/common-calculations/

| OriginResponseStatus | Meaning | Included? |
|---|---|---|
| `0` | HIT — CF served from cache, no origin request | ❌ Excluded |
| `304` | Revalidated — origin confirmed no change, no body | ❌ Excluded |
| `200`, `206`, `4xx`, `5xx`, etc. | Real origin response with body | ✅ Included |

**Time bucketing:** uses `EdgeEndTimestamp` (request end time), consistent with CF dashboard.

---

## Prerequisites

- Cloudflare account with Workers Paid plan (for Queue + D1)
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/) v3+
- A CF API Token with permission to read the target R2 bucket

---

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/CFChinaNetwork/origin-bw-monitor.git
cd origin-bw-monitor
```

### 2. Create D1 database

```bash
wrangler d1 create bw-stats
```

Copy the `database_id` from the output and paste it into `wrangler.toml`:

```toml
[[d1_databases]]
binding       = "DB"
database_name = "bw-stats"
database_id   = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"   # ← paste here
```

### 3. Initialize D1 tables

```bash
wrangler d1 execute bw-stats --file=./schema.sql --remote
```

### 4. Create Queues

```bash
wrangler queues create bw-ingest-queue
wrangler queues create bw-ingest-dlq
```

### 5. Edit wrangler.toml

Fill in your target R2 account and bucket:

```toml
[vars]
CUSTOMER_ACCOUNT_ID = "abc123..."          # Target account ID (where Logpush R2 lives)
CUSTOMER_BUCKET     = "my-logpush-bucket"  # R2 bucket name
LOG_PREFIX          = "logs/"             # Logpush path prefix
START_TIME          = "2026-04-01T00:00:00Z"  # Start from this date (UTC)
```

### 6. Set Secrets

```bash
# CF API Token with R2 Read permission on the target bucket
# Create at: CF Dashboard → My Profile → API Tokens → Create Token
# Required permissions: Account > R2 Storage > Read
wrangler secret put CF_API_TOKEN

# (Optional) Dashboard access token — leave empty to disable auth
wrangler secret put DASHBOARD_TOKEN
```

### 7. Deploy

```bash
wrangler deploy
```

### 8. Import historical data

The Cron trigger runs every 10 minutes and picks up new files automatically.
To import historical files manually, use the `/api/import` endpoint:

```bash
# Import first page (50 files) starting from START_TIME
curl -X POST "https://origin-bw-monitor.<your-subdomain>.workers.dev/api/import" \
  -H "Content-Type: application/json" \
  -d '{"page_size": 50}'

# If response shows truncated=true, continue with next_cursor:
curl -X POST ".../api/import" \
  -H "Content-Type: application/json" \
  -d '{"page_size": 50, "cursor": "<next_cursor_from_previous_response>"}'
```

### 9. Open the dashboard

```
https://origin-bw-monitor.<your-subdomain>.workers.dev/?zone=example.com&hours=24
```

---

## Configuration Reference

All configuration is in `wrangler.toml [vars]`. **Never hard-code secrets in code or toml.**

| Variable | Default | Description |
|---|---|---|
| `CUSTOMER_ACCOUNT_ID` | _(required)_ | CF Account ID where the Logpush R2 bucket lives |
| `CUSTOMER_BUCKET` | _(required)_ | R2 bucket name containing Logpush files |
| `LOG_PREFIX` | `logs/` | Path prefix inside the R2 bucket |
| `START_TIME` | `1970-01-01T00:00:00Z` | Only process files written after this UTC time |
| `LAG_MINUTES` | `10` | Safe Lag Window: skip files newer than `now - LAG` minutes |
| `SCAN_BATCH_SIZE` | `20` | Max files to enqueue per Cron run |
| `RATE_LIMIT_DELAY_MS` | `300` | Delay between CF API calls (ms). CF limit: 1200 req/5 min |
| `MAX_PROCESSING_MIN` | `15` | Processing timeout (min). Stuck files are re-queued after this |
| `DATA_RETENTION_DAYS` | `7` | D1 data retention. Auto-cleaned daily at UTC 02:00 |
| `LOG_LEVEL` | `info` | Log verbosity: `debug` / `info` / `warn` / `error` |
| `DASHBOARD_TOKEN` | _(empty)_ | Dashboard auth token. Empty = no auth (dev only) |

**Secrets (set via `wrangler secret put`):**

| Secret | Description |
|---|---|
| `CF_API_TOKEN` | CF API Token with R2 Read access on `CUSTOMER_ACCOUNT_ID` |
| `DASHBOARD_TOKEN` | (Optional) Access token for Dashboard UI |

---

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Dashboard chart UI |
| `/api/stats?zone=&hours=` | GET | Bandwidth data as JSON |
| `/api/import` | POST | Manually trigger file import (paginated) |
| `/api/status` | GET | System status (files processed, data range, config) |
| `/api/retry` | POST | Re-queue failed files |

---

## Data Model

**`bw_stats` table:**
```sql
minute_utc  TEXT    -- UTC minute, e.g. "2026-04-14T03:01"
zone        TEXT    -- ClientRequestHost (domain name)
sum_bytes   INTEGER -- SUM(CacheResponseBytes) for this minute
```

**`processed_files` table:** tracks which R2 files have been processed (prevents duplicates).

---

## Estimated Latency

| Scenario | Latency | Reason |
|---|---|---|
| Real-time (Cron mode) | ~20 min | Cron 10 min + LAG 10 min + processing |
| Historical bulk import | ~1 file/sec | CF API rate limit 300ms/file |
| 5,000 files/day | ~1.5 hours per full day | Sequential processing |
| Chart query | < 500ms | D1 local SQL |

---

## GitHub Actions (Auto Deploy)

Push to `main` triggers automatic deployment via GitHub Actions.

Set the following secret in your GitHub repository:
- `CLOUDFLARE_API_TOKEN` — token with Workers Deploy permission

---

## Why use API Token instead of Global API Key?

| | Global API Key | API Token |
|---|---|---|
| Scope | Full account access | Minimum required permissions |
| Revocability | Must change password | Revoke single token |
| Auditability | No per-token logging | Per-token activity log |
| Security | High risk if leaked | Minimal blast radius |

**Always use API Token for production deployments.**

---

## License

MIT

---

---

# CF 回源带宽监控工具

从客户 Logpush R2 日志中实时计算回源带宽，生成分钟级时序图表。

---

## 架构

```
客户 R2（只读，Logpush 日志）
    ↓  CF REST API（LIST + GET，绝不写入）
[Cron Worker]  每10分钟扫描新文件 → 入队
    ↓
[bw-ingest-queue]
    ↓
[Queue Consumer]  流式解析 → 官方公式 → 写入 D1
    ↓
[D1 数据库]  分钟级带宽聚合
    ↓
[Dashboard Worker]  HTTP → 查询 D1 → Chart.js 图表
```

**核心设计原则：**
- **对客户 R2 只读** — 仅 `LIST` 和 `GET` 操作，绝不写入
- **无需迁移数据** — 直接读取现有 Logpush R2 桶
- **跨账号访问** — Worker 部署在自己账号，通过 API Token 读取任意授权 R2 桶
- **流式解析** — 10~150 MB 的 gzip 文件逐行处理，无 OOM 风险
- **Safe Lag Window** — 只处理 `LAG_MINUTES` 分钟前的文件，确保该分钟内所有日志已落盘

---

## 计算公式

**回源带宽（官方公式）：**

```
SUM(CacheResponseBytes)
WHERE OriginResponseStatus NOT IN (0, 304)
÷ 60 秒 × 8 bits = Mbps
```

参考：https://developers.cloudflare.com/logs/faq/common-calculations/

| OriginResponseStatus | 含义 | 是否计入 |
|---|---|---|
| `0` | HIT — CF 从缓存返回，未联系源站 | ❌ 排除 |
| `304` | Revalidated — 源站确认未变更，无 body | ❌ 排除 |
| `200`、`206`、`4xx`、`5xx` 等 | 源站有真实响应（含 body） | ✅ 计入 |

**时间分桶：** 使用 `EdgeEndTimestamp`（请求结束时间），与 CF 控制台口径一致。

---

## 前置条件

- Cloudflare Workers Paid 计划（Queue + D1 需要付费计划）
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/) v3+
- 有权限读取目标 R2 Bucket 的 CF API Token

---

## 快速部署

### 1. 克隆仓库

```bash
git clone https://github.com/CFChinaNetwork/origin-bw-monitor.git
cd origin-bw-monitor
```

### 2. 创建 D1 数据库

```bash
wrangler d1 create bw-stats
```

将输出的 `database_id` 填入 `wrangler.toml`：

```toml
[[d1_databases]]
database_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"   # ← 粘贴到这里
```

### 3. 初始化 D1 表结构

```bash
wrangler d1 execute bw-stats --file=./schema.sql --remote
```

### 4. 创建 Queue

```bash
wrangler queues create bw-ingest-queue
wrangler queues create bw-ingest-dlq
```

### 5. 修改 wrangler.toml

填写目标 R2 的账号和 Bucket 信息：

```toml
[vars]
CUSTOMER_ACCOUNT_ID = "客户的CF_Account_ID"
CUSTOMER_BUCKET     = "logpush桶名"
LOG_PREFIX          = "logs/"
START_TIME          = "2026-04-01T00:00:00Z"   # 从哪天开始分析
```

### 6. 设置 Secrets

```bash
# CF API Token，需要目标 R2 Bucket 的读取权限
# 创建方式：CF Dashboard → My Profile → API Tokens → Create Token
# 所需权限：Account > R2 Storage > Read
wrangler secret put CF_API_TOKEN

# （可选）Dashboard 访问鉴权 Token，留空则不鉴权
wrangler secret put DASHBOARD_TOKEN
```

### 7. 部署

```bash
wrangler deploy
```

### 8. 导入历史数据

Cron 每10分钟自动扫描并处理新文件。如需导入历史数据，使用 `/api/import` 接口：

```bash
# 导入第一批（50个文件）
curl -X POST "https://origin-bw-monitor.<子域名>.workers.dev/api/import" \
  -H "Content-Type: application/json" \
  -d '{"page_size": 50}'

# 如果响应中 truncated=true，用 next_cursor 继续下一页：
curl -X POST ".../api/import" \
  -H "Content-Type: application/json" \
  -d '{"page_size": 50, "cursor": "<上次返回的next_cursor>"}'
```

### 9. 打开图表

```
https://origin-bw-monitor.<子域名>.workers.dev/?zone=example.com&hours=24
```

---

## 配置说明

所有配置项集中在 `wrangler.toml [vars]`，**敏感信息绝不写在代码或 toml 文件中**。

| 变量 | 默认值 | 说明 |
|---|---|---|
| `CUSTOMER_ACCOUNT_ID` | _（必填）_ | Logpush R2 桶所在的 CF Account ID |
| `CUSTOMER_BUCKET` | _（必填）_ | Logpush R2 Bucket 名称 |
| `LOG_PREFIX` | `logs/` | R2 桶内 Logpush 文件的路径前缀 |
| `START_TIME` | `1970-01-01T00:00:00Z` | 只处理此 UTC 时间点之后的文件 |
| `LAG_MINUTES` | `10` | Safe Lag Window（分钟），跳过太新的文件 |
| `SCAN_BATCH_SIZE` | `20` | Cron 每次最多入队文件数 |
| `RATE_LIMIT_DELAY_MS` | `300` | CF API 调用间隔（毫秒），防触发 Rate Limit |
| `MAX_PROCESSING_MIN` | `15` | 处理超时阈值（分钟），超时文件自动重试 |
| `DATA_RETENTION_DAYS` | `7` | D1 数据保留天数，每日 UTC 02:00 自动清理 |
| `LOG_LEVEL` | `info` | 日志级别：`debug` / `info` / `warn` / `error` |
| `DASHBOARD_TOKEN` | _（空）_ | Dashboard 访问 Token，空 = 不鉴权（仅开发用）|

**Secrets（通过 `wrangler secret put` 设置）：**

| Secret | 说明 |
|---|---|
| `CF_API_TOKEN` | 有 `CUSTOMER_ACCOUNT_ID` 账号 R2 Read 权限的 CF API Token |
| `DASHBOARD_TOKEN` | （可选）Dashboard 访问 Token |

---

## API 接口

| 接口 | 方法 | 说明 |
|---|---|---|
| `/` | GET | Dashboard 图表页面 |
| `/api/stats?zone=&hours=` | GET | 带宽数据 JSON |
| `/api/import` | POST | 手动分页导入历史文件 |
| `/api/status` | GET | 系统状态（处理进度、数据范围、配置） |
| `/api/retry` | POST | 重试失败的文件 |

---

## 延迟说明

| 场景 | 延迟 | 原因 |
|---|---|---|
| 实时模式（Cron）| 约 20 分钟 | Cron 10 分钟 + LAG 10 分钟 + 处理时间 |
| 历史批量导入 | 约 1 文件/秒 | CF API Rate Limit 300ms/文件 |
| 图表查询响应 | < 500ms | D1 本地 SQL |

---

## GitHub Actions 自动部署

推送到 `main` 分支自动触发部署。

在 GitHub 仓库 Settings → Secrets → Actions 中设置：
- `CLOUDFLARE_API_TOKEN` — 有 Workers Deploy 权限的 CF API Token

---

## 为什么使用 API Token 而不是 Global API Key？

| | Global API Key | API Token |
|---|---|---|
| 权限范围 | 账号完全控制权 | 最小必要权限 |
| 可撤销性 | 需更改密码 | 直接撤销单个 Token |
| 安全风险 | 泄露后影响整个账号 | 影响范围可控 |

**生产环境务必使用 API Token。**

---

## License

MIT
