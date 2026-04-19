-- 分钟级带宽聚合表（只存带宽，回源请求数由 CF 控制台 Origin Status Code Tab 提供）
-- 幂等设计：主键包含 r2_key，每个文件的贡献独立存储
-- Queue 重试/Worker 崩溃重跑同一文件时，INSERT OR REPLACE 会覆盖而非累加，保证数据不翻倍
-- 查询时用 SUM() 按 (minute_utc, zone) 聚合得到真实带宽
CREATE TABLE IF NOT EXISTS bw_stats (
    minute_utc  TEXT    NOT NULL,
    zone        TEXT    NOT NULL DEFAULT 'all',
    r2_key      TEXT    NOT NULL DEFAULT '',
    sum_bytes   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (minute_utc, zone, r2_key)
);

-- 文件处理状态表
CREATE TABLE IF NOT EXISTS processed_files (
    r2_key           TEXT    PRIMARY KEY,
    status           TEXT    NOT NULL DEFAULT 'processing',
    started_at       TEXT    NOT NULL,
    finished_at      TEXT,
    line_count       INTEGER NOT NULL DEFAULT 0,
    file_minute_min  TEXT,
    file_minute_max  TEXT,
    retry_count      INTEGER NOT NULL DEFAULT 0,
    error_msg        TEXT
);

CREATE INDEX IF NOT EXISTS idx_bw_minute   ON bw_stats(minute_utc);
CREATE INDEX IF NOT EXISTS idx_bw_zone     ON bw_stats(zone);
CREATE INDEX IF NOT EXISTS idx_file_status ON processed_files(status);
CREATE INDEX IF NOT EXISTS idx_file_time   ON processed_files(started_at);

-- 告警历史表（防重复告警 + 告警记录）
CREATE TABLE IF NOT EXISTS alert_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type  TEXT    NOT NULL,   -- 'threshold'（固定阈值）| 'spike'（突增）
    zone        TEXT    NOT NULL,
    minute_utc  TEXT    NOT NULL,   -- 触发告警的分钟
    mbps        REAL    NOT NULL,   -- 触发时的带宽值
    detail      TEXT,               -- 告警详情 JSON（如历史峰值、倍数等）
    alerted_at  TEXT    NOT NULL    -- 告警发出的时间（ISO 8601）
);

CREATE INDEX IF NOT EXISTS idx_alert_zone_type ON alert_history(zone, alert_type, alerted_at);
