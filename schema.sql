-- 分钟级带宽聚合表（只存带宽，回源请求数由 CF 控制台 Origin Status Code Tab 提供）
CREATE TABLE IF NOT EXISTS bw_stats (
    minute_utc  TEXT    NOT NULL,
    zone        TEXT    NOT NULL DEFAULT 'all',
    sum_bytes   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (minute_utc, zone)
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
