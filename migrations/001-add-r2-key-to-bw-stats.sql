-- Migration 001: Add r2_key to bw_stats for idempotent writes
-- Date: 2026-04-19
-- Fix: Bug #1 — UPSERT accumulation caused data doubling on Queue retry
--
-- 使用方式（生产环境）：
--   wrangler d1 execute bw-stats --file=./migrations/001-add-r2-key-to-bw-stats.sql --remote
--
-- 迁移策略：
--   - 旧累加数据保留，r2_key 设为空字符串（代表"migration 前的遗留累加值"）
--   - 新数据写入时用实际 r2_key，INSERT OR REPLACE 保证幂等
--   - SUM() 查询会把新旧数据一并累加，平滑过渡
--
-- 注意：建议迁移前先备份 D1：
--   wrangler d1 export bw-stats --output=backup-pre-001.sql --remote

-- Step 1: 重命名旧表
ALTER TABLE bw_stats RENAME TO bw_stats_v1;

-- Step 2: 创建新表（含 r2_key 列）
CREATE TABLE bw_stats (
    minute_utc  TEXT    NOT NULL,
    zone        TEXT    NOT NULL DEFAULT 'all',
    r2_key      TEXT    NOT NULL DEFAULT '',
    sum_bytes   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (minute_utc, zone, r2_key)
);

-- Step 3: 迁移旧数据（r2_key='' 表示 migration 前的遗留值）
INSERT INTO bw_stats (minute_utc, zone, r2_key, sum_bytes)
SELECT minute_utc, zone, '', sum_bytes FROM bw_stats_v1;

-- Step 4: 重建索引
CREATE INDEX IF NOT EXISTS idx_bw_minute ON bw_stats(minute_utc);
CREATE INDEX IF NOT EXISTS idx_bw_zone   ON bw_stats(zone);

-- Step 5: 旧表保留一段时间，验证数据无误后手动删除
-- DROP TABLE bw_stats_v1;
