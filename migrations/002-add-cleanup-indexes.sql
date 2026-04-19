-- Migration 002: Add indexes to accelerate cleanup operations (#19)
-- Date: 2026-04-19
-- 幂等：用 CREATE INDEX IF NOT EXISTS，可重复执行

-- alert_history 按 alerted_at 范围删除，加单列索引
CREATE INDEX IF NOT EXISTS idx_alert_time     ON alert_history(alerted_at);

-- processed_files 按 finished_at 范围删除，加单列索引
CREATE INDEX IF NOT EXISTS idx_file_finished  ON processed_files(finished_at);
