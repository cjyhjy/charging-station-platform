-- A-01 提案（待 B-02 确认并落为正式迁移）：用户资料与账号注销支撑。
-- 本文件是 A-01 模块的迁移建议案，未写入 backend/migrations/；
-- B-02 确认后按规范转为递增迁移。

ALTER TABLE user_accounts
    ADD COLUMN IF NOT EXISTS avatar_url TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- 注销匿名化不新增列：display_name 置为 '已注销用户'，phone 匿名化为
-- 'deleted-{id}@invalid'（UNIQUE 冲突由 id 保证不重复），password_hash 清空，
-- status 置 DISABLED，deleted_at 记录时间。匿名订单/钱包记录按需求保留。

CREATE INDEX IF NOT EXISTS idx_user_accounts_deleted_at
    ON user_accounts (deleted_at) WHERE deleted_at IS NOT NULL;
