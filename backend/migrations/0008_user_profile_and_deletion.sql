-- A-01 用户资料与注销支撑（B-02 正式迁移，来源：A-01 提案
-- docs/migration/proposals/a-01-user-profile-migration-0008.sql）。
--
-- A 线交付了 auth.ProfileView / ProfileUpdate 与 AccountMutation 端口，PostgreSQL 适配器由 B 线负责；
-- 本迁移是那份提案的正式版本，并在提案之上补了三条不变量约束与一条存储上界，理由写在每一条旁边。
--
-- Additive：不改动 0001..0007 的任何内容。

ALTER TABLE user_accounts
    -- 头像地址：空串表示没有头像（契约里 avatarUrl 可省略）。
    ADD COLUMN IF NOT EXISTS avatar_url TEXT NOT NULL DEFAULT '',
    -- 注销时间：NULL = 未注销。注销采用"就地匿名化"而不是删除行，
    -- 因为订单/钱包/账务记录必须按保留策略留存（UC-U-05）。
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- 注销是一个复合事实：手机号与昵称被匿名化、凭据被清空、账号被禁用、时间被记录。适配器一次
-- UPDATE 完成全部动作，但如果没有约束，将来任何一条遗漏字段的 UPDATE 都会留下一个"已注销却
-- 仍可登录"的账号——这种账号不会报错，只会被忽略，直到有人发现它能登录。因此把它变成数据库
-- 层面的不变量：
ALTER TABLE user_accounts
    -- 已注销 ⇒ 必须已禁用。
    ADD CONSTRAINT user_accounts_deleted_is_disabled
        CHECK (deleted_at IS NULL OR status = 'DISABLED'),
    -- 已注销 ⇒ 凭据必须已清空（password_hash 列本身 NOT NULL，所以清空即空串）。
    ADD CONSTRAINT user_accounts_deleted_has_no_credentials
        CHECK (deleted_at IS NULL OR password_hash = ''),
    -- 头像地址的存储上界：契约没有限制长度，但列是 TEXT，没有上界就等于允许把任意大小的内容
    -- 塞进用户表。512 足够任何 CDN 地址；若将来确实需要更长，用新的递增迁移放宽。
    ADD CONSTRAINT user_accounts_avatar_url_length
        CHECK (char_length(avatar_url) <= 512);

-- 注销账号在查询里被过滤（deleted_at IS NULL 是每个读路径的条件），因此按"未注销"过滤的索引
-- 只在有注销数据时才有意义：部分索引既给出查找能力，又不会为绝大多数 NULL 行付出代价。
CREATE INDEX IF NOT EXISTS idx_user_accounts_deleted_at
    ON user_accounts (deleted_at) WHERE deleted_at IS NOT NULL;
