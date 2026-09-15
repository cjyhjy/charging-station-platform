-- Down script for 0008_user_profile_and_deletion.sql.
--
-- It is deliberately NOT part of the embedded migration set: the runner embeds *.sql at the top level
-- of backend/migrations and refuses duplicate versions, so a down script living there would be applied
-- as a second migration of the same version. Run it by hand:
--
--   psql "$NCS_POSTGRES_DSN" -f backend/migrations/down/0008_user_profile_and_deletion.down.sql
--
-- Dropping the two columns destroys data: every avatar URL and every deletion timestamp is gone, and
-- rows that were anonymized by UC-U-05 stay anonymized (their phone numbers cannot be recovered by
-- rolling back — that is the point of the scheme). Only run this to undo a migration that was applied
-- by mistake, on a database where nothing depends on it yet.
--
-- The constraints must go before the columns they reference.

ALTER TABLE user_accounts
    DROP CONSTRAINT IF EXISTS user_accounts_avatar_url_length,
    DROP CONSTRAINT IF EXISTS user_accounts_deleted_has_no_credentials,
    DROP CONSTRAINT IF EXISTS user_accounts_deleted_is_disabled;

DROP INDEX IF EXISTS idx_user_accounts_deleted_at;

ALTER TABLE user_accounts
    DROP COLUMN IF EXISTS deleted_at,
    DROP COLUMN IF EXISTS avatar_url;
