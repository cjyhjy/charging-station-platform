-- Down script for 0006_event_consumptions.sql.
--
-- It is deliberately NOT part of the embedded migration set: the runner embeds *.sql at the
-- top level of backend/migrations and refuses duplicate versions, so a down script living
-- there would be applied as a second migration of the same version. Run it by hand:
--
--   psql "$NCS_POSTGRES_DSN" -f backend/migrations/down/0006_event_consumptions.down.sql
--
-- Only run it after every process that writes consumption records has been stopped: the
-- table is what makes a redelivery safe, so dropping it while a worker runs would remove
-- the duplicate protection the worker relies on.
--
-- The outbox index is dropped as well, because the publisher created for BE-I-01 uses it.

DROP INDEX IF EXISTS idx_outbox_unpublished_identity;

DROP INDEX IF EXISTS idx_event_consumptions_outcome;
DROP INDEX IF EXISTS idx_event_consumptions_lease;

DROP TABLE IF EXISTS event_consumptions;
