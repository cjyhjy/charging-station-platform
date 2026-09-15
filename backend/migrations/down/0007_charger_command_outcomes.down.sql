-- Down script for 0007_charger_command_outcomes.sql.
--
-- It is deliberately NOT part of the embedded migration set: the runner embeds *.sql at the
-- top level of backend/migrations and refuses duplicate versions, so a down script living
-- there would be applied as a second migration of the same version. Run it by hand:
--
--   psql "$NCS_POSTGRES_DSN" -f backend/migrations/down/0007_charger_command_outcomes.down.sql
--
-- Only run it after every process that records device outcomes has been stopped. The table is what
-- keeps a duplicate delivery of the same command result from being applied twice, so dropping it
-- while a worker runs removes the protection the recovery path relies on.
--
-- Dropping it does not change any order or charger state: it only forgets which commands have
-- already been answered, which is why a re-applied duplicate after a rollback would be silent.

DROP INDEX IF EXISTS idx_charger_command_outcomes_charger;

DROP TABLE IF EXISTS charger_command_outcomes;
