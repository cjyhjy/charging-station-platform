-- BE-I-02 review fix: a device verdict is a permanent fact about one command.
--
-- The outcome of a charger command was deduplicated through idempotency_records, whose record
-- expires after 24 hours. That window is right for a client retry and wrong for a device verdict:
-- an order stuck in STOPPING is still meaningful a day later - it is exactly the state the STOP
-- recovery sweep works on - so once the idempotency record expired, a duplicate delivery of the
-- same STOP result was treated as a first delivery, decided "applied" again (the order deliberately
-- stays STOPPING and the charger is already FAULT, so nothing changes) and wrote a second
-- CHARGER_COMMAND_COMPLETED.
--
-- This table is the command's outcome, not a replay cache, so it has no expiry: one row per
-- command id, written in the same transaction as the state change it describes. It is deliberately
-- small and never pruned, like the outbox rows that record what was sent to the device.
CREATE TABLE IF NOT EXISTS charger_command_outcomes (
    -- The command id is the gateway's idempotency key, so it is the natural primary key here:
    -- one device command has exactly one verdict.
    command_id TEXT PRIMARY KEY,
    charger_id BIGINT NOT NULL REFERENCES chargers (id),
    -- Empty for the station-level RESTART, which belongs to no order.
    order_no TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL CHECK (action IN ('RESTART', 'START_CHARGING', 'STOP_CHARGING')),
    -- Only the frozen device outcome enum may be stored, so the database itself refuses a value the
    -- platform must not act on (a timeout, an unknown status).
    result TEXT NOT NULL CHECK (result IN ('COMPLETED', 'FAILED')),
    -- Whether this verdict changed anything. A verdict that changed nothing is still recorded:
    -- the command has been answered, and a later delivery must be recognised as the same answer
    -- rather than evaluated again.
    applied BOOLEAN NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The recovery sweep and the audit both ask "what did this charger's commands answer", newest
-- first, so the answers are reachable by charger.
CREATE INDEX IF NOT EXISTS idx_charger_command_outcomes_charger
    ON charger_command_outcomes (charger_id, recorded_at DESC);
