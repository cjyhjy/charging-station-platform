-- BE-I-01: the authoritative consumption record.
--
-- B-04 established the contract this table implements (internal/event/consumption.go):
-- the record is what makes a repeated delivery safe, because Redis may lose its keys and
-- may be unreachable. Until now that contract had only an in-memory implementation, which
-- means "restart and do not apply the event twice" was not a real guarantee.
--
-- One row per event_id. The state machine mirrors the Go type exactly so the SQL and the
-- contract cannot drift apart silently:
--
--   ''              a live reservation (owner holds the attempt)
--   'DEAD_LETTERING' the current attempt has claimed the decision to park the event
--   'SUCCEEDED'      terminal: applied
--   'DEAD_LETTERED'  terminal: parked on the dead-letter stream
--   'FAILED'         the attempt ran and failed; a redelivery may take the event over
--   'ABANDONED'      the attempt was given back before the event was applied; the next
--                    reservation reuses this attempt's number (see Release)
--
-- Additive migration: no existing file is modified.

CREATE TABLE IF NOT EXISTS event_consumptions (
    event_id        TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL DEFAULT '',
    aggregate_type  TEXT NOT NULL DEFAULT '',
    aggregate_id    TEXT NOT NULL DEFAULT '',
    trace_id        TEXT NOT NULL DEFAULT '',
    -- Stream coordinates: an operator moves from the record back to the exact entry.
    stream          TEXT NOT NULL DEFAULT '',
    stream_id       TEXT NOT NULL DEFAULT '',
    consumer        TEXT NOT NULL DEFAULT '',
    -- delivery_count is the transport's count, recorded for diagnostics only. It is NOT
    -- the retry budget: reclaims inflate it while another lease is held.
    delivery_count  INTEGER NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
    -- attempts is the store's own count, and it is what the retry budget is measured
    -- against. Release deliberately leaves it alone; Fail advances it on the next Begin.
    attempts        INTEGER NOT NULL DEFAULT 1 CHECK (attempts >= 1),
    outcome         TEXT NOT NULL DEFAULT '',
    detail          TEXT NOT NULL DEFAULT '',
    -- owner identifies the attempt that holds the row. It is minted per reservation, so a
    -- stale holder can never finalise, release or park an attempt that took over.
    owner           TEXT NOT NULL DEFAULT '',
    -- lease_until is when another delivery may take the row over. NULL means no lease is
    -- held, which is the case for every terminal outcome and for FAILED/ABANDONED.
    lease_until     TIMESTAMPTZ,
    reserved_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT event_consumptions_outcome_check CHECK (
        outcome IN ('', 'DEAD_LETTERING', 'SUCCEEDED', 'DEAD_LETTERED', 'FAILED', 'ABANDONED')
    )
);

-- Begin must be able to tell a live lease from an expired one for every event that a
-- redelivery asks about, which is the common case under backlog.
CREATE INDEX IF NOT EXISTS idx_event_consumptions_lease
    ON event_consumptions (lease_until)
    WHERE outcome = '' OR outcome = 'DEAD_LETTERING';

-- The dead-letter claim is looked up by owner when the parked entry is written, and the
-- janitor query (records left mid-park by a crashed process) walks the same column.
CREATE INDEX IF NOT EXISTS idx_event_consumptions_outcome
    ON event_consumptions (outcome, updated_at);

-- The publisher reads unpublished rows in a stable order. The existing partial index
-- (next_attempt_at, created_at) does not include the identity, so equal timestamps can
-- order differently between two passes; ordering by id makes the batch deterministic and
-- keeps a retry from skipping a row that was never attempted.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished_identity
    ON outbox_events (next_attempt_at, id)
    WHERE published_at IS NULL;
