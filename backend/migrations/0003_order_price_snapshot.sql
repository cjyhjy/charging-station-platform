-- BE-A-04 review: an order must be billed against the price quoted when it
-- was created, not against whatever the charger charges later. The snapshot
-- is taken inside the creation transaction under the charger row lock.
-- Additive: earlier migration checksums are unchanged.

ALTER TABLE charging_orders
    ADD COLUMN IF NOT EXISTS price_per_kwh_cents INTEGER NOT NULL DEFAULT 0 CHECK (price_per_kwh_cents >= 0);
