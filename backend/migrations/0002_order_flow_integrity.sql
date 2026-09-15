-- BE-A-04: order flow integrity.
-- UC-U-07 requires flow uniqueness per user ("活动流程唯一性"); the partial
-- unique index gives PostgreSQL the final concurrency guarantee, mirroring
-- uq_orders_charger_active. Additive: 0001 checksums are unchanged.

CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_user_active
ON charging_orders (user_id)
WHERE status IN ('CREATED', 'STARTING', 'CHARGING', 'STOPPING');
