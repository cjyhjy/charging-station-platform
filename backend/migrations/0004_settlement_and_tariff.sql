-- BE-A-04 review: settlement, timing and tariff structure.
-- Additive: earlier migration checksums are unchanged.

-- The start request timestamp bounds the STARTING window independently of
-- the order creation time (an order created 14 minutes ago and started then
-- must not time out one minute later).
ALTER TABLE charging_orders
    ADD COLUMN IF NOT EXISTS start_requested_at TIMESTAMPTZ;

-- Pending-payment tracking (UC-U-09): completing a charge freezes the bill,
-- writes the bill detail and records the payment state without touching the
-- wallet. Deduction happens when the user confirms the order; an unsettled
-- (PENDING) order blocks new flows per the "at most one unsettled order"
-- requirement.
ALTER TABLE charging_orders
    ADD COLUMN IF NOT EXISTS paid_cents BIGINT NOT NULL DEFAULT 0 CHECK (paid_cents >= 0),
    ADD COLUMN IF NOT EXISTS payment_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (payment_status IN ('PENDING', 'PAID', 'PARTIAL_PAID')),
    ADD COLUMN IF NOT EXISTS service_price_per_kwh_cents INTEGER NOT NULL DEFAULT 0
        CHECK (service_price_per_kwh_cents >= 0);

-- BR-05 fee structure: the bill is energy x (electricity snapshot + service
-- snapshot). The optional off-peak window carries the time-of-use tariff;
-- a charger without the window simply uses the peak price at all hours.
ALTER TABLE chargers
    ADD COLUMN IF NOT EXISTS service_price_per_kwh_cents INTEGER NOT NULL DEFAULT 0 CHECK (service_price_per_kwh_cents >= 0),
    ADD COLUMN IF NOT EXISTS off_peak_electricity_price_per_kwh_cents INTEGER,
    ADD COLUMN IF NOT EXISTS off_peak_start_hour SMALLINT,
    ADD COLUMN IF NOT EXISTS off_peak_end_hour SMALLINT;
