-- BE-A-04 review: time-of-use settlement requires the full tariff snapshot
-- on the order, taken when charging starts: off-peak price and window in
-- addition to the peak electricity price and service fee from 0004.
-- NULL off-peak columns mean the charger has no time-of-use tariff.
-- Additive: earlier migration checksums are unchanged.

ALTER TABLE charging_orders
    ADD COLUMN IF NOT EXISTS off_peak_price_per_kwh_cents INTEGER,
    ADD COLUMN IF NOT EXISTS off_peak_start_hour SMALLINT,
    ADD COLUMN IF NOT EXISTS off_peak_end_hour SMALLINT;
