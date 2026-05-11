-- 0006_views.sql

-- Wide view: prices history with station details (BI primary)
CREATE OR REPLACE VIEW mart.vw_prices_history AS
SELECT
    h.id,
    h.station_id,
    s.name              AS station_name,
    s.brand,
    s.postcode,
    s.city,
    s.latitude,
    s.longitude,
    h.fuel_type,
    h.price_pence,
    h.forecourt_updated_at,
    h.recorded_at,
    DATE(h.recorded_at AT TIME ZONE 'Europe/London') AS recorded_date_uk,
    EXTRACT(HOUR FROM h.recorded_at AT TIME ZONE 'Europe/London') AS recorded_hour_uk,
    s.is_supermarket,
    s.is_24h
FROM mart.prices_history h
JOIN mart.stations s ON s.station_id = h.station_id;

COMMENT ON VIEW mart.vw_prices_history IS
  'BI-primary: every recorded price change with full station context.';

-- Current snapshot for BI
CREATE OR REPLACE VIEW mart.vw_prices_current AS
SELECT
    s.station_id,
    s.name AS station_name,
    s.brand,
    s.postcode,
    s.city,
    s.latitude,
    s.longitude,
    s.is_supermarket,
    s.is_24h,
    s.is_temp_closed,
    s.is_perm_closed,
    p.fuel_type,
    p.price_pence,
    p.forecourt_updated_at,
    p.last_updated_at
FROM mart.stations s
LEFT JOIN mart.prices_current p ON p.station_id = s.station_id;

COMMENT ON VIEW mart.vw_prices_current IS
  'Current state of all stations with their latest prices (one row per station × fuel).';

-- Daily aggregates per brand (BI dashboards)
CREATE OR REPLACE VIEW mart.vw_daily_avg_prices AS
SELECT
    DATE(recorded_at AT TIME ZONE 'Europe/London') AS price_date,
    fuel_type,
    s.brand,
    COUNT(DISTINCT h.station_id) AS station_count,
    ROUND(AVG(price_pence)::numeric, 1) AS avg_price_pence,
    MIN(price_pence) AS min_price_pence,
    MAX(price_pence) AS max_price_pence,
    ROUND(STDDEV(price_pence)::numeric, 2) AS stddev_price_pence
FROM mart.prices_history h
JOIN mart.stations s ON s.station_id = h.station_id
WHERE s.brand IS NOT NULL
GROUP BY 1, 2, 3;

COMMENT ON VIEW mart.vw_daily_avg_prices IS
  'Daily price aggregates per fuel × brand. Useful for trend dashboards.';
