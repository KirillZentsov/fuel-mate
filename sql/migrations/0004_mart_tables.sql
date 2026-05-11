-- 0004_mart_tables.sql

CREATE TABLE mart.stations (
    station_id           TEXT PRIMARY KEY,
    name                 TEXT,
    brand                TEXT,
    postcode             TEXT,
    address              TEXT,
    city                 TEXT,
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION,
    is_supermarket       BOOLEAN DEFAULT FALSE,
    is_24h               BOOLEAN DEFAULT FALSE,
    is_temp_closed       BOOLEAN DEFAULT FALSE,
    is_perm_closed       BOOLEAN DEFAULT FALSE,
    opening_hours        JSONB,
    amenities            JSONB,
    forecourt_updated_at TIMESTAMPTZ,
    last_updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_mart_stations_postcode ON mart.stations (postcode);
CREATE INDEX idx_mart_stations_brand    ON mart.stations (brand);
CREATE INDEX idx_mart_stations_geo      ON mart.stations (latitude, longitude);

CREATE TABLE mart.prices_current (
    station_id           TEXT NOT NULL REFERENCES mart.stations(station_id),
    fuel_type            TEXT NOT NULL,
    price_pence          NUMERIC(6,1) NOT NULL,
    forecourt_updated_at TIMESTAMPTZ,
    last_updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, fuel_type)
);

CREATE INDEX idx_mart_prices_current_fuel ON mart.prices_current (fuel_type);

CREATE TABLE mart.prices_history (
    id                   BIGSERIAL PRIMARY KEY,
    station_id           TEXT NOT NULL REFERENCES mart.stations(station_id),
    fuel_type            TEXT NOT NULL,
    price_pence          NUMERIC(6,1) NOT NULL,
    forecourt_updated_at TIMESTAMPTZ,
    recorded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    dump_id              BIGINT NOT NULL REFERENCES raw.fuel_data_dumps(id)
);

CREATE INDEX idx_mart_history_station_fuel_time
    ON mart.prices_history (station_id, fuel_type, recorded_at DESC);
CREATE INDEX idx_mart_history_recorded_at
    ON mart.prices_history (recorded_at DESC);

COMMENT ON TABLE mart.stations IS
  'Latest snapshot. UPSERT from staging. Historical versions in staging.stations.';
COMMENT ON TABLE mart.prices_current IS 'Latest price per station × fuel.';
COMMENT ON TABLE mart.prices_history IS
  'Only actual price changes. Source for BI trend analysis.';
