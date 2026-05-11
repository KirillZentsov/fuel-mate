-- 0003_staging_tables.sql

CREATE TABLE staging.stations (
    id                   BIGSERIAL PRIMARY KEY,
    dump_id              BIGINT NOT NULL REFERENCES raw.fuel_data_dumps(id),
    station_id           TEXT NOT NULL,
    name                 TEXT,
    brand                TEXT,
    postcode             TEXT,
    address              TEXT,
    city                 TEXT,
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION,
    is_supermarket       BOOLEAN,
    is_24h               BOOLEAN,
    is_temp_closed       BOOLEAN,
    is_perm_closed       BOOLEAN,
    opening_hours        JSONB,
    amenities            JSONB,
    forecourt_updated_at TIMESTAMPTZ,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_stg_stations_station_id ON staging.stations (station_id);
CREATE INDEX idx_stg_stations_dump_id    ON staging.stations (dump_id);
CREATE INDEX idx_stg_stations_loaded_at  ON staging.stations (loaded_at DESC);

CREATE TABLE staging.prices (
    id                   BIGSERIAL PRIMARY KEY,
    dump_id              BIGINT NOT NULL REFERENCES raw.fuel_data_dumps(id),
    station_id           TEXT NOT NULL,
    fuel_type            TEXT NOT NULL,
    price_pence          NUMERIC(6,1) NOT NULL,
    forecourt_updated_at TIMESTAMPTZ,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_stg_prices_station_fuel ON staging.prices (station_id, fuel_type);
CREATE INDEX idx_stg_prices_dump_id      ON staging.prices (dump_id);
CREATE INDEX idx_stg_prices_loaded_at    ON staging.prices (loaded_at DESC);

COMMENT ON TABLE staging.stations IS
  'Append-only. Every CSV row creates a new row. Source for historical analysis.';
COMMENT ON TABLE staging.prices IS
  'Append-only. Source of truth for all price data.';
