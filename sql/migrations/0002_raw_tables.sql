-- 0002_raw_tables.sql

CREATE TABLE raw.fuel_data_dumps (
    id              BIGSERIAL PRIMARY KEY,
    file_name       TEXT NOT NULL,
    release_url     TEXT,
    sha256          CHAR(64) NOT NULL,
    row_count       INTEGER NOT NULL,
    forecourt_min_ts TIMESTAMPTZ,
    forecourt_max_ts TIMESTAMPTZ,
    downloaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sha256)
);

CREATE INDEX idx_raw_dumps_downloaded_at
    ON raw.fuel_data_dumps (downloaded_at DESC);

COMMENT ON TABLE raw.fuel_data_dumps IS
  'One row per successful CSV download. sha256 prevents duplicate loads.';
