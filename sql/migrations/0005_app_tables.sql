-- 0005_app_tables.sql

CREATE TABLE app.users (
    user_id         BIGINT PRIMARY KEY,
    fuel_type       TEXT NOT NULL DEFAULT 'e10',
    search_radius   INTEGER NOT NULL DEFAULT 5,
    alerts_mode     TEXT NOT NULL DEFAULT 'big_only',
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_app_users_active ON app.users (is_active) WHERE is_active = TRUE;

COMMENT ON COLUMN app.users.fuel_type IS
  'One of: e10, e5, b7s, b7p';
COMMENT ON COLUMN app.users.alerts_mode IS
  'One of: all_changes, big_only (default, threshold 2p), off';

CREATE TABLE app.favourites (
    user_id                 BIGINT NOT NULL REFERENCES app.users(user_id) ON DELETE CASCADE,
    station_id              TEXT NOT NULL REFERENCES mart.stations(station_id),
    last_notified_e10       NUMERIC(6,1),
    last_notified_e5        NUMERIC(6,1),
    last_notified_b7s       NUMERIC(6,1),
    last_notified_b7p       NUMERIC(6,1),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, station_id)
);

CREATE INDEX idx_app_fav_user ON app.favourites (user_id);

COMMENT ON COLUMN app.favourites.last_notified_e10 IS
  'Price snapshot from when last alert was sent. Used for change detection.';

CREATE TABLE app.alerts_log (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    station_id      TEXT NOT NULL,
    fuel_type       TEXT NOT NULL,
    old_price_pence NUMERIC(6,1),
    new_price_pence NUMERIC(6,1),
    delta_pence     NUMERIC(6,1) GENERATED ALWAYS AS (new_price_pence - old_price_pence) STORED,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivery_status TEXT NOT NULL DEFAULT 'sent'
);

CREATE INDEX idx_alerts_user_time ON app.alerts_log (user_id, sent_at DESC);
CREATE INDEX idx_alerts_sent_at   ON app.alerts_log (sent_at DESC);

COMMENT ON COLUMN app.alerts_log.delivery_status IS
  'One of: sent, failed, skipped';
