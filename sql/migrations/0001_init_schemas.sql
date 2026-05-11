-- 0001_init_schemas.sql

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS app;

COMMENT ON SCHEMA raw     IS 'Metadata about CSV dumps in GitHub Releases';
COMMENT ON SCHEMA staging IS 'Append-only history of all parsed records';
COMMENT ON SCHEMA mart    IS 'Current state + history for app and BI';
COMMENT ON SCHEMA app     IS 'Bot operational data: users, favourites, alerts';
