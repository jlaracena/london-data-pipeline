-- Forest eBikes — London data pipeline
-- Raw layer DDL: creates all ingestion tables if they don't already exist.
-- Executed once by postgres on first container start.

-- ── raw_weather ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_weather (
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    location_city   VARCHAR(100) NOT NULL,
    temperature_2m  FLOAT,
    windspeed_10m   FLOAT,
    weathercode     INTEGER,
    forecast_date   TIMESTAMPTZ  NOT NULL,
    _source         VARCHAR(50)  NOT NULL,
    CONSTRAINT uq_weather_forecast UNIQUE (forecast_date, location_city)
);

-- ── raw_air_quality ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_air_quality (
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    location_city   VARCHAR(100) NOT NULL,
    location_id     INTEGER      NOT NULL,
    parameter       VARCHAR(50)  NOT NULL,
    value           FLOAT,
    unit            VARCHAR(50),
    measured_at     TIMESTAMPTZ  NOT NULL,
    _source         VARCHAR(50)  NOT NULL,
    CONSTRAINT uq_air_quality_measurement UNIQUE (location_id, parameter, measured_at)
);

-- ── raw_news ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_news (
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    article_id      VARCHAR(32)  NOT NULL,          -- MD5 hex digest of URL
    title           TEXT,
    description     TEXT,
    url             TEXT,
    published_at    TIMESTAMPTZ,
    source_name     VARCHAR(200),
    _source         VARCHAR(50)  NOT NULL,
    CONSTRAINT uq_news_article UNIQUE (article_id)
);

-- ── raw_countries ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_countries (
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    country_code    VARCHAR(10)  NOT NULL,
    country_name    VARCHAR(200),
    capital         VARCHAR(200),
    population      BIGINT,
    area_km2        FLOAT,
    region          VARCHAR(100),
    languages       TEXT[],
    currencies      TEXT[],
    _source         VARCHAR(50)  NOT NULL,
    CONSTRAINT uq_countries_code UNIQUE (country_code)
);

-- ── raw_tfl_bikepoints ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_tfl_bikepoints (
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    snapshot_date   DATE         NOT NULL DEFAULT CURRENT_DATE,
    station_id      VARCHAR(50)  NOT NULL,
    station_name    VARCHAR(200),
    lat             FLOAT,
    lon             FLOAT,
    nb_bikes        INTEGER,
    nb_empty_docks  INTEGER,
    nb_docks        INTEGER,
    _source         VARCHAR(50)  NOT NULL,
    CONSTRAINT uq_tfl_bikepoints UNIQUE (station_id, snapshot_date)
);

-- ── raw_bank_holidays ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_bank_holidays (
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    division        VARCHAR(50)  NOT NULL,
    title           VARCHAR(200) NOT NULL,
    holiday_date    VARCHAR(10)  NOT NULL,  -- YYYY-MM-DD string
    notes           TEXT,
    bunting         BOOLEAN,
    _source         VARCHAR(50)  NOT NULL,
    CONSTRAINT uq_bank_holidays UNIQUE (division, holiday_date, title)
);

-- ── raw_crime ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_crime (
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    crime_id        BIGINT       NOT NULL,
    persistent_id   VARCHAR(100),
    category        VARCHAR(100),
    location_type   VARCHAR(50),
    lat             FLOAT,
    lon             FLOAT,
    street_name     VARCHAR(200),
    outcome_status  VARCHAR(200),
    month           VARCHAR(7),              -- YYYY-MM
    _source         VARCHAR(50)  NOT NULL,
    CONSTRAINT uq_crime UNIQUE (crime_id)
);
