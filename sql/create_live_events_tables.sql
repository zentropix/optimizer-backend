-- ============================================================
-- 1. Offers lookup table
-- ============================================================
CREATE TABLE IF NOT EXISTS public.voluum_offers (
    offer_id       VARCHAR(64) PRIMARY KEY,
    offer_name     TEXT,
    created_at     TIMESTAMP DEFAULT NOW(),
    updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_voluum_offers_offer_id
    ON public.voluum_offers (offer_id);

-- ============================================================
-- 2. Campaigns lookup table
-- ============================================================
CREATE TABLE IF NOT EXISTS public.voluum_campaigns (
    campaign_id    VARCHAR(64) PRIMARY KEY,
    campaign_name  TEXT,
    created_at     TIMESTAMP DEFAULT NOW(),
    updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_voluum_campaigns_campaign_id
    ON public.voluum_campaigns (campaign_id);

-- ============================================================
-- 3. Live events table (deduplicated by custom_variable_1)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.voluum_live_events (
    id                  BIGSERIAL PRIMARY KEY,
    click_id            VARCHAR(64),
    campaign_id         VARCHAR(64),
    campaign_name       TEXT,
    offer_id            VARCHAR(64),
    offer_name          TEXT,
    timestamp           TIMESTAMP,
    external_id         TEXT,
    traffic_source_id   VARCHAR(64),
    lander_id           VARCHAR(64),
    affiliate_network_id VARCHAR(64),
    brand               TEXT,
    browser             TEXT,
    browser_version     TEXT,
    device              TEXT,
    model               TEXT,
    os                  TEXT,
    os_version          TEXT,
    city                TEXT,
    region              TEXT,
    country_code        VARCHAR(10),
    isp                 TEXT,
    ip                  TEXT,
    referrer            TEXT,
    url                 TEXT,
    user_agent          TEXT,
    language            TEXT,
    mobile_carrier      TEXT,
    connection_type     TEXT,
    flow_id             VARCHAR(64),
    path_id             VARCHAR(64),
    custom_variable_1   TEXT NOT NULL,
    custom_variable_2   TEXT,
    custom_variable_3   TEXT,
    custom_variable_4   TEXT,
    custom_variable_5   TEXT,
    custom_variable_6   TEXT,
    custom_variable_7   TEXT,
    custom_variable_8   TEXT,
    custom_variable_9   TEXT,
    custom_variable_10  TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (custom_variable_1)
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_voluum_live_events_offer_id
    ON public.voluum_live_events (offer_id);

CREATE INDEX IF NOT EXISTS idx_voluum_live_events_campaign_id
    ON public.voluum_live_events (campaign_id);

CREATE INDEX IF NOT EXISTS idx_voluum_live_events_timestamp
    ON public.voluum_live_events (timestamp);

CREATE INDEX IF NOT EXISTS idx_voluum_live_events_country_code
    ON public.voluum_live_events (country_code);

CREATE INDEX IF NOT EXISTS idx_voluum_live_events_custom_variable_1
    ON public.voluum_live_events (custom_variable_1);

-- ============================================================
-- 4. Raw live SNS data (stores everything as-is, no dedup)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.raw_live_voluum_sns_data (
    id                  BIGSERIAL PRIMARY KEY,
    click_id            VARCHAR(64),
    campaign_id         VARCHAR(64),
    offer_id            VARCHAR(64),
    timestamp           TIMESTAMP,
    external_id         TEXT,
    traffic_source_id   VARCHAR(64),
    lander_id           VARCHAR(64),
    affiliate_network_id VARCHAR(64),
    brand               TEXT,
    browser             TEXT,
    browser_version     TEXT,
    device              TEXT,
    model               TEXT,
    os                  TEXT,
    os_version          TEXT,
    city                TEXT,
    region              TEXT,
    country_code        VARCHAR(10),
    isp                 TEXT,
    ip                  TEXT,
    referrer            TEXT,
    custom_variables    TEXT[],
    url                 TEXT,
    user_agent          TEXT,
    language            TEXT,
    mobile_carrier      TEXT,
    connection_type     TEXT,
    flow_id             VARCHAR(64),
    path_id             VARCHAR(64),
    created_at          TIMESTAMP DEFAULT NOW()
);
