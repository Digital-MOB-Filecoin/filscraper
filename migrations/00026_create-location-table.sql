CREATE TABLE IF NOT EXISTS fil_miners_location
(
    miner text NOT NULL,
    lat float NOT NULL,
    long float NOT NULL,
    ba TEXT,
    ba_date Timestamptz DEFAULT NOW(),
    region TEXT,
    country TEXT,
    city TEXT,
    locations int NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fil_miners_location_miner ON fil_miners_location(miner);
CREATE INDEX IF NOT EXISTS idx_fil_miners_location_ba ON fil_miners_location(ba);
CREATE INDEX IF NOT EXISTS idx_fil_miners_location_lat_long ON fil_miners_location(lat, long);