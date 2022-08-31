CREATE MATERIALIZED VIEW IF NOT EXISTS fil_location_view
AS
    SELECT * FROM fil_miners_location
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_wt_view
AS
    SELECT * FROM fil_wt
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_un_view
AS
    SELECT * FROM fil_un
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_location_view ON fil_location_view(miner, lat, long, ba, country, city);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_wt_view ON fil_wt_view(ba, date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_un_view ON fil_un_view(country);
