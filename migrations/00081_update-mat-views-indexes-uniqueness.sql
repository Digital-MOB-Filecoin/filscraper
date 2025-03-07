DROP INDEX IF EXISTS idx_fil_location_view;
CREATE UNIQUE INDEX  idx_fil_location_view ON fil_location_view(miner, lat, long, ba, country, city, region);

DROP INDEX IF EXISTS idx_fil_miners_data_view_country_v9;
CREATE UNIQUE INDEX idx_fil_miners_data_view_country_v9 ON fil_miners_data_view_country_v9(miner, date, country, region, city, renewable_energy_kW, wt_value);