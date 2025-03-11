DROP INDEX IF EXISTS idx_fil_emissions_view_v8;
CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_emissions_view_v8 ON fil_emissions_view_v8(miner, date, country, region, city, wt_value);
