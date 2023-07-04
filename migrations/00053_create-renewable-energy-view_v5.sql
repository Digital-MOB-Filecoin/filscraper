CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_view_v5
AS
             SELECT miner,
                country,
                date,
                SUM(COALESCE(fil_renewable_energy.energyWh, 0)) as energyWh
         FROM fil_renewable_energy
         GROUP BY miner, country, date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_view_v5 ON fil_renewable_energy_view_v5(miner, date, country);