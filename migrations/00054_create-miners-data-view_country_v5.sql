CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_data_view_country_v5
AS
with datapoints as (
                    SELECT
                        fil_emissions_view_v4.miner,
                        fil_emissions_view_v4.country AS country,
                        region,
                        city,
                        total,
                        total_per_day,
                        avg_un_value,
                        avg_wt_value,
                        COALESCE(fil_renewable_energy_view_v5.energyWh / COALESCE(location_count.count, 1), 0) AS renewableEnergyWh,
                        fil_emissions_view_v4.date
                    FROM fil_emissions_view_v4
                    LEFT JOIN fil_renewable_energy_view_v5
                    ON fil_renewable_energy_view_v5.miner = fil_emissions_view_v4.miner
                    AND fil_renewable_energy_view_v5.date = fil_emissions_view_v4.date
                    AND fil_renewable_energy_view_v5.country = fil_emissions_view_v4.country
                    LEFT JOIN (
                        SELECT miner, country, COUNT(DISTINCT city) AS count
                        FROM fil_emissions_view_v4
                        GROUP BY miner, country
                    ) AS location_count
                    ON fil_emissions_view_v4.miner = location_count.miner
                    AND fil_emissions_view_v4.country = location_count.country
                ),
                     data as (SELECT
                          miner,
                          country,
                          region,
                          city,
                          total,
                          total_per_day,
                          avg_un_value,
                          avg_wt_value,
                          renewable_energy_kW,
                          energy_use_kW_lower,
                          energy_use_kW_estimate,
                          energy_use_kW_upper,
                          date
                          FROM (
                              SELECT
                                  date,
                                  miner,
                                  country,
                                  region,
                                  city,
                                  total,
                                  total_per_day,
                                  avg_un_value,
                                  avg_wt_value,
                                  COALESCE(renewableEnergyWh, 0 ) / 1000 as renewable_energy_kW,
                                  ( ROUND( AVG(total) ) * 24 * 0.0000009688 + SUM(total_per_day) * 0.0064516254) * 1.18 AS energy_use_kW_lower,
                                  ( ROUND( AVG(total) ) * 24 * 0.0000032212 + SUM(total_per_day) * 0.0366833157) * 1.57 AS energy_use_kW_estimate,
                                  ( ROUND( AVG(total) ) * 24 * 0.0000086973 + SUM(total_per_day) * 0.0601295421) * 1.93 AS energy_use_kW_upper
                              FROM datapoints
                              WHERE miner IS NOT NULL AND date IS NOT NULL
                              GROUP BY miner,date, country, region, city, total, total_per_day, avg_wt_value, avg_un_value, renewableEnergyWh
                       ) q)
SELECT * FROM data ORDER BY date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miners_data_view_country_v5 ON fil_miners_data_view_country_v5(miner, date, country, region, city);