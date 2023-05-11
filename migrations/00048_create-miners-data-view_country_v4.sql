CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_data_view_country_v4
AS
with datapoints as (
                    SELECT
                        fil_emissions_view_v3.miner,
                        fil_renewable_energy_view_v4.country as country,
                        region,
                        city,
                        total,
                        total_per_day,
                        avg_un_value,
                        avg_wt_value,
                        energywh as renewableEnergyWh,
                        fil_emissions_view_v3.date
                    FROM fil_emissions_view_v3
                    left join fil_renewable_energy_view_v4 ON fil_renewable_energy_view_v4.miner = fil_emissions_view_v3.miner and fil_renewable_energy_view_v4.date = fil_emissions_view_v3.date and fil_renewable_energy_view_v4.country = fil_emissions_view_v3.country
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miners_data_view_country_v4 ON fil_miners_data_view_country_v4(miner, date, country, region, city);