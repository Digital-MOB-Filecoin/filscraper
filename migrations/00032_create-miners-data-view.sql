CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_data_view
AS
with datapoints as (
                    SELECT
                        fil_emissions_view.miner,
                        fil_emissions_view.total,
                        fil_emissions_view.total_per_day,
                        fil_emissions_view.avg_un_value,
                        fil_emissions_view.avg_wt_value,
                        fil_renewable_energy_view_v3.energywh as renewableEnergyWh,
                        fil_emissions_view.date
                    FROM fil_emissions_view
                    left join fil_renewable_energy_view_v3 ON fil_renewable_energy_view_v3.miner = fil_emissions_view.miner and fil_renewable_energy_view_v3.date = fil_emissions_view.date
                ),
                     data as (SELECT
                          miner,
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
                              GROUP BY miner,date, total, total_per_day, avg_wt_value, avg_un_value, renewableEnergyWh
                       ) q)
SELECT * FROM data ORDER BY date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miners_data_view ON fil_miners_data_view(miner, date);