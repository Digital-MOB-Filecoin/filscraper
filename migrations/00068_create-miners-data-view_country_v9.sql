CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_data_view_country_v9
AS
with datapoints as (
SELECT
                    fil_emissions_view_v8.miner,
                    fil_emissions_view_v8.country AS country,
                    region,
                    city,
                    total,
                    total_per_day,
                    un_value,
                    ef_value,
                    wt_value,
                    COALESCE(fil_renewable_energy_view_v5.energyWh / COALESCE(location_count.count, 1), 0) AS renewableEnergyWh,
                    fil_emissions_view_v8.date
                    FROM fil_emissions_view_v8
                    LEFT JOIN fil_renewable_energy_view_v5
                            ON fil_renewable_energy_view_v5.miner = fil_emissions_view_v8.miner
                            AND fil_renewable_energy_view_v5.date = fil_emissions_view_v8.date
                            AND ((fil_renewable_energy_view_v5.country = fil_emissions_view_v8.country OR
                            EXISTS (SELECT 1 FROM fil_country_exceptions WHERE fil_country_exceptions.exception = fil_renewable_energy_view_v5.country AND fil_country_exceptions.country = fil_emissions_view_v8.country)))
                    LEFT JOIN (
                        SELECT miner, country, COUNT(DISTINCT city) AS count
                        FROM fil_emissions_view_v8
                        GROUP BY miner, country
                    ) AS location_count
                    ON fil_emissions_view_v8.miner = location_count.miner
                    AND (fil_emissions_view_v8.country = location_count.country or EXISTS (SELECT 1 FROM fil_country_exceptions WHERE fil_country_exceptions.exception = fil_emissions_view_v8.country AND fil_country_exceptions.country = location_count.country))
                ),
                     data as (SELECT
                          miner,
                          country,
                          region,
                          city,
                          total,
                          total_per_day,
                          un_value,
                          ef_value,
                          wt_value,
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
                                  un_value,
                                  ef_value,
                                  wt_value,
                                  COALESCE(renewableEnergyWh, 0 ) / 1000 as renewable_energy_kW,
                                  ( ROUND( AVG(total) ) * 24 * 0.00000055941949 + SUM(total_per_day) * 0.00843961073) * 1.2 AS energy_use_kW_lower,
                                  ( ROUND( AVG(total) ) * 24 * 0.00000446676 + SUM(total_per_day) * 0.02330019758) * 1.426 AS energy_use_kW_estimate,
                                  ( ROUND( AVG(total) ) * 24 * 0.00001073741 + SUM(total_per_day) * 0.12348030976) * 1.79 AS energy_use_kW_upper
                              FROM datapoints
                              WHERE miner IS NOT NULL AND date IS NOT NULL
                              GROUP BY miner,date, country, region, city, total, total_per_day, wt_value, un_value, ef_value, renewableEnergyWh
                       ) q)
SELECT * FROM data ORDER BY date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miners_data_view_country_v9 ON fil_miners_data_view_country_v9(miner, date, country, region, city, renewable_energy_kW);