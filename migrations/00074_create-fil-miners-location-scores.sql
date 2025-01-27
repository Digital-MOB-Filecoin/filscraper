CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_location_scores
AS
with minerAverage as (SELECT date,
                             miner,
                             country,
                             greatest(0, SUM((energy_use_kW_estimate - renewable_energy_kW) *
                                             COALESCE(wt_value, (
                                                 COALESCE(
                                                         (SELECT value
                                                          FROM fil_default_grid_marginal_emissions_factors def
                                                          WHERE def.country = f.country),
                                                         (SELECT value
                                                          FROM fil_default_grid_marginal_emissions_factors def
                                                          WHERE def.country = 'N/A')
                                                 )
                                                 )))) as "miner_emissions",
                             SUM(total)               as "miner_data_storage_capacity",
                             greatest(0, SUM((energy_use_kW_estimate - renewable_energy_kW) *
                                             COALESCE(wt_value, (
                                                 COALESCE(
                                                         (SELECT value
                                                          FROM fil_default_grid_marginal_emissions_factors def
                                                          WHERE def.country = f.country),
                                                         (SELECT value
                                                          FROM fil_default_grid_marginal_emissions_factors def
                                                          WHERE def.country = 'N/A')
                                                 )
                                                 )))) /
                             NULLIF(SUM(total), 0)    as sp_ratio
                      FROM fil_miners_data_view_country_v9 f
                      WHERE (date::date >= '2020-08-25'::date)
                        AND (date::date <= now()::date)
                      GROUP BY date, miner, country, wt_value),
     networkAverage as (select date,
                               SUM(miner_emissions) / NULLIF(SUM(miner_data_storage_capacity), 0) as network_average_ratio
                        from minerAverage
                        group by date),
     locationScores as (select minerAverage.date,
                               minerAverage.miner,
                               minerAverage.country,
                               CASE
                                   WHEN sp_ratio IS NULL OR network_average_ratio IS NULL OR sp_ratio = 0 OR
                                        network_average_ratio = 0
                                       THEN 1
                                   ELSE 0.15 + 0.85 * POWER(0.5, sp_ratio / network_average_ratio)
                                   END AS location_score
                        from networkAverage
                                 join minerAverage on networkAverage.date = minerAverage.date
                        where sp_ratio is not null
                          and network_average_ratio > 0)
select date,
    miner,
    avg("location_score") as location_score
from locationScores
group by date, miner
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_miners_location_scores ON fil_miners_location_scores(date, miner);