CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_emission_scores
AS
with networkAverage as (SELECT date,
                               greatest(0, SUM((energy_use_kW_estimate - renewable_energy_kW) *
                                               (CAST(COALESCE(ef_value, un_value, 436) AS decimal)))) /
                               NULLIF(SUM(total), 0) as network_average_ratio
                        FROM fil_miners_data_view_country_v9
                        WHERE (date::date >= '2020-08-25'::date)
                          AND (date::date <= now()::date)
                        GROUP BY date),
     minerAverage as (SELECT date,
                             miner,
                             greatest(0, SUM((energy_use_kW_estimate - renewable_energy_kW) *
                                             (CAST(COALESCE(ef_value, un_value, 436) AS decimal)))) /
                             NULLIF(SUM(total), 0) as sp_ratio
                      FROM fil_miners_data_view_country_v9
                      WHERE (date::date >= '2020-08-25'::date)
                        AND (date::date <= now()::date)
                      GROUP BY date, miner)
select minerAverage.date,
       minerAverage.miner,
       CASE
           WHEN sp_ratio IS NULL OR network_average_ratio IS NULL OR sp_ratio = 0 OR network_average_ratio = 0
               THEN 0
           ELSE POWER(0.5, sp_ratio / network_average_ratio)
           END AS "emission_score"
from networkAverage
         join minerAverage on networkAverage.date = minerAverage.date
where sp_ratio is not null and network_average_ratio > 0
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_miners_emission_scores ON fil_miners_emission_scores(date, miner);