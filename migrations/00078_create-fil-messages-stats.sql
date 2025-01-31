CREATE MATERIALIZED VIEW IF NOT EXISTS fil_messages_stats
AS
with messages AS (SELECT day, message_count from fil_messages_per_day),
     energy_per_day AS (with capacity as (SELECT ROUND(AVG(total_per_day))           AS total_per_day,
                                                 ROUND(AVG(total))                   AS total,
                                                 DATE_TRUNC('day', date::date)::date AS date
                                          FROM (SELECT date,
                                                       SUM(total_per_day) AS total_per_day,
                                                       SUM(total)         AS total
                                                FROM fil_miners_data_view_country_v9
                                                WHERE (date::date >= '2020-08-25'::date)
                                                  AND (date::date <= now()::date)
                                                GROUP BY date) q1
                                          GROUP BY date),
                             energy as (SELECT date,
                                               SUM(
                                               (total * 24 * 0.00000055941949 + SUM(total_per_day) * 0.00843961073) *
                                               1.2)
                                               OVER (ORDER BY date) as "energy_use_kW_lower",
                                               SUM((total * 24 * 0.00000446676 + SUM(total_per_day) * 0.02330019758) *
                                                   1.426)
                                               OVER (ORDER BY date) as "energy_use_kW_estimate",
                                               SUM(
                                               (total * 24 * 0.00001073741 + SUM(total_per_day) * 0.12348030976) * 1.79)
                                               OVER (ORDER BY date) as "energy_use_kW_upper"
                                        FROM capacity
                                        GROUP BY date, total, total_per_day
                                        ORDER BY date)
                        select date,
                               coalesce("energy_use_kW_lower" - LAG("energy_use_kW_lower") OVER (ORDER BY date),
                                        "energy_use_kW_lower")    as "energy_use_kW_lower",
                               coalesce("energy_use_kW_estimate" - LAG("energy_use_kW_estimate") OVER (ORDER BY date),
                                        "energy_use_kW_estimate") as "energy_use_kW_estimate",
                               coalesce("energy_use_kW_upper" - LAG("energy_use_kW_upper") OVER (ORDER BY date),
                                        "energy_use_kW_upper")    as "energy_use_kW_upper"
                        from energy),
     emissions_per_day as (SELECT SUM((energy_use_kW_lower - renewable_energy_kW) *
                                      (CAST(COALESCE(ef_value, un_value, 436) AS decimal))) as emissions_lower,
                                  SUM((energy_use_kW_estimate - renewable_energy_kW) *
                                      (CAST(COALESCE(ef_value, un_value, 436) AS decimal))) as emissions_estimate,
                                  SUM((energy_use_kW_upper - renewable_energy_kW) *
                                      (CAST(COALESCE(ef_value, un_value, 436) AS decimal))) as emissions_upper,
                                  date
                           FROM fil_miners_data_view_country_v9
                           WHERE (date::date >= '2020-07-25'::date)
                             AND (date::date <= now()::date)
                           GROUP BY date)
select m.day                                    as date,
       "energy_use_kW_lower" / message_count    as "energy_kw_lower_per_messages",
       "energy_use_kW_estimate" / message_count as "energy_kw_estimate_per_messages",
       "energy_use_kW_upper" / message_count    as "energy_kw_upper_per_messages",
       "emissions_lower" / message_count        as "emissions_lower_per_messages",
       "emissions_estimate" / message_count     as "emissions_estimate_per_messages",
       "emissions_upper" / message_count        as "emissions_upper_per_messages"
from messages m
    left join energy_per_day e on m.day = e.date
    left join emissions_per_day em on m.day = em.date
order by day
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_stats ON fil_messages_stats(date);