CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_energy_re_share
AS
WITH miner_data AS (
    SELECT
        miner,
        country,
        date::date AS day,
        SUM(total) AS total_aggregated
    FROM fil_miners_data_view_country_v9
    WHERE date::date BETWEEN '2020-08-25' AND now()::date
    GROUP BY miner, country, date::date
), miner_sealed AS (
    SELECT
        miner,
        date::date AS day,
        SUM("sealed_GiB") AS sealed_GiB_aggregated
    FROM fil_sealed_capacity_view_v2
    WHERE date::date BETWEEN '2020-08-25' AND now()::date
    GROUP BY miner, date::date
), miner_combined AS (
    SELECT
        coalesce(m.day, s.day) as day,
        coalesce(m.miner, s.miner) as miner,
        m.country,
        coalesce(m.total_aggregated, 0) as total_aggregated,
        COALESCE(s.sealed_GiB_aggregated, 0) AS sealed_GiB_aggregated
    FROM miner_data m
             FULL OUTER JOIN miner_sealed s
                       ON m.miner = s.miner AND m.day = s.day
), miner_energy_calculations AS (
    SELECT
        mc.day,
        mc.miner,
        mc.country,
        mc.total_aggregated,
        mc.sealed_GiB_aggregated,
        COALESCE(
                (SELECT value
                 FROM fil_re_share_electricity_capacity_2023 re
                 WHERE re.country = mc.country),
                (SELECT value
                 FROM fil_re_share_electricity_capacity_2023 re
                 WHERE re.country = 'N/A')
        ) AS re_share_percentage,
        (mc.total_aggregated * 0.00000055941949 + mc.sealed_GiB_aggregated * 0.00035165044) * 1.2 AS "total_energy_kW_lower",
        (mc.total_aggregated * 0.00000446676 + mc.sealed_GiB_aggregated * 0.00097084156) * 1.426 AS "total_energy_kW_estimate",
        (mc.total_aggregated * 0.00001073741 + mc.sealed_GiB_aggregated * 0.0051450129) * 1.79 AS "total_energy_kW_upper"
    FROM miner_combined mc
), miner_energy_calculations_with_re_share AS (
    select *,
           "total_energy_kW_lower" * re_share_percentage / 100 AS "total_energy_kW_lower_re_share",
           "total_energy_kW_estimate" * re_share_percentage / 100 AS "total_energy_kW_estimate_re_share",
           "total_energy_kW_upper" * re_share_percentage / 100 AS "total_energy_kW_upper_re_share"
    from miner_energy_calculations
)
select * from miner_energy_calculations_with_re_share
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_miners_energy_re_share ON fil_miners_energy_re_share(day, country, miner);