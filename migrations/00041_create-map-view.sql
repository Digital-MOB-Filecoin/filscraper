CREATE MATERIALIZED VIEW IF NOT EXISTS fil_map_view
AS
with emissions_data as (SELECT
            country,
            date,
            ROUND(SUM(SUM(cumulative_emissions_estimate)) over (partition by country order by date)) as emissions
            FROM (
                SELECT
                    country,
                    date,
                    greatest(0,SUM((energy_use_kW_estimate - renewable_energy_kW) * COALESCE(avg_wt_value, avg_un_value, 0))) as cumulative_emissions_estimate
                FROM fil_miners_data_view_country_v3
                GROUP BY country, date
            ) q GROUP BY country, date ORDER BY date desc),
    latest_datapoint as (SELECT MAX(date) as date FROM emissions_data),
    total_emissions_data as (SELECT
                            country,
                            emissions
                        FROM emissions_data,
                             latest_datapoint
                        WHERE emissions_data.date = latest_datapoint.date),
    storage_providers_data as (SELECT country, count(miner) as storage_providers FROM fil_location_view GROUP BY country)
    SELECT
            storage_providers_data.country,
            emissions,
            storage_providers
    FROM storage_providers_data
    LEFT JOIN total_emissions_data ON total_emissions_data.country = storage_providers_data.country ORDER BY storage_providers DESC
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_map_view ON fil_map_view(country);