CREATE MATERIALIZED VIEW IF NOT EXISTS fil_emissions_view_v4
AS
with data as (SELECT
       fil_location_view.miner,
       ba,
       fil_location_view.country,
       fil_location_view.region,
       fil_location_view.city,
       locations,
       fil_un_view.value as un_value
FROM fil_location_view
    LEFT JOIN fil_un_view ON fil_location_view.country = fil_un_view.country),
    dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-01-01'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
    data_points as ( SELECT data.miner, data.ba, data.country, data.region, data.city, data.locations, data.un_value, dates.date FROM data CROSS JOIN dates),
    capacity_data as ( SELECT
       data_points.miner,
       data_points.ba,
       country,
       region,
       city,
       locations,
       un_value,
       fil_wt_view.value as wt_value,
       (COALESCE(LEAST(fil_miner_view_days_lily_v1.total_per_day, fil_miner_view_days_v4.total_per_day), 0) / COALESCE(locations, 1)) as total_per_day,
       (COALESCE(fil_miner_view_days_lily_v1.total, fil_miner_view_days_v4.total, 0) / COALESCE(locations, 1)) as total,
       data_points.date
    FROM data_points
    LEFT JOIN fil_wt_view ON data_points.ba = fil_wt_view.ba and data_points.date = fil_wt_view.date
    LEFT JOIN fil_miner_view_days_lily_v1 ON data_points.miner = fil_miner_view_days_lily_v1.miner AND data_points.date = fil_miner_view_days_lily_v1.date
    LEFT JOIN fil_miner_view_days_v4 ON data_points.miner = fil_miner_view_days_v4.miner AND data_points.date = fil_miner_view_days_v4.date)
    SELECT
           miner,
           total_per_day,
           total,
           country,
           region,
           city,
           locations,
           sum(wt_value) / locations as avg_wt_value,
           sum(un_value) / locations as avg_un_value,
           capacity_data.date
    FROM capacity_data
    GROUP BY miner, locations, country, region, city, date, total_per_day, total
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_emissions_view_v4 ON fil_emissions_view_v4(miner, date, country, region, city);