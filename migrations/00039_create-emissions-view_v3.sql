CREATE MATERIALIZED VIEW IF NOT EXISTS fil_emissions_view_v3
AS
with data as (SELECT
       miner,
       ba,
       fil_location_view.country,
       locations,
       fil_un_view.value as un_value
FROM fil_location_view
    LEFT OUTER JOIN fil_un_view ON fil_location_view.country = fil_un_view.country),
    dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-08-25'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
    data_points as ( SELECT data.miner, data.ba, data.country, data.locations, data.un_value, dates.date FROM data CROSS JOIN dates),
    wt_un_data as ( SELECT
       miner,
       data_points.ba,
       country,
       locations,
       un_value,
       fil_wt_view.value as wt_value,
       data_points.date
    FROM data_points
    LEFT OUTER JOIN fil_wt_view ON data_points.ba = fil_wt_view.ba and data_points.date = fil_wt_view.date),
    emissions_data as (SELECT
       miner,
       locations,
       sum(wt_value) / locations as avg_wt_value,
       sum(un_value) / locations as avg_un_value,
       date
    FROM wt_un_data
    GROUP BY miner,locations,date),
     capacity_data as (SELECT emissions_data.miner,
                              emissions_data.date,
                              (COALESCE(LEAST(fil_miner_view_days_lily_v1.total_per_day, fil_miner_view_days_v4.total_per_day), 0) / locations) as total_per_day,
                              (COALESCE(fil_miner_view_days_lily_v1.total, fil_miner_view_days_v4.total, 0) / locations) as total
                        FROM emissions_data
                        FULL JOIN fil_miner_view_days_lily_v1 ON emissions_data.miner = fil_miner_view_days_lily_v1.miner AND emissions_data.date = fil_miner_view_days_lily_v1.date
                        FULL JOIN fil_miner_view_days_v4 ON emissions_data.miner = fil_miner_view_days_v4.miner AND emissions_data.date = fil_miner_view_days_v4.date)
    SELECT
           emissions_data.miner,
           avg_wt_value,
           avg_un_value,
           total_per_day,
           total,
           emissions_data.date
    FROM emissions_data
    FULL JOIN capacity_data ON capacity_data.miner = emissions_data.miner AND capacity_data.date = emissions_data.date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_emissions_view_v3 ON fil_emissions_view_v3(miner, date);