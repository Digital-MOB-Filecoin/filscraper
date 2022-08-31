CREATE MATERIALIZED VIEW IF NOT EXISTS fil_emissions_view
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
       sum(wt_value) / locations as avg_wt_value,
       sum(un_value) / locations as avg_un_value,
       date
    FROM wt_un_data
    GROUP BY miner,locations,date),
     capacity_data as (SELECT emissions_data.miner,
                              emissions_data.date,
                              COALESCE(fil_miner_view_days.total_per_day, 0) as total_per_day
                        FROM emissions_data
                        FULL JOIN fil_miner_view_days ON emissions_data.miner = fil_miner_view_days.miner AND emissions_data.date = fil_miner_view_days.date),
    total_capacity_data as (SELECT capacity_data.miner,
                                   capacity_data.date,
                                   capacity_data.total_per_day,
                                   SUM(SUM(capacity_data.total_per_day)) OVER (PARTITION BY miner ORDER BY date) as total
                            FROM capacity_data GROUP BY miner,date,total_per_day
         )
    SELECT
           emissions_data.miner,
           avg_wt_value,
           avg_un_value,
           total_per_day,
           total,
           emissions_data.date
    FROM emissions_data
    FULL JOIN total_capacity_data ON total_capacity_data.miner = emissions_data.miner AND total_capacity_data.date = emissions_data.date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_emissions_view ON fil_emissions_view(miner, date);