CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_days_v4
AS
with
     miners as (SELECT miner as miner FROM fil_miners_view_v3),
     dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-08-25'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
     data_points as ( SELECT miners.miner, dates.date FROM miners CROSS JOIN dates),
     capacity_data as (SELECT data_points.miner,
                              data_points.date,
                              COALESCE(fil_miner_view_days.total_per_day, 0) as total_per_day
                        FROM data_points
                        FULL JOIN fil_miner_view_days ON data_points.miner = fil_miner_view_days.miner AND data_points.date = fil_miner_view_days.date)

     SELECT capacity_data.miner,
            capacity_data.date,
            capacity_data.total_per_day,
            SUM(SUM(capacity_data.total_per_day)) OVER (PARTITION BY miner ORDER BY date) as total
     FROM capacity_data GROUP BY miner,date,total_per_day
WITH DATA;