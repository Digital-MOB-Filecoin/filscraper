CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_days_lily_v1
AS
with
     capacity_data_lily as ( SELECT
                            miner,
                            (power / 1073741824) as total,
                            greatest(0, (power - COALESCE(LEAD(power, -1) OVER (ORDER BY date), 0)) / 1073741824)  AS total_per_day,
                            date
                            FROM
                            fil_miner_days_lily order by date),
     miners as (SELECT miner as miner FROM fil_miners_view_v3),
     dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-10-15'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
     data_points as ( SELECT miners.miner, dates.date FROM miners CROSS JOIN dates)
     SELECT data_points.miner,
            data_points.date,
            COALESCE(capacity_data_lily.total_per_day, 0) as total,
            COALESCE(capacity_data_lily.total_per_day, 0) as total_per_day
    FROM data_points
    LEFT JOIN capacity_data_lily ON data_points.miner = capacity_data_lily.miner AND data_points.date = capacity_data_lily.date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_miner_view_days_lily_v1 ON fil_miner_view_days_lily_v1(miner, date);