CREATE MATERIALIZED VIEW IF NOT EXISTS fil_network_view_epochs_v2
AS
select epoch,
        SUM(commited_per_epoch) as commited_per_epoch,
        SUM(used_per_epoch) as used_per_epoch,
        ( SUM(used_per_epoch) / (SUM(NULLIF(commited_per_epoch,0)) + SUM(NULLIF(used_per_epoch,0))) ) as fraction_per_epoch,
        ( (SUM(commited_per_epoch) + SUM(used_per_epoch)) / 1073741824) as total_per_epoch,
        SUM(commited) as commited,
        SUM(used) as used,
        SUM(total) as total,
        to_timestamp(1598281200 + epoch * 30) as timestamp
    from fil_miner_view_epochs_v2 GROUP BY epoch order by epoch
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_network_view_epochs_v2 ON fil_network_view_epochs_v2(epoch);

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_network_view_days_v2
AS
    SELECT
        commited,
        used,
        total,
        (used / NULLIF(total,0)) AS fraction,
        total_per_day,
        date::date AS date
        FROM(
                SELECT 
                    ROUND(AVG(commited))               AS commited,
                    ROUND(AVG(used))                   AS used,
                    ROUND(AVG(total))                  AS total,
                    ROUND(SUM(total_per_epoch))        AS total_per_day,
                    date_trunc('day', timestamp)       AS date
                FROM fil_network_view_epochs_v2
                GROUP BY date
                ORDER BY date
            ) q WHERE total > 0
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_network_view_days_v2 ON fil_network_view_days_v2(date);