CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_epochs_v2
AS
SELECT  epoch,
        miner,
        activated,
        terminated,
        COALESCE((commited / NULLIF(activated,0)),0) as sector_size,
        (commited - terminated * COALESCE((commited / NULLIF(activated,0)),0)) as commited_per_epoch,
        used as used_per_epoch,
        fraction as fraction_per_epoch,
        (((commited - terminated * COALESCE((commited / NULLIF(activated,0)),0)) + used) / 1073741824) as total_per_epoch,
        SUM(SUM((commited - terminated * COALESCE((commited / NULLIF(activated,0)),0)) / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS commited,
        SUM(SUM(used / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS used,
        SUM(SUM(((commited - terminated * COALESCE((commited / NULLIF(activated,0)),0))+ used) / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS total,
               to_timestamp(1598281200 + epoch * 30) as timestamp
FROM fil_miner_events GROUP BY miner,commited,used,fraction,epoch,activated,terminated order by epoch;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miner_view_epochs_v2 ON fil_miner_view_epochs_v2(miner,epoch);

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_days_v2
AS
    SELECT
        miner,
        commited,
        used,
        total,
        (used / total) AS fraction,
        total_per_day,
        date::date AS date
    FROM (
            SELECT
                miner,
                ROUND(AVG(commited))                            AS commited,
                ROUND(AVG(used))                                AS used,
                ROUND(AVG(total))                               AS total,
                ROUND(SUM(total_per_epoch))                     AS total_per_day,
                date_trunc('day', timestamp) AS date
            FROM fil_miner_view_epochs
            GROUP BY miner,date
            ORDER BY date
            ) q WHERE total > 0
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miner_view_days_v2 ON fil_miner_view_days_v2(miner,date);

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_view_v2
AS 
   SELECT miner,
       MAX(total) AS power,
       MAX(used) AS used
    FROM fil_miner_view_epochs
    GROUP BY miner order by power DESC
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miners_view_v2 ON fil_miners_view_v2(miner);