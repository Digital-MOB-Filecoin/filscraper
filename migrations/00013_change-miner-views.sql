DROP MATERIALIZED VIEW fil_miner_view_epochs CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_epochs
AS
with
                    sector as(
                      SELECT miner, sector_size as size FROM fil_miners
                    ),

                    miner_events as (
                        SELECT * FROM fil_miner_events
                    ),

                    miners_data as (select sector.size, miner_events.* from sector
                    full outer join miner_events on sector.miner = miner_events.miner)

select epoch,
        miner,
        (commited + (recovered - terminated - faults) * size) as commited_per_epoch,
        used as used_per_epoch,
        fraction as fraction_per_epoch,
        (recovered - terminated - faults) * size as faults,
        (((commited + (recovered - terminated - faults) * size) + used) / 1073741824) as total_per_epoch,
        SUM(SUM((commited + (recovered - terminated - faults) * size) / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS commited,
        SUM(SUM(used / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS used,
        SUM(SUM(((commited + (recovered - terminated - faults) * size) + used) / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS total,
               to_timestamp(1598281200 + epoch * 30) as timestamp
    from miners_data GROUP BY miner,commited,used,fraction,epoch,recovered,terminated,faults,size order by epoch;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miner_view_epochs ON fil_miner_view_epochs(miner,epoch);

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_days
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miner_view_days ON fil_miner_view_days(miner,date);

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_view
AS 
   SELECT miner,
       MAX(total) AS power,
       MAX(used) AS used
    FROM fil_miner_view_epochs
    GROUP BY miner order by power DESC
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miners_view ON fil_miners_view(miner);