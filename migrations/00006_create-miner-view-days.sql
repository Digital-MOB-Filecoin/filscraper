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