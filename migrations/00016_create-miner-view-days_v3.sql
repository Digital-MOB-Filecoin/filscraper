CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_days_v3
AS
    SELECT
        miner,
        commited,
        used,
        total,
        COALESCE((used / NULLIF(total,0)),0) AS fraction,
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
            ) q
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_fil_miner_view_days_v3 ON fil_miner_view_days_v3(miner,date);