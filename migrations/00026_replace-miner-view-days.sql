DROP MATERIALIZED VIEW IF EXISTS fil_miner_view_days CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_days
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_new_fil_miner_view_days ON fil_miner_view_days(miner, date);