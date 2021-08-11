CREATE MATERIALIZED VIEW IF NOT EXISTS fil_network_view_days
AS
    SELECT
        commited,
        used,
        total,
        (used / total) AS fraction,
        total_per_day,
        date::date AS date
        FROM(
                SELECT 
                    ROUND(AVG(commited))               AS commited,
                    ROUND(AVG(used))                   AS used,
                    ROUND(AVG(total))                  AS total,
                    ROUND(SUM(total_per_epoch))        AS total_per_day,
                    date_trunc('day', timestamp)       AS date
                FROM fil_network_view_epochs
                GROUP BY date
                ORDER BY date
            ) q WHERE total > 0
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_network_view_days ON fil_network_view_days(date);