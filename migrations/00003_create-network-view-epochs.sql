CREATE MATERIALIZED VIEW IF NOT EXISTS fil_network_view_epochs
AS
    select epoch,
        commited as commited_per_epoch,
        used as used_per_epoch,
        fraction as fraction_per_epoch,
        (total / 1073741824) as total_per_epoch,
        SUM(SUM(commited / 1073741824)) OVER(ORDER BY epoch) AS commited,
        SUM(SUM(used / 1073741824)) OVER(ORDER BY epoch) AS used,
        SUM(SUM(total / 1073741824)) OVER (ORDER BY epoch) AS total,
        to_timestamp(1598281200 + epoch * 30) as timestamp
    from fil_network GROUP BY epoch order by epoch
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_network_view_epochs ON fil_network_view_epochs(epoch);