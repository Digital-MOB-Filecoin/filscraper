CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_epochs
AS 
    select epoch,
        miner,
        commited as commited_per_epoch,
        used as used_per_epoch,
        fraction as fraction_per_epoch,
        ((commited + used) / 1073741824) as total_per_epoch,
        SUM(SUM(commited / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS commited,
        SUM(SUM(used / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS used,
        SUM(SUM(total / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS total,
               to_timestamp(1598281200 + epoch * 30) as timestamp
    from fil_miner_events GROUP BY miner,commited,used,fraction,epoch,total order by epoch
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miner_view_epochs ON fil_miner_view_epochs(miner,epoch)