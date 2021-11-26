CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_view_v3
AS 
   SELECT miner,
       MAX(total) AS power,
       MAX(commited) AS used
    FROM fil_miner_view_epochs
    GROUP BY miner order by used DESC
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miners_view_v3 ON fil_miners_view_v3(miner);