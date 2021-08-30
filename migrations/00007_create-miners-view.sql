CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_view
AS 
   SELECT miner,
       MAX(total) AS power,
       MAX(used) AS used
    FROM fil_miner_view_epochs
    GROUP BY miner order by power DESC
WITH DATA;