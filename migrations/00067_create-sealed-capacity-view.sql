CREATE MATERIALIZED VIEW IF NOT EXISTS fil_sealed_capacity_view_v2
AS
SELECT date(to_timestamp((1598306400 + epoch * 30)::bigint)) as date,
       sum(commited + used) / 1073741824 AS "sealed_GiB",
       miner
FROM fil_miner_events
GROUP BY date, miner
ORDER BY date, miner
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_sealed_capacity_view_v2 ON fil_sealed_capacity_view_v2(miner, date);