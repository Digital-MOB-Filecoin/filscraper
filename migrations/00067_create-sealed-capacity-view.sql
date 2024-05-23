CREATE MATERIALIZED VIEW IF NOT EXISTS fil_sealed_capacity_view_v2
AS
SELECT date(to_timestamp((1598306400 + epoch * 30)::bigint) at time zone 'Europe/Bucharest' at time zone 'utc') as date,
       sum(commited + used) / 1073741824                                                                        AS "sealed_GiB",
       miner
FROM fil_miner_events
GROUP BY date, miner
ORDER BY date, miner
WITH DATA;