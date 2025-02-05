CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miners_green_scores_v2
AS
select COALESCE(e.date, c.date, l.date)                                              AS date,
       COALESCE(e.miner, c.miner, l.miner)                                            as miner,
       (COALESCE(e.emission_score, 0) + COALESCE(c.confidence_score, 0) + COALESCE(l.location_score, 0)) / 3 AS green_score
from fil_miners_emission_scores e
    full outer join fil_miners_confidence_scores c on e.date = c.date and e.miner = c.miner
    full outer join fil_miners_location_scores l on COALESCE(e.date, c.date) = l.date and COALESCE(e.miner, c.miner) = l.miner
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_miners_green_scores_v2 ON fil_miners_green_scores_v2(date, miner);