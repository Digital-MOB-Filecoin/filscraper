CREATE TABLE IF NOT EXISTS fil_miners_confidence_scores (
    miner TEXT NOT NULL,
    date DATE NOT NULL,
    confidence_score NUMERIC NOT NULL,
    PRIMARY KEY (miner_id, date)
    );

CREATE UNIQUE INDEX IF NOT EXISTS idx_miners_confidence_scores ON fil_miners_confidence_scores(miner_id, date);