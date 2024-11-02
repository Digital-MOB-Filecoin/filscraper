CREATE TABLE IF NOT EXISTS fil_miners_confidence_scores (
    miner TEXT NOT NULL,
    date DATE NOT NULL,
    confidence_score NUMERIC NOT NULL,
    PRIMARY KEY (miner, date)
    );