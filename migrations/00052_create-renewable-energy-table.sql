CREATE TABLE IF NOT EXISTS fil_renewable_energy
(
    miner text NOT NULL,
    allocation_cid text NOT NULL,
    date timestamp NOT NULL,
    energyWh NUMERIC,
    country text NOT NULL,
    UNIQUE (allocation_cid, miner, country, date)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy ON fil_renewable_energy(allocation_cid, miner, country, date);