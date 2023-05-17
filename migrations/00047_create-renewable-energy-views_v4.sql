CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_transactions_view_v4
AS
         SELECT miner,
                country,
                date,
                SUM(COALESCE(fil_renewable_energy_from_transactions.energyWh, 0)) as energyWhFromTransactions
         FROM fil_renewable_energy_from_transactions
         GROUP BY miner, country, date
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_contracts_view_v4
AS
             SELECT miner,
                country,
                date,
                SUM(COALESCE(fil_renewable_energy_from_contracts.energyWh, 0)) as energyWhFromContracts
         FROM fil_renewable_energy_from_contracts
         GROUP BY miner, country, date
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_view_v4 AS
WITH
    miners AS (
        SELECT DISTINCT miner, date, country FROM (
            SELECT miner, date, country FROM fil_renewable_energy_from_transactions_view_v4
            UNION
            SELECT miner, date, country FROM fil_renewable_energy_from_contracts_view_v4
        ) AS combined_data
    )
SELECT
    miners.miner,
    miners.date,
    miners.country,
    COALESCE(fil_renewable_energy_from_transactions_view_v4.energyWhFromTransactions, 0) as energyWhFromTransactions,
    COALESCE(fil_renewable_energy_from_contracts_view_v4.energyWhFromContracts, 0) as energyWhFromContracts,
    COALESCE(fil_renewable_energy_from_transactions_view_v4.energyWhFromTransactions, 0) + COALESCE(fil_renewable_energy_from_contracts_view_v4.energyWhFromContracts, 0) as energyWh
FROM miners
LEFT JOIN fil_renewable_energy_from_transactions_view_v4
    ON miners.miner = fil_renewable_energy_from_transactions_view_v4.miner
    AND miners.date = fil_renewable_energy_from_transactions_view_v4.date
    AND miners.country = fil_renewable_energy_from_transactions_view_v4.country
LEFT JOIN fil_renewable_energy_from_contracts_view_v4
    ON miners.miner = fil_renewable_energy_from_contracts_view_v4.miner
    AND miners.date = fil_renewable_energy_from_contracts_view_v4.date
    AND miners.country = fil_renewable_energy_from_contracts_view_v4.country
WITH DATA;



CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_from_transactions_view_v4 ON fil_renewable_energy_from_transactions_view_v4(id, miner, date, country);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_from_contracts_view_v4 ON fil_renewable_energy_from_contracts_view_v4(id, miner, date, country);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_view_v4 ON fil_renewable_energy_view_v4(miner, date, country);