DROP MATERIALIZED VIEW fil_renewable_energy_view_v3 CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_view_V3
AS
with transactions as (SELECT
                                  "miner",
                                  "date",
                                  SUM(energyWh)  as energyWh
                                FROM fil_renewable_energy_from_transactions_view_v3 GROUP BY "miner", "date"),
     contracts as (SELECT
                                  "miner",
                                  "date",
                                  SUM(energyWh)  as energyWh
                                FROM fil_renewable_energy_from_contracts_view_v3 GROUP BY "miner", "date")
SELECT transactions.miner,
       transactions.date,
       transactions.energyWh as energyWhFromTransactions,
       contracts.energyWh as energyWhFromContracts,
       transactions.energyWh + contracts.energyWh as energyWh
       FROM transactions
                        FULL JOIN contracts ON transactions.miner = contracts.miner AND transactions.date = contracts.date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_view_V3 ON fil_renewable_energy_view_V3(miner,date);