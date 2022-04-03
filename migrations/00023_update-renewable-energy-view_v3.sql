DROP MATERIALIZED VIEW fil_renewable_energy_view_v3 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_view_v3
AS
with data as (SELECT
       fil_renewable_energy_from_transactions_view_v3.miner,
       fil_renewable_energy_from_transactions_view_v3.date,
       fil_renewable_energy_from_transactions_view_v3.energyWh as energyWhFromtransactions,
       fil_renewable_energy_from_contracts_view_v3.energyWh as energyWhFromContracts
FROM fil_renewable_energy_from_transactions_view_v3
RIGHT JOIN fil_renewable_energy_from_contracts_view_v3 ON
    fil_renewable_energy_from_transactions_view_v3.miner = fil_renewable_energy_from_contracts_view_v3.miner AND
    fil_renewable_energy_from_transactions_view_v3.date = fil_renewable_energy_from_contracts_view_v3.date)
    SELECT data.miner,
           data.date,
           SUM(data.energyWhFromtransactions + data.energyWhFromContracts) AS energyWh
    FROM data  GROUP BY (date, miner)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_view_v3 ON fil_renewable_energy_view_v3(miner,date);