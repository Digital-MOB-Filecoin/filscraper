CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_transactions_view
AS
with
     miners as (SELECT id as miner FROM fil_renewable_energy_miners),
     dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-08-25'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
     data_points as ( SELECT miners.miner, dates.date FROM miners CROSS JOIN dates),
     energy_data_from_transactions as (SELECT data_points.miner,
                                              data_points.date,
                                              COALESCE(fil_renewable_energy_from_transactions.energyWh, 0) as energyWhFromTransactions
                        FROM data_points
                        FULL JOIN fil_renewable_energy_from_transactions ON data_points.miner = fil_renewable_energy_from_transactions.miner AND data_points.date = fil_renewable_energy_from_transactions.date)

     SELECT energy_data_from_transactions.miner,
            energy_data_from_transactions.date,
            SUM(SUM(energy_data_from_transactions.energyWhFromTransactions))  OVER (PARTITION BY energy_data_from_transactions.miner ORDER BY energy_data_from_transactions.date) as energyWh
     FROM energy_data_from_transactions GROUP BY energy_data_from_transactions.miner, energy_data_from_transactions.date, energyWhFromTransactions
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_contracts_view
AS
with
     miners as (SELECT id as miner FROM fil_renewable_energy_miners),
     dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-08-25'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
     data_points as ( SELECT miners.miner, dates.date FROM miners CROSS JOIN dates),
     energy_data_from_contracts as (SELECT data_points.miner,
                                              data_points.date,
                                              COALESCE(fil_renewable_energy_from_contracts.energyWh, 0) as energyWhFromTransactions
                        FROM data_points
                        FULL JOIN fil_renewable_energy_from_contracts ON data_points.miner = fil_renewable_energy_from_contracts.miner AND data_points.date = fil_renewable_energy_from_contracts.date)

     SELECT energy_data_from_contracts.miner,
            energy_data_from_contracts.date,
            SUM(SUM(energy_data_from_contracts.energyWhFromTransactions))  OVER (PARTITION BY energy_data_from_contracts.miner ORDER BY energy_data_from_contracts.date) as energyWh
     FROM energy_data_from_contracts GROUP BY energy_data_from_contracts.miner, energy_data_from_contracts.date, energyWhFromTransactions
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_view
AS
SELECT fil_renewable_energy_from_transactions_view.miner,
       fil_renewable_energy_from_transactions_view.date,
       fil_renewable_energy_from_transactions_view.energyWh + fil_renewable_energy_from_contracts_view.energyWh as energyWh
FROM fil_renewable_energy_from_transactions_view
FULL JOIN fil_renewable_energy_from_contracts_view ON
    fil_renewable_energy_from_transactions_view.miner = fil_renewable_energy_from_contracts_view.miner AND
    fil_renewable_energy_from_transactions_view.date = fil_renewable_energy_from_contracts_view.date
WITH DATA;