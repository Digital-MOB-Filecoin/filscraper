CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_transactions_view_v2
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
            energy_data_from_transactions.energyWhFromTransactions as energyWh
     FROM energy_data_from_transactions GROUP BY energy_data_from_transactions.miner, energy_data_from_transactions.date, energyWhFromTransactions
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_contracts_view_v2
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
            energy_data_from_contracts.energyWhFromTransactions as energyWh
     FROM energy_data_from_contracts GROUP BY energy_data_from_contracts.miner, energy_data_from_contracts.date, energyWhFromTransactions
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_view_v2
AS
SELECT fil_renewable_energy_from_transactions_view_v2.miner,
       fil_renewable_energy_from_transactions_view_v2.date,
       fil_renewable_energy_from_transactions_view_v2.energyWh + fil_renewable_energy_from_contracts_view_v2.energyWh as energyWh
FROM fil_renewable_energy_from_transactions_view_v2
FULL JOIN fil_renewable_energy_from_contracts_view_v2 ON
    fil_renewable_energy_from_transactions_view_v2.miner = fil_renewable_energy_from_contracts_view_v2.miner AND
    fil_renewable_energy_from_transactions_view_v2.date = fil_renewable_energy_from_contracts_view_v2.date
WITH DATA;