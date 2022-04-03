CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_transactions_view_v3
AS
with
     miners as (SELECT id as miner FROM fil_renewable_energy_miners),
     dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-07-01'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
     data_points as ( SELECT miners.miner, dates.date FROM miners CROSS JOIN dates),
     energy_data_from_transactions as (SELECT data_points.miner,
                                              data_points.date,
                                              fil_renewable_energy_from_transactions.transaction_id as id,
                                              COALESCE(fil_renewable_energy_from_transactions.energyWh, 0) as energyWhFromTransactions
                        FROM data_points
                        FULL JOIN fil_renewable_energy_from_transactions ON data_points.miner = fil_renewable_energy_from_transactions.miner AND data_points.date = fil_renewable_energy_from_transactions.date)

     SELECT
            energy_data_from_transactions.id,
            energy_data_from_transactions.miner,
            energy_data_from_transactions.date,
            energy_data_from_transactions.energyWhFromTransactions as energyWh
     FROM energy_data_from_transactions
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_contracts_view_v3
AS
with
     miners as (SELECT id as miner FROM fil_renewable_energy_miners),
     dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-07-01'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
     data_points as ( SELECT miners.miner, dates.date FROM miners CROSS JOIN dates),
     energy_data_from_contracts as (SELECT data_points.miner,
                                              data_points.date,
                                              fil_renewable_energy_from_contracts.contract_id as id,
                                              COALESCE(fil_renewable_energy_from_contracts.energyWh, 0) as energyWhFromContracts
                        FROM data_points
                        FULL JOIN fil_renewable_energy_from_contracts ON data_points.miner = fil_renewable_energy_from_contracts.miner AND data_points.date = fil_renewable_energy_from_contracts.date)

     SELECT
            energy_data_from_contracts.id,
            energy_data_from_contracts.miner,
            energy_data_from_contracts.date,
            energy_data_from_contracts.energyWhFromContracts as energyWh
     FROM energy_data_from_contracts
WITH DATA;

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
           SUM(data.energyWhFromtransactions + data.energyWhFromContracts)
    FROM data  GROUP BY (date, miner)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_from_transactions_view_v3 ON fil_renewable_energy_from_transactions_view_v3(id, miner, date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_from_contracts_view_v3 ON fil_renewable_energy_from_contracts_view_v3(id, miner, date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_view_v3 ON fil_renewable_energy_view_v3(miner,date);