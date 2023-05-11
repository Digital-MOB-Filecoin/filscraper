CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_transactions_view_v4
AS
with
     miners as (SELECT id as miner FROM fil_renewable_energy_miners),
     dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-07-01'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
     data_points as ( SELECT miners.miner, dates.date FROM miners CROSS JOIN dates),
     energy_data_from_transactions as (SELECT data_points.miner,
                                              data_points.date,
                                              fil_renewable_energy_from_transactions.country,
                                              fil_renewable_energy_from_transactions.transaction_id as id,
                                              COALESCE(fil_renewable_energy_from_transactions.energyWh, 0) as energyWhFromTransactions
                        FROM data_points
                        FULL JOIN fil_renewable_energy_from_transactions ON data_points.miner = fil_renewable_energy_from_transactions.miner AND data_points.date = fil_renewable_energy_from_transactions.date)

     SELECT
            energy_data_from_transactions.id,
            energy_data_from_transactions.miner,
            energy_data_from_transactions.date,
            energy_data_from_transactions.country,
            energy_data_from_transactions.energyWhFromTransactions as energyWh
     FROM energy_data_from_transactions
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_from_contracts_view_v4
AS
with
     miners as (SELECT id as miner FROM fil_renewable_energy_miners),
     dates as (SELECT date_trunc('day', dd)::date as date FROM generate_series('2020-07-01'::timestamp, NOW()::timestamp, '1 day'::interval) dd),
     data_points as ( SELECT miners.miner, dates.date FROM miners CROSS JOIN dates),
     energy_data_from_contracts as (SELECT data_points.miner,
                                              data_points.date,
                                              fil_renewable_energy_from_contracts.country,
                                              fil_renewable_energy_from_contracts.contract_id as id,
                                              COALESCE(fil_renewable_energy_from_contracts.energyWh, 0) as energyWhFromContracts
                        FROM data_points
                        FULL JOIN fil_renewable_energy_from_contracts ON data_points.miner = fil_renewable_energy_from_contracts.miner AND data_points.date = fil_renewable_energy_from_contracts.date)

     SELECT
            energy_data_from_contracts.id,
            energy_data_from_contracts.miner,
            energy_data_from_contracts.date,
            energy_data_from_contracts.country,
            energy_data_from_contracts.energyWhFromContracts as energyWh
     FROM energy_data_from_contracts
WITH DATA;


CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_from_transactions_view_v4 ON fil_renewable_energy_from_transactions_view_v4(id, miner, date, country);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_from_contracts_view_v4 ON fil_renewable_energy_from_contracts_view_v4(id, miner, date, country);


CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_view_v4
AS
with transactions as (SELECT
                                  miner,
                                  date,
                                  country,
                                  SUM(energyWh)  as energyWh
                                FROM fil_renewable_energy_from_transactions_view_v4 GROUP BY miner, date, country),
     contracts as (SELECT
                                  miner,
                                  date,
                                  country,
                                  SUM(energyWh)  as energyWh
                                FROM fil_renewable_energy_from_contracts_view_v4 GROUP BY miner, date, country)
SELECT transactions.miner,
       transactions.date,
       transactions.energyWh as energyWhFromTransactions,
       contracts.energyWh as energyWhFromContracts,
       transactions.country as country,
       transactions.energyWh + contracts.energyWh as energyWh
       FROM transactions
      FULL JOIN contracts ON transactions.miner = contracts.miner AND transactions.date = contracts.date AND transactions.country = contracts.country
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_view_v4 ON fil_renewable_energy_view_v4(miner, date, country);