ALTER TABLE fil_renewable_energy_transactions ADD COLUMN generation jsonb;
ALTER TABLE fil_renewable_energy_transactions ADD COLUMN country text;
ALTER TABLE fil_renewable_energy_contracts ADD COLUMN country text;