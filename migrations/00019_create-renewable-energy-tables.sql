CREATE TABLE IF NOT EXISTS fil_renewable_energy_miners
(
    id text NOT NULL,
    buyer_id text NOT NULL,
    blockchain_address text,
    created_at timestamp,
    updated_at timestamp,
    recs_total bigint,
    PRIMARY KEY (id) 
);

CREATE TABLE IF NOT EXISTS fil_renewable_energy_transactions
(
    id text NOT NULL,
    miner_id text NOT NULL,
    page_url text NOT NULL,
    data_url text NOT NULL,
    seller_id text,
    reporting_start timestamp NOT NULL,
    reporting_start_timezone_offset int,
    reporting_end timestamp NOT NULL,
    reporting_end_timezone_offset int,
    tx_hash text,
    buyer_id text,
    contract_id text,
    created_at timestamp,
    updated_at timestamp,
    reporting_start_local Timestamptz,
    reporting_end_local Timestamptz,
    gen_id text,
    gen_region text,
    gen_country text,
    gen_energy_source text,
    gen_product_type text,
    gen_generator_id text,
    gen_generator_name text,
    gen_generation_start timestamp,
    gen_generation_start_timezone_offset int,
    gen_generation_end timestamp,
    gen_generation_end_timezone_offset int,
    gen_tx_hash text,
    gen_initial_seller_id text,
    gen_beneficiary text,
    gen_redemption_date timestamp,
    gen_commissioning_date timestamp,
    gen_label text,
    gen_created_at timestamp,
    gen_updated_at timestamp,
    gen_energyWh bigint,
    gen_generation_start_local Timestamptz,
    gen_generation_end_local Timestamptz,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS fil_renewable_energy_contracts
(
    id text NOT NULL,
    miner_id text NOT NULL,
    product_type text,
    energy_sources jsonb,
    contract_date timestamp,
    delivery_date timestamp,
    reporting_start timestamp,
    reporting_end timestamp,
    buyer jsonb,
    seller jsonb,
    open_volume bigint,
    delivered_volume bigint,
    purchases jsonb,
    timezone_offset int,
    filecoin_node jsonb,
    external_id text,
    label text,
    created_at timestamp,
    updated_at timestamp,
    country_region_map jsonb,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS fil_renewable_energy_from_transactions
(
    miner text NOT NULL,
    transaction_id text NOT NULL,
    date timestamp NOT NULL,
    energyWh NUMERIC,
    UNIQUE (transaction_id, date)
);

CREATE TABLE IF NOT EXISTS fil_renewable_energy_from_contracts
(
    miner text NOT NULL,
    contract_id text NOT NULL,
    date timestamp NOT NULL,
    energyWh NUMERIC,
    UNIQUE (contract_id, date)
);