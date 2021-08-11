CREATE TABLE IF NOT EXISTS fil_blocks
(
    Block bigint NOT NULL UNIQUE,
    Msgs bigint NOT NULL,
    Created timestamp default now(),
    PRIMARY KEY (Block) 
);

CREATE TABLE IF NOT EXISTS fil_bad_blocks
(
    Block bigint NOT NULL,
    Created timestamp default now(),
    PRIMARY KEY (Block) 
);

CREATE TABLE IF NOT EXISTS fil_messages
(
    "CID" text NOT NULL,
    "Block" bigint NOT NULL,
    "From" text NOT NULL,
    "To" text NOT NULL,
    "Nonce" bigint NOT NULL,
    "Value" text NOT NULL,
    "GasLimit" bigint NOT NULL,
    "GasFeeCap" text NOT NULL,
    "GasPremium" text NOT NULL,
    "Method" integer NOT NULL,
    "Params" text NOT NULL,
    "ExitCode" integer,
    "Return" text,
    "GasUsed" bigint,
    "Version" integer NOT NULL
);

CREATE TABLE IF NOT EXISTS fil_sectors
(
    sector bigint NOT NULL,
    miner text NOT NULL,
    type text NOT NULL,
    size bigint NOT NULL,
    start_epoch bigint NOT NULL,
    end_epoch bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS fil_sector_events
(
    type text NOT NULL,
    miner text NOT NULL,
    sector bigint NOT NULL,
    epoch bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS fil_deals
(
    deal bigint NOT NULL UNIQUE,
    sector bigint NOT NULL,
    miner text NOT NULL,
    start_epoch bigint NOT NULL,
    end_epoch bigint NOT NULL,
    PRIMARY KEY (deal)
);

CREATE TABLE IF NOT EXISTS fil_miner_events
(
    miner text NOT NULL,
    commited bigint NOT NULL,
    used bigint NOT NULL,
    total bigint NOT NULL,
    fraction NUMERIC(6,5) NOT NULL,
    activated bigint NOT NULL,
    terminated bigint NOT NULL,
    faults bigint NOT NULL,
    recovered bigint NOT NULL,
    proofs bigint NOT NULL,
    epoch bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS fil_network
(
    epoch bigint NOT NULL UNIQUE,
    commited bigint NOT NULL,
    used bigint NOT NULL,
    total bigint NOT NULL,
    fraction NUMERIC(5,5) NOT NULL,
    PRIMARY KEY (epoch) 
);

CREATE TABLE IF NOT EXISTS fil_miners
(
    miner text NOT NULL UNIQUE,
    sector_size bigint NOT NULL
);