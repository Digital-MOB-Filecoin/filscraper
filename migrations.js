const { Pool } = require("pg");
const config = require('./config');


class Migrations {

    constructor() {
        this.pool = new Pool(config.database);
    }

    async create_filblocks_table() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE TABLE IF NOT EXISTS fil_blocks\
        (\
            Block bigint NOT NULL UNIQUE,\
            Msgs bigint NOT NULL,\
            Created timestamp default now(),\
            PRIMARY KEY (Block) \
        )");

        client.release()
    }

    async create_filbadblocks_table() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE TABLE IF NOT EXISTS fil_bad_blocks\
        (\
            Block bigint NOT NULL,\
            Created timestamp default now(),\
            PRIMARY KEY (Block) \
        )");

        client.release()
    }

    async create_filmessages_table() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE TABLE IF NOT EXISTS fil_messages\
        (\
            \"CID\" text NOT NULL,\
            \"Block\" bigint NOT NULL,\
            \"From\" text NOT NULL,\
            \"To\" text NOT NULL,\
            \"Nonce\" bigint NOT NULL,\
            \"Value\" text NOT NULL,\
            \"GasLimit\" bigint NOT NULL,\
            \"GasFeeCap\" text NOT NULL,\
            \"GasPremium\" text NOT NULL,\
            \"Method\" integer NOT NULL,\
            \"Params\" text NOT NULL,\
            \"ExitCode\" integer,\
            \"Return\" text,\
            \"GasUsed\" bigint,\
            \"Version\" integer NOT NULL\
        )");

        client.release()
    }

    async create_filsectors_table() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE TABLE IF NOT EXISTS fil_sectors\
        (\
            sector bigint NOT NULL,\
            miner text NOT NULL,\
            type text NOT NULL,\
            size bigint NOT NULL,\
            start_epoch bigint NOT NULL,\
            end_epoch bigint NOT NULL\
        )");

        client.release()
    }

    async create_filsectorevents_table() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE TABLE IF NOT EXISTS fil_sector_events\
        (\
            type text NOT NULL,\
            miner text NOT NULL,\
            sector bigint NOT NULL,\
            epoch bigint NOT NULL\
        )");

        client.release()
    }

    async create_fildeals_table() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE TABLE IF NOT EXISTS fil_deals\
        (\
            deal bigint NOT NULL UNIQUE,\
            sector bigint NOT NULL,\
            miner text NOT NULL,\
            start_epoch bigint NOT NULL,\
            end_epoch bigint NOT NULL,\
            PRIMARY KEY (deal) \
        )");

        client.release()
    }

    async create_filminerevents_table() {
        const client = await this.pool.connect();

        await client.query("\
            CREATE TABLE IF NOT EXISTS fil_miner_events\
            (\
                miner text NOT NULL,\
                commited bigint NOT NULL,\
                used bigint NOT NULL,\
                total bigint NOT NULL,\
                fraction NUMERIC(6,5) NOT NULL,\
                activated bigint NOT NULL,\
                terminated bigint NOT NULL,\
                faults bigint NOT NULL,\
                recovered bigint NOT NULL,\
                proofs bigint NOT NULL,\
                epoch bigint NOT NULL\
            )");

        client.release()
    }

    async create_filnetwork_table() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE TABLE IF NOT EXISTS fil_network\
        (\
            epoch bigint NOT NULL UNIQUE,\
            commited bigint NOT NULL,\
            used bigint NOT NULL,\
            total bigint NOT NULL,\
            fraction NUMERIC(5,5) NOT NULL,\
            PRIMARY KEY (epoch) \
        )");

        client.release()
    }

    async create_filminers_table() {
        const client = await this.pool.connect();

        await client.query("\
            CREATE TABLE IF NOT EXISTS fil_miners\
            (\
                miner text NOT NULL UNIQUE,\
                sector_size bigint NOT NULL\
            )");

        client.release()
    }

    async reprocess() {
        const client = await this.pool.connect();

        await client.query("\
        DROP TABLE IF EXISTS fil_network CASCADE;\
        DROP TABLE IF EXISTS fil_miner_events CASCADE;\
        DROP TABLE IF EXISTS fil_deals CASCADE;\
        DROP TABLE IF EXISTS fil_sectors CASCADE;\
        DROP TABLE IF EXISTS fil_sector_events CASCADE;\
        DROP TABLE IF EXISTS fil_bad_blocks CASCADE;\
        DROP TABLE IF EXISTS fil_blocks CASCADE;\
        ");

        client.release()
    }

    async create_indexes() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE INDEX IF NOT EXISTS idx_fil_messages ON fil_messages(\"Block\");\
        CREATE INDEX IF NOT EXISTS idx_fil_network ON fil_network(epoch);\
        CREATE INDEX IF NOT EXISTS idx_fil_miner_events ON fil_miner_events(epoch);\
        CREATE INDEX IF NOT EXISTS idx_fil_bad_blocks ON fil_bad_blocks(block);\
        CREATE INDEX IF NOT EXISTS idx_fil_blocks ON fil_blocks(block);\
        CREATE INDEX IF NOT EXISTS idx_fil_miners ON fil_miners(miner);\
        ");

        client.release()
    }

    async run() {
        await this.create_filblocks_table();
        await this.create_filbadblocks_table();
        await this.create_filmessages_table();
        await this.create_filsectors_table();
        await this.create_filsectorevents_table();
        await this.create_fildeals_table();
        await this.create_filminerevents_table();
        await this.create_filnetwork_table();
        await this.create_filminers_table();
        await this.create_indexes();
    }
}

module.exports = {
    Migrations
}

