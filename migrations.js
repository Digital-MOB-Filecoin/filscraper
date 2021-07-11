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
            CID text NOT NULL,\
            Block bigint NOT NULL,\
            \"from\" text NOT NULL,\
            \"to\" text NOT NULL,\
            Nonce bigint NOT NULL,\
            Value text NOT NULL,\
            GasLimit bigint NOT NULL,\
            GasFeeCap text NOT NULL,\
            GasPremium text NOT NULL,\
            Method integer NOT NULL,\
            Params text NOT NULL,\
            ExitCode integer,\
            Return text,\
            GasUsed bigint,\
            Version integer NOT NULL\
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

    async create_filnetwork_table() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE TABLE IF NOT EXISTS fil_network\
        (\
            epoch bigint NOT NULL UNIQUE,\
            commited bigint NOT NULL,\
            used bigint NOT NULL,\
            PRIMARY KEY (epoch) \
        )");

        client.release()
    }

    async run() {
        await this.create_filblocks_table();
        await this.create_filbadblocks_table();
        await this.create_filmessages_table();
        await this.create_filsectors_table();
        await this.create_filnetwork_table();
    }
}

module.exports = {
    Migrations
}

