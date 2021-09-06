const { Pool } = require("pg");
const { migrate } = require("postgres-migrations");
const config = require('./config');

class Migrations {

    constructor() {
        this.pool = new Pool(config.database);
    }

    async run() {
        // Note: when passing a client, it is assumed that the database already exists
        const client = await this.pool.connect();
        try {
            await migrate({ client }, "migrations");
        } finally {
            await client.end()
        }
    }

    async create_indexes() {
        await this.pool.query("\
        CREATE INDEX IF NOT EXISTS idx_fil_network ON fil_network(epoch);\
        CREATE INDEX IF NOT EXISTS idx_fil_miner_events ON fil_miner_events(epoch);\
        CREATE INDEX IF NOT EXISTS idx_fil_miners ON fil_miners(miner);\
        ");
    }

    async reprocess() {
        await this.pool.query("\
        TRUNCATE TABLE fil_network CASCADE;\
        TRUNCATE TABLE fil_miner_events CASCADE;\
        TRUNCATE TABLE fil_deals CASCADE;\
        TRUNCATE TABLE fil_sectors CASCADE;\
        TRUNCATE TABLE fil_sector_events CASCADE;\
        TRUNCATE TABLE fil_bad_blocks CASCADE;\
        TRUNCATE TABLE fil_blocks CASCADE;\
        DROP INDEX IF EXISTS idx_fil_network;\
        DROP INDEX IF EXISTS idx_fil_miner_events;\
        DROP INDEX IF EXISTS idx_fil_miners;\
        ");
    }
}

module.exports = {
    Migrations
}
