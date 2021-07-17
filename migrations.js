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

        client.release();
    }

    async create_network_view_epochs() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE MATERIALIZED VIEW IF NOT EXISTS fil_network_view_epochs\
        AS\
        select epoch,\
            commited as commited_per_epoch,\
            used as used_per_epoch,\
            fraction as fraction_per_epoch,\
            (total / 1073741824) as total_per_epoch,\
            SUM(SUM(commited / 1073741824)) OVER(ORDER BY epoch) AS commited,\
            SUM(SUM(used / 1073741824)) OVER(ORDER BY epoch) AS used,\
            SUM(SUM(total / 1073741824)) OVER (ORDER BY epoch) AS total,\
            to_timestamp(1598281200 + epoch * 30) as timestamp\
        from fil_network GROUP BY epoch order by epoch\
        WITH DATA;\
        ");

        await client.query("CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_network_view_epochs ON fil_network_view_epochs(epoch)");

        client.release();
    }

    async create_network_view_days() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE MATERIALIZED VIEW IF NOT EXISTS fil_network_view_days\
        AS\
        SELECT\
            commited,\
            used,\
            total,\
            (used / total) AS fraction,\
            avg_total_per_epoch,\
            date::date AS date\
            FROM(\
                SELECT \
                    ROUND(AVG(commited))               AS commited,\
                    ROUND(AVG(used))                   AS used,\
                    ROUND(AVG(total))                  AS total,\
                    ROUND(AVG(total_per_epoch))        AS avg_total_per_epoch,\
                    date_trunc('day', timestamp)       AS date\
                FROM fil_network_view_epochs\
                GROUP BY date\
                ORDER BY date\
            ) q WITH DATA;\
        ");

        await client.query("CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_network_view_days ON fil_network_view_days(date)");

        client.release();
    }

    async create_miner_view_epochs() {
        const client = await this.pool.connect();

        await client.query("\
        CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_epochs\
        AS \
        select epoch,\
               miner,\
               commited as commited_per_epoch,\
               used as used_per_epoch,\
               fraction as fraction_per_epoch,\
               ((commited + used) / 1073741824) as total_per_epoch,\
               SUM(SUM(commited / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS commited,\
               SUM(SUM(used / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS used,\
               SUM(SUM(total / 1073741824)) OVER (PARTITION BY miner ORDER BY epoch) AS total,\
               to_timestamp(1598281200 + epoch * 30) as timestamp\
        from fil_miner_events GROUP BY miner,commited,used,fraction,epoch,total order by epoch\
        WITH DATA;\
        ");

        await client.query("CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miner_view_epochs ON fil_miner_view_epochs(miner,epoch)");

        client.release();
    }

    async create_miner_view_days() {
        const client = await this.pool.connect();

        await client.query("\
            CREATE MATERIALIZED VIEW IF NOT EXISTS fil_miner_view_days\
            AS\
            SELECT\
                miner,\
                commited,\
                used,\
                total,\
                (used / total) AS fraction,\
                avg_total_per_epoch,\
                date::date AS date\
            FROM (\
                SELECT\
                    miner,\
                    ROUND(AVG(commited))                            AS commited,\
                    ROUND(AVG(used))                                AS used,\
                    ROUND(AVG(total))                               AS total,\
                    ROUND(AVG(total_per_epoch))                     AS avg_total_per_epoch,\
                    date_trunc('day', timestamp) AS date\
                FROM fil_miner_view_epochs\
                GROUP BY miner,date\
                ORDER BY date\
                ) q WHERE total > 0\
            WITH DATA;\
        ");

        await client.query("CREATE UNIQUE INDEX IF NOT EXISTS idx_fil_miner_view_days ON fil_miner_view_days(miner,date)");
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
        await this.create_network_view_epochs();
        await this.create_network_view_days();
        await this.create_miner_view_epochs();
        await this.create_miner_view_days();
    }
}

module.exports = {
    Migrations
}

