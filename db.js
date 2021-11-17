const { Pool } = require("pg");
const config = require('./config');
const { INFO, ERROR, WARNING } = require('./logs');

class DB {

    constructor() {
        this.pool = new Pool(config.database);
    }

    async save_messages(msgs) {
        const client = await this.pool.connect();

        for (let i = 0; i < msgs.length; i++) {
            const msg = msgs[i];
            try {
                const { '/': msgCid } = msg.CID;
                const { '/': msgCid2 } = msg.Cid;

                await client.query(`\
        INSERT INTO fil_messages (\"CID\", \"Block\", \"From\", \"To\", \"Nonce\", \"Value\", \"GasLimit\", \"GasFeeCap\", \"GasPremium\", \"Method\", \"Params\", \"ExitCode\", \"Return\", \"GasUsed\", \"Version\", \"Cid\") \
        VALUES ('${msgCid}', \
        '${msg.Block}', \
        '${msg.From}', \
        '${msg.To}', \
        '${msg.Nonce}', \
        '${msg.Value}', \
        '${msg.GasLimit}', \
        '${msg.GasFeeCap}', \
        '${msg.GasPremium}', \
        '${msg.Method}', \
        '${msg.Params}', \
        '${msg.ExitCode}', \
        '${msg.Return}', \
        '${msg.GasUsed}', \
        '${msg.Version}',
        '${msgCid2}') \
`);

            } catch (err) {
                WARNING(`[SaveMessages] ${err}`)
            }

        }

        client.release();
    }

    async save_messages_cids(msgs) {
        const client = await this.pool.connect();

        for (let i = 0; i < msgs.length; i++) {
            const msg = msgs[i];
            try {
                const { '/': msgCid } = msg.CID;
                const { '/': msgCid2 } = msg.Cid;

                await client.query(`\
                UPDATE fil_messages SET \"Cid\" = '${msgCid2}' \
                WHERE \"Block\" = ${msg.Block} AND \"CID\" = '${msgCid}'`);
            } catch (err) {
                WARNING(`[SaveMessagesCids] ${err}`)
            }

        }

        client.release();
    }

    async save_block(block, msgs, msg_cid) {
        const client = await this.pool.connect();
        try {
            await client.query(`\
           INSERT INTO fil_blocks (Block, Msgs, msg_cid) \
           VALUES ('${block}', '${msgs}', '${msg_cid}') `);


        } catch (err) {
            WARNING(`[SaveBlock] ${err}`)
        }
        client.release()
    }

    async mark_block_with_msg_cid(block) {
        const client = await this.pool.connect();
        try {
            await client.query(`UPDATE fil_blocks SET msg_cid = true WHERE block = ${block};`);
        } catch (err) {
            WARNING(`[MarkBlockMsgCid] ${err}`)
        }
        client.release()
    }

    async save_bad_block(block) {
        const client = await this.pool.connect();
        try {
            await client.query(`\
           INSERT INTO fil_bad_blocks (Block) \
           VALUES ('${block}') `);


        } catch (err) {
            WARNING(`[SaveBadBlock] ${err}`)
        }
        client.release()
    }

    async get_start_block() {
        const client = await this.pool.connect();
        let block = 0;
        try {
            const result = await client.query(`\
        SELECT MAX(Block) \
        FROM fil_blocks `);

            if (result?.rows[0]?.max) {
                block = parseInt(result?.rows[0]?.max);
            }
        } catch (err) {
            WARNING(`[GetMaxBlock] ${err}`)
        }
        client.release()

        return block;
    }

    async get_bad_blocks(limit, offset) {
        const client = await this.pool.connect();
        let rows = undefined;
        try {
            const result = await client.query(`\
        SELECT block FROM fil_bad_blocks ORDER BY block LIMIT ${limit} OFFSET ${offset}`);

            if (result?.rows) {
                rows = result?.rows;
            }
        } catch (err) {
            WARNING(`[GetBadBlocks] ${err}`)
        }
        client.release()

        return rows;
    }

    async get_messages(block) {
        const client = await this.pool.connect();
        let messages = [];
        try {
            const result = await client.query(`\
        SELECT * FROM fil_messages WHERE \"Block\"=${block}`);

            if (result?.rows) {
                messages = result?.rows;
            }
        } catch (err) {
            WARNING(`[GetMessages] ${err}`)
        }
        client.release()

        return messages;
    }

    async have_block(block) {
        const client = await this.pool.connect();
        let found = false;

        try {
            const result = await client.query(`\
        SELECT EXISTS(SELECT 1 FROM fil_blocks WHERE Block = ${block})`);

            if (result?.rows[0]?.exists) {
                found = true;
            }

        } catch (err) {
            WARNING(`[HaveBlock] ${err}`)
        }
        client.release()

        return found;
    }

    async have_messages(block) {
        const client = await this.pool.connect();
        let found = false;

        try {
            const result = await client.query(`\
        SELECT EXISTS(SELECT 1 FROM fil_messages WHERE \"Block\" = ${block})`);

            if (result?.rows[0]?.exists) {
                found = true;
            }

        } catch (err) {
            WARNING(`[HaveMessages] ${err}`)
        }
        client.release()

        return found;
    }

    async save_sectors(sectors_info) {
        let values = '';
        for (let i = 0; i < sectors_info.length - 1; i++) {
            let sector_info = sectors_info[i];
            values += `('${sector_info.sector}', \
                        '${sector_info.miner}',\
                        '${sector_info.type}',\
                        '${sector_info.size}',\
                        '${sector_info.start_epoch}',\
                        '${sector_info.end_epoch}'),`;
        }

        values += `('${sectors_info[sectors_info.length-1].sector}', \
        '${sectors_info[sectors_info.length-1].miner}',\
        '${sectors_info[sectors_info.length-1].type}',\
        '${sectors_info[sectors_info.length-1].size}',\
        '${sectors_info[sectors_info.length-1].start_epoch}',\
        '${sectors_info[sectors_info.length-1].end_epoch}');`;

        try {
            await this.pool.query(`\
                INSERT INTO fil_sectors (sector, miner, type, size, start_epoch, end_epoch) \
                VALUES ${values}`);

        } catch (err) {
            WARNING(`[SaveSectors] ${err}`)
        }
    }

    async save_sectors_events(sectors_events) {
        let values = '';
        for (let i = 0; i < sectors_events.length - 1; i++) {
            let sector_events = sectors_events[i];

            values += `('${sector_events.type}', \
                        '${sector_events.miner}', \
                        '${sector_events.sector}', \
                        '${sector_events.epoch}'), `
        }

        values += `('${sectors_events[sectors_events.length-1].type}', \
                    '${sectors_events[sectors_events.length-1].miner}', \
                    '${sectors_events[sectors_events.length-1].sector}', \
                    '${sectors_events[sectors_events.length-1].epoch}');`

        try {
            await this.pool.query(`\
           INSERT INTO fil_sector_events (type, miner, sector, epoch) \
           VALUES  ${values}`);


        } catch (err) {
            WARNING(`[SaveSectorsEvents] ${err}`)
        }
    }

    async save_miners_events(miners_events) {
        let values = '';
        for (let i = 0; i < miners_events.length - 1; i++) {
            let miner_events = miners_events[i];
            values += `('${miner_events.miner}', \
            '${miner_events.commited.toString(10)}',\
            '${miner_events.used.toString(10)}',\
            '${miner_events.total.toString(10)}',\
            '${miner_events.fraction.toPrecision(5)}',\
            '${miner_events.activated}',\
            '${miner_events.terminated}',\
            '${miner_events.faults}',\
            '${miner_events.recovered}',\
            '${miner_events.proofs}',\
            '${miner_events.epoch}'),`
        }

        values += `('${miners_events[miners_events.length-1].miner}', \
        '${miners_events[miners_events.length-1].commited.toString(10)}',\
        '${miners_events[miners_events.length-1].used.toString(10)}',\
        '${miners_events[miners_events.length-1].total.toString(10)}',\
        '${miners_events[miners_events.length-1].fraction.toPrecision(5)}',\
        '${miners_events[miners_events.length-1].activated}',\
        '${miners_events[miners_events.length-1].terminated}',\
        '${miners_events[miners_events.length-1].faults}',\
        '${miners_events[miners_events.length-1].recovered}',\
        '${miners_events[miners_events.length-1].proofs}',\
        '${miners_events[miners_events.length-1].epoch}');`

        try {
            await this.pool.query(`\
           INSERT INTO fil_miner_events (miner, commited, used, total, fraction, activated, terminated, faults, recovered, proofs, epoch) \
           VALUES ${values} `);


        } catch (err) {
            WARNING(`[SaveMinerEvents] ${err}`)
        }
    }

    async save_deals(deals_info) {
        let values = '';
        for (let i = 0; i < deals_info.length - 1; i++) {
            let deal_info = deals_info[i];
            values += `('${deal_info.deal}', \
                        '${deal_info.sector}',\
                        '${deal_info.miner}',\
                        '${deal_info.start_epoch}',\
                        '${deal_info.end_epoch}'),`;
        }

        values += `('${deals_info[deals_info.length-1].deal}', \
                    '${deals_info[deals_info.length-1].sector}',\
                    '${deals_info[deals_info.length-1].miner}',\
                    '${deals_info[deals_info.length-1].start_epoch}',\
                    '${deals_info[deals_info.length-1].end_epoch}');`;

        try {
            await this.pool.query(`\
           INSERT INTO fil_deals (deal, sector, miner, start_epoch, end_epoch) \
           VALUES ${values}`);


        } catch (err) {
            WARNING(`[SaveDeal] ${err}`)
        }
    }

    async save_network(network_info) {
        try {
            await this.pool.query(`\
           INSERT INTO fil_network (epoch, commited, used, total, fraction) \
           VALUES ('${network_info.epoch}', \
                   '${network_info.commited.toString(10)}', \
                   '${network_info.used.toString(10)}', \
                   '${network_info.total.toString(10)}', \
                   '${network_info.fraction.toPrecision(5)}') `);


        } catch (err) {
            WARNING(`[SaveNetwork] ${err}`)
        }
    }

    async get_sector_size(miner) {
        const client = await this.pool.connect();
        let sector_size = undefined;
        try {
            const result = await client.query(`\
        SELECT sector_size FROM fil_miners WHERE miner = \'${miner}\' LIMIT 1;`);

            if (result?.rows) {
                sector_size = result?.rows[0]?.sector_size;
            }
        } catch (err) {
            WARNING(`[GetSectorSize] ${err}`)
        }
        client.release()

        return sector_size;
    }

    async save_sector_size(miner, sector_size) {
        const client = await this.pool.connect();
        try {
            await client.query(`\
           INSERT INTO fil_miners (miner, sector_size) \
           VALUES ('${miner}', '${sector_size}') `);


        } catch (err) {
            WARNING(`[SaveSectorSize] ${err}`)
        }
        client.release()
    }

    async refresh_network_view_epochs() {
        const client = await this.pool.connect();
        try {
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_network_view_epochs WITH DATA;\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_network_view_epochs_v2 WITH DATA;\
            ");

        } catch (err) {
            WARNING(`[RefreshNetworkMatViewEpochs] ${err}`)
        }
        client.release();
    }

    async refresh_network_view_days() {
        const client = await this.pool.connect();
        try {
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_network_view_days WITH DATA;\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_network_view_days_v2 WITH DATA;\
            ");

        } catch (err) {
            WARNING(`[RefreshNetworkMatViewDays] ${err}`)
        }
        client.release();
    }

    async refresh_miner_view_epochs() {
        const client = await this.pool.connect();
        try {
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miner_view_epochs WITH DATA;\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miner_view_epochs_v2 WITH DATA;\
            ");

        } catch (err) {
            WARNING(`[RefreshMinerMatViewEpochs] ${err}`)
        }
        client.release();
    }

    async refresh_miner_view_days() {
        const client = await this.pool.connect();
        try {
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miner_view_days WITH DATA;\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miner_view_days_v2 WITH DATA;\
            ");

        } catch (err) {
            WARNING(`[RefreshMinerMatViewDays] ${err}`)
        }
        client.release();
    }

    async refresh_miners_view() {
        const client = await this.pool.connect();
        try {
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miners_view WITH DATA;\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miners_view_v2 WITH DATA;\
            ");

        } catch (err) {
            WARNING(`[RefreshMinersMatView] ${err}`)
        }
        client.release();
    }

    async get_missing_blocks(head) {
        const client = await this.pool.connect();
        let missing_blocks = undefined;
        try {
            const result = await client.query(`\
            SELECT s.i AS missing_block \
            FROM generate_series(${head}, 0, -1) s(i) \
            WHERE (NOT EXISTS (SELECT 1 FROM fil_blocks WHERE block = s.i)) AND \
            (NOT EXISTS (SELECT 1 FROM fil_bad_blocks WHERE block = s.i)); `);

            if (result?.rows) {
                missing_blocks = result?.rows;
            }
        } catch (err) {
            WARNING(`[GetMissingBlocks] ${err}`)
        }
        client.release();

        return missing_blocks;
    } 
    async get_blocks_with_missing_cid(head) {
        const client = await this.pool.connect();
        let blocks_with_missing_cid = undefined;
        try {
            const result = await client.query(`\
            SELECT s.i AS block_with_missing_cid \
            FROM generate_series(${head}, 0, -1) s(i) \
            WHERE ( EXISTS (SELECT 1 FROM fil_blocks WHERE (block = s.i and msg_cid is null))); `);

            if (result?.rows) {
                blocks_with_missing_cid = result?.rows;
            }
        } catch (err) {
            WARNING(`[GetBlocksWithMissingCid] ${err}`)
        }
        client.release();

        return blocks_with_missing_cid;
    } 
}

module.exports = {
    DB
}