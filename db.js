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

                await client.query(`\
        INSERT INTO fil_messages (\"CID\", \"Block\", \"From\", \"To\", \"Nonce\", \"Value\", \"GasLimit\", \"GasFeeCap\", \"GasPremium\", \"Method\", \"Params\", \"ExitCode\", \"Return\", \"GasUsed\", \"Version\") \
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
        '${msg.Version}') \
`);

            } catch (err) {
                WARNING(`[SaveMessages] ${err}`)
            }

        }

        client.release();
    }

    async save_block(block, msgs) {
        const client = await this.pool.connect();
        try {
            await client.query(`\
           INSERT INTO fil_blocks (Block, Msgs) \
           VALUES ('${block}', '${msgs}') `);


        } catch (err) {
            WARNING(`[SaveBlock] ${err?.detail}`)
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
            WARNING(`[SaveBadBlock] ${err?.detail}`)
        }
        client.release()
    }

    async get_start_block() {
        const client = await this.pool.connect();
        let block = config.scraper.start;
        try {
            const result = await client.query(`\
        SELECT MAX(Block) \
        FROM fil_blocks `);

            if (result?.rows[0]?.max) {
                block = result?.rows[0]?.max;
            }
        } catch (err) {
            WARNING(`[GetMaxBlock] ${err?.detail}`)
        }
        client.release()

        return block;
    }

    async get_bad_blocks(limit, offset) {
        const client = await this.pool.connect();
        let rows = undefined;
        try {
            const result = await client.query(`\
        SELECT block FROM fil_messages ORDER BY block LIMIT ${limit} OFFSET ${offset}`);

            if (result?.rows) {
                rows = result?.rows;
            }
        } catch (err) {
            WARNING(`[GetBadBlocks] ${err?.detail}`)
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

    async save_sector(sector_info) {
        const client = await this.pool.connect();
        try {
            await client.query(`\
           INSERT INTO fil_sectors (sector, miner, type, size, start_epoch, end_epoch) \
           VALUES ('${sector_info.sector}', '${sector_info.miner}','${sector_info.type}','${sector_info.size}','${sector_info.start_epoch}','${sector_info.end_epoch}') `);


        } catch (err) {
            WARNING(`[SaveSector] ${err}`)
        }
        client.release()
    }

    async save_sector_events(sector_events) {
        const client = await this.pool.connect();
        try {
            await client.query(`\
           INSERT INTO fil_sector_events (type, miner, sector, epoch) \
           VALUES ('${sector_events.type}', '${sector_events.miner}','${sector_events.sector}','${sector_events.epoch}') `);


        } catch (err) {
            WARNING(`[SaveSectorEvents] ${err}`)
        }
        client.release()
    }

    async save_miner_events(miner, events) {
        const client = await this.pool.connect();
        try {
            await client.query(`\
           INSERT INTO fil_miner_events (miner, commited, used, total, fraction, activated, terminated, faults, recovered, proofs, epoch) \
           VALUES ('${miner}', '${events.commited.toString(10)}','${events.used.toString(10)}','${events.total.toString(10)}','${events.fraction.toPrecision(5)}','${events.activated}','${events.terminated}','${events.faults}','${events.recovered}','${events.proofs}','${events.epoch}') `);


        } catch (err) {
            WARNING(`[SaveMinerEvents] ${err}`)
        }
        client.release()
    }

    async save_deal(deal_info) {
        const client = await this.pool.connect();
        try {
            await client.query(`\
           INSERT INTO fil_deals (deal, sector, miner, start_epoch, end_epoch) \
           VALUES ('${deal_info.deal}', '${deal_info.sector}','${deal_info.miner}','${deal_info.start_epoch}','${deal_info.end_epoch}') `);


        } catch (err) {
            WARNING(`[SaveDeal] ${err}`)
        }
        client.release()
    }

    async save_network(network_info) {
        const client = await this.pool.connect();
        try {
            await client.query(`\
           INSERT INTO fil_network (epoch, commited, used, total, fraction) \
           VALUES ('${network_info.epoch}', '${network_info.commited.toString(10)}','${network_info.used.toString(10)}','${network_info.total.toString(10)}','${network_info.fraction.toPrecision(5)}') `);


        } catch (err) {
            WARNING(`[SaveNetwork] ${err}`)
        }
        client.release()
    }
}

module.exports = {
    DB
}