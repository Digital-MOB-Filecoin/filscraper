const { Pool } = require("pg");
const config = require('./config');
const { INFO, ERROR, WARNING } = require('./logs');

// Type parser to use for timestamp without time zone
// This will keep node-pg from parsing the value into a Date object and give you the raw timestamp string instead.
var types = require('pg').types;
types.setTypeParser(1114, function(stringValue) {
  return stringValue;
})

function FormatNull(t) {
    if (JSON.stringify(t) == 'null') {
        return t;
    } else {
        return '\'' + t + '\'';
    }
}

function FormatText(t) {
    if (!t) {
        return;
    }

    return t.replace(/'/g, "''");
}

class DB {

    constructor() {
        this.pool = new Pool(config.database);
    }

    async Query(query, log) {
        let result = undefined;
        try {
            result = await this.pool.query(query);
        } catch (err) {
            WARNING(`[${log}] ${query} -> ${err}`)
        }

        return result;
    }

    async save_messages(msgs, from_filinfo = false) {
        const client = await this.pool.connect();

        for (let i = 0; i < msgs.length; i++) {
            const msg = msgs[i];
            try {
                const { '/': msgCid } = msg.CID;
                const { '/': msgCid2 } = msg.Cid;

                await client.query(`\
        INSERT INTO fil_messages (\"CID\", \"Block\", \"From\", \"To\", \"Nonce\", \"Value\", \"GasLimit\", \"GasFeeCap\", \"GasPremium\", \"Method\", \"Params\", \"ExitCode\", \"Return\", \"GasUsed\", \"Version\", \"Cid\") \
        VALUES ('${from_filinfo ? msg.CID : msgCid}', \
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
        '${from_filinfo ? msg.Cid : msgCid2}') \
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
        SELECT block FROM fil_bad_blocks WHERE block > 1200000 ORDER BY block LIMIT ${limit} OFFSET ${offset}`);

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
            ");

        } catch (err) {
            WARNING(`[RefreshMinerMatViewEpochs] ${err}`)
        }
        client.release();
    }

    async refresh_miner_view_days() {
        const client = await this.pool.connect();
        try {
            //fil_miner_view_days_v4 depends on fil_miners_view_v3 and fil_miner_view_days
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miners_view_v3 WITH DATA;\
            ");
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miner_view_days WITH DATA;\
            ");
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miner_view_days_v4 WITH DATA;\
            ");

        } catch (err) {
            WARNING(`[RefreshMinerMatViewDays] ${err}`)
        }
        client.release();
    }

    async refresh_energy_ratio_views() {
        const client = await this.pool.connect();
        try {
            //fil_renewable_energy_ratio_network_view depends on fil_renewable_energy_ratio_miner_view
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_renewable_energy_ratio_miner_view WITH DATA;\
            ");
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_renewable_energy_ratio_network_view WITH DATA;\
            ");

        } catch (err) {
            WARNING(`[RefreshEnergyRatioViews] ${err}`)
        }
        client.release();
    }

    async refresh_renewable_energy_views() {
        const client = await this.pool.connect();
        try {
            await client.query("\
            REFRESH MATERIALIZED VIEW fil_renewable_energy_from_transactions_view_v4 WITH DATA;\
            ");
            await client.query("\
            REFRESH MATERIALIZED VIEW fil_renewable_energy_from_contracts_view_v4 WITH DATA;\
            ");
            await client.query("\
            REFRESH MATERIALIZED VIEW fil_renewable_energy_view_v4 WITH DATA;\
            ");

        } catch (err) {
            WARNING(`[RefreshRenewableEnergyMatViews] ${err}`)
        }
        client.release();
    }

    async refresh_miners_view() {
        const client = await this.pool.connect();
        try {
            await client.query("\
            REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miners_view WITH DATA;\
            ");

        } catch (err) {
            WARNING(`[RefreshMinersMatView] ${err}`)
        }
        client.release();
    }

    async get_missing_blocks(head, start = 0) {
        const client = await this.pool.connect();
        let missing_blocks = undefined;
        try {
            const result = await client.query(`\
            SELECT s.i AS missing_block \
            FROM generate_series(${head}, ${start}, -1) s(i) \
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

    async reset_renewable_energy_data() {
        await this.Query(`
        TRUNCATE fil_renewable_energy_miners; \
        TRUNCATE fil_renewable_energy_transactions; \
        TRUNCATE fil_renewable_energy_contracts; \
        TRUNCATE fil_renewable_energy_from_contracts; \
        TRUNCATE fil_renewable_energy_from_transactions; \
        `,'ResetRenewableEnergyData');
    }

    async save_miner_renewable_energy(miner) {
        try {
            let values = `'${miner.id}', \
                        '${miner.buyerId}',\
                        ${FormatNull(miner.blockchainAddress)},\
                        ${FormatNull(miner.createdAt)},\
                        ${FormatNull(miner.updatedAt)},\
                        ${miner.recsTotal}`;

            await this.Query(`
                UPDATE fil_renewable_energy_miners SET buyer_id='${miner.buyerId}', \
                                 blockchain_address=${FormatNull(miner.blockchainAddress)}, \
                                 created_at=${FormatNull(miner.createdAt)}, \
                                 updated_at=${FormatNull(miner.updatedAt)}, \
                                 recs_total='${miner.recsTotal}'\
                    WHERE id='${miner.id}'; \
                INSERT INTO fil_renewable_energy_miners (id, buyer_id, blockchain_address, created_at, updated_at, recs_total) \
                    SELECT ${values} WHERE NOT EXISTS (SELECT 1 FROM fil_renewable_energy_miners WHERE id='${miner.id}');`,
                    'SaveMinerRenewableEnergy');

        } catch (err) {
            WARNING(`[SaveMinerRenewableEnergy] -> ${err}`)
        }
    }

    async save_transaction_renewable_energy(transaction) {
        try {
            let values = `'${transaction.id}', \
                        '${transaction.miner_id}', \
                        '${transaction.pageUrl}',\
                        '${transaction.dataUrl}',\
                        ${FormatNull(transaction.sellerId)},\
                        '${transaction.reportingStart}',\
                        ${transaction.reportingStartTimezoneOffset},\
                        '${transaction.reportingEnd}',\
                        ${transaction.reportingEndTimezoneOffset},\
                        ${FormatNull(transaction.txHash)},\
                        ${FormatNull(transaction.buyerId)},\
                        ${FormatNull(transaction.contractId)},\
                        ${FormatNull(transaction.createdAt)},\
                        ${FormatNull(transaction.updatedAt)},\
                        ${FormatNull(transaction.reportingStartLocal)},\
                        ${FormatNull(transaction.reportingEndLocal)},\
                        ${FormatNull(transaction.generation.id)},\
                        ${FormatNull(transaction.generation.region)},\
                        ${FormatNull(transaction.generation.country)},\
                        ${FormatNull(transaction.generation.energySource)},\
                        ${FormatNull(transaction.generation.productType)},\
                        ${FormatNull(transaction.generation.generatorId)},\
                        ${FormatNull(transaction.generation.generatorName)},\
                        ${FormatNull(transaction.generation.generationStart)},\
                        ${transaction.generation.generationStartTimezoneOffset},\
                        ${FormatNull(transaction.generation.generationEnd)},\
                        ${transaction.generation.generationEndTimezoneOffset},\
                        ${FormatNull(transaction.generation.txHash)},\
                        ${FormatNull(transaction.generation.initialSellerId)},\
                        ${FormatNull(transaction.generation.beneficiary)},\
                        ${FormatNull(transaction.generation.redemptionDate)},\
                        ${FormatNull(transaction.generation.commissioningDate)},\
                        ${FormatNull(transaction.generation.label)},\
                        ${FormatNull(transaction.generation.createdAt)},\
                        ${FormatNull(transaction.generation.updatedAt)},\
                        ${transaction.generation.energyWh},\
                        ${FormatNull(transaction.generation.generationStartLocal)},\
                        ${FormatNull(transaction.generation.generationEndLocal)},\
                        '${JSON.stringify(transaction.generation)}',\
                        ${FormatNull(transaction.generation.country)}`;
                        

            await this.Query(`
                UPDATE fil_renewable_energy_transactions SET 
                                 page_url='${transaction.pageUrl}',\
                                 data_url='${transaction.dataUrl}',\
                                 seller_id=${FormatNull(transaction.sellerId)},\
                                 reporting_start='${transaction.reportingStart}',\
                                 reporting_start_timezone_offset=${transaction.reportingStartTimezoneOffset},\
                                 reporting_end='${transaction.reportingEnd}',\
                                 reporting_end_timezone_offset=${transaction.reportingEndTimezoneOffset},\
                                 tx_hash=${FormatNull(transaction.txHash)},\
                                 buyer_id=${FormatNull(transaction.buyerId)},\
                                 contract_id=${FormatNull(transaction.contractId)},\
                                 created_at=${FormatNull(transaction.createdAt)},\
                                 updated_at=${FormatNull(transaction.updatedAt)},\
                                 reporting_start_local=${FormatNull(transaction.reportingStartLocal)},\
                                 reporting_end_local=${FormatNull(transaction.reportingEndLocal)},\
                                 gen_id=${FormatNull(transaction.generation.id)},\
                                 gen_region=${FormatNull(transaction.generation.region)},\
                                 gen_country=${FormatNull(transaction.generation.country)},\
                                 gen_energy_source=${FormatNull(transaction.generation.energySource)},\
                                 gen_product_type=${FormatNull(transaction.generation.productType)},\
                                 gen_generator_id=${FormatNull(transaction.generation.generatorId)},\
                                 gen_generator_name=${FormatNull(transaction.generation.generatorName)},\
                                 gen_generation_start=${FormatNull(transaction.generation.generationStart)},\
                                 gen_generation_start_timezone_offset=${transaction.generation.generationStartTimezoneOffset},\
                                 gen_generation_end=${FormatNull(transaction.generation.generationEnd)},\
                                 gen_generation_end_timezone_offset=${transaction.generation.generationEndTimezoneOffset},\
                                 gen_tx_hash=${FormatNull(transaction.generation.txHash)},\
                                 gen_initial_seller_id=${FormatNull(transaction.generation.initialSellerId)},\
                                 gen_beneficiary=${FormatNull(transaction.generation.beneficiary)},\
                                 gen_redemption_date=${FormatNull(transaction.generation.redemptionDate)},\
                                 gen_commissioning_date=${FormatNull(transaction.generation.commissioningDate)},\
                                 gen_label=${FormatNull(transaction.generation.label)},\
                                 gen_created_at=${FormatNull(transaction.generation.createdAt)},\
                                 gen_updated_at=${FormatNull(transaction.generation.updatedAt)},\
                                 gen_energyWh=${transaction.generation.energyWh},\
                                 gen_generation_start_local=${FormatNull(transaction.generation.generationStartLocal)},\
                                 gen_generation_end_local=${FormatNull(transaction.generation.generationEndLocal)},\
                                 generation='${JSON.stringify(transaction.generation)}',\
                                 country=${FormatNull(transaction.generation.country)}\
                    WHERE id='${transaction.id}'; \
                INSERT INTO fil_renewable_energy_transactions ( \
                    id, \
                    miner_id, \
                    page_url, \
                    data_url, \
                    seller_id, \
                    reporting_start, \
                    reporting_start_timezone_offset, \
                    reporting_end, \
                    reporting_end_timezone_offset, \
                    tx_hash, \
                    buyer_id, \
                    contract_id, \
                    created_at, \
                    updated_at, \
                    reporting_start_local, \
                    reporting_end_local, \
                    gen_id, \
                    gen_region, \
                    gen_country, \
                    gen_energy_source, \
                    gen_product_type, \
                    gen_generator_id, \
                    gen_generator_name, \
                    gen_generation_start, \
                    gen_generation_start_timezone_offset, \
                    gen_generation_end, \
                    gen_generation_end_timezone_offset, \
                    gen_tx_hash, \
                    gen_initial_seller_id, \
                    gen_beneficiary, \
                    gen_redemption_date, \
                    gen_commissioning_date, \
                    gen_label, \
                    gen_created_at, \
                    gen_updated_at, \
                    gen_energyWh, \
                    gen_generation_start_local, \
                    gen_generation_end_local, \
                    generation, \
                    country \
                    ) \
                    SELECT ${values} WHERE NOT EXISTS (SELECT 1 FROM fil_renewable_energy_transactions WHERE id='${transaction.id}');`,
                    'SaveTransactionRenewableEnergy');

        } catch (err) {
            WARNING(`[SaveTransactionRenewableEnergy] -> ${err}`)
        }
    }

    async save_renewable_energy_from_transactions(transaction) {
        let id = transaction.id;
        let miner = transaction.miner_id;
        let totalEnergy = transaction.recsSoldWh;
        let query = await this.Query(`SELECT t.date::text FROM generate_series(timestamp '${transaction.generation.generationStart}', timestamp '${transaction.generation.generationEnd}', interval  '1 day') AS t(date);`);
        let data_points = query?.rows;
        let country = FormatNull(transaction.generation.country);

        if (data_points && data_points?.length) {

            for (let i = 0; i < data_points.length; i++) {
                let processed_date = data_points[i].date?.split(' ')[0];
                let data = {
                    miner: miner,
                    transaction_id: id,
                    energyWh: totalEnergy / data_points.length,
                    date: processed_date,
                    country: country,
                };

                try {
                    let values = `'${data.miner}', \
                        '${data.transaction_id}', \
                        '${data.date}',\
                        '${data.energyWh}',\
                         ${data.country}`;

                    await this.Query(`
                UPDATE fil_renewable_energy_from_transactions SET 
                                energyWh='${data.energyWh}'\
                    WHERE miner='${data.miner}' AND transaction_id='${data.transaction_id}' AND date='${data.date}' AND country=${data.country}; \
                INSERT INTO fil_renewable_energy_from_transactions ( \
                    miner, \
                    transaction_id, \
                    date, \
                    energyWh, \
                    country \
                    ) \
                    SELECT ${values} WHERE NOT EXISTS (SELECT 1 FROM fil_renewable_energy_from_transactions WHERE miner='${data.miner}' AND transaction_id='${data.transaction_id}' AND date='${data.date}' AND country=${data.country});`,
                        'SaveRenewableEnergyFromTransactions');

                } catch (err) {
                    WARNING(`[SaveRenewableEnergyFromTransactions] -> ${err}`)
                }

            }
        }
    }

    async save_contract_renewable_energy(contract) {
        try {
            let values = `'${contract.id}', \
                        '${contract.miner_id}', \
                        ${FormatNull(contract.productType)},\
                        '${JSON.stringify(contract.energySources)}',\
                        ${FormatNull(contract.contractDate)},\
                        ${FormatNull(contract.deliveryDate)},\
                        ${FormatNull(contract.reportingStart)},\
                        ${FormatNull(contract.reportingEnd)},\
                        '${JSON.stringify(contract.buyer)}',\
                        '${JSON.stringify(contract.seller)}',\
                        ${parseInt(contract.openVolume)},\
                        ${parseInt(contract.deliveredVolume)},\
                        '${JSON.stringify(contract.purchases)}',\
                        ${contract.timezoneOffset},\
                        '${JSON.stringify(contract.filecoinNode)}',\
                        ${FormatNull(contract.externalId)},\
                        ${FormatNull(contract.label)},\
                        ${FormatNull(contract.createdAt)},\
                        ${FormatNull(contract.updatedAt)},\
                        '${JSON.stringify(contract.countryRegionMap)}',\
                        ${FormatNull(contract.countryRegionMap[0]?.country)}`;      

            await this.Query(`
                UPDATE fil_renewable_energy_contracts SET 
                            product_type=${FormatNull(contract.productType)},\
                            energy_sources='${JSON.stringify(contract.energySources)}',\
                            contract_date=${FormatNull(contract.contractDate)},\
                            delivery_date=${FormatNull(contract.deliveryDate)},\
                            reporting_start=${FormatNull(contract.reportingStart)},\
                            reporting_end=${FormatNull(contract.reportingEnd)},\
                            buyer='${JSON.stringify(contract.buyer)}',\
                            seller='${JSON.stringify(contract.seller)}',\
                            open_volume=${parseInt(contract.openVolume)},\
                            delivered_volume=${parseInt(contract.deliveredVolume)},\
                            purchases='${JSON.stringify(contract.purchases)}',\
                            timezone_offset=${contract.timezoneOffset},\
                            filecoin_node='${JSON.stringify(contract.filecoinNode)}',\
                            external_id=${FormatNull(contract.externalId)},\
                            label=${FormatNull(contract.label)},\
                            created_at=${FormatNull(contract.createdAt)},\
                            updated_at=${FormatNull(contract.updatedAt)},\
                            country_region_map='${JSON.stringify(contract.countryRegionMap)}',\
                            country=${FormatNull(contract.countryRegionMap[0]?.country)}\
                    WHERE id='${contract.id}'; \
                INSERT INTO fil_renewable_energy_contracts ( \
                    id, \
                    miner_id, \
                    product_type, \
                    energy_sources, \
                    contract_date, \
                    delivery_date, \
                    reporting_start, \
                    reporting_end, \
                    buyer, \
                    seller, \
                    open_volume, \
                    delivered_volume, \
                    purchases, \
                    timezone_offset, \
                    filecoin_node, \
                    external_id, \
                    label, \
                    created_at, \
                    updated_at, \
                    country_region_map, \
                    country \
                    ) \
                    SELECT ${values} WHERE NOT EXISTS (SELECT 1 FROM fil_renewable_energy_contracts WHERE id='${contract.id}');`,
                    'SaveContractRenewableEnergy');

        } catch (err) {
            WARNING(`[SaveContractRenewableEnergy] -> ${err}`)
        }
    }

    async save_renewable_energy_from_contracts(contract) {
        let id = contract.id;
        let miner = contract.miner_id;
        let totalEnergy = contract.openVolume;
        let query = await this.Query(`SELECT t.date::text FROM generate_series(timestamp '${contract.reportingStart}', timestamp '${contract.reportingEnd}', interval  '1 day') AS t(date);`);
        let data_points = query?.rows;
        let country = FormatNull(contract.countryRegionMap[0]?.country);

        //INFO(`[SaveRenewableEnergyFromContracts] for ${miner} contract.id: ${id} , openVolume: ${totalEnergy}`);

        if (data_points && data_points?.length) {
            for (let i = 0; i < data_points.length; i++) {
                let processed_date = data_points[i].date?.split(' ')[0];
                let data = {
                    miner: miner,
                    contract_id: id,
                    energyWh: totalEnergy / data_points.length,
                    date: processed_date,
                    country: country,
                };

                try {
                    let values = `'${data.miner}', \
                        '${data.contract_id}', \
                        '${data.date}',\
                        '${data.energyWh}',\
                         ${data.country}`;

                    await this.Query(`
                UPDATE fil_renewable_energy_from_contracts SET 
                                energyWh='${data.energyWh}'\
                    WHERE miner='${data.miner}' AND contract_id='${data.contract_id}' AND date='${data.date}' AND country=${data.country}; \
                INSERT INTO fil_renewable_energy_from_contracts ( \
                    miner, \
                    contract_id, \
                    date, \
                    energyWh, \
                    country \
                    ) \
                    SELECT ${values} WHERE NOT EXISTS (SELECT 1 FROM fil_renewable_energy_from_contracts WHERE miner='${data.miner}' AND contract_id='${data.contract_id}' AND date='${data.date}'  AND country=${data.country});`,
                        'SaveRenewableEnergyFromContracts');

                } catch (err) {
                    WARNING(`[SaveRenewableEnergyFromContracts] -> ${err}`)
                }

            }
        }
    }

    async location_get() {
        let locations = [];

        const result = await this.Query(`SELECT * FROM fil_miners_location`, 'LocationGet');
        if (result?.rows) {
            locations = result?.rows;
        }

        return locations;
    }

    async location_add(list) {
        if (list.length) {
            let values = '';
            for (const item of list) {
                values += `('${item.miner}', \
                        ${item.lat},\
                        ${item.long},\
                        ${FormatNull(item.ba)},\
                        ${FormatNull(item.region)},\
                        ${FormatNull(item.country)},\
                        ${FormatNull(FormatText(item.city))},\
                        ${item.locations}),`;
            }

            values = values.slice(0, -1) + ';';


            await this.Query(`\
           INSERT INTO fil_miners_location (miner, lat, long, ba, region, country, city, locations) \
           VALUES ${values}`, 'LocationAdd');
        }
    }

    async location_delete(miner) {
        await this.Query(`DELETE FROM fil_miners_location WHERE miner = '${miner}';`, 'LocationDelete');
    }

    async get_ba_list() {
        let locations = [];

        const result = await this.Query(`SELECT DISTINCT ba FROM fil_miners_location WHERE ba is not null;`, 'LocationGetBAList');
        if (result?.rows) {
            locations = result?.rows;
        }

        return locations;
    }

    async get_ba_start_date(ba) {
        let start_date = [];

        const result = await this.Query(`SELECT MAX(date) FROM fil_wt WHERE ba = '${ba}';`, 'LocationGetBAStartDate');
        if (result?.rows) {
            start_date = result?.rows[0].max;
        }

        return start_date;
    }

    async wt_data_add(list) {
        if (list.length) {
            let values = '';
            for (const item of list) {
                values += `('${item.ba}', \
                        ${item.value},\
                        '${item.date}'),`;
            }

            values = values.slice(0, -1) + ';';


            await this.Query(`\
           INSERT INTO fil_wt (ba, value, date) \
           VALUES ${values}`, 'WTDataAdd');
        }
    }

    async get_lily_start_date() {
        let start_date = [];

        const result = await this.Query(`SELECT MAX(date) FROM fil_miner_days_lily;`, 'LilyGetStartDate');
        if (result?.rows) {
            start_date = result?.rows[0].max;
        }

        return start_date;
    }

    async save_lily_data(data) {
        let query = '';
        for (let i = 0; i < data.length; i++) {
            try {
                let d = data[i];
                let values = `'${d.miner_id}', \
                            '${d.raw_byte_power}',\
                            '${d.stat_date}'`;
                query += `\
                    INSERT INTO fil_miner_days_lily (miner, power, date) \
                        SELECT ${values} WHERE NOT EXISTS (SELECT 1 FROM fil_miner_days_lily WHERE miner='${d.miner_id}' AND date='${d.stat_date}');`;
            } catch (err) {
                WARNING(`[SaveLilyData] -> ${err}`)
            }
        }
        await this.Query(query, 'SaveLilyData');
    }


    async refresh_emissions_views() {
        INFO('Refresh Emissions Energy Views');
        try {
            await this.Query("REFRESH MATERIALIZED VIEW CONCURRENTLY fil_location_view WITH DATA;", 'RefreshEmissionsMatViews');
            await this.Query("REFRESH MATERIALIZED VIEW CONCURRENTLY fil_wt_view WITH DATA;", 'RefreshEmissionsMatViews');
            await this.Query("REFRESH MATERIALIZED VIEW CONCURRENTLY fil_un_view WITH DATA;", 'RefreshEmissionsMatViews');
            await this.Query("REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miner_view_days_lily_v1 WITH DATA;", 'RefreshEmissionsMatViews');
            await this.Query("REFRESH MATERIALIZED VIEW CONCURRENTLY fil_emissions_view_v4 WITH DATA;", 'RefreshEmissionsMatViews');
            await this.Query("REFRESH MATERIALIZED VIEW CONCURRENTLY fil_miners_data_view_country_v4 WITH DATA;", 'RefreshEmissionsMatViews');
            await this.Query("REFRESH MATERIALIZED VIEW CONCURRENTLY fil_map_view_v3 WITH DATA;", 'RefreshEmissionsMatViews');
        } catch (err) {
            WARNING(`[RefreshEmissionsMatViews] ${err}`)
        }
        INFO('Refresh Emissions Energy Views, done');
    }
}

module.exports = {
    DB
}