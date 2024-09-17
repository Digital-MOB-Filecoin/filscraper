const config = require('./config');
const cbor = require('borc');
const BN = require('bn.js');
const Big = require('big.js');
const { version } = require('./package.json');
const { INFO, ERROR, WARNING } = require('./logs');
const { FilecoinChainInfo } = require('./filecoinchaininfo');
const { Lotus } = require('./lotus');
const { Migrations } = require('./migrations');
const { DB } = require('./db');
const { MinerMethods } = require('./miner-methods');
const { decodeRLE2 } = require('./rle');
const { hdiff } = require('./utils');
const { ZeroLabsClient } = require('./zerolabs-client');
const { Location } = require('./location');
const { WT } = require('./watttime');
const { LilyClient } = require('./lily-client');
const { FilinfoClient } = require('./filinfo-client');
require('./cron');

const SCRAPE_LIMIT = 10 // blocks
const INFURA_SCRAPE_LIMIT = 2 // blocks
const INSERT_LIMIT = 100 // rows
const RESCRAPE_INTERVAL = 24 // hours
let last_rescrape = new Date(new Date().setHours(new Date().getHours() - 2));
let miner_sectors = new Map();

let filecoinChainInfo = new FilecoinChainInfo(config.lotus.api, config.lotus.token);
let filecoinChainInfoInfura = new FilecoinChainInfo(config.lotus.api_infura, config.lotus.token);
let zeroLabsClient = new ZeroLabsClient(config.scraper.renewable_energy_api, config.scraper.renewable_energy_token);
let lotus_infura = new Lotus(config.lotus.api_infura,  config.lotus.token);
let migrations = new Migrations();
let db = new DB();
let location = new Location();
let wt = new WT();
let lilyClient = new LilyClient(config.lily.api, config.lily.token, config.lily.start_date);
let filinfoClient = new FilinfoClient(config.scraper.filinfo_api);

let stop = false;

function decode_sectors(buffer) {
    let sectors = [];
    try {
        let decoded_rle = decodeRLE2(buffer);
        if (decoded_rle) {
            sectors = decoded_rle;
        }
    } catch (error) {
        ERROR(`[decodeRLE2] failed for params: ${buffer}`);
    }

    return sectors;
}

function get_miner_events(miner_events, miner, epoch) {
    let miner_event;

    if (miner_events.has(miner)) {
        miner_event = miner_events.get(miner);
    } else {
        miner_event = {
            miner: miner,
            commited: new BN('0', 10),
            used: new BN('0', 10),
            total: new BN('0', 10),
            activated: 0,
            terminated: 0,
            faults: 0,
            recovered: 0,
            proofs: 0,
            epoch: epoch
        }
    }

    return miner_event;
}

async function get_sector_size(miner) {
    let sector_size = 34359738368;

    if (miner_sectors.has(miner)) {
        sector_size = miner_sectors.get(miner);
    } else {
        sector_size = await db.get_sector_size(miner);

        if (!sector_size) {
            const minerInfo = await lotus_infura.StateMinerInfo(miner);

            if (minerInfo?.data?.result?.SectorSize) {
                let sectorSize = minerInfo?.data.result.SectorSize;
                sector_size = sectorSize;
                await db.save_sector_size(miner, sector_size);

                INFO(`[GetSectorSize] miner: ${miner} -> ${sector_size}`);
            } else {
                ERROR(`[GetSectorSize] miner: ${miner} lotus.StateMinerInfo:  ${JSON.stringify(minerInfo?.data)}`);
            }
        }

        miner_sectors.set(miner, sector_size);
    }

    return sector_size;
}

async function process_messages(block, messages) {
    let miner_events = new Map();
    var messagesSlice = messages;

    var used = new BN('0', 10);
    var commited = new BN('0', 10);
    let deals = [];
    let sectors = [];
    let sectors_events = [];
    let miners_events = [];

    while (messagesSlice.length) {
        await Promise.all(messagesSlice.splice(0, SCRAPE_LIMIT).map(async (msg) => {
            if (msg.ExitCode == 0 && msg.Params && msg.To.startsWith('f0')) {
                let miner = msg.To;
                let decoded_params = [];
                try {
                    decoded_params = cbor.decode(msg.Params, 'base64');
                } catch (error) {
                    if (msg.Params != 'null') {
                        ERROR(`[ProcessMessages] error cbor.decode[${msg.Params}] : ${error}`);
                    }
                }

                if (decoded_params.length > 0) {
                    switch (msg.Method) {
                        case MinerMethods.PreCommitSector: {
                            const preCommitSector = {
                                DealIDs: decoded_params[4],
                                Expiration: decoded_params[5],
                                ReplaceCapacity: decoded_params[6],
                                ReplaceSectorDeadline: decoded_params[7],
                                ReplaceSectorNumber: decoded_params[8],
                                ReplaceSectorPartition: decoded_params[9],
                                SealProof: decoded_params[0],
                                SealRandEpoch: decoded_params[3],
                                SealedCID: decoded_params[2],
                                SectorNumber: decoded_params[1]
                            }

                            const sector_size = await get_sector_size(miner);

                            let sector_info = {
                                sector: preCommitSector.SectorNumber,
                                miner: miner,
                                type: 'commited',
                                size: sector_size,
                                start_epoch: msg.Block,
                                end_epoch: preCommitSector.Expiration,
                            }

                            if (preCommitSector.DealIDs?.length == 0) {
                                sectors.push(sector_info);
                                commited = commited.add(new BN(sector_size));

                                let miner_event = get_miner_events(miner_events, miner, msg.Block)
                                miner_event.activated++;
                                miner_event.commited = miner_event.commited.add(new BN(sector_size));
                                miner_events.set(miner, miner_event);
                            } else {
                                //used sector
                                sector_info.type = 'used';
                                sectors.push(sector_info);
                                used = used.add(new BN(sector_size));

                                for (let i = 0; i < preCommitSector.DealIDs.length; i++) {
                                    let deal_info = {
                                        deal: preCommitSector.DealIDs[i],
                                        sector: preCommitSector.SectorNumber,
                                        miner: miner,
                                        start_epoch: msg.Block,
                                        end_epoch: preCommitSector.Expiration,
                                    }
                                    deals.push(deal_info);
                                }

                                let miner_event = get_miner_events(miner_events, miner, msg.Block)
                                miner_event.activated++;
                                miner_event.used = miner_event.used.add(new BN(sector_size));
                                miner_events.set(miner, miner_event);
                            }
                        }
                            break;
                        case MinerMethods.PreCommitSectorBatch2: {
                            let all_sectors = decoded_params[0];
                            for (let i = 0; i < all_sectors.length; i++) {
                                let current_sector = all_sectors[i];
                                const preCommitSector = {
                                    DealIDs: current_sector[4],
                                    Expiration: current_sector[5],
                                    UnsealedSectorCid: current_sector[6],
                                    SealProof: current_sector[0],
                                    SealRandEpoch: current_sector[3],
                                    SealedCID: current_sector[2],
                                    SectorNumber: current_sector[1]
                                }

                                const sector_size = await get_sector_size(miner);

                                let sector_info = {
                                    sector: preCommitSector.SectorNumber,
                                    miner: miner,
                                    type: 'commited',
                                    size: sector_size,
                                    start_epoch: msg.Block,
                                    end_epoch: preCommitSector.Expiration,
                                }

                                if (preCommitSector.DealIDs?.length == 0) {
                                    sectors.push(sector_info);
                                    commited = commited.add(new BN(sector_size));

                                    let miner_event = get_miner_events(miner_events, miner, msg.Block)
                                    miner_event.activated++;
                                    miner_event.commited = miner_event.commited.add(new BN(sector_size));
                                    miner_events.set(miner, miner_event);
                                } else {
                                    //used sector
                                    sector_info.type = 'used';
                                    sectors.push(sector_info);
                                    used = used.add(new BN(sector_size));

                                    for (let i = 0; i < preCommitSector.DealIDs.length; i++) {
                                        let deal_info = {
                                            deal: preCommitSector.DealIDs[i],
                                            sector: preCommitSector.SectorNumber,
                                            miner: miner,
                                            start_epoch: msg.Block,
                                            end_epoch: preCommitSector.Expiration,
                                        }
                                        deals.push(deal_info);
                                    }

                                    let miner_event = get_miner_events(miner_events, miner, msg.Block)
                                    miner_event.activated++;
                                    miner_event.used = miner_event.used.add(new BN(sector_size));
                                    miner_events.set(miner, miner_event);

                                }
                            }
                        }
                            break;
                        case MinerMethods.TerminateSectors: {
                            let sectors = decode_sectors(Buffer.from(decoded_params[0][0][2]));

                            for (let i = 0; i < sectors.length; i++) {
                                let sector_events = {
                                    type: 'terminate',
                                    miner: miner,
                                    sector: sectors[i],
                                    epoch: msg.Block,
                                }

                                sectors_events.push(sector_events);
                            }

                            let miner_event = get_miner_events(miner_events, miner, msg.Block)
                            miner_event.terminated++;
                            miner_events.set(miner, miner_event);
                        }
                            break;
                        case MinerMethods.DeclareFaults: {
                            let sectors = decode_sectors(Buffer.from(decoded_params[0][0][2]));

                            for (let i = 0; i < sectors.length; i++) {
                                let sector_events = {
                                    type: 'fault',
                                    miner: miner,
                                    sector: sectors[i],
                                    epoch: msg.Block,
                                }

                                sectors_events.push(sector_events);
                            }

                            let miner_event = get_miner_events(miner_events, miner, msg.Block)
                            miner_event.faults++;
                            miner_events.set(miner, miner_event);
                        }
                            break;
                        case MinerMethods.DeclareFaultsRecovered: {
                            let sectors;
                            try {
                                sectors = decode_sectors(Buffer.from(decoded_params[0][0][2]));
                            } catch (e) {
                                ERROR('[ProcessMessages] MinerMethods.DeclareFaultsRecovered decode error');
                                console.log(`RAW SECTORS ${decoded_params[0][0][2]}`);
                                console.error(e);
                                break;
                            }

                            for (let i = 0; i < sectors.length; i++) {
                                let sector_events = {
                                    type: 'recovered',
                                    miner: miner,
                                    sector: sectors[i],
                                    epoch: msg.Block,
                                }

                                sectors_events.push(sector_events);
                            }

                            let miner_event = get_miner_events(miner_events, miner, msg.Block)
                            miner_event.recovered++;
                            miner_events.set(miner, miner_event);
                        }
                            break;
                        case MinerMethods.ProveCommitSector: {
                            let sector_events = {
                                type: 'proof',
                                miner: miner,
                                sector: decoded_params[0],
                                epoch: msg.Block,
                            }

                            sectors_events.push(sector_events);

                            let miner_event = get_miner_events(miner_events, miner, msg.Block)
                            miner_event.proofs++;
                            miner_events.set(miner, miner_event);
                        }
                            break;

                        default:
                    }
                }
            }
        }))
    }

    INFO(`[ProcessBlock] ${block} sectors: ${sectors.length} , sector events: ${sectors_events.length} , miner events: ${miner_events.size} , deals ${deals.length}`);

    let sectorsSlice = sectors;
    while (sectorsSlice.length) {
        await db.save_sectors(sectorsSlice.splice(0, INSERT_LIMIT));
    }

    let sectorsEventsSlice = sectors_events;
    while (sectorsEventsSlice.length) {
        await db.save_sectors_events(sectorsEventsSlice.splice(0, INSERT_LIMIT));
    }

    let dealsSlice = deals;
    while (dealsSlice.length) {
        await db.save_deals(dealsSlice.splice(0, INSERT_LIMIT));
    }

    miner_events.forEach((value, key, map) => {
        value.total = value.commited.add(value.used);
        let miner_fraction = new Big('0');

        if (!value.total.isZero()) {
            miner_fraction = new Big(value.used.toString(10));
            miner_fraction = miner_fraction.div(new Big(value.total.toString(10)));
        }
        value.fraction = miner_fraction;

        miners_events.push({miner: key, ...value})
    });

    let minersEventsSlice = miners_events;
    while (minersEventsSlice.length) {
        await db.save_miners_events(minersEventsSlice.splice(0, INSERT_LIMIT));
    }

}

async function scrape_block(block, msg, rescrape, reprocess) {
    let scraped_from_db = false;
    const found = await db.have_block(block);
    if (found) {
        //TODO: delete bad block mark if exists
        INFO(`[${msg}] ${block} already scraped, skipping`);
        return;
    }

    let messages = [];

    const found_messages = await db.have_messages(block);
    if (found_messages) {
        INFO(`[${msg}] ${block} from db`);
        messages = await db.get_messages(block);
        scraped_from_db = true;
    } else if (!reprocess) {
        INFO(`[${msg}] ${block}`);
        if (rescrape) {
            messages = await filecoinChainInfo.GetBlockMessages(block);
        } else {
            messages = await filecoinChainInfoInfura.GetBlockMessages(block);
        }
    }

    if (messages && messages.length > 0) {
        INFO(`[${msg}] ${block}, ${messages.length} messages`);
        await db.save_block(block, messages.length, !scraped_from_db);
        if (!scraped_from_db) {
            await db.save_messages(messages);
        }
        await process_messages(block, messages);

        INFO(`[${msg}] ${block} done`);
    } 
    // scrape_block_from_filinfo will mark as bad block
    /*else {
        await db.save_bad_block(block)
        WARNING(`[${msg}] ${block} mark as bad block`);
    }*/
}

async function scrape_block_from_filinfo(block, msg) {
    let scraped_from_db = false;
    const found = await db.have_block(block);
    if (found) {
        //TODO: delete bad block mark if exists
        INFO(`[${msg}] ${block} already scraped, skipping`);
        return;
    }

    let messages = [];

    const found_messages = await db.have_messages(block);
    if (found_messages) {
        INFO(`[${msg}] ${block} from db`);
        messages = await db.get_messages(block);
        scraped_from_db = true;
    } else {
        messages = await filinfoClient.getMessages(block);
    }

    if (messages && messages.length > 0) {
        INFO(`[${msg}] ${block}, ${messages.length} messages`);
        await db.save_block(block, messages.length, !scraped_from_db);
        if (!scraped_from_db) {
            await db.save_messages(messages, true);
        }
        await process_messages(block, messages);

        INFO(`[${msg}] ${block} done`);
    } else {
        await db.save_bad_block(block)
        WARNING(`[${msg}] ${block} mark as bad block`);
    }
}

async function scrape(reprocess, check_for_missing_blocks) {
    const chainHead = await filecoinChainInfoInfura.GetChainHead();
    if (!chainHead) {
        ERROR(`[Scrape] error : unable to get chain head`);
        return;
    }

    let start_block = parseInt(config.scraper.start);
    const end_block = chainHead - 1;

    if (!check_for_missing_blocks) {
        start_block = await db.get_start_block();

        if ((end_block - start_block) > 1000) {
            start_block = end_block - 1000;
        }
    }

    var scrape_start_time = new Date().getTime();
    INFO(`[Scrape] scrape from ${start_block} to ${end_block}`);

    let blocks = [];
    for (let i = start_block; i < end_block; i++) {
        blocks.push(i);
    }

    let no_blocks = blocks.length;

    let scrape_limit = SCRAPE_LIMIT;
    if (!reprocess) {
        scrape_limit = INFURA_SCRAPE_LIMIT;
    }

    var blocksSlice = blocks;
    while (blocksSlice.length) {
        await Promise.all(blocksSlice.splice(0, scrape_limit).map(async (block) => {
            try {
                await scrape_block(block,'ScrapeBlock', check_for_missing_blocks, reprocess);
            } catch (error) {
                ERROR(`[Scrape] error :`);
                console.error(error);
            }
        }));
    }

    var scrape_end_time = new Date().getTime();
    var duration = (scrape_end_time - scrape_start_time) / 1000;

    INFO(`[Scrape] scrape ${no_blocks} in ${duration} sec`);
}

async function rescrape() {
    let blocks;
    let i = 0;

    if (hdiff(last_rescrape) < RESCRAPE_INTERVAL) {
        INFO('[Rescrape] skip');
        return;
    }

    INFO('[Rescrape]');

    last_rescrape = Date.now();

    do {
        blocks = await db.get_bad_blocks(SCRAPE_LIMIT, i * SCRAPE_LIMIT);

        if (blocks) {
            await Promise.all(blocks.map(async (block) => {
                try {
                    await scrape_block(parseInt(block.block), 'RescrapeBadBlock', true, false);
                } catch (error) {
                    ERROR(`[Rescrape] error :`);
                    console.error(error);
                }
            }));
        }

        i++;

    } while (blocks && blocks?.length == SCRAPE_LIMIT);
}

async function rescrape_missing_blocks(reprocess) {
    INFO(`[RescrapeMissingBlocks]`);
    const chainHead = await filecoinChainInfoInfura.GetChainHead();
    if (!chainHead) {
        ERROR(`[RescrapeMissingBlocks] error : unable to get chain head`);
        return;
    }
    const head = chainHead - 1;

    INFO(`[RescrapeMissingBlocks] from [${head}, 0]`);
    let missing_blocks = await db.get_missing_blocks(head);
    INFO(`[RescrapeMissingBlocks] total missing blocks: ${missing_blocks.length}`);

    var blocksSlice = missing_blocks;
    while (blocksSlice.length) {
        await Promise.all(blocksSlice.splice(0, SCRAPE_LIMIT).map(async (item) => {
            try {
                await scrape_block(parseInt(item.missing_block),'RescrapeMissingBlock', true, reprocess);
            } catch (error) {
                ERROR(`[RescrapeMissingBlocks] error :`);
                console.error(error);
            }
        }));
    }
}

async function rescrape_missing_blocks_from_filinfo() {
    INFO(`[RescrapeMissingBlockFromFilinfo]`);
    const chainHead = await filecoinChainInfoInfura.GetChainHead();
    if (!chainHead) {
        ERROR(`[RescrapeMissingBlockFromFilinfo] error : unable to get chain head`);
        return;
    }
    const head = chainHead - 1;
    const start = 2671000;

    INFO(`[RescrapeMissingBlockFromFilinfo] from [${head - 10}, ${start}]`);
    
    let missing_blocks = await db.get_missing_blocks(head - 10, start);
    INFO(`[RescrapeMissingBlockFromFilinfo] total missing blocks: ${missing_blocks.length}`);

    var blocksSlice = missing_blocks;
    while (blocksSlice.length) {
        await Promise.all(blocksSlice.splice(0, INFURA_SCRAPE_LIMIT).map(async (item) => {
            try {
                await scrape_block_from_filinfo(parseInt(item.missing_block),'RescrapeMissingBlockFromFilinfo');
            } catch (error) {
                ERROR(`[RescrapeMissingBlockFromFilinfo] error :`);
                console.error(error);
            }
        }));
    }
}

async function rescrape_msg_cid() {
    INFO(`[RescrapeMsgCid]`);
    let head = await db.get_start_block();
    INFO(`[RescrapeMsgCid] from [${head}, 0]`);
    let blocks_with_missing_cid = await db.get_blocks_with_missing_cid(head);

    INFO(`[RescrapeMsgCid] total blocks with missing cid: ${blocks_with_missing_cid.length}`);

    let tipSetKey = null;
    let tmpTipSetKey = null;

    var blocksSlice = blocks_with_missing_cid;
    while (blocksSlice.length) {
        await Promise.all(blocksSlice.splice(0, SCRAPE_LIMIT).map(async (item) => {
            try {
                let block = parseInt(item.block_with_missing_cid);

                INFO(`[RescrapeMsgCid] ${block}`);
                const result = await filecoinChainInfo.GetBlockMessagesByTipSet(block, tipSetKey);
                if (result) {
                    tmpTipSetKey = result.tipSetKey;

                    if (result?.messages.length) {
                        await db.save_messages_cids(result?.messages);
                        await db.mark_block_with_msg_cid(block);
                        INFO(`[RescrapeMsgCid] ${block} done`);
                    } else {
                        ERROR(`[RescrapeMsgCid] ${block} no messages`);
                    }
                } else {
                    INFO(`[RescrapeMsgCid] ${block} error: ${JSON.stringify(result)}`);
                }
            } catch (error) {
                ERROR(`[RescrapeMsgCid] ${item.block_with_missing_cid} error :`, error);
            }
        }));

        tipSetKey = tmpTipSetKey;
    }
}


async function rescrape_msg_cid_filplus() {
    INFO(`[RescrapeMsgCidFilPlus]`);

    const { FILPLUS_CIDS } = require('./filplus-cids')
   
    let blocks_with_missing_cid = FILPLUS_CIDS.sort((a,b)=>b-a);

    INFO(`[RescrapeMsgCidFilPlus] total blocks with missing cid: ${blocks_with_missing_cid.length}`);

    let tipSetKey = null;
    let tmpTipSetKey = null;

    var blocksSlice = blocks_with_missing_cid;
    while (blocksSlice.length) {
        await Promise.all(blocksSlice.splice(0, SCRAPE_LIMIT).map(async (item) => {
            try {
                let block = parseInt(item);

                INFO(`[RescrapeMsgCidFilPlus] ${block}`);
                const result = await filecoinChainInfo.GetBlockMessagesByTipSet(block, tipSetKey);
                if (result) {
                    tmpTipSetKey = result.tipSetKey;

                    if (result?.messages.length) {
                        await db.save_messages_cids(result?.messages);
                        await db.mark_block_with_msg_cid(block);
                        INFO(`[RescrapeMsgCidFilPlus] ${block} done`);
                    } else {
                        ERROR(`[RescrapeMsgCidFilPlus] ${block} no messages`);
                    }
                } else {
                    INFO(`[RescrapeMsgCidFilPlus] ${block} error: ${JSON.stringify(result)}`);
                }
            } catch (error) {
                ERROR(`[RescrapeMsgCidFilPlus] ${item.block_with_missing_cid} error :`, error);
            }
        }));

        tipSetKey = tmpTipSetKey;
    }
}

async function update_renewable_energy() {
    //TODO scrape every 24 hours
    //TODO update views every 24 hours

    INFO('[UpdateRenewableEnergy] start');

    try {
        let minersResponse = await zeroLabsClient.GetMiners();
        if (minersResponse?.status == 200 && minersResponse?.data) {
            miners = minersResponse.data.data;

            if (miners?.length > 0) {
                INFO('[UpdateRenewableEnergy] reset renewable energy data');
                await db.reset_renewable_energy_data();
                INFO('[UpdateRenewableEnergy] reset renewable energy data, done');


                for (const miner of miners) {
                    let transactionsResponse = await zeroLabsClient.GetTransactions(miner.id);
                    let contractsResponse = await zeroLabsClient.GetContracts(miner.id);

                    if (transactionsResponse?.status == 200 &&
                        contractsResponse?.status == 200 &&
                        transactionsResponse?.data &&
                        contractsResponse?.data) {

                        let transactions = transactionsResponse.data?.transactions;
                        let contracts = contractsResponse.data?.contracts;

                        let minerData = { ...miner, recsTotal: transactionsResponse.data?.recsTotal };

                        INFO(`[UpdateRenewableEnergy] for ${miner.id} , transactions: ${transactions.length} , contracts: ${contracts.length}`);
                        await db.save_miner_renewable_energy(minerData);

                        if (transactions?.length) {
                            for (const transaction of transactions) {
                                let transactionData = { ...transaction, miner_id: miner.id };
                                await db.save_renewable_energy_from_transactions(transactionData);
                                await db.save_transaction_renewable_energy(transactionData);
                            }
                        }

                        if (contracts?.length) {
                            for (const contract of contracts) {
                                let contractData = { ...contract, miner_id: miner.id };
                                await db.save_renewable_energy_from_contracts(contractData)
                                await db.save_contract_renewable_energy(contractData);
                            }
                        }
                    }
                }
            }
        }

    } catch (e) {
        ERROR(`[UpdateRenewableEnergy] error : ${e}`);
    }

    INFO('[UpdateRenewableEnergy] done');
}


async function filscraper_version() {
    INFO(`FilScraper version: ${version}`);
};

const pause = (timeout) => new Promise(res => setTimeout(res, timeout * 1000));

async function refresh_views() {
    INFO('Refresh Views');
    await db.refresh_miner_view_epochs();
    await db.refresh_miner_view_days();
    await db.refresh_miners_view();
    await db.refresh_sealed_capacity_view();
    INFO('Refresh Views, done');
}

async function refresh_renewable_energy_views() {
    INFO('Refresh Renewable Energy Views');
    await db.refresh_renewable_energy_views();
    await db.refresh_energy_ratio_views();
    INFO('Refresh Renewable Energy Views, done');
}

const mainLoop = async _ => {
    let last_update_renewable_energy = 0 /*Date.now()*/;
    let last_update_emissions = 0;

    try {
        let reprocess = false;
        if (config.scraper.reprocess == 1) {
            WARNING('Reprocess');
            reprocess = true;
        }

        INFO('Run migrations');
        await migrations.run();
        await migrations.create_indexes();
        INFO('Run migrations, done');

        if (config.scraper.reprocess != 1 && config.scraper.lock_views != 1) {
            if (config.scraper.await_refresh_views != 1) {
                refresh_views();
            } else {
                await refresh_views();
            }
            
            setInterval(async () => {
                await refresh_views();
            }, 24 * 3600 * 1000); // refresh every 24 hours
        }

        if (config.scraper.rescrape_msg_cid_filplus == 1) {
            await rescrape_msg_cid_filplus();
        }

        while (!stop) {
            //await rescrape_missing_blocks_from_filinfo();

            let current_timestamp = Date.now();
            if ((current_timestamp - last_update_emissions) > 8*3600*1000) {
                await lilyClient.update();
                await location.update();
                await wt.update();
                await db.refresh_emissions_views();
                last_update_emissions = current_timestamp;
            }
            /*if ((current_timestamp - last_update_renewable_energy) > 12*3600*1000) {
                await update_renewable_energy();
                await refresh_renewable_energy_views();
                last_update_renewable_energy = current_timestamp;
            }*/

            if (config.scraper.rescrape_msg_cid == 1) {
                await rescrape_msg_cid();
            }
            if (config.scraper.rescrape_missing_blocks == 1) {
                await rescrape_missing_blocks(reprocess);
            }
            await scrape(reprocess, config.scraper.check_missing_blocks == 1);
            await rescrape();

            INFO(`Pause for 60 seconds`);
            await pause(60);
        }

    } catch (error) {
        ERROR(`[MainLoop] error :`);
        console.error(error);
        ERROR(`Shutting down`);
        process.exit(1);
    }

}

if(config.scraper.disable_main_loop == 0) {
    mainLoop();
} else {
    INFO('Main Loop disabled');
}

function shutdown(exitCode = 0) {
    stop = true;
    setTimeout(() => {
        INFO(`Shutdown`);
        process.exit(exitCode);
    }, 3000);
}
//listen for TERM signal .e.g. kill
process.on('SIGTERM', shutdown);
// listen for INT signal e.g. Ctrl-C
process.on('SIGINT', shutdown);
