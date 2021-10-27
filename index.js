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

const SCRAPE_LIMIT = 10 // blocks
const INSERT_LIMIT = 100 // rows
const RESCRAPE_INTERVAL = 24 // hours
let last_rescrape = new Date(new Date().setHours(new Date().getHours() - 2));
let miner_sectors = new Map();

let filecoinChainInfo = new FilecoinChainInfo(config.lotus.api, config.lotus.token);
let filecoinChainInfoInfura = new FilecoinChainInfo(config.lotus.api_infura, 'token');
let lotus_infura = new Lotus(config.lotus.api_infura, 'token');
let migrations = new Migrations();
let db = new DB();
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
                        case MinerMethods.PreCommitSectorBatch: {
                            let all_sectors = decoded_params[0];
                            for (let i = 0; i < all_sectors.length; i++) {
                                let current_sector = all_sectors[i];
                                const preCommitSector = {
                                    DealIDs: current_sector[4],
                                    Expiration: current_sector[5],
                                    ReplaceCapacity: current_sector[6],
                                    ReplaceSectorDeadline: current_sector[7],
                                    ReplaceSectorNumber: current_sector[8],
                                    ReplaceSectorPartition: current_sector[9],
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
                            let sectors = decode_sectors(Buffer.from(decoded_params[0][0][2]));

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

    let total = commited.add(used);

    let fraction = new Big('0');

    if (!total.isZero()) {
        fraction = new Big(used.toString(10));
        fraction = fraction.div(new Big(total.toString(10)));
    }

    let network_info = {
        epoch: block, 
        used: used, 
        commited: commited,
        total: total,
        fraction: fraction
    }

    INFO(`[ProcessBlock] ${block} sectors: ${sectors.length} , sector events: ${sectors_events.length} , miner events: ${miner_events.size} , deals ${deals.length}`);

    await db.save_network(network_info);

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

    let start_block = 0;
    const end_block = chainHead - 1;

    if (!check_for_missing_blocks) {
        start_block = await db.get_start_block();
    }

    var scrape_start_time = new Date().getTime();
    INFO(`[Scrape] scrape from ${start_block} to ${end_block}`);

    let blocks = [];
    for (let i = start_block; i <= end_block; i++) {
        blocks.push(i);
    }

    let no_blocks = blocks.length;

    var blocksSlice = blocks;
    while (blocksSlice.length) {
        await Promise.all(blocksSlice.splice(0, SCRAPE_LIMIT).map(async (block) => {
            try {
                await scrape_block(block,'ScrapeBlock', false, reprocess);
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


async function filscraper_version() {
    INFO(`FilScraper version: ${version}`);
};

const pause = (timeout) => new Promise(res => setTimeout(res, timeout * 1000));

async function refresh_views() {
    INFO('Refresh Views');
    await db.refresh_network_view_epochs();
    await db.refresh_network_view_days();
    await db.refresh_miner_view_epochs();
    await db.refresh_miner_view_days();
    await db.refresh_miners_view();
    INFO('Refresh Views, done');
}

const mainLoop = async _ => {
    try {
        let reprocess = false;
        if (config.scraper.reprocess == 1) {
            WARNING('Reprocess');
            reprocess = true;
        }

        INFO('Run migrations');
        await migrations.run();
        INFO('Run migrations, done');

        if (config.scraper.reprocess != 1 && config.scraper.lock_views != 1) {
            await migrations.create_indexes();
            if (config.scraper.await_refresh_views != 1) {
                refresh_views();
            } else {
                await refresh_views();
            }
            
            setInterval(async () => {
                await refresh_views();
            }, 12 * 3600 * 1000); // refresh every 12 hours
        }

        if (config.scraper.rescrape_msg_cid_filplus == 1) {
            await rescrape_msg_cid_filplus();
        }

        while (!stop) {
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

mainLoop();

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