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

const SCRAPE_LIMIT = 100 // blocks
const RESCRAPE_INTERVAL = 1 // hours
let last_rescrape = Date.now();

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
    let sector_size = await db.get_sector_size(miner);

    if (!sector_size) {
        const minerInfo = await lotus_infura.StateMinerInfo(miner);

        if (minerInfo?.data?.result?.SectorSize) {
            let sectorSize = minerInfo?.data.result.SectorSize;
            sector_size = sectorSize;
            await db.save_sector_size(miner, sector_size);

            INFO(`[GetSectorSize] miner: ${miner} -> ${sector_size}`);
        } else {
            ERROR(`[GetSectorSize] miner: ${miner} lotus.StateMinerInfo:  ${JSON.stringify(minerInfo?.data)}`);
            sector_size = 34359738368;
        }
    }

    return sector_size;
}

async function process_messages(block, messages) {
    let miner_events = new Map();
    var messagesSlice = messages;

    var used = new BN('0', 10);
    var commited = new BN('0', 10);

    while (messagesSlice.length) {
        await Promise.all(messagesSlice.splice(0, 50).map(async (msg) => {
            if (msg.ExitCode == 0 && msg.Params && msg.To.startsWith('f0')) {
                let miner = msg.To;
                let decoded_params = [];
                try {
                    decoded_params = cbor.decode(msg.Params, 'base64');
                } catch (error) {
                    ERROR(`[ProcessMessages] error cbor.decode[${msg.Params}] : ${error}`);
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
                                db.save_sector(sector_info);
                                commited = commited.add(new BN(sector_size));

                                let miner_event = get_miner_events(miner_events, miner, msg.Block)
                                miner_event.activated++;
                                miner_event.commited = miner_event.commited.add(new BN(sector_size));
                                miner_events.set(miner, miner_event);
                            } else {
                                //used sector
                                sector_info.type = 'used';
                                db.save_sector(sector_info);
                                used = used.add(new BN(sector_size));

                                for (let i = 0; i < preCommitSector.DealIDs.length; i++) {
                                    let deal_info = {
                                        deal: preCommitSector.DealIDs[i],
                                        sector: preCommitSector.SectorNumber,
                                        miner: miner,
                                        start_epoch: msg.Block,
                                        end_epoch: preCommitSector.Expiration,
                                    }
                                    db.save_deal(deal_info);
                                }

                                let miner_event = get_miner_events(miner_events, miner, msg.Block)
                                miner_event.activated++;
                                miner_event.used = miner_event.used.add(new BN(sector_size));
                                miner_events.set(miner, miner_event);
                            }
                        }
                            break;
                        case MinerMethods.PreCommitSectorBatch: {
                            sector_batch = decoded_params[0][0];
                            for (let i = 0; i < sector_batch.length; i++) {
                                const preCommitSector = {
                                    DealIDs: sector_batch[4],
                                    Expiration: sector_batch[5],
                                    ReplaceCapacity: sector_batch[6],
                                    ReplaceSectorDeadline: sector_batch[7],
                                    ReplaceSectorNumber: sector_batch[8],
                                    ReplaceSectorPartition: sector_batch[9],
                                    SealProof: sector_batch[0],
                                    SealRandEpoch: sector_batch[3],
                                    SealedCID: sector_batch[2],
                                    SectorNumber: sector_batch[1]
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
                                    db.save_sector(sector_info);
                                    commited = commited.add(new BN(sector_size));

                                    let miner_event = get_miner_events(miner_events, miner, msg.Block)
                                    miner_event.activated++;
                                    miner_event.commited = miner_event.commited.add(new BN(sector_size));
                                    miner_events.set(miner, miner_event);
                                } else {
                                    //used sector
                                    sector_info.type = 'used';
                                    db.save_sector(sector_info);
                                    used = used.add(new BN(sector_size));

                                    for (let i = 0; i < preCommitSector.DealIDs.length; i++) {
                                        let deal_info = {
                                            deal: preCommitSector.DealIDs[i],
                                            sector: preCommitSector.SectorNumber,
                                            miner: miner,
                                            start_epoch: msg.Block,
                                            end_epoch: preCommitSector.Expiration,
                                        }
                                        db.save_deal(deal_info);
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

                                db.save_sector_events(sector_events);
                            }

                            let miner_event = get_miner_events(miner_events, miner, msg.Block)
                            miner_event.terminated++;
                            miner_events.set(miner, miner_event);

                            INFO(`[TerminateSectors] Miner:${miner} sectors: ${sectors} sectors`);
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

                                db.save_sector_events(sector_events);
                            }

                            let miner_event = get_miner_events(miner_events, miner, msg.Block)
                            miner_event.faults++;
                            miner_events.set(miner, miner_event);

                            INFO(`[DeclareFaults] Miner:${miner} for ${sectors.length} sectors`);
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

                                db.save_sector_events(sector_events);
                            }

                            let miner_event = get_miner_events(miner_events, miner, msg.Block)
                            miner_event.recovered++;
                            miner_events.set(miner, miner_event);

                            INFO(`[DeclareFaultsRecovered] Miner:${miner} for ${sectors.length} sectors`);
                        }
                            break;
                        case MinerMethods.ProveCommitSector: {
                            let sector_events = {
                                type: 'proof',
                                miner: miner,
                                sector: decoded_params[0],
                                epoch: msg.Block,
                            }

                            db.save_sector_events(sector_events);

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

    await db.save_network(network_info);

    miner_events.forEach((value, key, map) => {
        value.total = value.commited.add(value.used);
        let miner_fraction = new Big('0');

        if (!value.total.isZero()) {
            miner_fraction = new Big(value.used.toString(10));
            miner_fraction = miner_fraction.div(new Big(value.total.toString(10)));
        }
        value.fraction = miner_fraction;

        db.save_miner_events(key, value);
    });
}

async function scrape_block(block, msg, rescrape = false) {
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
        INFO(`[${msg}] ${block}`);
        if (rescrape) {
            messages = await filecoinChainInfo.GetBlockMessages(block);
        } else {
            messages = await filecoinChainInfoInfura.GetBlockMessages(block);
        }
    }

    if (messages && messages.length > 0) {
        INFO(`[${msg}] ${block}, ${messages.length} messages`);
        await db.save_block(block, messages.length, true);
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

async function scrape() {
    const chainHead = await filecoinChainInfoInfura.GetChainHead();
    if (!chainHead) {
        ERROR(`[Scrape] error : unable to get chain head`);
        return;
    }

    let start_block = await db.get_start_block();
    const end_block = chainHead - 1;

    INFO(`[Scrape] from ${start_block} to ${end_block}`);

    let blocks = [];
    for (let i = start_block; i <= end_block; i++) {
        blocks.push(i);
    }

    var blocksSlice = blocks;
    while (blocksSlice.length) {
        await Promise.all(blocksSlice.splice(0, SCRAPE_LIMIT).map(async (block) => {
            try {
                await scrape_block(block,'ScrapeBlock');
            } catch (error) {
                ERROR(`[Scrape] error :`);
                console.error(error);
            }
        }));
    }
}

async function rescrape() {
    let blocks;
    let i = 0;

    if (hdiff(last_rescrape) < RESCRAPE_INTERVAL) {
        INFO('[Rescrape] skip');
        return;
    }

    last_rescrape = Date.now();

    do {
        blocks = await db.get_bad_blocks(SCRAPE_LIMIT, i * SCRAPE_LIMIT);

        if (blocks) {
            await Promise.all(blocks.map(async (block) => {
                try {
                    await scrape_block(block.block, 'RescrapeBadBlock', true);
                } catch (error) {
                    ERROR(`[Rescrape] error :`);
                    console.error(error);
                }
            }));
        }

        i++;

    } while (blocks && blocks?.length == SCRAPE_LIMIT);
}

async function rescrape_missing_blocks() {
    INFO(`[RescrapeMissingBlocks]`);
    let head = await db.get_start_block();
    INFO(`[RescrapeMissingBlocks] from [0,${head}]`);
    let missing_blocks = await db.get_missing_blocks(head);
    INFO(`[RescrapeMissingBlocks] total missing blocks: ${missing_blocks.length}`);

    var blocksSlice = missing_blocks;
    while (blocksSlice.length) {
        await Promise.all(blocksSlice.splice(0, SCRAPE_LIMIT).map(async (item) => {
            try {
                await scrape_block(item.missing_block,'RescrapeMissingBlock');
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
        await Promise.all(blocksSlice.splice(0, 10).map(async (item) => {
            try {
                INFO(`[RescrapeMsgCid] ${item.block_with_missing_cid}`);
                const result = await filecoinChainInfo.GetBlockMessagesByTipSet(item.block_with_missing_cid, tipSetKey);
                if (result) {
                    tmpTipSetKey = result.tipSetKey;

                    if (result?.messages.length) {
                        await db.save_messages_cids(result?.messages);
                        await db.mark_block_with_msg_cid(item.block_with_missing_cid);
                        INFO(`[RescrapeMsgCid] ${item.block_with_missing_cid} done`);
                    } else {
                        ERROR(`[RescrapeMsgCid] ${item.block_with_missing_cid} no messages`);
                    }
                }
            } catch (error) {
                ERROR(`[RescrapeMsgCid] ${item.block_with_missing_cid} error :` , error);
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
        if (config.scraper.reprocess == 1) {
            WARNING('Reprocess');
            await migrations.reprocess();
        }

        INFO('Run migrations');
        await migrations.run();
        INFO('Run migrations, done');

        refresh_views();

        setInterval(async () => {
            await refresh_views();
        }, 12 * 3600 * 1000); // refresh every 12 hours

        while (!stop) {
            if (config.scraper.rescrape_msg_cid) {
                await rescrape_msg_cid();
            }
            if (config.scraper.rescrape_missing_blocks) {
                await rescrape_missing_blocks();
            }
            await scrape();
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