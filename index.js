const config = require('./config');
const cbor = require('borc');
const BN = require('bn.js');
const Big = require('big.js');
const { version } = require('./package.json');
const { INFO, ERROR, WARNING } = require('./logs');
const { FilecoinChainInfo } = require('./filecoinchaininfo');
const { Migrations } = require('./migrations');
const { DB } = require('./db');
const { MinerMethods } = require('./miner-methods');
const { decodeRLE2 } = require('./rle');
const { hdiff } = require('./utils');

const SCRAPE_LIMIT = 100 // blocks
const RESCRAPE_INTERVAL = 1 // hours
let last_rescrape = Date.now();

let filecoinChainInfo = new FilecoinChainInfo(config.lotus.api, config.lotus.token);
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

                            //const minerInfo = await lotus.StateMinerInfo(miner.address);
                            //let sectorSize = minerInfo.result.SectorSize;
                            //TODO: sector size cache[miner]
                            const sector_size = 34359738368;

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

                                // 'miners' TABLE -> lotus.StateMinerInfo
                                // miner
                                // sector_size




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

async function scrape_block(block) {
    let scraped_from_db = false;
    const found = await db.have_block(block);
    if (found) {
        INFO(`[ScrapeBlock] ${block} already scraped, skipping`);
        return;
    }

    let messages = [];

    const found_messages = await db.have_messages(block);
    if (found_messages) {
        INFO(`[ScrapeBlock] ${block} from db`);
        messages = await db.get_messages(block);
        scraped_from_db = true;
    } else {
        INFO(`[ScrapeBlock] ${block}`);
        messages = await filecoinChainInfo.GetBlockMessages(block);
    }

    if (messages && messages.length > 0) {
        INFO(`[ScrapeBlock] ${block}, ${messages.length} messages`);
        await db.save_block(block, messages.length);
        if (!scraped_from_db) {
            await db.save_messages(messages);
        }
        await process_messages(block, messages);

        INFO(`[ScrapeBlock] ${block} done`);
    } else {
        await db.save_bad_block(block)
        WARNING(`[ScrapeBlock] ${block} mark as bad block`);
    }
}

async function scrape() {
    const chainHead = await filecoinChainInfo.GetChainHead();
    if (!chainHead) {
        ERROR(`[Scrape] error : unable to get chain head`);
        return;
    }

    let start_block = await db.get_start_block();
    const end_block = chainHead - 1;

    if (start_block < config.scraper.start) {
        start_block = config.scraper.start;
    }

    INFO(`[Scrape] from ${start_block} to ${end_block}`);

    let blocks = [];
    for (let i = start_block; i <= end_block; i++) {
        blocks.push(i);
    }

    var blocksSlice = blocks;
    while (blocksSlice.length) {
        await Promise.all(blocksSlice.splice(0, SCRAPE_LIMIT).map(async (block) => {
            try {
                await scrape_block(block);
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

        await Promise.all(blocks.map(async (block) => {
            try {
                await scrape_block(block.block);
            } catch (error) {
                ERROR(`[Rescrape] error :`);
                console.error(error);
            }
        }));

        i++;

    } while (blocks?.length == SCRAPE_LIMIT);
}


async function filscraper_version() {
    INFO(`FilScraper version: ${version}`);
};

const pause = (timeout) => new Promise(res => setTimeout(res, timeout * 1000));

const mainLoop = async _ => {
    try {
        if (config.scraper.reprocess == 1) {
            WARNING('Reprocess');
            await migrations.reprocess();
        }

        await migrations.run();

        while (!stop) {

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