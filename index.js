const config = require('./config');
const { version } = require('./package.json');
const { INFO, ERROR, WARNING } = require('./logs');
const { FilecoinChainInfo } = require('./filecoinchaininfo');
const { create_filmessages_table, create_filblocks_table, create_filbadblocks_table } = require('./migrations');
const { save_messages, save_block, save_bad_block, get_start_block, get_bad_blocks, have_block } = require('./db');
const { hdiff } = require('./utils');

const SCRAPE_LIMIT = 10 // blocks
const RESCRAPE_INTERVAL = 1 // hours
let last_rescrape = Date.now();

let filecoinChainInfo = new FilecoinChainInfo(config.lotus.api, config.lotus.token);
let stop = false;

async function scrape_block(block) {
    const found = await have_block(block)
    if (found) {
        INFO(`[ScrapeBlock] ${block} already scraped, skipping`);
        return;
    }

    INFO(`[ScrapeBlock] ${block}`);
    const messages = await filecoinChainInfo.GetBlockMessages(block);

    if (messages && messages.length > 0) {
        INFO(`[ScrapeBlock] ${block}, ${messages.length} messages`);
        await save_block(block, messages.length);
        await save_messages(messages);

        INFO(`[ScrapeBlock] ${block} done`);
    } else {
        save_bad_block(block)
        WARNING(`[ScrapeBlock] ${block} mark as bad block`);
    }
}

async function scrape() {
    const chainHead = await filecoinChainInfo.GetChainHead();
    if (!chainHead) {
        ERROR(`[Scrape] error : unable to get chain head`);
        return;
    }

    const start_block = await get_start_block();
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
        blocks = await get_bad_blocks(SCRAPE_LIMIT, i * SCRAPE_LIMIT);

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
        await filscraper_version();
        await create_filblocks_table();
        await create_filbadblocks_table();
        await create_filmessages_table();

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