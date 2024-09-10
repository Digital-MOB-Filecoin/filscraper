const cron = require('node-cron');
const { unixToHeight} = require("./helpers");
const { DB } = require("./db");
const { INFO } = require("./logs");

let db = new DB();

cron.schedule('0 0 * * *', async () => {
    const now = new Date();
    const twoMonthsAgo = now.setMonth(now.getMonth() - 2);
    const twoMonthsAgoUnix = Math.floor(twoMonthsAgo / 1000);
    const twoMonthsAgoBlockHeight = unixToHeight(twoMonthsAgoUnix);

    INFO(`[Cron] Deleting messages older than ${twoMonthsAgoBlockHeight} block height`);
    await db.delete_old_messages(twoMonthsAgoBlockHeight);
    INFO(`[Cron] Deleted messages older than ${twoMonthsAgoBlockHeight} block height`);
});