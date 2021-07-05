const config = require('./config');
const { Pool } = require("pg");
const { version } = require('./package.json');
const { INFO, ERROR, WARNING } = require('./logs');
const { save_messages, save_block, save_bad_block, get_start_block, get_bad_blocks, have_block } = require('./db');

var express = require("express");
var cors = require('cors');
var app = express();

app.use(cors());
app.use(express.json());

const pool = new Pool(config.database);

app.get("/get_block:block", async function (req, res, next) {
    let code = 200;
    let msg = 'successful';

    //TODO : check if we have the block
    //SELECT using LIMIT 

    if (req.params?.block) {
        const block = req.params?.block;

        try {
            const client = await pool.connect()
            var result = await client.query(`SELECT * FROM filmessages WHERE block=${block}`);
            client.release();

            res.json(result.rows);
            INFO(`FilScraper API get_block[${block}] ${result.rows.length} messages`);

        } catch (e) {
            code = 401;
            msg = 'Failed to get block';
            INFO(`FilScraper API get_block[${block}] error: ${e}`);
            res.status(code).send(msg);
        }

    }

});

app.listen(config.scraper.api_port, () => {
    console.log("FilScraper API running on port " + config.scraper.api_port);
   });