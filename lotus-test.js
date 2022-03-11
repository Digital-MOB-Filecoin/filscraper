const axios = require('axios');
const Big = require('big.js');
const BN = require('bn.js');
const cbor = require('borc');

const filrep_api = 'https://api.filrep.io/api/miners?limit=5000&offset=0&order=desc&sortBy=rawPower';
//const filgreen_api = 'https://api.filgreen.d.interplanetary.one/miners?limit=6000&offset=0&order=desc&sortBy=power';
const filgreen_api = 'https://api.filecoin.energy/miners?limit=6000&offset=0&order=desc&sortBy=power';

const pause = (timeout) => new Promise(res => setTimeout(res, timeout));

async function GetMsgParamsFilFox(cid) {
    //https://filfox.info/api/v1/message/bafy2bzacea7k7vmqbymtuecxvvwamegbfvsrwipqq4tiz4ze2zte6pp2stvdk
    //https://api.filgreen.d.interplanetary.one/filchain?cid=bafy2bzaceduki2kusgcdhcuuisgsnaggf4rbs42t2q5bbhfw5zkzoqb4vtpq4

    const resp = (await axios.get(`https://filfox.info/api/v1/message/${cid}`)).data;
    //console.log(resp);

    await pause(1000); //ms

    return resp;
}

async function GetMsgParams(cid) {
    //https://filfox.info/api/v1/message/bafy2bzacea7k7vmqbymtuecxvvwamegbfvsrwipqq4tiz4ze2zte6pp2stvdk
    //https://api.filgreen.d.interplanetary.one/filchain?cid=bafy2bzaceduki2kusgcdhcuuisgsnaggf4rbs42t2q5bbhfw5zkzoqb4vtpq4

    const resp = (await axios.get(`https://api.filgreen.d.interplanetary.one/filchain?cid=${cid}`)).data;
    //console.log(filfox_resp);

    await pause(100); //ms

    return resp[0]?.Params;
}

async function PreCommitSectorDecoder(miner) {
    //miner = "f01135819";
    let filfox_api = `https://filfox.info/api/v1/address/${miner}/messages?pageSize=100&page=0&method=PreCommitSectorBatch`
    let filfox_resp;
    let totalCount = 0;
    let totalSectors = 0;
    let proccessed_msgs = 0;
    let pageSize = 100;
    let page = 0;

    do {
        filfox_resp = (await axios.get(`https://filfox.info/api/v1/address/${miner}/messages`,
            {
                params: {
                    pageSize: pageSize,
                    page: page,
                    method: "PreCommitSectorBatch"
                }
            })).data;

        if (!totalCount) {
            totalCount = filfox_resp.totalCount;
        }

        let messages = filfox_resp.messages;

        for (let i = 0; i < messages.length; i++) {
            let msg = messages[i];

            if (msg.receipt.exitCode == 0) {
                //console.log(msg);

                const params = await GetMsgParams(msg.cid);

                const filfox_params = await GetMsgParamsFilFox(msg.cid);

                totalSectors += filfox_params?.decodedParams?.Sectors?.length;

                if (params) {

                    //totalSectors += params?.decodedParams?.Sectors?.length;

                    let decoded_params = [];
                    try {
                        decoded_params = cbor.decode(params, 'base64');
                    } catch (error) {
                        if (params != 'null') {
                            console.log(`[ProcessMessages] error cbor.decode[${params}] : ${error}`);
                        }
                    }

                    if (decoded_params.length > 0) {
                        let sector_batch = decoded_params[0];

                        //console.log(decoded_params[0]);

                        let p=0;
                        while (sector_batch[p]) {
                            //console.log("p:", p);
                            //console.log(sector_batch[p]);
                            p++;
                        };

                        console.log("cid:", msg.cid,
                        "length:", sector_batch?.length, 
                        "filfox_length:", filfox_params?.decodedParams?.Sectors?.length,
                        "p:", p,
                        );
                        //console.log(sector_batch);

                        //totalSectors += sector_batch?.length;
                    }
                }
            }
        }
                

        proccessed_msgs += messages.length;

        console.log("page:", page, "total msgs:", messages.length, "totalSectors:", totalSectors);

        page++;

    } while (filfox_resp.messages.length === pageSize);

    
   
    console.log("total msgs", totalCount, "processed msgs:", proccessed_msgs, "totalSectors:", totalSectors);

}


(async () => {
    const { FilecoinChainInfo } = require('./filecoinchaininfo');
    const { Lotus } = require('./lotus');

    /*const lotus = new Lotus(
        "https://1uZeAWQG42pwwvUTngIdjgMs2hf:3a528932afd9adf568891de291496101@filecoin.infura.io",
        "config.lotus.token"
    );*/

    //console.log(await lotus.StateListMiners());

    let filecoinChainInfo = new FilecoinChainInfo(
        "https://1uZeAWQG42pwwvUTngIdjgMs2hf:3a528932afd9adf568891de291496101@filecoin.infura.io",
        "config.lotus.token"
    );

    //await filecoinChainInfo.GetBlockMessages(100878,   async (messages) => { console.log(messages)}, async (error) => { console.log(error)})

    let lotus = new Lotus(
        "https://1uZeAWQG42pwwvUTngIdjgMs2hf:3a528932afd9adf568891de291496101@filecoin.infura.io",
        "config.lotus.token");

    /*const minerInfo = await lotus.StateMinerInfo('f066102');
    console.log(minerInfo.data);

    let minersList = await lotus.StateListMiners();
    console.log(minersList.data);

    let miners = minersList.data.result;

    let f0 = 0;
    let fn = 0;

    for (i=0; i < miners.length; i++) {

        if (miners[i].startsWith('f0')) {
            f0++;
        } else {
            fn++;
        }
    }

    console.log(`f0 = ${f0} , fn = ${fn}`);*/

    //await filecoinChainInfo.GetMessages(885711, 885811, async (messages) => { console.log(messages)}, async (error) => { console.log(error)})

    
    //////////////////////////////////////////////////////////////////////
    /*
    let miners = new Map();

    let filrep_miners = (await axios.get(filrep_api)).data.miners;
    let filgreen_miners = (await axios.get(filgreen_api)).data.miners;

    filrep_miners.forEach(miner => {
        if (miner.rawPower) {
            let rawPowerGiB = new Big(miner.rawPower.toString(10));
            rawPowerGiB = rawPowerGiB.div(new Big('1073741824'));
            miners.set(miner.address, 
                {
                    miner: miner.address, 
                    name: miner.name, 
                    region: miner.region,
                    repsysPowerGiB: rawPowerGiB.toNumber(), 
                    filgreenPowerGiB: 0});
        }
    });

    let count = 0;

    filgreen_miners.forEach(miner => {
        let m = miners.get(miner.miner);

        if (m) {
            m.filgreenPowerGiB = parseInt(miner.power);
            miners.set(miner.miner, m);

            if ((m.repsysPowerGiB - m.filgreenPowerGiB) > 50000 * 1024) {
                console.log(miner.miner, Math.round((m.repsysPowerGiB - m.filgreenPowerGiB) / 1024), 'TiB');
                count++;
            }
        }
    });

    console.log('miners wrong power :', count);
    */
    //////////////////////////////////////////////////////////////////////

    await PreCommitSectorDecoder("f01135819");


}

)();