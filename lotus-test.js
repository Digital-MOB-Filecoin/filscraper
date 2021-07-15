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

    const minerInfo = await lotus.StateMinerInfo('f066102');
    console.log(minerInfo.data);

    //await filecoinChainInfo.GetMessages(885711, 885811, async (messages) => { console.log(messages)}, async (error) => { console.log(error)})

}

)();