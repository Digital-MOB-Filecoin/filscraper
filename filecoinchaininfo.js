const { Lotus } = require('./lotus');
const { INFO, ERROR } = require('./logs');

class FilecoinChainInfo {
    constructor(api, token) {
        this.lotus = new Lotus(api, token);
    }

    Range(start, end) {
        return Array(end - start + 1).fill().map((_, idx) => start + idx)
    }

    async GetMessages(startBlock, endBlock, onMessagesCallback, onErrorCallback) {
        const chainHead = await this.lotus.ChainHead();
        if (chainHead.status != 200) {
            onErrorCallback(`chainHead.status : ${chainHead.status} , statusText: ${chainHead.statusText}`);
            return [];
        }

        if (endBlock > chainHead.result.Height) {
            if (onErrorCallback) {
                onErrorCallback(`endBlock[${endBlock}, is bigger then chainHead[${chainHead.result.Height}]]`);
            }
            endBlock = chainHead.result.Height;
        }

        const blocks = this.Range(startBlock, endBlock);
        let messages = [];

        var blocksSlice = blocks;
        while (blocksSlice.length) {
            await Promise.all(blocksSlice.splice(0, 10).map(async (block) => {
                try {
                    var tipSet = (await this.lotus.ChainGetTipSetByHeight(block, chainHead.result.Cids)).result;
                    const { '/': blockCid } = tipSet.Cids[0];
                    let new_messages = (await this.lotus.ChainGetParentMessages(blockCid)).result;
                    let receipts = (await this.lotus.ChainGetParentReceipts(blockCid)).result;

                    if (!new_messages) {
                        new_messages = [];
                    }
                    new_messages = new_messages.map((msg, r) => ({ ...msg.Message, cid: msg.Cid, receipt: receipts[r], block: block}));
                    messages = [...messages, ...new_messages];

                } catch (e) {
                    if (onErrorCallback) {
                        onErrorCallback(e.message);
                    }
                }
            }));
        }

        onMessagesCallback(messages);
    }

    CheckResponse(method, response) {
        if (response?.status != 200) {
            ERROR(`${method} status: ${response?.status} ${response?.statusText}`);
            return false;
        }

        if (response.data.error) {
            ERROR(`[${method}] error : ${JSON.stringify(response.data.error)}`);
            return false;
        }

        if (!response.data.result) {
            ERROR(`[${method}] empty result`);
            return false;
        }

        return true;
    }

    async GetChainHead() {
        const chainHeadResponse = await this.lotus.ChainHead();

        if (!this.CheckResponse('ChainHead', chainHeadResponse)) {
            return undefined
        }

        return chainHeadResponse.data.result.Height;
    }

    async GetBlockMessages(block) {
        let messages = [];
        let block_to_scrape = parseInt(block);

        try {
            const chainHeadResponse = await this.lotus.ChainHead();
            if (!this.CheckResponse('ChainHead', chainHeadResponse)) {
                return undefined
            }

            if (block_to_scrape >= chainHeadResponse.data.result.Height) {
                ERROR(`[GetBlockMessages] block[${block_to_scrape}], is bigger then chainHead[${chainHeadResponse.data.result.Height}]`);
                return undefined
            }

            var tipSetResponse = await this.lotus.ChainGetTipSetByHeight(block_to_scrape + 1, chainHeadResponse.data.result.Cids);
            if (!this.CheckResponse('ChainGetTipSetByHeight', tipSetResponse)) {
                return undefined
            }

            const { '/': blockCid } = tipSetResponse.data.result.Cids[0];
            const parentMessagesResponse = await this.lotus.ChainGetParentMessages(blockCid);
            if (!this.CheckResponse('ChainGetParentMessages', parentMessagesResponse)) {
                return undefined
            }

            let new_messages = parentMessagesResponse.data.result;
            const receiptsResponse = await this.lotus.ChainGetParentReceipts(blockCid)
            if (!this.CheckResponse('ChainGetParentReceipts', receiptsResponse)) {
                return undefined
            }

            let receipts = receiptsResponse.data.result;

            if (!new_messages) {
                new_messages = [];
            }

            new_messages = new_messages.map((msg, r) => ({ Cid: msg.Cid, ...msg.Message, ExitCode: receipts[r].ExitCode, Return: receipts[r].Return, GasUsed: receipts[r].GasUsed, Block: block_to_scrape }));
            messages = [...messages, ...new_messages];

        } catch (e) {
            ERROR(`[GetBlockMessages] ${e.message}`);
        }

        return messages;
    }

    async GetTipSetKey(block) {
        const chainHeadResponse = await this.lotus.ChainHead();

        if (!this.CheckResponse('ChainHead', chainHeadResponse)) {
            return undefined
        }

        var tipSetResponse = await this.lotus.ChainGetTipSetByHeight(block, chainHeadResponse.data.result.Cids);
        if (!this.CheckResponse('ChainGetTipSetByHeight', tipSetResponse)) {
            return undefined
        }

        return tipSetResponse.data.result.Cids;
    }

    async GetBlockMessagesByTipSet(block, tipSetKey) {
        let messages = [];
        let new_tipSetKey = undefined;
        let block_to_scrape = parseInt(block);

        try {
            if (!tipSetKey) {
                const chainHeadResponse = await this.lotus.ChainHead();
                tipSetKey = chainHeadResponse.data.result.Cids;

                if (!this.CheckResponse('ChainHead', chainHeadResponse)) {
                    return undefined
                }
            }

            var tipSetResponse = await this.lotus.ChainGetTipSetByHeight(block_to_scrape + 1, tipSetKey);
            if (!this.CheckResponse('ChainGetTipSetByHeight', tipSetResponse)) {
                return undefined
            }

            new_tipSetKey = tipSetResponse.data.result.Cids;

            const { '/': blockCid } = tipSetResponse.data.result.Cids[0];
            const parentMessagesResponse = await this.lotus.ChainGetParentMessages(blockCid);
            if (!this.CheckResponse('ChainGetParentMessages', parentMessagesResponse)) {
                return undefined
            }

            let new_messages = parentMessagesResponse.data.result;
            const receiptsResponse = await this.lotus.ChainGetParentReceipts(blockCid)
            if (!this.CheckResponse('ChainGetParentReceipts', receiptsResponse)) {
                return undefined
            }

            let receipts = receiptsResponse.data.result;

            if (!new_messages) {
                new_messages = [];
            }

            new_messages = new_messages.map((msg, r) => ({ Cid: msg.Cid, ...msg.Message, ExitCode: receipts[r].ExitCode, Return: receipts[r].Return, GasUsed: receipts[r].GasUsed, Block: block_to_scrape }));
            messages = [...messages, ...new_messages];

        } catch (e) {
            ERROR(`[GetBlockMessagesByTipSet] ${e.message}`);
        }

        return {tipSetKey: new_tipSetKey, messages: messages};;
    }

}

module.exports = {
    FilecoinChainInfo
};
