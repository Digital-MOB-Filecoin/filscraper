'use strict';

const axios = require('axios');
const { INFO, ERROR, WARNING } = require('./logs');

class ZeroLabsClient {
    constructor(api, token) {
        this.api = api;
        this.token = token;
    }

    async Get(url) {
        let response = undefined;

        try {
            if (this.token) {
                response = await axios.get(url, {
                    headers: {
                        Authorization: `token ${this.token}`,
                    }
                });
            } else {
                response = await axios.get(url);
            }
    
        } catch (e) {
            ERROR(`Get ${url} -> error: ${e}`);
        }

        return response;
    }

    GetMiners() {
        return this.Get(this.api);
    }

    GetTransactions(miner) {
        return this.Get(this.api + '/' + miner + '/transactions');
    }

    GetContracts(miner) {
        return this.Get(this.api + '/' + miner + '/contracts');
    }
}

module.exports = {
    ZeroLabsClient
};