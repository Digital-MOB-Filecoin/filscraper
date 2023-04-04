const axios = require('axios');
const { INFO, ERROR, WARNING } = require('./logs');

class FilinfoClient {
    constructor(api) {
        this.api = api;
    }

    async get(url) {
        let response = undefined;

        try {
            response = await axios.get(url, { timeout: 30000 });
        } catch (e) {
            ERROR(`Get ${url} -> error: ${e}`);
        }

        return response;
    }

    async getMessages(block) {
        const limit = 500;
        let offset = 0;
        let messages = [];
        let data;

        do {
            data = (await this.get(this.api + `?block=${block}&offset=${offset}&limit=${limit}`))?.data;
            if (data?.length) {
                messages.push(...data);
                break;
            }
            offset+= 500;
        } while (data?.length == 500);
       
        return messages;
    }
}

module.exports = {
    FilinfoClient
};