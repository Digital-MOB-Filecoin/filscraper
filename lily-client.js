const axios = require('axios');
const { INFO, ERROR, WARNING } = require('./logs');
const { DB } = require('./db');

let db = new DB();

class LilyClient {
    constructor(api, token, start_date) {
        this.api = api;
        this.token = token;
        this.start_date = start_date;
    }

    async get(url) {
        let response = undefined;

        try {
            if (this.token) {
                response = await axios.get(url, {
                    headers: {
                        Authorization: `Bearer ${this.token}`,
                    },
                    timeout: 30000 //30 sec
                });
            } else {
                response = await axios.get(url, {timeout: 30000});
            }
    
        } catch (e) {
            ERROR(`Get ${url} -> error: ${e}`);
        }

        return response;
    }

    async getDay(day) {
        let data = (await this.get(this.api + `?state_date=${day}`))?.data?.data;

        if (data?.length > 0) {
            await db.save_lily_data(data);
        }

        return data?.length;
    }

    addDays(d, days) {
        var date = new Date(d);
        date.setDate(date.getDate() + days);
        return date;
    }

    getSeries(start, end) {
        const startDate = new Date(start);
        const stopDate = new Date(end);
        var dateArray = new Array();
        var currentDate = startDate;
        while (currentDate <= stopDate) {
            dateArray.push((new Date(currentDate)).toISOString().split('T')[0]);
            currentDate = this.addDays(currentDate, 1);
        }
        return dateArray;
    }

    async update() {
        let endDate = (new Date());
        endDate.setDate(endDate.getDate() - 1);
        endDate = endDate.toISOString().split('T')[0];

        let startDate = await db.get_lily_start_date();
        if (!startDate) {
            startDate = this.start_date;
        } else {
            startDate = new Date(startDate);
            startDate.setDate(startDate.getDate() + 1);
            startDate = startDate.toISOString().split('T')[0];
        }

        let series = this.getSeries(startDate, endDate);

        INFO(`[Lily_update] get data for : ${series.length} days`);

        for (const d of series) {
            INFO(`[Lily_update] get data for : ${d}`);
            let items = await this.getDay(d);
            INFO(`[Lily_update] get data for : ${d} done, ${items} items`);
        }
    }
}

module.exports = {
    LilyClient
};