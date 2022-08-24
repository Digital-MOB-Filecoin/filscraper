const config = require('./config');
const { INFO, ERROR, WARNING } = require('./logs');
const { DB } = require('./db');

let db = new DB();

const axios = require('axios').default;

class WT {
    constructor() {
        this.api = config.scraper.wt_api;
        this.user = config.scraper.wt_user;
        this.password = config.scraper.wt_password;
        this.token = undefined;
    }

    async login() {
        try {
            let response = await axios.get(this.api + '/login ', {
                auth: {
                    username: this.user,
                    password: this.password
                }
            });
    
            this.token = response.data?.token;
        } catch(err) {
            ERROR(`[WT_login] ${err}`);
        }
    }

    async get(endpoint, params) {
        let response;
        try {
            response = await axios.get(this.api + endpoint, {
                params: params,
                headers: {
                    'Authorization': 'Bearer ' + this.token
                }
            });
        } catch (err) {
            response = err?.response;
            if (err?.response?.status != 404) {
                ERROR(`[WT_get] ${err?.response?.data} status ${err?.response?.status}`);
            }
        }

        return response;
    }

    async get_data(ba, starttime, endtime) {
        let params = {
            'ba': ba,
            'starttime': starttime,
            'endtime': endtime
        };

        console.log(params);

        if (!this.token) {
            await this.login();
        }

        let response = await this.get('/data', params);

        if (response?.status == 401) {
            INFO(`[WT_get_data] login and retry : ${JSON.stringify(params)}`);
            await this.login();
            response = await this.get('/data', params);
        }
        
        return response?.data;
    }

    async get_ba(latitude, longitude) {
        let params = {
            'latitude': latitude,
            'longitude': longitude
        };
        
        if (!this.token) {
            await this.login();
        }

        let response = await this.get('/ba-from-loc', params);

        if (response?.status == 401) {
            INFO(`[WT_get_ba] login and retry : ${JSON.stringify(params)}`);
            await this.login();
            response = await this.get('/ba-from-loc', params);
        }

        return response?.data;
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

    groupByDay(list) {
        let map = new Map();
            for (const item of list) {
                let key = item.point_time.split('T')[0];
                if (!map.has(key)) {
                    map.set(key, []);
                }
    
                let dataPoints = map.get(key);
                dataPoints.push(item);
                map.set(key, dataPoints);
            }
            return map;
        }

    async update() {
        const ba_list = await db.get_ba_list();
        let endDate = (new Date());
        endDate.setDate(endDate.getDate() - 1);
        endDate = endDate.toISOString().split('T')[0];
        let wt_data = [];

        for (const item of ba_list) {
            let startDate = await db.get_ba_start_date(item.ba);
            if (!startDate) {
                startDate = '2020-08-25';
            } else {
                startDate = new Date(startDate);
                startDate.setDate(startDate.getDate() + 1);
                startDate = startDate.toISOString().split('T')[0];
            }

            var sd = new Date(startDate);
            var ed = new Date(endDate);
            if (sd.getTime() < ed.getTime()) {
                INFO(`[WT.update] ba: ${item.ba} interval ${startDate} - ${endDate}`);

                let series = this.getSeries(startDate, endDate);
                if (series) {
                    var seriesSlice = series;
                    while (seriesSlice.length) {
                        let currentSlice = seriesSlice.splice(0, 30);
                        const starttime = currentSlice[0];
                        const endtime = currentSlice[currentSlice.length - 1];

                        let data = await this.get_data(item.ba, starttime, endtime);
                        INFO(`[WT.update] ba: ${item.ba} get interval ${starttime} - ${endtime} ${}`);

                        let groupedByDayData = this.groupByDay(data);

                        for (const [key, value] of groupedByDayData) {
                            let num_points = 0;
                            let sum = 0;
                            for (const item of value) {
                                num_points++;
                                sum += item.value;
                            }

                            wt_data.push({
                                ba: item.ba,
                                value: Math.ceil(sum / num_points),
                                date: key
                            })
                        }
                    }

                    await db.wt_data_add(wt_data);

                    INFO(`[WT.update] ba: ${item.ba} interval ${startDate} - ${endDate} saved ${wt_data.length} items`);
                }
            } else {
                INFO(`[WT.update] ba: ${item.ba} up to date.`);
            }
        }
    }
}

module.exports = {
    WT
}
