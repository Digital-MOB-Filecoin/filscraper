const config = require('./config');
const { INFO, ERROR, WARNING } = require('./logs');
const { DB } = require('./db');

let db = new DB();

const axios = require('axios').default;

const pause = (timeout) => new Promise(res => setTimeout(res, timeout * 1000));

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
                },
                timeout: 10000 //10 sec
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
                },
                timeout: 30000 //30 sec
            });
        } catch (err) {
            response = err?.response;
            if (err?.response?.status != 404) {
                console.error(err?.response?.data);
                ERROR(`[WT_get] ${err?.response?.data} status ${err?.response?.status}`);
            }
        }

        return response;
    }

    async get_data(region, starttime, endtime) {
        let params = {
            'region': region,
            'start': starttime,
            'end': endtime,
            'signal_type': 'co2_moer'
        };

        if (!this.token) {
            await this.login();
        }

        let response = await this.get('/v3/historical', params);

        if (response?.status == 401 || response?.status == 403) {
            INFO(`[WT_get_data] login and retry : ${JSON.stringify(params)}`);
            await pause(1);
            await this.login();
            response = await this.get('/v3/historical', params);
        }

        return response?.data.data;
    }

    async get_ba(latitude, longitude) {
        let params = {
            'latitude': latitude,
            'longitude': longitude,
            'signal_type': 'co2_moer',
        };
        
        if (!this.token) {
            await this.login();
        }

        let response = await this.get('/v3/region-from-loc', params);

        if (response?.status == 401 || response?.status == 403) {
            INFO(`[WT_get_ba] login and retry : ${JSON.stringify(params)}`);
            await pause(1);
            await this.login();
            response = await this.get('/v3/region-from-loc', params);
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
            dateArray.push((new Date(currentDate)).toISOString());
            currentDate = this.addDays(currentDate, 1);
        }
        return dateArray;
    }

    groupByDay(list) {
        let map = new Map();
        for (const item of list) {
            if (item?.point_time) {
                let key = item?.point_time.split('T')[0];
                if (!map.has(key)) {
                    map.set(key, []);
                }

                let dataPoints = map.get(key);
                dataPoints.push(item);
                map.set(key, dataPoints);
            }
        }
        return map;
    }

    async update() {
        try {
            const ba_list = await db.get_ba_list();
            let endDate = (new Date());
            endDate.setDate(endDate.getDate() - 1);
            endDate = endDate.toISOString();

            for (const item of ba_list) {
                let wt_data = [];
                let startDate = await db.get_ba_start_date(item.ba);
                if (!startDate) {
                    startDate = new Date('2020-08-25').toISOString();
                } else {
                    startDate = new Date(startDate);
                    startDate.setDate(startDate.getDate() + 1);
                    startDate = startDate.toISOString();
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
                            INFO(`[WT.update] ba: ${item.ba} get interval ${starttime} - ${endtime} ${data.length} items`);

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


        } catch (err) {
            ERROR(`[WT.update] ${err}`);
        }

    }
}

module.exports = {
    WT
}
