const config = require('./config');
const { INFO, ERROR, WARNING } = require('./logs');
const { DB } = require('./db');

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

    async get_data(latitude, longitude, starttime, endtime) {
        //. If a data call returns HTTP 401 error code, you will need to call /login again to receive a new t
        try {
            let response = await axios.get(this.api + '/data ', {
                params: {
                    'latitude': latitude,
                    'longitude': longitude,
                    'starttime': starttime,
                    'endtime': endtime
                },
                headers: {
                    'Authorization': 'Bearer ' + this.token
                }
            });
        } catch (err) {
            ERROR(`[WT_get_data] ${err}`);
        }
    }

    async get_ba(latitude, longitude) {
        //. If a data call returns HTTP 401 error code, you will need to call /login again to receive a new t
        let ret = undefined;
        try {
            let response = await axios.get(this.api + '/ba-from-loc ', {
                params: {
                    'latitude': latitude,
                    'longitude': longitude
                },
                headers: {
                    'Authorization': 'Bearer ' + this.token
                }
            });
            ret = response?.data;
        } catch (err) {
            //WARNING(`[WT_get_data] ${err}`);
        }

        return ret;
    }

    async update() {
        //select unique ba s
        //get start date default or from db
        //generate time series
        //split time series in 31
        //requests
        //averege per day 
        //save in db
    }
}

module.exports = {
    WT
}
