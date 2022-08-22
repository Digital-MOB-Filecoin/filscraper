const config = require('./config');
const { INFO, ERROR } = require('./logs');
const { DB } = require('./db');
const { WT } = require('./watttime');
const { ddiff } = require('./utils');

const axios = require('axios').default;
let db = new DB();
let wt = new WT();

class Location {
    constructor() {
        this.api = config.scraper.location_api;
    }

    groupByMiner(list) {
    let map = new Map();
        for (const item of list) {
            if (!map.has(item.miner)) {
                map.set(item.miner, []);
            }

            let minerLocations = map.get(item.miner);
            minerLocations.push(item);
            map.set(item.miner, minerLocations);
        }
        return map;
    }


    extractLocation(listFromAPI) {
        let list = [];

        for (const item of listFromAPI) {
            list.push({
                miner: item.provider,
                lat: item.lat,
                long: item.long,
                region: item.region ? item.region : 'n/a',
                country: item.country ? item.country : 'n/a',
                city: item.city ? item.city : 'n/a',
                locations: item.numLocations
            });
        }

        return list;
    }

    ba_refresh(item) {
        let ret = false;

        if (ddiff(item.ba_date) > 30) {
            INFO(`[Location] be_refresh for ${JSON.stringify(item)}`)
            ret = true;
        }

        return ret;
    }

    async update() {
        try {
            INFO(`[Location] update start.`);
            const res = await axios.get(this.api);
            let providerLocations = res?.data?.providerLocations;
            if (providerLocations) {
                let listFromDB = await db.location_get();
                let groupedByMinerFromDB = this.groupByMiner(listFromDB)
                let groupedByMinerFromAPI = this.groupByMiner(this.extractLocation(providerLocations));
                let to_add = [];
                let to_delete = [];

                for (const [key, value] of groupedByMinerFromAPI) {
                    if (!groupedByMinerFromDB.has(key)) {
                        to_add = to_add.concat(value);
                    } else {
                        const minerLocationsFromDB = groupedByMinerFromDB.get(key);

                        for (const item of value) {
                            let exactMatch = false;

                            for (const itemFromDB of minerLocationsFromDB) {
                                if ((item.miner == itemFromDB.miner) &&
                                (item.lat == itemFromDB.lat) &&
                                (item.long == itemFromDB.long) &&
                                (item.region == itemFromDB.region) &&
                                (item.country == itemFromDB.country) &&
                                (item.city == itemFromDB.city) &&
                                (item.locations == itemFromDB.locations) &&
                                !this.ba_refresh(itemFromDB)) {
                                    exactMatch = true;
                                }
                            }

                            if (!exactMatch) {
                                to_delete.push(key);
                                to_add = to_add.concat(value);
                                break;
                            }
                        }
                    }
                }

                let to_add_with_ba = [];

                if (to_add.length) {
                    let unique_locations = new Map();
                    let unique_locations_ba = new Map();

                    for (const item of to_add) {
                        let key = item.lat + '-' + item.long;
                        if (!unique_locations.has(key)) {
                            unique_locations.set(key, {
                                lat: item.lat,
                                long: item.long,
                            })
                        }
                    }

                    INFO(`[Location] unique locations ${unique_locations.size}`);

                    await wt.login();
                    for (const [key, value] of unique_locations) {
                        let resp = await wt.get_ba(value.lat, value.long);
                        if (resp && resp.abbrev) {
                            INFO(`[Location] ba ${JSON.stringify(resp)} for ${JSON.stringify(value)}`);

                            unique_locations_ba.set(key, resp.abbrev);
                        }
                    }

                    INFO(`[Location] unique locations with ba ${unique_locations_ba.size}`);

                    to_add_with_ba = [];

                    for (const item of to_add) {
                        let key = item.lat + '-' + item.long;
                        to_add_with_ba.push({
                            ...item,
                            ba: unique_locations_ba.get(key) ? unique_locations_ba.get(key) : null
                        });
                    }
                }

                if (to_delete.length) {
                    INFO(`[Location] delete ${to_delete.length} items`);

                    for (const miner of to_delete) {
                        await db.location_delete(miner);
                    }
                }

                if (to_add_with_ba.length) {
                    INFO(`[Location] add ${to_add_with_ba.length} items`);
                    await db.location_add(to_add_with_ba);
                }

                INFO(`[Location] update completed.`);
            } else {
                ERROR(`[Location] upable to get location data`);
            } 
        } catch (err) {
            ERROR(`[Location] ${err}`);
        }
    }
}

module.exports = {
    Location
}
