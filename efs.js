const fs = require('fs');
const write = require('write');
const { parse } = require('csv-parse');
const getCountryISO2 = require("country-iso-3-to-2");

getCountryISO2("BRA");

const rs = fs.createReadStream('./EFs/EmissionFactors-EFs.csv');
const parser = parse({ columns: true }, function (err, data) {
    if (!err && data) {
        let count = 0;
        let text = '';
        let unique_locations = new Map();
        for (d of data) {
            let country = getCountryISO2(d.CountryISO3);
            let value = parseFloat(d.IEA2020);

            if (parseFloat(d.IEA2021Estimated)) {
                value = parseFloat(d.IEA2021Estimated);
            }

            //console.log(d.Country, d.CountryISO3, getCountryISO2(d.CountryISO3), d.IEA2020, d.IEA2021Estimated);
            if (country && !unique_locations.has(country) && value) {
                let line = `INSERT INTO fil_emission_factors (country, value) VALUES ('${country}', ${value});\n`;
                text += line;
                unique_locations.set(country, 1);
            }
            count++;
        }
        console.log('count', count);

        write.sync('./EFs/emission_factors.sql', text, { newline: true }); 
    } else {
        console.log(data);
        console.log(err);
    }
});
rs.pipe(parser);