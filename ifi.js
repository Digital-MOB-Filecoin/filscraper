const fs = require('fs');
const write = require('write');
const { parse } = require('csv-parse');

const rs = fs.createReadStream('./IFI/IFIDefaultGridFactors2021v3.csv');
const parser = parse({ columns: true }, function (err, data) {
    if (!err && data) {
        let text = '';
        let unique_locations = new Map();
        for (d of data) {
            if (!unique_locations.has(d.ISO3166CountryCode)) {
                let line = `INSERT INTO fil_un (country, value) VALUES ('${d.ISO3166CountryCode}', ${parseInt(d.Emission)});\n`;
                text += line;
                unique_locations.set(d.ISO3166CountryCode, 1);
            }
        }

        write.sync('./IFI/un.sql', text, { newline: true }); 
    } else {
        console.log(data);
        console.log(err);
    }
});
rs.pipe(parser);