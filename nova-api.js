const { default: axios } = require("axios");
const { ERROR } = require("./logs");
const { DB } = require("./db");
const config = require("./config");

const db = new DB();

class NovaApi {
  constructor() {
    this.api = config.scraper.nova_api;
    this.apiKey = config.scraper.nova_api_key;
  }

  getDatesBetween(startDate, endDate) {
    const dates = [];
    let currentDate = new Date(startDate);

    while (currentDate <= new Date(endDate)) {
      dates.push(currentDate.toISOString().split("T")[0]);
      currentDate.setDate(currentDate.getDate() + 1);
    }
    return dates;
  }

  async get(endpoint) {
    let response;
    try {
      response = await axios.get(this.api + endpoint, {
        headers: {
          ["x-apy-key"]: "Bearer " + this.apiKey,
          ContentType: "application/json",
        },
        timeout: 30000,
      });
    } catch (err) {
      response = err?.response;
      if (err?.response?.status != 404) {
        console.error(err?.response?.data);
        ERROR(
          `[Nova_API_get] ${err?.response?.data} status ${err?.response?.status}`,
        );
      }
    }

    return response;
  }

  async update() {
    try {
      const response = await this.get("/greenscores");
      const data = response.data;

      const insertPromises = [];
      for (const greenscore of data.greenscores) {
        const { greenscore_data } = greenscore;
        const {
          report_start_date,
          report_end_date,
          confidence_score,
          provider_network,
        } = greenscore_data;

        const dates = this.getDatesBetween(report_start_date, report_end_date);

        for (const minerId of provider_network.miner_ids) {
          for (const date of dates) {
            insertPromises.push(
              db.nova_api_data_add({ minerId, date, confidence_score }),
            );
          }
        }
      }

      await Promise.all(insertPromises);
      console.log("Confidence scores inserted successfully.");
    } catch (err) {
      ERROR(`[WT.update] ${err}`);
    }
  }
}

module.exports = {
  NovaApi,
};

// call nova api
// normalize the data
// insert the data into the database
// show the date in the frontend on the same logic as miners emissions score
