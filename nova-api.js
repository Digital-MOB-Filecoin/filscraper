const { default: axios } = require("axios");
const { ERROR, INFO } = require("./logs");
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
      const response = await this.get("/miners");
      const nodes = response.data.data;

      const minerConfidenceScores = [];

      for (const node of nodes) {
        for (const document of node.documents) {
          const {
            miner_ids,
            report_start_date,
            report_end_date,
            confidence_score,
          } = document.data.audit_document.greenscore;

          const dates = this.getDatesBetween(
            report_start_date,
            report_end_date,
          );

          for (const minerId of miner_ids) {
            for (const date of dates) {
              minerConfidenceScores.push({
                minerId,
                date,
                confidenceScore: confidence_score,
              });
            }
          }
        }
      }

      await db.nova_api_data_add(minerConfidenceScores);
      INFO("[Nova_API_success] Confidence scores inserted successfully.");
    } catch (err) {
      ERROR(`[Nova_API_error] ${err}`);
    }
  }
}

module.exports = {
  NovaApi,
};
