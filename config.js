module.exports = {
    scraper: {
      start: process.env.SCRAPER_START || 0,
      api_port: process.env.SCRAPER_API_PORT || 3000,
      reprocess: process.env.SCRAPER_REPROCESS || 0,
      lock_views: process.env.SCRAPER_LOCK_VIEWS || 0,
      rescrape_missing_blocks: process.env.SCRAPER_RESCRAPE_MISSING_BLOCKS || 0,
      rescrape_msg_cid: process.env.SCRAPER_RESCRAPE_MSG_CID || 0
    },
    database: {
        user: process.env.DB_USER || '',
        host: process.env.DB_HOST || '',
        database: process.env.DB_NAME || '',
        password: process.env.DB_PASSWORD || '',
        port: process.env.DB_PORT || 5432
      },
    lotus: {
      api_infura: process.env.LOTUS_API_INFURA || '',
      api: process.env.LOTUS_API || '',
      token: process.env.LOTUS_TOKEN || ''
    }
  };