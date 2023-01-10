module.exports = {
    scraper: {
      start: process.env.SCRAPER_START || 0,
      api_port: process.env.SCRAPER_API_PORT || 3000,
      reprocess: process.env.SCRAPER_REPROCESS || 0,
      lock_views: process.env.SCRAPER_LOCK_VIEWS || 0,
      await_refresh_views: process.env.SCRAPER_AWAIT_REFRESH_VIEWS || 0,
      check_missing_blocks: process.env.SCRAPER_CHECK_MISSING_BLOCKS || 0,
      rescrape_missing_blocks: process.env.SCRAPER_RESCRAPE_MISSING_BLOCKS || 0,
      rescrape_msg_cid: process.env.SCRAPER_RESCRAPE_MSG_CID || 0,
      rescrape_msg_cid_filplus: process.env.SCRAPER_RESCRAPE_MSG_CID_FILPLUS || 0,
      renewable_energy_api: process.env.SCRAPER_RENEWABLE_ENERGY_API || '',
      renewable_energy_token: process.env.SCRAPER_RENEWABLE_ENERGY_TOKEN || '',
      location_api: process.env.SCRAPER_LOCATION_API || '',
      wt_api: process.env.SCRAPER_WT_API || '',
      wt_user: process.env.SCRAPER_WT_USER || '',
      wt_password: process.env.SCRAPER_WT_PASSWORD || '',
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
    },
    lily: {
      api: process.env.SPACESCOPE_API || '',
      token: process.env.SPACESCOPE_TOKEN || '',
      start_date: process.env.SPACESCOPE_START_DATE || ''
    }
  };