module.exports = {
    scraper: {
      start: process.env.SCRAPER_START || 0,
      api_port: process.env.SCRAPER_API_PORT || 3000,
      reprocess: process.env.SCRAPER_REPROCESS || 0
    },
    database: {
        user: process.env.DB_USER || '',
        host: process.env.DB_HOST || '',
        database: process.env.DB_NAME || '',
        password: process.env.DB_PASSWORD || '',
        port: process.env.DB_PORT || 5432
      },
    lotus: {
      api: process.env.LOTUS_API || '',
      token: process.env.LOTUS_TOKEN || ''
    }
  };