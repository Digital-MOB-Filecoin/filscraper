CREATE TABLE IF NOT EXISTS fil_country_exceptions
(
  country TEXT NOT NULL,
  exception TEXT NOT NULL,
  UNIQUE (country, exception)
);

CREATE INDEX IF NOT EXISTS idx_fil_country_exceptions_country ON fil_country_exceptions(country);
CREATE INDEX IF NOT EXISTS idx_fil_country_exceptions_exception ON fil_country_exceptions(exception);