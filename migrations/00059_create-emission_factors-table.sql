CREATE TABLE IF NOT EXISTS fil_emission_factors
(
    country TEXT NOT NULL,
    value NUMERIC NOT NULL,
    UNIQUE (country)
);

CREATE INDEX IF NOT EXISTS idx_fil_emission_factors_country ON fil_emission_factors(country);