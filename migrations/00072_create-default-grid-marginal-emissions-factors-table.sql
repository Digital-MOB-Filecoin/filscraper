CREATE TABLE IF NOT EXISTS fil_default_grid_marginal_emissions_factors
(
    country TEXT NOT NULL,
    value NUMERIC NOT NULL,
    UNIQUE (country)
);

CREATE INDEX IF NOT EXISTS idx_fil_default_grid_marginal_emissions_factors_country ON fil_default_grid_marginal_emissions_factors(country);