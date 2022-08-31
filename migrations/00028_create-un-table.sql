CREATE TABLE IF NOT EXISTS fil_un
(
    country TEXT NOT NULL,
    value integer NOT NULL,
    UNIQUE (country)
);

CREATE INDEX IF NOT EXISTS idx_fil_un_country ON fil_un(country);