CREATE TABLE IF NOT EXISTS fil_wt
(
    ba TEXT NOT NULL,
    value integer NOT NULL,
    date Timestamptz NOT NULL,
    UNIQUE (ba, date)
);

CREATE INDEX IF NOT EXISTS idx_fil_wt_ba ON fil_wt(ba);