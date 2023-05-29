INSERT INTO fil_un (country, value)
SELECT 'SN', 870
WHERE NOT EXISTS (
    SELECT 1
    FROM fil_un
    WHERE country = 'SN'
)

UPDATE fil_un SET value = 68 WHERE country = 'SE';