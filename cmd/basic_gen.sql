WITH
    data AS (
        SELECT
            -- (CURRENT_TIMESTAMP - ((n * 2) || ' days')::INTERVAL) AS created_at
            now () as created_at
        FROM
            generate_series (1, 3) AS n
    )
INSERT INTO
    basic (created_at)
SELECT
    created_at
FROM
    data;

UPDATE basic
SET
    created_at = now () - INTERVAL '9 days'
WHERE
    id > 0;

DELETE FROM basic
WHERE
    id > 0;
