INSERT INTO sample_table (
    small_int, integer_col, big_int, decimal_col, numeric_col,
    real_col, double_col, char_col, varchar_col, text_col,
    boolean_col, date_col, time_col, timestamp_col, timestamptz_col,
    interval_col, bytea_col, uuid_col, json_col, jsonb_col,
    int_array, text_array, point_col, line_col, tsvector_col,
    created_by, updated_by
)
SELECT
    -- Numeric types
    (random() * 1000)::smallint AS small_int,
    (random() * 10000)::integer AS integer_col,
    (random() * 1000000)::bigint AS big_int,
    (random() * 1000)::numeric(10,2) AS decimal_col,
    (random() * 1000)::numeric(10,2) AS numeric_col,
    random()::real * 100 AS real_col,
    random() * 1000 AS double_col,

    -- Character types
    lpad(n::text, 10, 'X') AS char_col,
    'varchar-' || n AS varchar_col,
    'This is a long text column for sample data row #' || n AS text_col,

    -- Boolean
    n % 2 = 0 AS boolean_col,

    -- Date/Time types
    current_date - (n % 365) * interval '1 day' AS date_col,
    current_time - (n % 24) * interval '1 hour' AS time_col,
    current_timestamp - (n % 30) * interval '1 day' - (n % 24) * interval '1 hour' AS timestamp_col,
    current_timestamp - (n % 30) * interval '1 day' - (n % 24) * interval '1 hour' AS timestamptz_col,
    (n || ' days')::interval AS interval_col,

    -- Binary data
    decode(md5(n::text), 'hex') AS bytea_col,

    -- UUID
    uuid_generate_v4() AS uuid_col,

    -- JSON types
    json_build_object(
        'id', n,
        'name', 'Sample ' || n,
        'tags', array['tag1', 'tag2'],
        'metadata', json_build_object('created', current_date, 'priority', n % 5)
    ) AS json_col,

    jsonb_build_object(
        'id', n,
        'name', 'Sample ' || n,
        'tags', array['tag1', 'tag2'],
        'metadata', json_build_object('created', current_date, 'priority', n % 5)
    ) AS jsonb_col,

    -- Array types
    ARRAY[n, n*2, n*3] AS int_array,
    ARRAY['text' || n, 'sample' || n, 'data' || n] AS text_array,

    -- Geometric types
    point(n, n*1.5) AS point_col,
    line(point(n, n), point(n+1, n+1)) AS line_col,

    -- Full-text search
    to_tsvector('english', 'This is sample text data for row number ' || n ||
                ' with some searchable content keywords postgres database') AS tsvector_col,

    -- Audit fields
    'system' AS created_by,
    'system' AS updated_by

FROM generate_series(1, 5) AS n;
