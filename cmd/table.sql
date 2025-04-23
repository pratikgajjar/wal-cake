CREATE TABLE sample_table (
    id SERIAL PRIMARY KEY,
    -- Numeric types
    small_int SMALLINT,                     -- 2-byte integer (-32768 to 32767)
    integer_col INTEGER,                    -- 4-byte integer (-2147483648 to 2147483647)
    big_int BIGINT,                         -- 8-byte integer
    decimal_col DECIMAL(10, 2),             -- Exact numeric with precision and scale
    numeric_col NUMERIC(10, 2),             -- Exact numeric (same as DECIMAL)
    real_col REAL,                          -- 4-byte floating point
    double_col DOUBLE PRECISION,            -- 8-byte floating point
    -- Character types
    char_col CHAR(10),                      -- Fixed-length character string
    varchar_col VARCHAR(255),               -- Variable-length character string
    text_col TEXT,                          -- Variable unlimited length text
    -- Boolean type
    boolean_col BOOLEAN,                    -- true/false
    -- Date/Time types
    date_col DATE,                          -- Date (no time)
    time_col TIME,                          -- Time (no date)
    timestamp_col TIMESTAMP,                -- Date and time
    timestamptz_col TIMESTAMP WITH TIME ZONE, -- Date and time with timezone
    interval_col INTERVAL,                  -- Time interval
    -- Binary data
    bytea_col BYTEA,                        -- Binary data ("byte array")
    -- UUID
    uuid_col UUID,                          -- Universally Unique Identifier
    -- JSON types
    json_col JSON,                          -- JSON data
    jsonb_col JSONB,                        -- Binary JSON data (more efficient)
    int_array INTEGER[],                    -- Array of integers
    text_array TEXT[],                      -- Array of text
    -- Geometric types
    point_col POINT,                        -- Geometric point (x,y)
    line_col LINE,                          -- Infinite line
    -- Special types
    -- Full-text search
    tsvector_col TSVECTOR,                  -- Text search document
    -- Audit fields (common in many applications)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);


CREATE INDEX idx_sample_table_varchar ON sample_table(varchar_col);

-- Optional: Add a GIN index for full-text search
CREATE INDEX idx_sample_table_tsvector ON sample_table USING GIN (tsvector_col);

-- Optional: Add a function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_sample_table_modtime
BEFORE UPDATE ON sample_table
FOR EACH ROW EXECUTE FUNCTION update_modified_column();
`