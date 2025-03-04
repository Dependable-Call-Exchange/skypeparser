"""
Database models for raw and cleaned Skype data storage.
"""

CREATE_RAW_TABLES_SQL = """
-- Raw data storage
CREATE TABLE IF NOT EXISTS raw_skype_exports (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    export_date TIMESTAMP NOT NULL,
    CONSTRAINT uq_file_hash UNIQUE (file_hash)
);

-- Create index for faster JSON querying
CREATE INDEX IF NOT EXISTS idx_raw_data_gin ON raw_skype_exports USING gin (raw_data);
CREATE INDEX IF NOT EXISTS idx_export_date ON raw_skype_exports (export_date DESC);

-- Cleaned data storage
CREATE TABLE IF NOT EXISTS cleaned_skype_exports (
    id SERIAL PRIMARY KEY,
    raw_export_id INTEGER NOT NULL REFERENCES raw_skype_exports(id),
    cleaned_data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    cleaning_version VARCHAR(10) NOT NULL,
    CONSTRAINT fk_raw_export
        FOREIGN KEY(raw_export_id)
        REFERENCES raw_skype_exports(id)
        ON DELETE CASCADE
);

-- Create index for faster JSON querying
CREATE INDEX IF NOT EXISTS idx_cleaned_data_gin ON cleaned_skype_exports USING gin (cleaned_data);
CREATE INDEX IF NOT EXISTS idx_cleaned_raw_export_id ON cleaned_skype_exports(raw_export_id);
CREATE INDEX IF NOT EXISTS idx_cleaning_version ON cleaned_skype_exports(cleaning_version);

-- Create view for easy querying of latest cleaned versions
CREATE OR REPLACE VIEW latest_cleaned_exports AS
SELECT
    ce.*,
    re.file_name,
    re.export_date
FROM cleaned_skype_exports ce
JOIN raw_skype_exports re ON ce.raw_export_id = re.id
WHERE ce.id IN (
    SELECT MAX(id)
    FROM cleaned_skype_exports
    GROUP BY raw_export_id
);
"""

# SQL for checking duplicate files
CHECK_DUPLICATE_SQL = """
SELECT id, file_name, export_date
FROM raw_skype_exports
WHERE file_hash = %s;
"""

# SQL for inserting raw data
INSERT_RAW_DATA_SQL = """
INSERT INTO raw_skype_exports (raw_data, file_name, file_hash, export_date)
VALUES (%s, %s, %s, %s)
ON CONFLICT (file_hash) DO NOTHING
RETURNING id;
"""

# SQL for inserting cleaned data
INSERT_CLEANED_DATA_SQL = """
INSERT INTO cleaned_skype_exports (raw_export_id, cleaned_data, cleaning_version)
VALUES (%s, %s, %s)
RETURNING id;
"""

# SQL for retrieving latest cleaned version
GET_LATEST_CLEANED_SQL = """
SELECT ce.*, re.file_name, re.export_date
FROM cleaned_skype_exports ce
JOIN raw_skype_exports re ON ce.raw_export_id = re.id
WHERE ce.raw_export_id = %s
ORDER BY ce.id DESC
LIMIT 1;
"""