CREATE SCHEMA IF NOT EXISTS snapshots;

CREATE OR REPLACE TABLE snapshots.weather_cleaned_snapshot AS
SELECT 
    *,
    CURRENT_TIMESTAMP AS snapshot_timestamp
FROM silver.weather_cleaned;

-- Create an index on the snapshot timestamp for better query performance
CREATE INDEX IF NOT EXISTS idx_snapshot_timestamp 
ON snapshots.weather_cleaned_snapshot(snapshot_timestamp);
