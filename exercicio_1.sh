echo '{"id":1,"name":"Alice","event_ts":"2025-01-01T10:00:00"}' > raw.json
echo '{"id":2,"name":"Bob","event_ts":"2025-01-01T10:05:00"}'>> raw.json


duckdb -c "COPY
             (SELECT * FROM read_json('raw.json')
             ) TO 'lakehouse/bronze/events_raw.parquet'(FORMAT PARQUET);"


duckdb -c "COPY
            (SELECT 
                    id:: INTEGER,
                    upper(name) as name,
                    CAST(event_ts AS TIMESTAMP) AS event_ts
             FROM 'lakehouse/bronze/events_raw.parquet')
             TO 'lakehouse/silver/events_clean.parquet'(FORMAT PARQUET);"

duckdb -c "COPY
            (SELECT 
                    date_trunc('hour', event_ts) AS hour,
                    count(*) AS event_count
             FROM 'lakehouse/silver/events_clean.parquet'
             GROUP BY hour)
             TO 'lakehouse/gold/events_hourly.parquet'(FORMAT PARQUET);"