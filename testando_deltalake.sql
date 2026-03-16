INSTALL delta;
LOAD delta;

COPY (SELECT * FROM read_parquet('bronze/events_raw.parquet'))
TO 'lakehouse/bronze_delta/'
WITH (FORMAT 'DELTA');