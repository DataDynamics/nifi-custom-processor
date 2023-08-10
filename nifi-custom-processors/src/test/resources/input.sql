CREATE TABLE input
(
    col_int INT,
    col_float FLOAT,
    col_timestamp TIMESTAMP,
    col_timestamp_millis TIMESTAMP,
    col_timestamp_micros TIMESTAMP,
    PRIMARY KEY(col_int)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU;