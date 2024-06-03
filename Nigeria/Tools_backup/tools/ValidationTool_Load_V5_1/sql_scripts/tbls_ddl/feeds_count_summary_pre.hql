CREATE EXTERNAL TABLE audit.feeds_count_summary_pre(
  `feed_name` string,
  `numberofline` bigint,
  `numberoffiles` bigint,
  `loaded_date` timestamp,
  `user_id` string)
PARTITIONED BY (
  `file_dt` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/audit/feeds_count_summary_pre'
TBLPROPERTIES (
  'parquet.compression'='GZIP',
  'transient_lastDdlTime'='1541517297');

