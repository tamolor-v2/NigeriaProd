CREATE EXTERNAL TABLE audit.`feeds_count_stats_summary_pre`(
  `numberoffiles` bigint,
  `recordscount` bigint,
  `processedbykamanja` bigint,
  `feed_name` string)
PARTITIONED BY (
  `file_dt` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/audit/feeds_count_stats_summary_pre'
TBLPROPERTIES (
  'transient_lastDdlTime'='1541498056');

