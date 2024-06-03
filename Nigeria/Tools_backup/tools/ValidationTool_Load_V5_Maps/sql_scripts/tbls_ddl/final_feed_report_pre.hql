CREATE EXTERNAL TABLE `audit.final_feed_report_pre`(
  `feed_name` string,
  `hive_records` bigint,
  `hive_files` bigint,
  `ops_lines` bigint,
  `ops_files` bigint,
  `rejected_records` bigint,
  `rejected_files` bigint,
  `rejected_ratio` double,
  `diff` bigint,
  `diff_ratio` double,
  `status` string,
  `partition_line` bigint,
  `unique_msisdn` bigint,
  `stats_files` bigint,
  `files_stats_count` bigint,
  `rejected_main_reason` string)
PARTITIONED BY (
  `tbl_dt` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/audit/final_feed_report_pre'
TBLPROPERTIES (
  'transient_lastDdlTime'='1541498210');

