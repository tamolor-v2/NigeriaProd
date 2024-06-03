CREATE EXTERNAL TABLE audit.`files_count_stats_summary_pre`(
  `fullfilename` string,
  `filename` string,
  `processedrecordscount` bigint,
  `recordscount` bigint,
  `status` string)
PARTITIONED BY (
  `file_dt` int,
  `tbl_dt` int,
  `feedname` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/audit/files_count_stats_summary_pre'
TBLPROPERTIES (
  'transient_lastDdlTime'='1541498153');

