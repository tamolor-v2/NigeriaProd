CREATE EXTERNAL TABLE audit.files_count_summary_pre(
  `fullfilename` string,
  `file_name` string,
  `numberofline` bigint,
  `loaded_date` timestamp,
  `user_id` string)
PARTITIONED BY (
  `file_dt` int,
  `tbl_dt` int,
  `feed_name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/audit/files_count_summary_pre'
TBLPROPERTIES (
  'parquet.compression'='GZIP',
  'transient_lastDdlTime'='1532503346');
