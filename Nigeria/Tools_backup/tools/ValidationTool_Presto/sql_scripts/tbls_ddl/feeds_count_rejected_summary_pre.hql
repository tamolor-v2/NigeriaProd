CREATE EXTERNAL TABLE audit.`feeds_count_rejected_summary_pre`(
  `feed_name` string,
  `numberofline` bigint,
  `main_reason` string)
PARTITIONED BY (
  `tbl_dt` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/audit/feeds_count_rejected_summary_pre'
TBLPROPERTIES (
  'transient_lastDdlTime'='1532503349');
