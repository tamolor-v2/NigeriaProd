CREATE EXTERNAL TABLE audit.`files_count_rejected_summary_pre`(
  `fullfilename` string,
  `filename` string,
  `numberofline` bigint,
  `main_reason` string)
PARTITIONED BY (
  `rejecteddate` int,
  `tbl_dt` int,
  `feedname` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas//FlareData/audit/files_count_rejected_summary_pre'
TBLPROPERTIES (
  'transient_lastDdlTime'='1532503347');
