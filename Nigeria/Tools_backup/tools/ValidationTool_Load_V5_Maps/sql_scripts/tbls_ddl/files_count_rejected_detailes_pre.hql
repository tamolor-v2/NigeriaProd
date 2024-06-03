CREATE EXTERNAL TABLE audit.`files_count_rejected_detailes_pre`(
  `path` string,
  `numberofline` bigint,
  `main_reason` string)
PARTITIONED BY (
  `rejecteddate` int,
  `feed_name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/audit/files_count_rejected_detailes_pre'
TBLPROPERTIES (
  'parquet.compression'='GZIP',
  'transient_lastDdlTime'='1532503348');
