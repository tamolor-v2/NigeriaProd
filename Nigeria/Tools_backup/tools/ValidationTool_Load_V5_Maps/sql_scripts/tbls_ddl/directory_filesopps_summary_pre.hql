CREATE EXTERNAL TABLE audit.`directory_filesopps_summary_pre`(
  `feedname` string,
  `numberofline` bigint,
  `numberoffile` bigint)
PARTITIONED BY (
  `file_dt` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/audit/directory_filesopps_summary_pre'
TBLPROPERTIES (
  'parquet.compression'='GZIP',
  'transient_lastDdlTime'='1532503349');
