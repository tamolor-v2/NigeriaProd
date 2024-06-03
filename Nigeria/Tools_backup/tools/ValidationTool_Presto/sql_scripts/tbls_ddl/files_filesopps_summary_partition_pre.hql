CREATE EXTERNAL TABLE audit.`files_filesopps_summary_partition_pre`(
  `file_name` string,
  `lines` bigint)
PARTITIONED BY (
  `file_dt` int,
  `feed_name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/audit/files_filesopps_summary_partition_pre'
TBLPROPERTIES (
  'transient_lastDdlTime'='1533574323');
