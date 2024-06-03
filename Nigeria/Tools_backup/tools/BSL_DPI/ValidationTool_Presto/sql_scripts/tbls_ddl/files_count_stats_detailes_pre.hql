CREATE EXTERNAL TABLE audit.`files_count_stats_detailes_pre`(
  `seq` bigint,
  `path` string,
  `status` string,
  `starttime` string,
  `recordscount` bigint,
  `processedrecordscount` bigint)
PARTITIONED BY (
  `tbl_dt` int,
  `feed_name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas//FlareData/audit/files_count_stats_detailes_pre'
TBLPROPERTIES (
  'parquet.compression'='GZIP',
  'transient_lastDdlTime'='1541498141');

