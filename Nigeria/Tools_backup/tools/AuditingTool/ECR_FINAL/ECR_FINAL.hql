CREATE EXTERNAL TABLE prodfeeds.ecr (
job_id string,
job_name string,
log_type string,
run_id string,
start_time string,
end_time string,
status string,
message string,
affected_records string,
date_key int,
event_timestamp_enrich bigint,
original_timestamp_enrich string
)
PARTITIONED BY (
  tbl_dt int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://daasza/user/daasuser/flareprod/prodfeeds/ECR_FINAL'
TBLPROPERTIES (
  'parquet.compression'='GZIP');
