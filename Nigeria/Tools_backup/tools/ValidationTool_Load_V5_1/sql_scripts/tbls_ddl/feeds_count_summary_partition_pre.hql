 CREATE EXTERNAL TABLE `audit.feeds_count_summary_partition_pre`( 
   `numberofline` bigint,                           
   `numberofuniquemsisdn` bigint,                   
   `numberofuniquefile` bigint,                     
   `feed_name` string,                              
   `loaded_date` timestamp,                         
   `user_id` string,                                
   `max_duration` bigint,                           
   `sum_duration` bigint,                           
   `avg_duration` decimal(30,4),                    
   `min_duration` bigint,                           
   `max_upload` bigint,                             
   `sum_upload` bigint,                             
   `avg_upload` decimal(30,4),                      
   `min_upload` bigint,                             
   `max_download` bigint,                           
   `sum_download` bigint,                           
   `avg_download` decimal(30,4),                    
   `min_download` bigint,                           
   `max_revenue` decimal(30,4),                     
   `sum_revenue` decimal(30,4),                     
   `avg_revenue` decimal(30,4),                     
   `min_revenue` decimal(30,4),                     
   `max_da_amount_value` decimal(30,4),             
   `sum_da_amount_value` decimal(30,4),             
   `avg_da_amount_value` decimal(30,4),             
   `min_da_amount_value` decimal(30,4),             
   `max_ma_amount_value` decimal(30,4),             
   `sum_ma_amount_value` decimal(30,4),             
   `avg_ma_amount_value` decimal(30,4),             
   `min_ma_amount_value` decimal(30,4))             
 PARTITIONED BY (                                   
   `tbl_dt` int)                                    
 ROW FORMAT SERDE                                   
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  
 STORED AS INPUTFORMAT                              
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'  
 OUTPUTFORMAT                                       
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
 LOCATION                                           
   'hdfs://ngdaas/FlareData/audit/files_count_summary_partition_pre' 
 TBLPROPERTIES (                                    
   'bucketing_version'='2',                         
   'parquet.compression'='GZIP',                    
   'transient_lastDdlTime'='1542110413');