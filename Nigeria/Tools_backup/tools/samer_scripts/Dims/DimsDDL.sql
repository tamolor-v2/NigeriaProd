use flare_8;
CREATE EXTERNAL TABLE `flare_8.dim_network_type`(
  `network_type_code` int,
  `network_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_NETWORK_TYPE';
  
CREATE EXTERNAL TABLE `flare_8.dim_call_type`(
  `call_type_code` int,
  `call_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_CALL_TYPE';
 
 
CREATE EXTERNAL TABLE `flare_8.dim_bill_type`(
  `bill_type_code` int,
  `bill_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_BILL_TYPE';
  
  
CREATE EXTERNAL TABLE `flare_8.dim_charge_type`(
  `charge_type_code` int,
  `charge_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_CHARGE_TYPE';  
  
  
CREATE EXTERNAL TABLE `flare_8.dim_service_type`(
  `service_type_code` int,
  `service_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_SERVICE_TYPE';  
  
  

CREATE EXTERNAL TABLE `flare_8.dim_location_type`(
  `location_type_code` int,
  `location_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_LOCATION_TYPE';  
  
  
CREATE EXTERNAL TABLE `flare_8.dim_sms_type`(
  `sms_type_code` int,
  `sms_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_SMS_TYPE'; 
  
  
CREATE EXTERNAL TABLE `flare_8.dim_data_type`(
  `data_type_code` int,
  `data_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_DATA_TYPE';   
  
  
CREATE EXTERNAL TABLE `flare_8.dim_opco`(
  `opco_name` int,
  `opco_business_type` string,
  `opco_country` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_OPCO';   
  

  
CREATE EXTERNAL TABLE `flare_8.dim_event_type`(
  `event_type` int,
  `event_name` string,
  `rgs_ind` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/EventTypeLookUp'
  
  
CREATE EXTERNAL TABLE `flare_8.dim_dashboard_event`(
  `dashboard_event_code` int,
  `dashboard_event_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_DASHBOARD_EVENT';   

  
CREATE EXTERNAL TABLE `flare_8.dim_subscriber_type`(
  `subscriber_type_code` int,
  `subscriber_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_SUBSCRIBER_TYPE';   
 
 CREATE EXTERNAL TABLE `flare_8.dim_recharge_type`(
  `recharge_type_code` int,
  `recharge_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_RECHARGE_TYPE';   
 

CREATE EXTERNAL TABLE `flare_8.dim_transaction_type`(
  `transaction_type_code` int,
  `transaction_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_TRANSACTION_TYPE';   
  
  
CREATE EXTERNAL TABLE `flare_8.dim_smartphone_type`(
  `smartphone_type_code` int,
  `smartphone_type_description` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ngdaas/FlareData/output_8/DIM_SMARTPHONE_TYPE';   
