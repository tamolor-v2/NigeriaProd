ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 15 --status 0 --hostname "$hostName" --step 1 --brokers $kafkaHostList --topic $TopicName
### push to kafka here

currentDate=$1
echo "hdfs dfs -rm /FlareData/output_8/NEWREG_BIOUPDT_POOL/tbl_dt\=$currentDate/*"
hdfs dfs -rm /FlareData/output_8/NEWREG_BIOUPDT_POOL/tbl_dt\=$currentDate/*
##================================= this query for msisdn_key !=0 =================================

/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute " insert  into flare_8.newreg_bioupdt_pool select cast (date_key as varchar) ,'W' source ,report_gen_date ,mobl_num_voice_v ,status_code_v ,subs_title_v ,first_name ,last_name ,middle_name ,gender ,state_of_origin ,lga_of_origin ,mother_maiden_name ,transaction_id ,uploaded_date ,vendor_channel ,sim_reg_device_id ,device_user_id ,reg_state ,reg_lga ,religion ,registration_state ,registration_lga ,birthdate ,address ,city ,city_desc ,district ,district_desc ,country ,country_desc ,occupation ,alternate_number ,eye_balling_status ,dob ,file_name ,file_offset ,kamanja_loaded_date ,file_mod_date,msisdn_key ,event_timestamp_enrich,original_timestamp_enrich,tbl_dt as date_from_source,tbl_dt from flare_8.newreg_bioupdt_pool_weekly where tbl_dt = ${currentDate}" --output-format CSV

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 15 --status 1 --hostname "$hostName" --step 1 --brokers $kafkaHostList --topic $TopicName
### push to kafka here
