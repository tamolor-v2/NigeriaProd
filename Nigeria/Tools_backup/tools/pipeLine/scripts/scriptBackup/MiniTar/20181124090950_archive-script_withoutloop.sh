#!/bin/bash

date=$1

####report_folder=/mnt/beegfs/tmp/archive_reports/
report_folder=/mnt/beegfs/tools/Crontab/logs/archive_reports/${date}
infolder=/mnt/beegfs/production/archived
removeDir=`date '+%C%y%m%d' -d "$end_date-63 days"`
mkdir ${report_folder}
rm -r /mnt/beegfs/tools/Crontab/logs/archive_reports/${removeDir}
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 1 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete original message already mini tarred"
### push to kafka here
# delete files
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $infolder/AIR_ADJ_DA/$date/$date $infolder/AIR_ADJ_MA/$date/$date $infolder/AIR_REFILL_AC/$date/$date $infolder/AIR_REFILL_DA/$date/$date $infolder/AIR_REFILL_MA/$date/$date $infolder/BUNDLE4U_GPRS/$date/$date $infolder/BUNDLE4U_VOICE/$date/$date $infolder/CB_SERV_MAST_VIEW/$date/$date $infolder/CCN_GPRS_AC/$date/$date $infolder/CCN_GPRS_DA/$date/$date $infolder/CCN_GPRS_MA/$date/$date $infolder/CCN_SMS_AC/$date/$date $infolder/CCN_SMS_DA/$date/$date $infolder/CCN_SMS_MA/$date/$date $infolder/CCN_VOICE_AC/$date/$date $infolder/CCN_VOICE_DA/$date/$date $infolder/CCN_VOICE_MA/$date/$date $infolder/Containers/$date/$date $infolder/CUG_ACCESS_FEES/$date/$date $infolder/DMC_DUMP_ALL/$date/$date $infolder/DMC_DUMP_ALL_unsplitted/$date/$date $infolder/EWP_FINANCIAL_LOG/$date/$date $infolder/GGSN/$date/$date $infolder/HSDP/$date/$date $infolder/HSDP_tmp/$date/$date $infolder/MAPS_INV_2G/$date/$date $infolder/MAPS_INV_3G/$date/$date $infolder/MAPS_INV_4G/$date/$date $infolder/MOBILE_MONEY/$date/$date $infolder/MSC/$date/$date $infolder/SDP_ACC_ADJ_AC/$date/$date $infolder/SDP_ACC_ADJ_MA/$date/$date $infolder/SDP_ADJ_DA/$date/$date $infolder/SDP_DMP_MA/$date/$date $infolder/SGSN/$date/$date $infolder/WBS_PM_RATED_CDRS/$date/$date $infolder/SDP_DUMP_SUBSCRIBER/$date/$date $infolder/SDP_DUMP_OFFER/$date/$date $infolder/RECON/$date/$date $infolder/AGL_CRM_COUNTRY_MAP/$date/$date $infolder/AGL_CRM_LGA_MAP/$date/$date $infolder/AGL_CRM_STATE_MAP/$date/$date $infolder/CALL_REASON/$date/$date $infolder/CALL_REASON_MONTHLY/$date/$date $infolder/MNP_PORTING_BROADCAST/$date/$date $infolder/MVAS_DND_MSISDN_REP_CDR/$date/$date $infolder/NEWREG_BIOUPDT_POOL/$date/$date $infolder/SDP_DMP_DA/$date/$date $infolder/LBN/$date/$date $infolder/UDC_DUMP/$date/$date $infolder/CB_NEWREG_BIOUPDT_POOL_DAILY/$date/$date $infolder/NEWREG_BIOUPDT_POOL_WEEKLY/$date/$date  -nt 32 -vad -rmp $infolder -out $report_folder -dp 15 -tf 1 -nosim -lbl mini_delete_ -of -od -fnfp 5

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 1 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete original message already mini tarred"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 2 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar most of feeds"
### push to kafka here

# tarring most of the feeds
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $infolder/AIR_ADJ_DA/$date $infolder/AIR_REFILL_MA/$date $infolder/CCN_GPRS_DA/$date $infolder/CCN_SMS_MA/$date $infolder/GGSN/$date $infolder/MSC/$date $infolder/SDP_ADJ_DA/$date $infolder/AIR_ADJ_MA/$date $infolder/BUNDLE4U_GPRS/$date $infolder/CCN_GPRS_MA/$date $infolder/CCN_VOICE_AC/$date $infolder/HSDP/$date $infolder/SDP_DMP_MA/$date $infolder/AIR_REFILL_AC/$date $infolder/BUNDLE4U_VOICE/$date $infolder/CCN_SMS_AC/$date $infolder/CCN_VOICE_DA/$date $infolder/DMC_DUMP_ALL/$date $infolder/MAPS_INV_2G/$date $infolder/SDP_ACC_ADJ_AC/$date $infolder/SGSN/$date $infolder/AIR_REFILL_DA/$date $infolder/CCN_GPRS_AC/$date $infolder/CCN_SMS_DA/$date $infolder/CCN_VOICE_MA/$date $infolder/EWP_FINANCIAL_LOG/$date $infolder/MAPS_INV_4G/$date $infolder/SDP_ACC_ADJ_MA/$date $infolder/WBS_PM_RATED_CDRS/$date $infolder/SDP_DMP_AC/$date  $infolder/SDP_DMP_DA/$date $infolder/SDP_DUMP_OFFER/$date $infolder/SDP_DUMP_SUBSCRIBER/$date $infolder/RECON/$date $infolder/CB_SERV_MAST_VIEW/$date $infolder/CUG_ACCESS_FEES/$date $infolder/SDP_DMP_MA/$date $infolder/MAPS_INV_3G/$date $infolder/MOBILE_MONEY/$date  $infolder/UDC_DUMP/$date  "$infolder/#3/#1/#1" -nt 32 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^([0-9]{8})([0-9]{2}).*_(AIR_ADJ_DA|AIR_ADJ_MA|AIR_REFILL_AC|AIR_REFILL_DA|AIR_REFILL_MA|BUNDLE4U_GPRS|BUNDLE4U_VOICE|CCN_GPRS_AC|CCN_GPRS_DA|CCN_GPRS_MA|CCN_SMS_AC|CCN_SMS_DA|CCN_SMS_MA|CCN_VOICE_AC|CCN_VOICE_DA|CCN_VOICE_MA|DMC_DUMP_ALL|EWP_FINANCIAL_LOG|GGSN|SDP_ACC_ADJ_AC|SDP_ADJ_DA|SDP_ACC_ADJ_MA|SDP_DMP_MA|SGSN|SDP_DMP_DA|UDC_DUMP|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS)_.*$" -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^([0-9]{8})([0-9]{2}).*_(AIR_ADJ_DA|AIR_ADJ_MA|AIR_REFILL_AC|AIR_REFILL_DA|AIR_REFILL_MA|BUNDLE4U_GPRS|BUNDLE4U_VOICE|CCN_GPRS_AC|CCN_GPRS_DA|CCN_GPRS_MA|CCN_SMS_AC|CCN_SMS_DA|CCN_SMS_MA|CCN_VOICE_AC|CCN_VOICE_DA|CCN_VOICE_MA|DMC_DUMP_ALL|EWP_FINANCIAL_LOG|GGSN|SDP_ACC_ADJ_AC|SDP_ADJ_DA|SDP_ACC_ADJ_MA|SDP_DMP_MA|SGSN|SDP_DMP_DA|UDC_DUMP|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS)_.*$" -nosim -lbl mini_taring_  2>&1 | tee "$report_folder/mini_taring_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_$mytime.log)
 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 2 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar most of feeds : $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 3 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar EWP_FINANCIAL_LOG"
### push to kafka here
#
#tar Fin log feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/EWP_FINANCIAL_LOG/$date -outarchive "$infolder/EWP_FINANCIAL_LOG/#1/#1" -nt 32 -cvzf -tarPrefix "EWP_FINANCIAL_LOG-#1-#2" -rgrm "$infolder/EWP_FINANCIAL_LOG" -groupregex "^financiallog-([0-9]{8})([0-9]{2}).*$"   -effl "Archive-*.*$" -iffl "^financiallog-([0-9]{8})([0-9]{2}).*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_EWP_FINANCIAL_ 2>&1 | tee "$report_folder/mini_taring_EWP_FINANCIAL_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_EWP_FINANCIAL_LOG_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 3 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar EWP_FINANCIAL_LOG: $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 4 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar AIR_REFILL_DA"
### push to kafka here

#tar AIR_REFILL_DA
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/AIR_REFILL_DA/$date -outarchive "$infolder/AIR_REFILL_DA/#1/#1" -nt 32 -cvzf -tarPrefix "AIR_REFILL_DA-#1-#2" -rgrm "$infolder/AIR_REFILL_DA" -groupregex "^([0-9]{8})([0-9]{2}).*_(AIR_REFILL_SubDA)_.*$"   -effl "Archive-*.*$" -iffl "^([0-9]{8})([0-9]{2}).*_(AIR_REFILL_SubDA)_.*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_AIR_REFILL_DA_ 2>&1 | tee "$report_folder/AIR_REFILL_DA_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/AIR_REFILL_DA_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 4 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar AIR_REFILL_DA: $numberOfFiles"
### push to kafka here


 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 5 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar BUNDLE4U_VOICE"
### push to kafka here

#tar BUNDLE4U_VOICE
#--------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/BUNDLE4U_VOICE/$date -outarchive "$infolder/BUNDLE4U_VOICE/#1/#1" -nt 32 -cvzf -tarPrefix "BUNDLE4U_VOICE-#1-#2" -rgrm "$infolder/BUNDLE4U_VOICE" -groupregex "^([0-9]{8})([0-9]{2}).*_(CCN_VOICE_AP)_.*$"   -effl "Archive-*.*$" -iffl "^([0-9]{8})([0-9]{2}).*_(CCN_VOICE_AP)_.*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_BUNDLE4U_VOICE_ 2>&1 | tee "$report_folder/mini_taring_BUNDLE4U_VOICE_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_BUNDLE4U_VOICE_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 5 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar BUNDLE4U_VOICE: $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 6 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar BUNDLE4U_GPRS"
### push to kafka here

#tar BUNDLE4U_GPRS
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/BUNDLE4U_GPRS/$date -outarchive "$infolder/BUNDLE4U_GPRS/#1/#1" -nt 32 -cvzf -tarPrefix "BUNDLE4U_GPRS-#1-#2" -rgrm "$infolder/BUNDLE4U_GPRS" -groupregex "^([0-9]{8})([0-9]{2}).*_(CCN_GPRS)_.*$"   -effl "Archive-*.*$" -iffl "^([0-9]{8})([0-9]{2}).*_(CCN_GPRS)_.*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_BUNDLE4U_GPRS_ 2>&1 | tee "$report_folder/mini_taring_BUNDLE4U_GPRS_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_BUNDLE4U_GPRS_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 6 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar BUNDLE4U_GPRS: $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 7 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MAPS_INV_4G"
### push to kafka here

#tar MAPS_INV_4G
#----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_INV_4G/$date -outarchive "$infolder/MAPS_INV_4G/#1/#1" -nt 32 -cvzf -tarPrefix "MAPS_INV_4G-#1-#2" -rgrm "$infolder/MAPS_INV_4G" -groupregex "^Nigeria_BIB_4GCell_INV_([0-9]{8})([0-9]{2}).*$"   -effl "Archive-*.*$" -iffl "^Nigeria_BIB_4GCell_INV_([0-9]{8})([0-9]{2}).*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_MAPS_INV_4G_ 2>&1 | tee "$report_folder/mini_taring_MAPS_INV_4G_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_MAPS_INV_4G_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 7 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MAPS_INV_4G: $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 8 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MAPS_INV_3G"
### push to kafka here
#tar MAPS_INV_3G
#----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_INV_3G/$date -outarchive "$infolder/MAPS_INV_3G/#1/#1" -nt 32 -cvzf -tarPrefix "MAPS_INV_3G-#1-#2" -rgrm "$infolder/MAPS_INV_3G" -groupregex "^Nigeria_BIB_3GCell_INV_([0-9]{8})([0-9]{2}).*$"   -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3GCell_INV_([0-9]{8})([0-9]{2}).*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_MAPS_INV_3G_ 2>&1 | tee "$report_folder/mini_taring_MAPS_INV_3G_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_MAPS_INV_3G_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 8 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MAPS_INV_3G: $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 9 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MAPS_INV_2G"
### push to kafka here

#tar MAPS_INV_2G
#----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_INV_2G/$date -outarchive "$infolder/MAPS_INV_2G/#1/#1" -nt 32 -cvzf -tarPrefix "MAPS_INV_2G-#1-#2" -rgrm "$infolder/MAPS_INV_2G" -groupregex "^Nigeria_BIB_2GCell_INV_([0-9]{8})([0-9]{2}).*$"   -effl "Archive-*.*$" -iffl "^Nigeria_BIB_2GCell_INV_([0-9]{8})([0-9]{2}).*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_MAPS_INV_2G_ 2>&1 | tee "$report_folder/mini_taring_MAPS_INV_2G_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_MAPS_INV_2G_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 9 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MAPS_INV_2G: $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 10 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar DMC_DUMP_ALL"
### push to kafka here

#tar DMC_DUMP_ALL
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/DMC_DUMP_ALL/$date -outarchive "$infolder/DMC_DUMP_ALL/#1/#1" -nt 32 -cvzf -tarPrefix "DMC_DUMP_ALL-#1" -rgrm "$infolder/DMC_DUMP_ALL" -groupregex "^.*([0-9]{8})_DUMP.*$"   -effl "Archive-*.*$" -iffl "^.*([0-9]{8})_DUMP.*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_DMC_DUMP_ALL_ 2>&1 | tee "$report_folder/mini_taring_DMC_DUMP_ALL_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_DMC_DUMP_ALL_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 10 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar DMC_DUMP_ALL: $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 11 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar GGS"
### push to kafka here

#GGSN
#-----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/GGSN/$date -outarchive "$infolder/GGSN/#1/#1" -nt 32 -cvzf -tarPrefix "GGSN-#1-#2" -rgrm "$infolder/GGSN" -groupregex "^([0-9]{8})([0-9]{2}).*_(GGSN)[\.|_].*$"   -effl "Archive-*.*$" -iffl "^([0-9]{8})([0-9]{2}).*_(GGSN)[\.|_].*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_GGSN_ 2>&1 | tee "$report_folder/mini_taring_GGSN_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_GGSN_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 11 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar GGS: $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 12 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MSC"
### push to kafka here

#MSC
#----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MSC/$date -outarchive "$infolder/MSC/#1/#1" -nt 32 -cvzf -tarPrefix "MSC-#1-#2" -rgrm "$infolder/MSC" -groupregex "^([0-9]{8})([0-9]{2}).*_(MSC)[\.|_].*$"   -effl "Archive-*.*$" -iffl "^([0-9]{8})([0-9]{2}).*_(MSC)[\.|_].*$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_MSC_ 2>&1 | tee "$report_folder/mini_taring_MSC_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_MSC_$mytime.log)

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 12 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MSC: $numberOfFiles"
### push to kafka here

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 13 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_ACC_ADJ_DA"
#--SDP_ADJ_DA
#------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="SDP_ADJ_DA"
regex="^([0-9]{8})([0-9]{2}).*_(SDP_ACC_ADJ_DA)[\.|_].*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1-#2" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 13 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_ACC_ADJ_DA: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 14 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar HSDP"

#--HSDP
#------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="HSDP"
regex="^([0-9]{8})([0-9]{2}).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1-#2" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 14 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar HSDP: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 15 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SGSN"

#SGSN
#----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="SGSN"
regex="^([0-9]{8})([0-9]{2}).*_(SGSN)[\.|_].*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1-#2" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 15 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SGSN: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 16 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DMP_AC"

#SDP_DMP_AC
#----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="SDP_DMP_AC"
regex=".*DUMP([0-9]{8})([0-9]{2}).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1-#2" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 16 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DMP_AC: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 17 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DMP_MA"

#SDP_DMP_MA
#----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="SDP_DMP_MA"
regex=".*_([0-9]{8})([0-9]{2}).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1-#2" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 17 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DMP_MA: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 18 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DMP_DA"

#SDP_DMP_DA
#----------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="SDP_DMP_DA"
regex=".*DUMP([0-9]{8})([0-9]{2}).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1-#2" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 18 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DMP_DA: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 19 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DUMP_OFFER"


#SDP_DUMP_OFFER
#----------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="SDP_DUMP_OFFER"
regex=".*offer\.([0-9]{8})([0-9]{2}).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1-#2" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 19 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DUMP_OFFER: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 20 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DUMP_SUBSCRIBER"


#SDP_DUMP_SUBSCRIBER
#----------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="SDP_DUMP_SUBSCRIBER"
regex=".*subscriber_offer\.([0-9]{8})([0-9]{2}).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1-#2" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 20 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar SDP_DUMP_SUBSCRIBER: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 21 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar RECON"

#RECON
#----------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="RECON"
regex="^(RECON)_.*_([0-9]{8}).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#2/#2" -nt 32 -cvzf -tarPrefix "$feed-#2" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 21 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar RECON: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 22 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar PM_RATED"


#tar PM_RATED feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="WBS_PM_RATED_CDRS"
regex="^([0-9]{8})_(WBS_PM_RATED_CDRS)_.*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 22 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar PM_RATED: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 23 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar CALL_REASON_MONTHLY"

#tar CALL_REASON_MONTHLY feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="CALL_REASON_MONTHLY"
regex="^([0-9]{6})_(CALL_REASON_MONTHLY)_.*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/$date/$date" -nt 32 -cvzf -tarPrefix "$feed-$date" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 23 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar CALL_REASON_MONTHLY: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 24 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar CALL_REASON"

#tar CALL_REASON feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="CALL_REASON"
regex="^([0-9]{8})_(CALL_REASON)_.*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 24 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar CALL_REASON: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 25 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MNP_PORTING_BROADCAST"

#tar CALL_REASON_MONTHLY feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="MNP_PORTING_BROADCAST"
regex="^([0-9]{8})_(MNP_PORTING_BROADCAST).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 25 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MNP_PORTING_BROADCAST: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 26 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar NEWREG_BIOUPDT_POOL"

#tar NEWREG_BIOUPDT_POOL feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="NEWREG_BIOUPDT_POOL"
regex="^([0-9]{8})_(NEWREG_BIOUPDT_POOL)_.*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 26 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar NEWREG_BIOUPDT_POOL: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 27 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MVAS_DND_MSISDN_REP_CDR"

#tar MVAS_DND_MSISDN_REP_CDR feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="MVAS_DND_MSISDN_REP_CDR"
regex="^DND_NUM_([0-9]{8})([0-9]{6}).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 27 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MVAS_DND_MSISDN_REP_CDR: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 28 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar AGL_CRM_COUNTRY_MAP"

#tar AGL_CRM_COUNTRY_MAP feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="AGL_CRM_COUNTRY_MAP"
regex="^([0-9]{8})_(AGL_CRM_COUNTRY_MAP)_.*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 28 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar AGL_CRM_COUNTRY_MAP: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 29 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar AGL_CRM_LGA_MAP"

#tar AGL_CRM_LGA_MAP feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="AGL_CRM_LGA_MAP"
regex="^([0-9]{8})_(AGL_CRM_LGA_MAP)_.*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 29 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar AGL_CRM_LGA_MAP: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 30 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar AGL_CRM_STATE_MAP"

#tar AGL_CRM_STATE_MAP feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="AGL_CRM_STATE_MAP"
regex="^([0-9]{8})_(AGL_CRM_STATE_MAP)_.*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 30 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar AGL_CRM_STATE_MAP: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 31 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar CUG_ACCESS_FEES"

#tar CUG_ACCESS_FEES feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="CUG_ACCESS_FEES"
regex="^([0-9]{8})_(CUG_ACCESS_FEES)_.*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 31 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar CUG_ACCESS_FEES: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 32 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar CB_SERV_MAST_VIEW"

#tar CB_SERV_MAST_VIEW feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="CB_SERV_MAST_VIEW"
regex="^([0-9]{8})_(CB_SERV_MAST_VIEW)_.*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 32 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar CB_SERV_MAST_VIEW: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 33 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MOBILE_MONEY"

#tar MOBILE_MONEY
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="MOBILE_MONEY"
regex="(?=[a-zA-Z_]*)([0-9]{8})(?=.*)"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 33 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar MOBILE_MONEY: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 34 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar DPI"

#moving DPI data
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed=DPI
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -nt 32 -iffl "Archive-*.*$" -out $report_folder -dp 15 -tf 1 -of -od -mv $infolder/$feed/$date/$date -nosim -lbl moving_${feed}_ 2>&1 | tee "$report_folder/moving_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/moving_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 34 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar DPI: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 35 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar LBN"

#LBN
#-----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/LBN/$date -outarchive "$infolder/LBN/#1/#1" -nt 32 -cvzf -tarPrefix "LBN-#1-#2" -rgrm "$infolder/LBN" -groupregex "^([0-9]{8})([0-9]{2}).*_(TNP)$"   -effl "Archive-*.*$" -iffl "^([0-9]{8})([0-9]{2}).*_(TNP)$" -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_LBN_ 2>&1 | tee "$report_folder/mini_taring_LBN_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_LBN_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 35 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar LBN: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 36 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar UDC_DUMP"

#UDC_DUMP
#-----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/UDC_DUMP/$date -outarchive "$infolder/UDC_DUMP/#1/#1" -nt 32 -cvzf -tarPrefix "UDC_DUMP-#1" -rgrm "$infolder/UDC_DUMP" -groupregex "^.*([0-9]{8}).*$"   -effl "Archive-*.*$" -iffl  "^.*([0-9]{8}).*$"  -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_UDC_DUMP_ 2>&1 | tee "$report_folder/mini_taring_UDC_DUMP_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_UDC_DUMP_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 36 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar UDC_DUMP: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 37 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar NEWREG_BIOUPDT_POOL_DAILY"

#tar NEWREG_BIOUPDT_POOL_DAILY feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="CB_NEWREG_BIOUPDT_POOL_DAILY"
regex="^([0-9]{8})_(CB_NEWREG_BIOUPDT_POOL_DAILY).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 37 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar NEWREG_BIOUPDT_POOL_DAILY: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 38 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar NEWREG_BIOUPDT_POOL_WEEKLY"

#tar NEWREG_BIOUPDT_POOL_WEEKLY feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
feed="NEWREG_BIOUPDT_POOL_WEEKLY"
regex="^([0-9]{8})_(NEWREG_BIOUPDT_POOL_WEEKLY).*$"
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/$feed/$date -outarchive "$infolder/$feed/#1/#1" -nt 32 -cvzf -tarPrefix "$feed-#1" -rgrm "$infolder/$feed" -groupregex $regex   -effl "Archive-*.*$" -iffl $regex -out $report_folder -dp 15 -tf 1 -ts 80 -nosim -lbl mini_taring_${feed}_ 2>&1 | tee "$report_folder/mini_taring_${feed}_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_${feed}_$mytime.log)

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 38 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar NEWREG_BIOUPDT_POOL_WEEKLY: $numberOfFiles"

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 0 --hostname "$hostName" --step 39 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete original files"


# delete files
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $infolder/AIR_ADJ_DA/$date/$date $infolder/AIR_ADJ_MA/$date/$date $infolder/AIR_REFILL_AC/$date/$date $infolder/AIR_REFILL_DA/$date/$date $infolder/AIR_REFILL_MA/$date/$date $infolder/BUNDLE4U_GPRS/$date/$date $infolder/BUNDLE4U_VOICE/$date/$date $infolder/CB_SERV_MAST_VIEW/$date/$date $infolder/CCN_GPRS_AC/$date/$date $infolder/CCN_GPRS_DA/$date/$date $infolder/CCN_GPRS_MA/$date/$date $infolder/CCN_SMS_AC/$date/$date $infolder/CCN_SMS_DA/$date/$date $infolder/CCN_SMS_MA/$date/$date $infolder/CCN_VOICE_AC/$date/$date $infolder/CCN_VOICE_DA/$date/$date $infolder/CCN_VOICE_MA/$date/$date $infolder/Containers/$date/$date $infolder/CUG_ACCESS_FEES/$date/$date $infolder/DMC_DUMP_ALL/$date/$date $infolder/DMC_DUMP_ALL_unsplitted/$date/$date $infolder/EWP_FINANCIAL_LOG/$date/$date $infolder/GGSN/$date/$date $infolder/HSDP/$date/$date $infolder/HSDP_tmp/$date/$date $infolder/MAPS_INV_2G/$date/$date $infolder/MAPS_INV_3G/$date/$date $infolder/MAPS_INV_4G/$date/$date $infolder/MOBILE_MONEY/$date/$date $infolder/MSC/$date/$date $infolder/SDP_ACC_ADJ_AC/$date/$date $infolder/SDP_ACC_ADJ_MA/$date/$date $infolder/SDP_ADJ_DA/$date/$date $infolder/SDP_DMP_MA/$date/$date $infolder/SGSN/$date/$date $infolder/WBS_PM_RATED_CDRS/$date/$date $infolder/SDP_DUMP_SUBSCRIBER/$date/$date $infolder/SDP_DUMP_OFFER/$date/$date $infolder/RECON/$date/$date $infolder/AGL_CRM_COUNTRY_MAP/$date/$date $infolder/AGL_CRM_LGA_MAP/$date/$date $infolder/AGL_CRM_STATE_MAP/$date/$date $infolder/CALL_REASON/$date/$date $infolder/CALL_REASON_MONTHLY/$date/$date $infolder/MNP_PORTING_BROADCAST/$date/$date $infolder/MVAS_DND_MSISDN_REP_CDR/$date/$date $infolder/NEWREG_BIOUPDT_POOL/$date/$date $infolder/SDP_DMP_DA/$date/$date $infolder/LBN/$date/$date $infolder/UDC_DUMP/$date/$date $infolder/CB_NEWREG_BIOUPDT_POOL_DAILY/$date/$date $infolder/NEWREG_BIOUPDT_POOL_WEEKLY/$date/$date  -nt 32 -vad -rmp $infolder -out $report_folder -dp 15 -tf 1 -nosim -lbl mini_delete_ -of -od -fnfp 5

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 19 --status 1 --hostname "$hostName" --step 39 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete original files"

