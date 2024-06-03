#!/bin/bash
?

?
date=$1
?

?
####report_folder=/mnt/beegfs/tmp/archive_reports/doubletarring
?
report_folder=/mnt/beegfs/tools/Crontab/logs/doubletarring/${date}
?
from=/mnt/beegfs/production/archived
?
hdfs=/archive
?
removeDate=`date '+%C%y%m%d' -d "$end_date-65 days"`
?

?
mkdir ${report_folder}
?
rm -r /mnt/beegfs/tools/Crontab/logs/doubletarring/${removeDate}
?
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
?
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
?
TopicName=CentralMetaStore
?
hostName=$(hostname)
?
 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 18 --status 0 --hostname "$hostName" --step 1 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete mini tar files already double tarred"
?
### push to kafka here
?

?
# delete files
?
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
?
time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $hdfs/AIR_ADJ_DA/$date $hdfs/AIR_ADJ_MA/$date $hdfs/AIR_REFILL_AC/$date $hdfs/AIR_REFILL_DA/$date $hdfs/AIR_REFILL_MA/$date $hdfs/BUNDLE4U_GPRS/$date $hdfs/BUNDLE4U_VOICE/$date $hdfs/CB_SERV_MAST_VIEW/$date $hdfs/CCN_GPRS_AC/$date $hdfs/CCN_GPRS_DA/$date $hdfs/CCN_GPRS_MA/$date $hdfs/CCN_SMS_AC/$date $hdfs/CCN_SMS_DA/$date $hdfs/CCN_SMS_MA/$date $hdfs/CCN_VOICE_AC/$date $hdfs/CCN_VOICE_DA/$date $hdfs/CCN_VOICE_MA/$date $hdfs/Containers/$date $hdfs/CUG_ACCESS_FEES/$date $hdfs/DMC_DUMP_ALL/$date $hdfs/DMC_DUMP_ALL_unsplitted/$date $hdfs/EWP_FINANCIAL_LOG/$date $hdfs/GGSN/$date $hdfs/HSDP/$date $hdfs/HSDP_tmp/$date $hdfs/MAPS_INV_2G/$date $hdfs/MAPS_INV_3G/$date $hdfs/MAPS_INV_4G/$date $hdfs/MOBILE_MONEY/$date $hdfs/MSC/$date $hdfs/SDP_ACC_ADJ_AC/$date $hdfs/SDP_ACC_ADJ_MA/$date $hdfs/SDP_ADJ_DA/$date $hdfs/SDP_DMP_MA/$date $hdfs/SGSN/$date $hdfs/WBS_PM_RATED_CDRS/$date $hdfs/SDP_DUMP_SUBSCRIBER/$date $hdfs/SDP_DUMP_OFFER/$date $hdfs/RECON/$date $hdfs/DPI/$date $hdfs/AGL_CRM_COUNTRY_MAP/$date $hdfs/AGL_CRM_LGA_MAP/$date $hdfs/AGL_CRM_STATE_MAP/$date $hdfs/CALL_REASON/$date $hdfs/CALL_REASON_MONTHLY/$date $hdfs/MNP_PORTING_BROADCAST/$date $hdfs/MVAS_DND_MSISDN_REP_CDR/$date $hdfs/NEWREG_BIOUPDT_POOL/$date $hdfs/SDP_DMP_DA/$date $hdfs/LBN/$date $hdfs/UDC_DUMP/$date $hdfs/CB_NEWREG_BIOUPDT_POOL_DAILY/$date $hdfs/NEWREG_BIOUPDT_POOL_WEEKLY/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/MSC_DAAS/$date $hdfs/CIS_CDR/$date $hdfs/MSC_DAAS/$date $hdfs/CIS_CDR/$date $hdfs/MSC_DAAS/$date $hdfs/CIS_CDR/$date -nt 32 -vad -rmp $from/#fn/$date/$date -out $report_folder -dp 15 -tf 1 -nosim -lbl double_taring_delete_ -of -od -fnfp 2 -hdfs "hdfs://ngdaas/" -kktf "/etc/security/keytabs/flare.keytab" -kp "flare@MTN.COM" -fromHdfs
?

?
  bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 18 --status 1 --hostname "$hostName" --step 1 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete mini tar files already double tarred"
?
### push to kafka here
?

?
  bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 18 --status 0 --hostname "$hostName" --step 2 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "double tar files"
?
### push to kafka here
?
# tarring most of the feeds
?
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
?
time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $from/AIR_ADJ_DA/$date $from/AIR_ADJ_MA/$date $from/AIR_REFILL_AC/$date/$date $from/AIR_REFILL_DA/$date/$date $from/AIR_REFILL_MA/$date/$date $from/BUNDLE4U_GPRS/$date/$date $from/BUNDLE4U_VOICE/$date/$date $from/CB_SERV_MAST_VIEW/$date/$date $from/CCN_GPRS_AC/$date/$date $from/CCN_GPRS_DA/$date/$date $from/CCN_GPRS_MA/$date/$date $from/CCN_SMS_AC/$date/$date $from/CCN_SMS_DA/$date/$date $from/CCN_SMS_MA/$date/$date $from/CCN_VOICE_AC/$date/$date $from/CCN_VOICE_DA/$date/$date $from/CCN_VOICE_MA/$date/$date $from/Containers/$date/$date $from/CUG_ACCESS_FEES/$date/$date $from/DMC_DUMP_ALL/$date/$date $from/DMC_DUMP_ALL_unsplitted/$date/$date $from/EWP_FINANCIAL_LOG/$date/$date $from/GGSN/$date/$date $from/HSDP/$date/$date $from/HSDP_tmp/$date/$date $from/MAPS_INV_2G/$date/$date $from/MAPS_INV_3G/$date/$date $from/MAPS_INV_4G/$date/$date $from/MOBILE_MONEY/$date/$date $from/MSC/$date/$date $from/SDP_ACC_ADJ_AC/$date/$date $from/SDP_ACC_ADJ_MA/$date/$date $from/SDP_ADJ_DA/$date/$date $from/SDP_DMP_MA/$date/$date $from/SGSN/$date/$date $from/WBS_PM_RATED_CDRS/$date/$date $from/SDP_DUMP_SUBSCRIBER/$date/$date $from/SDP_DUMP_OFFER/$date/$date $from/RECON/$date/$date $from/DPI/$date/$date $from/AGL_CRM_COUNTRY_MAP/$date/$date $from/AGL_CRM_LGA_MAP/$date/$date $from/AGL_CRM_STATE_MAP/$date/$date $from/CALL_REASON/$date/$date $from/CALL_REASON_MONTHLY/$date/$date $from/MNP_PORTING_BROADCAST/$date/$date $from/MVAS_DND_MSISDN_REP_CDR/$date/$date $from/NEWREG_BIOUPDT_POOL/$date/$date $from/SDP_DMP_DA/$date/$date $from/LBN/$date/$date $from/UDC_DUMP/$date/$date $from/CB_NEWREG_BIOUPDT_POOL_DAILY/$date/$date $from/NEWREG_BIOUPDT_POOL_WEEKLY/$date/$date    "$hdfs/#1/#2" -nt 32 -cvzf -tarPrefix "#1-#2" -rgrm "$from/#1/#2/#2" -groupregex "(AIR_ADJ_DA|AIR_ADJ_MA|AIR_REFILL_AC|AIR_REFILL_DA|AIR_REFILL_MA|BUNDLE4U_GPRS|BUNDLE4U_VOICE|CB_SERV_MAST_VIEW|CCN_GPRS_AC|CCN_GPRS_DA|CCN_GPRS_MA|CCN_SMS_AC|CCN_SMS_DA|CCN_SMS_MA|CCN_VOICE_AC|CCN_VOICE_DA|CCN_VOICE_MA|Containers|CUG_ACCESS_FEES|DMC_DUMP_ALL|DMC_DUMP_ALL_unsplitted|EWP_FINANCIAL_LOG|GGSN|HSDP|HSDP_tmp|MAPS_INV_2G|MAPS_INV_3G|MAPS_INV_4G|MOBILE_MONEY|MSC|SDP_ACC_ADJ_AC|SDP_ACC_ADJ_MA|SDP_ADJ_DA|SDP_DMP_MA|SGSN|WBS_PM_RATED_CDRS|SDP_DUMP_SUBSCRIBER|SDP_DUMP_OFFER|RECON|DPI|AGL_CRM_COUNTRY_MAP|AGL_CRM_LGA_MAP|AGL_CRM_STATE_MAP|CALL_REASON|CALL_REASON_MONTHLY|MNP_PORTING_BROADCAST|MVAS_DND_MSISDN_REP_CDR|NEWREG_BIOUPDT_POOL|SDP_DMP_DA|LBN|UDC_DUMP|CB_NEWREG_BIOUPDT_POOL_DAILY|NEWREG_BIOUPDT_POOL_WEEKLY|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|CIS_CDR|MSC_DAAS|CIS_CDR|MSC_DAAS|CIS_CDR)-*.([0-9]{8})" -out $report_folder -dp 15 -tf 1 -ts 120 -iffl "(AIR_ADJ_DA|AIR_ADJ_MA|AIR_REFILL_AC|AIR_REFILL_DA|AIR_REFILL_MA|BUNDLE4U_GPRS|BUNDLE4U_VOICE|CB_SERV_MAST_VIEW|CCN_GPRS_AC|CCN_GPRS_DA|CCN_GPRS_MA|CCN_SMS_AC|CCN_SMS_DA|CCN_SMS_MA|CCN_VOICE_AC|CCN_VOICE_DA|CCN_VOICE_MA|Containers|CUG_ACCESS_FEES|DMC_DUMP_ALL|DMC_DUMP_ALL_unsplitted|EWP_FINANCIAL_LOG|GGSN|HSDP|HSDP_tmp|MAPS_INV_2G|MAPS_INV_3G|MAPS_INV_4G|MOBILE_MONEY|MSC|SDP_ACC_ADJ_AC|SDP_ACC_ADJ_MA|SDP_ADJ_DA|SDP_DMP_MA|SGSN|WBS_PM_RATED_CDRS|SDP_DUMP_SUBSCRIBER|SDP_DUMP_OFFER|RECON|DPI|AGL_CRM_COUNTRY_MAP|AGL_CRM_LGA_MAP|AGL_CRM_STATE_MAP|CALL_REASON|CALL_REASON_MONTHLY|MNP_PORTING_BROADCAST|MVAS_DND_MSISDN_REP_CDR|NEWREG_BIOUPDT_POOL|SDP_DMP_DA|LBN|UDC_DUMP|CB_NEWREG_BIOUPDT_POOL_DAILY|NEWREG_BIOUPDT_POOL_WEEKLY|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|MSC_DAAS|CIS_CDR|MSC_DAAS|CIS_CDR|MSC_DAAS|CIS_CDR)-*.([0-9]{8})" -nosim -lbl double_taring_ -hdfs "hdfs://ngdaas/" -kktf "/etc/security/keytabs/flare.keytab" -kp "flare@MTN.COM" -toHdfs 2>&1 | tee "$report_folder/double_taring_$mytime.log"
?

?
numberOfFiles=$(tail -n 3 $report_folder/double_taring_$mytime.log)
?
 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 18 --status 1 --hostname "$hostName" --step 2 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "double tar files: $numberOfFiles"
?
### push to kafka here
?

?
 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 18 --status 0 --hostname "$hostName" --step 3 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete mini tar files"
?
### push to kafka here
?

?
# delete files
?
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
?
time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $hdfs/AIR_ADJ_DA/$date $hdfs/AIR_ADJ_MA/$date $hdfs/AIR_REFILL_AC/$date $hdfs/AIR_REFILL_DA/$date $hdfs/AIR_REFILL_MA/$date $hdfs/BUNDLE4U_GPRS/$date $hdfs/BUNDLE4U_VOICE/$date $hdfs/CB_SERV_MAST_VIEW/$date $hdfs/CCN_GPRS_AC/$date $hdfs/CCN_GPRS_DA/$date $hdfs/CCN_GPRS_MA/$date $hdfs/CCN_SMS_AC/$date $hdfs/CCN_SMS_DA/$date $hdfs/CCN_SMS_MA/$date $hdfs/CCN_VOICE_AC/$date $hdfs/CCN_VOICE_DA/$date $hdfs/CCN_VOICE_MA/$date $hdfs/Containers/$date $hdfs/CUG_ACCESS_FEES/$date $hdfs/DMC_DUMP_ALL/$date $hdfs/DMC_DUMP_ALL_unsplitted/$date $hdfs/EWP_FINANCIAL_LOG/$date $hdfs/GGSN/$date $hdfs/HSDP/$date $hdfs/HSDP_tmp/$date $hdfs/MAPS_INV_2G/$date $hdfs/MAPS_INV_3G/$date $hdfs/MAPS_INV_4G/$date $hdfs/MOBILE_MONEY/$date $hdfs/MSC/$date $hdfs/SDP_ACC_ADJ_AC/$date $hdfs/SDP_ACC_ADJ_MA/$date $hdfs/SDP_ADJ_DA/$date $hdfs/SDP_DMP_MA/$date $hdfs/SGSN/$date $hdfs/WBS_PM_RATED_CDRS/$date $hdfs/SDP_DUMP_SUBSCRIBER/$date $hdfs/SDP_DUMP_OFFER/$date $hdfs/RECON/$date $hdfs/DPI/$date $hdfs/AGL_CRM_COUNTRY_MAP/$date $hdfs/AGL_CRM_LGA_MAP/$date $hdfs/AGL_CRM_STATE_MAP/$date $hdfs/CALL_REASON/$date $hdfs/CALL_REASON_MONTHLY/$date $hdfs/MNP_PORTING_BROADCAST/$date $hdfs/MVAS_DND_MSISDN_REP_CDR/$date $hdfs/NEWREG_BIOUPDT_POOL/$date $hdfs/SDP_DMP_DA/$date $hdfs/LBN/$date $hdfs/UDC_DUMP/$date $hdfs/CB_NEWREG_BIOUPDT_POOL_DAILY/$dat $hdfs/NEWREG_BIOUPDT_POOL_WEEKLY/$dat -nt 32 -vad -rmp $from/#fn/$date/$date -out $report_folder -dp 15 -tf 1 -nosim -lbl double_taring_delete_ -of -od -fnfp 2 -hdfs "hdfs://ngdaas/" -kktf "/etc/security/keytabs/flare.keytab" -kp "flare@MTN.COM" -fromHdfs
?

?
 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 18 --status 1 --hostname "$hostName" --step 3 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete mini tar files"
?
### push to kafka here
?

?
