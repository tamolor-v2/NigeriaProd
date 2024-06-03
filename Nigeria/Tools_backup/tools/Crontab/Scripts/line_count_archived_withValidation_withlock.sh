#!/bin/bash 

yest=$(date -d '-1 day' '+%Y%m%d')
PIDFILE=/home/daasuser/PIDFiles/line_count_archived1.pid
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
check_pid_file(){
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
#    echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived already running for $yest>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
 #   exit 1
  return 2
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
  #    echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived for $yest Could not create PID file>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
      echo "Could not create PID file"
   #   exit 1
    return 2
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
   # echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived for $yest Could not create PID file>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
    echo "Could not create PID file"
    #exit 1
  return 2
  fi
fi
}
removeDir=$(date -d '-60 day' '+%Y%m%d')
mytime() {
date +"%Y-%m-%d_%H-%M-%S"
}

main_process(){
currDate=$(date +"%Y%m%d")
common_path="/mnt/beegfs/production/archived"
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
echo "Job: line_count_archived. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 28 --status 0 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/linecount/${yest}\" --brokers $kafkaHostList --topic $TopicName --general_message \"line count step\""
#Send start linecount email 
ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Line_Count_Archived Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
mkdir -p /mnt/beegfs/tools/Crontab/logs/linecount/${yest}
rm -r /mnt/beegfs/tools/Crontab/logs/linecount/${removeDir}
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in $common_path/AIR_ADJ_DA/$yest $common_path/AIR_REFILL_MA/$yest $common_path/CCN_GPRS_DA/$yest $common_path/CCN_SMS_MA/$yest $common_path/GGSN/$yest $common_path/MSC/$yest $common_path/SDP_ADJ_DA/$yest $common_path/AIR_ADJ_MA/$yest $common_path/BUNDLE4U_GPRS/$yest $common_path/CCN_GPRS_MA/$yest $common_path/CCN_VOICE_AC/$yest $common_path/HSDP/$yest $common_path/SDP_DMP_MA/$yest $common_path/AIR_REFILL_AC/$yest $common_path/BUNDLE4U_VOICE/$yest $common_path/CCN_SMS_AC/$yest $common_path/CCN_VOICE_DA/$yest $common_path/DMC_DUMP_ALL/$yest $common_path/MAPS_INV_2G/$yest $common_path/SDP_ACC_ADJ_AC/$yest $common_path/SGSN/$yest $common_path/AIR_REFILL_DA/$yest $common_path/CCN_GPRS_AC/$yest $common_path/CCN_SMS_DA/$yest $common_path/CCN_VOICE_MA/$yest $common_path/EWP_FINANCIAL_LOG/$yest $common_path/MAPS_INV_4G/$yest $common_path/SDP_ACC_ADJ_MA/$yest $common_path/WBS_PM_RATED_CDRS/$yest $common_path/SDP_DMP_AC/$yest $common_path/SDP_DMP_DA/$yest $common_path/SDP_DUMP_OFFER/$yest $common_path/SDP_DUMP_SUBSCRIBER/$yest $common_path/RECON/$yest $common_path/CB_SERV_MAST_VIEW/$yest $common_path/CUG_ACCESS_FEES/$yest $common_path/SDP_DMP_MA/$yest $common_path/MAPS_INV_3G/$yest $common_path/MOBILE_MONEY/$yest $common_path/AGL_CRM_COUNTRY_MAP/$yest $common_path/AGL_CRM_LGA_MAP/$yest $common_path/AGL_CRM_STATE_MAP/$yest $common_path/CALL_REASON/$yest $common_path/CALL_REASON_MONTHLY/$yest $common_path/MNP_PORTING_BROADCAST/$yest $common_path/MVAS_DND_MSISDN_REP_CDR/$yest $common_path/NEWREG_BIOUPDT_POOL/$yest $common_path/UDC_DUMP/$yest $common_path/LBN/$yest $common_path/CB_NEWREG_BIOUPDT_POOL_DAILY/$yest $common_path/NEWREG_BIOUPDT_POOL_WEEKLY/$yest $common_path/CIS_CDR/$yest $common_path/MSC_DaaS/$yest $common_path/AGL_BILL_ANALYZER_MONTHLY/$yest $common_path/MSO_PROCESS_AVG_TIME/$yest $common_path/USSD_LICENSE_USAGE/$yest $common_path/USSD_TRAFFIC_SUCCESS_RATE/$yest $common_path/USSD_ERROR_CODE_BREAKDOWN/$yest $common_path/SMSC_ERROR_BREAKDOWN_PER_ACCOUNT/$yest $common_path/SMSC_TOTAL_SUBMIT_SUCCESS_RATE/$yest $common_path/SMSC_TOTAL_DELIVERY_SUCCESS_RATE/$yest $common_path/SMSC_LICENSE/$yest $common_path/SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE/$yest $common_path/XML_FILES/$yest $common_path/MyMTNApp/$yest $common_path/CGW_API/$yest $common_path/IB_API/$yest $common_path/SAG_API/$yest $common_path/SCREAM_SERVICE/$yest -out /mnt/beegfs/tools/Crontab/logs/linecount/${yest} -od -tf 1 -nt 40 -dp 15 -of -op lineCount -lbl lineCount$yest -lcwi /mnt/beegfs/tools/Crontab/logs/listIncoming_LineCount -fnfp 5 -rg "$yest" 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/linecount/${yest}/linecount_archived_$yest_$(mytime).log"
#Send finish linecount email
ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation.sh\" Finished LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Line_Count_Archived Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo "Job: line_count_archived. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
echo "Job: Validation_Tool. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#Send Start Validation Email
ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation.sh\" Start Validation at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Validation_Tool Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

find /mnt/beegfs/tools/Crontab/logs/linecount/${yest}/latest/fileinfo*.gz  -type f -exec gunzip {} \;
hdfs dfs -mkdir -p /FlareData/output_8/files_filesopps_summary/tbl_dt\=$yest
hadoop fs -put /mnt/beegfs/tools/Crontab/logs/linecount/${yest}/latest/*.txt /FlareData/output_8/files_filesopps_summary/tbl_dt\=$yest
 kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
hive -e 'msck repair table flare_8.files_filesopps_summary;'
###bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_new.sh  ${yest} all D 2>&1 | tee  /home/daasuser/ValidationTool_Load/logs/Directory_$(date +%Y%m%d_%s).log
##bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_new.sh $yest all VRSDF  2>&1 | tee /home/daasuser/ValidationTool_Load/logs/ValidationTool_$(date +%Y%m%d_%s).txt
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/linecount/${yest}/linecount_archived_$yest_$(mytime).log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 28 --status 1 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/linecount/${yest}\" --brokers $kafkaHostList --topic $TopicName --general_message \"line count step: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 28 --status 0 --hostname \"$hostName\" --step 2 --log_directory \" /mnt/beegfs/tools/ValidationTool_Load/logs\" --brokers $kafkaHostList --topic $TopicName --general_message \"validation tool  step\""
bash /mnt/beegfs/tools/ValidationTool_Load/bin/InvokeValidation_new.sh $yest all VRSDF  2>&1 | tee /mnt/beegfs/tools/ValidationTool_Load/logs/ValidationTool_$(date +%Y%m%d_%s).txt

echo "Job: Validation_Tool. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
echo "Job: Generate_HTML_Report. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs/tools/validationCheck/script/validationCheck.sh ## generate html code
echo "Job: Generate_HTML_Report. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#Send Finish validation Email
ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation.sh\" Finished Validation at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Validation_Tool Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 28 --status 1 --hostname \"$hostName\" --step 2 --log_directory \"/mnt/beegfs/tools/ValidationTool_Load/logs\" --brokers $kafkaHostList --topic $TopicName --general_message \"validation tool  step\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 28 --status 0 --hostname \"$hostName\" --step 3 --brokers $kafkaHostList --topic $TopicName --general_message \"trending report step\""

echo "Job: Generate_Trending_Report. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
cd /mnt/beegfs/tools/TrendingReport/bin
bash /mnt/beegfs/tools/TrendingReport/bin/TrendingScript.sh

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 28 --status 1 --hostname \"$hostName\" --step 3 --brokers $kafkaHostList --topic $TopicName --general_message \"trending report step\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 28 --status 0 --hostname \"$hostName\" --step 4 --log_directory \/mnt/beegfs/tools/ValidationTool_Load/logs/\" --brokers $kafkaHostList --topic $TopicName --general_message \"validation tool part 2 step\""

bash /mnt/beegfs/tools/ValidationTool_Load/bin/InvokeValidation_new.sh $yest all PH  2>&1 | tee /mnt/beegfs/tools/ValidationTool_Load/logs/ValidationTool_PH_$(date +%Y%m%d_%s).txt
echo "Job: Generate_Trending_Report. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 28 --status 1 --hostname \"$hostName\" --step 4 --log_directory \"/mnt/beegfs/tools/ValidationTool_Load/logs/\" --brokers $kafkaHostList --topic $TopicName --general_message \"validation tool part 2 step\""
rm $PIDFILE
}

for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 2m"
     ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<Line_Count_Archived already running for $yest, attempt $i>' '$emailReceiver' "
     sleep 20m
else
     main_process
     break
fi
done
