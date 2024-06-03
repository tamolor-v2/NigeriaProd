#!/bin/bash 
yest=$(date -d '-1 day' '+%Y%m%d')
yest=$1
PIDFILE=/home/daasuser/PIDFiles/line_count_archived_V5_1.pid
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email_s.dat)
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
  return 2
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
   #   exit 1
    return 2
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
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
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLineCountLogs=/mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/general_logs

echo "Job: line_count_archived_V5_1. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLineCountLogs}/${currDate}/genral_logs_${currDate}.log
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email_s.dat)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 3 --status 0 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/${yest}\" --brokers $kafkaHostList --topic $TopicName --general_message \"line count step\""

#Send start linecount email 
ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation_V5_1.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Line_Count_Archived_V5_1 Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

mkdir -p /mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/${yest}
rm -r /mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/${removeDir}
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/archived/AVAYA_CLID/$yest /mnt/beegfs/production/archived/AVAYA_IVR/$yest /mnt/beegfs/production/archived/TAPIN_VOICE/$yest /mnt/beegfs/production/archived/TAPIN_GPRS/$yest /mnt/beegfs/production/archived/TAPOUT_VOICE/$yest /mnt/beegfs/production/archived/TAPOUT_GPRS/$yest /mnt/beegfs/production/archived/CS5_SDP_PAM_ALL/$yest /mnt/beegfs/production/archived/SDP_TBL_MA/$yest /mnt/beegfs/production/archived/SDP_NBB_MA/$yest /mnt/beegfs/production/archived/SDP_VVE_AC/$yest /mnt/beegfs/production/archived/SDP_VVE_MA/$yest /mnt/beegfs/production/archived/SDP_VVE_CLR_DA/$yest -out /mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/${yest} -od -tf 1 -nt 40 -dp 15 -of -op lineCount -lbl lineCount_V5_1_$yest -fnfp 5 -rg "$yest" 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/${yest}/linecount_archived_$yest_$(mytime).log"

#Send finish linecount email
ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation_V5_1.sh\" Finished LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Line_Count_Archived_V5_1 Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo "Job: line_count_archived. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLineCountLogs}/${currDate}/genral_logs_${currDate}.log
echo "Job: Validation_Tool_V5_1. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLineCountLogs}/${currDate}/genral_logs_${currDate}.log
#Send Start Validation Email
ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation_V5_1.sh\" Start Validation _V5_1 at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Validation_Tool_V5_1 Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

find /mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/${yest}/latest/fileinfo*.gz  -type f -exec gunzip {} \;
hdfs dfs -mkdir -p /FlareData/audit/files_filesopps_summar_pre/tbl_dt\=$yest
hadoop fs -put /mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/${yest}/latest/*.txt /FlareData/audit/files_filesopps_summary_pre/tbl_dt\=$yest
 kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
hive -e 'msck repair table audit.files_filesopps_summary_pre ;'
###bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_new.sh  ${yest} all D 2>&1 | tee  /home/daasuser/ValidationTool_Load/logs/Directory_$(date +%Y%m%d_%s).log
##bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_new.sh $yest all VRSDF  2>&1 | tee /home/daasuser/ValidationTool_Load/logs/ValidationTool_$(date +%Y%m%d_%s).txt
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/${yest}/linecount_archived_$yest_$(mytime).log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 3 --status 1 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/lineCount/${yest}\" --brokers $kafkaHostList --topic $TopicName --general_message \"line count step: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 4 --status 0 --hostname \"$hostName\" --step 2 --log_directory \" /mnt/beegfs/tools/ValidationTool_Load_V5_1/logs\" --brokers $kafkaHostList --topic $TopicName --general_message \"validation tool {V5}  step\""
bash /mnt/beegfs/tools/ValidationTool_Load_V5_1/bin/runValidationAuto_V5_1.sh $yest   2>&1 | tee /mnt/beegfs/tools/ValidationTool_Load_V5_1/logs/ValidationTool_V5_1_$(date +%Y%m%d_%s).txt

echo "Job: Validation_Tool_V5_1. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLineCountLogs}/${currDate}/genral_logs_${currDate}.log
echo "Job: Generate_HTML_Report. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLineCountLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs/tools/validationCheck/script/validationCheck.sh ## generate html code
echo "Job: Generate_HTML_Report. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLineCountLogs}/${currDate}/genral_logs_${currDate}.log
#Send Finish validation Email
ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation_V5_1.sh\" Finished Validation _V5_1 at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Validation_Tool_V5_1 Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 4 --status 1 --hostname \"$hostName\" --step 2 --log_directory \"/mnt/beegfs/tools/ValidationTool_Load_V5_1/logs\" --brokers $kafkaHostList --topic $TopicName --general_message \"validation tool V5_1 step\""


ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 4 --status 0 --hostname \"$hostName\" --step 3 --log_directory \/mnt/beegfs/tools/ValidationTool_Load_V5_1/logs/\" --brokers $kafkaHostList --topic $TopicName --general_message \"validation tool V5_1 part 2 step\""

bash /mnt/beegfs/tools/ValidationTool_Load_V5_1/bin/runValidationAuto_V5_1.sh $yest   2>&1 | tee /mnt/beegfs/tools/ValidationTool_Load_V5_1/logs/ValidationTool_PH_$(date +%Y%m%d_%s).txt
echo "Job: Generate_Trending_Report. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLineCountLogs}/${currDate}/genral_logs_${currDate}.log

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 4 --status 1 --hostname \"$hostName\" --step 3 --log_directory \"/mnt/beegfs/tools/ValidationTool_Load_V5_1/logs/\" --brokers $kafkaHostList --topic $TopicName --general_message \"validation tool _V5_1 part 2 step\""
rm $PIDFILE
}

for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 2m"
     ssh edge01002 " echo -e 'CronJob \"line_count_archived_withValidation_V5_1.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<Line_Count_Archived_V5_1 already running for $yest, attempt $i>' '$emailReceiver' "
     sleep 20m
else
     main_process
     break
fi
done


