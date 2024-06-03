#!/bin/bash 

yest=$(date -d '-1 day' '+%Y%m%d')
PIDFILE=/home/daasuser/PIDFiles/line_count_archived.pid
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
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
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
echo "Job: line_count_archived_part3. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
#Send start linecount email 
echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Line_Count_Archived_part3 Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
mkdir -p /mnt/beegfs/tools/Crontab/logs/linecount/${yest}
rm -r /mnt/beegfs/tools/Crontab/logs/linecount/${removeDir}
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/archived/HSDP/$yest /mnt/beegfs/production/archived/WBS_PM_RATED_CDRS/$yest /mnt/beegfs/production/archived/CB_SERV_MAST_VIEW/$yest /mnt/beegfs/production/archived/CUG_ACCESS_FEES/$yest /mnt/beegfs/production/archived/AGL_CRM_COUNTRY_MAP/$yest /mnt/beegfs/production/archived/AGL_CRM_LGA_MAP/$yest /mnt/beegfs/production/archived/AGL_CRM_STATE_MAP/$yest /mnt/beegfs/production/archived/CALL_REASON/$yest  /mnt/beegfs/production/archived/CALL_REASON_MONTHLY/$yest /mnt/beegfs/production/archived/MNP_PORTING_BROADCAST/$yest /mnt/beegfs/production/archived/NEWREG_BIOUPDT_POOL/$yest  -out /mnt/beegfs/tools/Crontab/logs/linecount/${yest} -od -tf 1 -nt 40 -dp 15 -of -op lineCount -lbl lineCount$yest -lcwi /mnt/beegfs/tools/Crontab/logs/listIncoming_LineCount -fnfp 5 -rg "$yest" 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/linecount/${yest}/linecount_archived_$yest_$(mytime).log"

#Send finish linecount email
echo -e "CronJob \"line_count_archived_withValidation.sh\" Finished LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Line_Count_Archived Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"

echo "Job: line_count_archived. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
echo "Job: Validation_Tool. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#Send Start Validation Email
echo -e "CronJob \"line_count_archived_withValidation.sh\" Start Validation at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Validation_Tool Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"

find /mnt/beegfs/tools/Crontab/logs/linecount/${yest}/latest/fileinfo*.gz  -type f -exec gunzip {} \;
hdfs dfs -mkdir -p /FlareData/output_8/files_filesopps_summary/tbl_dt\=$yest
hadoop fs -put /mnt/beegfs/tools/Crontab/logs/linecount/${yest}/latest/*.txt /FlareData/output_8/files_filesopps_summary/tbl_dt\=$yest
 kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
hive -e 'msck repair table flare_8.files_filesopps_summary;'
###bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_new.sh  ${yest} all D 2>&1 | tee  /home/daasuser/ValidationTool_Load/logs/Directory_$(date +%Y%m%d_%s).log
bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_part3.sh $yest all VRSDF  2>&1 | tee /home/daasuser/ValidationTool_Load/logs/ValidationTool_part3_$(date +%Y%m%d_%s).txt

echo "Job: Validation_Tool_part3. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
echo "Job: Generate_HTML_Report_part3. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs/tools/validationCheck/script/validationCheck.sh ## generate html code
echo "Job: Generate_HTML_Report_part3. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#Send Finish validation Email
echo -e "CronJob \"line_count_archived_withValidation_part3.sh\" Finished Validation at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Validation_Tool_part3 Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"

bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_part3.sh $yest all PH  2>&1 | tee /home/daasuser/ValidationTool_Load/logs/ValidationTool_PH_part3_$(date +%Y%m%d_%s).txt
rm $PIDFILE
}

for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 2m"
     echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived already running for $yest, attempt $i>" "$emailReceiver"
     sleep 20m
else
     main_process
     break
fi
done
