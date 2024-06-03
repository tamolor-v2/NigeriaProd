#!/bin/bash 

yest=$(date -d '-1 day' '+%Y%m%d')
PIDFILE=/home/daasuser/PIDFiles/line_count_archived.pid
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
    echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived already running for $yest>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
    exit 1
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived for $yest Could not create PID file>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
      echo "Could not create PID file"
      exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived for $yest Could not create PID file>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
    echo "Could not create PID file"
    exit 1
  fi
fi
removeDir=$(date -d '-60 day' '+%Y%m%d')
mytime() {
date +"%Y-%m-%d_%H-%M-%S"
}

#Send start linecount email 
echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Line_Count_Archived Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
mkdir -p /mnt/beegfs/tools/Crontab/logs/linecount/${yest}
rm -r /mnt/beegfs/tools/Crontab/logs/linecount/${removeDir}
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/archived/AIR_ADJ_DA/$yest /mnt/beegfs/production/archived/AIR_REFILL_MA/$yest /mnt/beegfs/production/archived/CCN_GPRS_DA/$yest /mnt/beegfs/production/archived/CCN_SMS_MA/$yest /mnt/beegfs/production/archived/GGSN/$yest /mnt/beegfs/production/archived/MSC/$yest /mnt/beegfs/production/archived/SDP_ADJ_DA/$yest /mnt/beegfs/production/archived/AIR_ADJ_MA/$yest /mnt/beegfs/production/archived/BUNDLE4U_GPRS/$yest /mnt/beegfs/production/archived/CCN_GPRS_MA/$yest /mnt/beegfs/production/archived/CCN_VOICE_AC/$yest /mnt/beegfs/production/archived/HSDP/$yest /mnt/beegfs/production/archived/SDP_DMP_MA/$yest /mnt/beegfs/production/archived/AIR_REFILL_AC/$yest /mnt/beegfs/production/archived/BUNDLE4U_VOICE/$yest /mnt/beegfs/production/archived/CCN_SMS_AC/$yest /mnt/beegfs/production/archived/CCN_VOICE_DA/$yest /mnt/beegfs/production/archived/DMC_DUMP_ALL/$yest /mnt/beegfs/production/archived/MAPS_INV_2G/$yest /mnt/beegfs/production/archived/SDP_ACC_ADJ_AC/$yest /mnt/beegfs/production/archived/SGSN/$yest /mnt/beegfs/production/archived/AIR_REFILL_DA/$yest /mnt/beegfs/production/archived/CCN_GPRS_AC/$yest /mnt/beegfs/production/archived/CCN_SMS_DA/$yest /mnt/beegfs/production/archived/CCN_VOICE_MA/$yest /mnt/beegfs/production/archived/EWP_FINANCIAL_LOG/$yest /mnt/beegfs/production/archived/MAPS_INV_4G/$yest /mnt/beegfs/production/archived/SDP_ACC_ADJ_MA/$yest /mnt/beegfs/production/archived/WBS_PM_RATED_CDRS/$yest /mnt/beegfs/production/archived/SDP_DMP_AC/$yest  /mnt/beegfs/production/archived/SDP_DMP_DA/$yest /mnt/beegfs/production/archived/SDP_DUMP_OFFER/$yest /mnt/beegfs/production/archived/SDP_DUMP_SUBSCRIBER/$yest /mnt/beegfs/production/archived/RECON/$yest /mnt/beegfs/production/archived/CB_SERV_MAST_VIEW/$yest /mnt/beegfs/production/archived/CUG_ACCESS_FEES/$yest  /mnt/beegfs/production/archived/SDP_DMP_MA/$yest  /mnt/beegfs/production/archived/MAPS_INV_3G/$yest  /mnt/beegfs/production/archived/MOBILE_MONEY/$yest  /mnt/beegfs/production/archived/AGL_CRM_COUNTRY_MAP/$yest /mnt/beegfs/production/archived/AGL_CRM_LGA_MAP/$yest /mnt/beegfs/production/archived/AGL_CRM_STATE_MAP/$yest /mnt/beegfs/production/archived/CALL_REASON/$yest  /mnt/beegfs/production/archived/CALL_REASON_MONTHLY/$yest /mnt/beegfs/production/archived/MNP_PORTING_BROADCAST/$yest /mnt/beegfs/production/archived/MVAS_DND_MSISDN_REP_CDR/$yest /mnt/beegfs/production/archived/NEWREG_BIOUPDT_POOL/$yest -out /mnt/beegfs/tools/Crontab/logs/linecount/${yest} -od -tf 1 -nt 32 -dp 15 -of -op lineCount -lbl lineCount$yest -lcwi /mnt/beegfs/tools/Crontab/logs/listIncoming_LineCount -fnfp 5 -rg "$yest" 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/linecount/${yest}/linecount_archived_$yest_$(mytime).log"

#Send finish linecount email
echo -e "CronJob \"line_count_archived_withValidation.sh\" Finished LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Line_Count_Archived Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"

#Send Start Validation Email
echo -e "CronJob \"line_count_archived_withValidation.sh\" Start Validation at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Validation_Tool Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com,  wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"

find /mnt/beegfs/tools/Crontab/logs/linecount/${yest}/latest/fileinfo*.gz  -type f -exec gunzip {} \;
hdfs dfs -mkdir -p /FlareData/output_8/files_filesopps_summary/tbl_dt\=$yest
hadoop fs -put /mnt/beegfs/tools/Crontab/logs/linecount/${yest}/latest/*.txt /FlareData/output_8/files_filesopps_summary/tbl_dt\=$yest
 kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
hive -e 'msck repair table flare_8.files_filesopps_summary;'
###bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_new.sh  ${yest} all D 2>&1 | tee  /home/daasuser/ValidationTool_Load/logs/Directory_$(date +%Y%m%d_%s).log
bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_new.sh $yest all VRSDF  2>&1 | tee /home/daasuser/ValidationTool_Load/logs/ValidationTool_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/validationCheck/script/validationCheck.sh ## generate html code

#Send Finish validation Email
echo -e "CronJob \"line_count_archived_withValidation.sh\" Finished Validation at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Validation_Tool Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"

cd mnt/beegfs/tools/TrendingReport/bin
bash /mnt/beegfs/tools/TrendingReport/bin/TrendingScript.sh

rm $PIDFILE