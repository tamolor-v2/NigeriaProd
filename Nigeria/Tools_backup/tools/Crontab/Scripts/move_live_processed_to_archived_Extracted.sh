PIDFILE=/home/daasuser/PIDFiles/move_live_processed_to_archived_PM_RATED.pid
check_pid_file(){
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
    #exit 1
return 2
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
return 2
      #exit 1
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
 
main_process(){
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
currDate=$(date +"%Y%m%d")
removeDir=$(date -d '-60 day' '+%Y%m%d')
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
#move All_Processed_feeds
mkdir -p /mnt/beegfs/tools/Crontab/logs/move/${currDate}
rm -r /mnt/beegfs/tools/Crontab/logs/move/${removeDir}
mkdir -p  ${generalLogs}/${currDate}
currDate=$1
#move PM_RATED feed
#-----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/WBS_PM_RATED_CDRS/processed_2/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(WBS_PM_RATED_CDRS)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_PM_RATED_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_PM_RATED_LOG_$mytime.log"

#move AGL_CRM_COUNTRY_MAP
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_CRM_COUNTRY_MAP/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(AGL_CRM_COUNTRY_MAP)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_CRM_COUNTRY_MAP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_COUNTRY_MAP_LOG_$mytime.log"


#move CALL_REASON
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CALL_REASON/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CALL_REASON)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CALL_REASON_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_LOG_$mytime.log"


#move NEWREG_BIOUPDT_POOL_DAILY
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 35 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move NEWREG_BIOUPDT_POOL_DAILY\""


time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/NEWREG_BIOUPDT_POOL_DAILY/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CB_NEWREG_BIOUPDT_POOL_DAILY).*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 35 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move NEWREG_BIOUPDT_POOL_DAILY: $numberOfFiles\""


#move AGL_CRM_LGA_MAP
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_CRM_LGA_MAP/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(AGL_CRM_LGA_MAP)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_CRM_LGA_MAP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_LGA_MAP_LOG_$mytime.log"




#move AGL_CRM_STATE_MAP
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_CRM_STATE_MAP/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(AGL_CRM_STATE_MAP)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_CRM_STATE_MAP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_STATE_MAP_LOG_$mytime.log"





#move CUG_ACCESS_FEES feed
#-----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CUG_ACCESS_FEES/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CUG_ACCESS_FEES)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CUG_ACCESS_FEES_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CUG_ACCESS_LOG_$mytime.log"




#move CB_SERV_MAST_VIEW feed
#-----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CB_SERV_MAST_VIEW/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CB_SERV_MAST_VIEW)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CB_SERV_MAST_VIEW_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CB_SERV_MAST_VIEW_LOG_$mytime.log"




#move NEWREG_BIOUPDT_POOL
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/NEWREG_BIOUPDT_POOL/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(NEWREG_BIOUPDT_POOL)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_NEWREG_BIOUPDT_POOL__ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_LOG_$mytime.log"






#move MNP_PORTING_BROADCAST
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/MNP_PORTING_BROADCAST/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(MNP_PORTING_BROADCAST).*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_MNP_PORTING_BROADCAST_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MNP_PORTING_BROADCAST_LOG_$mytime.log"











#echo "Job: move_live_processed_to_archived. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

rm $PIDFILE
#List incoming files in live cluster
#--------------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/  -out /mnt/beegfs/tmp/waddah/list -od -tf 10 -nt 32 -dp 15 -of -op list -lbl listIncomingLive 2>&1 | tee "/mnt/beegfs/tmp/waddah/list/list_live_Incoming$(mytime).log"

echo "Job: move_live_processed_to_archived. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
}

for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 5m"
     emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
     ssh edge01002 " echo -e 'CronJob move processed to archived already running ....' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<Move_Processed_To_Archived already running, attempt $i>' '$emailReceiver' "
     sleep 5m
else
     main_process
     break
fi
done

