PIDFILE=/home/daasuser/PIDFiles/move_live_processed_to_archived_v5.pid

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
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}\" --brokers $kafkaHostList --topic $TopicName --general_message \"move all feeds\""


#move NEWREG_BIOUPDT_POOL_WEEKLY
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")


#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/NEWREG_BIOUPDT_POOL_WEEKLY/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(NEWREG_BIOUPDT_POOL_WEEKLY).*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_NEWREG_BIOUPDT_POOL_WEEKLY__ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_WEEKLY_LOG_$mytime.log"

#add new feeds
rm $PIDFILE

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
     ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status -1 --hostname \"$hostName\" --step 1 --brokers $kafkaHostList --topic $TopicName --error_message \"Move_Processed_To_Archived already running, attempt $i\""
     sleep 5m
else
     main_process
     break
fi
done
