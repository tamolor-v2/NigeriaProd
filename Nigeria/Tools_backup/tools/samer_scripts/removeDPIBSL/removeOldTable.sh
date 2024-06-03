PIDFILE=/home/daasuser/PIDFiles/deletDPIBSLOldTables.pid
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
removeOldPartition(){
echo $1
}
main_process(){
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
currDate=$(date +"%Y%m%d")
date_5Days=$(date +%Y%m%d -d "$DATE -6 day")
removeOldPartition $date_5Days
}

for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 5m"
    # emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
     #echo -e "CronJob \"move processed to archived\" already running at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Move_Processed_To_Archived already running, attempt $i>" "$emailReceiver"
     sleep 5m
else
     main_process
     break
fi
done
