#!/bin/bash

PIDFILE=/home/daasuser/PIDFiles/MoveFromlocal_crontab_MOBILE_MONEY.pid
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
    exit 1
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
      exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo "Could not create PID file"
    exit 1
  fi
fi

currDate=$(date +"%Y%m%d")
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /data/data_lz/beegfs/live/MOBILE_MONEY/incoming/ -out /mnt/beegfs_bsl/production/movefromlocaltodfs/report/ -mv /mnt/beegfs_bsl/live/MOBILE_MONEY/incoming/ -od -of -tf 1 -op lineCount -nt 32 -dp 15  -rcm -re -nosim -effl "^.*\.tmp$" 2>&1 | tee "/mnt/beegfs_bsl/production/logs/movefromlocaltodfs_${currDate}_$mytime.log"



