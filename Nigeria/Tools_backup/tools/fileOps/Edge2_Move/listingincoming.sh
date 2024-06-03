#!/bin/bash

PIDFILE=/home/daasuser/PIDFiles/MoveFromlocal_listingincoming_crontab.pid
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

mkdir -p /mnt/beegfs_bsl/production/logs/${currDate}

java -Xmx1g -Xms1g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /data/data_lz/beegfs/live -out /nas/share05/production/listingincoming/report/${currDate}/ -od -of -tf 1 -lbl incoming  -op list -nt 10 -dp 15  -effl "^.*\.(_COPYING_|tmp|TMP)$" -effl "^\..*" 2>&1 | tee "/mnt/beegfs_bsl/production/logs/${currDate}/istingincoming_${currDate}_$mytime.log"

mv /nas/share05/production/listingincoming/report/${currDate}/latest/* /nas/share05/production/listingincoming/report/${currDate}/
