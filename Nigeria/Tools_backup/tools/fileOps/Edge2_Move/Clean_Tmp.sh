#!/bin/bash

PIDFILE=/home/daasuser/PIDFiles/CleanUp_Tmp_crontab.pid
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


from=$(date -d '-1000 day' '+%Y/%m/%d-00:00:00')
to=$(date --date '-1 hours' +"%Y/%m/%d-%H:%M:%S")
today=$(%Y%m%d)
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
report_path=/mnt/beegfs_bsl/Clean_Move/CleanUp_Tmp/report


time java -Xmx200m -Xms200m -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT.jar -in /data/data_lz/beegfs/live/ -nt 32 -out $report_path/${today} -dp 15 -tf 1 -op delete -nosim -lbl delete  -fbd $from $to -of -od -iffl "^.*\.tmp$"
