#!/bin/bash

PIDFILE=/home/daasuser/PIDFiles/move_TMP_DMP_DUMP_ALL_crontab.pid
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


from=$(date -d '-10000 day' '+%Y/%m/%d-00:00:00')
to=$(date -d '-1 day' '+%Y/%m/%d-23:59:59')
today=$(%Y%m%d)
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
report_path=/mnt/beegfs_bsl/Clean_Move/CleanUp_Tmp/report

mkdir -p  /mnt/beegfs_bsl/Clean_Move/CleanUp_Tmp/logs/${today}

time java -Xmx1g -Xms1g -Dlog4j.configurationFile=/nas/share05/FlareProd/Run/Load/config/fdi_archive_log4j2.xml -jar /nas/share05/FlareProd/Run/Load/libs/FileOps-1.0.1.191128.jar -in /data/data_lz/FdiBase/verified/TMP_DMP_DUMP_ALL -mv /nas/share05/archived/TMP_DMP_DUMP_ALL -nt 1 -out $report_path/${today} -dp 15 -tf 1 -op list -nosim -lbl list -of -od -rcm -re 2>&1 | tee "/mnt/beegfs_bsl/Clean_Move/CleanUp_Tmp/logs/${today}move_TMP_DMP_DUMP_ALL_${currDate}_$mytime.log"
