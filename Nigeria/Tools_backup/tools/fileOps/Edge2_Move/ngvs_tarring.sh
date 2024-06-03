#!/bin/bash

PIDFILE=/home/daasuser/PIDFiles/NGVS_CDR_CTRL_crontab_lz.pid
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


time java -Xmx2g -Xms2g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /nas/share05/FlareProd/Run/Load/libs/FileOps-1.0.1.191128.jar -outarchive "/nas/share05/archived/NGVS_CDR_CTRL/#1/#2" -in "/data/data_lz/beegfs/live/NGVS_CDR_CTRL/incoming" -effl "^(^[.].*|.*\.(_COPYING_|tmp|TMP|DS_Store))$" -iffl "VSCDR.*([0-9]{8})-([0-9]{2})*.json.CTRLFile" -tarPrefix "NGVS_CDR_CTRL-#1-#2" -nosim -groupregex "VSCDR.*([0-9]{8})-([0-9]{2})*.json.CTRLFile" -cvzf -ts 80 -rgrm "/data/data_lz/beegfs/live/" -vd -lbl "NGVS_CDR_CTRL" -nt 5 -dp 15 -od -of -out /mnt/beegfs_bsl/production/NGVS_CDR_CTRL/report_new/minitarring/${currDate}
