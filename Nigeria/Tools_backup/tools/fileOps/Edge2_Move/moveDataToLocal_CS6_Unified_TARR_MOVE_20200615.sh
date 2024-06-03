





PIDFILE=/home/daasuser/PIDFiles/MoveFromlocal_CS6_mv_tarr.pid
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


bash /nas/share05/tools/minitarringLists/mini_CS6.sh "CCN_CDR_GPRS"  "^([0-9]{8})([0-9]{6}).*_(CCN_).*$" "/data/data_lz/beegfs/live/CS6_Unified/CCN_CDR_GPRS/incoming"  "#1#2"


bash /nas/share05/tools/minitarringLists/mini_CS6.sh "CCN_CDR_SMS"  "^([0-9]{8})([0-9]{6}).*_(CCN_SMS).*$" "/data/data_lz/beegfs/live/CS6_Unified/CCN_CDR_SMS/incoming"  "#1#2"


bash /nas/share05/tools/minitarringLists/mini_CS6.sh "CCN_CDR_VOICE"  "^([0-9]{8})([0-9]{6}).*_(CCN_VOICE).*$" "/data/data_lz/beegfs/live/CS6_Unified/CCN_CDR_VOICE/incoming"  "#1#2"
