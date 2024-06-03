#!/bin/bash
PIDFILE=/home/daasuser/PIDFiles/extractIncremental_D_DIRECT_EVENT_YYYYMM_LIVE.pid
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
cd /nas/share05/tools/ExtractTools/D_DIRECT_EVENT_YYYYMM_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/nas/share05/tools/ExtractTools/D_DIRECT_EVENT_YYYYMM_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
filename="D_DIRECT_EVENT_YYYYMM_LIVE_"$yyyymmdd".csv"
load_date=`date '+%Y-%m-%d %H:%M:%S'`
month_to_run=$(date -d "last month" '+%Y%m')
#month_to_run=201905
array=($(/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_CDR/thisPWD#123@10.1.110.58:1522/agi_p1
set term off
set termout off
set echo off
set underline off
set colsep ','
set pages 40000
SET LONG 50000;
set trimout on
set trimspool on
set feedback off
set heading off
set headsep off
SET LINESIZE 30000
set LONGCHUNKSIZE 30000
set pagesize 0
set wrap off 
select distinct dw_date_key from TT_MSO_1.D_DIRECT_EVENT_$month_to_run;
EOF))
for i in "${array[@]}"
do
   echo "start for tbl_dt : $i"
   bash /nas/share05/tools/ExtractTools/D_DIRECT_EVENT_YYYYMM_LIVE/templete.sh $i
   echo "end for tbl_dt : $i"
   
done

hive -e "msck repair table flare_8.D_DIRECT_EVENT;"
rm $PIDFILE
echo "Job: Extract D_DIRECT_EVENT_YYYYMM_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" 
exit



