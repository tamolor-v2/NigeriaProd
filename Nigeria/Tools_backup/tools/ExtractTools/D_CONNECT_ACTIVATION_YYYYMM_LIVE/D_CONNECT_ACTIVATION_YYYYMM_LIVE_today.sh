#!/bin/bash
PIDFILE=/home/daasuser/PIDFiles/extractIncremental_D_CONNECT_ACTIVATION_YYYYMM_LIVE.pid
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
cd /mnt/beegfs/tools/ExtractTools/D_CONNECT_ACTIVATION_YYYYMM_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/mnt/beegfs/tools/ExtractTools/D_CONNECT_ACTIVATION_YYYYMM_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
filename="D_CONNECT_ACTIVATION_YYYYMM_LIVE_"$yyyymmdd".csv"
load_date=`date '+%Y-%m-%d %H:%M:%S'`
month_to_run=$(date -d "-1 month -$(($(date +%d)-1)) days" +%Y%m)
#month_to_run=201905
echo $month_to_run
#tbl_dt=20181001
echo "running for $month_to_run"
tbl_dt=$(date -d "-1 month -$(($(date +%d)-1)) days" +%Y%m%d)
tbl_dt="${month_to_run}01"
echo "$tbl_dt"
#exit
kinit -k -t /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
cd /mnt/beegfs/tools/ExtractTools/D_CONNECT_ACTIVATION_YYYYMM_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/mnt/beegfs/tools/ExtractTools/D_CONNECT_ACTIVATION_YYYYMM_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
filename="D_CONNECT_ACTIVATION_YYYYMM_LIVE_"$yyyymmdd"_"$tbl_dt".csv"
/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_CDR/thisPWD#123@10.1.208.215:1522/agi_p1
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


spool $filename
select
ACCOUNT_NUMBER||'|'||
SERVICE_ID||'|'||
to_char(ACTIVATION_DATE,'yyyymmdd hh24miss')||'|'||
CLIENT_NAME||'|'||
COMPANY_NAME||'|'||
SUBSCRIBER_STATUS_DESC||'|'||
to_char(STATUS_DATE,'yyyymmdd hh24miss')||'|'||
CONTRACT_TYPE_NAME||'|'||
SUBSCRIBER_RATE_PLAN_NAME||'|'||
SUBSCRIBER_TYPE||'|'||
MRC||'|'||
INSTALLATION_CHARGE||'|'||
ACCOUNT_BALANCE||'|'||
'234' || service_id
from TT_MSO_1.D_CONNECT_ACTIVATION_$month_to_run;

spool off
quit
EOF

#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
gzip -f $filename

DIRECTORY="hdfs://ngdaas/FlareData/output_8/D_CONNECT_ACTIVATION_YYYYMM_LIVE/tbl_dt=${tbl_dt}/"
full_path="${DIRECTORY}*"


hadoop fs -test -d $DIRECTORY
    if [ $? == 0 ]
            then
                echo "${now} Start Delete Old Data for date ${month_to_run}"
                hadoop fs -rm $full_path
                echo $full_path
                echo "${now} End Delete Old Data for date ${month_to_run}"

    fi
hadoop fs -mkdir -p /FlareData/output_8/D_CONNECT_ACTIVATION_YYYYMM_LIVE/tbl_dt=${tbl_dt}
echo "hadoop fs -mkdir -p /FlareData/output_8/D_CONNECT_ACTIVATION_YYYYMM_LIVE/tbl_dt=${tbl_dt}"
hadoop fs -put "$filename.gz" /FlareData/output_8/D_CONNECT_ACTIVATION_YYYYMM_LIVE/tbl_dt=${tbl_dt}
echo "hadoop fs -put $filename.gz /FlareData/output_8/D_CONNECT_ACTIVATION_YYYYMM_LIVE/tbl_dt=${tbl_dt}"

hive -e "msck repair table flare_8.D_CONNECT_ACTIVATION;"
rm $PIDFILE
echo "Job: Extract D_CONNECT_ACTIVATION_YYYYMM_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" 
exit



