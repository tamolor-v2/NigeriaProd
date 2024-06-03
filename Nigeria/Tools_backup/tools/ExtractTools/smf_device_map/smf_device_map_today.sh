#!/bin/bash
PIDFILE=/home/daasuser/PIDFiles/extractIncremental_smf_device_map_LIVE.pid
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
cd /mnt/beegfs/tools/ExtractTools/smf_device_map/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/mnt/beegfs/tools/ExtractTools/smf_device_map_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
filename="smf_device_map_LIVE_"$yyyymmdd".csv"
load_date=`date '+%Y-%m-%d %H:%M:%S'`
/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
BIB_DAAS/Pa33word#456@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.122.4)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIBP11)))'
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
CODE||'|'||
MAC_ADDRESS||'|'||
DEALER||'|'||
DEALER_CODE||'|'||
DEPT||'|'||
DEALER_EMAIL||'|'||
DEALER_MOBILE||'|'||
CREATE_DT||'|'||
DATE_KEY||'|'||
DEVICE_ID from stg_cdr.smf_device_map
spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
gzip -f $filename
DIRECTORY="hdfs://ngdaas/FlareData/output_8/smf_device_map/tbl_dt=${yyyymmdd}/"
full_path="${DIRECTORY}*"
DIRECTORY_yest="hdfs://ngdaas/FlareData/output_8/smf_device_map/tbl_dt=${yest}/"
full_path_yest="${DIRECTORY_yest}*"

hadoop fs -test -d $DIRECTORY
    if [ $? == 0 ]
            then
                echo "${now} Start Delete Old Data for date ${yyyymmdd}"
                hadoop fs -rm $full_path
                echo $full_path
                echo "${now} End Delete Old Data for date ${yyyymmdd}"

    fi


hadoop fs -mkdir -p /FlareData/output_8/smf_device_map/tbl_dt=${yyyymmdd}
echo "hadoop fs -mkdir -p /FlareData/output_8/smf_device_map/tbl_dt=${yyyymmdd}"
hadoop fs -put "$filename.gz" /FlareData/output_8/smf_device_map/tbl_dt=${yyyymmdd}
echo "hadoop fs -put $filename.gz /FlareData/output_8/smf_device_map/tbl_dt=${yyyymmdd}"

full_path_file="${DIRECTORY}${filename}.gz"
hadoop fs -test -f $full_path_file
    if [ $? == 0 ]
            then
                hadoop fs -test -z $full_path_file
                if [ $? != 0 ]
                then
                                   hadoop fs -test -d $DIRECTORY_yest
                                                if [ $? == 0 ]
                                                                then
                                                                        echo "${now} Start Delete Old Data for date yest"
                                                                        hadoop fs -rm $full_path_yest
                                                                        echo $full_path_yest
                                                                        echo "${now} End Delete Old Data for date yest"

                                                fi

                 fi

    fi
hive -e "msck repair table flare_8.smf_device_map;"
#mkdir -p /mnt/beegfs/tools/ExtractTools/smf_device_map_LIVE/old/${yyyymmdd}
#echo "mkdir -p /mnt/beegfs/tools/ExtractTools/smf_device_map_LIVE/old/${yyyymmdd}"
#mv "${var}.gz" /mnt/beegfs/tools/ExtractTools/smf_device_map_LIVE/old/${yyyymmdd}/
#echo "moving ${var}.gz /mnt/beegfs/tools/ExtractTools/smf_device_map_LIVE/old/${yyyymmdd}/"
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
#gzip -f $filename
#mv $filename "$filename_$(date +"%Y-%m-%d_%H-%M-%S").csv.gz"
rm $PIDFILE
echo "Job: Extract smf_device_map_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" 
exit
