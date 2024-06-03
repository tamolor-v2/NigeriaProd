#!/bin/bash
PIDFILE=/home/daasuser/PIDFiles/extractIncremental_CUG_ACCESS_FEES2.pid
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

cd /mnt/beegfs_bsl/tools/ExtractTools/CUG_ACCESS_FEES2/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
yyyymmdd=`date  "+%Y%m%d_%H%M%S"`
#date_key=`date  "+%Y-%m-%d"`
date_key=`date -d  $1 "+%Y-%m-%d"`
echo "$yyyymmdd"
#kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
filename="CUG_ACCESS_FEES2_${yyyymmdd}.csv"
#maxSeq=$(</mnt/beegfs_bsl/tools/ExtractTools/CUG_ACCESS_FEES2/staging/maxSeq.txt)
echo "started spooling $filename"
echo "select ACCOUNT_CODE_N BIG,MOBILE_NUMBER_V ,ACCOUNT_NAME_V ,to_char(timestamp_d,'yyyymmdd hh24miss'),AMOUNT_V ,OFFER_DESC_V ,SERVICE_CLASS_N ,BILL_CYCL_FULL_CODE_N from  tt_mso_1.cug_access_fees2;"

/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_CDR/thisPWD#123@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.208.215)(PORT=1522))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=agi_p1)))'
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
trim(ACCOUNT_CODE_N),
trim(MOBILE_NUMBER_V),
trim(ACCOUNT_NAME_V),
trim(to_char(timestamp_d,'yyyymmdd hh24miss')),
trim(AMOUNT_V),
trim(OFFER_DESC_V),
trim(SERVICE_CLASS_N),
trim(BILL_CYCL_FULL_CODE_N)
from  tt_mso_1.cug_access_fees2;
spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
###exit
tbl_dt=$yyyymmdd
					hadoop fs -mkdir -p /FlareData/output_8/CUG_ACCESS_FEES2/tbl_dt=${tbl_dt}
					hadoop fs -put  $filename /FlareData/output_8/CUG_ACCESS_FEES2/tbl_dt=${tbl_dt}

mv /mnt/beegfs_bsl/tools/ExtractTools/CUG_ACCESS_FEES2/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/CUG_ACCESS_FEES2/incoming
hive -e "msck repair table flare_8.UG_ACCESS_FEES2"

#awk -v date="$(date +"%Y%m%d%H%M%S")" -F"|" '{print > "/mnt/beegfs_bsl/tools/ExtractTools/CUG_ACCESS_FEES2/staging/CUG_ACCESS_FEES2_"substr($14,1,8)"_"date".txt"}' $filename
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
#gzip -f $filename >$filename$(date +"%Y-%m-%d_%H-%M-%S")
rm $PIDFILE

echo "Job: Extract CUG_ACCESS_FEES2. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")"