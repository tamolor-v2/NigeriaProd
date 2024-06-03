#!/bin/bash

tbl_dt=$1
#month_to_run=$(date -d "-1 month -$(($(date +%d)-1)) days" +%Y%m)
month_to_run=$(date -d "last month" '+%Y%m')
#month_to_run=201904
kinit -k -t /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
cd /nas/share05/tools/ExtractTools/D_DIRECT_EVENT_YYYYMM_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/nas/share05/tools/ExtractTools/D_DIRECT_EVENT_YYYYMM_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
filename="D_DIRECT_EVENT_YYYYMM_LIVE_"$yyyymmdd"_"$tbl_dt".csv"
/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
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


spool $filename
select 
MSISDN||'|'||
TERMINATING||'|'||
BILLABLE_CALL_DURATION||'|'||
ACTUAL_CALL_DURATION||'|'||
TOTAL_CALL_COST||'|'||
DW_DATE_KEY||'|'||
CLIENT_NAME||'|'||
COMPANY_NAME||'|'||
SUBSCRIBER_STATUS_DESC||'|'||
CONTRACT_TYPE_NAME||'|'||
SUBSCRIBER_RATE_PLAN_NAME||'|'||
SUBSCRIBER_TYPE||'|'||
TARIFF_ZONE_NAME||'|'||
CALL_ID
from TT_MSO_1.D_DIRECT_EVENT_$month_to_run
where DW_DATE_KEY=$tbl_dt;
spool off
quit
EOF

#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
gzip -f $filename

DIRECTORY="hdfs://ngdaas/FlareData/output_8/D_DIRECT_EVENT_YYYYMM_LIVE/tbl_mon=${month_to_run}/tbl_dt=${tbl_dt}/"
full_path="${DIRECTORY}*"


hadoop fs -test -d $DIRECTORY
    if [ $? == 0 ]
            then
                echo "${now} Start Delete Old Data for date ${month_to_run}"
                hadoop fs -rm $full_path
                echo $full_path
                echo "${now} End Delete Old Data for date ${month_to_run}"

    fi
hadoop fs -mkdir -p /FlareData/output_8/D_DIRECT_EVENT_YYYYMM_LIVE/tbl_mon=${month_to_run}/tbl_dt=${tbl_dt}
echo "hadoop fs -mkdir -p /FlareData/output_8/D_DIRECT_EVENT_YYYYMM_LIVE/tbl_mon=${month_to_run}/tbl_dt=${tbl_dt}"
hadoop fs -put "$filename.gz" /FlareData/output_8/D_DIRECT_EVENT_YYYYMM_LIVE/tbl_mon=${month_to_run}/tbl_dt=${tbl_dt}
echo "hadoop fs -put $filename.gz /FlareData/output_8/D_DIRECT_EVENT_YYYYMM_LIVE/tbl_mon=${month_to_run}/tbl_dt=${tbl_dt}"
#echo "finish the tbl_dt=$tbl_dt"
