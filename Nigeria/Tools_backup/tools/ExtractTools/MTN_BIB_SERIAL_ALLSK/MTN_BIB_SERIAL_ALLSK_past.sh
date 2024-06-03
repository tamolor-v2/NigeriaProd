#!/bin/bash
PIDFILE=/home/daasuser/PIDFiles/extractIncremental_MTN_BIB_SERIAL_ALLSKE.pid
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
#yyyymm=`date  "+%Y%m"`
#yyyymm=201903
nowmonth=$(date +%Y-%m)
yyyymm=$(date -d "$nowmonth-15 last month" '+%Y%m')
yyyymm=$1
tbl_name="MTNBIB_SERIAL_ALLSK$yyyymm"
echo $tbl_name

month=$(echo $tbl_name |tail -c 7)
echo "$month"

cd  /nas/share05/tools/ExtractTools/MTN_BIB_SERIAL_ALLSK/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
yyyymmdd=`date  "+%Y%m%d"`
date_key=`date  "+%Y-%m-%d"`
echo "$yyyymmdd"
dt=$1
kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
filename="MTN_BIB_SERIAL_ALLSK_"$month"_"$yyyymmdd"_"$dt"01.csv"
#maxSeq=$(</mnt/beegfs_bsl/tools/ExtractTools/MTN_BIB_SERIAL_ALLSK/staging/maxSeq.txt)
echo "started spooling $filename"

echo "select YEAR_MONTH||'|'|| to_char(DATE_CREATED,'yyyymmdd hh24miss')||'|'|| ORDER_NO||'|'|| TRANSACTION_CODE||'|'|| CUSTOMER_NO||'|'|| CUSTOMER_NAME||'|'|| QUANTITY||'|'|| PART_NO||'|'|| SERIAL_NO||'|'|| PART_DESCRIPTION||'|'|| CONTRACT||'|'|| LOCATION_NO||'|'|| PRICE_UNIT||'|'|| COST from $tbl_name;"

/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_IFSUSER/Ligadata#8619@ojifspp02.mtn.com.ng:1521/ifs_p5.mtn.com.ng
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
YEAR_MONTH||'|'||
to_char(DATE_CREATED,'yyyymmdd hh24miss')||'|'||
ORDER_NO||'|'||
TRANSACTION_CODE||'|'||
CUSTOMER_NO||'|'||
CUSTOMER_NAME||'|'||
QUANTITY||'|'||
PART_NO||'|'||
SERIAL_NO||'|'||
PART_DESCRIPTION||'|'||
CONTRACT||'|'||
LOCATION_NO||'|'||
PRICE_UNIT||'|'||
COST
from ifsapp.$tbl_name;

spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename

DIRECTORY="hdfs://ngdaas/FlareData/output_8/MTN_BIB_SERIAL_ALLSK/tbl_dt=${month}"
full_path="${DIRECTORY}/*"

		gzip -f $filename 
hadoop fs -test -d $DIRECTORY
    if [ $? == 0 ]
            then
                echo "Deleting old date for date ${tbl_dt}"
                hadoop fs -rm $full_path
                echo $full_path
                echo "deleted data from $full_path"
	else

		hadoop fs -mkdir -p /FlareData/output_8/MTN_BIB_SERIAL_ALLSK/tbl_dt=${month}
		hive -e "msck repair table flare_8.MTN_BIB_SERIAL_ALLSK"
    fi
		mkdir -p /nas/share05/tools/ExtractTools/MTN_BIB_SERIAL_ALLSK/old/${month}
		hadoop fs -put "${filename}.gz" /FlareData/output_8/MTN_BIB_SERIAL_ALLSK/tbl_dt=${month}
		mv "${filename}.gz /nas/share05/tools/ExtractTools/MTN_BIB_SERIAL_ALLSK/old/${month}/


rm $PIDFILE

#echo "Job: Extract MTN_BIB_SERIAL_ALLSK. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")"
