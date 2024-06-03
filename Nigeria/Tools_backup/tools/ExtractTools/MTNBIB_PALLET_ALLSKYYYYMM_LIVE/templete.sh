#!/bin/bash

tbl_dt=$1
month_to_run=$2
#month_to_run=$(date -d "-1 month -$(($(date +%d)-1)) days" +%Y%m)
kinit -k -t /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
cd /nas/share05/tools/ExtractTools/MTNBIB_PALLET_ALLSKYYYYMM_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/nas/share05/tools/ExtractTools/MTNBIB_PALLET_ALLSKYYYYMM_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
filename="MTNBIB_PALLET_ALLSKYYYYMM_"$yyyymmdd"_"$tbl_dt".csv"
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
from ifsapp.MTNBIB_PALLET_ALLSK$month_to_run
where YEAR_MONTH=$tbl_dt;
spool off
quit
EOF

#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
gzip -f $filename

DIRECTORY="hdfs://ngdaas/FlareData/output_8/MTNBIB_PALLET_ALLSKYYYYMM_LIVE/tbl_dt=${tbl_dt}01/"
full_path="${DIRECTORY}*"


hadoop fs -test -d $DIRECTORY
    if [ $? == 0 ]
            then
                echo "${now} Start Delete Old Data for date ${month_to_run}"
                hadoop fs -rm $full_path
                echo $full_path
                echo "${now} End Delete Old Data for date ${month_to_run}"

    fi
hadoop fs -mkdir -p /FlareData/output_8/MTNBIB_PALLET_ALLSKYYYYMM_LIVE/tbl_dt=${tbl_dt}01
echo "hadoop fs -mkdir -p /FlareData/output_8/MTNBIB_PALLET_ALLSKYYYYMM_LIVE/tbl_dt=${tbl_dt}01"
hadoop fs -put "$filename.gz" /FlareData/output_8/MTNBIB_PALLET_ALLSKYYYYMM_LIVE/tbl_dt=${tbl_dt}01
echo "hadoop fs -put $filename.gz /FlareData/output_8/MTNBIB_PALLET_ALLSKYYYYMM_LIVE/tbl_dt=${tbl_dt}01"
#echo "finish the tbl_dt=$tbl_dt01"
