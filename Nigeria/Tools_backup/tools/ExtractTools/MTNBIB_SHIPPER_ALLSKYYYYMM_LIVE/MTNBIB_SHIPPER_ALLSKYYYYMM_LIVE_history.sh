#!/bin/bash
PIDFILE=/home/daasuser/PIDFiles/extractIncremental_MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE.pid
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
cd /mnt/beegfs/tools/ExtractTools/MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/mnt/beegfs/tools/ExtractTools/MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
filename="MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE_"$yyyymmdd".csv"
load_date=`date '+%Y-%m-%d %H:%M:%S'`
month_to_run=$(date -d "-1 month -$(($(date +%d)-1)) days" +%Y%m)
month_to_run=$1
#tbl_dt=20151101
tbl_dt=$(date -d "-1 month -$(($(date +%d)-1)) days" +%Y%m%d)
kinit -k -t /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
cd /mnt/beegfs/tools/ExtractTools/MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/mnt/beegfs/tools/ExtractTools/MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
filename="MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE_"$yyyymmdd"_"$tbl_dt".csv"
array=($(/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
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
select distinct YEAR_MONTH from ifsapp.MTNBIB_SHIPPER_ALLSK$month_to_run;
EOF))

for i in "${array[@]}"
do
   echo "start for tbl_dt : $i"
   bash /mnt/beegfs/tools/ExtractTools/MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE/templete.sh $i $month_to_run
   echo "end for tbl_dt : $i"
   
done

hive -e "msck repair table flare_8.MTN_BIB_SHIPPER_ALLSK;"
rm $PIDFILE
echo "Job: Extract MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" 
exit



