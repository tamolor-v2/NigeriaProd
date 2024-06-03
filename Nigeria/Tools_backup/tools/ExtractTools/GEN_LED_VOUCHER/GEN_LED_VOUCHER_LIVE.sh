#!/bin/bash
PIDFILE=/home/daasuser/PIDFiles/extractIncremental_GEN_LED_VOUCHER_LIVE.pid
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
cd /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/nas/share05/tools/ExtractTools/GEN_LED_VOUCHER_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
filename="${yest}_GEN_LED_VOUCHER_LIVE.csv"
load_date=`date '+%Y-%m-%d %H:%M:%S'`
bash /nas/share05/tools/ExtractTools/scripts/replaceDump.sh GEN_LED_VOUCHER ${yest} ${yest}

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
ACCOUNTING_PERIOD||'|'||
ACCOUNTING_YEAR||'|'||
APPROVAL_DATE||'|'|| 
APPROVED_BY_USERID||'|'|| 
STATUS_CANCELLED||'|'|| 
USERID||'|'||
VOUCHER_DATE||'|'||
VOUCHER_NO||'|'||
VOUCHER_TYPE  from IFSAPP.GEN_LED_VOUCHER  ;
spool off
quit
EOF
retVal=$?
kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
#gzip -f $filename
#mkdir -p /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/split/${yest}
#split -l 200000 -d --additional-suffix=.txt ${filename}.gz /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/split/${yest}/$filename
yest=$(date -d '-1 day' '+%Y%m%d')
mkdir -p /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/${yest}
chmod -R 777 /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/spool
cd /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/spool
#find -name '*.gz' -exec sh -c 'gunzip -d "${1%.*}" "$1"' _ {} \;
split -l 200000 -d --additional-suffix=.txt ${yest}_GEN_LED_VOUCHER_LIVE.csv /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/${yest}/${yest}_GEN_LED_VOUCHER

mv /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/${yest} /mnt/beegfs_bsl/live/DB_extract_lz/GEN_LED_VOUCHER/incoming
#DIRECTORY="hdfs://ngdaas/FlareData/output_8/GEN_LED_VOUCHER/tbl_dt=${yyyymmdd}/"
#full_path="${DIRECTORY}*"
#DIRECTORY_yest="hdfs://ngdaas/FlareData/output_8/GEN_LED_VOUCHER/tbl_dt=${yest}/"
#full_path_yest="${DIRECTORY_yest}*"
#if [ $retVal -eq 0 ];
#then

#hadoop fs -test -d $DIRECTORY
#    if [ $? == 0 ]
#            then
#                echo "${now} Start Delete Old Data for date ${yyyymmdd}"
#                hadoop fs -rm $full_pat
#                echo $full_path
#                echo "${now} End Delete Old Data for date ${yyyymmdd}"

 #   fi

  #              mv "$filename.gz" /mnt/beegfs/live/GEN_LED_VOUCHER/incoming/



#hadoop fs -mkdir -p /FlareData/output_8/GEN_LED_VOUCHER_LIVE/tbl_dt=${yyyymmdd}
#echo "hadoop fs -mkdir -p /FlareData/output_8/GEN_LED_VOUCHER_LIVE/tbl_dt=${yyyymmdd}"
#hadoop fs -put "$filename.gz" /FlareData/output_8/GEN_LED_VOUCHER_LIVE/tbl_dt=${yyyymmdd}
#echo "hadoop fs -put $filename.gz /FlareData/output_8/GEN_LED_VOUCHER_LIVE/tbl_dt=${yyyymmdd}"
#full_path_file="${DIRECTORY}${filename}.gz"
#hadoop fs -test -f $full_path_file
#    if [ $? == 0 ]
#            then
#                hadoop fs -test -z $full_path_file
#                if [ $? != 0 ]
#                then

echo "spool is success "
#				   hadoop fs -test -d $DIRECTORY_yest
#						if [ $? == 0 ]
#								then
#									echo "${now} Start Delete Old Data for date yest"
#									hadoop fs -rm $full_path_yest
#									echo $full_path_yest
#									echo "${now} End Delete Old Data for date yest"

#						fi
                   
#                 fi

#    fi
#fi
#kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM

#hadoop fs -mkdir -p /FlareData/output_8/GEN_LED_VOUCHER/tbl_dt=${yyyymmdd}
#hadoop fs -mkdir -p /FlareData/output_8/GEN_LED_VOUCHER/tbl_dt=${yest}
#hive -e "msck repair table flare_8.GEN_LED_VOUCHER;"
#mkdir -p /mnt/beegfs/tools/ExtractTools/GEN_LED_VOUCHER_LIVE/old/${yyyymmdd}
#echo "mkdir -p /mnt/beegfs/tools/ExtractTools/GEN_LED_VOUCHER_LIVE/old/${yyyymmdd}"
#mv "${var}.gz" /mnt/beegfs/tools/ExtractTools/GEN_LED_VOUCHER_LIVE/old/${yyyymmdd}/
#echo "moving ${var}.gz /mnt/beegfs/tools/ExtractTools/GEN_LED_VOUCHER_LIVE/old/${yyyymmdd}/"
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
#gzip -f $filename
#mv $filename "$filename_$(date +"%Y-%m-%d_%H-%M-%S").csv.gz"
rm $PIDFILE
echo "Job: Extract GEN_LED_VOUCHER_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" 
exit
