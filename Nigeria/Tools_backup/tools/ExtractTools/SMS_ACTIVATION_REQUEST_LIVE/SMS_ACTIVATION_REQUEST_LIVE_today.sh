#!/bin/bash
PIDFILE=/home/daasuser/PIDFiles/extractIncremental_SMS.pid
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

kinit -k -t /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
cd /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
tm=`date  "+%H%M%S"`
filename="SMS_ACTIVATION_REQUEST_LIVE_{$yyyymmdd}_${tm}.csv"
if [ ! -f "$file" ]
then
        maxSeq=0
        echo "0" >/mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/staging/maxSeq.txt
else
#       maxSeq=$(</mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/staging/maxSeq.txt)
        maxSeq=$( tail -n 1 /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/staging/maxSeq.txt)
fi
echo "Max Seq No from last run=$maxSeq"
/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_SFUSER/Ligadata#8619@ikbiosmdb01.mtn.com.ng:1521/bsm_p1.mtn.com.ng
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
trim(to_char(ID,'999999999999'))||'|'||
to_char(ACTIVATION_TIMESTAMP,'yyyymmdd hh24miss')||'|'||
to_char(SAR_PART_KEY,'yyyymmdd hh24miss')||'|'||
CUSTOMER_NAME||'|'||
ENROLLMENT_REF||'|'||
IS_INITIATOR||'|'||
PHONE_NUMBER||'|'||
to_char(RECEIPT_TIMESTAMP,'yyyymmdd hh24miss')||'|'||
to_char(REGISTRATION_TIMESTAMP,'yyyymmdd hh24miss')||'|'||
SENDER_NUMBER||'|'||
SERIAL_NUMBER||'|'||
STATE_ID||'|'||
STATUS||'|'||
UNIQUE_ID||'|'||
PHONE_NUMBER_STATUS_FK||'|'||
to_char(CRM_BIO_UPDATE_TIME,'yyyymmdd hh24miss')||'|'||
to_char(CRM_UPDATE_TIME,'yyyymmdd hh24miss')||'|'||
AGL_STATUS||'|'||
MSISDN_UPDATE_STATUS||'|'||
to_char(MSISDN_UPDATE_TIMESTAMP,'yyyymmdd hh24miss')||'|'||
PREVIOUS_UNIQUE_ID||'|'||
REGISTRATION_TYPE||'|'||
CONFIRMATION_STATUS||'|'||
to_char(CONFIRMATION_TIMESTAMP,'yyyymmdd hh24miss')||'|'||
BASIC_DATA_ID||'|'||
TRIM(LEADING 0 FROM TRIM(phone_number))||'|'||
to_char(RECEIPT_TIMESTAMP,'yyyymmdd')
	from biocapture.sms_activation_request
	where ID>$maxSeq 
	order by receipt_timestamp;
spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
maxSeq=$(cat $filename | awk -F"|" '{print $1}' | sort -nk1 | tail -1)
echo "Max_Seq=$maxSeq"
if [ -z "$maxSeq" ]
then
echo "couldn't find new records"
rm $filename
else
echo $maxSeq >>/mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/staging/maxSeq.txt
 dt="$(date +"%H%M%S")"
awk  -v date=$dt -F"|" '{print > "/mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/staging/SMS_ACTIVATION_REQUEST_LIVE_"substr($27,1,8)"_"date".txt"}' $filename 
#zcat $filename | awk -F"|" '{print $1}' | sort -nk1 | tail -1
files=(/mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/staging/SMS_ACTIVATION_REQUEST_LIVE*)
for var in "${files[@]}"
do
echo "file: $var"
gzip -f $var
tbl_dt="${var:95:8}"
re='^[0-9]+$'
if ! [[ $tbl_dt =~ $re ]] 
then
   echo "tbl_dt couldn't be extracted correctly $tbl_dt"
else
echo "$tbl_dt"
hadoop fs -mkdir -p /FlareData/output_8/SMS_ACTIVATION_REQUEST_LIVE/tbl_dt=${tbl_dt}
echo "hadoop fs -mkdir -p /FlareData/output_8/SMS_ACTIVATION_REQUEST_LIVE/tbl_dt=${tbl_dt}"
hadoop fs -put "${var}.gz" /FlareData/output_8/SMS_ACTIVATION_REQUEST_LIVE/tbl_dt=${tbl_dt}
echo "hadoop fs -put ${var}.gz /FlareData/output_8/SMS_ACTIVATION_REQUEST_LIVE/tbl_dt=${tbl_dt}"
mkdir -p /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/old/${tbl_dt}
echo "mkdir -p /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/old/${tbl_dt}"
mv "${var}.gz" /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/old/${tbl_dt}/
echo "moving ${var}.gz /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/old/${tbl_dt}/"
fi
  # do something on $var
done
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
gzip -f $filename
#mv $filename "$filename_$(date +"%Y-%m-%d_%H-%M-%S").csv.gz"
fi
echo "Max_Seq=$maxSeq"
hive -e "msck repair table flare_8.SMS_ACTIVATION_REQUEST_LIVE"
rm $PIDFILE
echo "Job: Extract SMS_ACTIVATION_REQUEST_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" 
for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 5m"
     sleep 5m
else
     break
fi
done
exit
