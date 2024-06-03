#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

PIDFILE=/home/daasuser/PIDFiles/extractIncremental_NEW_REG.pid
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

cd /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
yyyymmdd=`date  "+%Y%m%d"`
echo "$yyyymmdd"
#kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
filename="NEWREG_BIOUPDT_POOL_LIVE_"$yyyymmdd".csv"
#maxSeq=$(</mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt)
file="/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt"
if [ ! -f "$file" ]
then
	maxSeq=0  
	echo "0" >/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt 
else
#	maxSeq=$(</mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt)
	maxSeq=$( tail -n 1 /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt)
fi
echo "Max Seq No from last run=$maxSeq"
echo "started spooling $filename"

echo "select trim(to_char(SEQ_NO_N,'9999999999'))||'|'|| MSISDN_V||'|'|| 234||''||MSISDN_V||'|'|| LAST_NAME_V||'|'|| SIM_NUMBER_V||'|'|| FIRST_NAME_V||'|'|| MOTHER_MAIDEN_V||'|'|| GENDER_V||'|'|| to_char(DATE_OF_BIRTH,'yyyymmdd hh24miss')||'|'|| NATIONALITY_V||'|'|| PIN_REF_NUM_V||'|'|| STATUS_V||'|'|| SIM_REG_TYPE_V||'|'|| to_char(UPDATED_DT,'yyyymmdd hh24miss')||'|'|| REMARKS_V||'|'|| PROVIDENT_STATUS||'|'|| AGL_STATUS||'|'||  replace(replace(xmlserialize(document ISL_REQUEST_X as clob),chr(10),''),chr(13),'') ||'|'||  INSTANCE_ID_N||'|'|| SESSION_TOKEN_V||'|'|| ACTION_CODE_V||'|'|| EYEBALL_STATUS_V||'|'|| EYEBALL_USER_N||'|'|| to_char(EYEBALL_ON_D,'yyyymmdd hh24miss')||'|'|| EYEBALL_REMARKS_V||'|'|| RECORD_LOCKED_BY_N||'|'|| to_char(RECORD_LOCKED_ON_D,'yyyymmdd hh24miss')||'|'||  replace(replace(xmlserialize(document ADDNL_ATTRB_X as clob),chr(10),''),chr(13),'') ||'|'||  EYEBALL_TYPE_V||'|'|| SIMREG_KIT_NUM_V||'|'|| AGENT_NAME_V||'|'|| SERV_ADDNL_FLD_1_V||'|'|| SERV_ADDNL_FLD_2_V||'|'|| SERV_ADDNL_FLD_3_V||'|'|| SERV_ADDNL_FLD_4_V||'|'|| SERV_ADDNL_FLD_5_V||'|'|| IS_POSTED_TO_CLM||'|'|| to_char(POSTED_TO_CLM_DATE,'yyyymmdd hh24miss')||'|'|| to_char(REQ_RECEIVED_DATE_FROM_CLM,'yyyymmdd hh24miss')||'|'|| EYEBALL_DETAILS_V||'|'|| EYEBALL_RECURRINGIMG_V         from CBS_TBL_CUST.CB_NEWREG_BIOUPDT_POOL where  SEQ_NO_N> ${maxSeq} and updated_dt>to_date('20181130 23:59:59','yyyymmdd hh24:mi:ss')"

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
SEQ_NO_N||'|'||
MSISDN_V||'|'||
234||''||MSISDN_V||'|'||
LAST_NAME_V||'|'||
SIM_NUMBER_V||'|'||
FIRST_NAME_V||'|'||
MOTHER_MAIDEN_V||'|'||
GENDER_V||'|'||
to_char(DATE_OF_BIRTH,'yyyymmdd hh24miss')||'|'||
NATIONALITY_V||'|'||
PIN_REF_NUM_V||'|'||
STATUS_V||'|'||
SIM_REG_TYPE_V||'|'||
to_char(UPDATED_DT,'yyyymmdd hh24miss')||'|'||
REMARKS_V||'|'||
PROVIDENT_STATUS||'|'||
AGL_STATUS||'|'||
 replace(replace(xmlserialize(document ISL_REQUEST_X as clob),chr(10),''),chr(13),'') ||'|'||
 INSTANCE_ID_N||'|'||
SESSION_TOKEN_V||'|'||
ACTION_CODE_V||'|'||
EYEBALL_STATUS_V||'|'||
EYEBALL_USER_N||'|'||
to_char(EYEBALL_ON_D,'yyyymmdd hh24miss')||'|'||
EYEBALL_REMARKS_V||'|'||
RECORD_LOCKED_BY_N||'|'||
to_char(RECORD_LOCKED_ON_D,'yyyymmdd hh24miss')||'|'||
 replace(replace(xmlserialize(document ADDNL_ATTRB_X as clob),chr(10),''),chr(13),'') ||'|'||
 EYEBALL_TYPE_V||'|'||
SIMREG_KIT_NUM_V||'|'||
AGENT_NAME_V||'|'||
SERV_ADDNL_FLD_1_V||'|'||
SERV_ADDNL_FLD_2_V||'|'||
SERV_ADDNL_FLD_3_V||'|'||
SERV_ADDNL_FLD_4_V||'|'||
SERV_ADDNL_FLD_5_V||'|'||
IS_POSTED_TO_CLM||'|'||
to_char(POSTED_TO_CLM_DATE,'yyyymmdd hh24miss')||'|'||
to_char(REQ_RECEIVED_DATE_FROM_CLM,'yyyymmdd hh24miss')||'|'||
EYEBALL_DETAILS_V||'|'||
EYEBALL_RECURRINGIMG_V 
	from CBS_TBL_CUST.CB_NEWREG_BIOUPDT_POOL 
	where  SEQ_NO_N> ${maxSeq} and updated_dt>to_date('20181130 23:59:59','yyyymmdd hh24:mi:ss');
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
echo $maxSeq >>/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt
 dt="$(date +"%H%M%S")"
echo "time=$dt"
sort -t"|" -k14 -o $filename $filename
awk  -v date=${dt} -F"|" '{print > "/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/NEWREG_BIOUPDT_POOL_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
files=(/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/NEWREG_BIOUPDT_POOL_LIVE*)
for var in "${files[@]}"
echo "f_n: ${var}"
do
tbl_dt="${var:89:8}"
re='^[0-9]+$'
if ! [[ $tbl_dt =~ $re ]]
then
   echo "file name : $var, tbl_dt couldn't be extracted, extracted date = $tbl_dt"
else
echo "var=$var"
gzip -f $var
tbl_dt="${var:83:8}"
#hadoop fs -mkdir -p /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${tbl_dt}
echo "hadoop fs -mkdir -p /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${tbl_dt}"
echo "hadoop fs -put '${var}.gz' /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${tbl_dt}"
#hadoop fs -put "${var}.gz" /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${tbl_dt}
#mkdir -p /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/old/${tbl_dt}
#mv "${var}.gz" /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/old/${tbl_dt}/
fi
  # do something on $var
done
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
mv $filename ${tbl_dt}_$dt_$filename
gzip -f ${tbl_dt}_$dt_$filename 

#mv $filename "$filename_$(date +"%Y-%m-%d_%H-%M-%S").csv.gz"
fi
echo "Max_Seq=$maxSeq"
#hive -e "msck repair table flare_8.NEWREG_BIOUPDT_POOL_LIVE"

#awk -v date="$(date +"%Y%m%d%H%M%S")" -F"|" '{print > "/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/NEWREG_BIOUPDT_POOL_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
#gzip -f $filename >$filename$(date +"%Y-%m-%d_%H-%M-%S")
rm $PIDFILE

echo "Job: Extract NEWREG_BIOUPDT_POOL_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")"
