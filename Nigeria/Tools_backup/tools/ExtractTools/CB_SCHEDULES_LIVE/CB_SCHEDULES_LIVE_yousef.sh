#!/bin/bash
PIDFILE=/home/daasuser/PIDFiles/extractIncremental_CB_SCHEDULES_LIVE.pid
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

cd /mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
yyyymmdd=`date  "+%Y%m%d_%H%M%S"`
#date_key=`date  "+%Y-%m-%d"`
date_key=`date -d  $1 "+%Y-%m-%d"`
echo "$yyyymmdd"
echo $date_key
#kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
filename="CB_SCHEDULES_LIVE_${yyyymmdd}_$1.csv"
#maxSeq=$(</mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/staging/maxSeq.txt)
echo "started spooling $filename"

echo "select /*+ parallel 12 */ SERVICE_CODE_V||'|'|| SERVICE_KEY_CODE_V||'|'|| SERV_ACC_LINK_CODE_N||'|'|| SCHEME_REF_CODE_N||'|'|| PROFIT_CENTRE_CODE||'|'|| COLLECTED_UPFRONT_V||'|'|| PRIORITY_N||'|'|| ACTION_PARM_STRG_V||'|'|| to_char(PROCESS_ON_DATE_D,'yyyymmdd hh24miss')||'|'|| STATUS_OPTN_V||'|'|| CRITICAL_SERVICE_FLG_V||'|'|| GROUP_CODE_V||'|'|| to_char(EXECUTED_DT,'yyyymmdd hh24miss')||'|'|| USER_CODE_N||'|'|| FROM_SELF_CARE_FLG_V||'|'|| SCHDL_LINK_CODE_N||'|'|| ACCOUNT_LINK_CODE_N||'|'|| PROV_CHRONO_NUM_N||'|'|| ORD_PROV_REQUEST_V||'|'|| to_char(LAST_MODIFIED_DATE_D,'yyyymmdd hh24miss')||'|'|| ACC_SERV_FLG_V||'|'|| FAILURE_REASON_V||'|'|| WAIVE_OFF_APPROVAL_V||'|'|| UPLOAD_SEQUENCE_N||'|'|| SESSION_TOKEN_V||'|'|| to_char(LAST_MODIFIED_DATE_D,'yyyymmddhh24miss') from tt_mso_1.cb_schedules where PROCESS_ON_DATE_D between to_date('${date_key} 00:00:00','yyyy-mm-dd hh24:mi:ss') and to_date('${date_key} 23:59:59','yyyy-mm-dd hh24:mi:ss');"

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
select /*+ parallel 12 */
SERVICE_CODE_V||'|'||
SERVICE_KEY_CODE_V||'|'||
SERV_ACC_LINK_CODE_N||'|'||
SCHEME_REF_CODE_N||'|'||
PROFIT_CENTRE_CODE||'|'||
COLLECTED_UPFRONT_V||'|'||
PRIORITY_N||'|'||
ACTION_PARM_STRG_V||'|'||
to_char(PROCESS_ON_DATE_D,'yyyymmdd hh24miss')||'|'||
STATUS_OPTN_V||'|'||
CRITICAL_SERVICE_FLG_V||'|'||
GROUP_CODE_V||'|'||
to_char(EXECUTED_DT,'yyyymmdd hh24miss')||'|'||
USER_CODE_N||'|'||
FROM_SELF_CARE_FLG_V||'|'||
SCHDL_LINK_CODE_N||'|'||
ACCOUNT_LINK_CODE_N||'|'||
PROV_CHRONO_NUM_N||'|'||
ORD_PROV_REQUEST_V||'|'||
to_char(LAST_MODIFIED_DATE_D,'yyyymmdd hh24miss')||'|'||
ACC_SERV_FLG_V||'|'||
FAILURE_REASON_V||'|'||
WAIVE_OFF_APPROVAL_V||'|'||
UPLOAD_SEQUENCE_N||'|'||
SESSION_TOKEN_V||'|'||
to_char(LAST_MODIFIED_DATE_D,'yyyymmddhh24miss')
from tt_mso_1.cb_schedules where PROCESS_ON_DATE_D between 
to_date('${date_key} 00:00:00','yyyy-mm-dd hh24:mi:ss') and
to_date('${date_key} 23:59:59','yyyy-mm-dd hh24:mi:ss');

spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
###exit
maxSeq=$(cat $filename | awk -F"|" '{print $1}' | sort -nk1 | tail -1) 
echo "Max_Seq=$maxSeq"
if [ -z "$maxSeq" ]
	then
	echo "couldn't find new records"
	rm $filename
else
	echo $maxSeq >>/mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/staging/maxSeq.txt
	 dt="$(date +"%H%M%S")"
	echo "time=$dt"
	sort -t"|" -k14 -o $filename $filename
	awk  -v date=${dt} -F"|" '{print > "/mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/staging/CB_SCHEDULES_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
	files=(/mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/staging/CB_SCHEDULES_LIVE*)
	for var in "${files[@]}"
		do
			tbl_dt="${var:89:8}"
			re='^[0-9]+$'
			if ! [[ $tbl_dt =~ $re ]]
				then
					echo "file name : $var, tbl_dt couldn't be extracted, extracted date = $tbl_dt"
				else
					echo "$var"
					gzip -f $var
					hadoop fs -mkdir -p /FlareData/output_8/CB_SCHEDULES_LIVE/tbl_dt=${tbl_dt}
					hadoop fs -put "${var}.gz" /FlareData/output_8/CB_SCHEDULES_LIVE/tbl_dt=${tbl_dt}
					mkdir -p /mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/old/${tbl_dt}
					mv "${var}.gz" /mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/old/${tbl_dt}/
			fi
	done
	mv $filename ${tbl_dt}_$dt_$filename
	gzip -f ${tbl_dt}_$dt_$filename 

#mv $filename "$filename_$(date +"%Y-%m-%d_%H-%M-%S").csv.gz"
fi
echo "Max_Seq=$maxSeq"
hive -e "msck repair table flare_8.CB_SCHEDULES_LIVE"

#awk -v date="$(date +"%Y%m%d%H%M%S")" -F"|" '{print > "/mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/staging/CB_SCHEDULES_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
#gzip -f $filename >$filename$(date +"%Y-%m-%d_%H-%M-%S")
rm $PIDFILE

echo "Job: Extract CB_SCHEDULES_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")"
