cd /mnt/beegfs_bsl/tools/ExtractTools/tbl_data_agent_registration_vw_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
file=/mnt/beegfs_bsl/tools/ExtractTools/tbl_data_agent_registration_vw_LIVE/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
filename="${yyyymmdd}_tbl_data_agent_registration_vw_LIVE.csv"
load_date=`date '+%Y-%m-%d %H:%M:%S'`
/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_TPPUSER/Ligadata#8619@ojtppdb-scan.mtn.com.ng:1521/tpp_stb2.mtn.com.ng
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
translate(CID, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(MSISDN, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(TRIM(UPPER(AGENT_FIRST_NAME)), chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(TRIM(UPPER(AGENT_LAST_NAME)), chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(TRIM(UPPER(AGENT_LOCATION)), chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(to_char(DATE_OF_SUBMISSION,'yyyymmdd hh24miss'), chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(AGENT_EMAIL_ID, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(AUDIT_USER, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(CHANNEL_NAME, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(STATUS, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(TRIM(UPPER(TRANSID)), chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(to_char(APPROVE_DATE,'yyyymmdd hh24miss'), chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(SENDERS_MSISDN, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(SHORT_CODE, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(SMS_KEYWORD, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(REJECT_REASON, chr(10)||chr(11)||chr(13) , ' ')||'|'||
translate(TYPE, chr(10)||chr(11)||chr(13) , ' ')
from tpp.tbl_data_agent_registration_vw;
spool off
quit
EOF
