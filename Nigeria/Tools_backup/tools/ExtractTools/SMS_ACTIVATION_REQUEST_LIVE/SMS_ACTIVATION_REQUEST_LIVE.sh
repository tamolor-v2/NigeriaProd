#!/bin/bash
cd /nas/share05/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
yyyymmdd=`date -d $1 "+%Y%m%d"`
echo "$yyyymmdd"
#kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
filename="SMS_ACTIVATION_REQUEST_LIVE_"$yyyymmdd".csv"
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
ID||'|'||
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
where receipt_timestamp between
to_date('${yyyymmdd} 000000','yyyymmdd hh24miss') and to_date('${yyyymmdd} 235959','yyyymmdd hh24miss');
spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
gzip -f $filename
exit
