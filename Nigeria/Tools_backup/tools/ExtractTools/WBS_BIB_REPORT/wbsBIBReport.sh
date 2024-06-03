#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

cd /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1

#start_date=$1
#end_date=$2
yyyymmdd=$1

#kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM

filename="wbs_bib_report_"$yyyymmdd".csv"
/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
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

SELECT
GROUPKEY || '|' ||
CASH_FLOW || '|' ||
CASH_FLOW_N || '|' ||
TIER || '|' ||
TIER_N || '|' ||
BILLING_OPERATOR_G || '|' ||
BILLING_OPERATOR_N_G || '|' ||
COMPONENT_DIRECTION_G || '|' ||
EVENT_DIRECTION_G || '|' ||
to_char(EVENT_START_DATE_G,'yyyymmdd') || '|' ||
INCOMING_OPERATOR_G || '|' ||
INCOMING_OPERATOR_N_G || '|' ||
INCOMING_PRODUCT_G || '|' ||
INCOMING_PRODUCT_N_G || '|' ||
OUTGOING_OPERATOR_G || '|' ||
OUTGOING_OPERATOR_N_G || '|' ||
OUTGOING_PRODUCT_G || '|' ||
OUTGOING_PRODUCT_N_G || '|' ||
TIME_PREMIUM_G || '|' ||
ACTUAL_USAGE_D || '|' ||
CALL_COUNT_D || '|' ||
FRANCHISE_N_D || '|' ||
INCOMING_NODE_D || '|' ||
INCOMING_PATH_D || '|' ||
OUTGOING_NODE_D || '|' ||
OUTGOING_PATH_D || '|' ||
TIER_D
FROM WBS_CLIENT.WBS_BIB_REPORT@WBS_DB_LINK.MTN.COM.NG
where to_char(EVENT_START_DATE_G,'yyyymmdd')='$yyyymmdd';
spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
gzip -f $filename
exit
