#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

msisdnLastDigit=$1

cd /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
yyyymmdd=`date  "+%Y%m%d"`
date_key=`date  "+%Y-%m-%d"`
echo "$yyyymmdd"
#kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
filename="CB_SERV_MAST_VIEW_LIVE_${yyyymmdd}_${msisdnLastDigit}.csv"
#maxSeq=$(</mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/staging/maxSeq.txt)
echo "started spooling $filename"
echo "$filename"
echo "select /*+ parallel 12 */ ACCOUNT_LINK_CODE_N||'|'|| SERVICE_CODE_V||'|'|| ACCOUNT_CODE_N||'|'|| SUB_SERVICE_CODE||'|'|| STATUS_CODE_V||'|'|| SUBS_NAME_V||'|'|| LAST_NAME_V||'|'|| SUBS_TITLE_V||'|'|| PACKAGE_CODE_V||'|'|| TARIFF_CODE_V||'|'|| to_char(ACTIVATION_DATE_D,'yyyymmdd hh24miss')||'|'|| to_char(ERASED_DATE_D,'yyyymmdd hh24miss')||'|'|| to_char(SUSPENDED_DATE_D,'yyyymmdd hh24miss')||'|'|| to_char(REACTIVATION_DATE_D,'yyyymmdd hh24miss')||'|'|| MOBL_NUM_VOICE_V||'|'|| MOBL_NUM_DATA_V||'|'|| MOBL_NUM_FAX_V||'|'|| to_char(PRE_TERMINATE_DATE_D,'yyyymmdd hh24miss')||'|'|| CONTRACT_TYPE_V||'|'|| to_char(CONTRACT_START_D,'yyyymmdd hh24miss')||'|'|| to_char(CONTRACT_END_D,'yyyymmdd hh24miss')||'|'|| ACTIVATED_BY_USER_CODE_N||'|'|| to_char(REGISTRATION_DATE_D,'yyyymmdd hh24miss')||'|'|| SIM_NUM_V||'|'|| IMSI_NUM_N||'|'|| IC_NUMBER_V||'|'|| X_DIRECTORY_LEVEL_N||'|'|| CABLE_TYPE_N||'|'|| ADDITIONAL_LINE_SITE_FLG_V||'|'|| ADDITIONAL_LINE_SITE_QTY_N||'|'|| GEO_LOC_ZONE_CODE_V||'|'|| SALES_PERSON_CODE_V||'|'|| ADDITIONAL_SIM_FLAG_V||'|'|| ADDITIONAL_IMSI_NUM_N||'|'|| ADDITIONAL_SIM_NUM_V||'|'|| ALLOW_MARKETING_CALLS_V||'|'|| SEVICE_IDENTIFIER_V||'|'|| BILL_CYCL_CODE_N||'|'|| CHURN_FLAG_V||'|'|| SUBSCRIBER_CATEGORY_V||'|'|| SUBSCRIBER_SUB_CATEGORY_V||'|'|| PREFERRED_LANGUAGE_V||'|'|| HYBRID_TYPE_V||'|'|| HYBRID_SERVICE_STATUS_V||'|'|| BC_TARIFF_CHANGE_TYPE_V||'|'|| COMMITMENT_AMOUNT_N||'|'|| CONTACT_NUMBER_V||'|'|| DATE_OF_BIRTH||'|'|| STATE_OF_ORIGIN_V||'|'|| LGA_OF_ORIGIN_V||'|'|| MOTHER_MAIDEN_V||'|'|| PRINT_ZERO_INVOICE_FLG_V||'|'|| to_char(DATE_D,'yyyymmdd hh24miss')||'|'|| '234'||MOBL_NUM_VOICE_V||'|'|| substr(MOBL_NUM_VOICE_V,-1) from cbs_tbl_core.cb_serv_mast_view where decode(substr(MOBL_NUM_VOICE_V,-1),'0',0,'1',0,'2',0,'3',0,'4',0,'5',0,'6',0,'7',0,'8',0,'9',0,1)=1; "
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
/*+ parallel 12 */
ACCOUNT_LINK_CODE_N||'|'||
SERVICE_CODE_V||'|'||
ACCOUNT_CODE_N||'|'||
SUB_SERVICE_CODE||'|'||
STATUS_CODE_V||'|'||
SUBS_NAME_V||'|'||
LAST_NAME_V||'|'||
SUBS_TITLE_V||'|'||
PACKAGE_CODE_V||'|'||
TARIFF_CODE_V||'|'||
to_char(ACTIVATION_DATE_D,'yyyymmdd hh24miss')||'|'||
to_char(ERASED_DATE_D,'yyyymmdd hh24miss')||'|'||
to_char(SUSPENDED_DATE_D,'yyyymmdd hh24miss')||'|'||
to_char(REACTIVATION_DATE_D,'yyyymmdd hh24miss')||'|'||
MOBL_NUM_VOICE_V||'|'||
MOBL_NUM_DATA_V||'|'||
MOBL_NUM_FAX_V||'|'||
to_char(PRE_TERMINATE_DATE_D,'yyyymmdd hh24miss')||'|'||
CONTRACT_TYPE_V||'|'||
to_char(CONTRACT_START_D,'yyyymmdd hh24miss')||'|'||
to_char(CONTRACT_END_D,'yyyymmdd hh24miss')||'|'||
ACTIVATED_BY_USER_CODE_N||'|'||
to_char(REGISTRATION_DATE_D,'yyyymmdd hh24miss')||'|'||
SIM_NUM_V||'|'||
IMSI_NUM_N||'|'||
IC_NUMBER_V||'|'||
X_DIRECTORY_LEVEL_N||'|'||
CABLE_TYPE_N||'|'||
ADDITIONAL_LINE_SITE_FLG_V||'|'||
ADDITIONAL_LINE_SITE_QTY_N||'|'||
GEO_LOC_ZONE_CODE_V||'|'||
SALES_PERSON_CODE_V||'|'||
ADDITIONAL_SIM_FLAG_V||'|'||
ADDITIONAL_IMSI_NUM_N||'|'||
ADDITIONAL_SIM_NUM_V||'|'||
ALLOW_MARKETING_CALLS_V||'|'||
SEVICE_IDENTIFIER_V||'|'||
BILL_CYCL_CODE_N||'|'||
CHURN_FLAG_V||'|'||
SUBSCRIBER_CATEGORY_V||'|'||
SUBSCRIBER_SUB_CATEGORY_V||'|'||
PREFERRED_LANGUAGE_V||'|'||
HYBRID_TYPE_V||'|'||
HYBRID_SERVICE_STATUS_V||'|'||
BC_TARIFF_CHANGE_TYPE_V||'|'||
COMMITMENT_AMOUNT_N||'|'||
CONTACT_NUMBER_V||'|'||
DATE_OF_BIRTH||'|'||
STATE_OF_ORIGIN_V||'|'||
LGA_OF_ORIGIN_V||'|'||
MOTHER_MAIDEN_V||'|'||
PRINT_ZERO_INVOICE_FLG_V||'|'||
to_char(DATE_D,'yyyymmdd hh24miss')||'|'||
0||'|'||
-1
from cbs_tbl_core.cb_serv_mast_view where 
decode(substr(MOBL_NUM_VOICE_V,-1),'0',0,'1',0,'2',0,'3',0,'4',0,'5',0,'6',0,'7',0,'8',0,'9',0,1)=1;

spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
exit
maxSeq=$(cat $filename | awk -F"|" '{print $1}' | sort -nk1 | tail -1) 
echo "Max_Seq=$maxSeq"
if [ -z "$maxSeq" ]
	then
	echo "couldn't find new records"
	rm $filename
else
	echo $maxSeq >>/mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/staging/maxSeq.txt
	 dt="$(date +"%H%M%S")"
	echo "time=$dt"
	sort -t"|" -k14 -o $filename $filename
	awk  -v date=${dt} -F"|" '{print > "/mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/staging/CB_SERV_MAST_VIEW_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
	files=(/mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/staging/CB_SERV_MAST_VIEW_LIVE*)
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
					hadoop fs -mkdir -p /FlareData/output_8/CB_SERV_MAST_VIEW_LIVE/tbl_dt=${tbl_dt}
					hadoop fs -put "${var}.gz" /FlareData/output_8/CB_SERV_MAST_VIEW_LIVE/tbl_dt=${tbl_dt}
					mkdir -p /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/old/${tbl_dt}
					mv "${var}.gz" /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/old/${tbl_dt}/
			fi
	done
	mv $filename ${tbl_dt}_$dt_$filename
	gzip -f ${tbl_dt}_$dt_$filename 

#mv $filename "$filename_$(date +"%Y-%m-%d_%H-%M-%S").csv.gz"
fi
echo "Max_Seq=$maxSeq"
hive -e "msck repair table flare_8.CB_SERV_MAST_VIEW_LIVE"

#awk -v date="$(date +"%Y%m%d%H%M%S")" -F"|" '{print > "/mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/staging/CB_SERV_MAST_VIEW_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
#gzip -f $filename >$filename$(date +"%Y-%m-%d_%H-%M-%S")
rm $PIDFILE

echo "Job: Extract CB_SERV_MAST_VIEW_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")"
