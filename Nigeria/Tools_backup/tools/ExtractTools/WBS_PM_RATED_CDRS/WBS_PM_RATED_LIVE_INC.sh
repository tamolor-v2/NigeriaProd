#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

day=$1
yesterday=`date  "+%Y%m%d" -d "1 day ago"`
cd /nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
yyyymmdd=`date  "+%Y%m%d"`
date_key=`date  "+%Y-%m-%d"`
echo "$yyyymmdd"
#kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
dt="$(date +"%H%M%S")"
processDay=`date  "+%Y%m%d" -d "${day} day ago"`
 dt=$(date +"%H%M%S")
date=$yyyymmdd
today=$yyyymmdd
filename="${yyyymmdd}_WBS_PM_RATED_LIVE_${yyyymmdd}_${dt}.csv"
#maxSeq=$( tail -n 1 /nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/maxSeq_${day}.txt)
maxSeq=$(/opt/presto/bin/presto   --server master01004:8099 --catalog hive5 --execute  "select date_format(max(date_parse(process_date,'%Y-%m-%d %H:%i:%s.%f')),'%Y%m%d%H%i%s') from flare_8.wbs_pm_rated_cdrs where tbl_dt=$today group by tbl_dt order by tbl_dt"| tr -d '"')

echo "MaxSeq=$maxSeq"
echo "processDay=$processDay"
#$(</nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/maxSeq_${msisdnLastDigit}.txt)
echo "started spooling $filename"
echo "$filename"
echo "maxSeq=$maxSeq"
#echo "select /*+ parallel 12 */ * from wbs_client.wbs_cdr_${processDay} where process_date > to_timestamp('${maxSeq}','yyyymmddhh24missFF') and ANUM is not null"
echo "select /*+ parallel 12 */ cdr_file_no||'|'|| cdr_uci_no||'|'|| cdr_uci_element||'|'|| cdr_key_id||'|'|| call_scenario||'|'|| remuneration_indicator||'|'|| billing_base_direction||'|'|| franchise||'|'|| product_group||'|'|| reverse_indicator||'|'|| event_direction||'|'|| derived_event_direction||'|'|| billing_rating_scenario_type||'|'|| billed_product||'|'|| rating_scenario||'|'|| rating_rule_dependency_ind||'|'|| estimate_indicator||'|'|| accounting_method||'|'|| cash_flow||'|'|| component_direction||'|'|| statement_direction||'|'|| billing_method||'|'|| rating_component||'|'|| call_attempt_indicator||'|'|| parameter_matrix||'|'|| parameter_matrix_line||'|'|| step_number||'|'|| rate_type||'|'|| time_premium||'|'|| billing_operator||'|'|| rate_owner_operator||'|'|| settlement_operator||'|'|| rate_name||'|'|| tier||'|'|| tier_group||'|'|| tier_type||'|'|| optimum_poi||'|'|| adhoc_summary_ind||'|'|| billing_summary_ind||'|'|| complementary_summary_ind||'|'|| traffic_summary_ind||'|'|| minimum_charge_ind||'|'|| call_duration_bucket||'|'|| time_of_day_bucket||'|'|| user_summarisation||'|'|| incoming_node||'|'|| incoming_product||'|'|| incoming_operator||'|'|| incoming_path||'|'|| incoming_poi||'|'|| outgoing_node||'|'|| outgoing_product||'|'|| outgoing_operator||'|'|| outgoing_path||'|'|| outgoing_poi||'|'|| traffic_rating_scenario_type||'|'|| traffic_route_type||'|'|| traffic_route||'|'|| traffic_agreement_operator||'|'|| traffic_negotiation_dir||'|'|| billing_route_type||'|'|| billing_route||'|'|| billing_agreement_operator||'|'|| billing_negotiation_dir||'|'|| route_identifier||'|'|| refile_indicator||'|'|| agreement_naag_anum_level||'|'|| agreement_naag_anum||'|'|| agreement_naag_bnum_level||'|'|| agreement_naag_bnum||'|'|| world_view_naag_anum||'|'|| world_view_anum||'|'|| world_view_naag_bnum||'|'|| world_view_bnum||'|'|| traffic_base_direction||'|'|| data_unit||'|'|| data_unit_2||'|'|| data_unit_3||'|'|| data_unit_4||'|'|| discrete_rating_parameter_1||'|'|| discrete_rating_parameter_2||'|'|| discrete_rating_parameter_3||'|'|| revenue_share_currency_1||'|'|| revenue_share_currency_2||'|'|| revenue_share_currency_3||'|'|| anum_operator||'|'|| anum_cnp||'|'|| traffic_naag||'|'|| naag_anum_level||'|'|| recon_naag_anum||'|'|| network_address_aggr_anum||'|'|| network_type_anum||'|'|| bnum_operator||'|'|| bnum_cnp||'|'|| naag_bnum_level||'|'|| network_type_bnum||'|'|| recon_naag_bnum||'|'|| network_address_aggr_bnum||'|'|| derived_product_indicator||'|'|| anum||'|'|| bnum||'|'|| record_sequence_number||'|'|| user_data||'|'|| user_data_2||'|'|| user_data_3||'|'|| call_count||'|'|| start_call_count||'|'|| rate_step_call_count||'|'|| apportioned_call_count||'|'|| apportioned_duration_seconds||'|'|| actual_usage||'|'|| charged_usage||'|'|| charged_units||'|'|| event_duration||'|'|| minimum_charge_adjustment||'|'|| maximum_charge_adjustment||'|'|| network_duration||'|'|| data_volume||'|'|| data_volume_2||'|'|| data_volume_3||'|'|| data_volume_4||'|'|| revenue_share_amount_1||'|'|| revenue_share_amount_2||'|'|| revenue_share_amount_3||'|'|| base_amount||'|'|| amount||'|'|| dlys_detail_id||'|'|| traffic_period||'|'|| to_char(message_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'|| to_char(billing_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'|| to_char(adjusted_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'|| to_char(process_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'|| to_char(event_start_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'|| event_start_time||'|'|| network_start_date||'|'|| network_start_time||'|'|| billing_start_time||'|'|| billing_end_time||'|'|| flat_rate_charge||'|'|| rate_step_flat_charge||'|'|| unit_cost_used||'|'|| charge_bundle||'|'|| base_amount_factor_used||'|'|| refund_factor||'|'|| currency||'|'|| currency_conversion_rate||'|'|| base_unit||'|'|| rate_unit||'|'|| rounded_unit_id||'|'|| record_type||'|'|| link_field||'|'|| reason_for_cleardown||'|'|| repair_indicator||'|'|| rated_billing_period||'|'|| traffic_movement_ctr||'|'|| rte_lookup_style||'|'|| cdr_source||'|'|| carrier_destination_name||'|'|| carrier_destination_alias from wbs_client.wbs_cdr_20190109 where ANUM is not null and process_date > to_date('20190109131157','yyyymmddhh24miss') and billing_date >= to_date('20190109 00:00:00','YYYYMMDD HH24:MI:SS') AND billing_date <= to_date('20190109 23:59:59','YYYYMMDD HH24:MI:SS');"
/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_CDR/thispwd#456@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.232.166)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ictwbs_p1)))'
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
cdr_file_no||'|'||
cdr_uci_no||'|'||
cdr_uci_element||'|'||
cdr_key_id||'|'||
call_scenario||'|'||
remuneration_indicator||'|'||
billing_base_direction||'|'||
franchise||'|'||
product_group||'|'||
reverse_indicator||'|'||
event_direction||'|'||
derived_event_direction||'|'||
billing_rating_scenario_type||'|'||
billed_product||'|'||
rating_scenario||'|'||
rating_rule_dependency_ind||'|'||
estimate_indicator||'|'||
accounting_method||'|'||
cash_flow||'|'||
component_direction||'|'||
statement_direction||'|'||
billing_method||'|'||
rating_component||'|'||
call_attempt_indicator||'|'||
parameter_matrix||'|'||
parameter_matrix_line||'|'||
step_number||'|'||
rate_type||'|'||
time_premium||'|'||
billing_operator||'|'||
rate_owner_operator||'|'||
settlement_operator||'|'||
rate_name||'|'||
tier||'|'||
tier_group||'|'||
tier_type||'|'||
optimum_poi||'|'||
adhoc_summary_ind||'|'||
billing_summary_ind||'|'||
complementary_summary_ind||'|'||
traffic_summary_ind||'|'||
minimum_charge_ind||'|'||
call_duration_bucket||'|'||
time_of_day_bucket||'|'||
user_summarisation||'|'||
incoming_node||'|'||
incoming_product||'|'||
incoming_operator||'|'||
incoming_path||'|'||
incoming_poi||'|'||
outgoing_node||'|'||
outgoing_product||'|'||
outgoing_operator||'|'||
outgoing_path||'|'||
outgoing_poi||'|'||
traffic_rating_scenario_type||'|'||
traffic_route_type||'|'||
traffic_route||'|'||
traffic_agreement_operator||'|'||
traffic_negotiation_dir||'|'||
billing_route_type||'|'||
billing_route||'|'||
billing_agreement_operator||'|'||
billing_negotiation_dir||'|'||
route_identifier||'|'||
refile_indicator||'|'||
agreement_naag_anum_level||'|'||
agreement_naag_anum||'|'||
agreement_naag_bnum_level||'|'||
agreement_naag_bnum||'|'||
world_view_naag_anum||'|'||
world_view_anum||'|'||
world_view_naag_bnum||'|'||
world_view_bnum||'|'||
traffic_base_direction||'|'||
data_unit||'|'||
data_unit_2||'|'||
data_unit_3||'|'||
data_unit_4||'|'||
discrete_rating_parameter_1||'|'||
discrete_rating_parameter_2||'|'||
discrete_rating_parameter_3||'|'||
revenue_share_currency_1||'|'||
revenue_share_currency_2||'|'||
revenue_share_currency_3||'|'||
anum_operator||'|'||
anum_cnp||'|'||
traffic_naag||'|'||
naag_anum_level||'|'||
recon_naag_anum||'|'||
network_address_aggr_anum||'|'||
network_type_anum||'|'||
bnum_operator||'|'||
bnum_cnp||'|'||
naag_bnum_level||'|'||
network_type_bnum||'|'||
recon_naag_bnum||'|'||
network_address_aggr_bnum||'|'||
derived_product_indicator||'|'||
anum||'|'||
bnum||'|'||
record_sequence_number||'|'||
user_data||'|'||
user_data_2||'|'||
user_data_3||'|'||
call_count||'|'||
start_call_count||'|'||
rate_step_call_count||'|'||
apportioned_call_count||'|'||
apportioned_duration_seconds||'|'||
actual_usage||'|'||
charged_usage||'|'||
charged_units||'|'||
event_duration||'|'||
minimum_charge_adjustment||'|'||
maximum_charge_adjustment||'|'||
network_duration||'|'||
data_volume||'|'||
data_volume_2||'|'||
data_volume_3||'|'||
data_volume_4||'|'||
revenue_share_amount_1||'|'||
revenue_share_amount_2||'|'||
revenue_share_amount_3||'|'||
base_amount||'|'||
amount||'|'||
dlys_detail_id||'|'||
traffic_period||'|'||
to_char(message_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'||
to_char(billing_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'||
to_char(adjusted_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'||
to_char(process_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'||
to_char(event_start_date,'yyyy-mm-dd hh24:mi:ss')||'.0|'||
event_start_time||'|'||
network_start_date||'|'||
network_start_time||'|'||
billing_start_time||'|'||
billing_end_time||'|'||
flat_rate_charge||'|'||
rate_step_flat_charge||'|'||
unit_cost_used||'|'||
charge_bundle||'|'||
base_amount_factor_used||'|'||
refund_factor||'|'||
currency||'|'||
currency_conversion_rate||'|'||
base_unit||'|'||
rate_unit||'|'||
rounded_unit_id||'|'||
record_type||'|'||
link_field||'|'||
reason_for_cleardown||'|'||
repair_indicator||'|'||
rated_billing_period||'|'||
traffic_movement_ctr||'|'||
rte_lookup_style||'|'||
cdr_source||'|'||
carrier_destination_name||'|'||
carrier_destination_alias
from wbs_client.wbs_cdr_20190109
where ANUM is not null and process_date > to_date('${maxSeq}','yyyymmddhh24miss') and billing_date >= to_date('${date} 00:00:00','YYYYMMDD HH24:MI:SS') AND billing_date <= to_date('${date} 23:59:59','YYYYMMDD HH24:MI:SS');
spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
exit
maxSeq=$(cat $filename | awk -F"|" '{print $53}' | sort -nk1 | tail -1) 
if [[ $maxSeq == *"ERROR"* ]]; then
  echo "Error: maxSeq couldn't be set"
  echo "Max_Seq=$maxSeq"
  exit
fi
echo "Max_Seq=$maxSeq"
if [ -z "$maxSeq" ]
	then
	echo "couldn't find new records"
	rm $filename
else
	echo $maxSeq >>/nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/maxSeq_${processDate}.txt
	 #dt="$(date +"%H%M%S")"
	echo "time=$dt"
	sort -t"|" -k53 -o $filename $filename
	#awk  -v date=${dt} -F"|" '{print > "/nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/WBS_PM_RATED_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
	#files=(/nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/spool/WBS_PM_RATED_LIVE*)
	tbl_dt=$processDay
	gzip -f $filename
DIRECTORY="hdfs://ngdaas/FlareData/output_8/WBS_PM_RATED_CDRS_LIVE/tbl_dt=${tbl_dt}"
full_path="${DIRECTORY}/*"

hadoop fs -test -d $DIRECTORY
    if [ $? == 0 ]
            then
                echo "Path for date: ${tbl_dt} exists"
	    else
		hadoop fs -mkdir -p /FlareData/output_8/WBS_PM_RATED_CDRS_LIVE/tbl_dt=${tbl_dt}/
                hive -e "msck repair table flare_8.WBS_PM_RATED_CDRS_LIVE"	
	    fi
                hadoop fs -put "${filename}.gz" /FlareData/output_8/WBS_PM_RATED_CDRS_LIVE/tbl_dt=${tbl_dt}
                mkdir -p /nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/old/${tbl_dt}
                mv "${filename}.gz" /nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/old/${tbl_dt}
fi
echo "Max_Seq=$maxSeq"
echo $maxSeq >>/nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/maxSeq_${Day}.txt
#awk -v date="$(date +"%Y%m%d%H%M%S")" -F"|" '{print > "/nas/share05/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/WBS_PM_RATED_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
#gzip -f $filename >$filename$(date +"%Y-%m-%d_%H-%M-%S")

echo "Job: Extract WBS_PM_RATED_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")"
