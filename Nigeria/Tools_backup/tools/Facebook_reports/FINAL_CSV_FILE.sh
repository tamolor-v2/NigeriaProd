#Date_run=`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select cast(date_format(date_trunc('day', current_date), '%Y-%m-%d') as Varchar) as date_time; "`
#DateRun=${Date_run//\"}

yest=$1 #(date -d '-0 day' '+%Y%m%d')
#vprevmon1=$(date "+%Y%m01" -d "$DateRun -1 month -1 day");
#vlastday1=$(date "+%Y-%m-%d" -d "$vprevmon1 +1 month -1 day");
#year=`date +%Y`
#quarter=$(( ($(date +%-m)-1)/3+1 ))
schemaName=$2
tempFileName="fb_report_sla_dq_monthly_ng_${yest}.csv" #facebook_fbc_nwi.DSD.raw_fb_mtn_cell_info_${vlastday1}_CY${year}Q${quarter}_ng.csv"
 
mkdir -p /nas/share05/archived/GLOBAL_CONNECT/DASHBOARD_FILES/${yest}/
 
/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema $schemaName --output-format TSV_HEADER --execute "select * from flare_8.fb_report_sla_dq_monthly" | tr '\t' ',' > /nas/share05/archived/GLOBAL_CONNECT/DASHBOARD_FILES/$yest/${tempFileName}

scp /nas/share05/archived/GLOBAL_CONNECT/DASHBOARD_FILES/$yest/${tempFileName} 10.1.197.142:/data/data_lz/beegfs/GC_REPORTS/DASHBOARD_FILES
