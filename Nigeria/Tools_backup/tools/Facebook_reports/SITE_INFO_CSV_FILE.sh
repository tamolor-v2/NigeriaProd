Date_run=`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select cast(date_format(date_trunc('day', current_date), '%Y-%m-%d') as Varchar) as date_time; "`
DateRun=${Date_run//\"}

yest=$(date -d '-0 day' '+%Y%m%d')
vprevmon1=$(date "+%Y%m01" -d "$DateRun -1 month -1 day");
vlastday1=$(date "+%Y-%m-%d" -d "$vprevmon1 +1 month -1 day");

year=`date +%Y`
quarter=$(( ($(date -d $vlastday1 +%-m)-1)/3+1 )) #$(( ($(date +%-m)-1)/3+1 ))
tempFileName="facebook_fbc_nwi.DSD.raw_fb_mtn_site_info_${vlastday1}_CY${year}Q${quarter}_ng.csv"
 
mkdir -p /nas/share05/archived//GLOBAL_CONNECT/SITE_INFO/${yest}/
 
/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format TSV_HEADER --execute "select country,site_id,latitude_deg,longitude_deg,status,operation_date,active_ran_list,backhaul,capacity_uplink_kbps,capacity_downlink_kbps,fiber_date,site_type,date_time from stage.vw_fb_site_info_report where lower(status) <> 'status'" | tr '\t' ',' > /nas/share05/archived//GLOBAL_CONNECT/SITE_INFO/$yest/${tempFileName}

#sed -i "s/\"//g" /nas/share05/archived//GLOBAL_CONNECT/SITE_INFO/$yest/${tempFileName}
#scp /nas/share05/archived//GLOBAL_CONNECT/SITE_INFO/$yest/${tempFileName} 10.1.197.142:/data/data_lz/beegfs/GC_REPORTS/SITE_INFORMATION/
scp /nas/share05/archived//GLOBAL_CONNECT/SITE_INFO/$yest/${tempFileName} 10.1.197.142:/ftpout/FACEBOOK/MTNNG_EVA/
