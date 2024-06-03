Date_run=`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select cast(date_format(date_trunc('day', current_date), '%Y-%m-%d') as Varchar) as date_time; "`
DateRun=${Date_run//\"}

yest=$(date -d '-0 day' '+%Y%m%d')
vprevmon1=$(date "+%Y%m01" -d "$DateRun -1 month -1 day");
vlastday1=$(date "+%Y-%m-%d" -d "$vprevmon1 +1 month -1 day");
year=`date +%Y`
quarter=$(( ($(date -d $vlastday1 +%-m)-1)/3+1 )) #$(( ($(date +%-m)-1)/3+1 ))
tempFileName="facebook_fbc_nwi.DSD.raw_fb_mtn_pop_data_info_${vlastday1}_CY${year}Q${quarter}_ng.csv"
 
mkdir -p /nas/share05/archived//GLOBAL_CONNECT/POPULATION_DATA_SOURCE/${yest}/
 
/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format TSV_HEADER --execute "select distinct 'NG' as country, pop_source , pop_source_vers, pop_source_link , growth_source , growth_source_vers , growth_source_link from flare_8.POPULATION_DS s where tbl_dt in (select max(tbl_dt) from flare_8.POPULATION_DS) and s.pop_source <>''" | tr '\t' ',' > /nas/share05/archived//GLOBAL_CONNECT/POPULATION_DATA_SOURCE/$yest/${tempFileName}

#sed -i "s/\"//g" /nas/share05/archived//GLOBAL_CONNECT/POPULATION_DATA_SOURCE/$yest/${tempFileName}
scp /nas/share05/archived//GLOBAL_CONNECT/POPULATION_DATA_SOURCE/$yest/${tempFileName} 10.1.197.142:/ftpout/FACEBOOK/MTNNG_EVA/ #/data/data_lz/beegfs/GC_REPORTS/POPULATION_DATA_SOURCE/
