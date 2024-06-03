#! /bin/bash
yest=$(date -d '-2 day' '+%Y%m%d')

mytime() {
date +"%Y%m%d"
}
schemaName="flare_8"
declare -a feedsArray=(BUNDLE4U_GPRS BUNDLE4U_VOICE CB_SERV_MAST_VIEW CS5_AIR_ADJ_DA CS5_AIR_ADJ_MA CS5_AIR_REFILL_AC CS5_AIR_REFILL_DA CS5_AIR_REFILL_MA CS5_CCN_GPRS_AC CS5_CCN_GPRS_DA CS5_CCN_GPRS_MA CS5_CCN_SMS_AC CS5_CCN_SMS_DA CS5_CCN_SMS_MA CS5_CCN_VOICE_AC CS5_CCN_VOICE_DA CS5_CCN_VOICE_MA CS5_SDP_ACC_ADJ_AC CS5_SDP_ACC_ADJ_DA CS5_SDP_ACC_ADJ_MA CUG_ACCESS_FEES DAY0 DMC_DUMP_ALL FIN_LOG GGSN_CDR HSDP_CDR MOBILE_MONEY MSC_CDR SDP_DMP_MA SGSN_CDR WBS_PM_RATED_CDRS MAPS2G MAPS3G MAPS4G RECON )
for feed in "${feedsArray[@]}"
do
echo "processing: $schemaName.$feed"
hasData=`hive -S -e "select 1 from $schemaName.$feed where tbl_dt=$yest limit 1"`
OUT=$?
if [ $OUT -eq 0 ];then
if [ $hasData -eq 1 ]
then 
echo "$schemaName.$feed has data for partition $yest"
else
echo "Warning: $schemaName.$feed  has no data for partition $yest"
fi
else
echo "ERROR: $schemaName.$feed  has an exception with  partition $yest"
fi
done
