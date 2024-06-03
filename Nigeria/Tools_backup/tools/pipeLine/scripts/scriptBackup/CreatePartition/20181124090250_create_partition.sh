#!/bin/bash
hdfs_dir=/FlareData/output_8
db_name=flare_8
number_of_days=7
start_date=$(date -d '+1 day' '+%Y%m%d')
for feed in CS5_CCN_VOICE_AC CS5_CCN_VOICE_DA CS5_CCN_VOICE_MA CS5_CCN_SMS_AC CS5_CCN_SMS_DA CS5_CCN_SMS_MA CS5_CCN_GPRS_AC CS5_CCN_GPRS_DA CS5_CCN_GPRS_MA CS5_AIR_REFILL_AC CS5_AIR_REFILL_DA CS5_AIR_REFILL_MA CS5_SDP_ACC_ADJ_AC CS5_SDP_ACC_ADJ_DA CS5_SDP_ACC_ADJ_MA CS5_AIR_ADJ_DA CS5_AIR_ADJ_MA MSC_CDR BUNDLE4U_VOICE BUNDLE4U_GPRS GGSN_CDR RECON SGSN_CDR WBS_PM_RATED_CDRS MAPS2G MAPS3G MAPS4G FIN_LOG DMC_DUMP_ALL HSDP_CDR MOBILE_MONEY CB_SERV_MAST_VIEW CUG_ACCESS_FEES SDP_DMP_MA MVAS_DND_MSISDN_REPORT MNP_PORTING_BROADCAST CALL_REASON NEWREG_BIOUPDT_POOL AGL_CRM_COUNTRY_MAP AGL_CRM_LGA_MAP AGL_CRM_STATE_MAP NEWREG_BIOUPDT_POOL_WEEKLY NEWREG_BIOUPDT_POOL_DAILY UDC_DUMP SDP_DMP_DA  MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS MSC_DAAS
do
  kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
  echo $feed
  for ((i=0;i<${number_of_days};i++)) 
  do
    day=`date --date="${start_date} +$i day" +%Y%m%d`
    echo "hdfs dfs -mkdir ${hdfs_dir}/${feed}/tbl_dt\=$day"
    hdfs dfs -mkdir ${hdfs_dir}/${feed}/tbl_dt\=$day
  done
  echo "msck repair table ${db_name}.$feed"
  hive -e "msck repair table ${db_name}.$feed"
done
