set tez.am.resource.memory.mb = 2048;
set hive.tez.container.size=8096;
set tez.runtime.io.sort.mb = 409;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
 insert overwrite table databasename.files_filesopps_summary_partition partition (feed_name,file_dt) 
 select distinct name,lines,feed_name,file_dt from
 (select name,lines,trim(case 
 when path like "%VOICE_AC%" then "CS5_CCN_VOICE_AC"
 when path like "%VOICE_MA%" then "CS5_CCN_VOICE_MA"
 when path like "%VOICE_DA%" then "CS5_CCN_VOICE_DA"
 when path like "%SMS_AC%" then "CS5_CCN_SMS_AC"
 when path like "%SMS_MA%" then "CS5_CCN_SMS_MA"
 when path like "%SMS_DA%" then "CS5_CCN_SMS_DA"
 when path like "%GPRS_AC%" then "CS5_CCN_GPRS_AC"
 when path like "%GPRS_MA%" then "CS5_CCN_GPRS_MA"
 when path like "%GPRS_DA%" then "CS5_CCN_GPRS_DA"
 when path like "%SDP_ACC_ADJ_AC%" then "CS5_SDP_ACC_ADJ_AC"
 when path like "%SDP_ACC_ADJ_MA%" then "CS5_SDP_ACC_ADJ_MA"
 when path like "%SDP_ADJ_DA%" then "CS5_SDP_ACC_ADJ_DA"
 when path like "%FINANCIAL_LOG%" then "FIN_LOG"
 when path like "%ADJ_MA%" then "CS5_AIR_ADJ_MA"
 when path like "%ADJ_DA%" then "CS5_AIR_ADJ_DA"
 when path like "%REFILL_AC%" then "CS5_AIR_REFILL_AC"
 when path like "%REFILL_MA%" then "CS5_AIR_REFILL_MA"
 when path like "%REFILL_DA%" then "CS5_AIR_REFILL_DA"
 when path like "%GGSN%" then "GGSN_CDR"
 when path like "%MSC%" then "MSC_CDR"
 when path like "%WBS_PM_RATED_CDRS%" then "WBS_PM_RATED_CDRS"
 when path like "%CB_SERV_MAST_VIEW%" then "CB_SERV_MAST_VIEW"
 when path like "%BUNDLE4U_VOICE%" then "BUNDLE4U_VOICE"
 when path like "%BUNDLE4U_GPRS%" then "BUNDLE4U_GPRS"
 when path like "%RECON%" then "RECON"
 when path like "%SGSN%" then "SGSN_CDR"
 when path like "%MAPS_INV_2G%" then "MAPS2G"
 when path like "%MAPS_INV_3G%" then "MAPS3G"
 when path like "%MAPS_INV_4G%" then "MAPS4G"
 when path like "%DMC_DUMP_ALL%" then "DMC_DUMP_ALL"
 when path like "%/HSDP/%"  then "HSDP_CDR"
 when path like "%MOBILE_MONEY%" then "MOBILE_MONEY"
 when path like "%CUG_ACCESS%" then "CUG_ACCESS_FEES" 
 when path like "%SDP_DMP_MA%" then "SDP_DMP_MA"
 when path like "%SDP_DMP_DA%" then "SDP_DMP_DA"
 else "UK" end) feed_name,case when upper(path) like "%PART%" then 1907 else nvl(split(substring(substring(path,INSTR(path,'ArchiveKeyWork')+3),1),"/")[2],1970) end file_dt 
 from databasename.files_filesopps_summary where lines > 0 and path like "%targetdate%") FilesOps ;
