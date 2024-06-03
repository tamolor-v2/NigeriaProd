use databasename;
insert overwrite table flaretest.Directory_filesopps_summary partition (tbl_dt) 
select feed_name,sum(lines),tbl_dt from
 (select distinct feed_name,name,lines,tbl_dt from
 (select trim(case when path like "%VOICE_MA%"  then  "CCN_VOICE_MA"
 when path like "%VOICE_DA%"  then  "CCN_VOICE_DA"
 when path like "%SMS_MA%"  then  "CCN_SMS_MA  "
 when path like "%SMS_DA%"  then  "CCN_SMS_DA"
 when path like "%GPRS_MA%"  then  "CCN_GPRS_MA"
 when path like "%GPRS_DA%"  then  "CCN_GPRS_DA"
 when path like "%DUMP_MA%"  then  "SDP_DUMP_MA_CSV"
 when path like "%DUMP_DA%"  then  "SDP_DUMP_DA_CSV"
 when path like "%SDP_ACC_ADJ_MA%"  then  "SDP_ACC_ADJ_MA"
 when path like "%SDP_ACC_ADJ_DA%"  then  "SDP_ACC_ADJ_DA"
 when path like "%EOD_USER_ACC_BAL%"  then  "EWP_EOD_USER_ACC_BAL"
 when path like "%EOD_REGISTRATIONS%"  then  "EWP_EOD_REGISTRATIONS"
 when path like "%EOD_TRANSACTIONS%"  then  "EWP_EOD_TRANSACTIONS"
 when path like "%FIN_LOG%"  then  "FIN_LOG"
 when path like "%EOD_CUSTODY_ACC_BAL%"  then  "EWP_EOD_CUSTODY_ACC_BAL"
 when path like "%ADJ_MA%"  then  "AIR_ADJ_MA"
 when path like "%ADJ_DA%"  then  "AIR_ADJ_DA"
 when path like "%REFILL_MA%"  then  "AIR_REFILL_MA"
 when path like "%REFILL_DA%"  then  "AIR_REFILL_DA"
 when path like "%GGSN%"  then  "GGSN_CDR"
 when path like "%EVD_CDR%"  then  "EVD_CDR"
 when path like "%MSC_CDR%"  then  "MSC_CDR"
 when path like "%ADC_SUBS_HANDSETS%"  then  "ADC_SUBS_HANDSETS"
 when path like "%PM_RATED%"  then  "PM_RATED"
 when path like "%GSM_SERV_MASTER%"  then  "GSM_SERV_MASTER"
 else "UK" end) feed_name ,coalesce(cast(CASE																
WHEN path LIKE "%VOICE_MA%" 
THEN substring(name,1,8) 
WHEN path LIKE "%VOICE_DA%" 
THEN substring(name,1,8)  
WHEN path LIKE "%SMS_MA%"
THEN substring(name,1,8)  
WHEN path LIKE "%SMS_DA%"  
THEN substring(name,1,8)  
WHEN path LIKE "%GPRS_MA%" 
THEN substring(name,1,8)  
WHEN path LIKE "%GPRS_DA%" 
THEN substring(name,1,8)  
WHEN path LIKE "%DUMP_MA%" 
THEN  from_unixtime(unix_timestamp(substring(substring(name,INSTR(name,'subscriber')+11),1,11)  
	,"dd-MMM-yyyy"),"yyyyMMdd")  
WHEN path LIKE "%DUMP_DA%" 
THEN  from_unixtime(unix_timestamp(substring(substring(name,INSTR(name,'dedicatedacc')+13),1,11)
,"dd-MMM-yyyy"),"yyyyMMdd")  
WHEN path LIKE "%SDP_ACC_ADJ_MA%"
THEN 
  substring(name,1,8) 
WHEN path LIKE "%SDP_ACC_ADJ_DA%"
THEN  substring(name,1,8)
WHEN path LIKE "%EOD_USER_ACC_BAL%" 
THEN substring(substring(name,INSTR(name,'user-account-balances')+22),1,8)  
WHEN path LIKE "%EOD_REGISTRATIONS%"
THEN 
 substring(substring(name,INSTR(name,'registrations')+14),1,8) 
WHEN path LIKE "%EOD_TRANSACTIONS%" 
THEN 
  substring(substring(name,INSTR(name,'transactions')+13),1,8) 
WHEN path LIKE "%FIN_LOG%" 
THEN  substring(substring(name,INSTR(name,'financiallog')+13),1,8)
WHEN path LIKE "%EOD_CUSTODY_ACC_BAL%" 
THEN substring(substring(name,INSTR(name,'system-account-balances')+24),1,8)
WHEN path LIKE "%ADJ_MA%"  
THEN substring(name,1,8)  
WHEN path LIKE "%ADJ_DA%"  
THEN substring(name,1,8)  
WHEN path LIKE "%REFILL_MA%"  
THEN substring(name,1,8)  
  
WHEN path LIKE "%REFILL_DA%"  
THEN substring(name,1,8)  
  
WHEN path LIKE "%GGSN%" 
THEN substring(name,1,8)  
  
WHEN path LIKE "%EVD_CDR%" 
THEN substring(name,12,8)
WHEN path LIKE "%MSC_CDR%" 
THEN substring(name,1,8)  
WHEN path LIKE "%ADC_SUBS_HANDSET%" 
THEN substring(substring(name,INSTR(name,'subscribers')+12),1,8)  
WHEN path LIKE "%PM_RATED%"
THEN  "19700101" 
  
WHEN path LIKE "%GSM_SERV_MASTER%"  
THEN "19700101"
  
ELSE 
  "19700101"  
 END as int ),19700101)  tbl_dt  ,* from flaretest.files_filesopps_summary ) FilesOps  
) a group by  
feed_name,tbl_dt

