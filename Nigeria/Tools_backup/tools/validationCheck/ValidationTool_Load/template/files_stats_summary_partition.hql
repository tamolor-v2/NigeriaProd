set hive.exec.dynamic.partition.mode=nonstrict;
set tez.am.resource.memory.mb = 3048;
set hive.tez.container.size=32096;
set tez.runtime.io.sort.mb = 4090;
insert overwrite table databasename.files_count_stats_summary partition (file_dt, tbl_dt,feedname)   
SELECT path,   
split(path,"\/")[size(split(path,"\/"))-1] ,
processedrecordscount,
recordscount,
status,
coalesce(cast(
CASE  WHEN feed_name LIKE "%VOICE_MA%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)  
WHEN feed_name LIKE "%VOICE_DA%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%VOICE_AC%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%SMS_MA%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%SMS_DA%"   
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%SMS_AC%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%GPRS_MA%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%GPRS_DA%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%GPRS_AC%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%REFILL_MA%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%REFILL_DA%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%REFILL_AC%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%SDP_ACC_ADJ_MA%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)  
WHEN feed_name LIKE "%SDP_ACC_ADJ_DA%"  
THEN  substring(substring(path,INSTR(path,'incoming')+9),1,8) 
WHEN feed_name LIKE "%SDP_ACC_ADJ_AC%"  
THEN  substring(substring(path,INSTR(path,'incoming')+9),1,8) 
WHEN feed_name LIKE "%AIR_ADJ_MA%"   
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%AIR_ADJ_DA%"   
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%MSC%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%BUNDLE4U_VOICE%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%BUNDLE4U_GPRS%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%GGSN%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)   
WHEN feed_name LIKE "%SGSN%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)  
WHEN feed_name LIKE "%MAPS2G%"  
THEN substring(substring(path,INSTR(path,'Nigeria_BIB_2GCell_INV')+23),1,8)   
WHEN feed_name LIKE "%MAPS3G%"  
THEN substring(substring(path,INSTR(path,'Nigeria_BIB_3GCell_INV')+23),1,8)   
WHEN feed_name LIKE "%MAPS4G%"  
THEN substring(substring(path,INSTR(path,'Nigeria_BIB_4GCell_INV')+23),1,8)   
WHEN feed_name LIKE "%FIN_LOG%"  
THEN substring(substring(path,INSTR(path,'financiallog')+13),1,8)   
WHEN feed_name LIKE "%DMC_DUMP_ALL%"  
THEN substring(substring(path,INSTR(path,'ALL_HANDSET_DATA')+17),1,8)   
WHEN feed_name LIKE "%HSDP_CDR%"  
THEN substring(substring(path,INSTR(path,'HSDP_live')+10),1,8)   
WHEN feed_name LIKE "%WBS_PM_RATED_CDRS%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)  
WHEN feed_name LIKE "%CUG_ACCESS_FEES%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)  
WHEN feed_name LIKE "%CB_SERV_MAST_VIEW%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),1,8)  
WHEN feed_name LIKE "%SDP_DMP_MA%"  
THEN substring(substring(path,INSTR(path,'incoming')+9),8,8)  
WHEN feed_name LIKE "%MOBILE_MONEY%"  
THEN from_unixtime(unix_timestamp(substring(substring(path,INSTR(path,'incoming')+9),1,8) ,'MMddyyyy'), 'yyyyMMdd')   
WHEN feed_name LIKE "%RECON%"
THEN substring(split(path,'_')[2],1,8)   
ELSE    "19700101"    END as int ),19700101)  file_dt, 
tbl_dt, 
feed_name
from databasename.files_count_stats_detailes where tbl_dt>=predate and seq=1;
