start transaction;
delete from cvm_db.CVM20_REFILL_AND_DATA_BUNDLE_TMP1 where tbl_dt = yyyymmddRunDate and attribute_type= 'weekly' ; 
insert into cvm_db.cvm20_refill_and_data_bundle_tmp1  
( msisdn_key,weekid,mon_sbsc_amt,tue_sbsc_amt,wed_sbsc_amt,thu_sbsc_amt,fri_sbsc_amt,sat_sbsc_amt,sun_sbsc_amt,mon_sbsc_cnt,
tue_sbsc_cnt,wed_sbsc_cnt,thu_sbsc_cnt,fri_sbsc_cnt,sat_sbsc_cnt,sun_sbsc_cnt,tot_sbsc_amt, sbsc_fb_amt ,
sbsc_count_fb ,sbsc_count_ussd ,sbsc_ussd_amt ,sbsc_count_rs ,sbsc_rs_amt ,sbsc_others_amt ,sbsc_count_others ,
total_data_bundle_count ,total_combo_bundle_count,sbsc_count_voucher,sbsc_voucher_amt,total_sms_bundle_count,
total_voice_bundle_count,total_vas_bundle_count,max_rch_deno,max_sbsc_deno ,total_other_bundle_count, total_bundle_count, attribute_type, tbl_dt) 
SELECT
  msisdn_key
, cast ("date_format"("date_trunc"('week', "date_parse"(CAST(tbl_dt AS varchar(10)), '%Y%m%d')), '%Y%m%d')  as varchar)
, "sum"((CASE WHEN (dow = 'MON') THEN totalcharge_money ELSE 0 END)) mon_sbsc_amt
, "sum"((CASE WHEN (dow = 'TUE') THEN totalcharge_money ELSE 0 END)) tue_sbsc_amt
, "sum"((CASE WHEN (dow = 'WED') THEN totalcharge_money ELSE 0 END)) wed_sbsc_amt
, "sum"((CASE WHEN (dow = 'THU') THEN totalcharge_money ELSE 0 END)) thu_sbsc_amt
, "sum"((CASE WHEN (dow = 'FRI') THEN totalcharge_money ELSE 0 END)) fri_sbsc_amt
, "sum"((CASE WHEN (dow = 'SAT') THEN totalcharge_money ELSE 0 END)) sat_sbsc_amt
, "sum"((CASE WHEN (dow = 'SUN') THEN totalcharge_money ELSE 0 END)) sun_sbsc_amt
, "sum"((CASE WHEN (dow = 'MON') THEN 1 ELSE 0 END)) mon_sbsc_cnt
, "sum"((CASE WHEN (dow = 'TUE') THEN 1 ELSE 0 END)) tue_sbsc_cnt
, "sum"((CASE WHEN (dow = 'WED') THEN 1 ELSE 0 END)) wed_sbsc_cnt
, "sum"((CASE WHEN (dow = 'THU') THEN 1 ELSE 0 END)) thu_sbsc_cnt
, "sum"((CASE WHEN (dow = 'FRI') THEN 1 ELSE 0 END)) fri_sbsc_cnt
, "sum"((CASE WHEN (dow = 'SAT') THEN 1 ELSE 0 END)) sat_sbsc_cnt
, "sum"((CASE WHEN (dow = 'SUN') THEN 1 ELSE 0 END)) sun_sbsc_cnt
, "sum"(totalcharge_money) tot_sbsc_amt
, "sum"((CASE WHEN ("upper"(channel) IN ('FB')) THEN totalcharge_money ELSE 0 END)) sbsc_FB_amt
, "sum"((CASE WHEN ("upper"(channel) IN ('FB')) THEN 1 ELSE 0 END)) sbsc_count_FB
, "sum"((CASE WHEN ("upper"(channel) IN ('USSD')) THEN 1 ELSE 0 END)) sbsc_count_USSD
, "sum"((CASE WHEN ("upper"(channel) IN ('USSD')) THEN totalcharge_money ELSE 0 END)) sbsc_USSD_amt
, "sum"((CASE WHEN ("upper"(channel) IN ('RS')) THEN 1 ELSE 0 END)) sbsc_count_RS
, "sum"((CASE WHEN ("upper"(channel) IN ('RS')) THEN totalcharge_money ELSE 0 END)) sbsc_RS_amt
, "sum"((CASE WHEN (NOT ("upper"(channel) IN ('FB', 'USSD', 'RS'))) THEN totalcharge_money ELSE 0 END)) sbsc_Others_amt
, "sum"((CASE WHEN (NOT ("upper"(channel) IN ('FB', 'USSD', 'RS'))) THEN 1 ELSE 0 END)) sbsc_count_Others
, "sum"((CASE WHEN (bundle_type = 'DATA') THEN 1 ELSE 0 END)) total_data_bundle_count
, "sum"((CASE WHEN (bundle_type = 'COMBO') THEN 1 ELSE 0 END)) total_combo_bundle_count
, "sum"((CASE WHEN (channel IN ('EVD', 'DYA', 'VTU')) THEN 1 ELSE 0 END)) sbsc_count_voucher
, "sum"((CASE WHEN (channel IN ('EVD', 'DYA', 'VTU')) THEN totalcharge_money ELSE 0 END)) sbsc_voucher_amt
, "sum"((CASE WHEN ((channel IN ('SMS')) OR (channel LIKE 'SP%SMS%')) THEN 1 ELSE 0 END)) total_sms_bundle_count
, "sum"((CASE WHEN (bundle_type = 'VOICE') THEN 1 ELSE 0 END)) total_voice_bundle_count
, "sum"((CASE WHEN (channel IN ('USSD', 'SMARTAPP', 'SP Smart APP', 'MTNONLINE')) THEN 1 ELSE 0 END)) total_vas_bundle_count
, "max"((CASE WHEN (channel IN ('EVD', 'DYA', 'VTU')) THEN totalcharge_money ELSE 0 END)) max_rch_deno
, "max"(totalcharge_money) max_sbsc_deno
, "sum"((CASE WHEN ("upper"(channel) LIKE '%OTHER%') THEN 1 ELSE 0 END)) total_other_bundle_count
, "count"(*) total_bundle_count
, 'weekly' attribute_type
, cast ("date_format"("date_trunc"('week', "date_parse"(CAST(tbl_dt AS varchar(10)), '%Y%m%d')), '%Y%m%d')  as int)   
FROM
 cvm_db.cvm20_vw_refill_and_data_bundle_uat a
WHERE a.tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
GROUP BY msisdn_key, 
 cast ("date_format"("date_trunc"('week', "date_parse"(CAST(tbl_dt AS varchar(10)), '%Y%m%d')), '%Y%m%d')  as varchar),
cast ("date_format"("date_trunc"('week', "date_parse"(CAST(tbl_dt AS varchar(10)), '%Y%m%d')), '%Y%m%d')  as int)   ; 
commit;
