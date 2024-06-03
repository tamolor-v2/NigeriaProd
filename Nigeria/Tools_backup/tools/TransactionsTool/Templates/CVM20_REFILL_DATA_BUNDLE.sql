start transaction;
delete from cvm_db.CVM20_REFILL_AND_DATA_BUNDLE_UAT_TMP2 where  tbl_dt=yyyymmddRunDate;
insert into cvm_db.cvm20_refill_and_data_bundle_uat_tmp2
(msisdn_key,weekid  ,mon_sbsc_amt,tue_sbsc_amt,wed_sbsc_amt,thu_sbsc_amt,fri_sbsc_amt,sat_sbsc_amt,sun_sbsc_amt,mon_sbsc_cnt,tue_sbsc_cnt,wed_sbsc_cnt,
thu_sbsc_cnt,fri_sbsc_cnt,sat_sbsc_cnt,sun_sbsc_cnt,tot_sbsc_amt,total_bundle_count,sbsc_fb_amt,sbsc_count_fb,sbsc_count_ussd,sbsc_ussd_amt,
sbsc_count_rs,sbsc_rs_amt,sbsc_others_amt,sbsc_count_others,total_data_bundle_count,total_combo_bundle_count,sbsc_count_voucher,sbsc_voucher_amt,
total_sms_bundle_count,total_voice_bundle_count,total_vas_bundle_count,max_rch_deno,max_sbsc_deno,total_other_bundle_count,avg_rchg_gap,average_sbsc_gap,
average_sbsc_gap_voice,average_sbsc_gap_data,average_sbsc_gap_sms,average_sbsc_gap_combo,average_sbsc_gap_vas,tbl_dt)
select 
coalesce(a.msisdn_key ,b.msisdn_key,c.msisdn_key,d.msisdn_key,e.msisdn_key,f.msisdn_key,g.msisdn_key,h.msisdn_key) msisdn_key,
coalesce(a.weekid ,b.weekid,c.weekid,d.weekid,e.weekid,f.weekid,g.weekid,h.weekid) weekid, 
coalesce(a.mon_sbsc_amt ,0)mon_sbsc_amt,coalesce(a.tue_sbsc_amt ,0)tue_sbsc_amt,coalesce(a.wed_sbsc_amt ,0)wed_sbsc_amt,coalesce(a.thu_sbsc_amt ,0)thu_sbsc_amt,coalesce(a.fri_sbsc_amt ,0) fri_sbsc_amt,
coalesce(a.sat_sbsc_amt ,0)sat_sbsc_amt,coalesce(a.sun_sbsc_amt,0) sun_sbsc_amt, coalesce(a.mon_sbsc_cnt ,0)mon_sbsc_cnt,coalesce(a.tue_sbsc_cnt ,0)tue_sbsc_cnt,coalesce(a.wed_sbsc_cnt ,0)wed_sbsc_cnt,
coalesce(a.thu_sbsc_cnt ,0)thu_sbsc_cnt,coalesce(a.fri_sbsc_cnt ,0)fri_sbsc_cnt,coalesce(a.sat_sbsc_cnt ,0)sat_sbsc_cnt,coalesce(a.sun_sbsc_cnt,0) sun_sbsc_cnt,
coalesce(a.tot_sbsc_amt ,0) tot_sbsc_amt,coalesce(a.total_bundle_count ,0) total_bundle_count,coalesce(a.sbsc_FB_amt ,0)sbsc_FB_amt,coalesce(a.sbsc_count_FB ,0) sbsc_count_FB,
coalesce(a.sbsc_count_USSD ,0) sbsc_count_USSD,coalesce(a.sbsc_USSD_amt,0) sbsc_USSD_amt, coalesce(a.sbsc_count_RS ,0) sbsc_count_RS,coalesce(a.sbsc_RS_amt ,0) sbsc_RS_amt,coalesce(a.sbsc_Others_amt ,0) sbsc_Others_amt,
coalesce(a.sbsc_count_Others ,0) sbsc_count_Others,coalesce(a.total_data_bundle_count,0)total_data_bundle_count, 
coalesce(a.total_combo_bundle_count ,0) total_combo_bundle_count,coalesce(a.sbsc_count_voucher ,0) sbsc_count_voucher,
coalesce(a.sbsc_voucher_amt ,0) sbsc_voucher_amt,coalesce(a.total_sms_bundle_count,0) total_sms_bundle_count, coalesce(a.total_voice_bundle_count ,0) total_voice_bundle_count,
coalesce(a.total_vas_bundle_count ,0) total_vas_bundle_count,
coalesce(a.max_rch_deno ,0)max_rch_deno,coalesce(a.max_sbsc_deno,0) max_sbsc_deno, coalesce(a.total_other_bundle_count,0) total_other_bundle_count,
b.avg_rchg_gap,
c.average_sbsc_gap,d.average_sbsc_gap_voice,e.average_sbsc_gap_data,
f.average_sbsc_gap_sms,g.average_sbsc_gap_combo,h.average_sbsc_gap_vas,
coalesce(a.tbl_dt, b.tbl_dt, c.tbl_dt,d.tbl_dt,e.tbl_dt,f.tbl_dt,g.tbl_dt, h.tbl_dt ) tbl_dt
from cvm_db.cvm20_rbs_average_rch_gap_tmp2 b  
left join cvm_db.cvm20_rbs_average_sbsc_gap_tmp1 c 
on b.msisdn_key = c.msisdn_key     and b.tbl_dt = c.tbl_dt 
left join cvm_db.cvm20_refill_and_data_bundle_tmp1 a
on coalesce(b.msisdn_key, c.msisdn_key)  = a.msisdn_key    and coalesce(b.tbl_dt, c.tbl_dt)  = a.tbl_dt  
left  join cvm_db.cvm20_rbs_average_sbsc_gap_voice_tmp1 d
on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key) = d.msisdn_key and coalesce(a.tbl_dt, b.tbl_dt, c.tbl_dt)  = d.tbl_dt 
left  join cvm_db.cvm20_rbs_average_sbsc_gap_data_tmp1 e
on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key,d.msisdn_key) = e.msisdn_key and coalesce(a.tbl_dt, b.tbl_dt, c.tbl_dt,d.tbl_dt)  = e.tbl_dt 
left join cvm_db.cvm20_rbs_average_sbsc_gap_sms_tmp1 f
on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key,d.msisdn_key,e.msisdn_key) = f.msisdn_key 
and coalesce(a.tbl_dt, b.tbl_dt, c.tbl_dt,d.tbl_dt,e.tbl_dt)  = f.tbl_dt 
left join cvm_db.cvm20_rbs_average_sbsc_gap_combo_tmp1 g 
on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key,d.msisdn_key,e.msisdn_key,f.msisdn_key) = g.msisdn_key
and coalesce(a.tbl_dt, b.tbl_dt, c.tbl_dt,d.tbl_dt,e.tbl_dt,f.tbl_dt)  = g.tbl_dt 
left join cvm_db.cvm20_rbs_average_sbsc_gap_vas_tmp1 h
on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key,d.msisdn_key,e.msisdn_key,f.msisdn_key,g.msisdn_key) = h.msisdn_key
and coalesce(a.tbl_dt, b.tbl_dt, c.tbl_dt,d.tbl_dt,e.tbl_dt,f.tbl_dt,g.tbl_dt)  = h.tbl_dt 
where b.tbl_dt=yyyymmddRunDate;
commit;
