start transaction;
delete from nigeria.cm_profile_bdail where tbl_dt=yyyymmdd;
insert into nigeria.cm_profile_bdail
select 
a.msisdn_key,
a.subscriber_type,
max(case when a.tbl_dt=yyyymmdd then a.dola end) dola,
cast(sum(a.act_days_data) as int) dou_data,
cast(sum(greatest(a.act_days_sms_onnet_moc,a.act_days_sms_offnet_moc,a.act_days_sms_intl_moc)) as int) dou_sms,
cast(sum(greatest(a.act_days_onnet_moc,a.act_days_offnet_moc,a.act_days_intl_moc)) as int) dou_voice,
cast(sum(a.rch_count_digital+a.rch_count_voucher) as int) total_refill_count,
sum(cast(0.0 as double)) MoMo_Closing_Balance,
cast(sum(a.momo_dep_cnt+a.momo_wit_cnt+a.momo_p2p_trx_cnt) as int) MoMo_Total_Txns,
max(a.tbl_dt) tbl_dt
from flare_8.customersubject a where
a.tbl_dt=yyyymmdd and a.tbl_dt between 
cast(date_format(date_add('day',-29,date_parse(cast(yyyymmdd as varchar),'%Y%m%d')),'%Y%m%d') as int) and yyyymmdd 
and a.aggr='daily' and (a.is_in_today_sdp or a.is_in_prevdays_sdp)
group by a.msisdn_key,a.subscriber_type;
commit;
