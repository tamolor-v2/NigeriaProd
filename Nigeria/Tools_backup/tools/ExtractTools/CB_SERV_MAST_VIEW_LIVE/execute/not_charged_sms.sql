start transaction;
delete from nigeria.not_charged_sms where tbl_dt=20190108;
insert into nigeria.not_charged_sms
select
msisdn_key,
sum(case when network_type_enrich_2=0 then c_amount else 0.0 end) as sms_onnet_out_c_amt,
sum(case when network_type_enrich_2=0 then nc_amount else 0.0 end) as sms_onnet_out_nc_amt,
sum(case when network_type_enrich_2=0 then bonus_amount else 0.0 end) as sms_onnet_out_amt_free,
sum(case when network_type_enrich_2=0 then c_sms else 0 end) as sms_onnet_out_c_counter,
sum(case when network_type_enrich_2=0 then nc_sms else 0 end) as sms_onnet_out_nc_counter,
sum(case when network_type_enrich_2=0 then bonus_sms else 0 end) as sms_onnet_out_counter_free,

sum(case when network_type_enrich_2=1 then c_amount else 0.0 end) as sms_offnet_out_c_amt,
sum(case when network_type_enrich_2=1 then nc_amount else 0.0 end) as sms_offnet_out_nc_amt,
sum(case when network_type_enrich_2=1 then bonus_amount else 0.0 end) as sms_offnet_out_amt_free,
sum(case when network_type_enrich_2=1 then c_sms else 0 end) as sms_offnet_out_c_counter,
sum(case when network_type_enrich_2=1 then nc_sms else 0 end) as sms_offnet_out_nc_counter,
sum(case when network_type_enrich_2=1 then bonus_sms else 0 end) as sms_offnet_out_counter_free,

sum(case when network_type_enrich_2=3 then c_amount else 0.0 end) as sms_int_out_c_amt,
sum(case when network_type_enrich_2=3 then nc_amount else 0.0 end) as sms_int_out_nc_amt,
sum(case when network_type_enrich_2=3 then bonus_amount else 0.0 end) as sms_int_out_amt_free,
sum(case when network_type_enrich_2=3 then c_sms else 0 end) as sms_int_out_c_counter,
sum(case when network_type_enrich_2=3 then nc_sms else 0 end) as sms_int_out_nc_counter,
sum(case when network_type_enrich_2=3 then bonus_sms else 0 end) as sms_int_out_counter_free,
tbl_dt
from
(
select
tbl_dt,
msisdn_key,
network_type_enrich_2,
c_sms,
nc_sms,
case when da_bonus_amount>0 then 1 else 0 end bonus_sms,
(ma_amount_used+da_c_amount) as c_amount,
da_nc_amount as nc_amount,
da_bonus_amount as bonus_amount
from flare_8.vp_cs5_ccn_sms_ma_dasplit where tbl_dt =20190108
and call_type_enrich=1
) a
group by
tbl_dt,
msisdn_key;
commit;
