start transaction;
delete from nigeria.not_charged where tbl_dt=20190108;
insert into nigeria.not_charged
select
msisdn_key,
sum(case when network_type_enrich_2=0 then (ma_c_duration+da_c_duration) else 0.0 end) as voi_onnet_out_c_secs,
sum(case when network_type_enrich_2=0 and (ma_c_duration+da_c_duration)>0 then 1 else 0 end) as voi_onnet_out_c_counter,
sum(case when network_type_enrich_2=0 then (ma_nc_duration+da_nc_duration) else 0.0 end) as voi_onnet_out_nc_secs,
sum(case when network_type_enrich_2=0 and (ma_nc_duration+da_nc_duration)>0 then 1 else 0 end) as voi_onnet_out_nc_counter,
sum(case when network_type_enrich_2=0 then (da_bonus_duration) else 0.0 end) as voi_onnet_out_secs_free,
sum(case when network_type_enrich_2=0 and (da_bonus_duration)>0 then 1 else 0 end) as voi_onnet_out_counter_free,

sum(case when network_type_enrich_2=1 then (ma_c_duration+da_c_duration) else 0.0 end) as voi_offnet_out_c_secs,
sum(case when network_type_enrich_2=1 and (ma_c_duration+da_c_duration)>0 then 1 else 0 end) as voi_offnet_out_c_counter,
sum(case when network_type_enrich_2=1 then (ma_nc_duration+da_nc_duration) else 0.0 end) as voi_offnet_out_nc_secs,
sum(case when network_type_enrich_2=1 and (ma_nc_duration+da_nc_duration)>0 then 1 else 0 end) as voi_offnet_out_nc_counter,
sum(case when network_type_enrich_2=1 then (da_bonus_duration) else 0.0 end) as voi_offnet_out_secs_free,
sum(case when network_type_enrich_2=1 and (da_bonus_duration)>0 then 1 else 0 end) as voi_offnet_out_counter_free,

sum(case when network_type_enrich_2=3 then (ma_c_duration+da_c_duration) else 0.0 end) as voi_int_out_c_secs,
sum(case when network_type_enrich_2=3 and (ma_c_duration+da_c_duration)>0 then 1 else 0 end) as voi_int_out_c_counter,
sum(case when network_type_enrich_2=3 then (ma_nc_duration+da_nc_duration) else 0.0 end) as voi_int_out_nc_secs,
sum(case when network_type_enrich_2=3 and (ma_nc_duration+da_nc_duration)>0 then 1 else 0 end) as voi_int_out_nc_counter,
sum(case when network_type_enrich_2=3 then (da_bonus_duration) else 0.0 end) as voi_int_out_secs_free,
sum(case when network_type_enrich_2=3 and (da_bonus_duration)>0 then 1 else 0 end) as voi_int_out_counter_free,
tbl_dt
from
(
select
tbl_dt,
msisdn_key,
(case when (recordType = '20' and length (networkcd) = 0 and network_type_enrich != 2 and network_type_enrich != 0) then
bslnetworktype(tbl_dt, network_type_enrich, netflag, dialeddigit, called_calling_number) else network_type_enrich end) 
as network_type_enrich_2,
bill_type_enrich,
duration,
ma_c_duration,
da_c_duration,
ma_nc_duration,
da_nc_duration,
da_bonus_duration,
da_unknown_duration
from flare_8.vp_cs5_ccn_voice_ma_dasplit where tbl_dt=20190108
and
call_type_enrich=1
) a
group by
tbl_dt,
msisdn_key;
commit;
