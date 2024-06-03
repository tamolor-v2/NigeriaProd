start transaction;
delete from nigeria.not_charged_data where tbl_dt=yyyymmdd;
insert into nigeria.not_charged_data
select
msisdn_key,
sum(coalesce(c_volume,0.0))/1024 as data_kb_c,
sum(case when c_volume>0 then 1 else 0 end) as data_kb_c_counter,
sum(c_amount) data_c_amount,
sum(coalesce(nc_volume,0.0))/1024 as data_kb_nc,
sum(case when nc_volume>0 then 1 else 0 end) as data_kb_nc_counter,
sum(nc_amount) data_nc_amount,
sum(coalesce(da_bonus_volume,0.0))/1024 as data_kb_free,
sum(case when da_bonus_volume>0 then 1 else 0 end) as data_kb_free_counter,
sum(bonus_amount) data_amount_free,tbl_dt
from
(
select
tbl_dt,
msisdn_key,
c_volume,nc_volume,da_bonus_volume,
c_amount,nc_amount,da_bonus_amount as bonus_amount
from flare_8.vp_cs5_ccn_gprs_ma_dasplit where tbl_dt = yyyymmdd
and
network_type='local'
) a
group by
tbl_dt,
msisdn_key;
commit;
