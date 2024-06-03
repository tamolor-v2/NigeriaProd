start transaction;
delete from nigeria.sit_payg_bundled_usage where tbl_dt=20190108;
insert into nigeria.sit_payg_bundled_usage
select
msisdn_key,
sum(case when data_type='4G' then (ma_c_volume+ma_nc_volume) else 0 end)/1024/1024 DATA_4G_Paygo_Usage,
sum(case when data_type='4G' then (da_c_volume+da_nc_volume) else 0 end)/1024/1024 DATA_4G_Bundled_Usage,
sum(case when data_type='3G' then (ma_c_volume+ma_nc_volume) else 0 end)/1024/1024 DATA_3G_Paygo_Usage,
sum(case when data_type='3G' then (da_c_volume+da_nc_volume) else 0 end)/1024/1024 DATA_3G_Bundled_Usage,
sum(case when data_type='2G' then (ma_c_volume+ma_nc_volume) else 0 end)/1024/1024 DATA_2G_Paygo_Usage,
sum(case when data_type='2G' then (da_c_volume+da_nc_volume) else 0 end)/1024/1024 DATA_2G_Bundled_Usage,
sum(case when data_type='4G' then (c_amount) else 0 end) data_4g_paygo_revenue,
sum(case when data_type='4G' then (nc_amount) else 0 end) data_4g_bundled_revenue,
sum(case when data_type='3G' then (c_amount) else 0 end) data_3g_paygo_revenue,
sum(case when data_type='3G' then (nc_amount) else 0 end) data_3g_bundled_revenue,
sum(case when data_type='2G' then (c_amount) else 0 end) data_2g_paygo_revenue,
sum(case when data_type='2G' then (nc_amount) else 0 end) data_2g_bundled_revenue,
sum(nc_amount) as total_data_bundled_revenue,
sum(case when network_type='roam' then (c_amount) else 0.0 end) as international_data_revenue,
sum(ma_amount_used) data_ma_rev, 
sum(da_c_amount) data_da_rev,
sum(da_bonus_amount) data_da_bonus,
tbl_dt
from flare_8.vp_cs5_ccn_gprs_ma_dasplit
where tbl_dt=20190108
group by tbl_dt,msisdn_key;
commit;
