start transaction;
insert into flare_8.FB_CELL_INFO_REPORT_BASE_2
select 		
country,
x.site_id,
gateway_id,
ran,
ci,
lac,
cgi,
eci,
tac,
tai,
enodeb_id,
ecgi,
status,
frequency_mhz,
tx_power_dbm, 
operation_date, 
date_time,
date_key,		
cell_year,
missing_site_id,
missing_cgi,
incorrect_cgi,
missing_ecgi,
incorrect_ecgi,
missing_tac,
missing_lac,
missing_frequency,
missing_tx_power,
mobile_site ,
missing_operation_date,
inactive_site,
active_status,
non_active_status,
case when y.site_id is null then 1 else 0 end as missing_from_site_info,
not_in_maps,
case when y.site_id is null then 0 else valid_record end as valid_record,
new_cell,
report_month
from flare_8.fb_cell_info_report_base x
left join(select distinct site_id from flare_8.vw_fb_site_info_report
) y 
on x.site_id = y.site_id
where report_month = firstDayOfMonth
;
commit;

