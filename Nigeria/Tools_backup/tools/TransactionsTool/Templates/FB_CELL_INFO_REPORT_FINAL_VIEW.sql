start transaction;
create or replace view flare_8.vw_fb_cell_info_report as
select 
		country,
		site_id,
		gateway_id,
		ran,
		case when ran='4G' then null else ci end as ci,
		case when ran='4G' then null else lac end as lac,
		case when ran='4G' then null else cgi end as cgi,
		case when ran in ('3G','2G') then null else eci end as eci,
		case when ran in ('3G','2G') then null else tac end as tac,
		case when ran in ('3G','2G') then null else tai end as tai,
		case when ran in ('3G','2G') then null else enodeb_id end as enodeb_id,
		case when ran in ('3G','2G') then null else ecgi end as ecgi,
		status,
		frequency_mhz,
		tx_power_dbm,  
		operation_date,
		date_time
from flare_8.fb_cell_info_report_base
where report_month=firstDayOfMonth and valid_record=1 and status = 'ACTIVE';
commit;
