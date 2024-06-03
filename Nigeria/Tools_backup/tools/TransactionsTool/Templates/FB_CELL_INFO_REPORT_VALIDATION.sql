start transaction;
delete from flare_8.fb_cell_info_validation where report_month=firstDayOfMonth;

insert into  flare_8.fb_cell_info_validation 
select 
	'Nigeria Cell Info Check' as Validation_Check,
	cell_year,
	ran,
	new_cell,
	count(*) as total_records ,
	sum(valid_record) as valid_record,
	sum(missing_cgi) as missing_cgi,
	sum(missing_site_id) as missing_site_id,
	sum(incorrect_cgi) as incorrect_cgi,
	sum(missing_ecgi) as missing_ecgi,
	sum(incorrect_ecgi) as incorrect_ecgi,
	sum(missing_tac) as missing_tac,
	sum(missing_lac) as missing_lac,
	sum(missing_frequency) as missing_frequency,
	sum(missing_tx_power) as missing_tx_power,
	sum(mobile_site) as mobile_site,
	sum(missing_operation_date) as missing_operation_date ,
	sum(inactive_site) as inactive_site,
	sum(active_status) as active_status,
	sum(non_active_status) as non_active_status,
	sum(missing_from_site_info) as missing_from_site_info,
        count(*)-sum(valid_record) as invalid_record,
        1.000*sum(valid_record)/count(*) as dq_percentage,
        firstDayOfMonth as report_month
from flare_8.fb_cell_info_report_base
where report_month = firstDayOfMonth
group by 1,2,3,4,report_month
;
commit;
