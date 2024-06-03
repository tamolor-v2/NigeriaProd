start transaction;
delete from flare_8.fb_site_info_validation where report_month = firstDayOfMonth;

insert into flare_8.fb_site_info_validation
select 
	'Nigeria Site Info Check' as Validation_Check,
	site_year,
	active_ran_list,
	new_site,
	count(*) as total_records ,
	sum(valid_record) as valid_record,
	SUM(active_status) as active_status,
        SUM(inactive_status) as inactive_status,
	sum(mobile_site) as mobile_site,
	sum(missing_site_id) as missing_site_id,
	sum(missing_location) as missing_location,
	sum(missing_backhaul) as missing_backhaul,
	sum(missing_capacity) as missing_capacity,
	sum(missing_fibre_date) as missing_fibre_date,
	SUM(missing_operation_date) as missing_operation_date ,
	SUM(missing_ran_list) as missing_ran_list ,
	SUM(missing_site_type) as missing_site_type ,
	SUM(incorrect_status) as incorrect_status,
	SUM(missing_status) as missing_status,
	SUM(decom_status) as decom_status,
        count(*)-sum(valid_record) as invalid_record,
        1.000*sum(valid_record)/count(*) as dq_percentage,
        firstDayOfMonth as report_month
from flare_8.fb_site_info_report_base
where report_month = firstDayOfMonth and site_year>= 2020
group by 1,2,3,4,report_month;


commit;
