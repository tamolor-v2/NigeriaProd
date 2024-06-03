start transaction;
delete from flare_8.fb_cell_qos_validation where report_month=firstDayOfMonth;
insert into flare_8.fb_cell_qos_validation

select 
 'Nigeria QoS Validation' as Validation_Check,
 substr(date_time,1,4) as qos_year,
 count(*) as total_records ,
 sum(valid_record) as valid_record,
 count(*)-sum(valid_record) as invalid_record,
 1.000*sum(valid_record)/count(*) as dq_percentage,
 count(distinct site_id) as number_of_sites,
 sum(missing_site_id) as missing_site_id,
 sum(missing_ci) as missing_ci,
 sum(missing_cgi) as missing_cgi,
 sum(incorrect_cgi) as incorrect_cgi,
 SUM(missing_eci) as missing_eci ,
 SUM(not_on_cellInfo_2g) as not_on_cellInfo_2g,
 SUM(not_on_cellInfo_3g) as not_on_cellInfo_3g,
 SUM(not_on_cellInfo_4g) as not_on_cellInfo_4g,
 sum(inactive_cells) as inactive_cells,
 report_month
from flare_8.fb_cell_qos_report_base
where report_month=firstDayOfMonth 
group by 1,2,report_month
;
commit;
