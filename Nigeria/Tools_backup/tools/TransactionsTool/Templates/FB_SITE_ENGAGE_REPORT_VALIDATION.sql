start transaction;

delete from flare_8.FB_SITE_ENGAGEMENT_VALIDATION where report_month = firstDayOfMonth;
insert into flare_8.FB_SITE_ENGAGEMENT_VALIDATION
select 
 'Nigeria Site Engagement Validation' as Validation_Check,
 substr(date_time,1,4) as qos_year,
 count(*) as total_records ,
 sum(valid_record) as valid_record,
 count(*)-sum(valid_record) as invalid_record,
 1.000*sum(valid_record)/count(*) as dq_percentage,
 count(distinct site_id) as number_of_sites,
 sum(missing_site_id) as missing_site_id,
 sum(mobile_site) as mobile_site,
 sum(missing_status) as missing_status,
 sum(missing_maps_site_info) as missing_maps_site_info ,
 firstDayOfMonth report_month
from flare_8.vw_fb_site_engage_234_daily
group by 1,2,12
;

commit;
