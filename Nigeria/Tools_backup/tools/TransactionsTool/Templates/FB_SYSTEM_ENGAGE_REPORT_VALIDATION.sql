start transaction;
delete from flare_8.FB_SYSTEM_ENGAGEMENT_VALIDATION where report_month = firstDayOfMonth;
insert into flare_8.FB_SYSTEM_ENGAGEMENT_VALIDATION
select 
 'Nigeria System Engagement Validation' as Validation_Check,
 substr(date_time,1,4) as qos_year,
 count(*) as total_records  ,
 firstDayOfMonth report_month
from flare_8.vw_fb_system_engage_report
group by 1,2,4
;
commit;
