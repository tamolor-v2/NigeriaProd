start transaction;

create or replace view flare_8.vw_fb_site_info_report as
select 
 country,
 site_id,
 latitude_deg,
 longitude_deg,
 status,
 operation_date,
 active_ran_list,
 backhaul,
 capacity_uplink_kbps,
 capacity_downlink_kbps,
 fiber_date,
 site_type,
 date_time 
from flare_8.fb_site_info_report_base
where report_month=firstDayOfMonth and valid_record=1 ;


commit;
