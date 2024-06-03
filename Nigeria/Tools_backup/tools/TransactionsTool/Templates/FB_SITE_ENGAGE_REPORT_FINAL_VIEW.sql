start transaction;

create or replace view flare_8.vw_fb_site_engage_report as
select 
   'NG' as country,
   site_id,
   ran,
   users_monthly,
   users_daily,
   data_vol_down_GB,
   data_vol_up_GB,    
   date_time
from flare_8.vw_fb_site_engage_234_daily
where valid_record =1
union all
select 
   'NG' as country,
   site_id,
   'ALL' ran,
   sum(users_monthly) as users_monthly,
   sum(users_daily) as users_daily,
   sum(data_vol_down_GB) as data_vol_down_GB,
   sum(data_vol_up_GB) as data_vol_up_GB, 
   date_time
from flare_8.vw_fb_site_engage_234_daily
where valid_record =1
group by 1,2,3,8;

commit;

