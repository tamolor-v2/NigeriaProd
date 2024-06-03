start transaction;

create or replace view flare_8.vw_fb_site_engage_all_daily as
select 
   tbl_dt,
   date_time,
   site_id,
   'ALL' as ran,
   status,
   mobile_site,
   sum(users_monthly) as users_monthly,
   sum(users_daily) as users_daily,
   sum(data_vol_down_GB) as data_vol_down_GB,
   sum(data_vol_up_GB) as data_vol_up_GB   
from flare_8.vw_fb_site_engage_234_daily
group by 1,2,3,4,5,6;

commit;
