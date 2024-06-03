start transaction;

create or replace view flare_8.vw_fb_system_engage_report as 
select 
  country ,
  ran ,
  sum(users_monthly) as users_monthly,
  sum(users_daily) as users_daily,
  sum(data_vol_down_GB) as data_vol_down_GB,
  sum(data_vol_up_GB) as data_vol_up_GB,
  date_time 
from flare_8.vw_fb_site_engage_report
group by country, ran, date_time;


commit;

