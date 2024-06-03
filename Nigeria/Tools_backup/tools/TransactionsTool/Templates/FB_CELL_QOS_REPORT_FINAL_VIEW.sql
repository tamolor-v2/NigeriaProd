start transaction;
create or replace view flare_8.vw_fb_cell_qos_report as
select 
country,site_id,ran,ci,cgi,eci,ecgi,data_vol_down_GB,data_vol_up_GB,tput_down_kbps,
tput_up_kbps,tput_user_down_kbps,tput_user_up_kbps,date_time
from flare_8.fb_cell_qos_report_base
where report_month=firstDayOfMonth and valid_record=1;
commit;
