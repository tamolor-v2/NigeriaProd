start transaction;
delete from flare_8.VW_SITE_INFO_REJECTED where tbl_dt >= firstDayOfpreviousMonth ; 
insert into  flare_8.VW_SITE_INFO_REJECTED 
select 
date_format(from_unixtime(fsys_loaded_ts/1000),'%Y-%m-%d')  as load_date,
*, 
file_date as tbl_dt
from flare_8.rejecteddata 
where file_date between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
    and msgtype = 'com.mtn.messages.maps_site_info';
commit;
