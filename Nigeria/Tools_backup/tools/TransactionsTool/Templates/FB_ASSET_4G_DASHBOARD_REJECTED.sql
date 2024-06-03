start transaction;
delete from flare_8.vw_mtnn_asset_extract_4g_cells_rejected where tbl_dt >= firstDayOfpreviousMonth ; 
insert into  flare_8.vw_mtnn_asset_extract_4g_cells_rejected 
select  
date_format(from_unixtime(fsys_loaded_ts/1000),'%Y-%m-%d')  as load_date,
*,
file_date as tbl_dt
from flare_8.rejecteddata 
where file_date between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
    and msgtype = 'com.mtn.messages.mtnn_asset_extract_4g_cells';
commit;
