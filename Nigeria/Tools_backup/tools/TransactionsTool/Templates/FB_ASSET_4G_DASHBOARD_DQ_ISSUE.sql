start transaction;
delete from flare_8.vw_MTNN_ASSET_EXTRACT_4G_CELLS_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.vw_MTNN_ASSET_EXTRACT_4G_CELLS_dq_issues 
with cell_info_test as ( 
select 

		case when upper(cell_status) ='ACTIVE' and ecgi in ( null,'') then 1 
		 when upper(cell_status) ='ACTIVE' and (ecgi not like '621%' or length(ecgi)< 14 ) then 1  
	else 0 end as incorrect_cgi,
	case when upper(cell_status) ='ACTIVE' and tx_power_dbm in ( null,0) then 1 else 0 end as incorrect_power_dbm,
	*
from flare_8.MTNN_ASSET_EXTRACT_4G_CELLS where tbl_dt >= firstDayOfpreviousMonth and tbl_dt<= lastDayOfLastMonth )
select * 
from cell_info_test 
where incorrect_cgi+incorrect_power_dbm>0;
commit;
