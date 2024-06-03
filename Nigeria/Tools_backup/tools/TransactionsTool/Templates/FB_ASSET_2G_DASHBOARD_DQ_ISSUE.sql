start transaction;
delete from flare_8.vw_mtnn_asset_extract_2g_cells_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.vw_mtnn_asset_extract_2g_cells_dq_issues 
with cell_info_test as ( 
select 

	case when upper(cell_status) ='ACTIVE' and gsm_cell_identity in ( null,'') then 1 else 0 end as incorrect_cell_name,
	case when upper(cell_status) ='ACTIVE' and average_tx_power_dbm in ( null,0) then 1 else 0 end as incorrect_power_dbm,
	*
from flare_8.MTNN_ASSET_EXTRACT_2G_CELLS  where tbl_dt >= firstDayOfpreviousMonth and tbl_dt<= lastDayOfLastMonth)
select * 
from cell_info_test 
where incorrect_cell_name+incorrect_power_dbm>0;

commit;