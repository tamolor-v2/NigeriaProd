start transaction;
delete from flare_8.vw_mtnn_asset_extract_2g_cells_duplicates where tbl_dt >= firstDayOfpreviousMonth ;  
insert into  flare_8.vw_mtnn_asset_extract_2g_cells_duplicates  
select x.*
from 
	(select * from flare_8.MTNN_ASSET_EXTRACT_2G_CELLS ) x,
	( 	select tbl_dt,gsm_cell_identity,count(*) cnt 
		from flare_8.MTNN_ASSET_EXTRACT_2G_CELLS 
		group by 1,2
		having count(*) > 1) y
	where x.gsm_cell_identity=y.gsm_cell_identity and x.tbl_dt=y.tbl_dt;
	commit;
