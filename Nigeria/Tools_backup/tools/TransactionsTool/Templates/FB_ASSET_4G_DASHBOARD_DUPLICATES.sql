start transaction;
delete from flare_8.vw_MTNN_ASSET_EXTRACT_4G_CELLS_duplicates where tbl_dt >= firstDayOfpreviousMonth ;  
insert into  flare_8.vw_MTNN_ASSET_EXTRACT_4G_CELLS_duplicates  
select x.*
from 
	(select * from flare_8.MTNN_ASSET_EXTRACT_4G_CELLS ) x,
	( 	select tbl_dt,ecgi,count(*) cnt 
		from flare_8.MTNN_ASSET_EXTRACT_4G_CELLS 
		group by 1,2
		having count(*) > 1) y
	where x.ecgi=y.ecgi and x.tbl_dt=y.tbl_dt;
	commit;
