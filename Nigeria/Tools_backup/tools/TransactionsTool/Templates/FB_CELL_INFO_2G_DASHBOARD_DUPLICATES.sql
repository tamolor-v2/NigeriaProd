start transaction;
delete from flare_8.vw_cell_info_2g_duplicates where tbl_dt >= firstDayOfpreviousMonth ;  
insert into  flare_8.vw_cell_info_2g_duplicates  
select x.*
from 
	(select * from flare_8.CELL_INFO_2G ) x,
	( 	select tbl_dt,cell_2g,cgi,count(*) cnt 
		from flare_8.CELL_INFO_2G  where tbl_dt >= firstDayOfpreviousMonth  
		group by 1,2,3
		having count(*) > 1) y
	where x.cell_2g=y.cell_2g and x.cgi=y.cgi and x.tbl_dt=y.tbl_dt;
	commit;
