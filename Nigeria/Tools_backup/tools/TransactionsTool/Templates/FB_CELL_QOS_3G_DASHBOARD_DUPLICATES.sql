start transaction;
delete from flare_8.vw_CELL_QOS_3G_duplicates where tbl_dt >= firstDayOfpreviousMonth ;  
insert into  flare_8.vw_CELL_QOS_3G_duplicates
select x.*
from 
	(select * from flare_8.CELL_QOS_3G ) x,
	( 	select tbl_dt,time,wcell_3g,cgi,count(*) cnt 
		from flare_8.CELL_QOS_3G 
		group by 1,2,3,4
		having count(*) > 1) y
	where x.wcell_3g=y.wcell_3g and x.cgi=y.cgi and x.tbl_dt=y.tbl_dt and x.time=y.time;

	commit;
