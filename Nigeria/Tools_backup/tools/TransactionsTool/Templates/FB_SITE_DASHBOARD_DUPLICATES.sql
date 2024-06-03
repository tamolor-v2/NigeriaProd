start transaction;
delete from flare_8.vw_maps_site_info_duplicates where tbl_dt >= firstDayOfpreviousMonth ;  
insert into  flare_8.vw_maps_site_info_duplicates  
select x.*
from 
	(select * from flare_8.MAPS_SITE_INFO ) x,
	( 	select tbl_dt,bts_2g,count(*) cnt 
		from flare_8.MAPS_SITE_INFO 
		group by 1,2
		having count(*) > 1) y
	where x.bts_2g=y.bts_2g and x.tbl_dt=y.tbl_dt;
	commit;
