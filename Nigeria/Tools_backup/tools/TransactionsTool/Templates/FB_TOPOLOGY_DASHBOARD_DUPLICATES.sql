start transaction;
delete from flare_8.vw_topology_map_duplicates where tbl_dt >= firstDayOfpreviousMonth ;  
insert into  flare_8.vw_topology_map_duplicates
select x.*
from 
	(select * from flare_8.topology_map ) x,
	( 	select tbl_dt,siteid,count(*) cnt 
		from flare_8.topology_map 
		group by 1,2
		having count(*) > 1) y
	where x.siteid=y.siteid and x.tbl_dt=y.tbl_dt;
	commit;
