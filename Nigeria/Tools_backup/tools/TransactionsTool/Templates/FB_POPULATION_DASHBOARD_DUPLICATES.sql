start transaction;
delete from flare_8.vw_population_duplicates where tbl_dt >= firstDayOfpreviousMonth ;  
insert into  flare_8.vw_population_duplicates
select x.*
from 
	(select * from flare_8.population_ds ) x,
	( 	select tbl_dt,			
			pop_source,
			pop_source_vers,
			pop_source_link,count(*) cnt 
		from flare_8.population_ds 
		group by 1,2,3,4
		having count(*) > 1) y
	where x.pop_source=y.pop_source 
		and x.pop_source_vers=y.pop_source_vers 
		and x.pop_source_link=y.pop_source_link 
		and x.tbl_dt=y.tbl_dt;
	commit;
