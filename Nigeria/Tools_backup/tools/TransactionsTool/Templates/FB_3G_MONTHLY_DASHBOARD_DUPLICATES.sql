start transaction;
delete from flare_8.vw_MTN_NG_3G_MONTHLY_duplicates where tbl_dt >= firstDayOfpreviousMonth ;  
insert into  flare_8.vw_MTN_NG_3G_MONTHLY_duplicates
select x.*
from 
	(select * from flare_8.MTN_NG_3G_MONTHLY ) x,
	( 	select tbl_dt,site,count(*) cnt 
		from flare_8.MTN_NG_3G_MONTHLY 
		group by 1,2
		having count(*) > 1) y
	where x.site=y.site and x.tbl_dt=y.tbl_dt ;
	commit;
