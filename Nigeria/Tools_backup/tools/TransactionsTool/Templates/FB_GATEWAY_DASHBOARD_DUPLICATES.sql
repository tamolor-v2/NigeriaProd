start transaction;
delete from flare_8.vw_gateway_duplicates where tbl_dt >= firstDayOfpreviousMonth ;  
insert into  flare_8.vw_gateway_duplicates
select x.*
from 
	(select * from flare_8.mtn_ng_gateway_information ) x,
	( 	select tbl_dt,			
			gateway_id,
			ip_prefix,
			gatway_ip,count(*) cnt 
		from flarestg.mtn_ng_gateway_information 
		group by 1,2,3,4
		having count(*) > 1) y
	where x.gateway_id=y.gateway_id 
		and x.ip_prefix=y.ip_prefix 
		and x.gatway_ip=y.gatway_ip 
		and x.tbl_dt=y.tbl_dt;
	commit;
