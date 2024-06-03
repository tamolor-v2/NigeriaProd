start transaction;
delete from flare_8.vw_maps_site_info_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.vw_maps_site_info_dq_issues 
with cell_info_test as ( 
select 

		case when upper(status) ='ACTIVE' and bts_2g in ( null,'') then 1 else 0 end as missing_site,
		case when status in ( null,'') then 1 else 0 end as missing_status,
		case when FirstCollectTime is null then 1 else 0 end as missing_FirstCollectTime,
	*
from flare_8.MAPS_SITE_INFO )
select * 
from cell_info_test 
where missing_site+missing_status+missing_FirstCollectTime>0;
commit;