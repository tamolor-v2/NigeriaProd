start transaction;
delete from flare_8.vw_topology_map_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.vw_topology_map_dq_issues 
with cell_info_test as ( 
select 

	case when SITEID in ( null,'') then 1 else 0 end as missing_site,
	case when LONG in (null,0)  then 1 else 0 end as missing_longitude,
	case when lat in (null,0)  then 1 else 0 end as missing_latitude,
	case when SITE_TYPE in ( null,'')  then 1 else 0 end as missing_site_type,
	case when Site_Tx_Capacity is null  then 1 else 0 end as missing_uplink_kbps,
	case when fiber_date is null then 1 else 0 end as missing_fibre_date,
	*
from flare_8.topology_map )
select * 
from cell_info_test 
where missing_site+missing_longitude+missing_latitude+missing_site_type+missing_uplink_kbps+missing_fibre_date>0;
commit;
