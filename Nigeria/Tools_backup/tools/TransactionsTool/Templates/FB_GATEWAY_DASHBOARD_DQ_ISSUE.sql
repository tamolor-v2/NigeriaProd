start transaction;
delete from flare_8.vw_gateway_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.vw_gateway_dq_issues 
with cell_info_test as ( 
select 
	case when country <> 'NG' then 1 else 0 end as incorrect_country,
	case when gateway_id in ( null,'0','0.0.0.0') then 1 else 0 end as incorrect_gateway_id,
	case when gatway_ip in ( null,'0','0.0.0.0') then 1 else 0 end as incorrect_gateway_ip,
	*
from flare_8.mtn_ng_gateway_information )
select * 
from cell_info_test 
where incorrect_country+incorrect_gateway_id+incorrect_gateway_ip>0;

commit;
