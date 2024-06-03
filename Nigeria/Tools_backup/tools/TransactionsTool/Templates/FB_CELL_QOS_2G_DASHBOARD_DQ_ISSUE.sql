start transaction;
delete from flare_8.vw_cell_qos_2g_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
insert into flare_8.vw_cell_qos_2g_dq_issues 
with cell_info_test as ( 
select 

	case when SITE in ( null,'') then 1 else 0 end as missing_site,
	case when cgi in ( null,'') then 1 
		 when cgi not like '621%' or length(cgi)< 14  then 1  
	else 0 end as incorrect_cgi,	
	case when cell_2g in ( null,'') then 1 else 0 end as missing_cell,
	*
from flare_8.CELL_QOS_2G where tbl_dt >= firstDayOfpreviousMonth and tbl_dt<=lastDayOfLastMonth)
select * 
from cell_info_test 
where missing_cell+incorrect_cgi+missing_site>0;
commit;
