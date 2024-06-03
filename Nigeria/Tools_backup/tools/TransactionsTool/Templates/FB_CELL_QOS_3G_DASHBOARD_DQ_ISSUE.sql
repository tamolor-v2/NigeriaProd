start transaction;
delete from flare_8.vw_CELL_QOS_3G_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
insert into flare_8.vw_CELL_QOS_3G_dq_issues 
with cell_info_test as ( 
select 

	case when SITE in ( null,'') then 1 else 0 end as missing_site,
	case when cgi in ( null,'') then 1 
		 when cgi not like '621%' or length(cgi)< 14  then 1  
	else 0 end as incorrect_cgi,	
	case when wcell_3g in ( null,'') then 1 else 0 end as missing_cell,
	*
from flare_8.CELL_QOS_3G where tbl_dt >= firstDayOfpreviousMonth and tbl_dt<=lastDayOfLastMonth)
select * 
from cell_info_test 
where missing_cell+incorrect_cgi+missing_site>0;
commit;