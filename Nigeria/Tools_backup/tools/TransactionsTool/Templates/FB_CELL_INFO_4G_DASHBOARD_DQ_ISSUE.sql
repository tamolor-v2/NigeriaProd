start transaction;
delete from flare_8.vw_cell_info_4g_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
insert into flare_8.vw_cell_info_4g_dq_issues with cell_info_test as ( 
select 

	case when status ='Active' and cell_4g in ( null,'') then 1 else 0 end as incorrect_cell_name,
	case when status ='Active' and cgi in ( null,'') then 1 
		 when status ='Active' and (cgi not like '621%' or length(cgi)< 14 ) then 1  
	else 0 end as incorrect_cgi,
	case when status ='Active' and site in (null,'') then 1  else 0 end as missing_site,
	case when status ='Active' and freq_band in (null,'') then 1  else 0 end as missing_frequency,
	case when status ='Active' and tac in (null,'') then 1  else 0 end as missing_tac,
	*
from flare_8.CELL_INFO_4G where tbl_dt >= firstDayOfpreviousMonth and tbl_dt<=lastDayOfLastMonth )
select * 
from cell_info_test 
where incorrect_cell_name+incorrect_cgi+missing_site+missing_frequency+missing_tac>0;
commit;
