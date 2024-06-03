start transaction;
delete from flare_8.vw_MTN_NG_3G_MONTHLY_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.vw_MTN_NG_3G_MONTHLY_dq_issues 
with cell_info_test as ( 
select 

	case when SITE in ( null,'') then 1 else 0 end as missing_site,
	*
from flare_8.mtn_ng_3g_monthly )
select * 
from cell_info_test 
where missing_site>0;
commit;
