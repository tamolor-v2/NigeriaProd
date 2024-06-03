start transaction;
delete from flare_8.vw_population_dq_issues  where tbl_dt >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.vw_population_dq_issues 
with cell_info_test as ( 
select 
	case when country <> 'NG' then 1 else 0 end as incorrect_country,
	case when pop_source in ( null,'') then 1 else 0 end as missing_pop_source,
	case when pop_source_link in ( null,'') then 1 else 0 end as missing_pop_link,
	case when pop_source_vers =null then 1 else 0 end as missing_pop_version,
	*
from flare_8.population_ds )
select * 
from cell_info_test 
where incorrect_country+missing_pop_source+missing_pop_link+missing_pop_version>0;
commit;
