start transaction;
create table flare_8.fb_report_sla_dq_monthly as 
select x.*,y.dq_percentage as report_dq 
from flare_8.fb_report_sla_dq_detail  x
left join 
	flare_8.fb_validation_dq_summary y
	on (x.report_month=y.report_month 
	and x.report_name=y.report_name);
commit;