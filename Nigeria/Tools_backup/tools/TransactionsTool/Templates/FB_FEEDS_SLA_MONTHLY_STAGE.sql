start transaction;
delete from flare_8.fb_feed_sla_dq_monthly where tbl_dt >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.fb_feed_sla_dq_monthly select report_month,date_key,source,feed_name,feed_sla_date,opco,feed_proper_name,feed_frequency,file_received_date,dq_index,received_files,missing_files,missing_files_rag,expected_files,in_sla,sla_rag,dq_rag,report_month  from flare_8.fb_feed_sla_dq_monthly_temp10;
commit;
