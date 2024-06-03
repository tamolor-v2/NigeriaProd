start transaction;
delete from flare_8.fb_coverage_dq_rpt where tbl_month >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.fb_coverage_dq_rpt select tbl_month ,tbl_month_key ,report_month ,file_name ,date_key ,file_date ,opco ,feed_name ,feed_frequency ,file_received ,file_received_rag ,coverage_maps ,source ,tbl_month from flare_8.fb_coverage_dq_rpt4 ;
commit;
