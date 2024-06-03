start transaction;
delete from flare_8.fb_maps_site_info_dq_rpt where tbl_month >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.fb_maps_site_info_dq_rpt select tbl_dt,tbl_month_key,report_month,date_key,file_date,opco,feed_name,table_name,feed_frequency,file_received,file_received_rag,total_records,rejected_records,rejected_rag,average_7_days,average_7_days_rag,trend_rag,dq_issues,dq_rag,dq_percentage,duplicates,duplicate_rag,missing_site,missing_site_rag,missing_status,missing_status_rag,missing_firstcollecttime,missing_firstcollecttime_rag,maps_site_info,source,tbl_month from flare_8.fb_maps_site_info_dq_rpt6 ;
commit;