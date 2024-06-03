start transaction;
delete from flare_8.fb_CELL_QOS_3G_dq_rpt where tbl_month >= firstDayOfpreviousMonth ; 
insert into flare_8.fb_CELL_QOS_3G_dq_rpt select tbl_dt,date_key,report_month,file_date,opco,feed_name,table_name,feed_frequency,file_received,file_received_rag,total_records,rejected_records,rejected_rag,average_7_days,average_7_days_rag,trend_rag,dq_issues,dq_rag,dq_percentage,duplicates,duplicate_rag,missing_cell,missing_cell_rag,incorrect_cgi,incorrect_cgi_rag,missing_site,missing_site_rag,cell_qos_3g,source,tbl_month from flare_8.fb_CELL_QOS_3G_dq_rpt8 ;
commit;