start transaction;
delete from flare_8.fb_cell_info_4g_dq_rpt where tbl_month >= firstDayOfpreviousMonth ; 
insert into flare_8.fb_cell_info_4g_dq_rpt select tbl_dt,tbl_month_key,report_month,date_key,file_date,opco,feed_name,table_name,feed_frequency,file_received,file_received_rag,total_records,rejected_records,rejected_rag,average_7_days,average_7_days_rag,trend_rag,dq_issues,dq_rag,dq_percentage,duplicates,duplicate_rag,incorrect_cell_name,incorrect_cell_name_rag,incorrect_cgi,incorrect_cgi_rag,missing_site,missing_site_rag,missing_frequency,missing_frequency_rag,missing_tac,missing_tac_rag,cell_info_4g,source,tbl_month from flare_8.fb_cell_info_4g_dq_rpt9 ;
commit;