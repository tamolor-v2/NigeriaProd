start transaction;
delete from flare_8.fb_gateway_dq_rpt where tbl_month >= firstDayOfpreviousMonth ; 
commit;
start transaction;
insert into flare_8.fb_gateway_dq_rpt select tbl_dt,tbl_month_key,report_month,date_key,file_date,opco,feed_name,table_name,feed_frequency,file_received,file_received_rag,total_records,rejected_records,rejected_rag,average_7_days,average_7_days_rag,trend_rag,dq_issues,dq_rag,dq_percentage,duplicates,duplicate_rag,incorrect_country,incorrect_country_rag,incorrect_gateway_id,incorrect_gateway_id_rag,incorrect_gateway_ip,incorrect_gateway_ip_rag,gateway_info,source,tbl_month from flare_8.fb_gateway_dq_rpt3 ;
commit;