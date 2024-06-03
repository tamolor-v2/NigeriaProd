insert into dq.feeds_count_rejected_summary
 select feedname,sum(numberofline), tbl_dt  
  from (select distinct feedname,filename,numberofline,tbl_dt from audit.files_count_rejected_summary_pre) c  
 group by feedname,tbl_dt;
