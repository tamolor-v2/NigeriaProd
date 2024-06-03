insert into databaseTables.feeds_count_rejected_summary_pre
 select feedname,sum(numberofline), tbl_dt  
  from (select distinct feedname,filename,numberofline,tbl_dt from databaseTables.files_count_rejected_summary_pre) c  
 group by feedname,tbl_dt;
