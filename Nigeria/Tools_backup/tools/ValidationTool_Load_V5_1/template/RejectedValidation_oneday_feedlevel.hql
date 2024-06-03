  insert into databaseTables.feeds_count_rejected_summary_pre 
  select a.feedname,sum(a.numberofline), b.main_reason, a.tbl_dt  
  from databaseTables.files_count_rejected_summary_pre a 
  inner join  
  (select row_number() over (partition by x.feedname order by x.numberofline desc) seq, x.feedname, x.main_reason from 
  ( select feedname, sum(numberofline) numberofline , main_reason from databaseTables.files_count_rejected_summary_pre where tbl_dt = targetdate group by feedname, main_reason)x ) b 
  on a.feedname = b.feedname 
  where a.tbl_dt=targetdate
  and b.seq = 1 
  group by a.feedname,a.tbl_dt , b.main_reason; 
