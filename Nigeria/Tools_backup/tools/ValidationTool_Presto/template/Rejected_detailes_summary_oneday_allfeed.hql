insert into databaseTables.files_count_rejected_summary_pre
  SELECT path,                                                                 
 split(path,'/')[cardinality(split(path,'/'))] ,           
 numberofline, main_reason , rejecteddate,
 rejecteddate  file_dt,feed_name 
 from databaseTables.files_count_rejected_detailes_pre where rejecteddate=targetdate;
