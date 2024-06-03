insert into audit.files_count_rejected_summary_pre
  SELECT path,                                                                 
 split(path,'/')[cardinality(split(path,'/'))] ,           
 numberofline, main_reason , rejecteddate,
 rejecteddate  file_dt ,feed_name
 from audit.files_count_rejected_detailes where rejecteddate=targetdate;
