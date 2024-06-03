 
use databasename;
insert overwrite table databasename.final_feed_report partition (tbl_dt=targetdate) 
 select fcs.feed_name HiveTargetTable,  coalesce(fcs.numberofline ,0) feed_name_lineNumber,
 a.feed_name DiroctoryFeedName,  coalesce(a.linecount ,0) DiroctoryFeedNameLineCount,
 b.feed_name RjectedFeedname,  coalesce(b.numberofline,0) TotalRejectedCount , 
 coalesce(a.linecount,0)-coalesce(fcs.numberofline,0) Deff , 
 (coalesce(a.linecount,0)-coalesce(fcs.numberofline,0))/coalesce(a.linecount,0)*100 ratio ,  
 case when coalesce(a.linecount ,0) <> coalesce(fcs.numberofline ,0)+coalesce(b.numberofline,0) 
 then "Not Match" else "Match" end status  from
 (select * from databasename.feeds_count_summary s where
 tbl_dt=targetdate )fcs 
 left outer join  (select * from Directory_filesopps_summary a where 
 (tbl_dt=targetdate and feedname not like "%EOD_USER_ACC_BAL%") or (tbl_dt=nextday and feedname  like "%EOD_USER_ACC_BAL%" ) 
   ) a
 on fcs.feed_name=a.feed_name left outer join   
 (select * from databasename.feeds_count_rejected_summary where 
 (tbl_dt=targetdate and feedname not like "%EOD_USER_ACC_BAL%") or (tbl_dt=nextday and feedname  like "%EOD_USER_ACC_BAL%" ) 
  ) b   on fcs.feed_name=b.feed_name
