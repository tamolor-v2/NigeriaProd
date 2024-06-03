
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;



insert overwrite table flaretest.final_files_report partition(tbl_dt) select * from flaretest.final_files_report where flare_feed_name <> "FeedNameValues" ;

insert into table flaretest.final_files_report partition (tbl_dt)
 
 
 select fcs.feed_name,ffs.filename,ffs.numberofline,fcs.feed_name,a.name,a.lines,b.feedname,b.filename,b.numberofline
 ,coalesce(a.lines,0)-(coalesce(ffs.numberofline,0)+coalesce(b.numberofline,0)) Deff ,
  (coalesce(a.lines,0)-(coalesce(ffs.numberofline,0)+coalesce(b.numberofline,0)))/coalesce(a.lines,0)*100 ratio ,  
   case when coalesce(a.lines ,0) <> (coalesce(ffs.numberofline ,0)+coalesce(b.numberofline,0) )
 then "Not Match" else "Match" end status,fcs.tbl_dt
 
 
  from 
 (select * from flaretest.feeds_count_summary s 
 where feed_name="FeedNameValues" ) fcs left outer join 
  (select * from flaretest.files_count_summary s 
   )ffs 
 on fcs.feed_name=ffs.feedname and fcs.tbl_dt=ffs.tbl_dt
 
 left outer join  (select distinct name,lines  from flaretest.files_filesopps_summary_distinct )  a 
on ffs.filename=a.name 
left outer join 
 (select * from flaretest.files_count_rejected_summary_new   ) b
    on ffs.feedname=b.feedname and ffs.filename=b.filename and ffs.tbl_dt=b.tbl_dt;
