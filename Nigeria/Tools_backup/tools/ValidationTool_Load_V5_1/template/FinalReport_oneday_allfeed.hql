insert into databaseTables.final_feed_report_pre
select 
z.feed_name feed_name,  
coalesce(fcs.numberofline ,0) Hive_Records, 
fcs.numberoffiles    Hive_Files,  
coalesce(a.numberofline ,0) Ops_Lines, 
a.numberoffile            Ops_Files ,   
coalesce(b.numberofline,0) Rejected_Records,        
fcr.numberoffiles Rejected_Files,                 
cast (try(coalesce(b.numberofline,0)/coalesce(a.numberofline ,0))*100 as decimal(9,4)) Rejected_Ratio, 
(coalesce(fcs.numberofline,0)+coalesce(b.numberofline,0))-coalesce(a.numberofline,0) Diff ,       
cast(try(((coalesce(fcs.numberofline,0)+coalesce(b.numberofline,0))-coalesce(a.numberofline,0))/coalesce(a.numberofline,0))*100 as decimal(9,4)) Diff_Ratio ,     
case when coalesce(a.numberofline ,0) <> coalesce(fcs.numberofline ,0)+coalesce(b.numberofline,0) then 'No' else 'Yes' end Status, 
coalesce(fcsp.numberofline,0) Partition_Line, 
coalesce(fcsp.numberofuniquemsisdn,0) Unique_MSISDN, 
fs.numberoffiles Stats_Files, 
fs.recordscount files_status_count, 
coalesce(b.main_reason,'-'), 
coalesce(fcs.file_dt,targetdate)  tbl_dt 
from  databaseTables.dim_feed z  
left outer join  
(select * from databaseTables.feeds_count_summary_pre s  where file_dt= targetdate )fcs  
 on z.feed_name = fcs.feed_name 
 left outer join  
 (select * from databaseTables.Directory_filesopps_summary_pre a) a 
 on fcs.feed_name=a.feedname and fcs.file_dt=a.file_dt  
 left outer join             
 (select * from databaseTables.feeds_count_rejected_summary_pre ) b   on 
 fcs.feed_name=b.feed_name and fcs.file_dt=b.tbl_dt 
left outer join 
(select feed_name, numberofuniquemsisdn, numberofline, tbl_dt from databaseTables.feeds_count_summary_partition_pre) fcsp 
on fcs.feed_name = fcsp.feed_name and fcs.file_dt = fcsp.tbl_dt 
left outer join (select feedname, count(distinct filename) as numberoffiles, rejecteddate from databaseTables.files_count_rejected_summary_pre where  
rejecteddate = targetdate and filename like '%targetdate%' group by feedname, rejecteddate) fcr 
on fcs.feed_name = fcr.feedname and fcs.file_dt = fcr.rejecteddate 
left outer join (select * from databaseTables.feeds_count_stats_summary_pre where file_dt= targetdate) fs  
on upper(fcs.feed_name) = fs.feed_name and fcs.file_dt = fs.file_dt;
