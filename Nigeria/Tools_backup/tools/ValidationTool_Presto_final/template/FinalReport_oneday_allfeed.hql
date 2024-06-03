insert into dq.final_feed_report_audit
select
z.feed_name feed_name, --
sum(v.all_output_cnt) Hive_Records, --
sum(case when length(v.output_fl_name) > 0 then 1 else 0 end)    Hive_Files, --
sum(v.fileOps_recs_cnt) Ops_Lines, --
sum(case when length(v.fileOps_fl_name) > 0 then 1 else 0 end) Ops_Files ,--
sum(v.rejected_rec_cnt) Rejected_Records, --
sum(case when length(v.rejected_fl_name) > 0 then 1 else 0 end) Rejected_Files, --
cast (try(sum(v.rejected_rec_cnt)/sum(v.fileOps_recs_cnt))*100 as decimal(9,4)) Rejected_Ratio, -- 
sum(all_output_cnt + dup_rec_cnt + rejected_rec_cnt)- sum(fileOps_recs_cnt) Diff , -- 
cast(try((sum(all_output_cnt + dup_rec_cnt + rejected_rec_cnt)- sum(fileOps_recs_cnt))/sum(fileOps_recs_cnt))*100 as decimal(9,4)) Diff_Ratio , --
case when sum(fileOps_recs_cnt) <> sum(all_output_cnt + dup_rec_cnt + rejected_rec_cnt) then 'No' else 'Yes' end Status,
0 Partition_Line,
0 Unique_MSISDN,
0 Stats_Files,
0 files_status_count,
coalesce(r.main_reason,'-'),
'run_date',
aud.start_time,
aud.end_time,
sum(dup_rec_cnt) dups_lines,
coalesce(v.file_dt,targetdate)  tbl_dt
from ( select * from dim.dim_feed z where z.stage=stage_number  ) z
left outer join
audit.validation_file_report as v 
on z.feed_name = v.feed_name and v.file_dt = targetdate
left outer join (select * from audit.validation_audit where tbl_dt= targetdate) aud on aud.run_time='run_date' and z.feed_name=aud.feed_name 
left outer join (select substring(fsys_msgtype,18) as feed_name, main_reason, count(*) cnt ,row_number() over (partition by substring(fsys_msgtype,18) order by count(*) desc) row_num from feeds.rejecteddata where file_date=targetdate and msgtype = 'com.mtn.messages.RejectedNameValues'   group by substring(fsys_msgtype,18), main_reason) r on z.feed_name=r.feed_name and r.row_num = 1
group by z.feed_name,coalesce(v.file_dt,targetdate),coalesce(r.main_reason,'-'),aud.start_time,aud.end_time;
