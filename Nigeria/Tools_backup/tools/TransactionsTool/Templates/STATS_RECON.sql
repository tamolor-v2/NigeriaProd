start transaction;
insert into flare_8.STATS_RECON
select cast(a.record_count as bigint) as recon_record_count,coalesce(b.base_record_count,0) as base_record_count,coalesce(c.rejected_record_count,0) as rejected_record_count,a.file_name_v as file_name,a.tbl_dt as tbl_dt
from
flare_8.STATS_CDR_RECON a 
left join
(select count(*) as base_record_count,base_file_name from flare_8.STAT_BILL_USSD where tbl_dt between yyyymmddRunDate and runDatePlus3 group by 2) b
on (a.file_name_v=b.base_file_name)
left join 
(select count(*) as rejected_record_count, split(fsys_filename,'/')[7] as rejected_filename from flare_8.rejecteddata where file_date =yyyymmddRunDate and msgtype like '%stat_bill_ussd%' group by 2) c
on (a.file_name_v = c.rejected_filename)
where a.tbl_dt=yyyymmddRunDate  and a.record_count <>'' and a.file_name_v <>'' and  a.file_name_v <> 'total';
commit;
