insert into DatabaseTables.validation_file_report
select 
  coalesce(coalesce(output_dup_tbl.fl_name, rejected_tbl.fl_name),fileOps_tbl.file_name) as fl_name
, coalesce(output_dup_tbl.output_fl_name, '') as output_fl_name
, coalesce(output_dup_tbl.dup_fl_name, '') as dup_fl_name
, coalesce(output_dup_tbl.output_rec_cnt, 0) as output_rec_cnt
, coalesce(output_dup_tbl.dup_rec_cnt, 0) as dup_rec_cnt
, coalesce(output_dup_tbl.all_output_cnt, 0) as all_output_cnt
, coalesce(rejected_tbl.fl_name, '') as rejected_fl_name
, coalesce(rejected_tbl.rec_cnt, 0) as rejected_rec_cnt
, coalesce(fileOps_tbl.file_name, '') as fileOps_fl_name -- new
, coalesce(fileOps_tbl.fileOps_recs,0) as fileOps_recs_cnt -- new
, targetdate as file_dt
, 'FeedNameValues' as feed_name
from
(
select 
  coalesce(output_tbl.fl_name, dup_tbl.fl_name) as fl_name
, coalesce(output_tbl.fl_name, '') as output_fl_name
, coalesce(dup_tbl.fl_name, '') as dup_fl_name
, coalesce(output_tbl.rec_cnt, 0) as output_rec_cnt
, coalesce(dup_tbl.rec_cnt, 0) as dup_rec_cnt
, coalesce(output_tbl.all_recs, 0) as all_output_cnt
from
(
select  reverse(element_at(split(reverse(file_name), '/'), 1)) as fl_name, -1 as rec_cnt, count(*) as all_recs
from databasename.FeedNameValues 
where tbl_dt between previousDay and nextDay and coalesce(try_cast( FeedFileterValues as int), 0) = targetdate
group by reverse(element_at(split(reverse(file_name), '/'), 1))

) output_tbl
full join
(

select fl_name, count(*) as rec_cnt
from
(
select 
  reverse(element_at(split(reverse(file_name), '/'), 1)) as fl_name
, file_offset
from databasename.dupdata 
where tbl_dt between previousDay and targetdate and feedtype = 'FeedNameValues' and coalesce(try_cast(FeedFileterValues as int), 0) = targetdate
group by reverse(element_at(split(reverse(file_name), '/'), 1)), file_offset

EXCEPT

select 
  reverse(element_at(split(reverse(file_name), '/'), 1)) as fl_name
, file_offset
from databasename.FeedNameValues 
where tbl_dt between previousDay and targetdate and coalesce(try_cast(FeedFileterValues as int), 0) = targetdate
and reverse(element_at(split(reverse(file_name), '/'), 1)) in (
select reverse(element_at(split(reverse(file_name), '/'), 1))
from databasename.dupdata 
where tbl_dt between previousDay and targetdate and feedtype = 'FeedNameValues' and coalesce(try_cast(FeedFileterValues as int), 0) = targetdate
group by reverse(element_at(split(reverse(file_name), '/'), 1))
)
group by reverse(element_at(split(reverse(file_name), '/'), 1)), file_offset
)
group by fl_name

) dup_tbl
on (output_tbl.fl_name = dup_tbl.fl_name)
) as output_dup_tbl
full join
(
select fl_name, count(*) as rec_cnt
from
(
select 
  reverse(element_at(split(reverse(fsys_filename), '/'), 1)) as fl_name
, fsys_fileoffset as file_offset
from databasename.rejecteddata 
where file_date = targetdate and msgtype = 'com.mtn.messages.FeedNameValues' and coalesce(try_cast(FeedFileterRejValues as int), 0) = targetdate 
group by reverse(element_at(split(reverse(fsys_filename), '/'), 1)), fsys_fileoffset

EXCEPT

select 
  reverse(element_at(split(reverse(file_name), '/'), 1)) as fl_name
, file_offset
from databasename.dupdata 
where tbl_dt between previousDay and targetdate and feedtype = 'FeedNameValues' and coalesce(try_cast(FeedFileterValues as int), 0) = targetdate 
and reverse(element_at(split(reverse(file_name), '/'), 1)) in (
select 
reverse(element_at(split(reverse(fsys_filename), '/'), 1))
from databasename.rejecteddata 
where file_date = targetdate and msgtype = 'com.mtn.messages.FeedNameValues' and coalesce(try_cast(FeedFileterRejValues as int), 0) = targetdate 
group by reverse(element_at(split(reverse(fsys_filename), '/'), 1))
)
group by reverse(element_at(split(reverse(file_name), '/'), 1)), file_offset

EXCEPT

select 
  reverse(element_at(split(reverse(file_name), '/'), 1)) as fl_name
, file_offset
from databasename.FeedNameValues 
where tbl_dt between previousDay and targetdate and coalesce(try_cast(FeedFileterValues as int), 0) = targetdate 
and reverse(element_at(split(reverse(file_name), '/'), 1)) in (
select 
reverse(element_at(split(reverse(fsys_filename), '/'), 1))
from databasename.rejecteddata 
where file_date = targetdate and msgtype = 'com.mtn.messages.FeedNameValues' and coalesce(try_cast(FeedFileterRejValues as int), 0) = targetdate 
group by reverse(element_at(split(reverse(fsys_filename), '/'), 1))
)
group by reverse(element_at(split(reverse(file_name), '/'), 1)), file_offset
)
group by fl_name
) as rejected_tbl
on (output_dup_tbl.fl_name = rejected_tbl.fl_name)
full join 
(
select distinct name as file_name,lines as fileOps_recs,cast(file_dt as int) as file_dt,feed_name from
(select name,lines,'FeedNameValues' feed_name, FeedNameReg as file_dt 
from DatabaseTables.files_filesopps_summary a  
where lines > 0 and tbl_dt between previousDay and fileOpsNextDay and path like '%FeedNameCondition%') FilesOps where file_dt like 'targetdate'
) as fileOps_tbl
on (output_dup_tbl.fl_name = fileOps_tbl.file_name);
