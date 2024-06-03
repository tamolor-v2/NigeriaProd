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
where tbl_dt DistributedPartitions and coalesce(try_cast( FeedFileterValues as int), 0) = targetdate and (case when length(file_mod_date) <= 12 then cast(to_unixtime(date_parse(file_mod_date,'%Y-%m-%d')) as bigint)*1000  else cast(file_mod_date as bigint) end) <= (DateTimeFilter - 1000 * 60 * 30)
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
where tbl_dt DistributedPartitions
 and feedtype = 'FeedNameValues' 
 and coalesce(try_cast(FeedFileterValues as int), 0) = targetdate 
AND (case when length(file_mod_date) <= 12 then cast(to_unixtime(date_parse(file_mod_date,'%Y-%m-%d')) as bigint)*1000  else cast(file_mod_date as bigint) end) <= (DateTimeFilter - 1000 * 60 * 30)
group by reverse(element_at(split(reverse(file_name), '/'), 1)), file_offset

EXCEPT

select
  reverse(element_at(split(reverse(file_name), '/'), 1)) as fl_name
, file_offset
from databasename.FeedNameValues
where tbl_dt DistributedPartitions 
and coalesce(try_cast(FeedFileterValues as int), 0) = targetdate
and (case when length(file_mod_date) <= 12 then cast(to_unixtime(date_parse(file_mod_date,'%Y-%m-%d')) as bigint)*1000  else cast(file_mod_date as bigint) end) <= (DateTimeFilter - 1000 * 60 * 30)
and reverse(element_at(split(reverse(file_name), '/'), 1)) in (
select reverse(element_at(split(reverse(file_name), '/'), 1))
from databasename.dupdata
where tbl_dt DistributedPartitions
and feedtype = 'FeedNameValues' 
and coalesce(try_cast(FeedFileterValues as int), 0) = targetdate
and (case when length(file_mod_date) <= 12 then cast(to_unixtime(date_parse(file_mod_date,'%Y-%m-%d')) as bigint)*1000  else cast(file_mod_date as bigint) end) <= (DateTimeFilter - 1000 * 60 * 30)
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
where file_date = targetdate 
and msgtype = 'com.mtn.messages.FeedNameValues' 
and coalesce(try_cast(FeedFileterRejValues as int), 0) = targetdate
AND cast(fsys_landing_ts as bigint)  <= (DateTimeFilter - 1000 * 60 * 30)
group by reverse(element_at(split(reverse(fsys_filename), '/'), 1)), fsys_fileoffset

EXCEPT

SELECT REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)) AS FL_NAME,
       FILE_OFFSET
  FROM databasename.DUPDATA
 WHERE TBL_DT DistributedPartitions
   AND FEEDTYPE = 'FeedNameValues'
   AND COALESCE(TRY_CAST(FeedFileterValues AS INT), 0) = targetdate
   AND (case when length(file_mod_date) <= 12 then cast(to_unixtime(date_parse(file_mod_date,'%Y-%m-%d')) as bigint)*1000  else cast(file_mod_date as bigint) end) <= (DateTimeFilter - 1000 * 60 * 30)
   AND REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)) IN
       (SELECT REVERSE(ELEMENT_AT(SPLIT(REVERSE(FSYS_FILENAME), '/'), 1))
          FROM databasename.REJECTEDDATA
         WHERE FILE_DATE = targetdate
           AND MSGTYPE = 'com.mtn.messages.FeedNameValues'
           AND COALESCE(TRY_CAST(FeedFileterRejValues AS INT), 0) = targetdate
		   AND cast(fsys_landing_ts as bigint)  <= (DateTimeFilter - 1000 * 60 * 30)
         GROUP BY REVERSE(ELEMENT_AT(SPLIT(REVERSE(FSYS_FILENAME), '/'), 1)))
 GROUP BY REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)),
          FILE_OFFSET

EXCEPT

SELECT REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)) AS FL_NAME,
       FILE_OFFSET
  FROM databasename.FeedNameValues
 WHERE TBL_DT DistributedPartitions
   AND COALESCE(TRY_CAST(FeedFileterValues AS INT), 0) = targetdate
   AND (case when length(file_mod_date) <= 12 then cast(to_unixtime(date_parse(file_mod_date,'%Y-%m-%d')) as bigint)*1000  else cast(file_mod_date as bigint) end) <= (DateTimeFilter - 1000 * 60 * 30)
   AND REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)) IN
       (SELECT REVERSE(ELEMENT_AT(SPLIT(REVERSE(FSYS_FILENAME), '/'), 1))
          FROM databasename.REJECTEDDATA
         WHERE FILE_DATE = targetdate
           AND MSGTYPE = 'com.mtn.messages.FeedNameValues'
           AND COALESCE(TRY_CAST(FeedFileterRejValues AS INT), 0) =
               targetdate
		   AND cast(fsys_landing_ts as bigint)  <= (DateTimeFilter - 1000 * 60 * 30)
         GROUP BY REVERSE(ELEMENT_AT(SPLIT(REVERSE(FSYS_FILENAME), '/'), 1)))
 GROUP BY REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)),
          FILE_OFFSET)
 GROUP BY FL_NAME) AS REJECTED_TBL
    ON (OUTPUT_DUP_TBL.FL_NAME = REJECTED_TBL.FL_NAME)
  FULL JOIN (SELECT DISTINCT NAME AS FILE_NAME,
                             LINES AS FILEOPS_RECS,
                             CAST(FILE_DT AS INT) AS FILE_DT,
                             FEED_NAME
               FROM (SELECT NAME,
                            LINES,
                            'FeedNameValues' FEED_NAME,
                            FeedNameReg AS FILE_DT
                       FROM DatabaseTables.FILES_FILESOPPS_SUMMARY A
                      WHERE LINES > 0
                        AND TBL_DT BETWEEN previousDay AND fileOpsNextDay
                        AND PATH LIKE '%FeedNameCondition%'
                        AND cast(movecopydate as bigint)  <= (DateTimeFilter - 1000 * 60 * 30) ) FILESOPS
              WHERE FILE_DT LIKE 'targetdate') AS FILEOPS_TBL
    ON (OUTPUT_DUP_TBL.FL_NAME = FILEOPS_TBL.FILE_NAME);

