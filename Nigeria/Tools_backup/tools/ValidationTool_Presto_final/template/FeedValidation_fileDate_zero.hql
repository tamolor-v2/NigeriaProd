INSERT INTO DatabaseTables.validation_file_report
SELECT coalesce(coalesce(output_dup_tbl.fl_name, rejected_tbl.fl_name), fileOps_tbl.file_name) AS fl_name,
       coalesce(output_dup_tbl.output_fl_name, '') AS output_fl_name,
       coalesce(output_dup_tbl.dup_fl_name, '') AS dup_fl_name,
       coalesce(output_dup_tbl.output_rec_cnt, 0) AS output_rec_cnt,
       coalesce(output_dup_tbl.dup_rec_cnt, 0) AS dup_rec_cnt,
       coalesce(output_dup_tbl.all_output_cnt, 0) AS all_output_cnt,
       coalesce(rejected_tbl.fl_name, '') AS rejected_fl_name,
       coalesce(rejected_tbl.rec_cnt, 0) AS rejected_rec_cnt,
       coalesce(fileOps_tbl.file_name, '') AS fileOps_fl_name -- new
,
       coalesce(fileOps_tbl.fileOps_recs, 0) AS fileOps_recs_cnt -- new
,
       targetdate AS file_dt,
       'FeedNameValues' AS feed_name
FROM
  (SELECT coalesce(output_tbl.fl_name, dup_tbl.fl_name) AS fl_name,
          coalesce(output_tbl.fl_name, '') AS output_fl_name,
          coalesce(dup_tbl.fl_name, '') AS dup_fl_name,
          coalesce(output_tbl.rec_cnt, 0) AS output_rec_cnt,
          coalesce(dup_tbl.rec_cnt, 0) AS dup_rec_cnt,
          coalesce(output_tbl.all_recs, 0) AS all_output_cnt
   FROM
     (SELECT regexp_replace(fl_name, '.[0-9]+$') AS fl_name ,
             rec_cnt AS rec_cnt ,
             SUM(all_recs) AS all_recs
      FROM
     (SELECT reverse(element_at(split(reverse(file_name), '/'), 1)) AS fl_name,
             -1 AS rec_cnt,
             count(*) AS all_recs
      FROM databasename.FeedNameValues
      WHERE tbl_dt DistributedPartitions
        AND coalesce(try_cast(FeedFileterValues AS int), 0) = targetdate
        AND (CASE
		WHEN file_mod_date IS NULL or file_mod_date = '0' THEN 6300
                 WHEN length(file_mod_date) <= 12 THEN cast(to_unixtime(date_parse(file_mod_date, '%Y-%m-%d')) AS bigint)*1000
                 ELSE cast(file_mod_date AS bigint)
             END) <= (DateTimeFilter - 1000 * 60 * 30)
         GROUP BY reverse(element_at(split(reverse(file_name), '/'), 1)))
         GROUP BY rec_cnt, regexp_replace(fl_name, '.[0-9]+$')) output_tbl
   FULL JOIN
     (SELECT regexp_replace(fl_name, '.[0-9]+$') AS fl_name ,
             SUM(rec_cnt) AS rec_cnt
      FROM
     (SELECT fl_name,
             count(*) AS rec_cnt
      FROM
        (SELECT reverse(element_at(split(reverse(file_name), '/'), 1)) AS fl_name,
                file_offset
         FROM databasename.dupdata
         WHERE tbl_dt DistributedPartitions
           AND feedtype = 'FeedNameValues'
           AND coalesce(try_cast(FeedFileterValues AS int), 0) = targetdate
           AND (CASE
		    WHEN file_mod_date IS NULL or file_mod_date = '0' THEN 6300
                    WHEN length(file_mod_date) <= 12 THEN cast(to_unixtime(date_parse(file_mod_date, '%Y-%m-%d')) AS bigint)*1000
                    ELSE cast(file_mod_date AS bigint)
                END) <= (DateTimeFilter - 1000 * 60 * 30)
         GROUP BY reverse(element_at(split(reverse(file_name), '/'), 1)),
                  file_offset
         EXCEPT SELECT reverse(element_at(split(reverse(file_name), '/'), 1)) AS fl_name,
                       file_offset
         FROM databasename.FeedNameValues
         WHERE tbl_dt DistributedPartitions
           AND coalesce(try_cast(FeedFileterValues AS int), 0) = targetdate
           AND (CASE
		    WHEN file_mod_date IS NULL or file_mod_date = '0' THEN 6300
                    WHEN length(file_mod_date) <= 12 THEN cast(to_unixtime(date_parse(file_mod_date, '%Y-%m-%d')) AS bigint)*1000
                    ELSE cast(file_mod_date AS bigint)
                END) <= (DateTimeFilter - 1000 * 60 * 30)
           AND reverse(element_at(split(reverse(file_name), '/'), 1)) IN
             (SELECT reverse(element_at(split(reverse(file_name), '/'), 1))
              FROM databasename.dupdata
              WHERE tbl_dt DistributedPartitions
                AND feedtype = 'FeedNameValues'
                AND coalesce(try_cast(FeedFileterValues AS int), 0) = targetdate
                AND (CASE
			WHEN file_mod_date IS NULL or file_mod_date = '0' THEN 6300
                         WHEN length(file_mod_date) <= 12 THEN cast(to_unixtime(date_parse(file_mod_date, '%Y-%m-%d')) AS bigint)*1000
                         ELSE cast(file_mod_date AS bigint)
                     END) <= (DateTimeFilter - 1000 * 60 * 30)
              GROUP BY reverse(element_at(split(reverse(file_name), '/'), 1)))
         GROUP BY reverse(element_at(split(reverse(file_name), '/'), 1)),
                  file_offset)
         GROUP BY fl_name)
        GROUP BY regexp_replace(fl_name, '.[0-9]+$')) dup_tbl 
	ON (output_tbl.fl_name = dup_tbl.fl_name)) AS output_dup_tbl
FULL JOIN
  (SELECT regexp_replace(fl_name, '.[0-9]+$') AS FL_NAME ,
          SUM(rec_cnt) AS rec_cnt
   FROM
  (SELECT fl_name,
          count(*) AS rec_cnt
   FROM
     (SELECT reverse(element_at(split(reverse(fsys_filename), '/'), 1)) AS fl_name,
             fsys_fileoffset AS file_offset
      FROM databasename.rejecteddata
      WHERE msgtype = 'com.mtn.messages.RejectedNameValues'
        AND ((file_date = targetdate
              AND coalesce(try_cast(FeedFileterRejValues AS int), 0) = targetdate)
             OR (FILE_DATE = 0
                 AND coalesce(try_cast(FeedFileterRejValues AS int), 0) = targetdate
                 AND length(origmsg) > 1))
        AND cast(fsys_landing_ts AS bigint) <= (DateTimeFilter - 1000 * 60 * 30)
      GROUP BY reverse(element_at(split(reverse(fsys_filename), '/'), 1)),
               fsys_fileoffset
      EXCEPT SELECT REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)) AS FL_NAME,
                    FILE_OFFSET
      FROM databasename.DUPDATA
      WHERE TBL_DT DistributedPartitions
        AND FEEDTYPE = 'FeedNameValues'
        AND COALESCE(TRY_CAST(FeedFileterValues AS INT), 0) = targetdate
        AND (CASE
		WHEN file_mod_date IS NULL or file_mod_date = '0' THEN 6300
                 WHEN length(file_mod_date) <= 12 THEN cast(to_unixtime(date_parse(file_mod_date, '%Y-%m-%d')) AS bigint)*1000
                 ELSE cast(file_mod_date AS bigint)
             END) <= (DateTimeFilter - 1000 * 60 * 30)
        AND REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)) IN
          (SELECT REVERSE(ELEMENT_AT(SPLIT(REVERSE(FSYS_FILENAME), '/'), 1))
           FROM databasename.REJECTEDDATA
           WHERE FILE_DATE = targetdate
             AND MSGTYPE = 'com.mtn.messages.RejectedNameValues'
             AND COALESCE(TRY_CAST(FeedFileterRejValues AS INT), 0) = targetdate
             AND cast(fsys_landing_ts AS bigint) <= (DateTimeFilter - 1000 * 60 * 30)
           GROUP BY REVERSE(ELEMENT_AT(SPLIT(REVERSE(FSYS_FILENAME), '/'), 1)))
      GROUP BY REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)),
               FILE_OFFSET
      EXCEPT SELECT REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)) AS FL_NAME,
                    FILE_OFFSET
      FROM databasename.FeedNameValues
      WHERE TBL_DT DistributedPartitions
        AND COALESCE(TRY_CAST(FeedFileterValues AS INT), 0) = targetdate
        AND (CASE
		WHEN file_mod_date IS NULL or file_mod_date = '0' THEN 6300
                 WHEN length(file_mod_date) <= 12 THEN cast(to_unixtime(date_parse(file_mod_date, '%Y-%m-%d')) AS bigint)*1000
                 ELSE cast(file_mod_date AS bigint)
             END) <= (DateTimeFilter - 1000 * 60 * 30)
        AND REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)) IN
          (SELECT REVERSE(ELEMENT_AT(SPLIT(REVERSE(FSYS_FILENAME), '/'), 1))
           FROM databasename.REJECTEDDATA
           WHERE FILE_DATE = targetdate
             AND MSGTYPE = 'com.mtn.messages.RejectedNameValues'
             AND COALESCE(TRY_CAST(FeedFileterRejValues AS INT), 0) = targetdate
             AND cast(fsys_landing_ts AS bigint) <= (DateTimeFilter - 1000 * 60 * 30)
           GROUP BY REVERSE(ELEMENT_AT(SPLIT(REVERSE(FSYS_FILENAME), '/'), 1)))
      GROUP BY REVERSE(ELEMENT_AT(SPLIT(REVERSE(FILE_NAME), '/'), 1)),
               FILE_OFFSET)
      GROUP BY FL_NAME)
   GROUP BY regexp_replace(fl_name, '.[0-9]+$'))AS REJECTED_TBL ON (OUTPUT_DUP_TBL.FL_NAME = REJECTED_TBL.FL_NAME)
FULL JOIN
  (SELECT DISTINCT NAME AS FILE_NAME,
                   LINES AS FILEOPS_RECS,
                   CAST(FILE_DT AS INT) AS FILE_DT,
                   FEED_NAME
   FROM
     (SELECT NAME,
             LINES,
             'FeedNameValues' FEED_NAME,
                              FeedNameReg AS FILE_DT
      FROM DatabaseTables.FILES_FILESOPPS_SUMMARY A
      WHERE LINES > 0
        AND TBL_DT BETWEEN previousDay AND fileOpsNextDay
        AND PATH LIKE '%FeedNameCondition%'
        AND cast(moddate AS bigint) <= (DateTimeFilter - 1000 * 60 * 30) ) FILESOPS
   WHERE FILE_DT LIKE 'targetdate') AS FILEOPS_TBL ON (OUTPUT_DUP_TBL.FL_NAME = FILEOPS_TBL.FILE_NAME);

