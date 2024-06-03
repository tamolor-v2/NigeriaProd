insert into ${validationSchema}.${validationTable}
select    ${validationFileName},
          a.max_rec_cnt                                                                                             AS stats_max_rec_cnt,
          a.files_cnt_in_stats                                                                                      AS stats_files_cnt,
          a.fl_last_processed                                                                                       AS stats_last_processed,
          coalesce(b.uniq_rec_cnt, 0)                                                                               AS output_uniq_rec_cnt,
          coalesce(b.total_file_recs, 0)                                                                            AS output_all_rec_cnt,
          (coalesce(b.total_file_recs, 0) - coalesce(b.uniq_rec_cnt, 0)) > 0                                        AS output_has_dups,
          coalesce(c.uniq_rec_cnt, 0)                                                                               AS rejected_uniq_rec_cnt,
          coalesce(c.total_rec_cnt, 0)                                                                              AS rejected_all_rec_cnt,
          coalesce(c.num_files, 0)                                                                                  AS rejected_fls_cnt,
          coalesce(d.uniq_rec_cnt, 0)                                                                               AS dup_uniq_rec_cnt,
          coalesce(d.total_rec_cnt, 0)                                                                              AS dup_all_rec_cnt,
          coalesce(d.num_files, 0)                                                                                  AS dup_fls_cnt,
          (a.max_rec_cnt - coalesce(b.uniq_rec_cnt, 0) - coalesce(c.uniq_rec_cnt, 0) - coalesce(d.uniq_rec_cnt, 0)) AS uniq_rec_cnt_diff,
          localtimestamp                                                                                            AS data_prepared_time,
          '${feedName}'                                                                                                AS msgtype,
          try_cast(${validationFileDate}  AS INT)  AS file_date
FROM      (

                   select
                            regexp_replace(fl_name, '.[0-9]+$') as fl_name,
			                sum (max_rec_cnt) as max_rec_cnt ,
			                max (fl_last_processed) as fl_last_processed,
			                sum (files_cnt_in_stats) as files_cnt_in_stats
			       from     (

                   select   ${fileStatsFileName}                                  as fl_name,
                            max(recordscount)                                     AS max_rec_cnt,
                            max(starttime)                                        AS fl_last_processed,
                            count(*)                                              AS files_cnt_in_stats
                   FROM     ${schema}.${fileStats}
                   WHERE    tbl_dt >= ${minTblDt}
                   AND      fsys_msgtype = '${statsType}'
                   AND      try_cast( ${fileStatsFileDate}  AS INT) BETWEEN ${stDt} AND ${edDt}
                   AND      starttimets <= ( ${START_TIME_IN_MILLISEC} - 1000 * 60 * 30 )
                   GROUP BY ${fileStatsFileName}
                   )
                   GROUP BY regexp_replace(fl_name, '.[0-9]+$')

                   ) a
left join
          (

          		    select  regexp_replace(fl_name, '.[0-9]+$')                as fl_name,
				            sum(uniq_rec_cnt)      as uniq_rec_cnt,
							sum(total_file_recs)   as total_file_recs
				    from (

                   select   ${feedFileName} as fl_name,
                            count(DISTINCT file_offset)                            AS uniq_rec_cnt,
                            count(*)                                               AS total_file_recs
                   FROM     ${schema}.${table}
                   WHERE
                            tbl_dt in (${tbl_dt})
                   AND      try_cast(  ${feedFileDate} AS INT)  BETWEEN ${stDt} AND ${edDt}
                   ${where_clause}
                   GROUP BY ${feedFileName}
                   )
                   group by regexp_replace(fl_name, '.[0-9]+$')


                   ) b
ON        (
                    a.fl_name = b.fl_name)
left join
          (

                    select   regexp_replace(fl_name, '.[0-9]+$')                 as fl_name,
				            sum(num_files)         as num_files,
							sum(uniq_rec_cnt)      as  uniq_rec_cnt,
							sum(total_rec_cnt)     as total_rec_cnt
				    from
                   (
                   select   ${rejectedFileName} as fl_name,
                            count(DISTINCT fsys_filename)                              AS num_files,
                            count(DISTINCT fsys_fileoffset)                            AS uniq_rec_cnt,
                            count(*)                                                   AS total_rec_cnt
                   FROM     ${schema}.rejecteddata
                   WHERE
                            file_date between ${stDt} AND ${edDt}
                   AND      msgtype = '${rejectedType}'
                   AND      try_cast(  ${rejectedFileDate}  AS INT) BETWEEN ${stDt} AND ${edDt}
                   GROUP BY ${rejectedFileName}
                   )
                   group by regexp_replace(fl_name, '.[0-9]+$')

                   ) c
ON        (
                    a.fl_name = c.fl_name)
left join
          (

                   select regexp_replace(fl_name, '.[0-9]+$')                 as fl_name,
				          sum(num_files)          as num_files,
						  sum(uniq_rec_cnt)       as uniq_rec_cnt ,
						  sum(total_rec_cnt)      as total_rec_cnt
				   from
				   (
                   select   ${dupDataFileName} AS fl_name,
                            count(DISTINCT file_name)                              AS num_files,
                            count(DISTINCT file_offset)                            AS uniq_rec_cnt,
                            count(*)                                               AS total_rec_cnt
                   FROM     ${schema}.dupdata
                   WHERE
                                   tbl_dt in (${tbl_dt})
                   AND      feedtype = '${table}'
                   AND      try_cast( ${dupDataFileDate}  AS INT) BETWEEN ${stDt} AND      ${edDt}
                   GROUP BY ${dupDataFileName}
                   )
                   group by regexp_replace(fl_name, '.[0-9]+$')

                   ) d
ON        ( a.fl_name = d.fl_name)
