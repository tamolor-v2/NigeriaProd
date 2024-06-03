SELECT fl_name,uniq_rec_cnt_diff
FROM   ${validationSchema}.${validationTable}
WHERE  msgtype = '${feedName}'
AND    file_date >= ${stDt}
AND    file_date <= ${edDt}
AND    uniq_rec_cnt_diff > 0
UNION
SELECT    a.name, (a.lines -b.recordscount) --a.lines
FROM      (
                   SELECT   name,lines
                   FROM     AUDIT.files_filesopps_summary
                   WHERE    tbl_dt >= ${stDt}
                   AND      tbl_dt <= ${edDt}
                   AND      name IS NOT NULL
                   AND      moveto IS NOT NULL
                   AND      length(moveto) > 0
                   AND      replace(moveto, '//', '/') LIKE '%${incomingDir}%'
                   AND      coalesce(try_cast(${fileopsFileNameDateRegGroup} AS INT), 0) BETWEEN ${stDt} AND  ${edDt}
                   AND      coalesce(try_cast(${fileopsFileNameHourRegGroup} AS INT), 0) BETWEEN ${stHr} AND  ${edHr}
                   AND      coalesce(try_cast( movecopydate as bigint),0) <= ( ${START_TIME_IN_MILLISEC} - 1000 * 60 * 30 )

                   GROUP BY name,lines ) a
left join
          (

                   select regexp_replace(fl_name, '.[0-9]+$') as fl_name,
                          sum(recordscount) recordscount
                          from (

                   SELECT   ${fileStatsFileName} AS fl_name,
                            max(recordscount) recordscount
                   FROM     ${schema}.${fileStats}
                   WHERE    tbl_dt >= ${minTblDt}
                                   AND      starttimets <= ( ${START_TIME_IN_MILLISEC} - 1000 * 60 * 30 )
                   GROUP BY ${fileStatsFileName} , recordscount
                   )
                   GROUP BY regexp_replace(fl_name, '.[0-9]+$')

           ) b
ON        ( a.name = b.fl_name)
WHERE     (b.fl_name IS NULL or (a.lines - b.recordscount ) >= 2 ) --b.fl_name IS NULL
AND       a.name IS NOT NULL
AND       length(a.name) > 0
