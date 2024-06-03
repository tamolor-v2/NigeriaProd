SELECT * FROM TT_MSO_1.CUG_ACCESS_FEES WHERE timestamp_d  > to_date('${second} 23:59:59','YYYYMMDD HH24:MI:SS') AND timestamp_d <= to_date('${DATE} 23:59:59','YYYYMMDD HH24:MI:SS')

