select * from WBS_CLIENT.WBS_CDR_${date} WHERE  ANUM is not null and process_date > to_date('${maxSeq}','yyyymmddhh24miss') and billing_date >= to_date('${date} 00:00:00','YYYYMMDD HH24:MI:SS') AND billing_date <= to_date('${date} ${hr}:59:59','YYYYMMDD HH24:MI:SS')  