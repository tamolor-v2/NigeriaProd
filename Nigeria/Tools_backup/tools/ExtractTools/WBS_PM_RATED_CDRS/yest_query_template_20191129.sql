select /*+ parallel 8 */ * from WBS_CLIENT.WBS_CDR_${date} WHERE  ANUM is not null --process_date >= to_date('${date}000000','yyyymmddhh24miss')  
