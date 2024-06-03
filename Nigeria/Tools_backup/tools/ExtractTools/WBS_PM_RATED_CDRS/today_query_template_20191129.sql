select /*+ parallel 8 */ * from WBS_CLIENT.WBS_CDR_${prev} WHERE  process_date >= to_date('${date}000000','yyyymmddhh24miss') and ANUM is not null  
