select /*+ parallel 8 */ * from WBS_CLIENT.WBS_CDR_${date} WHERE  SUBSTR (anum,-1,1)='${anum}' and process_date > to_date('${maxSeq}','yyyymmddhh24miss') 
