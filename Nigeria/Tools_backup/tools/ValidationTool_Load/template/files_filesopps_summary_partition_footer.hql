 else "UK" end) feed_name,case when upper(path) like "%PART%" then 1907 else nvl(split(substring(substring(path,INSTR(path,'ArchiveKeyWork')+3),1),"/")[2],1970) end file_dt
 from databasename.files_filesopps_summary where lines > 0 and tbl_dt="targetdate") FilesOps ;
