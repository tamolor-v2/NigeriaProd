set tez.am.resource.memory.mb = 2048;
set hive.tez.container.size=8096;
set tez.runtime.io.sort.mb = 409;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
 insert overwrite table databasename.files_filesopps_summary_partition partition (feed_name,file_dt)
 select distinct name,lines,feed_name,file_dt from
 (select name,lines, "FeedNameValues" feed_name,case when upper(path) like "%PART%" then 1907 else nvl(split(substring(substring(path,INSTR(path,'ArchiveKeyWork')+3),1),"/")[2],1970) end file_dt
 from databasename.files_filesopps_summary where lines > 0  and path like "%FeedNameCondition%" ) FilesOps ;

