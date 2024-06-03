set tez.am.resource.memory.mb = 3048;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.tez.container.size=40096;
INSERT OVERWRITE TABLE dev.CS5_CCN_VOICE_DA partition (tbl_dt) select  tbl_dt from (select *,row_number() over( partition by file_name,file_offset) seq from dev.CS5_CCN_VOICE_DA where tbl_dt =20180302) c where c.seq=1;
