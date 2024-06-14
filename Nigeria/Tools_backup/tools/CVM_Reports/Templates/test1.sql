start transaction;
insert into kamanja_test.nabil_test select count(*) from flare_8.cs6_sdp_cdr where tbl_dt=yyyymmddRunDate limit 10;
;commit;
