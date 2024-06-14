start transaction;
delete from cvm_db.cvm20_refill_data_bundle where tbl_dt =yyyymmddRunDate;
insert into cvm_db.cvm20_refill_data_bundle (
select a.*,yyyymmddRunDate from cvm_db.cvm20_refill_and_data_bundle_uat_tmp2 a );
commit;
