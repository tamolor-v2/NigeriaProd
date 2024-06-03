start transaction;
delete from cvm_db.cvm20_distributor_activations where tbl_dt=yyyymmddRunDate;
insert into cvm_db.cvm20_distributor_activations
(msisdn_key, lga_name,state_name, dealer_code, dealer, tbl_dt) 
select msisdn_key,lga_name, state_name ,dealer_code, dealer, cast(date_key as int) tbl_dt 
from nigeria.device_activation_gagc_cube gagc 
left join  
(select distinct crm_lga_id,  agl_lga_cd ,  description lga_name   
from  flare_8.agl_crm_lga_map ) aa on gagc.reg_lga = aa.crm_lga_id
left join  (select distinct crm_state_cd ,description state_name from flare_8.agl_crm_state_map ) bb
on gagc.reg_state = bb.crm_state_cd
where tbl_dt   = yyyymmddRunDate   and RGS_TOTAL_FLAG = 1;
commit;
