start transaction;
insert into flare_8.CR_BAL_EOD
select * from (select  *  from (select *,row_number() over(partition by agent_msisdn order by EOD_RNK desc,eod_bal )eod_max_rank from (
select EOD_Bal, Agent_msisdn, max(EOD_RNK)EOD_RNK_m, EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_from_bal_tot_aft as EOD_Bal, 
RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth  and yyyymmddRunDate
and hdr_type = 'RESERVATION' and status_code='EXECUTED' and instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_prf = 'MTN Airtime Service Provider Account Holder' 
and instruct_to_accnt_hldr_usr_name IN  ('airuser.sp', 'mtnairtime.sp') and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK
union all
select EOD_Bal, Agent_msisdn, max(EOD_RNK), EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_from_bal_tot_aft as EOD_Bal, 
RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth  and yyyymmddRunDate
and hdr_type = 'RESERVATION' and status_code='EXECUTED' and instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_prf =  'YDFS Data Bundle Service Provider Account New' 
and instruct_to_accnt_hldr_usr_name IN  ('cisuser.sp','mtndatabundle.sp') 
and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK
union all
select EOD_Bal, Agent_msisdn, max(EOD_RNK), EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_from_bal_tot_aft as EOD_Bal, RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth  and yyyymmddRunDate
and hdr_type = 'RESERVATION' and status_code='EXECUTED' and instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_prf =  'YDFS Service Provider Profile' 
and  instruct_to_accnt_hldr_usr_name in ('airtelairtime.sp','gloairtime.sp','9mobileairtime.sp','9mobilelogicalpin.sp','airtellogicalpin.sp','glologicalpin.sp')
and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK
union all
select EOD_Bal, Agent_msisdn, max(EOD_RNK), EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_from_bal_tot_aft as EOD_Bal, RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth  and yyyymmddRunDate
and hdr_type = 'RESERVATION' and status_code='EXECUTED' and instruct_hdr_type = 'PAYMENT'
and instruct_to_accnt_hldr_usr_name in ('9mobiledatabundle.sp','airteldatabundle.sp','glodatabundle.sp')
and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK
union all
select EOD_Bal, Agent_msisdn, max(EOD_RNK), EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_from_bal_tot_aft as EOD_Bal, RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth  and yyyymmddRunDate
and hdr_type = 'RESERVATION' and status_code='EXECUTED'and instruct_hdr_type in ('PAYMENT', 'PAYMENT_SEND')
and instruct_to_accnt_hldr_usr_prf in  ('YDFS Service Provider Profile', 'Star Times Service Provider Profile', 'Retail Agent Account Holder Profile WATU') and 
instruct_to_accnt_hldr_usr_name NOT in ('9mobiledatabundle.sp','9mobilelogicalpin.sp','airteldatabundle.sp','airtellogicalpin.sp','glodatabundle.sp','glologicalpin.sp','mtnlogicalpin.sp')
and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK
union all
select EOD_Bal, Agent_msisdn, max(EOD_RNK), EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_from_bal_tot_aft as EOD_Bal, RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth  and yyyymmddRunDate
and instruct_hdr_type = 'PAYMENT' and instruct_to_accnt_hldr_usr_prf = 'YDFS Banks Service Provider Profile'
and instruct_to_sp = 'AccessBank-External.sp' and hdr_type = 'RESERVATION' and status_code='EXECUTED' 
and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK
union all
select EOD_Bal, Agent_msisdn, max(EOD_RNK), EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_to_bal_tot_aft as EOD_Bal, RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth  and yyyymmddRunDate
and instruct_hdr_type = 'TRANSFER' and instruct_from_accnt_hldr_usr_name = 'flutter.sp' and 
instruct_hdr_type not in ('CUSTOM_COMMISSIONTRANSFER','COMMISSIONING', 'DEPOSIT') and hdr_type = 'RESERVATION' 
and status_code='EXECUTED' and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK
union all
select EOD_Bal, Agent_msisdn, max(EOD_RNK), EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_from_bal_tot_aft as EOD_Bal, RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type = 'CREATE_CASH_VOUCHER' and hdr_type = 'RESERVATION' 
and status_code='EXECUTED' and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK
union all
select EOD_Bal, Agent_msisdn, max(EOD_RNK), EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_to_bal_tot_aft as EOD_Bal, RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth  and yyyymmddRunDate
and instruct_hdr_type = 'REDEEM_CASH_VOUCHER'and hdr_type = 'RESERVATION' 
and status_code='EXECUTED' and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK
union all
select EOD_Bal, Agent_msisdn, max(EOD_RNK), EOD_RNK from(
select instruct_from_accnt_hldr_msisdn as Agent_msisdn, instruct_from_bal_tot_aft as EOD_Bal, RANK() OVER (
ORDER BY original_timestamp_enrich desc 
) as EOD_RNK
from flare_8.financial_log 
where tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type = 'TRANSFER_FROM_VOUCHER'
and hdr_type = 'RESERVATION' and status_code='EXECUTED' and msisdn_key <>0 ) 
group by Agent_msisdn,EOD_Bal,EOD_RNK)
) where eod_max_rank=1);
commit;
