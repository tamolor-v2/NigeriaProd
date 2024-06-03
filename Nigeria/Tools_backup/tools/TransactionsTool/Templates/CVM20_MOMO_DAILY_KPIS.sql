start transaction;
delete from cvm_db.cvm20_momo_daily_kpis where tbl_dt = yyyymmddRunDate ;
insert into cvm_db.cvm20_momo_daily_kpis
SELECT
CAST(date_format(date_trunc('week', date_parse(CAST(d.tbl_dt AS varchar(10)), '%Y%m%d')), '%Y%m%d') AS bigint) Week_start_date
, instruct_hdr_type
, try_cast(CASE 
WHEN (instruct_hdr_type = 'TRANSFER_FROM_VOUCHER') THEN msisdn 
WHEN (instruct_hdr_type = 'REDEEM_CASH_VOUCHER') THEN supplementary_data_receiver_voucher_user 
WHEN (instruct_hdr_type = 'CREATE_CASH_VOUCHER') THEN supplementary_data_sender_voucher_user 
WHEN regexp_like(instruct_to_accnt_hldr_usr_name, 'flutter.sp') THEN split_part(instruct_to_message, ':', 4) 
WHEN regexp_like(instruct_to_accnt_hldr_usr_name, 'AccessBank-External.sp') THEN split_part(instruct_to_message, ':', 4) 
WHEN (instruct_hdr_type = 'BILL PAYMENT') THEN split_part(instruct_from_message, ':', 2) 
WHEN (instruct_hdr_type LIKE 'CUSTOM_%') THEN substr(split_part(instruct_to_fri, ':', 2), 1, 13) 
WHEN (instruct_hdr_type = 'PAYMENT') THEN substr(split_part(instruct_to_fri, ':', 2), 1, 13) ELSE substr(split_part(instruct_to_fri, ':', 2), 1, 13) 
END  as bigint) MSISDN_key
, original_timestamp_enrich Transaction_Date
, instruct_from_accnt_hldr_msisdn Agent_ID_Initiator
, instruct_to_accnt_hldr_msisdn Agent_ID_receiver
, CASE 
WHEN regexp_like(Sub_Transaction_Type, 'MTN DATA|MTN LOGICAL|AIRTEL LOGICAL|AIRTEL DATA|GLO LOGICAL|GLO DATA|9MOBILE LOGICAL|9MOBILE DATA|GLO AIRTIME|9MOBILE AIRTIME|AIRTEL AIRTIME') THEN 'PAYMENT' 
WHEN regexp_like(instruct_to_accnt_hldr_usr_name, 'mtnairtime|cisuser|airuser|mtndatabundle|mtnlogicalpin') THEN 'PAYMENT' 
WHEN (Sub_Transaction_Type IN ('Bill Payment', 'MTN Airtime/MTN Data', 'Other Telco Airtime/Data', 'Assisted Deposit')) THEN 'PAYMENT' 
WHEN (Sub_Transaction_Type IN ('Assisted Deposit')) THEN 'PAYMENT' 
WHEN (Sub_Transaction_Type IN ('Bulk Token Redemption')) THEN 'TRANSFER_FROM_VOUCHER' 
WHEN (Sub_Transaction_Type IN ('Creation C2C')) THEN 'CREATE_CASH_VOUCHER' 
WHEN (Sub_Transaction_Type IN ('Redeem C2C')) THEN 'REDEEM_CASH_VOUCHER' 
WHEN (instruct_from_accnt_hldr_usr_name = 'flutter.sp') THEN instruct_hdr_type 
END Transaction_Type
, Sub_Transaction_Type
, instruct_to_accnt_hldr_usr_name
, instruct_from_accnt_hldr_usr_name
, instruct_hdr_context MOMO_Channel
, instruct_to_ifee Charge_on_receiver_agent
, Instruct_from_ifee Charge_on_Initiator_agent
, instruct_amount MOMO_Txn_Amt
, d.tbl_dt
FROM 
(SELECT
original_timestamp_enrich
, CASE 
WHEN (instruct_to_accnt_hldr_usr_name IN ('airuser.sp', 'mtnairtime.sp')) THEN 'MTN AIRTIME' 
WHEN (instruct_to_accnt_hldr_usr_name IN ('cisuser.sp', 'mtndatabundle.sp')) THEN 'MTN DATA' 
WHEN (instruct_to_accnt_hldr_usr_name = 'mtnlogicalpin.sp') THEN 'MTN LOGICAL' 
WHEN (instruct_to_accnt_hldr_usr_name = 'airtellogicalpin.sp') THEN 'AIRTEL LOGICAL' 
WHEN (instruct_to_accnt_hldr_usr_name = 'airteldatabundle.sp') THEN 'AIRTEL DATA' 
WHEN (instruct_to_accnt_hldr_usr_name = 'glologicalpin.sp') THEN 'GLO LOGICAL' 
WHEN (instruct_to_accnt_hldr_usr_name = 'glodatabundle.sp') THEN 'GLO DATA' 
WHEN (instruct_to_accnt_hldr_usr_name = '9mobilelogicalpin.sp') THEN '9MOBILE LOGICAL' 
WHEN (instruct_to_accnt_hldr_usr_name = '9mobiledatabundle.sp') THEN '9MOBILE DATA' 
WHEN (instruct_to_accnt_hldr_usr_name = 'airtelairtime.sp') THEN 'AIRTEL AIRTIME' 
WHEN (instruct_to_accnt_hldr_usr_name = 'gloairtime.sp') THEN 'GLO AIRTIME' 
WHEN (instruct_to_accnt_hldr_usr_name = '9mobileairtime.sp') THEN '9MOBILE AIRTIME' 
WHEN regexp_like(instruct_to_accnt_hldr_usr_name, 'creditswitchdstv|creditswitchgotv|jed|phedc|aedc|kedco|ekedc|fenix|kaedco|eedc|ikedc|startimes|betway|ibedc|afenco') THEN 'Bill Payment' 
WHEN regexp_like(instruct_to_accnt_hldr_usr_name, 'glodatabundle|9mobileairtime|gloairtime|9mobiledatabundle|glologicalpin|airtelairtime|airteldatabundle|9mobilelogicalpin|airtellogicalpin.sp') THEN 'Other Telco Airtime/Data' 
WHEN regexp_like(instruct_to_accnt_hldr_usr_name, 'AccessBank-External.sp') THEN 'Assisted Deposit'
WHEN (regexp_like(instruct_from_accnt_hldr_usr_name, 'flutter.sp') AND (NOT (instruct_hdr_type IN ('CUSTOM_COMMISSIONTRANSFER', 'COMMISSIONING', 'DEPOSIT')))) THEN 'Assisted Withdrawal' 
WHEN (instruct_hdr_type = 'CREATE_CASH_VOUCHER') THEN 'Creation C2C' 
WHEN (instruct_hdr_type = 'REDEEM_CASH_VOUCHER') THEN 'Redeem C2C'
WHEN (instruct_hdr_type = 'TRANSFER_FROM_VOUCHER') THEN 'Bulk Token Redemption' 
WHEN (instruct_hdr_type IN ('PAYMENT')) THEN 'Bill Payment' 
END SUB_TRANSACTION_TYPE
, CASE 
WHEN (TRY_CAST(instruct_to_accnt_hldr_msisdn AS bigint) > 0) THEN TRY_CAST(instruct_to_accnt_hldr_msisdn AS bigint) ELSE msisdn_key 
END agentmsisdn
, instruct_hdr_tid
, instruct_amount amount
, TRY_CAST(instruct_to_ifee AS double) instruct_to_ifee
, TRY_CAST(instruct_from_ifee AS double) instruct_from_ifee
, instruct_to_accnt_hldr_usr_name
, instruct_from_accnt_hldr_msisdn
, instruct_to_accnt_hldr_msisdn
, instruct_hdr_context
, instruct_from_message
, instruct_to_message
, instruct_amount
, instruct_to_fri
, instruct_from_fro_id
, instruct_hdr_type
, instruct_to_accnt_hldr_usr_prf
, instruct_from_accnt_hldr_usr_name
, TRY_CAST(instruct_from_fro_id AS bigint)  id
, tbl_dt
from  flare_8.financial_log l
WHERE tbl_dt = yyyymmddRunDate
and   status_code = 'EXECUTED'
AND   hdr_type = 'RESERVATION' 
AND   (regexp_like(instruct_hdr_type, 'CREATE_CASH_VOUCHER|REDEEM_CASH_VOUCHER|TRANSFER_FROM_VOUCHER|PAYMENT') OR instruct_hdr_type LIKE 'CUSTOM_%W') 
)  d
LEFT JOIN (SELECT
supplementary_data_sender_voucher_user
, supplementary_data_receiver_voucher_user
, TRY_CAST(s.transaction_id AS bigint) transaction_id
, tbl_dt
from  flare_8.audit_logs s
WHERE transaction_type IN ('CreateCashVoucher', 'RedeemVoucher') 
AND   status = 'SUCCESS'
and   tbl_dt = yyyymmddRunDate
)  s ON (d.instruct_hdr_tid = s.transaction_id)
LEFT JOIN (SELECT 
 id
, msisdn
, concat(firstname, ' ', surname) names
, tbl_dt
FROM flare_8.ewp_account_holders_dump dmp
where dmp.tbl_dt = yyyymmddRunDate
limit 1
)  e ON (d.id = e.id)
where d.Sub_Transaction_Type is not null;
commit;
