start transaction;
insert into CVM_DB.CVM20_REFILL_INFO 
SELECT
msisdn_key
,(CASE WHEN ("upper"(event_type) IN ('APPRECHG', 'BROADBAND DATA RESET', 'DATARESET', 'DEMAND', 'SMARTAPP', 'SP DATA RESET', 'SP ECW&DYA', 'SP SMART APP', 'MOD DATA', 'MOD FASTLINK', 'MOD GOODYBAG', 'MOD HELLOWORLDROAMING', 'MOD ICB', 'MOD VOICE', 'SMART WEB AIRTIME', 'MOD FASTLINKHOTDEALS')) THEN 'DIGITAL' WHEN (event_type IN ('VOUCHER')) THEN 'VOUCHER' ELSE 'ELECTRONIC' END) transactiontype
,"sum"(Amount) RefillAmount
,"sum"(rec_count) recharge_frequency
,'Naira' transactionCurrency
,cast(date_key as int) tbl_dt
FROM
  nigeria.daas_daily_usage_by_msisdn
WHERE (((date_key = yyyymmddRunDate) 
AND (product_type IN ('RECHARGES', 'VTU Other', 'Bank On-Demand'))) AND (NOT ("upper"(event_type) LIKE '%MIGRATION%')))
GROUP BY (CASE WHEN ("upper"(event_type) IN ('APPRECHG', 'BROADBAND DATA RESET', 'DATARESET', 'DEMAND', 'SMARTAPP', 'SP DATA RESET', 'SP ECW&DYA', 'SP SMART APP', 'MOD DATA', 'MOD FASTLINK', 'MOD GOODYBAG', 'MOD HELLOWORLDROAMING', 'MOD ICB', 'MOD VOICE', 'SMART WEB AIRTIME', 'MOD FASTLINKHOTDEALS')) THEN 'DIGITAL' WHEN (event_type IN ('VOUCHER')) THEN 'VOUCHER' ELSE 'ELECTRONIC' END), msisdn_key, date_key;
commit;
