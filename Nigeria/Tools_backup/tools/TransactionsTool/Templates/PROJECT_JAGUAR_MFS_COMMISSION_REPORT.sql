start transaction;
delete from kpi_reports.Project_Jaguar_MFS_Commission_Report where tbl_dt= cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int); 

insert into kpi_reports.Project_Jaguar_MFS_Commission_Report
(businessunitname,sourcetrxnumber,transactiontype,sourceeventdate,creditdate,rollupdate,transactionamtsourcecurr,sourcecurrencycode,ParticipantEmail,
participantname,attributenumber38,tbl_dt)
select businessunitname,sourcetrxnumber,transactiontype,sourceeventdate,creditdate,rollupdate,transactionamtsourcecurr,sourcecurrencycode,ParticipantEmail,
participantname,attributenumber38, cast(date_format(date_parse(rollupdate,'%m/%d/%Y'),'%Y%m%d') as int)
from (
WITH
registration AS (
SELECT
MSISDN_KEY
, DATE_KEY
, sim_reg_device KIT_TAG
, DEALER
, DEALER_CODE
, RGS_DATE
, RGS_CONV_DAY
, "row_number"() OVER (PARTITION BY msisdn_key ORDER BY date_key ASC) rnk
FROM
nigeria.device_activation_gagc_cube
WHERE (date_key BETWEEN  cast(substring(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d'),1,6) || '01' as int) 
          AND cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int))
       AND (RGS_TOTAL_FLAG = 1)
)
, Recharges AS (
SELECT
MSISDN_KEY
, "sum"(amount) first_recharge_total
, "min"(date_key) first_recharge_date_key
, event_type
, "count"(1) first_recharge_cnt
FROM
nigeria.daas_daily_usage_by_msisdn
WHERE (date_key BETWEEN  cast(substring(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d'),1,6) || '01' as int)
         AND cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int))
AND (event_type = 'DYA') AND (amount >= 100)
GROUP BY event_type, MSISDN_KEY
)
SELECT
'2201 - MTN Nigeria Communications' BusinessUnitName
, "concat"("concat"("concat"('MFS_PJ_', CAST("date_format"("date_parse"(CAST(RollupDate AS varchar), '%m/%d/%Y'), '%m%Y') AS varchar)), '_'), "lpad"(CAST("row_number"() OVER (PARTITION BY 1) AS varchar), 5, '0')) SourceTrxNumber
, 'MFS_PROJECT_JAGUAR' TransactionType
, SourceEventDate
, CreditDate
, RollupDate
, TransactionAmtSourceCurr
, SourceCurrencyCode
, (CASE WHEN ("trim"(new_account) = '') THEN sas_code ELSE new_account END) ParticipantEmail
, (CASE WHEN ("trim"(oracle_name) = '') THEN ParticipantName ELSE oracle_name END) ParticipantName
, AttributeNumber38
FROM
(
SELECT
"date_format"("date_add"('day', -1, "date_trunc"('month', "date_add"('month', 1, "date_parse"(CAST(a.date_key AS varchar), '%Y%m%d')))), '%m/%d/%Y') SourceEventDate
, "date_format"("date_add"('day', -1, "date_trunc"('month', "date_add"('month', 1, "date_parse"(CAST(a.date_key AS varchar), '%Y%m%d')))), '%m/%d/%Y') CreditDate
, "date_format"("date_add"('day', -1, "date_trunc"('month', "date_add"('month', 1, "date_parse"(CAST(a.date_key AS varchar), '%Y%m%d')))), '%m/%d/%Y') RollupDate
, '0' TransactionAmtSourceCurr
, 'NGN' SourceCurrencyCode
, a.dealer ParticipantName
, "sum"(first_recharge_cnt) AttributeNumber38
, c.new_account
, c.oracle_name
, sas_code
, a.dealer
FROM
((registration a
INNER JOIN "Recharges" b ON (a.msisdn_key = b.msisdn_key))
LEFT JOIN kpi_reports."IFS_ORACLE_FUSION_MAP" c ON (a.DEALER_CODE = c.sas_code))
WHERE ((a.date_key <= b.first_recharge_date_key) AND (a.rnk = 1))
GROUP BY a.dealer, sas_code, oracle_name, new_account, "date_format"("date_add"('day', -1, "date_trunc"('month', "date_add"('month', 1, "date_parse"(CAST(a.date_key AS varchar), '%Y%m%d')))), '%m/%d/%Y')
));

commit;
