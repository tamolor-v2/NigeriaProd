start transaction;
delete from kpi_reports.Project_Jaguar_MFS_Commission_Report_AG where tbl_dt= cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int); 

insert into kpi_reports.Project_Jaguar_MFS_Commission_Report_AG
(businessunitname,sourcetrxnumber,transactiontype,sourceeventdate,creditdate,rollupdate,transactionamtsourcecurr,sourcecurrencycode,ParticipantEmail,
participantname,attributenumber38,tbl_dt)
select businessunitname,sourcetrxnumber,transactiontype,sourceeventdate,creditdate,rollupdate,transactionamtsourcecurr,sourcecurrencycode,'' ParticipantEmail,
participantname,attributenumber38, cast(date_format(date_parse(rollupdate,'%m/%d/%Y'),'%Y%m%d') as int)
from
( with registration as
(
select MSISDN_KEY,DATE_KEY,sim_reg_device KIT_TAG, DEALER, DEALER_CODE,RGS_DATE,RGS_CONV_DAY,
row_number() over (partition by msisdn_key order by date_key asc ) rnk 
from nigeria.device_activation_gagc_cube
WHERE (date_key BETWEEN  cast(substring(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d'),1,6) || '01' as int)
          AND cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int))
and RGS_TOTAL_FLAG = 1 
),
Recharges as
(
select MSISDN_KEY,sum(amount) first_recharge_total,min(date_key) first_recharge_date_key,event_type, count(1) first_recharge_cnt
from nigeria.daas_daily_usage_by_msisdn
WHERE (date_key BETWEEN  cast(substring(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d'),1,6) || '01' as int)
          AND cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int))
and event_type = 'DYA'
and amount >= 100
group by event_type, MSISDN_KEY
)
select
'2201 - MTN Nigeria Communications' BusinessUnitName, 
concat(concat(concat('MFS_PJ_', CAST(date_format(date_parse(CAST(RollupDate AS varchar), '%m/%d/%Y'), '%m%Y') AS varchar)), '_'),
lpad(CAST(row_number() OVER (PARTITION BY 1) AS varchar), 5, '0')) SourceTrxNumber,
'MFS_PROJECT_JAGUAR' TransactionType,
SourceEventDate,CreditDate,RollupDate,TransactionAmtSourceCurr,SourceCurrencyCode,
KIT_TAG ParticipantEmail,
ParticipantName,
AttributeNumber38,ATTRIBUTE17
from(select
date_format(date_add('day', -1, date_trunc('month', date_add('month', 1, date_parse(cast(a.date_key as varchar),'%Y%m%d')))), '%m/%d/%Y') SourceEventDate
,date_format(date_add('day', -1, date_trunc('month', date_add('month', 1, date_parse(cast(a.date_key as varchar),'%Y%m%d')))), '%m/%d/%Y') CreditDate
,date_format(date_add('day', -1, date_trunc('month', date_add('month', 1, date_parse(cast(a.date_key as varchar),'%Y%m%d')))), '%m/%d/%Y') RollupDate
, '0' TransactionAmtSourceCurr
,'NGN' SourceCurrencyCode
, KIT_TAG ParticipantName
, sum(first_recharge_cnt) AttributeNumber38
, KIT_TAG ATTRIBUTE17, KIT_TAG
from registration a
inner join Recharges b on a.msisdn_key=b.msisdn_key
left join kpi_reports.IFS_ORACLE_FUSION_MAP c on a.DEALER_CODE =c.sas_code
where a.date_key <= b.first_recharge_date_key
and a.rnk=1
group by a.KIT_TAG,
date_format(date_add('day', -1, date_trunc('month', date_add('month', 1, date_parse(cast(a.date_key as varchar),'%Y%m%d')))), '%m/%d/%Y')
)) ;

commit;
