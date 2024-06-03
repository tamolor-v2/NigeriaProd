start transaction;
delete from kpi_reports.Boost_nin where tbl_dt= cast((substring(cast(yyyymmddRunDate as varchar(8)),1,6) || '20') as int);

insert into kpi_reports.Boost_nin
(BusinessUnitName,
SourceTrxNumber,
TransactionType,
SourceEventDate,
CreditDate,
RollupDate,
TransactionAmtSourceCurr,
SourceCurrencyCode,
ParticipantName,
ParticipantEmail,
AttributeNumber30,
Attribute24,
Attribute20,
tbl_dt)
with NIN_CTE as (
 select 
 concat(tracking_id ,a.node_machine_tag) key_id,
 CAST(parse_datetime(CAST(tbl_dt AS varchar), 'yyyyMMdd') AS date) as dt,
'2201 - MTN Nigeria Communications' as BusinessUnitName,
 a.node_machine_tag,
 tracking_id ,
 tbl_dt,
 case when a.node_machine_tag like '%/%' then Substr (regexp_extract(a.node_machine_tag,'/\d{5,}' ) ,2)
 when a.node_machine_tag like 'MTN%' then b.ParticipantEmail
 when a.node_machine_tag like 'DROID%' then b.ParticipantEmail
 else null end as ParticipantEmail
from flare_8.nin_enrollment a 
left join hive5.kpi_reports.NIN_NODE_PARTNERLOOKUP b on a.node_machine_tag=b.node_machine_tag 
where tbl_dt between cast(substring(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d'),1,6) || '21' as int) and cast((substring(cast(yyyymmddRunDate as varchar(8)),1,6) || '20') as int) 
and nin_status = 'GENERATED'
) 
select BusinessUnitName, 
 case when test between 1 and 9 then tmp_ 
 when test between 10 and 99 then replace (tmp_,'000','00')
 when test between 100 and 999 then replace (tmp_,'000','0')
 when test between 1000 and 9999 then replace (tmp_,'000','') 
 end as SourceTrxNumber,
 'NIN TRANSACTION TYPE_NG' as TransactionType,
 SourceEventDate,
 SourceEventDate as CreditDate,
 SourceEventDate as RollupDate ,
 0 TransactionAmtSourceCurr,
 'NGN' as SourceCurrencyCode,
 ParticipantName,
 ParticipantEmail, 
 COUNT_Track_ID as AttributeNumber30,
 'NG_Manual_TRX' Attribute24,
 'DAAS' Attribute20,
 cast((substring(cast(yyyymmddRunDate as varchar(8)),1,6) || '20') as int) 
 from (
 select COUNT_Track_ID , 
 ParticipantName,
 ParticipantEmail,
 BusinessUnitName,
 SourceEventDate ,
 'NIN_'|| substr (cast(tbl_dt as varchar),5,2) || '2022_000' || cast (row_number() over (order by tbl_dt desc ) as VARCHAR) as tmp_,
 row_number() over (order by tbl_dt desc ) as test, tbl_dt from (
 select COUNT_Track_ID ,
 ParticipantName,
 ParticipantEmail,
 BusinessUnitName, 
 tbl_dt,
 date_format(date_parse( cast(cast((substring(cast(yyyymmddRunDate as varchar(8)),1,6) || '20') as int) as varchar),'%Y%m%d'),'%m/%d/%Y') SourceEventDate 
from( 
 select count(distinct tracking_id) COUNT_Track_ID,
 ParticipantName,
 ParticipantEmail ,
 tbl_dt,
 BusinessUnitName from 
 ( select a.*,b.customer_name as ParticipantName from NIN_CTE a left join hive5.kpi_reports.ifs_account_customer_map b on a.ParticipantEmail=b.account_number 
) group by ParticipantName,ParticipantEmail ,tbl_dt,BusinessUnitName
)
)
);

commit;
