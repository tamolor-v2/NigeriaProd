start transaction;
delete from kpi_reports.boost_data_partner_commission_plan_report where tbl_dt= cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int);


insert into kpi_reports.boost_data_partner_commission_plan_report 
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
Quantity,
Attribute10,
Attribute23,
AttributeNumber2,
AttributeNumber26,
AttributeNumber31,
Attribute55,
Attribute56,
Attribute57,
Attribute58,
Attribute24,
Attribute20,
AttributeNumber14,
ATTRIBUTE_NUMBER15,
tbl_dt
)
with CTE_SD_GREY_IMIE  as
(
select 
BusinessUnitName,
SourceTrxNumber,
TransactionType,
SourceEventDate,
SourceEventDate CreditDate,
SourceEventDate RollupDate,
0 TransactionAmtSourceCurr,
SourceCurrencyCode,
ParticipantName,
ParticipantEmail,
0 Quantity,
null Attribute10,
null Attribute23,
0 AttributeNumber2,
0 AttributeNumber26,
0 AttributeNumber31,
NULL Attribute55,
 null Attribute56,
NULL Attribute57,
null Attribute58,
'S&D_Dealer_NG_Manual_TRX' Attribute24,
'DAAS_Load'Attribute20,
New_Old_Device_Imei_cnt AttributeNumber14,
Activation_Imei_cnt AttributeNumber15,
cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int)
FROM(
select *,
case when  test between 1 and 9 then  tmp_ 
                                            when test  between 10 and 99 then replace (tmp_,'000','00')
                                            when test  between 100 and 999 then replace (tmp_,'000','0')
                                            when test  between 1000 and 9999 then replace (tmp_,'000','') 
                                            when test  between 10000 and 99999 then replace (tmp_,'000','')
                                            when test  between 100000 and 999999 then replace (tmp_,'000','')
                                            end as SourceTrxNumber,
                                            date_format(SourceEventDate2,'%m/%d/%Y') as SourceEventDate
                                            FROM(
select *, row_number()  over (order by  SourceEventDate2  ) as test, 
'SD_DP_'||  substr (cast(tbl_dt as varchar),5,2) ||  substr (cast(tbl_dt as varchar),1,4)||  '_000' ||  cast (row_number()  over (order by  BusinessUnitName  ) as VARCHAR) as tmp_
FROM(
select T.*,
date_add('day', -1, date_add('month', 1, date_trunc('month',  date(parse_datetime(CAST(tbl_dt  AS varchar), 'yyyyMMdd')))))  SourceEventDate2  
from(
select '2201 - MTN Nigeria Communications' as BusinessUnitName,
'GREY_IMEI_DEVICE' as TransactionType,
0  TransactionAmtSourceCurr,
'NGN' SourceCurrencyCode,  ORACLE_CODE as ParticipantEmail,Customer_Name as ParticipantName ,tbl_dt , Activation_Imei_cnt,
New_Old_Device_Imei_cnt
from(
select SUM(Activation_Vol_check) as Activation_Imei_cnt,SUM( New_Old_Device_Vol_chk) as New_Old_Device_Imei_cnt,
ORACLE_CODE,Customer_Name, tbl_dt  from(
select imei_uploaded_by_agent,ORACLE_CODE,tbl_dt ,Customer_Name, case when VOL_MB >=200 then 1 else 0 end Activation_Vol_check,
case when VOL_MB >=1000 then 1 else 0 end New_Old_Device_Vol_chk
from(
select imei_uploaded_by_agent,tbl_dt ,BB.ORACLE_CODE,CC.Customer_Name,SUM(datavolume_mb)VOL_MB
  from (
select
agent_id,
agent_msisdn,
imei_uploaded_by_agent,
msisdn_uploaded_by_agent,
brand,
model_name,
imei_in_device,
msisdn_in_device,
date_format(reg_datetime,'%Y/%m/%d') as reg_datetime,
date_format(date_parse(cast(tracking_start_dt as varchar),'%Y%m%d'),'%Y-%m-%d') as tracking_start_dt,
date_format(date_parse(cast(tracking_end_dt as varchar),'%Y%m%d'),'%Y-%m-%d') as tracking_end_dt,
datavolume_mb,
rnk,
update_datetime,
 tbl_dt,
date_format(date_parse(cast(usg_mon as varchar)||'01','%Y%m%d'),'%Y-%m-%d') as usg_mon
from nigeria.imei_submission_commission_rpt
where
tbl_dt=cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int)
and usg_mon=cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m') as int)
and msisdn_in_device in (select msisdn_key from nigeria.segment5b5_mon
where tbl_dt = cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int) 
and aggr = 'monthly'
and dola between 0 and 89
and upper(flex_rge_1_txt) = 'NA'
and active_data_user = 0
and package_cd not in ('327', '329', '317', '318'))
) AA
left join  hive5.kpi_reports.SDagentActivation_ORACLE_map BB ON AA.agent_id=BB.UNIQUE_ID
left join hive5.kpi_reports.MASTERDATA_map_vw CC on CC.Account_Number=BB.ORACLE_CODE
where RNK=1
group by  imei_uploaded_by_agent,BB.ORACLE_CODE,CC.Customer_Name,tbl_dt 
)
)group by ORACLE_CODE,Customer_Name,tbl_dt  
)
)T 
)
)
)
),
 CTE_SD_OLD_IMIE  as
(
select 
BusinessUnitName,
SourceTrxNumber,
TransactionType,
SourceEventDate,
SourceEventDate CreditDate,
SourceEventDate RollupDate,
0 TransactionAmtSourceCurr,
SourceCurrencyCode,
ParticipantName,
ParticipantEmail,
0 Quantity,
null Attribute10,
null Attribute23,
0 AttributeNumber2,
0 AttributeNumber26,
0 AttributeNumber31,
NULL Attribute55,
 null Attribute56,
NULL Attribute57,
null Attribute58,
'S&D_Dealer_NG_Manual_TRX' Attribute24,
'DAAS_Load'Attribute20,
New_Old_Device_Imei_cnt AttributeNumber14,
Activation_Imei_cnt AttributeNumber15,
cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int)
FROM(
select *,
case when  test between 1 and 9 then  tmp_ 
                                            when test  between 10 and 99 then replace (tmp_,'000','00')
                                            when test  between 100 and 999 then replace (tmp_,'000','0')
                                            when test  between 1000 and 9999 then replace (tmp_,'000','') 
                                            when test  between 10000 and 99999 then replace (tmp_,'000','')
                                            when test  between 100000 and 999999 then replace (tmp_,'000','')
                                            end as SourceTrxNumber,
                                            date_format(SourceEventDate2,'%m/%d/%Y') as SourceEventDate
                                            FROM(
select *, row_number()  over (order by  SourceEventDate2  ) as test, 
'SD_DP_'||  substr (cast(tbl_dt as varchar),5,2) ||  substr (cast(tbl_dt as varchar),1,4)||  '_000' ||  cast (row_number()  over (order by  BusinessUnitName  ) as VARCHAR) as tmp_
FROM(
select T.*,
date_add('day', -1, date_add('month', 1, date_trunc('month',  date(parse_datetime(CAST(tbl_dt  AS varchar), 'yyyyMMdd')))))  SourceEventDate2  
from(
select '2201 - MTN Nigeria Communications' as BusinessUnitName,
'MTNN_IMEI_DEVICE' as TransactionType,
0  TransactionAmtSourceCurr,
'NGN' SourceCurrencyCode,  ORACLE_CODE as ParticipantEmail,Customer_Name as ParticipantName ,tbl_dt , Activation_Imei_cnt,
New_Old_Device_Imei_cnt
from(
select SUM(Activation_Vol_check) as Activation_Imei_cnt,SUM( New_Old_Device_Vol_chk) as New_Old_Device_Imei_cnt,
ORACLE_CODE,Customer_Name, tbl_dt  from(
select imei_uploaded_by_agent,ORACLE_CODE,tbl_dt ,Customer_Name, case when VOL_MB >=200 then 1 else 0 end Activation_Vol_check,
case when VOL_MB >=1000 then 1 else 0 end New_Old_Device_Vol_chk
from(
select imei_uploaded_by_agent,tbl_dt ,BB.ORACLE_CODE,CC.Customer_Name,SUM(datavolume_mb)VOL_MB
  from (
select
agent_id,
agent_msisdn,
imei_uploaded_by_agent,
msisdn_uploaded_by_agent,
brand,
model_name,
imei_in_device,
msisdn_in_device,
date_format(reg_datetime,'%Y/%m/%d') as reg_datetime,
date_format(date_parse(cast(tracking_start_dt as varchar),'%Y%m%d'),'%Y-%m-%d') as tracking_start_dt,
date_format(date_parse(cast(tracking_end_dt as varchar),'%Y%m%d'),'%Y-%m-%d') as tracking_end_dt,
datavolume_mb,
rnk,
update_datetime,
 tbl_dt,
date_format(date_parse(cast(usg_mon as varchar)||'01','%Y%m%d'),'%Y-%m-%d') as usg_mon
from nigeria.imei_submission_commission_rpt
where
tbl_dt=cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int)
and usg_mon=cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m') as int)
and msisdn_in_device in (select msisdn_key from nigeria.segment5b5_mon
where tbl_dt = cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int) 
and aggr = 'monthly'
and dola between 0 and 89
and upper(flex_rge_1_txt) = 'NA'
and active_data_user = 1
and package_cd not in ('327', '329', '317', '318'))
) AA
left join  hive5.kpi_reports.SDagentActivation_ORACLE_map BB ON AA.agent_id=BB.UNIQUE_ID
left join hive5.kpi_reports.MASTERDATA_map_vw CC on CC.Account_Number=BB.ORACLE_CODE
where RNK=1
group by  imei_uploaded_by_agent,BB.ORACLE_CODE,CC.Customer_Name,tbl_dt 
)
)group by ORACLE_CODE,Customer_Name,tbl_dt  
)
)T 
)
)
)
),
CTE_SD_SIM_SWAP as
(
select 
BusinessUnitName,
SourceTrxNumber,
TransactionType,
SourceEventDate,
SourceEventDate CreditDate,
SourceEventDate RollupDate,
TransactionAmtSourceCurr,
SourceCurrencyCode,
ParticipantName,
ParticipantEmail,
0 Quantity,
null Attribute10,
null Attribute23,
0 AttributeNumber2,
0 AttributeNumber26,
SIM_SWAP_COUNT AttributeNumber31,
NULL Attribute55,
Attribute56,
status Attribute57,
reason Attribute58,
'S&D_Dealer_NG_Manual_TRX' Attribute24,
'DAAS_Load'Attribute20,
0 AttributeNumber14,
0 AttributeNumber15,
cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int)
FROM(
select *,
case when  test between 1 and 9 then  tmp_ 
                                            when test  between 10 and 99 then replace (tmp_,'000','00')
                                            when test  between 100 and 999 then replace (tmp_,'000','0')
                                            when test  between 1000 and 9999 then replace (tmp_,'000','') 
                                            when test  between 10000 and 99999 then replace (tmp_,'000','')
                                            when test  between 100000 and 999999 then replace (tmp_,'000','')
                                            end as SourceTrxNumber,
                                            date_format(SourceEventDate2,'%m/%d/%Y') as SourceEventDate
FROM(
select *, row_number()  over (order by  SourceEventDate2  ) as test, 
'SD_DP_'||  substr (cast(tbl_dt as varchar),5,2) ||  substr (cast(tbl_dt as varchar),1,4)||  '_000' ||  cast (row_number()  over (order by  BusinessUnitName  ) as VARCHAR) as tmp_
from(
select distinct  T.*,K.Customer_Name as ParticipantName ,
case when Substr( upper(user_name) ,1,2) like 'FL'  then 'FREELANCER' else 'PARTNER' end as Attribute56,
date_add('day', -1, date_add('month', 1, date_trunc('month',  date(parse_datetime(CAST(tbl_dt  AS varchar), 'yyyyMMdd')))))  SourceEventDate2 
from(
select *, coalesce(usernamORACLE_CODE,shopORACLE_CODE) ParticipantEmail
 FROM(
select A.*, 
'2201 - MTN Nigeria Communications' as BusinessUnitName,
'TP_SIM_SWAP' as TransactionType,
0  TransactionAmtSourceCurr,
'NGN' SourceCurrencyCode, B.ORACLE_CODE as usernamORACLE_CODE,C.ORACLE_CODE as shopORACLE_CODE
from (
select  
status, user_name,  retail_shop, reason,tbl_dt,count(msisdn) SIM_SWAP_COUNT 
from FLARE_8.EDW_SIM_SWAP 
where tbl_dt >= cast(substring(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d'),1,6) || '01' as int) and tbl_dt<=cast(date_format(date_trunc('month', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')) + interval '-1' day, '%Y%m%d') as int) and status='EXECUTED'
group by 
status, user_name, retail_shop,reason,tbl_dt) A
left join hive5.kpi_reports.SDusername_ORACLE_map B  on A.user_name=B.username
left join hive5.kpi_reports.SDShop_ORACLE_map C on UPPER(A.retail_shop)=UPPER(C.shop)
)
) T
left join hive5.kpi_reports.MASTERDATA_map_vw k on T.ParticipantEmail=K.Account_Number
)
)
)
)
select * from CTE_SD_SIM_SWAP
union all
select * from CTE_SD_GREY_IMIE
union all
select * from CTE_SD_OLD_IMIE; 


commit;
