start transaction;
delete from cvm_db.cvm20_attributes where tbl_dt = yyyymmddRunDate and subject_area = 'Activity';
insert into cvm_db.cvm20_attributes 
select  
msisdn_key
  , attribute
  , attribute_value attribute_value
  , null attr_str
  ,yyyymmddRunDate week_started  
  ,yyyyRunDateWeek week_ended
  , yyyymmddRunDate tbl_dt
  ,'Activity' subject_area
from (select 
msisdn_key
,'act_days_voi_onnet_moc' Attribute
, count(msisdn_key) attribute_value
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate  and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_voice_onnet_outgoing = 0
group by msisdn_key
union all
select 
msisdn_key
,'act_days_voi_offnet_moc' Attribute
,count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_voice_offnet_outgoing = 0
group by msisdn_key
union all
select 
msisdn_key
 ,'act_days_voi_roam_moc' Attribute
,count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_voice_roaming_outgoing = 0
group by  msisdn_key
union all
select 
msisdn_key
 ,'act_days_voi_all_moc' Attribute
 ,count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_voice_rge = 0
group by  msisdn_key
union all
select 
msisdn_key
,'act_days_voi_offnet_mtc' Attribute
,count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola <=89
and exclusion_status = 'NA'
and dola_voice_offnet_incoming = 0
group by msisdn_key
union all
select 
msisdn_key
 ,'act_days_voi_intl_mtc' Attribute
 ,count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_voice_international_incoming = 0
group by msisdn_key
union all
select 
msisdn_key
, 'act_days_voi_roam_mtc' Attribute
, count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_voice_roaming_incoming = 0
group by msisdn_key
union all
select 
msisdn_key
, 'act_days_sms_all_mtc' Attribute
, count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_sms_international_incoming = 0
group by msisdn_key
union all
select 
msisdn_key
, 'act_days_sms_onnet_moc' Attribute
, count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_sms_onnet_outgoing = 0
group by msisdn_key
union all
select 
msisdn_key
, 'act_days_sms_offnet_moc' Attribute
, count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_sms_offnet_outgoing = 0
group by msisdn_key
union all
select 
msisdn_key
, 'act_days_sms_intl_moc' Attribute
, count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_sms_international_outgoing = 0
group by msisdn_key
union all
select 
msisdn_key
, 'act_days_sms_roam_moc' Attribute
, count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_sms_roaming_outgoing = 0
group by msisdn_key
union all
select 
msisdn_key
, 'act_days_sms_all_moc' Attribute
, count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_sms_rge= 0
group by msisdn_key
union all
select 
msisdn_key
 ,'act_days_data_roam' Attribute
 ,count(msisdn_key) cnt
from nigeria.segment5b5_usg
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr ='daily'
and data_roam_kb > 0
group by  msisdn_key
union all
select
 msisdn_key
 ,'act_days_data' Attribute
 ,count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_data_rge= 0
group by  msisdn_key
union all
select 
 msisdn_key
   , 'act_days_moc' Attribute
 , count(msisdn_key) CNT
  from (select
msisdn_key,
sum (case
when dola_sms_offnet_outgoing = 0 or
dola_sms_international_outgoing = 0 or
dola_sms_onnet_outgoing =0 or
dola_voice_international_outgoing =0 or
dola_voice_offnet_outgoing =0 or
dola_voice_onnet_outgoing =0 or
dola_data_outgoing =0 then 1 else 0
 end
) sts
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
group by msisdn_key
)
  where sts > 0
  group by msisdn_key
union all
  select 
  msisdn_key
  ,'act_days_rge' Attribute
  ,count(msisdn_key) cnt
  from nigeria.segment5b5_sub
  where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
  and aggr='daily'
  and dola = 0
  and exclusion_status = 'NA'
  group by msisdn_key
union all
select 
msisdn_key
,'act_days_mtc' Attribute
,sum (CNT)
from (select msisdn_key,count(msisdn_key) CNT from
(select msisdn_key,
 sum (case when dola_sms_international_incoming = 0
  or dola_voice_international_incoming =0 or dola_voice_offnet_incoming =0 then 1 else
  0 end)sts
  from nigeria.segment5b5_sub
  where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
  and aggr='daily'
  and dola<=89
  and exclusion_status = 'NA'
  group by msisdn_key
)
where sts > 0
group by msisdn_key
) group by msisdn_key
union all
  select 
  msisdn_key
  ,'act_days_roam' Attribute
  ,count(msisdn_key) cnt
  from nigeria.segment5b5_rev
  where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
  and aggr='daily'
  and roam_rev > 0
  group by msisdn_key
union all
select 
msisdn_key
 ,'act_days_voi_intl_moc' Attribute
,count(msisdn_key) cnt
from nigeria.segment5b5_sub
where tbl_Dt between yyyymmddRunDate and yyyyRunDateWeek
and aggr='daily'
and dola<=89
and exclusion_status = 'NA'
and dola_voice_international_outgoing = 0
group by msisdn_key
union all
select 
msisdn_key
,'number_of_active_periods' Attribute
, 1 attribute_value
from  flare_8.customersubject
where tbl_dt  between yyyymmddRunDate and yyyyRunDateWeek
and   (act_days_moc > 0 or act_days_mtc > 0)
and   aggr='daily'
group by msisdn_key
);
commit;
