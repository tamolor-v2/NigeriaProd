#!bin/bash

DATE_prev=$2
DATE=$1
Schema="flare_8"
Schema2="nigeria"
declare -a Feeds=("vw_er_subs_base" "vw_er_subs_base2" "vw_er_subs_base1" "vw_er_recharges" "vw_er_revenue" "vw_er_voice_usg" "vw_er_sms_usg" "vw_er_in_voice_traffic" "vw_er_data_traffic" "vw_engine_room_data_rec1" )
declare -a FeedsPos=(0 1 2 3 4 5 6 7 8 9 )


##Base views creation statements
##==============================

##Active and Churn Subs base

echo "Create ${Feeds[0]} view started.. "  
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " create view ${Schema}.${Feeds[0]} as select a.tbl_dt, a.msisdn_key, a.activation_dt, a.siteid site_id, a.tac tac_id, a.last_rge_dt,b.tbl_dt gross_connection_dt,0 churn_flag from ${Schema2}.segment5b5_sub a left join (select tbl_dt,msisdn_key from ${Schema2}.segment5b5_sub where tbl_dt = ${DATE} and aggr = 'daily' and total_rgs90 = 1 and gross_connection <> 0 and msisdn_key not in (select msisdn_key from ${Schema2}.segment5b5_sub where tbl_dt = ${DATE_prev} and aggr='daily'and total_rgs90=1 and gross_connection<>0)) b on a.tbl_dt = b.tbl_dt and a.msisdn_key = b.msisdn_key where a.aggr ='daily' and a.dola <=89 and a.exclusion_status = 'NA' and a.tbl_dt = ${DATE} "

## retunee
echo "Create ${Feeds[1]} view started.. " 
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " create view ${Schema}.${Feeds[1]} as select a.tbl_dt, a.msisdn_key, a.activation_dt, a.site_id, a.tac_id, a.last_rge_dt,a.gross_connection_dt, case when b.flag = 2 then b.flag else a.churn_flag end flag from ${Schema}.${Feeds[0]} a left join ( select tbl_dt, msisdn_key, 2 flag from ${Schema2}.segment5b5_sub where aggr ='daily' and dola = 0 and exclusion_status = 'NA' and tbl_dt = ${DATE} and sub_sts <> 'GROSS CONNECTIONS' and sub_sts = 'RETURNEES' and msisdn_key not in (select msisdn_key from ${Schema2}.segment5b5_sub where aggr ='daily' and tbl_dt = ${DATE_prev} and dola between 0 and 89) ) b on a.msisdn_key = b.msisdn_key"

## active subs, returnee 
 ## churn: flag = 1
echo "Create ${Feeds[2]} view started.. " 
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " create view ${Schema}.${Feeds[2]} as select * from ( select distinct a.tbl_dt, a.msisdn_key, a.activation_dt, a.site_id, a.tac_id, a.last_rge_dt,a.gross_connection_dt, null churn_dt,a.flag from ${Schema}.${Feeds[1]} a where tbl_dt = ${DATE} union all select distinct x.tbl_dt, x.msisdn_key, sub.activation_dt, sub.siteid, sub.tac, sub.last_rge_dt,sub.last_gross_connection_dt, x.tbl_dt churn_dt, 1 churn_flag from ${Schema2}.rgs_msisdn x , ${Schema2}.segment5b5_sub sub where x.tbl_dt = ${DATE} and x.mtd_gross_churnn = 1 and x.tbl_dt = sub.tbl_dt and x.msisdn_key = sub.msisdn_key and not exists ( select msisdn_key from ${Schema2}.rgs_msisdn y where tbl_dt = ${DATE_prev} and x.msisdn_key = y.msisdn_key and mtd_gross_churnn = 1 ) ) where last_rge_dt is not null"
 
## recharge count
###==============
echo "Create ${Feeds[3]} view started.. "  
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " create view ${Schema}.${Feeds[3]} as select date_key,msisdn_key, sum(rec_count) recharge_count, sum(case when product_type in ('VTU Other') then rec_count else 0 end) recharge_count_VTU_OTHER, sum(case when product_type in ('VTU Other') then amount else 0 end) recharge_revenue_VTU_OTHER, sum(case when product_type in ('RECHARGES') then rec_count else 0 end) recharge_count_RECHARGES, sum(case when product_type in ('RECHARGES') then amount else 0 end) recharge_revenue_RECHARGES, sum(case when product_type in ('Bank On-Demand') then rec_count else 0 end) recharge_count_BANK_ON_DEMAND, sum(case when product_type in ('Bank On-Demand') then amount else 0 end) recharge_revenue_BANK_ON_DEMAND from ${Schema2}.daas_daily_usage_by_msisdn where date_key = ${DATE} and product_type in('RECHARGES', 'VTU Other','Bank On-Demand') group by date_key,msisdn_key"
 
## ER Revenue
##===========
echo "Create ${Feeds[4]} view started.. "
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " create view ${Schema}.${Feeds[4]} as select m.tbl_dt, msisdn_key, sum(voice_rev) voice_revenue_total, sum(voice_rev_extra_val) voice_bundle_revenue, sum(voice_onnet_ma_rev) voice_bundle_onnet_revenue, sum(voice_offnet_ma_rev) voice_bundle_offnet_revenue, sum(voice_int_ma_rev) voice_bundle_international_revenue, sum(data_rev_extra_val) data_bundle_revenue, sum(data_2g_local_ma_rev) data_2g_revenue, sum(data_3g_local_ma_rev) data_3g_revenue, sum(data_4g_local_ma_rev) data_4g_revenue, sum(data_others_local_ma_rev) data_unknown_tech_revenue, sum(data_rev) data_revenue_total, sum(data_local_ma_rev) data_revenue_cdr,  sum(sms_rev) sms_revenue, sum(voice_onnet_ma_rev + voice_offnet_ma_rev+voice_int_ma_rev) voice_outgoing_revenue, sum(voice_onnet_ma_rev) voice_outgoing_onnet_revenue, sum(voice_offnet_ma_rev) voice_outgoing_offnet_revenue, sum(voice_int_ma_rev) voice_outgoing_international_revenue, sum(voice_onnet_ma_rev+voice_offnet_ma_rev+voice_int_ma_rev+voice_roam_in_ma_rev+voice_roam_out_ma_rev) voice_revenue, sum(voice_onnet_rev) voice_onnet_revenue, sum(voice_offnet_rev) voice_offnet_revenue, sum(voice_int_rev) voice_international_revenue from ${Schema2}.segment5b5_rev m where aggr ='daily' and m.tbl_dt = ${DATE} group by tbl_dt ,msisdn_key" 

## ER Usage
##========
echo "Create ${Feeds[5]} view started.. " 
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " create view ${Schema}.${Feeds[5]} as select tbl_dt, msisdn_key, sum(voice_onnet_nb_sou+voice_offnet_nb_sou+voice_int_nb_sou+voice_local_ma_da_sou)/60 voice_outgoing_minutes, sum(voice_onnet_sou)/60 voice_outgoing_onnet_minutes, sum(voice_offnet_sou)/60 voice_outgoing_offnet_minutes, sum(voice_int_sou)/60 voice_outgoing_international_minutes, sum(voice_onnet_sou)/60 voice_onnet_minutes, sum(voice_offnet_sou)/60 voice_offnet_minutes, sum(voice_int_sou)/60 voice_international_minutes from ${Schema2}.segment5b5_usg where aggr ='daily' and tbl_dt = ${DATE} group by tbl_dt ,msisdn_key"
 
# ER SMS
##======
echo "Create ${Feeds[6]} view started.. " 
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " create view ${Schema}.${Feeds[6]} as select tbl_dt,msisdn_key, count(1) sms_outgoing_count, sum(cast(totalcharge_money as Decimal )) sms_outgoing_revenue from ${Schema}.CS6_CCN_cdr a where tbl_dt = ${DATE} and servicetypeenrich = 'SMS' and totalcharge_money <> ''  and cast(totalcharge_money as Decimal ) > 0 and cast(netflag as int) = 0 group by tbl_dt,msisdn_key"
 
## ER Room Incoming Voice Traffic
##==============================
echo "Create ${Feeds[7]} view started.. "  
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " create view ${Schema}.${Feeds[7]} as
select
tbl_dt,msisdn_key,
sum(case when incoming_operator not in ('MSHUB','DC')
then call_duration else 0 end)/60 voice_incoming_minutes,
0 voice_incoming_onnet_minutes,
sum(case when incoming_operator not in ('MSHUB','DC') and anum_operator <> ''
then call_duration else 0 end)/60 voice_incoming_offnet_minutes,
sum(case when incoming_operator not in ('MSHUB','DC') and anum_operator = ''
then call_duration else 0 end)/60 voice_incoming_international_minutes
from ${Schema}.wbs_pm_rated_cdrs
where tbl_dt = ${DATE}
and call_direction = 'I'
group by tbl_dt, msisdn_key"
 
## ER Room Data traffic
##====================
echo "Create ${Feeds[8]} view started.. "  
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " create view ${Schema}.${Feeds[8]} as
select tbl_dt,msisdn_key,
sum(data_2g_roam_ma_kb+
data_2g_roam_da_unknown_kb+
data_2g_roam_da_chargeable_kb+
data_2g_local_ma_kb+
data_2g_local_da_unknown_kb+
data_2g_roam_ma_kb+
data_2g_roam_da_unknown_kb+
data_2g_local_da_chargeable_kb)/1024 data_2g_download_gb,
sum(data_3g_roam_ma_kb+
data_3g_roam_da_unknown_kb+
data_3g_local_ma_kb+
data_3g_local_da_unknown_kb+
data_3g_roam_da_chargeable_kb+
data_3g_local_da_chargeable_kb)/1024 data_3g_download_gb,
sum(data_4g_roam_ma_kb+
data_4g_roam_da_unknown_kb+
data_4g_local_da_chargeable_kb+
data_4g_roam_da_chargeable_kb+
data_4g_local_ma_kb+
data_4g_local_da_unknown_kb)/1024 data_4g_download_gb,
sum(data_others_roam_da_unknown_kb+
data_others_roam_da_chargeable_kb+
data_others_local_ma_kb+
data_others_local_da_unknown_kb+
data_others_local_da_chargeable_kb+
data_others_roam_ma_kb)/1024 data_unknown_tech_download_gb,
sum(data_kb/1024) data_download_gb
from ${Schema2}.segment5b5_usg
where aggr ='daily' and tbl_dt = ${DATE}
group by tbl_dt,msisdn_key"
 
## VAS bundle revenue
##-------------------
 
## RMS
##'BOX DOWNLOAD' -- added 'BOX DOWNLOAD' in filter
##'CALLERFEEL'
##'LINK-UP'
##'PHONEBOOK_BACKUP'
##'LOTTERY'
##'INSTANVOICE'
##'OTHER SDP VAS'
##'OTHER NON-SDP'
##'EBU'
echo "Insert into tbl_er_vas_bundle_rev table started.. "
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute " insert into ${Schema}.tbl_er_vas_bundle_rev
select *
from (
select
date_key,msisdn_key, sum(amount) REV_PRV,sum(partner_revenue_prv) partner_revenue_prv,
sum(mtn_revenue_prv) mtn_revenue_prv , 1 cat_type
from (
select date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) product_id, split_part(aa.network_type,'|',2) product_name,
ab.servicename, ab.partnername, ab.classification, upper(category) category, division division_name ,
sub_category,dndcategoryname , '0' service_category,'0' service_sub_category ,
sum( amount) amount,
sum(case when try_cast(ab.partnerrevenueshare as int) > 0 then amount*cast(ab.partnerrevenueshare as int)/100 else 0 end) partner_revenue_prv,
sum(case when try_cast(ab.mtnrevenueshare as int) > 0 then amount*cast(ab.mtnrevenueshare as int)/100 else 0 end) mtn_revenue_prv,
sum( rec_count ) charged_subscriptions,
sum( case when amount = 0 then 1 else 0 end ) non_charged_subscriptions
from ${Schema2}.daas_daily_usage_by_msisdn aa
join ${Schema2}.daas_enterprise_catalog_cr_202007
ab on split_part(aa.network_type,'|',1) = ab.productcd
where ( event_type in ('DIGITAL SERVICE') or product_type in ('HWSDP') )
and split_part(aa.network_type,'|',1) NOT IN ('23401220000021387','23401220000002022')
and split_part(aa.network_type,'|',5) not in ('2340110008059')
and upper(ab.classification) in ( 'RMS')
and upper(ab.division) in ('MARKETING')
and date_key between ab.start_date and ab.end_date
and date_key between ${DATE} and ${DATE}
group by date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) , split_part(aa.network_type,'|',2) ,
ab.servicename, ab.partnername, ab.classification, upper(category) , division ,
sub_category,dndcategoryname )
group by date_key, msisdn_key
union all
select
date_key, msisdn_key ,SUM(amount ) prv_rev , 0 partner_revenue_prv, 0 mtn_revenue_prv, 2
from
${Schema2}.daas_daily_usage_by_msisdn aa
where date_key between ${DATE} and ${DATE}
and (
upper(network_type) like '%CRBT%FEAT%' or
upper(event_type) in ( 'TUNE DOWNLOAD','BOX DOWNLOAD' )
)
group by date_key,msisdn_key
union all
select date_key, msisdn_key , sum(amount) REV_PRV, sum(partner_revenue_prv) partner_revenue_prv,
sum(mtn_revenue_prv) mtn_revenue_prv, 3
from (
select date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) product_id,
split_part(aa.network_type,'|',2) product_name,
ab.servicename, ab.partnername, ab.classification, upper(category) category, division division_name ,
sub_category,dndcategoryname , '0' service_category,'0' service_sub_category,
sum( amount) amount,
sum(case when try_cast(ab.partnerrevenueshare as int) > 0 then amount*cast(ab.partnerrevenueshare as int)/100 else 0 end) partner_revenue_prv,
sum(case when try_cast(ab.mtnrevenueshare as int) > 0 then amount*cast(ab.mtnrevenueshare as int)/100 else 0 end) mtn_revenue_prv,
sum( rec_count ) rec_count
from ${Schema2}.daas_daily_usage_by_msisdn aa
join ${Schema2}.daas_enterprise_catalog_cr_202007
ab on split_part(aa.network_type,'|',1) = ab.productcd
where
split_part(aa.network_type,'|',1) NOT IN ('23401220000021387','23401220000002022')
and split_part(aa.network_type,'|',5) not in ('2340110008059')
and upper(ab.classification) in ( 'CONTENT VAS')
and upper(ab.partnername) in ('HUAWEI_RBS' )
and upper(ab.division) in ('MARKETING')
and date_key between ab.start_date and ab.end_date
and date_key between ${DATE} and ${DATE}
group by date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) , split_part(aa.network_type,'|',2) ,
ab.servicename, ab.partnername, ab.classification, upper(category) , division ,
sub_category,dndcategoryname
) group by date_key,msisdn_key
union all 
select
tbl_dt , msisdn_key, sum(total_cost) prv_rev , 0 partner_revenue_prv,0 mtn_revenue_prv, 4
from ${Schema2}.daas_vas_short_code where tbl_dt between ${DATE} and ${DATE}
and ( upper(description) like '%LINK-UP%NAT%DEST%')
group by tbl_dt ,msisdn_key
union all
select date_key, msisdn_key, sum(amount) REV_PRV,
sum(partner_revenue_prv) partner_revenue_prv,
sum(mtn_revenue_prv) mtn_revenue_prv,5
from (
select date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) product_id,
split_part(aa.network_type,'|',2) product_name,
ab.servicename, ab.partnername, ab.classification, upper(category) category, division division_name ,
sub_category,dndcategoryname , '0' service_category,'0' service_sub_category ,
sum( amount) amount,
sum(case when try_cast(ab.partnerrevenueshare as int) > 0 then amount*cast(ab.partnerrevenueshare as double)/100 else 0 end) partner_revenue_prv,
sum(case when try_cast(ab.mtnrevenueshare as int) > 0 then amount*cast(ab.mtnrevenueshare as double)/100 else 0 end) mtn_revenue_prv,
sum( rec_count ) rec_count
from ${Schema2}.daas_daily_usage_by_msisdn aa
join ${Schema2}.daas_enterprise_catalog_cr_202007
ab on split_part(aa.network_type,'|',1) = ab.productcd
where
split_part(aa.network_type,'|',1) NOT IN ('23401220000021387','23401220000002022')
and split_part(aa.network_type,'|',5) not in ('2340110008059')
and upper(ab.classification) in ( 'CONTENT VAS')
and upper(partnername) in ('GEMALTO' )
and upper(division) in ('MARKETING')
and date_key between ab.start_date and ab.end_date
and date_key between ${DATE} and ${DATE}
group by date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) , split_part(aa.network_type,'|',2) ,
ab.servicename, ab.partnername, ab.classification, upper(category), division ,
sub_category,dndcategoryname )
group by date_key,msisdn_key
union all
select date_key, msisdn_key, sum(amount)*0.18 REV_PRV,
sum(partner_revenue_prv)*0.18 partner_revenue_prv, sum(mtn_revenue_prv)*0.18 mtn_revenue_prv, 6
from (
select date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) product_id,
split_part(aa.network_type,'|',2) productname,
ab.servicename, ab.partnername, ab.classification, upper(category) category, division division_name ,
sub_category,dndcategoryname , '0' service_category,'0' service_sub_category ,
sum( amount) amount,
sum(case when try_cast(ab.partnerrevenueshare as int) > 0 then amount*cast(ab.partnerrevenueshare as double)/100 else 0 end) partner_revenue_prv,
sum(case when try_cast(ab.mtnrevenueshare as int) > 0 then amount*cast(ab.mtnrevenueshare as double)/100 else 0 end) mtn_revenue_prv,
sum( rec_count ) rec_count
from ${Schema2}.daas_daily_usage_by_msisdn aa
join ${Schema2}.daas_enterprise_catalog_cr_202007
ab on split_part(aa.network_type,'|',1) = ab.productcd
where split_part(aa.network_type,'|',1) NOT IN ('23401220000021387','23401220000002022')
and split_part(aa.network_type,'|',5) not in ('2340110008059')
and upper(ab.classification) in ( 'CONTENT VAS')
and UPPER(SUB_CATEGORY) LIKE 'LOTT%'
and upper(division) in ('MARKETING')
and date_key between ab.start_date and ab.end_date
and date_key between ${DATE} and ${DATE}
group by date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) , split_part(aa.network_type,'|',2) ,
ab.servicename, ab.partnername, ab.classification, upper(category), division, sub_category,dndcategoryname
) group by date_key,msisdn_key 
union all
select tbl_dt,a_number ,
sum( event_amount) Revenue, 0 partner_revenue_prv,0 mtn_revenue_prv, 7
FROM ${Schema}.vp_voice_messaging
where tbl_dt between ${DATE} and ${DATE}
group by tbl_dt , a_number
union all
select date_key, msisdn_key, sum(amount) REV_PRV, sum(partner_revenue_prv) partner_revenue_prv,
sum(mtn_revenue_prv) mtn_revenue_prv, 8
from (
select date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) product_id,
split_part(aa.network_type,'|',2) productname,
ab.servicename, ab.partnername, ab.classification, upper(category) category, division division_name ,
sub_category,dndcategoryname , '0' service_category,'0' service_sub_category ,
sum( amount) amount,
sum(case when try_cast(ab.partnerrevenueshare as int) > 0 then amount*cast(ab.partnerrevenueshare as double)/100 else 0 end) partner_revenue_prv,
sum(case when try_cast(ab.mtnrevenueshare as int) > 0 then amount*cast(ab.mtnrevenueshare as double)/100 else 0 end) mtn_revenue_prv,
sum( rec_count ) rec_count
from ${Schema2}.daas_daily_usage_by_msisdn aa
join ${Schema2}.daas_enterprise_catalog_cr_202007
ab on split_part(aa.network_type,'|',1) = ab.productcd
where
split_part(aa.network_type,'|',1) NOT IN ('23401220000021387','23401220000002022')
and split_part(aa.network_type,'|',5) not in ('2340110008059')
and upper(ab.classification) in ( 'CONTENT VAS')
and UPPER(SUB_CATEGORY) NOT LIKE 'LOTT%'
and upper(ab.partnername) not in ('GEMALTO','MTN_CRBT','HUAWEI_RBS')
and upper(ab.division) in ('MARKETING')
and date_key between ${DATE} and ${DATE}
and date_key between ab.start_date and ab.end_date
group by date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) ,
split_part(aa.network_type,'|',2) ,
ab.servicename, ab.partnername, ab.classification, category, division ,
sub_category,dndcategoryname
) group by date_key,msisdn_key
union all
select
tbl_dt, msisdn_key, sum(total_cost) prv_rev , 0 partner_revenue_prv, 0 mtn_revenue_prv, 9
from ${Schema2}.daas_vas_short_code where tbl_dt between ${DATE} and ${DATE}
and upper(description) not like '%LINK-UP%'
and total_cost > 0.0
group by tbl_dt, msisdn_key
union all
select date_key, msisdn_key,
sum(amount) REV_PRV,sum(partner_revenue_prv) partner_revenue_prv,
sum(mtn_revenue_prv) mtn_revenue_prv, 10
from (
select date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) product_id,
split_part(aa.network_type,'|',2) product_name,
ab.servicename, ab.partnername, ab.classification,upper(category) category, division division_name ,
sub_category,dndcategoryname , '0' service_category,'0' service_sub_category ,
sum(CASE WHEN ab.productcd = '234012000006129' THEN amount*0.15
WHEN ab.productcd = '234012000021254' THEN amount*0.15 
ELSE amount END) amount,
sum(case
when try_cast(ab.partnerrevenueshare as int) > 0 and ab.productcd in ( '234012000006129','234012000021254' ) then 0.15*amount*cast(ab.partnerrevenueshare as double)/100
when try_cast(ab.partnerrevenueshare as int) > 0 and ab.productcd not in ( '234012000006129','234012000021254' ) then amount*cast(ab.partnerrevenueshare as double)/100
else 0 end) partner_revenue_prv,
sum(case
when try_cast(ab.mtnrevenueshare as int) > 0 and ab.productcd in ( '234012000006129','234012000021254' ) then 0.15*amount*cast(ab.mtnrevenueshare as double)/100
when try_cast(ab.mtnrevenueshare as int) > 0 and ab.productcd not in ( '234012000006129','234012000021254' ) then amount*cast(ab.mtnrevenueshare as double)/100
else 0 end) mtn_revenue_prv,
sum( rec_count ) rec_count
from ${Schema2}.daas_daily_usage_by_msisdn aa
join ${Schema2}.daas_enterprise_catalog_cr_202007
ab on split_part(aa.network_type,'|',1) = ab.productcd
where upper(ab.division) in ('EBU','EB')
and date_key between ab.start_date and ab.end_date
and date_key between ${DATE} and ${DATE}
group by date_dt,date_key, msisdn_key,event_type, product_type, split_part(aa.network_type,'|',1) , split_part(aa.network_type,'|',2) ,
ab.servicename, ab.partnername, ab.classification, upper(category), division ,
sub_category,dndcategoryname
) group by date_key,msisdn_key) "
 
## ER Room Data Records
## ====================
echo "Create ${Feeds[9]} view started.. "  
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute "create view ${Schema}.${Feeds[9]} as
select
subs.tbl_dt,0 as msisdn,activation_dt,site_id,tac_id,last_rge_dt,gross_connection_dt,churn_dt,flag
,recharge_count,recharge_revenue_VTU_OTHER,recharge_revenue_RECHARGES,recharge_revenue_BANK_ON_DEMAND
,recharge_count_VTU_OTHER,recharge_count_RECHARGES,recharge_count_BANK_ON_DEMAND, (h.rev_prv*0.925) vas_bundle_revenue,
voice_bundle_revenue,voice_bundle_onnet_revenue,voice_bundle_offnet_revenue,voice_bundle_international_revenue,
g.data_2g_download_gb, g.data_3g_download_gb, g.data_4g_download_gb, g.data_download_gb Data_total_GB,
data_bundle_revenue,data_2g_revenue,data_3g_revenue,data_4g_revenue,data_unknown_tech_revenue,data_revenue_cdr,data_revenue_total,sms_revenue,
c.voice_revenue_total,
voice_outgoing_revenue,voice_outgoing_onnet_revenue,voice_outgoing_offnet_revenue,voice_outgoing_international_revenue,voice_revenue,
voice_onnet_revenue,voice_offnet_revenue,voice_international_revenue,voice_outgoing_minutes,voice_outgoing_onnet_minutes,
voice_outgoing_offnet_minutes,voice_outgoing_international_minutes,voice_onnet_minutes,
voice_offnet_minutes,voice_international_minutes,sms_outgoing_count,sms_outgoing_revenue,to_hex(sha1(to_utf8(cast(subs.msisdn_key as varchar)))) hashed_msisdn_key,cast(subs.tbl_dt as int) as date_key
from ${Schema}.${Feeds[2]} subs
left join ${Schema}.${Feeds[3]} b
on subs.tbl_dt = b.date_key and subs.msisdn_key = b.msisdn_key
left join ${Schema}.${Feeds[4]} c
on subs.tbl_dt = c.tbl_dt and subs.msisdn_key = c.msisdn_key
left join ${Schema}.${Feeds[5]} d
on subs.tbl_dt = d.tbl_dt and subs.msisdn_key = d.msisdn_key
left join ${Schema}.${Feeds[6]} e
on subs.tbl_dt = e.tbl_dt and subs.msisdn_key = e.msisdn_key
left join ${Schema}.${Feeds[7]} f
on subs.tbl_dt = f.tbl_dt and subs.msisdn_key = f.msisdn_key
left join ${Schema}.${Feeds[8]} g
on subs.tbl_dt = g.tbl_dt and subs.msisdn_key = g.msisdn_key
left join ${Schema}.tbl_er_vas_bundle_rev h
on subs.tbl_dt = h.date_key and subs.msisdn_key = h.msisdn_key"


for i in ${FeedsPos[@]}
 do
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute "select count(*) from ${Schema}.${Feeds[$i]}")
  q11=$(echo $q1 |  sed "s/\"//g")
  if [ $q11 != 0 ]
   then
    echo "${Feeds[$i]} has $q11 count"
   else
    echo "${Feeds[$i]} didn't insert data, Will affect the insertion process! "
  fi
 done
