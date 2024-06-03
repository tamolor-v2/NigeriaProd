start transaction;
delete from nigeria.last_recharge_types where tbl_dt=20190108;
insert into nigeria.last_recharge_types
select
msisdn_key,
max(rch_last_date_voucher) as rch_last_date_voucher,
cast(substr(max(rch_last_date_voucher||':'||cast(rch_last_amount_voucher as varchar)),16) as double) as rch_last_amount_voucher,
sum(rch_voucher_amt) as rch_voucher_amt,
cast(sum(rch_voucher_cnt) as int) as rch_voucher_cnt,
max(rch_last_date_ondemand) as rch_last_date_ondemand,
cast(substr(max(rch_last_date_ondemand||':'||cast(rch_last_amount_ondemand as varchar)),16) as double) as rch_last_amount_ondemand,
sum(rch_ondemand_amt) as rch_ondemand_amt,
cast(sum(rch_ondemand_cnt) as int) as rch_ondemand_cnt,
max(rch_last_date_vtu) as rch_last_date_vtu,
cast(substr(max(rch_last_date_vtu||':'||cast(rch_last_amount_vtu as varchar)),16) as double) as rch_last_amount_vtu,
sum(rch_vtu_amt) as rch_vtu_amt,
cast(sum(rch_vtu_cnt) as int) as rch_vtu_cnt,
max(rch_last_date_dya) as rch_last_date_dya,
cast(substr(max(rch_last_date_dya||':'||cast(rch_last_amount_dya as varchar)),16) as double) as rch_last_amount_dya,
sum(rch_dya_amt) as rch_dya_amt,
cast(sum(rch_dya_cnt) as int) as rch_dya_cnt,
max(rch_last_date_sdpadjust) as rch_last_date_sdpadjust,
cast(substr(max(rch_last_date_sdpadjust||':'||cast(rch_last_amount_sdpadjust as varchar)),16) as double) as rch_last_amount_sdpadjust,
sum(rch_sdpadjust_amt) as rch_sdpadjust_amt,
cast(sum(rch_sdpadjust_cnt) as int) as rch_sdpadjust_cnt,
max(rch_last_date_others) as rch_last_date_others,
cast(substr(max(rch_last_date_others||':'||cast(rch_last_amount_others as varchar)),16) as double) as rch_last_amount_others,
sum(rch_others_amt) as rch_others_amt,
cast(sum(rch_others_cnt) as int) as rch_others_cnt,
tbl_dt
from
(
---First union holding the last date and last amount
select
msisdn_key,
max(rch_last_date_voucher) as rch_last_date_voucher,
max(rch_last_amount_voucher) as rch_last_amount_voucher,
max(cast(0 as double)) as rch_voucher_amt,
max(cast(0 as int)) as rch_voucher_cnt,
max(rch_last_date_ondemand) as rch_last_date_ondemand,
max(rch_last_amount_ondemand) as rch_last_amount_ondemand,
max(cast(0 as double)) as rch_ondemand_amt,
max(cast(0 as int)) as rch_ondemand_cnt,
max(rch_last_date_vtu) as rch_last_date_vtu,
max(rch_last_amount_vtu) as rch_last_amount_vtu,
max(cast(0 as double)) as rch_vtu_amt,
max(cast(0 as int)) as rch_vtu_cnt,
max(rch_last_date_dya) as rch_last_date_dya,
max(rch_last_amount_dya) as rch_last_amount_dya,
max(cast(0 as double)) as rch_dya_amt,
max(cast(0 as int)) as rch_dya_cnt,
max(rch_last_date_sdpadjust) as rch_last_date_sdpadjust,
max(rch_last_amount_sdpadjust) as rch_last_amount_sdpadjust,
max(cast(0 as double)) as rch_sdpadjust_amt,
max(cast(0 as int)) as rch_sdpadjust_cnt,
max(rch_last_date_others) as rch_last_date_others,
max(rch_last_amount_others) as rch_last_amount_others,
max(cast(0 as double)) as rch_others_amt,
max(cast(0 as int)) as rch_others_cnt,
tbl_dt
from (
select
tbl_dt,
msisdn_key,
row_number() over (partition by tbl_dt,msisdn_key,event_type order by original_timestamp_enrich desc, amount desc) rnk,
case when event_type='Voucher' then original_timestamp_enrich end rch_last_date_voucher,
case when event_type='Voucher' then amount end rch_last_amount_voucher,
case when event_type='Demand' then original_timestamp_enrich end rch_last_date_ondemand,
case when event_type='Demand' then amount end rch_last_amount_ondemand,
case when event_type='VTU' then original_timestamp_enrich end rch_last_date_vtu,
case when event_type='VTU' then amount end rch_last_amount_vtu,
case when event_type='DYA' then original_timestamp_enrich end rch_last_date_dya,
case when event_type='DYA' then amount end rch_last_amount_dya,
case when event_type='SDP Adjust' then original_timestamp_enrich end rch_last_date_sdpadjust,
case when event_type='SDP Adjust' then amount end rch_last_amount_sdpadjust,
case when event_type='Others Payment' then original_timestamp_enrich end rch_last_date_others,
case when event_type='Others Payment' then amount end rch_last_amount_others
from
(
select tbl_dt,msisdn_key,event_type,original_timestamp_enrich,amount
from (
select
tbl_dt,
msisdn_key,
event_type,
original_timestamp_enrich,
amount,
row_number() over (partition by tbl_dt,msisdn_key, event_type order by original_timestamp_enrich desc, 
origin_time_stamp desc,account_balance desc) rnk
from  
(select
msisdn_key,
tbl_dt,
CASE
when (upper(originnodetype) IN ('AIR','UGW')) then 'Voucher'
when (upper(originnodetype) = 'HWSDP') then 'Demand'
when (upper(originnodetype) = 'EXT') then 'VTU'
when (upper(originnodetype) = 'ECW') then 'DYA'
when (upper(originnodetype) = 'SDP') then 'SDP ADJUST'            
else
'Others Payment'
end event_type ,
cast(transaction_amt as double) amount,
original_timestamp_enrich,origin_time_stamp,cast(accountbalance as double) account_balance
FROM flare_8.cs5_air_refill_ma r
where r.tbl_dt=20190108
and transaction_amt>0
)
)
where rnk=1
)
) where rnk=1
group by tbl_dt,msisdn_key
union all
--Second union holding yesterday last date and amount
--joining customersubject ensuring is in sdp to take recycled numbers out
select
a.msisdn_key,
a.rch_last_date_voucher,
a.rch_last_amount_voucher,
cast(0 as double) as rch_voucher_amt,
cast(0 as int) as rch_voucher_cnt,
a.rch_last_date_ondemand,
a.rch_last_amount_ondemand,
cast(0 as double) as rch_ondemand_amt,
cast(0 as int) as rch_ondemand_cnt,
a.rch_last_date_vtu,
a.rch_last_amount_vtu,
cast(0 as double) as rch_vtu_amt,
cast(0 as int) as rch_vtu_cnt,
a.rch_last_date_dya,
a.rch_last_amount_dya,
cast(0 as double) as rch_dya_amt,
cast(0 as int) as rch_dya_cnt,
a.rch_last_date_sdpadjust,
a.rch_last_amount_sdpadjust,
cast(0 as double) as rch_sdpadjust_amt,
cast(0 as int) as rch_sdpadjust_cnt,
a.rch_last_date_others,
a.rch_last_amount_others,
cast(0 as double) as rch_others_amt,
cast(0 as int) as rch_others_cnt,
20190108 as tbl_dt
from nigeria.last_recharge_types a join flare_8.customersubject b 
on (b.tbl_dt=20190108 and a.msisdn_key=b.msisdn_key and b.aggr='daily' and b.is_in_today_sdp and
a.tbl_dt=cast(date_format(date_add('day',-1,date_parse(cast(20190108 as varchar),'%Y%m%d')),'%Y%m%d') as int))
union all
--Third union holding the total amount and count for the current day
(select
msisdn_key,
max(cast('20000101235959' as varchar)) as rch_last_date_voucher,
max(cast(0 as double)) as rch_last_amount_voucher,
sum(case when event_type='Voucher' then amount else cast(0 as double) end) as rch_voucher_amt,
sum(case when event_type='Voucher' then cnt else cast(0 as int) end) as rch_voucher_cnt,
max(cast('20000101235959' as varchar)) as rch_last_date_ondemand,
max(cast(0 as double)) as rch_last_amount_ondemand,
sum(case when event_type='Demand' then amount else cast(0 as double) end) as rch_ondemand_amt,
sum(case when event_type='Demand' then cnt else cast(0 as int) end) as rch_ondemand_cnt,
max(cast('20000101235959' as varchar)) as rch_last_date_vtu,
max(cast(0 as double)) as rch_last_amount_vtu,
sum(case when event_type='VTU' then amount else cast(0 as double) end) as rch_vtu_amt,
sum(case when event_type='VTU' then cnt else cast(0 as int) end) as rch_vtu_cnt,
max(cast('20000101235959' as varchar)) as rch_last_date_dya,
max(cast(0 as double)) as rch_last_amount_dya,
sum(case when event_type='DYA' then amount else cast(0 as double) end) as rch_dya_amt,
sum(case when event_type='DYA' then cnt else cast(0 as int) end) as rch_dya_cnt,
max(cast('20000101235959' as varchar)) as rch_last_date_sdpadjust,
max(cast(0 as double)) as rch_last_amount_sdpadjust,
sum(case when event_type='SDP Adjust' then amount else cast(0 as double) end) as rch_sdpadjust_amt,
sum(case when event_type='SDP Adjust' then cnt else cast(0 as int) end) as rch_sdpadjust_cnt,
max(cast('20000101235959' as varchar)) as rch_last_date_others,
max(cast(0 as double)) as rch_last_amount_others,
sum(case when event_type='Others Payment' then amount else cast(0 as double) end) as rch_others_amt,
sum(case when event_type='Others Payment' then cnt else cast(0 as int) end) as rch_others_cnt,
tbl_dt
from
(
select
msisdn_key,
tbl_dt,
CASE
when (upper(originnodetype) IN ('AIR','UGW')) then 'Voucher'
when (upper(originnodetype) = 'HWSDP') then 'Demand'
when (upper(originnodetype) = 'EXT') then 'VTU'
when (upper(originnodetype) = 'ECW') then 'DYA'
when (upper(originnodetype) = 'SDP') then 'SDP ADJUST'            
else
'Others Payment'
end event_type ,
sum(cast(transaction_amt as double)) amount,
cast(count(*) as int) cnt
FROM flare_8.cs5_air_refill_ma r
where r.tbl_dt=20190108
and transaction_amt>0
group by tbl_dt,msisdn_key,
CASE
when (upper(originnodetype) IN ('AIR','UGW')) then 'Voucher'
when (upper(originnodetype) = 'HWSDP') then 'Demand'
when (upper(originnodetype) = 'EXT') then 'VTU'
when (upper(originnodetype) = 'ECW') then 'DYA'
when (upper(originnodetype) = 'SDP') then 'SDP ADJUST'            
else
'Others Payment'
end
)
group by tbl_dt,msisdn_key)
)
group by tbl_dt,msisdn_key;
commit;
