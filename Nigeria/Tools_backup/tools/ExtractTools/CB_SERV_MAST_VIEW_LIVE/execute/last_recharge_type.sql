start transaction;
delete from nigeria.last_recharge_type where tbl_dt=20190108;
insert into nigeria.last_recharge_type
select
msisdn_key,
max(rch_last_date_voucher) rch_last_date_voucher,
max(rch_last_amount_voucher) rch_last_amount_voucher,
max(rch_last_date_ondemand) rch_last_date_ondemand,
max(rch_last_amount_ondemand) rch_last_amount_ondemand,
tbl_dt
from
(
select
msisdn_key,
max(rch_last_date_voucher) rch_last_date_voucher,
max(rch_last_amount_voucher) rch_last_amount_voucher,
max(rch_last_date_ondemand) rch_last_date_ondemand,
max(rch_last_amount_ondemand) rch_last_amount_ondemand,
tbl_dt
from (
select
tbl_dt,
msisdn_key,
rank() over (partition by tbl_dt,msisdn_key,event_type order by original_timestamp_enrich desc) rnk,
case when event_type='Voucher' then original_timestamp_enrich end RCH_LAST_DATE_VOUCHER,
case when event_type='Voucher' then amount end RCH_LAST_AMOUNT_VOUCHER,
case when event_type='Demand' then original_timestamp_enrich end RCH_LAST_DATE_ONDEMAND,
case when event_type='Demand' then amount end RCH_LAST_AMOUNT_ONDEMAND
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
row_number() over (partition by tbl_dt,msisdn_key, event_type order by original_timestamp_enrich desc, origin_time_stamp desc,account_balance desc) rnk
from  (select
            msisdn_key,
			tbl_dt,
            CASE
            when (upper(originnodetype)      IN ('AIR','UGW')) then 'Voucher'
            when (upper(originnodetype)      = 'HWSDP')       then 'Demand'
            else
            'Others Payment'
            end event_type ,
            cast(transaction_amt as double) amount,
            original_timestamp_enrich,origin_time_stamp,cast(accountbalance as double) account_balance
FROM    flare_8.cs5_air_refill_ma r
where   r.tbl_dt=20190108
and transaction_amt>0 and originnodetype in ('HWSDP','AIR','UGW')
)
)
where rnk=1
)
) where rnk=1 group by tbl_dt,msisdn_key 
union all
select
msisdn_key,
rch_last_date_voucher,
rch_last_amount_voucher,
rch_last_date_ondemand,
rch_last_amount_ondemand,
20190108 as tbl_dt
from nigeria.last_recharge_type where tbl_dt= cast(date_format(date_add('day',-1,date_parse(cast(20190108 as varchar),'%Y%m%d')),'%Y%m%d') as int)
)
group by tbl_dt,msisdn_key;
commit;
