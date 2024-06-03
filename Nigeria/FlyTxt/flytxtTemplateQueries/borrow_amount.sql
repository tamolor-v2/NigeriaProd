start transaction;
delete from nigeria.borrow_amount where tbl_dt=yyyymmdd;
insert into nigeria.borrow_amount
select 
msisdn_key,
xtratime_loan,xtratime_comm_rev,xtratime_loan-xtratime_comm_rev as xtratime_subscr_rev,
xtrabyte_loan,xtrabyte_comm_rev,xtrabyte_loan-xtrabyte_comm_rev as xtrabyte_subscr_rev,
tbl_dt
from
(select
tbl_dt,msisdn_key,
sum(case when transactiontype = 'Xtratime' then abs(cast(adjustmentamount as double)) else cast(0.0 as double) end) xtratime_loan,
sum(case when transactiontype = 'Xtratime' then abs(cast(adjustmentamount as double))*.15 else cast(0.0 as double) end) xtratime_comm_rev,
sum(case when transactiontype = 'Xtrabyte' then abs(cast(adjustmentamount as double)) else cast(0.0 as double) end) xtrabyte_loan,
sum(case when transactiontype = 'Xtrabyte' then abs(cast(adjustmentamount as double))* 3/23 else cast(0.0 as double) end) xtrabyte_comm_rev
from flare_8.cs5_sdp_acc_adj_ma
WHERE tbl_dt=yyyymmdd
AND originnodetype in ('ACS','USSD')
and adjustmentaction = '2'
and serviceclassid = '243'
and transactiontype in ('Xtratime','Xtrabyte')
group by tbl_dt,msisdn_key
);
commit;
