start transaction;
insert into flare_8.credit_score_final
select distinct
ahd.FIRST_NAME
,ahd.LAST_NAME 
,ahd.address 
,ahd.ACCOUNT_NUMBER 
,ahd.ACCOUNT_NAME 
,ahd.POS_MSISDN 
,ahd.currency 
,ahd.AGENT_PROFILE 
,ahd.ACTIVATED_AT 
,ahd.DATE_OF_BIRTH 
,ahd.gender
,round(ab.eod_bal,2) ACCOUNT_BALANCE
,round(av.avg_amt,2) AVERAGE_BALANCE
,round(com.commision,2) COMMISSION
,vou_vv.transferfromvouchercount VOUCHER_VOLUME
,round(vou_vv.transferfromvoucheramount) VOUCHER_VALUE
,vou_peer.voucher_peers
,vou_last.voucher_last
,vou_com.Commission_Amount commissioning_voucher
,cin_vv.cashin_volume
,cin_vv.cashin_value
,cin_com.commission_cash_in
,cin_peers.cashin_peers
,cin_last.cash_in_last
,cout_vv.cash_out_value
,cout_vv.cash_out_volume
,cout_com.commission_cash_out
,cout_peers.cash_out_peers
,cout_last.cash_out_last
,rev.revenue
,to_hex(md5(to_utf8(cast(coalesce(ahd.POS_MSISDN, 0) as varchar)))) hashed_msisdn
,ahd.tbl_dt
from (select
FIRST_NAME 
,LAST_NAME 
,address 
,ACCOUNT_NUMBER 
,ACCOUNT_NAME
,POS_MSISDN 
,currency 
,AGENT_PROFILE 
,ACTIVATED_AT 
,DATE_OF_BIRTH 
,gender
,msisdn_key
,tbl_dt
from (select
firstname FIRST_NAME
,surname LAST_NAME
,concat(addressline1, ' ',addressline2) address
,msisdn_key ACCOUNT_NUMBER
, concat(firstname,' ',surname) ACCOUNT_NAME
, msisdn_key POS_MSISDN
, 'Naira' currency
, profile AGENT_PROFILE
, activation_date ACTIVATED_AT
, dateofbirth DATE_OF_BIRTH
, gender
, tbl_dt
, row_number() OVER (PARTITION BY msisdn_key ORDER by concat(addressline1, ' ',addressline2) desc ) rnk
, msisdn_key
from
Flare_8.ewp_account_holders_dump e
where 
tbl_dt=yyyymmddRunDate
and profile not in ('Cash Voucher User Profile')
) 
where rnk = 1
) ahd
left join flare_8.cr_bal_eod ab 
on (ahd.msisdn_key=try_cast(ab.agent_msisdn as bigint))
left outer join 
(select agentmsisdn,round(avg(amount),2) avg_amt
from
(select
agentmsisdn
,row_number() OVER (PARTITION BY tbl_dt,agentmsisdn ORDER BY original_timestamp_enrich desc ) rnk
,original_timestamp_enrich
,instruct_hdr_type
,case 
when instruct_hdr_type in ('TRANSFER') then instruct_to_bal_tot_aft 
else instruct_from_bal_tot_aft 
end amount
,tbl_dt
from 
dataops_prod.financial_log_vp
where
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type not in ('COMMISSIONING') 
)
where rnk = 1
group by agentmsisdn
) av 
on (av.agentmsisdn = ahd.msisdn_key)
left outer join 
(select b.agentmsisdn,sum(coalesce(a.instruct_amount,0)) commision
from
(SELECT instruct_hdr_fid, instruct_amount, tbl_dt
from flare_8.financial_log
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type in ('COMMISSIONING')
and instruct_hdr_fid in (select instruct_hdr_fid
from flare_8.financial_log
where 
hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
)
) a,
(select agentmsisdn, instruct_hdr_fid, tbl_dt, instruct_hdr_type, amount
from dataops_prod.financial_log_vp
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type = 'PAYMENT'
and instruct_to_accnt_hldr_usr_name in ('airuser.sp','mtnlogicalpin.sp','cisuser.sp','airtelairtime.sp','gloairtime.sp',
 '9mobileairtime.sp','9mobilelogicalpin.sp','airtellogicalpin.sp','glologicalpin.sp'
 ,'9mobiledatabundle.sp','airteldatabundle.sp','glodatabundle.sp','AccessBank-External.sp', 
 'AccessBank.sp')
union all
select agentmsisdn, instruct_hdr_fid, tbl_dt, instruct_hdr_type, amount
from dataops_prod.financial_log_vp
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type in ('PAYMENT', 'PAYMENT_SEND')
and instruct_to_accnt_hldr_usr_prf in ('YDFS Service Provider Profile','Star Times Service Provider Profile', 
 'YELLOW PSB Billers Service Provider Profile', 'Yellow PSB Service Provider Profile',
 'XAAS External Originated Transaction Profile', 'Retail Agent Account Holder Profile WATU') 
and instruct_to_accnt_hldr_usr_name not in ('9mobiledatabundle.sp','9mobilelogicalpin.sp','airteldatabundle.sp','airtellogicalpin.sp',
 'glodatabundle.sp','glologicalpin.sp','mtnlogicalpin.sp')
union all 
select agentmsisdn, instruct_hdr_fid, tbl_dt, instruct_hdr_type, amount
from dataops_prod.financial_log_vp
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type in ('TRANSFER', 'CREATE_CASH_VOUCHER','REDEEM_CASH_VOUCHER','TRANSFER_TO_VOUCHER','TRANSFER_FROM_VOUCHER','TRANSFER_TO_VOUCHER')
) b
where b.instruct_hdr_fid = a.instruct_hdr_fid
group by b.agentmsisdn
) com 
on (com.agentmsisdn = ahd.msisdn_key)
left outer join 
(select count(*) transferfromvouchercount, sum(amount) transferfromvoucheramount, AgentMsisdn 
from dataops_prod.financial_log_vp
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type = 'TRANSFER_FROM_VOUCHER' 
group by AgentMsisdn
) vou_vv 
on (vou_vv.agentmsisdn = ahd.msisdn_key)
left outer join 
(with
Redeemed_Detail as 
(select 
try_cast(substring(split_part(instruct_hdr_user, ':', 2),1,7) as bigint) customer_id, 
try_cast(instruct_amount as bigint) amount,
tbl_dt redeemed_date, 
try_cast(instruct_to_accnt_hldr_msisdn as bigint) redeeming_agent, 
instruct_hdr_fid
FROM flare_8.financial_log
WHERE 
instruct_hdr_type in ('TRANSFER_FROM_VOUCHER')
and hdr_type = 'RESERVATION' 
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
),
CUSTOMER_DETAIL as 
(select distinct(id) id, msisdn, concat(firstname, ' ', surname) names 
from flare_8.ewp_account_holders_dump 
where tbl_dt = yyyymmddRunDate
)
select count (try_cast(b.msisdn as bigint)) VOUCHER_PEERS, 'Bulk Redemption' Transaction_Type, try_cast(a.redeeming_agent as bigint) agentMSISDN
from Redeemed_Detail a left join CUSTOMER_DETAIL b on try_cast(a.customer_id as bigint)=b.id 
group by 2,3
) vou_peer 
on (try_cast(vou_peer.agentmsisdn as bigint) = ahd.msisdn_key)
left outer join 
(select max (original_timestamp_enrich) VOUCHER_LAST, instruct_to_accnt_hldr_msisdn AgentMsisdn 
from flare_8.financial_log 
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate 
and instruct_hdr_type = 'TRANSFER_FROM_VOUCHER'
and status_code='EXECUTED' 
and hdr_type = 'RESERVATION'
group by instruct_to_accnt_hldr_msisdn) vou_last 
on (try_cast(vou_last.agentmsisdn as bigint) = ahd.msisdn_key)
left outer join
(with one as
(SELECT instruct_hdr_fid, instruct_amount, tbl_dt 
from flare_8.financial_log
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate 
and instruct_hdr_type in ('COMMISSIONING')
and instruct_hdr_fid in (select instruct_hdr_fid
from flare_8.financial_log
where 
hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate)
),
two as 
(SELECT 'Transfer From Voucher' Transaction_Type, instruct_hdr_fid, try_cast(instruct_to_accnt_hldr_msisdn as bigint) Agent, instruct_to_accnt_hldr_usr_prf Profile
from flare_8.financial_log
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate 
and status_code='EXECUTED'
and hdr_type = 'RESERVATION'
AND instruct_hdr_type = 'TRANSFER_FROM_VOUCHER'
and msisdn_key =0
)
select b.agent, sum(a.instruct_amount)Commission_Amount
from one a left join two b on b.instruct_hdr_fid=a.instruct_hdr_fid
where b.instruct_hdr_fid is not null and b.agent is not null
group by 1
)vou_com 
on (vou_com.agent = ahd.msisdn_key)
left outer join
(with AssistedWithdrawalcount as 
(
select count(*)assistedwithdrawalcount, instruct_to_accnt_hldr_msisdn from flare_8.financial_log where instruct_from_accnt_hldr_usr_name = 'flutter.sp' 
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate and status_code='EXECUTED' and hdr_type = 'RESERVATION' and instruct_hdr_type not in ('CUSTOM_COMMISSIONTRANSFER','COMMISSIONING', 'DEPOSIT') and 
instruct_hdr_fid in (select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate ) group by instruct_to_accnt_hldr_msisdn
),
AssistedWithdrawalvalue as 
(
select sum(instruct_amount)assistedwithdrawalamount, instruct_to_accnt_hldr_msisdn from flare_8.financial_log where instruct_from_accnt_hldr_usr_name = 'flutter.sp' 
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate and status_code='EXECUTED' and hdr_type = 'RESERVATION' and instruct_hdr_type not in ('CUSTOM_COMMISSIONTRANSFER','COMMISSIONING', 'DEPOSIT') and 
instruct_hdr_fid in (select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate ) group by instruct_to_accnt_hldr_msisdn)
select a.instruct_to_accnt_hldr_msisdn agentmsisdn ,a.assistedwithdrawalcount as CASHIN_volume, b.assistedwithdrawalamount as CASHIN_VALUE
from AssistedWithdrawalcount a left join AssistedWithdrawalvalue b on (a.instruct_to_accnt_hldr_msisdn = b.instruct_to_accnt_hldr_msisdn)
) cin_vv
on (try_cast(cin_vv.agentmsisdn as bigint)=ahd.msisdn_key)
left outer join 
(with one as
(SELECT instruct_hdr_fid, instruct_amount, tbl_dt 
from flare_8.financial_log
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type in ('COMMISSIONING')
and instruct_hdr_fid in (select instruct_hdr_fid
from flare_8.financial_log
where 
hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
)
),
two as (select 'Cash_in' Transaction_Type, instruct_hdr_fid, try_cast(instruct_to_accnt_hldr_msisdn as bigint) Agent
from flare_8.financial_log
where 
instruct_from_accnt_hldr_usr_name = 'flutter.sp' 
and instruct_hdr_type not in ('CUSTOM_COMMISSIONTRANSFER','COMMISSIONING', 'DEPOSIT')
and hdr_type = 'RESERVATION' 
and instruct_to_message is not null
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
union all
select 'Cash_out' Transaction_Type, instruct_hdr_fid, try_cast(msisdn_key as bigint) agentMSISDN
from flare_8.financial_log 
where 
instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_name in ('AccessBank-External.sp', 'AccessBank.sp' )
and hdr_type = 'RESERVATION' 
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
)
select b.agent agentmsisdn, b.Transaction_type, sum(a.instruct_amount)commission_cash_in
from one a left join two b on b.instruct_hdr_fid=a.instruct_hdr_fid
where 
b.instruct_hdr_fid is not null
and b.Transaction_type='Cash_in'
group by b.agent, b.Transaction_type
) cin_com
on (ahd.msisdn_key = cin_com.agentmsisdn)
left outer join 
(select count (distinct (try_cast(substr(split_part(instruct_to_message, ':', 2),1,13) as bigint))) cashin_peers, 
try_cast(instruct_to_accnt_hldr_msisdn as bigint) agentMSISDN
from flare_8.financial_log
where instruct_from_accnt_hldr_usr_name = 'flutter.sp' and instruct_hdr_type not in ('CUSTOM_COMMISSIONTRANSFER','COMMISSIONING', 'DEPOSIT')
and hdr_type = 'RESERVATION'
and instruct_to_message is not null
and status_code='EXECUTED' 
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in (select instruct_hdr_fid
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate )
group by try_cast(instruct_to_accnt_hldr_msisdn as bigint)
) cin_peers 
on (cin_peers.agentmsisdn = ahd.msisdn_key)
left outer join 
(select max (original_timestamp_enrich) CASH_in_LAST, try_cast(instruct_to_accnt_hldr_msisdn as bigint) agentMSISDN
from flare_8.financial_log
where instruct_from_accnt_hldr_usr_name = 'flutter.sp' and instruct_hdr_type not in ('CUSTOM_COMMISSIONTRANSFER','COMMISSIONING', 'DEPOSIT')
and hdr_type = 'RESERVATION'
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
group by try_cast(instruct_to_accnt_hldr_msisdn as bigint)
) cin_last on (cin_last.agentMSISDN = ahd.msisdn_key)
left outer join 
(with AssistedDepositcount as 
(
select count(*)assisteddepositcount, msisdn_key from flare_8.financial_log where instruct_hdr_type = 'PAYMENT' and instruct_to_accnt_hldr_usr_name in ('AccessBank-External.sp')
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate and status_code='EXECUTED' and hdr_type = 'RESERVATION' and 
instruct_hdr_fid in (select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate ) group by msisdn_key
),
AssistedDepositvalue as 
(
select sum(instruct_amount)assisteddepositamount, msisdn_key from flare_8.financial_log where instruct_hdr_type = 'PAYMENT' and instruct_to_accnt_hldr_usr_name in ('AccessBank-External.sp')
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate and status_code='EXECUTED' and hdr_type = 'RESERVATION' and 
instruct_hdr_fid in (select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate ) group by msisdn_key
)
select a.msisdn_key agentmsisdn ,a.assisteddepositcount as CASH_out_volume, b.assisteddepositamount as cash_out_value
from AssistedDepositcount a left join AssistedDepositvalue b on (a.msisdn_key = b.msisdn_key)
) cout_vv 
on (cout_vv.agentmsisdn= ahd.msisdn_key)
left outer join 
(with one as
(SELECT instruct_hdr_fid, instruct_amount, tbl_dt 
from flare_8.financial_log
where 
tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
and instruct_hdr_type in ('COMMISSIONING')
and instruct_hdr_fid in (select instruct_hdr_fid
from flare_8.financial_log
where 
hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
)
),
two as (select 'Cash_in' Transaction_Type, instruct_hdr_fid, try_cast(instruct_to_accnt_hldr_msisdn as bigint) Agent
from flare_8.financial_log
where 
instruct_from_accnt_hldr_usr_name = 'flutter.sp' 
and instruct_hdr_type not in ('CUSTOM_COMMISSIONTRANSFER','COMMISSIONING', 'DEPOSIT')
and hdr_type = 'RESERVATION' 
and instruct_to_message is not null
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
union all
select 'Cash_out' Transaction_Type, instruct_hdr_fid, try_cast(msisdn_key as bigint) agentMSISDN
from flare_8.financial_log 
where 
instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_name in ('AccessBank-External.sp', 'AccessBank.sp' )
and hdr_type = 'RESERVATION' 
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
)
select b.agent agentmsisdn, b.Transaction_type, sum(a.instruct_amount)commission_cash_out
from one a left join two b on b.instruct_hdr_fid=a.instruct_hdr_fid
where 
b.instruct_hdr_fid is not null
and b.Transaction_type='Cash_out'
group by b.agent, b.Transaction_type
) cout_com
on (ahd.msisdn_key = cout_com.agentmsisdn)
left outer join
(select count (distinct(try_cast(substr(split_part(instruct_to_message, ':', 2),1,13) as bigint))) cash_out_peers, msisdn_key agentMSISDN
from flare_8.financial_log
where instruct_to_accnt_hldr_usr_name in ('AccessBank-External.sp') and instruct_hdr_type = 'PAYMENT' and hdr_type = 'RESERVATION'
and instruct_to_message is not null
and status_code='EXECUTED' 
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and
instruct_hdr_fid in (select instruct_hdr_fid
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate )
group by msisdn_key
) cout_peers 
on (cout_peers.agentmsisdn = ahd.msisdn_key)
left outer join
(select max (original_timestamp_enrich) CASH_out_LAST, msisdn_key agentMSISDN
from flare_8.financial_log
where 
instruct_to_accnt_hldr_usr_name in ('AccessBank-External.sp') and instruct_hdr_type = 'PAYMENT'
and instruct_to_message is not null
and hdr_type = 'RESERVATION'
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth and yyyymmddRunDate
group by msisdn_key
) cout_last
on (cout_last.agentMSISDN=ahd.msisdn_key)
left outer join (select msisdn, round (sum(rev_amount_from + rev_amount),2) as Revenue
from (
SELECT 
(case 
when instruct_to_accnt_hldr_usr_name = 'airuser.sp' then 'MTN AIRTIME'
when instruct_to_accnt_hldr_usr_name = 'cisuser.sp' then 'MTN DATA'
end) as Transaction_Type, 
instruct_hdr_fid rev_count, (instruct_amount * 0.04) rev_amount_from, 0 rev_amount, instruct_amount amount, msisdn_key as msisdn, tbl_dt
from flare_8.financial_log
where tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and status_code='EXECUTED'
and instruct_hdr_type = 'PAYMENT'
and hdr_type = 'RESERVATION'
AND instruct_to_accnt_hldr_usr_prf in ('MTN Airtime Service Provider Account Holder', 'YDFS Data Bundle Service Provider Account New')
and instruct_to_accnt_hldr_usr_name in ('airuser.sp', 'cisuser.sp')
and msisdn_key <>0
and instruct_hdr_fid in (select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all
select 'Telco Airtime' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, msisdn_key msisdn, tbl_dt
from flare_8.financial_log 
where instruct_to_ifee <>0
and hdr_type = 'RESERVATION'
and status_code='EXECUTED'
and instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_name in ('airtelairtime.sp','gloairtime.sp','9mobileairtime.sp','9mobilelogicalpin.sp','airtellogicalpin.sp','glologicalpin.sp', 'mtnlogicalpin.sp', 'mtnairtime.sp')
and msisdn_key <>0
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in (select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all
select 'Telco Data' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, msisdn_key msisdn, tbl_dt
from flare_8.financial_log 
where instruct_to_ifee <>0
and hdr_type = 'RESERVATION'
and status_code='EXECUTED'
and instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_name in ('9mobiledatabundle.sp','airteldatabundle.sp','glodatabundle.sp', 'mtndatabundle.sp')
and msisdn_key <>0
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all
select 'Bill Payment' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, msisdn_key msisdn, tbl_dt
from flare_8.financial_log 
where hdr_type = 'RESERVATION'
and status_code='EXECUTED'
and instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_prf in ('YDFS Service Provider Profile','Star Times Service Provider Profile')
and instruct_to_accnt_hldr_usr_name not in ('9mobiledatabundle.sp','9mobilelogicalpin.sp','airteldatabundle.sp','airtellogicalpin.sp','glodatabundle.sp','glologicalpin.sp','mtnlogicalpin.sp', 'betway.sp', 'mtndatabundle.sp', 'mtnairtime.sp')
and msisdn_key <>0
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all
select 'Betway' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, msisdn_key msisdn, tbl_dt
from flare_8.financial_log 
where hdr_type = 'RESERVATION'
and status_code='EXECUTED'
and instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_name = 'betway.sp'
and msisdn_key <>0
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all 
select 'MERCHANT_WATU' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, msisdn_key msisdn, tbl_dt
from flare_8.financial_log 
where instruct_hdr_type = 'PAYMENT_SEND' 
and instruct_to_accnt_hldr_usr_prf = 'Retail Agent Account Holder Profile WATU'
and hdr_type = 'RESERVATION' 
and instruct_from_ifee <>0
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all 
select 'Assisted Deposit' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, msisdn_key msisdn, tbl_dt
from flare_8.financial_log 
where instruct_hdr_type = 'PAYMENT' 
and instruct_to_accnt_hldr_usr_prf = 'YDFS Banks Service Provider Profile'
and hdr_type = 'RESERVATION' 
and instruct_from_ifee <>0
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all 
select 'Assisted Withdrawal' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, try_cast(instruct_to_accnt_hldr_msisdn as bigint) msisdn, tbl_dt
from flare_8.financial_log 
where instruct_from_accnt_hldr_usr_name = 'flutter.sp' and instruct_hdr_type not in ('CUSTOM_COMMISSIONTRANSFER','COMMISSIONING', 'DEPOSIT')
and hdr_type = 'RESERVATION' 
and instruct_to_ifee <>0
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all 
select 'Create Cash Voucher' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, msisdn_key msisdn, tbl_dt
FROM flare_8.financial_log
WHERE instruct_hdr_type = 'CREATE_CASH_VOUCHER'
and hdr_type = 'RESERVATION'
and instruct_from_ifee <>0
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all 
select 'Redeem Cash Voucher' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, try_cast(instruct_to_accnt_hldr_msisdn as bigint) msisdn, tbl_dt
FROM flare_8.financial_log
WHERE instruct_hdr_type = 'REDEEM_CASH_VOUCHER'
and hdr_type = 'RESERVATION'
and instruct_to_ifee <>0
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all 
select 'Transfer From Voucher' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, try_cast(instruct_to_accnt_hldr_msisdn as bigint) msisdn, tbl_dt
FROM flare_8.financial_log
WHERE instruct_hdr_type = 'TRANSFER_FROM_VOUCHER'
and hdr_type = 'RESERVATION' 
and instruct_to_ifee <>0
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
union all
select 'Transfer To Voucher' Transaction_Type, instruct_hdr_fid rev_count, instruct_from_ifee rev_amount_from, instruct_to_ifee rev_amount, instruct_amount amount, try_cast(instruct_from_accnt_hldr_msisdn as bigint) msisdn, tbl_dt
FROM flare_8.financial_log
WHERE instruct_hdr_type = 'TRANSFER_TO_VOUCHER'
and hdr_type = 'RESERVATION' 
and instruct_from_ifee <>0
and status_code='EXECUTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate
and instruct_hdr_fid in 
(select instruct_hdr_fid 
from flare_8.financial_log
where hdr_type = 'TXN'
and status_code = 'COMMITTED'
and tbl_dt between yyyyRunDateMonth 
and yyyymmddRunDate)
)
group by 1) rev
on (ahd.msisdn_key=rev.msisdn);
commit;
