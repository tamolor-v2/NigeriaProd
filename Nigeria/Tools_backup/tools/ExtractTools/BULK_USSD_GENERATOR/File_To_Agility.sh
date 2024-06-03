#!bin/bash
##Generating_USSD_Files

day=$(date +"%Y%m%d")
hour=$(date +"%H%M%S")

/opt/presto/bin/presto --server 10.1.197.145:8999--catalog hive5 --schema flare_8 --execute "select LineofBusiness,chrononumber,Contracttype,Rating,CallDateTime,CallType,calledcallingnumber,Transmissiontype,Duration,Datavolume,ChargeAmount,Interactiontype,
TerminatingOperatorCountryCode,Service_Identifer,APNorCRBT_Name,ExtensionNumber,roamPCountryCode,ServiceClass,ProviderAccount,
case when length(dedicatedaccountid) <> 0 then  dedicatedaccountid||':'||'M'||':'||try_cast(dedicatedamountused as varchar) end DedicatedAccount,
UsageCounter,UsageCounter2,UsageCounter3
from(
select 1 LineofBusiness,1 Contracttype,1 Rating,try_cast(tbl_dt as varchar)||'000000' CallDateTime,'006' CallType,'*'||ussd_code||'*' calledcallingnumber,7 Transmissiontype,sum(try_cast(callduration as int)) Duration,sum(try_cast(datavolumeincoming1 as int)) Datavolume,
sum((try_cast(costofsession as double))) ChargeAmount,1 Interactiontype,'001' TerminatingOperatorCountryCode,servedaccount Service_Identifer,originatingservices APNorCRBT_Name,try_cast(null as int) ExtensionNumber,
234 roamPCountryCode,serviceclassid ServiceClass,try_cast(null as varchar) ProviderAccount,dedicatedaccountid,sum(try_cast(dedicatedamountused as double)) dedicatedamountused,try_cast(null as int) UsageCounter,
try_cast(null as varchar) UsageCounter2,try_cast(null as varchar) UsageCounter3
from flare_8.cs5_ccn_gprs_ma x
left join nigeria.ussd_msisdn_provider y
on (substr(trim(mobilenumber),-10) = substr(trim(provider_msisdn),-10))
where tbl_dt = try_cast(Date_format(date_add('DAY',-1,current_date), '%Y%m%d') as int)
and  (upper(chargingcontextid) LIKE '%SCAP%')
and  originatingservices = 'USSDGW'
and serviceclassid in ('228','229')
and try_cast(costofsession as double) > 0
group by tbl_dt,servedaccount,ussd_code,originatingservices,serviceclassid,dedicatedaccountid
) a
join
(
select chrononumber,mobilenumber
from(
select chrononumber,substr(trim(mobilenumber),-10) mobilenumber,
row_number() over (partition by mobilenumber order by chrononumber) as rnk
from flare_8.cs5_ccn_gprs_ma x
where tbl_dt = try_cast(Date_format(date_add('DAY',-1,current_date), '%Y%m%d') as int)
and  (upper(chargingcontextid) LIKE '%SCAP%')
and  originatingservices = 'USSDGW'
and serviceclassid in ('228','229')
and try_cast(costofsession as double) > 0
) where rnk = 1
) b on (a.Service_Identifer = b.mobilenumber) " --output-format CSV_HEADER | sed '1d' | sed 's/,/|/g' | awk '{gsub(/\"/,"")};1' > /mnt/beegfs_bsl/ctma/BULKUSSD_AGILITY/CBS_BulkUSSD_${day}_${hour}.txt
