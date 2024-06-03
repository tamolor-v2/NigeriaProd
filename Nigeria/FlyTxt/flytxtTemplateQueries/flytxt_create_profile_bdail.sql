start transaction;
delete from nigeria.cm_profile_bdail_sit_f where part_key=99;
insert into nigeria.cm_profile_bdail_sit_f
select msisdn_key,DOU_DATA,DOU_SMS,DOU_VOICE,Total_Refill_Count,Data_Balance,Data_Remaining_Pack_Balance,
Last_Sms_Recv_DT,Last_Sms_Sent_DT,LastDataUsageDT,LastVoiceUsageDT,MoMo_Closing_Balance,MoMo_Inactivity_Days,
MoMo_RGS_Status,MoMo_Total_Txns,MoMo_total_txns_fees,momoLastTxDate,Average_Refill_Amount,Max_Refill_Amount,
profile_Update_Date,99
from flare_8.vp_cm_profile_bdail
where tbl_dt=yyyymmdd and aggr='daily' and dola between 0 and 179
and length(try_cast(msisdn_key as varchar)) in (11,13) and (account_type='PREPAID' or account_type='POSTPAID');
commit;
