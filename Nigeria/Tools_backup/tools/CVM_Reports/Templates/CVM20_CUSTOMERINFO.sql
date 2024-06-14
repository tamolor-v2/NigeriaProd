start transaction;
Insert into CVM_DB.CVM20_CUSTOMERINFO
select 
sub.msisdn_key ,
sim.vlr_flag,
bio_data.first_name       ,  
bio_data.last_name        ,  
bio_data.middle_name      ,  
bio_data.gender           ,  
bio_data.mother_maiden_name ,  
bio_data.dob              ,   
bio_data.address          ,   
bio_data.city             ,  
bio_data.city_desc        ,  
bio_data.district         ,  
bio_data.district_desc    ,  
bio_data.country    country_bio      ,  
bio_data.country_desc     ,  
bio_data.occupation       ,  
bio_data.state_of_origin  ,  
bio_data.lga_of_origin    
,sub.tariff_type tariff_type
,sub.subscriber_type
,sub.number_of_sim
,sub.sim_type
,sub.lga_of_origin_desc
,sub.state_of_origin_desc
,sub.mnp_ind
,sub.activation_dt
,sub.churn_date
,sub.val_seg
,sub.dola 
,cs1.opco_business_type
,cs1.customer_type
,cs1.cons_type
,cs1.country
,cs1.opco_name
,cs1.ucid
,cs1.status
,cs1.b2b_type
,cs1.dnd_status dnd_flag
,cs1.muc
,cs1.muc_lat
,cs1.muc_lon
,cs1.muc_type
, bio_data.alternate_number
, uc.KEEP_MY_NUMBER_FLAG
,'' loyalty_points_balance
,'' loyalty_id
,'' loyalty_points_earned
,'' loyalty_points_redeemed 
,sub.YearID 
,sub.monthid
,sub.WeekID 
,sub.week_started 
,sub.week_ended
,cs1.tbl_dt
from CVM_DB.cvm20_subs_tmp sub
left join CVM_DB.cvm20_bio_data_tmp bio_data on sub.msisdn_key=bio_data.msisdn_key and sub.tbl_dt=bio_data.tbl_dt
left join CVM_DB.cvm20_customersubject_tmp cs1 on sub.msisdn_key=cs1.msisdn_key and sub.tbl_dt=cs1.tbl_dt
left join CVM_DB.cvm20_sim_tmp sim on sub.msisdn_key=sim.msisdn_key and sub.tbl_dt=sim.tbl_dt
left join CVM_DB.cvm20_usg_voi_usg_cluster_tmp uc on sub.msisdn_key=uc.msisdn_key and sub.tbl_dt=uc.tbl_dt
where sub.dola <= 91;
commit;
