start transaction;
INSERT into cvm_db.cvm20_rec_engine_dev_prod
(
msisdn_key 
,vlr_flag
,first_name 
,last_name
,middle_name
,gender
,mother_maiden_name
,dob
,address 
,city 
,city_desc
,district
,district_desc 
,country_bio
,country_desc
,occupation 
,state_of_origin
,lga_of_origin 
,tariff_type
,subscriber_type
,number_of_sim 
,sim_type
,lga_of_origin_desc
,state_of_origin_desc
,mnp_ind 
,activation_dt 
,churn_date 
,val_seg 
,opco_business_type
,customer_type 
,cons_type
,country 
,opco_name
,ucid 
,status
,b2b_type
,dnd_flag
,muc
,muc_lat 
,muc_lon 
,muc_type
,alternate_number 
,keep_my_number_flag 
,loyalty_points_balance 
,loyalty_id 
,loyalty_points_earned
,loyalty_points_redeemed
,voi_offnet_out_c_count 
,voi_offnet_out_nc_count
,voi_onnet_out_c_count
,voi_onnet_out_nc_count 
,voi_int_out_c_count 
,voi_int_out_nc_count
,voi_offnet_out_b_count 
,voi_offnet_out_nb_count
,voi_onnet_out_b_count
,voi_onnet_out_nb_count 
,voi_int_out_b_count 
,voi_int_out_nb_count
,voi_offnet_out_free_count 
,voi_onnet_out_free_count
,voi_int_out_free_count 
,voi_offnet_out_c_sec
,voi_offnet_out_nc_sec
,voi_onnet_out_c_sec 
,voi_onnet_out_nc_sec
,voi_int_out_c_sec
,voi_int_out_nc_sec
,voi_offnet_out_b_sec
,voi_offnet_out_nb_sec
,voi_onnet_out_b_sec 
,voi_onnet_out_nb_sec
,voi_int_out_b_sec
,voi_int_out_nb_sec
,voi_offnet_out_free_sec
,voi_onnet_out_free_sec 
,voi_int_out_free_sec
,voi_roam_out_free_sec
,voi_roam_in_free_sec
,voi_roam_out_sec 
,voi_roam_out_free_count
,voi_roam_in_free_count 
,voi_roam_out_count
,voi_fix_out_free_count 
,voi_fix_out_free_sec
,voi_fix_out_count
,voi_fix_out_sec
,voi_fix_in_count 
,voi_fix_in_sec
,voi_cc_out_count 
,voi_cc_out_sec
,voi_cc_in_count
,voi_cc_in_sec 
,sn_voi_total_mtc 
,sn_voi_total_moc 
,voi_offnet_in_count 
,voi_onnet_in_count
,voi_roam_in_count
,voi_int_in_count 
,voi_offnet_in_sec
,voi_onnet_in_sec 
,voi_roam_in_sec
,voi_int_in_sec
,voi_out_sec
,voi_out_count 
,voi_in_count
,voi_in_sec 
,voi_out_count_bundle
,voi_out_duration_bundle
,voi_out_count_payg
,voi_out_duration_payg
,fnf_call_tot_sec 
,fnf_call_count
,weekday_voi_count
,weekday_voi_sec
,weekend_voi_count
,weekend_voi_sec
,weekday_voi_onnet_count
,weekday_voi_onnet_sec
,weekend_voi_onnet_count
,weekend_voi_onnet_sec
,weekday_voi_offnet_count
,weekday_voi_offnet_sec 
,weekend_voi_offnet_count
,weekend_voi_offnet_sec 
,weekday_voi_intl_count 
,weekday_voi_intl_sec
,weekend_voi_intl_count 
,weekend_voi_intl_sec
,weekday_voi_roam_count 
,weekday_voi_roam_sec
,weekend_voi_roam_count 
,weekend_voi_roam_sec
,morning_voi_out_sec 
,afternoon_voi_out_sec
,evening_voi_out_sec 
,night_voi_out_sec
,peak_voi_out_sec 
,non_peak_voi_out_sec
,voi_volte_count
,voi_volte_sec 
,call_drop_rate
,call_success_rate
,sms_offnet_out_c_count 
,sms_offnet_out_nc_count
,sms_onnet_out_c_count
,sms_onnet_out_nc_count 
,sms_int_out_c_count 
,sms_int_out_nc_count
,sms_offnet_out_b_count 
,sms_offnet_out_nb_count
,sms_onnet_out_b_count
,sms_onnet_out_nb_count 
,sms_int_out_b_count 
,sms_int_out_nb_count
,sms_offnet_out_free_count 
,sms_onnet_out_free_count
,sms_int_out_free_count 
,sms_roam_out_count
,sms_roam_out_free_count
,sms_in_count
,sms_out_count 
,weekday_sms_out_count
,weekend_sms_out_count
,sms_out_count_bundle
,sms_out_count_payg
,data_dl_kb 
,data_up_kb 
,data_kb_2g 
,data_kb_3g 
,data_kb_4g 
,data_in_bundle_kb
,data_in_payg_kb
,data_free_kb
,data_roam_kb
,data_kb 
,data_session_cnt 
,data_session_cnt_2g 
,data_session_cnt_3g 
,data_session_cnt_4g 
,data_roam_session_cnt
,data_session_sec 
,data_session_sec_2g 
,data_session_sec_3g 
,data_session_sec_4g 
,data_roam_sec 
,weekday_kb 
,weekend_kb 
,weekday_session_cnt 
,weekend_session_cnt 
,morning_kb 
,afternoon_kb
,evening_kb 
,night_kb
,peak_hours_kb 
,non_peak_hours_kb
,data_kb_expired
,data_session_drop_rate 
,data_session_success_rate 
,total_resolved_count
,total_issue_registered_count 
,avg_service_rating
,rev_sms_onnet 
,rev_sms_offnet
,rev_sms_int
,rev_sms_roam
,rev_sms_other 
,rev_sms_total 
,rev_data_total
,rev_voi_onnet 
,rev_voi_offnet
,rev_voi_int
,rev_voi_roam
,rev_voi_fixed 
,rev_voi_out
,rev_voi_in 
,rev_voi_other 
,total_rev_voice_payg
,total_rev_data_payg 
,rev_voi_total 
,tot_rev 
,mon_rev 
,tue_rev 
,wed_rev 
,thu_rev 
,fri_rev 
,sat_rev 
,sun_rev 
,rev_vas 
,rev_digital_service1_ayoba
,rev_digital_service2_musictime
,rev_digital_services
,total_rev_voice_bundle 
,total_rev_data_bundle
,total_rev_combo_bundle 
,total_rev_sms_bundle
,total_rev_bundle 
,total_rev_vas_bundle
,total_rev_other_bundle 
,total_rev_sms_payg
,total_rev_other_payg
,total_rev_payg
,total_rev_other
,max_rev_vce_onnet
,max_rev_vce_offnet
,max_rev_vce_int
,max_rev_data
,active_arpu
,active_rev_vce_onnet
,active_rev_vce_offnet
,active_rev_vce_int
,active_rev_data
,last_date_vce_offnet
,last_date_vce_onnet 
,last_date_vce_int
,last_date_vce_roam
,last_date_sms_offnet
,last_date_sms_onnet 
,last_date_sms_int
,last_date_sms_roam
,last_data_date
,last_data_date_roam 
,ds_last_sms_in
,ds_last_sms_out
,ds_last_sms_rge
,ds_last_voi_in
,ds_last_voi_out
,ds_last_voi_rge
,ds_last_data_in
,ds_last_data_out 
,ds_last_data_rge 
,ds_last_airtime_transfer
,ds_last_airtime_receive
,ds_last_digital_crbt
,ds_last_digital_callerfeel
,ds_last_digital_phonebookbackup 
,ds_last_digital
,last_digital_rge_dt 
,last_rge_date 
,last_activity_date
,ds_digital_services_act
,ds_last_recharge_sbsc
,ds_last_recharge_sbsc_mymtn
,ds_last_recharge_sbsc_digital
,ds_ayoba_act
,ds_last_momo_rge 
,ds_momo_act
,ds_last_momo
,dola 
,act_days_voi_onnet_moc 
,act_days_voi_offnet_moc
,act_days_voi_intl_moc
,act_days_voi_roam_moc
,act_days_voi_all_moc
,act_days_voi_onnet_mtc 
,act_days_voi_offnet_mtc
,act_days_voi_intl_mtc
,act_days_voi_roam_mtc
,act_days_voi_all_mtc
,act_days_sms_onnet_moc 
,act_days_sms_offnet_moc
,act_days_sms_intl_moc
,act_days_sms_roam_moc
,act_days_sms_all_moc
,act_days_sms_all_mtc
,act_days_data 
,act_days_data_roam
,act_days_moc
,act_days_mtc
,act_days_rge
,act_days_roam 
,number_of_active_periods
,mon_bal 
,tue_bal 
,wed_bal 
,thu_bal 
,fri_bal 
,sat_bal 
,sun_bal 
,bal_avg_daily 
,bal_days_less_5
,bal_days_negative
,bal_times_less_5 
,bal_times_negative
,opening_balance
,closing_balance
,opening_momo_bal 
,closing_momo_bal 
,momo_act_date 
,momo_dep_cnt
,momo_dep_amt
,momo_wit_cnt
,momo_wit_amt
,momo_p2p_trx_cnt 
,momo_p2p_trx_amt 
,momo_type
,momo_total_trx_fees 
,momo_customer_prof
,momo_last_trx_type
,total_data_sessions_app1
,avg_data_per_session_app1 
,total_data_usage_app1
,total_data_sessions_app2
,avg_data_per_session_app2 
,total_data_usage_app2
,total_data_sessions_app3
,avg_data_per_session_app3 
,total_data_usage_app3
,total_data_sessions_app4
,avg_data_per_session_app4 
,total_data_usage_app4
,total_data_sessions_app5
,avg_data_per_session_app5 
,total_data_usage_app5
,imei 
,imsi 
,last_detection
,msc_gt
,last_configuration
,billing 
,tac
,brand
,model_name 
,device_category
,software_os_vendor
,software_os_name 
,software_os_version 
,ota
,data 
,weight
,screen_size
,screen_reso
,mms_receiver
,wap_support
,wapversion 
,internet
,hardware_gprs 
,hardware_edge 
,hardware_hsdpa
,hardware_hsupa
,camera
,camera_reso
,memorycard 
,gps
,video
,streaming_video
,radiofm 
,audio_amr
,audio_mp3
,bluetooth
,wlan 
,ptt
,syncmlds
,omadm
,nw_2g
,nw_3g
,nw_4g
,hardware_lte
,hardware_nfc
,sim_form_factor
,edge_ind
,gprs_ind
,display_width 
,display_height
,display_depth 
,display_touchscreen 
,lte_frequencies
,hardware_wifi 
,brand_name 
,mon_sbsc_amt
,tue_sbsc_amt
,wed_sbsc_amt
,thu_sbsc_amt
,fri_sbsc_amt
,sat_sbsc_amt
,sun_sbsc_amt
,mon_sbsc_cnt
,tue_sbsc_cnt
,wed_sbsc_cnt
,thu_sbsc_cnt
,fri_sbsc_cnt
,sat_sbsc_cnt
,sun_sbsc_cnt
,tot_sbsc_amt
,total_bundle_count
,sbsc_fb_amt
,sbsc_count_fb 
,sbsc_count_ussd
,sbsc_ussd_amt 
,sbsc_count_rs 
,sbsc_rs_amt
,sbsc_others_amt
,sbsc_count_others
,total_data_bundle_count
,total_combo_bundle_count
,sbsc_count_voucher
,sbsc_voucher_amt 
,total_sms_bundle_count 
,total_voice_bundle_count
,total_vas_bundle_count 
,max_rch_deno
,max_sbsc_deno 
,total_other_bundle_count
,avg_rchg_gap
,average_sbsc_gap 
,average_sbsc_gap_voice 
,average_sbsc_gap_data
,average_sbsc_gap_sms
,average_sbsc_gap_combo 
,average_sbsc_gap_vas
,sbsc_flag_mod 
,reg_flag_mod
,reg_flag_ayoba
,sbsc_flag_musictime 
,reg_flag_musictime
,sbsc_flag_ayoba
,sbsc_flag_mtnapp 
,reg_flag_mtnapp
,reg_flag_apps_n_services
,sbsc_flag_apps_n_services 
,vas_flag
,bundle_flag
,yearid
,monthid 
,weekid
,week_started
,week_ended
,tbl_dt
)
select
custinfo.msisdn_key
,custinfo.vlr_flag
,custinfo.first_name
,custinfo.last_name 
,custinfo.middle_name
,custinfo.gender 
,custinfo.mother_maiden_name 
,custinfo.dob 
,custinfo.address
,custinfo.city
,custinfo.city_desc 
,custinfo.district
,custinfo.district_desc
,custinfo.country_bio
,custinfo.country_desc 
,custinfo.occupation
,custinfo.state_of_origin 
,custinfo.lga_of_origin
,custinfo.tariff_type
,custinfo.subscriber_type 
,custinfo.number_of_sim
,custinfo.sim_type
,custinfo.lga_of_origin_desc 
,custinfo.state_of_origin_desc
,custinfo.mnp_ind
,custinfo.activation_dt
,custinfo.churn_date
,custinfo.val_seg
,custinfo.opco_business_type 
,custinfo.customer_type
,custinfo.cons_type 
,custinfo.country
,custinfo.opco_name 
,custinfo.ucid
,custinfo.status 
,custinfo.b2b_type
,custinfo.dnd_flag
,custinfo.muc 
,custinfo.muc_lat
,custinfo.muc_lon
,custinfo.muc_type
,custinfo.alternate_number
,custinfo.keep_my_number_flag
,custinfo.loyalty_points_balance
,custinfo.loyalty_id
,custinfo.loyalty_points_earned 
,loyalty_points_redeemed 
,usg_voi.voi_offnet_out_c_count
,usg_voi.voi_offnet_out_nc_count
,usg_voi.voi_onnet_out_c_count 
,usg_voi.voi_onnet_out_nc_count
,usg_voi.voi_int_out_c_count
,usg_voi.voi_int_out_nc_count
,usg_voi.voi_offnet_out_b_count
,usg_voi.voi_offnet_out_nb_count
,usg_voi.voi_onnet_out_b_count 
,usg_voi.voi_onnet_out_nb_count
,usg_voi.voi_int_out_b_count
,usg_voi.voi_int_out_nb_count
,usg_voi.voi_offnet_out_free_count
,usg_voi.voi_onnet_out_free_count 
,usg_voi.voi_int_out_free_count
,usg_voi.voi_offnet_out_c_sec
,usg_voi.voi_offnet_out_nc_sec 
,usg_voi.voi_onnet_out_c_sec
,usg_voi.voi_onnet_out_nc_sec
,usg_voi.voi_int_out_c_sec
,usg_voi.voi_int_out_nc_sec 
,usg_voi.voi_offnet_out_b_sec
,usg_voi.voi_offnet_out_nb_sec 
,usg_voi.voi_onnet_out_b_sec
,usg_voi.voi_onnet_out_nb_sec
,usg_voi.voi_int_out_b_sec
,usg_voi.voi_int_out_nb_sec 
,usg_voi.voi_offnet_out_free_sec
,usg_voi.voi_onnet_out_free_sec
,usg_voi.voi_int_out_free_sec
,usg_voi.voi_roam_out_free_sec 
,usg_voi.voi_roam_in_free_sec
,usg_voi.voi_roam_out_sec
,usg_voi.voi_roam_out_free_count
,usg_voi.voi_roam_in_free_count
,usg_voi.voi_roam_out_count 
,usg_voi.voi_fix_out_free_count
,usg_voi.voi_fix_out_free_sec
,usg_voi.voi_fix_out_count
,usg_voi.voi_fix_out_sec 
,usg_voi.voi_fix_in_count
,usg_voi.voi_fix_in_sec
,usg_voi.voi_cc_out_count
,usg_voi.voi_cc_out_sec
,usg_voi.voi_cc_in_count 
,usg_voi.voi_cc_in_sec
,usg_voi.sn_voi_total_mtc
,usg_voi.sn_voi_total_moc
,usg_voi.voi_offnet_in_count
,usg_voi.voi_onnet_in_count 
,usg_voi.voi_roam_in_count
,usg_voi.voi_int_in_count
,usg_voi.voi_offnet_in_sec
,usg_voi.voi_onnet_in_sec
,usg_voi.voi_roam_in_sec 
,usg_voi.voi_int_in_sec
,usg_voi.voi_out_sec
,usg_voi.voi_out_count
,usg_voi.voi_in_count 
,usg_voi.voi_in_sec
,usg_voi.voi_out_count_bundle
,usg_voi.voi_out_duration_bundle
,usg_voi.voi_out_count_payg 
,usg_voi.voi_out_duration_payg 
,usg_voi.fnf_call_tot_sec
,usg_voi.fnf_call_count
,usg_voi.weekday_voi_count
,usg_voi.weekday_voi_sec 
,usg_voi.weekend_voi_count
,usg_voi.weekend_voi_sec 
,usg_voi.weekday_voi_onnet_count
,usg_voi.weekday_voi_onnet_sec 
,usg_voi.weekend_voi_onnet_count
,usg_voi.weekend_voi_onnet_sec 
,usg_voi.weekday_voi_offnet_count 
,usg_voi.weekday_voi_offnet_sec
,usg_voi.weekend_voi_offnet_count 
,usg_voi.weekend_voi_offnet_sec
,usg_voi.weekday_voi_intl_count
,usg_voi.weekday_voi_intl_sec
,usg_voi.weekend_voi_intl_count
,usg_voi.weekend_voi_intl_sec
,usg_voi.weekday_voi_roam_count
,usg_voi.weekday_voi_roam_sec
,usg_voi.weekend_voi_roam_count
,usg_voi.weekend_voi_roam_sec
,usg_voi.morning_voi_out_sec
,usg_voi.afternoon_voi_out_sec 
,usg_voi.evening_voi_out_sec
,usg_voi.night_voi_out_sec
,usg_voi.peak_voi_out_sec
,usg_voi.non_peak_voi_out_sec
,usg_voi.voi_volte_count 
,usg_voi.voi_volte_sec
,usg_voi.call_drop_rate
,usg_voi.call_success_rate 
,usg_sms.sms_offnet_out_c_count
,usg_sms.sms_offnet_out_nc_count
,usg_sms.sms_onnet_out_c_count 
,usg_sms.sms_onnet_out_nc_count
,usg_sms.sms_int_out_c_count
,usg_sms.sms_int_out_nc_count
,usg_sms.sms_offnet_out_b_count
,usg_sms.sms_offnet_out_nb_count
,usg_sms.sms_onnet_out_b_count 
,usg_sms.sms_onnet_out_nb_count
,usg_sms.sms_int_out_b_count
,usg_sms.sms_int_out_nb_count
,usg_sms.sms_offnet_out_free_count
,usg_sms.sms_onnet_out_free_count 
,usg_sms.sms_int_out_free_count
,usg_sms.sms_roam_out_count 
,usg_sms.sms_roam_out_free_count
,usg_sms.sms_in_count 
,usg_sms.sms_out_count
,usg_sms.weekday_sms_out_count 
,usg_sms.weekend_sms_out_count 
,usg_sms.sms_out_count_bundle
,usg_sms.sms_out_count_payg
,usg_dat.data_dl_kb
,usg_dat.data_up_kb
,usg_dat.data_kb_2g
,usg_dat.data_kb_3g
,usg_dat.data_kb_4g
,usg_dat.data_in_bundle_kb 
,usg_dat.data_in_payg_kb
,usg_dat.data_free_kb
,usg_dat.data_roam_kb
,usg_dat.data_kb
,usg_dat.data_session_cnt
,usg_dat.data_session_cnt_2g
,usg_dat.data_session_cnt_3g
,usg_dat.data_session_cnt_4g
,usg_dat.data_roam_session_cnt
,usg_dat.data_session_sec
,usg_dat.data_session_sec_2g
,usg_dat.data_session_sec_3g
,usg_dat.data_session_sec_4g
,usg_dat.data_roam_sec
,usg_dat.weekday_kb
,usg_dat.weekend_kb
,usg_dat.weekday_session_cnt
,usg_dat.weekend_session_cnt
,usg_dat.morning_kb
,usg_dat.afternoon_kb
,usg_dat.evening_kb
,usg_dat.night_kb 
,usg_dat.peak_hours_kb
,usg_dat.non_peak_hours_kb 
,usg_dat.data_kb_expired
,usg_dat.data_session_drop_rate
,usg_dat.data_session_success_rate 
,cc.total_resolved_count 
,cc.total_issue_registered_count
,cc.avg_service_rating
,rev.rev_sms_onnet
,rev.rev_sms_offnet 
,rev.rev_sms_int 
,rev.rev_sms_roam
,rev.rev_sms_other
,rev.rev_sms_total
,rev.rev_data_total 
,rev.rev_voi_onnet
,rev.rev_voi_offnet 
,rev.rev_voi_int 
,rev.rev_voi_roam
,rev.rev_voi_fixed
,rev.rev_voi_out 
,rev.rev_voi_in
,rev.rev_voi_other
,rev.total_rev_voice_payg 
,rev.total_rev_data_payg
,rev.rev_voi_total
,rev.tot_rev
,rev.mon_rev
,rev.tue_rev
,rev.wed_rev
,rev.thu_rev
,rev.fri_rev
,rev.sat_rev
,rev.sun_rev
,rev.rev_vas
,rev.rev_digital_service1_ayoba 
,rev.rev_digital_service2_musictime
,rev.rev_digital_services 
,rev.total_rev_voice_bundle
,rev.total_rev_data_bundle
,rev.total_rev_combo_bundle
,rev.total_rev_sms_bundle 
,rev.total_rev_bundle
,rev.total_rev_vas_bundle 
,rev.total_rev_other_bundle
,rev.total_rev_sms_payg
,rev.total_rev_other_payg 
,rev.total_rev_payg 
,rev.total_rev_other
,rev.max_rev_vce_onnet 
,rev.max_rev_vce_offnet
,rev.max_rev_vce_int
,rev.max_rev_data
,rev.active_arpu 
,rev.active_rev_vce_onnet 
,rev.active_rev_vce_offnet
,rev.active_rev_vce_int
,rev.active_rev_data
,cvm2_act_in.last_date_vce_offnet 
,cvm2_act_in.last_date_vce_onnet
,cvm2_act_in.last_date_vce_int 
,cvm2_act_in.last_date_vce_roam
,cvm2_act_in.last_date_sms_offnet 
,cvm2_act_in.last_date_sms_onnet
,cvm2_act_in.last_date_sms_int 
,cvm2_act_in.last_date_sms_roam
,cvm2_act_in.last_data_date 
,cvm2_act_in.last_data_date_roam
,cvm2_act_in.ds_last_sms_in 
,cvm2_act_in.ds_last_sms_out
,cvm2_act_in.ds_last_sms_rge
,cvm2_act_in.ds_last_voi_in 
,cvm2_act_in.ds_last_voi_out
,cvm2_act_in.ds_last_voi_rge
,cvm2_act_in.ds_last_data_in
,cvm2_act_in.ds_last_data_out
,cvm2_act_in.ds_last_data_rge
,cvm2_act_in.ds_last_airtime_transfer
,cvm2_act_in.ds_last_airtime_receive 
,cvm2_act_in.ds_last_digital_crbt 
,cvm2_act_in.ds_last_digital_callerfeel 
,cvm2_act_in.ds_last_digital_phonebookbackup
,cvm2_act_in.ds_last_digital
,cvm2_act_in.last_digital_rge_dt
,cvm2_act_in.last_rge_date
,cvm2_act_in.last_activity_date
,cvm2_act_in.ds_digital_services_act 
,cvm2_act_in.ds_last_recharge_sbsc
,cvm2_act_in.ds_last_recharge_sbsc_mymtn
,cvm2_act_in.ds_last_recharge_sbsc_digital 
,cvm2_act_in.ds_ayoba_act
,cvm2_act_in.ds_last_momo_rge
,cvm2_act_in.ds_momo_act 
,cvm2_act_in.ds_last_momo
,cvm2_act_in.dola
,cvm2_act_in.act_days_voi_onnet_moc
,cvm2_act_in.act_days_voi_offnet_moc 
,cvm2_act_in.act_days_voi_intl_moc
,cvm2_act_in.act_days_voi_roam_moc
,cvm2_act_in.act_days_voi_all_moc 
,cvm2_act_in.act_days_voi_onnet_mtc
,cvm2_act_in.act_days_voi_offnet_mtc 
,cvm2_act_in.act_days_voi_intl_mtc
,cvm2_act_in.act_days_voi_roam_mtc
,cvm2_act_in.act_days_voi_all_mtc 
,cvm2_act_in.act_days_sms_onnet_moc
,cvm2_act_in.act_days_sms_offnet_moc 
,cvm2_act_in.act_days_sms_intl_moc
,cvm2_act_in.act_days_sms_roam_moc
,cvm2_act_in.act_days_sms_all_moc 
,cvm2_act_in.act_days_sms_all_mtc 
,cvm2_act_in.act_days_data
,cvm2_act_in.act_days_data_roam
,cvm2_act_in.act_days_moc
,cvm2_act_in.act_days_mtc
,cvm2_act_in.act_days_rge
,cvm2_act_in.act_days_roam
,cvm2_act_in.number_of_active_periods
,cvm2_act_in.mon_bal
,cvm2_act_in.tue_bal
,cvm2_act_in.wed_bal
,cvm2_act_in.thu_bal
,cvm2_act_in.fri_bal
,cvm2_act_in.sat_bal
,cvm2_act_in.sun_bal
,cvm2_act_in.bal_avg_daily
,cvm2_act_in.bal_days_less_5
,cvm2_act_in.bal_days_negative 
,cvm2_act_in.bal_times_less_5
,cvm2_act_in.bal_times_negative
,cvm2_act_in.opening_balance
,cvm2_act_in.closing_balance
,cvm2_act_in.opening_momo_bal
,cvm2_act_in.closing_momo_bal
,'' momo_act_date
,'' momo_dep_cnt
,'' momo_dep_amt
,'' momo_wit_cnt
,'' momo_wit_amt
,'' momo_p2p_trx_cnt
,'' momo_p2p_trx_amt
,'' momo_type
,'' momo_total_trx_fees
,'' momo_customer_prof
,'' momo_last_trx_type
,'' total_data_sessions_app1
,'' avg_data_per_session_app1
,'' total_data_usage_app1
,'' total_data_sessions_app2
,'' avg_data_per_session_app2
,'' total_data_usage_app2
,'' total_data_sessions_app3
,'' avg_data_per_session_app3
,'' total_data_usage_app3
,'' total_data_sessions_app4
,'' avg_data_per_session_app4
,'' total_data_usage_app4
,'' total_data_sessions_app5
,'' avg_data_per_session_app5
,'' total_data_usage_app5
,dev.imei
,dev.imsi
,dev.last_detection
,dev.msc_gt 
,dev.last_configuration 
,dev.billing
,dev.tac 
,dev.brand
,dev.model_name
,dev.device_category 
,dev.software_os_vendor 
,dev.software_os_name
,dev.software_os_version
,dev.ota 
,dev.data
,dev.weight 
,dev.screen_size
,dev.screen_reso
,dev.mms_receiver 
,dev.wap_support
,dev.wapversion
,dev.internet
,dev.hardware_gprs
,dev.hardware_edge
,dev.hardware_hsdpa
,dev.hardware_hsupa
,dev.camera 
,dev.camera_reso
,dev.memorycard
,dev.gps 
,dev.video
,dev.streaming_video 
,dev.radiofm
,dev.audio_amr 
,dev.audio_mp3 
,dev.bluetooth 
,dev.wlan
,dev.ptt 
,dev.syncmlds
,dev.omadm
,dev.nw_2g
,dev.nw_3g
,dev.nw_4g
,dev.hardware_lte 
,dev.hardware_nfc 
,dev.sim_form_factor 
,dev.edge_ind
,dev.gprs_ind
,dev.display_width
,dev.display_height
,dev.display_depth
,dev.display_touchscreen
,dev.lte_frequencies 
,dev.hardware_wifi
,dev.brand_name
,rbs.mon_sbsc_amt 
,rbs.tue_sbsc_amt 
,rbs.wed_sbsc_amt 
,rbs.thu_sbsc_amt 
,rbs.fri_sbsc_amt 
,rbs.sat_sbsc_amt 
,rbs.sun_sbsc_amt 
,rbs.mon_sbsc_cnt 
,rbs.tue_sbsc_cnt 
,rbs.wed_sbsc_cnt 
,rbs.thu_sbsc_cnt 
,rbs.fri_sbsc_cnt 
,rbs.sat_sbsc_cnt 
,rbs.sun_sbsc_cnt 
,rbs.tot_sbsc_amt 
,rbs.total_bundle_count 
,rbs.sbsc_fb_amt
,rbs.sbsc_count_fb
,rbs.sbsc_count_ussd 
,rbs.sbsc_ussd_amt
,rbs.sbsc_count_rs
,rbs.sbsc_rs_amt
,rbs.sbsc_others_amt 
,rbs.sbsc_count_others
,rbs.total_data_bundle_count
,rbs.total_combo_bundle_count 
,rbs.sbsc_count_voucher 
,rbs.sbsc_voucher_amt
,rbs.total_sms_bundle_count
,rbs.total_voice_bundle_count 
,rbs.total_vas_bundle_count
,rbs.max_rch_deno 
,rbs.max_sbsc_deno
,rbs.total_other_bundle_count 
,rbs.avg_rchg_gap 
,rbs.average_sbsc_gap
,rbs.average_sbsc_gap_voice
,rbs.average_sbsc_gap_data 
,rbs.average_sbsc_gap_sms
,rbs.average_sbsc_gap_combo
,rbs.average_sbsc_gap_vas
,rbs2.sbsc_flag_mod
,rbs2.reg_flag_mod
,rbs2.reg_flag_ayoba 
,rbs2.sbsc_flag_musictime
,rbs2.reg_flag_musictime
,rbs2.sbsc_flag_ayoba
,rbs2.sbsc_flag_mtnapp
,rbs2.reg_flag_mtnapp
,rbs2.reg_flag_apps_n_services
,rbs2.sbsc_flag_apps_n_services
,rbs2.vas_flag 
,rbs2.bundle_flag
,custinfo.yearid 
,custinfo.monthid
,custinfo.weekid 
,custinfo.week_started 
,custinfo.week_ended
,custinfo.tbl_dt 
from
CVM_DB.CVM20_CUSTOMERINFO custinfo
left join CVM_DB.CVM20_USG_DATA_SMS_VOI_PRD_TMP usg_voi on custinfo.tbl_dt = usg_voi.tbl_dt and custinfo.msisdn_key = usg_voi.msisdn_key
left join CVM_DB.CVM20_USG_SMS usg_sms on custinfo.tbl_dt = usg_sms.tbl_dt and custinfo.msisdn_key = usg_sms.msisdn_key
left join CVM_DB.CVM20_USG_DATA usg_dat on custinfo.tbl_dt = usg_dat.tbl_dt and custinfo.msisdn_key = usg_dat.msisdn_key
left join CVM_DB.CVM20_CC_TMP cc on custinfo.tbl_dt = cc.tbl_dt and custinfo.msisdn_key = cc.msisdn_key
left join CVM_DB.CVM20_REV_VOI_SMS_DATA_TMP rev on custinfo.tbl_dt = rev.tbl_dt and custinfo.msisdn_key = rev.msisdn_key
left join CVM_DB.CVM20_REC_ENGINE_DEV2 cvm2_act_in on custinfo.tbl_dt = cvm2_act_in.tbl_dt and custinfo.msisdn_key = cvm2_act_in.msisdn_key 
left join CVM_DB.CVM20_DEVICE dev on custinfo.tbl_dt = dev.tbl_dt and custinfo.msisdn_key = dev.msisdn_key 
left join CVM_DB.CVM20_REFILL_DATA_BUNDLE rbs on custinfo.tbl_dt = rbs.tbl_dt and custinfo.msisdn_key = rbs.msisdn_key 
left join CVM_DB.CVM20_REG_SBSCR_FLAGS rbs2 on custinfo.tbl_dt = rbs2.tbl_dt and custinfo.msisdn_key = rbs2.msisdn_key 
where custinfo.tbl_dt = yyyymmddRunDate ;
commit;
