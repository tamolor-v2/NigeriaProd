start transaction;
delete from CVM_DB.CVM20_BUNDLE_TRANSACTION where tbl_dt=yyyymmddRunDate;
insert into cvm_db.cvm20_bundle_transaction
(msisdn_key,transaction_date_time ,channel_name,product_id,product_name, product_name_views,product_type,product_subtype ,renewal_adhoc ,
original_timestamp_enrich ,action,expiry_time ,grace_period,offer_id,charging_amount ,transaction_charges ,auto_renewal_consent,tbl_dt)
select  
msisdn_key 
,calldate transaction_date_time
,channel channel_name 
,vascode product_id 
,product_name_orig  product_name  
,product_name product_name_views
,partner_name  product_type 
,service_name product_subtype
,'' renewal_adhoc 
,calldate original_timestamp_enrich
,'' action  
,'' expiry_time 
,'' grace_period   
,'' offer_id    
,sub_fee charging_amount
,'' transaction_charges 
,'' auto_renewal_consent 
,tbl_dt 
from 
(
select  
tbl_dt
,tbl_dt DATE_KEY
,MSISDN_KEY
,(CASE WHEN ((SUB_FEE = 0) AND (channel IN ('QRIOS', 'DYA', 'SMARTAPP', 'DATARESET', 'EVD'))) THEN cast(mod_amount as double) ELSE sub_fee END) SUB_FEE
,VASCODE
,serviceclassid SERVICECLASSID
,(CASE 
WHEN (vascode IN ('234012000010089', '23401220000011849')) THEN 'Carmudi daily' WHEN (vascode IN ('234012000010090', '23401220000011851')) THEN 'Carmudi Weekly' WHEN (vascode IN ('234012000010091', '23401220000011852')) THEN 'Carmudi Monthly' WHEN (vascode IN ('234012000010092', '23401220000011854')) THEN 'Eazy Taxi daily' WHEN (vascode IN ('234012000010093', '23401220000011855')) THEN 'Eazy Taxi Weekly' WHEN (vascode IN ('234012000010094', '23401220000011856')) THEN 'Eazy Taxi  Monthly' WHEN (vascode IN ('234012000010095', '23401220000011857')) THEN 'HELLO FOOD DAILY' WHEN (vascode IN ('234012000010096', '23401220000011858')) THEN 'HELLO FOOD WEEKLY ' WHEN (vascode IN ('234012000010097', '23401220000011859')) THEN 'HELLO FOOD MONTHLY' WHEN (vascode IN ('234012000010098', '23401220000011860')) THEN 'LAMUDI DAILY' WHEN (vascode IN ('234012000010099', '23401220000011862')) THEN 'LAMUDI WEEKLY' WHEN (vascode IN ('234012000010100', '23401220000011863')) THEN 'LAMUDI MONTHLY' WHEN (vascode IN ('23401220000019293', '125')) THEN 'TWITTER Daily' WHEN (vascode IN ('234012000012830', '23401220000015009', '126')) THEN 'TWITTER Weekly' WHEN (vascode IN ('234012000012831', '23401220000015010', '127')) THEN 'TWITTER Monthly' WHEN (vascode IN ('234012000012832', '23401220000015012', '129')) THEN 'FaceBook Weekly Bundle Plan' WHEN (vascode IN ('234012000012833', '23401220000015013', '130')) THEN 'FaceBook Monthly Data Bundle Plan' WHEN (vascode IN ('234012000012834', '23401220000015014', '132')) THEN '2GO weekly' WHEN (vascode IN ('234012000012835', '23401220000015015', '133')) THEN '2GO monthly' WHEN (vascode IN ('234012000012836', '23401220000015016', '135')) THEN 'NIMBUZZ WEEKLY' WHEN (vascode IN ('234012000012837', '23401220000015019', '138')) THEN 'WECHAT WEEKLY' WHEN (vascode IN ('234012000012838', '23401220000015018', '136')) THEN 'NIMBUZZ MONTHLY' WHEN (vascode IN ('234012000012840', '23401220000015021', '139')) THEN 'WECHAT MONTHLY' WHEN (vascode IN ('234012000012841', '23401220000015022', '141')) THEN 'WHATSAPP Weekly' WHEN (vascode IN ('234012000012842', '23401220000015023', '142')) THEN 'WHATSAPP MONTHLY' WHEN (vascode IN ('234012000012843', '23401220000015024', '144')) THEN 'ESKIMI WEEKLY' WHEN (vascode IN ('234012000012844', '23401220000015025', '145')) THEN 'ESKIMI MONTHLY' WHEN (vascode IN ('234012000012845', '23401220000015027')) THEN 'Wikipedia Weekly' WHEN (vascode IN ('234012000012847', '23401220000015028')) THEN 'Wikipedia Monthly' WHEN (vascode IN ('234012000015654', '23401220000018175', '149')) THEN 'Social Media Pass Daily' 
WHEN (vascode IN ('234012000015655', '23401220000018176', '150')) THEN 'Social Media Pass Weekly' WHEN (vascode IN ('234012000015656', '23401220000018177', '151')) THEN 'Social Media Pass Monthly' WHEN (vascode IN ('234012000016694', '23401220000019292')) THEN 'TWITTER Daily service' WHEN (vascode IN ('234012000016695', '23401220000019296', '23401220000019294', '128', '134')) THEN 'FaceBook Daily Bundle' WHEN (vascode IN ('234012000016696', '23401220000019295', '131')) THEN '2GO Daily service' WHEN (vascode IN ('234012000016697', '234012000016697')) THEN 'NIMBUZZ Daily' WHEN (vascode IN ('234012000016698', '23401220000019297', '137')) THEN 'WECHAT Daily service' WHEN (vascode IN ('234012000016699', '23401220000019298', '143')) THEN 'ESKIMI Daily service' WHEN (vascode IN ('234012000016700', '23401220000019300', '140')) THEN 'WHATSAPP Daily ' WHEN (vascode IN ('234012000017374', '23401220000020093', '146')) THEN 'INSTAGRAM Daily@25' WHEN (vascode IN ('234012000017375', '23401220000020094', '147')) THEN 'INSTAGRAM Weekly@50' WHEN (vascode IN ('234012000017376', '23401220000020095', '148')) THEN 'INSTAGRAM Monthly@150' WHEN (vascode = '23401220000010188') THEN 'mAcademy Bundle Weekly' WHEN (vascode = '23401220000010264') THEN 'MTN Kids pkg Daily' WHEN (vascode = '23401220000010265') THEN 'MTN Kids Package Weekly' WHEN (vascode = '23401220000010266') THEN 'MTN Kids Package Monthly' WHEN (vascode = '23401220000010271') THEN 'Facebook Bundle Weekly' WHEN (vascode = '23401220000010272') THEN 'Facebook Bundle Monthly' WHEN (vascode = '23401220000010279') THEN 'Jobberman Weekly' WHEN (vascode = '23401220000010280') THEN 'Jobberman Monthly' WHEN (vascode = '23401220000010282') THEN 'Whatsapp Weekly' WHEN (vascode = '23401220000010283') THEN 'Whatsapp Monthly' WHEN (vascode = '23401220000010332') THEN 'mAcademy' WHEN (vascode = '23401220000020702') THEN product_name WHEN (vascode = '23401220000020703') THEN product_name WHEN (vascode IN ('23401220000026429', '230')) THEN '1GB Instagram/TikTok Weekly bundle' WHEN (vascode IN ('23401220000026380', '231')) THEN '350MB Instagram/TikTok Daily bundle' WHEN (vascode = '298') THEN 'Pulse InstaBinge Heavy FALZ' WHEN (vascode = '108') THEN '1-hour StarTimes Video Streaaming Pack' WHEN (vascode = '109') THEN '3-hours StarTimes Video Streaaming Pack' WHEN (vascode = '297') THEN 'All night Weekly Streaming Pack (11pm - 6am)' WHEN (vascode = '296') THEN 'YouTube+Instagram+TikTok bundle' WHEN (vascode = '295') THEN 'All Social Bundles Monthly' WHEN (vascode = '98') THEN 'All night Daily Streaming pack (12-5am)' WHEN (vascode = '293') THEN 'All Social Bundles Daily' WHEN (vascode = '294') THEN 'All Social Bundles Weekly' WHEN (vascode = '313') THEN '1 Hour  Video Pack' 
WHEN (vascode = '314') THEN '3 Hours  Video Pack' WHEN (vascode = '951') THEN 'Tiktok Daily' WHEN (vascode = '952') THEN 'TikTok Weekly' 
WHEN (VASCODE IN ('234012000017630', '23401220000020401', '117')) THEN 'Combo Large (Zone 2)' WHEN (VASCODE IN ('234012000017631', '23401220000020402', '118')) THEN 'Data Small (Zone 1)' WHEN (VASCODE IN ('23401220000012160', '23401220000012160')) THEN 'MTN Roaming Bundle China-Max' WHEN (VASCODE IN ('234012000017626', '23401220000020395')) THEN 'Combo Large (Zone 1)' WHEN (VASCODE IN ('234012000012292', '23401220000014371')) THEN 'Hello World Monthly Zone 2' WHEN (VASCODE IN ('234012000017633', '23401220000020404')) THEN 'Data Small (Zone 2)' WHEN (VASCODE IN ('234012000017619', '23401220000020386', '122')) THEN 'UAE Only' WHEN (VASCODE IN ('234012000017634', '23401220000020405', '121')) THEN 'Data Large (Zone 2)' 
WHEN (VASCODE IN ('234012000017629', '23401220000020400', '116')) THEN 'Combo Medium (Zone 2)' WHEN (VASCODE IN ('234012000017625', '23401220000020394')) THEN 'Combo Medium (Zone 1)' WHEN (VASCODE IN ('234012000012291', '23401220000014370')) THEN 'Hello World weekly Zone 2' WHEN (VASCODE IN ('234012000017622', '23401220000020389', '115')) THEN 'Combo Small (Zone 1)' WHEN (VASCODE IN ('234012000017636', '23401220000020407')) THEN 'Data Large (Zone 3)' WHEN (VASCODE IN ('234012000012290', '23401220000014369')) THEN 'Hello World Monthly Zone 1' WHEN (VASCODE IN ('23401220000012159', '23401220000012159')) THEN 'MTN Roaming Bundle China' WHEN (VASCODE IN ('234012000017628', '23401220000020399')) THEN 'Combo Small (Zone 2)' WHEN (VASCODE IN ('234012000017635', '23401220000020406')) THEN 'Data small (Zone 3)' WHEN (VASCODE IN ('234012000017632', '23401220000020403', '119')) THEN ' Data Large (Zone 1)' WHEN (VASCODE IN ('234012000012289', '23401220000014368')) THEN 'Hello World Weekly Zone 1' WHEN (VASCODE IN ('234012000012851', '23401220000015034')) THEN 'Hello World Daily Zone 1' WHEN (VASCODE IN ('234012000012852', '23401220000015035')) THEN 'Hello World Daily Zone 2' 
WHEN (VASCODE = '23401220000025930') THEN 'World Cup data only offer 1.5GB' WHEN (VASCODE = '23401220000025928') THEN 'World Cup data only offer 750MB ' 
WHEN (VASCODE = '23401220000025926') THEN 'World Cup data only offer 500MB' 
WHEN (VASCODE IN ('234012000011592', '23401220000013411', '208')) THEN 'MTN Pulse 1.5GB Weekly bundle' 
WHEN (VASCODE IN ('234012000015755', '23401220000012617', '191')) THEN '40MB Daily Plan' WHEN (VASCODE IN ('234012000015755', '23401220000011356', '123')) THEN '100MB Daily Plan' WHEN (VASCODE IN ('234012000015755', '23401220000011360', '192')) THEN '200MB 3-day plan' WHEN (VASCODE IN ('234012000015775', '23401220000011361', '193')) THEN '750MB 2-week plan' WHEN (VASCODE IN ('234012000015775', '23401220000020082', '152')) THEN '350MB Weekly data plan' WHEN (VASCODE IN ('234012000015776', '23401220000011365', '195')) THEN '4.5GB 1-Month All Day plan' WHEN (VASCODE IN ('234012000015776', '23401220000011363', '194')) THEN '1.5GB 1-Month Mobile Data plan ' WHEN (VASCODE IN ('234012000015776', '23401220000020086', '196')) THEN '12GB Monthly Plan' WHEN (VASCODE IN ('234012000015776', '23401220000012619', '197')) THEN '20GB Monthly Plan' WHEN (VASCODE IN ('234012000015776', '23401220000012620', '198')) THEN '40GB Monthly Plan' WHEN (VASCODE IN ('234012000015777', '23401220000012701', '199')) THEN '100GB 2-Month Plan' WHEN (VASCODE IN ('234012000015778', '23401220000012622', '200')) THEN '400GB 3-Month Plan' WHEN (VASCODE IN ('234012000015778', '23401220000012631', '204')) THEN '4.5TB Yearly Plan' WHEN (VASCODE IN ('234012000015779', '23401220000012625', '202')) THEN '325GB 180 Days' WHEN (VASCODE IN ('234012000015780', '23401220000012630', '203')) THEN '2.5TB Yearly Plan' WHEN (VASCODE IN ('234012000015780', '23401220000012631')) THEN '1500GB 365 Days' WHEN (VASCODE IN ('234012000015781', '23401220000016108')) THEN '500MB (12am-5am)' WHEN (VASCODE IN ('234012000015782', '23401220000016111')) THEN '500MB (WEEKEND) SAT-SUN' WHEN (VASCODE IN ('234012000015782', '23401220000016112')) THEN '1.2GB (WEEKEND) SAT-SUN' 
WHEN (VASCODE IN ('234012000016425', '23401220000012645', '207')) THEN '20GB (Weekdays) 7am -7pm Mon-Fri' WHEN (VASCODE IN ('234012000016425', '23401220000012644', '206')) THEN '10GB (Weekdays) 7am -7pm Mon-Fri' 
WHEN (VASCODE IN ('234012000016834', '23401220000019434')) THEN '1MB Daily' WHEN (VASCODE IN ('234012000016835', '23401220000019435')) THEN '5MB Daily' WHEN (VASCODE IN ('234012000016837', '23401220000019437', '213')) THEN '2GB Weekly data plan' 
WHEN (VASCODE IN ('23401220000013017', '23401220000013017', '23401220000020629')) THEN 'MTN Pulse 500MB Night bundle' WHEN (VASCODE IN ('23401220000021248', '153')) THEN '2GB + 4GB YouTube Night Monthly Plan' WHEN (VASCODE IN ('23401220000026373', '23401220000026653', '67')) THEN '500MB MTN Pulse Night bundle' WHEN (VASCODE IN ('23401220000026374', '23401220000026652', '66')) THEN '250MB MTN Pulse Night bundle' WHEN (VASCODE IN ('23401220000026436', '216')) THEN '4GB Month All Day plan(winback)' WHEN (VASCODE IN ('23401220000026437', '215')) THEN '1GB Weekly data plan(winback)' WHEN (VASCODE IN ('23401220000026438', '214')) THEN '250MB 3Days (winback)' WHEN (VASCODE IN ('23401220000012623', '201')) THEN '600GB 3-Month Plan' WHEN (VASCODE IN ('23401220000027212', '210')) THEN '160GB 2-Month Plan' WHEN (VASCODE IN ('23401220000027224', '209')) THEN '750MB Weekly Plan' WHEN (VASCODE IN ('23401220000027225', '211')) THEN '3GB Monthly Plan' WHEN (VASCODE IN ('23401220000027226', '212')) THEN '25GB Monthly Plan' WHEN (VASCODE = '258') THEN '30MB Daily Plan' WHEN (VASCODE = '276') THEN '1GB  Daily Plan' WHEN (VASCODE = '277') THEN '50GB Monthly 4G Plan' WHEN (VASCODE = '278') THEN '75GB Monthly Plan' WHEN (VASCODE = '279') THEN '2GB 2-Day Plan' WHEN (VASCODE = '280') THEN '25GB Monthly 4G Plan' WHEN (VASCODE IN ('23401220000026535', '456')) THEN '700MB 14Days bundle - (For Teen website ONLY)' WHEN (VASCODE IN ('23401220000026534', '457')) THEN '200MB weekly bundle - (For Teen website ONLY)' WHEN (VASCODE = '308') THEN '10MB Weekly Plan' WHEN (VASCODE = '309') THEN '25MB Monthly Plan' WHEN (VASCODE = '310') THEN '50MB Monthly Plan' 
WHEN (VASCODE = '311') THEN '1GB Monthly Entry Bundle' WHEN (VASCODE = '734') THEN 'MTN Pulse 750MB 3DAYS bundle' WHEN (VASCODE = '738')  THEN 'XtraSpecial 3.75GB Monthly Plan' WHEN (VASCODE = '742') THEN '1GB Weekly Plan' WHEN (VASCODE = '743')  THEN '6GB Weekly Plan' WHEN (VASCODE = '744') THEN '6GB Monthly Plan' WHEN (VASCODE = '745') THEN '10GB Monthly Plan' 
WHEN (VASCODE = '746') THEN '120GB Monthly Plan' WHEN (VASCODE = '754') THEN '2.5GB 2-Day Plan' WHEN (VASCODE = '755') THEN '400GB Yearly Plan' WHEN (VASCODE = '260') THEN 'XtraSpecial 4.5GB monthly_plan' WHEN (VASCODE = '825') THEN 'XtraSpecial 1.5GB Bi-Weekly Plan' WHEN (VASCODE = '976') THEN '200GB Monthly Plan' 
WHEN (VASCODE = '977') THEN '800GB Monthly Plan'
WHEN (vascode IN ('23401220000014870', '234012000012691', '73')) THEN 'XtraValue Bundle V300' WHEN (vascode IN ('23401220000014871', '234012000012692', '83')) THEN 'XtraValue Bundle V500' WHEN (vascode IN ('23401220000014872', '234012000012693', '84')) THEN 'XtraValue Bundle V1000' WHEN (vascode IN ('23401220000014873', '234012000012694', '85')) THEN 'XtraValue Bundle V2000' WHEN (vascode IN ('23401220000014874', '234012000012695', '86')) THEN 'XtraValue Bundle V5000' WHEN (vascode IN ('23401220000014875', '234012000012696', '90')) THEN 'XtraValue Bundle D300' WHEN (vascode IN ('23401220000014876', '234012000012697', '91')) THEN 'XtraValue Bundle D500' WHEN (vascode IN ('23401220000014877', '234012000012698', '92')) THEN 'XtraValue Bundle D1000' WHEN (vascode IN ('23401220000014879', '234012000012699', '93')) THEN 'XtraValue Bundle D2000' WHEN (vascode IN ('23401220000014880', '234012000012700', '94')) THEN 'XtraValue Bundle D5000' 
WHEN (vascode IN ('23401220000015648', '234012000013469', '87')) THEN 'XtraVoice XtraTalk 10000' WHEN (vascode IN ('23401220000015649', '234012000013470', '88')) THEN 'XtraVoice XtraTalk 15000' WHEN (vascode IN ('23401220000015650', '234012000013471', '89')) THEN 'XtraVoice XtraTalk 20000' WHEN (vascode IN ('23401220000015651', '234012000013472', '95')) THEN 'XtraData 10000' WHEN (vascode IN ('23401220000015652', '234012000013473', '96')) THEN 'XtraData 15000' WHEN (vascode IN ('23401220000015655', '234012000013474', '97')) THEN 'XtraData 20000' WHEN (vascode IN ('23401220000016254', '234012000014055')) THEN 'new XtraValue Bundle V300' WHEN (vascode IN ('234012000014062', '23401220000016261')) THEN 'new XtraValue Bundle D1000' WHEN (vascode IN ('234012000014056', '23401220000016255')) THEN 'new XtraValue Bundle V500' WHEN (vascode = '23401220000016255') THEN 'new XtraValue Bundle V500' 
WHEN (vascode IN ('234012000014061', '23401220000016260')) THEN 'new XtraValue Bundle D500' WHEN (vascode IN ('23401220000016256', '234012000014057')) THEN 'new XtraValue Bundle V1000' WHEN (vascode IN ('23401220000016257', '234012000014058')) THEN 'new XtraValue Bundle V2000' WHEN (vascode IN ('23401220000016258', '234012000014059')) THEN 'new XtraValue Bundle V5000' WHEN (vascode IN ('23401220000016259', '234012000014060')) THEN 'new XtraValue Bundle D300' WHEN (vascode = '23401220000016260') THEN 'new XtraValue Bundle D500' WHEN (vascode = '23401220000016261') THEN 'new XtraValue Bundle D1000' WHEN (vascode IN ('23401220000016262', '234012000014063')) THEN 'new XtraValue Bundle D2000' WHEN (vascode = '23401220000016263') THEN 'new XtraValue Bundle D5000' WHEN (vascode IN ('23', '290')) THEN 'Xtracombo50' WHEN (vascode IN ('27', '291')) THEN 'Xtracombo100' WHEN (vascode IN ('51', '292')) THEN 'Xtracombo500' 
WHEN (VASCODE IN ('23401220000014870', '234012000012691', '73')) THEN 'XtraValue Bundle V300' WHEN (VASCODE IN ('23401220000014871', '234012000012692', '83')) THEN 'XtraValue Bundle V500' WHEN (VASCODE IN ('23401220000014872', '234012000012693', '84')) THEN 'XtraValue Bundle V1000' WHEN (VASCODE IN ('23401220000014873', '234012000012694', '85')) THEN 'XtraValue Bundle V2000' WHEN (VASCODE IN ('23401220000014874', '234012000012695', '86')) THEN 'XtraValue Bundle V5000' WHEN (VASCODE IN ('23401220000014875', '234012000012696', '90')) THEN 'XtraValue Bundle D300' WHEN (VASCODE IN ('23401220000014876', '234012000012697', '91')) THEN 'XtraValue Bundle D500' WHEN (VASCODE IN ('23401220000014877', '234012000012698', '92')) THEN 'XtraValue Bundle D1000' WHEN (VASCODE IN ('23401220000014879', '234012000012699', '93')) THEN 'XtraValue Bundle D2000' WHEN (VASCODE IN ('23401220000014880', '234012000012700', '94')) THEN 'XtraValue Bundle D5000' WHEN (VASCODE IN ('23401220000015648', '234012000013469', '87')) THEN 'XtraValue Bundle V10000' WHEN (VASCODE IN ('23401220000015649', '234012000013470', '88')) THEN 'XtraValue Bundle V15000' WHEN (VASCODE IN ('23401220000015650', '234012000013471', '89')) THEN 'XtraValue Bundle V20000' WHEN (VASCODE IN ('23401220000015651', '234012000013472', '95')) THEN 'XtraValue Bundle D10000' WHEN (VASCODE IN ('23401220000015652', '234012000013473', '96')) THEN 'XtraValue Bundle D15000' WHEN (VASCODE IN ('23401220000015655', '234012000013474', '97')) THEN 'XtraValue Bundle D20000' WHEN (VASCODE IN ('23401220000016254', '234012000014055')) THEN 'new XtraValue Bundle V300' WHEN (VASCODE IN ('234012000014062', '23401220000016261')) THEN 'new XtraValue Bundle D1000' WHEN (VASCODE IN ('234012000014056', '23401220000016255')) THEN 'new XtraValue Bundle V500' WHEN (VASCODE = '23401220000016255') THEN 'new XtraValue Bundle V500' 
WHEN (VASCODE IN ('234012000014061', '23401220000016260')) THEN 'new XtraValue Bundle D500' WHEN (VASCODE IN ('23401220000016256', '234012000014057')) THEN 'new XtraValue Bundle V1000' WHEN (VASCODE IN ('23401220000016257', '234012000014058')) THEN 'new XtraValue Bundle V2000' WHEN (VASCODE IN ('23401220000016258', '234012000014059')) THEN 'new XtraValue Bundle V5000' WHEN (VASCODE IN ('23401220000016259', '234012000014060')) THEN 'new XtraValue Bundle D300' WHEN (VASCODE = '23401220000016260') THEN 'new XtraValue Bundle D500' WHEN (VASCODE = '23401220000016261') THEN 'new XtraValue Bundle D1000' WHEN (VASCODE IN ('23401220000016262', '234012000014063')) THEN 'new XtraValue Bundle D2000' WHEN (VASCODE = '23401220000016263') THEN 'new XtraValue Bundle D5000' WHEN (VASCODE IN ('23', '290')) THEN 'Xtracombo50' 
WHEN (VASCODE IN ('27', '291')) THEN 'Xtracombo100' 
WHEN (VASCODE IN ('51', '292')) THEN 'Xtracombo500'
when (VASCODE IN ('1035')) THEN 'Instagram 3-Day Plan'       
ELSE product_name END) PRODUCT_NAME
, CHANNEL, calldate , 
case when vascode in  ('722') then 'LTE VPN'
when vascode in  ('726') then 'LTE INTERNET'
when  vascode in ('897') then 'TariffMigration'
when vascode in ( '908') then  'FIBERNET_PREPAID_STAFF'
else    partner_name end partner_name
, service_name, product_name product_name_orig
FROM
nigeria.hsdp_sumd
WHERE  tbl_dt = yyyymmddRunDate 
and vascode not in 
(select trim(productid) from nigeria.Digital_VAS_Product_Catalogue ab
where  upper(productname) not  like ('%MUSIC%TIME%'))
and vascode not in ('935','1026','961' ,'1030','1028','1029' ,'1027','23401220000030319' ,'330','23401220000028583','23401220000028584',
'1012','1014','23401220000026036','23401220000026037','23401220000028586','835','23401220000028585',
'440','444','658','234012000024167','234012000024747','234012000024280','234012000024316','234012000024094','234012000025451','234012000020657',
'234012000023974','234012000024369','234012000024727','234012000024979','234012000025057','234012000024450','23401220000029100','23401220000029400',
'23401220000027600','23401220000029400','23401220000024900','23401220000010200','23401220000025800','23401220000016200','23401220000029800','23401220000029800',
'23401220000029000','23401220000029400','23401220000018500','23401220000007600','23401220000029300','23401220000016100','23401220000028800','23401220000013900',
'23401220000028900','23401220000028800','23401220000029400','23401220000029100','23401220000007600','23401220000030200','23401220000024800','23401220000007600',
'23401220000016100','23401220000020200','23401220000029300','23401220000029800','23401220000030100','23401220000010200','23401220000026000','23401220000029900',
'23401220000028600','10000220000002600','23401220000013700','23401220000020100','23401220000028300','23401220000013900','23401220000025100','23401220000028400',
'23401220000028600','23401220000010200','23401220000029000','23401220000028900','23401220000012600','23401220000027600','23401220000028900','23401220000028300',
'23401220000010200','23401220000028800','23401220000029800','23401220000028700','23401220000028900','23401220000028900','23401220000029000','23401220000030200',
'23401220000029400','23401220000015000','23401220000027700','23401220000016100','23401220000029800','23401220000012600','23401220000028900','23401220000016200','23401220000007600',
'23401220000018500','23401220000016200','23401220000028300','23401220000027500','23401220000028900','23401220000027000','23401220000007600','23401220000013900','23401220000007600','23401220000003000','23401220000029700',
'23401220000028300','23401220000029300','23401220000029900','23401220000028300','23401220000029100','23401220000008800','23401220000010100','23401220000018500','23401220000028900','23401220000018500','23401220000029900','23401220000029900','23401220000029300',
'23401220000013900','23401220000018400','23401220000020000','23401220000004300','23401220000029000','23401220000018900','23401220000013000','23401220000018500',
'23401220000027100','23401220000029800','23401220000029000','23401220000018500','23401220000027600','23401220000030000','23401220000018500','23401220000029800',
'23401220000028900','23401220000026000','23401220000007600','23401220000030100','23401220000013900','23401220000028900','23401220000010300','23401220000028900',
'23401220000018400','23401220000029700','23401220000029400','23401220000013000','23401220000018500','23401220000018500','23401220000027600','23401220000029300',
'23401220000018500','23401220000027600','23401220000027300','23401220000028400','23401220000021100','23401220000016200','23401220000025300','23401220000007500','23401220000029800','23401220000018500','23401220000029800','23401220000010100',
'23401220000029800','23401220000029800','23401220000016100','23401220000015900','23401220000028900','23401220000029300','23401220000029400','23401220000029400','23401220000029300','23401220000029000',
'23401220000007600','23401220000013900','23401220000003000','23401220000028300','23401220000019200','23401220000018500') 
and sub_fee > 0 
and vascode not in  
(select distinct  vascode product_name 
from nigeria.hsdp_sumd where length(vascode) > 4
and upper(product_name) not like '%MUSIC%TIME%'  
and tbl_dt= yyyymmddRunDate) 
and vascode is not null 
);
commit;