start transaction;

delete from nigeria.clm_inbound_details_data
where tbl_dt=v_yyyy_mm_dd
;


insert into nigeria.clm_inbound_details_data  
select msisdn,occurrence,sub_date,offer_category,amount,product_name,product_subtype,revenue,price_of_offer,agent_id,flytxt_channel,retail_shop,retail_shop_type,msc_lga,msc_city,msc_state,date_parse(date_format(now(),'%Y%m%d %H%i%s'),'%Y%m%d %H%i%s') as update_datetime,tbl_dt
from
       (
                    select
                                 cl.msisdn
                               , row_number() over (partition by cl.msisdn order by
                                                    date_parse(try_cast(from_unixtime(cast (cl.start_date as bigint)/ 1000) as varchar), '%Y-%m-%d %H:%i:%s.%f') asc) as occurrence
                               , date_parse(try_cast(from_unixtime(cast (cl.start_date as bigint)/ 1000) as varchar), '%Y-%m-%d %H:%i:%s.%f') as                         sub_date
                               , cl.offer_category
                               , p.amount
                               , p.product_name
                               , p.product_subtype
                               , cl.revenue
                               , cl.price_of_offer
                               , cr.agent_id
                               , cl.channel as flytxt_channel
                               , cr.retail_shop
                               , case
                                              when lower(retail_shop) like '%bpo%'
                                                           then 'Call Center'
                                                           else 'Outlet'
                                 end retail_shop_type
                               , cr.msc_lga
                               , cr.msc_city
                               , cr.msc_state
                               , cr.tbl_dt
                    from
                                 flare_8.flytxt_inbound_events_data cl
                                 inner join
                                              flare_8.call_reason cr
                                              on
                                                           cl.msisdn                   = cast (cr.msisdn as varchar)
                                                           and trim(lower(cr.channel)) = 'phone'
                                                           and cl.tbl_dt               = cr.tbl_dt
                                                           and cl.channel              = 'CLM'
                                                           and cl.offer_status         = '1'
                                 left join
                                              (
                                                     select
                                                            t1.*
                                                     from
                                                            nigeria.cis_catalogue t1
                                                          , (
                                                                     select
                                                                              product_id
                                                                            , max(tbl_dt) max_tbl
                                                                     from
                                                                              nigeria.cis_catalogue
                                                                     group by
                                                                              product_id
                                                            )
                                                            t2
                                                     where
                                                            t1.product_id = t2.product_id
                                                            and t1.tbl_dt = t2.max_tbl
                                              )
                                              p
                                              on
                                                           p.product_id = split_part((split_part(SUBSTR(cl.offer_name, 11), '-', 1)), '_', 4)
                    where
                                 cr.tbl_dt =v_yyyy_mm_dd
                                 and cl.presented  = 1
                                 and cl.accepted   = 1
                                 and cl.conversion = 1
       )t
where
       occurrence = 1;
--select 
--split_part(SUBSTR(cl.offer_name,
--11),
--'-',
--1) as action,
--split_part((split_part(SUBSTR(cl.offer_name,11),'-',1)),'_',4) as offercode,
--cl.offer_status,
--cl.msisdn ,
--cl.start_date ,
--date_parse(try_cast(from_unixtime(cast (cl.start_date as bigint)/1000) as varchar),'%Y-%m-%d %H:%i:%s.%f') as sub_date,
--cl.offer_name ,
--cl.offer_category ,
--p.amount,
--p.product_id,
--p.product_name,
--p.product_type,
--p.product_subtype,
--cl.revenue ,
--cl.price_of_offer ,
--cr.user_name ,
--cr.agent_id ,
--cr.channel as call_reason_channel,
--cl.channel as flytxt_channel,
--cr.retail_shop,
--case
--when lower(retail_shop) like '%bpo%' then 'Call Center'
--else 'Outlet'
--end retail_shop_type,
--cr.msc_lga,
--cr.msc_city,
--cr.msc_state,
--cr.summary,
--cr.status,
--date_parse(date_format(now(),'%Y%m%d %H%i%s'),'%Y%m%d %H%i%s') as update_datetime,
--cr.tbl_dt
--from
--flare_8.flytxt_inbound_events_data cl
--inner join flare_8.call_reason cr on
--cl.msisdn = cast (cr.msisdn as varchar)
--and trim(lower(cr.channel)) = 'phone'
--and cl.tbl_dt = cr.tbl_dt
--and 
--    cl.channel = 'CLM'
--and  
--    cl.offer_status = '1'
--left join (
--select
--t1.*
--from
--nigeria.cis_catalogue t1,
--(
--select
--product_id,
--max(tbl_dt) max_tbl
--from
--nigeria.cis_catalogue
--group by
--product_id) t2
--where
--t1.product_id = t2.product_id
--and t1.tbl_dt = t2.max_tbl) p on
--p.product_id = split_part((split_part(SUBSTR(cl.offer_name,11),'-',1)),'_',4)
--where
--cr.tbl_dt = v_yyyy_mm_dd 
--and cl.presented =1 and cl.accepted =1 and cl.conversion=1;
--
--
commit;