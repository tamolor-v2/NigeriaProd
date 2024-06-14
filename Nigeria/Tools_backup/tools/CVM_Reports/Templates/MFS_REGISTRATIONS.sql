start transaction;
insert into engine_room.MFS_REGISTRATIONS 
select date_format(from_iso8601_timestamp(original_timestamp_enrich ),'%Y%m%d%H%i%s')  date_key, agent_id  , rpt_category, to_hex(sha1(to_utf8((coalesce (agent_msisdn,'') )))) agent_msisdn, to_hex(sha1(to_utf8((coalesce (msisdn,'') )))) customer_msidn,registration_status,registration_end_date,kamanja_loaded_date, cast(date_format(from_iso8601_timestamp(original_timestamp_enrich ),'%Y%m%d') as int) tbl_dt from (select  distinct split_part(split_part(identities,',',1),'(',2)  msisdn , case when recruited_type in ('EXT', 'USER', 'ID') then recruited_id else '' end agent_id , case when recruited_type in ('MSISDN') then recruited_id else '' end agent_msisdn , case when upper(status) in ('ACTIVE') then 'VALID' when upper(status) in ('BLOCKED','CLOSED','REGISTERED_BLOCKED') then 'INVALID' when upper(status) in ('REGISTERED','REGISTERED_CLOSED') then 'INCOMPLETE' else 'UNKNOWN' end registration_status , case when registration_date <> '' then date_format(from_iso8601_timestamp(registration_date ),'%Y%m%d%H%i%s') else '' end  registration_end_date ,aa.* , bb.* 
from  flare_8.provisioning_log aa  
left join 
nigeria.mfs_profile_categories bb 
on aa.profile_name = bb.profile_description 
where cast(date_format(from_iso8601_timestamp(original_timestamp_enrich ),'%Y%m%d') as int)   = yyyymmddRunDate and registration_date <> '')
;commit;
