select
       CASE
              WHEN count(*) = 2
                     THEN 'T'
                     ELSE 'F'
       end
from
       (
              select
                     case
                            when cnt_1 =0
                                   then 'F'
                                   else 'T'
                     end cnt_chk
              from
                     (
                            select
                                   count(*) cnt_1
                            from
                                   flare_8.flytxt_inbound_events_data
                            where
                                   tbl_dt=v_yyyy_mm_dd
                     )
              union all
              select
                     case
                            when cnt_2 =0
                                   then 'F'
                                   else 'T'
                     end
              from
                     (
                            select
                                   count(*) cnt_2
                            from
                                   flare_8.call_reason
                            where
                                   tbl_dt=v_yyyy_mm_dd
                     )
       )
where
       cnt_chk='T';