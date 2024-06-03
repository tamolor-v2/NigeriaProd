start transaction;
insert into engine_room.cells 
select 
coalesce(cdr_cgi,'') , coalesce(siteid,'') , coalesce(sitename,'') , coalesce(site_type,'') , coalesce(cluster,'') , coalesce(lon,'') , coalesce(lat,'')
from flare_8.VP_DIM_CELLSITE;
commit;
