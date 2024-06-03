select
a.COMPANY,
a.CURRENCY_TYPE,
a.CURRENCY_CODE,
a.VALID_FROM,
coalesce(b.VALID_TO,'20991231') valid_to,
a.CURRENCY_RATE,
a.CONV_FACTOR,
a.REF_CURRENCY_CODE,
a.OBJID,
a.OBJVERSION,
rownum as rwnum,
c.spool_rows
from
(select
1 as join_key,
COMPANY,
CURRENCY_TYPE,
CURRENCY_CODE,
to_char(VALID_FROM,'yyyymmdd') valid_from,
CURRENCY_RATE,
CONV_FACTOR,
REF_CURRENCY_CODE,
OBJID,
OBJVERSION,
row_number() over (partition by currency_code order by valid_from) rnk
from ifsapp.currency_rate) a left outer join
(select
currency_code,
to_char(valid_from-1,'yyyymmdd') valid_to,
row_number() over (partition by currency_code order by valid_from) rnk
from ifsapp.currency_rate) b
on (a.currency_code=b.currency_code and a.rnk=b.rnk-1)
left outer join (select 1 as join_key,count(1) spool_rows from ifsapp.currency_rate) c
on (a.join_key=c.join_key)
