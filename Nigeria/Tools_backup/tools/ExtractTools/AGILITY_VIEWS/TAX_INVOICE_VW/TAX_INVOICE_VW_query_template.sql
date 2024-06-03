select
year_month,
subscriber_code_n,
account_code_n,
bill_cycle_code_n,
service_id,
account_link_code_n,
package_name_v,
name,
'previous balance',
payment,
'adjustment and discount',
'subscription and vas',
'national call/sms',
'international call/sms',
'roaming call/sms',
data_usage,
vat,
total_charge,
amount_due
from  TT_MSO_1.TAX_INVOICE_VW

