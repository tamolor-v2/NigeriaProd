select
translate( BUDGET_YEAR, chr(10)||chr(11)||chr(13) , ' '), 
translate( BUDGET_VERSION, chr(10)||chr(11)||chr(13) , ' '), 
translate( POSTING_COMBINATION_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate( ACCOUNT, chr(10)||chr(11)||chr(13) , ' '), 
translate( CODE_F, chr(10)||chr(11)||chr(13) , ' '), 
translate( PROJECT_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate( BUDGET_PERIOD, chr(10)||chr(11)||chr(13) , ' '), 
translate( AMOUNT, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.BUDGET_PERIOD_AMOUNT 