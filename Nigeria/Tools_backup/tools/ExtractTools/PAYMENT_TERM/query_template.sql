select 
translate( PAY_TERM_ID , chr(10)||chr(11)||chr(13) , ' '), 
translate( DESCRIPTION , chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.PAYMENT_TERM  