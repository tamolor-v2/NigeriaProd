SELECT mobl_num_voice_v,subscriber_category_v,tariff_code_v,subscriber_sub_category_v,status_code_v,account_code_n FROM CBS_TBL_CORE.CB_SERV_MAST_VIEW WHERE SUBSTR(account_code_n,-1,1)='${MSSDN}'

