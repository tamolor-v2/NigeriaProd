select
  SUBSCRIBER_CODE_N
 ,ACCOUNT_CODE_N
 ,ACCOUNT_LINK_CODE_N
 ,translate(STATUS_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,ACTIVATION_DATE_D
 ,ERASED_DATE_D
 ,translate(CR_DAYS_OPTN_V, chr(10)||chr(11)||chr(13) , ' ')
 ,CR_DAYS_N
 ,translate(REVENUE_GRADE_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(TAX_POLICY_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(CHRG_INTRST_OPTN_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(CHRG_INTRST_RATE_N, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(INVOICE_IN_BASE_CURNCY_FLG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(CURRENCY_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(INSTALLMENT_FLAG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,BILL_CYCL_CODE_N
 ,translate(BILLING_REGION_V, chr(10)||chr(11)||chr(13) , ' ')
 ,BILL_GROUP_N
 ,translate(DTLS_TO_FAX_NUM_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(EMAIL_LIST_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PAYER_OPTN_FLG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,PAYER_CODE_N
 ,translate(PAYMENTS_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(AUTO_DEBIT_FLG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(DUNNING_SCHDL_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,NEXT_INVOICE_SEQ_NUM_N
 ,translate(SUBSCRIBER_CATEGORY_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(RISK_CATEGORY_V, chr(10)||chr(11)||chr(13) , ' ')
 ,CREDIT_RATING_N
 ,translate(SUBSCRIBER_SUB_CATEGORY_V, chr(10)||chr(11)||chr(13) , ' ')
 ,ACTIVATED_BY_N
 ,translate(ACCOUNT_NAME_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ACCOUNT_TITLE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(EXCLUDE_LPF, chr(10)||chr(11)||chr(13) , ' ')
 ,PRE_TERMINATION_DATE_D
 ,translate(BILL_PRESENTATION_LANG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SMS_TO_NUMBER_V, chr(10)||chr(11)||chr(13) , ' ')
 ,NO_OF_SERVICES_N
 ,translate(CONTACT_PERSON_NAME_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ID_TYPE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(IDENTIFICATION_NUM_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(VIRTUAL_INVOICE_ACC_FLG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,ACCOUNT_MANAGER_N
 ,CREDIT_CONTROLER_N
 ,translate(ACCOUNT_TYPE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(MUL_APPLICABLE_FLG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PREFERRED_CURR_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(BILL_PRESENT_CURR_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(OLD_ACCOUNT_NUM_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(EXCLUDE_PRINT_FLG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,EXCLUDE_TILL_DATE_D
 ,translate(EXCLUDED_REASON_V, chr(10)||chr(11)||chr(13) , ' ')
 ,EXCLUDED_BY_N
 ,translate(SPL_ATTRIBUTE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ACC_ATTR_1_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ACC_ATTR_2_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ACC_ATTR_3_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ACC_ATTR_4_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ACC_ATTR_5_V, chr(10)||chr(11)||chr(13) , ' ')
 ,ACC_ATTR_1_N
 ,ACC_ATTR_2_N
 ,ACC_ATTR_3_N
 ,ACC_ATTR_4_N
 ,ACC_ATTR_5_N
 ,POSTED_FLAG_N
 ,POSTED_DATE_D
 ,translate(PACKAGE_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PROV_FLG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,ACC_CHRONO_NUM_N
 ,translate(IND_SHARED_FLAG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(CS_STATUS_V, chr(10)||chr(11)||chr(13) , ' ')
 ,STATUS_CHANGE_DATE_D
 ,translate(ENTITY_TYPE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ACCOUNT_NAME_LN_V, chr(10)||chr(11)||chr(13) , ' ')
 ,LAST_MODIFIED_DATE_D
 ,translate(RENTAL_SPECIFIC_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SEND_EMAIL_NOTIFICATION_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SEND_SMS_NOTIFICATION_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PREF_COMMUNICATION_LANG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(DEFAULT_DUNNING_SCHED_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(CREDIT_CONTROL_APPLIC_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ACCOUNT_TYPE_FLAG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(NEW_EXIST_ACCT_FLAG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(ADD_ACC_SERV_FLAG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(BLACKLISTED_FLG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PROF_CR_VETTING_TYPE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(TAX_OPTN_V, chr(10)||chr(11)||chr(13) , ' ')
 ,DISPATCH_NOTIFY_OPTNS_N
 ,translate(SET_DISPATCH_FLG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(TREATMENT_APPL_FLAG_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(EXT_ACCOUNT_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 FROM TT_MSO_1.CB_ACCOUNT_MASTER WHERE SUBSTR(account_code_n,-1,1)='${MSSDN}'
