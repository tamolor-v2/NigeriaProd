package com.ligadata.dataobject

class FinLogFeed extends Feed {

  var HDR_TYPE: String = ""
  var HDR_VER: String = ""

  var INSTRUCT_HDR_TYPE: String = ""
  var INSTRUCT_HDR_TIME: String = ""
  var INSTRUCT_HDR_TID: Long = _
  var INSTRUCT_HDR_FID: Long = _
  var INSTRUCT_HDR_UID: Long = _
  var INSTRUCT_HDR_QID: Long = _
  var INSTRUCT_HDR_SID: Long = _
  var INSTRUCT_HDR_EXTID: String = ""
  var INSTRUCT_HDR_USER: String = ""
  var INSTRUCT_HDR_REALUSER: String = ""
  var INSTRUCT_HDR_RECORDS: Int = _
  var INSTRUCT_HDR_CONTEXT: String = ""
  var INSTRUCT_HDR_BATCH_ID: Long = _
  var INSTRUCT_HDR_CASH_USER_ID: String = ""
  var INSTRUCT_HDR_PROVIDER_CATEGORY: String = ""
  var INSTRUCT_HDR_OTC_ID: String = ""
  var INSTRUCT_AMOUNT: Double = _ // todo this is coming as int
  var INSTRUCT_CC: String = ""

  //FROM
  var INSTRUCT_FROM_FRI: String = ""
  var INSTRUCT_FROM_RFRI: String = ""
  var INSTRUCT_FROM_SP: String = ""
  var INSTRUCT_FROM_Off_NET: Boolean = _
  var INSTRUCT_FROM_MESSAGE: String = ""
  var INSTRUCT_FROM_RATE: Double = _
  var INSTRUCT_FROM_CC: String = ""
  var INSTRUCT_FROM_AMOUNT: Double = _
  var INSTRUCT_FROM_IFEE: Double = _
  var INSTRUCT_FROM_FEE_ACT_FRI: String = ""
  var INSTRUCT_FROM_BANK_DOMAIN_NAME: String = ""
  var INSTRUCT_FROM_VAT: Double = _
  var INSTRUCT_FROM_EXT_FEE: Double = _
  var INSTRUCT_FROM_LOY_FEE: Double = _
  var INSTRUCT_FROM_LOY_REWARD: Double = _

  var INSTRUCT_FROM_BAL_COM_BFR: Double = _
  var INSTRUCT_FROM_BAL_COM_AFT: Double = _

  var INSTRUCT_FROM_BAL_AVL_BFR: Double = _
  var INSTRUCT_FROM_BAL_AVL_AFT: Double = _

  var INSTRUCT_FROM_BAL_TOT_BFR: Double = _
  var INSTRUCT_FROM_BAL_TOT_AFT: Double = _

  var INSTRUCT_FROM_BAL_LOY_BFR: Double = _
  var INSTRUCT_FROM_BAL_LOY_AFT: Double = _

  var INSTRUCT_FROM_PROM_AMOUNT: Double = _
  var INSTRUCT_FROM_DISC_AMOUNT: Double = _
  var INSTRUCT_FROM_OFFR_IDS: String = ""

  //FROM-> fro
  var INSTRUCT_FROM_FRO_ID: Long = _
  var INSTRUCT_FROM_FRO_USER_NAME: String = ""
  var INSTRUCT_FROM_FRO_USER_PRF: String = ""
  var INSTRUCT_FROM_FRO_MSISDN: String = ""

  //FROM-> holder
  var INSTRUCT_FROM_HLDR_EID: String = ""
  var INSTRUCT_FROM_HLDR_FIRST_NAME: String = ""
  var INSTRUCT_FROM_HLDR_LAST_NAME: String = ""
  var INSTRUCT_FROM_HLDR_NAME_PREFIX: String = ""
  var INSTRUCT_FROM_HLDR_NAME_SUFFIX: String = ""
  var INSTRUCT_FROM_HLDR_GENDER: String = ""
  var INSTRUCT_FROM_HLDR_DOB: String = ""

  //FROM-> holder_address
  var INSTRUCT_FROM_HLDR_ADDR_TYPE: String = ""
  var INSTRUCT_FROM_HLDR_ADDR_LINE: String = ""
  var INSTRUCT_FROM_HLDR_ADDR_STREET: String = ""
  var INSTRUCT_FROM_HLDR_ADDR_NUMBER: String = ""
  var INSTRUCT_FROM_HLDR_ADDR_POSTAL: String = ""
  var INSTRUCT_FROM_HLDR_ADDR_TOWN: String = ""
  var INSTRUCT_FROM_HLDR_ADDR_PROVNC: String = ""
  var INSTRUCT_FROM_HLDR_ADDR_CNTRYC: String = ""

  //FROM-> holder_identifications
  var INSTRUCT_FROM_HLDR_ID_TYPE: String = ""
  var INSTRUCT_FROM_HLDR_ID_NUMBER: String = ""

  //FROM-> holder_account_holder
  var INSTRUCT_FROM_ACCNT_HLDR_ID: Long = _
  var INSTRUCT_FROM_ACCNT_HLDR_USR_NAME: String = ""
  var INSTRUCT_FROM_ACCNT_HLDR_USR_PRF: String = ""
  var INSTRUCT_FROM_ACCNT_HLDR_MSISDN: String = ""

  //FROM-> point_of_sale
  var INSTRUCT_FROM_POS_OWNER_ID: Long = _
  var INSTRUCT_FROM_POS_NAME: String = ""
  var INSTRUCT_FROM_POS_MSISDN: String = ""
  var INSTRUCT_FROM_POS_ID: Long = _

  //FROM-> fro_parents L1
  var INSTRUCT_FROM_FROPRTS_LEVEL_1: Double = _
  var INSTRUCT_FROM_FROPRTS_ID_1: Long = _
  var INSTRUCT_FROM_FROPRTS_USRPRF_1: String = ""
  var INSTRUCT_FROM_FROPRTS_FNAME_1: String = ""
  var INSTRUCT_FROM_FROPRTS_LNAME_1: String = ""
  var INSTRUCT_FROM_FROPRTS_CITY_1: String = ""
  var INSTRUCT_FROM_FROPRTS_PCODE_1: String = ""
  var INSTRUCT_FROM_FROPRTS_MSISDN_1: String = ""

  //FROM-> fro_parents L2
  var INSTRUCT_FROM_FROPRTS_LEVEL_2: Long = _
  var INSTRUCT_FROM_FROPRTS_ID_2: Long = _
  var INSTRUCT_FROM_FROPRTS_USRPRF_2: String = ""
  var INSTRUCT_FROM_FROPRTS_FNAME_2: String = ""
  var INSTRUCT_FROM_FROPRTS_LNAME_2: String = ""
  var INSTRUCT_FROM_FROPRTS_CITY_2: String = ""
  var INSTRUCT_FROM_FROPRTS_PCODE_2: String = ""
  var INSTRUCT_FROM_FROPRTS_MSISDN_2: String = ""

  //FROM-> fro_parents L3
  var INSTRUCT_FROM_FROPRTS_LEVEL_3: Long = _
  var INSTRUCT_FROM_FROPRTS_ID_3: Long = _
  var INSTRUCT_FROM_FROPRTS_USRPRF_3: String = ""
  var INSTRUCT_FROM_FROPRTS_FNAME_3: String = ""
  var INSTRUCT_FROM_FROPRTS_LNAME_3: String = ""
  var INSTRUCT_FROM_FROPRTS_CITY_3: String = ""
  var INSTRUCT_FROM_FROPRTS_PCODE_3: String = ""
  var INSTRUCT_FROM_FROPRTS_MSISDN_3: String = ""

  // TO
  var INSTRUCT_TO_FRI: String = ""
  var INSTRUCT_TO_RFRI: String = ""
  var INSTRUCT_TO_SP: String = ""
  var INSTRUCT_TO_Off_NET: Boolean = _
  var INSTRUCT_TO_MESSAGE: String = ""
  var INSTRUCT_TO_RATE: Double = _
  var INSTRUCT_TO_CC: String = ""
  var INSTRUCT_TO_AMOUNT: Double = _
  var INSTRUCT_TO_IFEE: Double = _
  var INSTRUCT_TO_FEE_ACT_FRI: String = ""
  var INSTRUCT_TO_BANK_DOMAIN_NAME: String = ""
  var INSTRUCT_TO_VAT: Double = _
  var INSTRUCT_TO_EXT_FEE: Double = _
  var INSTRUCT_TO_LOY_FEE: Double = _
  var INSTRUCT_TO_LOY_REWARD: Double = _

  var INSTRUCT_TO_BAL_COM_BFR: Double = _
  var INSTRUCT_TO_BAL_COM_AFT: Double = _

  var INSTRUCT_TO_BAL_AVL_BFR: Double = _
  var INSTRUCT_TO_BAL_AVL_AFT: Double = _

  var INSTRUCT_TO_BAL_TOT_BFR: Double = _
  var INSTRUCT_TO_BAL_TOT_AFT: Double = _

  var INSTRUCT_TO_BAL_LOY_BFR: Double = _
  var INSTRUCT_TO_BAL_LOY_AFT: Double = _

  var INSTRUCT_TO_PROM_AMOUNT: Double = _
  var INSTRUCT_TO_DISC_AMOUNT: Double = _
  var INSTRUCT_TO_OFFR_IDS: String = ""

  //TO-> fro
  var INSTRUCT_TO_FRO_ID: Long = _
  var INSTRUCT_TO_FRO_USER_NAME: String = ""
  var INSTRUCT_TO_FRO_USER_PRF: String = ""
  var INSTRUCT_TO_FRO_MSISDN: String = ""

  //TO-> holder
  var INSTRUCT_TO_HLDR_EID: String = ""
  var INSTRUCT_TO_HLDR_FIRST_NAME: String = ""
  var INSTRUCT_TO_HLDR_LAST_NAME: String = ""
  var INSTRUCT_TO_HLDR_NAME_PREFIX: String = ""
  var INSTRUCT_TO_HLDR_NAME_SUFFIX: String = ""
  var INSTRUCT_TO_HLDR_GENDER: String = ""
  var INSTRUCT_TO_HLDR_DOB: String = ""

  //TO-> holder_address
  var INSTRUCT_TO_HLDR_ADDR_TYPE: String = ""
  var INSTRUCT_TO_HLDR_ADDR_LINE: String = ""
  var INSTRUCT_TO_HLDR_ADDR_STREET: String = ""
  var INSTRUCT_TO_HLDR_ADDR_NUMBER: String = ""
  var INSTRUCT_TO_HLDR_ADDR_POSTAL: String = ""
  var INSTRUCT_TO_HLDR_ADDR_TOWN: String = ""
  var INSTRUCT_TO_HLDR_ADDR_PROVNC: String = ""
  var INSTRUCT_TO_HLDR_ADDR_CNTRYC: String = ""

  //TO-> holder_identifications
  var INSTRUCT_TO_HLDR_ID_TYPE: String = ""
  var INSTRUCT_TO_HLDR_ID_NUMBER: String = ""

  //TO-> holder_account_holder
  var INSTRUCT_TO_ACCNT_HLDR_ID: Long = _
  var INSTRUCT_TO_ACCNT_HLDR_USR_NAME: String = ""
  var INSTRUCT_TO_ACCNT_HLDR_USR_PRF: String = ""
  var INSTRUCT_TO_ACCNT_HLDR_MSISDN: String = ""

  //TO-> point_of_sale
  var INSTRUCT_TO_POS_OWNER_ID: Long = _
  var INSTRUCT_TO_POS_NAME: String = ""
  var INSTRUCT_TO_POS_MSISDN: String = ""
  var INSTRUCT_TO_POS_ID: Long = _

  //TO-> fro_parents L1
  var INSTRUCT_TO_FROPRTS_LEVEL_1: Double = _
  var INSTRUCT_TO_FROPRTS_ID_1: Long = _
  var INSTRUCT_TO_FROPRTS_USRPRF_1: String = ""
  var INSTRUCT_TO_FROPRTS_FNAME_1: String = ""
  var INSTRUCT_TO_FROPRTS_LNAME_1: String = ""
  var INSTRUCT_TO_FROPRTS_CITY_1: String = ""
  var INSTRUCT_TO_FROPRTS_PCODE_1: String = ""
  var INSTRUCT_TO_FROPRTS_MSISDN_1: String = ""

  //TO-> fro_parents L2
  var INSTRUCT_TO_FROPRTS_LEVEL_2: Long = _
  var INSTRUCT_TO_FROPRTS_ID_2: Long = _
  var INSTRUCT_TO_FROPRTS_USRPRF_2: String = ""
  var INSTRUCT_TO_FROPRTS_FNAME_2: String = ""
  var INSTRUCT_TO_FROPRTS_LNAME_2: String = ""
  var INSTRUCT_TO_FROPRTS_CITY_2: String = ""
  var INSTRUCT_TO_FROPRTS_PCODE_2: String = ""
  var INSTRUCT_TO_FROPRTS_MSISDN_2: String = ""

  //TO-> fro_parents L3
  var INSTRUCT_TO_FROPRTS_LEVEL_3: Long = _
  var INSTRUCT_TO_FROPRTS_ID_3: Long = _
  var INSTRUCT_TO_FROPRTS_USRPRF_3: String = ""
  var INSTRUCT_TO_FROPRTS_FNAME_3: String = ""
  var INSTRUCT_TO_FROPRTS_LNAME_3: String = ""
  var INSTRUCT_TO_FROPRTS_CITY_3: String = ""
  var INSTRUCT_TO_FROPRTS_PCODE_3: String = ""
  var INSTRUCT_TO_FROPRTS_MSISDN_3: String = ""


  var INSTRUCT_HDR_COMMENT: String = ""

  // status
  var STATUS_CODE: String = ""
  var STATUS_MSG: String = ""


  /*override def toString: String = {
    val sb = new StringBuilder
    sb.append(HDR_TYPE).append(Constants.DELIMITER)
      .append(HDR_VER).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_TIME).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_TID).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_FID).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_UID).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_QID).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_SID).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_EXTID).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_USER).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_REALUSER).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_RECORDS).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_CONTEXT).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_BATCH_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_CASH_USER_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_PROVIDER_CATEGORY).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_OTC_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_AMOUNT).append(Constants.DELIMITER)
      .append(INSTRUCT_CC).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FRI).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_RFRI).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_SP).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_Off_NET).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_MESSAGE).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_RATE).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_CC).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_AMOUNT).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_IFEE).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_BANK_DOMAIN_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_VAT).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_EXT_FEE).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_LOY_FEE).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_LOY_REWARD).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_BAL_COM_BFR).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_BAL_COM_AFT).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_BAL_AVL_BFR).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_BAL_AVL_AFT).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_BAL_TOT_BFR).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_BAL_TOT_AFT).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_BAL_LOY_BFR).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_BAL_LOY_AFT).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_PROM_AMOUNT).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_DISC_AMOUNT).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_OFFR_IDS).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FRO_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FRO_USER_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FRO_USER_PRF).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FRO_MSISDN).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_EID).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_FIRST_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_LAST_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_NAME_PREFIX).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_NAME_SUFFIX).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_GENDER).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_DOB).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ADDR_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ADDR_LINE).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ADDR_STREET).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ADDR_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ADDR_POSTAL).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ADDR_TOWN).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ADDR_PROVNC).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ADDR_CNTRYC).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ID_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_HLDR_ID_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_ACCNT_HLDR_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_ACCNT_HLDR_USR_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_ACCNT_HLDR_USR_PRF).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_ACCNT_HLDR_MSISDN).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_POS_OWNER_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_POS_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_POS_MSISDN).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_POS_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_LEVEL_1).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_ID_1).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_USRPRF_1).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_FNAME_1).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_LNAME_1).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_CITY_1).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_PCODE_1).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_MSISDN_1).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_LEVEL_2).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_ID_2).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_USRPRF_2).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_FNAME_2).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_LNAME_2).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_CITY_2).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_PCODE_2).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_MSISDN_2).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_LEVEL_3).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_ID_3).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_USRPRF_3).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_FNAME_3).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_LNAME_3).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_CITY_3).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_PCODE_3).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FROPRTS_MSISDN_3).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FRI).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_RFRI).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_SP).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_Off_NET).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_MESSAGE).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_RATE).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_CC).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_AMOUNT).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_IFEE).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_BANK_DOMAIN_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_VAT).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_EXT_FEE).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_LOY_FEE).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_LOY_REWARD).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_BAL_COM_BFR).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_BAL_COM_AFT).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_BAL_AVL_BFR).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_BAL_AVL_AFT).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_BAL_TOT_BFR).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_BAL_TOT_AFT).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_BAL_LOY_BFR).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_BAL_LOY_AFT).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_PROM_AMOUNT).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_DISC_AMOUNT).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_OFFR_IDS).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FRO_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FRO_USER_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FRO_USER_PRF).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FRO_MSISDN).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_EID).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_FIRST_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_LAST_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_NAME_PREFIX).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_NAME_SUFFIX).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_GENDER).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_DOB).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ADDR_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ADDR_LINE).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ADDR_STREET).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ADDR_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ADDR_POSTAL).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ADDR_TOWN).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ADDR_PROVNC).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ADDR_CNTRYC).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ID_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_HLDR_ID_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_ACCNT_HLDR_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_ACCNT_HLDR_USR_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_ACCNT_HLDR_USR_PRF).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_ACCNT_HLDR_MSISDN).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_POS_OWNER_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_POS_NAME).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_POS_MSISDN).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_POS_ID).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_LEVEL_1).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_ID_1).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_USRPRF_1).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_FNAME_1).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_LNAME_1).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_CITY_1).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_PCODE_1).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_MSISDN_1).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_LEVEL_2).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_ID_2).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_USRPRF_2).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_FNAME_2).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_LNAME_2).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_CITY_2).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_PCODE_2).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_MSISDN_2).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_LEVEL_3).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_ID_3).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_USRPRF_3).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_FNAME_3).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_LNAME_3).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_CITY_3).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_PCODE_3).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FROPRTS_MSISDN_3).append(Constants.DELIMITER)
      .append(INSTRUCT_HDR_COMMENT).append(Constants.DELIMITER)
      .append(STATUS_CODE).append(Constants.DELIMITER)
      .append(STATUS_MSG).append(Constants.DELIMITER)
      .append(INSTRUCT_FROM_FEE_ACT_FRI).append(Constants.DELIMITER)
      .append(INSTRUCT_TO_FEE_ACT_FRI).append(Constants.DELIMITER)
      .mkString
  }
*/
  override def getName: String = {
    "EWP_FIN_LOG"
  }


  override def getFields: Array[String] = {
    val arr: Array[String] = new Array(181)

    arr(0) = HDR_TYPE
    arr(1) = HDR_VER
    arr(2) = INSTRUCT_HDR_TYPE
    arr(3) = INSTRUCT_HDR_TIME
    arr(4) = INSTRUCT_HDR_TID.toString
    arr(5) = INSTRUCT_HDR_FID.toString
    arr(6) = INSTRUCT_HDR_UID.toString
    arr(7) = INSTRUCT_HDR_QID.toString
    arr(8) = INSTRUCT_HDR_SID.toString
    arr(9) = INSTRUCT_HDR_EXTID
    arr(10) = INSTRUCT_HDR_USER
    arr(11) = INSTRUCT_HDR_REALUSER
    arr(12) = INSTRUCT_HDR_RECORDS.toString
    arr(13) = INSTRUCT_HDR_CONTEXT
    arr(14) = INSTRUCT_HDR_BATCH_ID.toString
    arr(15) = INSTRUCT_HDR_CASH_USER_ID
    arr(16) = INSTRUCT_HDR_PROVIDER_CATEGORY
    arr(17) = INSTRUCT_HDR_OTC_ID
    arr(18) = INSTRUCT_AMOUNT.toString
    arr(19) = INSTRUCT_CC
    arr(20) = INSTRUCT_FROM_FRI
    arr(21) = INSTRUCT_FROM_RFRI
    arr(22) = INSTRUCT_FROM_SP
    arr(23) = INSTRUCT_FROM_Off_NET.toString
    arr(24) = INSTRUCT_FROM_MESSAGE
    arr(25) = INSTRUCT_FROM_RATE.toString
    arr(26) = INSTRUCT_FROM_CC
    arr(27) = INSTRUCT_FROM_AMOUNT.toString
    arr(28) = INSTRUCT_FROM_IFEE.toString
    arr(29) = INSTRUCT_FROM_BANK_DOMAIN_NAME
    arr(30) = INSTRUCT_FROM_VAT.toString
    arr(31) = INSTRUCT_FROM_EXT_FEE.toString
    arr(32) = INSTRUCT_FROM_LOY_FEE.toString
    arr(33) = INSTRUCT_FROM_LOY_REWARD.toString
    arr(34) = INSTRUCT_FROM_BAL_COM_BFR.toString
    arr(35) = INSTRUCT_FROM_BAL_COM_AFT.toString
    arr(36) = INSTRUCT_FROM_BAL_AVL_BFR.toString
    arr(37) = INSTRUCT_FROM_BAL_AVL_AFT.toString
    arr(38) = INSTRUCT_FROM_BAL_TOT_BFR.toString
    arr(39) = INSTRUCT_FROM_BAL_TOT_AFT.toString
    arr(40) = INSTRUCT_FROM_BAL_LOY_BFR.toString
    arr(41) = INSTRUCT_FROM_BAL_LOY_AFT.toString
    arr(42) = INSTRUCT_FROM_PROM_AMOUNT.toString
    arr(43) = INSTRUCT_FROM_DISC_AMOUNT.toString
    arr(44) = INSTRUCT_FROM_OFFR_IDS
    arr(45) = INSTRUCT_FROM_FRO_ID.toString
    arr(46) = INSTRUCT_FROM_FRO_USER_NAME.toString
    arr(47) = INSTRUCT_FROM_FRO_USER_PRF.toString
    arr(48) = INSTRUCT_FROM_FRO_MSISDN.toString
    arr(49) = INSTRUCT_FROM_HLDR_EID.toString
    arr(50) = INSTRUCT_FROM_HLDR_FIRST_NAME.toString
    arr(51) = INSTRUCT_FROM_HLDR_LAST_NAME.toString
    arr(52) = INSTRUCT_FROM_HLDR_NAME_PREFIX.toString
    arr(53) = INSTRUCT_FROM_HLDR_NAME_SUFFIX.toString
    arr(54) = INSTRUCT_FROM_HLDR_GENDER.toString
    arr(55) = INSTRUCT_FROM_HLDR_DOB
    arr(56) = INSTRUCT_FROM_HLDR_ADDR_TYPE
    arr(57) = INSTRUCT_FROM_HLDR_ADDR_LINE
    arr(58) = INSTRUCT_FROM_HLDR_ADDR_STREET
    arr(59) = INSTRUCT_FROM_HLDR_ADDR_NUMBER
    arr(60) = INSTRUCT_FROM_HLDR_ADDR_POSTAL
    arr(61) = INSTRUCT_FROM_HLDR_ADDR_TOWN
    arr(62) = INSTRUCT_FROM_HLDR_ADDR_PROVNC
    arr(63) = INSTRUCT_FROM_HLDR_ADDR_CNTRYC
    arr(64) = INSTRUCT_FROM_HLDR_ID_TYPE
    arr(65) = INSTRUCT_FROM_HLDR_ID_NUMBER
    arr(66) = INSTRUCT_FROM_ACCNT_HLDR_ID.toString
    arr(67) = INSTRUCT_FROM_ACCNT_HLDR_USR_NAME
    arr(68) = INSTRUCT_FROM_ACCNT_HLDR_USR_PRF
    arr(69) = INSTRUCT_FROM_ACCNT_HLDR_MSISDN
    arr(70) = INSTRUCT_FROM_POS_OWNER_ID.toString
    arr(71) = INSTRUCT_FROM_POS_NAME
    arr(72) = INSTRUCT_FROM_POS_MSISDN
    arr(73) = INSTRUCT_FROM_POS_ID.toString
    arr(74) = INSTRUCT_FROM_FROPRTS_LEVEL_1.toString
    arr(75) = INSTRUCT_FROM_FROPRTS_ID_1.toString
    arr(76) = INSTRUCT_FROM_FROPRTS_USRPRF_1
    arr(77) = INSTRUCT_FROM_FROPRTS_FNAME_1
    arr(78) = INSTRUCT_FROM_FROPRTS_LNAME_1
    arr(79) = INSTRUCT_FROM_FROPRTS_CITY_1
    arr(80) = INSTRUCT_FROM_FROPRTS_PCODE_1
    arr(81) = INSTRUCT_FROM_FROPRTS_MSISDN_1
    arr(82) = INSTRUCT_FROM_FROPRTS_LEVEL_2.toString
    arr(83) = INSTRUCT_FROM_FROPRTS_ID_2.toString
    arr(84) = INSTRUCT_FROM_FROPRTS_USRPRF_2
    arr(85) = INSTRUCT_FROM_FROPRTS_FNAME_2
    arr(86) = INSTRUCT_FROM_FROPRTS_LNAME_2
    arr(87) = INSTRUCT_FROM_FROPRTS_CITY_2
    arr(88) = INSTRUCT_FROM_FROPRTS_PCODE_2
    arr(89) = INSTRUCT_FROM_FROPRTS_MSISDN_2
    arr(90) = INSTRUCT_FROM_FROPRTS_LEVEL_3.toString
    arr(91) = INSTRUCT_FROM_FROPRTS_ID_3.toString
    arr(92) = INSTRUCT_FROM_FROPRTS_USRPRF_3
    arr(93) = INSTRUCT_FROM_FROPRTS_FNAME_3
    arr(94) = INSTRUCT_FROM_FROPRTS_LNAME_3
    arr(95) = INSTRUCT_FROM_FROPRTS_CITY_3
    arr(96) = INSTRUCT_FROM_FROPRTS_PCODE_3
    arr(97) = INSTRUCT_FROM_FROPRTS_MSISDN_3
    arr(98) = INSTRUCT_TO_FRI
    arr(99) = INSTRUCT_TO_RFRI
    arr(100) = INSTRUCT_TO_SP
    arr(101) = INSTRUCT_TO_Off_NET.toString
    arr(102) = INSTRUCT_TO_MESSAGE
    arr(103) = INSTRUCT_TO_RATE.toString
    arr(104) = INSTRUCT_TO_CC
    arr(105) = INSTRUCT_TO_AMOUNT.toString
    arr(106) = INSTRUCT_TO_IFEE.toString
    arr(107) = INSTRUCT_TO_BANK_DOMAIN_NAME
    arr(108) = INSTRUCT_TO_VAT.toString
    arr(109) = INSTRUCT_TO_EXT_FEE.toString
    arr(110) = INSTRUCT_TO_LOY_FEE.toString
    arr(111) = INSTRUCT_TO_LOY_REWARD.toString
    arr(112) = INSTRUCT_TO_BAL_COM_BFR.toString
    arr(113) = INSTRUCT_TO_BAL_COM_AFT.toString
    arr(114) = INSTRUCT_TO_BAL_AVL_BFR.toString
    arr(115) = INSTRUCT_TO_BAL_AVL_AFT.toString
    arr(116) = INSTRUCT_TO_BAL_TOT_BFR.toString
    arr(117) = INSTRUCT_TO_BAL_TOT_AFT.toString
    arr(118) = INSTRUCT_TO_BAL_LOY_BFR.toString
    arr(119) = INSTRUCT_TO_BAL_LOY_AFT.toString
    arr(120) = INSTRUCT_TO_PROM_AMOUNT.toString
    arr(121) = INSTRUCT_TO_DISC_AMOUNT.toString
    arr(122) = INSTRUCT_TO_OFFR_IDS.toString
    arr(123) = INSTRUCT_TO_FRO_ID.toString
    arr(124) = INSTRUCT_TO_FRO_USER_NAME
    arr(125) = INSTRUCT_TO_FRO_USER_PRF
    arr(126) = INSTRUCT_TO_FRO_MSISDN
    arr(127) = INSTRUCT_TO_HLDR_EID
    arr(128) = INSTRUCT_TO_HLDR_FIRST_NAME
    arr(129) = INSTRUCT_TO_HLDR_LAST_NAME
    arr(130) = INSTRUCT_TO_HLDR_NAME_PREFIX
    arr(131) = INSTRUCT_TO_HLDR_NAME_SUFFIX
    arr(132) = INSTRUCT_TO_HLDR_GENDER
    arr(133) = INSTRUCT_TO_HLDR_DOB
    arr(134) = INSTRUCT_TO_HLDR_ADDR_TYPE
    arr(135) = INSTRUCT_TO_HLDR_ADDR_LINE
    arr(136) = INSTRUCT_TO_HLDR_ADDR_STREET
    arr(137) = INSTRUCT_TO_HLDR_ADDR_NUMBER
    arr(138) = INSTRUCT_TO_HLDR_ADDR_POSTAL
    arr(139) = INSTRUCT_TO_HLDR_ADDR_TOWN
    arr(140) = INSTRUCT_TO_HLDR_ADDR_PROVNC
    arr(141) = INSTRUCT_TO_HLDR_ADDR_CNTRYC
    arr(142) = INSTRUCT_TO_HLDR_ID_TYPE
    arr(143) = INSTRUCT_TO_HLDR_ID_NUMBER
    arr(144) = INSTRUCT_TO_ACCNT_HLDR_ID.toString
    arr(145) = INSTRUCT_TO_ACCNT_HLDR_USR_NAME
    arr(146) = INSTRUCT_TO_ACCNT_HLDR_USR_PRF
    arr(147) = INSTRUCT_TO_ACCNT_HLDR_MSISDN
    arr(148) = INSTRUCT_TO_POS_OWNER_ID.toString
    arr(149) = INSTRUCT_TO_POS_NAME
    arr(150) = INSTRUCT_TO_POS_MSISDN
    arr(151) = INSTRUCT_TO_POS_ID.toString
    arr(152) = INSTRUCT_TO_FROPRTS_LEVEL_1.toString
    arr(153) = INSTRUCT_TO_FROPRTS_ID_1.toString
    arr(154) = INSTRUCT_TO_FROPRTS_USRPRF_1
    arr(155) = INSTRUCT_TO_FROPRTS_FNAME_1
    arr(156) = INSTRUCT_TO_FROPRTS_LNAME_1
    arr(157) = INSTRUCT_TO_FROPRTS_CITY_1
    arr(158) = INSTRUCT_TO_FROPRTS_PCODE_1
    arr(159) = INSTRUCT_TO_FROPRTS_MSISDN_1
    arr(160) = INSTRUCT_TO_FROPRTS_LEVEL_2.toString
    arr(161) = INSTRUCT_TO_FROPRTS_ID_2.toString
    arr(162) = INSTRUCT_TO_FROPRTS_USRPRF_2
    arr(163) = INSTRUCT_TO_FROPRTS_FNAME_2
    arr(164) = INSTRUCT_TO_FROPRTS_LNAME_2
    arr(165) = INSTRUCT_TO_FROPRTS_CITY_2
    arr(166) = INSTRUCT_TO_FROPRTS_PCODE_2
    arr(167) = INSTRUCT_TO_FROPRTS_MSISDN_2
    arr(168) = INSTRUCT_TO_FROPRTS_LEVEL_3.toString
    arr(169) = INSTRUCT_TO_FROPRTS_ID_3.toString
    arr(170) = INSTRUCT_TO_FROPRTS_USRPRF_3
    arr(171) = INSTRUCT_TO_FROPRTS_FNAME_3
    arr(172) = INSTRUCT_TO_FROPRTS_LNAME_3
    arr(173) = INSTRUCT_TO_FROPRTS_CITY_3
    arr(174) = INSTRUCT_TO_FROPRTS_PCODE_3
    arr(175) = INSTRUCT_TO_FROPRTS_MSISDN_3
    arr(176) = INSTRUCT_HDR_COMMENT
    arr(177) = STATUS_CODE
    arr(178) = STATUS_MSG
    arr(179) = INSTRUCT_FROM_FEE_ACT_FRI
    arr(180) = INSTRUCT_TO_FEE_ACT_FRI


    arr
  }
}


object FinLogCls {

  case class Header(Type: String,
                    Ver: String) //current version 2.0

  case class Status(Code: String, Msg: Option[String] = None)

  case class FinTranLogRcd(Header: Header, Instruction: InstructionWithOutType, Status: Status)

  case class FinLogOther(Header: Header, Instruction: InstructionWithType, Status: Status)

  case class SubHeaderWithOutType(Time: String,
                                  Tid: Long,
                                  Fid: Long,
                                  Uid: Long,
                                  Qid: Option[Long] = None,
                                  Sid: Long,
                                  Extid: Option[String] = None,
                                  User: String,
                                  RealUser: String,
                                  Records: Option[Int] = None,
                                  Context: Option[String],
                                  BatchId: Option[Long],
                                  CashUserId: Option[String]
                                 )


  case class SubHeaderWithType(Type: String,
                               Time: String,
                               Tid: Long,
                               Fid: Long,
                               Uid: Long,
                               Qid: Option[Long] = None,
                               Sid: Long,
                               User: String,
                               Extid: Option[String],
                               RealUser: String,
                               Records: Option[Int] = None,
                               Context: Option[String],
                               ProviderCategory: Option[String],
                               BatchId: Option[Long],
                               CashUserId: Option[String],
                               OtcId: Option[String],
                               Comment: Option[String] = None
                              )


  case class InstructionWithType(Header: SubHeaderWithType, Amount: Option[Double],
                                 Cc: Option[String],
                                 From: Option[Party],
                                 To: Option[Party])

  case class InstructionWithOutType(Header: SubHeaderWithOutType, Amount: Option[Double],
                                    Cc: Option[String],
                                    From: Option[Party],
                                    To: Option[Party])


  case class Committed(Before: Double, After: Double)

  case class Available(Before: Double, After: Double)

  case class Total(Before: Double, After: Double)

  case class Loy(Before: Double, After: Double)

  case class Fro(Id: Long, Username: Option[String] = None, UserProfile: String, MSISDN: Option[String] = None)

  //line - comma separated list of strings
  case class Address(Type: Option[String] = None,
                     Line: Option[String] = None,
                     Street: Option[String] = None,
                     Number: Option[String] = None,
                     PostCode: Option[String] = None,
                     Town: Option[String] = None,
                     Province: Option[String] = None,
                     Cc: Option[String] = None)

  case class Identifications(Type: Option[String] = None, Number: Option[String] = None)

  case class Holder(Eid: Option[String] = None,
                    FirstName: Option[String] = None,
                    LastName: Option[String] = null,
                    NamePrefix: Option[String] = None,
                    NameSuffix: Option[String] = None,
                    Gender: Option[String] = None,
                    Dob: Option[String] = None,
                    Address: Option[Address] = None,
                    identifications: Option[Identifications] = None)

  case class AccountHolder(Id: Long,
                           Username: Option[String] = None,
                           UserProfile: String,
                           MSISDN: Option[String] = None)

  case class PointOfSale(OwnerId: Long,
                         Name: String,
                         MSISDN: Option[String] = None,
                         Id: Long)

  case class FroParents(Level: Long,
                        Id: Long,
                        UserProfile: String,
                        FirstName: Option[String] = None,
                        LastName: Option[String] = None,
                        City: Option[String] = None,
                        PostalCode: Option[String] = None,
                        MSISDN: Option[String] = None)

  case class Party(Fri: Option[String] = None,
                   RFri: Option[String]=None,
                   SP: Option[String]=None,
                   OffNet: Boolean,
                   Message: Option[String] = None,
                   Rate: Option[Double],
                   Cc: Option[String]=None,
                   Amount: Option[Double] = None,
                   IFee: Option[Double] = None,
                   FeeAccountFRI: Option[String] = None,
                   BankDomainName: Option[String],
                   VAT: Option[Double] = None,
                   EFee: Option[Double] = None,
                   LoyFee: Option[Double] = None,
                   LoyReward: Option[Double] = None,
                   Committed: Option[Committed] = None,
                   Available: Option[Available] = None,
                   Total: Option[Total] = None,
                   Loy: Option[Loy] = None,
                   Promotion: Option[Double] = None,
                   Discount: Option[Double] = None,
                   OfferIdentities: Option[String] = None,
                   FRO: Option[Fro] = None,
                   Holder: Option[Holder] = None,
                   AccountHolder: Option[AccountHolder] = None,
                   PointOfSale: Option[PointOfSale] = None,
                   FROParents: Option[List[FroParents]] = None)

}