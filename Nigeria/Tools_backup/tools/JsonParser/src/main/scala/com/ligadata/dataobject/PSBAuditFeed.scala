package com.ligadata.dataobject

import com.ligadata.parsers.Constants

class PSBAuditFeed extends Feed { // todo adding more fields ( SupplementaryData & ExtensionData )





  var INITIATING_USER: String = ""
  var REAL_USER: String = ""
  var LOGGING_TIME: String = ""
  var SESSION_ID: Long = _
  var TRANSACTION_ID: Long = _
  var TRANSACTION_TYPE: String = ""
  var STATUS: String = ""
  var RECORD_VERSION: Int = _
  var ExtensionData: String = ""
  var LocationDataEntries: String = ""
  var AdditionalRatingEntries: String = ""
  var Imsi: String = ""
  var ApprovalInitiatedBy: String = ""
  var ApprovalId: Long = _
  var TagName: String = ""
  var NewTagName: String = ""
  var TransactionPeriod: String = ""
  var StatsticsTagLimit: String = ""
  var TagStatictics: String = ""
  var TagMinMaxBalances: String = ""
  var BlacklistAccountHolder: String = ""
  var RECEIVING_FRI: String = ""
  var SENDING_FRI: String = ""
  var AccountHolder_gvnNm : String = ""
  var AccountHolder_nm : String = ""
  var AccountHolder_gndr : String = ""
  var AccountHolder_lang : String = ""
  var AccountHolder_birthDt : String = ""
  var AccountHolder_prfssn : String = ""
  var AccountHolder_AccountHolderID : String = ""
  var AccountHolder_TaxNumber : String = ""
  var AccountHolder_TaxationCountry : String = ""
  var AccountHolder_TaxRegion : String = ""
  var AccountHolder_AccountHolderSettingName : String = ""
  var AccountHolder_AccountHolderSettingValue : String = ""
  var AccountHolder_HomeChargingRegionName : String = ""
  var AccountHolder_BankDomainName : String = ""
  var AccountHolder_LanguageCode : String = ""
  var AccountHolder_IndividualPerson : String = ""
  var AccountHolder_CommissioningFri : String = ""
  var AccountHolder_Msisdn : String = ""
  var AccountHolder_NormalizedMsisdn : String = ""
  var AccountHolder_TotalNumber : String = ""
  var AccountHolder_BlacklistedNumber : String = ""
  var AccountHolder_SkippedNumber : String = ""
  var AccountHolder_ScanStatus : String = ""
  var AccountHolder_Fri : String = ""
  var AccountHolder_FromAccountFri : String = ""
  var AccountHolder_Title : String = ""
  var AccountHolder_NumberOfRecords : String = ""
  var AccountHolder_TotalAmount : String = ""
  var SupplementaryData_ChangedFields : String = ""
  var SupplementaryData_Reason : String = ""
  var SupplementaryData_Token : String = ""
  var SupplementaryData_Invitedmsisdn : String = ""
  var SupplementaryData_Handler : String = ""
  var SupplementaryData_Expirydate : String = ""
  var SupplementaryData_ProfileName : String = ""
  var SupplementaryData_DefaultAccountProfile : String = ""
  var SupplementaryData_DefaultAccountType : String = ""
  var SupplementaryData_LoyalityAccountProfile : String = ""
  var SupplementaryData_Identity : String = ""
  var SupplementaryData_ParentAccountHolderId : String = ""
  var SupplementaryData_BankAccount : String = ""
  var SupplementaryData_ClearingNumber : String = ""
  var SupplementaryData_Currency : String = ""
  var SupplementaryData_Recruiter : String = ""
  var pstlAdr: String = ""
  var ctznsh: String = ""
  var othrId: String = ""


  override def getName: String = {
    Constants.AUDIT
  }

  override def getFields: Array[String] = {
    val arr: Array[String] = new Array(70)
    arr(0) = INITIATING_USER
    arr(1) = REAL_USER
    arr(2) = LOGGING_TIME
    arr(3) = SESSION_ID.toString
    arr(4) = TRANSACTION_ID.toString
    arr(5) = TRANSACTION_TYPE
    arr(6) = STATUS
    arr(7) = RECORD_VERSION.toString
    arr(8) = ExtensionData
    arr(9) = LocationDataEntries
    arr(10) = AdditionalRatingEntries
    arr(11) = Imsi
    arr(12) = ApprovalInitiatedBy
    arr(13) = ApprovalId.toString
    arr(14) = TagName
    arr(15) = NewTagName
    arr(16) = TransactionPeriod
    arr(17) = StatsticsTagLimit
    arr(18) = TagStatictics
    arr(19) = TagMinMaxBalances
    arr(20) = BlacklistAccountHolder
    arr(21) = RECEIVING_FRI
    arr(22) = SENDING_FRI
    arr(23) = AccountHolder_gvnNm
    arr(24) = AccountHolder_nm
    arr(25) = AccountHolder_gndr
    arr(26) = AccountHolder_lang
    arr(27) = AccountHolder_birthDt
    arr(28) = AccountHolder_prfssn
    arr(29) = AccountHolder_AccountHolderID
    arr(30) = AccountHolder_TaxNumber
    arr(31) = AccountHolder_TaxationCountry
    arr(32) = AccountHolder_TaxRegion
    arr(33) = AccountHolder_AccountHolderSettingName
    arr(34) = AccountHolder_AccountHolderSettingValue
    arr(35) = AccountHolder_HomeChargingRegionName
    arr(36) = AccountHolder_BankDomainName
    arr(37) = AccountHolder_LanguageCode
    arr(38) = AccountHolder_IndividualPerson
    arr(39) = AccountHolder_CommissioningFri
    arr(40) = AccountHolder_Msisdn
    arr(41) = AccountHolder_NormalizedMsisdn
    arr(42) = AccountHolder_TotalNumber
    arr(43) = AccountHolder_BlacklistedNumber
    arr(44) = AccountHolder_SkippedNumber
    arr(45) = AccountHolder_ScanStatus
    arr(46) = AccountHolder_Fri
    arr(47) = AccountHolder_FromAccountFri
    arr(48) = AccountHolder_Title
    arr(49) = AccountHolder_NumberOfRecords
    arr(50) = AccountHolder_TotalAmount
    arr(51) = SupplementaryData_ChangedFields
    arr(52) = SupplementaryData_Reason
    arr(53) = SupplementaryData_Token
    arr(54) = SupplementaryData_Invitedmsisdn
    arr(55) = SupplementaryData_Handler
    arr(56) = SupplementaryData_Expirydate
    arr(57) = SupplementaryData_ProfileName
    arr(58) = SupplementaryData_DefaultAccountProfile
    arr(59) = SupplementaryData_DefaultAccountType
    arr(60) = SupplementaryData_LoyalityAccountProfile
    arr(61) = SupplementaryData_Identity
    arr(62) = SupplementaryData_ParentAccountHolderId
    arr(63) = SupplementaryData_BankAccount
    arr(64) = SupplementaryData_ClearingNumber
    arr(65) = SupplementaryData_Currency
    arr(66) = SupplementaryData_Recruiter
    arr(67) = pstlAdr
    arr(68) = ctznsh
    arr(69) = othrId



    arr
  }
}