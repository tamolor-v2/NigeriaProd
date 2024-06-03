package com.ligadata.parsers


import com.ligadata.dataobject.{PSBAuditFeed, Feed}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}

import scala.util.Try
import org.json4s._
import org.json4s.native.JsonMethods._

///



object PSBAuditParser extends Parser {
  private implicit val jsonFormats: Formats = DefaultFormats



  override def parseJson(jsonString: String ): Feed = {

    try {
      val audit = parse(jsonString)

      val auditFeed = new PSBAuditFeed



      auditFeed.INITIATING_USER =   Try {(audit \ "InitiatingUser" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.REAL_USER =         Try {(audit \ "RealUser"       ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.LOGGING_TIME = Try {(audit \ "LoggingTime" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SESSION_ID =   Try {(audit \ "SessionId" )  .values.toString.toLong  }.getOrElse(0L)
      auditFeed.TRANSACTION_ID = Try {(audit \ "TransactionId" ).values.toString.toLong }.getOrElse(0L)
      auditFeed.TRANSACTION_TYPE = Try {(audit \ "TransactionType" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.STATUS =         Try {(audit \ "Status" ).       values.asInstanceOf[String]}.getOrElse("")
      auditFeed.RECORD_VERSION = Try {(audit \ "RecordVersion" ).values.toString.toInt}.getOrElse(0)
      auditFeed.ExtensionData =  Try { compact(render (audit \"ExtensionData"  )).toString }.getOrElse("")
      auditFeed.LocationDataEntries =  Try { compact(render (audit \"SupplementaryData" \ "LocationData" \ "LocationDataEntries"   )).toString }.getOrElse("")
      auditFeed.AdditionalRatingEntries =  Try { compact(render (audit \"SupplementaryData" \ "AdditionalRating" \ "AdditionalRatingEntries"   )).toString }.getOrElse("")
      auditFeed.Imsi =   Try { compact(render (audit \"SupplementaryData" \ "Imsi" \ "Imsi"   )).toString }.getOrElse("")
      auditFeed.ApprovalInitiatedBy =  Try {(audit \ "SupplementaryData"\ "ApprovalInitiatedBy").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.ApprovalId =  Try {(audit \ "SupplementaryData"\ "ApprovalInitiatedBy").values.toString.toLong}.getOrElse(0L)
      auditFeed.TagName =  Try {(audit \ "SupplementaryData"\ "TagName").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.NewTagName =  Try {(audit \ "SupplementaryData"\ "NewTagName").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.TransactionPeriod =  Try {(audit \ "SupplementaryData"\ "TransactionPeriod").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.StatsticsTagLimit =  Try { compact(render (audit \"SupplementaryData" \ "StatsticsTagLimit"   )).toString }.getOrElse("")
      auditFeed.TagStatictics =  Try { compact(render (audit \"SupplementaryData" \ "TagStatictics"   )).toString }.getOrElse("")
      auditFeed.TagMinMaxBalances =  Try { compact(render (audit \"SupplementaryData" \ "TagMinMaxBalances"   )).toString }.getOrElse("")
      auditFeed.BlacklistAccountHolder =  Try { compact(render (audit \"SupplementaryData" \ "BlacklistAccountHolder"    )).toString }.getOrElse("")
      auditFeed.RECEIVING_FRI =  Try {(audit \ "SupplementaryData" \ "ReceivingFri" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SENDING_FRI =   Try {(audit \ "SupplementaryData" \ "SendingFri" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_gvnNm  =  Try {(audit \ "SupplementaryData" \ "AccountHolder" \ "gvnNm" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_nm  = Try {(audit \ "SupplementaryData" \ "AccountHolder" \ "nm" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_gndr  = Try {(audit \ "SupplementaryData" \ "AccountHolder" \ "gndr" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_lang  =  Try {(audit \ "SupplementaryData" \ "AccountHolder" \ "lang" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_birthDt  =  Try {(audit \ "SupplementaryData" \ "AccountHolder" \ "birthDt" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_prfssn  =  Try {(audit \ "SupplementaryData" \ "AccountHolder" \ "prfssn" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_AccountHolderID  =  Try {(audit \ "SupplementaryData"\ "AccountHolderID").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_TaxNumber  =  Try {(audit \ "SupplementaryData"\ "TaxNumber").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_TaxationCountry  =  Try {(audit \ "SupplementaryData"\ "TaxationCountry").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_TaxRegion  =  Try {(audit \ "SupplementaryData"\ "TaxRegion ").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_AccountHolderSettingName  =  Try {(audit \ "SupplementaryData"\ "AccountHolderSettingName").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_AccountHolderSettingValue  =  Try {(audit \ "SupplementaryData"\ "AccountHolderSettingValue").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_HomeChargingRegionName  =  Try {(audit \ "SupplementaryData"\ "HomeChargingRegionName").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_BankDomainName  =  Try {(audit \ "SupplementaryData"\ "BankDomainName").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_LanguageCode  =  Try {(audit \ "SupplementaryData"\ "LanguageCode").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_IndividualPerson  =  Try {(audit \ "SupplementaryData"\ "IndividualPerson").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_CommissioningFri  = Try {(audit \ "SupplementaryData"\ "CommissioningFri").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_Msisdn  =  Try {(audit \ "SupplementaryData"\ "Msisdn").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_NormalizedMsisdn  = Try {(audit \ "SupplementaryData"\ "NormalizedMsisdn").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_TotalNumber  =  Try {(audit \ "SupplementaryData"\ "TotalNumber").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_BlacklistedNumber  =  Try {(audit \ "SupplementaryData"\ "BlacklistedNumber").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_SkippedNumber  =  Try {(audit \ "SupplementaryData"\ "SkippedNumber").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_ScanStatus  =  Try {(audit \ "SupplementaryData"\ "ScanStatus").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_Fri  =  Try {(audit \ "SupplementaryData"\ "Fri").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_FromAccountFri  =  Try {(audit \ "SupplementaryData"\ "FromAccountFri").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_Title  = Try {(audit \ "SupplementaryData"\ "Title").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_NumberOfRecords  =  Try {(audit \ "SupplementaryData"\ "NumberOfRecords").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.AccountHolder_TotalAmount  =  Try {(audit \ "SupplementaryData"\ "TotalAmount").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_ChangedFields  =  Try { compact(render (audit \ "SupplementaryData" \ "ChangedFields" )).toString }.getOrElse("")
      auditFeed.SupplementaryData_Reason  =    Try {(audit \ "SupplementaryData"\ "Reason").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_Token  =  Try {(audit \ "SupplementaryData"\ "token").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_Invitedmsisdn  =  Try {(audit \ "SupplementaryData"\ "token").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_Handler  =  Try {(audit \ "SupplementaryData"\ "handler").values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_Expirydate  =  Try {(audit \ "SupplementaryData" \"expirydate" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_ProfileName  =  Try {(audit \"SupplementaryData" \ "ProfileName" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_DefaultAccountProfile  =  Try {(audit \ "SupplementaryData" \"DefaultAccountProfile" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_DefaultAccountType  =  Try {(audit \ "SupplementaryData" \"DefaultAccountType" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_LoyalityAccountProfile  =  Try {(audit \ "SupplementaryData" \"LoyalityAccountProfile" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_Identity  =                Try {(audit \"SupplementaryData" \ "Identity" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_ParentAccountHolderId  =  Try {(audit \ "SupplementaryData" \ "AccountHolder" \ "ParentAccountHolderId" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_BankAccount  =    Try {(audit \"SupplementaryData" \ "BankAccount" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_ClearingNumber  =    Try {(audit \"SupplementaryData" \ "ClearingNumber" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_Currency  =   Try {(audit \"SupplementaryData" \ "Currency" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.SupplementaryData_Recruiter  =   Try {(audit \"SupplementaryData" \ "Recruiter" ).values.asInstanceOf[String]}.getOrElse("")
      auditFeed.pstlAdr =  Try { compact(render (audit \"SupplementaryData" \ "AccountHolder" \ "pstlAdr"   )).toString }.getOrElse("")
      auditFeed.ctznsh =  Try { compact(render (audit \"SupplementaryData" \ "AccountHolder" \ "ctznsh"   )).toString }.getOrElse("")
      auditFeed.othrId =  Try { compact(render (audit \"SupplementaryData" \ "AccountHolder" \ "othrId"   )).toString }.getOrElse("")



      auditFeed
    } catch {
      case ex: Exception => {
        throw new Exception(ex.getMessage, ex)
      }
    }


  }





}