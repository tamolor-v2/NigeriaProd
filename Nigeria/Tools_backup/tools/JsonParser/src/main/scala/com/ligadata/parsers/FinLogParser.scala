package com.ligadata.parsers

import com.ligadata.dataobject._
import com.ligadata.dataobject.{FinLogFeed,FinLogCls}
import org.json4s.{DefaultFormats, Formats, JObject, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse}


object FinLogParser extends Parser {
  private implicit val jsonFormats: Formats = DefaultFormats


  def parseJson(jsonString: String): Feed = {
    var obj: JValue = null
    try {
      obj = parse(jsonString).asInstanceOf[JObject]
    } catch {
      case ex: Exception => {
        throw new Exception(ex.getMessage)
      }
    }
    //get the Type to figure out the type of case class to use   -- use a single \ for first match and double \\ for entire tree parsing
    val objType: String = compact(obj \ "Header" \ "Type").replace("\"", "")

    val rec = objType match {
      case "TXN" =>
        val tmp = parse(jsonString).extract[FinLogCls.FinTranLogRcd]
        getFinTranLogRcd(tmp)
      case "QUOTE" | "RESERVATION" | "UPDATE" =>
        val tmp = parse(jsonString).extract[FinLogCls.FinLogOther]
        getFinOther(tmp)
      case _ => throw new Exception("Undefined Type")
    }
    rec
  }


  private def getFinTranLogRcd(txn: FinLogCls.FinTranLogRcd): FinLogFeed = {
    val record = new FinLogFeed
    record.HDR_TYPE = txn.Header.Type
    record.HDR_VER = txn.Header.Ver
    record.INSTRUCT_HDR_TIME = txn.Instruction.Header.Time
    record.INSTRUCT_HDR_TID = txn.Instruction.Header.Tid
    record.INSTRUCT_HDR_FID = txn.Instruction.Header.Fid
    record.INSTRUCT_HDR_UID = txn.Instruction.Header.Uid
    record.INSTRUCT_HDR_QID = ConversionHelper.getDefaultLong(txn.Instruction.Header.Qid)
    record.INSTRUCT_HDR_SID = txn.Instruction.Header.Sid
    record.INSTRUCT_HDR_EXTID = ConversionHelper.getDefaultString(txn.Instruction.Header.Extid)
    record.INSTRUCT_HDR_USER = txn.Instruction.Header.User
    record.INSTRUCT_HDR_REALUSER = txn.Instruction.Header.RealUser
    record.INSTRUCT_HDR_RECORDS = ConversionHelper.getDefaultInt(txn.Instruction.Header.Records)
    record.INSTRUCT_HDR_CONTEXT = ConversionHelper.getDefaultString(txn.Instruction.Header.Context)
    record.INSTRUCT_HDR_BATCH_ID = ConversionHelper.getDefaultLong(txn.Instruction.Header.BatchId)
    record.INSTRUCT_HDR_CASH_USER_ID = ConversionHelper.getDefaultString(txn.Instruction.Header.CashUserId)
    record.STATUS_CODE = txn.Status.Code
    record.STATUS_MSG = ConversionHelper.getDefaultString(txn.Status.Msg)
    record
  }

  private def getFinOther(finLog: FinLogCls.FinLogOther): FinLogFeed = {

    val record = new FinLogFeed
    record.HDR_TYPE = finLog.Header.Type
    record.HDR_VER = finLog.Header.Ver
    record.INSTRUCT_HDR_TYPE = finLog.Instruction.Header.Type
    record.INSTRUCT_HDR_TIME = finLog.Instruction.Header.Time
    record.INSTRUCT_HDR_TID = finLog.Instruction.Header.Tid
    record.INSTRUCT_HDR_FID = finLog.Instruction.Header.Fid
    record.INSTRUCT_HDR_UID = finLog.Instruction.Header.Uid
    record.INSTRUCT_HDR_QID = ConversionHelper.getDefaultLong(finLog.Instruction.Header.Qid)
    record.INSTRUCT_HDR_SID = finLog.Instruction.Header.Sid
    record.INSTRUCT_HDR_EXTID = ConversionHelper.getDefaultString(finLog.Instruction.Header.Extid)
    record.INSTRUCT_HDR_USER = finLog.Instruction.Header.User
    record.INSTRUCT_HDR_REALUSER = finLog.Instruction.Header.RealUser
    record.INSTRUCT_HDR_CONTEXT = ConversionHelper.getDefaultString(finLog.Instruction.Header.Context)
    record.INSTRUCT_HDR_PROVIDER_CATEGORY = ConversionHelper.getDefaultString(finLog.Instruction.Header.ProviderCategory)
    record.INSTRUCT_HDR_BATCH_ID = ConversionHelper.getDefaultLong(finLog.Instruction.Header.BatchId)
    record.INSTRUCT_HDR_OTC_ID = ConversionHelper.getDefaultString(finLog.Instruction.Header.OtcId)
    record.INSTRUCT_AMOUNT = ConversionHelper.getDefaultDouble(finLog.Instruction.Amount)
    record.INSTRUCT_CC = ConversionHelper.getDefaultString(finLog.Instruction.Cc)
    if (finLog.Instruction.From.isDefined) {
      record.INSTRUCT_FROM_FRI = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Fri)
      record.INSTRUCT_FROM_RFRI = ConversionHelper.getDefaultString(finLog.Instruction.From.get.RFri)
      record.INSTRUCT_FROM_SP = ConversionHelper.getDefaultString(finLog.Instruction.From.get.SP)
      record.INSTRUCT_FROM_Off_NET = finLog.Instruction.From.get.OffNet
      record.INSTRUCT_FROM_MESSAGE = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Message)
      record.INSTRUCT_FROM_RATE = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.Rate)
      record.INSTRUCT_FROM_CC = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Cc)
      record.INSTRUCT_FROM_AMOUNT = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.Amount)
      record.INSTRUCT_FROM_IFEE = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.IFee)
      record.INSTRUCT_FROM_FEE_ACT_FRI = ConversionHelper.getDefaultString(finLog.Instruction.From.get.FeeAccountFRI)
      record.INSTRUCT_FROM_BANK_DOMAIN_NAME = ConversionHelper.getDefaultString(finLog.Instruction.From.get.BankDomainName)
      record.INSTRUCT_FROM_VAT = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.VAT)
      record.INSTRUCT_FROM_EXT_FEE = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.EFee)
      record.INSTRUCT_FROM_LOY_FEE = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.LoyFee)
      record.INSTRUCT_FROM_LOY_REWARD = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.LoyReward)
      record.INSTRUCT_FROM_VAT = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.VAT)
      if (finLog.Instruction.From.get.Committed.isDefined) {
        record.INSTRUCT_FROM_BAL_COM_BFR = finLog.Instruction.From.get.Committed.get.Before
        record.INSTRUCT_FROM_BAL_COM_AFT = finLog.Instruction.From.get.Committed.get.After
      }
      if (finLog.Instruction.From.get.Available.isDefined) {
        record.INSTRUCT_FROM_BAL_AVL_AFT = finLog.Instruction.From.get.Available.get.After
        record.INSTRUCT_FROM_BAL_AVL_BFR = finLog.Instruction.From.get.Available.get.Before
      }
      if (finLog.Instruction.From.get.Total.isDefined) {
        record.INSTRUCT_FROM_BAL_TOT_AFT = finLog.Instruction.From.get.Total.get.After
        record.INSTRUCT_FROM_BAL_TOT_BFR = finLog.Instruction.From.get.Total.get.Before
      }
      if (finLog.Instruction.From.get.Loy.isDefined) {
        record.INSTRUCT_FROM_BAL_LOY_AFT = finLog.Instruction.From.get.Loy.get.After
        record.INSTRUCT_FROM_BAL_LOY_BFR = finLog.Instruction.From.get.Loy.get.Before
      }
      record.INSTRUCT_FROM_PROM_AMOUNT = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.Promotion)
      record.INSTRUCT_FROM_DISC_AMOUNT = ConversionHelper.getDefaultDouble(finLog.Instruction.From.get.Discount)
      if (finLog.Instruction.From.get.FRO.isDefined) {
        record.INSTRUCT_FROM_FRO_ID = finLog.Instruction.From.get.FRO.get.Id
        record.INSTRUCT_FROM_FRO_USER_NAME = ConversionHelper.getDefaultString(finLog.Instruction.From.get.FRO.get.Username)
        record.INSTRUCT_FROM_FRO_USER_PRF = finLog.Instruction.From.get.FRO.get.UserProfile
        record.INSTRUCT_FROM_FRO_MSISDN = ConversionHelper.getDefaultString(finLog.Instruction.From.get.FRO.get.MSISDN)
      }
      if (finLog.Instruction.From.get.AccountHolder.isDefined) {
        record.INSTRUCT_FROM_ACCNT_HLDR_ID = finLog.Instruction.From.get.AccountHolder.get.Id
        record.INSTRUCT_FROM_ACCNT_HLDR_USR_NAME = ConversionHelper.getDefaultString(finLog.Instruction.From.get.AccountHolder.get.Username)
        record.INSTRUCT_FROM_ACCNT_HLDR_USR_PRF = finLog.Instruction.From.get.AccountHolder.get.UserProfile
        record.INSTRUCT_FROM_ACCNT_HLDR_MSISDN = ConversionHelper.getDefaultString(finLog.Instruction.From.get.AccountHolder.get.MSISDN)
      }
      if (finLog.Instruction.From.get.Holder.isDefined) {
        record.INSTRUCT_FROM_HLDR_EID = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Eid)
        record.INSTRUCT_FROM_HLDR_FIRST_NAME = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.FirstName)
        record.INSTRUCT_FROM_HLDR_LAST_NAME = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.LastName)
        record.INSTRUCT_FROM_HLDR_NAME_PREFIX = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.NamePrefix)
        record.INSTRUCT_FROM_HLDR_NAME_SUFFIX = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.NameSuffix)
        record.INSTRUCT_FROM_HLDR_GENDER = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Gender)
        record.INSTRUCT_FROM_HLDR_DOB = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Dob)

        if (finLog.Instruction.From.get.Holder.get.Address.isDefined) {
          record.INSTRUCT_FROM_HLDR_ADDR_TYPE = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Address.get.Type)
          record.INSTRUCT_FROM_HLDR_ADDR_LINE = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Address.get.Line)
          record.INSTRUCT_FROM_HLDR_ADDR_STREET = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Address.get.Street)
          record.INSTRUCT_FROM_HLDR_ADDR_NUMBER = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Address.get.Number)
          record.INSTRUCT_FROM_HLDR_ADDR_POSTAL = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Address.get.PostCode)
          record.INSTRUCT_FROM_HLDR_ADDR_TOWN = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Address.get.Town)
          record.INSTRUCT_FROM_HLDR_ADDR_PROVNC = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Address.get.Province)
          record.INSTRUCT_FROM_HLDR_ADDR_CNTRYC = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.Address.get.Cc)
        }
        if (finLog.Instruction.From.get.Holder.get.identifications.isDefined) {
          record.INSTRUCT_FROM_HLDR_ID_NUMBER = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.identifications.get.Number)
          record.INSTRUCT_FROM_HLDR_ID_TYPE = ConversionHelper.getDefaultString(finLog.Instruction.From.get.Holder.get.identifications.get.Type)
        }


      }
      if (finLog.Instruction.From.get.PointOfSale.isDefined) {
        record.INSTRUCT_FROM_POS_OWNER_ID = finLog.Instruction.From.get.PointOfSale.get.OwnerId
        record.INSTRUCT_FROM_POS_NAME = ConversionHelper.getDefaultString(finLog.Instruction.From.get.AccountHolder.get.Username)
        record.INSTRUCT_FROM_POS_MSISDN = ConversionHelper.getDefaultString(finLog.Instruction.From.get.AccountHolder.get.MSISDN)
        record.INSTRUCT_FROM_POS_ID = finLog.Instruction.From.get.AccountHolder.get.Id
      }
      if (finLog.Instruction.From.get.FROParents.isDefined) {
        val froParents: List[FinLogCls.FroParents] = finLog.Instruction.From.get.FROParents.get

        froParents.foreach { f =>
          if (f.Level == 1) {
            record.INSTRUCT_FROM_FROPRTS_LEVEL_1 = f.Level
            record.INSTRUCT_FROM_FROPRTS_ID_1 = f.Id
            record.INSTRUCT_FROM_FROPRTS_USRPRF_1 = f.UserProfile
            record.INSTRUCT_FROM_FROPRTS_FNAME_1 = ConversionHelper.getDefaultString(f.FirstName)
            record.INSTRUCT_FROM_FROPRTS_LNAME_1 = ConversionHelper.getDefaultString(f.LastName)
            record.INSTRUCT_FROM_FROPRTS_CITY_1 = ConversionHelper.getDefaultString(f.City)
            record.INSTRUCT_FROM_FROPRTS_PCODE_1 = ConversionHelper.getDefaultString(f.PostalCode)
            record.INSTRUCT_FROM_FROPRTS_MSISDN_1 = ConversionHelper.getDefaultString(f.MSISDN)
          }
          if (f.Level == 2) {
            record.INSTRUCT_FROM_FROPRTS_LEVEL_2 = f.Level
            record.INSTRUCT_FROM_FROPRTS_ID_2 = f.Id
            record.INSTRUCT_FROM_FROPRTS_USRPRF_2 = f.UserProfile
            record.INSTRUCT_FROM_FROPRTS_FNAME_2 = ConversionHelper.getDefaultString(f.FirstName)
            record.INSTRUCT_FROM_FROPRTS_LNAME_2 = ConversionHelper.getDefaultString(f.LastName)
            record.INSTRUCT_FROM_FROPRTS_CITY_2 = ConversionHelper.getDefaultString(f.City)
            record.INSTRUCT_FROM_FROPRTS_PCODE_2 = ConversionHelper.getDefaultString(f.PostalCode)
            record.INSTRUCT_FROM_FROPRTS_MSISDN_2 = ConversionHelper.getDefaultString(f.MSISDN)
          }
          if (f.Level == 3) {
            record.INSTRUCT_FROM_FROPRTS_LEVEL_3 = f.Level
            record.INSTRUCT_FROM_FROPRTS_ID_3 = f.Id
            record.INSTRUCT_FROM_FROPRTS_USRPRF_3 = f.UserProfile
            record.INSTRUCT_FROM_FROPRTS_FNAME_3 = ConversionHelper.getDefaultString(f.FirstName)
            record.INSTRUCT_FROM_FROPRTS_LNAME_3 = ConversionHelper.getDefaultString(f.LastName)
            record.INSTRUCT_FROM_FROPRTS_CITY_3 = ConversionHelper.getDefaultString(f.City)
            record.INSTRUCT_FROM_FROPRTS_PCODE_3 = ConversionHelper.getDefaultString(f.PostalCode)
            record.INSTRUCT_FROM_FROPRTS_MSISDN_3 = ConversionHelper.getDefaultString(f.MSISDN)
          }
        }
      }
    }
    if (finLog.Instruction.To.isDefined) {
      record.INSTRUCT_TO_FRI = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Fri)
      record.INSTRUCT_TO_RFRI = ConversionHelper.getDefaultString(finLog.Instruction.To.get.RFri)
      record.INSTRUCT_TO_SP = ConversionHelper.getDefaultString(finLog.Instruction.To.get.SP)
      record.INSTRUCT_TO_Off_NET = finLog.Instruction.To.get.OffNet
      record.INSTRUCT_TO_MESSAGE = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Message)
      record.INSTRUCT_TO_RATE = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.Rate)
      record.INSTRUCT_TO_CC = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Cc)
      record.INSTRUCT_TO_AMOUNT = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.Amount)
      record.INSTRUCT_TO_IFEE = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.IFee)
      record.INSTRUCT_TO_FEE_ACT_FRI = ConversionHelper.getDefaultString(finLog.Instruction.To.get.FeeAccountFRI)
      record.INSTRUCT_TO_BANK_DOMAIN_NAME = ConversionHelper.getDefaultString(finLog.Instruction.To.get.BankDomainName)
      record.INSTRUCT_TO_VAT = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.VAT)
      record.INSTRUCT_TO_EXT_FEE = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.EFee)
      record.INSTRUCT_TO_LOY_FEE = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.LoyFee)
      record.INSTRUCT_TO_LOY_REWARD = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.LoyReward)
      record.INSTRUCT_TO_VAT = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.VAT)

      if (finLog.Instruction.To.get.Committed.isDefined) {
        record.INSTRUCT_TO_BAL_COM_BFR = finLog.Instruction.To.get.Committed.get.Before
        record.INSTRUCT_TO_BAL_COM_AFT = finLog.Instruction.To.get.Committed.get.After
      }


      if (finLog.Instruction.To.get.Available.isDefined) {
        record.INSTRUCT_TO_BAL_AVL_BFR = finLog.Instruction.To.get.Available.get.Before
        record.INSTRUCT_TO_BAL_AVL_AFT = finLog.Instruction.To.get.Available.get.After
      }

      if (finLog.Instruction.To.get.Total.isDefined) {
        record.INSTRUCT_TO_BAL_TOT_BFR = finLog.Instruction.To.get.Total.get.Before
        record.INSTRUCT_TO_BAL_TOT_AFT = finLog.Instruction.To.get.Total.get.After
      }

      if (finLog.Instruction.To.get.Loy.isDefined) {
        record.INSTRUCT_TO_BAL_LOY_BFR = finLog.Instruction.To.get.Loy.get.Before
        record.INSTRUCT_TO_BAL_LOY_AFT = finLog.Instruction.To.get.Loy.get.After
      }

      record.INSTRUCT_TO_PROM_AMOUNT = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.Promotion)
      record.INSTRUCT_TO_DISC_AMOUNT = ConversionHelper.getDefaultDouble(finLog.Instruction.To.get.Discount)

      if (finLog.Instruction.To.get.FRO.isDefined) {
        record.INSTRUCT_TO_FRO_ID = finLog.Instruction.To.get.FRO.get.Id
        record.INSTRUCT_TO_FRO_USER_NAME = ConversionHelper.getDefaultString(finLog.Instruction.To.get.FRO.get.Username)
        record.INSTRUCT_TO_FRO_USER_PRF = finLog.Instruction.To.get.FRO.get.UserProfile
        record.INSTRUCT_TO_FRO_MSISDN = ConversionHelper.getDefaultString(finLog.Instruction.To.get.FRO.get.MSISDN)
      }

      if (finLog.Instruction.To.get.AccountHolder.isDefined) {
        record.INSTRUCT_TO_ACCNT_HLDR_ID = finLog.Instruction.To.get.AccountHolder.get.Id
        record.INSTRUCT_TO_ACCNT_HLDR_USR_NAME = ConversionHelper.getDefaultString(finLog.Instruction.To.get.AccountHolder.get.Username)
        record.INSTRUCT_TO_ACCNT_HLDR_USR_PRF = finLog.Instruction.To.get.AccountHolder.get.UserProfile
        record.INSTRUCT_TO_ACCNT_HLDR_MSISDN = ConversionHelper.getDefaultString(finLog.Instruction.To.get.AccountHolder.get.MSISDN)
      }

      if (finLog.Instruction.To.get.Holder.isDefined) {
        record.INSTRUCT_TO_HLDR_EID = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Eid)
        record.INSTRUCT_TO_HLDR_FIRST_NAME = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.FirstName)
        record.INSTRUCT_TO_HLDR_LAST_NAME = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.LastName)
        record.INSTRUCT_TO_HLDR_NAME_PREFIX = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.NamePrefix)
        record.INSTRUCT_TO_HLDR_NAME_SUFFIX = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.NameSuffix)
        record.INSTRUCT_TO_HLDR_GENDER = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Gender)
        record.INSTRUCT_TO_HLDR_DOB = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Dob)

        if (finLog.Instruction.To.get.Holder.get.Address.isDefined) {
          record.INSTRUCT_TO_HLDR_ADDR_TYPE = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Address.get.Type)
          record.INSTRUCT_TO_HLDR_ADDR_LINE = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Address.get.Line)
          record.INSTRUCT_TO_HLDR_ADDR_STREET = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Address.get.Street)
          record.INSTRUCT_TO_HLDR_ADDR_NUMBER = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Address.get.Number)
          record.INSTRUCT_TO_HLDR_ADDR_POSTAL = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Address.get.PostCode)
          record.INSTRUCT_TO_HLDR_ADDR_TOWN = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Address.get.Town)
          record.INSTRUCT_TO_HLDR_ADDR_PROVNC = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Address.get.Province)
          record.INSTRUCT_TO_HLDR_ADDR_CNTRYC = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.Address.get.Cc)
        }
        if (finLog.Instruction.To.get.Holder.get.identifications.isDefined) {
          record.INSTRUCT_TO_HLDR_ID_NUMBER = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.identifications.get.Number)
          record.INSTRUCT_TO_HLDR_ID_TYPE = ConversionHelper.getDefaultString(finLog.Instruction.To.get.Holder.get.identifications.get.Type)
        }

      }

      if (finLog.Instruction.To.get.PointOfSale.isDefined) {
        record.INSTRUCT_TO_POS_OWNER_ID = finLog.Instruction.To.get.PointOfSale.get.OwnerId
        record.INSTRUCT_TO_POS_NAME = ConversionHelper.getDefaultString(finLog.Instruction.To.get.AccountHolder.get.Username)
        record.INSTRUCT_TO_POS_MSISDN = ConversionHelper.getDefaultString(finLog.Instruction.To.get.AccountHolder.get.MSISDN)
        record.INSTRUCT_TO_POS_ID = finLog.Instruction.To.get.AccountHolder.get.Id
      }

      if (finLog.Instruction.To.get.FROParents.isDefined) {
        val froParents: List[FinLogCls.FroParents] = finLog.Instruction.To.get.FROParents.get

        froParents.foreach { f =>
          if (f.Level == 1) {
            record.INSTRUCT_TO_FROPRTS_LEVEL_1 = f.Level
            record.INSTRUCT_TO_FROPRTS_ID_1 = f.Id
            record.INSTRUCT_TO_FROPRTS_USRPRF_1 = f.UserProfile
            record.INSTRUCT_TO_FROPRTS_FNAME_1 = ConversionHelper.getDefaultString(f.FirstName)
            record.INSTRUCT_TO_FROPRTS_LNAME_1 = ConversionHelper.getDefaultString(f.LastName)
            record.INSTRUCT_TO_FROPRTS_CITY_1 = ConversionHelper.getDefaultString(f.City)
            record.INSTRUCT_TO_FROPRTS_PCODE_1 = ConversionHelper.getDefaultString(f.PostalCode)
            record.INSTRUCT_TO_FROPRTS_MSISDN_1 = ConversionHelper.getDefaultString(f.MSISDN)
          }
          if (f.Level == 2) {
            record.INSTRUCT_TO_FROPRTS_LEVEL_2 = f.Level
            record.INSTRUCT_TO_FROPRTS_ID_2 = f.Id
            record.INSTRUCT_TO_FROPRTS_USRPRF_2 = f.UserProfile
            record.INSTRUCT_TO_FROPRTS_FNAME_2 = ConversionHelper.getDefaultString(f.FirstName)
            record.INSTRUCT_TO_FROPRTS_LNAME_2 = ConversionHelper.getDefaultString(f.LastName)
            record.INSTRUCT_TO_FROPRTS_CITY_2 = ConversionHelper.getDefaultString(f.City)
            record.INSTRUCT_TO_FROPRTS_PCODE_2 = ConversionHelper.getDefaultString(f.PostalCode)
            record.INSTRUCT_TO_FROPRTS_MSISDN_2 = ConversionHelper.getDefaultString(f.MSISDN)
          }
          if (f.Level == 3) {
            record.INSTRUCT_TO_FROPRTS_LEVEL_3 = f.Level
            record.INSTRUCT_TO_FROPRTS_ID_3 = f.Id
            record.INSTRUCT_TO_FROPRTS_USRPRF_3 = f.UserProfile
            record.INSTRUCT_TO_FROPRTS_FNAME_3 = ConversionHelper.getDefaultString(f.FirstName)
            record.INSTRUCT_TO_FROPRTS_LNAME_3 = ConversionHelper.getDefaultString(f.LastName)
            record.INSTRUCT_TO_FROPRTS_CITY_3 = ConversionHelper.getDefaultString(f.City)
            record.INSTRUCT_TO_FROPRTS_PCODE_3 = ConversionHelper.getDefaultString(f.PostalCode)
            record.INSTRUCT_TO_FROPRTS_MSISDN_3 = ConversionHelper.getDefaultString(f.MSISDN)
          }
        }
      }
    }
    record.INSTRUCT_HDR_COMMENT = ConversionHelper.getDefaultString(finLog.Instruction.Header.Comment)
    record.STATUS_CODE = finLog.Status.Code
    record.STATUS_MSG = ConversionHelper.getDefaultString(finLog.Status.Msg)
    record
  }


}