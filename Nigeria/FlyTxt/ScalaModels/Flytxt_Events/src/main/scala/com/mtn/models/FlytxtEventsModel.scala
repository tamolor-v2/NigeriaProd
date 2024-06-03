package com.mtn.models

import com.ligadata.KamanjaBase._
import com.ligadata.kamanja.metadata.ModelDef
import com.mtn.containers.ContainersInit
import org.apache.logging.log4j.{LogManager, Logger}
import com.mtn.messages._
import com.mtn.containers._
import com.mtn.flytxt.udfs._
import scala.collection.mutable.{ArrayBuffer => MuArray}
import scala.util.Try
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.ZonedDateTime

class FlyTxtEventsModelFactory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) {
  val logger: Logger = LogManager.getLogger(getClass.getName)

  override def createModelInstance(): ModelInstance = return new FlyTxtEventsModel(this)

  override def getModelName: String = "com.mtn.models.FlyTxtEventsModel"

  override def getVersion: String = "00.01.00"

  override def isModelInstanceReusable(): Boolean = true

}


class FlyTxtEventsModel(factory: ModelInstanceFactory) extends ModelInstance(factory) {
  lazy val logger: Logger = factory.asInstanceOf[FlyTxtEventsModelFactory].logger

  override def init(instanceMetadata: String): Unit = {
	logger.info("Initiate model ... ")
    ContainersInit.Init(vp_hsdp_lookup, vp_hsdp_lookup.asInstanceOf[RDDObject[ContainerInterface]])
    ContainersInit.Init(MNP_PORTING_BROADCAST, MNP_PORTING_BROADCAST.asInstanceOf[RDDObject[ContainerInterface]])
  }


  override def execute(txnCtxt: TransactionContext, execMsgsSet: Array[ContainerOrConcept], triggerdSetIndex: Int, outputDefault: Boolean): Array[ContainerOrConcept] = {
    var c=0
    if (execMsgsSet.length != 1) {
      logger.error("Expecting only CCN_CDR as input message")
      throw new Exception("Expecting only CCN_CDR as input message")
    }
    val finalOutput = MuArray[ContainerOrConcept]()
    if (execMsgsSet(0).isInstanceOf[CS6_AIR_CDR]) {
//      logger.warn("FlyTxtEventsModel triggered by CS6_AIR_CDR")
      c +=1
      val airRefillMaInputMessage: CS6_AIR_CDR = execMsgsSet(0).asInstanceOf[CS6_AIR_CDR]
      if (airRefillMaInputMessage == null) {
        logger.error("Expecting only CS6_AIR_CDR as input message")
        throw new Exception("Expecting only CS6_AIR_CDR as input message")
      }
       val CM_REFILL_EVENT_MESSAGE = getCmRefillEvents(airRefillMaInputMessage)
       finalOutput ++= CM_REFILL_EVENT_MESSAGE
    }

    if (execMsgsSet(0).isInstanceOf[CS6_CCN_CDR]) {
//      logger.warn("FlyTxtEventsModel triggered by CS6_CCN_CDR")
      c +=1
      val cs6CcnCdrInputMessage: CS6_CCN_CDR = execMsgsSet(0).asInstanceOf[CS6_CCN_CDR]
      if (cs6CcnCdrInputMessage == null) {
        logger.error("Expecting only CS6_CCN_CDR as input message")
        throw new Exception("Expecting only CS6_CCN_CDR as input message")
      }
 
       val CM_RBT_EVENT_MESSAGE = getCmRbtEvents(cs6CcnCdrInputMessage)
       val CM_PACK_SUB_EVENT_FLYTXT_MESSAGE = getCmPackSubEvents(cs6CcnCdrInputMessage)
       val CM_VAS_ENENT_MESSAGE = getCmVasEvents(cs6CcnCdrInputMessage)
       val FLYTXT_DATA_BALANCE_MESSAGE = getFlytxtDataBalance(cs6CcnCdrInputMessage)
       val FLYTXT_MAIN_ACCT_BALANCE_MESSAGE = getFlytxtMainAcctBal(cs6CcnCdrInputMessage)

       finalOutput ++= CM_PACK_SUB_EVENT_FLYTXT_MESSAGE
       finalOutput ++= CM_RBT_EVENT_MESSAGE
       finalOutput ++= CM_VAS_ENENT_MESSAGE
       finalOutput ++= FLYTXT_DATA_BALANCE_MESSAGE
       finalOutput ++= FLYTXT_MAIN_ACCT_BALANCE_MESSAGE
    }

    if (execMsgsSet(0).isInstanceOf[MSC_DAAS]) {
      val MScDaasInputMessage: MSC_DAAS = execMsgsSet(0).asInstanceOf[MSC_DAAS]
      if (MScDaasInputMessage == null) {
        logger.error("Expecting only MSC_DAAS as input message")
        throw new Exception("Expecting only MSC_DAAS as input message")
      }
//      logger.info("Received msg => " + MScDaasInputMessage)
      val FLYTXT_INCOMING_VOICE_USAGE_MESSAGE = getFlytxtIncomingVoice(MScDaasInputMessage)

      finalOutput ++= FLYTXT_INCOMING_VOICE_USAGE_MESSAGE
    }

    if (execMsgsSet(0).isInstanceOf[HSDP_CDR]) {
      val HSDPCDRInputMessage: HSDP_CDR = execMsgsSet(0).asInstanceOf[HSDP_CDR]
      if (HSDPCDRInputMessage == null) {
        logger.error("Expecting only HSDP_CDR as input message")
        throw new Exception("Expecting only HSDP_CDR as input message")
      }

      val FLYTXT_DATA_BUNDLE_EXPIRY_MESSAGE = getDataBundleExpiry(HSDPCDRInputMessage)
      finalOutput ++= FLYTXT_DATA_BUNDLE_EXPIRY_MESSAGE
    }

//    logger.warn("FlyTxtEventsModel is triggered. c= "+ c+ " finalOutput:"+ finalOutput.length)

    finalOutput.toArray
  }

  /**
    *
    * @param inputMsg
    * @return
    */
  private def getCmRefillEvents(inputMsg: CS6_AIR_CDR): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = cm_refill_event.createInstance()
    try {
	  if(inputMsg.cdr_type_main == "RR") {
      val origin_date = FlyTxtUDFs.getDate(inputMsg.original_timestamp_enrich)
      msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
      msg.tbl_dt = if(inputMsg.date_key == 0) 0 else inputMsg.date_key
      msg.original_timestamp = if(origin_date == null || origin_date.length == 0) "0" else origin_date
      msg.type_of_recharge =
        if (inputMsg.originnodetype.toUpperCase.equals("UGW"))
          1
        else if(inputMsg.originnodetype.toUpperCase.equals("SM") && inputMsg.originhostname.toUpperCase.equals("SMS"))
          2
        else if(inputMsg.originnodetype.toUpperCase.equals("ECW") || (inputMsg.originnodetype.toUpperCase.equals("EXT") && inputMsg.originhostname.toUpperCase.contains("VTM")))
          3
        else
          -1
      msg.value_of_recharge = if (inputMsg.transactionamount.toDouble == 0.00) -1 else inputMsg.transactionamount
      msg.balance_before_recharge = if(inputMsg.accountinformationbeforerefill == null || inputMsg.accountinformationbeforerefill.trim.length == 0 || inputMsg.accountinformationbeforerefill.split("~")(1) == null ) -1 else inputMsg.accountinformationbeforerefill.split("~")(1).toDouble
      msg.balance_after_recharge = if(inputMsg.accountinformationafterrefill == null || inputMsg.accountinformationafterrefill.trim.length == 0 || inputMsg.accountinformationafterrefill.split("~")(1) == null ) -1 else inputMsg.accountinformationafterrefill.split("~")(1).toDouble
      msg.counter = if(inputMsg.transactionamount.toDouble > 0 ) 1 else 0
      msgs += msg
     } 
	} catch {
      case ex: Throwable => {
//        logger.error(s"error while prepare CM_REFILL_EVENT message", ex)
	  ""
      }
    }
    msgs.toArray
  }


  /**
    *
    * @param inputMsg
    * @return
    */
  private def getCmVasEvents(inputMsg: CS6_CCN_CDR): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = cm_vas_event.createInstance()
    try {

      val product_name = FlyTxtUDFs.getProductName(inputMsg.vas_productid, "product")
      
      val balBefore = if(inputMsg.das != null && inputMsg.das.length>0 && inputMsg.das.contains("#")) inputMsg.das.split("#")(1).split("~")(1) else ""
      val balAfter  = if(inputMsg.das != null && inputMsg.das.length>0 && inputMsg.das.contains("#")) inputMsg.das.split("#")(1).split("~")(2) else ""   

      val vascodeExcludedFlag = FlyTxtUDFs.isExcludedVaas(inputMsg.vas_productid)
      
      val serviceIdPartner_name =  if(inputMsg.vas_productid != null || inputMsg.vas_productid.trim.length > 0) FlyTxtUDFs.getPartnerName(inputMsg.vas_productid, "service") else ""
      val productIdPartner_name =  if(inputMsg.vas_productid != null || inputMsg.vas_productid.trim.length > 0) FlyTxtUDFs.getPartnerName(inputMsg.vas_productid, "product") else ""
      val partner_name =
        if(inputMsg.vas_productid != null || inputMsg.vas_productid.trim.length > 0){
          if(productIdPartner_name == null || productIdPartner_name.length == 0 )
            serviceIdPartner_name
          else
            productIdPartner_name
        } else {
          inputMsg.vas_productid
        }

      val isIncludedProduct = FlyTxtUDFs.isIncludedProduct(partner_name)
      if((inputMsg.service_type_enrich_desc.contains("VAS") ||inputMsg.service_type_enrich_desc.contains("UNKNOWN")) && isIncludedProduct && vascodeExcludedFlag) {
        val origin_date = FlyTxtUDFs.getDate(inputMsg.original_timestamp_enrich)
        msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
        msg.tbl_dt = if(inputMsg.date_key == 0) 0 else inputMsg.date_key
        msg.original_timestamp = if(origin_date == null || origin_date.length == 0) "0" else origin_date
        msg.value_of_pack = if(inputMsg.totalcharge_money == null || inputMsg.totalcharge_money.trim.length == 0 ) "" else inputMsg.totalcharge_money
        msg.name_of_pack = if(product_name == null || product_name.trim.length == 0) "" else product_name
        msg.vas_event_type_identifier =
          if (inputMsg.scapv2_spi1 != null && inputMsg.scapv2_spi1.length > 0)
            if (inputMsg.scapv2_spi1.toUpperCase.contains("SYSTEM-AUTORENEW")) 3 else 1
          else
            1
        msg.validity =
          if (product_name.toUpperCase.contains("WEEKLY"))
            7
          else if (product_name.toUpperCase.contains("MONTHLY"))
            30
          else
            1
        msg.main_balance_before_event = if(balBefore.trim.length == 0) "" else balBefore
        msg.main_balance_after_event = if(balAfter.trim.length == 0) "" else balAfter
        msg.product_id = if(inputMsg.vas_productid == null || inputMsg.vas_productid.trim.length == 0) "" else inputMsg.vas_productid
        msgs += msg
      }
    } catch {
      case ex: Throwable => {
//        logger.error(s"error while prepare CM_VAS_EVENT message", ex)
	  ""
      }
    }
    msgs.toArray
  }

  /**
    *
    * @param inputMsg
    * @return
    */
  private def getCmRbtEvents(inputMsg: CS6_CCN_CDR): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = cm_rbt_event.createInstance()  // fix it to use output message not input
    try {
      if(inputMsg.vas_originating_services == "SM" && (inputMsg.service_type_enrich_desc.contains("VAS") ||inputMsg.service_type_enrich_desc.contains("UNKNOWN")) && !FlyTxtUDFs.isInMnpContianer(inputMsg.msisdn_key)) {
        val origin_date = FlyTxtUDFs.getDate(inputMsg.original_timestamp_enrich)
        val balBefore = if(inputMsg.das != null && inputMsg.das.length>0 && inputMsg.das.contains("#")) inputMsg.das.split("#")(1).split("~")(1) else ""
        val balAfter  = if(inputMsg.das != null && inputMsg.das.length>0 && inputMsg.das.contains("#")) inputMsg.das.split("#")(1).split("~")(2) else ""
        msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
        msg.tbl_dt = if(inputMsg.date_key == 0) 0 else inputMsg.date_key
        msg.original_timestamp = if(origin_date == null || origin_date.length == 0) "0" else origin_date
        msg.value_of_pack = if(inputMsg.totalcharge_money == null || inputMsg.totalcharge_money.trim.length == 0) "" else inputMsg.totalcharge_money
        msg.name_of_pack =
          if (inputMsg.scapv2_spi1 != null && inputMsg.scapv2_spi1.length > 0)
            if(inputMsg.scapv2_spi1.toUpperCase.equals("MONTHFEE"))
              "DEFAULT TUNE"
            else
              inputMsg.subscriptiontype
          else
            ""
        msg.vas_event_type_identifier =
          if (inputMsg.scapv2_spi1 != null && inputMsg.scapv2_spi1.length > 0)
            if (inputMsg.scapv2_spi1.toUpperCase.contains("SYSTEM-AUTORENEW")) 3 else 1
          else
            1
        msg.vas_productname = inputMsg.vas_productname
        msg.validity =
          if (inputMsg.scapv2_spi1 != null && inputMsg.scapv2_spi1.length > 0 && inputMsg.vas_16777417 != null && inputMsg.vas_16777417.length > 0 && inputMsg.totalcharge_money != null && Try(inputMsg.totalcharge_money.toDouble).isSuccess)
            if (inputMsg.subscriptiontype == "MONTHFEE")
              30
            else if (inputMsg.vas_16777417 == "TUNE DOWNLOAD" && inputMsg.totalcharge_money.toDouble >= 50.0)
              30
            else if (inputMsg.vas_16777417 == "TUNE DOWNLOAD" && inputMsg.totalcharge_money.toDouble < 50.0)
              7
            else if (inputMsg.vas_16777417 == "BOX DOWNLOAD" && inputMsg.totalcharge_money.toDouble >= 50.0)
              30
            else if (inputMsg.vas_16777417 == "BOX DOWNLOAD" && inputMsg.totalcharge_money.toDouble < 50.0)
              7
            else
              1
          else
            1
        msg.main_balance_before_event = if(balBefore.trim.length == 0) "" else balBefore
        msg.main_balance_after_event = if(balAfter.trim.length == 0) "" else balAfter
        msg.product_id = if(inputMsg.vas_productid == null || inputMsg.vas_productid.trim.length == 0) "" else inputMsg.vas_productid
        msgs += msg
      }
    } catch {
      case ex: Throwable => {
//        logger.error(s"error while prepare CM_RBT_EVENT message", ex)
	  ""
      }
    }
    msgs.toArray
  }
  /**
    *
    * @param inputMsg
    * @return
    */
  private def getCmPackSubEvents(inputMsg: CS6_CCN_CDR): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
//    val msg = cm_pack_sub_event.createInstance()  
    val msg = cm_pack_sub_event.createInstance()
//    val msg = CM_PACK_SUB_EVENT.createInstance()
    try {
      val balBefore = if(inputMsg.das != null && inputMsg.das.length>0 && inputMsg.das.contains("#")) inputMsg.das.split("#")(1).split("~")(1) else ""
      val balAfter  = if(inputMsg.das != null && inputMsg.das.length>0 && inputMsg.das.contains("#")) inputMsg.das.split("#")(1).split("~")(2) else ""
      val serviceIdProduct_name =  if(inputMsg.vas_productid != null || inputMsg.vas_productid.trim.length > 0) FlyTxtUDFs.getProductName(inputMsg.vas_productid, "service") else ""
      val productIdProduct_name =  if(inputMsg.vas_productid != null || inputMsg.vas_productid.trim.length > 0) FlyTxtUDFs.getProductName(inputMsg.vas_productid, "product") else ""
      val product_name =
        if(inputMsg.vas_productid != null || inputMsg.vas_productid.trim.length > 0) {
          if (productIdProduct_name == null || productIdProduct_name.length == 0)
            serviceIdProduct_name
          else
            productIdProduct_name
        } else {
          inputMsg.vas_productid
        }
      val serviceIdPartner_name =  if(inputMsg.vas_productid != null || inputMsg.vas_productid.trim.length > 0) FlyTxtUDFs.getPartnerName(inputMsg.vas_productid, "service") else ""// fix getPartnerName
      val productIdPartner_name =  if(inputMsg.vas_productid != null || inputMsg.vas_productid.trim.length > 0) FlyTxtUDFs.getPartnerName(inputMsg.vas_productid, "product") else ""// fix getPartnerName
      val partner_name =
        if(inputMsg.vas_productid != null || inputMsg.vas_productid.trim.length > 0){
          if(productIdPartner_name == null || productIdPartner_name.length == 0 )
            serviceIdPartner_name
          else
            productIdPartner_name
        } else {
          inputMsg.vas_productid
        }
      val isIncludedProduct = FlyTxtUDFs.isIncludedProduct(partner_name)
      val origin_date = FlyTxtUDFs.getDate(inputMsg.original_timestamp_enrich)
      if( (inputMsg.service_type_enrich_desc.toUpperCase.contains("VAS") || inputMsg.service_type_enrich_desc.toUpperCase.contains("UNKNOWN")) && (inputMsg.scapv2_spi1.toUpperCase.contains("CIS") ||  inputMsg.scapv2_spi1.toUpperCase.contains("SM")) && isIncludedProduct) {
        msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
        msg.tbl_dt = if(inputMsg.date_key == 0) 0 else inputMsg.date_key
        msg.original_timestamp_enrich = if(origin_date == null || origin_date.length == 0) "0" else origin_date
        msg.value_of_pack =
          if(inputMsg.vas_3ppchargeamount.toDouble > 0.00)
            inputMsg.vas_3ppchargeamount.toDouble
          else
            (inputMsg.ma_amount_used + inputMsg.amount_das_enrich)
        msg.name_of_pack = if(product_name == null || product_name.trim.length == 0) "" else product_name
        msg.vas_event_type_identifier =
          if (inputMsg.scapv2_spi1 != null && inputMsg.scapv2_spi1.length > 0)
            if (inputMsg.scapv2_spi1.toUpperCase.contains("SYSTEM-AUTORENEW")) 3 else 1
          else
            1
        msg.validity =
          if (product_name == null && product_name.length == 0)
            if(inputMsg.scapv2_spi1.toUpperCase.contains("WEEKLY"))
              7
            else if(inputMsg.scapv2_spi1.toUpperCase.contains("MONTHLY"))
              30
            else
              1
          else if(product_name.toUpperCase.contains("WEEKLY"))
            7
          else if(product_name.toUpperCase.contains("MONTHLY"))
            30
          else
            1
        msg.main_balance_before_event = if(balBefore.trim.length == 0) "" else balBefore 
        msg.main_balance_after_event = if(balAfter.trim.length == 0) "" else balAfter   
        msg.product_id = if(inputMsg.vas_productid == null || inputMsg.vas_productid.trim.length == 0) "" else inputMsg.vas_productid
        msgs += msg
      } 

    } catch {
      case ex: Throwable => {
//        logger.error(s"error while prepare CM_PACK_SUB_EVENT_FLYTXT message", ex)
	  ""
      }
    }
    msgs.toArray
  }

  /**
   *
   * @param inputMsg
   * @return
   */

  private def getFlytxtDataBalance(inputMsg: CS6_CCN_CDR): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = FLYTXT_DATA_BALANCE.createInstance()
    try {
      {
        val dataBalance = inputMsg.das.split("#")(1).split("~")(2)
        if ((((dataBalance.toDouble/0.0005)/1024) > 0 && ((dataBalance.toDouble/0.0005)/1024) <= 100  && dataBalance != null) && inputMsg.servicetype.toUpperCase.equals("GPRS")) {
        msg.msisdn_key = if (inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
        msg.data_balance_mb = if (dataBalance == null || dataBalance.length == 0)
          0.0
        else
          (dataBalance.toDouble / 0.0005) / 1024
        msg.band_type = if ((dataBalance.toDouble / 0.0005) / 1024 > 0.00 && (dataBalance.toDouble / 0.0005) / 1024 <= 20.00) ">0 - <=20MB" else if ((dataBalance.toDouble / 0.0005) / 1024 > 20.00 && (dataBalance.toDouble / 0.0005) / 1024 <= 50.00) ">20MB - <=50MB"  else ">50MB - <=100MB"
        msg.original_timestamp_enrich = if (inputMsg.original_timestamp_enrich == null || inputMsg.original_timestamp_enrich.length == 0) "0" else inputMsg.original_timestamp_enrich
        msg.tbl_dt = if (inputMsg.date_key == 0) 0 else inputMsg.date_key
        msgs += msg
      }
      }
    } catch {
      case ex: Throwable => {
//        logger.error(s"error while prepare FLYTXT_DATA_BALANCE message", ex)
	  ""
      }
    }
    msgs.toArray
  }

  /**
   *
   * @param inputMsg
   * @return
   */

  private def getFlytxtMainAcctBal(inputMsg: CS6_CCN_CDR): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = FLYTXT_MAIN_ACCT_BALANCE.createInstance()
    try {
      {
        val mainBalance = inputMsg.das.split("#")(1).split("~")(2).toDouble
        val balanceFilter = inputMsg.das.split("#")(1).split("~")(3)
        if(balanceFilter.toDouble > 0.0 && ((mainBalance > 0.0 && mainBalance <= 700) || (mainBalance >= 1000 && mainBalance <= 2500) || (mainBalance >= 5000 && mainBalance <= 10000)) && (inputMsg.servicetype.toUpperCase.equals("VOICE") || inputMsg.servicetype.toUpperCase.equals("SMS") || inputMsg.servicetype.toUpperCase.equals("GPRS"))) {
          msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
          msg.chargeable_event_type = if(inputMsg.locationmccmnc!="62130") inputMsg.service_type_enrich.concat(" ").concat("ROAMING") else inputMsg.service_type_enrich.concat(" ").concat(inputMsg.network_type_enrich_desc)  
	  msg.band_type = if(mainBalance > 0 && mainBalance <= 50) ">N0 - <=N50" else if (mainBalance > 50 && mainBalance <= 100) ">N50 - <=N100" else if (mainBalance > 100 && mainBalance <= 200) ">N100 - <=N200" else if (mainBalance > 500 && mainBalance <= 700) ">N500 - <=N700" else if (mainBalance > 1000 && mainBalance <= 2500) ">N1000 - <=N2500" else ">=N5000 - <=N10000"
          msg.balance = mainBalance
          msg.original_timestamp_enrich = if(inputMsg.original_timestamp_enrich == null || inputMsg.original_timestamp_enrich.length == 0) "0" else inputMsg.original_timestamp_enrich
          msg.tbl_dt = if (inputMsg.date_key == 0) 0 else inputMsg.date_key
          msgs += msg
        }
      }
    } catch {
      case ex: Throwable => {
//        logger.error(s"error while prepare FLYTXT_MAIN_ACCT_BALANCE message", ex)
	  ""
      }
    }
    msgs.toArray
  }

  /**
   *
   * @param inputMsg
   * @return
   */

  private def getFlytxtIncomingVoice(inputMsg: MSC_DAAS): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = FLYTXT_INCOMING_VOICE_USAGE.createInstance()
    try {
      { 
	val original_timestamp = getDateKey(inputMsg.original_timestamp_enrich)
        if ((inputMsg.duration.toDouble >= 60.00) && inputMsg.call_type == "MT") {
//        val original_timestamp = getDateTime(inputMsg.original_timestamp_enrich)
        msg.msisdn_key = if (inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
        msg.original_timestamp_enrich = if(original_timestamp == null || original_timestamp.length == 0) "0" else original_timestamp
        msg.call_type = if (inputMsg.on_net_ind == "Y") "OnNet" else if(inputMsg.on_net_ind != "Y" && inputMsg.standarized_terminating.contains("234")) "OffNet" else if (inputMsg.on_net_ind != "Y" && !inputMsg.standarized_terminating.contains("234")) "International" else "Others"
        msg.mou = (inputMsg.duration.toDouble / 60)
        msg.tbl_dt = if (inputMsg.date_key == 0) 0 else inputMsg.date_key
        msgs += msg
      } //else
        //   logger.warn(s"LogError --- getFlytxtIncomingVoice-> Ignoring MSC_DAAS input message -msisdn_key:${inputMsg.msisdn_key},duration:${inputMsg.duration},original_timestamp_enrich:${inputMsg.original_timestamp_enrich}")
      }
    } catch {
      case ex: Throwable => {
//        logger.error(s"error while prepare FLYTXT_INCOMING_VOICE_USAGE message", ex)
	  ""
      }
    }
    msgs.toArray
  }

  private def getDateKey(dateTime: String): String = {
    var dateKey:String = "19700101235959"
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dateKey1 = new SimpleDateFormat("yyyyMMddHHmmss")
      val df_result = df.parse(dateTime)
      dateKey = dateKey1.format(df_result).toString
    return dateKey
  } 


  /**
   *
   * @param inputMsg
   * @return
   */
  private def getDataBundleExpiry(inputMsg: HSDP_CDR): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = FLYTXT_DATA_BUNDLE_EXPIRY.createInstance()
    try {
      { val today_date=LocalDate.now.minusDays(0)
        val today_formatted=today_date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
        val product_name = FlyTxtUDFs.getProductName(inputMsg.productid, "product")
        val _today_formatted= inputMsg.expirydate.slice(0,8)
        if (_today_formatted == today_formatted && !product_name.contains("day"))
        {
          msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
          msg.expirydate = if(inputMsg.expirydate == null || inputMsg.expirydate.length == 0) "0" else inputMsg.expirydate
          msg.original_timestamp_enrich = if(inputMsg.original_timestamp_enrich == null || inputMsg.original_timestamp_enrich.length == 0) "0" else inputMsg.original_timestamp_enrich
          msg.productid = if(inputMsg.productid == null || inputMsg.productid.trim.length == 0 ) "" else inputMsg.productid
          msg.product_name = if (product_name == null || product_name.length == 0) "" else product_name
          msg.tbl_dt = if(inputMsg.date_key == 0) 0 else inputMsg.date_key
          msgs += msg

        } // else
        //        logger.warn(s"Taiye---> Ignoring HSDP_CDR input message=> _today_formatted:${_today_formatted}, today_formatted:${today_formatted},productid:${inputMsg.productid},  product_name:${product_name} ")
      }

    } catch {
      case ex: Throwable => {
//        logger.error(s"error while prepare FLYTXT_DATA_BUNDLE_EXPIRY message", ex)
	  ""
      }
    }
    msgs.toArray
  }

}

