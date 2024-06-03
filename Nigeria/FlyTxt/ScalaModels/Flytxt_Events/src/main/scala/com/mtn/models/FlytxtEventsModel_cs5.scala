package com.mtn.models

import com.ligadata.KamanjaBase._
import com.ligadata.kamanja.metadata.ModelDef
import com.mtn.containers.ContainersInit
import org.apache.logging.log4j.{LogManager, Logger}
import com.mtn.messages._
import com.mtn.containers._
import com.mtn.flytxt.udfs._
import scala.collection.mutable.{ArrayBuffer => MuArray}


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
    if (execMsgsSet(0).isInstanceOf[CS5_AIR_REFILL_MA]) {
      logger.warn("FlyTxtEventsModel triggered by CS5_AIR_REFILL_MA")
      c +=1
      val airRefillMaInputMessage: CS5_AIR_REFILL_MA = execMsgsSet(0).asInstanceOf[CS5_AIR_REFILL_MA]
      if (airRefillMaInputMessage == null) {
        logger.error("Expecting only CS5_AIR_REFILL_MA as input message")
        throw new Exception("Expecting only CS5_AIR_REFILL_MA as input message")
      }
       val CM_REFILL_EVENT_MESSAGE = getCmRefillEvents(airRefillMaInputMessage)
       finalOutput ++= CM_REFILL_EVENT_MESSAGE
    }

    if (execMsgsSet(0).isInstanceOf[CS5_CCN_GPRS_MA]) {
      logger.warn("FlyTxtEventsModel triggered by CS5_CCN_GPRS_MA")
      c +=1
      val cs5GprsMaInputMessage: CS5_CCN_GPRS_MA = execMsgsSet(0).asInstanceOf[CS5_CCN_GPRS_MA]
      if (cs5GprsMaInputMessage == null) {
        logger.error("Expecting only CS5_CCN_GPRS_MA as input message")
        throw new Exception("Expecting only CS5_CCN_GPRS_MA as input message")
      }

       val CM_VAS_ENENT_MESSAGE = getCmVasEvents(cs5GprsMaInputMessage)
       val CM_RBT_EVENT_MESSAGE = getCmRbtEvents(cs5GprsMaInputMessage)
       val CM_PACK_SUB_EVENT_FLYTXT_MESSAGE = getCmPackSubEvents(cs5GprsMaInputMessage)
      finalOutput ++= CM_VAS_ENENT_MESSAGE
      finalOutput ++= CM_RBT_EVENT_MESSAGE
      finalOutput ++= CM_PACK_SUB_EVENT_FLYTXT_MESSAGE
    }

    logger.warn("FlyTxtEventsModel is triggered. c= "+ c+ " finalOutput:"+ finalOutput.length)

    finalOutput.toArray
  }

  /**
    *
    * @param inputMsg
    * @return
    */
  private def getCmRefillEvents(inputMsg: CS5_AIR_REFILL_MA): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = cm_refill_event.createInstance()
    try {
      val origin_date = FlyTxtUDFs.getDate(inputMsg.original_timestamp_enrich)
      msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
      msg.tbl_dt = if(inputMsg.date_key == 0) 0 else inputMsg.date_key
      msg.original_timestamp = if(origin_date == null || origin_date.length == 0) "0" else origin_date
      msg.type_of_recharge =
        if (inputMsg.originnodetype.toUpperCase.equals("UGW"))
          1
        else if(inputMsg.originnodetype.toUpperCase.equals("HWSDP") && inputMsg.originhostname.toUpperCase.equals("SMS"))
          2
        else if(inputMsg.originnodetype.toUpperCase.equals("ECW") || (inputMsg.originnodetype.toUpperCase.equals("EXT") && inputMsg.originhostname.toUpperCase.contains("VTM")))
          3
        else
          -1
      msg.value_of_recharge = if (inputMsg.transaction_amt == 0.00) -1 else inputMsg.transaction_amt
      msg.balance_before_recharge = if(inputMsg.accountbalance == null || inputMsg.accountbalance.trim.length == 0) -1 else inputMsg.accountbalance.toDouble
      msg.balance_after_recharge = if(inputMsg.accountbalance____1 == null || inputMsg.accountbalance____1.trim.length == 0) -1 else inputMsg.accountbalance____1.toDouble
      msg.counter = if(inputMsg.transaction_amt > 0 ) 1 else 0
      msgs += msg
    } catch {
      case ex: Throwable => {
        logger.error(s"error while prepare CM_REFILL_EVENT message", ex)
      }
    }
    msgs.toArray
  }


  /**
    *
    * @param inputMsg
    * @return
    */
  private def getCmVasEvents(inputMsg: CS5_CCN_GPRS_MA): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = cm_vas_event.createInstance()
    try {
      val product_name = FlyTxtUDFs.getProductName(inputMsg.vascode, "product")
      val product_type = FlyTxtUDFs.getProductType(inputMsg.vascode, "product")
      val vascodeExcludedFlag = FlyTxtUDFs.isExcludedVaas(inputMsg.vascode)
      if(inputMsg.chargingcontextid.contains("SCAP") && inputMsg.originatingservices == "HWSDP"
        && inputMsg.recordtype == "20" && vascodeExcludedFlag) {
        val origin_date = FlyTxtUDFs.getDate(inputMsg.original_timestamp_enrich)
        msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
        msg.tbl_dt = if(inputMsg.date_key == 0) 0 else inputMsg.date_key
        msg.original_timestamp = if(origin_date == null || origin_date.length == 0) "0" else origin_date
        msg.value_of_pack = if(inputMsg.costofsession == null || inputMsg.costofsession.trim.length == 0 ) "" else inputMsg.costofsession
        msg.name_of_pack =
          if (product_name == null || product_name.length == 0)
            if(inputMsg.category == null || inputMsg.category.trim.length == 0)
              ""
            else
              inputMsg.category
          else
            product_name
        msg.vas_event_type_identifier =
          if (inputMsg.channel != null && inputMsg.channel.length > 0)
            if (inputMsg.channel.toUpperCase.equals("SYSTEM-AUTORENEW")) 3 else 1
          else
            1
        msg.validity =
          if (product_type == null || product_type.length == 0)
            if (product_name.toUpperCase.contains("WEEKLY"))
              7
            else if (product_name.toUpperCase.contains("MONTHLY"))
              30
            else
              1
          else
            1
        msg.main_balance_before_event = if(inputMsg.amountofsdpbeforesession == null || inputMsg.amountofsdpbeforesession.trim.length == 0) "" else inputMsg.amountofsdpbeforesession
        msg.main_balance_after_event = if(inputMsg.amountonsdpaftersession == null || inputMsg.amountonsdpaftersession.trim.length == 0) "" else inputMsg.amountonsdpaftersession
        msg.product_id = if(inputMsg.vascode == null || inputMsg.vascode.trim.length == 0) "" else inputMsg.vascode
        msgs += msg
      }
    } catch {
      case ex: Throwable => {
        logger.error(s"error while prepare CM_VAS_EVENT message", ex)
      }
    }
    msgs.toArray
  }

  /**
    *
    * @param inputMsg
    * @return
    */
  private def getCmRbtEvents(inputMsg: CS5_CCN_GPRS_MA): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = cm_rbt_event.createInstance()  // fix it to use output message not input
    try {
      if(inputMsg.chargingcontextid.contains("SCAP") && inputMsg.originatingservices == "HWCRBT" && inputMsg.recordtype == "20" && !FlyTxtUDFs.isInMnpContianer(inputMsg.msisdn_key)) {
        val origin_date = FlyTxtUDFs.getDate(inputMsg.original_timestamp_enrich)
        msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
        msg.tbl_dt = if(inputMsg.date_key == 0) 0 else inputMsg.date_key
        msg.original_timestamp = if(origin_date == null || origin_date.length == 0) "0" else origin_date
        msg.value_of_pack = if(inputMsg.costofsession == null || inputMsg.costofsession.trim.length == 0) "" else inputMsg.costofsession
        msg.name_of_pack =
          if (inputMsg.subscriptiontype != null && inputMsg.subscriptiontype.length > 0)
            if(inputMsg.subscriptiontype == "MONTHFEE")
              "DEFAULT TUNE"
            else
              inputMsg.subscriptiontype
          else
            ""
        msg.vas_event_type_identifier =
          if (inputMsg.channel != null && inputMsg.channel.length > 0)
            if (inputMsg.channel.toUpperCase.equals("SYSTEM-AUTORENEW")) 3 else 1
          else
            1
        msg.validity =
          if (inputMsg.subscriptiontype != null && inputMsg.subscriptiontype.length > 0)
            if (inputMsg.subscriptiontype == "MONTHFEE")
              30
            else if (inputMsg.subscriptiontype == "TUNE DOWNLOAD" && inputMsg.costofsession.toDouble >= 50.0)
              30
            else if (inputMsg.subscriptiontype == "TUNE DOWNLOAD" && inputMsg.costofsession.toDouble < 50.0)
              7
            else if (inputMsg.subscriptiontype == "BOX DOWNLOAD" && inputMsg.costofsession.toDouble >= 50.0)
              30
            else if (inputMsg.subscriptiontype == "BOX DOWNLOAD" && inputMsg.costofsession.toDouble < 50.0)
              7
            else
              1
          else
            1
        msg.main_balance_before_event = if(inputMsg.amountofsdpbeforesession == null || inputMsg.amountofsdpbeforesession.trim.length == 0) "" else inputMsg.amountofsdpbeforesession
        msg.main_balance_after_event = if(inputMsg.amountonsdpaftersession == null || inputMsg.amountonsdpaftersession.trim.length == 0) "" else inputMsg.amountonsdpaftersession
        msg.product_id = if(inputMsg.vascode == null || inputMsg.vascode.trim.length == 0) "" else inputMsg.vascode
        msgs += msg
      }
    } catch {
      case ex: Throwable => {
        logger.error(s"error while prepare CM_REFILL_EVENT message", ex)
      }
    }
    msgs.toArray
  }
  /**
    *
    * @param inputMsg
    * @return
    */
  private def getCmPackSubEvents(inputMsg: CS5_CCN_GPRS_MA): Array[ContainerOrConcept] = {
    val msgs: MuArray[ContainerOrConcept] = MuArray()
    val msg = CM_PACK_SUB_EVENT_FLYTXT.createInstance()  // fix it to use output message not input
//    val msg = cm_pack_sub_event_flytxt.createInstance() 
    try {
      val serviceIdProduct_name =  if(inputMsg.vascode != null || inputMsg.vascode.trim.length > 0) FlyTxtUDFs.getProductName(inputMsg.vascode, "service") else ""
      val productIdProduct_name =  if(inputMsg.vascode != null || inputMsg.vascode.trim.length > 0) FlyTxtUDFs.getProductName(inputMsg.vascode, "product") else ""
      //val product_type = FlyTxtUDFs.getProductType(inputMsg.vascode, "product")
      val product_name =
        if(inputMsg.vascode != null || inputMsg.vascode.trim.length > 0) {
          if (productIdProduct_name == null || productIdProduct_name.length == 0)
            serviceIdProduct_name
          else
            productIdProduct_name
        } else {
          inputMsg.vascode
        }
      val serviceIdPartner_name =  if(inputMsg.vascode != null || inputMsg.vascode.trim.length > 0) FlyTxtUDFs.getPartnerName(inputMsg.vascode, "service") else ""// fix getPartnerName
      val productIdPartner_name =  if(inputMsg.vascode != null || inputMsg.vascode.trim.length > 0) FlyTxtUDFs.getPartnerName(inputMsg.vascode, "product") else ""// fix getPartnerName
      val partner_name =
        if(inputMsg.vascode != null || inputMsg.vascode.trim.length > 0){
          if(productIdPartner_name == null || productIdPartner_name.length == 0 )
            serviceIdPartner_name
          else
            productIdPartner_name
        } else {
          inputMsg.vascode
        }

      /// (CASE
      // WHEN (""length""(vascode) > 0)
      // THEN COALESCE(""element_at""(e.map_partner_name, vascode), ""element_at""(e.map_partner_name_sid, vascode))
      // ELSE vascode END) partner_name
      val isIncludedProduct = FlyTxtUDFs.isIncludedProduct(partner_name)
      val origin_date = FlyTxtUDFs.getDate(inputMsg.original_timestamp_enrich)
      if(inputMsg.chargingcontextid.contains("SCAP") && isIncludedProduct) {
        msg.msisdn_key = if(inputMsg.msisdn_key == 0L) 0L else inputMsg.msisdn_key
        msg.tbl_dt = if(inputMsg.date_key == 0) 0 else inputMsg.date_key
        msg.original_timestamp_enrich = if(origin_date == null || origin_date.length == 0) "0" else origin_date
        msg.value_of_pack =
          if(inputMsg.costofsession == null || inputMsg.costofsession.length == 0)
            0.0
          else
            inputMsg.costofsession.toDouble
        msg.name_of_pack = if(product_name == null || product_name.trim.length == 0) "" else product_name
        msg.vas_event_type_identifier =
          if (inputMsg.channel != null && inputMsg.channel.length > 0)
            if (inputMsg.channel.toUpperCase.equals("SYSTEM-AUTORENEW")) 3 else 1
          else
            1
        msg.validity =
          if (product_name == null && product_name.length == 0)
            if(inputMsg.category.contains("WEEKLY"))
              7
            else if(inputMsg.category.contains("MONTHLY"))
              30
            else
              1
          else if(product_name.contains("WEEKLY"))
            7
          else if(product_name.contains("MONTHLY"))
            30
          else
            1
        msg.main_balance_before_event = if(inputMsg.amountofsdpbeforesession == null || inputMsg.amountofsdpbeforesession.trim.length == 0) "" else inputMsg.amountofsdpbeforesession
        msg.main_balance_after_event = if(inputMsg.amountonsdpaftersession == null || inputMsg.amountonsdpaftersession.trim.length == 0) "" else inputMsg.amountonsdpaftersession
        msg.product_id = if(inputMsg.vascode == null || inputMsg.vascode.trim.length == 0) "" else inputMsg.vascode
        msgs += msg
      }
    } catch {
      case ex: Throwable => {
        logger.error(s"error while prepare CM_PACK_SUB_EVENT_FLYTXT message", ex)
      }
    }
    msgs.toArray
  }
}
