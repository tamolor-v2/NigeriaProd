package com.ligadata.parsers


import com.ligadata.dataobject.AudiCls._
import com.ligadata.dataobject.{AuditFeed, Feed}
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.jackson.JsonMethods.parse

object AuditParser extends Parser {
  private implicit val jsonFormats: Formats = DefaultFormats


  override def parseJson(jsonString: String): Feed = {
    try {
      //val audit = parse(jsonString).extract[Audit]
      val auditMap = parse(jsonString).values.asInstanceOf[scala.collection.immutable.Map[String, Any]] //.extract[Map[String, _]]
      val auditFeed = getAuditFeed(auditMap)

      //val auditFeed = getAuditFeed(audit)
      auditFeed
    } catch {
      case ex: Exception => {
        throw new Exception(ex.getMessage, ex)
      }
    }
  }

  private def getAuditFeed(auditMap: Map[String, Any]): AuditFeed = {
    val auditFeed = new AuditFeed

    auditFeed.INITIATING_USER = auditMap.getOrElse("InitiatingUser", "").toString
    auditFeed.REAL_USER = auditMap.getOrElse("RealUser", "").toString
    auditFeed.LOGGING_TIME = auditMap.getOrElse("LoggingTime", "").toString
    auditFeed.SESSION_ID = if (auditMap.contains("SessionId")) auditMap("SessionId").toString else ""
    auditFeed.TRANSACTION_ID = if (auditMap.contains("TransactionId")) auditMap("TransactionId").toString else ""
    auditFeed.TRANSACTION_TYPE = if (auditMap.contains("TransactionType")) auditMap("TransactionType").toString else ""
    auditFeed.STATUS = auditMap.getOrElse("Status", "").toString
    auditFeed.RECORD_VERSION = if (auditMap.contains("RecordVersion")) auditMap("RecordVersion").toString else ""

    val supplementaryDataMap = auditMap.getOrElse("SupplementaryData", Map[String, Any]()).asInstanceOf[Map[String, Any]]
    if (supplementaryDataMap.nonEmpty) setFieldsFromSupplementaryDataMap(auditFeed, supplementaryDataMap)

    val extensionDataMap = auditMap.getOrElse("ExtensionData", Map[String, Any]()).asInstanceOf[Map[String, Any]]
    if (extensionDataMap.nonEmpty) setFieldsFromExtensionDataMap(auditFeed, extensionDataMap)
    auditFeed
  }

  def setFieldsFromSupplementaryDataMap(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    if (auditFeed != null && supplementaryDataMap != null && supplementaryDataMap.nonEmpty) {
      setFieldsFromLocationData(auditFeed, supplementaryDataMap)
      setSupplementaryDataFieldsFromFri(auditFeed, supplementaryDataMap)
      setSupplementaryDataSenderVoucherUser(auditFeed, supplementaryDataMap)
      setSupplementaryDataReceiverVoucherUser(auditFeed, supplementaryDataMap)
      setSupplementaryDataAmount(auditFeed, supplementaryDataMap)
      setSupplementaryDataBatchContext(auditFeed, supplementaryDataMap)  //Added From Here on 20201111
      setSupplementaryDataReceiverRegisteredAsAccountHolder(auditFeed, supplementaryDataMap)
      setSupplementaryDataInstructionType(auditFeed, supplementaryDataMap)
      setSupplementaryDataIncludeOffNet(auditFeed, supplementaryDataMap)
      setSupplementaryDataIncludeSenderCharges(auditFeed, supplementaryDataMap)

      auditFeed.SUPPLEMENTARY_DATA_VOUCHER_ID = if (supplementaryDataMap.contains("VoucherId")) supplementaryDataMap("VoucherId").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_RECEIVER_VOUCHER_USER_ID = if (supplementaryDataMap.contains("ReceiverVoucherUserId")) supplementaryDataMap("ReceiverVoucherUserId").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_RECEIVER_ACCOUNT_HOLDER_ID = if (supplementaryDataMap.contains("ReceiverAccountHolderId")) supplementaryDataMap("ReceiverAccountHolderId").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_BATCH_ID = if (supplementaryDataMap.contains("BatchId")) supplementaryDataMap("BatchId").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_EXTERNAL_BATCH_TRANSACTION_ID = if (supplementaryDataMap.contains("ExternalBatchTransactionId")) supplementaryDataMap("ExternalBatchTransactionId").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_FINANCIAL_TRANSACTION_ID = if (supplementaryDataMap.contains("FinancialTransactionId")) supplementaryDataMap("FinancialTransactionId").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_TRANSFER_TYPE = if (supplementaryDataMap.contains("TransferType")) supplementaryDataMap("TransferType").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_SENDER_FRI = if (supplementaryDataMap.contains("SendingFri")) supplementaryDataMap("SendingFri").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_SENDER_NOTE = if (supplementaryDataMap.contains("SenderNote")) supplementaryDataMap("SenderNote").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_RECEIVING_FRI = if (supplementaryDataMap.contains("ReceivingFri")) supplementaryDataMap("ReceivingFri").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_RECEIVING_MESSAGE = if (supplementaryDataMap.contains("ReceiverMessage")) supplementaryDataMap("ReceiverMessage").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_REFERENCE_ID = if (supplementaryDataMap.contains("ReferenceId")) supplementaryDataMap("ReferenceId").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_PROFILE_ID = if (supplementaryDataMap.contains("ProfileId")) supplementaryDataMap("ProfileId").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_PROFILE_NAME = if (supplementaryDataMap.contains("ProfileName")) supplementaryDataMap("ProfileName").toString else ""
      auditFeed.SUPPLEMENTARY_DATA_CHANGED_FIELDS = if (supplementaryDataMap.contains("ChangedFields")) supplementaryDataMap("ChangedFields").toString else ""
    }
  }

//  def setExtensionDataFailureReason(auditFeed: AuditFeed:, extensionDataMap: Map[String, Any]) = ???

  //Added this Block on 20211201
  def setFieldsFromExtensionDataMap(auditFeed: AuditFeed, extensionDataMap: Map[String, Any]): Unit = {
    if (auditFeed != null && extensionDataMap != null && extensionDataMap.nonEmpty) {

      auditFeed.EXTENSION_DATA_FAILURE_REASON = if (extensionDataMap.contains("failure_reason")) extensionDataMap("failure_reason").toString else ""
    }
  }

  def setSupplementaryDataSenderVoucherUser(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val senderVoucherUserJsonStr: String = if (supplementaryDataMap.contains("SenderVoucherUser")) supplementaryDataMap("SenderVoucherUser").asInstanceOf[String] else ""
    if (auditFeed != null && senderVoucherUserJsonStr != null && senderVoucherUserJsonStr.nonEmpty) {
      //val cleanedJsonStr = senderVoucherUserJsonStr.replace("\\", "")
      val senderVoucherUserMap = parse(senderVoucherUserJsonStr).values.asInstanceOf[scala.collection.immutable.Map[String, String]]
      val msisdn = senderVoucherUserMap.getOrElse("Msisdn", "0").toString
      auditFeed.SUPPLEMENTARY_DATA_SENDER_VOUCHER_USER = msisdn
    }
  }

  //def setSupplementaryDataAmount(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {def setSupplementaryDataAmount(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
  def setSupplementaryDataAmount(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val amount = if (supplementaryDataMap.contains("Amount")) supplementaryDataMap("Amount").toString else "0"
    //val amountMap = if (supplementaryDataMap.contains("Amount")) supplementaryDataMap("Amount").asInstanceOf[Map[String, Any]] else Map[String, Any]()
    if (auditFeed != null) {
      //if (auditFeed != null && amountMap != null && amountMap.nonEmpty) {
      //val amount = amountMap.getOrElse("Amount", "0.00").toString
      auditFeed.SUPPLEMENTARY_DATA_AMOUNT_AMOUNT = amount
    }
  }
  //  }

  def setSupplementaryDataReceiverVoucherUser(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val receiverVoucherUserJsonStr: String = if (supplementaryDataMap.contains("ReceiverVoucherUser")) supplementaryDataMap("ReceiverVoucherUser").asInstanceOf[String] else ""
    if (auditFeed != null && receiverVoucherUserJsonStr != null && receiverVoucherUserJsonStr.nonEmpty) {
      //val cleanedJsonStr = receiverVoucherUserJsonStr.replace("\\", "")
      val receiverVoucherUserMap = parse(receiverVoucherUserJsonStr).values.asInstanceOf[scala.collection.immutable.Map[String, String]]
      auditFeed.SUPPLEMENTARY_DATA_RECEIVER_VOUCHER_USER = receiverVoucherUserMap.getOrElse("Msisdn", "0").toString
      auditFeed.SUPPLEMENTARY_DATA_RECEIVER_VOUCHER_USER_IDENTIFICATION_NUMBER = receiverVoucherUserMap.getOrElse("IdentificationNumber", "").toString
    }
  }

  def setSupplementaryDataFieldsFromFri(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val friMap: Map[String, Any] = if (supplementaryDataMap.contains("Fri")) supplementaryDataMap("Fri").asInstanceOf[Map[String, Any]] else Map[String, Any]()
    if (auditFeed != null && friMap != null && friMap.nonEmpty) {
      if (friMap.contains("Fri")) {
        val fri = friMap("Fri").asInstanceOf[String]
        try {
          if (fri != null && fri.nonEmpty && fri.startsWith("FRI:") && fri.endsWith("/MSISDN"))
            auditFeed.SUPPLEMENTARY_DATA_FRI_FRI = fri.substring(4, fri.indexOf("/MSISDN"))
        } catch {
          case ex: Throwable =>
            auditFeed.SUPPLEMENTARY_DATA_FRI_FRI = "0"
        }
      }
    }
  }

  def setSupplementaryDataBatchContext(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val BatchContextJsonStr = if (supplementaryDataMap.contains("BatchContext")) supplementaryDataMap("BatchContext").toString else ""
    //val amountMap = if (supplementaryDataMap.contains("Amount")) supplementaryDataMap("Amount").asInstanceOf[Map[String, Any]] else Map[String, Any]()
    if (auditFeed != null) {
      //if (auditFeed != null && amountMap != null && amountMap.nonEmpty) {
      //val amount = amountMap.getOrElse("Amount", "0.00").toString
      auditFeed.SUPPLEMENTARY_DATA_BATCH_CONTEXT = BatchContextJsonStr
    }
  }

  def setSupplementaryDataReceiverRegisteredAsAccountHolder(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val RegisteredAsAcctHolderJsonStr = if (supplementaryDataMap.contains("ReceiverRegisteredAsAccountHolder")) supplementaryDataMap("ReceiverRegisteredAsAccountHolder").toString else ""
    if (auditFeed != null) {
      auditFeed.SUPPLEMENTARY_DATA_RECEIVER_REGISTERED_AS_ACCOUNT_HOLDER = RegisteredAsAcctHolderJsonStr
    }
  }

  def setSupplementaryDataInstructionType(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val InstructionTypeJsonStr = if (supplementaryDataMap.contains("InstructionType")) supplementaryDataMap("InstructionType").toString else ""
    if (auditFeed != null) {
      auditFeed.SUPPLEMENTARY_DATA_INSTRUCTION_TYPE = InstructionTypeJsonStr
    }
  }

  def setSupplementaryDataIncludeOffNet(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val IncludeOffNetJsonStr = if (supplementaryDataMap.contains("IncludeOffNet")) supplementaryDataMap("IncludeOffNet").toString else ""
    if (auditFeed != null) {
      auditFeed.SUPPLEMENTARY_DATA_INCLUDE_OFF_NET = IncludeOffNetJsonStr
    }
  }

  def setSupplementaryDataIncludeSenderCharges(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val IncludeSenderChargesJsonStr = if (supplementaryDataMap.contains("IncludeSenderCharges")) supplementaryDataMap("IncludeSenderCharges").toString else ""
    if (auditFeed != null) {
      auditFeed.SUPPLEMENTARY_DATA_INCLUDE_SENDER_CHARGES = IncludeSenderChargesJsonStr
    }
  }

  //Added this Block on 20211201
//  def setSupplementaryDataChangedFields(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
//    val changedFieldsJsonStr = if (supplementaryDataMap.contains("ChangedFields")) supplementaryDataMap("ChangedFields").toString else ""
//    if (auditFeed != null) {
//    if (auditFeed != null && changedFieldsJsonStr != null && changedFieldsJsonStr.nonEmpty) {
//      val changedFields = if (supplementaryDataMap.contains("Amount")) supplementaryDataMap("Amount").toString else "0"
//      val changedFieldsMap = parse(changedFieldsJsonStr).values.asInstanceOf[scala.collection.immutable.Map[String, String]]
//      val className = changedFieldsMap.getOrElse("ClassName", "").toString
//      val fieldName = changedFieldsMap.getOrElse("FieldName", "").toString
//      val valueBefore = changedFieldsMap.getOrElse("ValueBefore", "").toString
//      val valueAfter = changedFieldsMap.getOrElse("ValueAfter", "").toString

//      auditFeed.SUPPLEMENTARY_DATA_CHANGED_FIELDS_CLASS_NAME = className
//      auditFeed.SUPPLEMENTARY_DATA_CHANGED_FIELDS_FIELD_NAME = fieldName
//      auditFeed.SUPPLEMENTARY_DATA_CHANGED_FIELDS_VALUE_BEFORE = valueBefore
//      auditFeed.SUPPLEMENTARY_DATA_CHANGED_FIELDS = changedFieldsJsonStr
//    }
//  }

  def setFieldsFromLocationData(auditFeed: AuditFeed, supplementaryDataMap: Map[String, Any]): Unit = {
    val locationData: Map[String, Any] = if (supplementaryDataMap.contains("LocationData")) supplementaryDataMap("LocationData").asInstanceOf[Map[String, Any]] else Map[String, Any]()
    if (auditFeed != null && locationData != null && locationData.nonEmpty) {
      if (locationData.contains("LocationDataEntries")) {
        val locationDataEntriesList = locationData("LocationDataEntries").asInstanceOf[List[Map[String, String]]]
        setFieldsFromLocationDataEntries(auditFeed, locationDataEntriesList)
      }
    }
  }

  def setFieldsFromLocationDataEntries(auditFeed: AuditFeed, locationDataEntriesList: List[Map[String, String]]): Unit = {
    if (auditFeed != null && locationDataEntriesList != null && locationDataEntriesList.nonEmpty) {


    }
  }


}