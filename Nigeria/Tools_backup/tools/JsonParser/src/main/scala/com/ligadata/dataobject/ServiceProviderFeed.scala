package com.ligadata.dataobject

import com.ligadata.parsers.Constants

class ServiceProviderFeed extends Feed {

  var HTTPSTATUS: Int = _
  var DIRECTION: String = ""
  var TRANSACTIONID: Long = Long.MinValue
  var SESSIONID: Long = Long.MinValue
  var REQUESTID: Long = Long.MinValue
  var LOGDATE: String = ""
  var MESSAGES: String = ""
  var XMLNS_OP:String = ""
  var XMLNS_XS:String = ""
  var XMLNS_NS0:String= ""
  var EXTENSION_INSTITUTIONNAME : String = ""
  var EXTENSION_INSTITUTIONCODE : String = ""
  var EXTENSION_AGENTMSISDN : Long = 0L
  var EXTENSION_ACCOUNTNUMBER : String = ""
  var EXTENSION_CUSTOMERNAME : String = ""
  var PAYMENT_PROVIDERTRANSACTIONID : String = ""
  var PAYMENT_MESSAGE : String = ""
  var PAYMENT_TRANSACTIONID : String = ""
  var PAYMENT_STATUS : String = ""
  var PAYMENT_RECEIVINGFRI : String = ""

  override def getName: String = {
    Constants.SERVICE
  }

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder

    sb.append(HTTPSTATUS).append(Constants.DELIMITER)
      .append(DIRECTION).append(Constants.DELIMITER)
      .append(TRANSACTIONID).append(Constants.DELIMITER)
      .append(SESSIONID).append(Constants.DELIMITER)
      .append(REQUESTID).append(Constants.DELIMITER)
      .append(LOGDATE).append(Constants.DELIMITER)
      .append(MESSAGES).append(Constants.DELIMITER)
      .append(EXTENSION_INSTITUTIONNAME).append(Constants.DELIMITER)
      .append(EXTENSION_INSTITUTIONCODE).append(Constants.DELIMITER)
      .append(EXTENSION_AGENTMSISDN).append(Constants.DELIMITER)
      .append(EXTENSION_ACCOUNTNUMBER).append(Constants.DELIMITER)
      .append(EXTENSION_CUSTOMERNAME).append(Constants.DELIMITER)
      .append(PAYMENT_PROVIDERTRANSACTIONID).append(Constants.DELIMITER)
      .append(XMLNS_NS0).append(Constants.DELIMITER)
      .append(PAYMENT_MESSAGE).append(Constants.DELIMITER)
      .append(PAYMENT_TRANSACTIONID).append(Constants.DELIMITER)
      .append(PAYMENT_STATUS).append(Constants.DELIMITER)
      .append(PAYMENT_RECEIVINGFRI).append(Constants.DELIMITER)

    sb.mkString
  }

  override def getFields: Array[String] = {
    val arr: Array[String] = new Array(18)
    arr(0) = HTTPSTATUS.toString
    arr(1) = DIRECTION
    arr(2) = TRANSACTIONID.toString
    arr(3) = SESSIONID.toString
    arr(4) = REQUESTID.toString
    arr(5) = LOGDATE
    arr(6) = MESSAGES
    arr(7) =  EXTENSION_INSTITUTIONNAME
    arr(8) = EXTENSION_INSTITUTIONCODE
    arr(9) =  EXTENSION_AGENTMSISDN.toString
    arr(10) =  EXTENSION_ACCOUNTNUMBER
    arr(11) = EXTENSION_CUSTOMERNAME
    arr(12) = PAYMENT_PROVIDERTRANSACTIONID
    arr(13) =  XMLNS_NS0
    arr(14) = PAYMENT_MESSAGE
    arr(15) = PAYMENT_TRANSACTIONID
    arr(16) = PAYMENT_STATUS
    arr(17) = PAYMENT_RECEIVINGFRI

    arr
  }
}

object ServiceCls {

  case class ServiceLog(HttpStatus: Int,
                        Direction: String,
                        TransactionId: Long,
                        SessionId: Long,
                        RequestId:Long,
                        LogDate: String,
                        Message: String)
}