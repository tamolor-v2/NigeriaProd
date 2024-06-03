package com.ligadata.dataobject

import com.ligadata.parsers.Constants

class SessionFeed extends Feed {

  var LOG_DATA_ACTION: String = ""
  var LOG_DATA_SID: String = ""
  var LOG_DATA_TIME: String = ""
  var LOG_DATA_WHO: String = ""
  var LOG_DATA_PROTOCOL: String = ""
  var LOG_DATA_ADDRESS: String = ""
  var LOG_DATA_DETAILS: String = ""
  var LOG_DATA_CLIENT_ADDR_PATH: String = ""
  var SESSION_ID: Long = _

  override def getName: String = {
    Constants.SESSION
  }

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder

    sb.append(LOG_DATA_ACTION).append(Constants.DELIMITER)
      .append(LOG_DATA_SID).append(Constants.DELIMITER)
      .append(LOG_DATA_TIME).append(Constants.DELIMITER)
      .append(LOG_DATA_WHO).append(Constants.DELIMITER)
      .append(LOG_DATA_PROTOCOL).append(Constants.DELIMITER)
      .append(LOG_DATA_ADDRESS).append(Constants.DELIMITER)
      .append(LOG_DATA_DETAILS).append(Constants.DELIMITER)
      .append(LOG_DATA_CLIENT_ADDR_PATH).append(Constants.DELIMITER)
      .append(SESSION_ID).append(Constants.DELIMITER)
    sb.mkString

  }

  override def getFields: Array[String] = {
    val arr: Array[String] = new Array(9)
    arr(0) = LOG_DATA_ACTION
    arr(1) = LOG_DATA_SID.toString
    arr(2) = LOG_DATA_TIME
    arr(3) = LOG_DATA_WHO
    arr(4) = LOG_DATA_PROTOCOL
    arr(5) = LOG_DATA_ADDRESS
    arr(6) = LOG_DATA_DETAILS
    arr(7) = LOG_DATA_CLIENT_ADDR_PATH
    arr(8) = SESSION_ID.toString
    arr
  }
}

object SessionCls {

  case class SessionLog(LogData: LogData, SessionId: Long)

  case class LogData(action: String,
                     sid: Option[String] = None,
                     time: String,
                     who: String,
                     protocol: String,
                     address: String,
                     details: Option[String] = None,
                     clientAddressPath: Option[String] = None
                    )

}