package com.ligadata.parsers

import com.ligadata.dataobject.{Feed, SessionFeed}
import com.ligadata.dataobject.SessionCls
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods.parse

object SessionParser extends Parser {
  private implicit val jsonFormats: Formats = DefaultFormats

  override def parseJson(jsonString: String): Feed = {

    try {
      val sessionLog = parse(jsonString).extract[SessionCls.SessionLog]
      val sessionObj = getSessionObj(sessionLog)
      sessionObj
    } catch {
      case ex: Exception => {
        throw new Exception(ex.getMessage)
      }
    }
  }

  private def getSessionObj(session: SessionCls.SessionLog): SessionFeed = {

    val sessionObj = new SessionFeed

    sessionObj.LOG_DATA_ACTION = session.LogData.action
    sessionObj.LOG_DATA_SID = ConversionHelper.getDefaultString(session.LogData.sid)
    sessionObj.LOG_DATA_TIME = session.LogData.time
    sessionObj.LOG_DATA_WHO = session.LogData.who
    sessionObj.LOG_DATA_PROTOCOL = session.LogData.protocol
    sessionObj.LOG_DATA_ADDRESS = session.LogData.address
    sessionObj.LOG_DATA_DETAILS = ConversionHelper.getDefaultString(session.LogData.details)
    sessionObj.LOG_DATA_CLIENT_ADDR_PATH = ConversionHelper.getDefaultString(session.LogData.clientAddressPath)
    sessionObj.SESSION_ID = session.SessionId
    sessionObj
  }
}