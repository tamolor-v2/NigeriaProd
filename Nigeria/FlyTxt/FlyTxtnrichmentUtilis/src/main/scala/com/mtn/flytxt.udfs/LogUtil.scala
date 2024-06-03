package com.mtn.flytxt.udfs

import org.apache.logging.log4j.Logger

object LogUtil {
  private val MAX_ERROR_COUNTER = 20

  def logWarnAndUpdateCounter(logger: Logger, msg: () => String, currentCounter: Int): Int = {
    if (currentCounter >= MAX_ERROR_COUNTER) {
      logger.warn(msg.apply)
      0
    } else
      currentCounter + 1
  }

  def logErrorAndUpdateCounter(logger: Logger, msg: () => String, currentCounter: Int, throwable: Throwable): Int = {
    if (currentCounter >= MAX_ERROR_COUNTER) {
      logger.error(msg.apply, throwable)
      0
    } else
      currentCounter + 1
  }

  def logDebug(logger: Logger, msg: () => String): Unit = if (logger.isDebugEnabled) logger.debug(msg.apply)

}
