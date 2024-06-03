package com.mtn.containers

import org.apache.logging.log4j.LogManager
import collection.mutable.{Map => MuMap}

case class DimMnpPortingBroadcastRec(msisdn_key: Long,donor_id: String, recipient_id: String)

object DimMnpPortingBroadcast extends Container {
  private val LOGGER = LogManager.getLogger(this.getClass.getName)

  private val msisdn_key = "msisdn_key"
  private val donor_id = "donor_id"
  private val recipient_id = "recipient_id"


  override def loadContainerMap(): Unit = {
    try {
      val m = MuMap[String, Any]()
      LOGGER.warn("Loading Data from DimMnpPortingBroadcast container")
      val rdd = rddFactory.getRDD()
      LOGGER.warn(s"DimMnpPortingBroadcast RDD size : ${rdd.count}")
      val iterator = rdd.iterator
      while (iterator.hasNext) {
        val c = iterator.next()

        val dimRec = DimMnpPortingBroadcastRec(
          c.getOrElse(msisdn_key, 0L).asInstanceOf[Long],
          c.getOrElse(donor_id, "").asInstanceOf[String],
          c.getOrElse(recipient_id, "").asInstanceOf[String]
        )
        m.put(
          c.getOrElse(msisdn_key, 0L).toString.asInstanceOf[String],dimRec)
      }
      map = m.toMap
      LOGGER.warn(s"DimMnpPortingBroadcast was populated with ${map.size} entry")
    } catch {
      case ex: Exception => LOGGER.error("Exception while loading DimMnpPortingBroadcast into map", ex)
    }
  }


}