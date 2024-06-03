package com.mtn.containers

import org.apache.logging.log4j.LogManager
import collection.mutable.{Map => MuMap}

case class DimVpHsdpLookupRec(ID: String,PRODUCT_NAME: String, PRODUCT_TYPE: String, PARTNER_NAME: String)

object DimVpHsdpLookupProduct extends Container {
  private val LOGGER = LogManager.getLogger(this.getClass.getName)

  private val PRODUCT_TYPE = "product_type"
  private val PRODUCT_NAME = "product_name"
  private val PRODUCT_ID = "product_id"
  private val PARTNER_NAME = "partner_name"


  override def loadContainerMap(): Unit = {
    try {
      val m = MuMap[String, Any]()
      LOGGER.warn("Loading Data from DimMaps container")
      val rdd = rddFactory.getRDD()
      LOGGER.warn(s"DimMaps RDD size : ${rdd.count}")
      val iterator = rdd.iterator
      while (iterator.hasNext) {
        val c = iterator.next()

        val dimRec = DimVpHsdpLookupRec(
          c.getOrElse(PRODUCT_ID, "").asInstanceOf[String],
          c.getOrElse(PRODUCT_NAME, "").asInstanceOf[String],
          c.getOrElse(PRODUCT_TYPE, "").asInstanceOf[String],
          c.getOrElse(PARTNER_NAME, "").asInstanceOf[String]
        )
        m.put(
          c.getOrElse(PRODUCT_ID, "").asInstanceOf[String],dimRec)
//        LOGGER.warn(s" Test Hbase Lookup -Product_ID:${dimRec.PRODUCT_NAME}, -Partner_Name:${dimRec.PARTNER_NAME}")
      }
      map = m.toMap
      LOGGER.warn(s"DimVpHsdpLookupMap was populated with ${map.size} entry")
    } catch {
      case ex: Exception => LOGGER.error("Exception while loading DimVpHsdpLookup into map", ex)
    }
  }


}