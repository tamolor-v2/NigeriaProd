package com.mtn.containers

import java.util.concurrent.atomic.AtomicInteger

import com.ligadata.KamanjaBase.{ContainerFactoryInterface, ContainerInterface, RDDObject}
import org.apache.logging.log4j.LogManager

abstract class Container {
  private val LOGGER = LogManager.getLogger(this.getClass.getName)

  protected var isInitialized = false
  protected var map = Map[String, Any]()
  protected val mapLock = new Object
  protected val count = new AtomicInteger(0)
  protected var factory: ContainerFactoryInterface = _
  protected var rddFactory: RDDObject[ContainerInterface] = _

  def init(factory: ContainerFactoryInterface, rddFactory: RDDObject[ContainerInterface]) {
    LOGGER.warn(s"Entering Init Method for ${factory.FullName}")
    val cnt = count.get()
    if (cnt == 0) {
      this.synchronized {
        if (count.get() == 0) {
          if (isInitialized)
            return
          mapLock.synchronized {
            if (!isInitialized) {
              this.factory = factory
              this.rddFactory = rddFactory
              loadContainerMap()
              isInitialized = true
            }
          }
          count.incrementAndGet()
        }
      }
    }
  }

  def loadContainerMap(): Unit

  def getMap: Map[String, Any] = map
}

object ContainersInit {
  private val LOGGER = LogManager.getLogger(this.getClass.getName)

  def Init(factory: ContainerFactoryInterface, rddFactory: RDDObject[ContainerInterface]) {
    LOGGER.warn("Entering Init Method: " + factory.FullName)
    if (factory.FullName.compareToIgnoreCase("com.mtn.containers.vp_hsdp_lookup") == 0) {
      DimVpHsdpLookupProduct.init(factory, rddFactory)
      DimVpHsdpLookupService.init(factory, rddFactory)
    } else if (factory.FullName.compareToIgnoreCase("com.mtn.containers.mnp_porting_broadcast") == 0) {
      DimMnpPortingBroadcast.init(factory, rddFactory)
    } else {
      LOGGER.warn(s"container ${factory.FullName} not supported yet.")
    }
  }
}