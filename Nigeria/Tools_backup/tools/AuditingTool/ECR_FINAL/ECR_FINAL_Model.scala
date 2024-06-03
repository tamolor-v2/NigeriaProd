package com.mtn.models

import java.util

import com.ligadata.KamanjaBase._
import com.ligadata.kamanja.metadata.ModelDef
import com.mtn.messages.{ECR_FINAL, ECR_FINAL_input}
import com.mtn.udfs._
import org.apache.logging.log4j.{LogManager, Logger}

import scala.collection.mutable.ArrayBuffer

class ECR_FINAL_Model_Factory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) {
  override def createModelInstance(): ModelInstance = return new ECR_FINAL_Model(this)
  override def getModelName: String = "com.mtn.models.ECR_FINAL_Model"
  override def getVersion: String = "00.01.00"
  override def isModelInstanceReusable(): Boolean = true
  val logger: Logger = LogManager.getLogger(getClass.getName)
}


class ECR_FINAL_Model (factory: ModelInstanceFactory) extends ModelInstance(factory){
  lazy val logger: Logger = factory.asInstanceOf[ECR_FINAL_Model_Factory].logger

  override def execute(txnCtxt: TransactionContext, execMsgsSet: Array[ContainerOrConcept], triggerdSetIndex: Int, outputDefault: Boolean): Array[ContainerOrConcept] = {

    if (execMsgsSet.size != 1) {
      logger.error("Expecting only ECR_FINAL as input message")
      throw new Exception("Expecting only ECR_FINAL as input message")
    }

    val ECR_FINALInputMessage: ECR_FINAL_input = execMsgsSet(0).asInstanceOf[ECR_FINAL_input]

    if (ECR_FINALInputMessage == null) {
      logger.error("Expecting only ECR_FINAL as input message")
      throw new Exception("Expecting only ECR_FINAL as input message")
    }

    //val finalOutput = ArrayBuffer[ContainerOrConcept]()
    val msg = ECR_FINAL.createInstance()

    msg.job_id = ECR_FINALInputMessage.job_id
    msg.job_name = ECR_FINALInputMessage.job_name
    msg.log_type = ECR_FINALInputMessage.log_type
    msg.run_id = ECR_FINALInputMessage.run_id
    msg.start_time = ECR_FINALInputMessage.start_time
    msg.end_time = ECR_FINALInputMessage.end_time
    msg.status = ECR_FINALInputMessage.status
    msg.message = ECR_FINALInputMessage.message
    msg.affected_records = ECR_FINALInputMessage.affected_records
    msg.data_date  = ECR_FINALInputMessage.data_date 
    msg.from_date  = ECR_FINALInputMessage.from_date 
    msg.to_date  = ECR_FINALInputMessage.to_date 
    msg.step = ECR_FINALInputMessage.step   

   
    msg.date_key = CommonUDFs.getDateKey(ECR_FINALInputMessage.start_time,"yyyy-MM-dd HH:mm:ss.SSS")
    msg.event_timestamp_enrich = CommonUtils.convertToEpochDate(ECR_FINALInputMessage.start_time,"yyyy-MM-dd HH:mm:ss.SSS")
    msg.original_timestamp_enrich = ECR_FINALInputMessage.start_time


    msg.setTimePartitionData()
    
    Array(msg)
  }

}

