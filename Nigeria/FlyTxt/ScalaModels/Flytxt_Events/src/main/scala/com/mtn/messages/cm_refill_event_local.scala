package com.mtn.messages;
import org.json4s.jackson.JsonMethods._;
import org.json4s.DefaultFormats;
import org.json4s.Formats;
import com.ligadata.KamanjaBase._;
import com.ligadata.BaseTypes._;
import com.ligadata.Exceptions._;
import org.apache.logging.log4j.{ Logger, LogManager }
import java.util.Date;
import java.io.{ DataInputStream, DataOutputStream, ByteArrayOutputStream }
import java.nio.ByteBuffer
    
 
object cm_refill_event extends RDDObject[cm_refill_event] with MessageFactoryInterface { 
 
  val log = LogManager.getLogger(getClass)
	type T = cm_refill_event ;
	override def getFullTypeName: String = "com.mtn.messages.cm_refill_event"; 
	override def getTypeNameSpace: String = "com.mtn.messages"; 
	override def getTypeName: String = "cm_refill_event"; 
	override def getTypeVersion: String = "000000.000001.000000"; 
	override def getSchemaId: Int = 2005530; 
	private var elementId: Long = 0L; 
	override def setElementId(elemId: Long): Unit = { elementId = elemId; } 
	override def getElementId: Long = elementId; 
	override def getTenantId: String = "tenant1"; 
	override def createInstance: cm_refill_event = new cm_refill_event(cm_refill_event); 
	override def isFixed: Boolean = true; 
	def isCaseSensitive(): Boolean = false; 
	override def getContainerType: ContainerTypes.ContainerType = ContainerTypes.ContainerType.MESSAGE
	override def getFullName = getFullTypeName; 
	override def getRddTenantId = getTenantId; 
	override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this); 

    def build = new T(this)
    def build(from: T) = new T(from)
   override def getPartitionKeyNames: Array[String] = Array("msisdn_key"); 

  override def getPrimaryKeyNames: Array[String] = Array[String](); 
   
  
  override def getTimePartitionInfo: TimePartitionInfo = {
    var timePartitionInfo: TimePartitionInfo = new TimePartitionInfo();
    timePartitionInfo.setFieldName("tbl_dt");
    timePartitionInfo.setFormat("yyyyMMdd");
    timePartitionInfo.setTimePartitionType(TimePartitionInfo.TimePartitionType.DAILY);
    return timePartitionInfo
  }

       
    override def hasPrimaryKey(): Boolean = {
      val pKeys = getPrimaryKeyNames();
      return (pKeys != null && pKeys.length > 0);
    }

    override def hasPartitionKey(): Boolean = {
      val pKeys = getPartitionKeyNames();
      return (pKeys != null && pKeys.length > 0);
    }

    override def hasTimePartitionInfo(): Boolean = {
      val tmInfo = getTimePartitionInfo();
      return (tmInfo != null && tmInfo.getTimePartitionType != TimePartitionInfo.TimePartitionType.NONE);
    }
  
    override def getAvroSchema: String = """{ "type": "record",  "namespace" : "com.mtn.messages" , "name" : "cm_refill_event" , "fields":[{ "name" : "msisdn_key" , "type" : "long"},{ "name" : "tbl_dt" , "type" : "int"},{ "name" : "original_timestamp" , "type" : "string"},{ "name" : "type_of_recharge" , "type" : "int"},{ "name" : "value_of_recharge" , "type" : "double"},{ "name" : "balance_before_recharge" , "type" : "double"},{ "name" : "balance_after_recharge" , "type" : "double"},{ "name" : "counter" , "type" : "int"}]}""";  
  
      var attributeTypes = generateAttributeTypes;
    
    private def generateAttributeTypes(): Array[AttributeTypeInfo] = {
      var attributeTypes = new Array[AttributeTypeInfo](8);
                                                                        		 attributeTypes(0) = new AttributeTypeInfo("msisdn_key", 0, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(1) = new AttributeTypeInfo("tbl_dt", 1, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(2) = new AttributeTypeInfo("original_timestamp", 2, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(3) = new AttributeTypeInfo("type_of_recharge", 3, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(4) = new AttributeTypeInfo("value_of_recharge", 4, AttributeTypeInfo.TypeCategory.DOUBLE, -1, -1, 0)
		 attributeTypes(5) = new AttributeTypeInfo("balance_before_recharge", 5, AttributeTypeInfo.TypeCategory.DOUBLE, -1, -1, 0)
		 attributeTypes(6) = new AttributeTypeInfo("balance_after_recharge", 6, AttributeTypeInfo.TypeCategory.DOUBLE, -1, -1, 0)
		 attributeTypes(7) = new AttributeTypeInfo("counter", 7, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)


      return attributeTypes
    }
    

		 var keyTypes: Map[String, AttributeTypeInfo] = attributeTypes.map { a => (a.getName, a) }.toMap;
				def setFn0(curObj: cm_refill_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.msisdn_key = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field msisdn_key in message cm_refill_event") 
				} 
				def setFn1(curObj: cm_refill_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]){ 
				  curObj.tbl_dt = value.asInstanceOf[Int]; 
				  curObj.setTimePartitionData; 
				} else throw new Exception(s"Value is the not the correct type Int for field tbl_dt in message cm_refill_event") 
				} 
				def setFn2(curObj: cm_refill_event, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.original_timestamp = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field original_timestamp in message cm_refill_event") 
				} 
				def setFn3(curObj: cm_refill_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.type_of_recharge = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field type_of_recharge in message cm_refill_event") 
				} 
				def setFn4(curObj: cm_refill_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Double]) 
				  curObj.value_of_recharge = value.asInstanceOf[Double]; 
				 else throw new Exception(s"Value is the not the correct type Double for field value_of_recharge in message cm_refill_event") 
				} 
				def setFn5(curObj: cm_refill_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Double]) 
				  curObj.balance_before_recharge = value.asInstanceOf[Double]; 
				 else throw new Exception(s"Value is the not the correct type Double for field balance_before_recharge in message cm_refill_event") 
				} 
				def setFn6(curObj: cm_refill_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Double]) 
				  curObj.balance_after_recharge = value.asInstanceOf[Double]; 
				 else throw new Exception(s"Value is the not the correct type Double for field balance_after_recharge in message cm_refill_event") 
				} 
				def setFn7(curObj: cm_refill_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.counter = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field counter in message cm_refill_event") 
				} 

    val setFnArr = Array[(cm_refill_event, Any) => Unit](setFn0,setFn1,setFn2,setFn3,setFn4,setFn5,setFn6,setFn7    )
		def getFn0(curObj: cm_refill_event): AnyRef = curObj.msisdn_key.asInstanceOf[AnyRef]; 
		def getFn1(curObj: cm_refill_event): AnyRef = curObj.tbl_dt.asInstanceOf[AnyRef]; 
		def getFn2(curObj: cm_refill_event): AnyRef = curObj.original_timestamp.asInstanceOf[AnyRef]; 
		def getFn3(curObj: cm_refill_event): AnyRef = curObj.type_of_recharge.asInstanceOf[AnyRef]; 
		def getFn4(curObj: cm_refill_event): AnyRef = curObj.value_of_recharge.asInstanceOf[AnyRef]; 
		def getFn5(curObj: cm_refill_event): AnyRef = curObj.balance_before_recharge.asInstanceOf[AnyRef]; 
		def getFn6(curObj: cm_refill_event): AnyRef = curObj.balance_after_recharge.asInstanceOf[AnyRef]; 
		def getFn7(curObj: cm_refill_event): AnyRef = curObj.counter.asInstanceOf[AnyRef]; 

    val getFnArr = Array[(cm_refill_event) => AnyRef](getFn0,getFn1,getFn2,getFn3,getFn4,getFn5,getFn6,getFn7    )



    override def getRDDObject(): RDDObject[ContainerInterface] = cm_refill_event.asInstanceOf[RDDObject[ContainerInterface]];


    
    final override def convertFrom(srcObj: Any): T = convertFrom(createInstance(), srcObj);
      
    override def convertFrom(newVerObj: Any, oldVerobj: Any): ContainerInterface = {
      try {
        if (oldVerobj == null) return null;
        oldVerobj match {
          
      case oldVerobj: com.mtn.messages.cm_refill_event => { return  convertToVer1000000(oldVerobj); } 
          case _ => {
            throw new Exception("Unhandled Version Found");
          }
        }
      } catch {
        case e: Exception => {
          throw e
        }
      }
      return null;
    }
  
    private def convertToVer1000000(oldVerobj: com.mtn.messages.cm_refill_event): com.mtn.messages.cm_refill_event= {
      return oldVerobj
    }
  
      
  /****   DEPRECATED METHODS ***/
  override def FullName: String = getFullTypeName
  override def NameSpace: String = getTypeNameSpace
  override def Name: String = getTypeName
  override def Version: String = getTypeVersion
  override def CreateNewMessage: BaseMsg= createInstance.asInstanceOf[BaseMsg];
  override def CreateNewContainer: BaseContainer= null;
  override def IsFixed: Boolean = true
  override def IsKv: Boolean = false
  override def CanPersist: Boolean = false
  override def isMessage: Boolean = true
  override def isContainer: Boolean = false
  override def PartitionKeyData(inputdata: InputData): Array[String] = { throw new Exception("Deprecated method PartitionKeyData in obj cm_refill_event") };
  override def PrimaryKeyData(inputdata: InputData): Array[String] = throw new Exception("Deprecated method PrimaryKeyData in obj cm_refill_event");
  override def TimePartitionData(inputdata: InputData): Long = throw new Exception("Deprecated method TimePartitionData in obj cm_refill_event");
 override def NeedToTransformData: Boolean = false
    }

class cm_refill_event(factory: MessageFactoryInterface, other: cm_refill_event) extends MessageInterface(factory) { 
 
  val log = cm_refill_event.log

      var attributeTypes = cm_refill_event.attributeTypes
      
		 var keyTypes = cm_refill_event.keyTypes;
    
     if (other != null && other != this) {
      // call copying fields from other to local variables
      fromFunc(other)
    }
    
    override def save: Unit = { cm_refill_event.saveOne(this) }
  
    def Clone(): ContainerOrConcept = { cm_refill_event.build(this) }

		override def getPartitionKey: Array[String] = {
		var partitionKeys: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]();
		try {
		 partitionKeys += com.ligadata.BaseTypes.LongImpl.toString(get(caseSensitiveKey("msisdn_key")).asInstanceOf[Long]);
		 }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      		 partitionKeys.toArray; 

 		} 
 

		override def getPrimaryKey: Array[String] = Array[String]() 

    override def getAttributeType(name: String): AttributeTypeInfo = {
      if (name == null || name.trim() == "") return null;
      keyTypes.getOrElse(caseSensitiveKey(name), null)
    }
  
  
      var setFnArr = cm_refill_event.setFnArr
    
      var getFnArr = cm_refill_event.getFnArr
    
 		var msisdn_key : Long = _; 
 		var tbl_dt : Int = _; 
 		var original_timestamp : String = _; 
 		var type_of_recharge : Int = _; 
 		var value_of_recharge : Double = _; 
 		var balance_before_recharge : Double = _; 
 		var balance_after_recharge : Double = _; 
 		var counter : Int = _; 

    override def getAttributeTypes(): Array[AttributeTypeInfo] = {
      if (attributeTypes == null) return null;
      return attributeTypes
    }
    
    private def getWithReflection(keyName: String): AnyRef = {
      if(keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name "+keyName);
      val key = caseSensitiveKey(keyName);
      val ru = scala.reflect.runtime.universe
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val im = m.reflect(this)
      val fieldX = ru.typeOf[cm_refill_event].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
      val fmX = im.reflectField(fieldX)
      return fmX.get.asInstanceOf[AnyRef];      
    } 
   
    override def get(key: String): AnyRef = {
    try {
      // Try with reflection
      return getByName(caseSensitiveKey(key))
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        // Call By Name
        return getWithReflection(caseSensitiveKey(key))
        }
      }
    }      
    
    private def getByName(keyName: String): AnyRef = {
     if(keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name "+keyName);
      val key = caseSensitiveKey(keyName);
   
      if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message/container cm_refill_event", null);
      return get(keyTypes(key).getIndex)
  }

    private def getOrElseByName(key: String, defaultVal: Any): AnyRef = {
     if(key == null) throw new Exception("Please provide proper key name "+key);
      if (!keyTypes.contains(key)) {
          if(defaultVal == null) return null;
          return defaultVal.asInstanceOf[AnyRef];
      }
      return get(keyTypes(key).getIndex)
  }

  
    override def getOrElse(keyName: String, defaultVal: Any): AnyRef = { // Return (value)
      if (keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name "+keyName);
      val key = caseSensitiveKey(keyName);
      try {
        return getOrElseByName(key, defaultVal)
       } catch {
        case e: Exception => {
          log.debug("", e)
          if(defaultVal == null) return null;
          return defaultVal.asInstanceOf[AnyRef];
        }
      }
      return null;
    }
   
    override def get(index : Int) : AnyRef = {
      if (index >= 0 && index < getFnArr.length) {
        return getFnArr(index)(this)
      }
      throw new Exception(s"$index is out of range for message cm_refill_event");
    }
    
    override def getOrElse(index: Int, defaultVal: Any): AnyRef = { // Return (value)
      try {
        return get(index);
        } catch {
        case e: Exception => {
          log.debug("", e)
          if(defaultVal == null) return null;
          return defaultVal.asInstanceOf[AnyRef];
        }
      }
      return null;
    }
  
    override def getAttributeNames(): Array[String] = {
      try {
        if (keyTypes.isEmpty) {
          return null;
          } else {
          return keyTypes.keySet.toArray;
        }
      } catch {
        case e: Exception => {
          log.debug("", e)
          throw e
        }
      }
      return null;
    }
 
    override def getAllAttributeValues(): Array[AttributeValue] = { // Has ( value, attributetypeinfo))
      var attributeVals = new Array[AttributeValue](8);
      try{
 				attributeVals(0) = new AttributeValue(this.msisdn_key, attributeTypes(0)) 
				attributeVals(1) = new AttributeValue(this.tbl_dt, attributeTypes(1)) 
				attributeVals(2) = new AttributeValue(this.original_timestamp, attributeTypes(2)) 
				attributeVals(3) = new AttributeValue(this.type_of_recharge, attributeTypes(3)) 
				attributeVals(4) = new AttributeValue(this.value_of_recharge, attributeTypes(4)) 
				attributeVals(5) = new AttributeValue(this.balance_before_recharge, attributeTypes(5)) 
				attributeVals(6) = new AttributeValue(this.balance_after_recharge, attributeTypes(6)) 
				attributeVals(7) = new AttributeValue(this.counter, attributeTypes(7)) 
       
      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
      return attributeVals;
    }      
    
    override def getOnlyValuesForAllAttributes(): Array[Object] = {
      var allVals = new Array[Object](8);
      try{
 				allVals(0) = this.msisdn_key.asInstanceOf[AnyRef]; 
				allVals(1) = this.tbl_dt.asInstanceOf[AnyRef]; 
				allVals(2) = this.original_timestamp.asInstanceOf[AnyRef]; 
				allVals(3) = this.type_of_recharge.asInstanceOf[AnyRef]; 
				allVals(4) = this.value_of_recharge.asInstanceOf[AnyRef]; 
				allVals(5) = this.balance_before_recharge.asInstanceOf[AnyRef]; 
				allVals(6) = this.balance_after_recharge.asInstanceOf[AnyRef]; 
				allVals(7) = this.counter.asInstanceOf[AnyRef]; 

      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
      return allVals;
    }
    
    override def getAllAttributeTypesAndValues(): AllAttributeTypesAndValues = new AllAttributeTypesAndValues(getAttributeTypes, getOnlyValuesForAllAttributes)
    
    override def set(keyName: String, value: Any) = {
      if(keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name "+keyName);
      val key = caseSensitiveKey(keyName);
      try {
   
  			 if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message cm_refill_event", null)
			 set(keyTypes(key).getIndex, value); 

      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
    }
  
      
    def set(index : Int, value :Any): Unit = {
      if (value == null) throw new Exception(s"Value is null for index $index in message cm_refill_event ")
      if (index < 0 || index >= setFnArr.length) throw new Exception(s"$index is out of range for message cm_refill_event ")
      setFnArr(index)(this, value)
    }
    
    override def set(key: String, value: Any, valTyp: String) = {
      throw new Exception ("Set Func for Value and ValueType By Key is not supported for Fixed Messages" )
    }
  
    private def fromFunc(other: cm_refill_event): cm_refill_event = {  
   			this.msisdn_key = com.ligadata.BaseTypes.LongImpl.Clone(other.msisdn_key);
			this.tbl_dt = com.ligadata.BaseTypes.IntImpl.Clone(other.tbl_dt);
			this.original_timestamp = com.ligadata.BaseTypes.StringImpl.Clone(other.original_timestamp);
			this.type_of_recharge = com.ligadata.BaseTypes.IntImpl.Clone(other.type_of_recharge);
			this.value_of_recharge = com.ligadata.BaseTypes.DoubleImpl.Clone(other.value_of_recharge);
			this.balance_before_recharge = com.ligadata.BaseTypes.DoubleImpl.Clone(other.balance_before_recharge);
			this.balance_after_recharge = com.ligadata.BaseTypes.DoubleImpl.Clone(other.balance_after_recharge);
			this.counter = com.ligadata.BaseTypes.IntImpl.Clone(other.counter);

      this.setTimePartitionData(com.ligadata.BaseTypes.LongImpl.Clone(other.getTimePartitionData));
      return this;
    }
    

    override def computePartitionKeyHashIdValue(): Long = {
    var initialseed: Long = 0xcbf29ce484222325L
    var curVal: Long = initialseed
    
    val partitionKeys = Array[Any](msisdn_key)                
    if(partitionKeys != null && partitionKeys.size > 0){
      partitionKeys.foreach (pKV => {
         val byteArr = getKeyValueByteArray(pKV)
         curVal = com.ligadata.Utils.ComputeHashKey.generateHashKey(curVal, byteArr)
      })
    }
    return curVal;
  }

  override def getAllAttributesByteArrays(): Array[Array[Byte]] = {
    val outputArrayValues = scala.collection.mutable.ArrayBuffer[Array[Byte]]()
    val allAttributes = getOnlyValuesForAllAttributes()
    allAttributes.foreach (v => {
      if (v.isInstanceOf[ContainerInterface]) {
        outputArrayValues ++= v.asInstanceOf[ContainerInterface].getAllAttributesByteArrays()
      } else {
        outputArrayValues += getKeyValueByteArray(v)
      }
    })
    outputArrayValues.toArray
  }
 
  private val listSep = ",".getBytes

  private def getKeyValueByteArray(value: Any): Array[Byte] = {
    if (value == null) {
      return Array[Byte]()
    }
    // ???? is map & flatten is efficient than flatMap ?????
    if (value.isInstanceOf[List[_]]) {
      var firstVal = true
      return value.asInstanceOf[List[_]].flatMap(v => {
        if (firstVal) {
          firstVal = false
          getKeyValueByteArray(v)
        }
        else {
          listSep ++ getKeyValueByteArray(v)
        }
      }).toArray
    }
    if (value.isInstanceOf[Array[_]]) {
      var firstVal = true
      return value.asInstanceOf[Array[_]].flatMap(v => {
        if (firstVal) {
            firstVal = false
            getKeyValueByteArray(v)
          }
        else {
          listSep ++ getKeyValueByteArray(v)
        }
      })
    }

    value match {
      case b: Byte => Array[Byte](b)
      case bl: Boolean => if (bl) Array[Byte](1) else Array[Byte](0)
      case s: Short => ByteBuffer.allocate(2).putShort(s).array()
      case i: Int => ByteBuffer.allocate(4).putInt(i).array()
      case l: Long => ByteBuffer.allocate(8).putLong(l).array()
      case f: Float => ByteBuffer.allocate(4).putFloat(f).array()
      case d: Double => ByteBuffer.allocate(8).putDouble(d).array()
      case bi: BigInt => bi.toByteArray
      case x: String => x.getBytes
      // Handle other types here
    }
  }
 	 def withmsisdn_key(value: Long) : cm_refill_event = {
		 this.msisdn_key = value 
		 return this 
 	 } 
	 def withtbl_dt(value: Int) : cm_refill_event = {
		 this.tbl_dt = value 
		 return this 
 	 } 
	 def withoriginal_timestamp(value: String) : cm_refill_event = {
		 this.original_timestamp = value 
		 return this 
 	 } 
	 def withtype_of_recharge(value: Int) : cm_refill_event = {
		 this.type_of_recharge = value 
		 return this 
 	 } 
	 def withvalue_of_recharge(value: Double) : cm_refill_event = {
		 this.value_of_recharge = value 
		 return this 
 	 } 
	 def withbalance_before_recharge(value: Double) : cm_refill_event = {
		 this.balance_before_recharge = value 
		 return this 
 	 } 
	 def withbalance_after_recharge(value: Double) : cm_refill_event = {
		 this.balance_after_recharge = value 
		 return this 
 	 } 
	 def withcounter(value: Int) : cm_refill_event = {
		 this.counter = value 
		 return this 
 	 } 



    override def getRDDObject(): RDDObject[ContainerInterface] = cm_refill_event.asInstanceOf[RDDObject[ContainerInterface]];


        def isCaseSensitive(): Boolean = cm_refill_event.isCaseSensitive(); 
    def caseSensitiveKey(keyName: String): String = {
      if(isCaseSensitive)
        return keyName;
      else return keyName.toLowerCase;
    }


    
    def this(factory:MessageFactoryInterface) = {
      this(factory, null)
     }
    
    def this(other: cm_refill_event) = {
      this(other.getFactory.asInstanceOf[MessageFactoryInterface], other)
    }

}