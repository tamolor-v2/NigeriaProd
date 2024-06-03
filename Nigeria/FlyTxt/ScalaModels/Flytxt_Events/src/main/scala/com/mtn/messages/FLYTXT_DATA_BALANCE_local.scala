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


object FLYTXT_DATA_BALANCE extends RDDObject[FLYTXT_DATA_BALANCE] with MessageFactoryInterface {

  val log = LogManager.getLogger(getClass)
  type T = FLYTXT_DATA_BALANCE ;
  override def getFullTypeName: String = "com.mtn.messages.FLYTXT_DATA_BALANCE";
  override def getTypeNameSpace: String = "com.mtn.messages";
  override def getTypeName: String = "FLYTXT_DATA_BALANCE";
  override def getTypeVersion: String = "000000.000001.000000";
  override def getSchemaId: Int = 2006664;
  private var elementId: Long = 0L;
  override def setElementId(elemId: Long): Unit = { elementId = elemId; }
  override def getElementId: Long = elementId;
  override def getTenantId: String = "tenant1";
  override def createInstance: FLYTXT_DATA_BALANCE = new FLYTXT_DATA_BALANCE(FLYTXT_DATA_BALANCE);
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

  override def getAvroSchema: String = """{ "type": "record",  "namespace" : "com.mtn.messages" , "name" : "FLYTXT_DATA_BALANCE" , "fields":[{ "name" : "msisdn_key" , "type" : "long"},{ "name" : "data_balance" , "type" : "double"},{ "name" : "original_timestamp_enrich" , "type" : "string"},{ "name" : "tbl_dt" , "type" : "int"}]}""";

  var attributeTypes = generateAttributeTypes;

  private def generateAttributeTypes(): Array[AttributeTypeInfo] = {
    var attributeTypes = new Array[AttributeTypeInfo](4);
    attributeTypes(0) = new AttributeTypeInfo("msisdn_key", 0, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
    attributeTypes(1) = new AttributeTypeInfo("data_balance", 1, AttributeTypeInfo.TypeCategory.DOUBLE, -1, -1, 0)
    attributeTypes(2) = new AttributeTypeInfo("original_timestamp_enrich", 2, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
    attributeTypes(3) = new AttributeTypeInfo("tbl_dt", 3, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)


    return attributeTypes
  }


  var keyTypes: Map[String, AttributeTypeInfo] = attributeTypes.map { a => (a.getName, a) }.toMap;
  def setFn0(curObj: FLYTXT_DATA_BALANCE, value: Any): Unit ={
    if(value.isInstanceOf[String])
      curObj.msisdn_key = value.asInstanceOf[Long];
    else throw new Exception(s"Value is the not the correct type Long for field msisdn_key in message FLYTXT_DATA_BALANCE")
  }
  def setFn1(curObj: FLYTXT_DATA_BALANCE, value: Any): Unit ={
    if(value.isInstanceOf[Double])
      curObj.data_balance = value.asInstanceOf[Double];
    else throw new Exception(s"Value is the not the correct type Double for field amount in message FLYTXT_DATA_BALANCE")
  }
  def setFn2(curObj: FLYTXT_DATA_BALANCE, value: Any): Unit ={
    if(value.isInstanceOf[String])
      curObj.original_timestamp_enrich = value.asInstanceOf[String];
    else throw new Exception(s"Value is the not the correct type String for field original_timestamp_enrich in message FLYTXT_DATA_BALANCE")
  }
  def setFn3(curObj: FLYTXT_DATA_BALANCE, value: Any): Unit ={
    if(value.isInstanceOf[Int]){
      curObj.tbl_dt = value.asInstanceOf[Int];
      curObj.setTimePartitionData;
    } else throw new Exception(s"Value is the not the correct type Int for field tbl_dt in message FLYTXT_DATA_BALANCE")
  }

  val setFnArr = Array[(FLYTXT_DATA_BALANCE, Any) => Unit](setFn0,setFn1,setFn2,setFn3    )
  def getFn0(curObj: FLYTXT_DATA_BALANCE): AnyRef = curObj.msisdn_key.asInstanceOf[AnyRef];
  def getFn1(curObj: FLYTXT_DATA_BALANCE): AnyRef = curObj.data_balance.asInstanceOf[AnyRef];
  def getFn2(curObj: FLYTXT_DATA_BALANCE): AnyRef = curObj.original_timestamp_enrich.asInstanceOf[AnyRef];
  def getFn3(curObj: FLYTXT_DATA_BALANCE): AnyRef = curObj.tbl_dt.asInstanceOf[AnyRef];

  val getFnArr = Array[(FLYTXT_DATA_BALANCE) => AnyRef](getFn0,getFn1,getFn2,getFn3    )



  override def getRDDObject(): RDDObject[ContainerInterface] = FLYTXT_DATA_BALANCE.asInstanceOf[RDDObject[ContainerInterface]];



  final override def convertFrom(srcObj: Any): T = convertFrom(createInstance(), srcObj);

  override def convertFrom(newVerObj: Any, oldVerobj: Any): ContainerInterface = {
    try {
      if (oldVerobj == null) return null;
      oldVerobj match {

        case oldVerobj: com.mtn.messages.FLYTXT_DATA_BALANCE => { return  convertToVer1000000(oldVerobj); }
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

  private def convertToVer1000000(oldVerobj: com.mtn.messages.FLYTXT_DATA_BALANCE): com.mtn.messages.FLYTXT_DATA_BALANCE= {
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
  override def PartitionKeyData(inputdata: InputData): Array[String] = { throw new Exception("Deprecated method PartitionKeyData in obj FLYTXT_DATA_BALANCE") };
  override def PrimaryKeyData(inputdata: InputData): Array[String] = throw new Exception("Deprecated method PrimaryKeyData in obj FLYTXT_DATA_BALANCE");
  override def TimePartitionData(inputdata: InputData): Long = throw new Exception("Deprecated method TimePartitionData in obj FLYTXT_DATA_BALANCE");
  override def NeedToTransformData: Boolean = false
}

class FLYTXT_DATA_BALANCE(factory: MessageFactoryInterface, other: FLYTXT_DATA_BALANCE) extends MessageInterface(factory) {

  val log = FLYTXT_DATA_BALANCE.log

  var attributeTypes = FLYTXT_DATA_BALANCE.attributeTypes

  var keyTypes = FLYTXT_DATA_BALANCE.keyTypes;

  if (other != null && other != this) {
    // call copying fields from other to local variables
    fromFunc(other)
  }

  override def save: Unit = { FLYTXT_DATA_BALANCE.saveOne(this) }

  def Clone(): ContainerOrConcept = { FLYTXT_DATA_BALANCE.build(this) }

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


  var setFnArr = FLYTXT_DATA_BALANCE.setFnArr

  var getFnArr = FLYTXT_DATA_BALANCE.getFnArr

  var msisdn_key : Long = _;
  var data_balance : Double = _;
  var original_timestamp_enrich : String = _;
  var tbl_dt : Int = _;

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
    val fieldX = ru.typeOf[FLYTXT_DATA_BALANCE].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
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

    if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message/container FLYTXT_DATA_BALANCE", null);
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
    throw new Exception(s"$index is out of range for message FLYTXT_DATA_BALANCE");
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
    var attributeVals = new Array[AttributeValue](4);
    try{
      attributeVals(0) = new AttributeValue(this.msisdn_key, attributeTypes(0))
      attributeVals(1) = new AttributeValue(this.data_balance, attributeTypes(1))
      attributeVals(2) = new AttributeValue(this.original_timestamp_enrich, attributeTypes(2))
      attributeVals(3) = new AttributeValue(this.tbl_dt, attributeTypes(3))

    }catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    };

    return attributeVals;
  }

  override def getOnlyValuesForAllAttributes(): Array[Object] = {
    var allVals = new Array[Object](4);
    try{
      allVals(0) = this.msisdn_key.asInstanceOf[AnyRef];
      allVals(1) = this.data_balance.asInstanceOf[AnyRef];
      allVals(2) = this.original_timestamp_enrich.asInstanceOf[AnyRef];
      allVals(3) = this.tbl_dt.asInstanceOf[AnyRef];

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

      if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message FLYTXT_DATA_BALANCE", null)
      set(keyTypes(key).getIndex, value);

    }catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    };

  }


  def set(index : Int, value :Any): Unit = {
    if (value == null) throw new Exception(s"Value is null for index $index in message FLYTXT_DATA_BALANCE ")
    if (index < 0 || index >= setFnArr.length) throw new Exception(s"$index is out of range for message FLYTXT_DATA_BALANCE ")
    setFnArr(index)(this, value)
  }

  override def set(key: String, value: Any, valTyp: String) = {
    throw new Exception ("Set Func for Value and ValueType By Key is not supported for Fixed Messages" )
  }

  private def fromFunc(other: FLYTXT_DATA_BALANCE): FLYTXT_DATA_BALANCE = {
    this.msisdn_key = com.ligadata.BaseTypes.LongImpl.Clone(other.msisdn_key);
    this.data_balance = com.ligadata.BaseTypes.DoubleImpl.Clone(other.data_balance);
    this.original_timestamp_enrich = com.ligadata.BaseTypes.StringImpl.Clone(other.original_timestamp_enrich);
    this.tbl_dt = com.ligadata.BaseTypes.IntImpl.Clone(other.tbl_dt);

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
  def withmsisdn_key(value: Long) : FLYTXT_DATA_BALANCE = {
    this.msisdn_key = value
    return this
  }
  def withdata_balance(value: Double) : FLYTXT_DATA_BALANCE = {
    this.data_balance = value
    return this
  }
  def withoriginal_timestamp_enrich(value: String) : FLYTXT_DATA_BALANCE = {
    this.original_timestamp_enrich = value
    return this
  }
  def withtbl_dt(value: Int) : FLYTXT_DATA_BALANCE = {
    this.tbl_dt = value
    return this
  }



  override def getRDDObject(): RDDObject[ContainerInterface] = FLYTXT_DATA_BALANCE.asInstanceOf[RDDObject[ContainerInterface]];


  def isCaseSensitive(): Boolean = FLYTXT_DATA_BALANCE.isCaseSensitive();
  def caseSensitiveKey(keyName: String): String = {
    if(isCaseSensitive)
      return keyName;
    else return keyName.toLowerCase;
  }



  def this(factory:MessageFactoryInterface) = {
    this(factory, null)
  }

  def this(other: FLYTXT_DATA_BALANCE) = {
    this(other.getFactory.asInstanceOf[MessageFactoryInterface], other)
  }

}
