package portable_udfs.spark.data

import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.types.MapType
import portable_udfs.api.data.{StdArray, StdData, StdMap}
import portable_udfs.api.types.StdType
import portable_udfs.spark.types.StdSparkMapType
import portable_udfs.spark.util

import scala.collection.mutable


class StdSparkMap(private val _uMapData: MapData,
                  private val _mapType: MapType) extends StdMap {

  private val _keyType = _mapType.keyType
  private val _valueType = _mapType.valueType

  private var _mutableMap: mutable.Map[Any, Any] = _

  override def size(): Int = {
    if (_mutableMap == null) _uMapData.numElements()
    else _mutableMap.size
  }

  override def get(key: StdData): StdData = {
    // TODO: Spark's map (MapData) does not implement equals/hashcode
    // If the key is of stdType MapData, this will be problematic
    if (_mutableMap == null)
      _mutableMap = mkMutable()

    val uKey = key.getUnderlyingData
    val uVal: Option[Any] = _mutableMap.get(uKey)
    if (uVal.isDefined)
      util.wrapData(uVal.get, _valueType)
    else
      throw new RuntimeException("No val corresponding to key: " + key)
  }

  override def put(key: StdData, value: StdData): Unit = {
    if (_mutableMap == null) _mutableMap = mkMutable()
    val uKey = key.getUnderlyingData
    val uVal = value.getUnderlyingData
    _mutableMap.put(uKey, uVal)
  }

  override def keys(): StdArray = new StdSparkArray(_uMapData.keyArray(), _keyType)

  override def values(): StdArray = new StdSparkArray(_uMapData.valueArray(), _valueType)

  override def asMap: StdSparkMap = this

  override def getUnderlyingData: MapData = {
    if (_mutableMap == null) _uMapData
    else
      new ArrayBasedMapData(
        new GenericArrayData(_mutableMap.keys.toSeq),
        new GenericArrayData(_mutableMap.values.toSeq))
  }

  override def getType: StdType = StdSparkMapType(_keyType, _valueType)

  private def mkMutable(): mutable.Map[Any, Any] = {
    val r = mutable.Map[Any, Any]()
    _uMapData.foreach(_keyType, _valueType, (k, v) => {
      r.put(k, v)
    })
    r
  }

  override def getKeyType: StdType = util.wrapType(_keyType)

  override def getValueType: StdType = util.wrapType(_valueType)

}
