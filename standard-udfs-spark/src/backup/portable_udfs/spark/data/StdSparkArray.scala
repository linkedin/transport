package portable_udfs.spark.data

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.DataType
import portable_udfs.api.data.{StdArray, StdData}
import portable_udfs.api.types.StdType
import portable_udfs.spark.types.StdSparkArrayType
import portable_udfs.spark.util

import scala.collection.mutable.ArrayBuffer

class StdSparkArray(private var _uArrayData: ArrayData,
                    private val _elemType: DataType) extends StdArray {

  private var _mutableBuffer: ArrayBuffer[Any] = _

  override def asArray(): StdArray = this

  override def size(): Int = {
    if (_mutableBuffer != null) _mutableBuffer.size
    else _uArrayData.numElements()
  }

  override def get(idx: Int): StdData = {
    if (_mutableBuffer == null)
      util.wrapData(_uArrayData.get(idx, _elemType), _elemType)
    else
      util.wrapData(_mutableBuffer(idx), _elemType)
  }

  override def getUnderlyingData: ArrayData = {
    if (_mutableBuffer == null) _uArrayData
    else new GenericArrayData(_mutableBuffer)
  }

  override def getType: StdType = StdSparkArrayType(_elemType)

  override def add(e: StdData): Unit = {
    // Once add is called, we cannot use  Spark's readonly ArrayData API
    // we have to add elements to a mutable buffer and start using that
    // always instead of the readonly stdType
    if (_mutableBuffer == null) {
      // from now on mutable is in affect
      _mutableBuffer = mkMutable()
    }
    _mutableBuffer.append(e.getUnderlyingData)
  }

  def mkMutable(): ArrayBuffer[Any] = {
    val r = new ArrayBuffer[Any](_uArrayData.numElements())
    _uArrayData.foreach(_elemType, (i, e) => {
      r += e
    })
    r
  }

  override def iterator(): java.util.Iterator[StdData] = {
    var idx = 0
    new java.util.Iterator[StdData] {
      override def next(): StdData = {
        val e = get(idx)
        idx += 1
        e
      }
      override def hasNext: Boolean = idx < size()
    }
  }

  override def set(idx: Int, stdData: StdData): Unit = {
    if (_mutableBuffer == null) {
      _mutableBuffer = mkMutable();
    }
    _mutableBuffer(idx)= stdData.getUnderlyingData
  }

}
