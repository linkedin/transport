package portable_udfs.spark.data

import java.util.{ArrayList => JavaArrayList, List => JavaList}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType
import portable_udfs.api.data.{StdData, StdStruct}
import portable_udfs.api.types.StdType
import portable_udfs.spark.types.StdSparkStructType
import portable_udfs.spark.util

import scala.collection.mutable


class StdSparkStruct(private val _row: InternalRow,
                     private var _structDataType: StructType) extends StdStruct {

  private var _mutableBuffer: mutable.ArrayBuffer[Any] = _

  override def asStruct(): StdSparkStruct = this

  override def get(idx: Int): StdData = {
    val fieldDataType = _structDataType(idx).dataType
    if (_mutableBuffer == null) {
      val data = _row.get(idx, fieldDataType)
      util.wrapData(data, fieldDataType)
    }
    else {
      util.wrapData(_mutableBuffer(idx), fieldDataType)
    }
  }

  override def fields(): JavaList[StdData] = {
    val r = new JavaArrayList[StdData](_row.numFields)

    if (_mutableBuffer != null)
      _mutableBuffer.zipWithIndex.foreach {
        case (udata, idx) => {
          val e = util.wrapData(udata, _structDataType(idx).dataType)
          r.add(e)
        }
      }

    else
      _structDataType.fields.zipWithIndex.foreach {
        case (field, idx) => {
          val udata = _row.get(idx, field.dataType)
          val e = util.wrapData(udata, field.dataType)
          r.add(e)
        }
      }
    r
  }

  override def getType: StdType = StdSparkStructType(_structDataType)

  override def getUnderlyingData: InternalRow = {
    if (_mutableBuffer == null) _row
    else new GenericInternalRow(_mutableBuffer.toArray)
  }

  override def get(name: String): StdData =
    get(_structDataType.fieldIndex(name))

  override def update(idx: Int, data: StdData): Unit = {
    if (_mutableBuffer == null)
      _mutableBuffer = mkMutable()
    _mutableBuffer.update(idx, data.getUnderlyingData)
  }

  override def update(name: String, data: StdData): Unit = {
    update(_structDataType.fieldIndex(name), data)
  }

  override def size(): Int =
    if (_mutableBuffer != null) _mutableBuffer.size else _row.numFields

  private def mkMutable() = {
    val r = new mutable.ArrayBuffer[Any](_row.numFields)
    val fields = _structDataType.fields.zipWithIndex.foreach {
      case (field, idx) => r += _row.get(idx, field.dataType)
    }
    r
  }
}
