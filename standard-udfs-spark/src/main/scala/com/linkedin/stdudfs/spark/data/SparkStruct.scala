package com.linkedin.stdudfs.spark.data

import java.util.{List => JavaList}

import com.linkedin.stdudfs.api.data.{PlatformData, StdData, StdStruct}
import com.linkedin.stdudfs.spark.SparkWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable


case class SparkStruct(private var _row: InternalRow,
                       private val _structType: StructType) extends StdStruct with PlatformData {

  private var _mutableBuffer: mutable.ArrayBuffer[Any] = _

  override def getField(name: String): StdData = getField(_structType.fieldIndex(name))

  override def getField(index: Int): StdData = {
    val fieldDataType = _structType(index).dataType
    if (_mutableBuffer == null) {
      SparkWrapper.createStdData(_row.get(index, fieldDataType), fieldDataType)
    } else {
      SparkWrapper.createStdData(_mutableBuffer(index), fieldDataType)
    }
  }

  override def setField(name: String, value: StdData): Unit = {
    setField(_structType.fieldIndex(name), value)
  }

  override def setField(index: Int, value: StdData): Unit = {
    if (_mutableBuffer == null) {
      _mutableBuffer = createMutableStruct()
    }
    _mutableBuffer.update(index, value.asInstanceOf[PlatformData].getUnderlyingData)
  }

  private def createMutableStruct() = {
    mutable.ArrayBuffer.apply(_row.toSeq(_structType): _*)
  }

  override def fields(): JavaList[StdData] = {
    _structType.fields.indices.map(getField).asJava
  }

  override def getUnderlyingData: AnyRef = {
    if (_mutableBuffer == null) {
      _row
    } else {
      InternalRow.fromSeq(_mutableBuffer)
    }
  }

  override def setUnderlyingData(value: scala.Any): Unit = {
    _row = value.asInstanceOf[InternalRow]
    _mutableBuffer = null
  }
}
