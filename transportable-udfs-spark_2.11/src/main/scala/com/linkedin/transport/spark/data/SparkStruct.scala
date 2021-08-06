/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import java.util.{List => JavaList}

import com.linkedin.transport.api.data.{PlatformData, StdData, StdStruct}
import com.linkedin.transport.spark.SparkWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


case class SparkStruct(private var _row: InternalRow,
                       private val _structType: StructType) extends StdStruct with PlatformData {

  private var _mutableBuffer: ArrayBuffer[Any] = if (_row == null) createMutableStruct() else null

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
    _mutableBuffer(index) = value.asInstanceOf[PlatformData].getUnderlyingData
  }

  private def createMutableStruct() = {
    if (_row != null) {
      ArrayBuffer[Any](_row.toSeq(_structType): _*)
    } else {
      ArrayBuffer.fill[Any](_structType.length) {null}
    }
  }

  override def fields(): JavaList[StdData] = {
    _structType.indices.map(getField).asJava
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
