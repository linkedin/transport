/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.linkedin.stdudfs.spark.data

import java.util.{List => JavaList}

import com.linkedin.stdudfs.api.data.{PlatformData, StdData, StdStruct}
import com.linkedin.stdudfs.spark.SparkWrapper
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
