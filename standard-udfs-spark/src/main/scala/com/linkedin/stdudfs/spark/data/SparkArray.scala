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

import java.util

import com.linkedin.stdudfs.api.data.{PlatformData, StdArray, StdData}
import com.linkedin.stdudfs.spark.SparkWrapper
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.mutable.ArrayBuffer

case class SparkArray(private var _arrayData: ArrayData,
                      private val _arrayType: DataType) extends StdArray with PlatformData {

  private val _elementType = _arrayType.asInstanceOf[ArrayType].elementType
  private var _mutableBuffer: ArrayBuffer[Any] = if (_arrayData == null) createMutableArray() else null

  override def add(e: StdData): Unit = {
    // Once add is called, we cannot use  Spark's readonly ArrayData API
    // we have to add elements to a mutable buffer and start using that
    // always instead of the readonly stdType
    if (_mutableBuffer == null) {
      // from now on mutable is in affect
      _mutableBuffer = createMutableArray()
    }
    // TODO: Does not support inserting nulls. Should we?
    _mutableBuffer.append(e.asInstanceOf[PlatformData].getUnderlyingData)
  }

  private def createMutableArray(): ArrayBuffer[Any] = {
    var arrayBuffer: ArrayBuffer[Any] = null
    if (_arrayData == null) {
      arrayBuffer = new ArrayBuffer[Any]()
    } else {
      arrayBuffer = new ArrayBuffer[Any](_arrayData.numElements())
      _arrayData.foreach(_elementType, (i, e) => arrayBuffer.append(e))
    }
    arrayBuffer
  }

  override def getUnderlyingData: AnyRef = {
    if (_mutableBuffer == null) {
      _arrayData
    } else {
      ArrayData.toArrayData(_mutableBuffer)
    }
  }

  override def setUnderlyingData(value: scala.Any): Unit = {
    _arrayData = value.asInstanceOf[ArrayData]
    _mutableBuffer = null
  }

  override def iterator(): util.Iterator[StdData] = {
    new util.Iterator[StdData] {
      private var idx = 0

      override def next(): StdData = {
        val e = get(idx)
        idx += 1
        e
      }

      override def hasNext: Boolean = idx < size()
    }
  }

  override def size(): Int = {
    if (_mutableBuffer != null) {
      _mutableBuffer.size
    } else {
      _arrayData.numElements()
    }
  }

  override def get(idx: Int): StdData = {
    if (_mutableBuffer == null) {
      SparkWrapper.createStdData(_arrayData.get(idx, _elementType), _elementType)
    } else {
      SparkWrapper.createStdData(_mutableBuffer(idx), _elementType)
    }
  }
}
