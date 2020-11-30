/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import java.util

import com.linkedin.transport.api.data.{ArrayData, PlatformData}
import com.linkedin.transport.spark.SparkWrapper
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.mutable.ArrayBuffer

case class SparkArrayData[E](private var _arrayData: org.apache.spark.sql.catalyst.util.ArrayData,
                      private val _arrayType: DataType) extends ArrayData[E] with PlatformData {

  private val _elementType = _arrayType.asInstanceOf[ArrayType].elementType
  private var _mutableBuffer: ArrayBuffer[Any] = if (_arrayData == null) createMutableArray() else null

  override def add(e: E): Unit = {
    // Once add is called, we cannot use  Spark's readonly ArrayData API
    // we have to add elements to a mutable buffer and start using that
    // always instead of the readonly stdType
    if (_mutableBuffer == null) {
      // from now on mutable is in affect
      _mutableBuffer = createMutableArray()
    }
    // TODO: Does not support inserting nulls. Should we?
    _mutableBuffer.append(SparkWrapper.getPlatformData(e.asInstanceOf[Object]))
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
      org.apache.spark.sql.catalyst.util.ArrayData.toArrayData(_mutableBuffer)
    }
  }

  override def setUnderlyingData(value: scala.Any): Unit = {
    _arrayData = value.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
    _mutableBuffer = null
  }

  override def iterator(): util.Iterator[E] = {
    new util.Iterator[E] {
      private var idx = 0

      override def next(): E = {
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

  override def get(idx: Int): E = {
    if (_mutableBuffer == null) {
      SparkWrapper.createStdData(_arrayData.get(idx, _elementType), _elementType).asInstanceOf[E]
    } else {
      SparkWrapper.createStdData(_mutableBuffer(idx), _elementType).asInstanceOf[E]
    }
  }
}
