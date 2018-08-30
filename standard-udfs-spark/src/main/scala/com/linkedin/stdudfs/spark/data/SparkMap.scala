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

import com.linkedin.stdudfs.api.data.{PlatformData, StdData, StdMap}
import com.linkedin.stdudfs.spark.SparkWrapper
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.types.MapType

import scala.collection.mutable.Map


case class SparkMap(private var _mapData: MapData,
                    private val _mapType: MapType) extends StdMap with PlatformData {

  private val _keyType = _mapType.keyType
  private val _valueType = _mapType.valueType
  private var _mutableMap: Map[Any, Any] = if (_mapData == null) createMutableMap() else null

  override def put(key: StdData, value: StdData): Unit = {
    // TODO: Does not support inserting nulls. Should we?
    if (_mutableMap == null) {
      _mutableMap = createMutableMap()
    }
    _mutableMap.put(key.asInstanceOf[PlatformData].getUnderlyingData, value.asInstanceOf[PlatformData].getUnderlyingData)
  }

  override def keySet(): util.Set[StdData] = {
    new util.AbstractSet[StdData] {

      override def iterator(): util.Iterator[StdData] = new util.Iterator[StdData] {
        private val keysIterator = if (_mutableMap == null) _mapData.keyArray().array.iterator else _mutableMap.keysIterator

        override def next(): StdData = SparkWrapper.createStdData(keysIterator.next(), _keyType)

        override def hasNext: Boolean = keysIterator.hasNext
      }

      override def size(): Int = SparkMap.this.size()
    }
  }

  override def size(): Int = {
    if (_mutableMap == null) {
      _mapData.numElements()
    } else {
      _mutableMap.size
    }
  }

  override def values(): util.Collection[StdData] = {
    new util.AbstractCollection[StdData] {

      override def iterator(): util.Iterator[StdData] = new util.Iterator[StdData] {
        private val valueIterator = if (_mutableMap == null) _mapData.valueArray().array.iterator else _mutableMap.valuesIterator

        override def next(): StdData = SparkWrapper.createStdData(valueIterator.next(), _valueType)

        override def hasNext: Boolean = valueIterator.hasNext
      }

      override def size(): Int = SparkMap.this.size()
    }
  }

  override def containsKey(key: StdData): Boolean = get(key) != null

  override def get(key: StdData): StdData = {
    // Spark's complex data types (MapData, ArrayData, InternalRow) do not implement equals/hashcode
    // If the key is of the above complex data types, get() will return null
    if (_mutableMap == null) {
      _mutableMap = createMutableMap()
    }
    SparkWrapper.createStdData(_mutableMap.get(key.asInstanceOf[PlatformData].getUnderlyingData).orNull, _valueType)
  }

  private def createMutableMap(): Map[Any, Any] = {
    val mutableMap = Map.empty[Any, Any]
    if (_mapData != null) {
      _mapData.foreach(_keyType, _valueType, (k, v) => mutableMap.put(k, v))
    }
    mutableMap
  }

  override def getUnderlyingData: AnyRef = {
    if (_mutableMap == null) {
      _mapData
    } else {
      ArrayBasedMapData(_mutableMap)
    }
  }

  override def setUnderlyingData(value: scala.Any): Unit = {
    _mapData = value.asInstanceOf[MapData]
    _mutableMap = null
  }
}
