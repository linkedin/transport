/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import java.util

import com.linkedin.transport.api.data.{MapData, PlatformData}
import com.linkedin.transport.spark.SparkWrapper
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.MapType

import scala.collection.mutable.Map


case class SparkMapData[K, V](private var _mapData: org.apache.spark.sql.catalyst.util.MapData,
                    private val _mapType: MapType) extends MapData[K, V] with PlatformData {

  private val _keyType = _mapType.keyType
  private val _valueType = _mapType.valueType
  private var _mutableMap: Map[Any, Any] = if (_mapData == null) createMutableMap() else null

  override def put(key: K, value: V): Unit = {
    // TODO: Does not support inserting nulls. Should we?
    if (_mutableMap == null) {
      _mutableMap = createMutableMap()
    }
    _mutableMap.put(
      SparkWrapper.getPlatformData(key.asInstanceOf[Object]),
      SparkWrapper.getPlatformData(value.asInstanceOf[Object])
    )
  }

  override def keySet(): util.Set[K] = {
    val keysIterator: Iterator[Any] = if (_mutableMap == null) {
      new Iterator[Any] {
        var offset : Int = 0

        override def next(): Any = {
          offset += 1
          _mapData.keyArray().get(offset - 1, _keyType)
        }

        override def hasNext: Boolean = {
          offset < SparkMap.this.size()
        }
      }
    } else {
      _mutableMap.keysIterator
    }

    new util.AbstractSet[K] {

      override def iterator(): util.Iterator[K] = new util.Iterator[K] {

        override def next(): K = SparkWrapper.createStdData(keysIterator.next(), _keyType).asInstanceOf[K]

        override def hasNext: Boolean = keysIterator.hasNext
      }

      override def size(): Int = SparkMapData.this.size()
    }
  }

  override def size(): Int = {
    if (_mutableMap == null) {
      _mapData.numElements()
    } else {
      _mutableMap.size
    }
  }

  override def values(): util.Collection[V] = {
    val valueIterator: Iterator[Any] = if (_mutableMap == null) {
      new Iterator[Any] {
        var offset : Int = 0

        override def next(): Any = {
          offset += 1
          _mapData.valueArray().get(offset - 1, _valueType)
        }

        override def hasNext: Boolean = {
          offset < SparkMap.this.size()
        }
      }
    } else {
      _mutableMap.valuesIterator
    }

    new util.AbstractCollection[V] {

      override def iterator(): util.Iterator[V] = new util.Iterator[V] {

        override def next(): V = SparkWrapper.createStdData(valueIterator.next(), _valueType).asInstanceOf[V]

        override def hasNext: Boolean = valueIterator.hasNext
      }

      override def size(): Int = SparkMapData.this.size()
    }
  }

  override def containsKey(key: K): Boolean = get(key) != null

  override def get(key: K): V = {
    // Spark's complex data types (MapData, ArrayData, InternalRow) do not implement equals/hashcode
    // If the key is of the above complex data types, get() will return null
    if (_mutableMap == null) {
      _mutableMap = createMutableMap()
    }
    SparkWrapper.createStdData(_mutableMap.get(SparkWrapper.getPlatformData(key.asInstanceOf[Object])).orNull, _valueType)
      .asInstanceOf[V]
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
    _mapData = value.asInstanceOf[org.apache.spark.sql.catalyst.util.MapData]
    _mutableMap = null
  }
}
