/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{PlatformData, StdMap, StdString}
import com.linkedin.transport.spark.{SparkFactory, SparkWrapper}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{DataTypes, MapType}
import org.apache.spark.unsafe.types.UTF8String
import org.testng.Assert.{assertEquals, assertEqualsNoOrder, assertNotSame, assertSame}
import org.testng.annotations.Test

class TestSparkMap {

  val stdFactory = new SparkFactory(null)
  val mapData = ArrayBasedMapData(
    Array("k1", "k2", "k3").map(UTF8String.fromString), Array("v1", "v2", "v3").map(UTF8String.fromString)
  )
  val mapType = MapType(DataTypes.StringType, DataTypes.StringType)

  @Test
  def testCreateSparkMap(): Unit = {
    val stdMap = SparkWrapper.createStdData(mapData, mapType).asInstanceOf[StdMap]
    assertEquals(stdMap.size(), mapData.numElements())
    assertSame(stdMap.asInstanceOf[PlatformData].getUnderlyingData, mapData)
  }

  @Test
  def testSparkMapKeySet(): Unit = {
    val stdMap = SparkWrapper.createStdData(mapData, mapType).asInstanceOf[StdMap]
    assertEqualsNoOrder(stdMap.keySet().toArray, mapData.keyArray.array.map(s => stdFactory.createString(s.toString)))
  }

  @Test
  def testSparkMapValues(): Unit = {
    val stdMap = SparkWrapper.createStdData(mapData, mapType).asInstanceOf[StdMap]
    assertEqualsNoOrder(stdMap.values().toArray, mapData.valueArray.array.map(s => stdFactory.createString(s.toString)))
  }

  @Test
  def testSparkMapGet(): Unit = {
    val stdMap = SparkWrapper.createStdData(mapData, mapType).asInstanceOf[StdMap]
    mapData.keyArray.foreach(mapType.keyType, (idx, key) => {
      assertEquals(stdMap.get(stdFactory.createString(key.toString)).asInstanceOf[StdString].get,
        mapData.valueArray.array(idx).toString)
    })
    assertEquals(stdMap.containsKey(stdFactory.createString("nonExistentKey")), false)
    // Even for a get in SparkMap we create mutable Map since Spark's Impl is based of arrays. So underlying object should change
    assertNotSame(stdMap.asInstanceOf[PlatformData].getUnderlyingData, mapData)
  }

  @Test
  def testSparkMapContainsKey(): Unit = {
    val stdMap = SparkWrapper.createStdData(mapData, mapType).asInstanceOf[StdMap]
    assertEquals(stdMap.containsKey(stdFactory.createString("k3")), true)
    assertEquals(stdMap.containsKey(stdFactory.createString("k4")), false)
  }

  @Test
  def testSparkMapPut(): Unit = {
    val stdMap = SparkWrapper.createStdData(mapData, mapType).asInstanceOf[StdMap]
    val insertKey = stdFactory.createString("k4")
    val insertVal = stdFactory.createString("v4")
    stdMap.put(insertKey, insertVal)
    assertEquals(stdMap.size(), mapData.numElements() + 1)
    assertEquals(stdMap.get(stdFactory.createString("k4")), insertVal)
  }

  @Test
  def testSparkMapMutabilityReset(): Unit = {
    val stdMap = SparkWrapper.createStdData(mapData, mapType).asInstanceOf[StdMap]
    val insertKey = stdFactory.createString("k4")
    val insertVal = stdFactory.createString("v4")
    stdMap.put(insertKey, insertVal)
    stdMap.asInstanceOf[PlatformData].setUnderlyingData(mapData)
    // After underlying data is explicitly set, mutuable map should be removed
    assertSame(stdMap.asInstanceOf[PlatformData].getUnderlyingData, mapData)
  }
}
