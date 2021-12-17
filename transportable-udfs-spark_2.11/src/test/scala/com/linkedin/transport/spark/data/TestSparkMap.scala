/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{MapData, PlatformData}
import com.linkedin.transport.spark.{SparkTypeFactory, SparkConverters}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{DataTypes, MapType}
import org.apache.spark.unsafe.types.UTF8String
import org.testng.Assert.{assertEquals, assertEqualsNoOrder, assertNotSame, assertSame}
import org.testng.annotations.Test

class TestSparkMap {

  val typeFactory = new SparkTypeFactory(null)
  val mapData = ArrayBasedMapData(
    Array("k1", "k2", "k3").map(UTF8String.fromString), Array("v1", "v2", "v3").map(UTF8String.fromString)
  )
  val mapType = MapType(DataTypes.StringType, DataTypes.StringType)

  @Test
  def testCreateSparkMap(): Unit = {
    val map = SparkConverters.toTransportData(mapData, mapType).asInstanceOf[MapData[String, String]]
    assertEquals(map.size(), mapData.numElements())
    assertSame(map.asInstanceOf[PlatformData].getUnderlyingData, mapData)
  }

  @Test
  def testSparkMapKeySet(): Unit = {
    val map = SparkConverters.toTransportData(mapData, mapType).asInstanceOf[MapData[String, String]]
    assertEqualsNoOrder(map.keySet().toArray, mapData.keyArray.array.map(s => s.toString))
  }

  @Test
  def testSparkMapValues(): Unit = {
    val map = SparkConverters.toTransportData(mapData, mapType).asInstanceOf[MapData[String, String]]
    assertEqualsNoOrder(map.values().toArray, mapData.valueArray.array.map(s => s.toString))
  }

  @Test
  def testSparkMapGet(): Unit = {
    val map = SparkConverters.toTransportData(mapData, mapType).asInstanceOf[MapData[String, String]]
    mapData.keyArray.foreach(mapType.keyType, (idx, key) => {
      assertEquals(map.get(key.toString),
        mapData.valueArray.array(idx).toString)
    })
    assertEquals(map.containsKey("nonExistentKey"), false)
    // Even for a get in SparkMapData we create mutable Map since Spark's Impl is based of arrays. So underlying object should change
    assertNotSame(map.asInstanceOf[PlatformData].getUnderlyingData, mapData)
  }

  @Test
  def testSparkMapContainsKey(): Unit = {
    val map = SparkConverters.toTransportData(mapData, mapType).asInstanceOf[MapData[String, String]]
    assertEquals(map.containsKey("k3"), true)
    assertEquals(map.containsKey("k4"), false)
  }

  @Test
  def testSparkMapPut(): Unit = {
    val map = SparkConverters.toTransportData(mapData, mapType).asInstanceOf[MapData[String, String]]
    map.put("k4", "v4")
    assertEquals(map.size(), mapData.numElements() + 1)
    assertEquals(map.get("k4"), "v4")
  }

  @Test
  def testSparkMapMutabilityReset(): Unit = {
    val map = SparkConverters.toTransportData(mapData, mapType).asInstanceOf[MapData[String, String]]
    map.put("k4", "v4")
    map.asInstanceOf[PlatformData].setUnderlyingData(mapData)
    // After underlying data is explicitly set, mutuable map should be removed
    assertSame(map.asInstanceOf[PlatformData].getUnderlyingData, mapData)
  }
}
