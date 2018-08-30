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

import com.linkedin.stdudfs.api.data.{PlatformData, StdMap, StdString}
import com.linkedin.stdudfs.spark.{SparkFactory, SparkWrapper}
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
