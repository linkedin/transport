/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data
import com.linkedin.transport.api.data.{PlatformData}
import com.linkedin.transport.spark.{SparkTypeFactory, SparkConverters}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataTypes}
import org.testng.Assert.{assertEquals, assertNotSame, assertSame}
import org.testng.annotations.Test

class TestSparkArray {

  val typeFactory = new SparkTypeFactory(null)
  val arrayData = ArrayData.toArrayData(Array.range(0, 5)) // scalastyle:ignore magic.number
  val arrayType = ArrayType(DataTypes.IntegerType)

  @Test
  def testCreateSparkArray(): Unit = {
    val array = SparkConverters.toTransportData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    assertEquals(array.size(), arrayData.numElements())
    assertSame(array.asInstanceOf[PlatformData].getUnderlyingData, arrayData)
  }

  @Test
  def testSparkArrayGet(): Unit = {
    val array = SparkConverters.toTransportData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    (0 until array.size).foreach(idx => {
      assertEquals(array.get(idx), idx)
    })
  }

  @Test
  def testSparkArrayAdd(): Unit = {
    val array = SparkConverters.toTransportData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    array.add(5)
    // Since original ArrayData is immutable, a mutable ArrayBuffer should be created and set as the underlying object
    assertNotSame(array.asInstanceOf[PlatformData].getUnderlyingData, arrayData)
    assertEquals(array.size(), arrayData.numElements() + 1)
    assertEquals(array.get(array.size() - 1), 5)
  }

  @Test
  def testSparkArrayMutabilityReset(): Unit = {
    val array = SparkConverters.toTransportData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    array.add(5)
    array.asInstanceOf[PlatformData].setUnderlyingData(arrayData)
    // After underlying data is explicitly set, mutuable buffer should be removed
    assertSame(array.asInstanceOf[PlatformData].getUnderlyingData, arrayData)
  }
}
