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
    val row = SparkConverters.toTransportData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    (0 until row.size).foreach(idx => {
      assertEquals(row.get(idx), idx)
    })
  }

  @Test
  def testSparkArrayAdd(): Unit = {
    val row = SparkConverters.toTransportData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    row.add(5)
    // Since original ArrayData is immutable, a mutable ArrayBuffer should be created and set as the underlying object
    assertNotSame(row.asInstanceOf[PlatformData].getUnderlyingData, arrayData)
    assertEquals(row.size(), arrayData.numElements() + 1)
    assertEquals(row.get(row.size() - 1), 5)
  }

  @Test
  def testSparkArrayMutabilityReset(): Unit = {
    val row = SparkConverters.toTransportData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    row.add(5)
    row.asInstanceOf[PlatformData].setUnderlyingData(arrayData)
    // After underlying data is explicitly set, mutuable buffer should be removed
    assertSame(row.asInstanceOf[PlatformData].getUnderlyingData, arrayData)
  }
}
