/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data
import com.linkedin.transport.api.data.{PlatformData}
import com.linkedin.transport.spark.{SparkFactory, SparkWrapper}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataTypes}
import org.testng.Assert.{assertEquals, assertNotSame, assertSame}
import org.testng.annotations.Test

class TestSparkArray {

  val stdFactory = new SparkFactory(null)
  val arrayData = ArrayData.toArrayData(Array.range(0, 5)) // scalastyle:ignore magic.number
  val arrayType = ArrayType(DataTypes.IntegerType)

  @Test
  def testCreateSparkArray(): Unit = {
    val stdArray = SparkWrapper.createStdData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    assertEquals(stdArray.size(), arrayData.numElements())
    assertSame(stdArray.asInstanceOf[PlatformData].getUnderlyingData, arrayData)
  }

  @Test
  def testSparkArrayGet(): Unit = {
    val stdArray = SparkWrapper.createStdData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    (0 until stdArray.size).foreach(idx => {
      assertEquals(stdArray.get(idx), idx)
    })
  }

  @Test
  def testSparkArrayAdd(): Unit = {
    val stdArray = SparkWrapper.createStdData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    stdArray.add(5)
    // Since original ArrayData is immutable, a mutable ArrayBuffer should be created and set as the underlying object
    assertNotSame(stdArray.asInstanceOf[PlatformData].getUnderlyingData, arrayData)
    assertEquals(stdArray.size(), arrayData.numElements() + 1)
    assertEquals(stdArray.get(stdArray.size() - 1), 5)
  }

  @Test
  def testSparkArrayMutabilityReset(): Unit = {
    val stdArray = SparkWrapper.createStdData(arrayData, arrayType).asInstanceOf[data.ArrayData[Integer]]
    stdArray.add(5)
    stdArray.asInstanceOf[PlatformData].setUnderlyingData(arrayData)
    // After underlying data is explicitly set, mutuable buffer should be removed
    assertSame(stdArray.asInstanceOf[PlatformData].getUnderlyingData, arrayData)
  }
}
