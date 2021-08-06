/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{PlatformData, StdStruct}
import com.linkedin.transport.spark.{SparkFactory, SparkWrapper}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.testng.Assert.{assertEquals, assertNotSame, assertSame}
import org.testng.annotations.Test

class TestSparkStruct {
  val stdFactory = new SparkFactory(null)
  val dataArray = Array(UTF8String.fromString("str1"), 0, 2L, false, ArrayData.toArrayData(Array.range(0, 5))) // scalastyle:ignore magic.number
  val fieldNames = Array("strField", "intField", "longField", "boolField", "arrField")
  val fieldTypes = Array(DataTypes.StringType, DataTypes.IntegerType, DataTypes.LongType, DataTypes.BooleanType,
    ArrayType(DataTypes.IntegerType))
  val structData = InternalRow.fromSeq(dataArray)
  val structType = StructType(fieldNames.indices.map(idx => StructField(fieldNames(idx), fieldTypes(idx))))

  @Test
  def testCreateSparkStruct(): Unit = {
    val stdStruct = SparkWrapper.createStdData(structData, structType).asInstanceOf[StdStruct]
    assertSame(stdStruct.asInstanceOf[PlatformData].getUnderlyingData, structData)
  }

  @Test
  def testSparkStructGetField(): Unit = {
    val stdStruct = SparkWrapper.createStdData(structData, structType).asInstanceOf[StdStruct]
    dataArray.indices.foreach(idx => {
      assertEquals(stdStruct.getField(idx).asInstanceOf[PlatformData].getUnderlyingData, dataArray(idx))
      assertEquals(stdStruct.getField(fieldNames(idx)).asInstanceOf[PlatformData].getUnderlyingData, dataArray(idx))
    })
  }

  @Test
  def testSparkStructFields(): Unit = {
    val stdStruct = SparkWrapper.createStdData(structData, structType).asInstanceOf[StdStruct]
    assertEquals(stdStruct.fields().size(), structData.numFields)
    assertEquals(stdStruct.fields().toArray.map(f => f.asInstanceOf[PlatformData].getUnderlyingData), dataArray)
  }

  @Test
  def testSparkStructSetField(): Unit = {
    val stdStruct = SparkWrapper.createStdData(structData, structType).asInstanceOf[StdStruct]
    stdStruct.setField(1, stdFactory.createInteger(1))
    assertEquals(stdStruct.getField(1).asInstanceOf[PlatformData].getUnderlyingData, 1)
    stdStruct.setField(fieldNames(2), stdFactory.createLong(5)) // scalastyle:ignore magic.number
    assertEquals(stdStruct.getField(fieldNames(2)).asInstanceOf[PlatformData].getUnderlyingData, 5L) // scalastyle:ignore magic.number
    // Since original InternalRow is immutable, a mutable ArrayBuffer should be created and set as the underlying object
    assertNotSame(stdStruct.asInstanceOf[PlatformData].getUnderlyingData, structData)
  }

  @Test
  def testSparkStructMutabilityReset(): Unit = {
    val stdStruct = SparkWrapper.createStdData(structData, structType).asInstanceOf[StdStruct]
    stdStruct.setField(1, stdFactory.createInteger(1))
    stdStruct.asInstanceOf[PlatformData].setUnderlyingData(structData)
    // After underlying data is explicitly set, mutable buffer should be removed
    assertSame(stdStruct.asInstanceOf[PlatformData].getUnderlyingData, structData)
  }
}
