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

import com.linkedin.stdudfs.api.data.{PlatformData, StdStruct}
import com.linkedin.stdudfs.spark.{SparkFactory, SparkWrapper}
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
