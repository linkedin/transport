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
package com.linkedin.stdudfs.spark

import com.linkedin.stdudfs.api.data.PlatformData
import com.linkedin.stdudfs.spark.typesystem.{SparkBoundVariables, SparkTypeFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.testng.Assert._
import org.testng.annotations.Test

import scala.collection.JavaConverters._

class TestSparkFactory {

  val typeFactory: SparkTypeFactory = new SparkTypeFactory
  val stdFactory = new SparkFactory(new SparkBoundVariables)

  @Test
  def testCreatePrimitives(): Unit = {
    assertEquals(stdFactory.createInteger(1).get(), 1)
    assertEquals(stdFactory.createLong(1L).get(), 1L)
    assertEquals(stdFactory.createBoolean(true).get(), true)
    assertEquals(stdFactory.createString("").get(), "")
  }

  @Test
  def testCreateArray(): Unit = {
    var stdArray = stdFactory.createArray(stdFactory.createStdType("array(integer)"))
    assertEquals(stdArray.size(), 0)
    assertEquals(stdArray.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[GenericArrayData].array,
      Array.empty)
    val testArraySize = 10
    stdArray = stdFactory.createArray(stdFactory.createStdType("array(integer)"), testArraySize)
    // size should still be 0, since size passed in createArray is just expected number of entries in the future
    assertEquals(stdArray.size(), 0)
    assertEquals(stdArray.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[GenericArrayData].array,
      Array.empty)
  }

  @Test
  def testCreateMap(): Unit = {
    val stdMap = stdFactory.createMap(stdFactory.createStdType("map(varchar,bigint)"))
    assertEquals(stdMap.size(), 0)
    assertEquals(stdMap.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[ArrayBasedMapData].keyArray.array,
      Array.empty)
    assertEquals(stdMap.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[ArrayBasedMapData].valueArray.array,
      Array.empty)
  }

  @Test
  def testCreateStructFromStdType(): Unit = {
    val fieldNames = Array("strField", "intField", "longField", "boolField", "arrField")
    val fieldTypes = Array("varchar", "integer", "bigint", "boolean", "array(integer)")

    val stdStruct = stdFactory.createStruct(stdFactory.createStdType(fieldNames.zip(fieldTypes).map(x => x._1 + " " + x._2).mkString("row(", ", ", ")")))
    val internalRow = stdStruct.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[InternalRow]
    assertEquals(internalRow.numFields, fieldTypes.length)
    (0 until 5).foreach(idx => {
      assertEquals(internalRow.get(idx, stdFactory.createStdType(fieldTypes(idx)).underlyingType().asInstanceOf[DataType]), null)
    })
  }

  @Test
  def testCreateStructFromFieldNamesAndTypes(): Unit = {
    val fieldNames = Array("strField", "intField", "longField", "boolField", "arrField")
    val fieldTypes = Array("varchar", "integer", "bigint", "boolean", "array(integer)")

    val stdStruct = stdFactory.createStruct(fieldNames.toList.asJava, fieldTypes.map(stdFactory.createStdType).toList.asJava)
    val internalRow = stdStruct.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[InternalRow]
    assertEquals(internalRow.numFields, fieldTypes.length)
    (0 until 5).foreach(idx => {
      assertEquals(internalRow.get(idx, stdFactory.createStdType(fieldTypes(idx)).underlyingType().asInstanceOf[DataType]), null)
    })
  }

  @Test
  def testCreateStructFromFieldTypes(): Unit = {
    val fieldTypes = Array("varchar", "integer", "bigint", "boolean", "array(integer)")

    val stdStruct = stdFactory.createStruct(fieldTypes.map(stdFactory.createStdType).toList.asJava)
    val internalRow = stdStruct.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[InternalRow]
    assertEquals(internalRow.numFields, fieldTypes.length)
    (0 until 5).foreach(idx => {
      assertEquals(internalRow.get(idx, stdFactory.createStdType(fieldTypes(idx)).underlyingType().asInstanceOf[DataType]), null)
    })
  }
}
