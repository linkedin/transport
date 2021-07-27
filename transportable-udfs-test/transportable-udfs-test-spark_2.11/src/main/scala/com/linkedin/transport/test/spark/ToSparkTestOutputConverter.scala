/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spark

import java.nio.ByteBuffer
import java.util

import com.linkedin.transport.test.spi.{Row, ToPlatformTestOutputConverter}
import com.linkedin.transport.test.spi.types.TestType
import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.collection.JavaConversions._
import scala.collection.mutable.WrappedArray.make

class ToSparkTestOutputConverter extends ToPlatformTestOutputConverter {

  /**
    * Returns a [[scala.collection.mutable.WrappedArray]] for the given [[util.List]] while also converting nested
    * elements
    */
  override def getArrayData(array: util.List[AnyRef], elementType: TestType): AnyRef = make(
    array.map(convertToTestOutput(_, elementType)).toArray
  )

  /**
    * Returns a [[Map]] for the given [[util.Map]] while also converting nested elements
    */
  override def getMapData(map: util.Map[AnyRef, AnyRef], mapKeyType: TestType, mapValueType: TestType): AnyRef =
    map.map(entry => (convertToTestOutput(entry._1, mapKeyType), convertToTestOutput(entry._2, mapValueType))).toMap

  /**
    * Returns a [[GenericRow]] for the given [[com.linkedin.transport.test.spi.Row]] while also converting nested elements
    */
  override def getStructData(struct: Row, fieldTypes: util.List[TestType],
    fieldNames: util.List[String]): AnyRef = {
    new GenericRow(0.until(struct.getFields.size).map(i => convertToTestOutput(
      struct.getFields.get(i), fieldTypes.get(i))).toArray[Any])
  }

  override def getBinaryData(value: ByteBuffer): AnyRef = value.array()
}
