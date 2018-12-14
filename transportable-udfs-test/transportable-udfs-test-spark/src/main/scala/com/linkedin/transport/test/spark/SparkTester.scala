/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spark

import java.util

import com.linkedin.transport.api.StdFactory
import com.linkedin.transport.api.udf.{StdUDF, TopLevelStdUDF}
import com.linkedin.transport.spark.SparkFactory
import com.linkedin.transport.spark.typesystem.SparkBoundVariables
import com.linkedin.transport.test.spi.{SqlFunctionCallGenerator, SqlStdTester, ToPlatformTestOutputConverter}
import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, StdUDFTestUtils}
import org.testng.Assert

import scala.collection.JavaConversions._

class SparkTester extends SqlStdTester {

  private val _stdFactory: StdFactory = new SparkFactory(new SparkBoundVariables())
  private val _sparkSession: SparkSession = SparkSession.builder.master("local[1]").appName("transport-udfs").getOrCreate
  private val _sqlFunctionCallGenerator = new SparkSqlFunctionCallGenerator
  private val _testDataToOutputDataConverter = new ToSparkTestOutputConverter


  override def getStdFactory: StdFactory = _stdFactory

  override def getSqlFunctionCallGenerator: SqlFunctionCallGenerator = _sqlFunctionCallGenerator

  override def getToPlatformTestOutputConverter: ToPlatformTestOutputConverter = _testDataToOutputDataConverter

  override def assertFunctionCall(functionCallString: String, expectedOutputData: AnyRef,
    expectedOutputType: AnyRef): Unit = {
    val result =
      try {
        _sparkSession.sql("SELECT " + functionCallString).first
      } catch {
        case e: SparkException => throw e.getCause()
      }
    Assert.assertEquals(result.get(0), expectedOutputData)
    Assert.assertEquals(getModifiedResultType(result.schema.head.dataType), expectedOutputType)
  }

  override def setup(topLevelStdUDFClassesAndImplementations: util.Map[Class[_ <: TopLevelStdUDF],
    util.List[Class[_ <: StdUDF]]]): Unit = {
    topLevelStdUDFClassesAndImplementations.toMap.foreach(entry => {
      val functionName = entry._2.get(0).getConstructor().newInstance().asInstanceOf[TopLevelStdUDF].getFunctionName
      StdUDFTestUtils.register(functionName, entry._1, entry._2, _sparkSession)
    })
  }

  /**
    * Returns a modified [[DataType]] from the Spark query where nullability information is always set to true.
    * This is because the nullability information in the output [[DataType]] received from Spark is set based on the
    * runtime input values e.g. ARRAY(1, 2) will result in a [[DataType]] with `containsNull = false`.
    * [[com.linkedin.transport.api.types.StdType]] doesn't have a way to define nullability information for complex types.
    */
  private def getModifiedResultType(dataType: DataType): DataType = {
    dataType match {
      case a: ArrayType => ArrayType(getModifiedResultType(a.elementType), containsNull = true)
      case m: MapType => MapType(getModifiedResultType(m.keyType), getModifiedResultType(m.valueType),
        valueContainsNull = true)
      case s: StructType => StructType(s.map(x => StructField(x.name, getModifiedResultType(x.dataType),
        nullable = true)))
      case _ => dataType
    }
  }
}

