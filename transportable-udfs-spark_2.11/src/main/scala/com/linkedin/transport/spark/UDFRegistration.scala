/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark

import org.apache.spark.sql.{Column, SparkSession, TransportUDFUtils, functions}

/** A helper used for `com.linkedin.transport.api.udf.UDF` registrations in Spark */
object UDFRegistration {

  /**
    * Registers a `com.linkedin.transport.api.udf.UDF` in Spark so that it can be invoked through both the DataFrame
    * API and Spark SQL.
    *
    * For use through the Spark SQL API, invoke using the name passed as the argument to this method. For use through
    * the DataFrame API, use the [[SparkTransportUDF]] object returned by this method or use
    * `org.apache.spark.sql.functions.callUDF()` along with the name registered. In either case, the input arguments to
    * the UDF will be passed as parameters to the methods.
    *
    * ===Examples===
    *
    * Scala
    * {{{
    * // Registration
    * val myUDF = UDFRegistration.register("my_udf", classOf[MySparkUDF])
    *
    * // Use in Spark SQL
    * spark.sql("SELECT column1, my_udf(column2, column3) FROM temp")
    *
    * // Use in DataFrame API
    * dataframe.withColumn("result", myUDF($"column2", $"column3"))
    *
    * // Use in DataFrame API (Alternate)
    * import org.apache.spark.sql.functions.callUDF
    * dataframe.withColumn("result", callUDF("my_udf", $"column2", $"column3"))
    * }}}
    *
    * Java
    * {{{
    * import static org.apache.spark.sql.functions.*;
    * // Registration
    * SparkTransportUDF myUDF = UDFRegistration.register("my_udf", MySparkUDF.class);
    *
    * // Use in Spark SQL
    * spark.sql("SELECT column1, my_udf(column2, column3) FROM temp");
    *
    * // Use in DataFrame API
    * df.withColumn("result", myUDF.apply(col("column2"), col("column3")))
    *
    * // Use in DataFrame API (Alternate)
    * df.withColumn("result", callUDF("my_udf", col("column2"), col("column3")))
    * }}}
    *
    * @param name          the name for the `com.linkedin.transport.api.udf.UDF` which will be used to invoke it
    * @param sparkUDFClass the Spark wrapper class for the `com.linkedin.transport.api.udf.UDF`
    * @return a [[SparkTransportUDF]] which can be used to invoke the UDF in the DataFrame API
    */
  def register(name: String, sparkUDFClass: Class[_ <: SparkUDF]): SparkTransportUDF = {
    register(name, sparkUDFClass, SparkSession.builder().getOrCreate())
  }

  /**
    * Registers a `com.linkedin.transport.api.udf.UDF` in a custom `org.apache.spark.sql.SparkSession` so that it can
    * be invoked through both the DataFrame API and Spark SQL.
    *
    * Same as `UDFRegistration.register(String,Class[_<:SparkUDF])` but requires the
    * `org.apache.spark.sql.SparkSession` to be passed as an argument.
    * See `UDFRegistration.register(String,Class[_<:SparkUDF])` for details
    *
    * @param name          the name for the `com.linkedin.transport.api.udf.UDF` which will be used to invoke it
    * @param sparkUDFClass the Spark wrapper class for the `com.linkedin.transport.api.udf.UDF`
    * @param sparkSession  a `org.apache.spark.sql.SparkSession` to register the
    *                        `com.linkedin.transport.api.udf.UDF` into
    * @return a [[SparkTransportUDF]] which can be used to invoke the UDF in the DataFrame API
    */
  def register(name: String, sparkUDFClass: Class[_ <: SparkUDF],
               sparkSession: SparkSession): SparkTransportUDF = {
    TransportUDFUtils.register(name, sparkUDFClass, sparkSession)
    SparkTransportUDF(name)
  }
}

/** A helper class to use the registered `com.linkedin.transport.api.udf.UDF` in the DataFrame API */
case class SparkTransportUDF(name: String) {

  /**
    * Calls the UDF over the arguments passed
    *
    * @param columns the columns to be passed as arguments to the UDF
    * @return a `org.apache.spark.sql.Column` containing the result of the UDF evaluation
    */
  @scala.annotation.varargs
  def apply(columns: Column*): Column = {
    functions.callUDF(name, columns: _*)
  }
}

