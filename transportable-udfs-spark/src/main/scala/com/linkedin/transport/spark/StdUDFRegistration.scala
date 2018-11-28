/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark

import org.apache.spark.sql.{Column, SparkSession, StdUDFUtils, functions}

/** A helper used for `com.linkedin.stdudfs.api.udf.StdUDF` registrations in Spark */
object StdUDFRegistration {

  /**
    * Registers a `com.linkedin.stdudfs.api.udf.StdUDF` in Spark so that it can be invoked through both the DataFrame
    * API and Spark SQL.
    *
    * For use through the Spark SQL API, invoke using the name passed as the argument to this method. For use through
    * the DataFrame API, use the [[SparkStdUDF]] object returned by this method or use
    * `org.apache.spark.sql.functions.callUDF()` along with the name registered. In either case, the input arguments to
    * the UDF will be passed as parameters to the methods.
    *
    * ===Examples===
    *
    * Scala
    * {{{
    * // Registration
    * val myUDF = StdUDFRegistration.register("my_udf", classOf[MyStdUDFWrapper])
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
    * SparkStdUDF myUDF = StdUDFRegistration.register("my_udf", MyStdUDFWrapper.class);
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
    * @param name               the name for the `com.linkedin.stdudfs.api.udf.StdUDF` which will be used to invoke it
    * @param stdUDFWrapperClass the Spark wrapper class for the `com.linkedin.stdudfs.api.udf.StdUDF`
    *
    * @return a [[SparkStdUDF]] which can be used to invoke the UDF in the DataFrame API
    */
  def register(name: String, stdUDFWrapperClass: Class[_ <: StdUdfWrapper]): SparkStdUDF = {
    register(name, stdUDFWrapperClass, SparkSession.builder().getOrCreate())
  }

  /**
    * Registers a `com.linkedin.stdudfs.api.udf.StdUDF` in a custom `org.apache.spark.sql.SparkSession` so that it can
    * be invoked through both the DataFrame API and Spark SQL.
    *
    * Same as `StdUDFRegistration.register(String,Class[_<:StdUdfWrapper])` but requires the
    * `org.apache.spark.sql.SparkSession` to be passed as an argument.
    * See `StdUDFRegistration.register(String,Class[_<:StdUdfWrapper])` for details
    *
    * @param name               the name for the `com.linkedin.stdudfs.api.udf.StdUDF` which will be used to invoke it
    * @param stdUDFWrapperClass the Spark wrapper class for the `com.linkedin.stdudfs.api.udf.StdUDF`
    * @param sparkSession       a `org.apache.spark.sql.SparkSession` to register the
    *                           `com.linkedin.stdudfs.api.udf.StdUDF` into
    *
    * @return a [[SparkStdUDF]] which can be used to invoke the UDF in the DataFrame API
    */
  def register(name: String, stdUDFWrapperClass: Class[_ <: StdUdfWrapper],
               sparkSession: SparkSession): SparkStdUDF = {
    StdUDFUtils.register(name, stdUDFWrapperClass, sparkSession)
    SparkStdUDF(name)
  }
}

/** A helper class to use the registered `com.linkedin.stdudfs.api.udf.StdUDF` in the DataFrame API */
case class SparkStdUDF(name: String) {

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

