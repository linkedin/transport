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

