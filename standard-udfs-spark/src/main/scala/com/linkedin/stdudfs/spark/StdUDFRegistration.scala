package com.linkedin.stdudfs.spark

import com.linkedin.stdudfs.api.udf.StdUDF
import org.apache.spark.sql.{Column, StdUDFUtils, functions}

/** A helper used for [[StdUDF]] registrations in Spark */
object StdUDFRegistration {

  /**
    * Registers a [[StdUDF]] in Spark so that it can be invoked through both the DataFrame API and Spark SQL.
    *
    * For use through the Spark SQL API, invoke using the name passed as the argument to this method. For use through
    * the DataFrame API, use the [[SparkStdUDF]] object returned by this method or use [[functions.callUDF()]] along
    * with the name registered. In either case, the input arguments to the UDF will be passed as parameters to the
    * methods.
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
    * @param name               the name for the [[StdUDF]] which will be used to invoke it
    * @param stdUDFWrapperClass the Spark wrapper class for the [[StdUDF]]
    * @return a [[SparkStdUDF]] which can be used to invoke the UDF in the DataFrame API
    */

  def register(name: String, stdUDFWrapperClass: Class[_ <: StdUdfWrapper]): SparkStdUDF = {
    StdUDFUtils.register(name, stdUDFWrapperClass)
    SparkStdUDF(name)
  }
}

/** A helper class to use the registered [[StdUDF]] in the DataFrame API */
case class SparkStdUDF(name: String) {

  /**
    * Calls the UDF over the arguments passed
    *
    * @param columns the columns to be passed as arguments to the UDF
    * @return a [[Column]] containing the result of the UDF evaluation
    */
  @scala.annotation.varargs
  def apply(columns: Column*): Column = {
    functions.callUDF(name, columns: _*)
  }
}

