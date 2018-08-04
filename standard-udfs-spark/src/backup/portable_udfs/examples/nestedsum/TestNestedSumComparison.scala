package portable_udfs.examples.nestedsum

import java.io.File

import org.apache.spark.sql.{SessionUtility, SparkSession}
import util.Utility

/**
  * Spark query to test NestedSumExpr Spark wrapper UDF
  */
object TestNestedSumComparison {
  def query(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

    SessionUtility.registerSparkExpr(spark, "f", classOf[NestedSumExprNative])

    import spark.sql
    val data = new File(s"data/identity_profile/avro/identity_profile1000k.avro").getAbsolutePath
    sql("drop table if exists t")
    sql(s"CREATE TEMPORARY TABLE t USING com.databricks.spark.avro OPTIONS (path '${data}')")

    val df = sql("select f(key), f(scn), f(timestamp), f(value) from t")
    Utility.time {
      df.write.mode("overwrite").json("build/j")
    }
  }


  def main(args: Array[String]): Unit = {
    (1 to 5).foreach { _ => query() }
  }
}
