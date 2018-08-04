package portable_udfs.examples.nestedtransform

import org.apache.spark.sql.{SessionUtility, SparkSession}

case class B1(x: Int, y: String, z: Long)
case class A1(a: Map[String, B1])

/**
  * Spark query to test NestedTransformExpr Spark UDF wrapper
  */
object TestNestedTransform {
  def query(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

    SessionUtility.registerSparkExpr(spark, "f", classOf[NestedTransformExpr])

    import spark.implicits._
    import spark.sql

    List(
      A1(Map("k" -> B1(1, "x", 3L))),
      A1(Map("k" -> B1(2, "y", 4L)))
    ).toDF().createOrReplaceTempView("t")

    val df = sql("select f(a) from t")
    df.explain()
    df.collect().foreach(r => println(r))
  }

  def main(args: Array[String]): Unit = {
    query()
  }
}
