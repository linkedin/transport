package portable_udfs.examples.square

import org.apache.spark.sql.{SessionUtility, SparkSession}

case class R(intField: Int, longField: Long)

/**
  * Spark query to test SquareExpr Spark UDF wrapper
  */
object TestSquare {
  def query(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

    SessionUtility.registerSparkExpr(spark, "f", classOf[SquareExpr])

    import spark.implicits._
    import spark.sql

    List(
      R(1, 1l),
      R(2, 2l)
    ).toDF().createOrReplaceTempView("t")

    val df = sql("select f(intField), f(longField) from t")
    df.explain()
    /**
      * explain's output
      * == Physical Plan ==
      * LocalTableScan [squareexpr(intField)#15, squareexpr(longField)#16L]
      */
    df.collect().foreach(r => println(r))
  }


  def main(args: Array[String]): Unit = {
    query()
  }
}
