package portable_udfs.examples.IsTestMemberId

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BooleanType, DataType}

case class IsTestMemberIdTwoArgsFunctionWrapper(expr1: Expression, expr2: Expression) extends Expression
  with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    //val arg1 = expr1.eval(input).asInstanceOf[Int]
    //val arg2 = expr2.eval(input).asInstanceOf[String]
    print("walaa")
    /*val files = SparkSession.builder.getOrCreate.conf.get("tmpfiles")
    files.split(',').foreach ( x => {
      try {
        val filename = Paths.get(x).getFileName.toString
        val from = Paths.get(SparkFiles.get(filename))
        print(from)
      } catch {
        case e: Exception => print("Exception while copying file: " + e)
      }
    })*/
    true
  }

  override def dataType: DataType = {
    SparkSession.builder().getOrCreate().sparkContext.addFile("/Users/wmoustaf/worspace/standard-udfs_trunk/standard-udfs-presto/etc/udf-config.properties")
    BooleanType
  }

  override def children: Seq[Expression] = Seq(expr1, expr2)

  override def equals(that: Any): Boolean = false
}