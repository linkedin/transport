package portable_udfs.examples.nestedsum

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._

case class NestedSumExprNative(s: Expression) extends Expression with CodegenFallback {
  private val _udf = new NestedSumPortableUDF()

  override def nullable: Boolean = true

  def nestedSum(col: Any, dt: DataType): Int = {
    if (col == null) {
      return 0
    }
    var r = 0
    col match {
      case _ : MapData => {
        val md = col.asInstanceOf[MapData]
        val mdt = dt.asInstanceOf[MapType]
        (0 until md.numElements()).foreach(idx => {
          r += nestedSum(md.valueArray.get(idx, mdt.valueType), mdt.valueType)
        })
      }
      case _ : InternalRow => {
        val ir = col.asInstanceOf[InternalRow]
        val rdt = dt.asInstanceOf[StructType]
        for ((f, idx) <- rdt.fields.zipWithIndex) {
          r += nestedSum(ir.get(idx, f.dataType), f.dataType)
        }
      }

      case _ : ArrayData => {
        val ad = col.asInstanceOf[ArrayData]
        val adt = dt.asInstanceOf[ArrayType]
        (0 until ad.numElements()).foreach(idx => {
          r += nestedSum(ad.get(idx, adt.elementType), adt.elementType)
        })
      }

      case _ : Int => r += col.asInstanceOf[Int]
      case _ : Long => r += col.asInstanceOf[Long].toInt
      case _ : Any => r += 0
    }
    r
  }

  override def eval(input: InternalRow): Any = {
    val col = s.eval(input)

    val r = nestedSum(col, s.dataType)
    r
  }

  override def dataType: DataType = {
    DataTypes.IntegerType
  }

  override def children: Seq[Expression] = Seq(s)
}
