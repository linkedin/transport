package portable_udfs.examples.profilesections

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType}
import portable_udfs.spark.data.{StdSparkArray, StdSparkMap}

/**
  * This wrapper will be generated programmtically eventually.
  *
  * @param fields
  * @param fieldsOrder
  */
case class GetProfileSectionExpr(fields: Expression, fieldsOrder: Expression) extends Expression with CodegenFallback {

  val _udf: PortableGetProfileSectionUDF = new PortableGetProfileSectionUDF

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val map = fields.eval(input).asInstanceOf[MapData]
    val keys = fieldsOrder.eval(input).asInstanceOf[ArrayData]

    val r = _udf.apply(
      new StdSparkMap(map, fields.dataType.asInstanceOf[MapType]),
      new StdSparkArray(keys, fieldsOrder.dataType.asInstanceOf[ArrayType]))

    // no conversion happened, we built the platform specific array
    // and filled it with complex struct without converting them
    r.getUnderlyingData
  }

  override def dataType: DataType = {
    assert(fields.dataType.isInstanceOf[MapType])
    assert(fieldsOrder.dataType.isInstanceOf[ArrayType])

    val valType = fields.dataType.asInstanceOf[MapType].valueType
    new ArrayType(valType, true)
  }

  override def children: Seq[Expression] = Seq(fields, fieldsOrder)
}
