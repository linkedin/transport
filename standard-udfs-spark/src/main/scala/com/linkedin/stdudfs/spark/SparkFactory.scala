package com.linkedin.stdudfs.spark

import java.util.{List => JavaList}

import com.google.common.base.Preconditions
import com.linkedin.stdudfs.api.StdFactory
import com.linkedin.stdudfs.api.data._
import com.linkedin.stdudfs.api.types._
import com.linkedin.stdudfs.spark.data._
import com.linkedin.stdudfs.spark.typesystem.SparkTypeFactory
import com.linkedin.stdudfs.typesystem.{AbstractBoundVariables, TypeSignature}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class SparkFactory(private val _boundVariables: AbstractBoundVariables[DataType]) extends StdFactory {

  private val _sparkTypeFactory: SparkTypeFactory = new SparkTypeFactory

  override def createInteger(value: Int): StdInteger = SparkInteger(value)

  override def createLong(value: Long): StdLong = SparkLong(value)

  override def createBoolean(value: Boolean): StdBoolean = SparkBoolean(value)

  override def createString(value: String): StdString = {
    Preconditions.checkNotNull(value, "Cannot create a null StdString".asInstanceOf[Any])
    SparkString(UTF8String.fromString(value))
  }

  override def createArray(stdType: StdType): StdArray = createArray(stdType, 0)

  // we do not pass size to `new Array()` as the size argument of createArray is supposed to be just a hint about
  // the expected number of entries in the StdArray. `new Array(size)` will create an array with null entries
  override def createArray(stdType: StdType, size: Int): StdArray = SparkArray(
    null, stdType.underlyingType().asInstanceOf[ArrayType]
  )

  override def createMap(stdType: StdType): StdMap = SparkMap(
    //TODO: make these as separate mutable standard spark types
    null, stdType.underlyingType().asInstanceOf[MapType]
  )

  override def createStruct(fieldTypes: JavaList[StdType]): StdStruct = {
    createStruct(null, fieldTypes)
  }

  override def createStruct(fieldNames: JavaList[String], fieldTypes: JavaList[StdType]): StdStruct = {
    val structFields = new Array[StructField](fieldTypes.size())
    (0 until fieldTypes.size()).foreach({
      idx => {
        structFields(idx) = StructField(
          if (fieldNames == null) "field" + idx else fieldNames.get(idx),
          fieldTypes.get(idx).underlyingType().asInstanceOf[DataType]
        )
      }
    })
    SparkStruct(null, StructType(structFields))
  }

  override def createStruct(stdType: StdType): StdStruct = {
    //TODO: make these as separate mutable standard spark types
    val structType: StructType = stdType.underlyingType().asInstanceOf[StructType]
    SparkStruct(null, structType)
  }

  override def createStdType(typeSignature: String): StdType = SparkWrapper.createStdType(
    _sparkTypeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables))

}
