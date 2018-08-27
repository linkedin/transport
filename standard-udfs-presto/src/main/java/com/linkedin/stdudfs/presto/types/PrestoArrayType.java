package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.ArrayType;
import com.linkedin.stdudfs.api.types.StdArrayType;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.presto.PrestoWrapper;


public class PrestoArrayType implements StdArrayType {

  final ArrayType arrayType;

  public PrestoArrayType(ArrayType arrayType) {
    this.arrayType = arrayType;
  }

  @Override
  public StdType elementType() {
    return PrestoWrapper.createStdType(arrayType.getElementType());
  }

  @Override
  public Object underlyingType() {
    return arrayType;
  }
}
