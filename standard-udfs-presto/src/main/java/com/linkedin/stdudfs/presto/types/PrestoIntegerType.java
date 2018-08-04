package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.IntegerType;
import com.linkedin.stdudfs.api.types.StdIntegerType;


public class PrestoIntegerType implements StdIntegerType {

  final IntegerType integerType;

  public PrestoIntegerType(IntegerType integerType) {
    this.integerType = integerType;
  }

  @Override
  public Object underlyingType() {
    return integerType;
  }
}
