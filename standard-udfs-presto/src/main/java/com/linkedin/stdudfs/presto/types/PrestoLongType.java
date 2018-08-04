package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.BigintType;
import com.linkedin.stdudfs.api.types.StdIntegerType;


public class PrestoLongType implements StdIntegerType {

  final BigintType bigintType;

  public PrestoLongType(BigintType bigintType) {
    this.bigintType = bigintType;
  }

  @Override
  public Object underlyingType() {
    return bigintType;
  }
}
