package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.BooleanType;
import com.linkedin.stdudfs.api.types.StdBooleanType;


public class PrestoBooleanType implements StdBooleanType {

  final BooleanType booleanType;

  public PrestoBooleanType(BooleanType booleanType) {
    this.booleanType = booleanType;
  }

  @Override
  public Object underlyingType() {
    return booleanType;
  }
}
