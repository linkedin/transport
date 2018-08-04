package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.types.StdType;


public class PrestoType implements StdType {

  final Type type;

  public PrestoType(Type type) {
    this.type = type;
  }

  @Override
  public Object underlyingType() {
    return type;
  }
}
