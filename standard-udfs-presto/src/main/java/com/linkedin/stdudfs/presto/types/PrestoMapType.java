package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.MapType;
import com.linkedin.stdudfs.api.types.StdMapType;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.presto.PrestoWrapper;


public class PrestoMapType implements StdMapType {

  final MapType mapType;

  public PrestoMapType(MapType mapType) {
    this.mapType = mapType;
  }

  @Override
  public StdType keyType() {
    return PrestoWrapper.createStdType(mapType.getKeyType());
  }

  @Override
  public StdType valueType() {
    return PrestoWrapper.createStdType(mapType.getKeyType());
  }

  @Override
  public Object underlyingType() {
    return mapType;
  }
}
