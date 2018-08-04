package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.type.RowType;
import com.linkedin.stdudfs.api.types.StdStructType;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.presto.PrestoWrapper;
import java.util.List;
import java.util.stream.Collectors;


public class PrestoStructType implements StdStructType {

  final RowType rowType;

  public PrestoStructType(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public List<? extends StdType> fieldTypes() {
    return rowType.getFields().stream().map(f -> PrestoWrapper.createStdType(f.getType())).collect(Collectors.toList());
  }

  @Override
  public Object underlyingType() {
    return rowType;
  }
}
