/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.types;

import com.linkedin.transport.api.types.StdStructType;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.presto.PrestoWrapper;
import io.prestosql.spi.type.RowType;
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
