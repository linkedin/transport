/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdStructType;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.trino.TrinoWrapper;
import io.trino.spi.type.RowType;
import java.util.List;
import java.util.stream.Collectors;


public class TrinoStructType implements StdStructType {

  final RowType rowType;

  public TrinoStructType(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public List<? extends StdType> fieldTypes() {
    return rowType.getFields().stream().map(f -> TrinoWrapper.createStdType(f.getType())).collect(Collectors.toList());
  }

  @Override
  public Object underlyingType() {
    return rowType;
  }
}
