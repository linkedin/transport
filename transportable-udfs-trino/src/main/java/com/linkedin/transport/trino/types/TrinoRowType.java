/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.api.types.RowType;
import com.linkedin.transport.trino.TrinoWrapper;
import java.util.List;
import java.util.stream.Collectors;


public class TrinoRowType implements RowType {

  final io.trino.spi.type.RowType rowType;

  public TrinoRowType(io.trino.spi.type.RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public List<? extends DataType> fieldTypes() {
    return rowType.getFields().stream().map(f -> TrinoWrapper.createStdType(f.getType())).collect(Collectors.toList());
  }

  @Override
  public Object underlyingType() {
    return rowType;
  }
}
