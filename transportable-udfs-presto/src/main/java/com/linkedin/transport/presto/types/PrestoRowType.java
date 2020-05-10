/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.RowType;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.trino.TrinoWrapper;
import io.trino.spi.type.RowType;
import java.util.List;
import java.util.stream.Collectors;


<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/types/TrinoStructType.java
public class TrinoStructType implements RowType {
  final io.prestosql.spi.type.RowType rowType;

  public TrinoStructType(RowType rowType) {
=======
public class PrestoRowType implements RowType {

  final io.prestosql.spi.type.RowType rowType;

  public PrestoRowType(io.prestosql.spi.type.RowType rowType) {
>>>>>>> 7695140 (Address review comments):transportable-udfs-presto/src/main/java/com/linkedin/transport/presto/types/PrestoRowType.java
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
