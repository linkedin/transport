/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.trino.types.TrinoStructType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTrinoTypes {

  @Test
  public void testStructType() {
    RowType.Field field1 = RowType.field("fieldOne", BigintType.BIGINT);
    RowType.Field field2 = RowType.field("fieldTwo", DoubleType.DOUBLE);
    TrinoStructType trinoStructType = new TrinoStructType(RowType.rowType(field1, field2));
    Assert.assertEquals(trinoStructType.fieldNames(), ImmutableList.of("fieldOne", "fieldTwo"));
    Assert.assertEquals(trinoStructType.fieldTypes().stream().map(StdType::underlyingType).collect(Collectors.toList()),
        ImmutableList.of(BigintType.BIGINT, DoubleType.DOUBLE));
  }
}
