/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.data;

import com.linkedin.transport.api.data.StdString;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.BlockBuilder;

import static io.prestosql.spi.type.VarcharType.*;


public class PrestoString extends PrestoData implements StdString {

  Slice _slice;

  public PrestoString(Slice slice) {
    _slice = slice;
  }

  @Override
  public String get() {
    return _slice.toStringUtf8();
  }

  @Override
  public Object getUnderlyingData() {
    return _slice;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _slice = (Slice) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    VARCHAR.writeSlice(blockBuilder, _slice);
  }
}
