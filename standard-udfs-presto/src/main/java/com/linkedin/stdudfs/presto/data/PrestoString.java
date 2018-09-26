/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.presto.data;

import com.facebook.presto.spi.block.BlockBuilder;
import com.linkedin.stdudfs.api.data.StdString;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;


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
