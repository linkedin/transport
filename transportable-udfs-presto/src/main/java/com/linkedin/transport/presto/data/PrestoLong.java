/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.data;

import com.linkedin.transport.api.data.StdLong;
import io.prestosql.spi.block.BlockBuilder;

import static io.prestosql.spi.type.BigintType.*;


public class PrestoLong extends PrestoData implements StdLong {

  long _value;

  public PrestoLong(long value) {
    _value = value;
  }

  @Override
  public long get() {
    return _value;
  }

  @Override
  public Object getUnderlyingData() {
    return _value;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _value = (long) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    BIGINT.writeLong(blockBuilder, _value);
  }
}
