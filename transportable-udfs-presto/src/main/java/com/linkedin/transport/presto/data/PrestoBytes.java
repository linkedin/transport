/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.data;

import com.linkedin.transport.api.data.StdBytes;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.BlockBuilder;
import java.nio.ByteBuffer;

import static io.prestosql.spi.type.VarbinaryType.*;

public class PrestoBytes extends PrestoData implements StdBytes {

  Slice _slice;

  public PrestoBytes(Slice slice) {
    _slice = slice;
  }

  @Override
  public ByteBuffer get() {
    return _slice.toByteBuffer();
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
    VARBINARY.writeSlice(blockBuilder, _slice);
  }
}
