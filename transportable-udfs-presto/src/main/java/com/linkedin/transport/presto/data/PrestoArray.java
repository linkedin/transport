/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.data;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.presto.PrestoWrapper;
import java.util.Iterator;

import static com.facebook.presto.spi.type.TypeUtils.*;


public class PrestoArray extends PrestoData implements StdArray {

  private final StdFactory _stdFactory;
  private final ArrayType _arrayType;
  private final Type _elementType;

  private Block _block;
  private BlockBuilder _mutable;

  public PrestoArray(Block block, ArrayType arrayType, StdFactory stdFactory) {
    _block = block;
    _arrayType = arrayType;
    _elementType = arrayType.getElementType();
    _stdFactory = stdFactory;
  }

  public PrestoArray(ArrayType arrayType, int expectedEntries, StdFactory stdFactory) {
    _block = null;
    _elementType = arrayType.getElementType();
    _mutable = _elementType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), expectedEntries);
    _stdFactory = stdFactory;
    _arrayType = arrayType;
  }

  @Override
  public int size() {
    return _mutable == null ? _block.getPositionCount() : _mutable.getPositionCount();
  }

  @Override
  public StdData get(int idx) {
    Block sourceBlock = _mutable == null ? _block : _mutable;
    int position = PrestoWrapper.checkedIndexToBlockPosition(sourceBlock, idx);
    Object element = readNativeValue(_elementType, sourceBlock, position);
    return PrestoWrapper.createStdData(element, _elementType, _stdFactory);
  }

  @Override
  public void add(StdData e) {
    if (_mutable == null) {
      _mutable = _elementType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
    }
    ((PrestoData) e).writeToBlock(_mutable);
  }

  @Override
  public Object getUnderlyingData() {
    return _mutable == null ? _block : _mutable.build();
  }

  @Override
  public void setUnderlyingData(Object value) {
    _block = (Block) value;
  }

  @Override
  public Iterator<StdData> iterator() {
    return new Iterator<StdData>() {
      Block sourceBlock = _mutable == null ? _block : _mutable;
      int size = PrestoArray.this.size();
      int position = 0;

      @Override
      public boolean hasNext() {
        return position != size;
      }

      @Override
      public StdData next() {
        Object element = readNativeValue(_elementType, sourceBlock, position);
        position++;
        return PrestoWrapper.createStdData(element, _elementType, _stdFactory);
      }
    };
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    _arrayType.writeObject(blockBuilder, getUnderlyingData());
  }
}
