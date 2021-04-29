/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.data;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.presto.PrestoWrapper;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import java.util.Iterator;

import static io.prestosql.spi.type.TypeUtils.*;


public class PrestoArrayData<E> extends PrestoData implements ArrayData<E> {

  private final StdFactory _stdFactory;
  private final ArrayType _arrayType;
  private final Type _elementType;

  private Block _block;
  private BlockBuilder _mutable;

  public PrestoArrayData(Block block, ArrayType arrayType, StdFactory stdFactory) {
    _block = block;
    _arrayType = arrayType;
    _elementType = arrayType.getElementType();
    _stdFactory = stdFactory;
  }

  public PrestoArrayData(ArrayType arrayType, int expectedEntries, StdFactory stdFactory) {
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
  public E get(int idx) {
    Block sourceBlock = _mutable == null ? _block : _mutable;
    int position = PrestoWrapper.checkedIndexToBlockPosition(sourceBlock, idx);
    Object element = readNativeValue(_elementType, sourceBlock, position);
    return (E) PrestoWrapper.createStdData(element, _elementType, _stdFactory);
  }

  @Override
  public void add(E e) {
    if (_mutable == null) {
      _mutable = _elementType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
    }
    PrestoWrapper.writeToBlock(e, _mutable);
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
  public Iterator<E> iterator() {
    return new Iterator<E>() {
      Block sourceBlock = _mutable == null ? _block : _mutable;
      int size = PrestoArrayData.this.size();
      int position = 0;

      @Override
      public boolean hasNext() {
        return position != size;
      }

      @Override
      public E next() {
        Object element = readNativeValue(_elementType, sourceBlock, position);
        position++;
        return (E) PrestoWrapper.createStdData(element, _elementType, _stdFactory);
      }
    };
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    _arrayType.writeObject(blockBuilder, getUnderlyingData());
  }
}
