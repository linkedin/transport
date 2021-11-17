/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.data;

import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.trino.TrinoConverters;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import com.linkedin.transport.api.data.ArrayData;
import java.util.Iterator;

import static io.trino.spi.type.TypeUtils.*;


public class TrinoArrayData<E> extends TrinoData implements ArrayData<E> {

  private final TypeFactory _typeFactory;
  private final ArrayType _arrayType;
  private final Type _elementType;

  private Block _block;
  private BlockBuilder _mutable;

  public TrinoArrayData(Block block, ArrayType arrayType, TypeFactory typeFactory) {
    _block = block;
    _arrayType = arrayType;
    _elementType = arrayType.getElementType();
    _typeFactory = typeFactory;
  }

  public TrinoArrayData(ArrayType arrayType, int expectedEntries, TypeFactory typeFactory) {
    _block = null;
    _elementType = arrayType.getElementType();
    _mutable = _elementType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), expectedEntries);
    _typeFactory = typeFactory;
    _arrayType = arrayType;
  }

  @Override
  public int size() {
    return _mutable == null ? _block.getPositionCount() : _mutable.getPositionCount();
  }

  @Override
  public E get(int idx) {
    Block sourceBlock = _mutable == null ? _block : _mutable;
    int position = TrinoConverters.checkedIndexToBlockPosition(sourceBlock, idx);
    Object element = readNativeValue(_elementType, sourceBlock, position);
    return (E) TrinoConverters.toTransportData(element, _elementType, _typeFactory);
  }

  @Override
  public void add(E e) {
    if (_mutable == null) {
      _mutable = _elementType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
    }
    TrinoConverters.writeToBlock(e, _mutable);
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
      int size = TrinoArrayData.this.size();
      int position = 0;

      @Override
      public boolean hasNext() {
        return position != size;
      }

      @Override
      public E next() {
        Object element = readNativeValue(_elementType, sourceBlock, position);
        position++;
        return (E) TrinoConverters.toTransportData(element, _elementType, _typeFactory);
      }
    };
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    _arrayType.writeObject(blockBuilder, getUnderlyingData());
  }
}
